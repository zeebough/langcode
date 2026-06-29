import time
from pathlib import Path
from typing import List, Dict, Optional
from langchain.agents.middleware import AgentMiddleware, ModelRequest, Runtime
from langchain.agents.middleware.types import AgentState
from langchain_core.messages import SystemMessage


class SkillLoadingMiddleware(AgentMiddleware):
    """Middleware to load skills from the SKILL_DIR and inject into system prompt.
    
    Features:
    - Dynamic scanning of skill directory
    - Frontmatter parsing (YAML) for skill metadata
    - Caching: only re-scan when directory changes
    - Hot-pluggable: skills can be added/removed without restart
    """
    
    def __init__(self, work_dir: Path, skill_dir: Optional[Path] = None):
        self.work_dir = work_dir
        self.skill_dir = skill_dir or (work_dir / "skills")
        self._skill_cache: Dict[str, Dict] = {}
        self._cache_timestamp: float = 0
        self._cache_dir_hash: str = ""
    
    def _compute_dir_hash(self, dir_path: Path) -> str:
        """Compute a hash of directory structure for change detection."""
        if not dir_path.exists() or not dir_path.is_dir():
            return ""
        
        entries = []
        for item in sorted(dir_path.iterdir()):
            mtime = item.stat().st_mtime if item.exists() else 0
            entries.append(f"{item.name}:{mtime}")
        return "|".join(entries)
    
    def _parse_frontmatter(self, content: str) -> Dict[str, str]:
        """Parse YAML frontmatter from skill file content."""
        if not content.startswith("---"):
            return {}
        
        end_fm = content.find("\n---", 3)
        if end_fm == -1:
            return {}
        
        fm_text = content[3:end_fm].strip()
        result = {}
        
        current_key = None
        current_value = []
        multiline = False
        
        for line in fm_text.split("\n"):
            if not line.strip():
                continue
            
            if line.startswith(" ") or line.startswith("\t"):
                if multiline and current_key:
                    current_value.append(line.strip())
                continue
            
            if ":" in line:
                if current_key and current_value:
                    result[current_key] = " ".join(current_value) if not multiline else "\n".join(current_value)
                
                key, _, value = line.partition(":")
                key = key.strip()
                value = value.strip()
                
                if value.startswith("|") or value.startswith(">"):
                    multiline = True
                    current_value = []
                else:
                    multiline = False
                    current_value = [value] if value else []
                
                current_key = key
        
        if current_key and current_value:
            result[current_key] = " ".join(current_value) if not multiline else "\n".join(current_value)
        
        return result
    
    # 仅将frontmatter注入system prompt
    def _load_skill(self, skill_path: Path) -> Optional[Dict]:
        """Load a single skill's frontmatter from its directory."""
        skill_file = skill_path / "SKILL.md"
        if not skill_file.exists():
            return None
        
        try:
            content = skill_file.read_text(encoding="utf-8")
            frontmatter = self._parse_frontmatter(content)
            
            #end_fm = content.find("\n---", 3)
            #skill_content = content[end_fm + 4:].strip() if end_fm != -1 else content
            
            return {
                "name": frontmatter.get("name", skill_path.name),
                "description": frontmatter.get("description", ""),
                # "content": skill_content,
                # "path": str(skill_path),
                # "enabled": frontmatter.get("enabled", "true").lower() != "false"
            }
        except Exception:
            return None
    
    def _scan_skills(self) -> Dict[str, Dict]:
        """Scan skill directory and load all skills."""
        skills = {}
        
        if not self.skill_dir.exists() or not self.skill_dir.is_dir():
            return skills
        
        for item in self.skill_dir.iterdir():
            if item.is_dir() and not item.name.startswith("."):
                skill_data = self._load_skill(item)
                if skill_data and skill_data.get("enabled", True):
                    skills[skill_data["name"]] = skill_data
        
        return skills
    
    def _refresh_cache_if_needed(self) -> bool:
        """Refresh skill cache if directory has changed."""
        current_hash = self._compute_dir_hash(self.skill_dir)
        
        if current_hash != self._cache_dir_hash:
            self._skill_cache = self._scan_skills()
            self._cache_dir_hash = current_hash
            self._cache_timestamp = time.time()
            return True
        return False
    
    def _build_skill_prompt(self) -> str:
        """Build system prompt section from loaded skills."""
        if not self._skill_cache:
            return ""
        
        sections = []
        for name, skill in self._skill_cache.items():
            desc = skill.get("description", "")
            #content = skill.get("content", "")
            sections.append(f"""
{name}: {desc}
""")
            # {content}
        
        if not sections:
            return ""
        
        return "\n\nAvailable Skills:\n" + "\n".join(sections) + "\n"
    
    async def modify_model_request(
        self,
        request: ModelRequest,
        state: AgentState,
        runtime: Runtime,
    ) -> ModelRequest:
        """Inject skills into system prompt before model call."""
        self._refresh_cache_if_needed()
        
        skill_prompt = self._build_skill_prompt()
        if not skill_prompt:
            return request
        
        system_message = request.system_message
        if system_message is None:
            request = request.override(
                system_message=SystemMessage(content=skill_prompt)
            )
        else:
            existing_blocks = list(system_message.content_blocks)
            existing_blocks.append({"type": "text", "text": skill_prompt})
            request = request.override(
                system_message=SystemMessage(content_blocks=existing_blocks)
            )
        
        return request
    
    def get_skill_names(self) -> List[str]:
        """Get list of available skill names."""
        self._refresh_cache_if_needed()
        return list(self._skill_cache.keys())
    
    def get_skill(self, name: str) -> Optional[Dict]:
        """Get a specific skill by name."""
        self._refresh_cache_if_needed()
        return self._skill_cache.get(name)
    
    def clear_cache(self):
        """Force clear the skill cache."""
        self._skill_cache = {}
        self._cache_dir_hash = ""
        self._cache_timestamp = 0
