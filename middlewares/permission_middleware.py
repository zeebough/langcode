from pathlib import Path
import os
from langchain_core.messages import ToolMessage
from langchain.agents.middleware import AgentMiddleware

# ═══════════════════════════════════════════════════════════
#  Permission Middleware for LangChain Agent
# ═══════════════════════════════════════════════════════════

class PermissionMiddleware(AgentMiddleware):
    """Middleware to check permissions before tool execution."""
    def __init__(self, work_dir: Path = Path(os.getcwd())):
        self.WORK_DIR = work_dir
        self.DENY_LIST = ["rm -rf /", "sudo", "shutdown", "reboot", "mkfs", "dd if=", "> /dev/sda"]
        self.PERMISSION_RULES = [
        {"tools": ["write_file", "edit"],
        "check": lambda args: not (self.WORK_DIR / args.get("file_path", args.get("path", ""))).resolve().is_relative_to(self.WORK_DIR),
        "message": "Writing outside workspace"},
        {"tools": ["bash"],
        "check": lambda args: any(kw in args.get("command", "") for kw in ["rm ", "> /etc/", "chmod 777"]),
        "message": "Potentially destructive command"},
    ]
        

    # ═══════════════════════════════════════════════════════════
    #  Permission System - Three-Gate Pipeline
    # ═══════════════════════════════════════════════════════════

    # Gate 1: Hard deny list — always forbidden
    def check_deny_list(self,command: str) -> str | None:
        for pattern in self.DENY_LIST:
            if pattern in command:
                return f"Blocked: '{pattern}' is on the deny list"
        return None

    # Gate 2: Rule matching — context-dependent checks
    def check_rules(self,tool_name: str, args: dict) -> str | None:
        for rule in self.PERMISSION_RULES:
            if tool_name in rule["tools"] and rule["check"](args):
                return rule["message"]
        return None

    # Gate 3: User approval — wait for confirmation after rule match
    def ask_user(self, tool_name: str, args: dict, reason: str) -> bool:
        print(f"\n\033[33m⚠  {reason}\033[0m")
        print(f"   Tool: {tool_name}({args})")
        choice = input("   Allow? [y/N] ").strip().lower()
        return choice in ("y", "yes")
    
    async def awrap_tool_call(self, request, handler):
        """Intercept tool calls and apply permission gates."""
        tool_name = request.tool_call["name"]
        tool_args = request.tool_call.get("args", {})
        tool_id = request.tool_call["id"]
        
        # Gate 1: Deny list (for bash commands)
        if tool_name == "bash":
            command = tool_args.get("command", "")
            reason = self.check_deny_list(command)
            if reason:
                print(f"\n\033[31m⛔ {reason}\033[0m")
                return ToolMessage(content=f"Permission denied: {reason}", tool_call_id=tool_id)
        
        # Gate 2: Rule matching
        reason = self.check_rules(tool_name, tool_args)
        if reason:
            # Gate 3: User approval
            if not self.ask_user(tool_name, tool_args, reason):
                print(f"\n\033[31m⛔ Permission denied by user\033[0m")
                return ToolMessage(content="Permission denied by user", tool_call_id=tool_id)
        
        # All checks passed, execute tool
        return await handler(request)