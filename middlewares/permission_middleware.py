from pathlib import Path
import os
import time
import asyncio
from uuid import uuid4
from langchain_core.messages import ToolMessage
from langchain.agents.middleware import AgentMiddleware

DANGEROUS_PATTERNS = [
    {"tool": "bash", "pattern": "rm -rf"},
    {"tool": "bash", "pattern": "sudo"},
    {"tool": "bash", "pattern": "chmod 777"},
    {"tool": "bash", "pattern": "curl.*\\|.*sh"},
    {"tool": "bash", "pattern": "wget.*\\|.*sh"},
    {"tool": "write_file", "pattern": "/etc/"},
    {"tool": "write_file", "pattern": "/usr/"},
    {"tool": "edit_file", "pattern": "/etc/"},
    {"tool": "edit_file", "pattern": "/usr/"},
]

class PermissionMiddleware(AgentMiddleware):
    """Middleware to check permissions before tool execution."""
    def __init__(
        self,
        work_dir: Path = Path(os.getcwd()),
        message_hub=None,
        agent_name: str = "default",
    ):
        self.WORK_DIR = work_dir
        self.DENY_LIST = ["rm -rf /", "sudo", "shutdown", "reboot", "mkfs", "dd if=", "> /dev/sda"]
        self.PERMISSION_RULES = [
        {"tools": ["write_file", "edit_file"],
        "check": lambda args: not (self.WORK_DIR / args.get("file_path", args.get("path", ""))).resolve().is_relative_to(self.WORK_DIR),
        "message": "Writing outside workspace"},
        {"tools": ["bash"],
        "check": lambda args: any(kw in args.get("command", "") for kw in ["rm ", "> /etc/", "chmod 777"]),
        "message": "Potentially dangerous command"},
    ]
        self.message_hub = message_hub
        self.agent_name = agent_name
        

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
    
    async def _wait_for_permission_decision(
        self,
        request_id: str,
        timeout: int = 300,
        poll_interval: float = 0.5,
    ) -> dict:
        """轮询等待审批决定（每 500ms 检查收件箱）"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            await asyncio.sleep(poll_interval)
            
            if self.message_hub:
                messages = await self.message_hub.read_inbox(self.agent_name)
                for msg in messages:
                    if msg["msg_type"] == "permission_response":
                        content = msg["content"]
                        if content.get("request_id") == request_id:
                            return content
            
            if self.message_hub is None:
                await asyncio.sleep(0.1)
        
        return {"decision": "rejected", "reason": "Timeout waiting for approval"}
    
    async def awrap_tool_call(self, request, handler):
        """Intercept tool calls and apply permission gates."""
        tool_name = request.tool_call["name"]
        tool_args = request.tool_call.get("args", {})
        tool_id = request.tool_call["id"]
        
        if tool_name == "bash":
            command = tool_args.get("command", "")
            reason = self.check_deny_list(command)
            if reason:
                print(f"\n\033[31m⛔ {reason}\033[0m")
                return ToolMessage(content=f"Permission denied: {reason}", tool_call_id=tool_id)
        
        for rule in DANGEROUS_PATTERNS:
            if tool_name == rule["tool"]:
                cmd = tool_args.get("command", "") or tool_args.get("path", "")
                if rule["pattern"] in cmd:
                    if self.message_hub:
                        request_id = f"perm_{uuid4().hex[:8]}"
                        
                        await self.message_hub.send(
                            from_agent=self.agent_name,
                            to_agent="lead",
                            content={
                                "request_id": request_id,
                                "tool": tool_name,
                                "command": cmd,
                                "reason": f"Operation matches dangerous pattern: {rule['pattern']}",
                            },
                            msg_type="permission_request",
                        )
                        
                        await self.message_hub.log_permission_request(
                            request_id=request_id,
                            agent_name=self.agent_name,
                            tool_name=tool_name,
                            command=cmd,
                        )
                        
                        decision = await self._wait_for_permission_decision(request_id)
                        
                        await self.message_hub.log_permission_decision(
                            request_id=request_id,
                            decision=decision["decision"],
                            reason=decision.get("reason"),
                            decided_by="user",
                        )
                        
                        if decision["decision"] != "approved":
                            return ToolMessage(
                                content=f"Permission denied: {decision.get('reason', 'No reason provided')}",
                                tool_call_id=tool_id
                            )
                        
                        break
                    else:
                        reason = f"Operation matches dangerous pattern: {rule['pattern']}"
                        if not self.ask_user(tool_name, tool_args, reason):
                            print(f"\n\033[31m⛔ Permission denied by user\033[0m")
                            return ToolMessage(content="Permission denied by user", tool_call_id=tool_id)
        
        reason = self.check_rules(tool_name, tool_args)
        if reason:
            if not self.ask_user(tool_name, tool_args, reason):
                print(f"\n\033[31m⛔ Permission denied by user\033[0m")
                return ToolMessage(content="Permission denied by user", tool_call_id=tool_id)
        
        return await handler(request)