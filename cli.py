# cli.py
import os
import subprocess
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import AIMessage
from langchain.agents import create_agent
from langchain.agents.middleware import TodoListMiddleware
from langgraph.store.postgres import AsyncPostgresStore
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from typing import Any

from middlewares.context_compression_middleware import ContextCompressionMiddleware
from middlewares.memory_management_middleware import MemoryManagementMiddleware
from middlewares.permission_middleware import PermissionMiddleware
from middlewares.skill_loading_middleware import SkillLoadingMiddleware
from middlewares.error_recovery_middleware import ErrorRecoveryMiddleware
from lib.message_hub import AsyncPostgresMessageHub
from lib.lead_agent_tools import create_lead_agent_tools
from lib.dag_scheduler import DAGScheduler
from lib.db import create_async_pool
from logging import getLogger
import logging

logging.basicConfig(level=logging.INFO,filemode="a", filename="logs/langcode.log", format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = getLogger("__main__")

# 处理环境变量
os.environ.pop("API_KEY", None)
os.environ.pop("MODEL_NAME", None)
os.environ.pop("BASE_URL", None)
os.environ.pop("LIGHT_MODEL_NAME", None)
load_dotenv(override=True)
WORK_DIR=Path(os.getcwd())

try:
    import readline
    readline.parse_and_bind('set bind-tty-special-chars off')
    readline.parse_and_bind('set input-meta on')
    readline.parse_and_bind('set output-meta on')
    readline.parse_and_bind('set convert-meta off')
except ImportError:
    pass

#  Tools 
@tool
def read_file(file_path: str) -> str:
    """读取本地文件内容。参数：file_path 文件路径（字符串）"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"读取文件失败: {str(e)}"

@tool
def write_file(file_path: str, content: str) -> str:
    """写入内容到本地文件。参数：file_path 文件路径，content 文件内容"""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        return f"文件已写入: {file_path}"
    except Exception as e:
        return f"写入文件失败: {str(e)}"

@tool
def bash(command: str) -> str:
    """执行Bash命令。参数：command 要执行的命令（字符串）"""
    # 基础危险命令过滤（额外的防护，PermissionMiddleware 也会检查）
    dangerous = ["rm -rf /", "sudo", "shutdown", "reboot", "> /dev/"]
    if any(d in command for d in dangerous):
        return "Error: Dangerous command blocked"
    try:
        r = subprocess.run(command, shell=True, cwd=WORK_DIR,
                           capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=120)
        out = (r.stdout + r.stderr).strip()
        return out[:50000] if out else "(no output)"
    except subprocess.TimeoutExpired:
        return "Error: Timeout (120s)"
    except (FileNotFoundError, OSError) as e:
        return f"Error: {e}"

def safe_path(p: str) -> Path:
    path = (WORK_DIR / p).resolve()
    if not path.is_relative_to(WORK_DIR):
        raise ValueError(f"Path escapes workspace: {p}")
    return path

@tool
def edit(path: str, old_text: str, new_text: str) -> str:
    """编辑本地文件。参数：path 文件路径，old_text 要替换的文本，new_text 替换后的文本"""
    try:
        file_path = safe_path(path)
        text = file_path.read_text()
        if old_text not in text:
            return f"Error: text not found in {path}"
        file_path.write_text(text.replace(old_text, new_text, 1))
        return f"Edited {path}"
    except Exception as e:
        return f"Error: {e}"

@tool
def glob(pattern: str) -> str:
    """列出匹配的文件。参数：pattern glob 模式（字符串）"""
    import glob as g
    try:
        results = []
        for match in g.glob(pattern, root_dir=WORK_DIR):
            if (WORK_DIR / match).resolve().is_relative_to(WORK_DIR):
                results.append(match)
        return "\n".join(results) if results else "(no matches)"
    except Exception as e:
        return f"Error: {e}"

@tool
def load_skill(skill_name: str) -> str:
    """Load a skill by name from the skills directory. Returns the skill content.
    Parameter: skill_name - the name of the skill to load
    """
    skill_dir = WORK_DIR / "skills" / skill_name
    if not skill_dir.exists():
        return f"Skill '{skill_name}' not found"
    
    skill_file = skill_dir / "SKILL.md"
    if not skill_file.exists():
        return f"Skill '{skill_name}' has no SKILL.md file"
    
    try:
        content = skill_file.read_text(encoding="utf-8")
        return content
    except Exception as e:
        return f"Error loading skill: {e}"


# ═══════════════════════════════════════════════════════════
#  Agent Setup using create_agent
# ═══════════════════════════════════════════════════════════

async def create_coding_agent(checkpointer: AsyncPostgresSaver, store: AsyncPostgresStore, message_hub: AsyncPostgresMessageHub = None, sub_agents: dict = None, use_backup: bool = False) -> Any:
    """Create the coding agent."""
    
    llm = ChatOpenAI(
        model=os.getenv("MODEL_NAME"),
        api_key=os.getenv("API_KEY"),
        base_url=os.getenv("BASE_URL"),
        temperature=0.3,
        max_completion_tokens=8000
    )
    
    light_llm = ChatOpenAI(
        model=os.getenv("LIGHT_MODEL_NAME"),
        api_key=os.getenv("API_KEY"),
        base_url=os.getenv("BASE_URL"),
        temperature=0.3,
        max_completion_tokens=8000,
        verbose=False
    )
    
    tools = [read_file, write_file, bash, edit, glob, load_skill]
    
    if message_hub:
        lead_tools = create_lead_agent_tools(
            message_hub=message_hub,
            sub_agents=sub_agents,
            llm=llm,
            light_llm=light_llm,
            checkpointer=checkpointer,
            work_dir=WORK_DIR,
        )
        tools.extend(lead_tools)
    
    system_prompt = f"""You are a coding agent LangCode. Act, don't explain.
Working directory: {WORK_DIR}
Always think step by step. If you are unsure about the next step, ask the user for clarification.

**DAG Decomposition Trigger**:
If the user request meets ANY of the following criteria, you MUST use the `publish_dag` tool:
1. **Multi-step**: The task requires >=4 distinct logical steps
2. **Parallelizable**: The task contains naturally independent modules that can run simultaneously

Examples:
- "Generate a weekly report with sales data, competitor news, and sentiment analysis" → Trigger DAG
- "Fix the bug in line 42" → Execute directly

**User Explicit Trigger**:
If the user explicitly says "用 DAG 分解", "Decompose this task", or similar, you MUST trigger DAG decomposition.
"""
    
    context_compression = ContextCompressionMiddleware(llm=light_llm)
    error_recovery = ErrorRecoveryMiddleware(
        primary_llm=llm,
        fallback_llm=light_llm,
        context_compressor=context_compression,
        max_retries=5,
        max_continuation_attempts=2,
        max_tokens_for_continuation=64000,
        consecutive_529_threshold=3,
    )
    
    permission_middleware = PermissionMiddleware(
        work_dir=WORK_DIR,
        message_hub=message_hub,
        agent_name="lead",
    )
    
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
        middleware=[
            permission_middleware,
            #TodoListMiddleware(),
            MemoryManagementMiddleware(llm=light_llm, store=store),
            context_compression,
            SkillLoadingMiddleware(WORK_DIR),
            error_recovery,
        ],
        checkpointer=checkpointer,
    )
    return agent

# ═══════════════════════════════════════════════════════════
#  Streaming CLI
# ═══════════════════════════════════════════════════════════

async def run_streaming():
    """Streaming CLI with conversation memory."""
    pool = create_async_pool()
    await pool.open()

    checkpointer = AsyncPostgresSaver(pool)
    await checkpointer.setup()
    store = AsyncPostgresStore(pool)
    await store.setup()
    
    message_hub = AsyncPostgresMessageHub(pool)
    await message_hub.setup()
    
    scheduler = DAGScheduler(pool)
    await scheduler.setup()
    
    async def monitor_leased_tasks():
        """后台监控 lease 过期任务"""
        while True:
            await asyncio.sleep(10)
            try:
                reclaimed = await scheduler.reclaim_leased_tasks()
                if reclaimed > 0:
                    logger.info(f"Reclaimed {reclaimed} tasks from crashed agents")
            except Exception as e:
                logger.error(f"Error in monitor_leased_tasks: {e}")
    
    monitor_task = asyncio.create_task(monitor_leased_tasks())
    
    # 创建 sub_agents 字典，用于跟踪所有 sub agent
    sub_agents = {"_thread_id": None}  # thread_id 会在后面设置
    
    agent = await create_coding_agent(checkpointer, store, message_hub, sub_agents)
    
    user_id = input("输入用户 ID (空白则为匿名用户): ").strip() or "匿名用户"
    thread_id=input("输入 thread_id 恢复对话 (空白则为新对话): ").strip()
    if not thread_id or thread_id == "" or thread_id=="\n":
        thread_id=f"langcode_session_{id({})}"
    config = {"configurable": {"thread_id": thread_id, "user_id": user_id}}
    
    # 设置 sub_agents 的 thread_id
    if sub_agents:
        sub_agents["_thread_id"] = thread_id
    
    print("=" * 50)
    print("LangCode - 编码助手")
    print("输入 'exit' 退出，输入 'reset' 重置会话")
    print("=" * 50)
    print(f"🤖 Session started with thread_id: {thread_id} for user: {user_id}")
    
    while True:
        pending_permissions = await message_hub.get_pending_permissions()
        if pending_permissions:
            print("\n\033[33m[待审批权限请求]\033[0m")
            for i, perm in enumerate(pending_permissions, 1):
                print(f"  [{i}] {perm['agent_name']}: {perm['command']}")
                print(f"      工具：{perm['tool_name']}")
                print(f"      请求 ID: {perm['request_id']}")
            print("\n使用 approve_permission <request_id> [reason] 或 reject_permission <request_id> <reason>")
        
        user_input = input("\033[36m用户 >> \033[0m")
        if user_input.lower() == "exit":
            break
        if user_input.lower() == "reset":
            thread_id = f"langcode_session_{id({})}"
            config = {"configurable": {"thread_id": thread_id, "user_id": user_id}}
            print("🤖 会话已重置")
            continue
        
        if user_input.startswith("approve_permission "):
            parts = user_input.split(" ", 2)
            if len(parts) >= 2:
                request_id = parts[1]
                reason = parts[2] if len(parts) > 2 else ""
                tools = create_lead_agent_tools(
                    message_hub=message_hub,
                    llm=None,
                    light_llm=None,
                    checkpointer=None,
                    work_dir=None,
                )
                approve_tool = next(t for t in tools if t.name == "approve_permission")
                result = await approve_tool.ainvoke({"request_id": request_id, "reason": reason})
                print(f"\033[32m{result}\033[0m")
                continue
        
        if user_input.startswith("reject_permission "):
            parts = user_input.split(" ", 2)
            if len(parts) >= 3:
                request_id = parts[1]
                reason = parts[2]
                tools = create_lead_agent_tools(
                    message_hub=message_hub,
                    llm=None,
                    light_llm=None,
                    checkpointer=None,
                    work_dir=None,
                )
                reject_tool = next(t for t in tools if t.name == "reject_permission")
                result = await reject_tool.ainvoke({"request_id": request_id, "reason": reason})
                print(f"\033[31m{result}\033[0m")
                continue
        
        print(f"\033[32m🤖 LangCode >> \033[0m", end="", flush=True)
        
        full_response = ""
        is_truncated = False
        had_truncation_event = False  # 是否发生过截断事件
        response_metadata = None
        continuation_attempts = 0
        
        # 用于跟踪是否正在输出（用于清除已输出的内容）
        output_buffer = []
        
        async for event in agent.astream_events(
            input={"messages": [{"role": "user", "content": user_input}]},
            config={**config, "recursion_limit": 100},
            version="v2",
        ):
            kind = event.get("event")
            # 调试：打印所有事件类型
            # print(f"\n[EVENT] {kind}: {event.get('name')}")
            if kind == "on_chat_model_stream":
                tags = event.get("tags", [])
                if "internal_memory_call" in tags:
                    continue
                name = event.get("name", "")
                if "Middleware" in name:
                    continue
                content = event.get("data", {}).get("chunk", {}).content
                if content:
                    output_buffer.append(content)
                    print(content, end="", flush=True)
                    full_response += content
                continue
            
            # 捕获模型调用的最终结果，检查是否截断
            # 注意：可能有多个 on_chat_model_end 事件（原始调用 + 续写调用）
            if kind == "on_chat_model_end":
                # 检查响应元数据
                output_obj = event.get("data", {}).get("output")
                if isinstance(output_obj, AIMessage):
                    response_metadata = output_obj.response_metadata
                    finish_reason = response_metadata.get("finish_reason")
                    truncation_handled = response_metadata.get("truncation_handled")
                    
                    # 检查是否有截断标记（来自 middleware）
                    if truncation_handled:
                        had_truncation_event = True
                        is_truncated = False
                        continuation_attempts = response_metadata.get("continuation_attempts", 0)
                    # 第一次截断事件
                    elif finish_reason == "length":
                        had_truncation_event = True
                        is_truncated = True
                    # 如果之前有截断事件，现在有 stop 事件，说明续写成功
                    elif finish_reason == "stop" and had_truncation_event:
                        # 续写成功，但无法获取续写次数（因为续写调用的事件没有 truncation_handled 标记）
                        # 假设至少续写了 1 次
                        is_truncated = False
                        continuation_attempts = 1
                continue
                
            if kind == "on_tool_start" and event.get("name") == "write_todos":
                print("\n[Planning] Agent updated todo list")
                continue
            
            # if kind == "on_tool_end" and event.get("name") == "write_todos":
            #     todos = event.get("data", {}).get("output")
            #     if todos:
            #         print("\n[任务列表更新]")
            #         for todo in todos:
            #             print(f"  {todo['status']}: {todo['content']}")
            #     continue    
        print()   
        
        if is_truncated:
            # 未被中间件处理（续写失败或未配置中间件）
            print("\n\033[33m[系统] 模型回复超长，部分内容可能缺失\033[0m")
        elif had_truncation_event and not is_truncated:
            # 已被中间件成功处理
            print(f"\n\033[33m[系统] 模型回复超长，已自动续写 {continuation_attempts} 次\033[0m")
        
        # 检查是否有错误恢复的提示（从响应内容中检测）
        if full_response and "⚠️" in full_response:
            # 静默提示，不额外输出
            pass
        
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass
    
    await pool.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_streaming())
