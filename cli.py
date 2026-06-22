# cli.py
import datetime
import os
import subprocess
from pathlib import Path
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain.agents import create_agent
from langchain.agents.middleware import TodoListMiddleware
from langgraph.store.postgres import AsyncPostgresStore
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from typing import Any

from middlewares.context_compression_middleware import ContextCompressionMiddleware
from middlewares.memory_management_middleware import MemoryManagementMiddleware
from middlewares.permission_middleware import PermissionMiddleware

load_dotenv()
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
    """列出匹配的文件。参数：pattern glob模式（字符串）"""
    import glob as g
    try:
        results = []
        for match in g.glob(pattern, root_dir=WORK_DIR):
            if (WORK_DIR / match).resolve().is_relative_to(WORK_DIR):
                results.append(match)
        return "\n".join(results) if results else "(no matches)"
    except Exception as e:
        return f"Error: {e}"

# ═══════════════════════════════════════════════════════════
#  Agent Setup using create_agent
# ═══════════════════════════════════════════════════════════

async def create_coding_agent(checkpointer: AsyncPostgresSaver, store: AsyncPostgresStore) -> Any:
    """Create the coding agent."""
    tools = [read_file, write_file, bash, edit, glob]
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
    
    # Static system prompt (time is fixed at startup, not critical)
    system_prompt = f"""You are a coding agent LangCode. Act, don't explain.
Available tools: bash, read_file, write_file, edit_file, glob.
Working directory: {WORK_DIR}
Always think step by step. If you are unsure about the next step, ask the user for clarification.
"""
    # Create agent with middleware
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
        middleware=[
            PermissionMiddleware(WORK_DIR),      # Custom permission gate
            TodoListMiddleware(),        # Built-in todo planning
            MemoryManagementMiddleware(llm=light_llm, store=store),# 长期记忆存取
            ContextCompressionMiddleware(llm=light_llm),  # 上下文压缩
        ],
        checkpointer=checkpointer, # 短期记忆存取
    )
    return agent

# ═══════════════════════════════════════════════════════════
#  Streaming CLI
# ═══════════════════════════════════════════════════════════

async def run_streaming():
    """Streaming CLI with conversation memory."""
    # Initialize PostgreSQL connection pool and checkpointer
    DB_URI = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}?sslmode=disable"
    pool = AsyncConnectionPool(DB_URI, 
                               min_size=1, 
                               max_size=5,
                               timeout=120,
                               kwargs={
                                   "autocommit": True,
                                   "row_factory":dict_row
                               })
    await pool.open()

    # short-term memory within a session (thread_id)
    checkpointer = AsyncPostgresSaver(pool)
    await checkpointer.setup()
    # long-term memory across sessions, no vector search
    store = AsyncPostgresStore(pool)
    await store.setup()
    
    agent = await create_coding_agent(checkpointer, store)
    
    # 主体会话逻辑
    user_id = input("输入用户ID (空白则为匿名用户): ").strip() or "匿名用户"
    thread_id=input("输入thread_id恢复对话 (空白则为新对话): ").strip()
    if thread_id or thread_id == "" or thread_id=="\n":
        thread_id=f"langcode_session_{id({})}"
    config = {"configurable": {"thread_id": thread_id, "user_id": user_id}}
    
    print("=" * 50)
    print("LangCode - 编码助手")
    print("输入 'exit' 退出，输入 'reset' 重置会话")
    print("=" * 50)
    print(f"🤖 Session started with thread_id: {thread_id} for user: {user_id}")
    
    while True:
        user_input = input("\033[36m用户 >> \033[0m")
        if user_input.lower() == "exit":
            break
        if user_input.lower() == "reset":
            thread_id = f"langcode_session_{id({})}"
            config = {"configurable": {"thread_id": thread_id, "user_id": user_id}}
            print("🤖 会话已重置")
            continue
        
        print(f"\033[32m🤖 LangCode >> \033[0m", end="", flush=True)
        
        # Stream events from the agent
        async for event in agent.astream_events(
            input={"messages": [{"role": "user", "content": user_input}]},
            config=config,
            version="v2",
        ):
            kind = event.get("event")
            if kind == "on_chat_model_stream":
                # 过滤内部 LLM 调用（记忆提取、上下文压缩等）
                tags = event.get("tags", [])
                if "internal_memory_call" in tags:
                    continue
                # 只在主 agent 的 LLM 调用时输出（过滤 middleware 的调用）
                name = event.get("name", "")
                if "Middleware" in name:
                    continue
                content = event.get("data", {}).get("chunk", {}).content
                if content:
                    print(content, end="", flush=True)
                continue
                    
            # Optional: You can also listen for todo list updates
            if kind == "on_tool_start" and event.get("name") == "write_todos":
                print("\n[Planning] Agent updated todo list")
                continue
            
            if kind == "on_tool_end" and event.get("name") == "write_todos":
                todos = event.get("data", {}).get("output")
                if todos:
                    print("\n[任务列表更新]")
                    for todo in todos:
                        print(f"  {todo['status']}: {todo['content']}")
                continue
            
        print()  # newline after response
        
    # Clean up
    await pool.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_streaming())