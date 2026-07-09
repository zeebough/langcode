# lib/sub_agent.py
"""Sub Agent 创建和运行循环"""
import os
import asyncio
from pathlib import Path
from typing import Any
from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage
from langchain.agents import create_agent
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langchain_core.tools import tool

from middlewares.error_recovery_middleware import ErrorRecoveryMiddleware
from middlewares.context_compression_middleware import ContextCompressionMiddleware
from middlewares.permission_middleware import PermissionMiddleware
from lib.message_hub import AsyncPostgresMessageHub
from lib.dag_scheduler import DAGScheduler
import logging

logger = logging.getLogger(__name__)


async def create_sub_agent(
    name: str,
    role: str,
    task: str,
    llm: ChatOpenAI,
    checkpointer: AsyncPostgresSaver,
    message_hub: AsyncPostgresMessageHub,
    thread_id: str,
    work_dir: Path,
    light_llm: ChatOpenAI | None = None,
) -> Any:
    """创建 sub agent（复用 create_agent 和中间件）"""
    
    tools = [
        await _create_bash_tool(),
        await _create_read_file_tool(),
        await _create_write_file_tool(),
        await _create_edit_file_tool(),
        await _create_glob_tool(),
    ]
    
    permission_middleware = PermissionMiddleware(
        work_dir=work_dir,
        message_hub=message_hub,
        agent_name=name,
    )
    
    system_prompt = f"""You are '{name}', a {role} agent.
Working directory: {work_dir}
Task: {task}

Instructions:
1. Complete the task using available tools
2. If an operation requires permission, wait for approval
3. After completion, send result to 'lead' via send_message tool
4. Then wait for new tasks
"""
    
    middleware_list = []
    middleware_list.append(permission_middleware)
    
    if light_llm:
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
        middleware_list.append(context_compression)
        middleware_list.append(error_recovery)
    
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
        middleware=middleware_list,
        checkpointer=checkpointer,
    )
    
    return agent


async def run_sub_agent(
    name: str,
    role: str,
    llm: ChatOpenAI,
    checkpointer: AsyncPostgresSaver,
    message_hub: AsyncPostgresMessageHub,
    thread_id: str,
    work_dir: Path,
    light_llm: ChatOpenAI | None = None,
    max_rounds: int = 30,
):
    """Sub Agent 主动认领 + 被动通知循环"""
    
    scheduler = DAGScheduler(pool=message_hub.pool)
    current_task = None
    last_activity_time = asyncio.get_event_loop().time()
    lease_duration = 60
    
    async def renew_heartbeat():
        """后台心跳任务"""
        nonlocal current_task
        while current_task:
            await asyncio.sleep(25)
            if current_task:
                try:
                    await scheduler.renew_lease(
                        task_id=current_task["id"],
                        owner=name,
                        lease_duration=lease_duration
                    )
                    logger.debug(f"Task {current_task['id']} heartbeat renewed")
                except Exception as e:
                    logger.error(f"Heartbeat renewal failed: {e}")
    
    while True:
        task = await _claim_next_task(scheduler, name, thread_id)
        
        if task:
            current_task = task
            last_activity_time = asyncio.get_event_loop().time()
            
            heartbeat_task = asyncio.create_task(renew_heartbeat())
            
            try:
                agent = await create_sub_agent(
                    name=name,
                    role=role,
                    task=task["description"],
                    llm=llm,
                    checkpointer=checkpointer,
                    message_hub=message_hub,
                    thread_id=thread_id,
                    work_dir=work_dir,
                    light_llm=light_llm,
                )
                
                messages = [{"role": "user", "content": task["description"]}]
                config = {"configurable": {"thread_id": f"{thread_id}_{name}"}}
                
                result = None
                for _ in range(max_rounds):
                    response = await agent.ainvoke({"messages": messages}, config=config)
                    last_message = response["messages"][-1]
                    if isinstance(last_message, AIMessage) and not last_message.tool_calls:
                        result = last_message.content
                        break
                    messages = response["messages"]
                
                if result:
                    result_path = f"/tmp/task_results/{thread_id}/{task['id']}.txt"
                    os.makedirs(os.path.dirname(result_path), exist_ok=True)
                    with open(result_path, "w") as f:
                        f.write(result)
                    
                    await scheduler.complete_task(task["id"], result[:1000], result_path)
                    logger.info(f"Task {task['id']} completed by {name}")
                else:
                    retry_count = int(task.get("metadata", {}).get("retry_count", 0))
                    can_retry = await scheduler.fail_task(task["id"], "Task execution failed", retry_count)
                    if not can_retry:
                        logger.warning(f"Task {task['id']} failed permanently")
                        
            except Exception as e:
                logger.error(f"Task {task['id']} execution error: {e}")
                retry_count = int(task.get("metadata", {}).get("retry_count", 0))
                can_retry = await scheduler.fail_task(task["id"], str(e), retry_count)
                if not can_retry:
                    logger.warning(f"Task {task['id']} failed permanently after error")
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
                current_task = None
            continue
        
        elapsed = asyncio.get_event_loop().time() - last_activity_time
        if elapsed > 600:
            logger.info(f"Sub agent {name} shutting down due to idle timeout")
            await message_hub.send(
                from_agent=name,
                to_agent="lead",
                content={"agent_name": name, "reason": "idle_timeout"},
                msg_type="agent_shutdown",
                thread_id=thread_id,
            )
            break
        
        try:
            msg = await message_hub.wait_for_message(
                agent_name=name,
                timeout=5,
                msg_type="task_available",
                thread_id=thread_id,
            )
            if msg:
                continue
        except asyncio.TimeoutError:
            continue


async def _create_bash_tool():
    """创建 bash 工具"""
    import subprocess
    from langchain_core.tools import tool
    
    @tool
    def bash(command: str) -> str:
        """执行 Bash 命令。参数：command 要执行的命令（字符串）"""
        work_dir = Path(os.getcwd())
        try:
            r = subprocess.run(
                command,
                shell=True,
                cwd=work_dir,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=120
            )
            out = (r.stdout + r.stderr).strip()
            return out[:50000] if out else "(no output)"
        except subprocess.TimeoutExpired:
            return "Error: Timeout (120s)"
        except (FileNotFoundError, OSError) as e:
            return f"Error: {e}"
    
    return bash


async def _create_read_file_tool():
    """创建 read_file 工具"""
    from langchain_core.tools import tool
    
    @tool
    def read_file(file_path: str) -> str:
        """读取本地文件内容。参数：file_path 文件路径（字符串）"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            return f"读取文件失败：{str(e)}"
    
    return read_file


async def _create_write_file_tool():
    """创建 write_file 工具"""
    import os
    from langchain_core.tools import tool
    
    @tool
    def write_file(file_path: str, content: str) -> str:
        """写入内容到本地文件。参数：file_path 文件路径，content 文件内容"""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            return f"文件已写入：{file_path}"
        except Exception as e:
            return f"写入文件失败：{str(e)}"
    
    return write_file


async def _create_edit_file_tool():
    """创建 edit_file 工具"""
    from pathlib import Path
    from langchain_core.tools import tool
    
    work_dir = Path(os.getcwd())
    
    @tool
    def edit_file(path: str, old_text: str, new_text: str) -> str:
        """编辑本地文件。参数：path 文件路径，old_text 要替换的文本，new_text 替换后的文本"""
        try:
            file_path = (work_dir / path).resolve()
            if not str(file_path).startswith(str(work_dir)):
                return f"Error: Path escapes workspace: {path}"
            text = file_path.read_text(encoding="utf-8")
            if old_text not in text:
                return f"Error: text not found in {path}"
            file_path.write_text(text.replace(old_text, new_text, 1), encoding="utf-8")
            return f"Edited {path}"
        except Exception as e:
            return f"Error: {e}"
    
    return edit_file


async def _create_glob_tool():
    """创建 glob 工具"""
    import glob as g
    from langchain_core.tools import tool
    
    work_dir = Path(os.getcwd())
    
    @tool
    def glob(pattern: str) -> str:
        """列出匹配的文件。参数：pattern glob 模式（字符串）"""
        try:
            results = []
            for match in g.glob(pattern, root_dir=work_dir):
                match_path = (work_dir / match).resolve()
                if str(match_path).startswith(str(work_dir)):
                    results.append(match)
            return "\n".join(results) if results else "(no matches)"
        except Exception as e:
            return f"Error: {e}"
    
    return glob


async def _claim_next_task(scheduler: DAGScheduler, owner: str, thread_id: str) -> dict | None:
    """
    从任务看板认领一个任务
    
    Args:
        scheduler: DAGScheduler 实例
        owner: 认领者名称
        thread_id: 线程 ID
        
    Returns:
        认领的任务信息，无任务则返回 None
    """
    return await scheduler.claim_next_available_task(thread_id=thread_id, owner=owner)
