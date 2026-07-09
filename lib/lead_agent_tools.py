# lib/lead_agent_tools.py
"""Lead Agent 专用工具集"""
import asyncio
import json
from typing import Any
from pathlib import Path
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from lib.message_hub import AsyncPostgresMessageHub
from lib.sub_agent import run_sub_agent
from lib.dag_scheduler import DAGScheduler
import logging
import os

logger = logging.getLogger(__name__)


def create_lead_agent_tools(
    message_hub: AsyncPostgresMessageHub,
    sub_agents: dict[str, Any] = None,
    llm: ChatOpenAI = None,
    light_llm: ChatOpenAI = None,
    checkpointer: AsyncPostgresSaver = None,
    work_dir: Path = None,
) -> list:
    """创建 Lead Agent 工具集"""
    if sub_agents is None:
        sub_agents = {}
    
    if sub_agents.get("_running") is None:
        sub_agents["_running"] = {}
    
    scheduler = DAGScheduler(pool=message_hub.pool)

    def current_thread_id() -> str:
        return sub_agents.get("_thread_id") or "default"
    
    @tool
    async def spawn_sub_agent(name: str, role: str, task: str, max_rounds: int = 30) -> str:
        """创建一个新的 sub agent 并启动运行。参数：name - agent 名称，role - 角色描述，task - 初始任务，max_rounds - 最大轮数"""
        logger.info(f"spawn_sub_agent called: name={name}, role={role}, task={task}")
        if name in sub_agents and name not in ["_running"]:
            logger.warning(f"Sub agent '{name}' already exists")
            return f"Error: Sub agent '{name}' already exists"
        
        if llm is None or checkpointer is None or work_dir is None:
            logger.error("Missing dependencies for sub agent creation")
            return "Error: Sub agent system not properly initialized"
        
        # 创建 sub agent 并启动后台任务
        async def start_sub_agent():
            try:
                await run_sub_agent(
                    name=name,
                    role=role,
                    llm=llm,
                    checkpointer=checkpointer,
                    message_hub=message_hub,
                    thread_id=sub_agents.get("_thread_id", "default"),
                    work_dir=work_dir,
                    light_llm=light_llm,
                    max_rounds=max_rounds,
                )
            except Exception as e:
                logger.error(f"Sub agent '{name}' failed: {e}")
                sub_agents[name]["status"] = "failed"
                sub_agents[name]["error"] = str(e)
        
        # 记录 sub agent 信息
        sub_agents[name] = {
            "name": name,
            "role": role,
            "status": "starting",
            "current_task": task,
            "max_rounds": max_rounds,
        }
        
        # 启动后台任务
        bg_task = asyncio.create_task(start_sub_agent())
        sub_agents["_running"][name] = bg_task
        
        logger.info(f"Sub agent '{name}' created and started")
        return f"Sub agent '{name}' created successfully with role: {role}"
    
    @tool
    async def assign_task(agent_name: str, task: str) -> str:
        """分配任务给指定的 agent。参数：agent_name - agent 名称，task - 任务内容"""
        if agent_name not in sub_agents:
            return f"Error: Sub agent '{agent_name}' not found"
        
        sub_agent = sub_agents[agent_name]
        if sub_agent["status"] == "working":
            return f"Warning: Agent '{agent_name}' is still working. Task queued."
        
        await message_hub.send(
            from_agent="lead",
            to_agent=agent_name,
            content={"task": task},
            msg_type="task",
        )
        
        sub_agent["status"] = "working"
        sub_agent["current_task"] = task
        return f"Task assigned to '{agent_name}'"
    
    @tool
    def list_sub_agents() -> str:
        """查看所有 sub agent 的状态"""
        if not sub_agents:
            return "No sub agents created yet"
        
        lines = []
        for name, info in sub_agents.items():
            if name in ("_running", "_thread_id"):
                continue
            lines.append(f"  {name}: {info['role']} - {info['status']} - Task: {info['current_task']}")
        return "\n".join(lines)
    
    @tool
    def shutdown_agent(agent_name: str) -> str:
        """关闭指定的 agent。参数：agent_name - agent 名称"""
        if agent_name not in sub_agents:
            return f"Error: Sub agent '{agent_name}' not found"
        
        sub_agents[agent_name]["status"] = "shutdown"
        return f"Sub agent '{agent_name}' marked for shutdown"
    
    @tool
    async def send_message(to: str, content: str, msg_type: str = "message") -> str:
        """发送消息到 Message Hub。参数：to - 收件人，content - 消息内容，msg_type - 消息类型"""
        await message_hub.send(
            from_agent="lead",
            to_agent=to,
            content=content,
            msg_type=msg_type,
        )
        return f"Message sent to '{to}'"
    
    @tool
    async def publish_dag(dag_json: str) -> str:
        """
        发布 DAG 到任务看板
        参数：dag_json - 符合 DAG Decomposer 输出的 JSON 字符串
        """
        try:
            dag_data = json.loads(dag_json)
            
            result = await scheduler.insert_dag_to_db(
                dag_data=dag_data,
                thread_id=current_thread_id(),
                owner=None,
            )
            
            for agent_name in sub_agents.keys():
                if agent_name not in ["_running", "_thread_id"]:
                    await message_hub.send(
                        from_agent="lead",
                        to_agent=agent_name,
                        content={"notification": "new_tasks_available"},
                        msg_type="task_available",
                        thread_id=current_thread_id(),
                    )
            
            return f"Published {result['tasks_inserted']} tasks to board"
        except Exception as e:
            logger.error(f"Failed to publish DAG: {e}")
            return f"Error publishing DAG: {str(e)}"
    
    @tool
    async def get_task_board_status() -> str:
        """查看任务看板状态（pending/in_progress/completed/failed 数量）"""
        async with message_hub.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT status, COUNT(*) as count
                FROM tasks
                WHERE thread_id = %s
                GROUP BY status
                """,
                [current_thread_id()]
            )
            rows = await cursor.fetchall()
            if not rows:
                return "No tasks in board"
            return "\n".join([f"  {row['status']}: {row['count']}" for row in rows])
    
    @tool
    async def handle_agent_shutdown(agent_name: str, reason: str) -> str:
        """处理 Sub Agent shutdown 通知"""
        if agent_name in sub_agents:
            del sub_agents[agent_name]
        if "_running" in sub_agents and agent_name in sub_agents["_running"]:
            del sub_agents["_running"][agent_name]
        
        async with message_hub.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT COUNT(*) as pending_count
                FROM tasks
                WHERE thread_id = %s AND status = 'pending' AND blocked_by_count = 0
                """,
                [current_thread_id()]
            )
            row = await cursor.fetchone()
            pending_count = row["pending_count"] if row else 0
        
        if pending_count > 0:
            logger.info(f"Auto-spawning new sub agent to handle {pending_count} pending tasks")
            new_agent_name = f"agent_{id(asyncio.current_task())}"
            
            tools = create_lead_agent_tools(
                message_hub=message_hub,
                sub_agents=sub_agents,
                llm=llm,
                light_llm=light_llm,
                checkpointer=checkpointer,
                work_dir=work_dir,
            )
            spawn_tool = next(t for t in tools if t.name == "spawn_sub_agent")
            
            await spawn_tool.ainvoke({
                "name": new_agent_name,
                "role": "task_executor",
                "task": "Execute pending tasks from the task board",
                "max_rounds": 50,
            })
        
        return f"Sub agent '{agent_name}' shutdown handled"
    
    @tool
    async def check_inbox() -> str:
        """检查 Lead Agent 收件箱"""
        logger.info("check_inbox tool called")
        messages = await message_hub.read_inbox("lead")
        logger.info(f"check_inbox: found {len(messages)} messages")
        if not messages:
            return "Inbox is empty"
        
        lines = []
        for msg in messages:
            lines.append(f"  From: {msg['from_agent']} | Type: {msg['msg_type']} | Content: {msg['content']}")
        return "\n".join(lines)
    
    @tool
    async def list_pending_permissions() -> str:
        """列出所有待审批的权限请求"""
        pending = await message_hub.get_pending_permissions()
        if not pending:
            return "No pending permission requests"
        
        lines = []
        for perm in pending:
            lines.append(f"  [{perm['request_id']}] {perm['agent_name']}: {perm['tool_name']} - {perm['command']}")
        return "\n".join(lines)
    
    @tool
    async def approve_permission(request_id: str, reason: str = "") -> str:
        """批准权限请求。参数：request_id - 请求 ID, reason - 批准原因（可选）"""
        request = await message_hub.get_permission_request(request_id)
        if not request:
            return f"Error: Permission request '{request_id}' not found"
        
        await message_hub.log_permission_decision(
            request_id=request_id,
            decision="approved",
            reason=reason or "Approved by lead agent",
            decided_by="lead",
        )
        
        await message_hub.send(
            from_agent="lead",
            to_agent=request["agent_name"],
            content={
                "request_id": request_id,
                "decision": "approved",
                "reason": reason or "Approved by lead agent",
            },
            msg_type="permission_response",
        )
        
        return f"Permission request '{request_id}' approved"
    
    @tool
    async def reject_permission(request_id: str, reason: str) -> str:
        """拒绝权限请求。参数：request_id - 请求 ID, reason - 拒绝原因"""
        request = await message_hub.get_permission_request(request_id)
        if not request:
            return f"Error: Permission request '{request_id}' not found"
        
        await message_hub.log_permission_decision(
            request_id=request_id,
            decision="rejected",
            reason=reason,
            decided_by="lead",
        )
        
        await message_hub.send(
            from_agent="lead",
            to_agent=request["agent_name"],
            content={
                "request_id": request_id,
                "decision": "rejected",
                "reason": reason,
            },
            msg_type="permission_response",
        )
        
        return f"Permission request '{request_id}' rejected: {reason}"
    
    return [
        spawn_sub_agent,
        assign_task,
        list_sub_agents,
        shutdown_agent,
        send_message,
        check_inbox,
        list_pending_permissions,
        approve_permission,
        reject_permission,
        publish_dag,
        get_task_board_status,
        handle_agent_shutdown,
    ]
