# lib/message_hub.py
"""基于 PostgreSQL 的异步消息中心"""
import json
import asyncio
from typing import Any

from psycopg_pool import AsyncConnectionPool


CREATE_TABLES_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 消息表（Message Hub）
CREATE TABLE IF NOT EXISTS agent_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_agent VARCHAR(255) NOT NULL,
    to_agent VARCHAR(255) NOT NULL,
    content JSONB NOT NULL,
    msg_type VARCHAR(50) NOT NULL,
    thread_id VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    read_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_inbox ON agent_messages (to_agent, thread_id, created_at);
CREATE INDEX IF NOT EXISTS idx_unread ON agent_messages (to_agent, read_at) WHERE read_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_unread_claim ON agent_messages (to_agent, created_at, id) WHERE read_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_thread_unread_claim ON agent_messages (to_agent, thread_id, created_at, id) WHERE read_at IS NULL;

-- 权限审计表（独立表，方便审计）
CREATE TABLE IF NOT EXISTS permission_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id VARCHAR(255) UNIQUE NOT NULL,
    agent_name VARCHAR(255) NOT NULL,
    tool_name VARCHAR(100) NOT NULL,
    command TEXT NOT NULL,
    decision VARCHAR(20),
    reason TEXT,
    decided_by VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    decided_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_agent ON permission_audit_log (agent_name, created_at);
CREATE INDEX IF NOT EXISTS idx_decision ON permission_audit_log (decision, created_at);
"""


def _row_to_dict(cursor, row) -> dict[str, Any] | None:
    """兼容 dict_row 和默认 tuple row_factory。"""
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return dict(zip([desc.name for desc in cursor.description], row))


class AsyncPostgresMessageHub:
    """基于 PostgreSQL 的异步消息中心"""
    
    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool
    
    async def setup(self):
        """初始化表结构"""
        async with self.pool.connection() as conn:
            await conn.execute(CREATE_TABLES_SQL)
    
    async def send(
        self,
        from_agent: str,
        to_agent: str,
        content: dict | str,
        msg_type: str,
        thread_id: str | None = None,
    ):
        """发送消息并触发 NOTIFY"""
        if isinstance(content, str):
            content = {"text": content}
        
        payload = json.dumps({
            "to_agent": to_agent,
            "msg_type": msg_type,
            "thread_id": thread_id,
        })
        
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """INSERT INTO agent_messages 
                       (from_agent, to_agent, content, msg_type, thread_id)
                       VALUES (%s, %s, %s, %s, %s)""",
                    [from_agent, to_agent, json.dumps(content), msg_type, thread_id]
                )
                await conn.execute("SELECT pg_notify(%s, %s)", ["agent_message", payload])
    
    async def read_inbox(
        self,
        agent_name: str,
        thread_id: str | None = None,
        msg_type: str | None = None,
        content_match: dict[str, Any] | None = None,
    ) -> list[dict]:
        """读取并标记已读（消费式读取）"""
        filters = ["to_agent = %s", "read_at IS NULL"]
        params = [agent_name]

        if thread_id is not None:
            filters.append("thread_id = %s")
            params.append(thread_id)
        if msg_type is not None:
            filters.append("msg_type = %s")
            params.append(msg_type)
        if content_match is not None:
            filters.append("content @> %s::jsonb")
            params.append(json.dumps(content_match))

        where_clause = " AND ".join(filters)

        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                f"""WITH claimed AS (
                       SELECT id
                       FROM agent_messages
                       WHERE {where_clause}
                       ORDER BY created_at ASC, id ASC
                       FOR UPDATE SKIP LOCKED
                   ), updated AS (
                       UPDATE agent_messages AS m
                       SET read_at = NOW()
                       FROM claimed
                       WHERE m.id = claimed.id
                       RETURNING m.id, m.from_agent, m.content, m.msg_type, m.created_at
                   )
                   SELECT id, from_agent, content, msg_type, created_at
                   FROM updated
                   ORDER BY created_at ASC, id ASC""",
                params,
            )

            rows = await cursor.fetchall()
            return [_row_to_dict(cursor, row) for row in rows]
    
    async def wait_for_message(
        self,
        agent_name: str,
        timeout: int = 300,
        msg_type: str | None = None,
        thread_id: str | None = None,
    ) -> dict | None:
        """使用 LISTEN/NOTIFY 等待消息（用于 idle 轮询）"""
        messages = await self.read_inbox(agent_name, thread_id, msg_type)
        if messages:
            return messages[-1]

        async with self.pool.connection() as conn:
            await conn.execute("LISTEN agent_message")

            try:
                messages = await self.read_inbox(agent_name, thread_id, msg_type)
                if messages:
                    return messages[-1]

                start_time = asyncio.get_event_loop().time()
                
                while True:
                    remaining_time = timeout - (asyncio.get_event_loop().time() - start_time)
                    if remaining_time <= 0:
                        break
                    
                    try:
                        async for notify in conn.notifies(timeout=min(remaining_time, 10)):
                            payload = json.loads(notify.payload)
                            if not isinstance(payload, dict):
                                continue
                            if payload["to_agent"] != agent_name:
                                continue
                            if msg_type is not None and payload["msg_type"] != msg_type:
                                continue
                            if thread_id is not None and payload.get("thread_id") != thread_id:
                                continue

                            messages = await self.read_inbox(agent_name, thread_id, msg_type)
                            if messages:
                                return messages[-1]
                    except asyncio.TimeoutError:
                        continue
                    except (json.JSONDecodeError, KeyError):
                        continue

                return None
            finally:
                await conn.execute("UNLISTEN agent_message")
    
    async def log_permission_request(
        self,
        request_id: str,
        agent_name: str,
        tool_name: str,
        command: str,
    ):
        """记录权限请求到审计表"""
        async with self.pool.connection() as conn:
            await conn.execute(
                """INSERT INTO permission_audit_log
                   (request_id, agent_name, tool_name, command, created_at)
                   VALUES (%s, %s, %s, %s, NOW())""",
                [request_id, agent_name, tool_name, command]
            )
    
    async def log_permission_decision(
        self,
        request_id: str,
        decision: str,
        reason: str | None,
        decided_by: str,
    ):
        """记录权限决定到审计表"""
        async with self.pool.connection() as conn:
            await conn.execute(
                """UPDATE permission_audit_log
                   SET decision = %s, reason = %s, decided_by = %s, decided_at = NOW()
                   WHERE request_id = %s""",
                [decision, reason, decided_by, request_id]
            )
    
    async def get_pending_permissions(self) -> list[dict]:
        """获取所有待审批的权限请求"""
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """SELECT request_id, agent_name, tool_name, command, created_at
                   FROM permission_audit_log
                   WHERE decision IS NULL
                   ORDER BY created_at ASC"""
            )
            rows = await cursor.fetchall()
            return [_row_to_dict(cursor, row) for row in rows]
    
    async def get_permission_request(self, request_id: str) -> dict | None:
        """获取单个权限请求"""
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """SELECT request_id, agent_name, tool_name, command, created_at
                   FROM permission_audit_log
                   WHERE request_id = %s""",
                [request_id]
            )
            row = await cursor.fetchone()
            if row:
                return _row_to_dict(cursor, row)
            return None
