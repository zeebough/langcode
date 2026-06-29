# lib/message_hub.py
"""基于 PostgreSQL 的异步消息中心"""
import json
import asyncio
from psycopg_pool import AsyncConnectionPool


CREATE_TABLES_SQL = """
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
        
        payload = json.dumps({"to_agent": to_agent, "msg_type": msg_type})
        escaped_payload = payload.replace("'", "''")
        
        async with self.pool.connection() as conn:
            await conn.execute(
                """INSERT INTO agent_messages 
                   (from_agent, to_agent, content, msg_type, thread_id)
                   VALUES (%s, %s, %s, %s, %s)""",
                [from_agent, to_agent, json.dumps(content), msg_type, thread_id]
            )
            await conn.execute("NOTIFY agent_message, %s",escaped_payload)
    
    async def read_inbox(self, agent_name: str, thread_id: str | None = None) -> list[dict]:
        """读取并标记已读（消费式读取）"""
        async with self.pool.connection() as conn:
            if thread_id:
                cursor = await conn.execute(
                    """SELECT id, from_agent, content, msg_type, created_at
                       FROM agent_messages
                       WHERE to_agent = %s AND thread_id = %s AND read_at IS NULL
                       ORDER BY created_at ASC""",
                    [agent_name, thread_id]
                )
            else:
                cursor = await conn.execute(
                    """SELECT id, from_agent, content, msg_type, created_at
                       FROM agent_messages
                       WHERE to_agent = %s AND read_at IS NULL
                       ORDER BY created_at ASC""",
                    [agent_name]
                )
            
            rows=await cursor.fetchall()
            messages = [dict(row) for row in rows]
            
            if thread_id:
                await conn.execute(
                    """UPDATE agent_messages
                       SET read_at = NOW()
                       WHERE to_agent = %s AND thread_id = %s AND read_at IS NULL""",
                    [agent_name, thread_id]
                )
            else:
                await conn.execute(
                    """UPDATE agent_messages
                       SET read_at = NOW()
                       WHERE to_agent = %s AND read_at IS NULL""",
                    [agent_name]
                )
            
            return messages
    
    async def wait_for_message(
        self,
        agent_name: str,
        timeout: int = 300,
        msg_type: str | None = None,
        thread_id: str | None = None,
    ) -> dict | None:
        """使用 LISTEN/NOTIFY 等待消息（用于 idle 轮询）"""
        async with self.pool.connection() as conn:
            await conn.execute("LISTEN agent_message")
            
            start_time = asyncio.get_event_loop().time()
            
            while True:
                remaining_time = timeout - (asyncio.get_event_loop().time() - start_time)
                if remaining_time <= 0:
                    break
                
                try:
                    async for notify in conn.notifies(timeout=min(remaining_time, 10)):
                        payload = json.loads(notify.payload)
                        if payload["to_agent"] == agent_name:
                            if msg_type is None or payload["msg_type"] == msg_type:
                                messages = await self.read_inbox(agent_name, thread_id)
                                if messages:
                                    return messages[-1]
                except asyncio.TimeoutError:
                    continue
            
            return None
    
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
            return [dict(row) for row in rows]
    
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
                return dict(row)
            return None
