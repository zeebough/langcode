"""
DAG Scheduler - PostgreSQL 任务调度器
将分解后的 DAG 插入数据库的任务表和依赖边表

使用 psycopg_pool 连接池（与 message_hub.py 保持一致）
"""

import logging
import json
from typing import Dict, List, Any, Optional
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)


CREATE_DAG_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    description TEXT,
    thread_id TEXT,
    owner TEXT,
    status TEXT CHECK (status IN ('pending','in_progress','completed','failed')),
    blocked_by_count INT NOT NULL DEFAULT 0,
    claimed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    metadata JSONB,
    updated_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS thread_id TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS owner TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS last_heartbeat TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
CHECK (status IN ('pending','in_progress','completed','failed'));

CREATE INDEX IF NOT EXISTS idx_tasks_ready
ON tasks (status, blocked_by_count, updated_at)
WHERE status = 'pending' AND blocked_by_count = 0;

CREATE INDEX IF NOT EXISTS idx_tasks_lease
ON tasks (status, lease_expires_at)
WHERE status = 'in_progress';

CREATE INDEX IF NOT EXISTS idx_tasks_thread
ON tasks (thread_id, status, blocked_by_count, updated_at)
WHERE status = 'pending' AND blocked_by_count = 0;

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    blocker_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, blocker_id)
);

CREATE INDEX IF NOT EXISTS idx_deps_blocker ON task_dependencies (blocker_id);
"""


def _row_to_dict(cursor, row) -> Dict[str, Any]:
    """
    将 psycopg 的 row 转换为字典
    
    Args:
        cursor: 执行查询的 cursor
        row: 返回的行数据（元组）
        
    Returns:
        字典形式的行数据
    """
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    return dict(zip([desc.name for desc in cursor.description], row))


class DAGScheduler:
    """基于 PostgreSQL 连接池的 DAG 任务调度器"""
    
    def __init__(self, pool: AsyncConnectionPool):
        """
        Args:
            pool: psycopg_pool.AsyncConnectionPool 实例
        """
        self.pool = pool

    async def setup(self):
        """初始化 DAG 任务表、依赖表与高频认领索引。"""
        async with self.pool.connection() as conn:
            await conn.execute(CREATE_DAG_TABLES_SQL)
    
    async def insert_dag_to_db(
        self,
        dag_data: Dict[str, Any],
        thread_id: str,
        owner: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        将 DAG 插入数据库的 tasks 和 task_dependencies 表
        
        Args:
            dag_data: 分解后的 DAG 数据（包含 plan_summary 和 tasks）
            thread_id: 会话线程 ID
            owner: 可选的 owner 标识
            
        Returns:
            插入结果统计
        """
        tasks = dag_data.get("tasks", [])
        
        if not tasks:
            logger.warning("No tasks to insert")
            return {"tasks_inserted": 0, "dependencies_inserted": 0}
        
        async with self.pool.connection() as conn:
            async with conn.transaction():
                tasks_inserted = 0
                dependencies_inserted = 0
                
                for task in tasks:
                    blocked_by_count = len(task.get("blockedBy", []))
                    
                    metadata = task.get("metadata", {})
                    metadata_json = json.dumps(metadata) if metadata else None
                    
                    await conn.execute(
                        """
                        INSERT INTO tasks (id, subject, description, thread_id, owner, status, blocked_by_count, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            subject = EXCLUDED.subject,
                            description = EXCLUDED.description,
                            thread_id = EXCLUDED.thread_id,
                            owner = EXCLUDED.owner,
                            status = EXCLUDED.status,
                            blocked_by_count = EXCLUDED.blocked_by_count,
                            metadata = EXCLUDED.metadata,
                            updated_at = now()
                        """,
                        [
                            task["id"],
                            task["subject"],
                            task["description"],
                            thread_id,
                            owner,
                            "pending",
                            blocked_by_count,
                            metadata_json,
                        ]
                    )
                    tasks_inserted += 1
                    
                    for blocker_id in task.get("blockedBy", []):
                        await conn.execute(
                            """
                            INSERT INTO task_dependencies (task_id, blocker_id)
                            VALUES (%s, %s)
                            ON CONFLICT (task_id, blocker_id) DO NOTHING
                            """,
                            [task["id"], blocker_id]
                        )
                        dependencies_inserted += 1
                
                logger.info(
                    f"DAG inserted: {tasks_inserted} tasks, {dependencies_inserted} dependencies (thread_id={thread_id})"
                )
                
                return {
                    "tasks_inserted": tasks_inserted,
                    "dependencies_inserted": dependencies_inserted,
                    "plan_summary": dag_data.get("plan_summary", ""),
                }
    
    async def update_task_status(
        self,
        task_id: str,
        status: str,
        result: Optional[str] = None,
    ) -> bool:
        """
        更新任务状态
        
        Args:
            task_id: 任务 ID
            status: 新状态 ('pending', 'in_progress', 'completed')
            result: 可选的任务执行结果
            
        Returns:
            是否更新成功
        """
        if status not in ("pending", "in_progress", "completed"):
            logger.error(f"Invalid status: {status}")
            return False
        
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE tasks 
                    SET status = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    [status, task_id]
                )
                
                if status == "completed" and result:
                    await conn.execute(
                        """
                        UPDATE tasks 
                        SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                            updated_at = now()
                        WHERE id = %s
                        """,
                        [json.dumps({"result": result}), task_id]
                    )
                
            if status == "completed":
                async with self.pool.connection() as dep_conn:
                    dep_cursor = await dep_conn.execute(
                        """
                        SELECT task_id FROM task_dependencies WHERE blocker_id = %s
                        """,
                        [task_id]
                    )
                    dependents_result = await dep_cursor.fetchall()
                    
                    for row in dependents_result:
                        row_dict = _row_to_dict(dep_cursor, row)
                        dependent_task_id = row_dict["task_id"]
                        
                        deps_cursor = await dep_conn.execute(
                            """
                            SELECT blocker_id FROM task_dependencies 
                            WHERE task_id = %s
                            """,
                            [dependent_task_id]
                        )
                        deps = await deps_cursor.fetchall()
                        
                        completed_count = 0
                        for dep_row in deps:
                            dep_dict = _row_to_dict(deps_cursor, dep_row)
                            blocker_status_cursor = await dep_conn.execute(
                                """
                                SELECT status FROM tasks WHERE id = %s
                                """,
                                [dep_dict["blocker_id"]]
                            )
                            blocker_status = await blocker_status_cursor.fetchone()
                            if blocker_status:
                                blocker_status_dict = _row_to_dict(blocker_status_cursor, blocker_status)
                                if blocker_status_dict["status"] == "completed":
                                    completed_count += 1
                        
                        remaining_count = len(deps) - completed_count
                        
                        await dep_conn.execute(
                            """
                            UPDATE tasks 
                            SET blocked_by_count = %s, updated_at = now()
                            WHERE id = %s
                            """,
                            [remaining_count, dependent_task_id]
                        )
                        
                        if remaining_count == 0:
                            logger.info(f"Task {dependent_task_id} is now ready to run (all {len(deps)} dependencies completed)")
                
                logger.info(f"Task {task_id} status updated to {status}")
                return True
    
    async def get_ready_tasks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取可执行的任务（blocked_by_count = 0 且 status = pending）
        
        Args:
            limit: 最多返回的任务数
            
        Returns:
            可执行的任务列表
        """
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT id, subject, description, metadata
                FROM tasks
                WHERE status = 'pending' AND blocked_by_count = 0
                ORDER BY 
                    CASE WHEN metadata->>'priority' IS NOT NULL 
                         THEN (metadata->>'priority')::int 
                         ELSE 5 
                    END,
                    updated_at
                LIMIT %s
                """,
                [limit]
            )
            rows = await cursor.fetchall()
            
            ready_tasks = []
            for row in rows:
                row_dict = _row_to_dict(cursor, row)
                task = {
                    "id": row_dict["id"],
                    "subject": row_dict["subject"],
                    "description": row_dict["description"],
                }
                
                # psycopg 自动将 JSONB 解析为 dict
                if row_dict["metadata"]:
                    if isinstance(row_dict["metadata"], str):
                        task["metadata"] = json.loads(row_dict["metadata"])
                    else:
                        task["metadata"] = row_dict["metadata"]
                
                ready_tasks.append(task)
            
            return ready_tasks
    
    async def claim_task(self, task_id: str, owner: str) -> bool:
        """
        认领任务（使用 SKIP LOCKED 避免并发冲突）
        
        Args:
            task_id: 任务 ID
            owner: 认领者的标识（Agent ID）
            
        Returns:
            是否认领成功
        """
        async with self.pool.connection() as conn:
            async with conn.transaction():
                cursor = await conn.execute(
                    """
                    UPDATE tasks
                    SET owner = %s, 
                        claimed_at = NOW(),
                        lease_expires_at = NOW() + INTERVAL '60 seconds',
                        status = 'in_progress',
                        updated_at = NOW()
                    WHERE id = %s 
                      AND status = 'pending' 
                      AND blocked_by_count = 0
                    """,
                    [owner, task_id]
                )
                
                rows_updated = cursor.rowcount
                success = rows_updated > 0
                
                if success:
                    logger.info(f"Task {task_id} claimed by {owner}")
                else:
                    logger.debug(f"Task {task_id} could not be claimed (already taken or not ready)")
                
                return success

    async def claim_next_available_task(self, thread_id: str, owner: str) -> Optional[Dict[str, Any]]:
        """
        原子认领下一个可执行任务。

        相比先 get_available_tasks 再 claim_task，这个方法把选取和更新放在
        同一条 SQL 中，并使用 FOR UPDATE SKIP LOCKED，适合高并发 worker pull。
        """
        async with self.pool.connection() as conn:
            async with conn.transaction():
                cursor = await conn.execute(
                    """
                    WITH candidate AS (
                        SELECT id
                        FROM tasks
                        WHERE thread_id = %s
                          AND status = 'pending'
                          AND blocked_by_count = 0
                          AND (owner IS NULL OR lease_expires_at < NOW())
                        ORDER BY
                            CASE WHEN metadata->>'priority' IS NOT NULL
                                 THEN (metadata->>'priority')::int
                                 ELSE 5
                            END,
                            updated_at,
                            id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE tasks AS t
                    SET owner = %s,
                        claimed_at = NOW(),
                        lease_expires_at = NOW() + INTERVAL '60 seconds',
                        status = 'in_progress',
                        updated_at = NOW()
                    FROM candidate
                    WHERE t.id = candidate.id
                    RETURNING t.id, t.subject, t.description, t.metadata
                    """,
                    [thread_id, owner]
                )
                row = await cursor.fetchone()

                if not row:
                    return None

                task = _row_to_dict(cursor, row)
                if task.get("metadata") and isinstance(task["metadata"], str):
                    task["metadata"] = json.loads(task["metadata"])
                logger.info(f"Task {task['id']} claimed by {owner}")
                return task
    
    async def get_task_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        根据 ID 获取任务
        
        Args:
            task_id: 任务 ID
            
        Returns:
            任务信息字典，不存在则返回 None
        """
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT id, subject, description, owner, status, blocked_by_count, 
                       claimed_at, lease_expires_at, last_heartbeat, metadata
                FROM tasks
                WHERE id = %s
                """,
                [task_id]
            )
            row = await cursor.fetchone()
            
            if row:
                return _row_to_dict(cursor, row)
            
            return None
    
    async def get_task_dependencies(self, task_id: str) -> List[str]:
        """
        获取任务的依赖列表（blocked_by）
        
        Args:
            task_id: 任务 ID
            
        Returns:
            依赖的任务 ID 列表
        """
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT blocker_id FROM task_dependencies 
                WHERE task_id = %s
                ORDER BY blocker_id
                """,
                [task_id]
            )
            rows = await cursor.fetchall()
            return [_row_to_dict(cursor, row)["blocker_id"] for row in rows]
    
    async def get_task_dependents(self, task_id: str) -> List[str]:
        """
        获取依赖该任务的下游任务列表
        
        Args:
            task_id: 任务 ID
            
        Returns:
            下游任务 ID 列表
        """
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT task_id FROM task_dependencies 
                WHERE blocker_id = %s
                ORDER BY task_id
                """,
                [task_id]
            )
            rows = await cursor.fetchall()
            return [_row_to_dict(cursor, row)["task_id"] for row in rows]
    
    async def get_available_tasks(self, thread_id: str, limit: int = 10) -> List[Dict]:
        """
        获取可认领的任务（支持超时回收检测）
        
        Args:
            thread_id: 线程 ID
            limit: 最多返回的任务数
            
        Returns:
            可认领的任务列表
        """
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT id, subject, description, metadata
                FROM tasks
                WHERE thread_id = %s
                  AND status = 'pending'
                  AND blocked_by_count = 0
                  AND (owner IS NULL OR lease_expires_at < NOW())
                ORDER BY 
                    CASE WHEN metadata->>'priority' IS NOT NULL 
                         THEN (metadata->>'priority')::int 
                         ELSE 5 
                    END,
                    updated_at
                LIMIT %s
                """,
                [thread_id, limit]
            )
            rows = await cursor.fetchall()
            return [_row_to_dict(cursor, row) for row in rows]
    
    async def renew_lease(self, task_id: str, owner: str, lease_duration: int = 60) -> bool:
        """
        续期 lease
        
        Args:
            task_id: 任务 ID
            owner: 认领者标识
            lease_duration: lease 时长（秒）
            
        Returns:
            是否续期成功
        """
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE tasks
                SET lease_expires_at = NOW() + INTERVAL '%s seconds',
                    last_heartbeat = NOW(),
                    updated_at = NOW()
                WHERE id = %s AND owner = %s
                """,
                [lease_duration, task_id, owner]
            )
            return cursor.rowcount > 0
    
    async def fail_task(self, task_id: str, error: str, retry_count: int, max_retry: int = 1) -> bool:
        """
        任务失败处理
        
        Args:
            task_id: 任务 ID
            error: 错误信息
            retry_count: 当前重试次数
            max_retry: 最大重试次数
            
        Returns:
            是否可重试（True 表示可重试，False 表示失败）
        """
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if retry_count >= max_retry:
                    await conn.execute(
                        """
                        UPDATE tasks 
                        SET status = 'failed',
                            owner = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{last_error}',
                                to_jsonb(%s::text)
                            ) || jsonb_build_object('retry_count', to_jsonb(%s))
                        WHERE id = %s
                        """,
                        [error, retry_count, task_id]
                    )
                    return False
                else:
                    await conn.execute(
                        """
                        UPDATE tasks 
                        SET status = 'pending',
                            owner = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{last_error}',
                                to_jsonb(%s::text)
                            ) || jsonb_build_object('retry_count', to_jsonb(%s))
                        WHERE id = %s
                        """,
                        [error, retry_count, task_id]
                    )
                    return True
    
    async def complete_task(self, task_id: str, summary: str, result_path: str | None = None) -> bool:
        """
        完成任务
        
        Args:
            task_id: 任务 ID
            summary: 任务摘要（≤1000 字符）
            result_path: 结果文件路径（可选）
            
        Returns:
            是否完成成功
        """
        MAX_SUMMARY_LENGTH = 1000
        
        if len(summary) > MAX_SUMMARY_LENGTH:
            summary = summary[:MAX_SUMMARY_LENGTH-3] + "..."
        
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if result_path:
                    await conn.execute(
                        """
                        UPDATE tasks 
                        SET status = 'completed',
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                jsonb_set(
                                    COALESCE(metadata, '{}'::jsonb),
                                    '{summary}',
                                    to_jsonb(%s::text)
                                ),
                                '{result_path}',
                                to_jsonb(%s::text)
                            )
                        WHERE id = %s
                        """,
                        [summary, result_path, task_id]
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE tasks 
                        SET status = 'completed',
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{summary}',
                                to_jsonb(%s::text)
                            )
                        WHERE id = %s
                        """,
                        [summary, task_id]
                    )
                
                await self._update_downstream_dependencies(conn, task_id)
                
                return True
    
    async def _update_downstream_dependencies(self, conn, task_id: str):
        """
        更新下游任务的 blocked_by_count
        
        Args:
            conn: 数据库连接
            task_id: 已完成的任务 ID
        """
        cursor = await conn.execute(
            """
            SELECT task_id FROM task_dependencies WHERE blocker_id = %s
            """,
            [task_id]
        )
        dependents_result = await cursor.fetchall()
        
        for row in dependents_result:
            row_dict = _row_to_dict(cursor, row)
            dependent_task_id = row_dict["task_id"]
            
            deps_cursor = await conn.execute(
                """
                SELECT blocker_id FROM task_dependencies 
                WHERE task_id = %s
                """,
                [dependent_task_id]
            )
            deps = await deps_cursor.fetchall()
            
            completed_count = 0
            for dep_row in deps:
                dep_dict = _row_to_dict(deps_cursor, dep_row)
                blocker_status_cursor = await conn.execute(
                    """
                    SELECT status FROM tasks WHERE id = %s
                    """,
                    [dep_dict["blocker_id"]]
                )
                blocker_status = await blocker_status_cursor.fetchone()
                if blocker_status:
                    blocker_status_dict = _row_to_dict(blocker_status_cursor, blocker_status)
                    if blocker_status_dict["status"] == "completed":
                        completed_count += 1
            
            remaining_count = len(deps) - completed_count
            
            await conn.execute(
                """
                UPDATE tasks 
                SET blocked_by_count = %s, updated_at = NOW()
                WHERE id = %s
                """,
                [remaining_count, dependent_task_id]
            )
            
            if remaining_count == 0:
                logger.info(f"Task {dependent_task_id} is now ready to run (all {len(deps)} dependencies completed)")
    
    async def reclaim_leased_tasks(self) -> int:
        """
        回收 lease 过期的任务
        
        Returns:
            回收的任务数量
        """
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE tasks
                SET owner = NULL,
                    claimed_at = NULL,
                    lease_expires_at = NULL,
                    status = 'pending',
                    updated_at = NOW(),
                    metadata = jsonb_set(
                        COALESCE(metadata, '{}'::jsonb),
                        '{last_error}',
                        to_jsonb('Lease expired - agent crash detected'::text)
                    ) || jsonb_build_object(
                        'retry_count', 
                        to_jsonb(COALESCE((metadata->>'retry_count')::int, 0) + 1)
                    )
                WHERE status = 'in_progress'
                  AND lease_expires_at < NOW()
                  AND COALESCE((metadata->>'retry_count')::int, 0) < 2
                """
            )
            reclaimed = cursor.rowcount
            if reclaimed > 0:
                logger.info(f"Reclaimed {reclaimed} tasks from crashed agents")
            return reclaimed
