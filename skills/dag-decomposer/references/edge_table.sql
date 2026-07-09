CREATE TABLE tasks (
    id            TEXT PRIMARY KEY,
    subject       TEXT NOT NULL,
    description   TEXT,
    thread_id     TEXT,                     -- 会话线程 ID
    owner         TEXT,                     -- 认领的 Agent ID
    status        TEXT CHECK (status IN ('pending','in_progress','completed','failed')),
    blocked_by_count INT NOT NULL DEFAULT 0, -- 关键：当前未完成的上游依赖数量
    claimed_at    TIMESTAMPTZ,              -- 认领时间
    lease_expires_at TIMESTAMPTZ,           -- lease 过期时间
    last_heartbeat   TIMESTAMPTZ,           -- 最后心跳时间
    metadata      JSONB,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- 核心索引：Agent 认领时命中此索引
CREATE INDEX IF NOT EXISTS idx_tasks_ready ON tasks (status, blocked_by_count) 
WHERE status = 'pending' AND blocked_by_count = 0;

-- lease 过期检测索引
CREATE INDEX IF NOT EXISTS idx_tasks_lease 
ON tasks (status, lease_expires_at) 
WHERE status = 'in_progress';

-- thread_id 索引
CREATE INDEX IF NOT EXISTS idx_tasks_thread ON tasks (thread_id, status, blocked_by_count)
WHERE status = 'pending' AND blocked_by_count = 0;

-- 2. 依赖边表（专门存储 DAG 边）
CREATE TABLE task_dependencies (
    task_id     TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    blocker_id  TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, blocker_id) -- 防重复
);

-- 反向查询索引（用于快速找出“我阻塞了谁”）
CREATE INDEX idx_deps_blocker ON task_dependencies (blocker_id);