# Agent Team 机制设计方案

## 概述

实现 Lead Agent + Sub Agent 的团队机制，支持：
- Lead Agent 创建和管理多个 Sub Agent
- Sub Agent 独立执行多轮任务，完成后自动汇报
- 异步 Message Hub 支持持久化消息传递
- 权限冒泡机制：Sub Agent 遇到危险操作时请求 Lead Agent 审批

---

## 1. Lead Agent 新增工具

| 工具名 | 功能 | 参数 |
|--------|------|------|
| `spawn_sub_agent` | 创建 sub agent | `name: str, role: str, task: str, max_rounds: int = 30` |
| `assign_task` | 分配任务给 idle agent | `agent_name: str, task: str` |
| `list_sub_agents` | 查看所有 agent 状态 | - |
| `shutdown_agent` | 关闭 agent | `agent_name: str` |
| `send_message` | 发送消息到 Message Hub | `to: str, content: dict, msg_type: str` |
| `check_inbox` | 检查 Lead Agent 收件箱 | - |
| `list_pending_permissions` | 列出待审批的权限请求 | - |
| `approve_permission` | 批准权限请求 | `request_id: str, reason: str = ""` |
| `reject_permission` | 拒绝权限请求 | `request_id: str, reason: str` |

---

## 2. Message Hub 技术方案

### 2.1 技术选型

**使用原生 `psycopg_pool.AsyncConnectionPool` + PostgreSQL LISTEN/NOTIFY**

不选用 `AsyncPostgresStore` 的理由：
- `AsyncPostgresStore` 是 LangGraph 的 KV 存储抽象，适合存文档/记忆
- 消息队列需要：发布/订阅、按收件人查询、已读标记、超时轮询
- 需要直接使用原生 SQL + LISTEN/NOTIFY 实现实时通知

### 2.2 数据库表设计

```sql
-- 消息表（Message Hub）
CREATE TABLE agent_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_agent VARCHAR(255) NOT NULL,
    to_agent VARCHAR(255) NOT NULL,
    content JSONB NOT NULL,  -- 结构化内容
    msg_type VARCHAR(50) NOT NULL,  -- message/task/result/permission_request/permission_response
    thread_id VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    read_at TIMESTAMPTZ,
    
    INDEX idx_inbox (to_agent, thread_id, created_at),
    INDEX idx_unread (to_agent, read_at) WHERE read_at IS NULL
);

-- 权限审计表（独立表，方便审计）
CREATE TABLE permission_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id VARCHAR(255) UNIQUE NOT NULL,
    agent_name VARCHAR(255) NOT NULL,
    tool_name VARCHAR(100) NOT NULL,
    command TEXT NOT NULL,
    decision VARCHAR(20),  -- approved/rejected/timeout
    reason TEXT,
    decided_by VARCHAR(255),  -- 'user' 或 'system'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    decided_at TIMESTAMPTZ,
    
    INDEX idx_agent (agent_name, created_at),
    INDEX idx_decision (decision, created_at)
);
```

### 2.3 Message Hub 类设计

```python
# lib/message_hub.py
class AsyncPostgresMessageHub:
    """基于 PostgreSQL 的异步消息中心"""
    
    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool
    
    async def setup(self):
        """初始化表结构"""
        async with self.pool.connection() as conn:
            await conn.execute(CREATE_TABLE_SQL)
    
    async def send(
        self,
        from_agent: str,
        to_agent: str,
        content: dict | str,
        msg_type: str,
        thread_id: str | None = None,
    ):
        """发送消息并触发 NOTIFY"""
        async with self.pool.connection() as conn:
            await conn.execute(
                """INSERT INTO agent_messages 
                   (from_agent, to_agent, content, msg_type, thread_id)
                   VALUES (%s, %s, %s, %s, %s)""",
                [from_agent, to_agent, json.dumps(content), msg_type, thread_id]
            )
            await conn.execute(
                "NOTIFY agent_message, %s",
                [json.dumps({"to_agent": to_agent, "msg_type": msg_type})]
            )
    
    async def read_inbox(self, agent_name: str, thread_id: str | None = None) -> list[dict]:
        """读取并标记已读（消费式读取）"""
        async with self.pool.connection() as conn:
            # 查询未读消息
            rows = await conn.execute(
                """SELECT id, from_agent, content, msg_type, created_at
                   FROM agent_messages
                   WHERE to_agent = %s AND read_at IS NULL
                   ORDER BY created_at ASC""",
                [agent_name]
            )
            messages = [dict(row) for row in rows]
            
            # 标记已读
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
    ) -> dict | None:
        """使用 LISTEN/NOTIFY 等待消息（用于 idle 轮询）"""
        async with self.pool.connection() as conn:
            await conn.execute("LISTEN agent_message")
            
            # 等待通知
            async for notify in conn.notifies(timeout=timeout):
                payload = json.loads(notify.payload)
                if payload["to_agent"] == agent_name:
                    if msg_type is None or payload["msg_type"] == msg_type:
                        # 有新消息，读取它
                        messages = await self.read_inbox(agent_name)
                        if messages:
                            return messages[-1]
        
        return None  # 超时
    
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
```

---

## 3. Sub Agent Loop 设计

### 3.1 使用 `create_agent` 创建 Sub Agent

```python
# lib/sub_agent.py
async def create_sub_agent(
    name: str,
    role: str,
    task: str,
    llm: ChatOpenAI,
    checkpointer: AsyncPostgresSaver,
    message_hub: AsyncPostgresMessageHub,
    thread_id: str,
) -> Runnable:
    """创建 sub agent（复用 create_agent 和中间件）"""
    
    # Sub agent 简化工具集
    tools = [bash, read_file, write_file, edit_file, glob]
    
    # 权限中间件（硬编码危险模式）
    permission_middleware = PermissionMiddleware(
        work_dir=WORK_DIR,
        message_hub=message_hub,
        agent_name=name,
    )
    
    system_prompt = f"""You are '{name}', a {role} agent.
Working directory: {WORK_DIR}
Task: {task}

Instructions:
1. Complete the task using available tools
2. If an operation requires permission, wait for approval
3. After completion, send result to 'lead' via send_message tool
4. Then wait for new tasks
"""
    
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
        middleware=[
            permission_middleware,
            ErrorRecoveryMiddleware(...),  # 复用现有错误恢复
        ],
        checkpointer=checkpointer,
    )
    
    return agent
```

### 3.2 Sub Agent 执行循环

```python
async def run_sub_agent(
    name: str,
    role: str,
    initial_task: str,
    llm: ChatOpenAI,
    checkpointer: AsyncPostgresSaver,
    message_hub: AsyncPostgresMessageHub,
    thread_id: str,
    max_rounds: int = 30,
):
    """Sub Agent 独立执行循环"""
    
    # 状态
    status = "working"
    current_task = initial_task
    
    while status != "shutdown":
        if status == "idle":
            # Idle 等待：使用 LISTEN/NOTIFY 等待新任务
            task_msg = await message_hub.wait_for_message(
                agent_name=name,
                timeout=300,  # 5 分钟超时
                msg_type="task",
            )
            
            if task_msg is None:
                continue  # 超时，继续等待
            
            current_task = task_msg["content"]["task"]
            status = "working"
        
        # 执行任务
        agent = await create_sub_agent(
            name=name,
            role=role,
            task=current_task,
            llm=llm,
            checkpointer=checkpointer,
            message_hub=message_hub,
            thread_id=thread_id,
        )
        
        messages = [{"role": "user", "content": current_task}]
        config = {"configurable": {"thread_id": f"{thread_id}_{name}"}}
        
        result = None
        for round_num in range(max_rounds):
            # 执行一步
            response = await agent.ainvoke({"messages": messages}, config=config)
            
            # 检查是否完成（LLM 表示任务完成或无工具调用）
            last_message = response["messages"][-1]
            if isinstance(last_message, AIMessage):
                if not last_message.tool_calls:
                    result = last_message.content
                    break
            
            messages = response["messages"]
        
        # 汇报结果给 Lead Agent
        await message_hub.send(
            from_agent=name,
            to_agent="lead",
            content={"task": current_task, "result": result or "Task completed"},
            msg_type="result",
            thread_id=thread_id,
        )
        
        # 状态转为 idle
        status = "idle"
```

---

## 4. 权限冒泡机制

### 4.1 硬编码危险模式列表

```python
# middlewares/permission_middleware.py
DANGEROUS_PATTERNS = [
    {"tool": "bash", "pattern": "rm -rf"},
    {"tool": "bash", "pattern": "sudo"},
    {"tool": "bash", "pattern": "chmod 777"},
    {"tool": "bash", "pattern": "curl.*\\|.*sh"},
    {"tool": "bash", "pattern": "wget.*\\|.*sh"},
    {"tool": "write_file", "pattern": "/etc/"},
    {"tool": "write_file", "pattern": "/usr/"},
    {"tool": "edit_file", "pattern": "/etc/"},
]
```

### 4.2 权限中间件实现

```python
# middlewares/permission_middleware.py
class PermissionMiddleware(BaseMiddleware):
    """权限检查中间件 - 支持权限冒泡"""
    
    def __init__(
        self,
        work_dir: Path,
        message_hub: AsyncPostgresMessageHub,
        agent_name: str,
    ):
        self.work_dir = work_dir
        self.message_hub = message_hub
        self.agent_name = agent_name
    
    async def pre_tool_use(self, tool_name: str, tool_input: dict) -> str | None:
        """检查是否需要审批，返回 None 表示继续执行，返回 str 表示阻断"""
        for rule in DANGEROUS_PATTERNS:
            if tool_name == rule["tool"]:
                cmd = tool_input.get("command", "") or tool_input.get("path", "")
                if rule["pattern"] in cmd:
                    # 需要审批
                    request_id = f"perm_{uuid4().hex[:8]}"
                    
                    # 发送 permission_request 到 Lead
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
                    
                    # 记录审计日志
                    await self.message_hub.log_permission_request(
                        request_id=request_id,
                        agent_name=self.agent_name,
                        tool_name=tool_name,
                        command=cmd,
                    )
                    
                    # 等待审批（轮询，每 500ms 检查）
                    decision = await self._wait_for_permission_decision(request_id)
                    
                    # 记录审计结果
                    await self.message_hub.log_permission_decision(
                        request_id=request_id,
                        decision=decision["decision"],
                        reason=decision.get("reason"),
                        decided_by="user",
                    )
                    
                    if decision["decision"] != "approved":
                        return f"Permission denied: {decision.get('reason', 'No reason provided')}"
                    
                    return None  # 审批通过，继续执行
        
        return None  # 无需审批
    
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
            
            # 检查收件箱
            messages = await self.message_hub.read_inbox(self.agent_name)
            for msg in messages:
                if msg["msg_type"] == "permission_response":
                    content = msg["content"]
                    if content.get("request_id") == request_id:
                        return content
            
            # 超时检查（在消息处理中完成）
        
        # 超时，自动拒绝
        return {"decision": "rejected", "reason": "Timeout waiting for approval"}
```

### 4.3 Lead Agent 审批工具

```python
# cli.py - Lead Agent 工具
@tool
def list_pending_permissions() -> str:
    """列出所有待审批的权限请求"""
    # 查询 permission_audit_log 表中 decision IS NULL 的记录
    ...

@tool
def approve_permission(request_id: str, reason: str = "") -> str:
    """批准权限请求"""
    # 更新 permission_audit_log 表
    # 发送 permission_response 到请求 agent
    await message_hub.send(
        from_agent="lead",
        to_agent=request.agent_name,
        content={
            "request_id": request_id,
            "decision": "approved",
            "reason": reason,
        },
        msg_type="permission_response",
    )
    ...

@tool
def reject_permission(request_id: str, reason: str) -> str:
    """拒绝权限请求"""
    # 更新 permission_audit_log 表
    # 发送 permission_response 到请求 agent
    await message_hub.send(
        from_agent="lead",
        to_agent=request.agent_name,
        content={
            "request_id": request_id,
            "decision": "rejected",
            "reason": reason,
        },
        msg_type="permission_response",
    )
    ...
```

---

## 5. CLI 交互设计

### 5.1 审批队列显示

在下一个用户输入提示时显示待审批队列：

```python
# cli.py - run_streaming()
while True:
    # 检查待审批队列
    pending_permissions = await get_pending_permissions()
    if pending_permissions:
        print("\n\033[33m[待审批权限请求]\033[0m")
        for i, perm in enumerate(pending_permissions, 1):
            print(f"  [{i}] {perm['agent_name']}: {perm['command']}")
            print(f"      工具：{perm['tool_name']}")
            print(f"      请求 ID: {perm['request_id']}")
        print("\n使用 approve_permission <request_id> [reason] 或 reject_permission <request_id> <reason>")
    
    user_input = input("\033[36m用户 >> \033[0m")
    ...
```

### 5.2 审批命令

```python
# 用户输入示例：
# approve_permission perm_abc123 需要清理临时文件
# reject_permission perm_abc123 危险操作，不允许

# 在 CLI 中解析审批命令
if user_input.startswith("approve_permission "):
    parts = user_input.split(" ", 2)
    request_id = parts[1]
    reason = parts[2] if len(parts) > 2 else ""
    # 调用 approve_permission 工具
    ...
elif user_input.startswith("reject_permission "):
    parts = user_input.split(" ", 2)
    request_id = parts[1]
    reason = parts[2] if len(parts) > 2 else "No reason provided"
    # 调用 reject_permission 工具
    ...
```

---

## 6. 实施计划

### Phase 1: Message Hub
- [ ] 创建 `agent_messages` 表
- [ ] 创建 `permission_audit_log` 表
- [ ] 实现 `AsyncPostgresMessageHub` 类
- [ ] 实现 LISTEN/NOTIFY 等待机制

### Phase 2: 权限中间件
- [ ] 更新 `PermissionMiddleware` 支持权限冒泡
- [ ] 硬编码 `DANGEROUS_PATTERNS` 列表
- [ ] 实现轮询等待审批逻辑

### Phase 3: Sub Agent Loop
- [ ] 实现 `create_sub_agent` 函数
- [ ] 实现 `run_sub_agent` 循环
- [ ] 集成 PermissionMiddleware
- [ ] 实现 idle 等待 + 任务认领

### Phase 4: Lead Agent 工具
- [ ] 实现 `spawn_sub_agent` 工具
- [ ] 实现 `assign_task` 工具
- [ ] 实现 `list_sub_agents` 工具
- [ ] 实现 `shutdown_agent` 工具
- [ ] 实现 `list_pending_permissions` 工具
- [ ] 实现 `approve_permission` 工具
- [ ] 实现 `reject_permission` 工具

### Phase 5: CLI 集成
- [ ] 在 CLI 中显示待审批队列
- [ ] 实现审批命令解析
- [ ] 集成 Message Hub 连接
- [ ] 测试完整流程

---

## 7. 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                         Lead Agent                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Tools:                                                    │  │
│  │ - spawn_sub_agent, assign_task, list_sub_agents           │  │
│  │ - send_message, check_inbox                               │  │
│  │ - list_pending_permissions, approve_permission, reject    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              ▲                                   │
│                              │ permission_request/response       │
│                              ▼                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ CLI: 显示待审批队列，用户输入 Y/n 审批                      │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ PostgreSQL + LISTEN/NOTIFY
                              │ (AsyncPostgresMessageHub)
                              │
┌─────────────────────────────────────────────────────────────────┐
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Sub Agent 1 │  │ Sub Agent 2 │  │ Sub Agent 3 │             │
│  │ (working)   │  │ (idle)      │  │ (working)   │             │
│  │             │  │             │  │             │             │
│  │ Tools:      │  │ Tools:      │  │ Tools:      │             │
│  │ - bash      │  │ - bash      │  │ - bash      │             │
│  │ - read_file │  │ - read_file │  │ - read_file │             │
│  │ - write_file│  │ - write_file│  │ - write_file│             │
│  │ - edit_file │  │ - edit_file │  │ - edit_file │             │
│  │ - glob      │  │ - glob      │  │ - glob      │             │
│  │             │  │             │  │             │             │
│  │ Permission  │  │ Permission  │  │ Permission  │             │
│  │ Middleware  │  │ Middleware  │  │ Middleware  │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 8. 关键设计决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| **Message Hub** | 原生 psycopg + LISTEN/NOTIFY | AsyncPostgresStore 不支持 pub/sub |
| **权限规则** | 硬编码危险模式列表 | 保守设计，简单可靠 |
| **审批交互** | 下一轮用户输入时显示队列 | 不打断当前输出，用户体验好 |
| **批量审批** | 不支持 | 保守设计，每个请求单独审批 |
| **Sub Agent 创建** | 使用 `create_agent` | 复用现有中间件和 checkpointer |
| **Idle 等待** | LISTEN/NOTIFY + 轮询 | 平衡实时性和资源消耗 |

---

## 9. 文件结构

```
langcode/
├── cli.py                          # 主 CLI，集成 Lead Agent
├── lib/
│   ├── message_hub.py              # AsyncPostgresMessageHub
│   └── sub_agent.py                # create_sub_agent, run_sub_agent
├── middlewares/
│   └── permission_middleware.py    # 权限冒泡中间件
└── AGENT_TEAM_DESIGN.md            # 本设计文档
```
