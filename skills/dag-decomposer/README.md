# DAG Decomposer

将复杂任务分解为可并行执行的 DAG（有向无环图）任务结构，并存入 PostgreSQL 数据库供 sub-agents 执行。

## 功能特性

- ✅ **智能分解**：基于 LLM 将复杂请求分解为 >=4 个步骤的任务
- ✅ **DAG 构建**：自动识别任务依赖关系，构建 `blockedBy` 数组
- ✅ **检环验证**：使用 DFS 检测环，确保无死锁
- ✅ **错误重试**：指数退避 + 抖动策略处理 LLM 输出错误
- ✅ **数据库持久化**：插入 `tasks` 和 `task_dependencies` 表
- ✅ **中间件集成**：可无缝集成到 Agent 流程中

## 快速开始

### 1. 安装依赖

```bash
pip install asyncpg  # 推荐使用 asyncpg
```

### 2. 初始化数据库

```bash
psql -U langcode -d langcodedb -f skills/dag-decomposer/references/edge_table.sql
```

或者使用默认配置：
```bash
psql -h localhost -p 5432 -U langcode -d langcodedb -f skills/dag-decomposer/references/edge_table.sql
```

### 3. 使用高级 API

```python
import asyncio
from skills.dag_decomposer.decomposer import DAGDecomposer

async def main():
    decomposer = DAGDecomposer(
        db_connection_params={
            "host": "localhost",
            "port": 5432,
            "database": "langcode",
            "user": "postgres",
            "password": "your_password",
        }
    )
    
    user_request = """
    生成一份周报，需要：
    1. 从数据库查询上周销售数据
    2. 爬取竞争对手新闻
    3. 分析社交媒体情感
    4. 整合生成 Markdown 报告
    """
    
    result = await decomposer.decompose(
        user_request=user_request,
        thread_id="weekly_report_20260708",
        owner="lead_agent",
    )
    
    if result["success"]:
        print(f"分解成功：{result['task_count']} 个任务")
        for task in result["tasks"]:
            print(f"  - {task['id']}: {task['subject']}")

asyncio.run(main())
```

### 4. 使用中间件集成

```python
from middlewares.dag_decomposer_middleware import DAGDecomposerMiddleware
from skills.dag_decomposer.decomposer import DAGDecomposer

decomposer = DAGDecomposer(db_connection_params=...)

middleware = DAGDecomposerMiddleware(
    decomposer=decomposer,
    thread_id="session_123",
    min_steps_for_decomposition=4,
    auto_dispatch=True,
)

# 将中间件添加到 Agent 的 middleware 列表中
```

### 5. 分发任务到 Sub-Agents

```python
from middlewares.dag_decomposer_middleware import DAGTaskDispatcher

dispatcher = DAGTaskDispatcher(
    db_connection_params=db_params,
    message_hub=message_hub,
    thread_id="session_123",
)

available_agents = ["agent_1", "agent_2", "agent_3"]
dispatched = await dispatcher.dispatch_ready_tasks(available_agents)
print(f"分发了 {dispatched} 个任务")
```

## 组件说明

### 核心组件

| 文件 | 功能 |
|------|------|
| `components/prompt.py` | 构建 LLM 提示词（包含 JSON Schema 和示例） |
| `components/parser.py` | 解析 LLM 响应，提取 DAG 结构 |
| `components/cycle_detector.py` | DFS 检环 + DAG 完整性验证 |
| `components/retry_handler.py` | 错误重试（指数退避 + 抖动） |
| `decomposer.py` | 高级 API：`DAGDecomposer` 类 |
| `lib/dag_scheduler.py` | 数据库操作（插入、更新、查询） |
| `middlewares/dag_decomposer_middleware.py` | Agent 中间件集成 |

### 数据库表结构

**tasks 表**
```sql
CREATE TABLE tasks (
    id              TEXT PRIMARY KEY,
    subject         TEXT NOT NULL,
    description     TEXT,
    owner           TEXT,
    status          TEXT CHECK (status IN ('pending','in_progress','completed')),
    blocked_by_count INT NOT NULL DEFAULT 0,
    metadata        JSONB,
    updated_at      TIMESTAMPTZ DEFAULT now()
);
```

**task_dependencies 表**
```sql
CREATE TABLE task_dependencies (
    task_id     TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    blocker_id  TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, blocker_id)
);
```

## API 参考

### DAGDecomposer

```python
decomposer = DAGDecomposer(
    llm=None,                    # 可选的 LLM 实例
    db_connection_params=None,   # 数据库连接参数
    max_retries=3,               # 最大重试次数
)

result = await decomposer.decompose(
    user_request="...",          # 用户请求
    thread_id="session_123",     # 线程 ID
    owner="lead_agent",          # 可选的 owner
    save_to_db=True,             # 是否保存到数据库
)
```

### RetryHandler

```python
handler = RetryHandler(
    max_retries=3,
    base_delay=1.0,
    max_delay=30.0,
    backoff_factor=2.0,
    jitter_factor=0.5,
)

dag_data = await handler.execute_with_retry(
    llm_call=async_llm_function,
    validator=async_validation_function,
)
```

### validate_dag

```python
from skills.dag_decomposer.components import validate_dag

is_valid, errors = validate_dag(tasks)
if not is_valid:
    print("DAG 验证失败:", errors)
```

## 触发条件

DAG 分解器仅在以下情况激活：

1. **多步骤复杂度**：任务需要 >=4 个独立逻辑步骤
2. **明确并行性**：包含可独立执行的模块
3. **外部隔离**：子任务需要不同工具/API/环境
4. **上下文风险**：估计 token 消耗超过 70% 上下文窗口

## 测试

```bash
pytest tests/test_dag_decomposer.py -v
```

## 最佳实践

### 任务描述写作（3W1H）

- **What**：定义最终输出（如 "返回 JSON 对象，包含 `result` 键"）
- **Where**：提供输入位置（如 "数据库表 `orders.public`"）
- **How**：指定约束（如 "超时 30 秒"，"5xx 错误重试 3 次"）
- **Who**：如需特殊运行时，设置 `metadata.required_capability`

### 避免过度分解

- 如果分解出 >15 个任务，考虑合并内聚的顺序步骤
- 严格线性任务（1→2→3）可能不需要分解

## 错误处理

| 错误类型 | 处理方式 |
|---------|---------|
| JSON 解析失败 | 重试，最多 3 次 |
| Schema 验证失败 | 重试并要求 LLM 修正 |
| 检测到环 | 重试并提示合并相关任务 |
| 数据库错误 | 抛出异常，由上层处理 |

## 许可证

MIT
