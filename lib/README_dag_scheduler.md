# DAG Scheduler - psycopg_pool 版本

## API 统一说明

`lib/dag_scheduler.py` 已更新为使用 `psycopg_pool` 连接池，与 `lib/message_hub.py` 保持一致。

## 主要变更

### 1. 使用连接池

```python
from psycopg_pool import AsyncConnectionPool
from lib.dag_scheduler import DAGScheduler

pool = AsyncConnectionPool(
    conninfo="host=localhost port=5432 dbname=langcodedb user=langcode password=xxx",
    min_size=1,
    max_size=4,
)

scheduler = DAGScheduler(pool)
```

### 2. 行转换函数

由于 psycopg 3 的 `fetchall()` 默认返回元组，使用辅助函数 `_row_to_dict()` 转换为字典：

```python
def _row_to_dict(cursor, row) -> Dict[str, Any]:
    """将 psycopg 的 row 转换为字典"""
    if row is None:
        return None
    return dict(zip([desc.name for desc in cursor.description], row))
```

### 3. 获取受影响行数

使用 `cursor.rowcount` 而不是解析字符串：

```python
cursor = await conn.execute("UPDATE tasks SET ...", [params])
rows_updated = cursor.rowcount
success = rows_updated > 0
```

### 4. JSONB 处理

psycopg 自动将 JSONB 类型解析为 Python 字典，不需要 `json.loads()`：

```python
if row_dict["metadata"]:
    if isinstance(row_dict["metadata"], str):
        task["metadata"] = json.loads(row_dict["metadata"])
    else:
        task["metadata"] = row_dict["metadata"]  # psycopg 已解析为 dict
```

## 使用示例

```python
from psycopg_pool import AsyncConnectionPool
from lib.dag_scheduler import DAGScheduler

# 创建连接池
pool = AsyncConnectionPool(conninfo="...")

# 创建调度器
scheduler = DAGScheduler(pool)

# 插入 DAG
result = await scheduler.insert_dag_to_db(
    dag_data={
        "plan_summary": "测试",
        "tasks": [
            {"id": "task1", "subject": "任务 1", "description": "...", "blockedBy": []},
        ]
    },
    thread_id="session_123",
    owner="agent_1",
)

# 获取就绪任务
ready_tasks = await scheduler.get_ready_tasks(limit=10)

# 认领任务
claimed = await scheduler.claim_task(task_id="task1", owner="worker_1")

# 更新状态
await scheduler.update_task_status(
    task_id="task1",
    status="completed",
    result="任务完成",
)

# 获取任务详情
task = await scheduler.get_task_by_id("task1")

# 获取依赖关系
deps = await scheduler.get_task_dependencies("task1")
dependents = await scheduler.get_task_dependents("task1")

# 关闭连接池
await pool.close()
```

## 测试结果

```bash
# 测试基础操作
python tests/test_dag_scheduler.py

# 测试依赖更新
python tests/test_dependency_update.py
```

所有测试通过 ✅

## 与 message_hub.py 的一致性

| 特性 | message_hub.py | dag_scheduler.py |
|------|---------------|------------------|
| 连接池 | `AsyncConnectionPool` | `AsyncConnectionPool` |
| 获取连接 | `async with pool.connection()` | `async with pool.connection()` |
| 行转换 | `dict(row)` (依赖 row_factory) | `_row_to_dict(cursor, row)` |
| 参数占位符 | `%s` | `%s` |
| 参数传递 | `[param1, param2]` | `[param1, param2]` |
| 受影响行数 | `cursor.rowcount` | `cursor.rowcount` |

## 注意事项

1. **不要使用 `$1, $2` 占位符**：psycopg 使用 `%s`
2. **参数必须是列表**：不能用元组或单个值
3. **fetchall() 返回元组**：需要使用 `_row_to_dict()` 转换
4. **JSONB 自动解析**：psycopg 自动将 JSONB 转为 Python dict
5. **连接池管理**：使用完毕后调用 `await pool.close()`
