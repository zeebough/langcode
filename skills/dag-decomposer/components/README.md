# DAG Decomposer Components

本目录包含 DAG 分解器的核心组件：

## 文件说明

- `__init__.py` - 模块导出
- `prompt.py` - LLM 提示词模板构建器
- `parser.py` - LLM 响应解析器
- `cycle_detector.py` - DAG 检环与验证逻辑
- `retry_handler.py` - 错误重试处理器

## 使用方式

```python
from skills.dag_decomposer.components import (
    build_decomposer_prompt,
    parse_dag_response,
    detect_cycles,
    validate_dag,
    RetryHandler,
)

# 构建提示词
prompt = build_decomposer_prompt(user_request)

# 解析响应
dag_data, errors = parse_dag_response(llm_response)

# 验证 DAG
is_valid, errors = validate_dag(tasks)

# 使用重试处理器
retry_handler = RetryHandler(max_retries=3)
dag_data = await retry_handler.execute_with_retry(llm_call, validator)
```

## 主入口

使用高级 API 请参考 `skills/dag-decomposer/decomposer.py` 中的 `DAGDecomposer` 类。
