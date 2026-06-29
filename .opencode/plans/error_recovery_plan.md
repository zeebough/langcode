# 错误恢复实现方案

## 概述
在 agent loop 中添加错误恢复逻辑，采用**混合方案**：
1. **中间件层**：ErrorRecoveryMiddleware 处理 prompt_too_long 错误
2. **Agent Loop 层**：在 cli.py 中处理 429/529 API 错误，实现指数退避 + 备用模型切换
3. **配置**：通过环境变量配置备用模型

---

## 文件 1：middlewares/error_recovery_middleware.py（新增）

```python
from typing import List
from langchain.agents.middleware import AgentMiddleware, AgentState, Runtime
from langchain_core.messages import HumanMessage
from langchain_core.language_models import BaseChatModel
import logging

logger = logging.getLogger(__name__)


class ErrorRecoveryMiddleware(AgentMiddleware):
    """错误恢复中间件：处理 prompt_too_long 等错误并触发上下文压缩"""
    
    def __init__(
        self,
        context_compression_middleware: "ContextCompressionMiddleware",
    ):
        self.context_compression_middleware = context_compression_middleware
        
    async def abefore_model(
        self,
        state: AgentState,
        runtime: Runtime,
    ) -> dict[str, any] | None:
        """在模型调用前检查并处理错误状态"""
        messages = state.get("messages", [])
        if not messages:
            return None
        
        last_message = messages[-1]
        content = last_message.content if hasattr(last_message, 'content') else ""
        
        if isinstance(content, str) and "prompt_too_long" in content.lower():
            logger.warning("Detected prompt_too_long error, triggering reactive compact")
            messages = await self.context_compression_middleware._reactive_compact(messages)
            return {"messages": messages}
        
        return None
```

---

## 文件 2：cli.py（修改）

### 修改点 1：导入和配置（顶部）
```python
import asyncio
import random
import os
# ... 其他现有导入

# 备用模型配置
BACKUP_MODEL_NAME = os.getenv("BACKUP_MODEL_NAME")
BACKUP_BASE_URL = os.getenv("BACKUP_BASE_URL")

# 重试参数
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # 秒
MAX_BACKOFF = 30.0     # 秒
```

### 修改点 2：create_coding_agent 函数
```python
async def create_coding_agent(checkpointer: AsyncPostgresSaver, store: AsyncPostgresStore, use_backup: bool = False) -> Any:
    """Create the coding agent."""
    tools = [read_file, write_file, bash, edit, glob]
    
    # 根据 use_backup 参数选择主/备模型
    model_name = BACKUP_MODEL_NAME if use_backup else os.getenv("MODEL_NAME")
    base_url = BACKUP_BASE_URL if use_backup and BACKUP_BASE_URL else os.getenv("BASE_URL")
    
    llm = ChatOpenAI(
        model=model_name,
        api_key=os.getenv("API_KEY"),
        base_url=base_url,
        temperature=0.3,
        max_completion_tokens=8000
    )
    
    light_llm = ChatOpenAI(
        model=os.getenv("LIGHT_MODEL_NAME"),
        api_key=os.getenv("API_KEY"),
        base_url=os.getenv("BASE_URL"),
        temperature=0.3,
        max_completion_tokens=8000,
        verbose=False
    )
    
    system_prompt = f"""You are a coding agent LangCode. Act, don't explain.
Available tools: bash, read_file, write_file, edit_file, glob, load_skill.
Working directory: {WORK_DIR}
Always think step by step. If you are unsure about the next step, ask the user for clarification.
"""
    
    # 创建 context compression middleware 实例（供 error recovery 使用）
    context_compression = ContextCompressionMiddleware(llm=light_llm)
    
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
        middleware=[
            PermissionMiddleware(WORK_DIR),
            TodoListMiddleware(),
            MemoryManagementMiddleware(llm=light_llm, store=store),
            context_compression,
            ErrorRecoveryMiddleware(context_compression_middleware=context_compression),  # 新增
            SkillLoadingMiddleware(WORK_DIR),
        ],
        checkpointer=checkpointer,
    )
    return agent
```

### 修改点 3：run_streaming 函数中的 agent loop
```python
async def run_streaming():
    """Streaming CLI with conversation memory."""
    # ... 现有初始化代码保持不变 ...
    
    # 主体会话逻辑
    user_id = input("输入用户 ID (空白则为匿名用户): ").strip() or "匿名用户"
    thread_id=input("输入 thread_id 恢复对话 (空白则为新对话): ").strip()
    if not thread_id or thread_id == "" or thread_id=="\n":
        thread_id=f"langcode_session_{id({})}"
    config = {"configurable": {"thread_id": thread_id, "user_id": user_id}}
    
    print("=" * 50)
    print("LangCode - 编码助手")
    print("输入 'exit' 退出，输入 'reset' 重置会话")
    print("=" * 50)
    print(f"🤖 Session started with thread_id: {thread_id} for user: {user_id}")
    
    # 跟踪连续 529 错误次数
    consecutive_529_count = 0
    use_backup_model = False
    
    while True:
        user_input = input("\033[36m用户 >> \033[0m")
        if user_input.lower() == "exit":
            break
        if user_input.lower() == "reset":
            thread_id = f"langcode_session_{id({})}"
            config = {"configurable": {"thread_id": thread_id, "user_id": user_id}}
            print("🤖 会话已重置")
            continue
        
        # 重试逻辑
        attempt = 0
        backoff = INITIAL_BACKOFF
        last_error = None
        
        while attempt <= MAX_RETRIES:
            try:
                print(f"\033[32m🤖 LangCode >> \033[0m", end="", flush=True)
                
                # 如果已切换到备用模型，重新创建 agent
                if use_backup_model and attempt == 0:
                    agent = await create_coding_agent(checkpointer, store, use_backup=True)
                    print("\n[!] 已切换到备用模型")
                
                # Stream events from the agent
                async for event in agent.astream_events(
                    input={"messages": [{"role": "user", "content": user_input}]},
                    config=config,
                    version="v2",
                ):
                    kind = event.get("event")
                    if kind == "on_chat_model_stream":
                        tags = event.get("tags", [])
                        if "internal_memory_call" in tags:
                            continue
                        name = event.get("name", "")
                        if "Middleware" in name:
                            continue
                        content = event.get("data", {}).get("chunk", {}).content
                        if content:
                            print(content, end="", flush=True)
                        continue
                        
                    if kind == "on_tool_start" and event.get("name") == "write_todos":
                        print("\n[Planning] Agent updated todo list")
                        continue
                    
                    if kind == "on_tool_end" and event.get("name") == "write_todos":
                        todos = event.get("data", {}).get("output")
                        if todos:
                            print("\n[任务列表更新]")
                            for todo in todos:
                                print(f"  {todo['status']}: {todo['content']}")
                        continue
                
                print()  # newline after response
                
                # 成功响应，重置错误计数
                consecutive_529_count = 0
                break  # 退出重试循环
                
            except Exception as e:
                last_error = e
                error_str = str(e)
                
                # 检查是否是 429 或 529 错误
                if "429" in error_str or "529" in error_str or "rate limit" in error_str.lower():
                    if "529" in error_str:
                        consecutive_529_count += 1
                    else:
                        consecutive_529_count = 0  # 429 重置计数
                    
                    # 连续 5 次 529 错误，切换到备用模型
                    if consecutive_529_count >= 5 and not use_backup_model:
                        use_backup_model = True
                        print("\n[!] 连续 5 次 529 错误，切换到备用模型")
                        continue  # 立即重试，不等待
                    
                    # 指数退避 + 抖动
                    if attempt < MAX_RETRIES:
                        jitter = random.uniform(0, backoff * 0.1)
                        wait_time = min(backoff + jitter, MAX_BACKOFF)
                        print(f"\n[!] API 限流/临时故障，{wait_time:.1f}秒后重试 (尝试 {attempt + 1}/{MAX_RETRIES})")
                        await asyncio.sleep(wait_time)
                        backoff = min(backoff * 2, MAX_BACKOFF)
                        attempt += 1
                    else:
                        print(f"\n[!] 重试 {MAX_RETRIES} 次后仍失败：{e}")
                        break
                else:
                    # 其他错误，直接抛出
                    print(f"\n[!] 错误：{e}")
                    break
        
        if attempt > MAX_RETRIES and last_error:
            print(f"\n[!] 请求最终失败：{last_error}")
    
    # Clean up
    await pool.close()
```

---

## 文件 3：.env.example（更新）

```bash
# 主模型配置
MODEL_NAME=your-main-model
BASE_URL=https://api.main-model.com
API_KEY=your-api-key

# 轻量模型（用于记忆/压缩）
LIGHT_MODEL_NAME=your-light-model

# 备用模型配置（可选）
BACKUP_MODEL_NAME=your-backup-model
BACKUP_BASE_URL=https://api.backup-model.com

# PostgreSQL 配置
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_HOST=...
POSTGRES_PORT=...
POSTGRES_DB=...
```

---

## 执行步骤

1. **创建** `middlewares/error_recovery_middleware.py`
2. **修改** `cli.py`：
   - 顶部添加导入和配置
   - 修改 `create_coding_agent` 函数签名和逻辑
   - 修改 `run_streaming` 中的 agent loop 添加重试逻辑
3. **更新** `.env` 添加备用模型配置（可选）

---

## 关键特性

| 错误类型 | 处理策略 |
|---------|---------|
| `prompt_too_long` | ErrorRecoveryMiddleware 触发 reactive compact |
| `429` (限流) | 指数退避 + 抖动，重置连续 529 计数 |
| `529` (临时故障) | 指数退避 + 抖动，连续 5 次切换备用模型 |
| 其他错误 | 直接抛出，显示错误信息 |

**重试参数**：
- 初始等待：1s
- 最大等待：30s
- 最多重试：3 次
- 抖动范围：0 ~ 10% 的 backoff 时间

---

## 注意事项

1. **中间件顺序**：ErrorRecoveryMiddleware 必须在 ContextCompressionMiddleware 之后，以便访问其 `_reactive_compact` 方法
2. **类型提示**：`ErrorRecoveryMiddleware.__init__` 中使用了前向引用 `"ContextCompressionMiddleware"`，避免循环导入
3. **备用模型切换**：切换后当前会话会一直使用备用模型，直到会话重置
4. **错误检测**：通过错误消息字符串匹配检测 429/529，如 API 返回格式不同需调整检测逻辑

---

## 后续优化（可选）

1. **LLM Router 类**：如需更复杂的模型管理（健康检查、多备用模型池）
2. **持久化错误计数**：跨会话跟踪模型故障率
3. **告警通知**：连续错误时发送通知
4. **更细粒度的错误分类**：区分不同 API 提供商的错误格式
