from typing import List
from langchain.agents.middleware import AgentMiddleware, AgentState, Runtime
from langchain_core.messages import (
    BaseMessage, 
    ToolMessage, 
    AIMessage, 
    HumanMessage, 
)
from langchain_core.language_models import BaseChatModel
import tiktoken
import time
import hashlib
import aiofiles
import contextvars

# Context var to track internal LLM calls (avoid recursion)
_internal_call = contextvars.ContextVar("_internal_call", default=False)



class ContextCompressionMiddleware(AgentMiddleware):
    """在每次模型调用前执行上下文压缩 pipeline"""
    
    def __init__(
        self,
        llm: BaseChatModel,                     # 用于 autoCompact 的大模型
        max_tokens: int = 128000,         # 模型上下文窗口上限（例如 Claude 3 为 200k，GPT-4 为 128k）
        tool_result_size_threshold: int = 200 * 1024,  # 200KB
        tool_preview_chars: int = 2000,
        max_message_count: int = 50,      # snipCompact 保留头尾总数
        recent_tool_results_keep: int = 3, # microCompact 保留最近 N 条 tool_result
        auto_compact_max_retries: int = 3,
    ):
        self.llm = llm
        self.max_tokens = max_tokens
        self.tool_result_size_threshold = tool_result_size_threshold
        self.tool_preview_chars = tool_preview_chars
        self.max_message_count = max_message_count
        self.recent_tool_results_keep = recent_tool_results_keep
        self.auto_compact_max_retries = auto_compact_max_retries
        
    async def abefore_model(
        self,
        state: AgentState,
        runtime: Runtime,
    ) -> dict[str, any] | None:
        # 检查是否是内部 LLM 调用（避免递归）
        if _internal_call.get():
            return None
            
        messages = state.get("messages", [])
        if not messages:
            return None
        
        # 1. ToolResult Budget – 超大工具结果落盘预览
        messages = await self._tool_result_budget(messages)
        
        # 2. SnipCompact – 消息数量修剪
        messages = self._snip_compact(messages)
        
        # 3. MicroCompact – 旧 tool_result 占位符替换
        messages = self._micro_compact(messages)
        
        # 4. AutoCompact – 主动 LLM 摘要
        if self._estimate_tokens(messages) > self.max_tokens:
            messages = await self._auto_compact(messages)
        
        # 5. ReactiveCompact – 最后应急兜底
        if self._estimate_tokens(messages) > self.max_tokens:
            messages = await self.reactive_compact(messages)
        
        return {"messages": messages}
    
    # ---------- 各步骤实现 ----------
    async def _tool_result_budget(self, messages: List[BaseMessage]) -> List[BaseMessage]:
        """将超大 ToolMessage 的内容落盘，替换为 preview + 文件名"""
        new_messages = []
        for msg in messages:
            if isinstance(msg, ToolMessage) and msg.content:
                content = msg.content
                if len(content) > self.tool_result_size_threshold:
                    # 落盘：保存完整内容到文件（可存储到 store 或本地文件系统）
                    filename = await self._save_to_disk(content)
                    preview = content[:self.tool_preview_chars]
                    msg.content = f"[Tool result too large] File: {filename}\nPreview:\n{preview}"
            new_messages.append(msg)
        return new_messages
    
    def _snip_compact(self, messages: List[BaseMessage]) -> List[BaseMessage]:
        """消息数 > max_message_count 时保留头尾各一半"""
        if len(messages) <= self.max_message_count:
            return messages
        half = self.max_message_count // 2
        head = messages[:half]
        tail = messages[-half:]
        # 添加一个提示消息，告知裁剪
        snipped_note = HumanMessage(
            content=f"[Context trimmed: removed {len(messages) - self.max_message_count} messages in the middle]"
        )
        return head + [snipped_note] + tail
    
    def _micro_compact(self, messages: List[BaseMessage]) -> List[BaseMessage]:
        """旧的 tool_result 替换为占位符，仅保留最近 self.recent_tool_results_keep 条"""
        tool_msg_indices = [i for i, m in enumerate(messages) if isinstance(m, ToolMessage)]
        if len(tool_msg_indices) <= self.recent_tool_results_keep:
            return messages
        
        # 保留最近的 N 条完整 tool_result
        keep_indices = set(tool_msg_indices[-self.recent_tool_results_keep:])
        new_messages = []
        for i, msg in enumerate(messages):
            if isinstance(msg, ToolMessage) and i not in keep_indices:
                # 替换为占位符
                new_messages.append(HumanMessage(
                    content=f"[Previous tool result omitted: {msg.name}, re-run if needed]"
                ))
            else:
                new_messages.append(msg)
        return new_messages
    
    async def _auto_compact(self, messages: List[BaseMessage]) -> List[BaseMessage]:
        """尝试使用 LLM 生成摘要替换整个上下文，最多重试 auto_compact_max_retries 次"""
        for attempt in range(self.auto_compact_max_retries):
            try:
                # Set internal call flag to avoid triggering middleware
                token = _internal_call.set(True)
                try:
                    summary = await self._summarize_messages(messages)
                finally:
                    _internal_call.reset(token)
                # 替换为摘要消息 + 保留最近几条关键消息
                summary_msg = HumanMessage(content=f"[Auto Compacted] Summary: {summary}")
                # 保留最后 10 条原始消息（避免丢失最新信息）
                kept_recent = messages[-10:] if len(messages) > 10 else messages
                compacted = [summary_msg] + kept_recent
                # 检查摘要后是否低于阈值
                if self._estimate_tokens(compacted) <= self.max_tokens:
                    return compacted
            except Exception as e:
                # 重试失败则继续尝试，最后使用 reactive_compact
                continue
        # 重试用尽仍未达标，返回原消息（交给 reactive）
        return messages
    
    async def _summarize_messages(self, messages: List[BaseMessage]) -> str:
        """调用大模型生成摘要"""
        conv_text = "\n".join([f"{m.__class__.__name__}: {m.content[:500]}" for m in messages])
        prompt = ("Summarize this coding-agent conversation so work can continue.\n"
              "Preserve: 1. current goal, 2. key findings/decisions, 3. files read/changed, "
              "4. remaining work, 5. user constraints.\nBe compact but concrete, no more than 1000 tokens.\n\n" + conv_text)
        # Set internal call flag
        token = _internal_call.set(True)
        try:
            response = await self.llm.with_config(
                tags=["internal_memory_call"],
                metadata={"internal": True}
            ).ainvoke(prompt)
        finally:
            _internal_call.reset(token)
        return response.content if hasattr(response, 'content') else str(response)
    
    async def reactive_compact(self, messages: List[BaseMessage]) -> List[BaseMessage]:
        """应急兜底：固定保留最后 5 条消息 + 一个摘要（如果有）"""
        # 尝试生成一个快速摘要
        token = _internal_call.set(True)
        try:
            summary=await self._summarize_messages(messages) if messages else "No conversation history."
        finally:
            _internal_call.reset(token)
        tail_start = max(0, len(messages) - 5)
        if tail_start > 0 and isinstance(messages[tail_start], ToolMessage) and (isinstance(messages[tail_start - 1], AIMessage) and bool(getattr(messages[tail_start - 1],'tool_calls', []))):
            tail_start -= 1
        return [HumanMessage(content=f"[Reactive Compact] Summary: {summary}")] + messages[tail_start:]
    
    # ---------- 辅助方法 ----------
    def _estimate_tokens(self, messages: List[BaseMessage]) -> int:
        """估算消息列表的 token 数量（可使用 tiktoken 或模型特定计数器）"""
        encoding = tiktoken.get_encoding("cl100k_base")
        total = 0
        for msg in messages:
            total += len(encoding.encode(msg.content or ""))
            # 加上角色等元数据的开销
            total += 10
        return total
    
    async def _save_to_disk(self, content: str) -> str:
        """落盘工具结果，返回文件名。实际可存入文件系统或 BaseStore"""
        filename = f"tool_result_{hashlib.md5(content.encode()).hexdigest()[:8]}_{int(time.time())}.txt"
        async with aiofiles.open(f"/tmp/langchain_mem/{filename}", "w") as f:
            await f.write(content)
        return filename
    
    # def write_chat_history(
    #     self,
    #     messages: List[BaseMessage],
    #     session_id: str,
    #     metadata: Optional[dict] = None
    # ) -> None:
    #     """追加对话历史到 BaseStore（PostgreSQL），以 JSONL 形式存储
        
    #     Args:
    #         messages: 对话消息列表
    #         session_id: 会话 ID，用于标识对话会话
    #         metadata: 可选的元数据（如用户 ID、时间戳等）
    #     """
    #     if not self.store:
    #         return
        
    #     jsonl_records = []
    #     for msg in messages:
    #         record = {
    #             "session_id": session_id,
    #             "timestamp": time.time(),
    #             "role": msg.__class__.__name__,
    #             "content": msg.content,
    #             "metadata": metadata or {}
    #         }
            
    #         if isinstance(msg, AIMessage):
    #             if hasattr(msg, 'tool_calls') and msg.tool_calls:
    #                 record["tool_calls"] = msg.tool_calls
    #         elif isinstance(msg, ToolMessage):
    #             record["tool_call_id"] = getattr(msg, 'tool_call_id', None)
    #             record["name"] = getattr(msg, 'name', None)
            
    #         jsonl_records.append(json.dumps(record, ensure_ascii=False, default=str))
        
    #     jsonl_content = "\n".join(jsonl_records) + "\n"
    #     storage_key = f"chat_history:{session_id}:{int(time.time())}"
    #     self.store.mset([(storage_key, jsonl_content)])