import datetime
import contextvars
from typing import List, Dict
from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.language_models import BaseChatModel
from langgraph.store.base import BaseStore
from langchain.agents.middleware import AgentMiddleware, ModelRequest, Runtime
from langchain.agents.middleware.types import AgentState

from middlewares.memory_saver import MemorySaver

# Context var to track internal LLM calls (avoid recursion)
_internal_call = contextvars.ContextVar("_internal_call", default=False)


class MemoryManagementMiddleware(AgentMiddleware):
    """
    管理长期记忆的召回与注入。
    使用 LLM 从索引（description 列表）中选择最相关记忆，而非向量检索。
    """
    def __init__(self, llm: BaseChatModel, store: BaseStore, user_id: str = "user_id"):
        self.llm = llm
        self.store = store
        self.memory_saver = MemorySaver(llm, store, user_id)  # 复用 MemorySaver 的提取和保存逻辑
        self.user_id_key = user_id

    def _get_user_namespace(self, state: AgentState) -> tuple:
        return (self.user_id_key, "memories")

    async def _fetch_index(self, namespace: tuple, limit: int = 200) -> List[Dict[str, str]]:
        """从 store 中获取所有记忆的 description 和 key（最多 limit 条）"""
        items = await self.store.asearch(namespace, limit=limit)
        index = []
        for item in items:
            if item.value and "description" in item.value:
                index.append({
                    "key": item.key,
                    "description": item.value["description"],
                    "type": item.value.get("type", "contextual")
                })
        return index

    async def _select_relevant_keys(self, task: str, recent_context: str, index: List[Dict]) -> List[str]:
        """调用 LLM 选择最相关的记忆 key（最多5个）"""
        if not index:
            return []

        # 构建索引文本（限制每个描述的长度，防止过长）
        index_text = "\n".join([
            f"- {item['description']} (key: {item['key']})"
            for item in index
        ])

        prompt = f"""你是一个记忆检索助手。根据最近的对话，从以下记忆列表中选择最多5个最相关的记忆。

用户最新提问：
{task[:500]}

最近的对话：
{recent_context[:2000]}

记忆列表（每项包含描述和对应的 key）：
{index_text}

请只返回选中的 key 列表，用英文逗号分隔，不要包含其他内容。
例如：key1, key3, key7
"""
        response = await self.llm.with_config(
            callbacks=[],
            tags=["internal_memory_call"],
            metadata={"internal": True}
        ).ainvoke(prompt)
        # 解析响应
        raw_keys = [k.strip() for k in response.content.split(',') if k.strip()]
        # 去重并限制 ≤5
        unique_keys = list(dict.fromkeys(raw_keys))
        return unique_keys[:5]

    async def _load_memories(self, namespace: tuple, keys: List[str]) -> str:
        """根据 key 从 store 加载完整内容，并拼接成文本"""
        if not keys:
            return ""
        parts = []
        for key in keys:
            doc = await self.store.aget(namespace, key)
            if doc:
                mem_type = doc.value.get("type", "contextual")
                content = doc.value.get("content", "")
                parts.append(f"[{mem_type.upper()}] {content}")
        return "\n".join(parts)

    async def modify_model_request(
        self,
        request: ModelRequest,
        state: AgentState,
        runtime: Runtime,
    ) -> ModelRequest:
        # 检查是否是内部 LLM 调用（避免递归）
        if _internal_call.get():
            return request
        
        # 1. 检查最近一条用户消息是否包含禁用关键词
        messages = state.get("messages", [])
        if messages:
            last_msg = messages[-1]
            if last_msg.type == "human":
                content = last_msg.content.lower()
                if any(kw in content for kw in ["不要使用记忆", "禁用记忆", "忘记所有", "停止记忆"]):
                    return request

        # 2. 准备召回上下文
        task = ""
        if messages and messages[-1].type == "human":
            task = messages[-1].content
        recent_msgs = [m for m in messages[-6:] if m.type != "system"][-5:]
        recent_context = "\n".join([f"{m.type}: {m.content}" for m in recent_msgs])

        # 3. 获取用户命名空间
        namespace = self._get_user_namespace(state)

        # 4. 获取记忆索引
        index = await self._fetch_index(namespace, limit=200)
        if not index:
            return request

        # 5. LLM 选择相关记忆 key
        relevant_keys = await self._select_relevant_keys(task, recent_context, index)
        if not relevant_keys:
            return request

        # 6. 加载完整内容
        memory_text = await self._load_memories(namespace, relevant_keys)
        if not memory_text:
            return request

        # 7. 构建动态 system prompt 块
        time_block = f"Current time: {datetime.datetime.now().isoformat(timespec='seconds')}"
        memory_block = f"\n\nAvailable memories:\n{memory_text}"
        dynamic_content = time_block + memory_block

        # 8. 追加到 system message 的 content_blocks
        system_message = request.system_message
        if system_message is None:
            # 如果没有 system message，创建一个
            request = request.override(
                system_message=SystemMessage(content=dynamic_content)
            )
        else:
            # 获取现有 content_blocks 并追加新内容
            existing_blocks = list(system_message.content_blocks)
            # 添加新的 text block
            existing_blocks.append({"type": "text", "text": dynamic_content})
            request = request.override(
                system_message=SystemMessage(content_blocks=existing_blocks)
            )

        return request
    
    async def aafter_model(
        self,
        state: AgentState,
        runtime: Runtime,
    ) -> dict[str, any] | None:
        last_message=state["messages"][-1] if state["messages"] else None
        # 只有当模型回复且未调用工具时才进行记忆提取和保存，避免工具调用结果干扰记忆内容
        if not isinstance(last_message, AIMessage):
            return None
        if bool(getattr(last_message, 'tool_calls', [])):
            return None
        await self.memory_saver.extract_and_save(state["messages"])
        return None