from typing import List, Dict
from langchain_core.language_models import BaseChatModel
from langgraph.store.base import BaseStore
from langchain_core.messages import BaseMessage
import datetime
import json
import hashlib
import contextvars

# Context var to track internal LLM calls (avoid recursion)
_internal_call = contextvars.ContextVar("_internal_call", default=False)

class MemorySaver:
    def __init__(self, llm: BaseChatModel, store: BaseStore, user_id: str = "default_user"):
        self.llm = llm
        self.store = store
        self.user_id_key = user_id

    def _get_namespace(self) -> tuple:
        return (self.user_id_key, "memories")

    async def extract_and_save(self, messages: List[BaseMessage]) -> None:
        """
        从对话中提取三类记忆并保存到 store。
        """
        # 1. 选择最近相关对话（可限制轮数或 token 数）
        recent_msgs = messages[-10:]   # 取最近10条

        # 2. 调用 LLM 提取记忆
        extracted = await self._extract_memories(recent_msgs)

        # 3. 对每类记忆进行去重/更新并保存
        namespace = self._get_namespace()
        for mem_type, items in extracted.items():
            for item in items:
                await self._save_memory(namespace, mem_type, item)

    async def _extract_memories(self, messages: List[BaseMessage]) -> Dict[str, List[Dict]]:
        """使用 LLM 从对话中提取三类记忆，返回结构化数据。"""
        # 构建对话文本
        conversation = "\n".join([f"{m.type}: {m.content}" for m in messages])

        prompt = f"""
你是一个记忆提取助手。请分析以下对话，从中提取三类长期记忆，用于辅助未来的代码编写任务。

对话内容：
{conversation}

三类记忆定义：
1. **Semantic**（用户偏好）：用户个人的习惯、喜好、背景信息（例如编程语言偏好、代码风格、沟通方式等）。
2. **Procedural**（行为准则）：用户明示或暗示的工作流程、规则、规范（例如"提交前必须测试"、"不要使用第三方库"等）。
3. **Episodic**（过往经验）：过去发生的具体事件、问题解决经验、项目背景（例如"上次部署时遇到端口冲突"、"之前用过这个库处理 JSON"等）。

请按照如下 JSON 格式输出提取结果，若无则返回空列表：
{{
  "semantic": [{{"content": "...", "description": "..."}}],
  "procedural": [{{"content": "...", "description": "..."}}],
  "episodic": [{{"content": "...", "description": "..."}}]
}}

要求：
- 每条记忆的 "content" 为完整描述（可保留细节），"description" 为 10-20 字的简短摘要（用于索引）。
- 只提取新的、可能对后续对话有帮助的信息，避免重复已有记忆（假设 store 当前为空）。
- 如果某类无内容，返回空列表。
"""
        # Set internal call flag to avoid triggering middleware
        token = _internal_call.set(True)
        try:
            response = await self.llm.with_config(
                tags=["internal_memory_call"],
                metadata={"internal": True}
            ).ainvoke(prompt)
        finally:
            _internal_call.reset(token)
        # 解析 JSON（这里应使用 json.loads 并处理可能的 markdown 标记）
        # 假设 response.content 是纯 JSON 或包含在 ```json ... ``` 中
        content = response.content.strip() if response.content else ""
        if not content:
            # 空响应，返回空字典
            return {"semantic": [], "procedural": [], "episodic": []}
        if content.startswith("```json"):
            content = content[7:-3]
        elif content.startswith("```"):
            content = content[3:-3]
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            # JSON 解析失败，返回空字典
            print(f"Warning: Failed to parse memory JSON: {content[:200]}")
            return {"semantic": [], "procedural": [], "episodic": []}
        return data

    async def _save_memory(self, namespace: tuple, mem_type: str, item: Dict):
        """保存单条记忆，进行去重判断。"""
        # 生成唯一 key（可用内容哈希或时间戳+摘要）
        key = hashlib.md5(item["description"].encode()).hexdigest()[:12]

        # 检查是否已存在类似记忆（可用 LLM 判断或简单比较描述相似度，此处简化）
        existing = await self.store.aget(namespace, key)
        if existing:
            # 如果已存在，可选择更新 content（或忽略）
            # 此处简单实现：若存在则跳过，可拓展为 LLM 判断是否合并
            print(f"记忆已存在: {item['description']}")
            return

        # 存储完整数据
        await self.store.aput(
            namespace,
            key,
            {
                "type": mem_type,
                "content": item["content"],
                "description": item["description"],
                "timestamp": datetime.datetime.now().isoformat(),
            }
        )