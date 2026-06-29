import asyncio
import logging
import random
from typing import Any, List, Optional

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage, BaseMessage
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI
import os
from middlewares.context_vars import _internal_call

logger = logging.getLogger(__name__)

# 自定义异常类型，便于分类处理
class ContextLengthExceededError(Exception):
    """上下文超限异常"""
    pass

class ModelTemporarilyUnavailableError(Exception):
    """模型临时不可用（529等）"""
    pass

class TruncationError(Exception):
    """输出截断异常"""
    def __init__(self, message: str, partial_response: str):
        super().__init__(message)
        self.partial_response = partial_response


class ErrorRecoveryMiddleware(AgentMiddleware):
    """
    错误恢复中间件，处理三类错误：
    1. 输出截断 (finish_reason = "length") → 续写
    2. 上下文超限 (prompt_too_long) → 压缩后重试
    3. 临时故障 (429/529) → 指数退避+抖动，连续529切换备用模型
    """
    
    def __init__(
        self,
        primary_llm: BaseChatModel,
        fallback_llm: BaseChatModel,
        context_compressor: Optional[Any] = None,  # 可传入ContextCompressionMiddleware实例
        max_retries: int = 3,
        max_continuation_attempts: int = 2,
        max_tokens_for_continuation: int = 64000,
        consecutive_529_threshold: int = 3,
    ):
        self.primary_llm = primary_llm
        self.fallback_llm = fallback_llm
        self.context_compressor = context_compressor
        self.max_retries = max_retries
        self.max_continuation_attempts = max_continuation_attempts
        self.max_tokens_for_continuation = max_tokens_for_continuation
        self.consecutive_529_threshold = consecutive_529_threshold
        
        # 状态追踪（每个请求独立）
        self._consecutive_529_count = 0
        self._continuation_attempts = 0
        
        self.logger = logging.getLogger(__name__)
    
    def _is_truncation(self, response: AIMessage) -> bool:
        """检查是否因达到token上限而截断"""
        finish_reason = response.response_metadata.get("finish_reason")
        return finish_reason == "length"
    
    def _is_context_exceeded(self, error: Exception) -> bool:
        """检查是否为上下文超限错误"""
        error_str = str(error).lower()
        return (
            "context_length_exceeded" in error_str or
            "prompt_too_long" in error_str or
            "maximum context length" in error_str or
            "too many tokens" in error_str
        )
    
    def _is_temporary_failure(self, error: Exception) -> bool:
        """检查是否为临时故障（429/529）"""
        error_str = str(error).lower()
        return (
            "429" in error_str or
            "rate_limit" in error_str or
            "529" in error_str or
            "service_unavailable" in error_str
        )
    
    def _is_529_error(self, error: Exception) -> bool:
        """检查是否为529服务不可用"""
        error_str = str(error).lower()
        return "529" in error_str or "service_unavailable" in error_str
    
    async def _call_with_retry(
        self,
        llm: BaseChatModel,
        messages: List[BaseMessage],
        **kwargs,
    ) -> AIMessage:
        """
        带指数退避 + 抖动的重试调用
        """
        logger.info("ErrorRecoveryMiddleware._call_with_retry called")
        base_delay = 1.0
        max_delay = 60.0
        backoff_factor = 2.0
        
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                token = _internal_call.set(True)
                try:
                    return await llm.ainvoke(messages, **kwargs)
                finally:
                    _internal_call.reset(token)
            except Exception as e:
                last_error = e
                
                if not self._is_temporary_failure(e):
                    # 非临时故障直接抛出
                    raise
                
                # 检查是否为 529
                if self._is_529_error(e):
                    self._consecutive_529_count += 1
                    logger.warning(
                        f"ErrorRecoveryMiddleware: 529 错误 (第{attempt+1}次尝试)，连续次数：{self._consecutive_529_count}"
                    )
                    
                    # 连续 529 达到阈值，切换到备用模型
                    if self._consecutive_529_count >= self.consecutive_529_threshold:
                        logger.info("ErrorRecoveryMiddleware: 连续 529 错误，切换到备用模型")
                        try:
                            token = _internal_call.set(True)
                            try:
                                return await self.fallback_llm.ainvoke(messages, **kwargs)
                            finally:
                                _internal_call.reset(token)
                        except Exception as fallback_error:
                            logger.error(f"ErrorRecoveryMiddleware: 备用模型也失败：{fallback_error}")
                            raise fallback_error
                else:
                    # 非 529 的临时故障（如 429），重置 529 计数
                    self._consecutive_529_count = 0
                
                # 计算退避延迟（含抖动）
                delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                jitter = random.uniform(0, 0.5 * delay)
                total_delay = delay + jitter
                
                logger.info(f"ErrorRecoveryMiddleware: 临时故障，{total_delay:.2f}秒后重试 (尝试 {attempt+1}/{self.max_retries})")
                await asyncio.sleep(total_delay)
        
        # 所有重试都失败
        raise last_error
    
    async def _handle_truncation(
        self,
        request: Any,
        partial_response: str,
        **kwargs,
    ) -> AIMessage:
        """
        处理输出截断：发起续写
        """
        logger.info("ErrorRecoveryMiddleware._handle_truncation called")
        self._continuation_attempts += 1
        
        if self._continuation_attempts > self.max_continuation_attempts:
            logger.warning(f"ErrorRecoveryMiddleware: 续写尝试已达上限 ({self.max_continuation_attempts})")
            # 返回部分内容，让上层知晓
            return AIMessage(
                content=partial_response + "\n\n[⚠️ 回复被截断，已达到最大续写尝试次数]",
                response_metadata={"finish_reason": "length", "truncation_handled": True}
            )
        
        logger.info(f"ErrorRecoveryMiddleware: 检测到截断，发起第 {self._continuation_attempts} 次续写")
        
        # 构造续写提示
        continuation_prompt = HumanMessage(
            content="请继续完成上面的回答，直接从断点处继续，不要重复已输出的内容。"
        )
        
        # 保留 system_message 和原始消息
        continuation_messages = []
        if request.system_message:
            continuation_messages.append(request.system_message)
        continuation_messages.extend(request.messages)
        continuation_messages.extend([
            AIMessage(content=partial_response),
            continuation_prompt,
        ])
        
        # 创建临时 LLM 实例，使用更大的 token 限制进行续写
        continuation_llm = ChatOpenAI(
            model=os.getenv("MODEL_NAME"),
            api_key=os.getenv("API_KEY"),
            base_url=os.getenv("BASE_URL"),
            max_completion_tokens=self.max_tokens_for_continuation,
            temperature=0.3,
        )
        
        try:
            token = _internal_call.set(True)
            try:
                response = await continuation_llm.ainvoke(
                    continuation_messages,
                    **kwargs,
                )
            finally:
                _internal_call.reset(token)
            
            # 合并续写内容
            full_content = partial_response + response.content
            # 保留原始 response_metadata，但标记已处理
            response_metadata = response.response_metadata.copy()
            response_metadata["truncation_handled"] = True
            response_metadata["continuation_attempts"] = self._continuation_attempts
            # 续写成功后，将 finish_reason 改为 stop，表示完整回答
            response_metadata["finish_reason"] = "stop"
            
            return AIMessage(
                content=full_content,
                response_metadata=response_metadata,
            )
        except Exception as e:
            logger.error(f"ErrorRecoveryMiddleware: 续写失败：{e}")
            # 续写失败时返回部分内容
            return AIMessage(
                content=partial_response + "\n\n[⚠️ 续写失败，回复可能不完整]",
                response_metadata={"finish_reason": "length", "truncation_handled": False}
            )
    
    async def _handle_context_exceeded(
        self,
        messages: List[BaseMessage],
        **kwargs,
    ) -> AIMessage:
        """
        处理上下文超限：压缩后重试
        """
        self.logger.info("检测到上下文超限，尝试压缩")
        
        if self.context_compressor is None:
            self.logger.warning("未配置上下文压缩器，无法处理上下文超限")
            raise ContextLengthExceededError("上下文超限且未配置压缩器")
        
        # 调用上下文压缩
        try:
            token = _internal_call.set(True)
            try:
                compressed_messages = await self.context_compressor.reactive_compact(messages)
            finally:
                _internal_call.reset(token)
            self.logger.info(f"压缩完成，消息数：{len(messages)} → {len(compressed_messages)}")
            
            # 用压缩后的消息重试
            return await self._call_with_retry(
                self.primary_llm,
                compressed_messages,
                **kwargs,
            )
        except Exception as e:
            self.logger.error(f"压缩或重试失败: {e}")
            raise ContextLengthExceededError(f"上下文超限处理失败: {e}")
    
    async def awrap_model_call(self, request, handler):
        """
        中间件核心方法：拦截模型调用，实现错误恢复
        """
        # 检查是否是内部 LLM 调用（避免递归）
        if _internal_call.get():
            logger.info("ErrorRecoveryMiddleware.awrap_model_call: internal call, passing through")
            return await handler(request)
        
        logger.info("ErrorRecoveryMiddleware.awrap_model_call called")
        # 重置请求级别的状态
        self._consecutive_529_count = 0
        self._continuation_attempts = 0
        
        try:
            # 首次尝试
            response = await handler(request)
            
            # 检查是否截断（检查最后一个 AI 消息）
            from langchain_core.messages import AIMessage
            last_message = response.result[-1] if hasattr(response, 'result') and response.result else None
            if isinstance(last_message, AIMessage) and self._is_truncation(last_message):
                logger.info("ErrorRecoveryMiddleware: 检测到输出截断，触发续写")
                return await self._handle_truncation(
                    request,
                    last_message.content,
                )
            
            return response
            
        except Exception as e:
            # 分类处理各类错误
            
            if self._is_context_exceeded(e):
                # 上下文超限 → 压缩后重试
                try:
                    return await self._handle_context_exceeded(request.messages)
                except Exception as recovery_error:
                    logger.error(f"ErrorRecoveryMiddleware: 上下文超限恢复失败：{recovery_error}")
                    raise recovery_error
            
            elif self._is_temporary_failure(e):
                # 临时故障 → 已经由_call_with_retry 处理
                # 但如果是未被重试捕获的异常，在这里兜底
                logger.warning(f"ErrorRecoveryMiddleware: 临时故障未被重试捕获：{e}")
                raise e
            
            else:
                # 其他异常直接抛出
                raise e