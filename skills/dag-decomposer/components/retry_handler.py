"""
DAG Decomposer Retry Handler
实现指数退避 + 抖动的重试机制
"""

import asyncio
import random
import logging
import json
from typing import Any, Dict, Optional, Callable, Awaitable

logger = logging.getLogger(__name__)


class DecompositionError(Exception):
    """DAG 分解错误基类"""
    pass


class JSONParseError(DecompositionError):
    """JSON 解析错误"""
    pass


class SchemaValidationError(DecompositionError):
    """Schema 验证错误"""
    pass


class CycleDetectionError(DecompositionError):
    """检环错误"""
    pass


class RetryHandler:
    """
    处理 DAG 分解的重试逻辑
    
    支持以下错误类型：
    - JSONParseError: JSON 解析失败
    - SchemaValidationError: Schema 验证失败
    - CycleDetectionError: 检测到环或 DAG 验证失败
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        backoff_factor: float = 2.0,
        jitter_factor: float = 0.5,
    ):
        """
        Args:
            max_retries: 最大重试次数
            base_delay: 基础延迟（秒）
            max_delay: 最大延迟（秒）
            backoff_factor: 退避因子
            jitter_factor: 抖动因子（0-1）
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter_factor = jitter_factor
    
    def _calculate_delay(self, attempt: int) -> float:
        """
        计算当前尝试的延迟（含抖动）
        
        Args:
            attempt: 当前尝试次数（从 0 开始）
            
        Returns:
            延迟秒数
        """
        delay = min(self.base_delay * (self.backoff_factor ** attempt), self.max_delay)
        jitter = random.uniform(0, self.jitter_factor * delay)
        return delay + jitter
    
    def _extract_json(self, response: str) -> str:
        """
        从响应中提取 JSON（处理可能的 Markdown 包装）
        
        Args:
            response: LLM 的原始响应
            
        Returns:
            纯 JSON 字符串
        """
        response = response.strip()
        
        if response.startswith("```json"):
            response = response[7:]
        elif response.startswith("```"):
            response = response[3:]
        
        if response.endswith("```"):
            response = response[:-3]
        
        return response.strip()
    
    async def execute_with_retry(
        self,
        llm_call: Callable[[], Awaitable[str]],
        validator: Optional[Callable[[Dict[str, Any]], Awaitable[bool]]] = None,
    ) -> Dict[str, Any]:
        """
        执行 LLM 调用并重试直到成功或达到最大重试次数
        
        Args:
            llm_call: 异步 LLM 调用函数
            validator: 可选的验证函数，接收解析后的 JSON 并返回是否有效
            
        Returns:
            解析后的 DAG JSON 对象
            
        Raises:
            JSONParseError: JSON 解析失败
            SchemaValidationError: Schema 验证失败
            CycleDetectionError: 检环失败
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                response = await llm_call()
                
                try:
                    json_str = self._extract_json(response)
                    dag_data = json.loads(json_str)
                except json.JSONDecodeError as e:
                    raise JSONParseError(f"Failed to parse JSON response: {e}")
                
                if "plan_summary" not in dag_data:
                    raise SchemaValidationError("Missing required field: plan_summary")
                
                if "tasks" not in dag_data:
                    raise SchemaValidationError("Missing required field: tasks")
                
                if not isinstance(dag_data["tasks"], list):
                    raise SchemaValidationError("'tasks' must be an array")
                
                for i, task in enumerate(dag_data["tasks"]):
                    required_fields = ["id", "subject", "description", "blockedBy"]
                    for field in required_fields:
                        if field not in task:
                            raise SchemaValidationError(f"Task {i} missing required field: {field}")
                    
                    if not isinstance(task["blockedBy"], list):
                        raise SchemaValidationError(f"Task '{task['id']}': 'blockedBy' must be an array")
                
                if validator:
                    is_valid = await validator(dag_data)
                    if not is_valid:
                        raise SchemaValidationError("Custom validation failed")
                
                if attempt > 0:
                    logger.info(f"DAG decomposition succeeded after {attempt + 1} attempts")
                
                return dag_data
                
            except (JSONParseError, SchemaValidationError, CycleDetectionError) as e:
                last_error = e
                logger.warning(f"DAG decomposition attempt {attempt + 1}/{self.max_retries} failed: {e}")
                
                if attempt < self.max_retries - 1:
                    delay = self._calculate_delay(attempt)
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
        
        raise last_error
