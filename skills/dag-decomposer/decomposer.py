"""
DAG Decomposer - 完整集成示例
展示如何使用 DAG 分解组件将复杂任务分解并存入数据库
"""

import os
import logging
from typing import Dict, Any, Optional

from langchain_openai import ChatOpenAI

from .components.prompt import build_decomposer_prompt
from .components.parser import parse_dag_response
from .components.cycle_detector import validate_dag
from .components.retry_handler import RetryHandler
from lib.dag_scheduler import insert_dag_to_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DAGDecomposer:
    """
    DAG 分解器主类
    
    将复杂用户请求分解为可执行的 DAG 任务图，并存入 PostgreSQL 数据库
    """
    
    def __init__(
        self,
        llm: Optional[ChatOpenAI] = None,
        db_connection_params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ):
        """
        Args:
            llm: LLM 实例，默认使用环境变量配置的模型
            db_connection_params: 数据库连接参数
            max_retries: 最大重试次数
        """
        if llm is None:
            self.llm = ChatOpenAI(
                model=os.getenv("MODEL_NAME", "gpt-4o"),
                api_key=os.getenv("API_KEY"),
                base_url=os.getenv("BASE_URL"),
                temperature=0.3,
            )
        else:
            self.llm = llm
        
        self.db_params = db_connection_params or {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "5432")),
            "database": os.getenv("DB_NAME", "langcode"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD"),
        }
        
        self.retry_handler = RetryHandler(max_retries=max_retries)
    
    async def decompose(
        self,
        user_request: str,
        thread_id: str,
        owner: Optional[str] = None,
        save_to_db: bool = True,
    ) -> Dict[str, Any]:
        """
        分解用户请求为 DAG 任务
        
        Args:
            user_request: 用户原始请求
            thread_id: 会话线程 ID
            owner: 可选的 owner 标识
            save_to_db: 是否保存到数据库
            
        Returns:
            分解结果（包含 plan_summary, tasks, 和数据库插入统计）
            
        Raises:
            DecompositionError: 分解失败时抛出异常
        """
        prompt = build_decomposer_prompt(user_request)
        
        async def llm_call():
            response = await self.llm.ainvoke([{"role": "user", "content": prompt}])
            return response.content
        
        async def dag_validator(dag_data: Dict[str, Any]) -> bool:
            is_valid, errors = validate_dag(dag_data["tasks"])
            if not is_valid:
                logger.warning(f"DAG validation failed: {errors}")
                return False
            return True
        
        try:
            dag_data = await self.retry_handler.execute_with_retry(
                llm_call=llm_call,
                validator=dag_validator,
            )
            
            logger.info(f"DAG decomposition successful: {len(dag_data['tasks'])} tasks")
            
            result = {
                "success": True,
                "plan_summary": dag_data["plan_summary"],
                "tasks": dag_data["tasks"],
                "task_count": len(dag_data["tasks"]),
            }
            
            if save_to_db:
                try:
                    db_result = await insert_dag_to_db(
                        dag_data=dag_data,
                        connection_params=self.db_params,
                        thread_id=thread_id,
                        owner=owner,
                    )
                    result["db_result"] = db_result
                except Exception as db_error:
                    logger.error(f"Failed to save DAG to database: {db_error}")
                    result["success"] = False
                    result["error"] = f"Database error: {db_error}"
            
            return result
            
        except Exception as e:
            logger.error(f"DAG decomposition failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "task_count": 0,
            }
    
    async def decompose_and_validate(
        self,
        user_request: str,
        thread_id: str,
        owner: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        分解并验证 DAG（不保存到数据库）
        
        Args:
            user_request: 用户原始请求
            thread_id: 会话线程 ID
            owner: 可选的 owner 标识
            
        Returns:
            分解和验证结果
        """
        return await self.decompose(
            user_request=user_request,
            thread_id=thread_id,
            owner=owner,
            save_to_db=False,
        )


async def example_usage():
    """使用示例"""
    from langchain_openai import ChatOpenAI
    
    llm = ChatOpenAI(
        model="gpt-4o",
        api_key=os.getenv("API_KEY"),
        base_url=os.getenv("BASE_URL"),
        temperature=0.3,
    )
    
    db_params = {
        "host": "localhost",
        "port": 5432,
        "database": "langcode",
        "user": "postgres",
        "password": "your_password",
    }
    
    decomposer = DAGDecomposer(llm=llm, db_connection_params=db_params)
    
    user_request = """
    生成一份周报，需要包含以下内容：
    1. 从数据库查询上周的销售数据
    2. 爬取竞争对手的最新新闻
    3. 分析社交媒体上的用户情感
    4. 整合所有数据生成 Markdown 格式的周报
    """
    
    result = await decomposer.decompose(
        user_request=user_request,
        thread_id="weekly_report_20260708",
        owner="lead_agent",
    )
    
    if result["success"]:
        print(f"分解成功：{result['task_count']} 个任务")
        print(f"计划摘要：{result['plan_summary']}")
        for task in result["tasks"]:
            print(f"  - {task['id']}: {task['subject']}")
            if task.get("blockedBy"):
                print(f"    依赖：{task['blockedBy']}")
    else:
        print(f"分解失败：{result.get('error')}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(example_usage())
