"""
DAG Response Parser
解析 LLM 输出并提取 DAG 结构
"""

import json
import logging
from typing import Dict, List, Any, Tuple

logger = logging.getLogger(__name__)


def extract_json_from_response(response: str) -> str:
    """
    从响应中提取纯 JSON 字符串（移除 Markdown 包装）
    
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


def parse_dag_response(response: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    解析 LLM 的 DAG 分解响应
    
    Args:
        response: LLM 的原始响应字符串
        
    Returns:
        (dag_data, errors):
            - dag_data: 解析后的 DAG 数据（包含 plan_summary 和 tasks）
            - errors: 错误信息列表，如果成功则为空
    """
    errors = []
    
    try:
        json_str = extract_json_from_response(response)
        dag_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        return {}, [f"JSON 解析失败：{e}"]
    
    if not isinstance(dag_data, dict):
        return {}, ["响应必须是 JSON 对象"]
    
    if "plan_summary" not in dag_data:
        errors.append("缺少必需字段：plan_summary")
    
    if "tasks" not in dag_data:
        errors.append("缺少必需字段：tasks")
    
    if errors:
        return {}, errors
    
    if not isinstance(dag_data["tasks"], list):
        return {}, ["'tasks' 必须是数组"]
    
    task_ids = set()
    for i, task in enumerate(dag_data["tasks"]):
        if not isinstance(task, dict):
            errors.append(f"任务 {i} 必须是对象")
            continue
        
        required_fields = ["id", "subject", "description", "blockedBy"]
        for field in required_fields:
            if field not in task:
                errors.append(f"任务 {i} 缺少必需字段：{field}")
        
        if "id" in task:
            if task["id"] in task_ids:
                errors.append(f"重复的任务 ID: '{task['id']}'")
            task_ids.add(task["id"])
        
        if "blockedBy" in task and not isinstance(task["blockedBy"], list):
            errors.append(f"任务 '{task.get('id', i)}': 'blockedBy' 必须是数组")
    
    if errors:
        return {}, errors
    
    normalized_tasks = []
    for task in dag_data["tasks"]:
        normalized_task = {
            "id": task["id"],
            "subject": task["subject"],
            "description": task["description"],
            "blockedBy": task["blockedBy"],
        }
        
        if "metadata" in task and isinstance(task["metadata"], dict):
            normalized_task["metadata"] = task["metadata"]
        
        normalized_tasks.append(normalized_task)
    
    return {
        "plan_summary": dag_data["plan_summary"],
        "tasks": normalized_tasks,
    }, []
