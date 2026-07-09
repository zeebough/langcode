"""
DAG Cycle Detection and Validation
使用 DFS 检测环，并验证 DAG 的完整性
"""

from typing import Dict, List, Set, Tuple, Any
from collections import defaultdict


def detect_cycles(tasks: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    检测任务依赖图中是否存在环
    
    Args:
        tasks: 任务列表，每个任务包含 'id' 和 'blockedBy' 字段
        
    Returns:
        (has_cycle, cycle_path): 
            - has_cycle: 是否存在环
            - cycle_path: 如果存在环，返回构成环的路径；否则为空列表
    """
    task_ids = {task["id"] for task in tasks}
    
    adjacency = defaultdict(list)
    for task in tasks:
        task_id = task["id"]
        for blocker in task.get("blockedBy", []):
            if blocker in task_ids:
                adjacency[blocker].append(task_id)
    
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {task_id: WHITE for task_id in task_ids}
    parent = {task_id: None for task_id in task_ids}
    
    def dfs(node: str, path: List[str]) -> Tuple[bool, List[str]]:
        color[node] = GRAY
        
        for neighbor in adjacency[node]:
            if neighbor not in color:
                continue
            
            if color[neighbor] == GRAY:
                cycle_start = path.index(neighbor) if neighbor in path else 0
                cycle_path = path[cycle_start:] + [neighbor]
                return True, cycle_path
            
            if color[neighbor] == WHITE:
                parent[neighbor] = node
                has_cycle, cycle_path = dfs(neighbor, path + [neighbor])
                if has_cycle:
                    return True, cycle_path
        
        color[node] = BLACK
        return False, []
    
    for task_id in task_ids:
        if color[task_id] == WHITE:
            has_cycle, cycle_path = dfs(task_id, [task_id])
            if has_cycle:
                return True, cycle_path
    
    return False, []


def validate_references(tasks: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    验证所有 blockedBy 引用是否存在
    
    Args:
        tasks: 任务列表
        
    Returns:
        (is_valid, missing_refs):
            - is_valid: 引用是否全部有效
            - missing_refs: 缺失的引用列表
    """
    task_ids = {task["id"] for task in tasks}
    missing_refs = []
    
    for task in tasks:
        for blocker_id in task.get("blockedBy", []):
            if blocker_id not in task_ids:
                missing_refs.append(f"Task '{task['id']}' references non-existent blocker '{blocker_id}'")
    
    return len(missing_refs) == 0, missing_refs


def validate_has_root(tasks: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    验证是否至少有一个根节点（blockedBy 为空）
    
    Args:
        tasks: 任务列表
        
    Returns:
        (has_root, errors):
            - has_root: 是否存在根节点
            - errors: 错误信息列表
    """
    root_tasks = [task for task in tasks if len(task.get("blockedBy", [])) == 0]
    
    if not root_tasks:
        return False, ["No root task found (all tasks have dependencies). DAG is deadlocked."]
    
    return True, []


def validate_dag(tasks: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    全面验证 DAG 的有效性
    
    Args:
        tasks: 任务列表
        
    Returns:
        (is_valid, errors):
            - is_valid: DAG 是否有效
            - errors: 错误信息列表
    """
    errors = []
    
    if not tasks:
        return False, ["Task list is empty"]
    
    task_ids = [task["id"] for task in tasks]
    if len(task_ids) != len(set(task_ids)):
        duplicates = [tid for tid in task_ids if task_ids.count(tid) > 1]
        errors.append(f"Duplicate task IDs found: {set(duplicates)}")
    
    ref_valid, ref_errors = validate_references(tasks)
    if not ref_valid:
        errors.extend(ref_errors)
    
    has_root, root_errors = validate_has_root(tasks)
    if not has_root:
        errors.extend(root_errors)
    
    has_cycle, cycle_path = detect_cycles(tasks)
    if has_cycle:
        errors.append(f"Cycle detected in DAG: {' -> '.join(cycle_path)}")
    
    return len(errors) == 0, errors
