#!/usr/bin/env python
"""
DAG Decomposer 快速演示

展示如何将复杂任务分解为 DAG 并验证
"""

import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from skills.dag_decomposer.components.prompt import build_decomposer_prompt
from skills.dag_decomposer.components.parser import parse_dag_response
from skills.dag_decomposer.components.cycle_detector import validate_dag, detect_cycles
from skills.dag_decomposer.components.retry_handler import RetryHandler


async def demo():
    print("\n" + "="*70)
    print("DAG Decomposer 演示")
    print("="*70 + "\n")
    
    user_request = """
    生成一份市场分析周报，需要包含以下内容：
    1. 从公司数据库查询上周的销售数据（表名：sales.weekly）
    2. 爬取 5 个主要竞争对手的最新新闻稿
    3. 分析社交媒体上关于我们产品的情感倾向
    4. 整合所有数据生成 Markdown 格式的周报，包含销售趋势、竞争格局和情感热力图
    """
    
    print("📝 用户请求:")
    print(user_request)
    print()
    
    print("🔧 构建提示词...")
    prompt = build_decomposer_prompt(user_request)
    print(f"   提示词长度：{len(prompt)} 字符")
    print()
    
    print("🧪 模拟 LLM 响应（示例输出）:")
    mock_llm_response = """
    ```json
    {
        "plan_summary": "并行获取三个数据源（销售数据、竞争对手新闻、社交情感），然后整合生成最终报告",
        "tasks": [
            {
                "id": "fetch_sales",
                "subject": "查询销售数据",
                "description": "从 sales.weekly 表查询上周所有销售记录，导出为 CSV 格式保存到 /data/sales.csv",
                "blockedBy": []
            },
            {
                "id": "scrape_competitors",
                "subject": "爬取竞争对手新闻",
                "description": "使用浏览器工具爬取 5 个竞争对手网站的新闻稿页面，保存 HTML 到 /data/competitors/",
                "blockedBy": [],
                "metadata": {
                    "required_capability": "web_scraper",
                    "estimated_tokens": 5000
                }
            },
            {
                "id": "analyze_sentiment",
                "subject": "分析社交情感",
                "description": "调用 NLP API 分析社交媒体上关于产品的评论，计算情感得分，结果保存到 /data/sentiment.json",
                "blockedBy": [],
                "metadata": {
                    "required_capability": "nlp_api"
                }
            },
            {
                "id": "generate_report",
                "subject": "生成周报",
                "description": "整合 sales.csv、competitors/ 和 sentiment.json，生成包含销售趋势、竞争格局、情感热力图的 Markdown 报告",
                "blockedBy": ["fetch_sales", "scrape_competitors", "analyze_sentiment"]
            }
        ]
    }
    ```
    """
    
    print(mock_llm_response)
    print()
    
    print("📥 解析响应...")
    dag_data, errors = parse_dag_response(mock_llm_response)
    
    if errors:
        print(f"   ❌ 解析错误：{errors}")
        return
    
    print(f"   ✅ 解析成功")
    print(f"   - 计划摘要：{dag_data['plan_summary'][:50]}...")
    print(f"   - 任务数量：{len(dag_data['tasks'])}")
    print()
    
    print("✅ 验证 DAG...")
    is_valid, validation_errors = validate_dag(dag_data["tasks"])
    
    if not is_valid:
        print(f"   ❌ 验证失败：{validation_errors}")
        return
    
    print(f"   ✅ DAG 验证通过")
    
    has_cycle, cycle_path = detect_cycles(dag_data["tasks"])
    if has_cycle:
        print(f"   ❌ 检测到环：{' -> '.join(cycle_path)}")
        return
    
    print(f"   ✅ 无环检测通过")
    print()
    
    print("📊 任务依赖图:")
    for task in dag_data["tasks"]:
        deps = task.get("blockedBy", [])
        if deps:
            print(f"   {task['id']} ← {', '.join(deps)}")
        else:
            print(f"   {task['id']} (根节点)")
    print()
    
    print("🎯 并行分析:")
    root_tasks = [t for t in dag_data["tasks"] if not t.get("blockedBy")]
    
    all_blocker_ids = set()
    for task in dag_data["tasks"]:
        all_blocker_ids.update(task.get("blockedBy", []))
    
    final_tasks = [t for t in dag_data["tasks"] if t["id"] not in all_blocker_ids]
    
    print(f"   - 可并行执行的根任务：{len(root_tasks)} 个 ({', '.join([t['id'] for t in root_tasks])})")
    print(f"   - 最终汇聚任务：{len(final_tasks)} 个")
    print()
    
    print("🔄 测试重试处理器...")
    
    call_count = 0
    async def flaky_llm():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            return "invalid json"
        return mock_llm_response
    
    retry_handler = RetryHandler(max_retries=3, base_delay=0.01)
    
    try:
        result = await retry_handler.execute_with_retry(flaky_llm)
        print(f"   ✅ 重试成功（尝试 {call_count} 次）")
    except Exception as e:
        print(f"   ❌ 重试失败：{e}")
    
    print()
    print("="*70)
    print("演示完成！✨")
    print("="*70 + "\n")
    
    return dag_data


if __name__ == "__main__":
    asyncio.run(demo())
