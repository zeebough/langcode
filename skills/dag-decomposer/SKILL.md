---
name: dag-decomposer
description: Decompose complex, multi-step tasks into parallelizable subtasks using a DAG (Directed Acyclic Graph) structure. Trigger when a task has >=4 distinct steps, explicit parallelizable modules, or risks context overflow, enabling sub-agent teams to execute efficiently.
---

# DAG Decomposer Skill

## Purpose
Transform a complex user request into an executable Directed Acyclic Graph (DAG) of subtasks. This skill acts as a "Lead Agent" planner, defining clear interfaces between subtasks so that sub-agents can work concurrently without stepping on each other's toes.

The output of this skill is designed to map **0-cost** into a PostgreSQL-backed task scheduler that uses `blocked_by_count` for efficient agent claiming via `SKIP LOCKED`.

---

## When to Use (Trigger Conditions)

Activate this skill **only** if the user request meets **any** of the following criteria. Otherwise, execute the task directly using your own capabilities without spawning sub-agents.

1.  **Multi-step complexity**: The task requires **>=4** distinct logical steps to complete.
2.  **Explicit parallelism**: The task contains naturally independent modules that can run simultaneously (e.g., "Analyze sales data" and "Analyze competitor news").
3.  **External isolation**: Subtasks require different tools, API keys, or runtime environments (e.g., `web_scraper` vs. `gpu_inference`) that are better sandboxed.
4.  **Context window risk**: The total estimated token consumption for retrieving all necessary data exceeds 70% of your available context window.

---

## Core Principles for Decomposition

- **Single Responsibility**: Each subtask must be self-contained. A sub-agent receiving this task should be able to complete it without asking the Lead Agent for clarification.
- **DAG Construction**: Identify data flow explicitly. If Task B requires Task A's output, add A's ID to B's `blockedBy` array.
- **Cycle Prevention**: Ensure the graph is acyclic. If a cycle is detected (A blocks B, B blocks A), merge them into a single atomic subtask.
- **Maximize Parallelism**: Generate as many root nodes (`blockedBy: []`) as possible to fully utilize available sub-agents right from the start.

---

## Output Specification (JSON Schema)

You **MUST** output a single JSON object matching the schema below.
> **Critical**: Do **not** wrap it in Markdown code blocks (e.g., ```json) or add explanatory text outside the JSON. The downstream parser expects raw JSON.

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["plan_summary", "tasks"],
  "properties": {
    "plan_summary": {
      "type": "string",
      "description": "Concise explanation of the decomposition strategy, parallelization opportunities, and identified critical path."
    },
    "tasks": {
      "type": "array",
      "description": "List of all subtasks in the DAG. Dependencies are explicitly defined via the 'blockedBy' field.",
      "items": {
        "type": "object",
        "required": ["id", "subject", "description", "blockedBy"],
        "properties": {
          "id": {
            "type": "string",
            "pattern": "^[a-zA-Z0-9_-]+$",
            "description": "Unique semantic identifier (e.g., 'fetch_logs', 'train_model'). Must be unique across the list."
          },
          "subject": {
            "type": "string",
            "maxLength": 100,
            "description": "Short, action-oriented title (verb + noun), e.g., 'Clean user session data'."
          },
          "description": {
            "type": "string",
            "description": "Detailed, unambiguous instructions. Must include input sources, transformation logic, and expected output format for the sub-agent."
          },
          "blockedBy": {
            "type": "array",
            "description": "List of upstream task IDs that must complete before this task starts. Use an empty array ([]) for root tasks.",
            "items": { "type": "string" },
            "uniqueItems": true
          },
          "metadata": {
            "type": "object",
            "description": "Extended key-value pairs for scheduling optimization or specialized agent routing.",
            "properties": {
              "priority": { "type": "integer", "minimum": 1, "maximum": 10, "description": "Higher priority (lower number) gets scheduled first." },
              "required_capability": { "type": "string", "description": "Tags for agent selection, e.g., 'gpu', 'web_scraper', 'finance-api'." },
              "estimated_tokens": { "type": "integer", "description": "Estimated input/output token size for cost/rate limiting." }
            },
            "additionalProperties": true
          }
        }
      }
    }
  },
  "additionalProperties": false
}
```

### Example Output (Generating a Weekly Market Report)

```json
{
  "plan_summary": "Parallel fetching of three data sources (sales, competitors, sentiment). After all complete, generate the final markdown report.",
  "tasks": [
    {
      "id": "fetch_sales",
      "subject": "Fetch internal sales data",
      "description": "Query the 'sales.weekly' table in the data warehouse for the last 7 days. Export results as a CSV file to '/data/sales.csv'.",
      "blockedBy": []
    },
    {
      "id": "fetch_competitor",
      "subject": "Scrape competitor news",
      "description": "Scrape the top 5 competitor press release pages. Use the 'browser' tool. Save raw HTML to '/data/competitor_raw/'.",
      "blockedBy": []
    },
    {
      "id": "fetch_sentiment",
      "subject": "Analyze social sentiment",
      "description": "Call the NLP sentiment API (endpoint /v1/sentiment) with keywords 'ProductX'. Store the aggregated score in '/data/sentiment.json'.",
      "blockedBy": []
    },
    {
      "id": "merge_report",
      "subject": "Generate final weekly report",
      "description": "Combine '/data/sales.csv', '/data/competitor_raw/', and '/data/sentiment.json' into a single markdown file with sections: Sales Trends, Competitive Landscape, and Sentiment Heatmap.",
      "blockedBy": ["fetch_sales", "fetch_competitor", "fetch_sentiment"]
    }
  ]
}
```

## Best Practices for Writing Subtask Descriptions

The `description` field is the **sole instruction** for the sub-agent. To ensure zero back-and-forth, strictly follow the **3W1H** rule:

- **What**: Define the exact final output (e.g., "Return a JSON object with key `result`", "Save a Markdown table").
- **Where**: Provide explicit input locations (e.g., "Database table `orders.public`", "S3 bucket `s3://data/raw/`", "File path `/tmp/input.csv`").
- **How**: Specify constraints or critical logic (e.g., "Timeout after 30 seconds", "Retry on 5xx errors up to 3 times", "Filter rows where `status != 'active'`").
- **Who**: If a specialized runtime is needed, set `metadata.required_capability` (e.g., `gpu`, `high_memory`, `web_access`).

---

## Execution Workflow (Step-by-Step)

1.  **Analyze**: Break the target goal into atomic actions. Identify which actions require data from others.
2.  **Draft Dependencies**: Draw directed edges. Map the immediate prerequisites for every action.
3.  **Validate Locally**:
    - Ensure all `blockedBy` IDs exist in the `tasks` list (no dangling references).
    - Verify no circular references exist (A->B and B->A). If found, merge them.
4.  **Finalize JSON**: Remove any Markdown formatting and output the plain JSON object.

---

## Edge Cases & Fallbacks

- **Ambiguous Requests**: If the user's goal is unclear or lacks details, **do not force a fake decomposition**. Instead, output a single task with `id: "clarify_requirements"` and describe the specific ambiguity in the `plan_summary`. The system will treat this as a human-interaction task.
- **Too Many Tasks**: If decomposition yields more than **15** subtasks, consider merging cohesive, sequential steps (e.g., "Extract, Transform, Load" -> "ETL Batch") to reduce scheduler overhead and latency.
- **Zero Parallelism**: If the task is strictly linear (Step 1 -> Step 2 -> Step 3), you may still use this skill to isolate concerns, but it is often more efficient to execute it directly as the Lead Agent without spawning sub-agents (refer to the "When to Use" rules).

---

## Pre-Validation Checklist

Before finalizing your JSON output, perform this mental self-check:

- [ ] **Uniqueness**: Is every `id` unique across the `tasks` array?
- [ ] **Referential Integrity**: Do all IDs listed in `blockedBy` exist in the `tasks` list? (No ghosts)
- [ ] **Acyclicity**: Is the graph guaranteed to be acyclic? (No mutual dependencies)
- [ ] **Root Availability**: Is there at least one root task with `blockedBy: []`? (Otherwise, the DAG is deadlocked)
- [ ] **Actionability**: Would a brand-new sub-agent be able to execute the `description` without asking clarifying questions?
- [ ] **Schema Conformance**: Is the output a valid JSON object matching the schema (no trailing commas, correct data types)?

---

## Integration Context (For Awareness)

The output JSON directly feeds into the database layer:

- `blockedBy` array length → PostgreSQL `blocked_by_count`.
- Tasks with `blocked_by_count = 0` are immediately claimable by sub-agents using `SELECT FOR UPDATE SKIP LOCKED`.
- Failed tasks are reset with exponential backoff (handled by the scheduler, not this skill).

You do not need to worry about these mechanics; simply produce the correct DAG structure.

