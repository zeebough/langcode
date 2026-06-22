# LangCode - LangChain based Claude Code like coding agent

## Finished

- agent loop
- tool use
- permission check: (permission middleware (customized), deny/allow/ask)
- hooks: (middleware-based)
- todo write: (todo middleware)
- context compact + in-session memory (short-term memory): (async postgres checkpointer + context compression middleware (customized))
- memory: sematic (user preferences) + procedural (behavioral guidelines) + episodic (past experience), LLM-based retrieval, use files as indices for retrieval (long-term memory)
- system prompt: real-time assembly by the middleware sequence

## in_progress

- subagent
- skill-loading

## pending

- error recovery
- task system
- background tasks
- cron scheduler
- agent teams
- team protocols
- autonomous agents
- worktree isolation
- mcp plugin
