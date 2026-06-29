# middlewares/context_vars.py
"""共享的上下文变量，用于避免中间件内部 LLM 调用触发递归"""
import contextvars

_internal_call = contextvars.ContextVar("_internal_call", default=False)
