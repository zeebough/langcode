"""PostgreSQL connection helpers shared by CLI, tests, and load tests."""

from __future__ import annotations

import os
from dataclasses import dataclass

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


@dataclass(frozen=True)
class PoolConfig:
    min_size: int = 10
    max_size: int = 80
    timeout: float = 30.0
    max_waiting: int = 0


def get_postgres_uri() -> str:
    """Build the PostgreSQL URI from POSTGRES_* environment variables."""
    return (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
        f"/{os.getenv('POSTGRES_DB')}?sslmode=disable"
    )


def get_pool_config(env_prefix: str = "POSTGRES_POOL") -> PoolConfig:
    """Read pool sizing from env with local load-test friendly defaults.

    The default max_size=80 is intentionally far below 2000 virtual users: the
    pool should protect local PostgreSQL from connection explosions while still
    allowing hundreds to ~1000 short DB operations per second on a typical dev
    machine. Override with POSTGRES_POOL_MAX_SIZE or LOAD_POSTGRES_POOL_MAX_SIZE
    when running capacity exploration.
    """
    return PoolConfig(
        min_size=int(os.getenv(f"{env_prefix}_MIN_SIZE", os.getenv("POSTGRES_POOL_MIN_SIZE", "10"))),
        max_size=int(os.getenv(f"{env_prefix}_MAX_SIZE", os.getenv("POSTGRES_POOL_MAX_SIZE", "80"))),
        timeout=float(os.getenv(f"{env_prefix}_TIMEOUT", os.getenv("POSTGRES_POOL_TIMEOUT", "30"))),
        max_waiting=int(os.getenv(f"{env_prefix}_MAX_WAITING", os.getenv("POSTGRES_POOL_MAX_WAITING", "0"))),
    )


def create_async_pool(env_prefix: str = "POSTGRES_POOL") -> AsyncConnectionPool:
    """Create an AsyncConnectionPool with shared project defaults."""
    config = get_pool_config(env_prefix)
    return AsyncConnectionPool(
        get_postgres_uri(),
        min_size=config.min_size,
        max_size=config.max_size,
        timeout=config.timeout,
        max_waiting=config.max_waiting,
        kwargs={
            "autocommit": True,
            "row_factory": dict_row,
        },
        open=False,
    )
