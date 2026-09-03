"""Alembic environment — async engine (asyncpg), models' metadata as the target.

The database URL resolution order is:

1. ``sqlalchemy.url`` set programmatically on the Alembic ``Config`` (tests do this
   to point at their disposable database);
2. ``DATABASE_URL`` from the environment / ``.env`` via :mod:`settings`.

``alembic.ini`` leaves ``sqlalchemy.url`` blank on purpose.
"""

from __future__ import annotations

import asyncio
import logging
import warnings

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import Base
from logging_config import configure_logging
from settings import get_settings

config = context.config
target_metadata = Base.metadata

# Postgres normalizes generated-column expressions, so autogenerate cannot compare the two
# tsvector Computed defaults textually and warns on every `alembic check`. The columns are
# asserted directly in tests/test_migration.py (same filter in pyproject.toml).
warnings.filterwarnings("ignore", message="Computed default on .* cannot be modified")


def get_url() -> str:
    return config.get_main_option("sqlalchemy.url") or get_settings().database_url


def _configure_logging() -> None:
    """Configure structlog for the bare ``alembic`` CLI (alembic.ini has no [loggers] section).

    Programmatic callers — pytest, ``cli.py`` — already own the root logger; leave it alone when
    handlers are present so importing this module has no logging side effect for them.
    """
    if logging.getLogger().handlers:
        return
    settings = get_settings()
    configure_logging(level=settings.log_level, json_output=settings.log_json)


def run_migrations_offline() -> None:
    """Emit SQL to stdout without a database connection (``alembic upgrade head --sql``)."""
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(get_url(), poolclass=pool.NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await engine.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


_configure_logging()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
