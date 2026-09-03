"""Test fixtures: a disposable Postgres 16 with the Alembic migrations applied.

The database comes from ``TEST_DATABASE_URL`` when set — in the environment or in the repo's
``.env`` — otherwise from a ``postgres:16`` container started with testcontainers (needs
Docker). SPEC §3: tests run against real Postgres; there is no SQLite fallback.

A configured database is *disposable*: it is created if missing, its name must end in
``_test`` (so the development database can never be pointed at by mistake), and its
``public`` schema is dropped at the start of every session.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from dotenv import dotenv_values
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from testcontainers.community.postgres import PostgresContainer

ROOT = Path(__file__).resolve().parent.parent
POSTGRES_IMAGE = "postgres:16"  # the same image as docker-compose.yml
_DISPOSABLE_NAME = re.compile(r"[a-z0-9_]*_test|test")


def _configured_test_url() -> str | None:
    """``TEST_DATABASE_URL`` from the process environment, else from ``.env``."""
    return os.environ.get("TEST_DATABASE_URL") or dotenv_values(ROOT / ".env").get(
        "TEST_DATABASE_URL"
    )


async def _ensure_database(url: str) -> None:
    """Create the test database if it does not exist yet (via the ``postgres`` maintenance DB)."""
    target = make_url(url)
    engine = create_async_engine(target.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": target.database}
            )
            if not exists:
                await conn.execute(text(f'CREATE DATABASE "{target.database}"'))
    finally:
        await engine.dispose()


async def _reset_public_schema(url: str) -> None:
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    """Async URL of a disposable Postgres 16 database."""
    url = _configured_test_url()
    if url:
        name = make_url(url).database or ""
        if not _DISPOSABLE_NAME.fullmatch(name):
            pytest.fail(
                "TEST_DATABASE_URL must point at a disposable database whose name ends in "
                f"'_test' (got {name!r}): its public schema is dropped on every run."
            )
        asyncio.run(_ensure_database(url))
        yield url
        return

    try:
        container = PostgresContainer(POSTGRES_IMAGE, driver="asyncpg")
        container.start()
    except Exception as exc:
        pytest.fail(
            "Could not start a disposable Postgres with testcontainers "
            f"({type(exc).__name__}: {exc}). Start Docker, or set TEST_DATABASE_URL (in the "
            "environment or .env) to a disposable Postgres 16 database (postgresql+asyncpg://...)."
        )
    try:
        yield container.get_connection_url()
    finally:
        container.stop()


@pytest.fixture(scope="session")
def alembic_config(database_url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    # configparser interpolation: a literal % in a URL must be doubled.
    cfg.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return cfg


@pytest.fixture(scope="session")
def migrated_database(alembic_config: Config, database_url: str) -> str:
    """The disposable database after ``alembic upgrade head`` from an empty ``public`` schema.

    Runs synchronously: Alembic's env.py drives its own event loop (``asyncio.run``).
    """
    asyncio.run(_reset_public_schema(database_url))
    command.upgrade(alembic_config, "head")
    return database_url


@pytest.fixture
async def conn(migrated_database: str) -> AsyncIterator[AsyncConnection]:
    """A fresh asyncpg connection to the migrated database."""
    engine = create_async_engine(migrated_database)
    try:
        async with engine.connect() as connection:
            yield connection
    finally:
        await engine.dispose()
