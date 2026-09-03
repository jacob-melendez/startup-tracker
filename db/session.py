"""Async engine and session factory (SQLAlchemy 2.0 + asyncpg; SPEC §3).

Usage::

    async with session_scope() as session:
        ...

The engine is created lazily from ``settings.database_url`` and shared per process.
Tests and scripts that need a different database pass an explicit URL to
:func:`create_engine`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from settings import get_settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def create_engine(url: str | None = None, **kwargs: object) -> AsyncEngine:
    """Create a new async engine (asyncpg). ``url`` defaults to ``DATABASE_URL``."""
    options: dict[str, object] = {"pool_pre_ping": True}
    options.update(kwargs)
    return create_async_engine(url or get_settings().database_url, **options)


def get_engine() -> AsyncEngine:
    """The process-wide engine, created on first use."""
    global _engine
    if _engine is None:
        _engine = create_engine()
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """The process-wide session factory bound to :func:`get_engine`."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """A session that commits on success and rolls back on any exception."""
    async with get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except BaseException:
            await session.rollback()
            raise


async def dispose_engine() -> None:
    """Close the process-wide engine (call at shutdown or between event loops)."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None
