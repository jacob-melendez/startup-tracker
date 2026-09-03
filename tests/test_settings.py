"""Settings guard rails (SPEC §3, CLAUDE.md): Postgres via asyncpg is the only database."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from settings import Settings

ASYNCPG = "postgresql+asyncpg://"


@pytest.fixture(autouse=True)
def _no_database_url_from_the_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)


def test_default_database_url_is_async_postgres() -> None:
    assert Settings(_env_file=None).database_url.startswith(ASYNCPG)


@pytest.mark.parametrize(
    "url",
    [
        "sqlite:///tracker.db",
        "sqlite+aiosqlite:///:memory:",
        "postgresql://tracker:tracker@localhost/tracker",  # sync driver
        "postgresql+psycopg://tracker:tracker@localhost/tracker",
    ],
)
def test_non_asyncpg_urls_are_rejected(url: str) -> None:
    with pytest.raises(ValidationError, match="postgresql\\+asyncpg://"):
        Settings(_env_file=None, database_url=url)


def test_non_asyncpg_url_from_the_environment_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite:///tracker.db")
    with pytest.raises(ValidationError, match="postgresql\\+asyncpg://"):
        Settings(_env_file=None)


def test_asyncpg_url_from_the_environment_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", f"{ASYNCPG}u:p@db:5432/x")
    assert Settings(_env_file=None).database_url == f"{ASYNCPG}u:p@db:5432/x"
