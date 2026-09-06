"""Runtime settings via pydantic-settings (SPEC §3).

Values come from environment variables or a local ``.env`` file; ``.env.example`` documents
them. Import :func:`get_settings` rather than instantiating :class:`Settings` directly.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ASYNC_POSTGRES_SCHEME = "postgresql+asyncpg://"
APP_NAME = "startup-tracker"
APP_VERSION = "0.1"
# SEC's WAF wants "<name> <email>" with a real domain; "dev@localhost" is rejected (ingest/http.py).
_CONTACT_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = Field(
        default="postgresql+asyncpg://tracker:tracker@localhost:5432/tracker",
        description="Async SQLAlchemy URL. Postgres 16 via asyncpg only (SPEC §3).",
    )
    log_level: str = Field(default="INFO", description="Root log level name.")
    log_json: bool = Field(default=True, description="JSON log lines (SPEC §3) or console output.")
    contact_email: str | None = Field(
        default=None,
        description=(
            "Contact email placed in the outbound User-Agent (SPEC §4). SEC EDGAR's fair-access "
            "policy requires it; the sec_edgar connector refuses to run without it."
        ),
    )
    http_cache_dir: Path = Field(
        default=Path(".cache/http"),
        description="Directory for the ETag/Last-Modified response cache (SPEC §4, 24 h).",
    )
    http_timeout_seconds: float = Field(default=30.0, gt=0)

    @property
    def user_agent(self) -> str:
        """``"startup-tracker/0.1 you@example.com"`` — exactly the shape SEC accepts."""
        base = f"{APP_NAME}/{APP_VERSION}"
        return f"{base} {self.contact_email}" if self.contact_email else base

    @field_validator("contact_email")
    @classmethod
    def _looks_like_an_email(cls, value: str | None) -> str | None:
        # ``CONTACT_EMAIL=`` (as shipped in .env.example) means unset, not invalid.
        if value is None or not value.strip():
            return None
        value = value.strip()
        if not _CONTACT_EMAIL.fullmatch(value):
            msg = f"CONTACT_EMAIL must be an address with a real domain; got {value!r}"
            raise ValueError(msg)
        return value

    @field_validator("database_url")
    @classmethod
    def _postgres_only(cls, value: str) -> str:
        # Non-negotiable: no SQLite (or any other) fallback — see CLAUDE.md.
        if not value.startswith(ASYNC_POSTGRES_SCHEME):
            # Only the *scheme* is echoed back, never the value. This message is printed to
            # stderr verbatim by ``cli.py``'s callback and ``scheduler.py``'s ``main``, and the
            # single likeliest way to reach it is pasting a working libpq URL
            # (``postgresql://user:password@host/db``) that merely lacks ``+asyncpg`` — so the
            # value in hand is usually a live credential. The scheme is the whole of what the
            # reader needs, since the scheme is what is wrong. Everything after ``://`` is
            # userinfo, host and database, so cutting there cannot leak a password.
            scheme, separator, _ = value.partition("://")
            got = repr(scheme + separator) if separator else "a value with no scheme"
            msg = f"DATABASE_URL must start with {ASYNC_POSTGRES_SCHEME!r}; got {got}"
            raise ValueError(msg)
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
