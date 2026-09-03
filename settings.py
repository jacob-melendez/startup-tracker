"""Runtime settings via pydantic-settings (SPEC §3).

Values come from environment variables or a local ``.env`` file; ``.env.example`` documents
them. Import :func:`get_settings` rather than instantiating :class:`Settings` directly.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ASYNC_POSTGRES_SCHEME = "postgresql+asyncpg://"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = Field(
        default="postgresql+asyncpg://tracker:tracker@localhost:5432/tracker",
        description="Async SQLAlchemy URL. Postgres 16 via asyncpg only (SPEC §3).",
    )
    log_level: str = Field(default="INFO", description="Root log level name.")
    log_json: bool = Field(default=True, description="JSON log lines (SPEC §3) or console output.")

    @field_validator("database_url")
    @classmethod
    def _postgres_only(cls, value: str) -> str:
        # Non-negotiable: no SQLite (or any other) fallback — see CLAUDE.md.
        if not value.startswith(ASYNC_POSTGRES_SCHEME):
            msg = f"DATABASE_URL must start with {ASYNC_POSTGRES_SCHEME!r}; got {value!r}"
            raise ValueError(msg)
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
