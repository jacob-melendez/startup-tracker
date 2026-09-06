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


def rejection_message(url: str) -> str:
    """The validator's own message for ``url`` — the string, and the only part of the
    ``ValidationError`` that is ever printed.

    Both callers render the failure through ``cli.format_settings_error``, which reads
    ``error["msg"]``; pydantic's ``str(exc)`` additionally echoes ``input_value``, but nothing in
    this repo prints that. So the message is what has to be safe.
    """
    with pytest.raises(ValidationError) as caught:
        Settings(_env_file=None, database_url=url)
    errors = caught.value.errors()
    assert len(errors) == 1, errors
    return str(errors[0]["msg"])


def test_the_rejection_message_names_the_scheme_and_not_the_credentials() -> None:
    """The likeliest way to trip this validator is pasting a *working* libpq URL that merely
    lacks ``+asyncpg`` — so the rejected value is usually a live credential.

    ``cli.py``'s callback and ``scheduler.py``'s ``main`` both print this message to stderr,
    and stderr is what ends up in ``docker compose logs`` and in a pasted bug report. The scheme
    is what is wrong and the whole of what the reader needs; the password, user, host and
    database name are not.
    """
    message = rejection_message("postgresql://tracker:hunter2@db.internal:5432/prod")

    assert "'postgresql://'" in message
    for secret in ("hunter2", "tracker", "db.internal", "prod"):
        assert secret not in message


def test_a_url_with_no_scheme_at_all_is_described_rather_than_quoted() -> None:
    """There is nothing safe to cut at, so nothing of the value is echoed."""
    message = rejection_message("tracker:hunter2@db.internal")

    assert "a value with no scheme" in message
    assert "hunter2" not in message


# ------------------------------------------------------------ contact email / User-Agent (SPEC §4)


@pytest.mark.parametrize("value", ["", "   ", None])
def test_blank_contact_email_means_unset(value: str | None) -> None:
    settings = Settings(_env_file=None, contact_email=value)
    assert settings.contact_email is None
    assert settings.user_agent == "startup-tracker/0.1"


def test_contact_email_lands_in_the_user_agent_in_the_shape_sec_accepts() -> None:
    settings = Settings(_env_file=None, contact_email=" you@example.com ")
    assert settings.user_agent == "startup-tracker/0.1 you@example.com"


@pytest.mark.parametrize("value", ["dev@localhost", "not-an-email", "a@b", "@example.com"])
def test_contact_email_without_a_real_domain_is_rejected(value: str) -> None:
    with pytest.raises(ValidationError, match="CONTACT_EMAIL"):
        Settings(_env_file=None, contact_email=value)


def test_blank_contact_email_from_the_environment_means_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONTACT_EMAIL", "")
    assert Settings(_env_file=None).contact_email is None
