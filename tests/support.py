"""Shared helpers for the connector tests (SPEC §3: fixtures via ``respx``, never the network).

``tests/test_sec_edgar.py`` grew its own copies of these in Phase 2; the Phase 3 connectors
share them from here so eight test modules do not each redefine a clock and a client.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from db import enums
from ingest.base import CompanyTarget, FetchContext
from ingest.config import ConnectorConfig, load_connectors_config
from ingest.http import HttpClient, MemoryCache
from logging_config import get_logger

FIXTURES = Path(__file__).parent / "fixtures"
UA = "startup-tracker/0.1 tests@example.com"
#: A fixed "now" for every Phase 3 connector test.
NOW = datetime(2026, 9, 4, 12, tzinfo=UTC)


class FakeClock:
    """Deterministic time: the clocks only move when something sleeps, and every sleep is
    recorded rather than waited for, so a rate limiter costs a test nothing."""

    def __init__(self) -> None:
        self.mono = 1_000.0
        self.wall = 1_700_000_000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.mono

    def time(self) -> float:
        return self.wall

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.mono += seconds
        self.wall += seconds


def fixture_text(*parts: str) -> str:
    return FIXTURES.joinpath(*parts).read_text(encoding="utf-8")


def fixture_json(*parts: str) -> Any:
    return json.loads(fixture_text(*parts))


def connector_config(name: str, **options: Any) -> ConnectorConfig:
    """The connector's real ``config/connectors.yaml`` block with ``options`` overridden, so a
    test always exercises the shipped rate limit and robots policy (SPEC §7.2)."""
    config = load_connectors_config().get(name)
    return config.model_copy(update={"options": {**config.options, **options}})


def make_client(config: ConnectorConfig, clock: FakeClock, *, user_agent: str = UA) -> HttpClient:
    """An ``HttpClient`` built exactly as ``cli.py`` builds it, on the fake clock."""
    return HttpClient(
        user_agent=user_agent,
        rate_limit=config.rate_limit,
        respect_robots=config.respect_robots,
        cache=MemoryCache(),
        clock=clock.monotonic,
        sleep=clock.sleep,
        wall_clock=clock.time,
    )


def make_ctx(
    http: HttpClient,
    *,
    now: datetime = NOW,
    since: datetime | None = None,
    last_success_at: datetime | None = None,
    targets: Sequence[CompanyTarget] = (),
    logger_name: str = "tests",
) -> FetchContext:
    return FetchContext(
        http=http,
        now=now,
        since=since,
        last_success_at=last_success_at,
        run_id=1,
        log=get_logger(logger_name),
        targets=tuple(targets),
    )


def make_target(
    company_id: int = 1,
    *,
    name: str = "Astranis",
    domain: str | None = "astranis.com",
    website_url: str | None = None,
    status: enums.CompanyStatus = enums.CompanyStatus.ACTIVE,
    ats_provider: enums.AtsProvider | None = None,
    ats_token: str | None = None,
    external_id: str | None = None,
    last_seen_at: datetime | None = None,
) -> CompanyTarget:
    return CompanyTarget(
        company_id=company_id,
        name=name,
        domain=domain,
        website_url=website_url,
        status=status,
        ats_provider=ats_provider,
        ats_token=ats_token,
        external_id=external_id,
        last_seen_at=last_seen_at,
    )


async def collect(connector: Any, ctx: FetchContext) -> list[Any]:
    """Everything ``connector.fetch`` yields, as a list."""
    return [item async for item in connector.fetch(ctx)]
