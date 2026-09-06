"""Loaders for ``config/connectors.yaml`` and ``config/regions.yaml`` (SPEC §7.2, §11).

* ``connectors.yaml`` is the only place a schedule (cron cadence) or a rate limit is defined.
* ``regions.yaml`` is the only place Bay-Area-specific logic may live: the city list, the
  ``metro`` label that lands on ``Location.metro``, and the state.

Both files are validated with Pydantic on load so a typo fails fast with a path to the field.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Any

import yaml
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]  # no py.typed
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
CONNECTORS_YAML = CONFIG_DIR / "connectors.yaml"
REGIONS_YAML = CONFIG_DIR / "regions.yaml"

#: Connectors with a block in ``config/connectors.yaml`` but no implementation yet (SPEC §4
#: Tier 2 #6, #7). They are still configured, still listed by ``/runs`` and ``cli.py stats``,
#: and marked in both, so an unimplemented source reads as unimplemented rather than as one
#: that has silently never run.
#:
#: Written out here rather than derived from :func:`ingest.connectors.all_connectors` for two
#: reasons. Nothing under ``web/`` may import a connector module — that package pulls in the
#: HTTP client, and a request handler must not be one import away from an outbound call
#: (SPEC §2, CLAUDE.md) — while ``ingest.config`` reads local YAML and is already imported
#: there. And the registry is not the right question anyway: ``seed`` is implemented
#: (:mod:`ingest.seed`) yet deliberately absent from it, so "not in the registry" and "not
#: implemented" are different sets. ``tests/test_docs.py`` pins this one against the registry
#: and the YAML so it cannot drift from either.
UNIMPLEMENTED_CONNECTORS = frozenset({"product_hunt", "opencorporates"})


def _normalize_city(city: str) -> str:
    return " ".join(city.split()).casefold()


class RateLimit(BaseModel):
    """Per-host limits enforced by :class:`ingest.http.HttpClient`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    requests_per_second: float = Field(default=1.0, gt=0)
    max_requests_per_host_per_run: int | None = Field(default=None, gt=0)

    @property
    def min_interval(self) -> float:
        """Seconds between two requests to the same host."""
        return 1.0 / self.requests_per_second


class ConnectorConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    cadence: str | None = None
    enabled: bool = True
    rate_limit: RateLimit = RateLimit()
    respect_robots: bool = True
    options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("cadence")
    @classmethod
    def _five_field_cron(cls, value: str | None) -> str | None:
        """A cadence must be a cron expression APScheduler can actually run.

        The field count is checked first, so the specific "five-field" message still fires for
        ``"0 5 * *"``. Then the expression is *parsed*, because counting fields catches nothing
        about their values: ``"0 99 * * *"`` and ``"0 5 */3 * z"`` both have five. Parsing here
        is what makes this module's promise true — a typo fails on load with a path to the
        field (``connectors.sec_edgar.cadence``) rather than at the first thing that happens to
        parse it, which today is :func:`cadence_period` inside the ``/runs`` request handler,
        where an unhandled ``ValueError`` turns the one page that exists to show breakage into
        a 500.
        """
        if value is None:
            return value
        if len(value.split()) != 5:
            msg = f"cadence must be a five-field cron expression, got {value!r}"
            raise ValueError(msg)
        try:
            CronTrigger.from_crontab(value, timezone=UTC)
        except ValueError as exc:  # out-of-range field, unknown weekday name, bad step, ...
            msg = f"cadence {value!r} is not a valid cron expression: {exc}"
            raise ValueError(msg) from exc
        return value


#: An arbitrary fixed instant to measure a cadence from. Fixed, so :func:`cadence_period` is a
#: pure function of the cron expression: measuring from "now" would make a monthly cadence 28
#: days in February and 31 in March, and the staleness threshold would drift with the calendar.
_CADENCE_REFERENCE = datetime(2025, 1, 1, tzinfo=UTC)


@cache
def cadence_period(cadence: str | None) -> timedelta | None:
    """Nominal interval between two firings of a five-field cron expression (SPEC §9 "twice its
    cadence"). ``None`` for an on-demand connector (``cadence: null``).

    Uses APScheduler's ``CronTrigger`` — the same cron implementation the Phase 6 scheduler will
    run, so ``/runs`` can never disagree with the schedule about what "daily" means — measured
    between the next two fire times after a fixed reference instant. That makes an *irregular*
    cadence report its typical period rather than an exact one: ``0 9 2 * *`` (monthly) is 31
    days and ``0 5 */3 * *`` is 3 days even though both are shorter across some month
    boundaries. For deciding whether a connector has gone quiet, typical is what is wanted.

    ``None`` also comes back for an expression that can never fire (``0 0 30 2 *``); the caller
    treats that the same as on-demand, since a connector that never fires cannot be overdue.
    """
    if cadence is None:
        return None
    trigger = CronTrigger.from_crontab(cadence, timezone=UTC)
    # APScheduler ships no type information, so annotate what it hands back.
    first: datetime | None = trigger.get_next_fire_time(None, _CADENCE_REFERENCE)
    if first is None:
        return None
    second: datetime | None = trigger.get_next_fire_time(first, first + timedelta(seconds=1))
    if second is None:
        return None
    return second - first


class ConnectorsConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    connectors: dict[str, ConnectorConfig]

    @model_validator(mode="before")
    @classmethod
    def _inject_names(cls, data: Any) -> Any:
        """The YAML keys each block by connector name; copy the key into ``name``."""
        if isinstance(data, dict) and isinstance(data.get("connectors"), dict):
            data = dict(data)
            data["connectors"] = {
                key: {"name": key, **(value or {})} if isinstance(value, dict | None) else value
                for key, value in data["connectors"].items()
            }
        return data

    def get(self, name: str) -> ConnectorConfig:
        try:
            return self.connectors[name]
        except KeyError:
            known = ", ".join(sorted(self.connectors))
            msg = f"no connector {name!r} in {CONNECTORS_YAML.name}; known: {known}"
            raise KeyError(msg) from None


class RegionCity(BaseModel):
    """One configured city with the region it belongs to."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    city: str
    state: str
    country: str
    metro: str


class Region(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    metro: str = Field(min_length=1)
    state: str = Field(min_length=1)
    country: str = "US"
    cities: tuple[str, ...] = Field(min_length=1)


class RegionsConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    regions: tuple[Region, ...] = Field(min_length=1)

    @property
    def cities(self) -> tuple[RegionCity, ...]:
        return tuple(
            RegionCity(city=city, state=region.state, country=region.country, metro=region.metro)
            for region in self.regions
            for city in region.cities
        )

    def lookup(self, city: str, state: str) -> RegionCity | None:
        """The configured city matching ``city``/``state`` (case- and whitespace-insensitive)."""
        wanted = (_normalize_city(city), state.strip().casefold())
        for entry in self.cities:
            if (_normalize_city(entry.city), entry.state.casefold()) == wanted:
                return entry
        return None

    def metro_for(self, city: str, state: str) -> str | None:
        entry = self.lookup(city, state)
        return entry.metro if entry else None


def _load_yaml(path: Path) -> Any:
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@cache
def load_connectors_config(path: Path = CONNECTORS_YAML) -> ConnectorsConfig:
    return ConnectorsConfig.model_validate(_load_yaml(path))


@cache
def load_regions_config(path: Path = REGIONS_YAML) -> RegionsConfig:
    return RegionsConfig.model_validate(_load_yaml(path))
