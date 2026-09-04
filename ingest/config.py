"""Loaders for ``config/connectors.yaml`` and ``config/regions.yaml`` (SPEC §7.2, §11).

* ``connectors.yaml`` is the only place a schedule (cron cadence) or a rate limit is defined.
* ``regions.yaml`` is the only place Bay-Area-specific logic may live: the city list, the
  ``metro`` label that lands on ``Location.metro``, and the state.

Both files are validated with Pydantic on load so a typo fails fast with a path to the field.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
CONNECTORS_YAML = CONFIG_DIR / "connectors.yaml"
REGIONS_YAML = CONFIG_DIR / "regions.yaml"


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
        if value is not None and len(value.split()) != 5:
            msg = f"cadence must be a five-field cron expression, got {value!r}"
            raise ValueError(msg)
        return value


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
