"""Loaders for ``config/connectors.yaml``, ``config/regions.yaml`` and ``config/outreach.yaml``
(SPEC §7.2, §11, §12 Phase 8).

* ``connectors.yaml`` is the only place a schedule (cron cadence) or a rate limit is defined.
* ``outreach.yaml`` is the only place the outreach drafts live (SPEC §6, §12 Phase 8): the
  email subject and body, the LinkedIn connection note, the order the panel suggests people in,
  and the two lists that tell a shared inbox from a person and a legal entity from a human. The
  pitch is the thing most worth rewriting, so it is config and not code, and this module is
  where a rewrite that cannot be sent — an unknown placeholder, a note past LinkedIn's
  300-character cap — fails on load instead of inside a request handler.
* ``regions.yaml`` is the only place region-specific logic may live (SPEC §11, §12 Phase 7):
  the metros, their city lists, the ``metro`` label that lands on ``Location.metro``, the state
  each city sits in and how that state's name is written out, and the other spellings a source
  writes a city as (its aliases). No metro, city, state name or alias is named in this module —
  deriving them from the file is what lets a metro be added, or a source's spelling of one
  recognised, by editing YAML alone.

Every file here is validated with Pydantic on load so a typo fails fast with a path to the field.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path
from string import Formatter
from typing import Any, NamedTuple

import yaml
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]  # no py.typed
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from db import enums

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
CONNECTORS_YAML = CONFIG_DIR / "connectors.yaml"
REGIONS_YAML = CONFIG_DIR / "regions.yaml"
OUTREACH_YAML = CONFIG_DIR / "outreach.yaml"

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


def _require_text(value: str, field: str | None) -> str:
    """Trim ``value``; reject it when nothing is left.

    ``min_length=1`` alone accepts ``"   "``, and a whitespace-only metro would be written to
    ``Location.metro`` and offered as a blank option in the region filter. The trim is applied
    to the stored value rather than only checked, because comparison never trims the *config*
    side: :meth:`RegionsConfig.lookup` strips its argument but matches against the entry as
    stored, so a state written with a stray leading space would match nothing at all, and the
    typo would surface a whole ingest run later as a city nobody could find.
    """
    trimmed = value.strip()
    if not trimmed:
        msg = f"{field or 'value'} must not be blank, got {value!r}"
        raise ValueError(msg)
    return trimmed


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
    """One configured city with the region it belongs to.

    The resolved form of a ``cities:`` entry: ``state`` and ``state_name`` are already the
    entry's own override or the region's default, so nothing downstream has to know which of the
    two spellings the file used (see :class:`RegionCityEntry`).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: The canonical spelling. The one name this place is ever stored under, whichever spelling
    #: a source used to name it.
    city: str
    state: str
    #: How ``state`` is written out in prose, when the file says — ``None`` when it does not.
    #: Nothing is stored from it: it exists so a scanner over free text can tell a mention
    #: qualified with this city's own state from one qualified with somewhere else's, without a
    #: table of state names inside a connector (SPEC §11 puts every such literal in the config).
    state_name: str | None = None
    country: str
    metro: str
    #: The configured aliases, carried through from :class:`RegionCityEntry` so that a caller
    #: holding a resolved city can see the other spellings without re-reading the file — which
    #: is what a scanner over free text needs (:attr:`RegionsConfig.city_needles_longest_first`).
    aliases: tuple[str, ...] = ()

    @property
    def names(self) -> tuple[str, ...]:
        """Every spelling this city answers to: the canonical name first, then its aliases.

        The order is load-bearing where it is consumed: sorts over these are stable, so among
        equal-length spellings the canonical one is met first.
        """
        return (self.city, *self.aliases)


class CityNeedle(NamedTuple):
    """One spelling to look for in free text, and the city it resolves to.

    ``needle`` is the string to search for — a configured city's canonical name or one of its
    aliases. ``city`` is the :class:`RegionCity` it belongs to, always spelled canonically, so a
    scanner that matched an alias still files the record under the one name that place is stored
    under, with its state, country and metro already resolved.

    A :class:`~typing.NamedTuple` rather than a model: nothing parses one from YAML, it is
    derived from what was parsed, and the pairing is what callers unpack
    (``for needle, city in …``) as often as they attribute-access it.
    """

    needle: str
    city: RegionCity


class RegionCityEntry(BaseModel):
    """One entry of a region's ``cities:`` list, as written in the file.

    ``state`` is ``None`` for the bare-string form — a plain name in the ``cities:`` list, which
    takes the region's state; the mapping form ``{name, state, state_name, aliases}`` overrides
    the state for that one city and lists the other spellings sources use for it. The state
    override exists because a real metro crosses state lines, and one region-level ``state``
    cannot describe a metro reaching into two of them. ``country`` stays region-level while every
    configured metro is US (SPEC §1 scopes the expansion to other US startup hubs).

    ``aliases`` is what makes ``name`` *canonical* rather than merely first. Sources spell a
    place however they like, and a spelling this file does not know is not a near miss — the
    geography filter drops the record as out of region, so the count of companies a metro has is
    decided by orthography. An alias resolves to this same entry and the entry's ``name`` is
    what is stored, so recognising a spelling never costs a second row for one place
    (``uq_locations_city_state``). Which spellings earn a line is a measured question and
    ``config/regions.yaml`` answers it there, beside each alias, with the count that earned it.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1)
    state: str | None = Field(default=None, min_length=1)
    #: How this city's own ``state`` is written out in prose. Only meaningful beside a ``state``
    #: override — see :meth:`_state_name_belongs_to_a_state`.
    state_name: str | None = Field(default=None, min_length=1)
    aliases: tuple[str, ...] = ()

    @field_validator("name", "state", "state_name")
    @classmethod
    def _non_blank(cls, value: str | None, info: ValidationInfo) -> str | None:
        return None if value is None else _require_text(value, info.field_name)

    @field_validator("aliases")
    @classmethod
    def _non_blank_aliases(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Each alias trimmed and required to be non-empty.

        Blank is the part that matters: an empty spelling would be a needle every text contains,
        so a scanner would file the first company it read under this city. The trim is the same
        normalisation every other configured name gets (:func:`_require_text`) — an alias is
        compared after :func:`_normalize_city` on both sides, so padding here would not by itself
        stop a match, and it is trimmed anyway so that two spellings differing only in whitespace
        cannot slip past the one-city-per-spelling rule below as two distinct aliases.
        """
        return tuple(_require_text(alias, "alias") for alias in value)

    @model_validator(mode="after")
    def _state_name_belongs_to_a_state(self) -> RegionCityEntry:
        """``state_name`` may be given only alongside ``state``.

        The two are one fact written two ways, and :attr:`Region.resolved_cities` therefore takes
        both from the same place: a city that overrides its state takes its own name for it, and
        one that does not takes the region's pair. So a ``state_name`` here without a ``state``
        would be silently dropped in favour of the region's, and a ``state`` without a
        ``state_name`` would leave the city with no written-out spelling at all rather than
        inheriting the *wrong* one. The first is a typo worth failing on load with a path to the
        field; the second is legitimate (a written-out qualifier simply stops promoting a mention
        for that city — see :func:`ingest.connectors.hn_hiring._names_state`) and is allowed.
        """
        if self.state_name is not None and self.state is None:
            msg = (
                f"city {self.name!r} gives state_name {self.state_name!r} without a state; "
                "a per-city state name only applies beside a per-city state"
            )
            raise ValueError(msg)
        return self


class Region(BaseModel):
    """One metro: the label its cities are stored under, and the cities themselves."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    metro: str = Field(min_length=1)
    state: str = Field(min_length=1)
    #: How ``state`` is written out in prose, for a source that spells it out instead of coding
    #: it. Optional, and it costs nothing to omit: without it a written-out qualifier simply
    #: never promotes a mention of one of this region's cities, which loses a rank and never a
    #: record (:func:`ingest.connectors.hn_hiring._names_state`).
    state_name: str | None = Field(default=None, min_length=1)
    country: str = Field(default="US", min_length=1)
    #: ``false`` keeps a region's cities in the file while taking them out of ingestion and out
    #: of every lookup — a metro is switched off in one line. Rows already stored keep the metro
    #: they were ingested with (SPEC §2: the database is the historical record), so a disabled
    #: metro can still appear in the UI facet.
    enabled: bool = True
    cities: tuple[RegionCityEntry, ...] = Field(min_length=1)

    @field_validator("metro", "state", "country", "state_name")
    @classmethod
    def _non_blank(cls, value: str | None, info: ValidationInfo) -> str | None:
        return None if value is None else _require_text(value, info.field_name)

    @field_validator("cities", mode="before")
    @classmethod
    def _accept_bare_city_names(cls, value: Any) -> Any:
        """Promote the bare-string spelling of a city to the mapping form.

        Both spellings are accepted so the Phase 1-6 files keep loading unchanged, and only one
        shape reaches anything downstream. Normalising here — ``mode="before"``, on the raw
        list — rather than by widening the field to ``str | RegionCityEntry`` keeps
        ``extra="forbid"`` doing its job on the mapping form: a misspelled ``{name, stat}``
        fails on load with the path ``regions.1.cities.4.stat``, instead of silently dropping
        the override and filing that city under the region's default state.
        """
        if isinstance(value, list | tuple):
            return [{"name": item} if isinstance(item, str) else item for item in value]
        return value

    @property
    def resolved_cities(self) -> tuple[RegionCity, ...]:
        """This region's entries as :class:`RegionCity`, each carrying the state that applies to
        it: its own override where it has one, the region's default otherwise.

        ``state`` and ``state_name`` are taken from the same side of that choice, never mixed. A
        city that overrides its state sits in a different state from the rest of the region, so
        the region's name for its own state is not that city's — inheriting it would hand a
        text scanner the written-out spelling of the wrong place
        (:func:`ingest.connectors.hn_hiring._names_state`).
        """
        return tuple(
            RegionCity(
                city=entry.name,
                state=self.state if entry.state is None else entry.state,
                state_name=self.state_name if entry.state is None else entry.state_name,
                country=self.country,
                metro=self.metro,
                aliases=entry.aliases,
            )
            for entry in self.cities
        )


class RegionsConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    regions: tuple[Region, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _reject_unsatisfiable_files(self) -> RegionsConfig:
        """Four rules a region file must satisfy, each checked once at load.

        *One metro per label.* The metro string is a region's whole identity downstream — the
        UI facet, the scope entity resolution matches names within (SPEC §8), what
        ``sync-regions`` reconciles rows against — so two blocks claiming one label are two
        regions no consumer can tell apart. Compared case-insensitively, because two spellings
        of one label differing only in case would otherwise ship as two facet values, two
        resolution scopes and two metros for one place.

        *One metro per city, across the whole file.* ``uq_locations_city_state`` gives a city
        exactly one row and one metro, so a file mapping a city into two metros describes a
        database that cannot exist. Disabled regions are included: the check must not start
        failing the day someone flips ``enabled: true``, when the mistake was made here.

        *One city per spelling.* An alias is a name a lookup answers to, so it is held to the
        rule its city's own name is held to and against the same set of names: it may not
        collide, after normalisation, with another city's name or with another alias in the same
        state, anywhere in the file, disabled regions included. A spelling two cities answer to
        has no honest resolution — one of them would win by file order, which is the arbitrary
        tie-break :meth:`lookup_city_name` refuses to make — and an alias repeating a name
        already known is caught by the same rule, since it can only add a second answer or
        nothing at all. Aliases are checked in a pass of their own, after every canonical name
        is known, so an alias colliding with a city configured *later* in the file is reported
        as the alias collision it is rather than as a duplicate city.

        *At least one region enabled.* Every city list would otherwise be empty and a full
        ``refresh`` would fetch nothing and report success — a silent no-op is the one outcome
        an operator cannot distinguish from working.
        """
        seen_metros: dict[str, str] = {}
        for region in self.regions:
            first_metro = seen_metros.get(region.metro.casefold())
            if first_metro is not None:
                also = "" if first_metro == region.metro else f" (also spelled {first_metro!r})"
                msg = f"metro {region.metro!r} is configured twice{also}; one metro is one region"
                raise ValueError(msg)
            seen_metros[region.metro.casefold()] = region.metro

        seen_cities: dict[tuple[str, str], RegionCity] = {}
        for region in self.regions:
            for entry in region.resolved_cities:
                key = (_normalize_city(entry.city), entry.state.casefold())
                first_city = seen_cities.get(key)
                if first_city is not None:
                    msg = (
                        f"city {entry.city!r} ({entry.state}) is configured in two regions, "
                        f"{first_city.metro!r} and {entry.metro!r}; one city has one metro"
                    )
                    raise ValueError(msg)
                seen_cities[key] = entry

        seen_aliases: dict[tuple[str, str], RegionCity] = {}
        for region in self.regions:
            for entry in region.resolved_cities:
                for alias in entry.aliases:
                    key = (_normalize_city(alias), entry.state.casefold())
                    named = seen_cities.get(key)
                    if named is not None:
                        msg = (
                            f"alias {alias!r} of city {entry.city!r} ({entry.state}) in "
                            f"{entry.metro!r} is already the configured name of city "
                            f"{named.city!r} ({named.state}) in {named.metro!r}; "
                            "one spelling names one city"
                        )
                        raise ValueError(msg)
                    aliased = seen_aliases.get(key)
                    if aliased is not None:
                        msg = (
                            f"alias {alias!r} of city {entry.city!r} ({entry.state}) in "
                            f"{entry.metro!r} is also an alias of city {aliased.city!r} "
                            f"({aliased.state}) in {aliased.metro!r}; "
                            "one spelling names one city"
                        )
                        raise ValueError(msg)
                    seen_aliases[key] = entry

        if not any(region.enabled for region in self.regions):
            configured = ", ".join(region.metro for region in self.regions)
            msg = f"at least one region must be enabled; all of {configured} are disabled"
            raise ValueError(msg)
        return self

    @property
    def enabled_regions(self) -> tuple[Region, ...]:
        """The regions ingestion and lookups act on, in file order."""
        return tuple(region for region in self.regions if region.enabled)

    @property
    def cities(self) -> tuple[RegionCity, ...]:
        """Every city of every *enabled* region — the geography connectors filter by."""
        return tuple(entry for region in self.enabled_regions for entry in region.resolved_cities)

    @property
    def all_cities(self) -> tuple[RegionCity, ...]:
        """Every city of every region, disabled ones included.

        For consistency checks only. Nothing that fetches or ingests may read this: a disabled
        region's cities reaching a connector is exactly what ``enabled: false`` promises will
        not happen.
        """
        return tuple(entry for region in self.regions for entry in region.resolved_cities)

    @property
    def metros(self) -> tuple[str, ...]:
        """Enabled metro labels, in file order.

        ``dict.fromkeys`` rather than ``set``: file order is the order these are listed in, and
        an unordered set would shuffle the CLI's output run to run. The validator has already
        ruled out a repeat, so the de-duplication is only a guard.
        """
        return tuple(dict.fromkeys(region.metro for region in self.enabled_regions))

    def lookup(self, city: str, state: str) -> RegionCity | None:
        """The configured city matching ``city``/``state`` (case- and whitespace-insensitive),
        by its canonical name **or any of its aliases** — returned, either way, as the entry
        whose ``city`` is the canonical spelling.

        That last clause is the whole value of an alias. A source's own spelling stops being a
        record the geography filter throws away, and it does not become a second place either:
        the caller stores ``entry.city`` rather than the string it was handed (that is what
        ``_fill_metros`` does with this), so ``uq_locations_city_state`` keeps holding one row
        for one city and this file decides what that row is called.

        Enabled regions only — a disabled metro must not pull new records in.
        """
        wanted_city = _normalize_city(city)
        wanted_state = state.strip().casefold()
        for entry in self.cities:
            if entry.state.casefold() != wanted_state:
                continue
            if any(_normalize_city(name) == wanted_city for name in entry.names):
                return entry
        return None

    def metro_for(self, city: str, state: str) -> str | None:
        entry = self.lookup(city, state)
        return entry.metro if entry else None

    def lookup_city_name(self, city: str) -> RegionCity | None:
        """The configured city of that name — or of that alias — across enabled regions, when
        exactly one state has it; ``None`` when none does **and** when more than one does.

        A source that gives a bare city name and no state (a seed entry, a listing carrying only
        a city) is genuinely ambiguous once the config spans several states, because two of them
        can configure the same name, and there is no honest tie-break: picking the first
        configured match files the company under the wrong metro, which then scopes what SPEC §8
        will merge it with. Ambiguity resolves to ``None`` and the caller skips the record. A
        caller that does have a state must use :meth:`lookup`, which is exact.

        Aliases are searched alongside canonical names, so a bare name a source abbreviates is
        answered exactly as the spelled-out one is — including its ambiguity, since the loader
        holds an alias to the same one-city-per-spelling rule within a state but lets two states
        configure the same spelling, just as they may configure the same city name.
        """
        wanted = _normalize_city(city)
        matches = [
            entry
            for entry in self.cities
            if any(_normalize_city(name) == wanted for name in entry.names)
        ]
        # One distinct state, or nothing: zero matches and two states are both "cannot answer".
        if len({entry.state.casefold() for entry in matches}) != 1:
            return None
        return matches[0]

    @property
    def cities_longest_first(self) -> tuple[RegionCity, ...]:
        """:attr:`cities`, longest normalised city name first, file order among equal lengths —
        each city once, under its canonical name only.

        For a caller that wants the *cities* in an order where a longer configured name is never
        shadowed by a shorter one it contains as a whole-word substring (the shipped file has
        such a pair): taking the first match in file order instead would file every posting from
        the longer-named city under the shorter-named one, in the wrong state and the wrong
        metro. ``sorted`` is stable, so names of equal length keep file order and the result
        never depends on the sort's internals.

        **A scanner over free text wants** :attr:`city_needles_longest_first` **instead**, which
        offers a city's aliases as well and orders by the length of the spelling actually
        searched for. This property cannot serve that job — it carries one string per city, so
        every alias in the file would go unmatched — and it is named for the cities rather than
        for the search, which is why the two are separate rather than one overloaded sequence.
        """
        return tuple(sorted(self.cities, key=lambda entry: -len(_normalize_city(entry.city))))

    @property
    def city_needles_longest_first(self) -> tuple[CityNeedle, ...]:
        """Every spelling of every enabled city — canonical names *and* aliases — paired with
        the city it resolves to, longest needle first, file order among equal lengths.

        The sequence a scanner looking for a place inside free text must walk, and the reason it
        is needles rather than cities (:attr:`cities_longest_first`, which carries canonical
        names only): a city answers to several spellings, and each of them is a string to search
        for, so a sequence of cities leaves the scanner nothing to match an alias with.

        Ordering on the *needle* rather than on the city's name is what keeps the anti-shadowing
        rule true once aliases exist. An alias may be longer or shorter than its own city's name
        and than any other city's, so ordering by the city's name would let a short name shadow
        a longer alias — the exact failure the ordering exists to prevent, arriving through the
        very mechanism added to fix a related one. ``sorted`` is stable, so equal-length needles
        keep file order and a city's canonical name is met before its own aliases.
        """
        return tuple(
            sorted(
                (
                    CityNeedle(needle=name, city=entry)
                    for entry in self.cities
                    for name in entry.names
                ),
                key=lambda found: -len(_normalize_city(found.needle)),
            )
        )


#: LinkedIn's cap on the note attached to a connection request (SPEC §12 Phase 8). A draft one
#: character over is not a slightly long draft: LinkedIn refuses to send it, so the door this
#: whole tier exists to open is shut at the moment of use.
LINKEDIN_NOTE_LIMIT = 300

#: The pair of names the LinkedIn note is measured against on load. A template has no length
#: until its placeholders are filled, and the file cannot be checked against every company in
#: the database, so it is checked against one representative pair: a two-word company name and
#: a full personal name, both at the long end of what a corpus of small startups holds. This is
#: a budget check and not a guarantee — a longer real name still has to be clamped by whatever
#: renders the note — and what it buys is that a template fitting *nobody* fails here, where the
#: message can name the key and the overshoot, rather than at the moment of pasting.
_SAMPLE_COMPANY = "Northwind Robotics"
_SAMPLE_PERSON = "Alexandra Fitzgerald"

#: The placeholders each template may use. An email draft is built from a company row alone, so
#: it has nobody to name; a LinkedIn note is drafted for one person and may name both.
_EMAIL_PLACEHOLDERS = frozenset({"company"})
_LINKEDIN_PLACEHOLDERS = frozenset({"company", "person"})

#: How ``outreach.yaml``'s nested blocks map onto :class:`OutreachConfig`'s flat fields — see
#: :meth:`OutreachConfig._flatten_template_blocks`.
_TEMPLATE_BLOCKS = {
    "email": {"subject": "email_subject", "body": "email_body"},
    "linkedin": {"note": "linkedin_note"},
}

#: The same mapping read the other way, so a failure can quote the key the *file* spells rather
#: than the field the model calls it. Every other message about a template already says
#: ``email.subject``; a reader who is told ``email_subject`` has to translate before they can find
#: the line to edit, and there is no reason to make them.
_FILE_KEY_FOR_FIELD = {
    field: f"{block}.{key}"
    for block, keys in _TEMPLATE_BLOCKS.items()
    for key, field in keys.items()
}


def _placeholders(template: str, field: str) -> frozenset[str]:
    """The ``{name}`` fields ``template`` uses; ``ValueError`` naming ``field`` if it is not a
    template :meth:`str.format` could render.

    Parsed with :class:`string.Formatter` rather than matched with a regular expression, because
    that is the parser ``format`` itself will use at render time: a doubled ``{{`` is literal
    text to both, and an unmatched brace raises here instead of in a request handler. Positional
    fields (``{}``, ``{0}``) are rejected outright rather than reported as unknown names — the
    panel formats by keyword only, so a positional field is an ``IndexError`` waiting for the
    first company it is rendered for, and its "name" is not something a message can quote.
    """
    try:
        parsed = tuple(Formatter().parse(template))
    except ValueError as exc:  # a stray '{' or '}' in the file
        msg = f"{field} is not a valid template: {exc}"
        raise ValueError(msg) from exc
    names: set[str] = set()
    for _text, name, _spec, _conversion in parsed:
        if name is None:  # trailing literal text, no field
            continue
        if not name or name.isdigit():
            msg = (
                f"{field} uses the positional placeholder {'{' + name + '}'}; "
                "placeholders are filled in by name, so write them by name"
            )
            raise ValueError(msg)
        names.add(name)
    return frozenset(names)


class OutreachConfig(BaseModel):
    """``config/outreach.yaml``: the drafts the company panel offers, and the two lists that
    decide who it offers them to (SPEC §6, §12 Phase 8).

    The file nests (``email:`` with ``subject`` and ``body``) because that is how the pitch reads
    as a document; the model is flat because ``email_subject`` is the name the templating filters
    and the panel's door-picker are written against. :meth:`_flatten_template_blocks` is the
    whole of that translation, and it is the only place the two shapes are related.

    Nothing here has a default. A pitch, a priority order or a guard list carried in Python would
    be a second place the behaviour is defined — the one nobody thinks to edit — which is the
    same mistake CLAUDE.md rules out for classifier keywords. An omitted key fails on load; the
    two guard lists may be written empty, which switches their guard off in the open.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: Subject line of the ``mailto:`` draft (``{company}``).
    email_subject: str
    #: Body of the same draft (``{company}``). Kept short in the shipped file because the whole
    #: ``mailto:`` URL is percent-encoded and mail clients truncate a long one.
    email_body: str
    #: The connection-request note (``{company}``, ``{person}``), held to
    #: :data:`LINKEDIN_NOTE_LIMIT` characters once filled in.
    linkedin_note: str
    #: Who to ask for, best door first. Typed as the enum so a value that is not a
    #: ``Person.role_type`` fails on load naming the permitted set, and because a
    #: :class:`~enum.StrEnum` member still compares and hashes as its own string, a caller may
    #: match it against either a stored ``role_type`` or a plain string.
    contact_priority: tuple[enums.RoleType, ...] = Field(min_length=1)
    #: Local parts that mark an address as a shared inbox rather than a person's own. A set:
    #: membership is the only question ever asked of it, and order would mean nothing.
    role_address_prefixes: frozenset[str]
    #: Words that prove a stored "person" is a legal entity — an SEC Form D related-persons list
    #: yields fund and partnership names classified as founders, and the panel must never draft
    #: a note to one. A tuple, because a caller scans them in file order.
    entity_markers: tuple[str, ...]

    @model_validator(mode="before")
    @classmethod
    def _flatten_template_blocks(cls, data: Any) -> Any:
        """Fold the file's ``email:`` and ``linkedin:`` blocks into the flat template fields.

        Unknown sub-keys are rejected here by hand because ``extra="forbid"`` cannot see inside a
        block this validator has already consumed: left alone, ``email: {subjekt: ...}`` would
        fail as "email_subject: Field required", naming a key the file does not contain and
        sending the reader looking for the wrong thing. Naming ``email.subjekt`` is the whole
        point of validating on load.
        """
        if not isinstance(data, dict):
            return data
        data = dict(data)
        for block, keys in _TEMPLATE_BLOCKS.items():
            nested = data.pop(block, None)
            if nested is None:
                continue
            if not isinstance(nested, dict):
                msg = f"{block} must be a mapping of {', '.join(keys)}, got {nested!r}"
                raise ValueError(msg)
            for key, value in nested.items():
                field = keys.get(key)
                if field is None:
                    msg = f"unknown key {block}.{key}; {block} takes: {', '.join(sorted(keys))}"
                    raise ValueError(msg)
                data[field] = value
        return data

    @field_validator("email_subject", "email_body", "linkedin_note")
    @classmethod
    def _non_blank(cls, value: str, info: ValidationInfo) -> str:
        """Trimmed, and required to say something.

        The trim is what makes the length check below honest. A block scalar written ``|``
        instead of ``|-`` keeps a trailing newline and a quoted string keeps whatever padding
        was typed around it; neither is text anyone reads, and both count against LinkedIn's
        300. Blank is the other half: an empty subject or note is a draft that ships as a door
        with nothing written on it.

        The failure names the key as the file writes it (:data:`_FILE_KEY_FOR_FIELD`), because
        that is the line the reader has to go and fix.
        """
        field = info.field_name or ""
        return _require_text(value, _FILE_KEY_FOR_FIELD.get(field, field))

    @field_validator("contact_priority")
    @classmethod
    def _each_role_at_most_once(
        cls, value: tuple[enums.RoleType, ...]
    ) -> tuple[enums.RoleType, ...]:
        """A role may be listed once. The list is an order, so a second mention can never be
        reached, and a repeat is always a typo for a role that was meant to be there instead —
        silently ignoring it would leave that role's people unreachable in the panel."""
        seen: set[enums.RoleType] = set()
        for role in value:
            if role in seen:
                msg = f"contact_priority lists {role.value!r} twice; it is an order, not a set"
                raise ValueError(msg)
            seen.add(role)
        return value

    @field_validator("role_address_prefixes")
    @classmethod
    def _bare_local_parts(cls, value: frozenset[str]) -> frozenset[str]:
        """Each prefix trimmed and case-folded, and required to be a local part on its own.

        ``info@`` is how this is mistyped, and the failure it causes is invisible: a prefix is
        compared against the text before the ``@`` of an already-lower-cased address, so a stored
        ``@`` or a stray capital simply never matches, and the shared-inbox hint quietly stops
        appearing for that one prefix. A hint that is missing looks exactly like a hint that was
        not warranted, so the mistake is caught here instead. Case is folded rather than
        rejected, since only the ``@`` is a real error.
        """
        cleaned: set[str] = set()
        for prefix in value:
            text = _require_text(prefix, "role_address_prefixes").casefold()
            if "@" in text or any(character.isspace() for character in text):
                msg = (
                    f"role address prefix {prefix!r} must be the local part on its own, "
                    "without an @ or whitespace"
                )
                raise ValueError(msg)
            cleaned.add(text)
        return frozenset(cleaned)

    @field_validator("entity_markers")
    @classmethod
    def _non_blank_markers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Each marker trimmed and required to be non-empty.

        Blank is the part that matters: an empty marker is contained in every name, so every
        named person in the database would be read as a legal entity and every company would
        fall back to the company-wide search — the guard would silently delete the two tiers it
        exists to protect.
        """
        return tuple(_require_text(marker, "entity_markers") for marker in value)

    @model_validator(mode="after")
    def _reject_undraftable_templates(self) -> OutreachConfig:
        """Two rules a pitch file must satisfy, checked once on load rather than once per render.

        *Every placeholder is one the renderer will fill.* ``str.format`` raises ``KeyError`` for
        a name it was not given, and the only place these templates are ever formatted is while
        serving a page — so ``{firstname}`` written for ``{person}`` would turn one expanded
        company row into a 500 rather than into an obviously wrong draft. The vocabulary is short
        and fixed, so the honest place to catch the typo is here, naming both the key and the
        placeholder.

        *The LinkedIn note fits.* 300 characters is LinkedIn's cap on a connection-request note
        and it is measured on the note as sent, so a template is only as short as its filled-in
        form; it is measured here against :data:`_SAMPLE_COMPANY` and :data:`_SAMPLE_PERSON`. The
        message gives the overshoot, because "too long" without a number means editing and
        reloading until it stops failing.
        """
        for field, template, allowed in (
            ("email.subject", self.email_subject, _EMAIL_PLACEHOLDERS),
            ("email.body", self.email_body, _EMAIL_PLACEHOLDERS),
            ("linkedin.note", self.linkedin_note, _LINKEDIN_PLACEHOLDERS),
        ):
            unknown = _placeholders(template, field) - allowed
            if unknown:
                offered = ", ".join(sorted(f"{{{name}}}" for name in allowed))
                named = ", ".join(sorted(f"{{{name}}}" for name in unknown))
                msg = f"{field} uses {named}, which nothing fills in; it may use: {offered}"
                raise ValueError(msg)

        drafted = self.linkedin_note.format(company=_SAMPLE_COMPANY, person=_SAMPLE_PERSON)
        if len(drafted) > LINKEDIN_NOTE_LIMIT:
            over = len(drafted) - LINKEDIN_NOTE_LIMIT
            msg = (
                f"linkedin.note is {over} character{'' if over == 1 else 's'} too long: it "
                f"renders to {len(drafted)} characters for a company named {_SAMPLE_COMPANY!r} "
                f"and a person named {_SAMPLE_PERSON!r}, and LinkedIn caps a connection-request "
                f"note at {LINKEDIN_NOTE_LIMIT}"
            )
            raise ValueError(msg)
        return self


def _load_yaml(path: Path) -> Any:
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@cache
def load_connectors_config(path: Path = CONNECTORS_YAML) -> ConnectorsConfig:
    return ConnectorsConfig.model_validate(_load_yaml(path))


@cache
def load_regions_config(path: Path = REGIONS_YAML) -> RegionsConfig:
    """Parse and validate a region file. Cached on ``path``, since a connector, the pipeline and
    the CLI all ask for the same file and the answer is immutable.

    The cache is keyed on the path only, so a caller loading a *different* file per case — the
    alternate-config tests — needs a unique path each time, or ``cache_clear()``.
    """
    return RegionsConfig.model_validate(_load_yaml(path))


@cache
def load_outreach_config(path: Path = OUTREACH_YAML) -> OutreachConfig:
    """Parse and validate the outreach pitch (SPEC §6, §12 Phase 8). Cached on ``path`` like the
    two loaders above, and the cache earns more here than for either of them: this file is read
    while rendering a company row, so without it every expanded row would re-read and re-validate
    YAML inside a request handler.

    The cache is keyed on the path only, so a caller loading a *different* file per case needs a
    unique path each time, or ``cache_clear()``. Editing the shipped file therefore takes effect
    on the next process start, not on the next page load — the pitch is iterated between sessions
    of writing, and a per-request stat is not worth paying on every row.
    """
    return OutreachConfig.model_validate(_load_yaml(path))
