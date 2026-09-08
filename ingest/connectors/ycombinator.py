"""Y Combinator company directory connector (SPEC §4 Tier 1 #2).

YC publishes a public company index that its own directory page searches through Algolia. It
yields exactly what SPEC §4 promises — one-liner, batch, sector tags, website, team size — plus
``all_locations``, ``stage`` and ``status``, heavily concentrated in a handful of metros.

How a run works
---------------
1. :meth:`YCombinatorConnector.credentials` fetches ``https://www.ycombinator.com/companies``
   and reads the Algolia application id and **public search key** out of the page. The key is
   *not* hard-coded on purpose: it rotates, and the value published by older clients now
   answers ``403``. ``www.ycombinator.com/robots.txt`` allows ``/companies`` (only
   ``/companies?*``, the faceted query strings, is disallowed) — the shared client checks it.
2. One query reads the ``batch`` facet, then :meth:`fetch` issues **one query per batch**. The
   index caps ``page``-based pagination at :data:`PAGINATION_LIMIT` hits, so a broad query such
   as the name of a large configured city (3 141 hits for the largest when the fixtures were
   recorded) can never be walked; the largest single batch is a few hundred companies, so a
   per-batch query always can. Fifty-odd requests a week is the whole cost.
3. Every hit whose ``all_locations`` resolves to a city in ``config/regions.yaml`` becomes a
   :class:`~ingest.base.CompanyRecord`; the rest are counted and dropped.

**Work at a Startup is not ingested.** SPEC §4 says "where accessible"; its listings sit behind
a login, and this project neither authenticates to nor scrapes gated pages (SPEC §4). The
``isHiring`` flag is kept in the record's sectors-adjacent data instead, and the ATS connectors
supply the actual roles.
"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict, Field

from db import enums
from ingest.base import CompanyRecord, Connector, FetchContext, LocationRecord
from ingest.config import ConnectorConfig, RegionsConfig
from ingest.http import RobotsDisallowed
from logging_config import get_logger

log = get_logger(__name__)

#: The public directory page, whose markup carries the current Algolia credentials.
DIRECTORY_URL = "https://www.ycombinator.com/companies"
#: Algolia's multi-query endpoint for an application id.
QUERY_URL_TEMPLATE = "https://{app_id}-dsn.algolia.net/1/indexes/*/queries"
#: Algolia's default ``paginationLimitedTo``: no query can page past this many hits, which is
#: why the index is walked batch by batch.
PAGINATION_LIMIT = 1000

#: ``"app":"45BWZJ1SGC","key":"…"`` as the directory page embeds it.
_CREDENTIALS = re.compile(
    r'"(?:app|appId|applicationId)"\s*:\s*"(?P<app_id>[A-Z0-9]{6,32})"\s*,\s*'
    r'"(?:key|apiKey|searchApiKey)"\s*:\s*"(?P<key>[A-Za-z0-9+/=]{20,})"'
)
#: ``all_locations`` is a comma-separated trail: ``"<City>, ST, USA"``, sometimes several
#: offices joined by ``";"``. No metro or city is named here on purpose (SPEC §12 Phase 7):
#: ``config/regions.yaml`` decides which of them this connector keeps.
_LOCATION_SPLIT = re.compile(r"\s*;\s*")

#: YC's ``status`` values mapped onto ``Company.status`` / ``Company.stage`` (SPEC §5).
_STATUS: dict[str, enums.CompanyStatus] = {
    "active": enums.CompanyStatus.ACTIVE,
    "public": enums.CompanyStatus.ACTIVE,
    "acquired": enums.CompanyStatus.ACQUIRED,
    "inactive": enums.CompanyStatus.DEAD,
}
#: Only the stages YC actually asserts. "Early" is deliberately absent: it says nothing about
#: which round a company has raised, and leaving ``stage`` unset lets ``sec_edgar`` — which
#: outranks this connector (SPEC §8) — set it from a real Form D.
_STAGE: dict[str, enums.Stage] = {
    "public": enums.Stage.PUBLIC,
    "growth": enums.Stage.GROWTH,
}


class YCombinatorOptions(BaseModel):
    """``options`` for ``ycombinator`` in ``config/connectors.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    index_name: str = "YCCompany_production"
    hits_per_page: int = Field(default=1000, ge=1, le=PAGINATION_LIMIT)
    #: Facet the index is partitioned by. Every value must hold fewer than
    #: :data:`PAGINATION_LIMIT` companies or its hits cannot all be read.
    partition_facet: str = "batch"
    max_facet_values: int = Field(default=500, ge=1)
    #: Fall back to these credentials when the directory page cannot be read. Left empty by
    #: default: a stale key answers 403 and a silent empty run is worse than a loud failure.
    app_id: str | None = None
    api_key: str | None = None


@dataclass(frozen=True, slots=True)
class YCCredentials:
    app_id: str
    api_key: str


@dataclass(frozen=True, slots=True)
class YCCompany:
    """One Algolia hit plus the batch whose query produced it."""

    batch: str
    hit: dict[str, Any]


def parse_credentials(markup: str) -> YCCredentials | None:
    """The Algolia application id and public search key embedded in the directory page."""
    match = _CREDENTIALS.search(markup)
    if match is None:
        return None
    return YCCredentials(app_id=match.group("app_id"), api_key=match.group("key"))


def split_locations(value: object) -> list[tuple[str, str]]:
    """``"<City>, ST, USA"`` → ``[("<City>", "ST")]``.

    Only the ``city, state`` prefix matters; the country tail and any extra components are
    dropped. Entries that are not a ``city, state`` pair (``"Remote"``) yield nothing.
    """
    if not isinstance(value, str) or not value.strip():
        return []
    out: list[tuple[str, str]] = []
    for entry in _LOCATION_SPLIT.split(value):
        parts = [part.strip() for part in entry.split(",") if part.strip()]
        if len(parts) >= 2:
            out.append((parts[0], parts[1]))
    return out


class YCombinatorConnector(Connector[YCCompany]):
    """The YC directory (SPEC §4 Tier 1 #2, §12 Phase 3)."""

    name: ClassVar[str] = "ycombinator"

    def __init__(self, config: ConnectorConfig, regions: RegionsConfig) -> None:
        super().__init__(config, regions)
        self.options = YCombinatorOptions.model_validate(config.options)
        self.skipped: dict[str, int] = {}

    # ------------------------------------------------------------------ fetch

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[YCCompany]:
        """Read the credentials, enumerate the batches, and yield every hit."""
        self.skipped = {}
        credentials = await self.credentials(ctx)
        batches = await self.batches(ctx, credentials)
        ctx.log.info("ycombinator.start", app_id=credentials.app_id, batches=len(batches))
        yielded = 0
        for batch in batches:
            hits = await self._query_batch(ctx, credentials, batch)
            if hits is None:
                continue
            if len(hits) >= self.options.hits_per_page:
                problem = (
                    f"batch {batch!r} returned {len(hits)} hits, at the page size — companies "
                    "past it were not read (partition the index more finely)"
                )
                ctx.problems.append(problem)
                ctx.log.warning("ycombinator.page_full", batch=batch, hits=len(hits))
            for hit in hits:
                yielded += 1
                yield YCCompany(batch=batch, hit=hit)
        ctx.log.info(
            "ycombinator.summary",
            batches=len(batches),
            yielded=yielded,
            skipped=dict(self.skipped),
            requests=ctx.http.stats.requests,
        )

    async def credentials(self, ctx: FetchContext) -> YCCredentials:
        """The current Algolia credentials, read from the public directory page.

        Falls back to ``options.app_id``/``options.api_key`` when the page cannot be read or
        no longer embeds them; with neither, the run fails loudly rather than reporting an
        empty directory.
        """
        try:
            markup = await ctx.http.get_text(DIRECTORY_URL)
        except (httpx.HTTPStatusError, httpx.TransportError, RobotsDisallowed) as exc:
            ctx.log.warning("ycombinator.directory_failed", url=DIRECTORY_URL, error=str(exc))
            return self._configured_credentials(f"{DIRECTORY_URL} could not be fetched: {exc}")
        found = parse_credentials(markup)
        if found is None:
            return self._configured_credentials(
                f"{DIRECTORY_URL} no longer embeds an Algolia application id and search key"
            )
        return found

    def _configured_credentials(self, why: str) -> YCCredentials:
        if self.options.app_id and self.options.api_key:
            log.info("ycombinator.credentials_from_config", reason=why)
            return YCCredentials(self.options.app_id, self.options.api_key)
        msg = (
            f"ycombinator cannot reach the Algolia index: {why}, and no app_id/api_key is set "
            "in config/connectors.yaml"
        )
        raise RuntimeError(msg)

    async def batches(self, ctx: FetchContext, credentials: YCCredentials) -> list[str]:
        """Every value of the partition facet, largest first.

        Largest first because a facet value that has outgrown :data:`PAGINATION_LIMIT` is the
        thing an operator must know about, and reporting it early beats discovering it after
        fifty quiet queries.
        """
        params = (
            f"query=&hitsPerPage=0&page=0&facets=%5B%22{quote(self.options.partition_facet)}%22%5D"
            f"&maxValuesPerFacet={self.options.max_facet_values}"
        )
        result = await self._query(ctx, credentials, params)
        facets = result.get("facets") if isinstance(result, dict) else None
        values = facets.get(self.options.partition_facet) if isinstance(facets, dict) else None
        if not isinstance(values, dict) or not values:
            msg = (
                f"the {self.options.index_name} index returned no "
                f"{self.options.partition_facet!r} facet values to partition by"
            )
            raise RuntimeError(msg)
        counts = {str(name): int(count) for name, count in values.items()}
        for name, count in counts.items():
            if count > PAGINATION_LIMIT:
                problem = (
                    f"{self.options.partition_facet} {name!r} holds {count} companies, past "
                    f"Algolia's pagination limit of {PAGINATION_LIMIT}; some were not read"
                )
                ctx.problems.append(problem)
                ctx.log.warning("ycombinator.partition_too_large", value=name, count=count)
        return [name for name, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]

    async def _query_batch(
        self, ctx: FetchContext, credentials: YCCredentials, batch: str
    ) -> list[dict[str, Any]] | None:
        facet = f'[["{self.options.partition_facet}:{batch}"]]'
        params = (
            f"query=&hitsPerPage={self.options.hits_per_page}&page=0"
            f"&facetFilters={quote(facet, safe='')}"
        )
        try:
            result = await self._query(ctx, credentials, params)
        except (httpx.HTTPStatusError, httpx.TransportError, RobotsDisallowed) as exc:
            ctx.problems.append(f"batch {batch!r} could not be queried: {exc}")
            ctx.log.warning("ycombinator.batch_failed", batch=batch, error=str(exc))
            return None
        hits = result.get("hits") if isinstance(result, dict) else None
        return [hit for hit in hits if isinstance(hit, dict)] if isinstance(hits, list) else []

    async def _query(
        self, ctx: FetchContext, credentials: YCCredentials, params: str
    ) -> dict[str, Any]:
        """One Algolia multi-query, returning its single result block.

        The credentials travel as headers, never in the URL: the shared client caches by URL
        and the search key would otherwise end up in the on-disk cache file name (SPEC §4's
        24 h ETag cache).
        """
        url = QUERY_URL_TEMPLATE.format(app_id=credentials.app_id)
        body = json.dumps({"requests": [{"indexName": self.options.index_name, "params": params}]})
        payload = await ctx.http.post_json(
            url,
            content=body,
            headers={
                "X-Algolia-Application-Id": credentials.app_id,
                "X-Algolia-API-Key": credentials.api_key,
                "Content-Type": "application/json",
            },
        )
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list) or not results or not isinstance(results[0], dict):
            msg = f"unexpected Algolia response for {params!r}: {type(payload).__name__}"
            raise RuntimeError(msg)
        block: dict[str, Any] = results[0]
        return block

    # ------------------------------------------------------------------ to_records

    def to_records(self, raw: YCCompany) -> Iterable[CompanyRecord]:
        """One record per in-region company (SPEC §4's field list), or nothing.

        A hit is dropped, with a counter, when it has no usable name or when none of its
        ``all_locations`` entries is a city in ``config/regions.yaml`` — the index is national
        and this project is regional (SPEC §1). Everything else is mapped: ``website`` becomes
        the canonical domain, ``one_liner`` and ``long_description`` the two description
        fields, ``team_size`` the headcount estimate, and the batch, industries and tags become
        sectors.
        """
        hit = raw.hit
        name = hit.get("name")
        if not isinstance(name, str) or not name.strip():
            return self._skip("no_name", raw)
        locations = self._locations(hit.get("all_locations"))
        if not locations:
            return self._skip("outside_region", raw, all_locations=hit.get("all_locations"))

        status_value = str(hit.get("status") or "").strip().casefold()
        return [
            CompanyRecord(
                name=name.strip(),
                external_id=_text(hit.get("slug")) or _text(hit.get("objectID")),
                source_url=_profile_url(hit),
                domain=_text(hit.get("website")),
                website_url=_text(hit.get("website")),
                one_liner=_text(hit.get("one_liner")),
                thesis=_text(hit.get("long_description")),
                employee_est=_team_size(hit.get("team_size")),
                stage=_STAGE.get(status_value) or _STAGE.get(_stage_key(hit)),
                status=_STATUS.get(status_value),
                locations=locations,
                sectors=self._sectors(raw),
            )
        ]

    def _locations(self, value: object) -> tuple[LocationRecord, ...]:
        """The configured cities among a hit's ``all_locations``, first one flagged as HQ.

        YC lists offices most-important-first, so the first configured city is the best
        available guess at the headquarters; ``config/regions.yaml`` supplies the state, metro
        and canonical spelling.
        """
        records: list[LocationRecord] = []
        for city, state in split_locations(value):
            configured = self.regions.lookup(city, state)
            if configured is None:
                continue
            records.append(
                LocationRecord(
                    city=configured.city,
                    state=configured.state,
                    country=configured.country,
                    metro=configured.metro,
                    is_hq=not records,
                )
            )
        return tuple(records)

    def _sectors(self, raw: YCCompany) -> tuple[str, ...]:
        """Industries, sub-industry tail and tags, plus the YC batch as ``"YC W24"``.

        The batch is a sector in the loosest sense but it is the single most useful chip on a
        YC company row, and ``Sector`` is the only free-text taxonomy the model has (SPEC §5).
        """
        hit = raw.hit
        values: list[str] = []
        for key in ("industries", "tags"):
            entries = hit.get(key)
            if isinstance(entries, list):
                values.extend(entry.strip() for entry in entries if isinstance(entry, str))
        industry = _text(hit.get("industry"))
        if industry:
            values.append(industry)
        subindustry = _text(hit.get("subindustry"))
        if subindustry and "->" in subindustry:
            values.append(subindustry.split("->")[-1].strip())
        batch = _batch_label(raw.batch)
        if batch:
            values.append(batch)
        return tuple(dict.fromkeys(value for value in values if value))

    def _skip(self, reason: str, raw: YCCompany, **details: object) -> tuple[CompanyRecord, ...]:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1
        log.info(
            "ycombinator.skip",
            reason=reason,
            slug=raw.hit.get("slug"),
            name=raw.hit.get("name"),
            **details,
        )
        return ()


def _text(value: object) -> str | None:
    return value.strip() or None if isinstance(value, str) else None


def _team_size(value: object) -> int | None:
    """``team_size`` is an int in the index but ``null`` or a string on older records."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _stage_key(hit: dict[str, Any]) -> str:
    return str(hit.get("stage") or "").strip().casefold()


def _profile_url(hit: dict[str, Any]) -> str | None:
    slug = _text(hit.get("slug"))
    return f"{DIRECTORY_URL}/{slug}" if slug else DIRECTORY_URL


def _batch_label(batch: str) -> str | None:
    """``"Winter 2024"`` → ``"YC W24"``; anything unexpected is passed through as ``"YC …"``."""
    parts = batch.split()
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 4:
        season = parts[0][:1].upper()
        return f"YC {season}{parts[1][2:]}"
    return f"YC {batch}".strip() or None


def batch_names(counts: Sequence[tuple[str, int]]) -> list[str]:
    """Facet values ordered largest first — exposed for tests."""
    return [name for name, _ in sorted(counts, key=lambda kv: (-kv[1], kv[0]))]
