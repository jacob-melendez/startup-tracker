"""Shared skeleton for the four applicant-tracking-system connectors (SPEC §4 Tier 1 #3).

Greenhouse, Lever, Ashby and Workable all publish the same thing behind different shapes: given
a **board token**, one public JSON document lists every open role at that company. That is the
highest-value source for the actual goal of this project, so the four connectors differ only in
their endpoint and their field names — everything else lives here:

* **Which companies to visit.** These connectors enrich rather than discover: they set
  ``wants_targets``, so :func:`ingest.pipeline.run_connector` hands them the companies already
  in the database (:class:`~ingest.base.CompanyTarget`) and no connector opens a session.
* **Board-token discovery** (SPEC §4: "Board tokens are discovered by fetching a company's
  ``/careers`` page and regex-matching the embedded board URL. Persist the discovered token on
  the company row so discovery runs once, and re-run discovery only if the board 404s").
  :func:`discover_board` matches **every** provider's board URL, not just the caller's — a
  careers page answers the question for all four at once, so whichever ATS connector runs first
  writes the token and the other three simply use it (see :meth:`AtsConnector._discover`).
* **Re-discovery on 404.** A stored token whose board answers 404 is re-discovered inside the
  same run; the fresh token overwrites the stale one because the same connector owns the field
  (SPEC §8). Nothing is ever cleared to ``NULL``, because a ``None`` in a record means
  "unknown" and would be ignored by the merge.
* **Every record is ``enrich_only``.** An ATS board says nothing about where a company is or
  who it is; it must never create one. If entity resolution finds nothing, the pipeline skips
  the record (SPEC §4 Tier 2 rules applied here for the same reason).
* **``jobs_complete=True``.** The endpoint *is* the whole board, so a job that has vanished
  from it gets ``closed_at`` — never a delete (SPEC §2, §5).

Etiquette. Discovery fetches an arbitrary company's careers page, which is a Tier-3 fetch, so
the ATS connectors are configured at one request per host per two seconds and ``respect_robots:
true`` in ``config/connectors.yaml`` — the strictest thing a connector touches sets its limit.
Discovery reads at most :data:`MAX_DISCOVERY_PAGES` pages per company, well inside Tier 3's cap
of five pages per domain per run, and ``options.max_discovery_per_run`` bounds how many
companies a single run may probe at all.
"""

from __future__ import annotations

import re
from abc import abstractmethod
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, ClassVar
from urllib.parse import urljoin

import httpx
from pydantic import BaseModel, ConfigDict, Field

from db import enums
from ingest.base import CompanyRecord, CompanyTarget, Connector, FetchContext, JobRecord
from ingest.classify import classify
from ingest.config import ConnectorConfig, RegionsConfig
from ingest.htmlutil import html_to_text
from ingest.http import HostBudgetExceeded, RobotsDisallowed
from logging_config import get_logger

log = get_logger(__name__)

#: Pages one company may serve during discovery in one run (the home page plus the first
#: careers-ish path that answers). SPEC §4 Tier 3 allows five per domain per run.
MAX_DISCOVERY_PAGES = 3
#: How much of a job description is stored. Whole postings run to tens of kilobytes of
#: boilerplate; this is enough for the ``jobs`` tsvector (SPEC §5) and the expanded row (§9).
DESCRIPTION_LIMIT = 20_000

# Path segments that appear where a token would be but are part of the ATS's own URL space.
_RESERVED_TOKENS = frozenset(
    {"embed", "j", "jobs", "job", "api", "v0", "v1", "posting-api", "job-board", "job_board", ""}
)

#: The board URL each provider publishes, as it appears on a company's careers page. Recorded
#: examples: ``jobs.lever.co/atomcomputing`` (Atom Computing), ``job-boards.greenhouse.io/
#: sourcegraph91`` (Sourcegraph), ``boards.greenhouse.io/embed/job_board?for=…`` (Astranis),
#: ``jobs.ashbyhq.com/Linear`` (Linear, note the capital) — see ``tests/fixtures/careers/``.
BOARD_PATTERNS: dict[enums.AtsProvider, tuple[re.Pattern[str], ...]] = {
    enums.AtsProvider.GREENHOUSE: (
        re.compile(
            r"greenhouse\.io/(?:embed/)?job_?board(?:/js)?\?[^\"'\s]*\bfor=([A-Za-z0-9_-]+)"
        ),
        re.compile(r"(?:job-)?boards\.greenhouse\.io/(?:embed/)?([A-Za-z0-9_-]+)"),
        re.compile(r"boards-api\.greenhouse\.io/v1/boards/([A-Za-z0-9_-]+)"),
    ),
    enums.AtsProvider.LEVER: (
        re.compile(r"jobs\.(?:eu\.)?lever\.co/([A-Za-z0-9_-]+)"),
        re.compile(r"api\.lever\.co/v0/postings/([A-Za-z0-9_-]+)"),
    ),
    enums.AtsProvider.ASHBY: (
        re.compile(r"jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)"),
        re.compile(r"api\.ashbyhq\.com/posting-api/job-board/([A-Za-z0-9_-]+)"),
    ),
    enums.AtsProvider.WORKABLE: (
        re.compile(r"apply\.workable\.com/(?!j/)([A-Za-z0-9_-]+)"),
        re.compile(r"([A-Za-z0-9_-]+)\.workable\.com"),
    ),
}


class AtsOptions(BaseModel):
    """``options`` for an ATS connector in ``config/connectors.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: Companies whose careers page may be probed for a board token in one run. Discovery costs
    #: a fetch of somebody else's site, so it is rationed; the pipeline hands the targets over
    #: least-recently-visited first, so successive runs work through the whole database.
    max_discovery_per_run: int = Field(default=15, ge=0)
    #: Paths tried, in order, on the company's own site (SPEC §4: "fetching a company's
    #: /careers page"). The home page is tried first because a footer link often names the
    #: board directly.
    discovery_paths: tuple[str, ...] = ("/careers", "/jobs", "/careers/", "/company/careers")
    #: Fetch the home page before the careers paths.
    discover_from_home_page: bool = True


# --------------------------------------------------------------------------------- raw item


@dataclass(frozen=True, slots=True)
class AtsResult:
    """One company's outcome for one ATS run.

    Exactly one of these is true:

    * ``payload`` is set — the board was fetched and its jobs are in it;
    * ``token`` is set with no ``payload`` — a careers page named a board belonging to a
      *different* provider, so the token is recorded for that provider's connector to use;
    * neither — the company was probed and no board was found. The record is still emitted, with
      no fields at all, so a ``company_sources`` row marks the attempt and the ordering of
      :func:`db.queries.load_company_targets` moves the company to the back of the queue.
    """

    target: CompanyTarget
    provider: enums.AtsProvider | None = None
    token: str | None = None
    board_url: str | None = None
    payload: Any = None
    discovered: bool = False
    notes: dict[str, Any] = field(default_factory=dict)


# ------------------------------------------------------------------------- board discovery


def _clean_token(raw: str) -> str | None:
    token = raw.strip().strip("/")
    if not token or token.casefold() in _RESERVED_TOKENS or len(token) > 100:
        return None
    return token


def discover_board(markup: str) -> tuple[enums.AtsProvider, str] | None:
    """The first ATS board a page links to, as ``(provider, token)`` — or ``None``.

    Providers are tried in :data:`BOARD_PATTERNS` order and the first hit wins. Matching is
    done on the raw markup rather than on parsed ``href``s on purpose: recorded careers pages
    put the board URL inside an entity-escaped JSON attribute (Sentry) or a JavaScript embed
    snippet (Astranis) as often as in a plain link, and the host name survives escaping either
    way. Tokens that are really part of the ATS's own URL space (``/embed/``, ``/j/``) are
    rejected by :data:`_RESERVED_TOKENS`, so ``apply.workable.com/j/B76C11B977`` — a link to a
    single posting — never becomes a board token.
    """
    for provider, patterns in BOARD_PATTERNS.items():
        for pattern in patterns:
            for match in pattern.finditer(markup):
                token = _clean_token(match.group(1))
                if token is not None:
                    return provider, token
    return None


# --------------------------------------------------------------------------- the connector


class AtsConnector(Connector[AtsResult]):
    """Base class for ``greenhouse``, ``lever``, ``ashby`` and ``workable``."""

    #: The provider this connector owns; also the value written to ``Company.ats_provider``.
    provider: ClassVar[enums.AtsProvider]
    #: ``str.format`` template taking ``token``.
    board_url_template: ClassVar[str]
    wants_targets: ClassVar[bool] = True

    def __init__(self, config: ConnectorConfig, regions: RegionsConfig) -> None:
        super().__init__(config, regions)
        self.options = AtsOptions.model_validate(config.options)
        self.stats: dict[str, int] = {}

    # ------------------------------------------------------------------ subclass contract

    @abstractmethod
    def parse_jobs(self, payload: Any, token: str) -> list[JobRecord]:
        """Map one board payload to job records. Pure: no I/O, no database."""

    def board_url(self, token: str) -> str:
        return self.board_url_template.format(token=token)

    def board_has_content(self, payload: Any) -> bool:
        """Whether a 200 answer really is this company's board.

        Overridden by ``workable``, whose widget API answers ``200`` with an empty ``jobs``
        list for account names that were never Workable customers, so an empty board there is
        not proof of anything.
        """
        return payload is not None

    # ------------------------------------------------------------------ fetch

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[AtsResult]:
        """Walk the pre-loaded targets: fetch known boards, discover unknown ones.

        A company is this connector's business when it has no ``ats_provider`` at all (nobody
        has discovered its board yet) or when its provider is ours. A company that belongs to
        another provider is skipped without a request.
        """
        self.stats = {}
        discoveries_left = self.options.max_discovery_per_run
        ctx.log.info(
            "ats.start",
            provider=self.provider.value,
            targets=len(ctx.targets),
            discovery_budget=discoveries_left,
        )
        for target in ctx.targets:
            if target.ats_provider is not None and target.ats_provider is not self.provider:
                self._count("skipped_other_provider")
                continue
            if target.ats_provider is self.provider and target.ats_token:
                result = await self._fetch_known_board(ctx, target, discoveries_left)
                if result is not None and result.discovered:
                    discoveries_left -= 1
                if result is not None:
                    yield result
                continue
            if target.ats_provider is None:
                if discoveries_left <= 0:
                    self._count("discovery_budget_spent")
                    continue
                discoveries_left -= 1
                result = await self._discover(ctx, target)
                if result is not None:
                    yield result
        ctx.log.info(
            "ats.summary",
            provider=self.provider.value,
            outcomes=dict(self.stats),
            requests=ctx.http.stats.requests,
            problems=len(ctx.problems),
        )

    async def _fetch_known_board(
        self, ctx: FetchContext, target: CompanyTarget, discoveries_left: int
    ) -> AtsResult | None:
        """Fetch a company's stored board; on a 404, re-discover it (SPEC §4)."""
        token = target.ats_token or ""
        payload, status = await self._get_board(ctx, target, token)
        if payload is not None:
            self._count("board_fetched")
            return AtsResult(
                target=target,
                provider=self.provider,
                token=token,
                board_url=self.board_url(token),
                payload=payload,
            )
        if status == httpx.codes.NOT_FOUND and discoveries_left > 0:
            # SPEC §4: "re-run discovery only if the board 404s".
            self._count("board_gone")
            ctx.log.info(
                "ats.board_gone", company=target.name, token=token, provider=self.provider.value
            )
            return await self._discover(ctx, target, rediscovery=True)
        if status == httpx.codes.NOT_FOUND:
            self._count("board_gone_no_budget")
            ctx.problems.append(
                f"{target.name}: {self.provider.value} board {token!r} is gone (404) and the "
                "discovery budget for this run is spent"
            )
            return None
        # The board answered but carried nothing we recognise as a board. Deliberately *not*
        # the 404 path: re-discovering on this would re-probe the same careers page nightly.
        self._count("board_empty" if status == httpx.codes.OK else "board_failed")
        return None

    async def _get_board(
        self, ctx: FetchContext, target: CompanyTarget, token: str
    ) -> tuple[Any | None, int | None]:
        """``(payload, status)``; ``payload`` is ``None`` unless the board answered with one."""
        url = self.board_url(token)
        try:
            payload = await ctx.http.get_json(url)
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status != httpx.codes.NOT_FOUND:
                ctx.problems.append(f"{target.name}: {url} failed: HTTP {status}")
                ctx.log.warning("ats.board_error", company=target.name, url=url, status=status)
            return None, status
        except (httpx.TransportError, RobotsDisallowed, HostBudgetExceeded) as exc:
            ctx.problems.append(f"{target.name}: {url} failed: {type(exc).__name__}: {exc}")
            ctx.log.warning("ats.board_error", company=target.name, url=url, error=str(exc))
            return None, None
        except ValueError as exc:  # not JSON — Workable answers 404 with the text "Not Found"
            ctx.problems.append(f"{target.name}: {url} did not return JSON: {exc}")
            return None, None
        if not self.board_has_content(payload):
            # The request succeeded, so the status is OK; the caller decides what an answer
            # with no board in it means (Workable's shell accounts — see ``workable.py``).
            ctx.log.info("ats.board_empty", company=target.name, url=url, token=token)
            return None, httpx.codes.OK
        return payload, httpx.codes.OK

    async def _discover(
        self, ctx: FetchContext, target: CompanyTarget, *, rediscovery: bool = False
    ) -> AtsResult | None:
        """Fetch the company's own pages and regex-match an embedded board URL (SPEC §4).

        Whatever provider turns up is returned: a Lever board found by the Greenhouse run is
        still worth persisting, and next morning Lever's own run uses it without fetching the
        careers page again. When the found board is ours it is fetched immediately, so a
        newly discovered company yields its jobs in the very same run.
        """
        found = await self._scan_pages(ctx, target)
        if found is None:
            self._count("no_board_found")
            ctx.log.info("ats.no_board", company=target.name, domain=target.domain)
            # Still emit an empty record: it writes a ``company_sources`` row, which is what
            # moves this company to the back of the discovery queue on the next run.
            return AtsResult(target=target, discovered=True, notes={"outcome": "no_board_found"})
        provider, token = found
        if provider is not self.provider:
            self._count("token_for_other_provider")
            ctx.log.info(
                "ats.token_for_other_provider",
                company=target.name,
                provider=provider.value,
                token=token,
            )
            return AtsResult(target=target, provider=provider, token=token, discovered=True)

        payload, _status = await self._get_board(ctx, target, token)
        self._count("discovered_rediscovered" if rediscovery else "discovered")
        return AtsResult(
            target=target,
            provider=provider,
            token=token,
            board_url=self.board_url(token),
            payload=payload,
            discovered=True,
        )

    async def _scan_pages(
        self, ctx: FetchContext, target: CompanyTarget
    ) -> tuple[enums.AtsProvider, str] | None:
        """Try the company's home page and careers paths, newest information first."""
        home = target.home_url
        if home is None:
            return None
        urls: list[str] = []
        if self.options.discover_from_home_page:
            urls.append(home)
        urls.extend(urljoin(home, path) for path in self.options.discovery_paths)
        seen: set[str] = set()
        for url in urls[:MAX_DISCOVERY_PAGES]:
            if url in seen:
                continue
            seen.add(url)
            try:
                markup = await ctx.http.get_text(url)
            except (
                httpx.HTTPStatusError,
                httpx.TransportError,
                RobotsDisallowed,
                HostBudgetExceeded,
            ) as exc:
                ctx.log.debug(
                    "ats.discovery_fetch_failed", company=target.name, url=url, error=str(exc)
                )
                continue
            found = discover_board(markup)
            if found is not None:
                ctx.log.info(
                    "ats.board_discovered",
                    company=target.name,
                    url=url,
                    provider=found[0].value,
                    token=found[1],
                )
                return found
        return None

    def _count(self, key: str) -> None:
        self.stats[key] = self.stats.get(key, 0) + 1

    # ------------------------------------------------------------------ to_records

    def to_records(self, raw: AtsResult) -> Iterable[CompanyRecord]:
        """One ``enrich_only`` record per company (see the module docstring).

        ``jobs_complete`` is set only when a board was actually fetched: a token-only record
        must not close the jobs another connector reported, and a "nothing found" record has
        no job list to compare against at all.
        """
        jobs: tuple[JobRecord, ...] = ()
        if raw.payload is not None and raw.token:
            jobs = tuple(self.parse_jobs(raw.payload, raw.token))
        return [
            CompanyRecord(
                name=raw.target.name,
                # Only *our* provider's token identifies a company to *this* connector.
                external_id=raw.token if raw.provider is self.provider else None,
                source_url=raw.board_url,
                domain=raw.target.domain,
                ats_provider=raw.provider,
                ats_token=raw.token,
                jobs=jobs,
                jobs_complete=raw.payload is not None,
                enrich_only=True,
            )
        ]

    # ------------------------------------------------------------------ shared helpers

    def _job(
        self,
        *,
        external_id: str,
        title: str,
        url: str | None,
        location_text: str | None,
        is_remote: bool,
        posted_at: datetime | None,
        description: str | None,
        compensation_raw: str | None,
        employment_type_hint: str | None,
        payload: dict[str, Any],
    ) -> JobRecord:
        """Build one :class:`~ingest.base.JobRecord`, classified by ``ingest/classify.py``.

        Every role is ingested whatever the classifier says (SPEC §7.1): an unmatched title
        becomes ``role_family='other'`` and is stored and displayed like any other.
        """
        result = classify(title, description=description, employment_type_hint=employment_type_hint)
        return JobRecord(
            external_id=external_id,
            title=title,
            url=url,
            location_text=location_text,
            is_remote=is_remote,
            compensation_raw=compensation_raw,
            posted_at=posted_at,
            description_raw=description,
            raw_payload=payload,
            **result.as_job_fields(),
        )


# ------------------------------------------------------------------------- shared parsing


def parse_iso_datetime(value: object) -> datetime | None:
    """An ISO-8601 timestamp as an aware UTC datetime, or ``None``.

    ``JobRecord.posted_at`` rejects a naive value (asyncpg would store it as local time), so a
    timestamp without an offset — Ashby occasionally emits one — is read as UTC, which is what
    every one of these APIs means by it.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def parse_iso_date(value: object) -> datetime | None:
    """A date-only string (``"2026-07-10"``, Workable) as midnight UTC."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        day = date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


def parse_epoch_millis(value: object) -> datetime | None:
    """Milliseconds since the epoch (Lever ``createdAt``) as an aware UTC datetime."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def join_locations(values: Iterable[object]) -> str | None:
    """De-duplicated, order-preserving ``"A; B"`` for the location lists these APIs return."""
    seen: dict[str, None] = {}
    for value in values:
        if isinstance(value, str) and value.strip():
            seen.setdefault(value.strip(), None)
    return "; ".join(seen) or None


def description_text(*candidates: object) -> str | None:
    """The first non-empty candidate as plain text, capped at :data:`DESCRIPTION_LIMIT`."""
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            text = html_to_text(candidate, limit=DESCRIPTION_LIMIT)
            if text:
                return text
    return None


def format_salary_range(salary: object) -> str | None:
    """Lever's ``salaryRange`` object as one readable line (``"USD 75,000–100,000 per year"``)."""
    if not isinstance(salary, dict):
        return None
    minimum, maximum = salary.get("min"), salary.get("max")
    if not isinstance(minimum, int | float) or not isinstance(maximum, int | float):
        return None
    currency = str(salary.get("currency") or "").strip()
    interval = str(salary.get("interval") or "").strip().replace("-", " ")
    parts = [part for part in (currency, f"{minimum:,.0f}–{maximum:,.0f}", interval) if part]
    return " ".join(parts) or None


def jobs_list(payload: Any, key: str | None = "jobs") -> Sequence[dict[str, Any]]:
    """The job dicts inside a board payload: ``payload[key]`` when ``key`` is given (Greenhouse,
    Ashby, Workable), otherwise ``payload`` itself (Lever returns a bare list)."""
    block = payload.get(key) if key is not None and isinstance(payload, dict) else payload
    if not isinstance(block, list):
        return ()
    return [item for item in block if isinstance(item, dict)]
