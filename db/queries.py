"""The few genuinely complex statements (SPEC §3 "keep those in ``db/queries.py`` with a
docstring explaining the shape").

Ordinary reads and writes use ORM ``select()`` in place; what lives here is set-based SQL that
the ingest pipeline (:mod:`ingest.pipeline`) runs at well-defined points of a run:

* :func:`refresh_company_denormalized_columns` — the end-of-run recompute of
  ``Company.latest_round_id`` / ``open_job_count`` / ``latest_job_posted_at`` (SPEC §5);
* :func:`last_successful_run_started_at` — the incremental-window anchor (SPEC §7.2, §14.3);
* :func:`close_missing_jobs` — marks jobs that vanished from their source as closed, never
  deleting them (SPEC §2, §5);
* :func:`load_company_targets` — the pre-loaded work list for the connectors that enrich
  companies already in the database instead of discovering new ones (SPEC §4 Tier 1 #3,
  Tier 3).

Phase 4 (SPEC §9) adds the browse layer the web app reads:

* :func:`company_list_page` and :func:`job_list_page` — the filtered, keyset-paginated company
  and role lists. Their composition strategy (one predicate list, two ``LEFT JOIN``s, a single
  ``EXISTS`` for the role filters, search as an ``OR`` arm, keyset instead of ``OFFSET``) is
  spelled out at length in :func:`company_list_page`'s docstring;
* :func:`company_card_extras` — sectors, locations and the open-role-by-family breakdown for a
  whole rendered page at once, so the templates never issue a query per row;
* :func:`company_jobs` — every role of one company, deliberately unfiltered by anything the
  *page* asked for (SPEC §9's expanded row shows all roles regardless of the page-level
  filter), ordered and narrowed only by the in-row controls of that same sentence;
* :func:`facet_cities`, :func:`facet_sectors`, :func:`last_successful_runs` — the small lookups
  the filter form and ``/runs`` need.

None of it fetches anything: a web request handler only ever reads the local database (SPEC §2).
"""

from __future__ import annotations

import base64
import calendar
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any, Literal, cast

from sqlalchemy import (
    ColumnElement,
    CursorResult,
    Integer,
    Select,
    Text,
    and_,
    any_,
    bindparam,
    exists,
    false,
    func,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import BindParameter

from db import enums
from db.models import (
    Company,
    CompanyLocation,
    CompanySector,
    CompanySource,
    FetchRun,
    FundingRound,
    Job,
    JobBookmark,
    Location,
    Sector,
    Source,
    UserNote,
)
from ingest.base import CompanyTarget

#: ``FetchRun`` statuses whose fetch scanned its whole window (SPEC §7.2). A ``partial`` run
#: had per-item trouble but did finish scanning, so the next incremental window may start
#: from it; an ``error`` run stopped early and must not anchor a window (design brief step 1).
COMPLETED_RUN_STATUSES = (enums.FetchRunStatus.OK, enums.FetchRunStatus.PARTIAL)


async def refresh_company_denormalized_columns(
    session: AsyncSession, company_ids: Sequence[int] | None
) -> int:
    """Recompute the three denormalized ``companies`` columns of SPEC §5 in one statement.

    Why the pipeline calls this instead of database triggers (SPEC §5 "maintained by the
    ingest pipeline at end-of-run, not by triggers"): the pipeline is the only writer of jobs
    and rounds and it knows when a run is complete, so one set-based recompute at the end of
    the run replaces thousands of per-row trigger firings, keeps the write path visible in
    Python, and lets a partial run be reasoned about (the columns describe committed state,
    never a half-applied batch). Triggers would also have to be maintained in the migrations,
    outside the model's view.

    Shape — a single ``UPDATE companies SET ... `` whose three values are correlated scalar
    subqueries over the company being updated (design decision 4):

    * ``open_job_count`` = ``count(*)`` of the company's jobs with ``closed_at IS NULL``;
    * ``latest_job_posted_at`` = ``max(coalesce(posted_at, first_seen_at))`` over those same
      open jobs — ``first_seen_at`` stands in for sources that publish no posting date, so a
      job still counts as recent from the day we first saw it (it powers the default sort,
      SPEC §9);
    * ``latest_round_id`` = the id of the round with the greatest ``announced_date``
      (``NULLS LAST``; ties and undated rounds resolved by the highest id, i.e. the most
      recently ingested).

    A company with no open jobs / no rounds gets ``0`` / ``NULL`` / ``NULL``.

    ``company_ids=None`` recomputes every company; a sequence limits the statement to those
    ids (the ones a run touched). An empty sequence is a no-op. Returns the number of
    company rows updated.
    """
    if company_ids is not None and len(company_ids) == 0:
        return 0

    open_jobs = and_(Job.company_id == Company.id, Job.closed_at.is_(None))
    open_job_count = (
        select(func.count()).select_from(Job).where(open_jobs).correlate(Company).scalar_subquery()
    )
    latest_job_posted_at = (
        select(func.max(func.coalesce(Job.posted_at, Job.first_seen_at)))
        .select_from(Job)
        .where(open_jobs)
        .correlate(Company)
        .scalar_subquery()
    )
    latest_round_id = (
        select(FundingRound.id)
        .where(FundingRound.company_id == Company.id)
        .order_by(FundingRound.announced_date.desc().nulls_last(), FundingRound.id.desc())
        .limit(1)
        .correlate(Company)
        .scalar_subquery()
    )
    stmt = update(Company).values(
        open_job_count=open_job_count,
        latest_job_posted_at=latest_job_posted_at,
        latest_round_id=latest_round_id,
    )
    if company_ids is not None:
        stmt = stmt.where(Company.id.in_(list(company_ids)))
    # ``AsyncSession.execute`` is typed as the generic ``Result``; an UPDATE always yields a
    # ``CursorResult`` whose ``rowcount`` is the number of rows matched.
    result = cast("CursorResult[Any]", await session.execute(stmt))
    updated: int = result.rowcount
    return updated


async def load_company_targets(
    session: AsyncSession, connector: str, *, statuses: Sequence[enums.CompanyStatus] | None = None
) -> list[CompanyTarget]:
    """The work list for a connector that *enriches* companies rather than discovering them
    (design decision 5): the four ATS connectors and ``company_site``.

    Shape — one ``SELECT`` over ``companies`` with an **outer join** to this connector's own
    ``company_sources`` row, so a company the connector has never visited still comes back
    (with ``external_id`` and ``last_seen_at`` NULL) instead of being filtered out by an inner
    join. Ordering is ``last_seen_at ASC NULLS FIRST, id`` — never-visited companies first,
    then the least recently visited — which is what makes a per-run cap (``company_site``
    visits at most N companies a night, SPEC §4 Tier 3) round-robin the whole database instead
    of hammering the same first N every time.

    ``statuses`` defaults to ``active`` only: there is no point fetching the careers page of a
    company the seed loader marked ``dead`` (SPEC §10). Pass an explicit sequence to widen it.

    This lives here rather than in a connector because SPEC §11 and ``ingest/base.py`` keep
    connectors free of database access: :func:`ingest.pipeline.run_connector` runs this once
    per run and hands the result over in ``FetchContext.targets``.
    """
    wanted = tuple(statuses) if statuses is not None else (enums.CompanyStatus.ACTIVE,)
    source = aliased(CompanySource)
    rows = await session.execute(
        select(
            Company.id,
            Company.name,
            Company.domain,
            Company.website_url,
            Company.status,
            Company.ats_provider,
            Company.ats_token,
            source.external_id,
            source.last_seen_at,
        )
        .outerjoin(
            source,
            and_(source.company_id == Company.id, source.connector == connector),
        )
        .where(Company.status.in_(wanted))
        .order_by(source.last_seen_at.asc().nulls_first(), Company.id)
    )
    return [
        CompanyTarget(
            company_id=row.id,
            name=row.name,
            domain=row.domain,
            website_url=row.website_url,
            status=enums.CompanyStatus(row.status),
            ats_provider=(
                enums.AtsProvider(row.ats_provider) if row.ats_provider is not None else None
            ),
            ats_token=row.ats_token,
            external_id=row.external_id,
            last_seen_at=row.last_seen_at,
        )
        for row in rows
    ]


async def last_successful_run_started_at(session: AsyncSession, connector: str) -> datetime | None:
    """``started_at`` of the newest ``FetchRun`` for ``connector`` whose fetch completed —
    ``status IN (ok, partial)`` (:data:`COMPLETED_RUN_STATUSES`) — or ``None`` when there is
    none yet.

    This is the anchor of a connector's incremental window (SPEC §14.3 "then daily
    incremental"): a ``partial`` run scanned everything and merely had per-item trouble, so
    the window may advance past it; an ``error`` run did not finish scanning and is skipped —
    otherwise one 404ed filing would force the 18-month backfill on every run.
    """
    started_at: datetime | None = await session.scalar(
        select(func.max(FetchRun.started_at)).where(
            FetchRun.connector == connector, FetchRun.status.in_(COMPLETED_RUN_STATUSES)
        )
    )
    return started_at


async def close_missing_jobs(
    session: AsyncSession,
    *,
    company_id: int,
    connector: str,
    keep_external_ids: Iterable[str],
    now: datetime,
) -> int:
    """Set ``closed_at = now`` on ``connector``'s open jobs at ``company_id`` that are not in
    ``keep_external_ids`` (SPEC §2, §5: a job missing from its source is closed, never
    deleted — the database is the historical record).

    "This connector's jobs" are found by joining ``sources.connector`` through
    ``jobs.source_id`` (design decision 5), so a Greenhouse listing never closes a job that an
    HN comment reported. Jobs already closed are left alone (their original ``closed_at``
    stands). Returns the number of jobs closed.
    """
    keep = list(keep_external_ids)
    stmt = (
        update(Job)
        .where(
            Job.company_id == company_id,
            Job.closed_at.is_(None),
            exists().where(Source.id == Job.source_id, Source.connector == connector),
        )
        .values(closed_at=now)
    )
    if keep:
        stmt = stmt.where(Job.external_id.not_in(keep))
    result = cast("CursorResult[Any]", await session.execute(stmt))
    closed: int = result.rowcount
    return closed


# ========================================================= Phase 4 — the browse layer (SPEC §9)

#: Rows per page (SPEC §9 "Pagination, 50 per page").
PAGE_SIZE = 50
#: Upper bound on an explicit ``limit``. Keyset pagination is cheap, but a hand-edited URL
#: should not be able to ask for the whole table in one statement.
MAX_PAGE_SIZE = 200


class CompanySort(StrEnum):
    """Sort options for ``/`` (SPEC §9). Not a Postgres enum — a UI/query vocabulary that is
    also the wire format of the ``sort=`` query parameter and of a cursor's ``s`` field."""

    #: The default: companies that posted a role most recently float to the top.
    RECENT_JOB = "recent_job"
    FUNDING_DATE = "funding_date"
    AMOUNT = "amount"
    NEWEST = "newest"
    OPEN_ROLES = "open_roles"
    NAME = "name"


class JobSort(StrEnum):
    """Sort options for ``/roles`` (SPEC §9: "default sorted by ``posted_at DESC``")."""

    POSTED = "posted"
    FIRST_SEEN = "first_seen"
    COMPANY = "company"


class RoleSort(StrEnum):
    """Sort options for the roles table *inside* one expanded company row (SPEC §9's
    "sortable and filterable within the row").

    A vocabulary of its own rather than a reuse of :class:`JobSort`, because the two lists ask
    different questions. This one is exactly the columns that table renders, so every member
    names a header the user can click; ``JobSort.COMPANY`` would sort one company's roles by
    the one value all of them share, and ``first_seen`` is not a column of it at all.

    Direction is deliberately *not* folded into the members (no ``title_desc``): it is a
    second parameter, so that clicking a header twice reverses it and the vocabulary stays one
    member per column. :func:`role_sort_descending` gives each column the direction a reader
    expects when they first click it.
    """

    TITLE = "title"
    FAMILY = "family"
    TYPE = "type"
    SENIORITY = "seniority"
    LOCATION = "location"
    #: The default, and the one column that reads newest-first.
    POSTED = "posted"


@dataclass(frozen=True, slots=True, kw_only=True)
class Filters:
    """Every SPEC §9 filter, in one immutable object shared by ``/`` and ``/roles``.

    One object serves both views because the two statements differ only in what they select
    and order by — the filters mean the same thing in both, and factoring them here is what
    keeps "the same filter row" on two pages from drifting apart.

    Everything defaults to "unset", and an unset filter contributes no predicate at all. That
    is the mechanical guarantee behind SPEC §7.1's rule that classification never excludes:
    the no-filter case is a plain ordered scan, so every role family, every employment type
    and every ``flexible_signal`` value is in the default view.
    """

    q: str | None = None
    cities: tuple[str, ...] = ()
    sector_slugs: tuple[str, ...] = ()
    stages: tuple[enums.Stage, ...] = ()
    round_types: tuple[enums.RoundType, ...] = ()
    amount_min_usd: int | None = None
    amount_max_usd: int | None = None
    round_within_months: int | None = None
    role_families: tuple[enums.RoleFamily, ...] = ()
    employment_types: tuple[enums.EmploymentType, ...] = ()
    seniorities: tuple[enums.Seniority, ...] = ()
    flexible_only: bool = False
    has_open_roles: bool = False
    tracking_statuses: tuple[enums.TrackingStatus, ...] = ()
    #: ``/roles`` only, and never on by default — a closed job is history, not a listing.
    include_closed_jobs: bool = False

    @property
    def has_job_filters(self) -> bool:
        """True when at least one filter constrains an individual *role*.

        The company list only pays for its ``EXISTS`` subquery when this is true.
        """
        return bool(
            self.role_families or self.employment_types or self.seniorities or self.flexible_only
        )

    @property
    def is_empty(self) -> bool:
        """True when nothing is filtered — the default view."""
        return self == Filters()


@dataclass(frozen=True, slots=True, kw_only=True)
class LocationRef:
    """One of a company's cities, as the company row and detail page render it."""

    city: str
    state: str
    metro: str
    is_hq: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class CompanyRow:
    """One collapsed company row (SPEC §9). Flat by design: the templates read these names."""

    id: int
    name: str
    domain: str | None
    website_url: str | None
    one_liner: str | None
    stage: enums.Stage
    status: enums.CompanyStatus
    open_job_count: int
    latest_job_posted_at: datetime | None
    first_seen_at: datetime
    latest_round_type: enums.RoundType | None
    latest_round_amount_usd: int | None
    latest_round_announced_date: date | None
    #: ``None`` when the company has no ``user_notes`` row at all — distinct from an explicit
    #: :attr:`~db.enums.TrackingStatus.NONE`, and the collapsed row leans on the distinction:
    #: a saved note the user never gave a status to still leaves ``TrackingStatus.NONE`` here,
    #: and that is what earns the row its "Noted" pill (``_company_row.html``).
    tracking_status: enums.TrackingStatus | None
    rating: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class JobRow:
    """One role, denormalized with its company (``/roles`` and the expanded company row)."""

    id: int
    company_id: int
    company_name: str
    company_domain: str | None
    title: str
    url: str | None
    location_text: str | None
    is_remote: bool
    employment_type: enums.EmploymentType
    role_family: enums.RoleFamily
    seniority: enums.Seniority
    flexible_signal: bool
    compensation_raw: str | None
    posted_at: datetime | None
    first_seen_at: datetime
    closed_at: datetime | None
    #: False when the job has no ``job_bookmarks`` row.
    starred: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class CompanyExtras:
    """Per-company display data fetched for one page of rows, not per row (no N+1)."""

    sectors: tuple[str, ...] = ()
    locations: tuple[LocationRef, ...] = ()
    #: ``(family, open role count)``, most roles first — the §9 "compact breakdown".
    open_roles_by_family: tuple[tuple[enums.RoleFamily, int], ...] = ()

    @property
    def hq_city(self) -> str | None:
        """The headquarters city if one is flagged, else any city, else ``None``."""
        for location in self.locations:
            if location.is_hq:
                return location.city
        return self.locations[0].city if self.locations else None


@dataclass(frozen=True, slots=True)
class Page[RowT]:
    """One page of rows plus the token that fetches the next one.

    There is no total count on purpose — see :func:`company_list_page`.
    """

    rows: tuple[RowT, ...]
    #: An encoded :class:`Cursor`, or ``None`` when this is the last page.
    next_cursor: str | None
    limit: int


# ------------------------------------------------------------------------ the keyset cursor


class CursorError(ValueError):
    """A pagination token that cannot be trusted — malformed, or minted for another sort.

    The web layer turns this into a 400: a hand-edited or stale ``cursor=`` must fail loudly
    rather than silently return a page from the wrong ordering.
    """


#: The widths of the columns a cursor field is bound against. Every id in SPEC §5 is an
#: ``Identity()`` integer and ``companies.open_job_count`` is an ``integer``, while
#: ``funding_rounds.amount_usd`` is a ``BIGINT``. A number outside its column's range is not
#: "a row far down the list": asyncpg refuses to bind it and raises ``DataError`` from inside
#: the statement, which would turn a hand-edited token into a 500 instead of a 400.
_INT32_MAX = 2_147_483_647
_INT64_MAX = 9_223_372_036_854_775_807


def _check_bindable(key: str, token: str) -> None:
    """Refuse a string key Postgres cannot be handed at all.

    JSON carries two characters a URL-encoded query parameter cannot, and that a ``text`` bind
    parameter rejects: U+0000, which asyncpg answers with ``CharacterNotInRepertoireError``,
    and a lone surrogate, which has no UTF-8 encoding. Both would raise from *inside* the
    keyset statement — a bare 500 for a hand-edited token that every other malformed cursor
    answers with the 400 page — so they are refused here, where the rest of the key validation
    lives, rather than in the two string sorts that would trip over them.

    Nothing is lost by refusing: a ``text`` column cannot hold either character either, so no
    key the app ever mints can contain one. ``web.filters._text`` strips the same NUL out of
    the search box; the cursor is the one externally supplied string that arrives as JSON, so
    it is the one that can carry both.
    """
    if "\x00" in key:
        raise CursorError(f"cursor key contains a NUL character: {token!r}")
    try:
        key.encode()
    except UnicodeEncodeError as exc:
        raise CursorError(f"cursor key is not encodable text: {token!r}") from exc


@dataclass(frozen=True, slots=True)
class Cursor:
    """The position of the last row of a page: its sort-column value and its id.

    ``key`` is JSON-safe — a ``datetime``/``date`` travels as its ISO string and is parsed back
    with the sort's declared key type, so the round trip never loses a microsecond or a
    timezone. ``id`` is the tiebreak that makes the ordering total.

    The token is base64 of a compact JSON object rather than an opaque database offset because
    it is stateless: nothing is stored server-side, and it is self-describing enough to be
    validated (``s`` pins the sort it was minted for).
    """

    sort: str
    key: str | int | None
    id: int

    def encode(self) -> str:
        payload = json.dumps({"s": self.sort, "k": self.key, "i": self.id}, separators=(",", ":"))
        # Strip '=' padding: it survives a query string but reads badly and invites truncation
        # by well-meaning URL cleaners. ``decode`` restores it.
        return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")

    @classmethod
    def decode(cls, token: str) -> Cursor:
        """Parse a token, raising :class:`CursorError` on anything malformed.

        Deliberately strict — unknown or missing keys are rejected rather than defaulted — but
        it does *not* check that ``sort`` matches the request; that is the page function's job,
        because only the caller knows which sort was asked for.
        """
        try:
            padded = token + "=" * (-len(token) % 4)
            payload: object = json.loads(base64.urlsafe_b64decode(padded.encode()))
        except (ValueError, TypeError) as exc:  # binascii.Error and JSONDecodeError are ValueErrors
            raise CursorError(f"cursor is not a valid token: {token!r}") from exc
        if not isinstance(payload, dict) or set(payload) != {"s", "k", "i"}:
            raise CursorError(f"cursor payload must be an object with keys s/k/i: {token!r}")
        sort, key, row_id = payload["s"], payload["k"], payload["i"]
        # ``bool`` is an ``int`` subclass; a JSON ``true`` is not an id.
        if not isinstance(sort, str) or isinstance(row_id, bool) or not isinstance(row_id, int):
            raise CursorError(f"cursor has a bad sort or id: {token!r}")
        if not 0 <= row_id <= _INT32_MAX:
            raise CursorError(f"cursor id {row_id} is outside the range of a row id")
        if key is not None and (isinstance(key, bool) or not isinstance(key, str | int)):
            raise CursorError(f"cursor has a bad key: {token!r}")
        if isinstance(key, str):
            _check_bindable(key, token)
        return cls(sort=sort, key=key, id=row_id)


# ------------------------------------------------------------- shared joins and predicates

#: SPEC §5's denormalized pointer, joined by name so the SQL reads as it does in the docstring.
_LATEST_ROUND = aliased(FundingRound, name="latest_round")


def _months_ago(today: date, months: int) -> date:
    """``today`` minus ``months`` calendar months, clamping the day to the target month.

    Calendar arithmetic, not ``timedelta(days=30 * months)``: "within the last 3 months" has to
    mean the same span in February as in July, and the ``round_within_months`` filter is
    compared against a ``date`` column. The day is clamped to the last valid day of the target
    month, so 31 Mar minus one month is 28 Feb (29 Feb in a leap year) rather than an error.
    """
    total = today.year * 12 + (today.month - 1) - months
    year, month_index = divmod(total, 12)
    month = month_index + 1
    return date(year, month, min(today.day, calendar.monthrange(year, month)[1]))


def _fts_match(column: Any, query: str) -> ColumnElement[bool]:
    """``column @@ websearch_to_tsquery('english', :query)`` against a generated ``tsvector``.

    ``websearch_to_tsquery`` (rather than ``to_tsquery``) because the input is whatever the
    user typed: it accepts quoted phrases, ``or`` and a leading ``-``, and never raises on
    punctuation the way ``to_tsquery`` does.

    The cast exists because SQLAlchemy types a custom operator as ``Operators``; ``@@`` really
    does yield a boolean, and the surrounding predicate lists are ``ColumnElement[bool]``.
    """
    return cast(
        "ColumnElement[bool]", column.bool_op("@@")(func.websearch_to_tsquery("english", query))
    )


def _trigram_match(column: Any, query: str) -> ColumnElement[bool]:
    """``column % :query`` — pg_trgm's similarity operator, the typo tolerance of SPEC §9.

    ``%`` is true when trigram similarity exceeds the session's ``pg_trgm.similarity_threshold``
    (default ``0.3``), and it is the form the ``gin_trgm_ops`` indexes on ``companies.name`` and
    ``companies.normalized_name`` can answer; ``similarity(a, b) > 0.3`` computes the same
    number but cannot use those indexes. See :func:`_fts_match` for the cast.
    """
    return cast("ColumnElement[bool]", column.bool_op("%")(query))


def _search_text(q: str | None) -> str | None:
    """The search term, or ``None`` when there is nothing to search for.

    A blank or whitespace-only ``q`` must add no predicate at all: ``websearch_to_tsquery
    ('english', '')`` is an empty tsquery that matches nothing, so treating "" as a search
    would empty the page the moment the user cleared the box.
    """
    if q is None:
        return None
    stripped = q.strip()
    return stripped or None


def _company_search_predicate(query: str) -> ColumnElement[bool]:
    """Full-text search over the company ``tsvector``, OR trigram similarity on the two names.

    One expression, not two queries — see :func:`company_list_page`.
    """
    return or_(
        _fts_match(Company.search_vector, query),
        _trigram_match(Company.name, query),
        _trigram_match(Company.normalized_name, query),
    )


def _job_search_predicate(query: str) -> ColumnElement[bool]:
    """Full-text search over the job ``tsvector``, OR trigram similarity on the company name.

    ``jobs.title`` has no trigram index (SPEC §5 only asks for them on ``companies``), so the
    fuzzy arm stays on ``companies.name``: job text gets FTS, the company name gets the typo
    tolerance, and no unindexed trigram scan over 20,000 job titles is introduced.
    """
    return or_(_fts_match(Job.search_vector, query), _trigram_match(Company.name, query))


def _job_attribute_predicates(filters: Filters) -> list[ColumnElement[bool]]:
    """The SPEC §9 filters that constrain a single ``jobs`` row.

    Shared verbatim by both readings of those filters: ``/roles`` applies them to the row it is
    listing, and ``/`` applies them inside :func:`_open_jobs_exist`'s correlated subquery. One
    definition, so "part-time" can never mean two different things on two pages.

    ``closed_at`` is *not* handled here — the company list always means open roles, while
    ``/roles`` exposes closed ones behind :attr:`Filters.include_closed_jobs`.
    """
    predicates: list[ColumnElement[bool]] = []
    if filters.role_families:
        predicates.append(Job.role_family.in_(filters.role_families))
    if filters.employment_types:
        predicates.append(Job.employment_type.in_(filters.employment_types))
    if filters.seniorities:
        predicates.append(Job.seniority.in_(filters.seniorities))
    if filters.flexible_only:
        # SPEC §7.1: opt-in only. Nothing else in this module ever looks at flexible_signal.
        predicates.append(Job.flexible_signal.is_(True))
    return predicates


def _open_jobs_exist(filters: Filters) -> ColumnElement[bool]:
    """``EXISTS`` one open job at this company satisfying *all* the role filters at once."""
    return (
        select(Job.id)
        .where(Job.company_id == Company.id, Job.closed_at.is_(None))
        .where(*_job_attribute_predicates(filters))
        .correlate(Company)
        .exists()
    )


def _company_predicates(filters: Filters, *, now: datetime) -> list[ColumnElement[bool]]:
    """Every SPEC §9 filter that constrains the *company*, in one list.

    Search is not here: the two pages search different columns (see
    :func:`_company_search_predicate` and :func:`_job_search_predicate`), so each page function
    appends its own arm. Everything else is identical on ``/`` and ``/roles``, which is why
    both call this.

    The predicates reference :data:`_LATEST_ROUND` and ``user_notes``; a caller must have
    outer-joined both, or Postgres would silently cross-join them in.
    """
    predicates: list[ColumnElement[bool]] = []
    if filters.cities:
        # Semi-join, not a join: an inner join through company_locations would return a
        # company once per matching city and break both the page size and the keyset order.
        predicates.append(
            select(Location.id)
            .join(CompanyLocation, CompanyLocation.location_id == Location.id)
            .where(CompanyLocation.company_id == Company.id, Location.city.in_(filters.cities))
            .correlate(Company)
            .exists()
        )
    if filters.sector_slugs:
        predicates.append(
            select(Sector.id)
            .join(CompanySector, CompanySector.sector_id == Sector.id)
            .where(CompanySector.company_id == Company.id, Sector.slug.in_(filters.sector_slugs))
            .correlate(Company)
            .exists()
        )
    if filters.stages:
        predicates.append(Company.stage.in_(filters.stages))
    if filters.round_types:
        predicates.append(_LATEST_ROUND.round_type.in_(filters.round_types))
    if filters.amount_min_usd is not None:
        predicates.append(_LATEST_ROUND.amount_usd >= filters.amount_min_usd)
    if filters.amount_max_usd is not None:
        predicates.append(_LATEST_ROUND.amount_usd <= filters.amount_max_usd)
    if filters.round_within_months is not None:
        predicates.append(
            _LATEST_ROUND.announced_date >= _months_ago(now.date(), filters.round_within_months)
        )
    if filters.has_open_roles:
        # The denormalized column, not an EXISTS — see company_list_page's docstring.
        predicates.append(Company.open_job_count > 0)
    if filters.tracking_statuses:
        arms: list[ColumnElement[bool]] = [UserNote.status.in_(filters.tracking_statuses)]
        if enums.TrackingStatus.NONE in filters.tracking_statuses:
            # A company nobody has touched has no user_notes row at all; "none" must mean both.
            arms.append(UserNote.company_id.is_(None))
        predicates.append(or_(*arms))
    return predicates


# ---------------------------------------------------------------------- sorting and keysets


@dataclass(frozen=True, slots=True)
class _SortSpec:
    """How one sort option orders rows, and how its cursor key survives a round trip."""

    #: ``Any`` on purpose: a mapped attribute, an attribute of an ``aliased()`` entity and
    #: ``func.lower(...)`` are three unrelated types to mypy but the same thing to SQL, and
    #: SQLAlchemy's ``InstrumentedAttribute`` is not a ``ColumnElement`` subclass in its stubs.
    column: Any
    descending: bool
    #: How a cursor key for this column is parsed back — and, for the two numeric widths, the
    #: range the column can actually hold (see :data:`_INT32_MAX`).
    key_type: Literal["datetime", "date", "int", "bigint", "str"]


#: SPEC §9's company sorts. Every one is ``<column> <dir> NULLS LAST, companies.id ASC``.
_COMPANY_SORTS: dict[CompanySort, _SortSpec] = {
    CompanySort.RECENT_JOB: _SortSpec(
        column=Company.latest_job_posted_at, descending=True, key_type="datetime"
    ),
    CompanySort.FUNDING_DATE: _SortSpec(
        column=_LATEST_ROUND.announced_date, descending=True, key_type="date"
    ),
    # ``amount_usd`` is a BIGINT; the other numeric sort column is a plain integer.
    CompanySort.AMOUNT: _SortSpec(
        column=_LATEST_ROUND.amount_usd, descending=True, key_type="bigint"
    ),
    CompanySort.NEWEST: _SortSpec(
        column=Company.first_seen_at, descending=True, key_type="datetime"
    ),
    CompanySort.OPEN_ROLES: _SortSpec(
        column=Company.open_job_count, descending=True, key_type="int"
    ),
    CompanySort.NAME: _SortSpec(column=func.lower(Company.name), descending=False, key_type="str"),
}

#: SPEC §9's role sorts, ``<column> <dir> NULLS LAST, jobs.id ASC``.
_JOB_SORTS: dict[JobSort, _SortSpec] = {
    JobSort.POSTED: _SortSpec(column=Job.posted_at, descending=True, key_type="datetime"),
    JobSort.FIRST_SEEN: _SortSpec(column=Job.first_seen_at, descending=True, key_type="datetime"),
    JobSort.COMPANY: _SortSpec(column=func.lower(Company.name), descending=False, key_type="str"),
}

#: The in-row roles table's own columns (:class:`RoleSort`). The three enum columns order by
#: the Postgres type's declaration order rather than alphabetically, which is what makes them
#: worth clicking: seniority runs intern → executive, employment type puts ``full_time`` first
#: and both put ``unknown``/``other`` last, exactly as the filter selects list them.
#: ``key_type`` is only ever read by the keyset predicate, and the panel mints no cursor — one
#: company's roles are a bounded list — but it is filled in honestly rather than left to lie.
_ROLE_SORTS: dict[RoleSort, _SortSpec] = {
    RoleSort.TITLE: _SortSpec(column=func.lower(Job.title), descending=False, key_type="str"),
    RoleSort.FAMILY: _SortSpec(column=Job.role_family, descending=False, key_type="str"),
    RoleSort.TYPE: _SortSpec(column=Job.employment_type, descending=False, key_type="str"),
    RoleSort.SENIORITY: _SortSpec(column=Job.seniority, descending=False, key_type="str"),
    RoleSort.LOCATION: _SortSpec(
        column=func.lower(Job.location_text), descending=False, key_type="str"
    ),
    RoleSort.POSTED: _SortSpec(column=Job.posted_at, descending=True, key_type="datetime"),
}


def role_sort_descending(sort: RoleSort) -> bool:
    """The direction one in-row column sorts in until the user flips it.

    ``posted`` reads newest-first — the order the panel has always rendered, and the order the
    collapsed row's "latest job" number implies — while the five columns of words read A→Z.
    The web layer needs this to build a header link that asks for *its own* column's natural
    direction instead of inheriting whatever the currently sorted column is using.
    """
    return _ROLE_SORTS[sort].descending


def _role_sort_spec(sort: RoleSort | JobSort) -> _SortSpec:
    """The ordering behind one in-row sort, from whichever vocabulary named it."""
    return _ROLE_SORTS[sort] if isinstance(sort, RoleSort) else _JOB_SORTS[sort]


def _order_by(spec: _SortSpec, id_column: Any) -> list[Any]:
    """``<sort column> <dir> NULLS LAST, <id> ASC`` — a *total* order.

    The id tiebreak is not decoration: without it, rows sharing a sort value have no defined
    relative order, so a keyset cursor sitting inside such a tie could skip or repeat them.
    """
    ordered = spec.column.desc() if spec.descending else spec.column.asc()
    return [ordered.nulls_last(), id_column.asc()]


def _after_cursor(spec: _SortSpec, cursor: Cursor, id_column: Any) -> ColumnElement[bool]:
    """Rows strictly after the cursor, in the total order :func:`_order_by` imposes.

    Two cases, because ``NULLS LAST`` splits the ordering in two:

    * the cursor key is non-NULL — everything past it is a smaller value (for a descending
      sort), the same value with a larger id, or anything in the NULL tail;
    * the cursor key is NULL — we are already inside the NULL tail, so only a larger id counts.
    """
    if cursor.key is None:
        return and_(spec.column.is_(None), id_column > cursor.id)
    key = _decode_sort_key(spec.key_type, cursor.key)
    beyond = spec.column < key if spec.descending else spec.column > key
    return or_(beyond, and_(spec.column == key, id_column > cursor.id), spec.column.is_(None))


def _encode_sort_key(value: Any) -> str | int | None:
    """The sort column's value as JSON: timestamps and dates become ISO strings."""
    if value is None:
        return None
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, int):
        return value
    return str(value)


def _decode_sort_key(key_type: str, key: str | int) -> datetime | date | int | str:
    """Inverse of :func:`_encode_sort_key`, driven by the sort's declared key type.

    A key that does not parse — or that no value of the column could ever be, such as a number
    wider than the column itself — is a hand-edited token, not a database problem, so it raises
    :class:`CursorError` and the web layer answers 400 rather than letting asyncpg raise
    ``DataError`` from inside the statement. The ``str`` branch needs no check of its own: the
    characters a ``text`` parameter cannot carry are refused by :func:`_check_bindable` in
    :meth:`Cursor.decode`, so that guarantee holds for every key rather than only for the two
    string sorts that reach here.
    """
    try:
        if key_type == "datetime":
            return datetime.fromisoformat(str(key))
        if key_type == "date":
            return date.fromisoformat(str(key))
        if key_type in {"int", "bigint"}:
            number = int(key)
            limit = _INT32_MAX if key_type == "int" else _INT64_MAX
            if not -limit - 1 <= number <= limit:
                msg = f"{number} is outside the range of the column"
                raise ValueError(msg)
            return number
    except (TypeError, ValueError) as exc:
        raise CursorError(f"cursor key {key!r} is not a valid {key_type}") from exc
    return str(key)


def _check_limit(limit: int) -> None:
    if not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}; got {limit}")


def _check_cursor(cursor: Cursor | None, sort: StrEnum) -> None:
    """A cursor minted under one sort is meaningless under another.

    Its key is a value of the *old* sort column, so applying it to a new ordering would return
    an arbitrary slice. Refusing is the only safe answer: the UI drops ``cursor=`` whenever the
    sort changes, so seeing one here means the URL was edited or bookmarked mid-change.
    """
    if cursor is not None and cursor.sort != sort.value:
        raise CursorError(f"cursor was minted for sort {cursor.sort!r}, not {sort.value!r}")


def _id_array(company_ids: Sequence[int]) -> BindParameter[Sequence[int]]:
    """The ids of one rendered page as a single ``int[]`` bind parameter.

    ``= ANY(:ids)`` rather than ``IN (:id1, :id2, ...)``: one bind parameter means one prepared
    statement whatever the page size, instead of asyncpg preparing a new one for every distinct
    number of ids.
    """
    return bindparam("company_ids", value=list(company_ids), type_=ARRAY(Integer))


def _job_select() -> Select[Any]:
    """``jobs`` joined to its company and (outer) to its bookmark — the shape of a
    :class:`JobRow`.

    Shared by :func:`job_list_page` and :func:`company_jobs` so both pages render a role
    identically; each adds its own joins, filters and ordering on top.
    """
    return (
        select(
            Job.id,
            Job.company_id,
            Company.name.label("company_name"),
            Company.domain.label("company_domain"),
            Job.title,
            Job.url,
            Job.location_text,
            Job.is_remote,
            Job.employment_type,
            Job.role_family,
            Job.seniority,
            Job.flexible_signal,
            Job.compensation_raw,
            Job.posted_at,
            Job.first_seen_at,
            Job.closed_at,
            # No bookmark row means not starred; the toggle inserts one on first click.
            func.coalesce(JobBookmark.starred, false()).label("starred"),
        )
        .select_from(Job)
        .join(Company, Company.id == Job.company_id)
        .outerjoin(JobBookmark, JobBookmark.job_id == Job.id)
    )


def _job_row(row: Any) -> JobRow:
    """Build a :class:`JobRow` from a :func:`_job_select` result row."""
    return JobRow(
        id=row.id,
        company_id=row.company_id,
        company_name=row.company_name,
        company_domain=row.company_domain,
        title=row.title,
        url=row.url,
        location_text=row.location_text,
        is_remote=row.is_remote,
        employment_type=row.employment_type,
        role_family=row.role_family,
        seniority=row.seniority,
        flexible_signal=row.flexible_signal,
        compensation_raw=row.compensation_raw,
        posted_at=row.posted_at,
        first_seen_at=row.first_seen_at,
        closed_at=row.closed_at,
        starred=row.starred,
    )


# ------------------------------------------------------------------------- the page queries


async def company_list_page(
    session: AsyncSession,
    *,
    # B008 guards against a *mutable* default; ``Filters`` is a frozen dataclass, and spelling
    # the empty filter set as the default is what documents "no filters" as the normal case.
    filters: Filters = Filters(),  # noqa: B008
    sort: CompanySort = CompanySort.RECENT_JOB,
    cursor: Cursor | None = None,
    limit: int = PAGE_SIZE,
    now: datetime | None = None,
) -> Page[CompanyRow]:
    """One page of ``/`` — SPEC §9's company list, filtered, sorted and keyset-paginated.

    This docstring is the composition strategy SPEC §12 Phase 4 asks for: how eleven
    independent filters, six sorts and pagination end up as *one* parameterized statement.

    **One statement, one list of predicates.** Every §9 filter contributes zero or one boolean
    expression to a Python list (:func:`_company_predicates`, plus the search arm and the role
    ``EXISTS``) which becomes a single ``WHERE``. There is no branching between "the filtered
    query" and "the unfiltered query", and no string building — each predicate is a SQLAlchemy
    expression with bound parameters, so a value the user typed can never reach the SQL text.
    Because an unset filter appends nothing, the no-filter case degenerates to a plain scan of
    ``companies`` in sort order. That is not a happy accident but the mechanism behind SPEC
    §7.1: nothing is excluded by default, so every role family, every employment type and every
    ``flexible_signal`` value is reachable from the default view.

    **The latest round is a LEFT JOIN, not a correlated subquery.** ``companies.latest_round_id``
    is the denormalized pointer :func:`refresh_company_denormalized_columns` maintains at the
    end of each run (SPEC §5), so ``LEFT JOIN funding_rounds AS latest_round ON latest_round.id
    = companies.latest_round_id`` is a single primary-key lookup per row and serves four things
    at once: the round type/amount/date the row displays, the three round filters, and two of
    the six sorts. A window function over ``funding_rounds`` would compute the same answer per
    query; the pointer computes it once per run. Companies with no round survive the join with
    NULLs and are dropped only when a round filter is actually set — a filter on the
    right-hand side of a LEFT JOIN is exactly the "latest round matches" semantics §9 wants.

    **The tracking status is a LEFT JOIN too**, because a company the user has never touched
    has no ``user_notes`` row and must still appear. That makes ``tracking=none`` two things at
    once: ``user_notes.status = 'none'`` *or* ``user_notes.company_id IS NULL``. Both arms are
    in the predicate, so the filter agrees with the badge the row renders.

    **All the role filters collapse into exactly one EXISTS.** Role family, employment type,
    seniority and ``flexible_signal`` constrain *the same* open job, so they go together into
    one correlated ``EXISTS (SELECT 1 FROM jobs WHERE jobs.company_id = companies.id AND
    jobs.closed_at IS NULL AND ...)``. This is the meaningful reading of "companies having ≥1
    open role in those families" combined with the other role filters: ``family=software`` plus
    ``employment=part_time`` finds one part-time software role, not one part-time role and,
    separately, one software role. It also means the statement carries a single ``EXISTS``
    however many role filters are on, answered by ``ix_jobs_role_family_closed_at`` /
    ``ix_jobs_employment_type_closed_at``.

    **City and sector are semi-joins for the same reason.** Both are M:N; an inner join through
    ``company_locations`` would emit a company once per matching city, inflating the page and
    corrupting the keyset order. ``EXISTS`` returns each company at most once and stops at the
    first match.

    **Search is one OR arm, not a second query.** ``search_vector @@ websearch_to_tsquery
    ('english', :q)`` — the GIN-indexed generated column of SPEC §5 — OR pg_trgm's ``%``
    against ``companies.name`` and ``companies.normalized_name``, which are the two
    ``gin_trgm_ops`` indexes. That ``%`` arm *is* §9's "trigram fallback for typos"; ``%`` is
    true when similarity exceeds the session's ``pg_trgm.similarity_threshold`` (default 0.3).
    Expressing the fallback as an alternative *inside* the same statement, rather than as a
    second query run when the first returns nothing, is what keeps keyset pagination coherent:
    one predicate means one ordering, so page 2 of a typo'd search is still the continuation of
    page 1. A blank or whitespace-only ``q`` adds no predicate at all — an empty tsquery matches
    nothing, and clearing the search box must show everything, not nothing.

    **has_open_roles uses the denormalized ``companies.open_job_count > 0``**, not an
    ``EXISTS``. That is the number the row displays and the column the pipeline maintains;
    filtering on anything else would let the filter and the badge disagree after a partial run.
    Like the default sort, it is only as fresh as the last
    :func:`refresh_company_denormalized_columns`.

    **Keyset pagination, one OR-chain derived from the sort.** The ordering is always
    ``<sort column> <DESC|ASC> NULLS LAST, companies.id ASC`` (:func:`_order_by`); ``id`` is the
    unique tiebreak that makes it total. "The next page" is then a plain ``WHERE`` predicate
    (:func:`_after_cursor`) rather than an offset: for a descending nulls-last column it is
    ``col < :k OR (col = :k AND id > :i) OR col IS NULL`` when the cursor key is non-NULL, and
    ``col IS NULL AND id > :i`` once the cursor has crossed into the NULL tail; ascending
    mirrors it with ``>``. ``OFFSET`` was rejected for two reasons: it re-reads and discards
    every row before the offset, so page 40 costs forty pages of work, and it is defined
    against a snapshot that no longer exists — a refresh landing mid-browse shifts rows and the
    reader silently skips or re-sees some. A keyset predicate is anchored to a value, not a
    position, so it degrades gracefully instead of lying.

    **No ``COUNT(*)``.** A keyset list reports no grand total on purpose: counting matching rows
    costs a second full scan of the same predicate for a number nobody acts on. Instead the
    statement asks for ``limit + 1`` rows; the extra row is never returned, it only decides
    whether a "next" cursor is minted.

    **Per-page display data is a separate grouped query, never per row.** Sectors, locations
    and the role-family breakdown are not in this statement — joining them would multiply rows
    and defeat the keyset. :func:`company_card_extras` takes the ≤50 ids this function returned
    and fetches all three in three grouped ``= ANY(:ids)`` statements, which is three round
    trips per page instead of one hundred and fifty.

    Two sorting notes worth pinning down. :attr:`CompanySort.AMOUNT` sorts by the *latest*
    round's ``amount_usd`` — matching the "last round amount" filter and the amount the row
    displays; there is no total-raised column, and inventing one at query time would disagree
    with both. :attr:`CompanySort.NAME` sorts on ``lower(name)``, which has no btree index: at
    SPEC §13's target of 5,000 companies that sort is sub-millisecond, and indexing it would
    mean a migration this phase does not need.

    ``now`` is injectable so tests can pin ``round_within_months``; it defaults to
    ``datetime.now(UTC)``. Raises :class:`CursorError` for a cursor from another sort and
    ``ValueError`` for a limit outside ``1..200``.
    """
    _check_limit(limit)
    _check_cursor(cursor, sort)
    spec = _COMPANY_SORTS[sort]
    predicates = _company_predicates(filters, now=now or datetime.now(UTC))
    if (query := _search_text(filters.q)) is not None:
        predicates.append(_company_search_predicate(query))
    if filters.has_job_filters:
        predicates.append(_open_jobs_exist(filters))
    if cursor is not None:
        predicates.append(_after_cursor(spec, cursor, Company.id))

    stmt = (
        select(
            Company.id,
            Company.name,
            Company.domain,
            Company.website_url,
            Company.one_liner,
            Company.stage,
            Company.status,
            Company.open_job_count,
            Company.latest_job_posted_at,
            Company.first_seen_at,
            _LATEST_ROUND.round_type,
            _LATEST_ROUND.amount_usd,
            _LATEST_ROUND.announced_date,
            UserNote.status.label("tracking_status"),
            UserNote.rating,
            # Selected as well as ordered by, so the cursor carries exactly the value Postgres
            # sorted on (``lower(name)`` is not always Python's ``str.lower()``).
            spec.column.label("sort_key"),
        )
        .select_from(Company)
        .outerjoin(_LATEST_ROUND, _LATEST_ROUND.id == Company.latest_round_id)
        .outerjoin(UserNote, UserNote.company_id == Company.id)
        .where(*predicates)
        .order_by(*_order_by(spec, Company.id))
        .limit(limit + 1)
    )
    fetched = (await session.execute(stmt)).all()
    rows = [
        CompanyRow(
            id=row.id,
            name=row.name,
            domain=row.domain,
            website_url=row.website_url,
            one_liner=row.one_liner,
            stage=row.stage,
            status=row.status,
            open_job_count=row.open_job_count,
            latest_job_posted_at=row.latest_job_posted_at,
            first_seen_at=row.first_seen_at,
            latest_round_type=row.round_type,
            latest_round_amount_usd=row.amount_usd,
            latest_round_announced_date=row.announced_date,
            tracking_status=row.tracking_status,
            rating=row.rating,
        )
        for row in fetched[:limit]
    ]
    next_cursor = None
    if len(fetched) > limit:
        last = fetched[limit - 1]
        next_cursor = Cursor(
            sort=sort.value, key=_encode_sort_key(last.sort_key), id=last.id
        ).encode()
    return Page(rows=tuple(rows), next_cursor=next_cursor, limit=limit)


async def job_list_page(
    session: AsyncSession,
    *,
    # B008 guards against a *mutable* default; ``Filters`` is a frozen dataclass, and spelling
    # the empty filter set as the default is what documents "no filters" as the normal case.
    filters: Filters = Filters(),  # noqa: B008
    sort: JobSort = JobSort.POSTED,
    cursor: Cursor | None = None,
    limit: int = PAGE_SIZE,
    now: datetime | None = None,
) -> Page[JobRow]:
    """One page of ``/roles`` — SPEC §9's flat job list, "what opened this week".

    Same machinery as :func:`company_list_page` (read that docstring first); the differences
    are all consequences of listing jobs instead of companies:

    * the row is a job joined to its company, so the company-level filters
      (:func:`_company_predicates`) apply to the joined company and the role filters apply
      *directly* to the row rather than through an ``EXISTS`` — the same
      :func:`_job_attribute_predicates` either way, so the two pages cannot drift;
    * ``funding_rounds`` and ``user_notes`` are still outer-joined even though ``/roles`` does
      not display them, because the company-level filters may reference them; they cost one
      index lookup per emitted row;
    * closed roles are excluded unless :attr:`Filters.include_closed_jobs` is set. SPEC §2 keeps
      closed jobs forever as the historical record, and §9 lists openings, so this is the one
      predicate that is on by default — and it is a visibility rule about a job's lifecycle,
      not a classification, so §7.1 is untouched;
    * ``job_bookmarks`` is outer-joined for the star toggle: no row means not starred.
    """
    _check_limit(limit)
    _check_cursor(cursor, sort)
    spec = _JOB_SORTS[sort]
    predicates = _company_predicates(filters, now=now or datetime.now(UTC))
    predicates.extend(_job_attribute_predicates(filters))
    if not filters.include_closed_jobs:
        predicates.append(Job.closed_at.is_(None))
    if (query := _search_text(filters.q)) is not None:
        predicates.append(_job_search_predicate(query))
    if cursor is not None:
        predicates.append(_after_cursor(spec, cursor, Job.id))

    stmt = (
        _job_select()
        .outerjoin(_LATEST_ROUND, _LATEST_ROUND.id == Company.latest_round_id)
        .outerjoin(UserNote, UserNote.company_id == Company.id)
        .add_columns(spec.column.label("sort_key"))
        .where(*predicates)
        .order_by(*_order_by(spec, Job.id))
        .limit(limit + 1)
    )
    fetched = (await session.execute(stmt)).all()
    rows = [_job_row(row) for row in fetched[:limit]]
    next_cursor = None
    if len(fetched) > limit:
        last = fetched[limit - 1]
        next_cursor = Cursor(
            sort=sort.value, key=_encode_sort_key(last.sort_key), id=last.id
        ).encode()
    return Page(rows=tuple(rows), next_cursor=next_cursor, limit=limit)


# ------------------------------------------------------------ per-page and per-company reads


async def company_card_extras(
    session: AsyncSession, company_ids: Sequence[int]
) -> dict[int, CompanyExtras]:
    """Sectors, locations and the open-role breakdown for one rendered page of companies.

    Three grouped statements for the whole page instead of three per row: at 50 rows a page
    that is 3 round trips rather than 150, and it is why :func:`company_list_page` deliberately
    does *not* join these M:N tables (joining them would multiply rows and break the keyset).

    Every requested id is a key in the result, mapping to an empty :class:`CompanyExtras` when
    the company has no sectors, locations or open roles — the templates can then index without
    guarding, and an unknown id is simply empty rather than a ``KeyError``.

    Ordering is fixed here rather than in Jinja: sectors alphabetically, locations with the HQ
    first then by city, and the role-family breakdown by descending count then family name, so
    the chips are stable between renders.
    """
    ids = list(dict.fromkeys(company_ids))  # de-duplicate, preserve order
    if not ids:
        return {}
    sectors: dict[int, list[str]] = {company_id: [] for company_id in ids}
    locations: dict[int, list[LocationRef]] = {company_id: [] for company_id in ids}
    families: dict[int, list[tuple[enums.RoleFamily, int]]] = {company_id: [] for company_id in ids}

    sector_rows = await session.execute(
        select(CompanySector.company_id, Sector.name)
        .join(Sector, Sector.id == CompanySector.sector_id)
        .where(CompanySector.company_id == any_(_id_array(ids)))
        .order_by(CompanySector.company_id, Sector.name)
    )
    for company_id, name in sector_rows:
        sectors[company_id].append(name)

    location_rows = await session.execute(
        select(
            CompanyLocation.company_id,
            Location.city,
            Location.state,
            Location.metro,
            CompanyLocation.is_hq,
        )
        .join(Location, Location.id == CompanyLocation.location_id)
        .where(CompanyLocation.company_id == any_(_id_array(ids)))
        .order_by(CompanyLocation.company_id, CompanyLocation.is_hq.desc(), Location.city)
    )
    for company_id, city, state, metro, is_hq in location_rows:
        locations[company_id].append(LocationRef(city=city, state=state, metro=metro, is_hq=is_hq))

    open_count = func.count().label("open_count")
    # ``role_family::text``: a native Postgres enum sorts in *declaration* order, so ties would
    # come back software, sales, legal, other. The chips read better alphabetically, which is
    # also what a Python ``sorted()`` over the StrEnum values gives.
    family_name = Job.role_family.cast(Text)
    family_rows = await session.execute(
        select(Job.company_id, Job.role_family, open_count)
        .where(Job.company_id == any_(_id_array(ids)), Job.closed_at.is_(None))
        .group_by(Job.company_id, Job.role_family)
        .order_by(Job.company_id, open_count.desc(), family_name)
    )
    for company_id, role_family, count in family_rows:
        families[company_id].append((role_family, count))

    return {
        company_id: CompanyExtras(
            sectors=tuple(sectors[company_id]),
            locations=tuple(locations[company_id]),
            open_roles_by_family=tuple(families[company_id]),
        )
        for company_id in ids
    }


async def company_jobs(
    session: AsyncSession,
    company_id: int,
    *,
    include_closed: bool = False,
    sort: RoleSort | JobSort = RoleSort.POSTED,
    descending: bool | None = None,
    title_contains: str | None = None,
) -> list[JobRow]:
    """Every role of one company — the expanded row and ``/company/{id}``.

    Takes no :class:`Filters` *at all*, and that is the point: SPEC §9 says the expanded row
    shows "all roles regardless of the page-level role filter, with matching ones highlighted".
    Highlighting is the template's job; excluding is nobody's. Accepting a ``Filters`` here
    would make it too easy to hide a role the list promised was there.

    The rest of that same sentence — "sortable and filterable **within the row**" — is what
    the other three arguments are for, and they are a different thing entirely from a page-level
    filter: each one defaults to off, only a control the user operated *inside* the row can set
    one, and with the defaults this returns exactly what it always has, every open role newest
    first. That is what keeps §7.1's "classification never excludes" true of the panel the list
    lazy-loads, which asks for none of them.

    * ``sort`` and ``descending`` order the table by one of its own columns
      (:class:`RoleSort`). ``descending=None`` means that column's natural direction — see
      :func:`role_sort_descending`. A :class:`JobSort` is accepted too: ``/roles`` and the
      panel render the same :class:`JobRow`, so either vocabulary names a real column of it.
    * ``title_contains`` is a case-insensitive substring of the role *title* — the row's own
      search box, unrelated to the page's ``q``. ``autoescape`` makes a ``%`` or ``_`` the user
      typed match itself rather than acting as a ``LIKE`` wildcard.

    Open roles come first (``closed_at IS NULL`` descending puts them above the closed ones),
    then the requested sort, then id. No pagination: one company's roles are a bounded list.
    """
    predicates: list[ColumnElement[bool]] = [Job.company_id == company_id]
    if not include_closed:
        predicates.append(Job.closed_at.is_(None))
    if title_contains:
        predicates.append(Job.title.icontains(title_contains, autoescape=True))
    spec = _role_sort_spec(sort)
    if descending is not None:
        spec = replace(spec, descending=descending)
    stmt = (
        _job_select()
        .where(*predicates)
        .order_by(Job.closed_at.is_(None).desc(), *_order_by(spec, Job.id))
    )
    return [_job_row(row) for row in (await session.execute(stmt)).all()]


# --------------------------------------------------------------- facets and run health (§9)


async def facet_cities(session: AsyncSession) -> list[str]:
    """Every city the filter form offers, alphabetically.

    Drawn from ``locations`` rather than ``config/regions.yaml`` so the form offers what the
    database actually holds; the config decides which cities are *ingested* (SPEC §11).
    """
    rows = await session.execute(select(Location.city).distinct().order_by(Location.city))
    return list(rows.scalars())


async def facet_sectors(session: AsyncSession) -> list[tuple[str, str]]:
    """``(slug, name)`` for every sector, ordered by name — the sector multi-select.

    The slug is the wire value (``sector=fintech``) and the name is what the user reads.
    """
    rows = await session.execute(select(Sector.slug, Sector.name).order_by(Sector.name))
    return [(slug, name) for slug, name in rows]


async def last_successful_runs(session: AsyncSession) -> dict[str, datetime]:
    """``connector -> started_at`` of its most recent successful run.

    ``status = 'ok'`` only — deliberately narrower than :data:`COMPLETED_RUN_STATUSES`, which
    also accepts ``partial`` because a partial run still scanned its whole window and may
    anchor the next incremental fetch. Here the question is SPEC §9's "no *successful* run in
    over twice its cadence", and a run that had to log errors is not the thing whose absence
    ``/runs`` is meant to make loud.

    Connectors that have never succeeded are simply absent from the mapping; ``/runs`` lists
    every *configured* connector, so a missing key is exactly the "never ran" case it flags.
    """
    rows = await session.execute(
        select(FetchRun.connector, func.max(FetchRun.started_at))
        .where(FetchRun.status == enums.FetchRunStatus.OK)
        .group_by(FetchRun.connector)
    )
    return {connector: started_at for connector, started_at in rows}
