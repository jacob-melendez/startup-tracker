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

Phase 6 (SPEC §7.2, §8, §12 Phase 6) adds the operations layer the scheduler, ``/runs`` and the
two new CLI commands share:

* :func:`consecutive_failure_counts` — the failure streak per connector, a window function over
  ``fetch_runs`` (SPEC §7.2 "fails 3 consecutive runs");
* :func:`stats_snapshot` — every number ``cli.py stats`` prints, as one round trip;
* :func:`merge_candidate_rows` and :func:`merge_companies` — the read and the write behind
  ``cli.py merge-review``, which is the only thing in the system allowed to delete a company
  (SPEC §8 "Do not auto-merge").

None of it fetches anything: a web request handler only ever reads the local database (SPEC §2).
"""

from __future__ import annotations

import base64
import calendar
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, Literal, cast

from sqlalchemy import (
    ColumnElement,
    CursorResult,
    Executable,
    Integer,
    ScalarSelect,
    Select,
    Text,
    and_,
    any_,
    bindparam,
    case,
    delete,
    exists,
    false,
    func,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, aggregate_order_by
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import BindParameter

from db import enums
from db.models import (
    Company,
    CompanyLocation,
    CompanySector,
    CompanySource,
    Contact,
    FetchRun,
    FundingRound,
    Investor,
    Job,
    JobBookmark,
    Location,
    MergeCandidate,
    Person,
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


# ================================================ Phase 6 — operations (SPEC §7.2, §8, §12)

#: SPEC §7.2: "A connector that fails 3 consecutive runs should log at ERROR and surface
#: prominently on ``/runs``". One constant for both signals, so the scheduler's ERROR line and
#: the ``/runs`` banner can never disagree about what "failing" means.
CONSECUTIVE_FAILURE_ALERT = 3


async def consecutive_failure_counts(
    session: AsyncSession, *, connector: str | None = None
) -> dict[str, int]:
    """``connector -> how many of its most recent finished runs failed in a row`` (SPEC §7.2).

    Shape — one window function over ``fetch_runs`` plus a ``GROUP BY``, never a query per
    connector::

        SELECT connector, count(*) FROM (
            SELECT connector,
                   sum(CASE WHEN status <> 'error' THEN 1 ELSE 0 END) OVER (
                       PARTITION BY connector
                       ORDER BY started_at DESC, id DESC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS completed_above
              FROM fetch_runs
             WHERE finished_at IS NOT NULL) s
        WHERE s.completed_above = 0
        GROUP BY connector

    The running sum counts the runs that *did not* fail from the newest row down to and
    including the current one, so it is ``0`` exactly while every run at or above this one is an
    error — that is, while the row is inside the head streak. The first ``ok``/``partial`` makes
    the sum ``1``, and every older row inherits at least that, so nothing below the streak is
    ever counted. ``count(*)`` per connector over the ``0`` rows is therefore the streak length.

    Every clause earns its place:

    * **``finished_at IS NOT NULL``.** ``FetchRun.status`` defaults to ``error`` so that a run
      which dies mid-flight is recorded as one (SPEC §7.2), which means an *in-flight* run also
      reads ``error``. Counting it would report a failure for a run that is still going. A
      process killed mid-run leaves such a row for ever; excluding it means the row neither
      starts nor breaks a streak, which is the honest answer for a run whose outcome nobody
      knows.
    * **``ORDER BY started_at DESC, id DESC``.** Newest first, because the question is about the
      *current* streak. ``id`` breaks ties deterministically — two runs of one connector can
      share a ``started_at`` (``server_default=now()`` is the transaction clock), and without
      the tiebreak the window frame would be ambiguous and the count could wobble between
      calls.
    * **``partial`` is not a failure.** A partial run completed its scan and merely had per-item
      trouble, so it proves the source is reachable and ends the streak — the same reading
      :data:`COMPLETED_RUN_STATUSES` already uses for the incremental window. This is
      deliberately *wider* than :func:`last_successful_runs`, which counts ``ok`` alone: that
      one answers SPEC §9's "no successful run in over twice its cadence", this answers §7.2's
      "fails 3 consecutive runs". A connector can therefore show "last ok nine days ago" and
      "0 consecutive failures" at once, and both statements are true — it has been limping, not
      broken. ``/runs`` shows both numbers for exactly that reason.

    Connectors with no current streak (their newest finished run completed) are **absent** from
    the mapping rather than present with ``0``; callers use ``.get(name, 0)``. A connector that
    has never run, or a name that does not exist, likewise yields nothing — "never ran" is the
    staleness signal's business, not this one's.

    ``connector=`` narrows the whole statement to one connector so the scheduler can ask about
    the run it has just finished without scanning the table for the others.
    """
    # ``sum`` over a CASE rather than ``count(*) FILTER``: the frame has to be evaluated for
    # every row, including the failed ones, and only a plain aggregate can be windowed here.
    completed = case((FetchRun.status != enums.FetchRunStatus.ERROR, 1), else_=0)
    completed_above = func.sum(completed).over(
        partition_by=FetchRun.connector,
        order_by=(FetchRun.started_at.desc(), FetchRun.id.desc()),
        rows=(None, 0),
    )
    finished = select(
        FetchRun.connector.label("connector"), completed_above.label("completed_above")
    ).where(FetchRun.finished_at.is_not(None))
    if connector is not None:
        finished = finished.where(FetchRun.connector == connector)
    head = finished.subquery("finished_runs")
    rows = await session.execute(
        select(head.c.connector, func.count())
        .where(head.c.completed_above == 0)
        .group_by(head.c.connector)
    )
    return {name: streak for name, streak in rows}


# ------------------------------------------------------------------- cli.py stats (SPEC §12)

#: SPEC §12 Phase 6: "companies and jobs added in the last 7 days".
STATS_WINDOW_DAYS = 7


@dataclass(frozen=True, slots=True, kw_only=True)
class StatsSnapshot:
    """Every number ``cli.py stats`` prints, read in one statement (:func:`stats_snapshot`).

    Flat and pre-split by design: the CLI formats, it does not compute. Splitting
    ``companies``/``companies_active``/``companies_dead`` and the two contact confidences here
    rather than in the printer keeps the arithmetic inside the transaction that read it.
    """

    companies: int
    #: ``status='active'``; ``companies`` minus these two is the ``acquired`` remainder.
    companies_active: int
    companies_dead: int
    #: ``closed_at IS NULL`` — a closed job is history, never deleted (SPEC §2).
    jobs_open: int
    jobs_closed: int
    funding_rounds: int
    investors: int
    people: int
    #: SPEC §6's two confidences, counted apart because the difference is the point.
    contacts_published: int
    contacts_constructed: int
    #: Rows of ``locations`` / ``sectors`` — the vocabularies, not the M:N links.
    locations: int
    sectors: int
    #: ``resolved_at IS NULL`` — the queue ``merge-review`` works through (SPEC §8).
    merge_candidates_open: int
    merge_candidates_resolved: int
    fetch_runs: int
    user_notes: int
    job_bookmarks: int
    #: ``first_seen_at >= now - STATS_WINDOW_DAYS``.
    companies_added: int
    jobs_added: int


def _count_of(entity: Any, *predicates: ColumnElement[bool]) -> ScalarSelect[int]:
    """``(SELECT count(*) FROM <entity> WHERE ...)`` as one scalar sub-select."""
    return select(func.count()).select_from(entity).where(*predicates).scalar_subquery()


async def stats_snapshot(session: AsyncSession, *, now: datetime) -> StatsSnapshot:
    """The row counts, the two lifecycle splits and the 7-day intake — one round trip.

    Shape — a ``SELECT`` with **no FROM clause** whose every column is an independent scalar
    sub-select (``SELECT (SELECT count(*) FROM companies) AS companies, (SELECT count(*) FROM
    jobs WHERE closed_at IS NULL) AS jobs_open, ...``). Nineteen counts over nine unrelated
    tables have no join key in common, so joining them would either multiply rows or need
    nineteen ``GROUP BY`` queries; sub-selects keep it to one statement, which matters twice
    over: it is one round trip, and it is one snapshot — every number is read at the same
    MVCC instant, so the report cannot say 3,188 companies in one line and imply 3,190 in
    another because an ingest run committed in between.

    ``now`` is injected rather than read from the clock so a test can pin the boundary of the
    :data:`STATS_WINDOW_DAYS` window; the window is a half-open ``first_seen_at >= now - 7d``.
    """
    since = now - timedelta(days=STATS_WINDOW_DAYS)
    row = (
        await session.execute(
            select(
                _count_of(Company).label("companies"),
                _count_of(Company, Company.status == enums.CompanyStatus.ACTIVE).label("active"),
                _count_of(Company, Company.status == enums.CompanyStatus.DEAD).label("dead"),
                _count_of(Job, Job.closed_at.is_(None)).label("jobs_open"),
                _count_of(Job, Job.closed_at.is_not(None)).label("jobs_closed"),
                _count_of(FundingRound).label("funding_rounds"),
                _count_of(Investor).label("investors"),
                _count_of(Person).label("people"),
                _count_of(Contact, Contact.confidence == enums.ContactConfidence.PUBLISHED).label(
                    "contacts_published"
                ),
                _count_of(Contact, Contact.confidence == enums.ContactConfidence.CONSTRUCTED).label(
                    "contacts_constructed"
                ),
                _count_of(Location).label("locations"),
                _count_of(Sector).label("sectors"),
                _count_of(MergeCandidate, MergeCandidate.resolved_at.is_(None)).label("merge_open"),
                _count_of(MergeCandidate, MergeCandidate.resolved_at.is_not(None)).label(
                    "merge_resolved"
                ),
                _count_of(FetchRun).label("fetch_runs"),
                _count_of(UserNote).label("user_notes"),
                _count_of(JobBookmark).label("job_bookmarks"),
                _count_of(Company, Company.first_seen_at >= since).label("companies_added"),
                _count_of(Job, Job.first_seen_at >= since).label("jobs_added"),
            )
        )
    ).one()
    return StatsSnapshot(
        companies=row.companies,
        companies_active=row.active,
        companies_dead=row.dead,
        jobs_open=row.jobs_open,
        jobs_closed=row.jobs_closed,
        funding_rounds=row.funding_rounds,
        investors=row.investors,
        people=row.people,
        contacts_published=row.contacts_published,
        contacts_constructed=row.contacts_constructed,
        locations=row.locations,
        sectors=row.sectors,
        merge_candidates_open=row.merge_open,
        merge_candidates_resolved=row.merge_resolved,
        fetch_runs=row.fetch_runs,
        user_notes=row.user_notes,
        job_bookmarks=row.job_bookmarks,
        companies_added=row.companies_added,
        jobs_added=row.jobs_added,
    )


# ------------------------------------------------------------ cli.py merge-review (SPEC §8)

#: The ``companies`` columns a merge may copy from the discarded row onto the survivor.
#:
#: This **must** stay equal to :data:`ingest.pipeline.COMPANY_MERGE_FIELDS`, which is the source
#: of truth: those are the fields connectors write and whose owner is tracked in
#: ``Company.field_provenance`` (SPEC §8), and a merge fills exactly the same set. It is
#: re-declared instead of imported because importing it here would close a cycle —
#: ``ingest.pipeline`` imports :mod:`db.queries`, and :mod:`db.queries` imports
#: ``ingest.base`` — so a test asserts the two tuples are equal and fails the moment either
#: moves.
MERGEABLE_COMPANY_FIELDS: tuple[str, ...] = (
    "name",
    "domain",
    "website_url",
    "one_liner",
    "thesis",
    "founded_year",
    "employee_est",
    "stage",
    "status",
    "ats_provider",
    "ats_token",
)


@dataclass(frozen=True, slots=True, kw_only=True)
class CompanySummary:
    """One side of a duplicate pair, as ``merge-review`` renders it side by side."""

    id: int
    name: str
    domain: str | None
    #: The HQ city if one is flagged, else the alphabetically first — see :func:`_summary_columns`.
    city: str | None
    stage: enums.Stage
    status: enums.CompanyStatus
    #: The denormalized column (SPEC §5), i.e. the number ``/`` shows for this company.
    open_job_count: int
    funding_round_count: int
    contact_count: int
    first_seen_at: datetime
    last_seen_at: datetime
    #: ``company_sources.connector`` for this company, alphabetically.
    connectors: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class MergeCandidateRow:
    """An unresolved :class:`~db.models.MergeCandidate` with both companies summarized.

    ``a``/``b`` keep the stored orientation (``company_id_a < company_id_b``), which is what
    makes the reviewer's ``[1]``/``[2]`` keys stable between renders.
    """

    id: int
    similarity: float
    reason: str
    a: CompanySummary
    b: CompanySummary


def _summary_columns(company: Any, prefix: str) -> list[Any]:
    """The :class:`CompanySummary` columns for one side of a pair, labelled ``<prefix>_*``.

    The three aggregates are correlated scalar sub-selects rather than joins: ``funding_rounds``,
    ``contacts`` and ``company_sources`` are all 1:N, so joining them would emit the pair once
    per round times contact times connector. ``city`` is a ``LIMIT 1`` sub-select ordered
    HQ-first then alphabetically, the same rule :attr:`CompanyExtras.hq_city` applies.
    """
    return [
        company.id.label(f"{prefix}_id"),
        company.name.label(f"{prefix}_name"),
        company.domain.label(f"{prefix}_domain"),
        select(Location.city)
        .join(CompanyLocation, CompanyLocation.location_id == Location.id)
        .where(CompanyLocation.company_id == company.id)
        .order_by(CompanyLocation.is_hq.desc(), Location.city)
        .limit(1)
        .correlate(company)
        .scalar_subquery()
        .label(f"{prefix}_city"),
        company.stage.label(f"{prefix}_stage"),
        company.status.label(f"{prefix}_status"),
        company.open_job_count.label(f"{prefix}_open_job_count"),
        _correlated_count(FundingRound, FundingRound.company_id, company).label(f"{prefix}_rounds"),
        _correlated_count(Contact, Contact.company_id, company).label(f"{prefix}_contacts"),
        company.first_seen_at.label(f"{prefix}_first_seen_at"),
        company.last_seen_at.label(f"{prefix}_last_seen_at"),
        select(
            func.array_agg(
                aggregate_order_by(CompanySource.connector, CompanySource.connector.asc())
            )
        )
        .where(CompanySource.company_id == company.id)
        .correlate(company)
        .scalar_subquery()
        .label(f"{prefix}_connectors"),
    ]


def _correlated_count(entity: Any, foreign_key: Any, company: Any) -> ScalarSelect[int]:
    """``(SELECT count(*) FROM <entity> WHERE <fk> = <company alias>.id)``."""
    return (
        select(func.count())
        .select_from(entity)
        .where(foreign_key == company.id)
        .correlate(company)
        .scalar_subquery()
    )


def _summary(row: Any, prefix: str) -> CompanySummary:
    """Build one :class:`CompanySummary` from the ``<prefix>_*`` half of a result row."""
    return CompanySummary(
        id=getattr(row, f"{prefix}_id"),
        name=getattr(row, f"{prefix}_name"),
        domain=getattr(row, f"{prefix}_domain"),
        city=getattr(row, f"{prefix}_city"),
        stage=getattr(row, f"{prefix}_stage"),
        status=getattr(row, f"{prefix}_status"),
        open_job_count=getattr(row, f"{prefix}_open_job_count"),
        funding_round_count=getattr(row, f"{prefix}_rounds"),
        contact_count=getattr(row, f"{prefix}_contacts"),
        first_seen_at=getattr(row, f"{prefix}_first_seen_at"),
        last_seen_at=getattr(row, f"{prefix}_last_seen_at"),
        # ``array_agg`` over no rows is NULL, not an empty array.
        connectors=tuple(getattr(row, f"{prefix}_connectors") or ()),
    )


async def merge_candidate_rows(session: AsyncSession, *, limit: int) -> list[MergeCandidateRow]:
    """The unresolved duplicate pairs ``cli.py merge-review`` walks, most similar first.

    Shape — one statement for the *whole* list, not one per row: ``merge_candidates`` joined
    twice to ``companies`` (aliases ``company_a``/``company_b``), each side carrying its four
    scalar sub-selects (:func:`_summary_columns`). A reviewer needs both sides of every pair in
    front of them before deciding, and issuing eight queries per pair would turn a 20-pair
    review into 160 round trips.

    Ordering is ``similarity DESC, id ASC``: the most confident duplicates first, then oldest
    first so the list is stable while the reviewer works through it and a pair that was skipped
    keeps its place. Only ``resolved_at IS NULL`` rows appear — a pair the reviewer called "not
    a duplicate" is stamped resolved and must never be offered again (SPEC §8).

    Both joins are inner joins, which is safe: both FKs are ``NOT NULL`` and both cascade on
    delete, so a candidate whose company was merged away has already been deleted with it.
    """
    if limit < 1:
        raise ValueError(f"limit must be at least 1; got {limit}")
    company_a = aliased(Company, name="company_a")
    company_b = aliased(Company, name="company_b")
    rows = await session.execute(
        select(
            MergeCandidate.id,
            MergeCandidate.similarity,
            MergeCandidate.reason,
            *_summary_columns(company_a, "a"),
            *_summary_columns(company_b, "b"),
        )
        .join(company_a, company_a.id == MergeCandidate.company_id_a)
        .join(company_b, company_b.id == MergeCandidate.company_id_b)
        .where(MergeCandidate.resolved_at.is_(None))
        .order_by(MergeCandidate.similarity.desc(), MergeCandidate.id)
        .limit(limit)
    )
    return [
        MergeCandidateRow(
            id=row.id,
            similarity=row.similarity,
            reason=row.reason,
            a=_summary(row, "a"),
            b=_summary(row, "b"),
        )
        for row in rows
    ]


@dataclass(frozen=True, slots=True, kw_only=True)
class MergeResult:
    """What :func:`merge_companies` actually did, so the CLI can report it row by row.

    Every count is a fact about the database after the merge, not an estimate: a reviewer who
    is told "4 jobs moved, 1 discarded" can check both numbers, and ``note_discarded`` carries
    the user's own text back out so it can be printed rather than lost.
    """

    keep_id: int
    drop_id: int
    #: ``companies`` columns that were NULL on the survivor and were filled from the discarded
    #: row, in :data:`MERGEABLE_COMPANY_FIELDS` order.
    fields_filled: tuple[str, ...]
    jobs_moved: int
    #: Jobs whose ``external_id`` the survivor already had; the survivor's row is the one kept.
    jobs_discarded: int
    bookmarks_moved: int
    funding_rounds_moved: int
    people_moved: int
    people_deduped: int
    contacts_moved: int
    contacts_discarded: int
    locations_moved: int
    sectors_moved: int
    sources_moved: int
    #: ``(connector, external_id)`` pairs that could not be kept: ``company_sources`` is one row
    #: per ``(company, connector)``, so when both companies identify themselves to the same
    #: connector under *different* ids only the survivor's survives. Reported rather than dropped
    #: silently because that connector's next run no longer resolves the lost id by
    #: ``external_id`` (SPEC §8 step 0) and will create the duplicate again — the reviewer may
    #: want to merge the other way round instead. Empty in the ordinary case.
    sources_discarded: tuple[tuple[str, str], ...] = ()
    candidates_repointed: int
    note_moved: bool
    #: The discarded company's note text when both companies had a note; ``None`` otherwise.
    note_discarded: str | None
    #: Notes on starred duplicate jobs that could not be rescued, because the survivor's own
    #: copy of that posting was already starred. Handed back for the same reason
    #: :attr:`note_discarded` is: the caller prints them, so no user-written text is destroyed
    #: without being shown first. Empty in the ordinary case.
    bookmark_notes_discarded: tuple[str, ...] = ()


async def _execute_rowcount(session: AsyncSession, statement: Executable) -> int:
    """Run a DML statement and return how many rows it touched.

    ``AsyncSession.execute`` is typed as the generic ``Result``; an UPDATE or DELETE always
    yields a ``CursorResult`` whose ``rowcount`` is the number of rows affected.
    """
    result = cast("CursorResult[Any]", await session.execute(statement))
    affected: int = result.rowcount
    return affected


async def merge_companies(
    session: AsyncSession, *, keep_id: int, drop_id: int, now: datetime
) -> MergeResult:
    """Fold ``drop_id`` into ``keep_id`` — the write behind ``cli.py merge-review`` (SPEC §8).

    This is the one place in the system that deletes a company. ``db/models.py`` states the
    licence ("companies are only ever deleted by ``merge-review``") and it is narrow: the
    duplicate row, and the child rows that physically cannot survive a unique constraint on the
    survivor. Everything else is *moved*, because SPEC §2 makes the database the historical
    record. Nothing here is automatic — SPEC §8 forbids auto-merging, so a human chose both ids.

    **The order of operations is load-bearing.** Every child table hangs off ``companies`` with
    ``ON DELETE CASCADE``, so anything still pointing at the discarded row when the ``DELETE``
    lands is destroyed by the database:

    1. Read both rows (``SELECT ... FOR UPDATE``, ordered by id so two concurrent merges take
       the two locks in the same order, and so an ingest run cannot write to either company
       mid-merge). ``ValueError`` for equal ids or a company that does not exist. The discarded
       row's values are snapshotted *now*, because step 4 needs them after it is gone.
    2. Move the children, each with a ``NOT EXISTS`` guard against the survivor's own unique
       constraint, so the statement moves what can move and leaves the collisions behind:

       * ``job_bookmarks`` **first**. A drop-side job whose ``external_id`` the survivor already
         has is about to be discarded; if the user starred it and the survivor's twin carries no
         bookmark, the star moves to the twin. Doing this after the job move would be too late —
         the bookmark would already be cascade-deleted — and doing it without the "twin has no
         bookmark" guard would violate the bookmark's primary key.
       * ``jobs`` guarded on ``uq_jobs_company_id_external_id``. What stays behind is exactly the
         set of duplicated postings, counted as ``jobs_discarded``.
       * ``funding_rounds`` and ``people``: no unique constraint, so all of them move. People are
         then de-duplicated *within* the survivor on ``(full_name, title, linkedin_url)`` with
         NULLs folded to ``''``, keeping the lowest id. Only byte-identical rows are removed —
         never a fuzzy match — so no observation is lost, just a repeated one.
       * ``contacts`` (``uq_contacts_company_id_kind_value``), ``company_locations``,
         ``company_sectors`` and ``company_sources`` (composite primary keys): same guard. The
         survivor's row wins every collision, which is why a moved location keeps the discarded
         row's ``is_hq`` flag but a colliding one does not change the survivor's. Two collisions
         carry information the winning row would otherwise lose, and both are salvaged *before*
         the row that holds it is cascaded away: a contact the survivor only ``constructed``
         while the discarded company ``published`` it is promoted in place
         (:func:`_promote_colliding_contacts`), and a ``company_sources`` row whose
         ``external_id`` the survivor does not have adopts it
         (:func:`_adopt_source_external_ids`). What genuinely cannot be kept — a second
         ``external_id`` for a connector the survivor already identifies itself to — comes back
         in ``sources_discarded`` for the caller to print, because losing it means that
         connector will create the company again on its next run.
       * ``user_notes``: if the survivor has no note the row moves wholesale
         (``note_moved``) — and "no note" means no *content*, so the blank row an empty Save
         leaves behind does not outrank a filled one. If both hold something the survivor's
         stands and the other's *text* comes back in ``note_discarded`` — the user's own writing
         is never destroyed silently, it is handed to the caller to print.
       * ``merge_candidates``: every *other* pair that referenced the discarded company is
         repointed onto the survivor, renormalized to ``company_id_a < company_id_b`` and
         skipped when that pair already exists or would be a self-pair. The pair under review
         references the discarded company, so the cascade removes it — which is why a merged
         pair leaves no ``resolved_at`` row behind, while a "not a duplicate" decision does.
    3. ``DELETE FROM companies WHERE id = :drop``. This has to happen *before* step 4:
       ``ix_companies_domain`` is unique, so copying the discarded row's domain onto the
       survivor while both rows still exist raises a unique violation.
    4. Fill the survivor's NULLs from the snapshot: for each :data:`MERGEABLE_COMPANY_FIELDS`
       column the survivor has no value for, take the discarded row's value and, unless the
       survivor already claims that field, its ``field_provenance`` owner too (SPEC §8 — the
       survivor's own entries always win). ``first_seen_at`` becomes the earlier of the two and
       ``last_seen_at`` the later, so the survivor inherits the full observed history. ``name``,
       ``stage`` and ``status`` are ``NOT NULL`` and so are never filled; in particular ``name``
       cannot change here, which is what keeps ``normalized_name`` consistent with it without
       recomputing anything.
    5. Refresh the survivor's denormalized columns (SPEC §5): it has just gained jobs and rounds,
       so ``open_job_count``, ``latest_job_posted_at`` and ``latest_round_id`` are all stale, and
       ``/``'s default sort reads them.

    ``now`` stamps the moved ``user_notes`` row's ``updated_at`` (the column's ``onupdate`` would
    otherwise use the database clock), so one injected clock describes the whole review.

    **Nothing here commits.** The caller owns the transaction — ``merge-review`` commits once per
    pair, so a reviewer who quits after three merges keeps exactly those three, and a failure
    part-way through a single merge rolls the whole pair back rather than leaving a company
    half-folded.
    """
    if keep_id == drop_id:
        raise ValueError(f"cannot merge company {keep_id} into itself")
    snapshot = {
        row.id: row
        for row in await session.execute(
            select(
                Company.id,
                Company.field_provenance,
                Company.first_seen_at,
                Company.last_seen_at,
                *(getattr(Company, field) for field in MERGEABLE_COMPANY_FIELDS),
            )
            .where(Company.id.in_((keep_id, drop_id)))
            .order_by(Company.id)
            .with_for_update()
        )
    }
    missing = [company_id for company_id in (keep_id, drop_id) if company_id not in snapshot]
    if missing:
        raise ValueError(f"no such company: {', '.join(str(company_id) for company_id in missing)}")
    keep, drop = snapshot[keep_id], snapshot[drop_id]

    bookmarks_moved = await _rescue_bookmarks(session, keep_id=keep_id, drop_id=drop_id)
    jobs_moved = await _move_guarded(
        session, Job, Job.external_id, keep_id=keep_id, drop_id=drop_id
    )
    jobs_discarded = await _remaining(session, Job, drop_id)
    # After the rescue and the move, the jobs still on the discarded company are exactly the
    # duplicates about to be cascade-deleted, so this is the last moment their bookmark notes
    # can be read.
    bookmark_notes_discarded = await _stranded_bookmark_notes(session, drop_id)
    funding_rounds_moved = await _execute_rowcount(
        session,
        update(FundingRound).where(FundingRound.company_id == drop_id).values(company_id=keep_id),
    )
    people_moved = await _execute_rowcount(
        session, update(Person).where(Person.company_id == drop_id).values(company_id=keep_id)
    )
    people_deduped = await _dedupe_people(session, keep_id)
    await _promote_colliding_contacts(session, keep_id=keep_id, drop_id=drop_id)
    contacts_moved = await _move_guarded(
        session, Contact, Contact.kind, Contact.value, keep_id=keep_id, drop_id=drop_id
    )
    contacts_discarded = await _remaining(session, Contact, drop_id)
    locations_moved = await _move_guarded(
        session,
        CompanyLocation,
        CompanyLocation.location_id,
        keep_id=keep_id,
        drop_id=drop_id,
    )
    sectors_moved = await _move_guarded(
        session, CompanySector, CompanySector.sector_id, keep_id=keep_id, drop_id=drop_id
    )
    sources_moved = await _move_guarded(
        session, CompanySource, CompanySource.connector, keep_id=keep_id, drop_id=drop_id
    )
    await _adopt_source_external_ids(session, keep_id=keep_id, drop_id=drop_id)
    sources_discarded = await _stranded_source_external_ids(
        session, keep_id=keep_id, drop_id=drop_id
    )
    note_moved, note_discarded = await _merge_user_notes(
        session, keep_id=keep_id, drop_id=drop_id, now=now
    )
    candidates_repointed = await _repoint_merge_candidates(
        session, keep_id=keep_id, drop_id=drop_id
    )

    await session.execute(delete(Company).where(Company.id == drop_id))

    provenance: dict[str, str] = dict(keep.field_provenance)
    filled: dict[str, Any] = {}
    for field in MERGEABLE_COMPANY_FIELDS:
        if getattr(keep, field) is not None or getattr(drop, field) is None:
            continue
        filled[field] = getattr(drop, field)
        owner = drop.field_provenance.get(field)
        if owner is not None and field not in provenance:
            provenance[field] = owner
    await session.execute(
        update(Company)
        .where(Company.id == keep_id)
        .values(
            **filled,
            # JSONB: a fresh dict, never an in-place mutation (as ``ingest.pipeline`` does).
            field_provenance=provenance,
            first_seen_at=min(keep.first_seen_at, drop.first_seen_at),
            last_seen_at=max(keep.last_seen_at, drop.last_seen_at),
        )
    )
    await refresh_company_denormalized_columns(session, [keep_id])
    return MergeResult(
        keep_id=keep_id,
        drop_id=drop_id,
        fields_filled=tuple(filled),
        jobs_moved=jobs_moved,
        jobs_discarded=jobs_discarded,
        bookmarks_moved=bookmarks_moved,
        funding_rounds_moved=funding_rounds_moved,
        people_moved=people_moved,
        people_deduped=people_deduped,
        contacts_moved=contacts_moved,
        contacts_discarded=contacts_discarded,
        locations_moved=locations_moved,
        sectors_moved=sectors_moved,
        sources_moved=sources_moved,
        sources_discarded=sources_discarded,
        candidates_repointed=candidates_repointed,
        note_moved=note_moved,
        note_discarded=note_discarded,
        bookmark_notes_discarded=bookmark_notes_discarded,
    )


async def _stranded_bookmark_notes(session: AsyncSession, drop_id: int) -> tuple[str, ...]:
    """The notes on bookmarks that :func:`_rescue_bookmarks` could not move.

    Called after the rescue and the job move, when the jobs still pointing at the discarded
    company are exactly the duplicated postings the cascade is about to delete. A bookmark on
    one of them survived only if the survivor's twin had none; the rest carry the user's own
    words to the grave unless someone reads them out first, which is the same rule
    :func:`_merge_user_notes` applies to a company note. The star itself is not worth
    reporting — the survivor's copy of that posting is already starred — but the text is.
    """
    rows = await session.execute(
        select(JobBookmark.note)
        .join(Job, Job.id == JobBookmark.job_id)
        .where(Job.company_id == drop_id, JobBookmark.note.is_not(None))
        .order_by(JobBookmark.job_id)
    )
    return tuple(note for note in rows.scalars() if note)


async def _move_guarded(
    session: AsyncSession, entity: Any, *unique_columns: Any, keep_id: int, drop_id: int
) -> int:
    """Move ``entity``'s rows from ``drop_id`` to ``keep_id``, skipping the ones that would
    collide with a row the survivor already has.

    ``unique_columns`` are the columns that, together with ``company_id``, are unique — the
    ``external_id`` of a job, the ``(kind, value)`` of a contact, the second half of a composite
    primary key. The guard is a correlated ``NOT EXISTS`` against an alias of the same table::

        UPDATE <t> SET company_id = :keep
         WHERE <t>.company_id = :drop
           AND NOT EXISTS (SELECT 1 FROM <t> AS twin
                            WHERE twin.company_id = :keep AND twin.<u> = <t>.<u> ...)

    One statement, so a company with 300 jobs costs one round trip and not 300. The rows left
    behind are duplicates of rows the survivor already holds; they die with the company delete.
    """
    twin = aliased(entity, name="twin")
    matches = [getattr(twin, column.key) == column for column in unique_columns]
    collision = (
        select(twin.company_id)
        .where(twin.company_id == keep_id, *matches)
        .correlate(entity)
        .exists()
    )
    return await _execute_rowcount(
        session,
        update(entity).where(entity.company_id == drop_id, ~collision).values(company_id=keep_id),
    )


async def _promote_colliding_contacts(session: AsyncSession, *, keep_id: int, drop_id: int) -> None:
    """Lift the survivor's contact to ``published`` when the discarded company's twin is the
    observation.

    ``uq_contacts_company_id_kind_value`` ignores ``confidence``, so the same URL can sit on both
    companies as an address one of them published and as a link the other merely constructed from
    its domain (the common case: ``acme.com``'s footer links to ``linkedin.com/company/acme``,
    which is byte for byte what :mod:`ingest.contacts` builds for ``acme.io``). ``_move_guarded``
    resolves that collision by row, keeping the survivor's — which, when the survivor's is the
    constructed one, would delete an observation and leave a guess in its place. SPEC §6 forbids
    exactly that, and the ingest path guards it twice (``ingest.pipeline._upsert_contacts``'
    ``ON CONFLICT ... WHERE`` clause and ``_insert_constructed_contact``'s ``DO NOTHING``); this
    is the third place two rows for one ``(kind, value)`` can meet, and it applies the same rule.

    Shape — an ``UPDATE ... FROM`` promoting the *survivor's own row in place*, so the row id,
    the "survivor's row wins every collision" invariant and ``contacts_moved`` /
    ``contacts_discarded`` are all untouched; only the confidence and the ``source_id`` behind it
    change hands::

        UPDATE contacts SET confidence = 'published', source_id = loser.source_id
          FROM contacts AS loser
         WHERE contacts.company_id = :keep AND contacts.confidence <> 'published'
           AND loser.company_id = :drop AND loser.kind = contacts.kind
           AND loser.value = contacts.value AND loser.confidence = 'published'

    ``source_id`` travels with the promotion because it is the provenance of the observation: a
    constructed row has none by construction, so the survivor gains the ``sources`` row that
    proves where the address was seen. This runs *before* the move, while the discarded row is
    still there to read.
    """
    loser = aliased(Contact, name="loser")
    await session.execute(
        update(Contact)
        .where(
            Contact.company_id == keep_id,
            Contact.confidence != enums.ContactConfidence.PUBLISHED,
            loser.company_id == drop_id,
            loser.kind == Contact.kind,
            loser.value == Contact.value,
            loser.confidence == enums.ContactConfidence.PUBLISHED,
        )
        .values(confidence=enums.ContactConfidence.PUBLISHED, source_id=loser.source_id)
    )


async def _adopt_source_external_ids(session: AsyncSession, *, keep_id: int, drop_id: int) -> None:
    """Fill the survivor's missing ``company_sources.external_id`` from the discarded row.

    ``company_sources`` is keyed on ``(company_id, connector)``, so a drop-side row for a
    connector the survivor already lists cannot move — but its ``external_id`` is the identifier
    SPEC §8 step 0 resolves that connector's records by, and a survivor row that has none is
    strictly worse than one that has the discarded row's. This is the same "fill what the
    survivor does not know, never overwrite what it does" rule step 4 applies to the ``companies``
    columns, applied to the one column a composite-key collision would otherwise throw away for
    nothing::

        UPDATE company_sources SET external_id = stranded.external_id
          FROM company_sources AS stranded
         WHERE company_sources.company_id = :keep AND company_sources.external_id IS NULL
           AND stranded.company_id = :drop AND stranded.connector = company_sources.connector
           AND stranded.external_id IS NOT NULL

    A survivor that already has an id keeps it; that case is a genuine loss and is reported by
    :func:`_stranded_source_external_ids` instead.
    """
    stranded = aliased(CompanySource, name="stranded")
    await session.execute(
        update(CompanySource)
        .where(
            CompanySource.company_id == keep_id,
            CompanySource.external_id.is_(None),
            stranded.company_id == drop_id,
            stranded.connector == CompanySource.connector,
            stranded.external_id.is_not(None),
        )
        .values(external_id=stranded.external_id)
    )


async def _stranded_source_external_ids(
    session: AsyncSession, *, keep_id: int, drop_id: int
) -> tuple[tuple[str, str], ...]:
    """The ``(connector, external_id)`` pairs the merge cannot keep, for the caller to print.

    Called after the move and after :func:`_adopt_source_external_ids`, so what is left at
    ``drop_id`` with an ``external_id`` that the survivor's row for that connector does not now
    carry is exactly the set about to die with the cascade. One row per ``(company, connector)``
    is a primary key, not a policy, so there is nowhere to put a second id — which makes saying
    so the only available mitigation. It matters because SPEC §8 step 0 resolves a connector's
    own records through this column: with the id gone, the next run of that connector no longer
    recognises the record as one it has already seen and inserts the duplicate company the
    reviewer just merged away. The inner join is safe — a row only stays behind because the
    survivor has one for the same connector.
    """
    twin = aliased(CompanySource, name="twin")
    rows = await session.execute(
        select(CompanySource.connector, CompanySource.external_id)
        .join(
            twin,
            and_(twin.company_id == keep_id, twin.connector == CompanySource.connector),
        )
        .where(
            CompanySource.company_id == drop_id,
            CompanySource.external_id.is_not(None),
            twin.external_id.is_distinct_from(CompanySource.external_id),
        )
        .order_by(CompanySource.connector)
    )
    return tuple((row.connector, row.external_id) for row in rows)


async def _remaining(session: AsyncSession, entity: Any, drop_id: int) -> int:
    """How many of ``entity``'s rows :func:`_move_guarded` had to leave behind at ``drop_id``."""
    count: int | None = await session.scalar(
        select(func.count()).select_from(entity).where(entity.company_id == drop_id)
    )
    return count or 0


async def _rescue_bookmarks(session: AsyncSession, *, keep_id: int, drop_id: int) -> int:
    """Move a star off a job that is about to be discarded, onto the survivor's own copy.

    Shape — an ``UPDATE ... FROM`` over two aliases of ``jobs``::

        UPDATE job_bookmarks SET job_id = keep_twin.id
          FROM jobs AS drop_job, jobs AS keep_twin
         WHERE job_bookmarks.job_id = drop_job.id
           AND drop_job.company_id = :drop
           AND keep_twin.company_id = :keep
           AND keep_twin.external_id = drop_job.external_id
           AND NOT EXISTS (SELECT 1 FROM job_bookmarks b WHERE b.job_id = keep_twin.id)

    Only the *colliding* jobs are addressed (the join on ``external_id`` is what selects them);
    a bookmark on a job that simply moves needs no help, since the job keeps its id. The
    ``NOT EXISTS`` is the bookmark's own primary key: when the survivor's twin is already
    starred there is nothing to rescue, and that row's *note* is collected by
    :func:`_stranded_bookmark_notes` so the caller can print it before the cascade takes it.
    """
    drop_job = aliased(Job, name="drop_job")
    keep_twin = aliased(Job, name="keep_twin")
    twin_bookmark = aliased(JobBookmark, name="twin_bookmark")
    already_starred = (
        select(twin_bookmark.job_id)
        .where(twin_bookmark.job_id == keep_twin.id)
        .correlate(keep_twin)
        .exists()
    )
    return await _execute_rowcount(
        session,
        update(JobBookmark)
        .where(
            JobBookmark.job_id == drop_job.id,
            drop_job.company_id == drop_id,
            keep_twin.company_id == keep_id,
            keep_twin.external_id == drop_job.external_id,
            ~already_starred,
        )
        .values(job_id=keep_twin.id),
    )


async def _dedupe_people(session: AsyncSession, keep_id: int) -> int:
    """Delete people rows the survivor now holds twice, keeping the lowest id.

    ``people`` has no unique constraint, so the move above can leave the same person listed
    twice — the ``sec_edgar`` filing and the company's own team page name the same founder.
    Identity is exact: ``full_name`` plus ``title`` plus ``linkedin_url``, with NULLs folded to
    ``''`` so two rows that both lack a title still match. Never fuzzy — a near-match is a
    different person until a human says otherwise, which is the same rule SPEC §8 applies to
    companies. Deleting a byte-identical row loses no observation, and ``source_id`` is the only
    column that can differ; the lowest id (the first sighting) is the one kept.
    """
    twin = aliased(Person, name="earlier_person")
    duplicate = (
        select(twin.id)
        .where(
            twin.company_id == keep_id,
            twin.id < Person.id,
            twin.full_name == Person.full_name,
            func.coalesce(twin.title, "") == func.coalesce(Person.title, ""),
            func.coalesce(twin.linkedin_url, "") == func.coalesce(Person.linkedin_url, ""),
        )
        .correlate(Person)
        .exists()
    )
    return await _execute_rowcount(
        session, delete(Person).where(Person.company_id == keep_id, duplicate)
    )


async def _merge_user_notes(
    session: AsyncSession, *, keep_id: int, drop_id: int, now: datetime
) -> tuple[bool, str | None]:
    """``(moved, discarded note text)`` — the user's own writing, never destroyed silently.

    Three cases, and the third is the only one that loses anything: no note on the discarded
    company (nothing to do); a note there but none on the survivor (the row moves, keeping its
    status, rating and text); a note on both (the survivor's stands, and the other's text is
    returned so the caller can print it before the cascade takes the row). ``status`` and
    ``rating`` of a discarded note are not carried over — merging two pipeline states would be
    guessing, and the text is where the user's actual words are.

    "The survivor has a note" is a question about *content*, not about a row. ``save_note``
    (``web/routes/companies.py``) upserts unconditionally, so pressing Save with the form
    untouched — or clearing a note that had been written — leaves ``(status='none',
    rating=NULL, note=NULL)``: a row the UI itself renders exactly like no note at all. Letting
    that stub count as the survivor's note would silently destroy a real ``applied / 5`` on the
    other side *and* report nothing, since there is no text to hand back. So a content-free
    survivor row is deleted and the discarded row moves in its place; with nothing on either
    side there is no second pipeline state to guess between, which is what the paragraph above
    is protecting.
    """
    drop_note = (
        await session.execute(select(UserNote.note).where(UserNote.company_id == drop_id))
    ).first()
    if drop_note is None:
        return False, None
    keep_note = (
        await session.execute(
            select(UserNote.company_id).where(
                UserNote.company_id == keep_id,
                or_(
                    UserNote.status != enums.TrackingStatus.NONE,
                    UserNote.rating.is_not(None),
                    UserNote.note.is_not(None),
                ),
            )
        )
    ).first()
    if keep_note is not None:
        return False, drop_note.note
    # The survivor's row, if it has one at all, is the blank stub described above; it has to go
    # before the UPDATE below, which would otherwise collide with it on the primary key.
    await session.execute(delete(UserNote).where(UserNote.company_id == keep_id))
    await session.execute(
        update(UserNote)
        .where(UserNote.company_id == drop_id)
        .values(company_id=keep_id, updated_at=now)
    )
    return True, None


async def _repoint_merge_candidates(session: AsyncSession, *, keep_id: int, drop_id: int) -> int:
    """Point every *other* unresolved pair at the survivor instead of the discarded company.

    Shape — one ``UPDATE`` that recomputes both columns from the pair's *other* side::

        UPDATE merge_candidates
           SET company_id_a = least(:keep, other), company_id_b = greatest(:keep, other)
         WHERE (company_id_a = :drop OR company_id_b = :drop)
           AND company_id_a <> :keep AND company_id_b <> :keep
           AND resolved_at IS NULL
           AND NOT EXISTS (a row already holding that normalized pair)

    where ``other`` is the end of the pair that is not the discarded company. The
    ``least``/``greatest`` is what preserves the table's invariant that ``company_id_a <
    company_id_b`` (the pipeline writes pairs that way, and the unique constraint is on the
    ordered pair, so an un-normalized row would let the same pair be stored twice).

    Each of the other three clauses drops a row on purpose, and all three die with the company
    delete's cascade rather than moving:

    * The two ``<> :keep`` clauses skip the pair currently under review — and any other pair
      naming both companies — because repointing it would make it a self-pair.
    * ``resolved_at IS NULL`` keeps a *reviewed* pair from being carried over. A reviewer who
      marked ``(drop, third)`` "not a duplicate" said that about the discarded company; moving
      the verdict onto the survivor would assert something nobody decided, and it would stick
      for ever, because ``ingest.pipeline``'s candidate upsert refreshes a row only
      ``WHERE resolved_at IS NULL``. The pipeline is free to raise ``(keep, third)`` again on
      its own evidence, which is the honest outcome.
    * ``NOT EXISTS`` skips a pair the survivor already holds with the same third company, which
      would otherwise violate the unique constraint. Note the asymmetry this leaves: if that
      existing row is *resolved*, an unresolved suspicion about the discarded company is
      swallowed by the older verdict. That is the deliberate side of the trade — SPEC §8's
      "a reviewed pair must never be offered again" wins over re-raising it — and the constraint
      is on the pair regardless of resolution, so there is no third option that keeps both rows.
    """
    other = case(
        (MergeCandidate.company_id_a == drop_id, MergeCandidate.company_id_b),
        else_=MergeCandidate.company_id_a,
    )
    lower, upper = func.least(keep_id, other), func.greatest(keep_id, other)
    existing = aliased(MergeCandidate, name="existing_pair")
    duplicate = (
        select(existing.id)
        .where(existing.company_id_a == lower, existing.company_id_b == upper)
        .correlate(MergeCandidate)
        .exists()
    )
    return await _execute_rowcount(
        session,
        update(MergeCandidate)
        .where(
            or_(
                MergeCandidate.company_id_a == drop_id,
                MergeCandidate.company_id_b == drop_id,
            ),
            MergeCandidate.company_id_a != keep_id,
            MergeCandidate.company_id_b != keep_id,
            MergeCandidate.resolved_at.is_(None),
            ~duplicate,
        )
        .values(company_id_a=lower, company_id_b=upper),
    )
