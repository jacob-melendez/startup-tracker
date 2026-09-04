"""Connector contract and the typed records connectors hand to the pipeline (SPEC §11, §12).

A connector does exactly two things:

* :meth:`Connector.fetch` talks to one external source through the shared
  :class:`ingest.http.HttpClient` and yields raw items (a search hit plus its filing, one ATS
  job JSON, one RSS entry ...);
* :meth:`Connector.to_records` turns one raw item into zero or more :class:`CompanyRecord`
  values — plain data, validated by Pydantic, no database access.

Everything about the database is the pipeline's job (:mod:`ingest.pipeline`): entity resolution
(SPEC §8), priority-aware upserts through ``Company.field_provenance``, closing jobs that
disappeared from their source, the denormalized columns, and ``FetchRun`` bookkeeping. A
connector never opens a session, and nothing in this package is imported by a request handler
(SPEC §2).

Record conventions
------------------
* Records are frozen; collections are tuples (Pydantic converts lists).
* ``None`` means "unknown": the pipeline never overwrites a stored value with ``None``
  (SPEC §8), so a connector leaves out what it does not know rather than guessing.
* ``CompanyRecord.domain`` is raw text; the pipeline normalizes it with
  :func:`ingest.normalize.normalize_domain` and treats an unparseable value as ``None``.
* Classification (``role_family`` and friends, SPEC §7.1) is filled by ``ingest/classify.py``
  in Phase 3; the enum defaults (``other`` / ``unknown``) guarantee every job is storable now.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from db import enums
from ingest.config import ConnectorConfig, RegionsConfig

if TYPE_CHECKING:
    import structlog

    from ingest.http import HttpClient


class Record(BaseModel):
    """Base for every record: immutable, strict about field names, whitespace-trimmed."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)


class LocationRecord(Record):
    """One office. ``metro`` comes from ``config/regions.yaml`` (``None`` = outside every
    configured region); entity resolution scopes name matches to it (SPEC §8)."""

    city: str = Field(min_length=1)
    state: str = Field(min_length=1)
    country: str = "US"
    metro: str | None = None
    is_hq: bool = False


class InvestorRecord(Record):
    name: str = Field(min_length=1)
    is_lead: bool = False


class FundingRoundRecord(Record):
    """One financing event.

    ``external_id`` is the source's stable identifier for the *round* — for EDGAR the Form D
    file number, which the original filing and every amendment share — so a re-run or an
    amendment updates the existing row instead of adding one. The pipeline stores it, together
    with the connector name, inside ``raw_payload`` under the reserved keys ``external_id`` and
    ``connector`` and matches on them. With ``external_id=None`` the pipeline falls back to
    matching on (connector, round_type, announced_date).

    ``amount_usd=None`` means undisclosed (SPEC §5).
    """

    external_id: str | None = None
    round_type: enums.RoundType = enums.RoundType.UNKNOWN
    amount_usd: int | None = Field(default=None, ge=0)
    announced_date: date | None = None
    investors: tuple[InvestorRecord, ...] = ()
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    source_url: str | None = None


class PersonRecord(Record):
    """A named person. Never sourced from LinkedIn profiles (SPEC §4, §6); ``linkedin_url`` is
    only ever a URL the company itself published."""

    full_name: str = Field(min_length=1)
    title: str | None = None
    role_type: enums.RoleType | None = None
    linkedin_url: str | None = None


class ContactRecord(Record):
    """SPEC §6: ``confidence`` says whether the company published the value or we built it."""

    kind: enums.ContactKind
    value: str = Field(min_length=1)
    confidence: enums.ContactConfidence


class JobRecord(Record):
    """One open role at the company. ``external_id`` is unique per company at the source
    (SPEC §5 ``uq_jobs_company_id_external_id``).

    ``posted_at`` must carry a timezone. ``jobs.posted_at`` is ``timestamptz`` and asyncpg
    encodes a naive value as *local* time, so a connector building it from an epoch or a
    date-only string would shift it by the host's UTC offset and quietly corrupt both the
    column and the ``latest_job_posted_at`` sort (SPEC §9). Rejecting it here makes that a
    validation error the run records instead (design decision 8).
    """

    external_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    url: str | None = None
    location_text: str | None = None
    is_remote: bool = False
    employment_type: enums.EmploymentType = enums.EmploymentType.UNKNOWN
    role_family: enums.RoleFamily = enums.RoleFamily.OTHER
    seniority: enums.Seniority = enums.Seniority.UNKNOWN
    flexible_signal: bool = False
    compensation_raw: str | None = None
    posted_at: AwareDatetime | None = None
    description_raw: str | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class CompanyRecord(Record):
    """Everything one connector learned about one company from one fetch.

    ``external_id`` is the connector's own identifier for the company (EDGAR CIK, YC slug, ATS
    board token) and lands in ``CompanySource.external_id``; a later run of the same connector
    resolves the company through it before any name or domain matching (SPEC §8 step 0, see
    ``ingest/normalize.py``). ``source_url`` is where the data came from (``Source.url``).

    ``jobs_complete=True`` asserts that ``jobs`` is the company's *entire* open-job list at the
    source, which lets the pipeline set ``closed_at`` on jobs from this connector that are no
    longer listed (SPEC §2, §5 — never deleted). Connectors that see only a slice of the jobs
    (an HN comment, a single posting) leave it ``False``.
    """

    name: str = Field(min_length=1)
    external_id: str | None = None
    source_url: str | None = None
    domain: str | None = None
    website_url: str | None = None
    one_liner: str | None = None
    thesis: str | None = None
    founded_year: int | None = Field(default=None, ge=1800, le=2100)
    employee_est: int | None = Field(default=None, ge=0)
    stage: enums.Stage | None = None
    status: enums.CompanyStatus | None = None
    ats_provider: enums.AtsProvider | None = None
    ats_token: str | None = None
    locations: tuple[LocationRecord, ...] = ()
    sectors: tuple[str, ...] = ()
    funding_rounds: tuple[FundingRoundRecord, ...] = ()
    people: tuple[PersonRecord, ...] = ()
    contacts: tuple[ContactRecord, ...] = ()
    jobs: tuple[JobRecord, ...] = ()
    jobs_complete: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchContext:
    """What the pipeline hands a connector for one run.

    ``now`` is injected (UTC) so tests control time; ``since`` is an explicit lower bound from
    the CLI (``--since``), ``None`` meaning the connector applies its own incremental logic from
    ``last_success_at`` (the start of this connector's last successful run) or its configured
    backfill when there is none.

    ``problems`` is the connector's channel for non-fatal trouble — a filing that 404ed, a
    search window that hit the API's result cap. Append a one-line description and carry on;
    the pipeline records the run as ``partial`` and puts the messages in
    ``FetchRun.error_text`` (SPEC §7.2) so breakage is visible on ``/runs`` without aborting.
    """

    http: HttpClient
    now: datetime
    since: datetime | None
    last_success_at: datetime | None
    run_id: int
    log: structlog.stdlib.BoundLogger
    problems: list[str] = field(default_factory=list)


class Connector[RawT](ABC):
    """Base class for every connector (SPEC §11 ``ingest/base.py``).

    Subclasses set ``name`` (the key in ``config/connectors.yaml``, ``FetchRun.connector``,
    ``CompanySource.connector``) and implement :meth:`fetch` and :meth:`to_records`. ``cadence``
    is the cron expression from ``config/connectors.yaml`` — the only place a schedule is
    defined (SPEC §7.2) — and ``None`` for on-demand-only connectors.

    ``RawT`` is whatever :meth:`fetch` yields; keeping fetching and mapping separate means the
    mapping is unit-testable from recorded fixtures without any HTTP at all.
    """

    name: ClassVar[str]

    def __init__(self, config: ConnectorConfig, regions: RegionsConfig) -> None:
        if config.name != self.name:
            msg = f"{type(self).__name__} is {self.name!r} but was given config for {config.name!r}"
            raise ValueError(msg)
        self.config = config
        self.regions = regions

    @property
    def cadence(self) -> str | None:
        """Cron expression (UTC) from ``config/connectors.yaml``; ``None`` = on demand only."""
        return self.config.cadence

    @abstractmethod
    def fetch(self, ctx: FetchContext) -> AsyncIterator[RawT]:
        """Yield raw items from the source. All network I/O goes through ``ctx.http``."""

    @abstractmethod
    def to_records(self, raw: RawT) -> Iterable[CompanyRecord]:
        """Map one raw item to company records. Pure: no I/O, no database."""
