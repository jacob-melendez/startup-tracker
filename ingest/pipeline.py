"""Run orchestration, priority-aware upserts, denormalized-column refresh and ``FetchRun``
bookkeeping (SPEC §11 ``ingest/pipeline.py``; §2, §5, §7.2, §8).

The pipeline is the only code that turns connector output into database rows:

* :func:`run_connector` drives one connector run — a ``FetchRun`` row is written before the
  first fetch and finalized whatever happens (SPEC §7.2 "every run writes a FetchRun row
  regardless of outcome"), every record is upserted in its own transaction so one bad
  filing never rolls back the others, and the denormalized columns are refreshed at the end
  of the run (SPEC §5);
* :func:`upsert_company_record` resolves one :class:`~ingest.base.CompanyRecord` to a company
  (SPEC §8, via :mod:`ingest.normalize`) and upserts it and its child rows — locations,
  sectors, funding rounds, investors, people, contacts, jobs;
* :func:`merge_company_fields` is the pure priority rule of SPEC §8: a field is overwritten
  only when the incoming value is non-null and its connector outranks the connector recorded
  in ``Company.field_provenance``.

Nothing here makes a network request (SPEC §2): connectors fetch, the pipeline writes.
Nothing here deletes *observed* data: a job that vanished from its source gets ``closed_at``
(SPEC §2, §5) and a probable duplicate becomes a ``MergeCandidate`` for ``cli.py merge-review``
(SPEC §8). The single exception is a superseded **constructed** contact, which was never
observed in the first place — see :func:`_ensure_constructed_contacts`.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from sqlalchemy import ColumnElement, CursorResult, Select, delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
    Location,
    MergeCandidate,
    Person,
    RoundInvestor,
    Sector,
    Source,
)
from db.queries import (
    close_missing_jobs,
    last_successful_run_started_at,
    load_company_targets,
    refresh_company_denormalized_columns,
)
from ingest.base import (
    CompanyRecord,
    CompanyTarget,
    Connector,
    ContactRecord,
    FetchContext,
    FundingRoundRecord,
    LocationRecord,
    PersonRecord,
)
from ingest.config import RegionsConfig, load_regions_config
from ingest.contacts import constructed_linkedin_company_url, people_search_url
from ingest.http import HttpClient, utc_now
from ingest.normalize import (
    ResolutionKey,
    normalize_domain,
    normalize_name,
    record_merge_candidates,
    resolve_company,
    slugify,
)
from logging_config import get_logger

log = get_logger(__name__)

#: SPEC §8 "Connector priority: sec_edgar > ycombinator > ATS APIs > company_site >
#: funding_rss > product_hunt", highest first (design decision 1). Anything not listed —
#: ``hn_hiring``, the seed loader, ``opencorporates``, test fakes — ranks below all of these
#: and equal to each other, so two unlisted connectors never overwrite one another.
CONNECTOR_PRIORITY: tuple[str, ...] = (
    "sec_edgar",
    "ycombinator",
    "greenhouse",
    "lever",
    "ashby",
    "workable",
    "company_site",
    "funding_rss",
    "product_hunt",
)

#: The ``Company`` columns that connectors may write and whose provenance is tracked in
#: ``Company.field_provenance`` (SPEC §8). ``normalized_name`` is derived from ``name`` and
#: ``last_seen_at`` is refreshed on every touch, so neither is a merge field.
COMPANY_MERGE_FIELDS: tuple[str, ...] = (
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

#: ``FetchRun.error_text`` is capped here (design brief step 5): enough to read the first
#: dozens of problems on ``/runs`` (SPEC §9) without storing a megabyte of tracebacks.
ERROR_TEXT_LIMIT = 4000


def connector_rank(connector: str) -> int:
    """Priority of ``connector`` (SPEC §8): the higher, the more authoritative.

    Listed connectors rank ``len(CONNECTOR_PRIORITY)`` (``sec_edgar``) down to ``1``
    (``product_hunt``); an unlisted connector ranks ``0`` (design decision 1).
    """
    try:
        return len(CONNECTOR_PRIORITY) - CONNECTOR_PRIORITY.index(connector)
    except ValueError:
        return 0


def _may_overwrite(stored: Any, owner: str | None, connector: str) -> bool:
    """Design decision 1: write when the stored value is NULL, or the stored field has no
    provenance, or the incoming connector is the owner (a connector refreshes its own values),
    or it strictly outranks the owner (SPEC §8 "outranks")."""
    if stored is None or owner is None or owner == connector:
        return True
    return connector_rank(connector) > connector_rank(owner)


def merge_company_fields(
    existing: Mapping[str, Any],
    provenance: Mapping[str, str],
    incoming: Mapping[str, Any],
    connector: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    """The priority-aware merge of SPEC §8, as a pure function: returns ``(updates to apply,
    field_provenance after)``.

    For every field in :data:`COMPANY_MERGE_FIELDS` an incoming ``None`` is "unknown" and
    never written (SPEC §8 "overwrite a field only when the incoming value is non-null");
    otherwise :func:`_may_overwrite` decides from the stored value and the connector recorded
    in ``provenance``. A field the connector is allowed to write becomes the connector's in the
    returned provenance even when the value happens to be unchanged, so the mapping always
    names the most authoritative connector that has confirmed the value; the update itself is
    emitted only when the value differs.

    ``domain`` is the canonical key (SPEC §5, §8 step 1) and is only ever *filled*, never
    changed: a non-null stored domain is kept whoever reports a different one (design
    decision 1). Whenever ``name`` is written, ``normalized_name`` (SPEC §8 step 2) is written
    with it.
    """
    updates: dict[str, Any] = {}
    after: dict[str, str] = dict(provenance)
    for field in COMPANY_MERGE_FIELDS:
        value = incoming.get(field)
        if value is None:
            continue
        stored = existing.get(field)
        if field == "domain" and stored is not None:
            continue
        if not _may_overwrite(stored, after.get(field), connector):
            continue
        if value != stored:
            updates[field] = value
        after[field] = connector
    if "name" in updates:
        updates["normalized_name"] = normalize_name(updates["name"])
    return updates, after


@dataclass(frozen=True, slots=True)
class UpsertResult:
    """What :func:`upsert_company_record` did: the company the record landed on, whether that
    company was created by this record, and how many ``MergeCandidate`` rows were written.

    ``skipped`` marks the one case where nothing was written at all: an ``enrich_only`` record
    (SPEC §4 Tier 2 #5) whose company does not exist yet. ``company_id`` is then ``None``.
    """

    company_id: int | None
    created: bool
    merge_candidates: int
    skipped: bool = False


def _rowcount(result: object) -> int:
    """``rowcount`` of an INSERT/UPDATE result (``AsyncSession.execute`` is typed as the
    generic ``Result``; DML always yields a ``CursorResult``)."""
    count: int = cast("CursorResult[Any]", result).rowcount
    return count


# ------------------------------------------------------------------- company row (SPEC §8)


def _record_fields(record: CompanyRecord, domain: str | None) -> dict[str, Any]:
    """The incoming merge fields, with ``domain`` already normalized (an unparseable domain is
    ``None``, see :class:`~ingest.base.CompanyRecord`)."""
    fields = {name: getattr(record, name) for name in COMPANY_MERGE_FIELDS}
    fields["domain"] = domain
    return fields


def _company_fields(company: Company) -> dict[str, Any]:
    return {name: getattr(company, name) for name in COMPANY_MERGE_FIELDS}


def _resolution_metro(locations: Sequence[LocationRecord]) -> str | None:
    """The record's resolution metro (design decision 5b): the HQ location's metro, else the
    first location's, ``None`` when the record has no location with a metro."""
    for location in locations:
        if location.is_hq and location.metro is not None:
            return location.metro
    for location in locations:
        if location.metro is not None:
            return location.metro
    return None


def _fill_metros(
    locations: Sequence[LocationRecord], regions: RegionsConfig
) -> list[LocationRecord]:
    """Design decision 5b: fill a missing ``metro`` from ``config/regions.yaml`` and adopt the
    configured spelling of city/state/country — and of the metro itself — so ``Location`` rows
    never split on casing. ``config/regions.yaml`` is the only source of region labels
    (CLAUDE.md): a connector's own label for a configured city (``"bay area"``) is replaced,
    never persisted, because the record's resolution metro must equal a stored
    ``Location.metro`` for SPEC §8 steps 2/3 to find anything. A connector-supplied metro is
    kept only for a city the config does not know. Locations still without a metro are
    dropped here with a debug log — ``Location.metro`` is NOT NULL and nothing outside the
    configured regions is stored (SPEC §1)."""
    filled: list[LocationRecord] = []
    for location in locations:
        configured = regions.lookup(location.city, location.state)
        if configured is not None:
            if location.metro is not None and location.metro != configured.metro:
                log.debug(
                    "connector metro label replaced by the configured one",
                    city=configured.city,
                    state=configured.state,
                    reported=location.metro,
                    metro=configured.metro,
                )
            location = location.model_copy(
                update={
                    "city": configured.city,
                    "state": configured.state,
                    "country": configured.country,
                    "metro": configured.metro,
                }
            )
        if location.metro is None:
            log.debug(
                "location skipped: no configured metro",
                city=location.city,
                state=location.state,
            )
            continue
        filled.append(location)
    return filled


async def _record_domain_conflict(
    session: AsyncSession, company_id: int, other_id: int, domain: str
) -> int:
    """Design decision 1: the incoming domain already belongs to a *different* company. The
    domain is not moved and nothing is merged (SPEC §8 "Do not auto-merge"); the pair is
    written as a ``MergeCandidate`` with ``similarity=1.0`` for ``cli.py merge-review``, with
    the same upsert semantics as :func:`ingest.normalize.record_merge_candidates`: re-runs
    refresh the row unless a reviewer has resolved it."""
    a, b = min(company_id, other_id), max(company_id, other_id)
    stmt = insert(MergeCandidate).values(
        company_id_a=a,
        company_id_b=b,
        similarity=1.0,
        reason=f"domain {domain} reported for both companies",
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[MergeCandidate.company_id_a, MergeCandidate.company_id_b],
        set_={"similarity": stmt.excluded.similarity, "reason": stmt.excluded.reason},
        where=MergeCandidate.resolved_at.is_(None),
    )
    return _rowcount(await session.execute(stmt))


async def _lock_resolution_key(session: AsyncSession, key: str) -> None:
    """Serialize every transaction that may resolve to the same company until the caller's
    transaction ends: ``pg_advisory_xact_lock`` keyed on a 64-bit hash of ``key``, released at
    commit or rollback, no row required.

    Why: entity resolution (SPEC §8) *reads* — "does this domain already belong to another
    company?", "is there a company with this name in this metro?" — and then writes. Under
    READ COMMITTED a concurrent run (the ATS connectors share one cadence in
    ``config/connectors.yaml``, and ``refresh --now`` can overlap a scheduled run) can insert
    a matching row in between, so without the lock:

    * with a domain, the write blocks on ``ix_companies_domain`` and fails with an
      ``IntegrityError`` that rolls back the whole record — jobs and rounds included — instead
      of taking the decision-1 path (domain not written, ``MergeCandidate`` recorded);
    * without one, both transactions pass step 2 and insert, leaving two companies with the
      same ``normalized_name`` in one metro and *no* ``MergeCandidate`` — step 3 excludes
      exact-equal names, so the duplicate is never even offered to ``merge-review``.

    The key is the normalized domain when there is one (the canonical key, SPEC §5) and
    otherwise the metro and normalized name that step 2 matches on. It is taken before
    ``resolve_company`` and before any ``SELECT ... FOR UPDATE``, so no transaction ever holds
    a row lock while waiting here (no lock-order cycle). A hash collision merely serializes
    two unrelated records.
    """
    await session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(key, 0))))


async def _update_company(
    session: AsyncSession,
    company_id: int,
    incoming: dict[str, Any],
    connector: str,
    *,
    now: datetime,
) -> int:
    """Apply the priority merge to an existing company under ``SELECT ... FOR UPDATE`` (two
    runs may touch one company at once). Returns the number of ``MergeCandidate`` rows written
    for a domain conflict."""
    company = await session.scalar(
        select(Company).where(Company.id == company_id).with_for_update()
    )
    if company is None:  # resolved a moment ago; only ``merge-review`` deletes companies
        msg = f"company {company_id} vanished between resolution and update"
        raise RuntimeError(msg)
    stored_provenance: dict[str, str] = dict(company.field_provenance)
    updates, provenance = merge_company_fields(
        _company_fields(company), stored_provenance, incoming, connector
    )
    candidates = 0
    incoming_domain = incoming.get("domain")
    if (
        incoming_domain is not None
        and company.domain is not None
        and incoming_domain != company.domain
    ):
        log.info(
            "stored domain kept",
            company_id=company.id,
            stored=company.domain,
            reported=incoming_domain,
            connector=connector,
        )
    if "domain" in updates:
        # Read-then-write is safe here: ``upsert_company_record`` holds the advisory lock of
        # :func:`_lock_resolution_key` on this domain for the whole transaction.
        other_id = await session.scalar(
            select(Company.id).where(Company.domain == updates["domain"], Company.id != company.id)
        )
        if other_id is not None:
            log.info(
                "domain belongs to another company; not moved",
                company_id=company.id,
                other_company_id=other_id,
                domain=updates["domain"],
                connector=connector,
            )
            del updates["domain"]
            if "domain" in stored_provenance:
                provenance["domain"] = stored_provenance["domain"]
            else:
                provenance.pop("domain", None)
            candidates += await _record_domain_conflict(
                session, company.id, other_id, incoming_domain or ""
            )
    for field, value in updates.items():
        setattr(company, field, value)
    # JSONB: assign a new dict so the ORM sees the change (in-place mutation is not tracked).
    company.field_provenance = provenance
    company.last_seen_at = now
    await session.flush()
    return candidates


async def _insert_company(
    session: AsyncSession, incoming: dict[str, Any], connector: str, *, now: datetime
) -> int | None:
    """Insert a new company; ``None`` when a concurrent writer inserted the same domain first
    (``ON CONFLICT (domain) DO NOTHING`` — the caller re-resolves and updates instead). Two
    pipeline transactions cannot race here any more (:func:`_lock_resolution_key`); the branch stays
    for writers that do not take the lock."""
    updates, provenance = merge_company_fields({}, {}, incoming, connector)
    stmt = insert(Company).values(
        **updates, field_provenance=provenance, first_seen_at=now, last_seen_at=now
    )
    if updates.get("domain") is not None:
        stmt = stmt.on_conflict_do_nothing(index_elements=[Company.domain])
    return await session.scalar(stmt.returning(Company.id))


# ------------------------------------------------------------------ child rows (SPEC §5)


async def _upsert_company_source(
    session: AsyncSession, company_id: int, connector: str, external_id: str | None, now: datetime
) -> None:
    """``CompanySource`` provenance (SPEC §5): one row per (company, connector); an incoming
    ``None`` external id never erases a known one."""
    stmt = insert(CompanySource).values(
        company_id=company_id, connector=connector, external_id=external_id, last_seen_at=now
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[CompanySource.company_id, CompanySource.connector],
        set_={
            "external_id": func.coalesce(stmt.excluded.external_id, CompanySource.external_id),
            "last_seen_at": stmt.excluded.last_seen_at,
        },
    )
    await session.execute(stmt)


async def _upsert_locations(
    session: AsyncSession, company_id: int, locations: Sequence[LocationRecord]
) -> None:
    """``Location`` get-or-create on ``(city, state)`` (``uq_locations_city_state``) and the
    ``CompanyLocation`` link; ``is_hq`` is sticky (existing OR incoming) so a connector that
    does not know which office is HQ never demotes one."""
    seen: set[tuple[str, str]] = set()
    for location in locations:
        key = (location.city, location.state)
        if key in seen:
            continue
        seen.add(key)
        stmt = (
            insert(Location)
            .values(
                city=location.city,
                state=location.state,
                metro=location.metro,
                country=location.country,
            )
            .on_conflict_do_nothing(constraint="uq_locations_city_state")
            .returning(Location.id)
        )
        location_id = await session.scalar(stmt)
        if location_id is None:
            location_id = await session.scalar(
                select(Location.id).where(
                    Location.city == location.city, Location.state == location.state
                )
            )
        if location_id is None:
            msg = f"location {key!r} could neither be inserted nor found"
            raise RuntimeError(msg)
        link = insert(CompanyLocation).values(
            company_id=company_id, location_id=location_id, is_hq=location.is_hq
        )
        link = link.on_conflict_do_update(
            index_elements=[CompanyLocation.company_id, CompanyLocation.location_id],
            set_={"is_hq": or_(CompanyLocation.is_hq, link.excluded.is_hq)},
        )
        await session.execute(link)


async def _upsert_sectors(session: AsyncSession, company_id: int, sectors: Sequence[str]) -> None:
    """``Sector`` get-or-create on ``slug`` (SPEC §5) and the ``CompanySector`` link. The
    first spelling seen becomes ``Sector.name``; later casings map to the same slug."""
    seen: set[str] = set()
    for name in sectors:
        slug = slugify(name)
        if not slug or slug in seen:
            continue
        seen.add(slug)
        stmt = (
            insert(Sector)
            .values(name=name, slug=slug)
            .on_conflict_do_nothing()
            .returning(Sector.id)
        )
        sector_id = await session.scalar(stmt)
        if sector_id is None:
            sector_id = await session.scalar(select(Sector.id).where(Sector.slug == slug))
        if sector_id is None:
            msg = f"sector {slug!r} could neither be inserted nor found"
            raise RuntimeError(msg)
        await session.execute(
            insert(CompanySector)
            .values(company_id=company_id, sector_id=sector_id)
            .on_conflict_do_nothing()
        )


async def _investor_id(session: AsyncSession, name: str) -> int:
    """``Investor`` get-or-create by ``name`` (``uq_investors_name``)."""
    stmt = (
        insert(Investor)
        .values(name=name)
        .on_conflict_do_nothing(index_elements=[Investor.name])
        .returning(Investor.id)
    )
    investor_id = await session.scalar(stmt)
    if investor_id is None:
        investor_id = await session.scalar(select(Investor.id).where(Investor.name == name))
    if investor_id is None:
        msg = f"investor {name!r} could neither be inserted nor found"
        raise RuntimeError(msg)
    return investor_id


async def _find_round(
    session: AsyncSession, company_id: int, connector: str, record: FundingRoundRecord
) -> FundingRound | None:
    """Round identity (design decision 3): ``(company_id, raw_payload->>'connector',
    raw_payload->>'external_id')`` — for EDGAR the Form D file number, shared by the original
    filing and its amendments — else ``(company_id, connector, round_type, announced_date)``."""
    # ``jsonb_extract_path_text(raw_payload, 'k')`` is ``raw_payload ->> 'k'`` with a typed
    # SQLAlchemy signature.
    connector_key = func.jsonb_extract_path_text(FundingRound.raw_payload, "connector")
    external_key = func.jsonb_extract_path_text(FundingRound.raw_payload, "external_id")
    stmt: Select[tuple[FundingRound]] = select(FundingRound).where(
        FundingRound.company_id == company_id, connector_key == connector
    )
    if record.external_id is not None:
        stmt = stmt.where(external_key == record.external_id)
    else:
        same_date: ColumnElement[bool] = (
            FundingRound.announced_date.is_(None)
            if record.announced_date is None
            else FundingRound.announced_date == record.announced_date
        )
        stmt = stmt.where(
            external_key.is_(None), FundingRound.round_type == record.round_type, same_date
        )
    found: FundingRound | None = await session.scalar(stmt.order_by(FundingRound.id).limit(1))
    return found


async def _upsert_funding_rounds(
    session: AsyncSession,
    company_id: int,
    connector: str,
    rounds: Sequence[FundingRoundRecord],
    source_id: int,
) -> None:
    """Funding rounds (SPEC §5) matched by :func:`_find_round`; an existing round is updated
    in place (an amendment with a new amount replaces the old one) — ``None`` and
    ``RoundType.UNKNOWN`` are "unknown" and never overwrite a known value. Investors are
    get-or-created by name and linked with ``is_lead``."""
    for record in rounds:
        payload: dict[str, Any] = {
            **record.raw_payload,
            "external_id": record.external_id,
            "connector": connector,
        }
        existing = await _find_round(session, company_id, connector, record)
        if existing is None:
            existing = FundingRound(
                company_id=company_id,
                round_type=record.round_type,
                amount_usd=record.amount_usd,
                announced_date=record.announced_date,
                source_id=source_id,
                raw_payload=payload,
                notes=record.notes,
            )
            session.add(existing)
        else:
            if record.round_type is not enums.RoundType.UNKNOWN:
                existing.round_type = record.round_type
            if record.amount_usd is not None:
                existing.amount_usd = record.amount_usd
            if record.announced_date is not None:
                existing.announced_date = record.announced_date
            if record.notes is not None:
                existing.notes = record.notes
            existing.source_id = source_id
            existing.raw_payload = payload
        await session.flush()
        for investor in record.investors:
            investor_id = await _investor_id(session, investor.name)
            link = insert(RoundInvestor).values(
                round_id=existing.id, investor_id=investor_id, is_lead=investor.is_lead
            )
            link = link.on_conflict_do_update(
                index_elements=[RoundInvestor.round_id, RoundInvestor.investor_id],
                set_={"is_lead": link.excluded.is_lead},
            )
            await session.execute(link)


async def _upsert_people(
    session: AsyncSession, company_id: int, people: Sequence[PersonRecord], source_id: int
) -> None:
    """People (SPEC §5) matched on ``(company_id, lower(full_name))``; non-null incoming
    fields update the row. Never sourced from LinkedIn profiles (SPEC §4, §6) — the pipeline
    stores what the connector found in a filing or on the company's own pages.

    Both sides of the match are lowercased by Postgres (``lower(full_name) = lower(:name)``).
    Python's ``str.lower()`` disagrees with the database's for locale-sensitive letters
    (Turkish dotted capital I, capital sharp S, and under a C locale every non-ASCII letter),
    so a key computed in Python would never match the stored row and every re-run of the
    same filing would add another ``Person``. A name repeated inside one record is
    deduplicated by the same database comparison: each person is flushed before the next
    lookup, so the repeat finds the row just written."""
    for record in people:
        person = await session.scalar(
            select(Person)
            .where(
                Person.company_id == company_id,
                func.lower(Person.full_name) == func.lower(record.full_name),
            )
            .order_by(Person.id)
            .limit(1)
        )
        if person is None:
            session.add(
                Person(
                    company_id=company_id,
                    full_name=record.full_name,
                    title=record.title,
                    role_type=record.role_type,
                    linkedin_url=record.linkedin_url,
                    source_id=source_id,
                )
            )
        else:
            if record.title is not None:
                person.title = record.title
            if record.role_type is not None:
                person.role_type = record.role_type
            if record.linkedin_url is not None:
                person.linkedin_url = record.linkedin_url
            person.source_id = source_id
        await session.flush()


async def _upsert_contacts(
    session: AsyncSession, company_id: int, contacts: Sequence[ContactRecord], source_id: int
) -> None:
    """Contacts (SPEC §5, §6) upserted on ``(company_id, kind, value)``. On conflict
    ``published`` beats ``constructed``: a stored published value is never downgraded to a
    constructed one, otherwise the row takes the incoming confidence and source."""
    for record in contacts:
        stmt = insert(Contact).values(
            company_id=company_id,
            kind=record.kind,
            value=record.value,
            confidence=record.confidence,
            source_id=source_id,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_contacts_company_id_kind_value",
            set_={"confidence": stmt.excluded.confidence, "source_id": stmt.excluded.source_id},
            where=or_(
                Contact.confidence != enums.ContactConfidence.PUBLISHED,
                stmt.excluded.confidence == enums.ContactConfidence.PUBLISHED,
            ),
        )
        await session.execute(stmt)


async def _insert_constructed_contact(
    session: AsyncSession, company_id: int, kind: enums.ContactKind, value: str
) -> None:
    """Write one ``confidence='constructed'`` contact, leaving an existing row alone.

    ``ON CONFLICT DO NOTHING`` rather than :func:`_upsert_contacts`' ``DO UPDATE``: the stored
    row for this exact value may already be ``published`` (a footer that links to precisely the
    URL we would have constructed, which is the common case — ``astranis.com`` links to
    ``linkedin.com/company/astranis``). Overwriting it would downgrade an observation to a
    guess, which SPEC §6 forbids in the one direction that matters.

    ``source_id`` is ``NULL`` by construction: nothing was fetched to produce this URL, so
    there is no ``Source`` row that could honestly be pointed at (SPEC §5 ``Source`` is "what
    was fetched, and when").
    """
    stmt = (
        insert(Contact)
        .values(
            company_id=company_id,
            kind=kind,
            value=value,
            confidence=enums.ContactConfidence.CONSTRUCTED,
            source_id=None,
        )
        .on_conflict_do_nothing(constraint="uq_contacts_company_id_kind_value")
    )
    await session.execute(stmt)


async def _drop_superseded_constructed(
    session: AsyncSession, company_id: int, kind: enums.ContactKind, *, keep: str | None
) -> int:
    """Delete this company's ``constructed`` contacts of ``kind``, except the value in ``keep``.

    Only ``constructed`` rows are ever in scope — a ``published`` row is an observation and is
    untouchable here (SPEC §2, §5). See :func:`_ensure_constructed_contacts` for why a derived
    row may be deleted at all.
    """
    stmt = delete(Contact).where(
        Contact.company_id == company_id,
        Contact.kind == kind,
        Contact.confidence == enums.ContactConfidence.CONSTRUCTED,
    )
    if keep is not None:
        stmt = stmt.where(Contact.value != keep)
    return _rowcount(await session.execute(stmt))


async def _ensure_constructed_contacts(session: AsyncSession, company_id: int) -> None:
    """The two deterministic SPEC §6 links every company gets, reconciled against what the
    company itself published.

    SPEC §6 is a rule about the company, not about one connector's page: *"if the company's own
    website footer links to LinkedIn, store that URL with ``confidence='published'``. Otherwise
    construct ``https://www.linkedin.com/company/{slug}`` from the domain"*, and *"store a
    people-search deep link"* for the user to click. So construction happens here, on **every
    company upsert** — including the thousands the Tier-3 ``company_site`` connector has never
    visited — rather than inside a connector that only ever sees the sites it was allowed to
    fetch.

    "Every upsert", not "every company in the database": this is the only call site and its only
    caller is :func:`upsert_company_record`, so a company gets these links when it is *upserted*,
    not because it exists. A database built under Phase 5 has them everywhere, because every row
    in ``companies`` is written by that upsert; one carried over from an earlier phase gains them
    company by company as each is next upserted, and nothing backfills the remainder (a row with
    no ``website_url`` is skipped by ``company_site`` on every run for ever). ``docs/SOURCES.md``
    states the same scope for the reader deciding whether to expect the links after an upgrade.

    The **stored** ``name`` and ``domain`` are read back rather than taken from the record: an
    ``enrich_only`` record (SPEC §4 Tier 2 #5) carries whatever name its feed used, while the
    row holds the name the highest-priority connector supplied (SPEC §8). The people search
    must be built from the latter, and after :func:`_update_company` has flushed, that is what
    the read returns.

    Order, per SPEC §6:

    1. ``linkedin_company`` — a ``published`` row of this kind means the "otherwise" branch does
       not apply, so nothing is constructed and any leftover constructed row is dropped. That is
       the ``sourcegraph.com`` case: its footer publishes ``/company/4803356``, a numeric id that
       can never equal the constructed ``/company/sourcegraph``, so without the drop a company
       would show two different LinkedIn pages and one of them would be wrong. When the published
       and the constructed URL happen to be identical, :func:`_upsert_contacts` has already
       promoted the single row in place and there is nothing left to drop.
       With no published row the constructed URL is written, and any *other* constructed row of
       this kind goes: the domain changed, and the old slug points at somebody else's company.
       A company with no usable domain gets no constructed URL and keeps whatever it has — a
       slug guessed from nothing is a link to a stranger.
    2. ``linkedin_people`` — the people-search deep link, rebuilt from the stored name, with any
       constructed row carrying a different value dropped (the name was corrected, and the old
       search finds the wrong company). ``published`` rows of this kind, if a connector ever
       writes one, are left alone.

    **Why deleting these rows does not violate "never delete on refresh" (SPEC §2, §5).** That
    rule protects *observed* data: a job that disappeared from its board is history, so it gets
    ``closed_at`` and stays queryable, and the same goes for every ``published`` contact — an
    address the company once printed was true when we saw it. A ``constructed`` row is not an
    observation at all. It is a pure function of the company's current ``name`` and ``domain``,
    recomputed on every upsert; when an input changes, the old output is not a historical fact
    but a dead search shortcut that silently sends the user to the wrong company. There is
    nothing to preserve and something to get wrong, so it is deleted rather than kept. Nothing
    in this function can touch a ``published`` row: the insert is ``DO NOTHING`` and every
    delete filters on ``confidence = 'constructed'``.
    """
    result = await session.execute(
        select(Company.name, Company.domain).where(Company.id == company_id)
    )
    stored = result.tuples().one_or_none()
    if stored is None:  # only ``merge-review`` deletes companies
        return
    name, domain = stored

    published_company_url = await session.scalar(
        select(Contact.id)
        .where(
            Contact.company_id == company_id,
            Contact.kind == enums.ContactKind.LINKEDIN_COMPANY,
            Contact.confidence == enums.ContactConfidence.PUBLISHED,
        )
        .limit(1)
    )
    company_url = (
        None if published_company_url is not None else constructed_linkedin_company_url(domain)
    )
    if company_url is not None:
        await _insert_constructed_contact(
            session, company_id, enums.ContactKind.LINKEDIN_COMPANY, company_url
        )
    if published_company_url is not None or company_url is not None:
        # Skipped entirely when there is neither a published URL nor a domain to build one from:
        # there is no better link to replace the stale one with, so keeping it loses nothing.
        dropped = await _drop_superseded_constructed(
            session, company_id, enums.ContactKind.LINKEDIN_COMPANY, keep=company_url
        )
        if dropped:
            log.info(
                "superseded constructed LinkedIn company link removed",
                company_id=company_id,
                removed=dropped,
                published=published_company_url is not None,
                kept=company_url,
            )

    # ``Company.name`` is NOT NULL but a connector may have supplied whitespace; a search for an
    # empty phrase matches every company in the city, which is worse than offering no link.
    if name.strip():
        people_url = people_search_url(name)
        await _insert_constructed_contact(
            session, company_id, enums.ContactKind.LINKEDIN_PEOPLE, people_url
        )
        dropped = await _drop_superseded_constructed(
            session, company_id, enums.ContactKind.LINKEDIN_PEOPLE, keep=people_url
        )
        if dropped:
            log.info(
                "superseded constructed people search removed",
                company_id=company_id,
                removed=dropped,
            )


async def _upsert_jobs(
    session: AsyncSession,
    company_id: int,
    connector: str,
    record: CompanyRecord,
    source_id: int,
    *,
    now: datetime,
) -> None:
    """Jobs (SPEC §2, §5; design decision 6): upsert on ``(company_id, external_id)``. A
    re-seen job gets ``last_seen_at = now`` and ``closed_at = NULL`` (it re-opened);
    ``first_seen_at`` is kept; nullable text fields keep their stored value when the incoming
    one is ``None``. Classification columns (SPEC §7.1) take the incoming values — the
    record already carries the fallback members, so every job is storable. When the record
    asserts ``jobs_complete``, this connector's other open jobs at the company are closed —
    never deleted."""
    for job in record.jobs:
        stmt = insert(Job).values(
            company_id=company_id,
            external_id=job.external_id,
            title=job.title,
            url=job.url,
            location_text=job.location_text,
            is_remote=job.is_remote,
            employment_type=job.employment_type,
            role_family=job.role_family,
            seniority=job.seniority,
            flexible_signal=job.flexible_signal,
            compensation_raw=job.compensation_raw,
            posted_at=job.posted_at,
            first_seen_at=now,
            last_seen_at=now,
            closed_at=None,
            description_raw=job.description_raw,
            raw_payload=job.raw_payload,
            source_id=source_id,
        )
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            constraint="uq_jobs_company_id_external_id",
            set_={
                "title": excluded.title,
                "url": func.coalesce(excluded.url, Job.url),
                "location_text": func.coalesce(excluded.location_text, Job.location_text),
                "is_remote": excluded.is_remote,
                "employment_type": excluded.employment_type,
                "role_family": excluded.role_family,
                "seniority": excluded.seniority,
                "flexible_signal": excluded.flexible_signal,
                "compensation_raw": func.coalesce(excluded.compensation_raw, Job.compensation_raw),
                "posted_at": func.coalesce(excluded.posted_at, Job.posted_at),
                "description_raw": func.coalesce(excluded.description_raw, Job.description_raw),
                "raw_payload": excluded.raw_payload,
                "source_id": excluded.source_id,
                "last_seen_at": excluded.last_seen_at,
                "closed_at": None,
            },
        )
        await session.execute(stmt)
    if record.jobs_complete:
        closed = await close_missing_jobs(
            session,
            company_id=company_id,
            connector=connector,
            keep_external_ids=[job.external_id for job in record.jobs],
            now=now,
        )
        if closed:
            log.info("jobs closed", company_id=company_id, connector=connector, closed=closed)


async def upsert_company_record(
    session: AsyncSession,
    record: CompanyRecord,
    connector: str,
    *,
    now: datetime,
    regions: RegionsConfig | None = None,
) -> UpsertResult:
    """Resolve ``record`` to a company (SPEC §8) and upsert it and its child rows, all in the
    caller's transaction.

    Order (design brief §3.3): normalize (``normalize_domain``, ``normalize_name``, metro from
    the HQ location else the first location, after :func:`_fill_metros`) → take the advisory
    lock on whatever this record resolves by (:func:`_lock_resolution_key`, so concurrent runs
    cannot race the domain-conflict check of decision 1 or the name match of SPEC §8 step 2)
    → ``resolve_company``
    → update under ``SELECT ... FOR UPDATE`` or insert (``ON CONFLICT (domain) DO NOTHING``;
    if the row was lost to a concurrent insert, re-resolve by domain and update) →
    ``CompanySource`` → one ``Source`` row per record per run that every child row points at
    (design decision 5) → locations → sectors → funding rounds and investors → people →
    contacts, then the constructed SPEC §6 links reconciled against them
    (:func:`_ensure_constructed_contacts`, which must run *after* ``_upsert_contacts`` so a
    LinkedIn URL published in this very record already counts) → jobs →
    ``MergeCandidate`` rows for the trigram candidates when the company was
    created (SPEC §8 step 3, never merged). ``last_seen_at = now`` on every touched company.

    ``regions`` defaults to ``config/regions.yaml`` (:func:`ingest.config.load_regions_config`);
    :func:`run_connector` passes the connector's own. ``now`` must be timezone-aware (design
    decision 8): the columns it fills are ``timestamptz`` and asyncpg reads a naive value as
    local time.
    """
    if now.tzinfo is None:
        msg = f"upsert_company_record(now=...) must be timezone-aware, got {now!r}"
        raise ValueError(msg)
    regions = regions if regions is not None else load_regions_config()
    domain = normalize_domain(record.domain)
    if record.domain is not None and domain is None:
        log.info("unparseable domain ignored", connector=connector, domain=record.domain)
    normalized_name = normalize_name(record.name)
    locations = _fill_metros(record.locations, regions)
    metro = _resolution_metro(locations)
    incoming = _record_fields(record, domain)
    # Whatever this record will resolve on: the domain, else the (metro, name) pair step 2
    # matches. A record with neither takes no lock — nothing can collide with it.
    lock_key = (
        domain if domain is not None else (f"{metro}\x1f{normalized_name}" if metro else None)
    )
    if lock_key is not None:
        await _lock_resolution_key(session, lock_key)

    resolution = await resolve_company(
        session,
        ResolutionKey(
            connector=connector,
            external_id=record.external_id,
            domain=domain,
            normalized_name=normalized_name,
            metro=metro,
            # Only an enrich-only record may be matched by name outside its own metro, and only
            # when the match is unique (design decision 4, ``ingest/normalize.py``).
            search_metros=(
                tuple(region.metro for region in regions.regions) if record.enrich_only else ()
            ),
        ),
    )
    created = False
    merge_candidates = 0
    company_id = resolution.company_id
    if company_id is None and record.enrich_only:
        # SPEC §4 Tier 2 #5: this connector may only enrich, never introduce a company.
        log.info(
            "enrich-only record skipped: no matching company",
            connector=connector,
            name=record.name,
            domain=domain,
        )
        return UpsertResult(None, created=False, merge_candidates=0, skipped=True)
    if company_id is None:
        company_id = await _insert_company(session, incoming, connector, now=now)
        if company_id is not None:
            created = True
        else:
            company_id = await session.scalar(select(Company.id).where(Company.domain == domain))
            if company_id is None:
                msg = f"company with domain {domain!r} could neither be inserted nor found"
                raise RuntimeError(msg)
            log.info("lost insert race; updating", company_id=company_id, domain=domain)
    if not created:
        merge_candidates += await _update_company(session, company_id, incoming, connector, now=now)

    await _upsert_company_source(session, company_id, connector, record.external_id, now)
    source = Source(connector=connector, url=record.source_url, fetched_at=now)
    session.add(source)
    await session.flush()

    await _upsert_locations(session, company_id, locations)
    await _upsert_sectors(session, company_id, record.sectors)
    await _upsert_funding_rounds(session, company_id, connector, record.funding_rounds, source.id)
    await _upsert_people(session, company_id, record.people, source.id)
    await _upsert_contacts(session, company_id, record.contacts, source.id)
    await _ensure_constructed_contacts(session, company_id)
    await _upsert_jobs(session, company_id, connector, record, source.id, now=now)
    if created and resolution.candidates:
        merge_candidates += await record_merge_candidates(
            session, company_id, resolution.candidates, metro=metro
        )
    log.debug(
        "company upserted",
        company_id=company_id,
        created=created,
        matched_by=resolution.matched_by,
        connector=connector,
        merge_candidates=merge_candidates,
    )
    return UpsertResult(company_id, created, merge_candidates)


# ------------------------------------------------------------- run orchestration (SPEC §7.2)


def _describe(exc: BaseException) -> str:
    """One exception as one line.

    :func:`join_error_messages` separates messages with newlines, so a message that carries
    its own newlines would be indistinguishable from several failures — and SQLAlchemy's
    ``DBAPIError`` (statement + parameters) and Pydantic's ``ValidationError`` (one block per
    field) both span many lines. Collapsing the whitespace keeps ``FetchRun.error_text``
    countable and keeps the ``/runs`` page (SPEC §9) readable. The full traceback is still
    logged at the point of failure.
    """
    return " ".join(f"{type(exc).__name__}: {exc}".split())


def join_error_messages(messages: Sequence[str], limit: int = ERROR_TEXT_LIMIT) -> str | None:
    """``FetchRun.error_text``: the messages joined by newlines, truncated to ``limit``
    characters with a ``"... (+N more)"`` suffix where ``N`` counts the messages that did not
    fit in full. ``None`` when there are no messages."""
    if not messages:
        return None
    joined = "\n".join(messages)
    if len(joined) <= limit:
        return joined
    kept = 0
    used = 0
    for index, message in enumerate(messages):
        suffix = f"... (+{len(messages) - index} more)"
        # ``used`` characters so far, a newline, the message, a newline, then the suffix.
        if used + (1 if kept else 0) + len(message) + 1 + len(suffix) > limit:
            break
        used += (1 if kept else 0) + len(message)
        kept += 1
    dropped = len(messages) - kept
    suffix = f"... (+{dropped} more)"
    if kept == 0:
        # The first message alone does not fit: keep its head and count the others.
        suffix = f"... (+{dropped - 1} more)"
        head = messages[0][: limit - len(suffix) - 1]
        return f"{head}\n{suffix}"
    return "\n".join([*messages[:kept], suffix])


async def _start_run(
    session_factory: async_sessionmaker[AsyncSession], connector: str, now: datetime
) -> tuple[int, datetime | None]:
    """Insert the ``FetchRun`` row in its own committed transaction — before anything is
    fetched, so a crash mid-run still leaves a row with the default ``error`` status (SPEC
    §7.2) — and read the incremental anchor (:func:`last_successful_run_started_at`)."""
    async with session_factory() as session, session.begin():
        last_success_at = await last_successful_run_started_at(session, connector)
        run = FetchRun(connector=connector, started_at=now)
        session.add(run)
        await session.flush()
        return run.id, last_success_at


async def _finish_run(
    session_factory: async_sessionmaker[AsyncSession],
    run_id: int,
    *,
    status: enums.FetchRunStatus,
    n_fetched: int,
    n_upserted: int,
    error_text: str | None,
) -> FetchRun:
    """Finalize the ``FetchRun`` row and return it detached (safe to read after commit)."""
    async with session_factory() as session, session.begin():
        run = await session.get(FetchRun, run_id)
        if run is None:  # only a manual delete could do this mid-run
            msg = f"fetch run {run_id} vanished during the run"
            raise RuntimeError(msg)
        run.finished_at = utc_now()
        run.status = status
        run.n_fetched = n_fetched
        run.n_upserted = n_upserted
        run.error_text = error_text
        await session.flush()
        await session.refresh(run)
        session.expunge(run)
        return run


class _TargetsUnavailable(Exception):
    """Internal: the connector's pre-loaded work list could not be read, so its fetch is not
    attempted. Raised and caught inside :func:`run_connector` only, so the run still ends with
    a finalized ``FetchRun`` (SPEC §7.2)."""


async def run_connector(
    connector: Connector[Any],
    *,
    session_factory: async_sessionmaker[AsyncSession],
    http: HttpClient,
    since: datetime | None = None,
    now: datetime | None = None,
) -> FetchRun:
    """Run one connector end to end and return its finalized ``FetchRun`` (SPEC §7.2 "every
    run writes a FetchRun row regardless of outcome").

    1. ``now = now or utc_now()``. The ``FetchRun`` is inserted and committed first (status
       stays the database default ``error`` until finalized) and ``last_success_at`` is read
       — the ``started_at`` of the newest earlier run whose fetch completed (``ok`` or
       ``partial``; see :func:`db.queries.last_successful_run_started_at`).
    2. A :class:`~ingest.base.FetchContext` is built with a logger bound to the connector and
       run id. When the connector sets ``wants_targets`` (design decision 5), the companies
       already in the database are loaded first (:func:`db.queries.load_company_targets`) and
       handed over in ``ctx.targets``, so no connector needs a session of its own.
    3. For every raw item ``connector.fetch`` yields, ``connector.to_records`` runs inside a
       try/except (a failing item is logged with ``exception``, its message recorded, and
       skipped) and each record is upserted in **its own transaction** so one bad record never
       rolls back the others; ``n_upserted`` counts records written. An ``enrich_only`` record
       whose company does not exist writes nothing (SPEC §4 Tier 2 #5) and counts as neither
       upserted nor touched — it is not an error, so it does not make the run ``partial``.
    4. If ``fetch`` itself raises, iteration stops and the run is ``error`` regardless of how
       much was upserted: the scan did not complete, so the next run must not advance its
       incremental window past this one.
    5. The denormalized columns of the touched companies are refreshed (SPEC §5;
       :func:`db.queries.refresh_company_denormalized_columns`); a failure there is recorded,
       never propagated. Then, always, the ``FetchRun`` is finalized: ``ok`` when the fetch
       completed with no item errors and no ``ctx.problems``; ``partial`` when it completed
       but there were item errors, problems or a refresh failure; ``error`` when ``fetch``
       raised or an unexpected exception escaped. ``error_text`` is the joined messages —
       fetch exception first, then item and refresh errors, then ``ctx.problems`` — capped at
       :data:`ERROR_TEXT_LIMIT` characters.
    6. Connector, record and refresh failures never raise out of here; only a failure to
       write the initial ``FetchRun`` (database down) propagates.
    """
    now = now if now is not None else utc_now()
    # Design decision 8: every timestamp is timezone-aware UTC. ``started_at``/``last_seen_at``
    # are ``timestamptz`` and asyncpg encodes a naive datetime as *local* time, so a naive
    # value here would silently shift every row the run writes by the host's UTC offset.
    for label, value in (("now", now), ("since", since)):
        if value is not None and value.tzinfo is None:
            msg = f"run_connector({label}=...) must be timezone-aware, got {value!r}"
            raise ValueError(msg)
    name = connector.name
    run_id, last_success_at = await _start_run(session_factory, name, now)
    run_log = log.bind(connector=name, run_id=run_id)
    targets: tuple[CompanyTarget, ...] = ()
    targets_error: str | None = None
    if connector.wants_targets:
        # Design decision 5: connectors that enrich rather than discover get their work list
        # from here, so ``ingest/base.py``'s "a connector never opens a session" still holds.
        # A failure here is recorded like any other, never raised: the ``FetchRun`` row is
        # already written and SPEC §7.2 requires it to be finalized.
        try:
            async with session_factory() as session:
                targets = tuple(await load_company_targets(session, name))
        except Exception as exc:
            run_log.exception("targets could not be loaded")
            targets_error = f"targets: {_describe(exc)}"
        else:
            run_log.info("targets loaded", targets=len(targets))
    ctx = FetchContext(
        http=http,
        now=now,
        since=since,
        last_success_at=last_success_at,
        run_id=run_id,
        log=run_log,
        targets=targets,
    )
    run_log.info("run started", since=since, last_success_at=last_success_at)

    n_fetched = 0
    n_upserted = 0
    n_skipped = 0
    touched: set[int] = set()
    # A work list that could not be loaded fails the run before a single request is made:
    # a connector handed no targets would otherwise look like a connector with nothing to do.
    fetch_error: str | None = targets_error
    item_errors: list[str] = []
    fetch_completed = False
    try:
        try:
            if fetch_error is not None:
                raise _TargetsUnavailable(fetch_error)
            async for raw in connector.fetch(ctx):
                n_fetched += 1
                try:
                    records = list(connector.to_records(raw))
                except Exception as exc:
                    run_log.exception("to_records failed", item=n_fetched)
                    item_errors.append(f"item {n_fetched}: to_records: {_describe(exc)}")
                    continue
                for record in records:
                    try:
                        async with session_factory() as session, session.begin():
                            result = await upsert_company_record(
                                session, record, name, now=now, regions=connector.regions
                            )
                    except Exception as exc:
                        run_log.exception("upsert failed", item=n_fetched, name=record.name)
                        item_errors.append(
                            f"item {n_fetched} ({record.name!r}): upsert: {_describe(exc)}"
                        )
                        continue
                    if result.skipped or result.company_id is None:
                        n_skipped += 1
                        continue
                    n_upserted += 1
                    touched.add(result.company_id)
            fetch_completed = True
        except _TargetsUnavailable:
            pass  # already logged and already in ``fetch_error``
        except Exception as exc:
            run_log.exception("fetch failed", n_fetched=n_fetched, n_upserted=n_upserted)
            fetch_error = f"fetch: {_describe(exc)}"

        try:
            async with session_factory() as session, session.begin():
                refreshed = await refresh_company_denormalized_columns(session, sorted(touched))
            run_log.debug("denormalized columns refreshed", companies=refreshed)
        except Exception as exc:
            run_log.exception("denormalized refresh failed", companies=len(touched))
            item_errors.append(f"denormalized refresh: {_describe(exc)}")
    finally:
        if fetch_error is not None or not fetch_completed:
            status = enums.FetchRunStatus.ERROR
        elif item_errors or ctx.problems:
            status = enums.FetchRunStatus.PARTIAL
        else:
            status = enums.FetchRunStatus.OK
        messages = [
            *([fetch_error] if fetch_error is not None else []),
            *item_errors,
            *ctx.problems,
        ]
        run = await _finish_run(
            session_factory,
            run_id,
            status=status,
            n_fetched=n_fetched,
            n_upserted=n_upserted,
            error_text=join_error_messages(messages),
        )
        summary = {
            "status": status.value,
            "n_fetched": n_fetched,
            "n_upserted": n_upserted,
            "n_skipped": n_skipped,
            "companies": len(touched),
            "item_errors": len(item_errors),
            "problems": len(ctx.problems),
            "http_requests": http.stats.requests,
        }
        if status is enums.FetchRunStatus.ERROR:
            run_log.error("run finished", **summary, error=fetch_error)
        else:
            run_log.info("run finished", **summary)
    return run
