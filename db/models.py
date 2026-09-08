"""SQLAlchemy 2.0 typed declarative models — every table in SPEC §5.

Conventions
-----------
* Alembic owns the DDL (SPEC §3, §5). Tests apply the migrations; nothing calls
  ``Base.metadata.create_all``.
* Postgres-native types only (SPEC §3): JSONB for raw payloads, native ENUM types for
  controlled vocabularies (:mod:`db.enums`), generated ``tsvector`` columns with GIN
  indexes for search, and ``pg_trgm`` GIN indexes for fuzzy entity resolution (§5, §8).
* Denormalized columns on :class:`Company` — ``latest_round_id``, ``latest_job_posted_at``,
  ``open_job_count`` — are maintained by the ingest pipeline at the end of each run, not by
  database triggers (SPEC §5). Triggers would hide the write path and make partial runs
  harder to reason about; the pipeline already knows when a run is complete.
* Every index and constraint is named explicitly (see ``NAMING_CONVENTION``) so the migration
  and the tests can refer to them by name.
* Timestamps are ``timestamptz``; the database supplies ``now()`` defaults.
* Parent -> child relationships pair ``cascade="all, delete-orphan"`` with
  ``passive_deletes=True``: loaded children are deleted by the ORM, unloaded ones by the
  database's ``ON DELETE CASCADE`` (companies are only ever deleted by ``merge-review``).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Computed,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Index,
    MetaData,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ENUM, JSONB, TSVECTOR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from db import enums

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# Search expressions (SPEC §5 "Search indexing"). The two-argument ``to_tsvector`` is
# IMMUTABLE, which a generated column requires; the one-argument form is only STABLE.
COMPANY_SEARCH_EXPR = (
    "to_tsvector('english', coalesce(name, '') || ' ' || coalesce(one_liner, '') "
    "|| ' ' || coalesce(thesis, ''))"
)
JOB_SEARCH_EXPR = (
    "to_tsvector('english', coalesce(title, '') || ' ' || coalesce(description_raw, ''))"
)


def pg_enum(type_name: str) -> ENUM:
    """Column type for the native Postgres ENUM ``type_name`` (see ``db.enums.PG_ENUMS``).

    ``create_type=False``: table DDL never creates the type implicitly. The migration creates
    and drops every enum type explicitly, so upgrade and downgrade stay symmetric.
    """
    return ENUM(
        enums.PG_ENUMS[type_name],
        name=type_name,
        create_type=False,
        values_callable=lambda members: [m.value for m in members],
    )


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    type_annotation_map = {datetime: DateTime(timezone=True)}


# --------------------------------------------------------------------------- core


class Company(Base):
    """A startup. Canonical key is the normalized ``domain`` (SPEC §5, §8).

    ``domain`` is nullable because §8 step 2 resolves domain-less records by
    ``normalized_name`` within a metro; the unique index still enforces one row per domain
    (Postgres treats NULLs as distinct).
    """

    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    name: Mapped[str]
    normalized_name: Mapped[str]
    domain: Mapped[str | None]
    website_url: Mapped[str | None]
    one_liner: Mapped[str | None] = mapped_column(Text)
    thesis: Mapped[str | None] = mapped_column(Text)
    founded_year: Mapped[int | None] = mapped_column(SmallInteger)
    employee_est: Mapped[int | None]
    stage: Mapped[enums.Stage] = mapped_column(
        pg_enum("stage"), server_default=enums.Stage.UNKNOWN.value
    )
    status: Mapped[enums.CompanyStatus] = mapped_column(
        pg_enum("company_status"), server_default=enums.CompanyStatus.ACTIVE.value
    )
    ats_provider: Mapped[enums.AtsProvider | None] = mapped_column(pg_enum("ats_provider"))
    ats_token: Mapped[str | None]
    # Denormalized (SPEC §5) — refreshed by the ingest pipeline at end of run, never by triggers.
    latest_round_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "funding_rounds.id",
            ondelete="SET NULL",
            use_alter=True,
            name="fk_companies_latest_round_id_funding_rounds",
        )
    )
    latest_job_posted_at: Mapped[datetime | None]
    open_job_count: Mapped[int] = mapped_column(server_default=text("0"))
    # SPEC §8: field name -> connector that last wrote it, for priority-aware upserts.
    field_provenance: Mapped[dict[str, Any]] = mapped_column(
        JSONB, server_default=text("'{}'::jsonb")
    )
    first_seen_at: Mapped[datetime] = mapped_column(server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(server_default=func.now())
    search_vector: Mapped[str | None] = mapped_column(
        TSVECTOR, Computed(COMPANY_SEARCH_EXPR, persisted=True)
    )

    latest_round: Mapped[FundingRound | None] = relationship(
        foreign_keys=[latest_round_id], post_update=True
    )
    funding_rounds: Mapped[list[FundingRound]] = relationship(
        back_populates="company",
        foreign_keys="FundingRound.company_id",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    location_links: Mapped[list[CompanyLocation]] = relationship(
        back_populates="company", cascade="all, delete-orphan", passive_deletes=True
    )
    locations: Mapped[list[Location]] = relationship(secondary="company_locations", viewonly=True)
    sector_links: Mapped[list[CompanySector]] = relationship(
        back_populates="company", cascade="all, delete-orphan", passive_deletes=True
    )
    sectors: Mapped[list[Sector]] = relationship(secondary="company_sectors", viewonly=True)
    people: Mapped[list[Person]] = relationship(
        back_populates="company", cascade="all, delete-orphan", passive_deletes=True
    )
    contacts: Mapped[list[Contact]] = relationship(
        back_populates="company", cascade="all, delete-orphan", passive_deletes=True
    )
    jobs: Mapped[list[Job]] = relationship(
        back_populates="company", cascade="all, delete-orphan", passive_deletes=True
    )
    source_links: Mapped[list[CompanySource]] = relationship(
        back_populates="company", cascade="all, delete-orphan", passive_deletes=True
    )
    user_note: Mapped[UserNote | None] = relationship(
        back_populates="company", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        Index("ix_companies_domain", "domain", unique=True),
        Index("ix_companies_stage", "stage"),
        Index("ix_companies_search_vector", "search_vector", postgresql_using="gin"),
        Index(
            "ix_companies_normalized_name_trgm",
            "normalized_name",
            postgresql_using="gin",
            postgresql_ops={"normalized_name": "gin_trgm_ops"},
        ),
        Index(
            "ix_companies_name_trgm",
            "name",
            postgresql_using="gin",
            postgresql_ops={"name": "gin_trgm_ops"},
        ),
    )


class Location(Base):
    """A city. ``metro`` is the expansion hook (SPEC §5): its value comes from
    ``config/regions.yaml`` at ingest time — there is no database default on purpose, so no
    region-specific logic lives in the schema and adding a metro is a config edit, not a
    migration."""

    __tablename__ = "locations"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    city: Mapped[str]
    state: Mapped[str]
    metro: Mapped[str]
    country: Mapped[str] = mapped_column(server_default="US")

    __table_args__ = (UniqueConstraint("city", "state", name="uq_locations_city_state"),)


class CompanyLocation(Base):
    __tablename__ = "company_locations"

    company_id: Mapped[int] = mapped_column(
        ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True
    )
    location_id: Mapped[int] = mapped_column(
        ForeignKey("locations.id", ondelete="CASCADE"), primary_key=True
    )
    is_hq: Mapped[bool] = mapped_column(server_default=text("false"))

    company: Mapped[Company] = relationship(back_populates="location_links")
    location: Mapped[Location] = relationship()

    __table_args__ = (Index("ix_company_locations_location_id", "location_id"),)


class Sector(Base):
    __tablename__ = "sectors"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    name: Mapped[str]
    slug: Mapped[str]

    __table_args__ = (
        UniqueConstraint("name", name="uq_sectors_name"),
        UniqueConstraint("slug", name="uq_sectors_slug"),
    )


class CompanySector(Base):
    __tablename__ = "company_sectors"

    company_id: Mapped[int] = mapped_column(
        ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True
    )
    sector_id: Mapped[int] = mapped_column(
        ForeignKey("sectors.id", ondelete="CASCADE"), primary_key=True
    )

    company: Mapped[Company] = relationship(back_populates="sector_links")
    sector: Mapped[Sector] = relationship()

    # Not in the SPEC §5 index list; added so the sector filter (§9) does not scan the join
    # table, mirroring ``ix_company_locations_location_id``.
    __table_args__ = (Index("ix_company_sectors_sector_id", "sector_id"),)


class FundingRound(Base):
    __tablename__ = "funding_rounds"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"))
    round_type: Mapped[enums.RoundType] = mapped_column(
        pg_enum("round_type"), server_default=enums.RoundType.UNKNOWN.value
    )
    amount_usd: Mapped[int | None] = mapped_column(BigInteger)  # NULL = undisclosed
    announced_date: Mapped[date | None]
    source_id: Mapped[int | None] = mapped_column(ForeignKey("sources.id", ondelete="SET NULL"))
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    notes: Mapped[str | None] = mapped_column(Text)

    company: Mapped[Company] = relationship(
        back_populates="funding_rounds", foreign_keys=[company_id]
    )
    source: Mapped[Source | None] = relationship()
    investor_links: Mapped[list[RoundInvestor]] = relationship(
        back_populates="round", cascade="all, delete-orphan", passive_deletes=True
    )
    investors: Mapped[list[Investor]] = relationship(secondary="round_investors", viewonly=True)


class Investor(Base):
    __tablename__ = "investors"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    name: Mapped[str]

    __table_args__ = (UniqueConstraint("name", name="uq_investors_name"),)


class RoundInvestor(Base):
    __tablename__ = "round_investors"

    round_id: Mapped[int] = mapped_column(
        ForeignKey("funding_rounds.id", ondelete="CASCADE"), primary_key=True
    )
    investor_id: Mapped[int] = mapped_column(
        ForeignKey("investors.id", ondelete="CASCADE"), primary_key=True
    )
    is_lead: Mapped[bool] = mapped_column(server_default=text("false"))

    round: Mapped[FundingRound] = relationship(back_populates="investor_links")
    investor: Mapped[Investor] = relationship()


class Person(Base):
    """A named person at a company (founder, exec, recruiter, eng lead). Never sourced from
    LinkedIn profiles (SPEC §4, §6) — only from filings, directories, and the company's own
    published pages."""

    __tablename__ = "people"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"))
    full_name: Mapped[str]
    title: Mapped[str | None]
    role_type: Mapped[enums.RoleType | None] = mapped_column(pg_enum("role_type"))
    linkedin_url: Mapped[str | None]
    source_id: Mapped[int | None] = mapped_column(ForeignKey("sources.id", ondelete="SET NULL"))

    company: Mapped[Company] = relationship(back_populates="people")
    source: Mapped[Source | None] = relationship()


class Contact(Base):
    """A way to reach a company. ``confidence`` separates addresses the company published
    from URLs we constructed (SPEC §6)."""

    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"))
    kind: Mapped[enums.ContactKind] = mapped_column(pg_enum("contact_kind"))
    value: Mapped[str]
    confidence: Mapped[enums.ContactConfidence] = mapped_column(pg_enum("contact_confidence"))
    source_id: Mapped[int | None] = mapped_column(ForeignKey("sources.id", ondelete="SET NULL"))

    company: Mapped[Company] = relationship(back_populates="contacts")
    source: Mapped[Source | None] = relationship()

    __table_args__ = (
        UniqueConstraint("company_id", "kind", "value", name="uq_contacts_company_id_kind_value"),
    )


class Job(Base):
    """An open (or historical) role. Rows are never deleted on refresh: a job missing from its
    source gets ``closed_at`` (SPEC §2, §5). Classification columns default to the fallback
    members so an unclassified role is always storable (SPEC §7.1)."""

    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"))
    external_id: Mapped[str]
    title: Mapped[str]
    url: Mapped[str | None]
    location_text: Mapped[str | None]
    is_remote: Mapped[bool] = mapped_column(server_default=text("false"))
    employment_type: Mapped[enums.EmploymentType] = mapped_column(
        pg_enum("employment_type"), server_default=enums.EmploymentType.UNKNOWN.value
    )
    role_family: Mapped[enums.RoleFamily] = mapped_column(
        pg_enum("role_family"), server_default=enums.RoleFamily.OTHER.value
    )
    seniority: Mapped[enums.Seniority] = mapped_column(
        pg_enum("seniority"), server_default=enums.Seniority.UNKNOWN.value
    )
    # SPEC §7.1: student-compatibility hint. A badge and an opt-in filter, never a default one.
    flexible_signal: Mapped[bool] = mapped_column(server_default=text("false"))
    compensation_raw: Mapped[str | None]
    posted_at: Mapped[datetime | None]
    first_seen_at: Mapped[datetime] = mapped_column(server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(server_default=func.now())
    closed_at: Mapped[datetime | None]
    description_raw: Mapped[str | None] = mapped_column(Text)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    source_id: Mapped[int | None] = mapped_column(ForeignKey("sources.id", ondelete="SET NULL"))
    search_vector: Mapped[str | None] = mapped_column(
        TSVECTOR, Computed(JOB_SEARCH_EXPR, persisted=True)
    )

    company: Mapped[Company] = relationship(back_populates="jobs")
    source: Mapped[Source | None] = relationship()
    bookmark: Mapped[JobBookmark | None] = relationship(
        back_populates="job", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        UniqueConstraint("company_id", "external_id", name="uq_jobs_company_id_external_id"),
        Index("ix_jobs_company_id", "company_id"),
        Index("ix_jobs_role_family_closed_at", "role_family", "closed_at"),
        Index("ix_jobs_employment_type_closed_at", "employment_type", "closed_at"),
        Index("ix_jobs_search_vector", "search_vector", postgresql_using="gin"),
    )


# --------------------------------------------------------------------- operational


class Source(Base):
    """One fetch of one URL by one connector — provenance for rounds, people, contacts, jobs."""

    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    connector: Mapped[str]
    url: Mapped[str | None]
    fetched_at: Mapped[datetime] = mapped_column(server_default=func.now())


class CompanySource(Base):
    """Which connectors have seen a company, under what external id, and when (SPEC §5)."""

    __tablename__ = "company_sources"

    company_id: Mapped[int] = mapped_column(
        ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True
    )
    connector: Mapped[str] = mapped_column(primary_key=True)
    external_id: Mapped[str | None]
    last_seen_at: Mapped[datetime] = mapped_column(server_default=func.now())

    company: Mapped[Company] = relationship(back_populates="source_links")


class FetchRun(Base):
    """One connector run. Every run writes a row regardless of outcome (SPEC §7.2).

    ``status`` defaults to ``error`` so a run that crashes before finishing is recorded as
    such; the pipeline sets ``ok``/``partial`` when it completes."""

    __tablename__ = "fetch_runs"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    connector: Mapped[str]
    started_at: Mapped[datetime] = mapped_column(server_default=func.now())
    finished_at: Mapped[datetime | None]
    status: Mapped[enums.FetchRunStatus] = mapped_column(
        pg_enum("fetch_run_status"), server_default=enums.FetchRunStatus.ERROR.value
    )
    n_fetched: Mapped[int] = mapped_column(server_default=text("0"))
    n_upserted: Mapped[int] = mapped_column(server_default=text("0"))
    error_text: Mapped[str | None] = mapped_column(Text)


class MergeCandidate(Base):
    """A probable duplicate pair found by trigram similarity (SPEC §8). Never auto-merged;
    resolved by ``cli.py merge-review``."""

    __tablename__ = "merge_candidates"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    company_id_a: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"))
    company_id_b: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"))
    similarity: Mapped[float] = mapped_column(Float)
    reason: Mapped[str]
    resolved_at: Mapped[datetime | None]

    company_a: Mapped[Company] = relationship(foreign_keys=[company_id_a])
    company_b: Mapped[Company] = relationship(foreign_keys=[company_id_b])

    __table_args__ = (
        # Not in SPEC §5; lets the pipeline upsert a pair instead of appending one per run.
        UniqueConstraint(
            "company_id_a", "company_id_b", name="uq_merge_candidates_company_id_a_company_id_b"
        ),
    )


# --------------------------------------------------------------- personal tracking


class UserNote(Base):
    """The user's status, rating, and note for a company — one row per company (SPEC §5)."""

    __tablename__ = "user_notes"

    company_id: Mapped[int] = mapped_column(
        ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True
    )
    status: Mapped[enums.TrackingStatus] = mapped_column(
        pg_enum("tracking_status"), server_default=enums.TrackingStatus.NONE.value
    )
    rating: Mapped[int | None] = mapped_column(SmallInteger)
    note: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    company: Mapped[Company] = relationship(back_populates="user_note")

    # The "ck" naming convention expands the name to ck_user_notes_rating_range.
    __table_args__ = (CheckConstraint("rating BETWEEN 1 AND 5", name="rating_range"),)


class JobBookmark(Base):
    """A starred job with an optional note — one row per job (SPEC §5)."""

    __tablename__ = "job_bookmarks"

    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True)
    starred: Mapped[bool] = mapped_column(server_default=text("false"))
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    job: Mapped[Job] = relationship(back_populates="bookmark")


class SavedSearch(Base):
    __tablename__ = "saved_searches"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    name: Mapped[str]
    query_json: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    __table_args__ = (UniqueConstraint("name", name="uq_saved_searches_name"),)


# ----------------------------------------------------------- expression indexes
# Declared after the classes so the column expressions can be used directly (SPEC §5).

Index(
    "ix_companies_latest_job_posted_at",
    Company.latest_job_posted_at.desc().nulls_last(),
)
Index("ix_jobs_posted_at", Job.posted_at.desc())
Index(
    "ix_funding_rounds_company_id_announced_date",
    FundingRound.company_id,
    FundingRound.announced_date.desc(),
)
