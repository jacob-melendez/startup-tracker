"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-09-03 00:00:00+00:00

Hand-reviewed initial schema: every table, enum, index, and constraint in SPEC §5, plus the
``pg_trgm`` extension (§5 "Search indexing").

Review notes — what autogenerate does not produce and was added or checked by hand:

* ``CREATE EXTENSION pg_trgm`` runs first; the ``gin_trgm_ops`` indexes need it.
* Every enum type is created and dropped explicitly (the models declare
  ``create_type=False``), so ``downgrade()`` is symmetric and leaves no types behind.
* ``companies.latest_round_id -> funding_rounds.id`` forms a cycle with
  ``funding_rounds.company_id -> companies.id``; the first is added with ``ALTER TABLE``
  after both tables exist (``use_alter=True`` on the model) and dropped first on downgrade.
* The two generated ``tsvector`` columns and the expression indexes
  (``DESC NULLS LAST``, ``DESC``) are written verbatim from ``db/models.py``.
* Constraint names follow ``db.models.NAMING_CONVENTION``; ``tests/test_migration.py``
  asserts every name and that the models and this schema agree.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen snapshot of db.enums.PG_ENUMS at this revision (member order matters to Postgres).
ENUMS: dict[str, tuple[str, ...]] = {
    "stage": (
        "pre_seed",
        "seed",
        "series_a",
        "series_b",
        "series_c",
        "series_d_plus",
        "growth",
        "public",
        "unknown",
    ),
    "company_status": ("active", "acquired", "dead"),
    "ats_provider": ("greenhouse", "lever", "ashby", "workable"),
    "round_type": (
        "pre_seed",
        "seed",
        "series_a",
        "series_b",
        "series_c",
        "series_d_plus",
        "convertible_note",
        "safe",
        "debt",
        "grant",
        "other",
        "unknown",
    ),
    "role_type": ("founder", "exec", "recruiter", "eng_lead"),
    "contact_kind": ("email", "linkedin_company", "careers_page", "x", "github", "contact_form"),
    "contact_confidence": ("published", "constructed"),
    "employment_type": (
        "full_time",
        "part_time",
        "contract",
        "internship",
        "co_op",
        "temporary",
        "unknown",
    ),
    "role_family": (
        "software",
        "infrastructure",
        "ml_ai",
        "data",
        "hardware",
        "robotics",
        "research",
        "product",
        "design",
        "security",
        "qa",
        "sales",
        "marketing",
        "bizops",
        "finance",
        "people",
        "operations",
        "legal",
        "other",
    ),
    "seniority": (
        "intern",
        "new_grad",
        "junior",
        "mid",
        "senior",
        "staff",
        "principal",
        "lead",
        "manager",
        "director",
        "executive",
        "unknown",
    ),
    "fetch_run_status": ("ok", "partial", "error"),
    "tracking_status": ("none", "interested", "applied", "in_process", "rejected", "offer"),
}

# Creation order respects foreign keys; downgrade drops in reverse.
TABLES: tuple[str, ...] = (
    "sources",
    "companies",
    "locations",
    "company_locations",
    "sectors",
    "company_sectors",
    "funding_rounds",
    "investors",
    "round_investors",
    "people",
    "contacts",
    "jobs",
    "company_sources",
    "fetch_runs",
    "merge_candidates",
    "user_notes",
    "job_bookmarks",
    "saved_searches",
)

COMPANY_SEARCH_EXPR = (
    "to_tsvector('english', coalesce(name, '') || ' ' || coalesce(one_liner, '') "
    "|| ' ' || coalesce(thesis, ''))"
)
JOB_SEARCH_EXPR = (
    "to_tsvector('english', coalesce(title, '') || ' ' || coalesce(description_raw, ''))"
)

TIMESTAMPTZ = sa.DateTime(timezone=True)
NOW = sa.text("now()")
FALSE = sa.text("false")
ZERO = sa.text("0")


def _enum(name: str) -> postgresql.ENUM:
    return postgresql.ENUM(*ENUMS[name], name=name, create_type=False)


def _id() -> sa.Column[int]:
    return sa.Column("id", sa.Integer(), sa.Identity(), nullable=False)


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    bind = op.get_bind()
    for name in ENUMS:
        _enum(name).create(bind, checkfirst=True)

    # ---------------------------------------------------------------- operational: sources
    op.create_table(
        "sources",
        _id(),
        sa.Column("connector", sa.String(), nullable=False),
        sa.Column("url", sa.String(), nullable=True),
        sa.Column("fetched_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_sources"),
    )

    # -------------------------------------------------------------------- core: companies
    op.create_table(
        "companies",
        _id(),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("normalized_name", sa.String(), nullable=False),
        sa.Column("domain", sa.String(), nullable=True),
        sa.Column("website_url", sa.String(), nullable=True),
        sa.Column("one_liner", sa.Text(), nullable=True),
        sa.Column("thesis", sa.Text(), nullable=True),
        sa.Column("founded_year", sa.SmallInteger(), nullable=True),
        sa.Column("employee_est", sa.Integer(), nullable=True),
        sa.Column("stage", _enum("stage"), server_default="unknown", nullable=False),
        sa.Column("status", _enum("company_status"), server_default="active", nullable=False),
        sa.Column("ats_provider", _enum("ats_provider"), nullable=True),
        sa.Column("ats_token", sa.String(), nullable=True),
        # FK to funding_rounds added below (cycle).
        sa.Column("latest_round_id", sa.Integer(), nullable=True),
        sa.Column("latest_job_posted_at", TIMESTAMPTZ, nullable=True),
        sa.Column("open_job_count", sa.Integer(), server_default=ZERO, nullable=False),
        sa.Column(
            "field_provenance",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("first_seen_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.Column("last_seen_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed(COMPANY_SEARCH_EXPR, persisted=True),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_companies"),
    )
    op.create_index("ix_companies_domain", "companies", ["domain"], unique=True)
    op.create_index("ix_companies_stage", "companies", ["stage"])
    op.create_index(
        "ix_companies_latest_job_posted_at",
        "companies",
        [sa.text("latest_job_posted_at DESC NULLS LAST")],
    )
    op.create_index(
        "ix_companies_search_vector", "companies", ["search_vector"], postgresql_using="gin"
    )
    op.create_index(
        "ix_companies_normalized_name_trgm",
        "companies",
        ["normalized_name"],
        postgresql_using="gin",
        postgresql_ops={"normalized_name": "gin_trgm_ops"},
    )
    op.create_index(
        "ix_companies_name_trgm",
        "companies",
        ["name"],
        postgresql_using="gin",
        postgresql_ops={"name": "gin_trgm_ops"},
    )

    # ------------------------------------------------------------------- core: locations
    op.create_table(
        "locations",
        _id(),
        sa.Column("city", sa.String(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("metro", sa.String(), nullable=False),
        sa.Column("country", sa.String(), server_default="US", nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_locations"),
        sa.UniqueConstraint("city", "state", name="uq_locations_city_state"),
    )
    op.create_table(
        "company_locations",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("location_id", sa.Integer(), nullable=False),
        sa.Column("is_hq", sa.Boolean(), server_default=FALSE, nullable=False),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_company_locations_company_id_companies",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["locations.id"],
            name="fk_company_locations_location_id_locations",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("company_id", "location_id", name="pk_company_locations"),
    )
    op.create_index("ix_company_locations_location_id", "company_locations", ["location_id"])

    # --------------------------------------------------------------------- core: sectors
    op.create_table(
        "sectors",
        _id(),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("slug", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_sectors"),
        sa.UniqueConstraint("name", name="uq_sectors_name"),
        sa.UniqueConstraint("slug", name="uq_sectors_slug"),
    )
    op.create_table(
        "company_sectors",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("sector_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_company_sectors_company_id_companies",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["sector_id"],
            ["sectors.id"],
            name="fk_company_sectors_sector_id_sectors",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("company_id", "sector_id", name="pk_company_sectors"),
    )
    op.create_index("ix_company_sectors_sector_id", "company_sectors", ["sector_id"])

    # ------------------------------------------------------------------- core: funding
    op.create_table(
        "funding_rounds",
        _id(),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("round_type", _enum("round_type"), server_default="unknown", nullable=False),
        sa.Column("amount_usd", sa.BigInteger(), nullable=True),
        sa.Column("announced_date", sa.Date(), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_funding_rounds_company_id_companies",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name="fk_funding_rounds_source_id_sources",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_funding_rounds"),
    )
    op.create_index(
        "ix_funding_rounds_company_id_announced_date",
        "funding_rounds",
        ["company_id", sa.text("announced_date DESC")],
    )
    op.create_table(
        "investors",
        _id(),
        sa.Column("name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_investors"),
        sa.UniqueConstraint("name", name="uq_investors_name"),
    )
    op.create_table(
        "round_investors",
        sa.Column("round_id", sa.Integer(), nullable=False),
        sa.Column("investor_id", sa.Integer(), nullable=False),
        sa.Column("is_lead", sa.Boolean(), server_default=FALSE, nullable=False),
        sa.ForeignKeyConstraint(
            ["round_id"],
            ["funding_rounds.id"],
            name="fk_round_investors_round_id_funding_rounds",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["investor_id"],
            ["investors.id"],
            name="fk_round_investors_investor_id_investors",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("round_id", "investor_id", name="pk_round_investors"),
    )

    # ------------------------------------------------------------ core: people, contacts
    op.create_table(
        "people",
        _id(),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("full_name", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=True),
        sa.Column("role_type", _enum("role_type"), nullable=True),
        sa.Column("linkedin_url", sa.String(), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_people_company_id_companies",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"], ["sources.id"], name="fk_people_source_id_sources", ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_people"),
    )
    op.create_table(
        "contacts",
        _id(),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("kind", _enum("contact_kind"), nullable=False),
        sa.Column("value", sa.String(), nullable=False),
        sa.Column("confidence", _enum("contact_confidence"), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_contacts_company_id_companies",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"], ["sources.id"], name="fk_contacts_source_id_sources", ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_contacts"),
        sa.UniqueConstraint(
            "company_id", "kind", "value", name="uq_contacts_company_id_kind_value"
        ),
    )

    # ------------------------------------------------------------------------ core: jobs
    op.create_table(
        "jobs",
        _id(),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("external_id", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("url", sa.String(), nullable=True),
        sa.Column("location_text", sa.String(), nullable=True),
        sa.Column("is_remote", sa.Boolean(), server_default=FALSE, nullable=False),
        sa.Column(
            "employment_type", _enum("employment_type"), server_default="unknown", nullable=False
        ),
        sa.Column("role_family", _enum("role_family"), server_default="other", nullable=False),
        sa.Column("seniority", _enum("seniority"), server_default="unknown", nullable=False),
        sa.Column("flexible_signal", sa.Boolean(), server_default=FALSE, nullable=False),
        sa.Column("compensation_raw", sa.String(), nullable=True),
        sa.Column("posted_at", TIMESTAMPTZ, nullable=True),
        sa.Column("first_seen_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.Column("last_seen_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.Column("closed_at", TIMESTAMPTZ, nullable=True),
        sa.Column("description_raw", sa.Text(), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed(JOB_SEARCH_EXPR, persisted=True),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_jobs_company_id_companies",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"], ["sources.id"], name="fk_jobs_source_id_sources", ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
        sa.UniqueConstraint("company_id", "external_id", name="uq_jobs_company_id_external_id"),
    )
    op.create_index("ix_jobs_company_id", "jobs", ["company_id"])
    op.create_index("ix_jobs_role_family_closed_at", "jobs", ["role_family", "closed_at"])
    op.create_index("ix_jobs_employment_type_closed_at", "jobs", ["employment_type", "closed_at"])
    op.create_index("ix_jobs_posted_at", "jobs", [sa.text("posted_at DESC")])
    op.create_index("ix_jobs_search_vector", "jobs", ["search_vector"], postgresql_using="gin")

    # ------------------------------------------------------------- operational: the rest
    op.create_table(
        "company_sources",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("connector", sa.String(), nullable=False),
        sa.Column("external_id", sa.String(), nullable=True),
        sa.Column("last_seen_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_company_sources_company_id_companies",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("company_id", "connector", name="pk_company_sources"),
    )
    op.create_table(
        "fetch_runs",
        _id(),
        sa.Column("connector", sa.String(), nullable=False),
        sa.Column("started_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.Column("finished_at", TIMESTAMPTZ, nullable=True),
        sa.Column("status", _enum("fetch_run_status"), server_default="error", nullable=False),
        sa.Column("n_fetched", sa.Integer(), server_default=ZERO, nullable=False),
        sa.Column("n_upserted", sa.Integer(), server_default=ZERO, nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_fetch_runs"),
    )
    op.create_table(
        "merge_candidates",
        _id(),
        sa.Column("company_id_a", sa.Integer(), nullable=False),
        sa.Column("company_id_b", sa.Integer(), nullable=False),
        sa.Column("similarity", sa.Float(), nullable=False),
        sa.Column("reason", sa.String(), nullable=False),
        sa.Column("resolved_at", TIMESTAMPTZ, nullable=True),
        sa.ForeignKeyConstraint(
            ["company_id_a"],
            ["companies.id"],
            name="fk_merge_candidates_company_id_a_companies",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["company_id_b"],
            ["companies.id"],
            name="fk_merge_candidates_company_id_b_companies",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_merge_candidates"),
        sa.UniqueConstraint(
            "company_id_a", "company_id_b", name="uq_merge_candidates_company_id_a_company_id_b"
        ),
    )

    # ------------------------------------------------------------- personal tracking
    op.create_table(
        "user_notes",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("status", _enum("tracking_status"), server_default="none", nullable=False),
        sa.Column("rating", sa.SmallInteger(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("updated_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        # op.f(): final name; otherwise the "ck" naming convention would wrap it again.
        sa.CheckConstraint("rating BETWEEN 1 AND 5", name=op.f("ck_user_notes_rating_range")),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_user_notes_company_id_companies",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("company_id", name="pk_user_notes"),
    )
    op.create_table(
        "job_bookmarks",
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("starred", sa.Boolean(), server_default=FALSE, nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_job_bookmarks_job_id_jobs", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("job_id", name="pk_job_bookmarks"),
    )
    op.create_table(
        "saved_searches",
        _id(),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("query_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_saved_searches"),
        sa.UniqueConstraint("name", name="uq_saved_searches_name"),
    )

    # ---------------------------------------------- close the companies <-> rounds cycle
    op.create_foreign_key(
        "fk_companies_latest_round_id_funding_rounds",
        "companies",
        "funding_rounds",
        ["latest_round_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_companies_latest_round_id_funding_rounds", "companies", type_="foreignkey"
    )
    for table in reversed(TABLES):
        op.drop_table(table)

    bind = op.get_bind()
    for name in reversed(ENUMS):
        _enum(name).drop(bind, checkfirst=True)

    op.execute("DROP EXTENSION IF EXISTS pg_trgm")
