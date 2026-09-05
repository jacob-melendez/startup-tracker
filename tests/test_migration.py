"""The migrations produce exactly the schema SPEC §5 and §6 describe.

Every assertion is against the live Postgres catalog after ``alembic upgrade head``:
tables, enum types and their members, indexes (including the expression, GIN, and trigram
ones), constraints, the generated ``tsvector`` columns, the ``pg_trgm`` extension — and
that ``db/models.py`` agrees with what the migrations built. Expected names are written out
literally rather than derived from the models so a change to either side is caught. Revision
``0002`` (SPEC §6's ``contact_kind.linkedin_people``) is exercised both ways, since Postgres
cannot drop an enum label and its downgrade has to rebuild the type.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from db.enums import PG_ENUMS, ContactKind
from db.models import Base

EXPECTED_TABLES = {
    # core
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
    # operational
    "sources",
    "company_sources",
    "fetch_runs",
    "merge_candidates",
    # personal tracking
    "user_notes",
    "job_bookmarks",
    "saved_searches",
}

EXPECTED_ENUM_TYPES = {
    "stage",
    "company_status",
    "ats_provider",
    "round_type",
    "role_type",
    "contact_kind",
    "contact_confidence",
    "employment_type",
    "role_family",
    "seniority",
    "fetch_run_status",
    "tracking_status",
}

# contact_kind as revision 0001 created it — what 0002's downgrade must leave behind.
CONTACT_KIND_AT_0001 = [
    "email",
    "linkedin_company",
    "careers_page",
    "x",
    "github",
    "contact_form",
]

ROLE_FAMILIES = (
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
)

# index name -> fragments that must appear in its pg_indexes.indexdef
EXPECTED_INDEXES: dict[str, tuple[str, ...]] = {
    "ix_companies_domain": ("CREATE UNIQUE INDEX",),
    "ix_companies_stage": ("(stage)",),
    "ix_companies_latest_job_posted_at": ("latest_job_posted_at DESC NULLS LAST",),
    "ix_companies_search_vector": ("USING gin (search_vector)",),
    "ix_companies_normalized_name_trgm": ("USING gin (normalized_name gin_trgm_ops)",),
    "ix_companies_name_trgm": ("USING gin (name gin_trgm_ops)",),
    "ix_company_locations_location_id": ("(location_id)",),
    "ix_company_sectors_sector_id": ("(sector_id)",),
    "ix_funding_rounds_company_id_announced_date": ("(company_id, announced_date DESC)",),
    "ix_jobs_company_id": ("(company_id)",),
    "ix_jobs_role_family_closed_at": ("(role_family, closed_at)",),
    "ix_jobs_employment_type_closed_at": ("(employment_type, closed_at)",),
    "ix_jobs_posted_at": ("(posted_at DESC)",),
    "ix_jobs_search_vector": ("USING gin (search_vector)",),
}

EXPECTED_UNIQUE_CONSTRAINTS = {
    "uq_locations_city_state",
    "uq_sectors_name",
    "uq_sectors_slug",
    "uq_investors_name",
    "uq_contacts_company_id_kind_value",
    "uq_jobs_company_id_external_id",
    "uq_merge_candidates_company_id_a_company_id_b",
    "uq_saved_searches_name",
}

EXPECTED_FOREIGN_KEYS = {
    "fk_companies_latest_round_id_funding_rounds",
    "fk_company_locations_company_id_companies",
    "fk_company_locations_location_id_locations",
    "fk_company_sectors_company_id_companies",
    "fk_company_sectors_sector_id_sectors",
    "fk_funding_rounds_company_id_companies",
    "fk_funding_rounds_source_id_sources",
    "fk_round_investors_round_id_funding_rounds",
    "fk_round_investors_investor_id_investors",
    "fk_people_company_id_companies",
    "fk_people_source_id_sources",
    "fk_contacts_company_id_companies",
    "fk_contacts_source_id_sources",
    "fk_jobs_company_id_companies",
    "fk_jobs_source_id_sources",
    "fk_company_sources_company_id_companies",
    "fk_merge_candidates_company_id_a_companies",
    "fk_merge_candidates_company_id_b_companies",
    "fk_user_notes_company_id_companies",
    "fk_job_bookmarks_job_id_jobs",
}


async def _rows(conn: AsyncConnection, sql: str) -> Sequence[Any]:
    return (await conn.execute(text(sql))).all()


async def _public_tables(conn: AsyncConnection) -> set[str]:
    rows = await _rows(
        conn,
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
    )
    return {r.table_name for r in rows}


async def _public_enums(conn: AsyncConnection) -> dict[str, list[str]]:
    rows = await _rows(
        conn,
        "SELECT t.typname, e.enumlabel FROM pg_type t "
        "JOIN pg_enum e ON e.enumtypid = t.oid "
        "JOIN pg_namespace n ON n.oid = t.typnamespace "
        "WHERE n.nspname = 'public' ORDER BY t.typname, e.enumsortorder",
    )
    enums: dict[str, list[str]] = {}
    for r in rows:
        enums.setdefault(r.typname, []).append(r.enumlabel)
    return enums


async def _extensions(conn: AsyncConnection) -> set[str]:
    return {r.extname for r in await _rows(conn, "SELECT extname FROM pg_extension")}


# ------------------------------------------------------------------------------ tests


async def test_pg_trgm_extension_is_installed(conn: AsyncConnection) -> None:
    assert "pg_trgm" in await _extensions(conn)


async def test_every_table_exists(conn: AsyncConnection) -> None:
    tables = await _public_tables(conn)
    assert tables - {"alembic_version"} == EXPECTED_TABLES
    assert set(Base.metadata.tables) == EXPECTED_TABLES


async def test_every_enum_type_exists_with_its_members(conn: AsyncConnection) -> None:
    enums = await _public_enums(conn)
    assert set(enums) == EXPECTED_ENUM_TYPES
    assert set(PG_ENUMS) == EXPECTED_ENUM_TYPES
    for type_name, python_enum in PG_ENUMS.items():
        assert enums[type_name] == [m.value for m in python_enum], type_name
    # SPEC §7.1: all 19 role families, ``other`` as the fallback.
    assert enums["role_family"] == list(ROLE_FAMILIES)
    assert len(ROLE_FAMILIES) == 19
    assert enums["employment_type"] == [
        "full_time", "part_time", "contract", "internship", "co_op", "temporary", "unknown",
    ]  # fmt: skip
    assert enums["seniority"] == [
        "intern", "new_grad", "junior", "mid", "senior", "staff", "principal", "lead",
        "manager", "director", "executive", "unknown",
    ]  # fmt: skip
    assert enums["company_status"] == ["active", "acquired", "dead"]
    assert enums["ats_provider"] == ["greenhouse", "lever", "ashby", "workable"]
    assert enums["role_type"] == ["founder", "exec", "recruiter", "eng_lead"]
    # SPEC §6: ``linkedin_people`` (revision 0002) sorts straight after ``linkedin_company``.
    assert enums["contact_kind"] == [
        "email", "linkedin_company", "linkedin_people",
        "careers_page", "x", "github", "contact_form",
    ]  # fmt: skip
    assert enums["contact_confidence"] == ["published", "constructed"]
    assert enums["fetch_run_status"] == ["ok", "partial", "error"]
    assert enums["tracking_status"] == [
        "none", "interested", "applied", "in_process", "rejected", "offer",
    ]  # fmt: skip


async def test_every_index_exists_with_the_right_definition(conn: AsyncConnection) -> None:
    rows = await _rows(
        conn, "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'"
    )
    indexes = {r.indexname: r.indexdef for r in rows}
    secondary = {name for name in indexes if name.startswith("ix_")}
    assert secondary == set(EXPECTED_INDEXES)
    for name, fragments in EXPECTED_INDEXES.items():
        for fragment in fragments:
            assert fragment in indexes[name], f"{name}: {indexes[name]}"
    # SPEC §5: default sort index and trigram + full-text GIN indexes.
    assert indexes["ix_companies_latest_job_posted_at"].endswith(
        "USING btree (latest_job_posted_at DESC NULLS LAST)"
    )
    assert indexes["ix_companies_domain"].startswith("CREATE UNIQUE INDEX")


async def test_primary_key_unique_and_check_constraints(conn: AsyncConnection) -> None:
    # pg_constraint.contype is the single-byte "char" type; cast it or asyncpg returns bytes.
    rows = await _rows(
        conn,
        "SELECT c.conname, c.contype::text AS contype, c.conrelid::regclass::text AS table_name "
        "FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
        "WHERE n.nspname = 'public'",
    )
    by_type: dict[str, set[str]] = {}
    for r in rows:
        by_type.setdefault(r.contype, set()).add(r.conname)
    assert by_type["p"] == {f"pk_{table}" for table in EXPECTED_TABLES} | {"alembic_version_pkc"}
    assert by_type["u"] == EXPECTED_UNIQUE_CONSTRAINTS
    assert by_type["c"] == {"ck_user_notes_rating_range"}
    # Composite primary keys on the join / one-row-per-parent tables.
    pk_columns = {
        r.table_name: r.columns
        for r in await _rows(
            conn,
            "SELECT c.conrelid::regclass::text AS table_name, "
            "array_to_string(array_agg(a.attname ORDER BY a.attnum), ',') AS columns "
            "FROM pg_constraint c JOIN pg_attribute a "
            "ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey) "
            "WHERE c.contype = 'p' GROUP BY c.conrelid",
        )
    }
    assert pk_columns["company_locations"] == "company_id,location_id"
    assert pk_columns["company_sectors"] == "company_id,sector_id"
    assert pk_columns["round_investors"] == "round_id,investor_id"
    assert pk_columns["company_sources"] == "company_id,connector"
    assert pk_columns["user_notes"] == "company_id"
    assert pk_columns["job_bookmarks"] == "job_id"


async def test_foreign_keys(conn: AsyncConnection) -> None:
    rows = await _rows(
        conn,
        "SELECT conname, confdeltype::text AS confdeltype FROM pg_constraint WHERE contype = 'f'",
    )
    fks = {r.conname: r.confdeltype for r in rows}
    assert set(fks) == EXPECTED_FOREIGN_KEYS
    # Children of a company cascade; provenance and the denormalized round pointer null out.
    assert fks["fk_jobs_company_id_companies"] == "c"
    assert fks["fk_companies_latest_round_id_funding_rounds"] == "n"
    assert fks["fk_jobs_source_id_sources"] == "n"


async def test_generated_tsvector_columns(conn: AsyncConnection) -> None:
    rows = await _rows(
        conn,
        "SELECT table_name, data_type, is_generated, generation_expression "
        "FROM information_schema.columns "
        "WHERE table_schema = 'public' AND column_name = 'search_vector' ORDER BY table_name",
    )
    assert [r.table_name for r in rows] == ["companies", "jobs"]
    for r in rows:
        assert r.data_type == "tsvector", r
        assert r.is_generated == "ALWAYS", r
        assert "to_tsvector('english'" in r.generation_expression, r
    by_table = {r.table_name: r.generation_expression for r in rows}
    for column in ("name", "one_liner", "thesis"):
        assert column in by_table["companies"]
    for column in ("title", "description_raw"):
        assert column in by_table["jobs"]


async def test_columns_defined_outside_spec_section_5_exist(conn: AsyncConnection) -> None:
    rows = await _rows(
        conn,
        "SELECT table_name, column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns WHERE table_schema = 'public' "
        "AND (table_name, column_name) IN "
        "(('companies', 'field_provenance'), ('jobs', 'flexible_signal'))",
    )
    cols = {(r.table_name, r.column_name): r for r in rows}
    provenance = cols[("companies", "field_provenance")]  # SPEC §8
    assert provenance.data_type == "jsonb" and provenance.is_nullable == "NO"
    flexible = cols[("jobs", "flexible_signal")]  # SPEC §7.1
    assert flexible.data_type == "boolean" and flexible.is_nullable == "NO"
    assert flexible.column_default == "false"


async def test_classification_columns_default_to_the_fallback_members(
    conn: AsyncConnection,
) -> None:
    """SPEC §7.1: an unclassified role is still storable — the schema itself guarantees it."""
    rows = await _rows(
        conn,
        "SELECT column_name, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_name = 'jobs' "
        "AND column_name IN ('role_family', 'employment_type', 'seniority')",
    )
    defaults = {r.column_name: (r.is_nullable, r.column_default) for r in rows}
    assert defaults["role_family"] == ("NO", "'other'::role_family")
    assert defaults["employment_type"] == ("NO", "'unknown'::employment_type")
    assert defaults["seniority"] == ("NO", "'unknown'::seniority")


def test_orm_mappers_configure() -> None:
    """Relationship wiring (FK cycle, viewonly secondaries, one-to-ones) is valid. No DB needed."""
    Base.registry.configure()


async def test_models_agree_with_the_migrated_schema(conn: AsyncConnection) -> None:
    """Alembic autogenerate sees no difference between db/models.py and the database."""

    def _diff(sync_conn: Connection) -> list[Any]:
        ctx = MigrationContext.configure(
            sync_conn, opts={"compare_type": True, "compare_server_default": True}
        )
        return list(compare_metadata(ctx, Base.metadata))

    assert await conn.run_sync(_diff) == []


def test_revision_0002_places_linkedin_people_and_downgrades_by_rebuilding_the_type(
    alembic_config: Config, migrated_database: str
) -> None:
    """SPEC §6: the "Find people →" kind exists, sorts in place, and its downgrade is real.

    Postgres cannot drop an enum label, so ``0002`` rebuilds ``contact_kind`` — which only
    works once the ``linkedin_people`` rows are gone. Seeding one proves the ``DELETE`` runs
    (without it the ``ALTER COLUMN ... USING`` fails on an invalid enum input) and that it is
    scoped to the constructed kind: the published contact next to it survives.
    """
    domain = "enum-round-trip.example"

    async def _seed() -> None:
        engine = create_async_engine(migrated_database)
        try:
            async with engine.begin() as c:
                company_id = await c.scalar(
                    text(
                        "INSERT INTO companies (name, normalized_name, domain) "
                        "VALUES ('Enum Round Trip', 'enum round trip', :domain) RETURNING id"
                    ),
                    {"domain": domain},
                )
                await c.execute(
                    text(
                        "INSERT INTO contacts (company_id, kind, value, confidence) VALUES "
                        "(:id, 'linkedin_people', :people, 'constructed'), "
                        "(:id, 'careers_page', :careers, 'published')"
                    ),
                    {
                        "id": company_id,
                        "people": "https://www.linkedin.com/search/results/people/?keywords=x",
                        "careers": f"https://{domain}/careers",
                    },
                )
        finally:
            await engine.dispose()

    async def _labels() -> list[str]:
        engine = create_async_engine(migrated_database)
        try:
            async with engine.connect() as c:
                return (await _public_enums(c))["contact_kind"]
        finally:
            await engine.dispose()

    async def _seeded_kinds() -> list[str]:
        engine = create_async_engine(migrated_database)
        try:
            async with engine.connect() as c:
                rows = await _rows(
                    c,
                    "SELECT contacts.kind::text AS kind FROM contacts JOIN companies "
                    f"ON companies.id = contacts.company_id WHERE companies.domain = '{domain}'",
                )
                return sorted(r.kind for r in rows)
        finally:
            await engine.dispose()

    async def _cleanup() -> None:
        engine = create_async_engine(migrated_database)
        try:
            async with engine.begin() as c:
                await c.execute(
                    text("DELETE FROM companies WHERE domain = :domain"), {"domain": domain}
                )
        finally:
            await engine.dispose()

    asyncio.run(_seed())
    try:
        assert asyncio.run(_seeded_kinds()) == ["careers_page", "linkedin_people"]
        command.downgrade(alembic_config, "-1")
        assert asyncio.run(_labels()) == CONTACT_KIND_AT_0001
        assert asyncio.run(_seeded_kinds()) == ["careers_page"]
    finally:
        # Restore the session fixture's invariant (database at head) even if an assertion fails.
        command.upgrade(alembic_config, "head")
        asyncio.run(_cleanup())
    labels = asyncio.run(_labels())
    assert labels == [member.value for member in ContactKind]
    # The AFTER clause, not append order: enumsortorder puts it right after the company page.
    assert labels.index("linkedin_people") == labels.index("linkedin_company") + 1


def test_downgrade_removes_everything_and_upgrade_restores_it(
    alembic_config: Config, migrated_database: str
) -> None:
    async def _state() -> tuple[set[str], dict[str, list[str]], set[str]]:
        engine = create_async_engine(migrated_database)
        try:
            async with engine.connect() as c:
                return await _public_tables(c), await _public_enums(c), await _extensions(c)
        finally:
            await engine.dispose()

    command.downgrade(alembic_config, "base")
    try:
        tables, enums, extensions = asyncio.run(_state())
        assert tables == {"alembic_version"}
        assert enums == {}
        assert "pg_trgm" not in extensions
    finally:
        # Restore the session fixture's invariant (database at head) even if an assertion fails.
        command.upgrade(alembic_config, "head")
    tables, enums, extensions = asyncio.run(_state())
    assert tables - {"alembic_version"} == EXPECTED_TABLES
    assert set(enums) == EXPECTED_ENUM_TYPES
    assert "pg_trgm" in extensions
