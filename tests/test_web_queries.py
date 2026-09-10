"""``db.queries``' Phase 4 browse layer (SPEC §9): the filtered, sorted, keyset-paginated
company and role lists and the small lookups around them. Real Postgres, rows inserted with the
ORM factories in ``tests/support_web.py``.

Three things here are load-bearing rather than incidental:

* **the default sort** is ``latest_job_posted_at DESC NULLS LAST`` and SPEC §13 requires a test
  to say so — :func:`test_default_company_sort_is_latest_job_posted_at_desc_nulls_last`;
* **keyset pagination is walked to exhaustion for every sort** and compared against the same
  query at a huge limit, which is the only way to catch a cursor that skips or repeats a row at
  a tie or at the NULL boundary;
* **the role filters conjoin on one job**, so a company with a part-time marketing role and a
  full-time software role does not match ``family=software`` plus ``employment=part_time``.

SPEC §12 Phase 7 added a fourth: **``metro`` is a filter beside ``city``, not above it**. The
two are independent semi-joins, so they compose as an ``AND`` over a company's whole set of
offices rather than as one geography, and neither may turn a company with several matching
offices into several rows — which would break the page size and the keyset order at once. The
``metro_*`` tests below are that pair of properties.

SPEC §12 Phase 8 adds a fifth, and it is the same shape a third time: **``has_published_email``
is a semi-join over ``contacts``**, because a company may publish several addresses and an
inner join would return it once per address. What is new is the second half of its predicate —
``confidence``, which is what separates an address somebody offered from a link this app built
out of a company name (§6), and therefore what the filter's whole promise rests on.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db import enums
from db.models import Company, Contact
from db.queries import (
    MAX_PAGE_SIZE,
    PAGE_SIZE,
    CityFacet,
    CompanyRow,
    CompanySort,
    Cursor,
    CursorError,
    Filters,
    JobRow,
    JobSort,
    Page,
    _months_ago,
    company_card_extras,
    company_jobs,
    company_list_page,
    facet_cities,
    facet_metros,
    facet_sectors,
    job_list_page,
    last_successful_runs,
)
from tests.support_web import (
    DAY,
    NOW,
    make_bookmark,
    make_company,
    make_contact,
    make_job,
    make_location,
    make_note,
    make_round,
    make_run,
    make_sector,
    refresh_denormalized,
)

# ------------------------------------------------------------------------------- helpers


def ids(page: Page[CompanyRow] | Page[JobRow]) -> list[int]:
    return [row.id for row in page.rows]


async def walk_companies(
    session: AsyncSession,
    *,
    sort: CompanySort,
    filters: Filters | None = None,
    limit: int = 3,
) -> tuple[list[int], int]:
    """Every id of a company list, page by page through the cursor, plus the page count."""
    active = filters if filters is not None else Filters()
    collected: list[int] = []
    cursor: Cursor | None = None
    pages = 0
    while True:
        page = await company_list_page(
            session, filters=active, sort=sort, cursor=cursor, limit=limit
        )
        collected.extend(ids(page))
        pages += 1
        if page.next_cursor is None:
            return collected, pages
        cursor = Cursor.decode(page.next_cursor)


async def walk_jobs(
    session: AsyncSession, *, sort: JobSort, filters: Filters | None = None, limit: int = 3
) -> tuple[list[int], int]:
    active = filters if filters is not None else Filters()
    collected: list[int] = []
    cursor: Cursor | None = None
    pages = 0
    while True:
        page = await job_list_page(session, filters=active, sort=sort, cursor=cursor, limit=limit)
        collected.extend(ids(page))
        pages += 1
        if page.next_cursor is None:
            return collected, pages
        cursor = Cursor.decode(page.next_cursor)


# ------------------------------------------------------------------- Cursor (SPEC §9 keyset)


@pytest.mark.parametrize(
    "cursor",
    [
        Cursor(sort="recent_job", key="2026-09-04T12:00:00+00:00", id=7),
        Cursor(sort="amount", key=1_500_000, id=1),
        Cursor(sort="name", key="acme", id=99),
        Cursor(sort="funding_date", key=None, id=3),
    ],
)
def test_cursor_round_trip(cursor: Cursor) -> None:
    assert Cursor.decode(cursor.encode()) == cursor
    # The token is padding-free so it survives a query string untouched.
    assert "=" not in cursor.encode()


@pytest.mark.parametrize(
    "token",
    [
        "",
        "not-base64!!",
        "eyJzIjoicmVjZW50X2pvYiIsImsiOm51bGwsImkiOjF9"[:12],  # truncated
        "bnVsbA",  # valid base64 of `null` — not an object
        "eyJzIjoicmVjZW50X2pvYiJ9",  # {"s": ...} — missing k and i
        "eyJzIjoxLCJrIjpudWxsLCJpIjoxfQ",  # {"s": 1, ...} — sort is not a string
        "eyJzIjoiYSIsImsiOm51bGwsImkiOnRydWV9",  # {"i": true} — a bool is not an id
        "eyJzIjoiYSIsImsiOnt9LCJpIjoxfQ",  # {"k": {}} — a key is a scalar or null
        # An id no row can have: every id column in SPEC §5 is an `integer`, so asyncpg would
        # refuse to bind these and raise DataError from inside the statement — a 500 from a
        # hand-edited token. They have to be rejected here instead.
        Cursor(sort="recent_job", key=None, id=10**14).encode(),
        Cursor(sort="posted", key=None, id=-(10**14)).encode(),
    ],
)
def test_cursor_decode_rejects_malformed_tokens(token: str) -> None:
    with pytest.raises(CursorError):
        Cursor.decode(token)


async def test_a_cursor_key_wider_than_its_column_is_refused(session: AsyncSession) -> None:
    """Same failure mode one level down: the *key* is bound against the sort column too."""
    company = await make_company(session, "Acme")
    await make_round(session, company, amount_usd=3_000_000_000)

    with pytest.raises(CursorError):
        # companies.open_job_count is an `integer`.
        await company_list_page(
            session, sort=CompanySort.OPEN_ROLES, cursor=Cursor(sort="open_roles", key=10**14, id=1)
        )
    with pytest.raises(CursorError):
        await company_list_page(
            session, sort=CompanySort.AMOUNT, cursor=Cursor(sort="amount", key=10**30, id=1)
        )

    # ...but funding_rounds.amount_usd is a BIGINT, so a $3B round must still paginate.
    page = await company_list_page(
        session, sort=CompanySort.AMOUNT, cursor=Cursor(sort="amount", key=4_000_000_000, id=0)
    )
    assert ids(page) == [company.id]


async def test_a_cursor_from_another_sort_is_refused(session: AsyncSession) -> None:
    """A key from a different ordering would silently return an arbitrary slice."""
    for index in range(4):
        await make_company(session, f"Company {index}")
    page = await company_list_page(session, sort=CompanySort.NAME, limit=2)
    assert page.next_cursor is not None
    stale = Cursor.decode(page.next_cursor)

    with pytest.raises(CursorError):
        await company_list_page(session, sort=CompanySort.NEWEST, cursor=stale, limit=2)


async def test_limit_is_validated(session: AsyncSession) -> None:
    assert PAGE_SIZE == 50  # SPEC §9 "pagination, 50 per page"
    for bad in (0, -1, MAX_PAGE_SIZE + 1):
        with pytest.raises(ValueError, match="limit must be between"):
            await company_list_page(session, limit=bad)
        with pytest.raises(ValueError, match="limit must be between"):
            await job_list_page(session, limit=bad)


# ------------------------------------------------------------------ calendar month arithmetic


@pytest.mark.parametrize(
    ("today", "months", "expected"),
    [
        (date(2026, 9, 4), 1, date(2026, 8, 4)),
        (date(2026, 1, 15), 3, date(2025, 10, 15)),
        (date(2026, 3, 31), 1, date(2026, 2, 28)),  # day clamped to a short month
        (date(2024, 3, 31), 1, date(2024, 2, 29)),  # ...and to a leap February
        (date(2026, 9, 4), 12, date(2025, 9, 4)),
        (date(2026, 9, 4), 120, date(2016, 9, 4)),
        (date(2026, 9, 4), 0, date(2026, 9, 4)),
    ],
)
def test_months_ago(today: date, months: int, expected: date) -> None:
    assert _months_ago(today, months) == expected


# ------------------------------------------------------------------ individual §9 filters


async def test_city_filter_is_a_semi_join(session: AsyncSession) -> None:
    """A company in two matching cities appears once, not twice."""
    both = await make_company(session, "Both")
    await make_location(session, both, "Oakland", is_hq=True)
    await make_location(session, both, "Berkeley")
    berkeley = await make_company(session, "Berkeley only")
    await make_location(session, berkeley, "Berkeley")
    await make_company(session, "Nowhere")

    page = await company_list_page(
        session, filters=Filters(cities=("Oakland", "Berkeley")), sort=CompanySort.NAME
    )
    assert ids(page) == [berkeley.id, both.id]


async def test_metro_filter_selects_a_whole_region_across_state_lines(
    session: AsyncSession,
) -> None:
    """SPEC §12 Phase 7's region selector: it filters ``locations.metro``, nothing else.

    The New York metro is deliberately represented by one NY city and one NJ one, because that
    is the case that separates the column that is meant from every plausible substitute. A
    predicate that read ``locations.state``, or that matched the metro name against the city,
    would return Brooklyn and drop Jersey City — and would go on passing against a
    single-state region forever.
    """
    bridge = await make_company(session, "Bridge Robotics")
    await make_location(session, bridge, "Brooklyn", is_hq=True)
    harbor = await make_company(session, "Harbor Data")
    await make_location(session, harbor, "Jersey City", is_hq=True)
    pike = await make_company(session, "Pike Systems")
    await make_location(session, pike, "Seattle")
    await make_company(session, "Unplaced")  # no location row at all

    page = await company_list_page(
        session, filters=Filters(metros=("New York",)), sort=CompanySort.NAME
    )
    assert ids(page) == [bridge.id, harbor.id]

    # Two regions at once are an OR within the one filter, as every other multi-select is.
    both = await company_list_page(
        session, filters=Filters(metros=("New York", "Seattle")), sort=CompanySort.NAME
    )
    assert ids(both) == [bridge.id, harbor.id, pike.id]
    # A region nothing has been ingested for is an empty page, never an error: the value is
    # free text from the data, not an enum (``web.filters`` says so in as many words).
    assert ids(await company_list_page(session, filters=Filters(metros=("Nowhere",)))) == []


async def test_metro_and_city_are_independent_and_predicates(session: AsyncSession) -> None:
    """``metro=New York&city=Oakland`` wants an office in each, not one office in both.

    The two filters are separate ``EXISTS`` subqueries over a company's offices, which is what
    lets the UI leave the City select alone when a region is picked. Folding them into one
    subquery — the obvious simplification, since both scope ``locations`` — would quietly mean
    "one office that is in Oakland *and* in the New York metro", i.e. nothing, and this
    bicoastal company is the only shape that tells the two readings apart.
    """
    bicoastal = await make_company(session, "Bicoastal")
    await make_location(session, bicoastal, "Oakland", is_hq=True)
    await make_location(session, bicoastal, "Brooklyn")
    oakland_only = await make_company(session, "Oakland only")
    await make_location(session, oakland_only, "Oakland")
    brooklyn_only = await make_company(session, "Brooklyn only")
    await make_location(session, brooklyn_only, "Brooklyn")

    together = await company_list_page(
        session, filters=Filters(cities=("Oakland",), metros=("New York",))
    )
    assert ids(together) == [bicoastal.id]

    # Each half alone is strictly wider, so the conjunction is what did the narrowing.
    city_only = await company_list_page(session, filters=Filters(cities=("Oakland",)))
    assert set(ids(city_only)) == {bicoastal.id, oakland_only.id}
    metro_only = await company_list_page(session, filters=Filters(metros=("New York",)))
    assert set(ids(metro_only)) == {bicoastal.id, brooklyn_only.id}


async def test_metro_composes_with_a_role_filter(session: AsyncSession) -> None:
    """A geography filter and a role filter narrow together — SPEC §9's "all combinable".

    Both pages are asked, because ``/`` and ``/roles`` reach the metro predicate by the same
    ``_company_predicates`` call and a regression in it would be visible in whichever one the
    test happened to skip.
    """
    ny_data = await make_company(session, "Empire Analytics")
    await make_location(session, ny_data, "Brooklyn")
    wanted = await make_job(session, ny_data, "Data engineer", role_family=enums.RoleFamily.DATA)
    ny_design = await make_company(session, "Empire Studio")
    await make_location(session, ny_design, "New York")
    await make_job(session, ny_design, "Product designer", role_family=enums.RoleFamily.DESIGN)
    bay_data = await make_company(session, "Bay Analytics")
    await make_location(session, bay_data, "Oakland")
    await make_job(session, bay_data, "Data engineer", role_family=enums.RoleFamily.DATA)

    filters = Filters(metros=("New York",), role_families=(enums.RoleFamily.DATA,))
    assert ids(await company_list_page(session, filters=filters)) == [ny_data.id]
    assert ids(await job_list_page(session, filters=filters)) == [wanted.id]


async def test_a_company_with_several_offices_in_one_metro_appears_once(
    session: AsyncSession,
) -> None:
    """The anti-fan-out property, and the reason ``metro`` is a semi-join rather than a join.

    An inner join through ``company_locations`` would return this company three times — once
    per matching office — which silently costs the page two of its 50 rows and makes the keyset
    cursor point at a row the next page has already shown. The role list repeats the same
    predicates over a second join, so it is asked too: there the duplication would be per role
    *and* per office.
    """
    crosstown = await make_company(session, "Crosstown")
    await make_location(session, crosstown, "Brooklyn", is_hq=True)
    await make_location(session, crosstown, "Jersey City")
    await make_location(session, crosstown, "New York")
    role = await make_job(session, crosstown, "Engineer")
    neighbour = await make_company(session, "Neighbour")
    await make_location(session, neighbour, "Brooklyn")
    # Outside the region, so "appears once" cannot be satisfied by the filter doing nothing.
    absent = await make_company(session, "Absent")
    await make_location(session, absent, "Oakland")
    await make_job(session, absent, "Engineer")

    filters = Filters(metros=("New York",))
    page = await company_list_page(session, filters=filters, sort=CompanySort.NAME)
    assert ids(page) == [crosstown.id, neighbour.id]
    assert ids(await job_list_page(session, filters=filters)) == [role.id]


async def test_the_metro_filter_holds_across_a_keyset_page_boundary(
    session: AsyncSession,
) -> None:
    """The cursor predicate is ANDed with the metro filter, page 2 as much as page 1.

    The two regions are interleaved by name, so the sort walks in and out of the filtered set
    on every page: a metro predicate that only reached the first statement would not merely add
    rows at the end, it would splice the Bay Area companies into the middle of the walk.
    """
    wanted: list[int] = []
    unwanted: list[int] = []
    for index in range(11):
        company = await make_company(session, f"Metro co {index:02d}")
        if index % 3:
            await make_location(session, company, "Brooklyn")
            wanted.append(company.id)
        else:
            await make_location(session, company, "Oakland")
            unwanted.append(company.id)

    filters = Filters(metros=("New York",))
    whole = await company_list_page(
        session, filters=filters, sort=CompanySort.NAME, limit=MAX_PAGE_SIZE
    )
    walked, pages = await walk_companies(session, sort=CompanySort.NAME, filters=filters, limit=3)

    assert pages == 3, "seven rows in pages of three"
    assert walked == ids(whole) == wanted
    assert len(set(walked)) == len(wanted) == 7
    assert set(walked).isdisjoint(unwanted)


async def test_sector_filter(session: AsyncSession) -> None:
    fintech = await make_company(session, "Fintech co")
    await make_sector(session, fintech, "Fintech")
    biotech = await make_company(session, "Biotech co")
    await make_sector(session, biotech, "Biotech")

    page = await company_list_page(session, filters=Filters(sector_slugs=("fintech",)))
    assert ids(page) == [fintech.id]


async def test_stage_filter(session: AsyncSession) -> None:
    seed = await make_company(session, "Seed co", stage=enums.Stage.SEED)
    series_a = await make_company(session, "A co", stage=enums.Stage.SERIES_A)
    await make_company(session, "Unknown co")

    page = await company_list_page(
        session,
        filters=Filters(stages=(enums.Stage.SEED, enums.Stage.SERIES_A)),
        sort=CompanySort.NAME,
    )
    assert ids(page) == [series_a.id, seed.id]


async def test_round_type_filter_reads_the_latest_round(session: AsyncSession) -> None:
    """The filter matches the *latest* round, not any round the company ever raised."""
    graduated = await make_company(session, "Graduated")
    await make_round(
        session, graduated, round_type=enums.RoundType.SEED, announced_date=date(2024, 1, 1)
    )
    await make_round(
        session, graduated, round_type=enums.RoundType.SERIES_A, announced_date=date(2026, 1, 1)
    )
    still_seed = await make_company(session, "Still seed")
    await make_round(
        session, still_seed, round_type=enums.RoundType.SEED, announced_date=date(2026, 2, 1)
    )
    await make_company(session, "No round")

    page = await company_list_page(session, filters=Filters(round_types=(enums.RoundType.SEED,)))
    assert ids(page) == [still_seed.id]


async def test_amount_range_excludes_an_undisclosed_amount(session: AsyncSession) -> None:
    """A NULL ``amount_usd`` is *undisclosed* (SPEC §5), so a range cannot claim it matches."""
    small = await make_company(session, "Small")
    await make_round(session, small, amount_usd=2_000_000)
    medium = await make_company(session, "Medium")
    await make_round(session, medium, amount_usd=20_000_000)
    large = await make_company(session, "Large")
    await make_round(session, large, amount_usd=200_000_000)
    undisclosed = await make_company(session, "Undisclosed")
    await make_round(session, undisclosed, amount_usd=None)

    page = await company_list_page(
        session,
        filters=Filters(amount_min_usd=5_000_000, amount_max_usd=100_000_000),
        sort=CompanySort.NAME,
    )
    assert ids(page) == [medium.id]

    only_min = await company_list_page(
        session, filters=Filters(amount_min_usd=20_000_000), sort=CompanySort.NAME
    )
    assert ids(only_min) == [large.id, medium.id]

    only_max = await company_list_page(
        session, filters=Filters(amount_max_usd=2_000_000), sort=CompanySort.NAME
    )
    assert ids(only_max) == [small.id]


async def test_round_recency_filter_uses_the_injected_clock(session: AsyncSession) -> None:
    recent = await make_company(session, "Recent")
    await make_round(session, recent, announced_date=date(2026, 7, 1))
    old = await make_company(session, "Old")
    await make_round(session, old, announced_date=date(2025, 1, 1))
    undated = await make_company(session, "Undated")
    await make_round(session, undated, announced_date=None)

    page = await company_list_page(
        session, filters=Filters(round_within_months=6), now=datetime(2026, 9, 4, tzinfo=UTC)
    )
    assert ids(page) == [recent.id]

    # Pinning the clock a year later moves the boundary past the same round.
    later = await company_list_page(
        session, filters=Filters(round_within_months=6), now=datetime(2027, 9, 4, tzinfo=UTC)
    )
    assert ids(later) == []


async def test_role_family_employment_and_seniority_filters(session: AsyncSession) -> None:
    software = await make_company(session, "Software co")
    await make_job(
        session,
        software,
        "Backend engineer",
        role_family=enums.RoleFamily.SOFTWARE,
        employment_type=enums.EmploymentType.FULL_TIME,
        seniority=enums.Seniority.SENIOR,
    )
    design = await make_company(session, "Design co")
    await make_job(
        session,
        design,
        "Product designer",
        role_family=enums.RoleFamily.DESIGN,
        employment_type=enums.EmploymentType.CONTRACT,
        seniority=enums.Seniority.MID,
    )
    await make_company(session, "No roles")

    families = await company_list_page(
        session, filters=Filters(role_families=(enums.RoleFamily.SOFTWARE,))
    )
    assert ids(families) == [software.id]

    employment = await company_list_page(
        session, filters=Filters(employment_types=(enums.EmploymentType.CONTRACT,))
    )
    assert ids(employment) == [design.id]

    seniorities = await company_list_page(
        session, filters=Filters(seniorities=(enums.Seniority.SENIOR,))
    )
    assert ids(seniorities) == [software.id]


async def test_role_filters_must_be_satisfied_by_one_job(session: AsyncSession) -> None:
    """SPEC §9's meaning: *one* open role matching every active role filter at once.

    The mixed company has a part-time marketing role and a full-time software role, so it has a
    part-time role and it has a software role — but no part-time *software* role.
    """
    mixed = await make_company(session, "Mixed")
    await make_job(
        session,
        mixed,
        "Marketing associate",
        role_family=enums.RoleFamily.MARKETING,
        employment_type=enums.EmploymentType.PART_TIME,
    )
    await make_job(
        session,
        mixed,
        "Staff engineer",
        role_family=enums.RoleFamily.SOFTWARE,
        employment_type=enums.EmploymentType.FULL_TIME,
    )
    matching = await make_company(session, "Matching")
    await make_job(
        session,
        matching,
        "Part-time backend engineer",
        role_family=enums.RoleFamily.SOFTWARE,
        employment_type=enums.EmploymentType.PART_TIME,
    )

    both = Filters(
        role_families=(enums.RoleFamily.SOFTWARE,),
        employment_types=(enums.EmploymentType.PART_TIME,),
    )
    assert ids(await company_list_page(session, filters=both)) == [matching.id]
    # Each filter alone still matches the mixed company — the conjunction is what excludes it.
    assert mixed.id in ids(
        await company_list_page(
            session, filters=Filters(role_families=(enums.RoleFamily.SOFTWARE,))
        )
    )
    assert mixed.id in ids(
        await company_list_page(
            session, filters=Filters(employment_types=(enums.EmploymentType.PART_TIME,))
        )
    )


async def test_role_filters_ignore_closed_roles(session: AsyncSession) -> None:
    """The company list is about *open* roles; a closed one cannot satisfy the EXISTS."""
    company = await make_company(session, "Closed only")
    await make_job(session, company, "Gone", role_family=enums.RoleFamily.DATA, closed_at=NOW - DAY)

    page = await company_list_page(session, filters=Filters(role_families=(enums.RoleFamily.DATA,)))
    assert ids(page) == []


async def test_flexible_only_is_opt_in(session: AsyncSession) -> None:
    """SPEC §7.1: a badge and an opt-in filter that hides nothing when it is off."""
    flexible = await make_company(session, "Flexible")
    await make_job(session, flexible, "Part-time intern", flexible_signal=True)
    rigid = await make_company(session, "Rigid")
    await make_job(session, rigid, "Full-time engineer", flexible_signal=False)

    assert set(ids(await company_list_page(session))) == {flexible.id, rigid.id}
    assert ids(await company_list_page(session, filters=Filters(flexible_only=True))) == [
        flexible.id
    ]


async def test_has_open_roles_reads_the_denormalized_count(session: AsyncSession) -> None:
    open_roles = await make_company(session, "Hiring")
    await make_job(session, open_roles, "Engineer")
    closed = await make_company(session, "Not hiring")
    await make_job(session, closed, "Was hiring", closed_at=NOW)
    await make_company(session, "Never hired")

    page = await company_list_page(session, filters=Filters(has_open_roles=True))
    assert ids(page) == [open_roles.id]
    # The filter and the badge must agree: both come from `companies.open_job_count`.
    assert page.rows[0].open_job_count == 1


async def test_has_published_email_is_a_semi_join_over_addresses_somebody_published(
    session: AsyncSession,
) -> None:
    """SPEC §12 Phase 8's "way in": one row per reachable company, and *published* means it.

    Three properties, and the corpus is built so that none of them can pass by accident.

    **Two published addresses are still one row.** ``contacts`` is many-per-company, so an
    inner join would emit Reachable Labs twice — costing the 50-row page a row and leaving the
    keyset cursor pointing at a company the next page has already shown. That is the reason
    this predicate is an ``EXISTS``, exactly as ``city`` and ``metro`` are, and two addresses
    on one company is the only shape that tells a semi-join from a join.

    **Both halves of ``kind = email AND confidence = published`` are load-bearing**, and the
    corpus holds the company that proves each. Guessed Labs has a ``constructed`` address:
    SPEC §6 forbids this app from ever inventing one — no pattern-guessing, no SMTP probing —
    while the column will happily hold it, so ``confidence`` is what keeps the filter's promise
    that somebody offered this address. Paged Labs has a careers page it published itself:
    a real row, published by the company, that is not something you can write to — so ``kind``
    is what stops "has any contact at all" from passing for "has a way in", which on the live
    database would be the difference between 397 companies and all 10,219 of them (measured
    2026-09-09; a corpus count is prose here, since a test may not read that database).

    **It constrains the company, not a role.** Roleless Labs has an address and no jobs at all,
    and is returned. Folding this into ``Filters.has_job_filters`` — where it looks at first
    glance like it belongs, since it sits beside ``has_open_roles`` in the parameter table —
    would quietly add "and is hiring" to it, which is the opposite of the point: an unposted
    part-time role is the thing this phase exists to go asking for.
    """
    reachable = await make_company(session, "Reachable Labs")
    await make_contact(
        session,
        reachable,
        kind=enums.ContactKind.EMAIL,
        value="ada@reachable.example",
        confidence=enums.ContactConfidence.PUBLISHED,
    )
    await make_contact(
        session,
        reachable,
        kind=enums.ContactKind.EMAIL,
        value="grace@reachable.example",
        confidence=enums.ContactConfidence.PUBLISHED,
    )
    newer_role = await make_job(session, reachable, "Engineer", posted_at=NOW)
    older_role = await make_job(session, reachable, "Designer", posted_at=NOW - DAY)

    roleless = await make_company(session, "Roleless Labs")
    await make_contact(
        session,
        roleless,
        kind=enums.ContactKind.EMAIL,
        value="hiring@roleless.example",
        confidence=enums.ContactConfidence.PUBLISHED,
    )

    guessed = await make_company(session, "Guessed Labs")
    await make_contact(
        session,
        guessed,
        kind=enums.ContactKind.EMAIL,
        value="hello@guessed.example",
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )
    await make_job(session, guessed, "Engineer")

    paged = await make_company(session, "Paged Labs")
    await make_contact(
        session,
        paged,
        kind=enums.ContactKind.CAREERS_PAGE,
        value="https://paged.example/careers",
        confidence=enums.ContactConfidence.PUBLISHED,
    )

    searchable = await make_company(session, "Searchable Labs")
    await make_contact(
        session,
        searchable,
        kind=enums.ContactKind.LINKEDIN_PEOPLE,
        value="https://www.linkedin.com/search/results/people/?keywords=Searchable+Labs",
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )
    await make_company(session, "Silent Labs")  # no contacts row at all

    # The premise the "appears once" assertion rests on: there really are two rows for a join to
    # fan out on. Without this, deleting one of the two addresses above would leave that
    # assertion passing and testing nothing.
    stored = await session.scalar(
        select(func.count()).select_from(Contact).where(Contact.company_id == reachable.id)
    )
    assert stored == 2

    wanted = Filters(has_published_email=True)
    page = await company_list_page(session, filters=wanted, sort=CompanySort.NAME)
    assert ids(page) == [reachable.id, roleless.id]

    # ``/roles`` repeats the same predicates over a second join, where a fan-out would multiply
    # per role *and* per address: two roles times two addresses is four rows of two jobs.
    assert ids(await job_list_page(session, filters=wanted)) == [newer_role.id, older_role.id]

    # Off, every one of them is listed — the filter narrows, it is never a default (§7.1). This
    # is what says the three exclusions above were the predicate rather than missing rows.
    everything = set(ids(await company_list_page(session, limit=MAX_PAGE_SIZE)))
    assert len(everything) == 6
    assert {guessed.id, paged.id, searchable.id} <= everything


async def test_tracking_none_matches_a_company_with_no_user_note(session: AsyncSession) -> None:
    """A company nobody has touched has no ``user_notes`` row and must still be "not tracked"."""
    untouched = await make_company(session, "Untouched")
    explicit = await make_company(session, "Explicit none")
    await make_note(session, explicit, status=enums.TrackingStatus.NONE)
    applied = await make_company(session, "Applied")
    await make_note(session, applied, status=enums.TrackingStatus.APPLIED, rating=4)

    none_page = await company_list_page(
        session, filters=Filters(tracking_statuses=(enums.TrackingStatus.NONE,))
    )
    assert set(ids(none_page)) == {untouched.id, explicit.id}

    applied_page = await company_list_page(
        session, filters=Filters(tracking_statuses=(enums.TrackingStatus.APPLIED,))
    )
    assert ids(applied_page) == [applied.id]
    assert applied_page.rows[0].tracking_status == enums.TrackingStatus.APPLIED
    assert applied_page.rows[0].rating == 4

    # The untracked company's row carries a NULL status, not the string "none".
    everything = await company_list_page(session, sort=CompanySort.NAME)
    by_id = {row.id: row for row in everything.rows}
    assert by_id[untouched.id].tracking_status is None
    assert by_id[explicit.id].tracking_status == enums.TrackingStatus.NONE


@pytest.mark.parametrize("wanted", list(enums.TrackingStatus))
async def test_every_tracking_status_selects_exactly_its_own_company(
    session: AsyncSession, wanted: enums.TrackingStatus
) -> None:
    """One company per member, plus one with no row at all — which belongs to ``none``."""
    by_status = {}
    for member in enums.TrackingStatus:
        company = await make_company(session, f"{member.value} co")
        await make_note(session, company, status=member)
        by_status[member] = company.id
    untracked = await make_company(session, "Untracked co")

    page = await company_list_page(session, filters=Filters(tracking_statuses=(wanted,)))

    expected = {by_status[wanted]}
    if wanted is enums.TrackingStatus.NONE:
        expected.add(untracked.id)
    assert set(ids(page)) == expected


async def test_five_filters_at_once(session: AsyncSession) -> None:
    """One statement, one WHERE: the filters compose without special cases."""
    winner = await make_company(session, "Winner", stage=enums.Stage.SERIES_A)
    await make_location(session, winner, "Oakland", is_hq=True)
    await make_sector(session, winner, "Climate")
    await make_round(session, winner, amount_usd=12_000_000, announced_date=date(2026, 6, 1))
    await make_job(session, winner, "ML engineer", role_family=enums.RoleFamily.ML_AI)

    # Each of these differs from the winner in exactly one of the five dimensions.
    wrong_city = await make_company(session, "Wrong city", stage=enums.Stage.SERIES_A)
    await make_location(session, wrong_city, "San Jose")
    await make_sector(session, wrong_city, "Climate")
    await make_round(session, wrong_city, amount_usd=12_000_000, announced_date=date(2026, 6, 1))
    await make_job(session, wrong_city, "ML engineer", role_family=enums.RoleFamily.ML_AI)

    wrong_family = await make_company(session, "Wrong family", stage=enums.Stage.SERIES_A)
    await make_location(session, wrong_family, "Oakland")
    await make_sector(session, wrong_family, "Climate")
    await make_round(session, wrong_family, amount_usd=12_000_000, announced_date=date(2026, 6, 1))
    await make_job(session, wrong_family, "Recruiter", role_family=enums.RoleFamily.PEOPLE)

    filters = Filters(
        cities=("Oakland",),
        sector_slugs=("climate",),
        stages=(enums.Stage.SERIES_A,),
        amount_min_usd=10_000_000,
        role_families=(enums.RoleFamily.ML_AI,),
    )
    assert ids(await company_list_page(session, filters=filters)) == [winner.id]


# --------------------------------------------------------------------------------- search


async def test_search_full_text_trigram_and_blank(session: AsyncSession) -> None:
    """One statement with two arms: the FTS ``tsvector`` and pg_trgm's ``%`` typo fallback."""
    photonics = await make_company(
        session, "Vertex Photonics", one_liner="Silicon photonic interconnects for datacenters"
    )
    nimbus = await make_company(session, "Nimbus Analytics", one_liner="Warehouse-native BI")
    await make_company(session, "Ferrous Robotics", one_liner="Autonomous forklifts")

    # Exact FTS hit against the generated tsvector (name + one_liner + thesis).
    assert ids(await company_list_page(session, filters=Filters(q="interconnects"))) == [
        photonics.id
    ]
    # A typo no tsquery can match: 'analitics' does not stem to 'analyt'. The trigram arm does.
    assert ids(await company_list_page(session, filters=Filters(q="Nimbus Analitics"))) == [
        nimbus.id
    ]
    # Neither arm.
    assert ids(await company_list_page(session, filters=Filters(q="zzzqqq nonexistent"))) == []
    # Blank and whitespace-only are *not* filters: clearing the box shows everything.
    assert len((await company_list_page(session, filters=Filters(q=""))).rows) == 3
    assert len((await company_list_page(session, filters=Filters(q="   "))).rows) == 3


async def test_job_search_covers_title_and_company_name(session: AsyncSession) -> None:
    company = await make_company(session, "Helion Energy")
    wanted = await make_job(session, company, "Plasma physicist")
    await make_job(session, company, "Office manager")

    assert ids(await job_list_page(session, filters=Filters(q="plasma"))) == [wanted.id]
    # A typo'd *company* name still finds its jobs through the trigram arm.
    typo = await job_list_page(session, filters=Filters(q="Helian Energy"), sort=JobSort.FIRST_SEEN)
    assert len(typo.rows) == 2


# --------------------------------------------------------------------------------- sorting


async def test_default_company_sort_is_latest_job_posted_at_desc_nulls_last(
    session: AsyncSession,
) -> None:
    """SPEC §9 and §13: the default order, asserted directly.

    ``latest_job_posted_at`` is the denormalized column the pipeline recomputes at end of run
    (SPEC §5), so this also pins that the list reads that column rather than re-deriving it.
    """
    newest = await make_company(session, "Newest")
    await make_job(session, newest, "Engineer", posted_at=NOW)
    middle = await make_company(session, "Middle")
    await make_job(session, middle, "Engineer", posted_at=NOW - 3 * DAY)
    oldest = await make_company(session, "Oldest")
    await make_job(session, oldest, "Engineer", posted_at=NOW - 30 * DAY)
    silent = await make_company(session, "Silent")  # no open roles at all -> NULL
    closed_only = await make_company(session, "Closed only")
    await make_job(session, closed_only, "Gone", posted_at=NOW, closed_at=NOW)

    page = await company_list_page(session)
    assert page.rows[0].id == newest.id
    assert [row.id for row in page.rows[:3]] == [newest.id, middle.id, oldest.id]
    # NULLs last, and in the id tiebreak's order.
    assert [row.id for row in page.rows[3:]] == sorted([silent.id, closed_only.id])
    assert [row.latest_job_posted_at for row in page.rows[3:]] == [None, None]
    assert page.rows[0].latest_job_posted_at == NOW


async def test_sort_ties_break_by_id(session: AsyncSession) -> None:
    """Without the id tiebreak a cursor sitting inside a tie could skip or repeat rows."""
    first = await make_company(session, "Tied")
    second = await make_company(session, "Tied")
    third = await make_company(session, "Tied")
    for company in (first, second, third):
        await make_job(session, company, "Engineer", posted_at=NOW)

    for sort in (CompanySort.RECENT_JOB, CompanySort.NAME, CompanySort.OPEN_ROLES):
        page = await company_list_page(session, sort=sort)
        assert ids(page) == [first.id, second.id, third.id], sort


async def seed_sort_corpus(session: AsyncSession) -> list[int]:
    """Nine companies spanning every sort's NULL boundary and a tie in each column."""
    made: list[int] = []
    plan: list[tuple[str, datetime | None, int | None, date | None]] = [
        # (name, job posted_at, round amount, round announced_date)
        ("Alpha", NOW, 5_000_000, date(2026, 1, 1)),
        ("alpha", NOW, 5_000_000, date(2026, 1, 1)),  # ties on every column, incl. lower(name)
        ("Bravo", NOW - DAY, 50_000_000, date(2025, 6, 1)),
        ("Charlie", NOW - 2 * DAY, None, date(2024, 3, 3)),  # NULL amount
        ("Delta", None, 1_000_000, None),  # no open role, undated round
        ("Echo", NOW - 10 * DAY, 900_000, date(2023, 12, 31)),
        ("Foxtrot", None, None, None),  # no round at all, no roles
        ("Golf", NOW - 20 * DAY, 250_000_000, date(2026, 8, 8)),
        ("Hotel", NOW - 21 * DAY, None, None),
    ]
    for index, (name, posted_at, amount, announced) in enumerate(plan):
        company = await make_company(session, name, first_seen_at=NOW - index * DAY)
        if posted_at is not None:
            await make_job(session, company, "Engineer", posted_at=posted_at, refresh=False)
        if amount is not None or announced is not None:
            await make_round(
                session, company, amount_usd=amount, announced_date=announced, refresh=False
            )
        made.append(company.id)
    await refresh_denormalized(session)
    return made


@pytest.mark.parametrize("sort", list(CompanySort))
async def test_keyset_pagination_walks_every_company_sort_to_exhaustion(
    session: AsyncSession, sort: CompanySort
) -> None:
    """The concatenated pages equal one huge page: no duplicate, no gap, NULL tail included."""
    made = await seed_sort_corpus(session)

    whole = await company_list_page(session, sort=sort, limit=MAX_PAGE_SIZE)
    assert whole.next_cursor is None
    walked, pages = await walk_companies(session, sort=sort, limit=3)

    assert pages == 3, f"{sort} should need three pages of three"
    assert walked == ids(whole)
    assert sorted(walked) == sorted(made)
    assert len(set(walked)) == len(made)


@pytest.mark.parametrize("sort", list(CompanySort))
async def test_every_company_sort_orders_nulls_last(
    session: AsyncSession, sort: CompanySort
) -> None:
    """Whatever the sort, a row with no value for it is at the end and is still *present*."""
    await seed_sort_corpus(session)
    page = await company_list_page(session, sort=sort, limit=MAX_PAGE_SIZE)

    keys = {
        CompanySort.RECENT_JOB: lambda row: row.latest_job_posted_at,
        CompanySort.FUNDING_DATE: lambda row: row.latest_round_announced_date,
        CompanySort.AMOUNT: lambda row: row.latest_round_amount_usd,
        CompanySort.NEWEST: lambda row: row.first_seen_at,
        CompanySort.OPEN_ROLES: lambda row: row.open_job_count,
        CompanySort.NAME: lambda row: row.name.lower(),
    }
    values = [keys[sort](row) for row in page.rows]
    present = [value for value in values if value is not None]
    assert values[: len(present)] == present, "a NULL sorted before a value"
    if sort is not CompanySort.NAME:
        assert present == sorted(present, reverse=True)
    else:
        assert present == sorted(present)
    assert len(page.rows) == 9, "no sort may drop a row"


async def test_keyset_pagination_survives_an_active_filter(session: AsyncSession) -> None:
    """The cursor predicate is ANDed with the filters, so a filtered walk stays coherent."""
    for index in range(7):
        company = await make_company(session, f"Hiring {index}")
        await make_job(session, company, "Engineer", posted_at=NOW - index * DAY)
    for index in range(3):
        await make_company(session, f"Quiet {index}")

    filters = Filters(has_open_roles=True)
    walked, pages = await walk_companies(session, sort=CompanySort.RECENT_JOB, filters=filters)
    whole = await company_list_page(session, filters=filters, limit=MAX_PAGE_SIZE)

    assert pages == 3
    assert walked == ids(whole)
    assert len(walked) == 7


# ----------------------------------------------------------------------- job_list_page (§9)


async def test_job_list_excludes_closed_roles_unless_asked(session: AsyncSession) -> None:
    company = await make_company(session, "Acme")
    open_job = await make_job(session, company, "Open role")
    closed_job = await make_job(session, company, "Closed role", closed_at=NOW - DAY)

    assert ids(await job_list_page(session)) == [open_job.id]
    with_closed = await job_list_page(
        session, filters=Filters(include_closed_jobs=True), sort=JobSort.FIRST_SEEN
    )
    assert set(ids(with_closed)) == {open_job.id, closed_job.id}


async def test_job_list_applies_company_and_role_filters_together(session: AsyncSession) -> None:
    oakland = await make_company(session, "Oakland co")
    await make_location(session, oakland, "Oakland")
    wanted = await make_job(
        session, oakland, "Data engineer", role_family=enums.RoleFamily.DATA, is_remote=True
    )
    await make_job(session, oakland, "Recruiter", role_family=enums.RoleFamily.PEOPLE)
    elsewhere = await make_company(session, "San Jose co")
    await make_location(session, elsewhere, "San Jose")
    await make_job(session, elsewhere, "Data engineer", role_family=enums.RoleFamily.DATA)

    page = await job_list_page(
        session, filters=Filters(cities=("Oakland",), role_families=(enums.RoleFamily.DATA,))
    )
    assert ids(page) == [wanted.id]
    row = page.rows[0]
    assert (row.company_id, row.company_name, row.is_remote) == (oakland.id, "Oakland co", True)


async def test_job_row_reports_its_bookmark(session: AsyncSession) -> None:
    company = await make_company(session, "Acme")
    starred = await make_job(session, company, "Starred", posted_at=NOW)
    unstarred = await make_job(session, company, "Plain", posted_at=NOW - DAY)
    cleared = await make_job(session, company, "Cleared", posted_at=NOW - 2 * DAY)
    await make_bookmark(session, starred, starred=True)
    await make_bookmark(session, cleared, starred=False)

    page = await job_list_page(session)
    assert [(row.id, row.starred) for row in page.rows] == [
        (starred.id, True),
        (unstarred.id, False),
        (cleared.id, False),
    ]


@pytest.mark.parametrize("sort", list(JobSort))
async def test_keyset_pagination_walks_every_job_sort_to_exhaustion(
    session: AsyncSession, sort: JobSort
) -> None:
    alpha = await make_company(session, "Alpha")
    bravo = await make_company(session, "bravo")  # lower(name) puts it after Alpha
    plan: list[tuple[Company, datetime | None]] = [
        (alpha, NOW),
        (alpha, NOW),  # a tie on posted_at
        (bravo, NOW - DAY),
        (bravo, None),  # the NULL tail of `posted`
        (alpha, None),
        (bravo, NOW - 5 * DAY),
        (alpha, NOW - 9 * DAY),
    ]
    made: list[int] = []
    for index, (company, posted_at) in enumerate(plan):
        job = await make_job(
            session,
            company,
            f"Role {index}",
            posted_at=posted_at,
            first_seen_at=NOW - index * DAY,
            refresh=False,
        )
        made.append(job.id)
    await refresh_denormalized(session)

    whole = await job_list_page(session, sort=sort, limit=MAX_PAGE_SIZE)
    walked, pages = await walk_jobs(session, sort=sort, limit=3)

    assert pages == 3
    assert walked == ids(whole)
    assert sorted(walked) == sorted(made)


# ------------------------------------------------------- per-page extras and one company's roles


async def test_company_card_extras_covers_every_requested_id(session: AsyncSession) -> None:
    rich = await make_company(session, "Rich")
    await make_sector(session, rich, "Robotics")
    await make_sector(session, rich, "Climate")
    await make_location(session, rich, "Berkeley")
    await make_location(session, rich, "Oakland", is_hq=True)
    for _ in range(3):
        await make_job(session, rich, "Engineer", role_family=enums.RoleFamily.SOFTWARE)
    await make_job(session, rich, "Designer", role_family=enums.RoleFamily.DESIGN)
    await make_job(session, rich, "Closed", role_family=enums.RoleFamily.SALES, closed_at=NOW)
    bare = await make_company(session, "Bare")

    extras = await company_card_extras(session, [rich.id, bare.id, rich.id, 99_999])

    assert set(extras) == {rich.id, bare.id, 99_999}
    assert extras[rich.id].sectors == ("Climate", "Robotics")  # alphabetical
    assert [(loc.city, loc.is_hq) for loc in extras[rich.id].locations] == [
        ("Oakland", True),
        ("Berkeley", False),
    ]
    assert extras[rich.id].hq_city == "Oakland"
    # Count descending, then family name; a closed role is not an open role.
    assert extras[rich.id].open_roles_by_family == (
        (enums.RoleFamily.SOFTWARE, 3),
        (enums.RoleFamily.DESIGN, 1),
    )
    assert extras[bare.id].sectors == ()
    assert extras[bare.id].hq_city is None
    assert extras[99_999].open_roles_by_family == ()
    assert await company_card_extras(session, []) == {}


async def test_hq_city_falls_back_to_any_city(session: AsyncSession) -> None:
    company = await make_company(session, "No HQ flag")
    await make_location(session, company, "Fremont")
    extras = await company_card_extras(session, [company.id])
    assert extras[company.id].hq_city == "Fremont"


async def test_a_geographically_filtered_row_names_the_office_it_matched_on(
    session: AsyncSession,
) -> None:
    """SPEC §12 Phase 7's region selector, and SPEC §9's collapsed row, have to agree.

    The row names one city, the HQ. The filter asks whether *any* location qualifies — the only
    reading that does not hide a real office — so a company headquartered in one metro with an
    office in another is a correct hit for either, and without this the row would show a city
    outside the filter and read as a leak. On the live Y Combinator data that was 19 of the 300
    rows across the six single-region filters.
    """
    company = await make_company(session, "Two offices")
    await make_location(session, company, "San Francisco", is_hq=True)
    await make_location(session, company, "Seattle")
    extras = (await company_card_extras(session, [company.id]))[company.id]

    # Filtered to the metro the HQ is *not* in: the row says which office answered.
    assert extras.matched_office(Filters(metros=("Seattle",))) == "Seattle"
    # ...and to the one it is in: nothing to add, the row already names it.
    assert extras.matched_office(Filters(metros=("Bay Area",))) is None
    # The city filter has always had the same shape and gets the same treatment.
    assert extras.matched_office(Filters(cities=("Seattle",))) == "Seattle"
    assert extras.matched_office(Filters(cities=("San Francisco",))) is None
    # Nothing geographic filtered: the row must not sprout a second city.
    assert extras.matched_office(Filters()) is None
    assert extras.matched_office(Filters(role_families=(enums.RoleFamily.SOFTWARE,))) is None


async def test_matched_office_is_silent_for_a_company_the_filter_would_not_return(
    session: AsyncSession,
) -> None:
    """A row the filter did not produce must not claim an office it does not have.

    Unreachable through ``/`` — the page only ever holds extras for rows the filter returned —
    but :func:`company_card_extras` is a public query and the property has to be total.
    """
    company = await make_company(session, "Elsewhere only")
    await make_location(session, company, "Austin", is_hq=True)
    extras = (await company_card_extras(session, [company.id]))[company.id]
    assert extras.matched_office(Filters(metros=("Boston",))) is None

    located_nowhere = await make_company(session, "No office at all")
    bare = (await company_card_extras(session, [located_nowhere.id]))[located_nowhere.id]
    assert bare.matched_office(Filters(metros=("Boston",))) is None


async def test_company_jobs_is_unfiltered_and_ordered(session: AsyncSession) -> None:
    """SPEC §9: the expanded row shows every role, whatever the page-level filter says."""
    company = await make_company(session, "Acme")
    other = await make_company(session, "Other")
    software = await make_job(
        session, company, "Backend", role_family=enums.RoleFamily.SOFTWARE, posted_at=NOW
    )
    marketing = await make_job(
        session,
        company,
        "Growth marketer",
        role_family=enums.RoleFamily.MARKETING,
        posted_at=NOW - DAY,
    )
    closed = await make_job(session, company, "Gone", posted_at=NOW, closed_at=NOW)
    await make_job(session, other, "Someone else's role")

    open_only = await company_jobs(session, company.id)
    assert [row.id for row in open_only] == [software.id, marketing.id]

    with_closed = await company_jobs(session, company.id, include_closed=True)
    # Open roles first, then the closed ones — a closed role can never outrank an open one.
    assert [row.id for row in with_closed] == [software.id, marketing.id, closed.id]
    assert with_closed[-1].closed_at is not None

    by_company = await company_jobs(session, company.id, sort=JobSort.COMPANY)
    assert {row.company_name for row in by_company} == {"Acme"}


# --------------------------------------------------------------- facets and run health (§9)


async def test_facets(session: AsyncSession) -> None:
    """What the filter form offers, read from the tables rather than from the config (§11).

    ``facet_cities`` is a row per city since SPEC §12 Phase 7, ordered by metro then city so
    the City select's ``<optgroup>`` per metro is contiguous without the template sorting
    anything itself. The state and the metro ride along for display only — the ``city=`` wire
    value is still the bare name.
    """
    first = await make_company(session, "First")
    second = await make_company(session, "Second")
    await make_location(session, first, "Oakland")
    await make_location(session, second, "Oakland")  # the same city, listed once
    await make_location(session, second, "Berkeley")
    await make_location(session, second, "Jersey City")
    await make_sector(session, first, "Climate", slug="climate")
    await make_sector(session, second, "AI Infrastructure", slug="ai-infra")

    assert await facet_cities(session) == [
        CityFacet(city="Berkeley", state="CA", metro="Bay Area"),
        CityFacet(city="Oakland", state="CA", metro="Bay Area"),
        CityFacet(city="Jersey City", state="NJ", metro="New York"),
    ]
    # Alphabetical, and one entry per metro however many of its cities are in the table.
    assert await facet_metros(session) == ["Bay Area", "New York"]
    assert await facet_sectors(session) == [
        ("ai-infra", "AI Infrastructure"),
        ("climate", "Climate"),
    ]


async def test_last_successful_runs_counts_ok_only(session: AsyncSession) -> None:
    """SPEC §9 asks about a *successful* run, so ``partial`` does not clear the staleness clock."""
    await make_run(session, "greenhouse", started_at=NOW - 5 * DAY, status=enums.FetchRunStatus.OK)
    await make_run(session, "greenhouse", started_at=NOW - DAY, status=enums.FetchRunStatus.OK)
    await make_run(session, "greenhouse", started_at=NOW, status=enums.FetchRunStatus.ERROR)
    await make_run(session, "lever", started_at=NOW, status=enums.FetchRunStatus.PARTIAL)
    await make_run(session, "ashby", started_at=NOW, status=enums.FetchRunStatus.ERROR)

    assert await last_successful_runs(session) == {"greenhouse": NOW - DAY}
