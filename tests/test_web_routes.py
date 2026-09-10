"""The four SPEC §9 pages over real HTTP: ``/``, ``/roles``, ``/company/{id}`` (plus its lazy
panel and note form), ``/runs``, and the bookmark toggle.

Driven through :func:`tests.support_web.app_client` — the real ``create_app`` over
``httpx.ASGITransport`` — so what is asserted is the response a browser receives: status code,
rendered markup, redirect target and the row the handler wrote. Two properties get the most
attention because they are the ones a refactor breaks silently:

* **the htmx contract** — ``/`` and ``/roles`` answer with a *fragment* for an htmx swap but
  with the *whole page* when htmx is restoring a history entry, and the keyset "Load more"
  button returns the same partial that first rendered the list;
* **a hand-edited URL is a page, never a stack trace or a 422 JSON blob** — an unknown enum
  value, a bad cursor, a non-numeric or out-of-int-range id and a missing row all land on
  ``error.html`` with the right status code.

Every test seeds through the ORM and then **commits**: the handler reads on its own session, so
uncommitted rows would be invisible to it.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlencode

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import JobBookmark, UserNote
from ingest.config import ConnectorConfig, cadence_period, load_connectors_config
from tests.support_web import (
    DAY,
    NOW,
    app_client,
    company_ids,
    flatten,
    form_state,
    job_ids,
    make_company,
    make_contact,
    make_job,
    make_location,
    make_note,
    make_round,
    make_run,
    make_sector,
    next_link,
    refresh_denormalized,
    select_options,
    walk_pages,
)
from web.routes.runs import connector_health
from web.templating import site_name

#: The header htmx puts on every request it makes.
HX = {"HX-Request": "true"}
#: ...plus this one when it is restoring a history entry and needs a whole page back.
HX_RESTORE = {"HX-Request": "true", "HX-History-Restore-Request": "true"}

TODAY = datetime.now(UTC).date()

#: One ``.run-row`` health line of ``/runs``: its modifier classes and the connector name.
_RUN_ROW = re.compile(r'<div class="run-row([^"]*)">\s*<span>([\w.-]+)</span>')
#: Every ``href`` on a page, for the URL-scheme sweep of :func:`web.templating.safe_url`.
_HREF = re.compile(r'href="([^"]*)"')
#: DOM ids and the htmx swap targets that name one.
_ID_ATTR = re.compile(r'\sid="([^"]+)"')
_HX_TARGET = re.compile(r'hx-target="#([^"]+)"')


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[httpx.AsyncClient]:
    async with app_client(session_factory) as http:
        yield http


# ----------------------------------------------------------------------------- the corpus


@dataclass(frozen=True, slots=True)
class Corpus:
    """Three companies chosen so that every SPEC §9 filter separates them differently."""

    northstar: int
    quiet: int
    tracked: int
    ns_software: int
    ns_marketing: int
    tracked_data: int
    tracked_closed: int


@pytest.fixture
async def corpus(session: AsyncSession) -> Corpus:
    northstar = await make_company(
        session,
        "Northstar Robotics",
        domain="northstar.example",
        website_url="https://northstar.example",
        one_liner="Warehouse robots that unload trucks",
        stage=enums.Stage.SERIES_A,
    )
    await make_location(session, northstar, "Oakland", is_hq=True)
    await make_sector(session, northstar, "Robotics")
    await make_round(
        session,
        northstar,
        round_type=enums.RoundType.SERIES_A,
        amount_usd=12_000_000,
        announced_date=TODAY - timedelta(days=10),
        investors=(("Foundry Group", True), ("Angel Collective", False)),
        refresh=False,
    )
    ns_software = await make_job(
        session,
        northstar,
        "Senior robotics engineer",
        url="https://northstar.example/jobs/1",
        location_text="Oakland, CA",
        role_family=enums.RoleFamily.SOFTWARE,
        employment_type=enums.EmploymentType.FULL_TIME,
        seniority=enums.Seniority.SENIOR,
        flexible_signal=True,
        refresh=False,
    )
    ns_marketing = await make_job(
        session,
        northstar,
        "Part-time growth marketer",
        role_family=enums.RoleFamily.MARKETING,
        employment_type=enums.EmploymentType.PART_TIME,
        seniority=enums.Seniority.MID,
        posted_at=NOW - DAY,
        refresh=False,
    )

    quiet = await make_company(session, "Quiet Ventures", stage=enums.Stage.SEED)
    await make_location(session, quiet, "Berkeley")
    await make_sector(session, quiet, "Fintech")
    await make_round(
        session,
        quiet,
        round_type=enums.RoundType.SEED,
        amount_usd=2_000_000,
        announced_date=date(2015, 1, 1),
        refresh=False,
    )

    # SPEC §12 Phase 8's "way in", as the two shapes the database really holds. Northstar
    # published an address on its own pages, so it is one of the few companies you can write to
    # directly; Quiet carries only the constructed people-search URL SPEC §6 builds for every
    # company from its name, which is a link to go looking, not somebody's inbox. The pair is
    # what makes ``has_email=1`` a filter rather than a no-op — a predicate that dropped either
    # half of ``kind = email AND confidence = published`` would return both of them.
    await make_contact(
        session,
        northstar,
        kind=enums.ContactKind.EMAIL,
        value="ada@northstar.example",
        confidence=enums.ContactConfidence.PUBLISHED,
    )
    await make_contact(
        session,
        quiet,
        kind=enums.ContactKind.LINKEDIN_PEOPLE,
        value="https://www.linkedin.com/search/results/people/?keywords=Quiet+Ventures",
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )

    tracked = await make_company(session, "Tracked Labs", stage=enums.Stage.PRE_SEED)
    await make_location(session, tracked, "San Jose")
    await make_note(session, tracked, status=enums.TrackingStatus.APPLIED, rating=5, note="Applied")
    tracked_data = await make_job(
        session,
        tracked,
        "Junior data analyst",
        role_family=enums.RoleFamily.DATA,
        employment_type=enums.EmploymentType.CONTRACT,
        seniority=enums.Seniority.JUNIOR,
        posted_at=NOW - 2 * DAY,
        refresh=False,
    )
    tracked_closed = await make_job(
        session,
        tracked,
        "Closed office manager",
        role_family=enums.RoleFamily.OPERATIONS,
        posted_at=NOW - 3 * DAY,
        closed_at=NOW - DAY,
        refresh=False,
    )

    await refresh_denormalized(session)
    await session.commit()
    return Corpus(
        northstar=northstar.id,
        quiet=quiet.id,
        tracked=tracked.id,
        ns_software=ns_software.id,
        ns_marketing=ns_marketing.id,
        tracked_data=tracked_data.id,
        tracked_closed=tracked_closed.id,
    )


# --------------------------------------------------------------------- the pages themselves


async def test_company_list_renders_every_section_of_a_row(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    response = await client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    body = response.text
    assert "<!doctype html>" in body.lower()
    assert 'class="filters"' in body and 'id="results"' in body
    assert company_ids(body) == [corpus.northstar, corpus.tracked, corpus.quiet]
    # SPEC §9's collapsed row: name, HQ city, sector chip, latest round, open count, families.
    assert f'<a href="/company/{corpus.northstar}">Northstar Robotics</a>' in body
    assert "<span>Oakland</span>" in body
    assert '<span class="chip sector">Robotics</span>' in body
    assert "Series A" in body and "$12.0M" not in body and "$12M" in body
    assert '<span class="count">2 open</span>' in body
    assert 'class="chip family"' in body


async def test_role_list_renders_every_open_role(client: httpx.AsyncClient, corpus: Corpus) -> None:
    response = await client.get("/roles")

    assert response.status_code == 200
    body = response.text
    # Default sort is posted_at DESC: the newest role first, the closed one absent.
    assert job_ids(body) == [corpus.ns_software, corpus.ns_marketing, corpus.tracked_data]
    assert "Senior robotics engineer" in body
    assert f'<a href="/company/{corpus.northstar}">Northstar Robotics</a>' in body
    assert '<span class="badge flexible">Flexible</span>' in body


async def test_runs_page_lists_configured_connectors_and_the_run_table(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    await make_run(
        session,
        "greenhouse",
        started_at=NOW - DAY,
        finished_at=NOW - DAY + timedelta(seconds=72),
        status=enums.FetchRunStatus.OK,
        n_fetched=41,
        n_upserted=7,
    )
    await make_run(
        session,
        "sec_edgar",
        started_at=NOW,
        # Finished, so `status` is the run's real outcome. The template renders the status cell
        # only for a finished run — see the in-flight test below.
        finished_at=NOW + timedelta(seconds=3),
        status=enums.FetchRunStatus.ERROR,
        error_text="HTTP 503 from efts.sec.gov",
    )
    await session.commit()

    response = await client.get("/runs")

    assert response.status_code == 200
    body = response.text
    assert "<h1>Ingestion health</h1>" in body
    # The last-100 table carries the class the stylesheet targets; `.run-row` is health only.
    assert '<table class="runs">' in body
    assert '<tr class="run-row' not in body
    assert "1m 12s" in body
    assert '<span class="status ok">OK</span>' in body
    assert '<span class="status error">Error</span>' in body
    assert "HTTP 503 from efts.sec.gov" in body
    # Every configured connector appears, including the ones with no implementation yet.
    named = {connector for _, connector in _RUN_ROW.findall(body)}
    assert {"greenhouse", "sec_edgar", "opencorporates", "product_hunt"} <= named


async def test_an_in_flight_run_reads_running_rather_than_error(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """``FetchRun.status`` defaults to ``error`` and the row is committed *before* the fetch
    starts (``ingest.pipeline._start_run``), so an unfinished run carries ``error`` while its
    outcome is simply not known yet.

    Rendering that default would label a healthy hour-long ``company_site`` sweep a failure for
    its whole duration, and would contradict the health block above it, which excludes
    unfinished runs from the failure streak for exactly this reason
    (``db.queries.consecutive_failure_counts``). The row is still listed — SPEC §9 wants the
    last 100 runs — with every cell whose value depends on the outcome held back.
    """
    await make_run(session, "company_site", started_at=NOW, finished_at=None)
    await session.commit()

    body = (await client.get("/runs")).text

    assert '<span class="muted">Running</span>' in body
    assert '<span class="status error">Error</span>' not in body


async def test_healthz_does_not_touch_the_database(client: httpx.AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_static_assets_are_served_locally(client: httpx.AsyncClient) -> None:
    for path in ("/static/styles.css", "/static/htmx.min.js"):
        response = await client.get(path)
        assert response.status_code == 200, path


# --------------------------------------------------------------------------- htmx contract


async def test_htmx_gets_a_fragment_and_a_history_restore_gets_the_page(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """A fragment for a swap; the whole page when htmx re-requests to restore history.

    Answering a restore with a fragment would blank the app: htmx replaces the whole body with
    what comes back, so the user would press Back and land on rows with no chrome.
    """
    fragment = await client.get("/", headers=HX)
    assert fragment.status_code == 200
    assert "<html" not in fragment.text
    assert 'class="filters"' not in fragment.text
    assert 'id="results"' not in fragment.text  # it is swapped *into* #results
    assert company_ids(fragment.text) == company_ids((await client.get("/")).text)

    restored = await client.get("/", headers=HX_RESTORE)
    assert "<!doctype html>" in restored.text.lower()
    assert 'id="results"' in restored.text

    role_fragment = await client.get("/roles", headers=HX)
    assert "<table" not in role_fragment.text
    assert role_fragment.text.lstrip().startswith("<tr")
    assert "<!doctype html>" in (await client.get("/roles", headers=HX_RESTORE)).text.lower()


async def test_load_more_carries_every_active_filter(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """The cursor is only valid for the query that minted it, so page 2 must repeat the filter.

    The city here contains a space on purpose: the "Load more" URL is rendered into an
    attribute, so it arrives percent-encoded *and* HTML-escaped, and a client that dropped
    either would silently paginate an unfiltered list.
    """
    wanted = await make_company(session, "San Jose Co")
    await make_location(session, wanted, "San Jose")
    other = await make_company(session, "Berkeley Co")
    await make_location(session, other, "Berkeley")
    for index in range(60):
        await make_job(
            session,
            wanted,
            f"Engineer {index:02d}",
            role_family=enums.RoleFamily.SOFTWARE,
            first_seen_at=NOW - index * DAY,
            refresh=False,
        )
    for index in range(10):
        await make_job(
            session,
            other,
            f"Elsewhere {index}",
            role_family=enums.RoleFamily.SOFTWARE,
            refresh=False,
        )
        await make_job(
            session, wanted, f"Designer {index}", role_family=enums.RoleFamily.DESIGN, refresh=False
        )
    await refresh_denormalized(session)
    await session.commit()

    url = "/roles?city=San+Jose&family=software&sort=first_seen"
    first = await client.get(url)
    follow_up = next_link(first.text)
    assert follow_up is not None
    assert "city=San+Jose" in follow_up and "family=software" in follow_up
    assert "&amp;" not in follow_up  # unescaped by `next_link`, as a browser would

    walked = flatten(await walk_pages(client, url, job_ids))
    assert len(walked) == 60 == len(set(walked))
    # Every id belongs to the filtered company's software roles, on page 2 as much as page 1.
    filtered_only = flatten(await walk_pages(client, "/roles?city=San+Jose", job_ids))
    assert set(walked) < set(filtered_only)
    assert len(filtered_only) == 70


async def test_an_htmx_fragment_carries_the_empty_state(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """A filter swap that matches nothing must say so inside ``#results``, not go blank."""
    companies = await client.get("/?q=zzzqqq+nothing", headers=HX)
    assert companies.status_code == 200
    assert "No companies match these filters" in companies.text
    assert '<a href="/">Clear filters</a>' in companies.text

    roles = await client.get("/roles?q=zzzqqq+nothing", headers=HX)
    assert "No roles match these filters" in roles.text
    assert roles.text.lstrip().startswith("<tr")  # still a table row, not a bare <p>


async def test_load_more_walks_the_whole_list(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """The button's URL is a real second page: disjoint ids, and 51 rows over two pages."""
    total = 51
    for index in range(total):
        company = await make_company(session, f"Company {index:03d}")
        await make_job(session, company, "Engineer", posted_at=NOW - index * DAY, refresh=False)
    await refresh_denormalized(session)
    await session.commit()

    first = await client.get("/")
    assert len(company_ids(first.text)) == 50
    follow_up = next_link(first.text)
    assert follow_up is not None and "cursor=" in follow_up

    second = await client.get(follow_up)
    assert second.status_code == 200
    assert set(company_ids(first.text)).isdisjoint(company_ids(second.text))
    assert next_link(second.text) is None

    walked = flatten(await walk_pages(client, "/"))
    assert len(walked) == total == len(set(walked))
    roles = flatten(await walk_pages(client, "/roles", job_ids))
    assert len(roles) == total == len(set(roles))


# ------------------------------------------------------------------ the §4.4 query parameters


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("", ("northstar", "tracked", "quiet")),
        ("q=Northstar", ("northstar",)),
        ("q=", ("northstar", "tracked", "quiet")),
        ("city=Oakland", ("northstar",)),
        ("city=Oakland&city=Berkeley", ("northstar", "quiet")),
        # The whole corpus sits in one region, so Region separates it from every other one
        # (SPEC §12 Phase 7); the dedicated tests below seed a second metro.
        ("metro=Bay+Area", ("northstar", "tracked", "quiet")),
        ("metro=New+York", ()),
        ("metro=", ("northstar", "tracked", "quiet")),
        ("metro=Bay+Area&city=Oakland", ("northstar",)),
        ("sector=fintech", ("quiet",)),
        ("stage=seed", ("quiet",)),
        ("stage=", ("northstar", "tracked", "quiet")),
        ("round=seed", ("quiet",)),
        ("amount_min=5000000", ("northstar",)),
        ("amount_max=5000000", ("quiet",)),
        ("round_months=6", ("northstar",)),
        ("family=marketing", ("northstar",)),
        ("employment=contract", ("tracked",)),
        ("seniority=junior", ("tracked",)),
        ("flexible=1", ("northstar",)),
        ("open_roles=1", ("northstar", "tracked")),
        # SPEC §12 Phase 8. Quiet has a contact row too, so a hit here is the confidence and
        # kind halves of the predicate doing the work rather than "the only company with any
        # contact at all"; the emptied parameter is the untouched checkbox, i.e. every company.
        ("has_email=1", ("northstar",)),
        ("has_email=", ("northstar", "tracked", "quiet")),
        ("tracking=applied", ("tracked",)),
        ("tracking=none", ("northstar", "quiet")),
        ("sort=name", ("northstar", "quiet", "tracked")),
        ("sort=open_roles", ("northstar", "tracked", "quiet")),
        ("unknown_parameter=1", ("northstar", "tracked", "quiet")),
        # One open role must satisfy every role filter at once (SPEC §9).
        ("family=software&employment=part_time", ()),
        ("family=marketing&employment=part_time", ("northstar",)),
    ],
)
async def test_company_filters_round_trip_through_http(
    client: httpx.AsyncClient, corpus: Corpus, query: str, expected: tuple[str, ...]
) -> None:
    response = await client.get(f"/?{query}")

    assert response.status_code == 200, response.text[:400]
    assert company_ids(response.text) == [getattr(corpus, name) for name in expected]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("", ("ns_software", "ns_marketing", "tracked_data")),
        ("family=data", ("tracked_data",)),
        ("employment=part_time", ("ns_marketing",)),
        ("seniority=senior", ("ns_software",)),
        ("flexible=1", ("ns_software",)),
        ("closed=1", ("ns_software", "ns_marketing", "tracked_data", "tracked_closed")),
        ("q=marketer", ("ns_marketing",)),
        ("sort=company", ("ns_software", "ns_marketing", "tracked_data")),
        ("city=San+Jose", ("tracked_data",)),
        ("metro=Bay+Area", ("ns_software", "ns_marketing", "tracked_data")),
        ("metro=New+York", ()),
        # A company-level filter on the role list: it selects whole companies, so what survives
        # is every open role of the one company you can write to (SPEC §12 Phase 8).
        ("has_email=1", ("ns_software", "ns_marketing")),
    ],
)
async def test_role_filters_round_trip_through_http(
    client: httpx.AsyncClient, corpus: Corpus, query: str, expected: tuple[str, ...]
) -> None:
    response = await client.get(f"/roles?{query}")

    assert response.status_code == 200, response.text[:400]
    assert job_ids(response.text) == [getattr(corpus, name) for name in expected]


# ------------------------------------------------- the region selector (SPEC §12 Phase 7)


async def test_an_empty_metro_parameter_is_the_default_view(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """``?metro=`` is an untouched control, not a request for the empty region.

    The module's second rule (``web.filters``): a GET form submits every field it has, and a
    hand-cleared URL keeps the parameter with nothing after the ``=``. Both have to be the
    unfiltered view. It is worth its own test for ``metro`` because the value is free text
    rather than an enum — nothing downstream would reject ``""``, so a blank that survived
    ``_clean`` would filter on a region no row can have and empty both pages instead of
    raising, which is the failure that looks like data loss rather than like a bug.
    """
    companies = await client.get("/?metro=")
    assert companies.status_code == 200
    assert company_ids(companies.text) == company_ids((await client.get("/")).text)
    roles = await client.get("/roles?metro=")
    assert job_ids(roles.text) == job_ids((await client.get("/roles")).text)
    # ...and the form comes back with nothing selected, so the next click is not narrower
    # either — an echoed blank would submit `?metro=` again and hide the difference forever.
    assert form_state(companies.text)["metro"] == []
    assert form_state(roles.text)["metro"] == []


async def test_an_unknown_metro_is_an_empty_page_not_an_error(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """An unrecognised region returns nothing and answers 200 — it is not a 400.

    ``metro`` and ``city`` are the two parameters with no closed vocabulary to validate
    against: their values are strings the connectors stored, and any list this code could check
    them against would be wrong again after the next run. So a stale bookmark naming a region
    that was renamed — or switched off in ``config/regions.yaml`` and later merged away — shows
    an empty result with the filter row still on screen to clear, rather than an error page.
    """
    companies = await client.get("/?metro=Atlantis")
    assert companies.status_code == 200
    assert company_ids(companies.text) == []
    assert "No companies match these filters" in companies.text

    roles = await client.get("/roles?metro=Atlantis")
    assert roles.status_code == 200
    assert job_ids(roles.text) == []
    assert "No roles match these filters" in roles.text


async def test_the_region_select_renders_the_facet_and_groups_the_cities(
    client: httpx.AsyncClient, session: AsyncSession, corpus: Corpus
) -> None:
    """Region offers the database's metros; City is grouped into one ``<optgroup>`` per metro.

    Both lists come from ``locations`` and not from ``config/regions.yaml`` (SPEC §11), so the
    form cannot offer a value that returns nothing. The grouping is what makes the City select
    usable once the config holds six regions instead of one, and it is rendered server-side:
    there is no JavaScript in this form, and picking a region deliberately does not repopulate
    the City list — the two filters compose as an ``AND``.
    """
    empire = await make_company(session, "Empire Data")
    await make_location(session, empire, "Brooklyn", is_hq=True)
    await make_location(session, empire, "Jersey City")
    pike = await make_company(session, "Pike Systems")
    await make_location(session, pike, "Seattle")
    await session.commit()

    body = (await client.get("/")).text

    # Every metro in the database, alphabetically, and no group of its own: the metro *is* the
    # wire value here.
    assert select_options(body, "#f-metro") == [
        (None, "Bay Area", "Bay Area"),
        (None, "New York", "New York"),
        (None, "Seattle", "Seattle"),
    ]
    # ...and every city under the region it belongs to, the New Jersey one included — the group
    # is the metro, not the state.
    assert select_options(body, "#f-city") == [
        ("Bay Area", "Berkeley", "Berkeley"),
        ("Bay Area", "Oakland", "Oakland"),
        ("Bay Area", "San Jose", "San Jose"),
        ("New York", "Brooklyn", "Brooklyn"),
        ("New York", "Jersey City", "Jersey City"),
        ("Seattle", "Seattle", "Seattle"),
    ]
    # The control is not decoration: what it offers is what the page then filters by.
    assert company_ids((await client.get("/?metro=New+York")).text) == [empire.id]
    assert company_ids((await client.get("/?metro=Seattle")).text) == [pike.id]


async def test_a_city_name_in_two_states_carries_its_state_into_the_label(
    client: httpx.AsyncClient, session: AsyncSession, corpus: Corpus
) -> None:
    """Two ``Newark``s: the label tells them apart, the submitted value stays the bare name.

    ``uq_locations_city_state`` makes ``locations`` one row per ``(city, state)``, so a name
    that repeats is by definition a name in two states — and without the state in the label the
    user is choosing between two identical-looking options. The state reaches the label *only*:
    the value is what every URL bookmarked before SPEC §12 Phase 7 holds, so the filter still
    matches on the name alone and matches both, and Region is how you narrow it to one. A name
    that occurs once keeps its bare label, which is all but one of them.
    """
    ironbound = await make_company(session, "Ironbound Labs")
    await make_location(session, ironbound, "Newark")  # NJ, in the New York metro
    dumbarton = await make_company(session, "Dumbarton Robotics")
    await make_location(session, dumbarton, "Newark", state="CA", metro="Bay Area")
    await session.commit()

    options = select_options((await client.get("/")).text, "#f-city")

    assert ("New York", "Newark", "Newark, NJ") in options
    assert ("Bay Area", "Newark", "Newark, CA") in options
    assert ("Bay Area", "Oakland", "Oakland") in options
    assert set(company_ids((await client.get("/?city=Newark")).text)) == {
        ironbound.id,
        dumbarton.id,
    }
    assert company_ids((await client.get("/?city=Newark&metro=New+York")).text) == [ironbound.id]


async def test_a_row_whose_hq_is_outside_the_filter_names_the_office_that_matched(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """The region selector must not look like it leaks (SPEC §12 Phase 7, SPEC §9's row).

    The collapsed row names the HQ city; the filter asks whether *any* office qualifies. So a
    company headquartered in one metro with an office in another is a correct hit for either,
    and a row showing only "San Francisco" in a list filtered to Seattle reads as a broken
    filter. It has to say which office answered. Measured on the live Y Combinator data before
    this landed: 19 of the 300 rows across the six single-region filters were unexplained.

    The marker is strictly additive — it appears only when the row is not already naming the
    matching city, and never at all when nothing geographic is filtered — so the default view
    is untouched.
    """
    split = await make_company(session, "Split Harbour")
    await make_location(session, split, "San Francisco", is_hq=True)
    await make_location(session, split, "Seattle")
    local = await make_company(session, "Pike Systems")
    await make_location(session, local, "Seattle", is_hq=True)
    await session.commit()

    filtered = (await client.get("/?metro=Seattle")).text
    assert set(company_ids(filtered)) == {split.id, local.id}
    # The out-of-region HQ is still what the row leads with, and the office explains it.
    assert "San Francisco" in filtered
    assert "Seattle office" in filtered
    # The company that is simply in Seattle says nothing extra — one marker, not two.
    assert filtered.count("office</span>") == 1

    # Filtered to the metro the HQ *is* in, the row has nothing to add.
    assert "office</span>" not in (await client.get("/?metro=Bay+Area")).text
    # And with nothing geographic filtered, no row may sprout a second city.
    assert "office</span>" not in (await client.get("/")).text
    # A city filter has always had the same shape and is treated the same way.
    assert "Seattle office" in (await client.get("/?city=Seattle")).text


async def test_load_more_carries_the_metro_filter(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """Page 2 of a filtered region is still that region.

    The "load more" URL is this request's URL with a new cursor
    (``web.templating.query_string``), which is why a new filter parameter needed no change
    there — and this is the test that says so, because the keyset cursor is only valid for the
    query that minted it and a dropped filter would quietly paginate the whole database. The
    region name contains a space, so it also pins that the value survives percent-encoding and
    the HTML-escaping of the attribute it is rendered into.
    """
    total = 51
    for index in range(total):
        company = await make_company(session, f"Empire {index:03d}")
        await make_location(session, company, "Brooklyn")
    elsewhere = await make_company(session, "Bay Co")
    await make_location(session, elsewhere, "Oakland")
    await session.commit()

    first = await client.get("/?metro=New+York")
    follow_up = next_link(first.text)
    assert follow_up is not None
    assert "metro=New+York" in follow_up
    assert "&amp;" not in follow_up  # unescaped by `next_link`, as a browser would

    walked = flatten(await walk_pages(client, "/?metro=New+York"))
    assert len(walked) == total == len(set(walked))
    assert elsewhere.id not in walked


async def test_the_metro_filter_reaches_both_the_company_and_the_role_list(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """One dependency, two pages — SPEC §9's "same filter set", asserted mechanically.

    ``metro`` is declared once, in ``web.filters.shared_filters``, and neither route was taught
    about it. Had it been added to ``/``'s own dependency instead — the shorter change, and the
    one a reader of ``companies.py`` alone would make — this is the assertion that would fail,
    with the two views a parameter apart and the shared filter form submitting to a page that
    ignores one of its controls.
    """
    empire = await make_company(session, "Empire Data")
    await make_location(session, empire, "Brooklyn")
    ny_role = await make_job(session, empire, "Data engineer", refresh=False)
    bay = await make_company(session, "Bay Data")
    await make_location(session, bay, "Oakland")
    await make_job(session, bay, "Data engineer", refresh=False)
    await refresh_denormalized(session)
    await session.commit()

    companies = await client.get("/?metro=New+York")
    roles = await client.get("/roles?metro=New+York")

    assert company_ids(companies.text) == [empire.id]
    assert job_ids(roles.text) == [ny_role.id]
    # ...and both pages render the one shared control with the region still selected, so the
    # next submission from either is the same query.
    assert form_state(companies.text)["metro"] == ["New York"]
    assert form_state(roles.text)["metro"] == ["New York"]


async def test_a_closed_role_is_badged_when_it_is_shown(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    body = (await client.get("/roles?closed=1")).text
    assert '<span class="badge closed">Closed</span>' in body
    assert '<span class="badge closed">Closed</span>' not in (await client.get("/roles")).text


# --------------------------------------------- the "way in" filter (SPEC §12 Phase 8)


async def test_the_way_in_filter_is_opt_in_and_never_narrows_by_default(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """``has_email`` selects companies you can write to, and only once it is ticked (§7.1).

    Two spellings of "unset" have to mean the same thing as no parameter at all: the absent one
    a first visit sends, and the ``?has_email=`` a GET form produces for an untouched box —
    ``web.filters``' second rule, and the one that costs the most if it slips here. Almost no
    company has a published address (31 of 9,220 when this phase was written), so a box that
    read a blank value as "false but set", or that came up ticked, would not trim the list: it
    would empty it, and the reader would be looking at 0.3% of the database believing it is all
    of it.

    The narrowing is asserted against a corpus where the excluded companies are not simply
    contactless — Quiet carries a contact row of its own — so what is being pinned is the
    predicate, not the absence of data.
    """
    everything = company_ids((await client.get("/")).text)
    assert everything == [corpus.northstar, corpus.tracked, corpus.quiet]

    assert company_ids((await client.get("/?has_email=")).text) == everything
    assert company_ids((await client.get("/?has_email=0")).text) == everything
    assert company_ids((await client.get("/?has_email=1")).text) == [corpus.northstar]

    # ...and the ticked box comes back ticked, so the next submission from the rendered form is
    # the same query rather than the unfiltered one.
    assert form_state((await client.get("/?has_email=1")).text)["has_email"] == ["1"]
    assert form_state((await client.get("/?has_email=")).text)["has_email"] == []


async def test_the_way_in_filter_composes_with_a_region_and_reaches_both_lists(
    client: httpx.AsyncClient, session: AsyncSession, corpus: Corpus
) -> None:
    """One dependency, two pages, and an ``AND`` beside every other filter — SPEC §9.

    ``has_email`` is declared once, in ``web.filters.shared_filters``, and neither route was
    taught about it; this is the assertion that says so. Had it been added to ``/``'s own
    dependency instead — the shorter change, and the one a reader of ``companies.py`` alone
    would make — ``/roles`` would silently ignore a control the shared filter form still
    renders and still submits to it.

    Composition is checked against Region rather than against another checkbox because the two
    are the phase's actual query: a part-time offer is made to a small company you can get to,
    so "reachable" is only useful *within* a metro. The out-of-region company is reachable and
    the out-of-reach company is in the region, so neither filter can produce this answer alone.
    """
    empire = await make_company(session, "Empire Data")
    await make_location(session, empire, "Brooklyn")
    # Older than every corpus role, so the role list's default ``posted_at DESC`` puts it last
    # and the assertion below is about the filter rather than about a tie-break.
    empire_role = await make_job(
        session, empire, "Data engineer", posted_at=NOW - 3 * DAY, refresh=False
    )
    await make_contact(
        session,
        empire,
        kind=enums.ContactKind.EMAIL,
        value="grace@empire.example",
        confidence=enums.ContactConfidence.PUBLISHED,
    )
    await refresh_denormalized(session)
    await session.commit()

    reachable = await client.get("/?has_email=1")
    assert set(company_ids(reachable.text)) == {corpus.northstar, empire.id}

    in_region = await client.get("/?has_email=1&metro=New+York")
    assert company_ids(in_region.text) == [empire.id]
    # Each half alone is strictly wider, so the conjunction is what did the narrowing: the
    # region on its own keeps Empire while dropping the reachable Bay Area company, and
    # reachability on its own keeps both.
    assert company_ids((await client.get("/?metro=New+York")).text) == [empire.id]
    assert corpus.northstar in company_ids(reachable.text)
    assert corpus.northstar not in company_ids(in_region.text)

    # The same filter on the role list, through the same dependency: a company-level predicate,
    # so what comes back is that company's roles rather than a marked-up subset of them.
    roles = await client.get("/roles?has_email=1&metro=New+York")
    assert job_ids(roles.text) == [empire_role.id]
    assert job_ids((await client.get("/roles?has_email=1")).text) == [
        corpus.ns_software,
        corpus.ns_marketing,
        empire_role.id,
    ]
    # ...and both pages re-render the one shared control holding its state (§9's "same filter
    # set"), so a form submitted from either lands on the same query.
    assert form_state(in_region.text)["has_email"] == ["1"]
    assert form_state(roles.text)["has_email"] == ["1"]


# ------------------------------------------------------- the filter form's own state (§9)

#: Every §4.4 parameter the company form carries, chosen so the whole set still selects
#: Northstar: the four role parameters are satisfied by one single role (``ns_software``),
#: which is what §9's one-EXISTS reading of the role filters demands, a company with no
#: ``user_notes`` row is what ``tracking=none`` means, and the published address the corpus
#: gives Northstar is what ``has_email=1`` (SPEC §12 Phase 8) asks for. This constant is the
#: closed-set half of the parameter table: a filter added to ``web.filters.shared_filters``
#: and not to this string is one nothing here ever submits alongside the others.
EVERY_COMPANY_FILTER = (
    "q=Northstar&city=Oakland&metro=Bay+Area&sector=robotics&stage=series_a&round=series_a"
    "&amount_min=1000000&amount_max=90000000&round_months=12"
    "&family=software&employment=full_time&seniority=senior&flexible=1"
    "&open_roles=1&has_email=1&tracking=none&sort=name"
)


async def test_the_filter_form_re_renders_its_own_active_state(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """Unaided, the rendered form must submit the very query the page was rendered for.

    ``hx-push-url`` puts the filtered URL in the address bar, so the *whole page* is rebuilt
    from that URL by three paths a live htmx session never exercises — a bookmark, the back
    button, and htmx's own history restore — and with JavaScript off it is the only path
    there is. A control that dropped its state would look identical on screen and quietly
    submit a wider query on the next click, which is why this is asserted on what the form
    would submit rather than on the markup that carries it.
    """
    response = await client.get(f"/?{EVERY_COMPANY_FILTER}")
    assert company_ids(response.text) == [corpus.northstar]

    state = form_state(response.text)
    assert state == {
        "q": ["Northstar"],
        "city": ["Oakland"],
        # The two geography controls are separate and both echo — a Region that came back empty
        # would widen the query on the next click without changing anything on screen.
        "metro": ["Bay Area"],
        "sector": ["robotics"],
        "stage": ["series_a"],
        "round": ["series_a"],
        "amount_min": ["1000000"],
        "amount_max": ["90000000"],
        "round_months": ["12"],
        "family": ["software"],
        "employment": ["full_time"],
        "seniority": ["senior"],
        "tracking": ["none"],
        "flexible": ["1"],
        "open_roles": ["1"],
        # A checkbox that came back clear looks identical to one that was never ticked, so the
        # next click would silently widen the list back to the 9,000-odd companies with no
        # address — which is the whole database (SPEC §12 Phase 8).
        "has_email": ["1"],
        "sort": ["name"],
        # `closed` is a /roles control; the company form must not offer it (§4.4).
    }

    # The echo is load-bearing, not decoration: submitting exactly what the form holds is
    # what the next click does, and it has to land on the same row.
    resubmitted = urlencode([(name, value) for name, values in state.items() for value in values])
    assert company_ids((await client.get(f"/?{resubmitted}")).text) == [corpus.northstar]

    # An htmx history restore re-requests the same URL and replaces the whole document, so
    # the form it brings back must carry the state too.
    restored = await client.get(f"/?{EVERY_COMPANY_FILTER}", headers=HX_RESTORE)
    assert form_state(restored.text) == state


async def test_the_roles_filter_form_re_renders_its_own_active_state(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """``/roles`` shares the form but owns two of its controls: ``closed`` and its own sort."""
    query = "q=marketer&city=Oakland&family=marketing&employment=part_time&closed=1&sort=company"
    response = await client.get(f"/roles?{query}")
    assert job_ids(response.text) == [corpus.ns_marketing]

    state = form_state(response.text)
    assert state["closed"] == ["1"]
    assert state["sort"] == ["company"]  # the JobSort vocabulary, not CompanySort's
    assert state["q"] == ["marketer"]
    assert state["city"] == ["Oakland"]
    assert state["family"] == ["marketing"]
    assert state["employment"] == ["part_time"]

    resubmitted = urlencode([(name, value) for name, values in state.items() for value in values])
    assert job_ids((await client.get(f"/roles?{resubmitted}")).text) == [corpus.ns_marketing]


async def test_an_unfiltered_page_pre_selects_nothing(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """The mirror image, and §7.1 restated as a property of the form itself.

    "Classification never excludes" only holds by default if the default form is empty: a
    control that came up selected on its own would filter the very first submission.
    """
    unset: dict[str, list[str]] = {
        "q": [""],
        "amount_min": [""],
        "amount_max": [""],
        "round_months": [""],
        "city": [],
        "metro": [],
        "sector": [],
        "stage": [],
        "round": [],
        "family": [],
        "employment": [],
        "seniority": [],
        "tracking": [],
        "flexible": [],
        "open_roles": [],
        # SPEC §12 Phase 8's box is opt-in like the two above it, and it is the one where a
        # default-on control would be indefensible: 9,189 of 9,220 companies have no published
        # address, so a form that shipped this ticked would open on 0.3% of the database.
        "has_email": [],
    }

    assert form_state((await client.get("/")).text) == unset | {"sort": ["recent_job"]}
    assert form_state((await client.get("/roles")).text) == unset | {
        "sort": ["posted"],
        "closed": [],
    }


# ------------------------------------------------------------------------- errors are pages


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("/company/999999", 404),
        ("/company/999999/panel", 404),
        ("/company/abc", 400),
        ("/company/0", 400),  # ids start at 1
        ("/company/2147483648", 400),  # past Postgres' int4 — not "a row that is missing"
        ("/?cursor=garbage", 400),
        ("/?cursor=bnVsbA", 400),
        ("/?stage=serious", 400),
        ("/?sort=sideways", 400),
        ("/roles?family=wizardry", 400),
        ("/?amount_min=-1", 400),
        ("/?amount_min=1.5", 400),
        ("/?round_months=0", 400),
        ("/?round_months=999", 400),
        # Numbers wider than the column they are bound against: without a bound they reach
        # asyncpg and raise DataError, i.e. a 500 from a URL anyone can type.
        ("/?amount_min=99999999999999999999", 400),
        ("/?amount_max=99999999999999999999", 400),
        ("/?cursor=eyJzIjoicmVjZW50X2pvYiIsImsiOm51bGwsImkiOjEwMDAwMDAwMDAwMDAwMH0", 400),
        (
            "/?sort=open_roles&cursor=eyJzIjoib3Blbl9yb2xlcyIsImsiOjEwMDAwMDAwMDAwMDAwMCwiaSI6MX0",
            400,
        ),
        (
            "/roles?sort=first_seen&cursor="
            "eyJzIjoiZmlyc3Rfc2VlbiIsImsiOm51bGwsImkiOjEwMDAwMDAwMDAwMDAwMH0",
            400,
        ),
    ],
)
async def test_a_bad_url_renders_the_error_page(
    client: httpx.AsyncClient, path: str, status: int
) -> None:
    response = await client.get(path)

    assert response.status_code == status
    assert response.headers["content-type"].startswith("text/html")
    assert 'class="error-page"' in response.text
    assert f"<h1>{status}</h1>" in response.text
    # Not FastAPI's raw 422 JSON blob, and the chrome is still there to navigate away with —
    # carrying the name derived from config/regions.yaml rather than one written into the
    # template (SPEC §11, §12 Phase 7). The derivation itself is pinned in tests/test_regions.py;
    # this is the assertion that the templates still call it.
    assert f'<a class="brand" href="/">{site_name()}</a>' in response.text


@pytest.mark.parametrize(
    "path",
    [
        "/?q=%25",  # pg_trgm's own operator, as a search term
        "/?q='\"",
        "/?q=" + "a" * 500,
        "/?q=%00",  # a NUL: Postgres text cannot hold one at all
        "/?city=%00",
        "/?metro=%00",
        "/?sector=%00",
        "/roles?q=%00",
        "/?q=+++",
        "/?amount_min=0&amount_max=0",
        "/?amount_min=100&amount_max=1",  # an inverted range matches nothing, it is not an error
        "/?stage=seed&stage=seed",  # a repeat is collapsed, not doubled
        "/?flexible=on&open_roles=true&tracking=none&tracking=applied",
        "/?q=&city=&sector=&stage=&round=&amount_min=&amount_max=&round_months=&sort=&cursor=",
        "/roles?q=%3Cscript%3E",
    ],
)
async def test_a_hostile_query_string_is_answered_not_crashed(
    client: httpx.AsyncClient, corpus: Corpus, path: str
) -> None:
    """Anything a URL bar can hold must be a page, never a 500 out of asyncpg."""
    response = await client.get(path)
    assert response.status_code == 200, response.text[:400]


async def test_a_blank_form_submission_is_the_default_view(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """A GET form submits every field it has, including the empty ones (``?q=&stage=``)."""
    blank = "/?q=&city=&metro=&sector=&stage=&round=&amount_min=&amount_max=&round_months=&sort="
    assert company_ids((await client.get(blank)).text) == company_ids((await client.get("/")).text)


async def test_an_unknown_enum_value_names_the_parameter(client: httpx.AsyncClient) -> None:
    response = await client.get("/?stage=serious")
    assert "unknown stage" in response.text
    assert "series_a" in response.text  # the accepted values are listed


async def test_a_cursor_minted_for_another_page_is_refused(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    for index in range(51):
        await make_company(session, f"Company {index:03d}")
    await session.commit()

    company_cursor = next_link((await client.get("/")).text)
    assert company_cursor is not None
    crossed = company_cursor.replace("/?", "/roles?")

    assert (await client.get(crossed)).status_code == 400


async def test_errors_are_json_when_html_was_not_asked_for(client: httpx.AsyncClient) -> None:
    response = await client.get("/company/999999", headers={"accept": "application/json"})
    assert response.status_code == 404
    assert response.json()["status_code"] == 404
    assert "999999" in response.json()["message"]


# ----------------------------------------------------- the company detail page and its panel


async def test_company_detail_page(client: httpx.AsyncClient, corpus: Corpus) -> None:
    response = await client.get(f"/company/{corpus.northstar}")

    assert response.status_code == 200
    body = response.text
    assert "<h1>Northstar Robotics</h1>" in body
    # SPEC §9's panel sections, as level-2 headings under the company name.
    for heading in ("About", "Locations", "Funding", "Open roles", "Contacts", "Provenance"):
        assert f"<h2>{heading}" in body, heading
    assert "Warehouse robots that unload trucks" in body
    assert "Oakland, CA · HQ" in body
    assert "Foundry Group" in body and "(lead)" in body
    assert 'class="note-form"' in body
    assert job_ids(body) == [corpus.ns_software, corpus.ns_marketing]


async def test_the_panel_shows_every_role_and_only_marks_the_matching_ones(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """SPEC §9, §7.1: a role filter highlights; it never removes a role from the panel."""
    unfiltered = await client.get(f"/company/{corpus.northstar}/panel")
    filtered = await client.get(f"/company/{corpus.northstar}/panel?family=software")

    assert unfiltered.status_code == filtered.status_code == 200
    assert "<html" not in filtered.text  # a fragment, swapped into the row
    assert (
        job_ids(filtered.text)
        == job_ids(unfiltered.text)
        == [
            corpus.ns_software,
            corpus.ns_marketing,
        ]
    )
    assert unfiltered.text.count('<tr class="match">') == 0
    assert filtered.text.count('<tr class="match">') == 1
    assert "Part-time growth marketer" in filtered.text

    # Two role filters conjoin, exactly as the company list's single EXISTS does.
    both = await client.get(
        f"/company/{corpus.northstar}/panel?family=software&employment=part_time"
    )
    assert len(job_ids(both.text)) == 2
    assert both.text.count('<tr class="match">') == 0


async def test_the_row_hands_its_role_filter_to_the_panel(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """``job_filter_query_string`` — only the role-level parameters travel to the panel."""
    body = (await client.get("/?family=software&city=Oakland&flexible=1")).text
    assert f'hx-get="/company/{corpus.northstar}/panel?family=software&amp;flexible=1"' in body
    plain = (await client.get("/")).text
    assert f'hx-get="/company/{corpus.northstar}/panel"' in plain


async def test_the_detail_page_hides_closed_roles_and_shows_the_saved_note(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """The two context values the panel handlers compute, on a company that exercises both.

    SPEC §9: the roles table is the *open* roles the collapsed row counted — a closed job is
    kept forever (§2 never deletes) but it is history, not a listing — and the note form
    renders what is actually stored. A blank note form is the dangerous half: it looks like a
    company you never tracked, and the next Save posts all three fields, so it would
    overwrite the status, rating and note that are in the database.

    ``/company/{id}`` and ``/company/{id}/panel`` compute the pair separately, so both are
    asked; ``corpus.tracked`` is the only company with a closed role *and* a saved note.
    """
    for path in (f"/company/{corpus.tracked}", f"/company/{corpus.tracked}/panel"):
        body = (await client.get(path)).text

        assert job_ids(body) == [corpus.tracked_data], path
        assert "Closed office manager" not in body, path
        assert form_state(body, "form.note-form") == {
            "status": ["applied"],
            "rating": ["5"],
            "note": ["Applied"],
        }, path


async def test_no_page_level_role_filter_ever_shrinks_the_panel(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """SPEC §9, §7.1: the collapsed row promised a count, so the panel must be able to show
    every one of those roles — whatever the page-level filter that opened it was.

    The panel URL is the row's own ``job_filter_qs`` appended, so the role parameters really
    do arrive here, and every one of them may only mark a row (``.match``), never remove it.
    """
    every_role = [corpus.ns_software, corpus.ns_marketing]
    assert job_ids((await client.get(f"/company/{corpus.northstar}/panel")).text) == every_role

    for query in (
        "family=software",
        "employment=part_time",
        "seniority=senior",
        "flexible=1",
        # A filter matching none of the company's roles is the case that would empty a table
        # built by filtering rather than by highlighting.
        "family=design&employment=internship&seniority=intern&flexible=1",
    ):
        panel = await client.get(f"/company/{corpus.northstar}/panel?{query}")
        assert panel.status_code == 200, query
        assert job_ids(panel.text) == every_role, query


# ------------------------------------------------------------------------- the note form (§9)


async def test_note_create_then_update(
    client: httpx.AsyncClient, session: AsyncSession, corpus: Corpus
) -> None:
    created = await client.post(
        f"/company/{corpus.northstar}/note",
        data={"status": "interested", "rating": "4", "note": "  Strong team  "},
        headers=HX,
    )

    assert created.status_code == 200
    assert 'class="note-form"' in created.text
    assert "<html" not in created.text
    assert '<option value="interested" selected>' in created.text
    assert '<option value="4" selected>4</option>' in created.text
    assert "Strong team" in created.text

    session.expire_all()
    note = await session.get(UserNote, corpus.northstar)
    assert note is not None
    assert (note.status, note.rating, note.note) == (
        enums.TrackingStatus.INTERESTED,
        4,
        "Strong team",  # trimmed
    )
    first_saved_at = note.updated_at

    updated = await client.post(
        f"/company/{corpus.northstar}/note",
        data={"status": "applied", "rating": "", "note": "   "},
        headers=HX,
    )

    assert updated.status_code == 200
    assert '<option value="applied" selected>' in updated.text
    session.expire_all()
    note = await session.get(UserNote, corpus.northstar)
    assert note is not None
    # An emptied field clears the column rather than storing "" or a zero rating.
    assert (note.status, note.rating, note.note) == (enums.TrackingStatus.APPLIED, None, None)
    # Strictly greater, because the upsert has to refresh the column itself: the model's
    # ORM-level `onupdate` does not fire for INSERT ... ON CONFLICT, and `>=` would be
    # satisfied by the frozen INSERT timestamp while `_note_form.html` went on telling the
    # user their latest edit landed at the time of their very first save. Two POSTs are two
    # requests and therefore two transactions, so `now()` really does advance between them.
    assert note.updated_at > first_saved_at


@pytest.mark.parametrize(
    "form",
    [
        {"status": "interested", "rating": "9", "note": ""},
        {"status": "interested", "rating": "0", "note": ""},
        {"status": "interested", "rating": "abc", "note": ""},
        {"status": "serious", "rating": "", "note": ""},
    ],
)
async def test_a_bad_note_field_is_a_400(
    client: httpx.AsyncClient, session: AsyncSession, corpus: Corpus, form: dict[str, str]
) -> None:
    response = await client.post(f"/company/{corpus.northstar}/note", data=form, headers=HX)

    assert response.status_code == 400
    assert 'class="error-page"' in response.text
    session.expire_all()
    assert await session.get(UserNote, corpus.northstar) is None


async def test_a_pasted_nul_byte_does_not_break_the_note(
    client: httpx.AsyncClient, session: AsyncSession, corpus: Corpus
) -> None:
    """A Postgres ``text`` column cannot hold U+0000, so it is dropped before the INSERT."""
    response = await client.post(
        f"/company/{corpus.quiet}/note",
        data={"status": "interested", "rating": "", "note": "before\x00after"},
        headers=HX,
    )

    assert response.status_code == 200
    session.expire_all()
    note = await session.get(UserNote, corpus.quiet)
    assert note is not None and note.note == "beforeafter"


async def test_a_note_for_an_unknown_company_is_a_404(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/company/999999/note", data={"status": "none", "rating": "", "note": ""}, headers=HX
    )
    assert response.status_code == 404


async def test_a_non_htmx_note_post_redirects(
    client: httpx.AsyncClient, session: AsyncSession, corpus: Corpus
) -> None:
    """With JavaScript off the form is an ordinary POST, so it answers 303 (never a re-post)."""
    response = await client.post(
        f"/company/{corpus.tracked}/note",
        data={"status": "offer", "rating": "5", "note": "Verbal"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == f"/company/{corpus.tracked}"
    session.expire_all()
    note = await session.get(UserNote, corpus.tracked)
    assert note is not None and note.status == enums.TrackingStatus.OFFER


# --------------------------------------------------------------------- the bookmark toggle (§9)


async def test_bookmark_toggles_on_then_off(
    client: httpx.AsyncClient, session: AsyncSession, corpus: Corpus
) -> None:
    url = f"/jobs/{corpus.ns_software}/bookmark"

    on = await client.post(url, headers=HX)
    assert on.status_code == 200
    assert 'class="star on"' in on.text
    assert 'aria-pressed="true"' in on.text
    session.expire_all()
    bookmark = await session.get(JobBookmark, corpus.ns_software)
    assert bookmark is not None and bookmark.starred is True

    off = await client.post(url, headers=HX)
    assert 'class="star on"' not in off.text
    assert 'aria-pressed="false"' in off.text
    session.expire_all()
    bookmark = await session.get(JobBookmark, corpus.ns_software)
    assert bookmark is not None and bookmark.starred is False

    # ...and the list agrees with the toggle.
    assert 'class="star on"' in (await client.post(url, headers=HX)).text
    assert 'class="star on"' in (await client.get("/roles")).text


async def test_bookmark_without_htmx_redirects_and_an_unknown_job_is_a_404(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    redirected = await client.post(f"/jobs/{corpus.tracked_data}/bookmark", follow_redirects=False)
    assert redirected.status_code == 303
    assert redirected.headers["location"] == "/roles"

    assert (await client.post("/jobs/999999/bookmark", headers=HX)).status_code == 404


# ------------------------------------------------------------- escaping and URL safety (§9)


async def test_a_hostile_job_url_and_title_never_reach_an_href(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """``safe_url`` plus Jinja autoescaping: job URLs are scraped from third-party pages."""
    company = await make_company(
        session, "Hostile <b>Corp</b>", website_url="javascript:alert('site')"
    )
    await make_job(
        session,
        company,
        "<script>alert(1)</script> Engineer",
        url="javascript:alert(1)",
        refresh=False,
    )
    await make_job(session, company, "Protocol relative", url="//evil.example/x", refresh=False)
    await refresh_denormalized(session)
    await session.commit()

    for path in ("/roles", "/", f"/company/{company.id}"):
        body = (await client.get(path)).text
        # Every href on the page names an allowed scheme — the rejected URLs are not among them.
        for href in _HREF.findall(body):
            assert href.startswith(("http://", "https://", "/", "#", "mailto:")), (path, href)
        assert "<script>alert(1)</script>" not in body, path
        assert "Hostile &lt;b&gt;Corp&lt;/b&gt;" in body, path

    roles = (await client.get("/roles")).text
    # The role still renders — an unusable URL degrades to plain text, it never hides a role.
    assert "&lt;script&gt;alert(1)&lt;/script&gt; Engineer" in roles
    assert "Protocol relative" in roles
    # ...and the refused value is visible as escaped text, not as a link.
    assert "javascript:alert(1)" not in roles
    assert 'href="//evil.example/x"' not in roles


async def test_no_page_has_a_duplicate_dom_id_or_a_dangling_hx_target(
    client: httpx.AsyncClient, corpus: Corpus
) -> None:
    """Two elements sharing an id make ``hx-target="#results"`` swap into an arbitrary one."""
    for path in ("/", "/roles", "/runs", f"/company/{corpus.northstar}", "/company/999999"):
        body = (await client.get(path)).text
        found = _ID_ATTR.findall(body)
        assert len(found) == len(set(found)), (path, sorted(found))
        for target in _HX_TARGET.findall(body):
            assert target in set(found), (path, target)


# ------------------------------------------------------------------- /runs staleness (SPEC §9)


def test_connector_health_marks_stale_only_past_twice_the_cadence() -> None:
    """SPEC §9: "no successful run in over twice its cadence"; ``now`` is injected to pin it."""
    now = datetime(2026, 9, 4, 12, tzinfo=UTC)
    health = {
        row.connector: row
        for row in connector_health(
            {
                "greenhouse": now - timedelta(hours=6),  # daily cadence: fresh
                "lever": now - timedelta(days=1, hours=23),  # just inside 2 x 1 day
                "ashby": now - timedelta(days=2, hours=1),  # just outside
                "sec_edgar": now - timedelta(days=5),  # 3-day cadence: inside 6 days
            },
            # SPEC §7.2's failure streaks are the other, independent signal (Phase 6); this
            # test is about §9's cadence one alone, so nothing is failing here.
            {},
            now=now,
        )
    }

    assert health["greenhouse"].stale is False
    assert health["lever"].stale is False
    assert health["ashby"].stale is True
    assert health["sec_edgar"].stale is False
    # Never run at all is the case the page exists for.
    assert health["workable"].last_ok is None
    assert health["workable"].stale is True
    # An on-demand connector has no schedule to have missed.
    assert health["opencorporates"].cadence is None
    assert health["opencorporates"].period is None
    assert health["opencorporates"].stale is False
    assert health["product_hunt"].implemented is False


async def test_runs_page_highlights_a_stale_connector(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    now = datetime.now(UTC)
    await make_run(session, "greenhouse", started_at=now - timedelta(hours=2))
    await make_run(session, "lever", started_at=now - timedelta(days=30))
    await make_run(
        session, "ashby", started_at=now - timedelta(minutes=5), status=enums.FetchRunStatus.PARTIAL
    )
    await session.commit()

    body = (await client.get("/runs")).text
    classes = {connector: css for css, connector in _RUN_ROW.findall(body)}

    assert classes["greenhouse"] == ""
    assert classes["lever"] == " stale"
    # `partial` is not `ok`: SPEC §9 asks about a *successful* run.
    assert classes["ashby"] == " stale"
    assert classes["workable"] == " stale"  # never ran
    assert classes["opencorporates"] == ""  # cadence: null
    assert body.count("<strong>Stale</strong>") == sum(
        1 for css in classes.values() if "stale" in css
    )


@pytest.mark.parametrize(
    "bad",
    [
        "0 99 * * *",  # hour out of range
        "60 0 * * *",  # minute out of range
        "0 5 */3 * z",  # unknown weekday name
        "* * * * 8",  # weekday out of range
    ],
)
def test_an_unparseable_cadence_is_refused_at_config_load(bad: str) -> None:
    """This is what keeps ``/runs`` from being the thing that discovers a cadence typo.

    ``connector_health`` parses every cadence in ``config/connectors.yaml`` on every render, and
    a ``ValueError`` from ``CronTrigger`` matches none of the app's exception handlers — so
    before ``ConnectorConfig`` parsed the expression rather than merely counting its fields, a
    one-character edit to that file turned the one page whose job is to surface breakage into an
    unhandled 500. Rejecting it on load makes the failure a message naming
    ``connectors.<name>.cadence`` instead (SPEC §7.2, §9; CLAUDE.md "edit YAML, not Python").
    """
    with pytest.raises(ValidationError, match="is not a valid cron expression"):
        ConnectorConfig(name="typo", cadence=bad)


def test_a_wrong_length_cadence_keeps_its_own_message() -> None:
    """The field-count check runs first, so ``"0 5 * *"`` still says what is wrong with it."""
    with pytest.raises(ValidationError, match="must be a five-field cron expression"):
        ConnectorConfig(name="typo", cadence="0 5 * *")


def test_every_shipped_cadence_parses() -> None:
    """The other half: the committed config must survive the validator it just gained."""
    for name, connector in load_connectors_config().connectors.items():
        assert (cadence_period(connector.cadence) is None) == (connector.cadence is None), name
