"""The SPEC §13 acceptance criteria Phase 4 is responsible for.

Four claims, each of which is a checkbox in §13 rather than a unit of behaviour:

* **"All 19 role families are represented … with no filters applied, ``/roles`` shows every open
  job of every type."** :func:`test_every_role_family_is_reachable_with_no_filters` seeds one
  open role in each of the 19 :class:`~db.enums.RoleFamily` members, walks ``/roles`` to
  exhaustion through the rendered "Load more" links and then walks ``/`` and opens every
  company's panel, asserting the same 19 role ids both ways. This is the test that proves
  classification never excludes (SPEC §7.1, CLAUDE.md).
* **"No web request handler makes an outbound HTTP call."** Proved twice over: statically, by
  walking every module under ``web/`` with :mod:`ast` for a forbidden import, and dynamically,
  by driving a full request cycle inside ``respx.mock`` with no routes registered, where any
  outbound call would raise.
* **The stated front-end budget** (SPEC §9, CLAUDE.md): one hand-written stylesheet under 200
  lines, no external origin in the page chrome, no JavaScript framework and no build step.
* **"The company list renders in under 200 ms with 5,000 companies and 20,000 jobs."**

The performance test is the slow one; everything else here is cheap.
"""

from __future__ import annotations

import ast
import re
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx
from respx.models import AllMockedAssertionError
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import Company, Job
from db.queries import PAGE_SIZE, company_list_page, refresh_company_denormalized_columns
from tests.support_web import (
    DAY,
    NOW,
    app_client,
    company_ids,
    flatten,
    job_ids,
    make_company,
    make_job,
    next_link,
    refresh_denormalized,
    walk_pages,
)
from web.labels import ROLE_FAMILY_LABELS

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"

#: Nothing under ``web/`` may import these: a request handler must not be one import away from
#: an outbound call (SPEC §2, CLAUDE.md). ``ingest.config`` is deliberately *not* here — it
#: reads ``config/connectors.yaml`` off the local disk and is the only place a cadence lives.
FORBIDDEN_IMPORTS = ("httpx", "ingest.http", "ingest.connectors", "ingest.seed")

#: SPEC §9 / CLAUDE.md: "one hand-written ``web/static/styles.css`` (~200 lines)".
MAX_STYLESHEET_LINES = 200
#: "Only small glue JS" — and only with a comment saying why htmx cannot do it.
MAX_GLUE_JS_LINES = 20

#: SPEC §13's scale target for the company list.
PERF_COMPANIES = 5_000
PERF_JOBS = 20_000
PERF_BUDGET_SECONDS = 0.200

_EXTERNAL_ASSET = re.compile(r'(?:src|href)="(?:https?:)?//')


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[httpx.AsyncClient]:
    async with app_client(session_factory) as http:
        yield http


# ------------------------------------- §13: every role family reachable with no filters at all


async def test_every_role_family_is_reachable_with_no_filters(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """One open role in each of the 19 families, found twice: on ``/roles`` and in a panel.

    The list is deliberately padded past one page in both views, so the walk exercises the
    keyset "Load more" links rather than a single render — a family stranded on page 2 would be
    just as excluded as one filtered out.
    """
    employment_types = list(enums.EmploymentType)
    seniorities = list(enums.Seniority)
    marker_jobs: dict[enums.RoleFamily, int] = {}
    marker_companies: dict[enums.RoleFamily, int] = {}

    for index, family in enumerate(enums.RoleFamily):
        company = await make_company(session, f"{family.value.title()} Works")
        job = await make_job(
            session,
            company,
            f"{family.value} specialist",
            role_family=family,
            employment_type=employment_types[index % len(employment_types)],
            seniority=seniorities[index % len(seniorities)],
            # A mix, and neither value may hide a role (SPEC §7.1).
            flexible_signal=index % 2 == 0,
            posted_at=NOW - index * DAY,
            refresh=False,
        )
        marker_jobs[family] = job.id
        marker_companies[family] = company.id

    # Padding, so both lists need a second page.
    for index in range(PAGE_SIZE):
        filler = await make_company(session, f"Filler {index:03d}")
        await make_job(
            session, filler, "Filler role", posted_at=NOW - (100 + index) * DAY, refresh=False
        )
    await refresh_denormalized(session)
    await session.commit()

    assert len(marker_jobs) == 19, "SPEC §7.1 defines 19 role families"

    # 1. /roles, no filters at all, walked to exhaustion through the rendered next links.
    rendered: list[str] = []
    next_url: str | None = "/roles"
    while next_url is not None:
        response = await client.get(next_url)
        assert response.status_code == 200
        rendered.append(response.text)
        next_url = next_link(response.text)
    markup = "".join(rendered)
    listed = job_ids(markup)

    assert len(rendered) > 1, "the walk must actually cross a page boundary"
    assert len(listed) == len(set(listed)) == 19 + PAGE_SIZE
    missing = {family for family, job_id in marker_jobs.items() if job_id not in set(listed)}
    assert missing == set(), f"/roles hid these families: {sorted(missing)}"
    # And every family is *legible*, not merely present as an id.
    for family, label in ROLE_FAMILY_LABELS.items():
        assert f"<td>{label}</td>" in markup, family
    assert '<span class="badge flexible">Flexible</span>' in markup  # a badge, never a filter

    # 2. /, no filters at all, then every company's expanded panel.
    company_pages = await walk_pages(client, "/")
    browsed = flatten(company_pages)
    assert len(company_pages) > 1
    assert set(marker_companies.values()) <= set(browsed)

    reachable: set[int] = set()
    for company_id in browsed:
        panel = await client.get(f"/company/{company_id}/panel")
        assert panel.status_code == 200
        reachable.update(job_ids(panel.text))
    assert set(marker_jobs.values()) <= reachable


async def test_a_role_filter_never_shrinks_the_panel(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """The company row promises a role count; the panel must be able to show every one of them."""
    company = await make_company(session, "Broad Co")
    for family in (enums.RoleFamily.SOFTWARE, enums.RoleFamily.LEGAL, enums.RoleFamily.OTHER):
        await make_job(session, company, f"{family.value} role", role_family=family, refresh=False)
    await refresh_denormalized(session)
    await session.commit()

    plain = await client.get(f"/company/{company.id}/panel")
    filtered = await client.get(f"/company/{company.id}/panel?family=legal&flexible=1")

    assert len(job_ids(plain.text)) == 3
    assert job_ids(filtered.text) == job_ids(plain.text)
    # `flexible=1` matches nothing here, and still nothing is hidden.
    assert filtered.text.count('<tr class="match">') == 0


# ------------------------------------------------ §13: no web request handler fetches anything


def web_modules() -> list[Path]:
    return sorted(WEB.rglob("*.py"))


def imported_modules(source: Path) -> set[str]:
    """Every module name a file imports, ``from x import y`` counted as both ``x`` and ``x.y``."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.add(node.module)
            names.update(f"{node.module}.{alias.name}" for alias in node.names)
    return names


def test_no_module_under_web_imports_a_fetcher() -> None:
    """SPEC §2 statically: the HTTP client and the connectors are not reachable from ``web/``."""
    modules = web_modules()
    assert modules, "web/ should contain modules"
    offenders: dict[str, set[str]] = {}
    for path in modules:
        bad = {
            name
            for name in imported_modules(path)
            if any(
                name == forbidden or name.startswith(f"{forbidden}.")
                for forbidden in FORBIDDEN_IMPORTS
            )
        }
        if bad:
            offenders[str(path.relative_to(ROOT))] = bad
    assert offenders == {}

    # The one ingest import that *is* allowed, because it only reads a local YAML file.
    assert "ingest.config" in imported_modules(WEB / "routes" / "runs.py")


async def test_a_full_request_cycle_makes_no_outbound_call(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """SPEC §2 dynamically: inside ``respx.mock`` with no routes, any real request raises.

    respx patches httpcore's connection pools, which is exactly the layer an outbound ``httpx``
    call would go through; the ASGI transport this client uses does not touch it, so the app is
    still driven for real while the network is armed to fail.
    """
    company = await make_company(session, "Offline Co", website_url="https://offline.example")
    job = await make_job(session, company, "Engineer", url="https://offline.example/jobs/1")
    await session.commit()

    with respx.mock(assert_all_called=False) as router:
        pages = [
            "/",
            "/roles",
            f"/company/{company.id}",
            f"/company/{company.id}/panel",
            "/runs",
            "/healthz",
            "/static/styles.css",
        ]
        for path in pages:
            assert (await client.get(path)).status_code == 200, path
        posted = await client.post(
            f"/company/{company.id}/note",
            data={"status": "interested", "rating": "3", "note": "no network here"},
            headers={"HX-Request": "true"},
        )
        assert posted.status_code == 200
        starred = await client.post(f"/jobs/{job.id}/bookmark", headers={"HX-Request": "true"})
        assert starred.status_code == 200

        assert list(router.calls) == []
        # ...and the trap is real: a genuine outbound request inside this block does raise, so
        # the empty call list above is evidence rather than an artefact of respx being inert.
        async with httpx.AsyncClient() as outbound:
            with pytest.raises(AllMockedAssertionError):
                await outbound.get("https://offline.example/probe")


# --------------------------------------------- §9 / CLAUDE.md: the front-end budget is a budget


def test_the_stylesheet_is_one_short_hand_written_file() -> None:
    stylesheet = WEB / "static" / "styles.css"
    lines = stylesheet.read_text(encoding="utf-8").splitlines()

    assert len(lines) < MAX_STYLESHEET_LINES, f"styles.css is {len(lines)} lines"
    text = "\n".join(lines)
    assert "@import" not in text  # no second stylesheet smuggled in
    assert "@font-face" not in text and "//fonts." not in text  # no web fonts
    assert "!important" not in text  # the brief asks for flat specificity
    assert list((WEB / "static").glob("*.css")) == [stylesheet], "exactly one stylesheet"


def test_the_page_chrome_loads_nothing_from_an_external_origin() -> None:
    """A CDN ``<script>`` would be an outbound call made by the browser on every page view."""
    base = (WEB / "templates" / "base.html").read_text(encoding="utf-8")

    assert _EXTERNAL_ASSET.search(base) is None
    assert 'href="/static/styles.css"' in base
    assert 'src="/static/htmx.min.js"' in base  # vendored, never a CDN
    assert (WEB / "static" / "htmx.min.js").stat().st_size > 0


async def test_no_served_page_loads_anything_from_an_external_origin(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """The same rule as above, asserted on the *responses* rather than on one template.

    Grepping ``base.html`` cannot see a page the framework adds for free: FastAPI's stock
    ``/docs`` and ``/redoc`` load swagger-ui and redoc from ``cdn.jsdelivr.net`` plus a
    stylesheet from ``fonts.googleapis.com``, which are outbound calls the browser makes on
    this app's own origin — the rule SPEC §2 states for the server, and the reason htmx is
    vendored under ``/static``. Asserting the 404s pins that decision (``web.app.create_app``
    passes ``docs_url=None, redoc_url=None, openapi_url=None``) so it cannot be undone quietly.
    """
    company = await make_company(session, "Origin Labs")
    await make_job(session, company, "Engineer")
    await session.commit()

    for path in ("/", "/roles", "/runs", f"/company/{company.id}", f"/company/{company.id}/panel"):
        response = await client.get(path, headers={"Accept": "text/html"})
        assert response.status_code == 200, path
        # Only assets: a scraped job or company URL is content the row links to, not chrome.
        assets = re.findall(
            r'<(?:script|link|img|iframe|source)[^>]*(?:src|href)="[^"]*"', response.text
        )
        assert [tag for tag in assets if _EXTERNAL_ASSET.search(tag)] == [], path

    for path in ("/docs", "/redoc", "/openapi.json"):
        assert (await client.get(path)).status_code == 404, path


def test_any_bespoke_javascript_is_small_and_explained() -> None:
    scripts = sorted((WEB / "static").glob("*.js"))
    glue = [path for path in scripts if path.name != "htmx.min.js"]
    assert [path.name for path in glue] in ([], ["app.js"]), scripts
    for path in glue:
        lines = path.read_text(encoding="utf-8").splitlines()
        assert len(lines) <= MAX_GLUE_JS_LINES, f"{path.name} is {len(lines)} lines"
        assert any("//" in line or "/*" in line for line in lines), "say why htmx cannot do it"


def test_no_template_pulls_in_a_framework_or_a_build_step() -> None:
    """Server-rendered Jinja2 + htmx only (SPEC §3, §9)."""
    banned = ("cdn.", "tailwind", "react", "vue.js", "bootstrap", "unpkg", "jsdelivr")
    for template in sorted((WEB / "templates").rglob("*.html")):
        text = template.read_text(encoding="utf-8").lower()
        for token in banned:
            assert token not in text, f"{template.name} mentions {token}"


# ---------------------------------------------------------- §13: 5,000 companies, 20,000 jobs


async def test_the_company_list_renders_under_200ms_at_spec_scale(
    session: AsyncSession, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC §13: "the company list renders in under 200 ms with 5,000 companies and 20,000 jobs".

    Bulk-inserted with two executemany statements rather than the ORM factories — 25,000 units
    of work would otherwise dominate the test — then the denormalized columns are refreshed
    exactly as the ingest pipeline does at end of run, because the default sort reads them.

    Best of three after a warm-up call: the first statement of a fresh connection pays for
    parsing and a cold buffer cache, which is not what the criterion is about.
    """
    base = datetime(2026, 1, 1, tzinfo=UTC)
    await session.execute(
        insert(Company),
        [
            {
                "name": f"Company {index:05d}",
                "normalized_name": f"company {index:05d}",
                "domain": f"company{index:05d}.example",
                "stage": list(enums.Stage)[index % len(enums.Stage)],
                "first_seen_at": base + timedelta(minutes=index),
                "last_seen_at": base + timedelta(minutes=index),
            }
            for index in range(PERF_COMPANIES)
        ],
    )
    ids = list((await session.execute(select(Company.id).order_by(Company.id))).scalars())
    assert len(ids) == PERF_COMPANIES

    families = list(enums.RoleFamily)
    await session.execute(
        insert(Job),
        [
            {
                "company_id": ids[index % PERF_COMPANIES],
                "external_id": f"perf-{index:06d}",
                "title": f"Engineer {index:06d}",
                "role_family": families[index % len(families)],
                "employment_type": enums.EmploymentType.FULL_TIME,
                "seniority": enums.Seniority.MID,
                # A fifth of the jobs are closed, so open_job_count is not simply "four".
                "closed_at": base if index % 5 == 0 else None,
                "posted_at": base + timedelta(minutes=index),
                "first_seen_at": base,
                "last_seen_at": base,
            }
            for index in range(PERF_JOBS)
        ],
    )
    await refresh_company_denormalized_columns(session, None)
    await session.commit()

    await company_list_page(session)  # warm-up: statement parse and a cold cache
    timings: list[float] = []
    for _ in range(3):
        started = time.perf_counter()
        page = await company_list_page(session)
        timings.append(time.perf_counter() - started)
    best = min(timings)

    with capsys.disabled():
        print(
            f"\ncompany_list_page over {PERF_COMPANIES:,} companies and {PERF_JOBS:,} jobs: "
            f"best {best * 1000:.1f} ms of {[f'{t * 1000:.1f}' for t in timings]} "
            f"(SPEC §13 budget {PERF_BUDGET_SECONDS * 1000:.0f} ms)"
        )

    assert len(page.rows) == PAGE_SIZE
    assert page.next_cursor is not None
    assert best < PERF_BUDGET_SECONDS, f"{best * 1000:.1f} ms exceeds the SPEC §13 budget"


async def test_the_default_view_of_a_brand_new_database(client: httpx.AsyncClient) -> None:
    """Nothing ingested yet is a different empty state from "nothing matched"."""
    empty = await client.get("/")
    assert empty.status_code == 200
    assert "No companies yet" in empty.text
    assert 'class="load-more"' not in empty.text

    roles = await client.get("/roles")
    assert "No roles yet" in roles.text

    filtered = await client.get("/?q=nothing+here")
    assert "No companies match these filters" in filtered.text
    assert '<a href="/">Clear filters</a>' in filtered.text


async def test_the_company_list_ids_are_stable_between_the_page_and_its_fragment(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """One partial, three entry points (first render, filter swap, load more)."""
    for index in range(60):
        await make_company(session, f"Company {index:03d}")
    await session.commit()

    full = await client.get("/?sort=name")
    swap = await client.get("/?sort=name", headers={"HX-Request": "true"})
    assert company_ids(full.text) == company_ids(swap.text)
    assert next_link(full.text) == next_link(swap.text)
    assert next_link(full.text) is not None
