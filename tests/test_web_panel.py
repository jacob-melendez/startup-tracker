"""The expanded row's own controls, its contacts block, and one cursor key that reached asyncpg.

SPEC §9 describes the panel's roles table as "title, role family, employment type, seniority,
location, posted date, link — **sortable and filterable within the row**, showing *all* roles
regardless of the page-level role filter, with matching ones highlighted". Those two halves
pull in opposite directions, and the first group of tests here is about the line between them:

* the page's role filter may only *mark* a row (§7.1, "classification never excludes"), so the
  panel the collapsed row lazy-loads must list every open role — the ``role_*`` parameters are
  absent from the URL it builds, and that is asserted here, not assumed;
* the row's own header links and filter box may genuinely reorder and shorten that table,
  because the user operated them, and they must survive a round trip through the URL so that
  the permalink shows the same table with JavaScript switched off.

The second group is §9's other block of the same panel — "Contacts block — published emails,
careers page, LinkedIn company link, 'Find people →', with published/constructed clearly
labeled" — which is where SPEC §6's last clause lands: "the UI must visually distinguish
``published`` from ``constructed`` so it's obvious which is a real address and which is a
search shortcut". A badge alone would not do that (it is one word in one colour, on a page
that is otherwise all links), so what is asserted is the structure a reader actually sees: two
groups under two headings, in document order, each row badged, one line of explanation above
both, and the people search rendered as the control §6 names rather than as a seventh URL.
Every one of those runs against **both** entry points, because ``/company/{id}`` is the page a
reader keeps and ``/company/{id}/panel`` is the fragment the list opens, and §9 calls the first
"a standalone, linkable version of" the second.

The third group is what SPEC §12 Phase 8 put *above* both of those, and it starts by asserting
the panel's section order the other way round from the way this file used to. Two open roles in
5,032 here are part-time, because a startup does not advertise part-time work — it invents it
when a specific person asks — so the row leads with a human to write to and keeps the roles
table below as evidence of budget rather than as things to apply to. Two doors follow from that
and both are asserted here: the published address, now a draft you can send rather than a sixth
link in a list, and — for the 9,822 companies of 10,219 that publish no address at all — the best
LinkedIn door the stored rows support, which makes it the main path and not a fallback. Nothing
in either is fetched: the profile URL was published on the company's own site, the search URLs
are constructed from its name (§6), and the note is text the reader pastes (§2, §4).

Every corpus count in this file is measured against the live database on 2026-09-09 and is prose,
never an assertion: a test may not read that database (the fixtures drop the public schema), so a
figure here is a dated note for the reader and moves whenever a connector runs. ``cli.py stats``
is the live number.

The cursor tests at the end belong to the same file only in that they are the other thing a
hand-edited URL can do: a ``k`` carrying a NUL or a lone surrogate used to reach the driver and
answer 500 where every other malformed token answers with the 400 page.
"""

from __future__ import annotations

import html
import re
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from selectolax.parser import HTMLParser, Node
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import Person
from db.queries import Cursor, CursorError
from ingest.config import LINKEDIN_NOTE_LIMIT, load_outreach_config
from tests.support_web import (
    DAY,
    NOW,
    app_client,
    job_ids,
    make_company,
    make_contact,
    make_job,
    refresh_denormalized,
)
from web.labels import label_for
from web.templating import MAILTO_MAX_URL

#: How far under :data:`web.templating.MAILTO_MAX_URL` the shipped draft must land when it is
#: rendered for this file's fixture company.
#:
#: A margin rather than the ceiling itself, because the ceiling cannot fail: ``mailto_url`` cuts
#: the body to fit, so ``len(href) <= MAILTO_MAX_URL`` is true of a mangled draft too. What can
#: fail is the draft growing until a *real* company name pushes it over — and the fixture's name
#: is short, so a URL that only just fits here is already cut for somebody.
#:
#: 400 is where that lands. The tightest real ceiling for the shipped body's punctuation is a
#: 1,067-character raw body (the subject at its own cap; ``tests/test_outreach_config.py``
#: measures this and budgets 1,000 against it). Growing the body from today's 722 to ~964 raw is
#: what pushes this fixture's URL past ``MAILTO_MAX_URL - 400`` — so this fires just before that
#: budget does, and both fire before anything is actually truncated.
MAILTO_HEADROOM = 400

#: Every ``<th aria-sort=...>`` of a rendered roles table, in column order.
_ARIA_SORT = re.compile(r'<th scope="col" aria-sort="([a-z]+)"')

STYLESHEET = Path(__file__).resolve().parent.parent / "web" / "static" / "styles.css"


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[httpx.AsyncClient]:
    async with app_client(session_factory) as http:
        yield http


@dataclass(frozen=True, slots=True)
class Panel:
    """One company whose four roles order differently under every column of the table."""

    company: int
    alpha: int
    zeta: int
    growth: int
    closed: int


@pytest.fixture
async def panel(session: AsyncSession) -> Panel:
    """Three open roles plus a closed one, chosen so that no two columns agree on an order.

    The values matter: the seniorities and families are picked so that the vocabulary's own
    order (``intern`` before ``staff`` before ``director``) and an alphabetical one disagree,
    which is the only way to tell which of the two the SQL actually used. ``Growth (100%
    remote)`` carries a literal ``%`` so the title filter can be shown to escape it.
    """
    company = await make_company(session, "Panel Labs")
    alpha = await make_job(
        session,
        company,
        "Alpha engineer",
        role_family=enums.RoleFamily.SOFTWARE,
        employment_type=enums.EmploymentType.FULL_TIME,
        seniority=enums.Seniority.STAFF,
        location_text="Oakland, CA",
        posted_at=NOW - DAY,
        refresh=False,
    )
    zeta = await make_job(
        session,
        company,
        "zeta designer",
        role_family=enums.RoleFamily.DESIGN,
        employment_type=enums.EmploymentType.CONTRACT,
        seniority=enums.Seniority.DIRECTOR,
        location_text=None,
        posted_at=NOW - 2 * DAY,
        refresh=False,
    )
    growth = await make_job(
        session,
        company,
        "Growth (100% remote)",
        role_family=enums.RoleFamily.OPERATIONS,
        employment_type=enums.EmploymentType.INTERNSHIP,
        seniority=enums.Seniority.INTERN,
        location_text="Berkeley, CA",
        posted_at=NOW,
        refresh=False,
    )
    closed = await make_job(
        session,
        company,
        "Closed archivist",
        role_family=enums.RoleFamily.OPERATIONS,
        posted_at=NOW - 3 * DAY,
        closed_at=NOW - DAY,
        refresh=False,
    )
    await refresh_denormalized(session)
    await session.commit()
    return Panel(
        company=company.id,
        alpha=alpha.id,
        zeta=zeta.id,
        growth=growth.id,
        closed=closed.id,
    )


async def roles(client: httpx.AsyncClient, panel: Panel, query: str = "") -> list[int]:
    """The job ids the panel lists for one in-row query string, in rendered order."""
    response = await client.get(f"/company/{panel.company}/panel{query}")
    assert response.status_code == 200, (query, response.status_code)
    return job_ids(response.text)


# ------------------------------------------- the invariant: the controls start off (§9, §7.1)


async def test_the_panel_the_list_opens_is_still_every_role(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """The default panel is unchanged by this phase: every open role, newest first.

    Asserted from both ends — the fragment itself, and the URL the collapsed row builds for it
    — because the second is what actually guarantees the first in a browser. A ``role_``
    parameter appearing in that ``hx-get`` would make the page-level filter able to shorten a
    table §9 says it may only highlight.
    """
    default = await roles(client, panel)
    assert default == [panel.growth, panel.alpha, panel.zeta]
    assert panel.closed not in default
    assert await roles(client, panel, "?family=software") == default

    row = (await client.get("/?family=software&role_q=engineer&role_closed=1")).text
    assert re.findall(r'hx-get="/company/\d+/panel[^"]*"', row) == [
        f'hx-get="/company/{panel.company}/panel?family=software"'
    ]


async def test_the_row_controls_do_not_reach_the_page_level_lists(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """The other direction: ``role_*`` is one row's business and no other view's.

    ``/`` and ``/roles`` ignore unknown parameters by design (a stale bookmark must not 400),
    so the risk is not an error but a silent change of meaning — ``role_closed=1`` quietly
    listing closed roles on ``/roles``, where ``closed=1`` is the opt-in.
    """
    tail = "role_q=engineer&role_closed=1&role_sort=title&role_dir=desc"
    listed = re.findall(r"/company/(\d+)/panel", (await client.get(f"/?{tail}")).text)
    assert listed == re.findall(r"/company/(\d+)/panel", (await client.get("/")).text)

    with_tail = job_ids((await client.get(f"/roles?{tail}")).text)
    assert with_tail == job_ids((await client.get("/roles")).text)
    assert panel.closed not in with_tail


# ------------------------------------------------------------- sortable within the row (§9)


async def test_every_column_sorts_by_its_own_vocabulary(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """One assertion per sortable column, each one distinguishable from every other order.

    The three enum columns order by the Postgres type's declaration order, not alphabetically:
    ``intern`` before ``staff`` before ``director`` is a seniority ladder, and it is the same
    order the filter select offers. Sorting them alphabetically would put "Director" first,
    which is why the fixture picked those three.
    """
    assert await roles(client, panel, "?role_sort=title") == [
        panel.alpha,
        panel.growth,
        panel.zeta,
    ]
    assert await roles(client, panel, "?role_sort=family") == [
        panel.alpha,  # software
        panel.zeta,  # design
        panel.growth,  # operations
    ]
    assert await roles(client, panel, "?role_sort=type") == [
        panel.alpha,  # full_time
        panel.zeta,  # contract
        panel.growth,  # internship
    ]
    assert await roles(client, panel, "?role_sort=seniority") == [
        panel.growth,  # intern
        panel.alpha,  # staff
        panel.zeta,  # director
    ]
    # NULLS LAST holds here too: the role with no location text sorts to the end.
    assert await roles(client, panel, "?role_sort=location") == [
        panel.growth,  # Berkeley
        panel.alpha,  # Oakland
        panel.zeta,  # NULL
    ]
    assert await roles(client, panel, "?role_sort=posted") == await roles(client, panel)


async def test_a_direction_reverses_one_column_and_defaults_per_column(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """``role_dir`` flips a column; absent, each column reads the way that column should.

    Title ascending is A→Z and posted descending is newest-first, so the default direction is
    a property of the column rather than a single global one — otherwise a first click on
    "Title" would inherit "Posted"'s descending order and read Z→A.
    """
    ascending = await roles(client, panel, "?role_sort=title")
    assert await roles(client, panel, "?role_sort=title&role_dir=asc") == ascending
    assert await roles(client, panel, "?role_sort=title&role_dir=desc") == ascending[::-1]
    # Posted is the one column whose natural direction is descending.
    assert await roles(client, panel, "?role_dir=asc") == (await roles(client, panel))[::-1]


async def test_the_headers_are_links_that_work_with_and_without_htmx(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """Each header is an ``<a href>`` first and an htmx swap second (brief §5.2's pattern).

    The href is not decoration: with JavaScript off it is the whole mechanism, and it points at
    the permalink, which answers the same parameters. Both URLs must carry the page's highlight
    and the row's filter, or sorting would silently clear them.
    """
    base = f"/company/{panel.company}"
    body = (await client.get(f"{base}/panel?family=software&role_sort=title&role_q=e")).text
    # The sorted column offers its own reverse...
    reversed_link = '?family=software&amp;role_sort=title&amp;role_dir=desc&amp;role_q=e"'
    assert f'href="{base}{reversed_link}' in body
    # ...every other column offers its own natural direction, with no role_dir at all.
    assert f'href="{base}?family=software&amp;role_sort=seniority&amp;role_q=e"' in body
    assert f'hx-get="{base}/panel?family=software&amp;role_sort=seniority&amp;role_q=e"' in body
    assert 'hx-target="closest .panel"' in body
    # aria-sort states the current column, once, and the other five say "none".
    assert _ARIA_SORT.findall(body) == ["ascending", "none", "none", "none", "none", "none"]


async def test_the_permalink_sorts_with_no_javascript_at_all(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """``/company/{id}`` answers the same ``role_*`` parameters, with plain anchors.

    The standalone page has no other panel to swap into, and the fragment would drop the page
    heading around it, so there the links carry no htmx attributes — following one is an
    ordinary navigation that lands on the same table in the same order.
    """
    response = await client.get(f"/company/{panel.company}?role_sort=title&role_dir=desc")

    assert response.status_code == 200
    body = response.text
    assert job_ids(body) == [panel.zeta, panel.growth, panel.alpha]
    assert "<h1>Panel Labs</h1>" in body
    assert "/panel" not in body
    assert f'href="/company/{panel.company}?role_sort=title"' in body
    assert _ARIA_SORT.findall(body) == ["descending", "none", "none", "none", "none", "none"]


# ----------------------------------------------------------- filterable within the row (§9)


async def test_the_title_filter_narrows_this_row_only(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """``role_q`` is a case-insensitive substring of the *title* — the row's own search box."""
    assert await roles(client, panel, "?role_q=engineer") == [panel.alpha]
    assert await roles(client, panel, "?role_q=ENGINEER") == [panel.alpha]
    assert await roles(client, panel, "?role_q=e") == [panel.growth, panel.alpha, panel.zeta]
    assert await roles(client, panel, "?role_q=nothing+here") == []
    # An emptied field means unset, exactly as it does for the page-level parameters.
    assert await roles(client, panel, "?role_q=") == await roles(client, panel)


async def test_the_title_filter_escapes_like_wildcards_and_drops_a_nul(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """A ``%`` the user typed matches a literal ``%``, and U+0000 is stripped, not bound.

    Without ``autoescape`` the first would match every role; without the NUL strip the second
    would raise ``CharacterNotInRepertoireError`` from inside the statement — a 500 from a URL
    anyone can type, the same failure ``web.filters._text`` already prevents for ``q``.
    """
    assert await roles(client, panel, "?role_q=100%25") == [panel.growth]
    assert await roles(client, panel, "?role_q=%25") == [panel.growth]
    assert await roles(client, panel, "?role_q=%00engineer") == [panel.alpha]


async def test_the_closed_toggle_is_opt_in_and_reversible(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """SPEC §2 keeps a closed role as history; §9's table is of openings until asked.

    The panel says out loud when it is narrowed and offers the way back, because while it is
    the count in its heading is no longer the open-role count the collapsed row promised.
    """
    assert await roles(client, panel, "?role_closed=1") == [
        panel.growth,
        panel.alpha,
        panel.zeta,
        panel.closed,  # open roles first, whatever the sort
    ]
    assert await roles(client, panel, "?role_closed=1&role_sort=title") == [
        panel.alpha,
        panel.growth,
        panel.zeta,
        panel.closed,
    ]
    narrowed = (await client.get(f"/company/{panel.company}/panel?role_q=engineer")).text
    assert "Narrowed to titles containing &ldquo;engineer&rdquo;." in narrowed
    assert f'<a href="/company/{panel.company}" hx-get="/company/{panel.company}/panel"' in narrowed
    assert ">Show every open role</a>" in narrowed

    widened = (await client.get(f"/company/{panel.company}/panel?role_closed=1")).text
    assert "Closed roles are listed too." in widened
    assert "Narrowed to titles" not in widened


async def test_the_filter_form_carries_the_rest_of_the_row_state(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """Narrowing the table must not clear the sort or the page's highlight, and vice versa.

    The form is a real ``method="get"`` form to the permalink, so with JavaScript off it round
    trips through hidden fields; the ids are suffixed with the company id because fifty
    expanded rows share one document.
    """
    body = (
        await client.get(
            f"/company/{panel.company}/panel?family=software&role_sort=title&role_dir=desc"
        )
    ).text

    assert f'action="/company/{panel.company}"' in body
    assert '<input type="hidden" name="family" value="software">' in body
    assert '<input type="hidden" name="role_sort" value="title">' in body
    assert '<input type="hidden" name="role_dir" value="desc">' in body
    assert f'id="role-q-{panel.company}"' in body
    assert f'id="role-closed-{panel.company}"' in body


@pytest.mark.parametrize(
    "query",
    ["?role_sort=company", "?role_sort=bogus", "?role_dir=sideways", "?role_dir=1"],
)
async def test_an_unknown_in_row_parameter_is_a_400_page(
    client: httpx.AsyncClient, panel: Panel, query: str
) -> None:
    """A wrong value is an error naming what it accepts; an *empty* one is merely unset.

    ``role_sort=company`` is in the list because ``/roles`` does accept it: the two sort
    vocabularies are deliberately separate, and the panel must not silently ignore one it
    cannot honour.
    """
    for path in (f"/company/{panel.company}/panel", f"/company/{panel.company}"):
        response = await client.get(f"{path}{query}")
        assert response.status_code == 400, (path, query)
        assert "expected one of" in response.text


# ---------------------------------- the contacts block: published vs constructed (§6, §9)

#: The published half of the fixture company. Written out because the assertions are about
#: *these* strings surviving the round trip into an attribute, not about a shape.
RICH_NAME = "Atom Computing"
#: Two addresses at one company, chosen so the SPEC §12 Phase 8 hint has both cases to render:
#: ``careers`` is one of ``config/outreach.yaml``'s ``role_address_prefixes`` and ``hr`` is not,
#: which is the difference between a ticket queue and somebody's own inbox.
CAREERS_EMAIL = "careers@atom-computing.example"
HR_EMAIL = "hr@atom-computing.example"
DIRECT_NAME = "Direct Labs"
FOUNDER_EMAIL = "ada.okonjo@direct-labs.example"
DIRECT_PROFILE_URL = "https://www.linkedin.com/in/ada-okonjo"
CAREERS_PAGE = "https://atom-computing.example/careers"
#: A numeric-id LinkedIn page is real (the recorded Sourcegraph home page has one) and it is
#: the interesting case: a published value that can never equal the constructed
#: ``/company/{slug}`` one, so the two halves genuinely have to be told apart.
PUBLISHED_LINKEDIN = "https://www.linkedin.com/company/4803356"
X_PROFILE = "https://x.com/atom_computing"
#: Every contact value is a scraped ``href`` (SPEC §4 Tier 3), so one of them is the thing
#: :func:`web.templating.safe_url` exists to refuse. An escaped ``javascript:`` still runs.
HOSTILE_FORM = "javascript:alert(1)"

#: The order the published group must render, from :data:`web.labels.CONTACT_KIND_ORDER`:
#: email, careers page, LinkedIn, X, contact form. The fixture inserts them scrambled.
PUBLISHED_ORDER = (
    CAREERS_EMAIL,
    HR_EMAIL,
    CAREERS_PAGE,
    PUBLISHED_LINKEDIN,
    X_PROFILE,
    HOSTILE_FORM,
)

#: SPEC §6's people-search deep link: the company name plus the four title keywords, encoded.
PEOPLE_SEARCH = (
    "https://www.linkedin.com/search/results/people/?keywords=%22Atom+Computing%22+"
    "%28founder+OR+recruiter+OR+%22head+of+engineering%22+OR+%22talent%22%29"
)
#: The other company: no site of its own was ever read, so both of its rows are constructed.
QUIET_NAME = "Quiet Labs"
QUIET_LINKEDIN = "https://www.linkedin.com/company/quiet-labs"
QUIET_PEOPLE_SEARCH = (
    "https://www.linkedin.com/search/results/people/?keywords=%22Quiet+Labs%22+"
    "%28founder+OR+recruiter+OR+%22head+of+engineering%22+OR+%22talent%22%29"
)
#: SPEC §6: a profile URL is only ever one the company published on its own team page.
PROFILE_URL = "https://www.linkedin.com/in/ben-bloom"

PUBLISHED_HEADING = "Published by the company"
CONSTRUCTED_HEADING = "Search shortcuts (constructed)"
PEOPLE_HEADING = "People"


@dataclass(frozen=True, slots=True)
class ContactFixture:
    """Three companies, one per shape SPEC §6's block has to render.

    ``rich`` has both halves and two people; ``shortcuts_only`` has nothing published, which is
    the common case for a company no Tier-3 fetch ever visited; ``silent`` has neither, which
    is every company on the day it is seeded.

    SPEC §12 Phase 8 reads the same three as its own three outcomes, without changing a row:
    ``rich`` published a profile URL (the first tier), ``shortcuts_only`` names somebody who has
    none (the second), and ``silent`` offers no door at all. That the two sets of shapes coincide
    is not a coincidence — the door a company offers *is* what it has published about its people
    — so the Phase 8 tests below reuse these and add a company only for what these cannot show.
    """

    rich: int
    shortcuts_only: int
    silent: int
    #: A personal address *and* a published profile, which is the one ranking the three above
    #: cannot show: an address that reaches a human outranks even a profile URL, while ``rich``'s
    #: two queue addresses do not.
    personal_email: int


async def make_person(
    session: AsyncSession,
    company_id: int,
    full_name: str,
    *,
    title: str | None = None,
    role_type: enums.RoleType | None = None,
    linkedin_url: str | None = None,
) -> Person:
    """One ``people`` row. Local to this module: no other web test needs the table."""
    person = Person(
        company_id=company_id,
        full_name=full_name,
        title=title,
        role_type=role_type,
        linkedin_url=linkedin_url,
    )
    session.add(person)
    await session.flush()
    return person


@pytest.fixture
async def contacts(session: AsyncSession) -> ContactFixture:
    """The contacts of three companies, inserted in an order no assertion below expects.

    The scrambling is load-bearing: the panel sorts each group by kind
    (``web.routes.companies._group_contacts``), and a test seeded in the rendered order would
    pass with that sort deleted.
    """
    rich = await make_company(session, RICH_NAME, domain="atom-computing.example")
    for kind, value in (
        (enums.ContactKind.X, X_PROFILE),
        (enums.ContactKind.CONTACT_FORM, HOSTILE_FORM),
        (enums.ContactKind.EMAIL, HR_EMAIL),
        (enums.ContactKind.LINKEDIN_COMPANY, PUBLISHED_LINKEDIN),
        (enums.ContactKind.CAREERS_PAGE, CAREERS_PAGE),
        (enums.ContactKind.EMAIL, CAREERS_EMAIL),
    ):
        await make_contact(
            session, rich, kind=kind, value=value, confidence=enums.ContactConfidence.PUBLISHED
        )
    # The one row §6 says every company gets, whatever its own site said.
    await make_contact(
        session,
        rich,
        kind=enums.ContactKind.LINKEDIN_PEOPLE,
        value=PEOPLE_SEARCH,
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )
    await make_person(
        session,
        rich.id,
        "Ben Bloom",
        title="CEO & Founder",
        role_type=enums.RoleType.FOUNDER,
        linkedin_url=PROFILE_URL,
    )
    # An officer out of an EDGAR filing: a real name and title, and no profile URL at all,
    # because §6 forbids guessing one.
    await make_person(
        session,
        rich.id,
        "Dana Reyes",
        title="Chief Financial Officer",
        role_type=enums.RoleType.EXEC,
    )

    quiet = await make_company(session, QUIET_NAME, domain="quiet-labs.example")
    for kind, value in (
        (enums.ContactKind.LINKEDIN_COMPANY, QUIET_LINKEDIN),
        (enums.ContactKind.LINKEDIN_PEOPLE, QUIET_PEOPLE_SEARCH),
    ):
        await make_contact(
            session, quiet, kind=kind, value=value, confidence=enums.ContactConfidence.CONSTRUCTED
        )
    await make_person(session, quiet.id, "Sam Okafor", title="Head of Operations")

    silent = await make_company(session, "Silent Co", domain="silent-co.example")

    direct = await make_company(session, DIRECT_NAME, domain="direct-labs.example")
    await make_contact(
        session,
        direct,
        kind=enums.ContactKind.EMAIL,
        value=FOUNDER_EMAIL,
        confidence=enums.ContactConfidence.PUBLISHED,
    )
    await make_person(
        session,
        direct.id,
        "Ada Okonjo",
        title="Co-founder",
        role_type=enums.RoleType.FOUNDER,
        linkedin_url=DIRECT_PROFILE_URL,
    )

    await session.commit()
    return ContactFixture(
        rich=rich.id,
        shortcuts_only=quiet.id,
        silent=silent.id,
        personal_email=direct.id,
    )


@pytest.fixture(params=["permalink", "fragment"])
def detail_url(request: pytest.FixtureRequest) -> Callable[[int], str]:
    """Both entry points to one panel: the standalone page (§9) and the htmx fragment.

    Every contacts assertion runs twice through this, because the two are rendered by two
    handlers and the block is what a reader keeps a permalink *for*.
    """
    suffix = "" if request.param == "permalink" else "/panel"

    def url(company_id: int) -> str:
        return f"/company/{company_id}{suffix}"

    return url


async def detail_html(
    client: httpx.AsyncClient, detail_url: Callable[[int], str], company_id: int
) -> str:
    response = await client.get(detail_url(company_id))
    assert response.status_code == 200, detail_url(company_id)
    return response.text


def squashed(node: Node) -> str:
    """A node's visible text with its whitespace collapsed to single spaces."""
    return " ".join(node.text().split())


def panel_section(markup: str, heading: str) -> Node:
    """One section of the rendered panel, found by the text of its ``<h2>``.

    By heading rather than by a class or a position, because the heading is the only part of a
    section a reader is promised: the panel has eight sections, they carry one class between
    them, and SPEC §12 Phase 8 has just changed the order they appear in.
    """
    for section in HTMLParser(markup).css("section.panel-section"):
        found = section.css_first("h2")
        if found is not None and squashed(found) == heading:
            return section
    raise AssertionError(f"the rendered panel has no {heading!r} section")


def contact_section(markup: str) -> Node:
    """The panel section headed "Contacts" — SPEC §9's contacts block."""
    return panel_section(markup, "Contacts")


def outreach_section(markup: str) -> Node:
    """The panel section headed "How to reach them" — SPEC §12 Phase 8's door and its draft."""
    return panel_section(markup, "How to reach them")


def contact_groups(markup: str) -> dict[str, list[Node]]:
    """Each contact ``<li>``, keyed by the group heading rendered above it, in document order.

    Grouped by position rather than by a class or a badge, because position is what a reader
    goes by: a row means "the company published this" only when the heading immediately above
    it says so. One flat list distinguished solely by its badges would arrive here as a single
    group and fail every assertion below — which is the point of grouping it this way.
    """
    groups: dict[str, list[Node]] = {}
    heading: str | None = None
    for node in contact_section(markup).iter():
        if node.tag == "h3":
            heading = node.text().strip()
            groups.setdefault(heading, [])
        elif node.tag == "ul":
            assert heading is not None, "a list of contacts with no heading above it"
            groups[heading].extend(node.css("li"))
    return groups


def group_html(groups: dict[str, list[Node]], heading: str) -> str:
    """One group's markup, entity-decoded, for substring assertions about stored values."""
    return html.unescape("".join(row.html or "" for row in groups.get(heading, [])))


def email_row(groups: dict[str, list[Node]], address: str) -> Node:
    """The contacts row that renders one address, from whichever group holds it."""
    for group in groups.values():
        for row in group:
            if address in squashed(row):
                return row
    raise AssertionError(f"no contacts row renders {address}")


def mailto_parts(anchor: Node) -> tuple[str, dict[str, list[str]]]:
    """A ``mailto:`` href split into the address it sends to and the draft it carries.

    Read straight off the attribute with no :func:`html.unescape`: the parser has already
    decoded the entities, and a second pass over a URL whose parameters are separated by bare
    ``&`` is not merely redundant but the one place in this file where it could change a value.
    """
    href = anchor.attributes.get("href") or ""
    parts = urlsplit(href)
    assert parts.scheme == "mailto", href
    return parts.path, parse_qs(parts.query)


def door_button(section: Node) -> Node:
    """The one control the outreach block offers, whichever of the three doors it picked.

    Unpacked as a single element on purpose: two buttons in this block would mean the reader is
    being asked to choose a door, which is the decision :func:`web.routes.companies.
    best_outreach_door` exists to have already made.
    """
    (button,) = section.css("a.button")
    return button


def drafted_note(section: Node) -> str:
    """The draft in the outreach block's ``<textarea>``, as it would be pasted.

    The leading newline is dropped for the reason ``tests.support_web._textarea_value`` drops
    it: an HTML parser swallows one immediately after the open tag, so a template that ever
    grows one must not silently change what this function says was drafted.
    """
    (textarea,) = section.css("textarea")
    text = textarea.text()
    return text[1:] if text.startswith("\n") else text


# ------------------------------------------- the order the expanded row leads with (§12 Phase 8)

#: Every ``<h2>`` of the expanded row, in the order SPEC §12 Phase 8 leaves them.
#:
#: Written out whole rather than as the one pair of indices that changed, because the whole list
#: is what a reader meets and the two sections that moved are only meaningful against the six
#: that did not. Phase 4 left this order with "Open roles" fourth and "Contacts" fifth; the
#: inversion is the phase, and the reason is measured: 2 of 5,032 open roles in this database are
#: part-time (2026-09-09), so a row that leads with a roles table leads with the thing that cannot
#: serve the reason the app exists.
PANEL_SECTIONS = (
    "About",
    "Locations",
    "Funding",
    "How to reach them",
    "Contacts",
    "Open roles",
    "Provenance",
    "Your notes",
)


def panel_headings(markup: str) -> list[str]:
    """The panel's section headings, in document order, without the roles count beside one.

    The count is stripped rather than written into the expected list because it is the one
    heading whose text depends on the fixture, and what is being asserted here is the order.
    """
    headings: list[str] = []
    for section in HTMLParser(markup).css("section.panel-section"):
        heading = section.css_first("h2")
        if heading is None:
            continue
        for count in heading.css("span.count"):
            count.decompose()
        headings.append(squashed(heading))
    return headings


async def test_the_panel_leads_with_a_human_and_not_with_the_roles_table(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """SPEC §12 Phase 8's reordering, asserted as the order rather than as two `in` checks.

    A startup does not advertise part-time work; it invents it when a specific person asks. So
    the two blocks about reaching a human sit above the table of things to apply to, and the
    roles below them are evidence that there is budget and unmet need. The whole list is pinned
    because "above" is a relation and only the sequence can express it — a section silently
    dropped or a ninth one appended is the same failure as one moved.

    Run against both entry points, which is the point of doing it here rather than once: the
    permalink and the htmx fragment are two handlers sharing one ``_panel_context``, so a
    reorder that reached only one of them would be invisible to a test that rendered either.
    """
    headings = panel_headings(await detail_html(client, detail_url, contacts.rich))

    assert headings == list(PANEL_SECTIONS)
    assert headings.index("Contacts") < headings.index("Open roles")
    assert headings.index("How to reach them") < headings.index("Contacts")


# ------------------------------------------------- published vs constructed, still (§6, §9)


async def test_published_and_constructed_are_two_groups_in_a_fixed_order(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """SPEC §6's last clause, as structure: which half a row is in is visible without reading it.

    Both directions are asserted — every published value under the published heading *and*
    under no other — because the failure this guards against is not a missing row but a row
    filed under the wrong promise: a constructed search shortcut presented as a real address is
    exactly the confusion §6 exists to prevent.
    """
    markup = await detail_html(client, detail_url, contacts.rich)
    groups = contact_groups(markup)

    assert list(groups) == [PUBLISHED_HEADING, CONSTRUCTED_HEADING, PEOPLE_HEADING]
    published = group_html(groups, PUBLISHED_HEADING)
    constructed = group_html(groups, CONSTRUCTED_HEADING)

    assert len(groups[PUBLISHED_HEADING]) == len(PUBLISHED_ORDER)
    for value in PUBLISHED_ORDER:
        assert value in published, value
        assert value not in constructed, value
    assert len(groups[CONSTRUCTED_HEADING]) == 1
    assert PEOPLE_SEARCH in constructed
    assert PEOPLE_SEARCH not in published

    # Ordered by kind (web.labels.CONTACT_KIND_ORDER), not by the order they were stored.
    positions = [published.index(value) for value in PUBLISHED_ORDER]
    assert positions == sorted(positions), "the published group is not in CONTACT_KIND_ORDER"


async def test_every_contact_row_carries_the_badge_for_its_half(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """The badge rides on the row as well as on the heading (SPEC §6, §9 "clearly labeled").

    Both, not either: the heading is what a reader scanning the block sees, and the badge is
    what survives the row being copied, screenshotted or read out of order by a screen reader.
    """
    groups = contact_groups(await detail_html(client, detail_url, contacts.rich))

    for row in groups[PUBLISHED_HEADING]:
        assert '<span class="badge published">Published</span>' in (row.html or ""), row.html
        assert "badge constructed" not in (row.html or ""), row.html
    for row in groups[CONSTRUCTED_HEADING]:
        assert '<span class="badge constructed">Constructed</span>' in (row.html or ""), row.html
        assert "badge published" not in (row.html or ""), row.html


def inline_pieces(row: Node) -> list[str]:
    """Each direct child of a row as a reader sees it, with the whitespace between dropped."""
    return [text for node in row.iter(include_text=True) if (text := squashed(node))]


async def test_no_label_is_welded_to_the_value_it_labels(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """Every row reads as separate words, not as one run-on string.

    The whitespace between two inline elements is content: HTML collapses it but never invents
    it, so a Jinja ``{%- … -#}`` that trims the newline between the kind label and the value
    welds them into ``Emailhr@atom-computing.com``. That is invisible in the template, invisible
    in a structural assertion about tags and badges, and the first thing a reader sees — and it
    is exactly what the email branch of ``contact_row`` did until this test was written.

    Asserted over every row of every group, including the people list, because the trimming is
    a per-branch property of the macros and only the branch that is wrong reads differently.
    """
    groups = contact_groups(await detail_html(client, detail_url, contacts.rich))
    rows = [row for group in groups.values() for row in group]
    assert len(rows) > len(groups), "expected several rows per group to check"

    for row in rows:
        pieces = inline_pieces(row)
        assert len(pieces) > 1, row.html  # a row with one piece cannot show the defect
        assert squashed(row) == " ".join(pieces), row.html


def _css_properties(css: str, selector: str) -> dict[str, str]:
    """Every declaration of every rule whose selector list names ``selector`` exactly.

    A deliberately tiny parser: comments stripped, then each ``selectors { declarations }``
    pair containing no nested braces — which skips the ``@media`` wrapper while keeping the
    rules inside it. Enough for the one question asked below.
    """
    properties: dict[str, str] = {}
    without_comments = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    for rule in re.finditer(r"([^{}]+)\{([^{}]*)\}", without_comments):
        if selector not in {part.strip() for part in rule.group(1).split(",")}:
            continue
        for declaration in rule.group(2).split(";"):
            name, _, value = declaration.partition(":")
            if value.strip():
                properties[name.strip()] = " ".join(value.split())
    return properties


def test_the_two_confidence_badges_differ_by_more_than_colour() -> None:
    """SPEC §6's "visually distinguish", asserted where it is actually implemented.

    Colour alone is not a distinction this app accepts anywhere — ``.chip.family.match`` says
    so in its own comment — and it is worth less here than there: the two badges sit inches
    apart in a list, and ``--ok`` against ``--warn`` is one of the pairs a red-green colour
    deficiency flattens. So the assertion is not "they are styled" but "they differ in
    something that is not colour".
    """
    css = STYLESHEET.read_text(encoding="utf-8")
    published = _css_properties(css, ".badge.published")
    constructed = _css_properties(css, ".badge.constructed")

    assert published and constructed, "one of the two confidence badges is not styled at all"
    non_colour = {
        name
        for name in set(published) | set(constructed)
        if name != "color" and published.get(name) != constructed.get(name)
    }
    assert non_colour, "published and constructed are told apart by colour alone"


async def test_one_line_above_both_groups_explains_the_difference(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """The phase prompt asks for "a one-line explanation of the difference" — one, and above.

    Above both groups because it is about the distinction rather than about either half, and
    one sentence because a paragraph in the middle of a contacts list is a paragraph nobody
    reads. Counting the sentences is the only way to keep the second requirement; asserting
    that no prose at all sits between the two group headings is the only way to keep the
    first, since a second explanation added lower down is still a second explanation.
    """
    nodes = list(contact_section(await detail_html(client, detail_url, contacts.rich)).iter())
    first_group = next(index for index, node in enumerate(nodes) if node.tag == "h3")
    people_at = next(
        index
        for index, node in enumerate(nodes)
        if node.tag == "h3" and squashed(node) == PEOPLE_HEADING
    )
    lead = [node for node in nodes[:first_group] if node.tag == "p"]

    assert len(lead) == 1, "expected exactly one explanation, before the first group heading"
    sentence = squashed(lead[0])
    assert sentence.count(".") == 1 and sentence.endswith("."), sentence
    assert "Published" in sentence
    assert "search shortcuts" in sentence.lower()
    # Both groups are populated for this company, so the only thing a paragraph between them
    # could be is more explanation — and there is already exactly one, above both.
    between = [squashed(node) for node in nodes[first_group:people_at] if node.tag == "p"]
    assert between == [], "the explanation belongs above both groups, not inside one"


async def test_the_people_search_is_the_find_people_control(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """SPEC §6: "The UI renders it as a **"Find people →"** button."

    An ``<a class="button">`` rather than a ``<button>``: this app ships no JavaScript of its
    own (SPEC §3, §9), so a control needing a click handler would not work at all. The kind
    label is suppressed on this row — the control already says what it is, and "Find people
    Find people →" is what printing both looks like.
    """
    groups = contact_groups(await detail_html(client, detail_url, contacts.rich))
    (row,) = groups[CONSTRUCTED_HEADING]
    markup = row.html or ""

    (anchor,) = row.css("a")
    assert html.unescape(anchor.attributes.get("href") or "") == PEOPLE_SEARCH
    assert "button" in (anchor.attributes.get("class") or "").split()
    assert squashed(anchor) == "Find people →"
    assert "<button" not in markup and "onclick" not in markup
    assert markup.count("Find people") == 1


async def test_an_email_is_a_mailto_and_every_other_value_goes_through_safe_url(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """Two schemes, two rules. ``mailto:`` is written by the template; ``http(s)`` is vetted.

    A contact value is a scraped ``href`` (SPEC §4 Tier 3), so ``javascript:`` is a real
    stored value, not a hypothetical: escaping does nothing to it, and only
    :func:`web.templating.safe_url` keeps it out of the attribute. It still has to be *shown*
    — SPEC §6 stores what the company published and the panel shows what was stored — so the
    refusal renders as inert text rather than as a silently dropped row.

    The ``mailto:`` half is asserted as the address it would send to rather than as the anchor's
    whole markup: SPEC §12 Phase 8 made it a button carrying a drafted subject and body, and
    *what* it drafts is the next test's business. What stays this test's business is that the
    one scheme ``safe_url`` refuses by design is still the one the email row is built on.
    """
    markup = await detail_html(client, detail_url, contacts.rich)
    groups = contact_groups(markup)
    published = group_html(groups, PUBLISHED_HEADING)

    for address in (CAREERS_EMAIL, HR_EMAIL):
        (anchor,) = email_row(groups, address).css("a")
        assert mailto_parts(anchor)[0] == address
    assert (
        f'<a href="{CAREERS_PAGE}" rel="noopener noreferrer" target="_blank">{CAREERS_PAGE}</a>'
    ) in published

    assert 'href="javascript:' not in markup.lower()
    assert f'<span class="muted">{HOSTILE_FORM}</span>' in published


async def test_a_published_email_is_a_button_carrying_a_drafted_message(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """SPEC §12 Phase 8's email door: the row's primary action, with the pitch already written.

    397 of 10,219 companies here publish an address, so where one exists it is worth more than a
    small link in a list of six — the button opens a composed draft and the plain address stays
    beside it, because the button is what you send with and the text is what you copy and what
    tells you who you are about to write to.

    The drafts are read back out of the URL and compared against ``config/outreach.yaml`` rather
    than against words written here. That is the rule this phase actually rests on: the pitch is
    the variable worth iterating on, so it lives in config and a test that spelled it out would
    make rewriting it a code change — which is exactly what CLAUDE.md's "edit YAML, not Python"
    rules out. What is pinned is that the panel sends *that* file's subject and body, with the
    company filled in, inside a URL a mail client will not silently truncate.

    That last clause is checked with headroom rather than with the ceiling itself. ``mailto_url``
    builds the href as ``prefix + quote_to_fit(body, MAILTO_MAX_URL - len(prefix))``, so
    ``len(href) <= MAILTO_MAX_URL`` holds by construction — it is true of a draft that was cut in
    half just as much as of one that fits, which makes it an assertion that cannot fail. The body
    comparison above is what actually catches a cut; the margin below is what catches the shipped
    pitch *drifting toward* one, while there is still room to notice.
    """
    outreach = load_outreach_config()
    groups = contact_groups(await detail_html(client, detail_url, contacts.rich))
    row = email_row(groups, HR_EMAIL)
    (anchor,) = row.css("a")
    address, draft = mailto_parts(anchor)

    assert "button" in (anchor.attributes.get("class") or "").split()
    assert address == HR_EMAIL
    assert draft["subject"] == [outreach.email_subject.format(company=RICH_NAME)]
    assert draft["body"] == [outreach.email_body.format(company=RICH_NAME)]

    href = anchor.attributes.get("href") or ""
    assert len(href) <= MAILTO_MAX_URL - MAILTO_HEADROOM, (
        f"the shipped draft builds a {len(href)}-character mailto: for a company named "
        f"{RICH_NAME!r}, within {MAILTO_MAX_URL - len(href)} of the {MAILTO_MAX_URL} ceiling. "
        "A longer company name than this fixture's would be cut, and the cut lands at the end "
        "of the body, which is where the ask is — shorten config/outreach.yaml's email.body"
    )

    # The address is text beside the button, not the button's own label: a row that linked the
    # address and said nothing else would be the pre-Phase-8 row with a class on it.
    assert HR_EMAIL in squashed(row)
    assert HR_EMAIL not in squashed(anchor)


async def test_a_shared_inbox_is_marked_a_personal_one_is_not_and_neither_is_hidden(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """SPEC §12 Phase 8's hint, and SPEC §7.1's rule that classification never excludes.

    ``careers@`` is a ticket queue and a scoped personal offer converts close to zero in one, so
    the reader is told which kind of address they are about to spend a draft on — measured on
    2026-09-09, all 9 of the addresses crawled off company sites are queues and 305 of the 437
    found in "Who is hiring?" comments are a named person's own. Both halves are asserted,
    because a hint that appeared on every row would distinguish nothing.

    And then the part that makes it a hint rather than a filter: neither row loses its address
    or its draft button. §7.1 is about role families, but the rule it states is general, and
    this is the first classification in the app with an obvious temptation to act on it.
    """
    outreach = load_outreach_config()
    # Stated rather than assumed, so a change to the vocabulary fails here saying what changed
    # instead of failing below looking like the template stopped rendering the hint. Both halves
    # are pinned: `careers` has to be a queue for the shared assertion to mean anything, and the
    # local part of FOUNDER_EMAIL has to *not* be one for the personal assertion to.
    assert "careers" in outreach.role_address_prefixes
    assert FOUNDER_EMAIL.partition("@")[0] not in outreach.role_address_prefixes

    shared_groups = contact_groups(await detail_html(client, detail_url, contacts.rich))
    shared = squashed(email_row(shared_groups, CAREERS_EMAIL))
    assert "general inbox" in shared and "direct" not in shared

    # A different company, because the two cannot coexist on one: an address that reaches a human
    # becomes the company's door, and `rich` exists to show a published *profile* winning.
    direct_groups = contact_groups(await detail_html(client, detail_url, contacts.personal_email))
    personal = squashed(email_row(direct_groups, FOUNDER_EMAIL))
    assert "direct" in personal and "general inbox" not in personal

    for groups, address in ((shared_groups, CAREERS_EMAIL), (direct_groups, FOUNDER_EMAIL)):
        row = email_row(groups, address)
        (anchor,) = row.css("a")
        assert mailto_parts(anchor)[0] == address, address
        assert address in squashed(row), address


async def test_an_address_that_reaches_a_person_outranks_even_a_published_profile(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """The door ranking's top tier, and the one place email beats LinkedIn (SPEC §12 Phase 8).

    Direct Labs published both a personal address and a profile URL. Email wins: there is no
    300-character cap on it, it needs no connection request, and the address was published by
    somebody expecting to be written to. So the door is a composed ``mailto:`` and there is no
    connection-note box under it — that draft travels inside the URL and there is nothing to
    paste.

    The mirror of this is ``rich``, whose only addresses are queues and whose door is therefore
    the profile: a scoped personal offer sent to ``careers@`` converts close to nothing, which is
    why a shared inbox is ranked *below* a named human rather than above one.
    """
    section = outreach_section(await detail_html(client, detail_url, contacts.personal_email))
    button = door_button(section)

    assert squashed(button) == "Draft an email →"
    assert mailto_parts(button)[0] == FOUNDER_EMAIL
    assert "direct" in squashed(section)
    assert not section.css("textarea"), "an email door carries its draft in the URL"
    # The profile is still published and still rendered below; it simply is not the door.
    assert DIRECT_PROFILE_URL not in (button.attributes.get("href") or "")


async def test_a_company_with_no_contacts_at_all_still_renders(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """The state every company is in the day it is seeded, and the panel must not stutter.

    No empty group, no explanation of a distinction there is nothing to distinguish, and no
    bare ``<ul>`` — an empty list with a heading over it reads as "we looked and found none
    published", which is a different claim from "nothing has looked here yet".
    """
    response = await client.get(detail_url(contacts.silent))
    assert response.status_code == 200
    section = contact_section(response.text)

    assert "No contacts on file yet." in section.text()
    assert section.css("h3") == []
    assert section.css("ul") == []
    assert "search shortcuts" not in section.text().lower()
    # The rest of the panel is intact: this is the whole thing, not a render that gave up.
    assert "<h2>Provenance</h2>" in response.text
    assert "<h2>Your notes</h2>" in response.text


async def test_a_company_with_only_shortcuts_says_nothing_was_published(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """The common case: no Tier-3 fetch has read this company's own site, so both rows are ours.

    The published heading still renders, with a line under it saying the group is empty. That
    is the honest reading of §6's "otherwise": the shortcuts are what is left when nothing was
    published, and dropping the heading would leave two constructed links looking like the
    whole truth about how to reach the company.

    This is also the only company with two constructed rows, so it is the only one that can
    say the button belongs to the people search *and to nothing else*. Asserting per row
    rather than over the group's markup is what makes that sayable: a substring check for both
    URLs cannot tell a row apart by its kind, and stays true when the constructed company page
    is dressed as the "Find people →" control — two identical buttons pointing at different
    URLs, one of them a company page with its "LinkedIn" label gone, which is the published /
    constructed-and-what-is-it confusion §6's last clause exists to prevent.
    """
    markup = await detail_html(client, detail_url, contacts.shortcuts_only)
    groups = contact_groups(markup)
    text = squashed(contact_section(markup))

    assert list(groups) == [PUBLISHED_HEADING, CONSTRUCTED_HEADING, PEOPLE_HEADING]
    assert groups[PUBLISHED_HEADING] == []
    assert len(groups[CONSTRUCTED_HEADING]) == 2
    company_row, people_row = groups[CONSTRUCTED_HEADING]

    # The company page is a labelled value like any other row, constructed or not.
    assert '<span class="muted">LinkedIn</span>' in (company_row.html or ""), company_row.html
    assert "Find people" not in (company_row.html or ""), company_row.html
    (company_anchor,) = company_row.css("a")
    assert html.unescape(company_anchor.attributes.get("href") or "") == QUIET_LINKEDIN
    assert squashed(company_anchor) == QUIET_LINKEDIN
    assert "button" not in (company_anchor.attributes.get("class") or "").split()

    # Only the people search is the control, here as on the company that has one of each.
    (people_anchor,) = people_row.css("a")
    assert html.unescape(people_anchor.attributes.get("href") or "") == QUIET_PEOPLE_SEARCH
    assert "button" in (people_anchor.attributes.get("class") or "").split()
    assert squashed(people_anchor) == "Find people →"
    assert len(contact_section(markup).css("a.button")) == 1

    assert "Nothing published on their own site yet." in text
    assert "No contacts on file yet." not in text


async def test_a_person_links_only_to_a_profile_the_company_published(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """SPEC §6 / §4: a profile URL is one the company itself published, and is never fetched.

    Hence the two people: one off a team page that linked its own founder, one off an EDGAR
    filing that named an officer and no URL. The sentence saying where those links came from
    is only worth printing when there is a link to say it about, so the company whose only
    person has no URL must not print it — that is the second half of this test.
    """
    markup = await detail_html(client, detail_url, contacts.rich)
    people = contact_groups(markup)[PEOPLE_HEADING]
    assert len(people) == 2
    linked, unlinked = people

    assert squashed(linked).startswith("Ben Bloom")
    assert [squashed(span) for span in linked.css("span.muted")] == ["CEO & Founder", "Founder"]
    (anchor,) = linked.css("a")
    assert anchor.attributes.get("href") == PROFILE_URL
    assert squashed(anchor) == "LinkedIn"

    assert squashed(unlinked).startswith("Dana Reyes")
    assert unlinked.css("a") == []

    assert "no LinkedIn page is ever fetched" in squashed(contact_section(markup))
    quiet = await detail_html(client, detail_url, contacts.shortcuts_only)
    assert "no LinkedIn page is ever fetched" not in squashed(contact_section(quiet))


async def test_the_contacts_block_is_identical_on_the_permalink_and_the_fragment(
    client: httpx.AsyncClient, contacts: ContactFixture
) -> None:
    """§9 calls ``/company/{id}`` "a standalone, linkable version of the expanded row".

    The two are rendered by two handlers through one context builder, and the roles table
    above deliberately differs between them (the fragment carries ``hx-*`` on its headers,
    the permalink cannot). The contacts block has no such difference to make, so any at all
    would be drift — and it is the half of the panel a reader keeps the permalink for.
    """
    page = await client.get(f"/company/{contacts.rich}")
    fragment = await client.get(f"/company/{contacts.rich}/panel")
    assert page.status_code == fragment.status_code == 200

    block = contact_section(page.text).html
    assert block == contact_section(fragment.text).html
    assert block is not None
    assert PUBLISHED_HEADING in block and CONSTRUCTED_HEADING in block


# ------------------------------------------------ the door the panel offers (§6, §12 Phase 8)

#: Two of the three tiers already have a company above: ``rich`` published somebody's profile
#: URL and ``quiet`` names somebody who has none. These are the shapes those two cannot show.
NOBODY_NAME = "Nobody Labs"
#: The company-wide search SPEC §6 stores for every named company — the third tier's whole door.
NOBODY_PEOPLE_SEARCH = (
    "https://www.linkedin.com/search/results/people/?keywords=%22Nobody+Labs%22+"
    "%28founder+OR+recruiter+OR+%22head+of+engineering%22+OR+%22talent%22%29"
)
FILED_NAME = "Ledger Works"
FILED_PEOPLE_SEARCH = (
    "https://www.linkedin.com/search/results/people/?keywords=%22Ledger+Works%22+"
    "%28founder+OR+recruiter+OR+%22head+of+engineering%22+OR+%22talent%22%29"
)
FILED_PERSON = "Priya Raman"
#: What an SEC Form D "related persons" list actually files a fund under, leading full stop and
#: all, stored with ``role_type=founder`` because that is what the filing calls it. The panel
#: must never draft "Hi <fund>, I'm a student" — see ``config/outreach.yaml``'s entity markers.
FORM_D_ENTITY = ". Northwind Real Estate Debt Fund GP LLC"
#: The same name without the punctuation the leading full stop puts in front of it, so a
#: substring assertion is about the entity rather than about how a stray "." collapses.
ENTITY_WORDS = "Northwind Real Estate Debt Fund GP LLC"
#: Deliberately absurd, and the only reason it exists: the shipped note renders to about 250
#: characters for a short company name, so a name this long is what proves the 300-character cap
#: is enforced on the rendered draft rather than only on the template at load time.
OVERLONG_NAME = ("Northwind " + "Advanced Robotics and Automation Systems " * 3).strip()
OVERLONG_PERSON = "Ada Vance"
#: One company name per character class that would otherwise break a ``mailto:`` instead of
#: travelling inside it: ``&`` starts a query parameter, ``"`` closes the attribute, and a
#: non-ASCII letter is not a byte a URL may carry at all.
PUNCTUATED_NAME = 'Ampersand & Söhne "Labs"'
PUNCTUATED_EMAIL = "ada@ampersand-soehne.example"
#: A name ending in a full stop — ``Inc.``, ``Ltd.``, ``Co.`` — which is how thousands of company
#: names are really written. Nothing else in this suite ends one in punctuation, which is how a
#: note template with ``{company}`` as its last word shipped rendering a doubled period.
ABBREVIATED_NAME = "Northwind Robotics, Inc."
ABBREVIATED_PERSON = "Ida Okafor"
#: A company that published a queue and named nobody — the fourth-ranked door, and the second
#: most common one in the live database: 118 of the 397 companies with an address publish only
#: shared inboxes, and 112 of those name nobody at all, so this is what they lead with.
QUEUE_ONLY_NAME = "Halyard Systems"
QUEUE_ONLY_EMAIL = "jobs@halyard-systems.example"
#: A name carrying a line break in the middle, copied from the shape of a real stored row: a
#: "Who is hiring?" comment whose leading line broke where the connector read it. SPEC §2 keeps
#: what the source said, so the panel is what has to cope.
BROKEN_NAME = "June 2025\nProphet Town Labs"
BROKEN_PERSON = "Theo Marsh"
#: Deliberately a shared inbox. It keeps the company's published address — so the ``mailto:`` in
#: the Contacts row is there to inspect — while demoting the outreach door to ``person_search``,
#: which is the door that renders a note. One company, both halves of the same bug.
BROKEN_EMAIL = "careers@prophet-town.example"


@dataclass(frozen=True, slots=True)
class DoorFixture:
    """Four companies, one per thing SPEC §12 Phase 8 has to get right that ``contacts`` cannot.

    ``nameless`` has a people search and nobody named, which is the third tier; ``filed`` has an
    EDGAR filing entity ranked above a real human, which is the guard; ``overlong`` has a name
    long enough to overrun the connection-note cap; ``punctuated`` has a name that has to survive
    percent-encoding into a ``mailto:``.

    The last two are about names as they are really *stored* rather than as they are really
    written: ``abbreviated`` ends in a full stop, and ``broken`` carries a line break in the
    middle. Both are shapes the live database holds and this suite otherwise never produced.
    """

    nameless: int
    filed: int
    overlong: int
    punctuated: int
    abbreviated: int
    broken: int
    queue_only: int


@pytest.fixture
async def doors(session: AsyncSession) -> DoorFixture:
    """The four companies above, each carrying only what its own tier needs."""
    nameless = await make_company(session, NOBODY_NAME, domain="nobody-labs.example")
    await make_contact(
        session,
        nameless,
        kind=enums.ContactKind.LINKEDIN_PEOPLE,
        value=NOBODY_PEOPLE_SEARCH,
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )

    filed = await make_company(session, FILED_NAME, domain="ledger-works.example")
    await make_contact(
        session,
        filed,
        kind=enums.ContactKind.LINKEDIN_PEOPLE,
        value=FILED_PEOPLE_SEARCH,
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )
    # Inserted first and classified `founder`, so it outranks the human on both keys
    # `web.routes.companies._people_worth_messaging` sorts by: with the guard removed, this is
    # the "person" the panel would offer, and the company still has a third-tier door to fall
    # back to, so a guard that skipped everybody would be told apart from one that works.
    await make_person(session, filed.id, FORM_D_ENTITY, role_type=enums.RoleType.FOUNDER)
    await make_person(session, filed.id, FILED_PERSON, title="Operations lead")

    overlong = await make_company(session, OVERLONG_NAME, domain="northwind-advanced.example")
    await make_person(session, overlong.id, OVERLONG_PERSON, title="Founder")

    punctuated = await make_company(session, PUNCTUATED_NAME, domain="ampersand-soehne.example")
    await make_contact(
        session,
        punctuated,
        kind=enums.ContactKind.EMAIL,
        value=PUNCTUATED_EMAIL,
        confidence=enums.ContactConfidence.PUBLISHED,
    )
    abbreviated = await make_company(session, ABBREVIATED_NAME, domain="northwind-robotics.example")
    await make_person(session, abbreviated.id, ABBREVIATED_PERSON, title="Founder")

    broken = await make_company(session, BROKEN_NAME, domain="prophet-town.example")
    await make_person(session, broken.id, BROKEN_PERSON, title="Founder")
    await make_contact(
        session,
        broken,
        kind=enums.ContactKind.EMAIL,
        value=BROKEN_EMAIL,
        confidence=enums.ContactConfidence.PUBLISHED,
    )

    queue_only = await make_company(session, QUEUE_ONLY_NAME, domain="halyard-systems.example")
    await make_contact(
        session,
        queue_only,
        kind=enums.ContactKind.EMAIL,
        value=QUEUE_ONLY_EMAIL,
        confidence=enums.ContactConfidence.PUBLISHED,
    )

    await session.commit()
    return DoorFixture(
        nameless=nameless.id,
        filed=filed.id,
        overlong=overlong.id,
        punctuated=punctuated.id,
        abbreviated=abbreviated.id,
        broken=broken.id,
        queue_only=queue_only.id,
    )


async def test_a_published_profile_is_the_door_the_panel_offers(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """Tier 1: 69 people in this database carry a profile URL, and it beats any search.

    The URL is one the company published on its own team page — SPEC §4 excludes LinkedIn from
    the connectors and SPEC §6 forbids constructing a profile link, so a stored one is always
    somebody's own. The block names them and their title, because who you are writing to is the
    decision the reader is being asked to make.

    The last two assertions are the tier's boundary rather than decoration: the company-wide
    search is still stored on this company and still rendered in the contacts block below, so a
    door-picker that fell through to it would produce a page that *looks* right, and "ask for a
    founder" printed next to a door that already names the founder is the same mistake read
    from the other end.
    """
    section = outreach_section(await detail_html(client, detail_url, contacts.rich))
    text = squashed(section)
    button = door_button(section)

    assert button.attributes.get("href") == PROFILE_URL
    assert squashed(button) == "Open their profile →"
    assert "Ben Bloom" in text
    assert "CEO & Founder" in text
    assert PEOPLE_SEARCH not in (section.html or "")
    assert "Ask for" not in text


async def test_a_named_person_with_no_profile_gets_a_search_scoped_to_them(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """Tier 2: 4,946 companies name somebody and only 69 people carry a URL, so this is the bulk.

    A search for the person *and* the company lands on one profile; the company-wide search
    lands on a list to triage, and the two are one keyword apart in the URL and worlds apart in
    what the reader has to do next. Both halves of the keyword expression are asserted, and so
    is the fact that the result is not the search already stored on this company — which is the
    only way to tell a scoped search from the fallback dressed in the person's name.
    """
    section = outreach_section(await detail_html(client, detail_url, contacts.shortcuts_only))
    button = door_button(section)
    href = button.attributes.get("href") or ""
    (keywords,) = parse_qs(urlsplit(href).query)["keywords"]

    assert squashed(button) == "Find Sam Okafor →"
    assert "Sam Okafor" in keywords
    assert QUIET_NAME in keywords
    assert href != QUIET_PEOPLE_SEARCH
    assert "Head of Operations" in squashed(section)


async def test_nobody_named_gets_the_company_wide_search_and_who_to_ask_for(
    client: httpx.AsyncClient, doors: DoorFixture, detail_url: Callable[[int], str]
) -> None:
    """Tier 3: the door every company has, now carrying the guidance it never used to.

    The URL is read off the stored ``linkedin_people`` contact rather than rebuilt, so the
    button here and the link in the contacts block below are the same search by construction —
    two search expressions for one company would be two different lists of people.

    The roles to ask for are rendered in ``contact_priority``'s own order. Asserted against the
    loaded config rather than against a list written here, because the order is a decision that
    is meant to be changed in ``config/outreach.yaml`` alone (CLAUDE.md: edit YAML, not Python);
    what the panel owes is to print that file's order, whatever it currently says.
    """
    outreach = load_outreach_config()
    section = outreach_section(await detail_html(client, detail_url, doors.nameless))
    button = door_button(section)
    expected = ", ".join(label_for(role) for role in outreach.contact_priority)

    assert button.attributes.get("href") == NOBODY_PEOPLE_SEARCH
    assert squashed(button) == "Find people →"
    assert f"Ask for, best first: {expected}." in squashed(section)


async def test_a_company_with_nobody_named_and_no_search_says_there_is_no_door(
    client: httpx.AsyncClient, contacts: ContactFixture, detail_url: Callable[[int], str]
) -> None:
    """The state every company is in the day it is seeded, and the block must not invent a door.

    A search built for a company whose name we do not have is a link to somebody else's
    company, and a draft with nowhere to paste it is worse than an empty section — so what
    renders is a sentence saying so, with no control and no textarea to fill in.
    """
    section = outreach_section(await detail_html(client, detail_url, contacts.silent))

    assert "No door on file" in squashed(section)
    assert section.css("a") == []
    assert section.css("textarea") == []


async def test_a_filing_entity_is_never_offered_as_someone_to_message(
    client: httpx.AsyncClient, doors: DoorFixture, detail_url: Callable[[int], str]
) -> None:
    """SPEC §12 Phase 8's entity guard: a Form D "related person" may be a fund, not a human.

    EDGAR files names like the one in :data:`FORM_D_ENTITY` in a related-persons list, and the
    ingest side stores them as founders because that is what the filing calls them. Ranked
    first by ``contact_priority`` and inserted first, this one wins every tiebreak the door
    picker has — so a panel that drafts "Hi <fund>, I'm a Stanford student" is one deleted
    marker away, and the human filed behind it is the only thing that proves the guard skipped
    rather than emptied.

    Skipped as a *door*, though, and not deleted: SPEC §7.1's rule that classification never
    excludes applies to the stored row, which still appears in the contacts block's people list
    — the panel shows what was stored (§6) and simply declines to write to it.
    """
    markup = await detail_html(client, detail_url, doors.filed)
    section = outreach_section(markup)
    text = squashed(section)

    assert squashed(door_button(section)) == f"Find {FILED_PERSON} →"
    assert FILED_PERSON in text
    assert ENTITY_WORDS not in text
    assert ENTITY_WORDS not in (section.html or "")
    assert ENTITY_WORDS in squashed(contact_section(markup))


async def test_every_drafted_note_fits_a_connection_request(
    client: httpx.AsyncClient,
    contacts: ContactFixture,
    doors: DoorFixture,
    detail_url: Callable[[int], str],
) -> None:
    """300 characters is LinkedIn's cap, and a draft one character over cannot be sent at all.

    Not "is usually short enough": LinkedIn refuses an over-long connection note rather than
    truncating it, so an unclamped draft shuts the door this whole tier exists to open. Asserted
    over every shape that renders one, because the cut lives in one helper and each tier reaches
    it with a different pair of names.

    The count beside the box has to agree with the box, since it is what an *edited* draft is
    kept legal by — a stale number is a reader confidently pasting something that will not send.

    The last two assertions are the ones that could fail today: a short company name renders the
    configured draft untouched, and the absurd one is cut on a word boundary and marked, so the
    sender sees that it was cut instead of discovering it in the reply.
    """
    outreach = load_outreach_config()
    everyone = (
        contacts.rich,
        contacts.shortcuts_only,
        doors.nameless,
        doors.filed,
        doors.overlong,
    )
    for company_id in everyone:
        section = outreach_section(await detail_html(client, detail_url, company_id))
        note = drafted_note(section)
        assert 0 < len(note) <= LINKEDIN_NOTE_LIMIT, (company_id, len(note))
        assert f"{len(note)} of {LINKEDIN_NOTE_LIMIT} characters" in squashed(section)

    rich = outreach_section(await detail_html(client, detail_url, contacts.rich))
    assert drafted_note(rich) == outreach.linkedin_note.format(
        company=RICH_NAME, person="Ben Bloom"
    )

    unclamped = outreach.linkedin_note.format(company=OVERLONG_NAME, person=OVERLONG_PERSON)
    assert len(unclamped) > LINKEDIN_NOTE_LIMIT, "the overlong fixture no longer overruns the cap"
    overlong = outreach_section(await detail_html(client, detail_url, doors.overlong))
    assert drafted_note(overlong).endswith("…")


async def test_a_company_name_full_of_punctuation_survives_into_the_mailto(
    client: httpx.AsyncClient, doors: DoorFixture, detail_url: Callable[[int], str]
) -> None:
    """The draft is a URL, and three characters in a company name are what a URL is made of.

    An unencoded ``&`` in the subject does not corrupt the draft, it *ends* it — everything
    after it is read as another URL parameter, which is the same class of injection
    :func:`web.templating.safe_url` exists for, arriving through the one link that cannot use
    it. So the assertions are made in both directions: the encoded form is what travels, and
    what comes back out of the URL is the name as stored, byte for byte.
    """
    outreach = load_outreach_config()
    groups = contact_groups(await detail_html(client, detail_url, doors.punctuated))
    (anchor,) = email_row(groups, PUNCTUATED_EMAIL).css("a")
    href = anchor.attributes.get("href") or ""
    address, draft = mailto_parts(anchor)

    assert address == PUNCTUATED_EMAIL
    assert draft["subject"] == [outreach.email_subject.format(company=PUNCTUATED_NAME)]
    assert draft["body"] == [outreach.email_body.format(company=PUNCTUATED_NAME)]
    assert "%26" in href and "%22" in href and "%C3%B6" in href
    # Exactly one ampersand left in the whole URL: the separator this function wrote itself.
    assert href.count("&") == 1


async def test_a_queue_is_still_the_door_when_it_is_the_only_one_and_is_marked_as_a_queue(
    client: httpx.AsyncClient, doors: DoorFixture, detail_url: Callable[[int], str]
) -> None:
    """The fourth-ranked door: a shared inbox that nothing outranks (SPEC §12 Phase 8).

    A queue is demoted below a named human, not discarded — so a company that published only
    ``jobs@`` and names nobody still leads with that address, because it is genuinely the best way
    in that this database holds for it. Measured on 2026-09-09, that is 112 of the 397 companies
    with an address, which makes this the second most common door in the corpus and the one branch
    of the outreach block that no other case here reaches.

    The mark is a **chip**, not a confidence badge, and that is the assertion worth making.
    ``.badge.published`` / ``.badge.constructed`` are SPEC §6's provenance pair — they say whether
    the company published a contact or this app constructed it — and this address *was* published.
    Rendering "general inbox" as ``badge constructed`` (which it was) tells the reader this app
    invented an address the company printed itself, and contradicts the ``Published`` badge on the
    very same address one section below.
    """
    markup = await detail_html(client, detail_url, doors.queue_only)
    section = outreach_section(markup)
    button = door_button(section)

    assert squashed(button) == "Draft an email →"
    assert mailto_parts(button)[0] == QUEUE_ONLY_EMAIL
    assert "general inbox" in squashed(section) and "direct" not in squashed(section)

    # Nothing is hidden by the demotion: the address is still its own contact row, still badged
    # `Published`, still carrying its own draft button (SPEC §7.1, §6).
    row = email_row(contact_groups(markup), QUEUE_ONLY_EMAIL)
    assert '<span class="badge published">Published</span>' in (row.html or "")
    assert "badge constructed" not in (section.html or ""), section.html


async def test_a_name_ending_in_a_full_stop_does_not_double_the_note_s_period(
    client: httpx.AsyncClient, doors: DoorFixture, detail_url: Callable[[int], str]
) -> None:
    """``{company}`` may not be the last thing in a sentence of ``linkedin.note``.

    A great many company names end in a full stop — ``Inc.``, ``Ltd.``, ``Co.`` — and a template
    that puts ``{company}`` immediately before its own period renders ``... Northwind Robotics,
    Inc..`` for every one of them. Measured over the live corpus when this was found, 9,103 of
    16,117 company-and-person pairs rendered a doubled period; the fix was to reword the note so a
    word follows ``{company}``, and this is what keeps it reworded.

    Asserted on the *rendered* note rather than on the template, because the template is config
    and may be rewritten freely — what may not change is that the rewrite still reads correctly
    for a name the corpus is full of. That is also why the check is a property of the output and
    not a comparison against fixed prose: CLAUDE.md's "edit YAML, not Python" means this file
    must not spell the pitch out.
    """
    section = outreach_section(await detail_html(client, detail_url, doors.abbreviated))
    note = drafted_note(section)

    assert ABBREVIATED_NAME in note, "the fixture's name is not in its own draft"
    assert ".." not in note, (
        f"the drafted note reads {note!r}. A company name ending in a full stop is doubling the "
        "one after it — move a word after {company} in config/outreach.yaml's linkedin.note"
    )


async def test_a_line_break_in_a_stored_name_never_reaches_a_draft(
    client: httpx.AsyncClient, doors: DoorFixture, detail_url: Callable[[int], str]
) -> None:
    """A company name is scraped text, and this one is stored with a newline inside it.

    The ``mailto:`` is where that matters. ``{company}`` is interpolated into the **subject**, a
    newline percent-encodes to ``%0A``, and the mail client decodes it back when it composes the
    draft — putting a line break into a Subject header, from a value that came off a third-party
    page. :func:`web.templating._mail_address` already refuses whitespace on the address half of
    the same URL for exactly that reason; :func:`web.templating.draft_name` is the other half.

    The note is the milder half of the same bug and is asserted too: a two-line company name in a
    connection request is merely wrong, but it is wrong in a box the reader pastes from.

    The name is not *rejected* anywhere — SPEC §2 keeps what the source said, and the row is still
    that company — so the last assertion is that both words survive the folding.
    """
    markup = await detail_html(client, detail_url, doors.broken)

    section = outreach_section(markup)
    note = drafted_note(section)
    assert "\n" not in note and "\r" not in note, repr(note)

    (anchor,) = email_row(contact_groups(markup), BROKEN_EMAIL).css("a")
    href = anchor.attributes.get("href") or ""
    _address, draft = mailto_parts(anchor)
    subject = draft["subject"][0]

    # The *subject* only. The body carries `%0A` by design — `config/outreach.yaml` folds its
    # paragraphs — and a newline is unremarkable in a body and a header split in a header.
    raw_subject = href.partition("?subject=")[2].partition("&body=")[0]
    assert "%0A" not in raw_subject.upper(), f"a newline reached the Subject header — {href!r}"
    assert "\n" not in subject and "\r" not in subject, repr(subject)
    # Folded to one space, not deleted: this is still that company's name.
    assert "June 2025 Prophet Town Labs" in subject
    assert "June 2025 Prophet Town Labs" in note


# ------------------------------------------------- a cursor key the driver cannot bind (§9)

#: The two characters JSON can carry that a Postgres ``text`` parameter cannot: a percent-
#: encoded URL parameter can express neither, so ``cursor=`` is the only way in.
UNBINDABLE_KEYS = ["a\x00b", "\ud800"]


@pytest.mark.parametrize("key", UNBINDABLE_KEYS)
def test_cursor_decode_refuses_a_key_that_cannot_be_bound(key: str) -> None:
    """``Cursor.decode`` rejects it, so the guarantee holds for every caller of every sort."""
    token = Cursor(sort="name", key=key, id=1).encode()
    with pytest.raises(CursorError):
        Cursor.decode(token)


@pytest.mark.parametrize("key", UNBINDABLE_KEYS)
@pytest.mark.parametrize(("path", "sort"), [("/", "name"), ("/roles", "company")])
async def test_a_crafted_cursor_key_is_the_400_page_not_a_500(
    client: httpx.AsyncClient, panel: Panel, path: str, sort: str, key: str
) -> None:
    """The two string-keyed sorts are the only ones whose key reaches a ``text`` parameter.

    Before the guard, this pair of URLs was a bare ``text/plain`` 500 out of the driver —
    ``CharacterNotInRepertoireError`` for the NUL and ``UnicodeEncodeError`` for the surrogate
    — while every other malformed token rendered ``error.html`` with a 400.
    """
    token = Cursor(sort=sort, key=key, id=1).encode()
    response = await client.get(f"{path}?sort={sort}&cursor={token}")

    assert response.status_code == 400
    assert response.headers["content-type"].startswith("text/html")


async def test_a_well_formed_cursor_key_still_pages(
    client: httpx.AsyncClient, panel: Panel
) -> None:
    """The control: rejecting those two characters costs nothing a real key could need."""
    token = Cursor(sort="name", key="panel lab", id=1).encode()
    response = await client.get(f"/?sort=name&cursor={token}")

    assert response.status_code == 200
    assert f'hx-get="/company/{panel.company}/panel"' in response.text
