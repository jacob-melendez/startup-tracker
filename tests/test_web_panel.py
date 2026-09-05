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

import httpx
import pytest
from selectolax.parser import HTMLParser, Node
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import Person
from db.queries import Cursor, CursorError
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
CAREERS_EMAIL = "careers@atom-computing.example"
HR_EMAIL = "hr@atom-computing.example"
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
    """

    rich: int
    shortcuts_only: int
    silent: int


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
    rich = await make_company(session, "Atom Computing", domain="atom-computing.example")
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

    quiet = await make_company(session, "Quiet Labs", domain="quiet-labs.example")
    for kind, value in (
        (enums.ContactKind.LINKEDIN_COMPANY, QUIET_LINKEDIN),
        (enums.ContactKind.LINKEDIN_PEOPLE, QUIET_PEOPLE_SEARCH),
    ):
        await make_contact(
            session, quiet, kind=kind, value=value, confidence=enums.ContactConfidence.CONSTRUCTED
        )
    await make_person(session, quiet.id, "Sam Okafor", title="Head of Operations")

    silent = await make_company(session, "Silent Co", domain="silent-co.example")
    await session.commit()
    return ContactFixture(rich=rich.id, shortcuts_only=quiet.id, silent=silent.id)


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


def contact_section(markup: str) -> Node:
    """The panel section headed "Contacts" — SPEC §9's contacts block."""
    for section in HTMLParser(markup).css("section.panel-section"):
        heading = section.css_first("h2")
        if heading is not None and heading.text().strip() == "Contacts":
            return section
    raise AssertionError("the rendered panel has no Contacts section")


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


def squashed(node: Node) -> str:
    """A node's visible text with its whitespace collapsed to single spaces."""
    return " ".join(node.text().split())


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
    """
    markup = await detail_html(client, detail_url, contacts.rich)
    published = group_html(contact_groups(markup), PUBLISHED_HEADING)

    assert f'<a href="mailto:{CAREERS_EMAIL}">{CAREERS_EMAIL}</a>' in published
    assert f'<a href="mailto:{HR_EMAIL}">{HR_EMAIL}</a>' in published
    assert (
        f'<a href="{CAREERS_PAGE}" rel="noopener noreferrer" target="_blank">{CAREERS_PAGE}</a>'
    ) in published

    assert 'href="javascript:' not in markup.lower()
    assert f'<span class="muted">{HOSTILE_FORM}</span>' in published


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
