"""The expanded row's own controls, and the one cursor key that used to reach asyncpg.

SPEC §9 describes the panel's roles table as "title, role family, employment type, seniority,
location, posted date, link — **sortable and filterable within the row**, showing *all* roles
regardless of the page-level role filter, with matching ones highlighted". Those two halves
pull in opposite directions, and every test here is about the line between them:

* the page's role filter may only *mark* a row (§7.1, "classification never excludes"), so the
  panel the collapsed row lazy-loads must list every open role — the ``role_*`` parameters are
  absent from the URL it builds, and that is asserted here, not assumed;
* the row's own header links and filter box may genuinely reorder and shorten that table,
  because the user operated them, and they must survive a round trip through the URL so that
  the permalink shows the same table with JavaScript switched off.

The cursor tests at the end belong to the same file only in that they are the other thing a
hand-edited URL can do: a ``k`` carrying a NUL or a lone surrogate used to reach the driver and
answer 500 where every other malformed token answers with the 400 page.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.queries import Cursor, CursorError
from tests.support_web import (
    DAY,
    NOW,
    app_client,
    job_ids,
    make_company,
    make_job,
    refresh_denormalized,
)

#: Every ``<th aria-sort=...>`` of a rendered roles table, in column order.
_ARIA_SORT = re.compile(r'<th scope="col" aria-sort="([a-z]+)"')


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
