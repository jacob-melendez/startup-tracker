"""``ingest.connectors.company_site`` — the only Tier-3 connector (SPEC §4 Tier 3, §6).

SPEC §13 requires a test that ``robots.txt`` is checked before every Tier-3 fetch; that is
:func:`test_robots_txt_is_checked_before_every_fetch` below. The rest of the first half covers
the other Tier-3 rules — one request per domain per two seconds, five pages per domain per run,
depth 1 only — and the enrichment the connector writes (§12 Phase 3).

The second half is SPEC §6 (§12 Phase 5): the contacts and people ``to_records`` reads off the
pages a visit already fetched. Those assertions run against markup recorded from the real sites,
because every one of them is a trap a tidied-up fixture would hide — a footer that is not a
``<footer>``, an uppercase address, a LinkedIn URL that is a numeric id, a team card whose only
visible text is "Visit our LinkedIn". ``to_records`` is pure, so most of them need no HTTP at
all; the ones that do replay fixtures through ``respx`` and never touch the network.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from datetime import datetime, timedelta

import httpx
import pytest
import respx

from db import enums
from ingest.base import CompanyRecord, CompanyTarget
from ingest.config import RegionsConfig, load_regions_config
from ingest.connectors.company_site import (
    MAX_PAGES_PER_COMPANY,
    CompanySiteConnector,
    CompanySiteOptions,
    SitePage,
    SiteVisit,
)
from ingest.http import HttpClient
from tests.support import (
    NOW,
    FakeClock,
    collect,
    connector_config,
    fixture_text,
    make_client,
    make_ctx,
    make_target,
)

HOME = "https://www.astranis.com/"
CAREERS = "https://www.astranis.com/careers"
ROBOTS = "https://www.astranis.com/robots.txt"

#: Atom Computing is the SPEC §6 site: the three pages a real visit fetches carry the published
#: address, the footer social block (on all three) and the team cards, one trap per page.
AC_HOME = "https://atom-computing.com/"
AC_ABOUT = "https://atom-computing.com/about-us/"
AC_CAREERS = "https://atom-computing.com/careers/"
AC_ROBOTS = "https://atom-computing.com/robots.txt"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


@pytest.fixture(scope="module")
def regions() -> RegionsConfig:
    return load_regions_config()


@pytest.fixture
def connector(regions: RegionsConfig) -> CompanySiteConnector:
    return CompanySiteConnector(connector_config("company_site"), regions)


@pytest.fixture
async def client(clock: FakeClock) -> AsyncIterator[HttpClient]:
    async with make_client(connector_config("company_site"), clock) as http:
        yield http


def mock_astranis(router: respx.MockRouter, *, robots: str | None = None) -> None:
    router.get(ROBOTS).mock(
        httpx.Response(
            200,
            text=robots
            if robots is not None
            else fixture_text("company_site", "astranis_robots.txt"),
        )
    )
    router.get(HOME).mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_home.html"))
    )
    router.get(CAREERS).mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_careers.html"))
    )


def astranis_target(*, last_seen_at: datetime | None = None) -> CompanyTarget:
    return make_target(
        name="Astranis", domain="astranis.com", website_url=HOME, last_seen_at=last_seen_at
    )


def mock_atom_computing(router: respx.MockRouter) -> None:
    """The whole of ``atom-computing.com`` as recorded: its own Yoast ``robots.txt`` and the
    three pages a depth-1 visit reaches — home, ``/about-us/``, ``/careers/``."""
    router.get(AC_ROBOTS).mock(
        httpx.Response(200, text=fixture_text("company_site", "wordpress_crawl_delay_robots.txt"))
    )
    for url, name in (
        (AC_HOME, "atom_computing_home.html"),
        (AC_ABOUT, "atom_computing_about.html"),
        (AC_CAREERS, "atom_computing_careers.html"),
    ):
        router.get(url).mock(httpx.Response(200, html=fixture_text("company_site", name)))


def atom_target() -> CompanyTarget:
    return make_target(2, name="Atom Computing", domain="atom-computing.com", website_url=AC_HOME)


# ------------------------------------------------------------------ SPEC §4 Tier 3 rules


async def test_robots_txt_is_checked_before_every_fetch(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    """SPEC §13: "``robots.txt`` is checked before every Tier-3 fetch, covered by a test.\""""
    robots = router.get(ROBOTS).mock(httpx.Response(200, text="User-agent: *\nAllow: /\n"))
    home = router.get(HOME).mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_home.html"))
    )
    router.get(CAREERS).mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_careers.html"))
    )
    await collect(connector, make_ctx(client, targets=[astranis_target()]))
    assert robots.called
    # robots.txt was requested before the home page, and only once for the origin.
    urls = [str(call.request.url) for call in router.calls]
    assert urls.index(ROBOTS) < urls.index(HOME)
    assert urls.count(ROBOTS) == 1
    assert home.called


async def test_a_disallowed_site_is_never_fetched(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    router.get(ROBOTS).mock(httpx.Response(200, text="User-agent: *\nDisallow: /\n"))
    home = router.get(HOME)
    results = await collect(connector, make_ctx(client, targets=[astranis_target()]))
    assert not home.called
    assert results == []
    assert connector.stats == {"robots_disallowed": 1, "home_unreachable": 1}


async def test_a_disallowed_subpath_only_blocks_that_path(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    mock_astranis(router, robots="User-agent: *\nAllow: /\nDisallow: /careers\n")
    careers = router.get(CAREERS)
    results = await collect(connector, make_ctx(client, targets=[astranis_target()]))
    assert not careers.called
    assert len(results) == 1
    assert results[0].careers_url is None


async def test_one_request_per_domain_per_two_seconds(
    router: respx.MockRouter, connector: CompanySiteConnector, clock: FakeClock
) -> None:
    """SPEC §4 Tier 3: "max 1 request per domain per 2 seconds"."""
    config = connector_config("company_site")
    assert config.rate_limit.requests_per_second == 0.5
    assert config.rate_limit.max_requests_per_host_per_run == 5
    mock_astranis(router)
    async with make_client(config, clock) as http:
        await collect(connector, make_ctx(http, targets=[astranis_target()]))
    assert clock.sleeps and all(sleep == pytest.approx(2.0) for sleep in clock.sleeps)


async def test_never_deeper_than_depth_one(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    """SPEC §4 Tier 3: "never follow deeper than depth 1 from the homepage". The careers page
    links on to more pages; none of them is fetched."""
    mock_astranis(router)
    deeper = router.get("https://www.astranis.com/careers/engineering")
    results = await collect(connector, make_ctx(client, targets=[astranis_target()]))
    assert not deeper.called
    assert [page.url for page in results[0].pages] == [HOME, CAREERS]


def test_depth_one_links_stay_on_the_company_domain(
    connector: CompanySiteConnector,
) -> None:
    """A careers page hosted on the ATS is that ATS's site; fetching it here would spend a
    Tier-3 budget on a Tier-1 API."""
    home = SitePage(
        HOME,
        """<html><body>
        <a href="/careers">Careers</a>
        <a href="https://job-boards.greenhouse.io/astranis">Open roles</a>
        <a href="https://blog.example.com/about">About us elsewhere</a>
        <a href="mailto:jobs@astranis.com">Email</a>
        <a href="/pricing">Pricing</a>
        <a href="/careers#open">Careers again</a>
        </body></html>""",
    )
    assert connector._depth_one_urls(home) == ["https://www.astranis.com/careers"]


def test_the_page_budget_is_the_home_page_plus_the_four_paths_spec_4_names() -> None:
    """SPEC §4 Tier 3 lists four depth-1 paths and caps a domain at five pages per run, and the
    two numbers are the same number: home plus one page each for ``/careers``, ``/jobs``,
    ``/about`` and ``/team``. A smaller budget here would pick which of the four to skip by the
    home page's link order, and SPEC §6's material is not interchangeable between them — the
    published address is on the careers page and the ``Person`` rows are on the team page."""
    assert MAX_PAGES_PER_COMPANY == 5
    config = connector_config("company_site")
    paths = CompanySiteOptions.model_validate(config.options).depth_one_paths
    assert len(paths) + 1 == MAX_PAGES_PER_COMPANY
    # ... and the client's own per-host budget is the SPEC §4 Tier 3 cap of five.
    assert config.rate_limit.max_requests_per_host_per_run == 5


def test_one_depth_one_page_per_section_rather_than_per_link(
    connector: CompanySiteConnector,
) -> None:
    """The budget is four pages and SPEC §4 names four *sections*, so the same section linked
    three ways must cost one page, not three.

    atom-computing.com's home page really does link ``/careers/``, ``/careers/`` again and
    ``/careers/employee-spotlights/``; a nav that also writes ``/about`` and ``/about/`` would
    otherwise spend both of the old budget's slots inside one section. The shallowest path wins,
    which is the section's own page rather than an article inside it, and the choice does not
    depend on which of them the page happens to print first.
    """
    home = SitePage(
        HOME,
        """<html><body>
        <a href="/careers/employee-spotlights/">Spotlights</a>
        <a href="/about/">About</a>
        <a href="/about">About</a>
        <a href="/careers/">Careers</a>
        <a href="/team">Team</a>
        <a href="/jobs">Jobs</a>
        </body></html>""",
    )
    assert connector._depth_one_urls(home) == [
        "https://www.astranis.com/careers/",
        "https://www.astranis.com/about",
        "https://www.astranis.com/team",
        "https://www.astranis.com/jobs",
    ]


def test_an_href_that_is_not_a_url_does_not_end_the_run(
    connector: CompanySiteConnector,
) -> None:
    """``urljoin`` raises on an unbalanced bracket in the authority, and an unrendered
    ``[[siteUrl]]`` template is one. Raising here escapes ``fetch``, which fails the whole run:
    every company behind this one in the least-recently-visited queue goes unvisited, and this
    company's ``last_seen_at`` is never written, so it heads the queue and kills the next run
    too. The bad href costs its own anchor and nothing else."""
    home = SitePage(
        HOME,
        """<html><body>
        <a href="https://[[siteUrl]]/contact">Contact</a>
        <a href="https://exam]ple.com/careers">Jobs elsewhere</a>
        <a href="/careers">Careers</a>
        </body></html>""",
    )
    assert connector._depth_one_urls(home) == ["https://www.astranis.com/careers"]


async def test_the_per_run_company_cap_is_honoured(
    router: respx.MockRouter, regions: RegionsConfig, client: HttpClient
) -> None:
    mock_astranis(router)
    router.get("https://b.example/robots.txt").mock(httpx.Response(404))
    router.get("https://b.example/").mock(httpx.Response(200, html="<html></html>"))
    connector = CompanySiteConnector(
        connector_config("company_site", max_companies_per_run=1), regions
    )
    targets = [astranis_target(), make_target(2, name="B", domain="b.example")]
    results = await collect(connector, make_ctx(client, targets=targets))
    assert len(results) == 1


async def test_a_recently_visited_company_is_skipped(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    """The targets arrive oldest-first, so the first fresh one ends the run."""
    target = astranis_target(last_seen_at=NOW - timedelta(days=1))
    assert await collect(connector, make_ctx(client, targets=[target])) == []
    assert client.stats.requests == 0
    assert connector.stats == {"visited_recently": 1}


async def test_a_company_with_no_website_is_skipped(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    target = make_target(name="No site", domain=None, website_url=None)
    assert await collect(connector, make_ctx(client, targets=[target])) == []
    assert connector.stats == {"no_website": 1}


async def test_an_unreachable_home_page_ends_that_company_quietly(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    router.get(ROBOTS).mock(httpx.Response(404))
    router.get(HOME).mock(httpx.Response(500))
    ctx = make_ctx(client, targets=[astranis_target()])
    assert await collect(connector, ctx) == []
    assert connector.stats["home_unreachable"] == 1
    # A Tier-3 site may simply decline; that is not a run-level problem (SPEC §4).
    assert ctx.problems == []


async def test_a_non_html_response_is_ignored(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    router.get(ROBOTS).mock(httpx.Response(404))
    router.get(HOME).mock(httpx.Response(200, json={"not": "html"}))
    assert await collect(connector, make_ctx(client, targets=[astranis_target()])) == []
    assert connector.stats["not_html"] == 1


# ------------------------------------------------------------------ enrichment


async def test_a_visit_enriches_the_company(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    mock_astranis(router)
    results = await collect(connector, make_ctx(client, targets=[astranis_target()]))
    record = one(connector, results[0])
    assert record.name == "Astranis"
    assert record.domain == "astranis.com"
    assert record.website_url == HOME
    assert record.one_liner and record.one_liner.startswith("Astranis designs, builds")
    # SPEC §4 Tier 3: the company published its own careers URL, and it leads the contacts.
    # The rest of what this site publishes is asserted in the SPEC §6 half of this module.
    assert (record.contacts[0].kind, record.contacts[0].value, record.contacts[0].confidence) == (
        enums.ContactKind.CAREERS_PAGE,
        CAREERS,
        enums.ContactConfidence.PUBLISHED,
    )
    # It only ever enriches: a redirect that landed somewhere odd must not create a company.
    assert record.enrich_only is True


def one(connector: CompanySiteConnector, visit: SiteVisit) -> CompanyRecord:
    records = list(connector.to_records(visit))
    assert len(records) == 1
    return records[0]


def test_the_one_liner_prefers_og_description(connector: CompanySiteConnector) -> None:
    home = SitePage(
        "https://x.example/",
        """<html><head><title>X | Brand</title>
        <meta name="description" content="Plain description."/>
        <meta property="og:description" content="Open Graph description."/>
        </head><body></body></html>""",
    )
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert record.one_liner == "Open Graph description."


def test_a_site_with_no_description_falls_back_to_its_title(
    connector: CompanySiteConnector,
) -> None:
    home = SitePage("https://x.example/", "<html><head><title>Widgets | Acme</title></head></html>")
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert record.one_liner == "Widgets"


def test_a_bare_brand_title_is_kept_whole(connector: CompanySiteConnector) -> None:
    home = SitePage("https://x.example/", "<html><head><title>Sourcegraph</title></head></html>")
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert record.one_liner == "Sourcegraph"


def test_a_long_description_fills_thesis_and_a_shortened_one_liner(
    connector: CompanySiteConnector,
) -> None:
    long_text = "We build things. " * 40
    home = SitePage(
        "https://x.example/",
        f'<html><head><meta name="description" content="{long_text}"/></head></html>',
    )
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert record.one_liner is not None and len(record.one_liner) <= 301
    assert record.one_liner.endswith("…")
    assert record.thesis is not None and len(record.thesis) > len(record.one_liner)


def test_a_short_description_does_not_duplicate_itself_into_thesis(
    connector: CompanySiteConnector,
) -> None:
    home = SitePage(
        "https://x.example/",
        '<html><head><meta name="description" content="Short."/></head></html>',
    )
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert (record.one_liner, record.thesis) == ("Short.", None)


def test_a_real_recorded_home_page_maps(connector: CompanySiteConnector) -> None:
    home = SitePage(
        "https://sourcegraph.com/", fixture_text("company_site", "sourcegraph_home.html")
    )
    record = one(
        connector,
        SiteVisit(
            target=make_target(name="Sourcegraph", domain="sourcegraph.com"),
            home=home,
            pages=(home,),
            careers_url="https://sourcegraph.com/jobs",
        ),
    )
    assert record.one_liner is not None and "complete context" in record.one_liner
    assert record.contacts[0].value == "https://sourcegraph.com/jobs"


# ------------------------------------------------------- SPEC §6 contacts, §5 Person


def recorded(name: str, url: str) -> SitePage:
    """One recorded page, exactly as :meth:`CompanySiteConnector._page` would have built it."""
    return SitePage(url, fixture_text("company_site", name))


def recorded_visit(
    target: CompanyTarget, *pages: SitePage, careers_url: str | None = None
) -> SiteVisit:
    """A visit over already-recorded pages. ``to_records`` is pure, so the extraction rules can
    be asserted from real markup without spending a request."""
    return SiteVisit(target=target, home=pages[0], pages=pages, careers_url=careers_url)


def astranis_visit() -> SiteVisit:
    return recorded_visit(
        astranis_target(),
        recorded("astranis_home.html", HOME),
        recorded("astranis_careers.html", CAREERS),
        careers_url=CAREERS,
    )


def sourcegraph_visit() -> SiteVisit:
    home = recorded("sourcegraph_home.html", "https://sourcegraph.com/")
    return recorded_visit(make_target(3, name="Sourcegraph", domain="sourcegraph.com"), home)


def atom_computing_visit() -> SiteVisit:
    return recorded_visit(
        atom_target(),
        recorded("atom_computing_home.html", AC_HOME),
        recorded("atom_computing_about.html", AC_ABOUT),
        recorded("atom_computing_careers.html", AC_CAREERS),
        careers_url=AC_CAREERS,
    )


def twelve_visit() -> SiteVisit:
    home = recorded("twelve_home.html", "https://twelve.co/")
    return recorded_visit(make_target(4, name="Twelve", domain="twelve.co"), home)


def synthetic_visit(markup: str) -> SiteVisit:
    """A hand-written page served from the Astranis home URL, for the few shapes no recorded
    fixture happens to carry."""
    home = SitePage(HOME, markup)
    return SiteVisit(target=astranis_target(), home=home, pages=(home,), careers_url=None)


def multi_page_visit(
    target: CompanyTarget, *pages: tuple[str, str], careers_url: str | None = None
) -> SiteVisit:
    """A hand-written visit over several ``(url, markup)`` pages, home first.

    The rules that read a *page* can be asserted from one; the ones that have to weigh a whole
    visit — which account is the company's own, which of two contact pages to keep, one person
    linked from two pages — cannot, and no recorded fixture carries those shapes.
    """
    built = tuple(SitePage(url, markup) for url, markup in pages)
    return SiteVisit(target=target, home=built[0], pages=built, careers_url=careers_url)


def pairs(record: CompanyRecord) -> list[tuple[enums.ContactKind, str]]:
    """``(kind, value)`` per contact, in the order the connector emitted them."""
    return [(contact.kind, contact.value) for contact in record.contacts]


def test_the_footer_social_links_are_stored_in_canonical_form(
    connector: CompanySiteConnector,
) -> None:
    """SPEC §4 Tier 3 "footer social links", SPEC §6 published values.

    Everything about this list is load-bearing. Astranis is a Webflow site with no ``<footer>``
    element at all, so these came from a plain ``<div class="social-link footernew">``; the
    ``/contact`` link is the nav's "Talk to Sales" button, which is why the contact form sorts
    before the social accounts; ``twitter.com/Astranis`` is stored under the host the link now
    lands on, so one account can never become two rows; the LinkedIn href's trailing slash is
    dropped, which is what lets the pipeline recognise the constructed URL for ``astranis.com``
    as this same row; and Instagram and YouTube are not SPEC §5 contact kinds, so they are
    dropped rather than filed under something approximate.
    """
    record = one(connector, astranis_visit())
    assert pairs(record) == [
        (enums.ContactKind.CAREERS_PAGE, CAREERS),
        (enums.ContactKind.CONTACT_FORM, "https://www.astranis.com/contact"),
        (enums.ContactKind.X, "https://x.com/Astranis"),
        (enums.ContactKind.LINKEDIN_COMPANY, "https://www.linkedin.com/company/astranis"),
    ]


def test_a_published_linkedin_url_may_be_a_numeric_company_id(
    connector: CompanySiteConnector,
) -> None:
    """SPEC §6 C1: what the footer links to is what is stored.

    ``sourcegraph.com`` links to ``/company/4803356``. It is nobody's guess and it is not the
    slug SPEC §6 constructs from the domain (``…/company/sourcegraph``), so the published row
    and the constructed one genuinely differ — the reconciliation case. Each social anchor's
    only child is an ``<svg>``, so the account can only have come from the ``href``.

    The page also carries both halves of the contact-form rule — the nav's
    ``/contact/request-info`` demo CTA and the footer's ``/contact`` — and only the shallower
    one is stored.
    """
    record = one(connector, sourcegraph_visit())
    assert pairs(record) == [
        (enums.ContactKind.CONTACT_FORM, "https://sourcegraph.com/contact"),
        (enums.ContactKind.GITHUB, "https://github.com/sourcegraph"),
        (enums.ContactKind.X, "https://x.com/Sourcegraph"),
        (enums.ContactKind.LINKEDIN_COMPANY, "https://www.linkedin.com/company/4803356"),
    ]


def test_a_query_string_and_a_trailing_slash_are_dropped_from_a_published_url(
    connector: CompanySiteConnector,
) -> None:
    """``twelve.co``'s footer link is ``…/company/twelveco2/?viewAsMember=true``, and it is the
    only social account the page yields: the Wix bug-tracker URL in its stylesheet is not an
    anchor, YouTube and Instagram are not SPEC §5 contact kinds. The published slug is also one
    the domain could never have produced (``twelve.co`` would construct ``…/company/twelve``).
    The second footer column's ``/contact`` follows it, in document order."""
    visit = twelve_visit()
    assert visit.home is not None and "github.com/wix/yoshi" in visit.home.markup
    record = one(connector, visit)
    assert pairs(record) == [
        (enums.ContactKind.LINKEDIN_COMPANY, "https://www.linkedin.com/company/twelveco2"),
        (enums.ContactKind.CONTACT_FORM, "https://www.twelve.co/contact"),
    ]


def test_a_url_that_is_not_an_anchor_is_never_a_contact(connector: CompanySiteConnector) -> None:
    """SPEC §6: a contact is a link the company published, not a string in its page.

    Bundled CSS and JS are full of other people's URLs — ``twelve.co`` really does ship a
    stylesheet citing a Wix bug tracker. Read as text, ``github.com/wix`` is a well-formed org
    link and ``x.com/wix`` a well-formed handle, so only reading ``<a href>`` values keeps
    somebody else's repository out of this company's contacts block. The same rule is what
    leaves an address printed as prose alone: SPEC §6 stores a ``mailto:`` the company chose
    to publish, not every string on the page shaped like an address.
    """
    record = one(
        connector,
        synthetic_visit(
            """<html><head>
            <style>/*! remove when https://github.com/wix is resolved */</style>
            </head><body>
            <script>var cdn = "https://x.com/wix";</script>
            <p>Write to us at hello@astranis.example</p>
            </body></html>"""
        ),
    )
    assert pairs(record) == []


def test_a_published_mailto_is_stored_once_and_lowercased(
    connector: CompanySiteConnector,
) -> None:
    """SPEC §6 C4 e-mails, and the cross-page dedupe that keeps the block readable.

    The only address on the whole site is ``mailto:HR@atom-computing.com`` on the careers page —
    an uppercase local part. It is stored lowercased because ``hn_hiring`` writes the same
    address in lowercase and ``uq_contacts_company_id_kind_value`` would otherwise hold both.
    The footer social block repeats on all three pages and still yields one row per account,
    and the Twitter href's ``?lang=en`` is dropped before the handle is canonicalised.
    """
    record = one(connector, atom_computing_visit())
    assert pairs(record) == [
        (enums.ContactKind.CAREERS_PAGE, AC_CAREERS),
        (enums.ContactKind.EMAIL, "hr@atom-computing.com"),
        (enums.ContactKind.CONTACT_FORM, "https://atom-computing.com/contact-us"),
        (enums.ContactKind.X, "https://x.com/atom_computing"),
        (enums.ContactKind.LINKEDIN_COMPANY, "https://www.linkedin.com/company/atom-computing"),
    ]


def test_a_share_widget_mailto_is_not_an_address(connector: CompanySiteConnector) -> None:
    """SPEC §6 C4: only an address the company published. ``mailto:?subject=…`` is a share
    button with no address in it at all, and a real one keeps nothing but the address."""
    record = one(
        connector,
        synthetic_visit(
            """<html><body>
            <a href="mailto:?subject=Astranis&amp;body=Look%20at%20this">Share by email</a>
            <a href="mailto:Jobs@Astranis.com?subject=Application">Jobs</a>
            </body></html>"""
        ),
    )
    assert pairs(record) == [(enums.ContactKind.EMAIL, "jobs@astranis.com")]


def test_published_emails_are_capped_per_company(regions: RegionsConfig) -> None:
    """``options.max_emails_per_company``: a site that prints a regional alias on every page
    must not turn the panel's contacts block into a directory. The cap keeps the addresses in
    document order, so the home and careers pages' addresses are the ones that survive."""
    connector = CompanySiteConnector(
        connector_config("company_site", max_emails_per_company=2), regions
    )
    record = one(
        connector,
        synthetic_visit(
            """<html><body>
            <a href="mailto:jobs@astranis.com">Jobs</a>
            <a href="mailto:press@astranis.com">Press</a>
            <a href="mailto:sales@astranis.com">Sales</a>
            </body></html>"""
        ),
    )
    assert pairs(record) == [
        (enums.ContactKind.EMAIL, "jobs@astranis.com"),
        (enums.ContactKind.EMAIL, "press@astranis.com"),
    ]


@pytest.mark.parametrize(
    "build",
    [astranis_visit, sourcegraph_visit, atom_computing_visit, twelve_visit],
    ids=["astranis", "sourcegraph", "atom-computing", "twelve"],
)
def test_every_contact_a_visit_produces_is_published(
    connector: CompanySiteConnector, build: Callable[[], SiteVisit]
) -> None:
    """SPEC §6: this connector reads pages the company controls, so every value it can return
    is one the company chose to publish. ``constructed`` belongs to the pipeline, which builds
    those links on every company upsert rather than only for the visited ones."""
    record = one(connector, build())
    assert record.contacts, "each of these sites publishes at least one contact"
    assert {contact.confidence for contact in record.contacts} == {
        enums.ContactConfidence.PUBLISHED
    }
    assert enums.ContactKind.LINKEDIN_PEOPLE not in {c.kind for c in record.contacts}


def test_a_site_that_links_to_no_linkedin_gets_no_linkedin_contact(
    connector: CompanySiteConnector,
) -> None:
    """SPEC §6 C2's "otherwise" is the pipeline's, not the connector's.

    The target here has a domain, so a connector that constructed anything would emit
    ``…/company/astranis`` and a people-search row for this page. Construction belongs to every
    upsert, including of companies a Tier-3 visit has never reached, so it happens once in
    :mod:`ingest.pipeline` instead.
    """
    record = one(
        connector,
        synthetic_visit('<html><body><a href="/careers">Careers</a></body></html>'),
    )
    assert record.domain == "astranis.com"
    assert pairs(record) == []


ACME = "https://acme-robotics.com/"
ACME_ABOUT = "https://acme-robotics.com/about"
#: An "Our investors" block, the ordinary furniture of a startup's about page. Every href in it
#: is a well-formed account on a host SPEC §4 Tier 3 lists.
INVESTORS = (
    "<html><body><h2>Our investors</h2><p>Backed by "
    '<a href="https://www.linkedin.com/company/andreessen-horowitz">a16z</a> and '
    '<a href="https://www.linkedin.com/company/y-combinator">Y Combinator</a>. '
    'We build on <a href="https://github.com/kubernetes/kubernetes">Kubernetes</a> and post '
    'from <a href="https://x.com/ycombinator">the batch account</a>.</p></body></html>'
)


def acme_target() -> CompanyTarget:
    return make_target(5, name="Acme Robotics", domain="acme-robotics.com", website_url=ACME)


def test_only_the_companys_own_accounts_are_published(
    connector: CompanySiteConnector,
) -> None:
    """SPEC §6 C1 stores the page the company publishes as *its own*, not every organisation
    page it links to.

    The company's footer link is on the home page and the investors' are on ``/about``, which
    is why this is a whole-visit question: read page by page, ``/about`` offers two LinkedIn
    company pages and neither is Acme's. Publishing one of them would put a fund in the panel's
    "Published by the company" list — and, because :mod:`ingest.pipeline` reads any published
    row of that kind as "the company's LinkedIn is known", would delete the constructed link to
    Acme itself. The same over-acceptance files a third party's GitHub org and X handle as ways
    to reach this company; nothing is deleted there, only wrong.
    """
    record = one(
        connector,
        multi_page_visit(
            acme_target(),
            (
                ACME,
                '<html><body><footer><a href="https://www.linkedin.com/company/acme-robotics/">'
                'LinkedIn</a><a href="https://x.com/acmerobotics">X</a>'
                '<a href="https://github.com/acme-robotics">GitHub</a></footer></body></html>',
            ),
            (ACME_ABOUT, INVESTORS),
        ),
    )
    assert pairs(record) == [
        (enums.ContactKind.LINKEDIN_COMPANY, "https://www.linkedin.com/company/acme-robotics"),
        (enums.ContactKind.X, "https://x.com/acmerobotics"),
        (enums.ContactKind.GITHUB, "https://github.com/acme-robotics"),
    ]


def test_a_visit_that_only_finds_other_companies_accounts_publishes_none(
    connector: CompanySiteConnector,
) -> None:
    """SPEC §6's "otherwise" branch, reached deliberately.

    With no link of its own anywhere in the visit, this connector must emit no
    ``linkedin_company`` row at all, so the pipeline constructs
    ``…/company/acme-robotics`` — a link to the right company, labelled ``constructed`` in the
    UI. Publishing a fund's page instead is worse than the search shortcut in every way: it is
    wrong, it claims to be verified, and it is never rebuilt once the deletion has happened.

    Two of them is what makes this decidable. A page whose *only* link of a kind is somebody
    else's is indistinguishable from one whose company writes its slug differently from its
    domain — ``twelve.co`` publishes ``…/company/twelveco2`` and ``sourcegraph.com`` a numeric
    id — so a lone candidate is still taken (see
    :func:`ingest.contacts.select_social_contacts`).
    """
    record = one(
        connector,
        multi_page_visit(
            acme_target(),
            (ACME, '<html><body><a href="/about">About</a></body></html>'),
            (
                ACME_ABOUT,
                "<html><body><h2>Our investors</h2><p>Backed by "
                '<a href="https://www.linkedin.com/company/andreessen-horowitz">a16z</a> and '
                '<a href="https://www.linkedin.com/company/y-combinator">YC</a>.</p>'
                "</body></html>",
            ),
        ),
    )
    assert pairs(record) == []


def test_one_contact_form_per_visit_and_the_home_pages_wins(
    connector: CompanySiteConnector,
) -> None:
    """A site has one contact page. ``extract_social_contacts`` keeps the shallowest URL *on a
    page*; across pages the first one the visit fetched wins, and ``pages[0]`` is the home page.

    Both halves matter and they can disagree: in the second visit below the home page's contact
    URL is the deeper of the two, and it still wins, because the home page is the one that says
    how a company would rather be contacted. Without the guard both rows are stored and the
    panel offers two competing "Contact form" links for one company.
    """
    record = one(
        connector,
        multi_page_visit(
            acme_target(),
            (ACME, '<html><body><a href="/contact">Contact</a></body></html>'),
            (
                "https://acme-robotics.com/careers",
                '<html><body><a href="/careers/contact-recruiting">Contact recruiting</a>'
                "</body></html>",
            ),
        ),
    )
    assert pairs(record) == [(enums.ContactKind.CONTACT_FORM, "https://acme-robotics.com/contact")]
    deeper = one(
        connector,
        multi_page_visit(
            acme_target(),
            (ACME, '<html><body><a href="/support/contact-sales">Talk to sales</a></body></html>'),
            (
                "https://acme-robotics.com/careers",
                '<html><body><a href="/contact">Contact</a></body></html>',
            ),
        ),
    )
    assert pairs(deeper) == [
        (enums.ContactKind.CONTACT_FORM, "https://acme-robotics.com/support/contact-sales")
    ]


def _person_card(name: str, title: str, url: str) -> str:
    return (
        f'<html><body><div class="card"><h3>{name}</h3><p>{title}</p>'
        f'<a href="{url}">LinkedIn</a></div></body></html>'
    )


def test_one_person_linked_from_two_pages_is_one_record(
    connector: CompanySiteConnector,
) -> None:
    """A founder is linked from the home page and again from ``/about``.

    :func:`ingest.contacts.extract_people` dedupes within a page, so both halves of this guard
    only ever fire across pages. The pipeline matches a person on
    ``(company_id, lower(full_name))`` and flushes each one before looking up the next, so two
    records for one person are an insert immediately followed by an update of the row just
    written — every week, with the winner decided by page order. The profile URL differs in the
    second visit below (a slug the page builder wrote two ways), which is the case the name
    half of the dedupe exists for.
    """
    same_url = one(
        connector,
        multi_page_visit(
            acme_target(),
            (ACME, _person_card("Ada Byron", "CTO", "https://www.linkedin.com/in/ada-byron")),
            (
                ACME_ABOUT,
                _person_card("Ada Byron", "CTO", "https://www.linkedin.com/in/ada-byron/"),
            ),
        ),
    )
    assert [(p.full_name, p.linkedin_url) for p in same_url.people] == [
        ("Ada Byron", "https://www.linkedin.com/in/ada-byron")
    ]
    two_slugs = one(
        connector,
        multi_page_visit(
            acme_target(),
            (ACME, _person_card("Ada Byron", "CTO", "https://www.linkedin.com/in/ada-byron")),
            (
                ACME_ABOUT,
                _person_card("Ada Byron", "CTO", "https://www.linkedin.com/in/ada-byron-12345"),
            ),
        ),
    )
    assert [(p.full_name, p.linkedin_url) for p in two_slugs.people] == [
        ("Ada Byron", "https://www.linkedin.com/in/ada-byron")
    ]
    assert len({p.full_name.casefold() for p in two_slugs.people}) == len(two_slugs.people)


def test_a_personal_profile_link_is_never_a_contact(connector: CompanySiteConnector) -> None:
    """SPEC §5: ``linkedin.com/in/<slug>`` is a :class:`Person`, never a company ``Contact``.
    Atom Computing's team cards publish three of them beside the footer's company link."""
    record = one(connector, atom_computing_visit())
    assert [p.linkedin_url for p in record.people] == [
        "https://www.linkedin.com/in/ben-bloom-8871598",
        "https://www.linkedin.com/in/kingjonathanp",
        "https://www.linkedin.com/in/sarah-murrow-1853248",
    ]
    assert not [c for c in record.contacts if "/in/" in c.value]


def test_people_come_from_the_published_team_page(connector: CompanySiteConnector) -> None:
    """SPEC §5 ``Person`` and SPEC §6's "no profile data is ever fetched".

    Every card on this page uses the same anchor text — "Visit our LinkedIn", from an
    ``<svg><title>`` — so a name can only have come from the card's own ``<h3>``. The stored
    name drops the published credential ("Ben Bloom, PhD"); the title is the next sibling
    ``<div>``, not the "Read Bio" anchor after it, and it is stored without the zero-width
    space and the ``&nbsp;``/``<br>`` the page builder left in it; ``role_type`` comes from
    ``config/classifiers.yaml`` (founder before exec, so "CEO & Founder" is a founder); and
    ``linkedin_url`` is only ever the URL the company itself printed next to the name.
    """
    visit = atom_computing_visit()
    # The furniture the real page wraps its cards in: one section heading in reach of all
    # three of them, and the same anchor text on every card. Neither is anybody's name.
    assert "Executive Leadership" in visit.pages[1].markup
    assert visit.pages[1].markup.count("Visit our LinkedIn") >= 3
    record = one(connector, visit)
    assert [(p.full_name, p.title, p.role_type, p.linkedin_url) for p in record.people] == [
        (
            "Ben Bloom",
            "CEO & Founder",
            enums.RoleType.FOUNDER,
            "https://www.linkedin.com/in/ben-bloom-8871598",
        ),
        (
            "Jonathan King",
            "Chief Scientist & Co-Founder",
            enums.RoleType.FOUNDER,
            "https://www.linkedin.com/in/kingjonathanp",
        ),
        (
            "Sarah Murrow",
            "VP, Human Resources",
            enums.RoleType.RECRUITER,
            "https://www.linkedin.com/in/sarah-murrow-1853248",
        ),
    ]


def _team_section(*slugs: str) -> str:
    """A team section whose cards carry no name of their own, so each card's name can only
    come from the section heading — Atom Computing's page shape, with a heading that reads
    like a person's name instead of one the name test rejects on sight."""
    cards = "".join(
        f'<div class="card"><a href="https://www.linkedin.com/in/{slug}/">'
        "Visit our LinkedIn</a></div>"
        for slug in slugs
    )
    return (
        "<html><body><section><h2>Ada Lovelace</h2>"
        f'<p class="role">Head of Engineering</p>{cards}</section></body></html>'
    )


def test_a_name_two_profiles_claim_is_a_heading_not_a_person(
    connector: CompanySiteConnector,
) -> None:
    """One name, two profile URLs, on one page: that is a section heading, and every person it
    produced is dropped.

    The two halves differ by one card and nothing else — same heading, same anchor text, same
    surrounding markup. A single card under a heading is a person whose name heads their own
    card, which is a real shape; a second card claiming that same name proves the heading
    belongs to the section rather than to anyone, and silently keeping either of them would
    put a person named after a page section into the database.
    """
    alone = one(connector, synthetic_visit(_team_section("ada")))
    assert [(p.full_name, p.title, p.role_type) for p in alone.people] == [
        ("Ada Lovelace", "Head of Engineering", enums.RoleType.ENG_LEAD)
    ]
    both = one(connector, synthetic_visit(_team_section("ada", "grace")))
    assert both.people == ()


def test_people_are_capped_per_company(regions: RegionsConfig) -> None:
    """``options.max_people_per_company``: an "our team" page can list several hundred, and the
    leadership sits at the top of it. ``0`` is the kill switch and stores nobody."""
    capped = CompanySiteConnector(
        connector_config("company_site", max_people_per_company=1), regions
    )
    assert [p.full_name for p in one(capped, atom_computing_visit()).people] == ["Ben Bloom"]
    off = CompanySiteConnector(connector_config("company_site", max_people_per_company=0), regions)
    assert one(off, atom_computing_visit()).people == ()


# ------------------------------------------------------- SPEC §6 over a real fetch


async def visit_atom_computing(connector: CompanySiteConnector) -> CompanyRecord:
    """One complete run against the mocked site. Each run gets a fresh client, because a
    later run starts with its own per-host budget and an empty 24 h cache."""
    async with make_client(connector_config("company_site"), FakeClock()) as http:
        results = await collect(connector, make_ctx(http, targets=[atom_target()]))
    assert len(results) == 1
    return one(connector, results[0])


async def test_contacts_are_read_from_every_page_the_visit_fetched(
    router: respx.MockRouter, connector: CompanySiteConnector
) -> None:
    """This site offers three pages within the Tier-3 budget, and SPEC §6's material is spread
    across all of them: the address is only on ``/careers/``, the people only on ``/about-us/``,
    the careers URL only discoverable from the home page's nav. One record carries all three.

    The home page also links ``/careers/employee-spotlights/``, an article *inside* the careers
    section; one page per section is what keeps it out of the fetch list.
    """
    mock_atom_computing(router)
    record = await visit_atom_computing(connector)
    assert [str(call.request.url) for call in router.calls] == [
        AC_ROBOTS,
        AC_HOME,
        AC_ABOUT,
        AC_CAREERS,
    ]
    assert (enums.ContactKind.EMAIL, "hr@atom-computing.com") in pairs(record)
    assert [p.full_name for p in record.people] == ["Ben Bloom", "Jonathan King", "Sarah Murrow"]
    assert (enums.ContactKind.CAREERS_PAGE, AC_CAREERS) in pairs(record)


FOUR_PATHS_HOME = "https://four.example/"
#: A nav that links all four of SPEC §4 Tier 3's paths, in an order that puts the two carrying
#: SPEC §6's material last. Which two a smaller budget would reach is decided here, by the
#: page's link order, and not by what the pages hold.
FOUR_PATHS_NAV = (
    "<html><head><title>Four | Example</title></head><body><nav>"
    '<a href="/about">About</a><a href="/jobs">Jobs</a>'
    '<a href="/careers">Careers</a><a href="/team">Team</a>'
    "</nav></body></html>"
)


def mock_four_paths(router: respx.MockRouter) -> None:
    """A site that publishes something different on each of SPEC §4 Tier 3's four paths."""
    router.get("https://four.example/robots.txt").mock(httpx.Response(404))
    router.get(FOUR_PATHS_HOME).mock(httpx.Response(200, html=FOUR_PATHS_NAV))
    for path, body in (
        ("about", "<p>We build things.</p>"),
        ("jobs", '<a href="https://job-boards.greenhouse.io/four">Open roles</a>'),
        # SPEC §6's own example of a published address: "a jobs alias on their careers page".
        ("careers", '<a href="mailto:Jobs@Four.example">Email the team</a>'),
        (
            "team",
            '<div class="card"><h3>Jane Doe</h3><p>Head of Talent</p>'
            '<a href="https://www.linkedin.com/in/janedoe">LinkedIn</a></div>',
        ),
    ):
        router.get(f"https://four.example/{path}").mock(
            httpx.Response(200, html=f"<html><body>{body}</body></html>")
        )


async def test_all_four_of_spec_4s_depth_one_paths_are_fetched(
    router: respx.MockRouter, connector: CompanySiteConnector
) -> None:
    """SPEC §4 Tier 3 names ``/careers``, ``/jobs``, ``/about`` and ``/team`` and caps the
    domain at five pages per run — home plus those four, exactly.

    A smaller per-company budget does not fetch "some of them", it fetches whichever the home
    page's nav happens to link first, permanently: the same two win every weekly run. Here that
    would be ``/about`` and ``/jobs``, and the two pages left behind are the only ones carrying
    anything — SPEC §6's published address is on ``/careers`` and the only profile link, which
    is this connector's sole source of ``Person`` rows, is on ``/team``.
    """
    mock_four_paths(router)
    target = make_target(6, name="Four", domain="four.example", website_url=FOUR_PATHS_HOME)
    async with make_client(connector_config("company_site"), FakeClock()) as http:
        ctx = make_ctx(http, targets=[target])
        results = await collect(connector, ctx)
    assert [str(call.request.url) for call in router.calls][1:] == [
        FOUR_PATHS_HOME,
        "https://four.example/about",
        "https://four.example/jobs",
        "https://four.example/careers",
        "https://four.example/team",
    ]
    # Five pages is also the client's per-host budget, so nothing was refused by it.
    assert "budget_exceeded" not in connector.stats
    record = one(connector, results[0])
    assert (enums.ContactKind.EMAIL, "jobs@four.example") in pairs(record)
    assert [(p.full_name, p.title, p.linkedin_url) for p in record.people] == [
        ("Jane Doe", "Head of Talent", "https://www.linkedin.com/in/janedoe")
    ]


async def test_a_second_identical_visit_produces_the_same_records(
    router: respx.MockRouter, connector: CompanySiteConnector
) -> None:
    """A weekly re-run of an unchanged site must produce byte-identical records.

    Contacts are upserted on ``(company_id, kind, value)`` and people are matched on
    ``(company_id, lower(full_name))``, so an extraction whose order or dedupe wobbled between
    runs would rewrite rows that did not change — and, within one run, two records for the
    same footer link would be an insert followed by an update of the row just inserted.
    """
    mock_atom_computing(router)
    first = await visit_atom_computing(connector)
    second = await visit_atom_computing(connector)
    assert first == second
    assert len(set(pairs(first))) == len(first.contacts)
    assert len({p.linkedin_url for p in first.people}) == len(first.people)
    assert len({p.full_name for p in first.people}) == len(first.people)
