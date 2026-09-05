"""``ingest.contacts`` — every SPEC §6 URL rule, as pure functions (SPEC §6, §4 Tier 3).

SPEC §6 is what this project does *instead* of scraping LinkedIn: a company page link is either
one the company published on its own site (``confidence='published'``) or one constructed from
the domain (``confidence='constructed'``), the people search is a deep link the UI renders as a
button, and an address is stored only when the company published it — "never guess, never
SMTP-verify". Every rule below is therefore about what may be *stored*, and each test names the
clause it defends.

Two properties are worth more than the individual cases and are asserted directly:

* a published slug that happens to equal the constructed one produces a **byte-identical**
  value, which is the whole reason ``ingest/pipeline.py`` can promote a constructed row to
  published instead of writing a second one, and the numeric-id case proves the other branch
  of §6's "otherwise" is real;
* extraction reads parsed ``<a href>`` values, never raw markup, so a URL that appears only in
  a page's script or style text is neither a contact nor a person.

The markup is the recorded fixtures under ``tests/fixtures/company_site/`` wherever a real page
is the better input; short inline markup is used where one rule is the whole point. Nothing here
performs I/O beyond reading those fixtures — SPEC §4 excludes LinkedIn from the connectors, and
this module never requests any of the URLs it builds.
"""

from __future__ import annotations

import ast
from collections.abc import Callable
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from db import enums
from ingest.base import PersonRecord
from ingest.classify import load_classifiers
from ingest.contacts import (
    canonical_linkedin_company_url,
    constructed_linkedin_company_url,
    extract_emails,
    extract_people,
    extract_social_contacts,
    normalize_email,
    people_search_url,
    select_social_contacts,
)
from tests.support import fixture_text

#: The literal prefixes SPEC §6 spells out, written here rather than imported from the module
#: under test so a change to the constant cannot silently change the expectation.
COMPANY_PREFIX = "https://www.linkedin.com/company/"
PEOPLE_SEARCH = "https://www.linkedin.com/search/results/people/"


def site(name: str) -> str:
    """One recorded company page (``tests/fixtures/company_site/``)."""
    return fixture_text("company_site", f"{name}.html")


def page(body: str) -> str:
    return f"<!doctype html><html><body>{body}</body></html>"


def anchor(href: str, text: str = "Follow us") -> str:
    return f'<a href="{href}">{text}</a>'


def card(name: str, title: str, url: str) -> str:
    """One team card in the shape Atom Computing publishes: the name in an ``<h3>``, the title
    in the next sibling, and a profile anchor whose only text is an icon's ``<title>``."""
    return (
        f'<div class="card"><h3>{name}</h3><div>{title}</div>'
        f'<a href="{url}"><svg><title>Visit our LinkedIn</title></svg></a></div>'
    )


def kinds(markup: str, base_url: str) -> set[enums.ContactKind]:
    return {kind for kind, _ in extract_social_contacts(markup, base_url)}


def keywords(url: str) -> str:
    """The decoded ``keywords`` parameter of a people-search link."""
    return parse_qs(urlsplit(url).query)["keywords"][0]


# --------------------------------------------------------- C2: the constructed company link


@pytest.mark.parametrize(
    ("domain", "slug"),
    [
        ("astranis.com", "astranis"),
        ("atom-computing.com", "atom-computing"),
        ("baseten.co", "baseten"),  # a two-letter TLD that is not a ccTLD suffix
        ("foo.co.uk", "foo"),  # ... unlike this one, where "co" is the second level
        ("careers.foo.com", "foo"),  # a subdomain is not part of the slug
        ("www.astranis.com", "astranis"),
        ("https://www.astranis.com/careers", "astranis"),  # a URL normalizes to its host
        ("ASTRANIS.COM", "astranis"),
    ],
)
def test_the_constructed_slug_is_the_registrable_label(domain: str, slug: str) -> None:
    """SPEC §6: "construct ``https://www.linkedin.com/company/{slug}`` from the domain"."""
    assert constructed_linkedin_company_url(domain) == f"{COMPANY_PREFIX}{slug}"


@pytest.mark.parametrize("domain", [None, "", "   ", "localhost", "."])
def test_without_a_domain_there_is_no_link_to_construct(domain: str | None) -> None:
    """A slug guessed from something that is not a domain is a link to *someone else's*
    company, which is worse than showing nothing."""
    assert constructed_linkedin_company_url(domain) is None


# ---------------------------------------------------------- C1: the published company link


@pytest.mark.parametrize(
    ("published", "stored"),
    [
        # astranis.com's footer, verbatim — a trailing slash.
        ("https://www.linkedin.com/company/astranis/", f"{COMPANY_PREFIX}astranis"),
        # twelve.co's footer, verbatim — a trailing slash and a query string.
        (
            "https://www.linkedin.com/company/twelveco2/?viewAsMember=true",
            f"{COMPANY_PREFIX}twelveco2",
        ),
        # sourcegraph.com's footer, verbatim — LinkedIn also serves numeric company ids.
        ("https://www.linkedin.com/company/4803356/", f"{COMPANY_PREFIX}4803356"),
        ("https://www.linkedin.com/company/acme/about", f"{COMPANY_PREFIX}acme"),
        ("https://www.linkedin.com/company/acme#footer", f"{COMPANY_PREFIX}acme"),
        ("https://uk.linkedin.com/company/acme", f"{COMPANY_PREFIX}acme"),
        ("http://linkedin.com/company/acme", f"{COMPANY_PREFIX}acme"),
        # The segment keeps the case the company published: LinkedIn slugs are case-insensitive
        # but a vanity slug is displayed as written, and lowercasing it here would make the
        # value differ from the same URL seen on another page only by our own edit.
        ("https://www.linkedin.com/company/AtomComputing/", f"{COMPANY_PREFIX}AtomComputing"),
    ],
)
def test_a_published_company_url_is_canonicalised(published: str, stored: str) -> None:
    """Host, query, fragment, trailing slash and trailing segments are dropped so the same page
    linked from two of a company's own pages is one row, not two."""
    assert canonical_linkedin_company_url(published) == stored


@pytest.mark.parametrize(
    "url",
    [
        pytest.param("https://www.linkedin.com/in/ben-bloom-8871598/", id="a-personal-profile"),
        pytest.param("https://www.linkedin.com/school/mit", id="a-school-page"),
        pytest.param("https://www.linkedin.com/company/", id="no-slug-at-all"),
        pytest.param("https://www.linkedin.com/company", id="the-directory"),
        pytest.param("https://example.com/company/acme", id="another-host"),
        pytest.param("linkedin.com/company/acme", id="no-scheme"),
        pytest.param("", id="empty"),
        pytest.param(None, id="none"),
    ],
)
def test_what_is_not_a_published_company_page(url: str | None) -> None:
    """``/in/<slug>`` is a *person* (SPEC §5 ``Person``) and must never become a company
    contact; everything else here is not a LinkedIn company page either."""
    assert canonical_linkedin_company_url(url) is None


def test_a_published_slug_matching_the_domain_is_identical_to_the_constructed_link() -> None:
    """C1 beating C2 costs nothing when the two agree: astranis.com's footer link
    canonicalises to exactly the URL SPEC §6 would construct, so the pipeline's upsert lands on
    the existing constructed row and promotes it in place instead of writing a second one."""
    (published,) = [
        value
        for kind, value in extract_social_contacts(
            site("astranis_home"), "https://www.astranis.com/"
        )
        if kind is enums.ContactKind.LINKEDIN_COMPANY
    ]
    assert published == constructed_linkedin_company_url("astranis.com")


def test_a_published_numeric_id_can_never_equal_the_constructed_slug() -> None:
    """sourcegraph.com publishes ``/company/4803356``. The constructed link is
    ``/company/sourcegraph``, so the two rows differ and the pipeline has to *choose* — this is
    the case that proves "otherwise" in C2 is a real branch and not a coincidence."""
    (published,) = [
        value
        for kind, value in extract_social_contacts(
            site("sourcegraph_home"), "https://sourcegraph.com/"
        )
        if kind is enums.ContactKind.LINKEDIN_COMPANY
    ]
    assert published == f"{COMPANY_PREFIX}4803356"
    assert published != constructed_linkedin_company_url("sourcegraph.com")


# ------------------------------------------------------------- C3: the people-search link


def test_the_people_search_link_is_the_expression_spec_6_spells_out() -> None:
    """SPEC §6: "a people-search deep link filtered to the company name plus title keywords
    (``founder OR recruiter OR "head of engineering" OR "talent"``)". Nothing else is
    appended, and no profile is fetched — this is a link for the user to click."""
    url = people_search_url("Atom Computing")
    assert url.startswith(f"{PEOPLE_SEARCH}?")
    assert keywords(url) == (
        '"Atom Computing" (founder OR recruiter OR "head of engineering" OR "talent")'
    )


def test_the_four_title_terms_live_in_the_yaml() -> None:
    """CLAUDE.md: "no keywords in code". The shipped ``config/classifiers.yaml`` carries §6's
    four terms with §6's quoting, and they are what the link is built from."""
    assert load_classifiers().people_search_terms == (
        "founder",
        "recruiter",
        '"head of engineering"',
        '"talent"',
    )
    assert keywords(people_search_url("Acme")) == keywords(
        people_search_url("Acme", load_classifiers().people_search_terms)
    )


def test_a_multi_word_term_is_quoted_so_it_cannot_split_into_two_operands() -> None:
    """A phrase added to the YAML without quotes would otherwise become ``head OR of OR
    engineering`` at the search engine and match every company in the city."""
    assert keywords(people_search_url("Acme", ["founder", "head of engineering"])) == (
        '"Acme" (founder OR "head of engineering")'
    )


def test_the_query_string_is_encoded() -> None:
    """A raw space, quote, parenthesis or ampersand in the query would truncate the search at
    the first one — the ampersand would start a second parameter."""
    query = urlsplit(people_search_url("Fizz & Co", ["founder"])).query
    assert not any(character in query for character in ' "()&')
    assert keywords(f"?{query}") == '"Fizz & Co" (founder)'


def test_a_quote_in_the_company_name_cannot_break_out_of_the_phrase() -> None:
    """Otherwise the phrase closes early and the rest of the name becomes loose keywords."""
    assert keywords(people_search_url('Acme "Rocket" Labs', ["founder"])) == (
        '"Acme Rocket Labs" (founder)'
    )


# ------------------------------------------------------------------------ C4: e-mail


@pytest.mark.parametrize(
    ("raw", "stored"),
    [
        # atom-computing.com/careers, verbatim: an uppercase local part. Stored as published in
        # this case it would collide with the same address seen lowercase in an HN comment.
        ("mailto:HR@atom-computing.com", "hr@atom-computing.com"),
        ("MAILTO:Jobs@Example.com", "jobs@example.com"),
        ("mailto:jobs@example.com?subject=Hello%20there", "jobs@example.com"),
        ("mailto:a@example.com,b@example.com", "a@example.com"),
        ("mailto:jobs%40example.com", "jobs@example.com"),
        ("mailto:<jobs@example.com>", "jobs@example.com"),
        ("  Jobs@Example.com  ", "jobs@example.com"),
    ],
)
def test_a_published_address_is_stored_lowercased(raw: str, stored: str) -> None:
    """SPEC §6 stores only what the company published; the *stored form* is lowercased so one
    address can never occupy two rows under ``uq_contacts_company_id_kind_value``."""
    assert normalize_email(raw) == stored


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("mailto:?subject=Check%20this%20out", id="a-share-widget-has-no-address"),
        pytest.param("mailto:", id="empty-mailto"),
        pytest.param("mailto:someone@localhost", id="a-domain-with-no-dot"),
        pytest.param("mailto:not an address", id="not-an-address"),
        pytest.param("https://example.com/contact", id="a-page-not-an-address"),
        pytest.param("", id="empty"),
        pytest.param(None, id="none"),
    ],
)
def test_what_is_not_a_published_address(raw: str | None) -> None:
    assert normalize_email(raw) is None


def test_the_one_address_atom_computing_publishes() -> None:
    """The whole recorded site has exactly one ``mailto:``, on the careers page, and it is
    stored lowercased (SPEC §4 Tier 3 names "any ``mailto:`` the company published")."""
    assert extract_emails(site("atom_computing_careers")) == ["hr@atom-computing.com"]
    assert extract_emails(site("atom_computing_home")) == []


def test_only_a_mailto_href_counts_as_published() -> None:
    """An address the company put behind a link is one it meant to publish; a string that
    merely looks like an address in body text or a script bundle is not a contact."""
    markup = page(
        "<p>Write to press@example.com</p>"
        "<script>var endpoint = 'telemetry@vendor.io';</script>"
        f"{anchor('mailto:?subject=Share%20this', 'Share')}"
        f"{anchor('mailto:jobs@example.com', 'Jobs')}"
    )
    assert extract_emails(markup) == ["jobs@example.com"]


def test_one_address_published_in_two_cases_is_one_row() -> None:
    markup = page(anchor("mailto:HR@example.com") + anchor("mailto:hr@example.com"))
    assert extract_emails(markup) == ["hr@example.com"]


# ------------------------------------------------- footer social links (SPEC §4 Tier 3, §6)


def test_the_footer_links_astranis_publishes() -> None:
    """Webflow renders no ``<footer>`` element at all, so extraction reads every anchor: the
    X account, the LinkedIn company page and the site's own contact page are contacts;
    Instagram and YouTube are not kinds SPEC §5 has a member for."""
    assert extract_social_contacts(site("astranis_home"), "https://www.astranis.com/") == [
        (enums.ContactKind.CONTACT_FORM, "https://www.astranis.com/contact"),
        (enums.ContactKind.X, "https://x.com/Astranis"),
        (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}astranis"),
    ]


def test_the_footer_links_sourcegraph_publishes() -> None:
    """Every social anchor's only child is an ``<svg>``, so the account can only be read from the
    href — and the GitHub org link is the one SPEC §4 Tier 3 calls a footer social link. The
    footer's own ``/contact`` link is a contact form on the company's own domain, and it sorts
    ahead of the accounts because that is where it sits in the document."""
    assert extract_social_contacts(site("sourcegraph_home"), "https://sourcegraph.com/") == [
        (enums.ContactKind.CONTACT_FORM, "https://sourcegraph.com/contact"),
        (enums.ContactKind.GITHUB, "https://github.com/sourcegraph"),
        (enums.ContactKind.X, "https://x.com/Sourcegraph"),
        (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}4803356"),
    ]


def test_a_twitter_link_with_a_query_string_is_stored_as_one_x_account() -> None:
    """atom-computing.com links to ``twitter.com/atom_computing?lang=en`` in single-quoted
    attributes. The query is dropped and the host rewritten, because that is where the link
    now lands and because one account must not produce two rows."""
    assert extract_social_contacts(
        site("atom_computing_careers"), "https://atom-computing.com/careers/"
    ) == [
        (enums.ContactKind.X, "https://x.com/atom_computing"),
        (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}atom-computing"),
    ]


def test_one_account_linked_from_both_hosts_is_one_contact() -> None:
    markup = page(anchor("https://twitter.com/Acme") + anchor("https://x.com/Acme"))
    assert extract_social_contacts(markup, "https://acme.com/") == [
        (enums.ContactKind.X, "https://x.com/Acme")
    ]


def test_a_url_that_is_only_in_script_or_style_text_is_not_a_contact() -> None:
    """twelve.co ships a Wix stylesheet whose comment cites
    ``github.com/wix/yoshi/issues/2689``: a regex over the markup would have filed a third
    party's bug tracker as Twelve's GitHub org.

    The rule is stronger than that one URL's shape — extraction reads parsed ``<a href>``
    values, so a URL that is *only* in script or style text is not a contact and not a person
    even when its shape is one the table accepts.
    """
    assert extract_social_contacts(site("twelve_home"), "https://twelve.co/") == [
        (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}twelveco2"),
        (enums.ContactKind.CONTACT_FORM, "https://www.twelve.co/contact"),
    ]
    unlinked = page(
        "<style>/* vendored from https://github.com/evil-corp */</style>"
        "<script>var social = 'https://x.com/evilcorp';</script>"
    )
    assert extract_social_contacts(unlinked, "https://acme.com/") == []


@pytest.mark.parametrize(
    "href",
    [
        pytest.param("https://twitter.com/intent/tweet?url=https%3A%2F%2Facme.com", id="intent"),
        pytest.param("https://x.com/share", id="share"),
        pytest.param("https://x.com/home", id="home"),
        pytest.param("https://x.com/hashtag/quantum", id="hashtag"),
        pytest.param("https://x.com/i/flow/login", id="i-flow"),
        pytest.param("https://x.com/login", id="login"),
        pytest.param("https://x.com/search?q=acme", id="search"),
        pytest.param("https://github.com/pricing", id="pricing"),
        pytest.param("https://www.facebook.com/sharer/sharer.php?u=x", id="facebook-sharer"),
        pytest.param("https://www.linkedin.com/sharing/share-offsite/?url=x", id="li-sharing"),
        pytest.param("https://www.linkedin.com/shareArticle?mini=true&url=x", id="shareArticle"),
        pytest.param("https://www.linkedin.com/in/ben-bloom-8871598/", id="a-person"),
        pytest.param("https://github.com/acme/cli/issues/12345", id="a-deep-github-path"),
        pytest.param("https://github.com/features/copilot", id="a-github-product-page"),
        pytest.param("https://github.com/", id="github-itself"),
        pytest.param("https://hubspot.com/contact", id="someone-elses-contact-page"),
        pytest.param("mailto:jobs@acme.com", id="mailto"),
        pytest.param("tel:+14155550100", id="tel"),
        pytest.param("javascript:void(0)", id="javascript"),
        pytest.param("#footer", id="a-bare-fragment"),
    ],
)
def test_shapes_that_are_never_a_contact(href: str) -> None:
    """Share and intent widgets, marketing pages, a *person's* profile and a link into a third
    party's repository are all links a company's own footer carries; none of them is a way to
    reach the company. A path deeper than the shape allows is rejected, not truncated."""
    assert extract_social_contacts(page(anchor(href)), "https://acme.com/") == []


def test_a_github_org_is_kept_when_the_link_points_at_one_of_its_repositories() -> None:
    """The single exception to "rejected, not truncated": ``<org>/<repo>`` is what a "we're on
    GitHub" link looks like, and the org is what it means."""
    assert extract_social_contacts(
        page(anchor("https://github.com/acme/acme-cli")), "https://acme.com/"
    ) == [(enums.ContactKind.GITHUB, "https://github.com/acme")]


def test_the_contact_form_is_the_companys_own_shallowest_contact_page() -> None:
    """A relative href resolves against the page it was found on, and the deeper paths that
    also contain "contact" are individual forms on the one contact page. Hand-written markup,
    because it also varies the *relative* href; the recorded evidence for the rule itself is
    the test below."""
    markup = page(
        anchor("https://acme.com/contact/request-info", "Request info")
        + anchor("/contact", "Contact")
    )
    assert extract_social_contacts(markup, "https://acme.com/about") == [
        (enums.ContactKind.CONTACT_FORM, "https://acme.com/contact")
    ]


def test_the_shallowest_contact_rule_on_the_page_it_was_written_for() -> None:
    """``docs/SOURCES.md`` documents the rule with sourcegraph's own pair of URLs — "``…/contact``
    wins over ``…/contact/request-info``" — and the recording carries both: the nav's demo CTA
    points at the deeper path, the footer's Contact link at the shallower one, in that document
    order. So the rule is shallowest-wins rather than first-seen-wins on real markup, not only
    on the hand-written page above.
    """
    markup = site("sourcegraph_home")
    assert 'href="/contact/request-info"' in markup, "the fixture lost the deeper recorded path"
    assert [
        contact
        for contact in extract_social_contacts(markup, "https://sourcegraph.com/")
        if contact[0] is enums.ContactKind.CONTACT_FORM
    ] == [(enums.ContactKind.CONTACT_FORM, "https://sourcegraph.com/contact")]


def test_an_href_that_is_not_a_url_at_all_skips_that_anchor() -> None:
    """``urljoin`` parses the href, so an unbalanced bracket in the authority raises rather
    than returning something unusable — and an unrendered ``[[template]]`` or a truncated IPv6
    literal is exactly that. Every input here is somebody else's markup, so one such href must
    cost that anchor and nothing else: the rest of the page still yields its contacts."""
    markup = page(
        anchor("https://[[siteUrl]]/contact", "Contact")
        + anchor("//[[host]]/track", "Track")
        + anchor("https://exam]ple.com/careers", "Jobs")
        + anchor("https://www.linkedin.com/company/acme", "LinkedIn")
    )
    assert extract_social_contacts(markup, "https://acme.com/") == [
        (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}acme")
    ]


def test_a_contact_page_is_only_a_contact_on_the_companys_own_domain() -> None:
    """``registrable`` comparison, not a substring: a support portal on a vendor's domain is
    not this company's contact form."""
    assert kinds(page(anchor("https://acme.zendesk.com/contact")), "https://acme.com/") == set()
    assert kinds(page(anchor("https://www.acme.com/contact")), "https://acme.com/") == {
        enums.ContactKind.CONTACT_FORM
    }


# ------------------------------------- whose account is it? (SPEC §6 C1 "otherwise construct")

#: The two LinkedIn company pages an "Our investors" block links. Both are real organisation
#: pages of exactly the shape ``_social_contact`` accepts, which is the whole difficulty.
A16Z = (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}andreessen-horowitz")
YC = (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}y-combinator")
OWN = (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}acme-robotics")


def test_the_one_account_a_visit_links_is_the_companys_own() -> None:
    """The ordinary case, and the reason a slug is not required to match the domain: what a
    company publishes about itself is what SPEC §6 C1 stores, even when the domain could never
    have produced it (``twelve.co`` → ``/company/twelveco2``, ``sourcegraph.com`` → a numeric
    id). One candidate of a kind is that company's account."""
    numeric = (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}4803356")
    assert select_social_contacts([numeric], domain="sourcegraph.com", name="Sourcegraph") == [
        numeric
    ]
    vanity = (enums.ContactKind.LINKEDIN_COMPANY, f"{COMPANY_PREFIX}twelveco2")
    assert select_social_contacts([vanity], domain="twelve.co", name="Twelve") == [vanity]


def test_an_investors_linkedin_page_is_not_the_companys_own() -> None:
    """SPEC §6 C1 scopes the published page to what the company publishes about *itself*.

    A "Backed by" block on ``/about`` links two funds' organisation pages, and they reach the
    extractor exactly as the company's own footer link does. Storing one of them as
    ``published`` puts a stranger's page in the panel under "Published by the company" — and,
    because the pipeline treats any published row of this kind as "the LinkedIn page is known",
    deletes the constructed link to the right company. The slug decides: it is the company's
    name or its domain label, or it is somebody else's.
    """
    assert select_social_contacts(
        [A16Z, OWN, YC], domain="acme-robotics.com", name="Acme Robotics"
    ) == [OWN]


def test_third_party_pages_with_no_link_of_the_companys_own_store_nothing() -> None:
    """The branch SPEC §6 spells "otherwise": with no way to tell which page is theirs, storing
    none is what lets the pipeline construct ``…/company/acme-robotics`` — a link to the right
    company, labelled constructed — instead of publishing a fund's page as fact."""
    assert select_social_contacts([A16Z, YC], domain="acme-robotics.com", name="Acme") == []


def test_the_same_rule_covers_the_github_org_and_the_x_handle() -> None:
    """A blog post links a third party's repository and a conference's handle; both are
    well-formed accounts on the two hosts SPEC §4 lists. Neither is a way to reach this
    company, and the fact that no *constructed* row exists for these kinds only makes the wrong
    row additive rather than destructive."""
    candidates = [
        (enums.ContactKind.GITHUB, "https://github.com/kubernetes"),
        (enums.ContactKind.GITHUB, "https://github.com/acme"),
        (enums.ContactKind.X, "https://x.com/ycombinator"),
        (enums.ContactKind.X, "https://x.com/Acme"),
    ]
    assert select_social_contacts(candidates, domain="acme.com", name="Acme, Inc.") == [
        (enums.ContactKind.GITHUB, "https://github.com/acme"),
        (enums.ContactKind.X, "https://x.com/Acme"),
    ]


def test_the_separator_a_host_prefers_is_not_a_different_company() -> None:
    """``atom-computing.com`` publishes ``x.com/atom_computing``: one identity, three
    spellings. Case and separators are not evidence of anything."""
    handle = (enums.ContactKind.X, "https://x.com/atom_computing")
    other = (enums.ContactKind.X, "https://x.com/quantumdaily")
    assert select_social_contacts(
        [other, handle], domain="atom-computing.com", name="Atom Computing"
    ) == [handle]


def test_the_contact_form_is_never_second_guessed() -> None:
    """It is already restricted to the company's own domain, so it passes through untouched
    even when the page's account links are too ambiguous to publish."""
    form = (enums.ContactKind.CONTACT_FORM, "https://acme-robotics.com/contact")
    assert select_social_contacts([form, A16Z, YC], domain="acme-robotics.com", name="Acme") == [
        form
    ]


# ---------------------------------------------------- people (SPEC §5 ``Person``, §6)


def test_the_people_atom_computing_publishes_on_its_about_page() -> None:
    """Name, title, ``role_type`` and a profile URL **the company itself published** — SPEC §4
    excludes LinkedIn, so nothing here was fetched from a profile.

    Every trap on the real page is exercised at once: the anchor text is the useless "Visit our
    LinkedIn", the name is an ``<h3>`` carrying a credential, the title is the next sibling and
    contains a zero-width space, and the "Read Bio" anchor between them points at ``http://``.
    """
    assert extract_people(site("atom_computing_about")) == [
        PersonRecord(
            full_name="Ben Bloom",
            title="CEO & Founder",
            role_type=enums.RoleType.FOUNDER,
            linkedin_url="https://www.linkedin.com/in/ben-bloom-8871598",
        ),
        PersonRecord(
            full_name="Jonathan King",
            title="Chief Scientist & Co-Founder",
            role_type=enums.RoleType.FOUNDER,
            linkedin_url="https://www.linkedin.com/in/kingjonathanp",
        ),
        PersonRecord(
            full_name="Sarah Murrow",
            title="VP, Human Resources",
            role_type=enums.RoleType.RECRUITER,
            linkedin_url="https://www.linkedin.com/in/sarah-murrow-1853248",
        ),
    ]


def test_the_section_heading_above_the_cards_is_not_a_person() -> None:
    """The failure mode this guards is silent: cards whose anchor text is identical all fall
    back to the same ``<h2>``, and the company acquires three employees called "Executive
    Leadership" (Atom Computing's real about page, below). A name claimed by more than one
    profile URL is a heading, and every person it produced is dropped."""
    trap = page(
        "<h2>Advisory Board</h2>"
        '<div><a href="https://www.linkedin.com/in/one"><svg><title>Visit our LinkedIn'
        "</title></svg></a></div>"
        '<div><a href="https://www.linkedin.com/in/two"><svg><title>Visit our LinkedIn'
        "</title></svg></a></div>"
    )
    assert extract_people(trap) == []
    assert "Executive Leadership" not in {
        person.full_name for person in extract_people(site("atom_computing_about"))
    }


def test_two_cards_with_their_own_headings_are_two_people() -> None:
    """The control for the rule above: the guard must not swallow a real team page."""
    markup = page(
        card("Ada Byron", "Chief Technology Officer", "https://www.linkedin.com/in/ada")
        + card("Grace Hopper", "VP of Engineering", "https://www.linkedin.com/in/grace")
    )
    assert [(person.full_name, person.role_type) for person in extract_people(markup)] == [
        ("Ada Byron", enums.RoleType.ENG_LEAD),
        ("Grace Hopper", enums.RoleType.ENG_LEAD),
    ]


def link_first_card(name: str, title: str, url: str) -> str:
    """A card whose profile link sits *above* the name: the photo is the link, which is how
    most team grids are built."""
    return (
        f'<div class="card"><a href="{url}"><img src="/p.jpg"></a>'
        f"<h3>{name}</h3><p>{title}</p></div>"
    )


def two_column_card(name: str, title: str, url: str) -> str:
    """Photo left, bio right — the anchor is in the column *before* the one holding the name."""
    return (
        f'<div class="row"><div class="photo"><a href="{url}"><img src="/p.jpg"></a></div>'
        f'<div class="bio"><h3>{name}</h3><p>{title}</p></div></div>'
    )


@pytest.mark.parametrize("build", [link_first_card, two_column_card], ids=["link-first", "column"])
def test_a_card_that_links_before_it_names_still_pairs_the_two(
    build: Callable[[str, str, str], str],
) -> None:
    """A name and a profile URL are one published fact, and the card is what binds them.

    Reading backwards from the anchor for the nearest heading leaves the card and lands in the
    previous one, so every person on the page takes the *next* person's LinkedIn profile and
    the last person is dropped entirely — a mis-attribution nothing downstream can catch,
    because each row is internally plausible and the panel prints the name as a link to a
    stranger's profile. Both shapes below put the link before the name, which is the ordinary
    way to publish a photo that is also a link.
    """
    markup = page(
        build("Jane Doe", "Chief Executive Officer", "https://www.linkedin.com/in/jane-doe")
        + build("John Smith", "Chief Technology Officer", "https://www.linkedin.com/in/john-smith")
        + build("Amy Lee", "Head of Talent", "https://www.linkedin.com/in/amy-lee")
    )
    assert extract_people(markup) == [
        PersonRecord(
            full_name="Jane Doe",
            title="Chief Executive Officer",
            role_type=enums.RoleType.EXEC,
            linkedin_url="https://www.linkedin.com/in/jane-doe",
        ),
        PersonRecord(
            full_name="John Smith",
            title="Chief Technology Officer",
            role_type=enums.RoleType.ENG_LEAD,
            linkedin_url="https://www.linkedin.com/in/john-smith",
        ),
        PersonRecord(
            full_name="Amy Lee",
            title="Head of Talent",
            role_type=enums.RoleType.RECRUITER,
            linkedin_url="https://www.linkedin.com/in/amy-lee",
        ),
    ]


@pytest.mark.parametrize(
    "markup",
    [
        pytest.param(
            '<a class="card" href="https://www.linkedin.com/in/jane-doe">'
            "<img src=/p.jpg><h3>Jane Doe</h3><p>Chief Technology Officer</p></a>",
            id="minified",
        ),
        pytest.param(
            '<a class="card" href="https://www.linkedin.com/in/jane-doe">\n'
            "  <img src=/p.jpg>\n  <h3>Jane Doe</h3>\n"
            "  <p>Chief Technology Officer</p>\n</a>",
            id="pretty-printed",
        ),
    ],
)
def test_a_card_wrapped_in_its_own_profile_link_is_a_name_and_a_title(markup: str) -> None:
    """``Node.text()`` concatenates every descendant, so the anchor's own text is the whole
    card: "Jane Doe Chief Technology Officer" pretty-printed and "Jane DoeCTO" minified, which
    is what a real page serves. Stored as ``full_name`` it is a person nobody is called, with no
    title and no ``role_type``, and the pipeline's ``(company_id, lower(full_name))`` key can
    never line that up with the same person seen anywhere else. The heading inside the anchor is
    the name; the line after it is the title."""
    assert extract_people(page(markup)) == [
        PersonRecord(
            full_name="Jane Doe",
            title="Chief Technology Officer",
            role_type=enums.RoleType.ENG_LEAD,
            linkedin_url="https://www.linkedin.com/in/jane-doe",
        )
    ]


def test_a_profile_link_inside_the_heading_that_names_the_person() -> None:
    """The other half of "the card's heading names this person": here the anchor *is* the
    heading's content, so the title is the line after the heading the anchor sits in. Without
    that the person is stored with no title and a NULL ``role_type``, invisible to any filter on
    it, and the omission is silent."""
    markup = page(
        '<div class="card"><h3><a href="https://www.linkedin.com/in/ada">Ada Byron</a></h3>'
        "<div>VP of Engineering</div></div>"
    )
    assert extract_people(markup) == [
        PersonRecord(
            full_name="Ada Byron",
            title="VP of Engineering",
            role_type=enums.RoleType.ENG_LEAD,
            linkedin_url="https://www.linkedin.com/in/ada",
        )
    ]


@pytest.mark.parametrize(
    ("published", "stored"),
    [
        ("Ben Bloom, PhD", "Ben Bloom"),
        ("Ada Byron, Ph.D.", "Ada Byron"),
        ("Rex Stout, Jr.", "Rex Stout"),
        ("Mary Ann, MD, MBA", "Mary Ann"),
        ("Jane Roe", "Jane Roe"),
    ],
)
def test_credentials_are_stripped_from_a_published_name(published: str, stored: str) -> None:
    """``Person.full_name`` is a name, not a byline: the same person listed with and without a
    doctorate must not become two rows."""
    markup = page(card(published, "Chief Scientist", "https://www.linkedin.com/in/x"))
    assert [person.full_name for person in extract_people(markup)] == [stored]


def test_an_unclassifiable_title_leaves_role_type_null_and_still_stores_the_person() -> None:
    """SPEC §5 makes ``role_type`` nullable and SPEC §6 stores only what was published — so an
    unmatched title is ``None``, never a guess from the name or the link, and the person is
    still a row."""
    markup = page(card("Rex Stout", "Barista", "https://www.linkedin.com/in/rex"))
    assert extract_people(markup) == [
        PersonRecord(
            full_name="Rex Stout",
            title="Barista",
            role_type=None,
            linkedin_url="https://www.linkedin.com/in/rex",
        )
    ]
    # ... and the published title itself is kept either way: only the classification is null.
    classified = page(card("Rex Stout", "Chief Barista", "https://www.linkedin.com/in/rex"))
    assert extract_people(classified)[0].role_type is enums.RoleType.EXEC


@pytest.mark.parametrize(
    "href",
    [
        "https://www.linkedin.com/in/jane-roe/",
        "//www.linkedin.com/in/jane-roe",
        "https://uk.linkedin.com/in/jane-roe?trk=x",
    ],
)
def test_the_profile_url_is_the_published_one_canonicalised(href: str) -> None:
    """The name may come from the anchor's own text when it looks like one; the URL is
    canonicalised so the same person linked from two pages is one row."""
    assert extract_people(page(anchor(href, "Jane Roe"))) == [
        PersonRecord(
            full_name="Jane Roe",
            title=None,
            role_type=None,
            linkedin_url="https://www.linkedin.com/in/jane-roe",
        )
    ]


def test_one_profile_linked_twice_is_one_person() -> None:
    markup = page(
        card("Ada Byron", "CTO", "https://www.linkedin.com/in/ada")
        + card("Ada B", "CTO", "https://www.linkedin.com/in/ada/")
    )
    assert [person.full_name for person in extract_people(markup)] == ["Ada Byron"]


def test_a_profile_url_in_a_script_is_not_a_person() -> None:
    """The same "anchors, never raw markup" rule on the people side: a ``Person`` row comes
    from a link the company put on the page next to a name, not from any
    ``linkedin.com/in`` string that its script bundle happens to carry."""
    markup = page(
        '<div class="card"><h3>Ada Byron</h3><div>Chief Technology Officer</div>'
        '<script>var profile = "https://www.linkedin.com/in/ada";</script></div>'
    )
    assert extract_people(markup) == []


def test_a_company_page_link_is_never_a_person() -> None:
    """astranis.com's footer links to its LinkedIn *company* page and to no profile at all."""
    assert extract_people(site("astranis_home")) == []
    assert extract_people(page(anchor(f"{COMPANY_PREFIX}acme"))) == []


# ------------------------------------------------- SPEC §4 "Excluded": written, never fetched

#: The hosts SPEC §4 forbids fetching. ``angel.co`` is Wellfound's old domain and still
#: resolves, so a connector written against it would be the same violation under another name.
EXCLUDED_HOSTS = ("linkedin.com", "crunchbase.com", "wellfound.com", "angel.co")

#: The only modules allowed to name one, and the reason each is not a fetch. Anything else that
#: names an excluded host is, on the evidence of the string alone, a connector for a source
#: SPEC §4 says not to build — which is precisely the mistake this test exists to make loud.
EXCLUDED_HOST_USES: dict[str, str] = {
    "ingest/contacts.py": (
        "SPEC §6: the module that writes LinkedIn URLs down. It imports no HTTP client, "
        "which the assertion below proves rather than assumes."
    ),
    "ingest/connectors/hn_hiring.py": (
        "linkedin.com appears in the list of hosts that are NOT a company's own domain, so "
        "a poster's profile link is never mistaken for their employer's website."
    ),
}

#: Reaching the network needs one of these. ``ingest.contacts`` may import neither.
_FETCHERS = ("httpx", "ingest.http", "ingest.connectors")

_SOURCE_ROOT = Path(__file__).resolve().parent.parent
#: Third-party code, the test suite itself (fixtures are *recordings* of excluded hosts, which
#: is the point of them) and Alembic revisions, which contain no URLs at all.
_NOT_FIRST_PARTY = frozenset({".venv", "tests", "migrations", ".git", "__pycache__"})


def _first_party_modules() -> list[Path]:
    return [
        path
        for path in sorted(_SOURCE_ROOT.rglob("*.py"))
        if not any(
            part in _NOT_FIRST_PARTY or part.startswith(".")
            for part in path.relative_to(_SOURCE_ROOT).parts
        )
    ]


def _docstring_ids(tree: ast.AST) -> set[int]:
    """The ``id()`` of every string constant that is a docstring.

    Prose about LinkedIn is exactly what this repository is supposed to be full of — SPEC §6 is
    explained in half a dozen module docstrings. Only a *live* string can become a request.
    """
    found: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            found.add(id(body[0].value))
    return found


def _modules_naming_an_excluded_host() -> dict[str, list[str]]:
    """``{module: [string literals]}`` for every live string carrying an excluded host."""
    hits: dict[str, list[str]] = {}
    for path in _first_party_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = _docstring_ids(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if id(node) in docstrings:
                continue
            if any(host in node.value.casefold() for host in EXCLUDED_HOSTS):
                hits.setdefault(str(path.relative_to(_SOURCE_ROOT)), []).append(node.value)
    return hits


def _imported_modules(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.add(node.module)
    return names


def test_only_two_modules_name_an_excluded_source_at_all() -> None:
    """SPEC §4 "Excluded", statically: LinkedIn is a string this project writes, never sends.

    A LinkedIn, Crunchbase or Wellfound connector would have to put the host in a live string
    somewhere, and it would land here as a module that is not on the list above. Docstrings are
    exempt because SPEC §6 is explained in prose all over this repository; a docstring cannot
    become a URL.
    """
    assert set(_modules_naming_an_excluded_host()) == set(EXCLUDED_HOST_USES)


def test_the_module_that_builds_linkedin_urls_cannot_fetch_one() -> None:
    """SPEC §6 C7, as a property of the import graph rather than a promise in a comment.

    ``ingest.contacts`` holds every LinkedIn URL in the codebase. It reaches the network
    through nothing: no ``httpx``, no shared client, no connector. So the URLs it builds can
    only be stored and rendered for the user to click — which is what SPEC §6 replaces
    scraping with, and the reason no profile data is ever fetched or stored.
    """
    imported = _imported_modules(_SOURCE_ROOT / "ingest" / "contacts.py")
    offenders = {
        name
        for name in imported
        if any(name == fetcher or name.startswith(f"{fetcher}.") for fetcher in _FETCHERS)
    }
    assert offenders == set(), f"ingest/contacts.py can reach the network via {offenders}"
