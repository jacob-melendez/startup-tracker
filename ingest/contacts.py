"""Every SPEC §6 contact rule, as pure functions (SPEC §6, §4 Tier 3, §12 Phase 5).

SPEC §6 is a set of *URL* rules, not a fetching strategy: a LinkedIn company page is either a
URL the company published on its own site (``confidence='published'``) or one we construct from
the domain (``confidence='constructed'``), the people search is a deep link we build, and an
e-mail is stored only when the company published it. **Nothing here ever requests
``linkedin.com``** — SPEC §4 excludes LinkedIn from the connectors entirely, and this module is
the reason it never needs to be fetched.

Keeping the rules here rather than in ``ingest/connectors/company_site.py`` matters because they
have two very different callers: the connector, which reads them *off a page it fetched*, and
:mod:`ingest.pipeline`, which *constructs* the deterministic links on every company upsert —
including for the companies the Tier-3 connector has never visited. One home means the
published and the constructed form of the same link are canonicalised by the same code, which is
what lets the pipeline recognise that a published URL and a constructed one are the same row
(see :func:`canonical_linkedin_company_url`).

Everything in this module is pure: no session, no clock, no network, no I/O beyond the cached
read of ``config/classifiers.yaml`` that :func:`ingest.classify.classify_person_role` already
performs for every job title. The extractors take markup and return plain values, so they are
unit-testable from a recorded fixture without any HTTP.

Two vocabularies stay in Python rather than in that YAML, deliberately: :data:`_NOT_A_NAME` and
:data:`_CREDENTIALS`. Neither is a classification rule — nothing routes on them and no row is
filtered by them. They are the test of whether a string is a person's *name* at all, in the same
way :data:`ingest.normalize.NAME_SUFFIXES` decides where a company name ends, and they are kept
beside that test for the same reason. CLAUDE.md's "no keywords in code" rule covers the four
SPEC §7.1 classifier vocabularies plus this module's own ``contacts.person_role_type`` and
``contacts.people_search_terms``, which do live in ``config/classifiers.yaml``. A name candidate
these words reject is logged with the word that rejected it, so a person the page published and
this module dropped traces back to one line here.

**Anchors, never a raw-markup regex.** Every extractor reads parsed ``<a href>`` values. A live
probe of ``twelve.co`` found ``github.com/wix/yoshi/issues/2689`` inside a bundled script; a
regex over the markup would have filed a third party's bug tracker as the company's GitHub org.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterator, Sequence
from typing import Literal
from urllib.parse import SplitResult, unquote, urlencode, urljoin, urlsplit

from selectolax.parser import Node

from db import enums
from ingest.base import PersonRecord
from ingest.classify import Classifiers, classify_person_role, load_classifiers
from ingest.htmlutil import iter_anchors, iter_links
from ingest.normalize import normalize_domain, normalize_name
from logging_config import get_logger

log = get_logger(__name__)

#: SPEC §6 spells the constructed company URL ``https://www.linkedin.com/company/{slug}``; the
#: same host is used when canonicalising a published URL so the two forms can be compared as
#: strings (``uk.linkedin.com/company/acme`` and ``linkedin.com/company/acme/about`` are one row).
LINKEDIN_COMPANY_PREFIX = "https://www.linkedin.com/company/"
LINKEDIN_PROFILE_PREFIX = "https://www.linkedin.com/in/"
#: SPEC §6 "People search": the deep link the UI renders as the "Find people →" button.
PEOPLE_SEARCH_BASE = "https://www.linkedin.com/search/results/people/"

_LINKEDIN = "linkedin.com"
_GITHUB = "github.com"
#: X is served from both hosts and the old one now redirects to the new one, so a footer that
#: still links to ``twitter.com`` is the same account and must not become a second contact row.
_X_HOSTS = frozenset({"x.com", "twitter.com"})

#: The contact kinds that name an *account* somewhere else, one per company. A page can offer
#: several well-formed candidates for each — its own and an investor's — so exactly one of them
#: may be published (:func:`select_social_contacts`). ``contact_form`` is not here: it is
#: already restricted to the company's own domain, and a site may publish only one anyway.
_ACCOUNT_KINDS = frozenset(
    {enums.ContactKind.LINKEDIN_COMPANY, enums.ContactKind.X, enums.ContactKind.GITHUB}
)

#: Two-label public suffixes that occur under a two-letter ccTLD (``foo.co.uk``). Only the ones
#: that actually appear in a startup's domain are listed; the full Public Suffix List is a
#: dependency and SPEC §6 needs no more than "the registrable label of the domain".
_SECOND_LEVEL_SUFFIXES = frozenset({"co", "com", "org", "net", "ac", "gov", "edu"})

#: Path segments that are a widget, a marketing page or a search — never an account. Checked
#: against the segment that identifies the account (the X handle, the GitHub org), so a share
#: button (``x.com/intent/tweet``, ``github.com/features``) never becomes a contact.
_NOT_AN_ACCOUNT = frozenset(
    {
        "intent",
        "share",
        "sharer",
        "sharearticle",
        "home",
        "search",
        "hashtag",
        "i",
        "login",
        "signup",
        "features",
        "pricing",
    }
)
#: GitHub's own reserved namespaces, which a "fork me on GitHub" or docs link lands in. Kept
#: separate from :data:`_NOT_AN_ACCOUNT` because they are only reserved *on github.com*: a
#: LinkedIn company slug or an X handle may legitimately be one of these words.
_GITHUB_RESERVED = frozenset(
    {
        "about",
        "apps",
        "collections",
        "contact",
        "enterprise",
        "explore",
        "marketplace",
        "new",
        "orgs",
        "readme",
        "security",
        "settings",
        "site",
        "sponsors",
        "topics",
        "trending",
    }
)

_HTTP_SCHEMES = frozenset({"http", "https"})
_HEADINGS = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})
_HEADING_SELECTOR = "h1,h2,h3,h4,h5,h6"
#: Elements whose text is furniture, never a name or a title.
_INVISIBLE = frozenset({"script", "style", "noscript", "template", "svg", "head"})

#: The address shape, written once. SPEC §6 allows exactly two sources — a ``mailto:`` on the
#: company's own site and an address a founder posted in an HN hiring comment — and they must
#: accept the same set, or the same address reaches ``contacts`` from one source and not the
#: other. Two compiled forms, one vocabulary: :data:`_EMAIL` validates a string that has already
#: been isolated (a ``mailto:`` href), :data:`EMAIL_IN_TEXT` finds addresses inside free prose.
_ADDRESS = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
_EMAIL = re.compile(_ADDRESS)
#: Word boundaries only matter when scanning prose, where the surrounding characters are not
#: ours: without them ``mail-alex@acme.com`` would yield ``alex@acme.com``.
EMAIL_IN_TEXT = re.compile(rf"\b{_ADDRESS}\b")
_ADDRESS_SEPARATOR = re.compile(r"[,;]")
#: A LinkedIn slug is ASCII-ish and short; a numeric company id (``4803356``) is also a slug.
_LINKEDIN_SEGMENT = re.compile(r"[A-Za-z0-9%._+-]{1,100}")
_X_HANDLE = re.compile(r"[A-Za-z0-9_]{1,15}")
_GITHUB_OWNER = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})")

_WHITESPACE = re.compile(r"\s+")
#: Everything a company's name, domain label, slug, handle and org may differ by: case and the
#: separator each host happens to prefer (``atom-computing`` / ``atom_computing``).
_NOT_IDENTITY = re.compile(r"[^a-z0-9]+")

#: Trailing credentials stripped off a published name, mirroring
#: :data:`ingest.normalize.NAME_SUFFIXES` for company names: "Ben Bloom, PhD" is one person
#: whether or not the page felt like listing the doctorate.
_CREDENTIALS = frozenset({"phd", "md", "mba", "jr", "sr", "ii", "iii", "msc", "ms", "jd", "esq"})
#: Words that prove a string is chrome, not a person: "Visit our LinkedIn", "Follow us",
#: "Executive Leadership", "Our Team".
_NOT_A_NAME = ("linkedin", "visit", "follow", "profile", "team", "leadership")
#: SPEC §6 stores no profile data, so a name is only ever taken from text the company published
#: next to the link. These bounds are what separates "Ben Bloom" from a sentence.
_NAME_MIN_TOKENS = 2
_NAME_MAX_TOKENS = 5
_NAME_MAX_CHARS = 60
_TITLE_MAX_CHARS = 80
#: How far up the DOM a name may be looked for. Deeper than this and the "nearest heading" is a
#: section heading shared by every card on the page, not the person's own name.
_MAX_ANCESTORS = 4


# ------------------------------------------------------------------------------- URL helpers


def _split(url: str) -> SplitResult | None:
    """``urlsplit`` for a string that came off somebody else's page; ``None`` if unusable."""
    try:
        parts = urlsplit(url)
    except ValueError:  # an invalid IPv6 literal is the only way urlsplit raises
        return None
    return parts if parts.scheme.lower() in _HTTP_SCHEMES else None


def resolve_url(base: str, href: str) -> str | None:
    """``urljoin`` for an href off somebody else's page; ``None`` when it is not a URL at all.

    ``urljoin`` parses the href, so it raises the same ``ValueError`` :func:`_split` guards
    against — an unbalanced bracket in the authority, which is what an unrendered template
    (``https://[[siteUrl]]/contact``) or a truncated IPv6 literal looks like. Every caller reads
    hrefs out of arbitrary third-party markup, where one such link must skip that anchor rather
    than end the page, so the join and the split are guarded together and in one place.
    """
    try:
        joined = urljoin(base, href)
        urlsplit(joined)
    except ValueError:
        log.debug("href is not a resolvable url", base=base, href=href)
        return None
    return joined


def _segments(parts: SplitResult) -> list[str]:
    """Non-empty path segments, so ``/company/acme/`` and ``/company/acme`` read alike."""
    return [segment for segment in parts.path.split("/") if segment]


def _registrable_domain(domain: str | None) -> str | None:
    """The registrable part of a normalized domain: ``careers.foo.com`` → ``foo.com``,
    ``foo.co.uk`` → ``foo.co.uk``, ``baseten.co`` → ``baseten.co``."""
    labels = _registrable_labels(domain)
    return ".".join(labels) if labels else None


def _registrable_labels(domain: str | None) -> tuple[str, ...]:
    """The registrable labels of ``domain``, or ``()`` when it has none.

    Three labels when the last is a two-letter ccTLD preceded by a known second-level suffix
    (``foo.co.uk``), two otherwise. This is deliberately not the Public Suffix List: SPEC §6
    only needs the label a company's LinkedIn slug is built from, and the PSL is a dependency
    and a monthly data refresh for a rule that ``co.uk`` exhausts in practice.
    """
    if not domain:
        return ()
    labels = domain.split(".")
    if len(labels) < 2:
        return ()
    if len(labels) >= 3 and len(labels[-1]) == 2 and labels[-2] in _SECOND_LEVEL_SUFFIXES:
        return tuple(labels[-3:])
    return tuple(labels[-2:])


#: The two LinkedIn path shapes SPEC §6 deals in, and nothing else.
_LINKEDIN_PREFIXES: dict[str, str] = {
    "company": LINKEDIN_COMPANY_PREFIX,
    "in": LINKEDIN_PROFILE_PREFIX,
}


def _linkedin_url(url: str, kind: Literal["company", "in"]) -> str | None:
    """Canonical ``https://www.linkedin.com/{kind}/<segment>``, or ``None``.

    ``kind`` is ``company`` (an organisation page) or ``in`` (a personal profile). Host case and
    any country prefix are dropped, as are the query, the fragment, a trailing slash and any
    trailing segment (``/company/acme/about`` → ``/company/acme``). The segment itself keeps the
    case the company published: real slugs are lowercase, but LinkedIn also serves numeric ids
    (``/company/4803356``, which is what sourcegraph.com links to) and mixed-case vanity slugs.
    """
    parts = _split(url)
    if parts is None or _registrable_domain(normalize_domain(url)) != _LINKEDIN:
        return None
    segments = _segments(parts)
    if len(segments) < 2 or segments[0].casefold() != kind:
        return None
    segment = segments[1]
    if not _LINKEDIN_SEGMENT.fullmatch(segment) or segment.casefold() in _NOT_AN_ACCOUNT:
        return None
    return f"{_LINKEDIN_PREFIXES[kind]}{segment}"


def canonical_linkedin_company_url(url: str | None) -> str | None:
    """SPEC §6 C1: a LinkedIn **company** URL the company itself published, canonicalised.

    Returns ``None`` for anything that is not one — including ``linkedin.com/in/...``, which is
    a person (see :func:`extract_people`) and never a company contact.

    Canonicalising both sides is what makes "published beats constructed" free: when a footer
    links to ``https://www.linkedin.com/company/astranis/`` the value stored is byte-identical to
    :func:`constructed_linkedin_company_url`'s output, so the pipeline's upsert finds the
    existing constructed row and promotes it in place instead of writing a second one.
    """
    return _linkedin_url(url, "company") if url else None


def constructed_linkedin_company_url(domain: str | None) -> str | None:
    """SPEC §6 C2: ``https://www.linkedin.com/company/{slug}`` built from the company's domain.

    The slug is the registrable label, lowercased: ``astranis.com`` → ``astranis``,
    ``atom-computing.com`` → ``atom-computing``, ``baseten.co`` → ``baseten``,
    ``careers.foo.com`` → ``foo``, ``foo.co.uk`` → ``foo``. ``None`` when the company has no
    usable domain — a guessed slug for an unknown domain is a link to somebody else's company.

    This is a **search shortcut**, not an observation: it is stored with
    ``confidence='constructed'`` and the UI must say so (SPEC §6 C5).
    """
    labels = _registrable_labels(normalize_domain(domain))
    slug = labels[0].casefold() if labels else ""
    return f"{LINKEDIN_COMPANY_PREFIX}{slug}" if slug else None


def _search_term(term: str) -> str:
    """One title keyword as it appears in the search expression.

    A term already quoted in ``config/classifiers.yaml`` is passed through verbatim — SPEC §6
    writes the expression as ``founder OR recruiter OR "head of engineering" OR "talent"``, and
    the quotes are part of the term's meaning (a quoted term is matched as a phrase rather than
    stemmed), which is why they live in the config next to the word rather than in a code rule.
    A term the config leaves bare but that contains whitespace is quoted here so it can never
    fall apart into two ``OR`` operands.
    """
    stripped = term.strip()
    if len(stripped) > 1 and stripped.startswith('"') and stripped.endswith('"'):
        return stripped
    return f'"{stripped}"' if _WHITESPACE.search(stripped) else stripped


def people_search_url(company_name: str, terms: Sequence[str] | None = None) -> str:
    """SPEC §6 C3: the people-search deep link for one company.

    ``"<Company Name>" (founder OR recruiter OR "head of engineering" OR "talent")``,
    URL-encoded into LinkedIn's people-search endpoint. The title keywords come from
    ``config/classifiers.yaml`` (``contacts.people_search_terms``), never from Python
    (CLAUDE.md), and nothing else is appended.

    **No profile data is fetched or stored** (SPEC §6): this is a link for the user to click.
    """
    vocabulary = terms if terms is not None else load_classifiers().people_search_terms
    # A stray double quote in the name would close the phrase early and turn the rest of the
    # name into loose keywords, matching every company in the city.
    name = _WHITESPACE.sub(" ", company_name.replace('"', " ")).strip()
    expression = " OR ".join(_search_term(term) for term in vocabulary if term.strip())
    keywords = f'"{name}" ({expression})' if expression else f'"{name}"'
    return f"{PEOPLE_SEARCH_BASE}?{urlencode({'keywords': keywords})}"


# ------------------------------------------------------------------------------------ e-mail


def normalize_email(raw: str | None) -> str | None:
    """One published address in its stored form, or ``None`` if it is not an address.

    Accepts either a bare address or a ``mailto:`` href. The ``?subject=…`` tail is dropped,
    percent escapes are decoded, and a comma- or semicolon-separated list keeps its first
    address. The result is **lowercased in full**: ``mailto:HR@atom-computing.com`` is real
    markup, and storing it as published would collide with the same address seen in lowercase
    on another page under ``uq_contacts_company_id_kind_value``.

    A share widget's ``mailto:?subject=…`` has no address and yields ``None``, as does anything
    whose domain has no dot. **Nothing here guesses an address** (SPEC §6: never guess, never
    SMTP-verify) — the input is always something a company published.
    """
    if raw is None:
        return None
    value = raw.strip()
    if value.casefold().startswith("mailto:"):
        value = value[len("mailto:") :]
    value = unquote(value.split("?", 1)[0]).strip()
    value = _ADDRESS_SEPARATOR.split(value, maxsplit=1)[0].strip().strip("<>").strip()
    return value.casefold() if _EMAIL.fullmatch(value) else None


def extract_emails(markup: str) -> list[str]:
    """Every published address on one page, in document order, deduped (SPEC §6 C4).

    Only ``mailto:`` hrefs count: an address the company put behind a link is one it meant to
    publish, while a string that merely looks like an address in the page's script bundle is
    somebody's telemetry endpoint.
    """
    seen: dict[str, None] = {}
    for href in iter_links(markup):
        if not href.casefold().startswith("mailto:"):
            continue
        address = normalize_email(href)
        if address is not None:
            seen.setdefault(address, None)
    return list(seen)


# ---------------------------------------------------------------------------- social contacts


def _social_contact(url: str, company: str | None) -> tuple[enums.ContactKind, str] | None:
    """Classify one absolute link as a contact **candidate**, or ``None`` (SPEC §4 Tier 3, §6).

    Only the four shapes SPEC §4 calls "footer social links" are accepted, and each is
    canonicalised so the same account linked from two pages is one row. A path deeper than the
    shape allows is **rejected, not truncated** — the only exception is GitHub's
    ``<org>/<repo>``, where the org is exactly what a "we're on GitHub" link means.

    The shape says the link is *an* account, never *whose*: an "Our investors" block links a
    perfectly well-formed ``linkedin.com/company/<slug>`` that belongs to a fund. Only the
    contact form is decided here (it must be on the company's own domain); which account link
    is the company's own is :func:`select_social_contacts`'s question, because answering it
    needs every page of the visit at once.
    """
    parts = _split(url)
    if parts is None:
        return None
    host = normalize_domain(url)
    registrable = _registrable_domain(host)
    segments = _segments(parts)

    if registrable == _LINKEDIN:
        # ``/in/<slug>`` is a person (see extract_people) and must never become a Contact.
        canonical = _linkedin_url(url, "company")
        return (enums.ContactKind.LINKEDIN_COMPANY, canonical) if canonical else None

    if registrable in _X_HOSTS:
        if len(segments) != 1:
            return None
        handle = segments[0]
        if not _X_HANDLE.fullmatch(handle) or handle.casefold() in _NOT_AN_ACCOUNT:
            return None
        return (enums.ContactKind.X, f"https://x.com/{handle}")

    if registrable == _GITHUB:
        if not 1 <= len(segments) <= 2:
            return None
        owner = segments[0]
        if not _GITHUB_OWNER.fullmatch(owner):
            return None
        if owner.casefold() in _NOT_AN_ACCOUNT or owner.casefold() in _GITHUB_RESERVED:
            return None
        return (enums.ContactKind.GITHUB, f"https://github.com/{owner}")

    if company is not None and registrable == company and "contact" in parts.path.casefold():
        # The company's own contact form. The fragment is dropped and a trailing slash trimmed
        # so ``/contact``, ``/contact/`` and ``/contact#form`` are one row.
        path = parts.path.rstrip("/") or "/"
        query = f"?{parts.query}" if parts.query else ""
        return (enums.ContactKind.CONTACT_FORM, f"{parts.scheme}://{parts.netloc}{path}{query}")

    return None


def extract_social_contacts(markup: str, base_url: str) -> list[tuple[enums.ContactKind, str]]:
    """The account links and contact form one page offers, as **candidates** (SPEC §4 Tier 3
    "footer social links", §6).

    **Anywhere on the page, not only inside a ``<footer>``.** SPEC §4 and §6 both say "footer"
    because that is where a company puts these links, not because the element is the rule: of
    the sites probed for this phase, ``www.astranis.com`` has no ``<footer>`` element at all
    (Webflow renders the social column as a plain ``<div class="social-link footernew">``) and
    ``atom-computing.com``'s LinkedIn anchor sits in an Oxygen builder's social-icons block
    halfway up the page. A footer-scoped search would find nothing on either, including on the
    very page SPEC §6's C1 case comes from.

    What a footer *would* have given for free is the knowledge that the account belongs to this
    company rather than to an investor, a partner or the agency that built the site. The shape
    test in :func:`_social_contact` cannot supply that, so this function returns candidates and
    :func:`select_social_contacts` — which sees every page of the visit at once — decides which
    of them is the company's own. Only what that function keeps may be stored as ``published``.

    ``base_url`` is the page this markup came from: it resolves relative hrefs (``/contact``)
    and identifies the company's own registrable domain, which is what makes a contact form
    recognisable without the caller passing the domain separately.

    Returns ``(kind, value)`` pairs in document order with duplicates removed, and at most one
    contact form.
    """
    company = _registrable_domain(normalize_domain(base_url))
    found: dict[tuple[enums.ContactKind, str], None] = {}
    for href in iter_links(markup):
        if href.startswith("#"):
            continue  # a bare fragment resolves to this page, which is not a contact link
        # A ``mailto:``/``tel:``/``javascript:`` href resolves unchanged and is then rejected by
        # _split's scheme check, so no separate filter is needed here. An href that is not a URL
        # at all — an unrendered ``[[template]]`` in the authority — yields None and is skipped.
        url = resolve_url(base_url, href)
        contact = _social_contact(url, company) if url is not None else None
        if contact is not None:
            found.setdefault(contact, None)
    # A site has one contact page; the deeper paths that also contain "contact" are individual
    # forms on it (``sourcegraph.com/contact/request-info``). Keeping them all would fill the
    # panel's contacts block with near-duplicates, so the shallowest URL — the one a human would
    # call "their contact page" — wins, ties going to the first in document order.
    forms = [pair for pair in found if pair[0] is enums.ContactKind.CONTACT_FORM]
    keep = min(forms, key=lambda pair: (pair[1].count("/"), len(pair[1]))) if forms else None
    return [pair for pair in found if pair[0] is not enums.ContactKind.CONTACT_FORM or pair == keep]


def _identity_key(value: str) -> str:
    """``Atom Computing``, ``atom-computing`` and ``atom_computing`` all → ``atomcomputing``."""
    return _NOT_IDENTITY.sub("", value.casefold())


def _identities(domain: str | None, name: str | None) -> frozenset[str]:
    """The forms of the company's own identity an account name may be written in.

    The registrable label of its domain and its normalized name, both reduced to letters and
    digits: a company writes its LinkedIn slug, its X handle and its GitHub org from one of the
    two, with whatever separator each host prefers.
    """
    labels = _registrable_labels(normalize_domain(domain))
    keys = {_identity_key(labels[0])} if labels else set()
    if name:
        keys.add(_identity_key(normalize_name(name)))
    keys.discard("")
    return frozenset(keys)


def _account_name(value: str) -> str:
    """The slug, handle or org out of a canonical account URL."""
    return value.rsplit("/", 1)[-1]


def select_social_contacts(
    candidates: Sequence[tuple[enums.ContactKind, str]],
    *,
    domain: str | None,
    name: str | None = None,
) -> list[tuple[enums.ContactKind, str]]:
    """The candidates that are **this company's** accounts (SPEC §6 C1).

    SPEC §6 scopes the published company page to the company's own website footer: the point of
    "otherwise construct" is that a *wrong* LinkedIn page is worse than a search shortcut the UI
    labels as constructed. A page's markup rarely marks a footer (see
    :func:`extract_social_contacts`), so identity decides it instead — and over the whole visit
    at once, because the company's own link on the home page and an investor's on ``/about``
    have to be weighed against each other rather than page by page:

    1. the only candidate of its kind across the visit is the company's — that is the ordinary
       case, and it is what accepts ``sourcegraph.com``'s numeric ``/company/4803356`` and
       ``twelve.co``'s ``/company/twelveco2``, neither of which its domain could have produced.
       It is also the limit of what a page can prove: a site whose one GitHub link is a
       citation of somebody else's repository, and which publishes no org of its own, is
       indistinguishable from one whose org is named nothing like the company;
    2. otherwise the first candidate whose slug, handle or org is the company's own name or
       domain label — which is what keeps ``/company/acme-robotics`` and drops the
       ``/company/y-combinator`` its "Backed by" block links;
    3. otherwise **none** of that kind. Several accounts and no way to tell which is theirs is
       exactly SPEC §6's "otherwise": the pipeline then constructs a link to the right company
       and the UI says it is constructed.

    Contact forms pass straight through — :func:`_social_contact` has already checked that one
    is on the company's own domain. Order is preserved, duplicates dropped.
    """
    pairs = list(dict.fromkeys(candidates))
    by_kind: dict[enums.ContactKind, list[str]] = {}
    for kind, value in pairs:
        if kind in _ACCOUNT_KINDS:
            by_kind.setdefault(kind, []).append(value)

    identities = _identities(domain, name)
    chosen: dict[enums.ContactKind, str | None] = {}
    for kind, values in by_kind.items():
        if len(values) == 1:
            chosen[kind] = values[0]
            continue
        mine = [value for value in values if _identity_key(_account_name(value)) in identities]
        chosen[kind] = mine[0] if mine else None
        log.debug(
            "several accounts of one kind, only the company's own is published",
            kind=kind.value,
            kept=chosen[kind],
            dropped=[value for value in values if value != chosen[kind]],
            identities=sorted(identities),
        )
    return [
        pair for pair in pairs if pair[0] not in _ACCOUNT_KINDS or chosen.get(pair[0]) == pair[1]
    ]


# ------------------------------------------------------------------------------------ people


def _clean(text: str | None) -> str:
    """Published text as it will be stored: entities already decoded by the parser, Unicode
    format characters dropped, whitespace collapsed.

    Category ``Cf`` is the zero-width space, the joiners, the directional marks and the BOM —
    invisible characters that page builders sprinkle into headings (Atom Computing publishes
    "CEO<U+200B> & Founder"). Stored text with one in it breaks search, dedupe and every eyeball
    comparison, so it is dropped rather than preserved.
    """
    if not text:
        return ""
    visible = "".join(char for char in text if unicodedata.category(char) != "Cf")
    return _WHITESPACE.sub(" ", visible).strip()


def _credential(token: str) -> str:
    return token.replace(".", "").replace(" ", "").casefold()


def _strip_credentials(name: str) -> str:
    """``Ben Bloom, PhD`` → ``Ben Bloom``; ``Jane Doe, MD, MBA`` → ``Jane Doe``."""
    parts = [part.strip() for part in name.split(",")]
    while len(parts) > 1 and _credential(parts[-1]) in _CREDENTIALS:
        parts.pop()
    return ", ".join(part for part in parts if part).strip(" ,")


def _person_name(raw: str | None) -> str | None:
    """``raw`` as a person's name, or ``None`` when it does not look like one.

    The test is deliberately conservative: this text is going into ``Person.full_name`` and the
    alternative to rejecting a doubtful string is a row that says the company employs someone
    called "Executive Leadership".
    """
    text = _strip_credentials(_clean(raw))
    if not text or len(text) > _NAME_MAX_CHARS or "@" in text:
        return None
    if any(character.isdigit() for character in text):
        return None
    lowered = text.casefold()
    furniture = next((word for word in _NOT_A_NAME if word in lowered), None)
    if furniture is not None:
        # Logged because it is the one rejection that can silently cost a real person: these
        # words are matched anywhere in the string, so the vocabulary is worth tracing back to.
        log.debug("name candidate rejected as page furniture", candidate=text, matched=furniture)
        return None
    tokens = text.split()
    if not _NAME_MIN_TOKENS <= len(tokens) <= _NAME_MAX_TOKENS:
        return None
    # Two capitalised tokens: a name has a given name and a family name, a sentence fragment
    # ("our founding story") does not.
    if sum(1 for token in tokens if token[:1].isupper()) < 2:
        return None
    return text


def _is_element(node: Node) -> bool:
    """selectolax exposes text nodes and comments as pseudo-tags beginning with ``-``."""
    tag = node.tag
    return bool(tag) and not tag.startswith("-") and tag not in _INVISIBLE


def _sole_heading(node: Node) -> Node | None:
    """The one heading in ``node``'s subtree, or ``None`` when it holds none or several.

    "Several" is an answer, not a tie to break: a container with two headings is a section, and
    picking either of them would attach one person's name to another person's link.
    """
    if not _is_element(node):
        return None
    headings = node.css(_HEADING_SELECTOR)
    return headings[0] if len(headings) == 1 else None


def _profiles_in(node: Node) -> int:
    """How many ``linkedin.com/in/`` anchors ``node``'s subtree holds."""
    return sum(1 for link in node.css("a") if _profile_url(link.attributes.get("href")) is not None)


def _nearest_heading(anchor: Node) -> tuple[Node, bool] | None:
    """The heading of the card this anchor belongs to, and whether the anchor is *inside* it.

    Climbs from the anchor up to :data:`_MAX_ANCESTORS` ancestors and stops at the first one
    whose subtree holds a heading: that container is the person's own card, so the heading names
    this person whether the card prints the name above the photo link (the very common
    ``<a><img></a><h3>Name</h3>`` and photo-left/bio-right shapes) or below it.

    Deliberately **not** the "nearest preceding heading" walk. Reading backwards out of an
    anchor's own card lands in the *previous* card, which pairs every person on the page with
    the next person's profile URL and drops the last one — a silent mis-attribution, and the
    kind SPEC §6 cares about most because the stored profile link is what the user clicks.

    ``None`` when the first heading-bearing ancestor is a section rather than a card: it holds
    more than one heading, or more than one profile link, so which name goes with this link is a
    guess. No person is better than a confident wrong one.

    The flag matters for the title: when the anchor sits inside the heading itself, the text
    after that heading is this person's job title (see :func:`extract_people`).
    """
    node = anchor
    for _ in range(_MAX_ANCESTORS):
        parent = node.parent
        if parent is None:
            return None
        if parent.tag in _HEADINGS:  # the anchor is inside the heading itself
            return parent, True
        headings = parent.css(_HEADING_SELECTOR)
        if headings:
            if len(headings) > 1 or _profiles_in(parent) > 1:
                return None
            return headings[0], False
        node = parent
    return None


def _title_after(heading: Node, name: str) -> str | None:
    """The job title a team card publishes under the name, or ``None``.

    Only the **first** element following the heading counts. Taking the first *acceptable* one
    instead would walk past a long bio and return whatever short link came next ("Read Bio").
    """
    node = heading.next
    while node is not None:
        if _is_element(node):
            text = _clean(node.text())
            if text:
                if len(text) > _TITLE_MAX_CHARS or text.casefold() == name.casefold():
                    return None
                return text
        node = node.next
    return None


def _profile_url(href: str | None) -> str | None:
    """``href`` as a canonical ``linkedin.com/in/<slug>`` URL, or ``None``."""
    if not href:
        return None
    # Protocol-relative; the page itself is served over https.
    target = f"https:{href}" if href.startswith("//") else href
    return _linkedin_url(target, "in")


def _profile_anchors(markup: str) -> Iterator[tuple[Node, str]]:
    """Every ``<a>`` whose href is a LinkedIn personal profile, with the canonical URL.

    Anchors come from :func:`ingest.htmlutil.iter_anchors`, the same walk
    :func:`extract_emails` and :func:`extract_social_contacts` read through
    :func:`~ingest.htmlutil.iter_links`, so no page can offer a link to one extractor and hide
    it from another. Only the node is needed on top of the href: the name is printed beside the
    link, not in it.
    """
    for anchor, href in iter_anchors(markup):
        url = _profile_url(href)
        if url is not None:
            yield anchor, url


def extract_people(markup: str, *, classifiers: Classifiers | None = None) -> list[PersonRecord]:
    """The people a company published on one of its own pages (SPEC §5 ``Person``, §6).

    A person is a ``linkedin.com/in/<slug>`` link the company put on its site, plus the name and
    title printed next to it. ``linkedin_url`` is therefore only ever a URL the company itself
    published — **no profile is ever fetched** (SPEC §4 excludes LinkedIn, SPEC §6 stores no
    profile data), which is exactly what :class:`ingest.base.PersonRecord` promises.

    ``role_type`` is classified from the published title by ``config/classifiers.yaml``
    (``contacts.person_role_type``) and stays ``None`` when there is no title or no rule
    matches. It is never guessed from the name or the link.

    The name is the card's, never the neighbours'. A card may print it in a heading above the
    link, below it, beside it in a second column, or inside the link itself; all four are the
    same card, and :func:`_nearest_heading` finds the heading of the container the link is in
    rather than the nearest one earlier in the document (which is the *previous* person).

    Two guards keep section furniture out of ``Person``:

    * a name candidate must look like a name (:func:`_person_name`), which is what rejects the
      "Visit our LinkedIn" that Atom Computing's cards use as their anchor text;
    * **a name claimed by more than one profile URL on the same page is a heading, not a
      person**, and every person it produced is dropped. That is the failure mode a section
      heading above a row of cards produces, and it is silent otherwise.

    Results are in document order and deduped by profile URL, first occurrence winning.
    """
    found: dict[str, tuple[str, str | None]] = {}
    for anchor, url in _profile_anchors(markup):
        if url in found:
            continue
        name: str | None = None
        title: str | None = None
        own = _sole_heading(anchor)
        if own is not None:
            # The whole card is the link (``<a><img><h3>Jane Doe</h3><p>CTO</p></a>``), so the
            # anchor's own text is the card glued together — "Jane DoeCTO" on the minified
            # markup a real page serves. The heading it wraps is the name, and the text after
            # that heading is the title.
            name = _person_name(own.text())
            title = _title_after(own, name) if name is not None else None
        else:
            name = _person_name(anchor.text())
            nearest = _nearest_heading(anchor)
            heading, inside_heading = nearest if nearest is not None else (None, False)
            if heading is not None:
                # The heading names this person only when the name came out of it, or when the
                # anchor sits inside it. Otherwise the anchor's own text is the name and the
                # heading belongs to the card around it, whose next line is that card's title
                # for somebody else.
                from_heading = _person_name(heading.text()) if name is None else None
                name = name if name is not None else from_heading
                if name is not None and (from_heading is not None or inside_heading):
                    title = _title_after(heading, name)
        if name is not None:
            found[url] = (name, title)

    claims: dict[str, int] = {}
    for name, _ in found.values():
        claims[name.casefold()] = claims.get(name.casefold(), 0) + 1

    people: list[PersonRecord] = []
    for url, (name, title) in found.items():
        if claims[name.casefold()] > 1:
            log.debug("person name shared by several profiles, treated as a heading", name=name)
            continue
        role_type, keyword = classify_person_role(title, classifiers=classifiers)
        people.append(
            PersonRecord(full_name=name, title=title, role_type=role_type, linkedin_url=url)
        )
        log.debug(
            "person extracted",
            full_name=name,
            title=title,
            role_type=role_type.value if role_type else None,
            matched=keyword,
        )
    return people
