"""Direct company-site enrichment — the only Tier-3 connector (SPEC §4 Tier 3).

For companies **already in the database**, fetch the company's own site to fill in what the
APIs do not carry: the one-liner from its ``og:description`` / ``<meta name="description">``,
the canonical ``website_url`` the redirects settle on, and the contact details SPEC §6 allows —
the careers page, published ``mailto:`` addresses, the footer's social links, and the people the
company names on its own team page.

This is the only connector that touches arbitrary third-party sites, so SPEC §4's rules for the
tier are all in force and all enforced by the shared client from this connector's
``config/connectors.yaml`` block:

* ``robots.txt`` is parsed and honoured **before every request** (``respect_robots: true``);
* at most one request per domain per two seconds (``requests_per_second: 0.5``), stretched
  further by a host's own ``Crawl-delay``;
* a hard cap of five pages per domain per run (``max_requests_per_host_per_run: 5``), on top of
  which :data:`MAX_PAGES_PER_COMPANY` bounds what this connector even asks for;
* responses are cached for 24 h with ``ETag``/``Last-Modified``;
* **never deeper than depth 1** — the home page, then links found on it that match
  ``options.depth_one_paths`` and stay on the same registrable domain. A link off-site or two
  hops down is never followed (:meth:`CompanySiteConnector._depth_one_urls`).

``options.max_companies_per_run`` bounds the whole run. The pipeline hands targets over
least-recently-visited first (:func:`db.queries.load_company_targets`), so successive Saturday
runs work through the entire database rather than re-fetching the same first N sites.

Contacts and people (SPEC §6). Everything this connector emits is ``confidence='published'``:
the company itself put the value on a page of its own. The rules that turn a page into contacts
live in :mod:`ingest.contacts` — one home for them, because :mod:`ingest.pipeline` needs the
same canonicalisation to recognise that a published LinkedIn URL and a constructed one are the
same row. The connector never emits a ``constructed`` contact: the deterministic
``linkedin.com/company/{slug}`` link and the "Find people →" search link belong to *every*
company, including the ones this connector has never visited, so the pipeline builds them.
Nothing on ``linkedin.com`` is ever fetched (SPEC §4 Excluded); a ``Person`` is a profile *link*
the company published beside a name it published, and no profile is ever requested.

All of that extraction happens in the pure :meth:`to_records`, over the pages :meth:`fetch`
already collected. It costs no extra request, so the Tier-3 budget above is untouched by it,
and it is testable from a recorded :class:`SitePage` with no HTTP at all.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field

from db import enums
from ingest.base import (
    CompanyRecord,
    CompanyTarget,
    Connector,
    ContactRecord,
    FetchContext,
    PersonRecord,
)
from ingest.config import ConnectorConfig, RegionsConfig
from ingest.contacts import (
    extract_emails,
    extract_people,
    extract_social_contacts,
    resolve_url,
    select_social_contacts,
)
from ingest.htmlutil import iter_links, meta_content, page_title
from ingest.http import HostBudgetExceeded, RobotsDisallowed
from ingest.normalize import normalize_domain
from logging_config import get_logger

log = get_logger(__name__)

#: Pages this connector fetches per company per run: the home page plus one depth-1 page for
#: each of SPEC §4 Tier 3's four paths ("``/careers``, ``/jobs``, ``/about``, ``/team``"). That
#: is exactly the tier's "hard cap of 5 pages per domain per run", which the client enforces
#: too — a lower number here would decide *which* of the four to skip by the home page's link
#: order, and SPEC §6's material is not interchangeable between them: the published address is
#: on the careers page and the people are on the team page.
MAX_PAGES_PER_COMPANY = 5
#: ``Company.one_liner`` is a chip on a list row (SPEC §9); a 900-character meta description is
#: a paragraph, so the short field is capped and the full text goes to ``thesis``.
ONE_LINER_LIMIT = 300
THESIS_LIMIT = 4000

_WHITESPACE = re.compile(r"\s+")
#: A title's trailing brand suffix — "Careers | Astranis", "About – Sourcegraph".
_TITLE_SUFFIX = re.compile(r"\s*[|·—–-]\s*[^|·—–-]{1,40}$")


class CompanySiteOptions(BaseModel):
    """``options`` for ``company_site`` in ``config/connectors.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: Companies visited per run. Politeness, not performance: this is somebody else's server.
    max_companies_per_run: int = Field(default=40, ge=1)
    #: Path fragments a depth-1 link must contain to be followed (SPEC §4 Tier 3: "``/careers``,
    #: ``/jobs``, ``/about``, ``/team``").
    depth_one_paths: tuple[str, ...] = ("careers", "jobs", "about", "team")
    #: Fragments that identify the careers page among the followed links.
    careers_paths: tuple[str, ...] = ("careers", "jobs")
    #: Revisit a company at most this often. The weekly cadence plus the per-run cap already
    #: spaces visits out; this makes the rule explicit and testable.
    min_revisit_days: int = Field(default=6, ge=0)
    #: SPEC §6: published ``mailto:`` addresses kept per company. A site that prints a regional
    #: sales alias on every page would otherwise turn the panel's contacts block into a
    #: directory; the earliest addresses in page order are the ones on the home and careers
    #: pages, which is what SPEC §6 asks for. ``0`` stores no address at all.
    max_emails_per_company: int = Field(default=10, ge=0)
    #: SPEC §5 ``Person`` / §6: people read off a published team page. An "our team" page can
    #: list several hundred; the leadership sits at the top of it. ``0`` stores no person.
    max_people_per_company: int = Field(default=25, ge=0)


# --------------------------------------------------------------------------------- raw item


@dataclass(frozen=True, slots=True)
class SitePage:
    """One page fetched from a company's own site."""

    url: str
    markup: str


@dataclass(frozen=True, slots=True)
class SiteVisit:
    """Everything one company's site gave up in one run."""

    target: CompanyTarget
    home: SitePage | None
    pages: tuple[SitePage, ...]
    careers_url: str | None


# ------------------------------------------------------------------------------- connector


class CompanySiteConnector(Connector[SiteVisit]):
    """Tier-3 company-site enrichment (SPEC §4 Tier 3, §12 Phase 3)."""

    name: ClassVar[str] = "company_site"
    wants_targets: ClassVar[bool] = True

    def __init__(self, config: ConnectorConfig, regions: RegionsConfig) -> None:
        super().__init__(config, regions)
        self.options = CompanySiteOptions.model_validate(config.options)
        self.stats: dict[str, int] = {}

    # ------------------------------------------------------------------ fetch

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[SiteVisit]:
        """Visit up to ``max_companies_per_run`` companies, home page then depth 1."""
        self.stats = {}
        visited = 0
        cutoff = ctx.now - timedelta(days=self.options.min_revisit_days)
        ctx.log.info(
            "company_site.start",
            targets=len(ctx.targets),
            budget=self.options.max_companies_per_run,
            min_revisit_days=self.options.min_revisit_days,
        )
        for target in ctx.targets:
            if visited >= self.options.max_companies_per_run:
                break
            if target.last_seen_at is not None and target.last_seen_at > cutoff:
                # The list is ordered oldest-first, so everything after this is fresher still.
                self._count("visited_recently")
                break
            if target.home_url is None:
                self._count("no_website")
                continue
            visit = await self._visit(ctx, target)
            if visit is None:
                continue
            visited += 1
            yield visit
        ctx.log.info(
            "company_site.summary",
            visited=visited,
            outcomes=dict(self.stats),
            requests=ctx.http.stats.requests,
            problems=len(ctx.problems),
        )

    async def _visit(self, ctx: FetchContext, target: CompanyTarget) -> SiteVisit | None:
        home_url = target.home_url
        if home_url is None:  # pragma: no cover - guarded by the caller
            return None
        home = await self._page(ctx, target, home_url)
        if home is None:
            self._count("home_unreachable")
            return None
        pages = [home]
        careers_url: str | None = None
        for url in self._depth_one_urls(home):
            if len(pages) >= MAX_PAGES_PER_COMPANY:
                break
            page = await self._page(ctx, target, url)
            if page is None:
                continue
            pages.append(page)
            if careers_url is None and self._is_careers(page.url):
                careers_url = page.url
        self._count("visited")
        return SiteVisit(target=target, home=home, pages=tuple(pages), careers_url=careers_url)

    async def _page(self, ctx: FetchContext, target: CompanyTarget, url: str) -> SitePage | None:
        """One governed fetch. Every refusal — robots, budget, 4xx, transport — is a debug log
        and a ``None``; SPEC §4 says a Tier-3 site may simply decline, and one company's
        Cloudflare must not colour the run."""
        try:
            response = await ctx.http.get(url)
        except RobotsDisallowed as exc:
            self._count("robots_disallowed")
            ctx.log.info("company_site.robots_disallowed", company=target.name, url=url)
            log.debug("company_site.robots", error=str(exc))
            return None
        except HostBudgetExceeded:
            self._count("budget_exceeded")
            ctx.log.info("company_site.budget_exceeded", company=target.name, url=url)
            return None
        except httpx.HTTPStatusError as exc:
            self._count(f"http_{exc.response.status_code}")
            ctx.log.info(
                "company_site.http_error",
                company=target.name,
                url=url,
                status=exc.response.status_code,
            )
            return None
        except httpx.TransportError as exc:
            self._count("transport_error")
            ctx.log.info(
                "company_site.transport_error", company=target.name, url=url, error=str(exc)
            )
            return None
        content_type = response.headers.get("content-type", "")
        if "html" not in content_type.casefold() and content_type:
            self._count("not_html")
            return None
        # The URL after redirects, which is what ``website_url`` should record.
        return SitePage(url=str(response.request.url), markup=response.text)

    def _depth_one_urls(self, home: SitePage) -> list[str]:
        """One same-site link per ``depth_one_paths`` fragment, shallowest first-seen wins.

        Depth 1 means *found on the home page* (SPEC §4 Tier 3: "never follow deeper than depth
        1 from the homepage"). Links to another registrable domain are dropped: a company's
        careers page hosted on its ATS is that ATS's site, not this company's, and fetching it
        here would spend a Tier-3 budget on a Tier-1 API.

        **One candidate per fragment**, because the budget is four pages and SPEC §4 names four
        *sections*. A home page links its careers section several times over — ``/careers`` in
        the nav, ``/careers/`` in the footer, ``/careers/employee-spotlights/`` in a teaser
        (atom-computing.com publishes all three) — and keeping every distinct string would spend
        the whole budget inside one section and never reach ``/team``. The shallowest path wins,
        which is the section's own page rather than an article inside it.

        An href that is not a URL at all is skipped: ``urljoin`` raises on an unbalanced bracket
        in the authority (an unrendered ``[[siteUrl]]`` template), and one such link on one
        company's home page must not end the run for every company behind it in the queue.
        """
        base = home.url
        base_host = urlsplit(base).hostname or ""
        base_domain = normalize_domain(base_host)
        best: dict[str, str] = {}
        for href in iter_links(home.markup):
            if href.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue
            url = resolve_url(base, href)
            if url is None:
                continue
            split = urlsplit(url)
            if split.scheme not in ("http", "https"):
                continue
            if normalize_domain(split.hostname or "") != base_domain:
                continue
            path = split.path.casefold()
            fragment = next((f for f in self.options.depth_one_paths if f in path), None)
            if fragment is None:
                continue
            canonical = url.split("#", 1)[0]
            if canonical.rstrip("/") == base.rstrip("/"):
                continue
            current = best.get(fragment)
            if current is None or _depth(canonical) < _depth(current):
                best[fragment] = canonical
        return list(best.values())

    def _is_careers(self, url: str) -> bool:
        path = urlsplit(url).path.casefold()
        return any(fragment in path for fragment in self.options.careers_paths)

    def _count(self, key: str) -> None:
        self.stats[key] = self.stats.get(key, 0) + 1

    # ------------------------------------------------------------------ to_records

    def to_records(self, raw: SiteVisit) -> Iterable[CompanyRecord]:
        """One ``enrich_only`` record per visited company.

        ``enrich_only`` because this connector only ever visits companies the pipeline handed
        it: it cannot discover a company, and a redirect that landed somewhere unexpected must
        not create one. ``one_liner`` and ``thesis`` come from the home page's description
        metas; ``website_url`` is the URL the redirects settled on, which is often the ``www.``
        form of a bare seeded domain. Contacts and people are read from **every** page the
        visit fetched — a ``mailto:`` usually lives on the careers page and the team lives on
        ``/about`` — and every one of them is ``published`` (SPEC §6).
        """
        if raw.home is None:  # pragma: no cover - fetch never yields a visit without a home
            return ()
        markup = raw.home.markup
        description = meta_content(markup, "og:description", "description", "twitter:description")
        title = page_title(markup)
        one_liner = _shorten(description or _strip_brand(title), ONE_LINER_LIMIT)
        thesis = _shorten(description, THESIS_LIMIT) if description else None
        return [
            CompanyRecord(
                name=raw.target.name,
                external_id=raw.target.domain,
                source_url=raw.home.url,
                domain=raw.target.domain,
                website_url=raw.home.url,
                one_liner=one_liner,
                thesis=thesis if thesis != one_liner else None,
                contacts=self._contacts(raw),
                people=self._people(raw),
                enrich_only=True,
            )
        ]

    def _contacts(self, raw: SiteVisit) -> tuple[ContactRecord, ...]:
        """Every contact the visit found, in a stable order (SPEC §4 Tier 3, §6).

        Careers page first, then ``mailto:`` addresses, then the footer's social links — each
        group in the order the pages were fetched and, within a page, in document order. That
        order is what decides which contacts survive ``options.max_emails_per_company`` and the
        one-``contact_form``-per-visit rule below, so the same pages always yield the same tuple
        and a weekly re-run of an unchanged site *adds* nothing. It does still write: each
        contact is upserted on its ``(company_id, kind, value)`` and re-observing a published one
        refreshes that row's ``source_id`` to the run that last saw it (:mod:`ingest.pipeline`).

        The account links are chosen across the **whole visit** rather than page by page
        (:func:`ingest.contacts.select_social_contacts`): a footer link on the home page has to
        beat an investor's link on ``/about``, and one page's only LinkedIn URL is not evidence
        of anything if another page carries the company's own. What SPEC §6 gets out of that is
        its "otherwise": when the visit cannot tell whose account a link is, this connector
        publishes none of that kind and the pipeline constructs the link instead.

        All of it is ``confidence='published'``: SPEC §6 lets this connector store only what
        the company itself put on its own site. The *constructed* LinkedIn links §6 also
        requires are built by :mod:`ingest.pipeline` for every company, visited or not.
        """
        contacts: list[ContactRecord] = []
        if raw.careers_url:
            contacts.append(_published(enums.ContactKind.CAREERS_PAGE, raw.careers_url))

        emails: dict[str, None] = {}
        social: dict[tuple[enums.ContactKind, str], None] = {}
        for page in raw.pages:
            for address in extract_emails(page.markup):
                emails.setdefault(address, None)
            for pair in extract_social_contacts(page.markup, page.url):
                social.setdefault(pair, None)

        kept = list(emails)[: self.options.max_emails_per_company]
        if len(kept) < len(emails):
            log.debug(
                "company_site.emails_capped",
                company=raw.target.name,
                found=len(emails),
                cap=self.options.max_emails_per_company,
            )
        contacts.extend(_published(enums.ContactKind.EMAIL, address) for address in kept)

        # ``extract_social_contacts`` already keeps one contact form per page; a site has one
        # contact page, so the first one across the visit — the home page's, since ``pages[0]``
        # is the home page — wins and a careers page's "contact recruiting" variant is dropped
        # rather than filed as a second way to reach the same company.
        seen_form = False
        mine = select_social_contacts(list(social), domain=raw.target.domain, name=raw.target.name)
        for kind, value in mine:
            if kind is enums.ContactKind.CONTACT_FORM:
                if seen_form:
                    continue
                seen_form = True
            contacts.append(_published(kind, value))
        return tuple(contacts)

    def _people(self, raw: SiteVisit) -> tuple[PersonRecord, ...]:
        """The people the company named on its own pages, in page order (SPEC §5, §6).

        Deduped by profile URL *and* by name: :mod:`ingest.pipeline` matches a person on
        ``(company_id, lower(full_name))``, so two records sharing a name in one visit would be
        an insert followed immediately by an update of the same row — and the second of them,
        being a different profile link for the same name, is far more likely to be a page's
        repeated furniture than a genuine namesake.
        """
        if self.options.max_people_per_company == 0:
            # The option is a kill switch, so re-parsing every page for anchors whose people
            # would all be discarded is work nobody asked for.
            return ()
        people: list[PersonRecord] = []
        urls: set[str] = set()
        names: set[str] = set()
        for page in raw.pages:
            for person in extract_people(page.markup):
                name = person.full_name.casefold()
                url = person.linkedin_url
                if name in names or (url is not None and url in urls):
                    continue
                if len(people) >= self.options.max_people_per_company:
                    log.debug(
                        "company_site.people_capped",
                        company=raw.target.name,
                        cap=self.options.max_people_per_company,
                    )
                    return tuple(people)
                names.add(name)
                if url is not None:
                    urls.add(url)
                people.append(person)
        return tuple(people)


def _published(kind: enums.ContactKind, value: str) -> ContactRecord:
    """One contact this connector saw on the company's own site.

    Always ``published`` — the connector reads pages the company controls, so every value it
    can return is one the company chose to publish (SPEC §6). Nothing here is ever guessed or
    constructed.
    """
    return ContactRecord(kind=kind, value=value, confidence=enums.ContactConfidence.PUBLISHED)


def _depth(url: str) -> tuple[int, int]:
    """How far into a site a URL reaches: path segments first, then length as the tie-break so
    ``/about`` beats ``/about/`` and the choice never depends on document order."""
    path = urlsplit(url).path
    return (len([segment for segment in path.split("/") if segment]), len(url))


def _shorten(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    text = _WHITESPACE.sub(" ", value).strip()
    if not text:
        return None
    return text if len(text) <= limit else text[:limit].rstrip() + "…"


def _strip_brand(title: str | None) -> str | None:
    """ "Code Understanding | Sourcegraph" → "Code Understanding".

    Only used when a site publishes no description at all; a bare brand name ("Sourcegraph")
    would shorten to nothing, so a title with no separator is returned unchanged.
    """
    if title is None:
        return None
    stripped = _TITLE_SUFFIX.sub("", title).strip()
    return stripped or title
