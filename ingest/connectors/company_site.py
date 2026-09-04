"""Direct company-site enrichment — the only Tier-3 connector (SPEC §4 Tier 3).

For companies **already in the database**, fetch the company's own site to fill in what the
APIs do not carry: the one-liner from its ``og:description`` / ``<meta name="description">``,
the canonical ``website_url`` the redirects settle on, and the careers page it publishes.

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

Scope in this phase. Contacts are only the careers page here (``kind='careers_page'``,
``confidence='published'`` — the company published the URL itself). Published ``mailto:``
addresses, footer social links, ``Person`` rows and the *constructed* LinkedIn search links are
SPEC §6 and are built in Phase 5 (SPEC §12); this connector is the fetch-and-parse half they
will hang off.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar
from urllib.parse import urljoin, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field

from db import enums
from ingest.base import (
    CompanyRecord,
    CompanyTarget,
    Connector,
    ContactRecord,
    FetchContext,
)
from ingest.config import ConnectorConfig, RegionsConfig
from ingest.htmlutil import iter_links, meta_content, page_title
from ingest.http import HostBudgetExceeded, RobotsDisallowed
from ingest.normalize import normalize_domain
from logging_config import get_logger

log = get_logger(__name__)

#: Pages this connector fetches per company per run: the home page plus at most two depth-1
#: pages. SPEC §4 Tier 3 caps a domain at five per run and the client enforces that too.
MAX_PAGES_PER_COMPANY = 3
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
        """Same-site links from the home page whose path matches ``depth_one_paths``.

        Depth 1 means *found on the home page* (SPEC §4 Tier 3: "never follow deeper than depth
        1 from the homepage"). Links to another registrable domain are dropped: a company's
        careers page hosted on its ATS is that ATS's site, not this company's, and fetching it
        here would spend a Tier-3 budget on a Tier-1 API.
        """
        base = home.url
        base_host = urlsplit(base).hostname or ""
        base_domain = normalize_domain(base_host)
        found: list[str] = []
        seen: set[str] = set()
        for href in iter_links(home.markup):
            if href.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue
            url = urljoin(base, href)
            split = urlsplit(url)
            if split.scheme not in ("http", "https"):
                continue
            if normalize_domain(split.hostname or "") != base_domain:
                continue
            path = split.path.casefold()
            if not any(fragment in path for fragment in self.options.depth_one_paths):
                continue
            canonical = url.split("#", 1)[0]
            if canonical in seen or canonical.rstrip("/") == base.rstrip("/"):
                continue
            seen.add(canonical)
            found.append(canonical)
        return found

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
        form of a bare seeded domain.
        """
        if raw.home is None:  # pragma: no cover - fetch never yields a visit without a home
            return ()
        markup = raw.home.markup
        description = meta_content(markup, "og:description", "description", "twitter:description")
        title = page_title(markup)
        one_liner = _shorten(description or _strip_brand(title), ONE_LINER_LIMIT)
        thesis = _shorten(description, THESIS_LIMIT) if description else None
        contacts: tuple[ContactRecord, ...] = ()
        if raw.careers_url:
            contacts = (
                ContactRecord(
                    kind=enums.ContactKind.CAREERS_PAGE,
                    value=raw.careers_url,
                    # SPEC §6: the company published this URL on its own site.
                    confidence=enums.ContactConfidence.PUBLISHED,
                ),
            )
        return [
            CompanyRecord(
                name=raw.target.name,
                external_id=raw.target.domain,
                source_url=raw.home.url,
                domain=raw.target.domain,
                website_url=raw.home.url,
                one_liner=one_liner,
                thesis=thesis if thesis != one_liner else None,
                contacts=contacts,
                enrich_only=True,
            )
        ]


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
