"""The seed loader: `config/seed_companies.yaml` → companies, with domain validation (SPEC §10).

Cold-starting from EDGAR alone is slow and the ATS connectors have nothing to discover board
tokens from, so `cli.py seed` bootstraps the database with the hand-picked companies of
SPEC §10 (58 entries, which §14.2 rounds to "55"). What the spec asks of the loader, and where
it happens here:

* **"resolve each domain (HEAD request, follow redirects)"** — :meth:`SeedConnector.validate`
  sends one ``HEAD https://{domain}/`` through the shared :class:`ingest.http.HttpClient`,
  which follows redirects one governed hop at a time and applies the rate limit and robots
  rules like any other fetch. A server that rejects ``HEAD`` (405/501) is retried with ``GET``.
* **"mark unreachable entries ``status='dead'`` and log rather than failing the run"** —
  see :func:`classify_response`: *unreachable* means the host never answered (DNS, connection,
  TLS, timeout — after the client's retries) or answered ``404``/``410``. Every other status,
  including the ``403`` that a bot-hostile WAF returns, means the host is alive and the entry
  keeps its status. Marking a live company dead because Cloudflare dislikes our User-Agent
  would be worse than useless.
* **"do not hardcode ATS tokens — let the discovery step in the connectors find them"** — the
  YAML has no token field at all and nothing here writes ``ats_provider``/``ats_token``.
* **"some entries may have been acquired or wound down, which the validation step will catch"**
  — a dead entry is still inserted, with ``status='dead'``: the database is the historical
  record (SPEC §2) and the UI can show what happened.

The loader is a :class:`~ingest.base.Connector` so it inherits the whole pipeline for free —
one ``FetchRun`` row (SPEC §7.2), priority-aware upserts, the denormalized refresh — but it is
deliberately **not** in :func:`ingest.connectors.all_connectors`, so ``refresh --all`` never
re-validates the whole bootstrap list on a schedule; ``cli.py seed`` builds it directly. It is
also unlisted in :data:`ingest.pipeline.CONNECTOR_PRIORITY` and so ranks below every connector:
a bootstrap value is replaced by the first connector that reports the same field for real.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import ClassVar

import httpx
import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from db import enums
from ingest.base import CompanyRecord, Connector, FetchContext, LocationRecord
from ingest.config import CONFIG_DIR, ConnectorConfig, RegionsConfig
from ingest.http import HostBudgetExceeded, RobotsDisallowed
from ingest.normalize import normalize_domain
from logging_config import get_logger

log = get_logger(__name__)

SEED_YAML = CONFIG_DIR / "seed_companies.yaml"

#: Statuses that mean "this domain is gone" (SPEC §10). Everything else — 200, 301 already
#: followed, 401, 403, 429, 500 — means a server answered, so the company is not dead.
GONE_STATUS = frozenset({404, 410})
#: A server that refuses ``HEAD`` outright; the probe is retried with ``GET``.
HEAD_UNSUPPORTED_STATUS = frozenset({405, 501})

_NON_WORD = re.compile(r"[^a-z0-9]+")


class SeedOptions(BaseModel):
    """``options`` for ``seed`` in ``config/connectors.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: Validate every domain over the network (SPEC §10). ``false`` loads the file as-is, which
    #: is what an offline first run wants.
    validate_domains: bool = True
    #: The scheme/path the probe uses. A bare host is not a URL; this is where it becomes one.
    probe_url_template: str = "https://{domain}/"


class SeedEntry(BaseModel):
    """One company in ``config/seed_companies.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1)
    domain: str = Field(min_length=1)
    city: str = Field(min_length=1)
    state: str | None = None
    sector: str | None = None
    #: Filled by :class:`SeedFile` from the group the entry was listed under.
    group: str = ""


class SeedFile(BaseModel):
    """``config/seed_companies.yaml``, validated on load."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    companies: dict[str, tuple[SeedEntry, ...]] = Field(min_length=1)
    sector_labels: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _inject_group(cls, data: object) -> object:
        """Copy each group key onto its entries so a record knows its sector hint."""
        if not isinstance(data, dict) or not isinstance(data.get("companies"), dict):
            return data
        groups = {
            group: [{**entry, "group": group} for entry in (entries or ())]
            for group, entries in data["companies"].items()
        }
        return {**data, "companies": groups}

    @property
    def entries(self) -> tuple[SeedEntry, ...]:
        return tuple(entry for entries in self.companies.values() for entry in entries)

    def sector_for(self, entry: SeedEntry) -> str | None:
        """The entry's own ``sector``, else the group's label from ``sector_labels``, else the
        group key title-cased with ``_and_`` read as ``&``."""
        if entry.sector:
            return entry.sector
        if not entry.group:
            return None
        label = self.sector_labels.get(entry.group)
        if label:
            return label
        return " ".join("&" if word == "and" else word.title() for word in entry.group.split("_"))


@cache
def load_seed_file(path: Path = SEED_YAML) -> SeedFile:
    with path.open(encoding="utf-8") as handle:
        return SeedFile.model_validate(yaml.safe_load(handle))


# ------------------------------------------------------------------------------ raw item


@dataclass(frozen=True, slots=True)
class SeedResult:
    """One entry after validation: the entry, the status it resolved to, the final URL the
    redirects landed on (``None`` when the host never answered) and a one-line reason."""

    entry: SeedEntry
    status: enums.CompanyStatus | None
    final_url: str | None
    reason: str


def classify_response(response: httpx.Response) -> tuple[enums.CompanyStatus | None, str]:
    """SPEC §10's reachability verdict for a response that *arrived*.

    ``404``/``410`` (:data:`GONE_STATUS`) is the only answer that marks a company ``dead``:
    the host is up and it says this site no longer exists. Anything else means a server
    answered and the company is not evidently gone, so the loader writes **no** status at all
    (``None``) and leaves whatever a more authoritative connector already recorded (SPEC §8) —
    a ``403`` from a bot-blocking WAF is a fact about us, not about the company.
    """
    status = response.status_code
    if status in GONE_STATUS:
        return enums.CompanyStatus.DEAD, f"HTTP {status} — the domain no longer serves a site"
    if 200 <= status < 400:
        return None, f"HTTP {status}"
    return None, f"HTTP {status} — the host answered, so the company is not marked dead"


class SeedConnector(Connector[SeedResult]):
    """The seed loader (SPEC §10, §12 Phase 3). See the module docstring."""

    name: ClassVar[str] = "seed"

    def __init__(
        self,
        config: ConnectorConfig,
        regions: RegionsConfig,
        *,
        seed_file: SeedFile | None = None,
    ) -> None:
        super().__init__(config, regions)
        self.options = SeedOptions.model_validate(config.options)
        self.seed = seed_file if seed_file is not None else load_seed_file()
        #: Entries dropped in :meth:`to_records`, by reason — the counter every connector keeps
        #: (``ycombinator``, ``hn_hiring``, ``sec_edgar``), so a seed entry that never reaches
        #: the database is counted in the same place a reader already looks for that number.
        self.skipped: dict[str, int] = {}

    # ------------------------------------------------------------------ fetch

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[SeedResult]:
        """Yield one :class:`SeedResult` per entry, validating its domain on the way.

        Every entry is yielded whatever the probe says: a company whose site is gone still
        belongs in the database, marked ``dead`` (SPEC §2 "the DB is the historical record").
        Validation problems are appended to ``ctx.problems`` so the run ends ``partial`` and
        the failures are visible on ``/runs`` — the run itself never fails (SPEC §10).
        """
        self.skipped = {}
        entries = self.seed.entries
        ctx.log.info(
            "seed.start", entries=len(entries), validate_domains=self.options.validate_domains
        )
        counts: dict[str, int] = {}
        for entry in entries:
            result = (
                await self.validate(ctx, entry)
                if self.options.validate_domains
                else SeedResult(entry, None, None, "domain validation disabled")
            )
            key = result.status.value if result.status is not None else "reachable"
            counts[key] = counts.get(key, 0) + 1
            yield result
        # Reached after the pipeline has mapped the last entry, so ``skipped`` is complete.
        ctx.log.info(
            "seed.summary", entries=len(entries), outcomes=counts, skipped=dict(self.skipped)
        )

    async def validate(self, ctx: FetchContext, entry: SeedEntry) -> SeedResult:
        """SPEC §10's domain validation for one entry: ``HEAD``, redirects followed.

        A ``405``/``501`` (the server refuses ``HEAD``) is retried once with ``GET``, which is
        the only way to tell "this host does not do HEAD" from "this host is gone". Transport
        failures that outlive the client's retries, and robots or budget refusals, are the
        *unreachable* case: the entry is marked ``dead`` and the reason recorded.
        """
        url = self.options.probe_url_template.format(domain=entry.domain)
        try:
            response = await ctx.http.head(url)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in HEAD_UNSUPPORTED_STATUS:
                return await self._probe_with_get(ctx, entry, url)
            return self._from_response(entry, exc.response)
        except httpx.TransportError as exc:
            return self._unreachable(ctx, entry, url, f"{type(exc).__name__}: {exc}")
        except RobotsDisallowed as exc:
            return self._from_robots_refusal(ctx, entry, url, exc)
        except HostBudgetExceeded as exc:
            # Our own limit, not the company's fault — do not mark it dead.
            ctx.problems.append(f"{entry.domain}: not probed: {exc}")
            ctx.log.info("seed.not_probed", domain=entry.domain, error=str(exc))
            return SeedResult(entry, None, None, f"not probed: {exc}")
        return self._from_response(entry, response)

    async def _probe_with_get(self, ctx: FetchContext, entry: SeedEntry, url: str) -> SeedResult:
        try:
            response = await ctx.http.get(url)
        except httpx.HTTPStatusError as exc:
            return self._from_response(entry, exc.response)
        except httpx.TransportError as exc:
            return self._unreachable(ctx, entry, url, f"{type(exc).__name__}: {exc}")
        except RobotsDisallowed as exc:
            return self._from_robots_refusal(ctx, entry, url, exc)
        except HostBudgetExceeded as exc:
            ctx.problems.append(f"{entry.domain}: not probed: {exc}")
            return SeedResult(entry, None, None, f"not probed: {exc}")
        return self._from_response(entry, response)

    def _from_robots_refusal(
        self, ctx: FetchContext, entry: SeedEntry, url: str, exc: RobotsDisallowed
    ) -> SeedResult:
        """Two very different things arrive here (:class:`~ingest.http.RobotsDisallowed`).

        ``reason="unreachable"`` means robots.txt could not be read at all — for a bare domain
        that is a dead host, which is exactly what SPEC §10 asks this loader to record.
        ``reason="disallowed"`` means the host answered and excluded us: it is alive, so its
        status is left alone and the skip is reported instead.
        """
        if exc.reason == "unreachable":
            return self._unreachable(ctx, entry, url, str(exc))
        ctx.problems.append(f"{entry.domain}: not probed: {exc}")
        ctx.log.info("seed.not_probed", domain=entry.domain, error=str(exc))
        return SeedResult(entry, None, None, f"not probed: {exc}")

    def _from_response(self, entry: SeedEntry, response: httpx.Response) -> SeedResult:
        status, reason = classify_response(response)
        final_url = str(response.request.url)
        log.info(
            "seed.validated",
            domain=entry.domain,
            http_status=response.status_code,
            company_status=status.value if status else None,
            final_url=final_url,
        )
        return SeedResult(entry, status, final_url, reason)

    def _unreachable(self, ctx: FetchContext, entry: SeedEntry, url: str, error: str) -> SeedResult:
        reason = f"unreachable: {error}"
        ctx.problems.append(f"{entry.domain}: marked dead — {reason}")
        ctx.log.warning("seed.unreachable", domain=entry.domain, url=url, error=error)
        return SeedResult(entry, enums.CompanyStatus.DEAD, None, reason)

    # ------------------------------------------------------------------ to_records

    def to_records(self, raw: SeedResult) -> Iterable[CompanyRecord]:
        """One :class:`~ingest.base.CompanyRecord` per entry.

        ``website_url`` is the URL the redirects settled on, so a seed whose site moved keeps
        a link that works. The canonical ``domain`` stays the one in the YAML even when the
        redirect crossed to another host: the file is the authority for what a seed entry *is*,
        and quietly re-keying it would let one typo swallow another company's row (SPEC §8 —
        a domain that already belongs elsewhere becomes a ``MergeCandidate``, never a merge).
        A cross-domain redirect is logged instead.

        ``state`` comes from ``config/regions.yaml`` (the only place region logic lives): the
        entry names a city, the config says which state and metro it is in. An entry that names
        a state is looked up on the pair; one that does not is looked up by name across the
        enabled regions (:meth:`~ingest.config.RegionsConfig.lookup_city_name`, which answers
        ``None`` for a bare name two enabled *states* configure rather than guessing one of
        them — whether those two states sit in different regions or, as the mapping form
        allows, in the same one). Either
        lookup also matches a city's configured *aliases* — the other spellings the file records
        for that same place — and answers with the entry itself, so ``configured.city`` below is
        the canonical spelling however the seed wrote it, and one place keeps one row
        (``uq_locations_city_state``) instead of gaining a second under the seed's own wording.

        A city the config cannot resolve — in no enabled region, or a bare name two enabled
        states both configure — is **skipped**, not an error: with several metros configured,
        switching one off with ``enabled: false`` is a one-line config edit, and every seeded
        company in that metro would otherwise crash ``make seed`` on the first entry instead of
        loading the rest. The entry is counted under ``outside_region`` — the reason every other
        connector uses for a record outside the configured regions, one key so one number
        answers "how many seeds did not land" — and logged at *warning* rather than the
        connectors' ``info``, because ``config/seed_companies.yaml`` is a hand-written file: a
        city no region claims is a mistake in one of the two configs, not the routine national
        noise a connector filters all day. The log line carries the entry's ``city`` and
        ``state``, which is what tells the two cases apart; the cure for an ambiguous name is a
        ``state:`` on the seed entry.
        """
        entry = raw.entry
        configured = self.regions.lookup(entry.city, entry.state) if entry.state else None
        if configured is None:
            configured = self.regions.lookup_city_name(entry.city)
        if configured is None:
            return self._skip("outside_region", entry, state=entry.state)

        if raw.final_url is not None:
            final_domain = normalize_domain(raw.final_url)
            seed_domain = normalize_domain(entry.domain)
            if final_domain is not None and seed_domain is not None and final_domain != seed_domain:
                log.info(
                    "seed.redirected_off_domain",
                    name=entry.name,
                    domain=seed_domain,
                    final_domain=final_domain,
                    final_url=raw.final_url,
                )

        sector = self.seed.sector_for(entry)
        return [
            CompanyRecord(
                name=entry.name,
                external_id=_slug(entry.name),
                source_url=raw.final_url,
                domain=entry.domain,
                website_url=raw.final_url,
                status=raw.status,
                locations=(
                    LocationRecord(
                        city=configured.city,
                        state=configured.state,
                        country=configured.country,
                        metro=configured.metro,
                        is_hq=True,
                    ),
                ),
                sectors=(sector,) if sector else (),
            )
        ]

    def _skip(self, reason: str, entry: SeedEntry, **details: object) -> tuple[CompanyRecord, ...]:
        """Drop one entry, counted and logged — nothing else in the run changes."""
        self.skipped[reason] = self.skipped.get(reason, 0) + 1
        log.warning(
            "seed.skip",
            reason=reason,
            name=entry.name,
            domain=entry.domain,
            city=entry.city,
            **details,
        )
        return ()


def _slug(name: str) -> str:
    """``CompanySource.external_id`` for a seed entry: stable across runs and independent of
    the domain, so re-seeding after a rename still resolves to the same company (SPEC §8
    step 0)."""
    return _NON_WORD.sub("-", name.casefold()).strip("-")
