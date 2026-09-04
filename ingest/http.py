"""Shared async HTTP client for every connector (SPEC §3, §4).

One :class:`HttpClient` per run enforces the source-etiquette rules so no connector can forget
them:

* **Per-host rate limiting** — at most ``rate_limit.requests_per_second`` requests to one host
  (SEC: ≤ 10/s; Tier-3 company sites: 1 per 2 s), and an optional per-run page budget per host
  (Tier 3: 5 pages per domain per run) — :class:`HostBudgetExceeded` when it is spent.
* **Retries** — ``tenacity`` with exponential backoff and jitter on transport errors, timeouts,
  429 and 5xx; ``Retry-After`` is honoured; other 4xx are never retried.
* **robots.txt** — fetched once per host (cached 24 h) and checked before every request;
  :class:`RobotsDisallowed` is raised *without* sending the request. Rules are matched per
  RFC 9309 by :class:`_RobotsRules` (``*`` and ``$`` wildcards, most-specific-rule-wins), not
  by ``urllib.robotparser``, which predates the RFC and would silently ignore both. A
  robots.txt that answers 4xx (missing, 401, 403) allows everything; a 5xx or a transport
  failure (after the same retries as any page) disallows the origin for the rest of the run;
  ``Crawl-delay`` stretches the per-host interval when longer.
* **Caching** — every response with an ``ETag`` or ``Last-Modified`` is stored; a later request
  for the same URL within 24 h is sent conditionally and a ``304`` is answered from the cache
  (``response.extensions["from_cache"] is True``). :class:`MemoryCache` for tests and one-off
  runs, :class:`FileCache` to persist across scheduled runs.
* **User-Agent** — ``"startup-tracker/0.1 contact@example.com"`` (see
  ``Settings.user_agent``). SEC's fair-access policy requires a contact email, and its WAF
  rejects anything more elaborate: parentheses, URLs, or an email without a real domain
  (``dev@localhost``) come back as 403 "Undeclared Automated Tool".

Connectors call :meth:`HttpClient.get` / :meth:`get_json` / :meth:`get_text`, plus
:meth:`head` (SPEC §10's seed-domain validation) and :meth:`post_json` (Algolia's search
endpoint, SPEC §4 Tier 1 #2) — and nothing else; tests mock the wire with ``respx`` and
recorded fixtures (SPEC §3).
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import dataclasses
import email.utils
import functools
import hashlib
import json
import os
import re
import tempfile
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx
import tenacity

from ingest.config import RateLimit
from logging_config import get_logger

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_ATTEMPTS = 5
CACHE_TTL_SECONDS = 24 * 60 * 60  # SPEC §4: cache with ETag/Last-Modified for 24 h
ROBOTS_TTL_SECONDS = 24 * 60 * 60
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

ParamValue = str | int | float
Params = Mapping[str, ParamValue]

# Redirects are followed by hand (design decision 11b) so that every hop runs through robots,
# budget and rate limit; httpx's own default of 20 is far more than a company site needs.
_MAX_REDIRECTS = 5
_REDIRECT_STATUS = frozenset({301, 302, 303, 307, 308})
# ``Retry-After`` is honoured for these statuses (design decision 12); anything larger than the
# cap is treated as the cap — a run must never park itself for an hour on one server's say-so.
_RETRY_AFTER_STATUS = frozenset({429, 503})
_RETRY_AFTER_CAP_SECONDS = 120.0
# httpx hands back the *decoded* body, so a replayed response must not claim to be encoded or
# sized; the rest of the headers (content-type, validators, ...) are replayed verbatim.
_UNCACHEABLE_HEADERS = frozenset({"content-encoding", "content-length", "transfer-encoding"})
_ROBOTS_PATH = "/robots.txt"
# httpx builds ``response.next_request`` even with ``follow_redirects=False`` and reports an
# unparsable ``Location`` header as a ``RemoteProtocolError`` with this message
# (``httpx._client.BaseClient._redirect_url``). It is a deterministic server answer, not a
# transport failure, so it must not be retried.
_INVALID_LOCATION_MESSAGE = "Invalid URL in location header"


class HttpClientError(Exception):
    """Base class for the client's own errors (transport/status errors stay ``httpx``'s)."""


class RobotsDisallowed(HttpClientError):
    """``robots.txt`` forbids this URL for our user agent; the request was not sent.

    ``reason`` separates the two ways that happens, because they say opposite things about the
    host. ``"disallowed"`` means a robots.txt was read and it excludes this path — the host is
    up and has told us no. ``"unreachable"`` means robots.txt could not be read at all (a 5xx,
    or a transport failure that outlived the retry policy), which RFC 9309 §2.3.1.4 says to
    treat as a full disallow — and which usually means the host itself is down. The seed
    loader (SPEC §10) needs the difference: an unreachable host is a dead domain, while a host
    that answered "no" is very much alive.
    """

    def __init__(self, url: str, user_agent: str, reason: str = "disallowed") -> None:
        detail = (
            "robots.txt could not be read" if reason == "unreachable" else "robots.txt disallows"
        )
        super().__init__(f"{detail} {url} for {user_agent!r}")
        self.url = url
        self.user_agent = user_agent
        self.reason = reason


class HostBudgetExceeded(HttpClientError):
    """The per-run request budget for this host is spent; the request was not sent."""

    def __init__(self, host: str, budget: int) -> None:
        super().__init__(f"per-run budget of {budget} requests to {host} exhausted")
        self.host = host
        self.budget = budget


@dataclass(frozen=True, slots=True)
class CacheEntry:
    """A cached response body with its validators. ``stored_at`` is a wall-clock timestamp
    (``time.time()``), so entries stay valid across processes."""

    status_code: int
    headers: dict[str, str]
    body: bytes
    etag: str | None
    last_modified: str | None
    stored_at: float


class HttpCache(Protocol):
    def get(self, url: str) -> CacheEntry | None: ...

    def set(self, url: str, entry: CacheEntry) -> None: ...


class MemoryCache:
    """Process-local cache — tests and one-off CLI runs."""

    def __init__(self) -> None:
        self._entries: dict[str, CacheEntry] = {}

    def get(self, url: str) -> CacheEntry | None:
        return self._entries.get(url)

    def set(self, url: str, entry: CacheEntry) -> None:
        self._entries[url] = entry

    def __len__(self) -> int:
        return len(self._entries)


def _optional_str(value: object) -> str | None:
    """A validator field from a cache file: a string, ``None``, or corrupt (``TypeError``)."""
    if value is None or isinstance(value, str):
        return value
    raise TypeError(f"expected str or None, got {type(value).__name__}")


class FileCache:
    """One JSON file per URL under ``directory`` (``Settings.http_cache_dir``), so the 24 h
    window survives across scheduled runs."""

    def __init__(self, directory: Path) -> None:
        # Created lazily on the first write, so constructing a client never touches the disk.
        self._directory = directory
        self._log = get_logger(__name__)

    @property
    def directory(self) -> Path:
        return self._directory

    def path_for(self, url: str) -> Path:
        """``<directory>/<sha256(url)>.json`` — the URL itself is kept inside the file."""
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self._directory / f"{digest}.json"

    def get(self, url: str) -> CacheEntry | None:
        """The stored entry, or ``None`` when there is none or the file is unreadable/corrupt
        (a damaged cache file is a miss, never an error — the fetch simply goes unconditional)."""
        path = self.path_for(url)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return CacheEntry(
                status_code=int(raw["status_code"]),
                headers={str(key): str(value) for key, value in raw["headers"].items()},
                body=base64.b64decode(raw["body"], validate=True),
                etag=_optional_str(raw.get("etag")),
                last_modified=_optional_str(raw.get("last_modified")),
                stored_at=float(raw["stored_at"]),
            )
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            # ValueError covers json.JSONDecodeError and binascii.Error alike.
            if path.exists():
                self._log.warning("http.cache_corrupt", url=url, path=str(path))
            return None

    def set(self, url: str, entry: CacheEntry) -> None:
        """Write the entry atomically (unique temp file + rename) so a crash mid-write leaves
        either the old file or the new one, never a truncated one.

        The temp name is unique per writer: the scheduler and a manual ``cli.py refresh`` share
        ``Settings.http_cache_dir`` and may store the same URL at the same moment. With a fixed
        name one rename could publish the other's half-written file (read back as corrupt) or
        fail outright; with unique names each rename publishes a complete file, last writer wins.
        """
        payload = {
            "url": url,
            "status_code": entry.status_code,
            "headers": entry.headers,
            "body": base64.b64encode(entry.body).decode("ascii"),
            "etag": entry.etag,
            "last_modified": entry.last_modified,
            "stored_at": entry.stored_at,
        }
        path = self.path_for(url)
        tmp_name: str | None = None
        try:
            self._directory.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self._directory,
                prefix=f"{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as tmp:
                tmp_name = tmp.name
                tmp.write(json.dumps(payload))
            os.replace(tmp_name, path)
        except OSError as exc:
            # The cache is an optimisation (SPEC §4); a full or read-only disk must not fail a run.
            self._log.warning("http.cache_write_failed", url=url, path=str(path), error=str(exc))
            if tmp_name is not None:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_name)


@dataclass(slots=True)
class HttpStats:
    """Counters for the run summary and tests."""

    requests: int = 0
    retries: int = 0
    cache_hits: int = 0
    robots_denied: int = 0
    by_host: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _RobotsRule:
    """One ``Allow:`` / ``Disallow:`` line: the compiled matcher and the pattern's octet
    count, which decides precedence between rules that both match (RFC 9309 §2.2.2)."""

    allow: bool
    matcher: re.Pattern[str]
    length: int


def _compile_robots_pattern(pattern: str) -> re.Pattern[str]:
    """Compile one robots.txt path pattern (RFC 9309 §2.2.2).

    Patterns match from the start of the path. ``*`` stands for any sequence of octets and a
    trailing ``$`` anchors the end of the path; every other character is literal. ``urllib``'s
    parser implements neither wildcard — it compares literal prefixes — so ``Disallow: /*?``
    and ``Disallow: /*.pdf$`` (both common, and both present in www.sec.gov's own file) would
    silently never match.
    """
    anchored = pattern.endswith("$")
    body = pattern[:-1] if anchored else pattern
    compiled = "".join(".*" if char == "*" else re.escape(char) for char in body)
    return re.compile(f"{compiled}$" if anchored else compiled)


class _RobotsRules:
    """The rules of one ``robots.txt`` that apply to one product token (RFC 9309 §2.2).

    Replaces :class:`urllib.robotparser.RobotFileParser`, which predates RFC 9309: it ignores
    ``*``/``$`` in patterns, resolves conflicts by first match rather than by the most
    specific one, and drops a fractional ``Crawl-delay``. Each of those silently *widens* what
    we fetch, and this client is the single point where SPEC §4's "honor robots.txt before
    every fetch" is enforced for every connector.
    """

    def __init__(self, rules: tuple[_RobotsRule, ...], crawl_delay: float | None) -> None:
        self._rules = rules
        self.crawl_delay = crawl_delay

    @classmethod
    def parse(cls, lines: Iterable[str], product: str) -> _RobotsRules:
        """Parse ``robots.txt`` and keep the group that applies to ``product``.

        Groups are introduced by one or more ``User-agent`` lines and every group naming the
        same token is merged (§2.2.1). Blank lines carry no meaning in RFC 9309 — they do
        *not* end a group, which is why www.sec.gov's ``Allow: /Archives/edgar/data`` (after a
        blank line and a comment) still belongs to its ``User-agent: *`` group. The group for
        our own token wins over ``*``; when neither is present nothing is restricted.
        """
        groups: dict[str, list[tuple[str, str]]] = {}
        agents: list[str] = []
        in_header = False
        for raw_line in lines:
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            raw_field, _, value = line.partition(":")
            field_name = raw_field.strip().lower()
            value = value.strip()
            if field_name == "user-agent":
                if not in_header:  # a new group starts after the previous group's rules
                    agents = []
                    in_header = True
                agents.append(value.lower())
                groups.setdefault(value.lower(), [])
                continue
            in_header = False
            for agent in agents:  # a directive before any User-agent line belongs to no group
                groups[agent].append((field_name, value))

        directives = groups.get(product.lower())
        if directives is None:
            directives = groups.get("*", [])
        rules: list[_RobotsRule] = []
        crawl_delay: float | None = None
        for field_name, value in directives:
            if field_name in ("allow", "disallow"):
                # "Disallow:" with an empty value imposes no restriction (§2.2.2).
                if value:
                    rules.append(
                        _RobotsRule(
                            field_name == "allow", _compile_robots_pattern(value), len(value)
                        )
                    )
            elif field_name == "crawl-delay":
                with contextlib.suppress(ValueError):
                    crawl_delay = float(value)
        return cls(tuple(rules), crawl_delay)

    def can_fetch(self, path: str) -> bool:
        """Whether ``path`` (the percent-encoded path plus query) may be fetched.

        RFC 9309 §2.2.2: the most specific match wins, measured by the pattern's octet count,
        and ``Allow`` wins a tie. No matching rule means allowed.
        """
        best: _RobotsRule | None = None
        for rule in self._rules:
            if rule.matcher.match(path) is None:
                continue
            if best is None or (rule.length, rule.allow) > (best.length, best.allow):
                best = rule
        return best is None or best.allow


@dataclass(frozen=True, slots=True)
class _RobotsEntry:
    """What we know about one origin's ``robots.txt`` (design decision 10).

    ``rules=None`` means the file could not be fetched (5xx or transport failure, after the
    retry policy gave up); RFC 9309 §2.3.1.4 and design decision 10 both say to assume
    everything is disallowed. That verdict is remembered exactly as long as a parsed file
    would be (:data:`ROBOTS_TTL_SECONDS`, longer than any run), so an outage disallows the
    origin *for the whole run* rather than letting a long run start fetching part-way through
    once the file comes back — and a dead host is not hammered with a retry burst per URL.
    """

    rules: _RobotsRules | None
    crawl_delay: float | None
    fetched_at: float  # monotonic seconds — the entry lives in memory for one client only

    def is_fresh(self, now: float) -> bool:
        return now - self.fetched_at < ROBOTS_TTL_SECONDS


def _rate_key(url: httpx.URL) -> str:
    """The rate-limit / budget bucket for ``url``: the host, lowercased, with one leading
    ``www.`` stripped so ``example.com`` and ``www.example.com`` share it (CLAUDE.md "per
    domain"; design decision 11)."""
    return (url.host or "").lower().removeprefix("www.")


def _robots_key(url: httpx.URL) -> str:
    """robots.txt is scoped to scheme + host + port (RFC 9309 §2.3), *not* to the ``www.``-less
    domain: ``www.example.com`` and ``example.com`` may publish different files."""
    port = f":{url.port}" if url.port is not None else ""
    return f"{url.scheme.lower()}://{(url.host or '').lower()}{port}"


def _product_token(user_agent: str) -> str:
    """``startup-tracker`` from ``startup-tracker/0.1 you@example.com`` — the token robots.txt
    ``User-agent`` lines are matched against (RFC 9309 §2.2.1)."""
    words = user_agent.split()
    first = words[0] if words else user_agent
    return first.split("/", 1)[0]


def _retry_after_seconds(value: str | None, *, now: float) -> float | None:
    """Seconds to wait for a ``Retry-After`` header — delta-seconds or an HTTP-date (RFC 9110
    §10.2.3) — clamped to ``[0, _RETRY_AFTER_CAP_SECONDS]``; ``None`` when absent or unparseable."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if value.isdigit():
        seconds = float(value)
    else:
        try:
            when = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:  # "-0000" dates come back naive; HTTP dates are always UTC
            when = when.replace(tzinfo=UTC)
        seconds = when.timestamp() - now
    return min(max(seconds, 0.0), _RETRY_AFTER_CAP_SECONDS)


def _cacheable_headers(headers: httpx.Headers) -> dict[str, str]:
    """The response headers worth replaying from the cache (keys come back lowercased)."""
    return {key: value for key, value in headers.items() if key not in _UNCACHEABLE_HEADERS}


def _is_retryable(exc: BaseException) -> bool:
    """Design decision 12: transport errors (timeouts included) and the statuses in
    :data:`RETRYABLE_STATUS` are retried; every other failure propagates at once.

    The one transport-typed error that is *not* retried is httpx's report of a malformed
    ``Location`` header (:data:`_INVALID_LOCATION_MESSAGE`): the server will send the same
    broken redirect every time, and retrying it would burn ``max_attempts`` requests, the
    exponential backoff, and a Tier-3 host's whole page budget on one bad header.
    """
    if isinstance(exc, httpx.RemoteProtocolError) and str(exc).startswith(
        _INVALID_LOCATION_MESSAGE
    ):
        return False
    if isinstance(exc, httpx.TransportError):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in RETRYABLE_STATUS


def _describe_failure(retry_state: tenacity.RetryCallState) -> str:
    outcome = retry_state.outcome
    exc = outcome.exception() if outcome is not None and outcome.failed else None
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    if exc is not None:
        return f"{type(exc).__name__}: {exc}"
    return "unknown"


class HttpClient:
    """The shared client. See the module docstring for the rules it enforces.

    ``clock`` (monotonic seconds), ``sleep`` and ``wall_clock`` are injectable so tests can run
    the rate limiter, retry backoff and cache expiry without waiting.
    """

    def __init__(
        self,
        *,
        user_agent: str,
        rate_limit: RateLimit | None = None,
        respect_robots: bool = True,
        cache: HttpCache | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if max_attempts < 1:
            msg = f"max_attempts must be at least 1, got {max_attempts}"
            raise ValueError(msg)
        self._user_agent = user_agent
        self._product = _product_token(user_agent)
        self._rate_limit = rate_limit if rate_limit is not None else RateLimit()
        self._respect_robots = respect_robots
        self._cache: HttpCache = cache if cache is not None else MemoryCache()
        self._max_attempts = max_attempts
        self._clock = clock
        self._sleep = sleep
        self._wall_clock = wall_clock
        self._stats = HttpStats()
        self._log = get_logger(__name__)
        self._backoff = tenacity.wait_exponential_jitter(initial=1, max=30)
        # Design decision 11b: redirects are followed by :meth:`get` itself, one governed hop at
        # a time. Design decision 7: the User-Agent goes on every request, robots.txt included.
        self._client = httpx.AsyncClient(
            transport=transport,
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
            follow_redirects=False,
        )
        self._closed = False
        # Per-host rate limiting and budget (design decision 11), keyed by :func:`_rate_key`.
        self._host_locks: dict[str, asyncio.Lock] = {}
        self._last_slot_at: dict[str, float] = {}
        self._crawl_delays: dict[str, float] = {}
        self._budget_used: dict[str, int] = {}
        # robots.txt per origin (design decision 10), keyed by :func:`_robots_key`.
        self._robots: dict[str, _RobotsEntry] = {}
        self._robots_locks: dict[str, asyncio.Lock] = {}

    @property
    def user_agent(self) -> str:
        return self._user_agent

    @property
    def stats(self) -> HttpStats:
        return self._stats

    # The etiquette settings the client was built with (SPEC §4). Read-only, and exposed so a
    # caller that assembles a client — ``cli.py`` from ``config/connectors.yaml`` and
    # ``Settings`` — can be tested for passing the *configured* limits rather than the defaults.

    @property
    def rate_limit(self) -> RateLimit:
        return self._rate_limit

    @property
    def respect_robots(self) -> bool:
        return self._respect_robots

    @property
    def cache(self) -> HttpCache:
        return self._cache

    # ------------------------------------------------------------------ public API

    async def get(
        self,
        url: str,
        *,
        params: Params | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """GET ``url`` honouring robots.txt, the per-host rate limit and budget, the cache, and
        the retry policy. Raises :class:`RobotsDisallowed`, :class:`HostBudgetExceeded`, or
        ``httpx.HTTPStatusError`` (after retries are exhausted for retryable statuses; at once
        for other 4xx).

        Redirects (301/302/303/307/308) are followed here rather than by httpx (design decision
        11b): every hop is a governed request — robots → budget → rate limit → send — so a
        Tier-3 site bouncing ``example.com → www.example.com`` cannot escape SPEC §4's rules,
        and every hop shows up in :attr:`stats`. More than ``_MAX_REDIRECTS`` hops raise
        ``httpx.TooManyRedirects``. Every returned response carries
        ``extensions["from_cache"]``.
        """
        return await self.request("GET", url, params=params, headers=headers)

    async def head(
        self,
        url: str,
        *,
        params: Params | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """HEAD ``url`` under exactly the same rules as :meth:`get`, following redirects.

        This is what SPEC §10 asks the seed loader to do to validate a domain ("resolve each
        domain (HEAD request, follow redirects)"): the status and the final URL are all it
        needs, and no body crosses the wire. Nothing is read from or written to the cache — a
        HEAD has no body to store, and storing its validators would let a later ``get`` of the
        same URL be answered by a 304 with no body at all.

        A server that answers ``405 Method Not Allowed`` or ``501 Not Implemented`` raises
        ``httpx.HTTPStatusError`` like any other 4xx/5xx; callers that need a body-less probe to
        work everywhere fall back to :meth:`get` (see ``ingest/seed.py``).
        """
        return await self.request("HEAD", url, params=params, headers=headers)

    async def post_json(
        self,
        url: str,
        *,
        content: str | bytes,
        params: Params | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        """POST ``content`` and decode the JSON answer, under the same rules as :meth:`get`.

        The one connector that needs it is ``ycombinator``: Algolia's search endpoint takes the
        query in a POST body and the credentials in headers (SPEC §4 Tier 1 #2). Nothing is
        cached — a POST is not a cache key — and redirects are *not* followed, because
        re-posting a body to a new location is a decision no client should make silently.
        """
        response = await self.request("POST", url, params=params, headers=headers, content=content)
        return response.json()

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Params | None = None,
        headers: Mapping[str, str] | None = None,
        content: str | bytes | None = None,
    ) -> httpx.Response:
        """:meth:`get`, :meth:`head` and :meth:`post_json` in one: the governed request loop.

        Redirects are followed for ``GET`` and ``HEAD`` only (see :meth:`get`); any other
        method returns the 3xx response to its caller untouched.
        """
        follow_redirects = method in ("GET", "HEAD")
        request_url = httpx.URL(url)
        if params:
            request_url = request_url.copy_merge_params(dict(params))
        extra_headers = dict(headers or {})
        for _hop in range(_MAX_REDIRECTS + 1):
            response = await self._fetch(request_url, extra_headers, method=method, content=content)
            location = response.headers.get("location")
            if (
                not follow_redirects
                or response.status_code not in _REDIRECT_STATUS
                or location is None
            ):
                return response
            next_url = request_url.join(location)
            await response.aclose()
            self._log.debug(
                "http.redirect",
                url=str(request_url),
                status=response.status_code,
                location=str(next_url),
            )
            request_url = next_url
        msg = f"Exceeded maximum allowed redirects ({_MAX_REDIRECTS}) fetching {url}"
        raise httpx.TooManyRedirects(msg, request=httpx.Request(method, request_url))

    async def get_json(
        self,
        url: str,
        *,
        params: Params | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        response = await self.get(url, params=params, headers=headers)
        return response.json()

    async def get_text(
        self,
        url: str,
        *,
        params: Params | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        response = await self.get(url, params=params, headers=headers)
        return response.text

    async def aclose(self) -> None:
        """Close the underlying httpx client. Idempotent: a second call is a no-op."""
        if self._closed:
            return
        self._closed = True
        await self._client.aclose()

    async def __aenter__(self) -> HttpClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ one hop

    async def _fetch(
        self,
        url: httpx.URL,
        extra_headers: dict[str, str],
        *,
        method: str = "GET",
        content: str | bytes | None = None,
    ) -> httpx.Response:
        """One redirect hop: robots check, then :meth:`_send_once` under the retry policy."""
        host = _rate_key(url)
        await self._check_robots(url, host)
        return await self._with_retries(
            url,
            functools.partial(
                self._send_once, url, host, extra_headers, method=method, content=content
            ),
        )

    async def _with_retries(
        self, url: httpx.URL, send: Callable[[], Awaitable[httpx.Response]]
    ) -> httpx.Response:
        """Run one ``send`` attempt under the retry policy — the same policy for page fetches
        and for robots.txt, so a transient blip on either is retried, not recorded.

        Design decision 12: ``tenacity`` with ``stop_after_attempt(max_attempts)`` and
        ``wait_exponential_jitter(initial=1, max=30)``; ``reraise=True`` so the *last* error —
        ``HTTPStatusError`` for a status failure, the ``TransportError`` otherwise — propagates
        instead of ``tenacity.RetryError``. Non-retryable failures (other 4xx,
        :class:`HostBudgetExceeded`, a malformed redirect) leave the loop on the first attempt.
        """

        def on_retry(retry_state: tenacity.RetryCallState) -> None:
            self._stats.retries += 1
            action = retry_state.next_action
            self._log.warning(
                "http.retry",
                url=str(url),
                attempt=retry_state.attempt_number,
                reason=_describe_failure(retry_state),
                wait_seconds=round(action.sleep, 3) if action is not None else None,
            )

        async for attempt in tenacity.AsyncRetrying(
            sleep=self._sleep,
            stop=tenacity.stop_after_attempt(self._max_attempts),
            wait=self._wait_before_retry,
            retry=tenacity.retry_if_exception(_is_retryable),
            before_sleep=on_retry,
            reraise=True,
        ):
            with attempt:
                return await send()
        msg = "tenacity loop ended without a result"  # pragma: no cover - reraise=True forbids it
        raise AssertionError(msg)

    def _wait_before_retry(self, retry_state: tenacity.RetryCallState) -> float:
        """Exponential backoff with jitter, stretched to ``Retry-After`` when a 429/503 sent one
        (design decision 12: wait *at least* that long, capped at 120 s)."""
        wait = self._backoff(retry_state)
        outcome = retry_state.outcome
        exc = outcome.exception() if outcome is not None and outcome.failed else None
        if (
            isinstance(exc, httpx.HTTPStatusError)
            and exc.response.status_code in _RETRY_AFTER_STATUS
        ):
            retry_after = _retry_after_seconds(
                exc.response.headers.get("retry-after"), now=self._wall_clock()
            )
            if retry_after is not None:
                wait = max(wait, retry_after)
        return wait

    async def _send_once(
        self,
        url: httpx.URL,
        host: str,
        extra_headers: dict[str, str],
        *,
        method: str = "GET",
        content: str | bytes | None = None,
    ) -> httpx.Response:
        """One attempt: budget → rate limit → conditional request → classify the answer.

        The budget is taken *before* sending and never given back — retries and redirect hops
        are real requests to the host (design decision 11) — and the (N+1)th raises
        :class:`HostBudgetExceeded` before waiting for a slot. The cache (SPEC §4, 24 h) turns
        the request conditional; a ``304`` becomes the cached body (design decision 9). A
        ``HEAD`` neither reads nor writes the cache: it carries no body to store.
        """
        self._take_budget(host)
        await self._wait_for_slot(host)

        cacheable = method == "GET"
        cache_key = str(url)
        entry = self._fresh_entry(cache_key) if cacheable else None
        request_headers = dict(extra_headers)
        if entry is not None:
            if entry.etag:
                request_headers["If-None-Match"] = entry.etag
            if entry.last_modified:
                request_headers["If-Modified-Since"] = entry.last_modified
        request = self._client.build_request(method, url, headers=request_headers, content=content)
        self._count_request(host)
        response = await self._client.send(request)
        status = response.status_code

        if status == 304:
            if entry is None:
                msg = f"HTTP 304 for {cache_key} without a conditional request"
                raise httpx.HTTPStatusError(msg, request=request, response=response)
            self._stats.cache_hits += 1
            # RFC 9111 §4.3.4: a 304 revalidates the stored entry — refresh its clock and pick
            # up any validators the server sent along.
            refreshed = dataclasses.replace(
                entry,
                etag=response.headers.get("etag", entry.etag),
                last_modified=response.headers.get("last-modified", entry.last_modified),
                stored_at=self._wall_clock(),
            )
            self._cache.set(cache_key, refreshed)
            self._log.debug("http.cache_hit", url=cache_key)
            return httpx.Response(
                refreshed.status_code,
                headers=refreshed.headers,
                content=refreshed.body,
                request=request,
                extensions={"from_cache": True},
            )
        if 200 <= status < 300:
            if cacheable:
                self._store(cache_key, response)
            response.extensions["from_cache"] = False
            return response
        if status in _REDIRECT_STATUS:
            response.extensions["from_cache"] = False
            return response
        reason = f" {response.reason_phrase}" if response.reason_phrase else ""
        msg = f"HTTP {status}{reason} for {cache_key}"
        raise httpx.HTTPStatusError(msg, request=request, response=response)

    # ------------------------------------------------------------------ rate limit + budget

    async def _wait_for_slot(self, host: str) -> None:
        """Design decision 11: one ``asyncio.Lock`` and one "last slot" monotonic timestamp per
        host; the next request may go out ``interval`` after it, where the interval is
        ``rate_limit.min_interval`` or the host's ``Crawl-delay``, whichever is longer.

        The interval is evaluated *when waiting*, not when the previous slot was taken, so a
        ``Crawl-delay`` learned from the robots.txt fetch already spaces the very first page
        request after it. A single sleep, never a spin: the lock is held while waiting so
        concurrent callers queue up behind it in order.
        """
        lock = self._host_locks.setdefault(host, asyncio.Lock())
        async with lock:
            now = self._clock()
            last = self._last_slot_at.get(host)
            if last is not None:
                interval = max(self._rate_limit.min_interval, self._crawl_delays.get(host, 0.0))
                next_allowed = last + interval
                if next_allowed > now:
                    await self._sleep(next_allowed - now)
                    now = max(self._clock(), next_allowed)
            self._last_slot_at[host] = now

    def _take_budget(self, host: str) -> None:
        """SPEC §4 Tier 3: at most ``max_requests_per_host_per_run`` requests per host per run."""
        budget = self._rate_limit.max_requests_per_host_per_run
        used = self._budget_used.get(host, 0)
        if budget is not None and used >= budget:
            raise HostBudgetExceeded(host, budget)
        self._budget_used[host] = used + 1

    def _count_request(self, host: str) -> None:
        """Every request actually sent — page fetches, retries, redirect hops and robots.txt —
        lands in :attr:`stats`; only the page fetches count against the budget."""
        self._stats.requests += 1
        self._stats.by_host[host] = self._stats.by_host.get(host, 0) + 1

    # ------------------------------------------------------------------ cache

    def _fresh_entry(self, cache_key: str) -> CacheEntry | None:
        """The cached entry unless it is older than 24 h (SPEC §4) — a stale entry is ignored
        and replaced by the full fetch that follows."""
        entry = self._cache.get(cache_key)
        if entry is None or self._wall_clock() - entry.stored_at >= CACHE_TTL_SECONDS:
            return None
        return entry

    def _store(self, cache_key: str, response: httpx.Response) -> None:
        """Store a 2xx that carries a validator (``ETag`` and/or ``Last-Modified``)."""
        etag = response.headers.get("etag")
        last_modified = response.headers.get("last-modified")
        if etag is None and last_modified is None:
            return
        self._cache.set(
            cache_key,
            CacheEntry(
                status_code=response.status_code,
                headers=_cacheable_headers(response.headers),
                body=response.content,
                etag=etag,
                last_modified=last_modified,
                stored_at=self._wall_clock(),
            ),
        )

    # ------------------------------------------------------------------ robots.txt

    async def _check_robots(self, url: httpx.URL, host: str) -> None:
        """SPEC §4: honour robots.txt before every request. ``respect_robots=False`` (an
        official API) skips the whole mechanism."""
        if not self._respect_robots:
            return
        entry = await self._robots_for(url, host)
        # RFC 9309 §2.2.2 matches the percent-encoded path *and* query, which is what
        # ``raw_path`` holds — patterns such as ``Disallow: /*?`` only work against it.
        path = url.raw_path.decode("ascii", "replace")
        allowed = entry.rules is not None and entry.rules.can_fetch(path)
        if not allowed:
            reason = "unreachable" if entry.rules is None else "disallowed"
            self._stats.robots_denied += 1
            self._log.info(
                "http.robots_denied", url=str(url), user_agent=self._user_agent, reason=reason
            )
            raise RobotsDisallowed(str(url), self._user_agent, reason)

    async def _robots_for(self, url: httpx.URL, host: str) -> _RobotsEntry:
        """The origin's robots.txt, fetched once per client and kept for 24 h in memory —
        whether it was parsed or turned out to be unreachable (design decision 10; see
        :class:`_RobotsEntry`). A ``Crawl-delay`` for our agent (or ``*``) stretches the
        host's interval."""
        key = _robots_key(url)
        lock = self._robots_locks.setdefault(key, asyncio.Lock())
        async with lock:
            entry = self._robots.get(key)
            if entry is not None and entry.is_fresh(self._clock()):
                return entry
            entry = await self._fetch_robots(url, host)
            self._robots[key] = entry
            if entry.crawl_delay is not None and entry.crawl_delay > self._crawl_delays.get(
                host, 0.0
            ):
                self._crawl_delays[host] = entry.crawl_delay
            return entry

    async def _fetch_robots(self, url: httpx.URL, host: str) -> _RobotsEntry:
        """RFC 9309 §2.3.1: 2xx → parse; 4xx (401/403 included) → no restrictions; 5xx or a
        transport failure that outlasts the retry policy → everything disallowed, remembered
        for the rest of the run (:class:`_RobotsEntry`). The fetch is rate-limited and retried
        like any other request but does not consume the per-run host budget."""
        robots_url = url.copy_with(path=_ROBOTS_PATH, query=None, fragment=None)
        try:
            response = await self._get_robots(robots_url)
        except httpx.HTTPStatusError as exc:
            # A retryable status (429 / 5xx) that survived every attempt: classify it below.
            response = exc.response
        except httpx.TransportError as exc:
            self._log.warning(
                "http.robots_unreachable", url=str(robots_url), error=f"{type(exc).__name__}: {exc}"
            )
            return _RobotsEntry(rules=None, crawl_delay=None, fetched_at=self._clock())

        if response is None or 400 <= response.status_code < 500:
            # Unavailable (RFC 9309 §2.3.1.3) — or lost in redirects (§2.3.1.2): unrestricted.
            # An empty rule set is exactly that: ``can_fetch`` answers True for every path.
            self._log.debug("http.robots_unavailable", url=str(robots_url), host=host)
            return _RobotsEntry(
                rules=_RobotsRules((), None), crawl_delay=None, fetched_at=self._clock()
            )
        if not 200 <= response.status_code < 300:
            self._log.warning(
                "http.robots_unreachable", url=str(robots_url), status=response.status_code
            )
            return _RobotsEntry(rules=None, crawl_delay=None, fetched_at=self._clock())
        rules = _RobotsRules.parse(response.text.splitlines(), self._product)
        self._log.debug("http.robots_loaded", url=str(robots_url), crawl_delay=rules.crawl_delay)
        return _RobotsEntry(rules=rules, crawl_delay=rules.crawl_delay, fetched_at=self._clock())

    async def _get_robots(self, robots_url: httpx.URL) -> httpx.Response | None:
        """Fetch robots.txt, following up to ``_MAX_REDIRECTS`` redirects (RFC 9309 §2.3.1.2)
        with the rate limiter and the retry policy applied to every hop; ``None`` when the
        redirects never settle. Raises the last ``HTTPStatusError`` / ``TransportError`` when
        a hop fails every attempt."""
        current = robots_url
        for _hop in range(_MAX_REDIRECTS + 1):
            response = await self._with_retries(
                current, functools.partial(self._send_robots_once, current)
            )
            location = response.headers.get("location")
            if response.status_code not in _REDIRECT_STATUS or location is None:
                return response
            await response.aclose()
            current = current.join(location)
        return None

    async def _send_robots_once(self, robots_url: httpx.URL) -> httpx.Response:
        """One robots.txt attempt: rate limit → send → surface a retryable status as an
        ``HTTPStatusError`` so :meth:`_with_retries` treats it like any page fetch. Every
        other answer (2xx, 3xx, 4xx, an unlisted 5xx) is returned for :meth:`_fetch_robots`
        to classify. No budget: the robots fetch is exempt (design decision 10)."""
        host = _rate_key(robots_url)
        await self._wait_for_slot(host)
        self._count_request(host)
        response = await self._client.get(robots_url)
        if response.status_code in RETRYABLE_STATUS:
            reason = f" {response.reason_phrase}" if response.reason_phrase else ""
            msg = f"HTTP {response.status_code}{reason} for {robots_url}"
            raise httpx.HTTPStatusError(msg, request=response.request, response=response)
        return response


def utc_now() -> datetime:
    """Timezone-aware UTC now — the one place the ingestion code reads the clock."""
    return datetime.now(UTC)
