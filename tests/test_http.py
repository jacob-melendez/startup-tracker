"""``ingest.http.HttpClient`` — the source-etiquette rules of SPEC §4 on a mocked wire.

Nothing here touches the network: ``respx`` answers every request (an un-mocked one fails) and
a :class:`FakeClock` stands in for ``time.monotonic``/``time.time``/``asyncio.sleep`` so the rate
limiter, retry backoff and cache expiry run instantly and every wait is recorded.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest
import respx

from ingest.config import RateLimit
from ingest.http import (
    CACHE_TTL_SECONDS,
    ROBOTS_TTL_SECONDS,
    CacheEntry,
    FileCache,
    HostBudgetExceeded,
    HttpClient,
    MemoryCache,
    RobotsDisallowed,
    utc_now,
)

UA = "startup-tracker/0.1 tests@example.com"
ALLOW_ALL = "User-agent: *\nDisallow:\n"
WALL_START = 1_700_000_000.0


class FakeClock:
    """Deterministic time: ``monotonic()``/``time()`` only move when something sleeps (or the
    test calls :meth:`advance`), and every sleep is recorded instead of waited for."""

    def __init__(self) -> None:
        self.mono = 1_000.0
        self.wall = WALL_START
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.mono

    def time(self) -> float:
        return self.wall

    def advance(self, seconds: float) -> None:
        self.mono += seconds
        self.wall += seconds

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    # assert_all_mocked=True (the default) makes any un-mocked request fail the test.
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


def make_client(
    clock: FakeClock,
    *,
    rate_limit: RateLimit | None = None,
    respect_robots: bool = True,
    cache: MemoryCache | FileCache | None = None,
    max_attempts: int = 3,
    user_agent: str = UA,
) -> HttpClient:
    return HttpClient(
        user_agent=user_agent,
        rate_limit=rate_limit,
        respect_robots=respect_robots,
        cache=cache,
        max_attempts=max_attempts,
        clock=clock.monotonic,
        sleep=clock.sleep,
        wall_clock=clock.time,
    )


def robots(
    router: respx.MockRouter, host: str, body: str = ALLOW_ALL, status: int = 200
) -> respx.Route:
    return router.get(f"https://{host}/robots.txt").mock(
        return_value=httpx.Response(status, text=body)
    )


# ------------------------------------------------------------------ User-Agent (SPEC §4)


async def test_user_agent_is_sent_on_every_request_including_robots(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    robots(router, "example.com")
    router.get("https://example.com/a").mock(return_value=httpx.Response(200, text="a"))
    router.get("https://example.com/b").mock(return_value=httpx.Response(200, text="b"))
    async with make_client(clock) as client:
        assert client.user_agent == UA
        await client.get("https://example.com/a")
        await client.get("https://example.com/b")
    assert len(router.calls) == 3  # robots + two pages
    assert {call.request.headers["user-agent"] for call in router.calls} == {UA}
    assert router.calls[0].request.url == "https://example.com/robots.txt"


async def test_params_and_extra_headers_are_sent(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    route = router.get("https://example.com/search").mock(return_value=httpx.Response(200))
    async with make_client(clock, respect_robots=False) as client:
        await client.get(
            "https://example.com/search",
            params={"q": "Palo Alto", "page": 2},
            headers={"Accept": "application/json"},
        )
    request = route.calls.last.request
    assert request.url.params["q"] == "Palo Alto"
    assert request.url.params["page"] == "2"
    assert request.headers["accept"] == "application/json"
    assert request.headers["user-agent"] == UA


# ------------------------------------------------------------ rate limiting (design decision 11)


async def test_two_requests_to_one_host_are_spaced_by_min_interval(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    router.get("https://example.com/a").mock(return_value=httpx.Response(200))
    router.get("https://example.com/b").mock(return_value=httpx.Response(200))
    limit = RateLimit(requests_per_second=0.5)  # SPEC §4 Tier 3: one request per 2 s
    async with make_client(clock, rate_limit=limit, respect_robots=False) as client:
        await client.get("https://example.com/a")
        await client.get("https://example.com/b")
        assert client.stats.requests == 2
        assert client.stats.by_host == {"example.com": 2}
    assert clock.sleeps == [pytest.approx(2.0)]


async def test_different_hosts_do_not_wait_for_each_other(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    router.get("https://one.example/a").mock(return_value=httpx.Response(200))
    router.get("https://two.example/a").mock(return_value=httpx.Response(200))
    async with make_client(clock, respect_robots=False) as client:
        await client.get("https://one.example/a")
        await client.get("https://two.example/a")
        assert client.stats.by_host == {"one.example": 1, "two.example": 1}
    assert clock.sleeps == []


async def test_www_and_bare_host_share_one_bucket(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    router.get("https://www.example.com/a").mock(return_value=httpx.Response(200))
    router.get("https://example.com/b").mock(return_value=httpx.Response(200))
    async with make_client(clock, respect_robots=False) as client:
        await client.get("https://www.example.com/a")
        await client.get("https://example.com/b")
        assert client.stats.by_host == {"example.com": 2}
    assert clock.sleeps == [pytest.approx(1.0)]


# ------------------------------------------------------------------ retries (design decision 12)


async def test_503_is_retried_with_exponential_backoff(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    route = router.get("https://example.com/a").mock(
        side_effect=[httpx.Response(503), httpx.Response(503), httpx.Response(200, text="ok")]
    )
    async with make_client(clock, respect_robots=False, max_attempts=5) as client:
        response = await client.get("https://example.com/a")
        assert response.status_code == 200
        assert response.text == "ok"
        assert client.stats.retries == 2
        assert client.stats.requests == 3
    assert route.call_count == 3
    # wait_exponential_jitter(initial=1): 1 + U(0,1), then 2 + U(0,1); each backoff already
    # exceeds the 1 s rate-limit interval, so no limiter sleep is recorded in between.
    assert len(clock.sleeps) == 2
    assert 1.0 <= clock.sleeps[0] <= 2.0
    assert 2.0 <= clock.sleeps[1] <= 3.0


async def test_single_retry_then_success(router: respx.MockRouter, clock: FakeClock) -> None:
    route = router.get("https://example.com/a").mock(
        side_effect=[httpx.Response(503), httpx.Response(200)]
    )
    async with make_client(clock, respect_robots=False) as client:
        await client.get("https://example.com/a")
        assert client.stats.retries == 1
    assert route.call_count == 2


async def test_retry_waits_for_the_rate_limiter_again(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    router.get("https://example.com/a").mock(side_effect=[httpx.Response(503), httpx.Response(200)])
    limit = RateLimit(requests_per_second=0.25)  # 4 s between requests
    async with make_client(clock, rate_limit=limit, respect_robots=False) as client:
        await client.get("https://example.com/a")
    # backoff (1-2 s) then the limiter tops it up to the 4 s interval
    assert len(clock.sleeps) == 2
    assert sum(clock.sleeps) == pytest.approx(4.0)


async def test_retry_after_seconds_is_honoured(router: respx.MockRouter, clock: FakeClock) -> None:
    router.get("https://example.com/a").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200),
        ]
    )
    async with make_client(clock, respect_robots=False) as client:
        await client.get("https://example.com/a")
        assert client.stats.retries == 1
    assert clock.sleeps == [pytest.approx(7.0)]


async def test_retry_after_http_date_and_cap(router: respx.MockRouter, clock: FakeClock) -> None:
    later = datetime.fromtimestamp(WALL_START + 30, tz=UTC)
    router.get("https://example.com/a").mock(
        side_effect=[
            httpx.Response(503, headers={"Retry-After": format_datetime(later, usegmt=True)}),
            httpx.Response(503, headers={"Retry-After": "3600"}),
            httpx.Response(200),
        ]
    )
    async with make_client(clock, respect_robots=False) as client:
        await client.get("https://example.com/a")
    assert clock.sleeps[0] == pytest.approx(30.0)
    assert clock.sleeps[1] == pytest.approx(120.0)  # capped


async def test_404_raises_immediately_after_one_request(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    route = router.get("https://example.com/missing").mock(return_value=httpx.Response(404))
    async with make_client(clock, respect_robots=False) as client:
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await client.get("https://example.com/missing")
        assert excinfo.value.response.status_code == 404
        assert client.stats.retries == 0
        assert client.stats.requests == 1
    assert route.call_count == 1
    assert clock.sleeps == []


async def test_transport_error_is_retried_then_succeeds(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    route = router.get("https://example.com/a").mock(
        side_effect=[httpx.ConnectTimeout("slow"), httpx.Response(200, text="ok")]
    )
    async with make_client(clock, respect_robots=False) as client:
        response = await client.get("https://example.com/a")
        assert response.text == "ok"
        assert client.stats.retries == 1
    assert route.call_count == 2


async def test_all_attempts_failing_raises_the_underlying_status_error(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    route = router.get("https://example.com/a").mock(return_value=httpx.Response(502))
    async with make_client(clock, respect_robots=False, max_attempts=3) as client:
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await client.get("https://example.com/a")
        assert excinfo.value.response.status_code == 502
        assert client.stats.retries == 2
    assert route.call_count == 3


async def test_all_attempts_failing_raises_the_underlying_transport_error(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    route = router.get("https://example.com/a").mock(side_effect=httpx.ConnectError("down"))
    async with make_client(clock, respect_robots=False, max_attempts=4) as client:
        with pytest.raises(httpx.ConnectError):
            await client.get("https://example.com/a")
        assert client.stats.retries == 3
    assert route.call_count == 4


async def test_malformed_location_header_is_not_retried(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """httpx reports an unparsable ``Location`` as a ``RemoteProtocolError`` (a transport error
    by type) even with ``follow_redirects=False``; the server will answer the same way every
    time, so it must cost one request, not ``max_attempts`` plus backoff plus budget."""
    route = router.get("https://example.com/a").mock(
        return_value=httpx.Response(301, headers={"Location": "http://[::1"})
    )
    limit = RateLimit(requests_per_second=1, max_requests_per_host_per_run=5)
    async with make_client(clock, rate_limit=limit, respect_robots=False, max_attempts=5) as client:
        with pytest.raises(httpx.RemoteProtocolError, match="Invalid URL in location header"):
            await client.get("https://example.com/a")
        assert client.stats.retries == 0
        assert client.stats.requests == 1
        # the other four budget slots are still available to the run
        router.get("https://example.com/b").mock(return_value=httpx.Response(200))
        await client.get("https://example.com/b")
    assert route.call_count == 1
    assert clock.sleeps == [pytest.approx(1.0)]  # only the limiter, no backoff


# ------------------------------------------------------------------ robots.txt (design decision 10)


async def test_robots_disallow_blocks_the_request_before_it_is_sent(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    robots(router, "example.com", "User-agent: *\nDisallow: /private/\n")
    target = router.get("https://example.com/private/page").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        with pytest.raises(RobotsDisallowed) as excinfo:
            await client.get("https://example.com/private/page")
        assert excinfo.value.url == "https://example.com/private/page"
        assert excinfo.value.user_agent == UA
        assert client.stats.robots_denied == 1
    assert target.call_count == 0


async def test_robots_matches_our_product_token(router: respx.MockRouter, clock: FakeClock) -> None:
    robots(
        router,
        "example.com",
        "User-agent: startup-tracker\nDisallow: /\n\nUser-agent: *\nAllow: /\n",
    )
    target = router.get("https://example.com/page").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/page")
    assert target.call_count == 0


async def test_robots_blank_lines_do_not_end_a_group(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """RFC 9309 §2.2: only a ``User-agent`` line starts a new group; a rule after a blank line
    (the shape of www.sec.gov's file) still belongs to the group above it."""
    robots(router, "example.com", "User-agent: *\nDisallow: /a\n\n# later\nDisallow: /b\n")
    router.get("https://example.com/c").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        await client.get("https://example.com/c")
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/a")
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/b")


#: RFC 9309 §2.2.2 wildcards, in the shapes real sites use. ``urllib.robotparser`` matches
#: literal prefixes only, so every one of these rules would be silently ignored by it.
WILDCARD_ROBOTS = "User-agent: *\nDisallow: /*/media/oembed\nDisallow: /*.pdf$\nDisallow: /*?\n"


@pytest.mark.parametrize(
    ("path", "allowed"),
    [
        ("/a/media/oembed", False),  # ``*`` matches any sequence, mid-pattern
        ("/deep/nested/media/oembed", False),
        ("/report.pdf", False),  # ``$`` anchors the end of the path
        ("/report.pdf.html", True),  # ...so a longer path is not caught
        ("/search?q=x", False),  # ``/*?`` blocks every query string
        ("/search", True),
        ("/about", True),
    ],
)
async def test_robots_wildcards_and_end_anchor_are_honoured(
    router: respx.MockRouter, clock: FakeClock, path: str, allowed: bool
) -> None:
    """SPEC §4/§13: robots.txt is honoured before every fetch. Patterns are matched against
    the percent-encoded path *and* query, so ``Disallow: /*?`` can only work off ``raw_path``."""
    robots(router, "example.com", WILDCARD_ROBOTS)
    route = router.get(f"https://example.com{path}").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        if allowed:
            await client.get(f"https://example.com{path}")
        else:
            with pytest.raises(RobotsDisallowed):
                await client.get(f"https://example.com{path}")
    assert route.call_count == (1 if allowed else 0)


@pytest.mark.parametrize(
    ("path", "allowed"),
    [("/a", False), ("/a/x", False), ("/a/b", True), ("/a/b/c", True)],
)
async def test_robots_most_specific_rule_wins(
    router: respx.MockRouter, clock: FakeClock, path: str, allowed: bool
) -> None:
    """RFC 9309 §2.2.2: when several rules match, the longest pattern decides — not the first
    one listed, which is what ``urllib.robotparser`` does."""
    robots(router, "example.com", "User-agent: *\nDisallow: /a\nAllow: /a/b\n")
    router.get(url__regex=r"https://example\.com/.*").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        if allowed:
            await client.get(f"https://example.com{path}")
        else:
            with pytest.raises(RobotsDisallowed):
                await client.get(f"https://example.com{path}")


async def test_robots_allow_wins_a_tie(router: respx.MockRouter, clock: FakeClock) -> None:
    """RFC 9309 §2.2.2: equally specific ``Allow`` and ``Disallow`` — the ``Allow`` wins."""
    robots(router, "example.com", "User-agent: *\nDisallow: /x\nAllow: /x\n")
    route = router.get("https://example.com/x").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        await client.get("https://example.com/x")
    assert route.call_count == 1


async def test_fractional_crawl_delay_is_honoured(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """``Crawl-delay: 2.5`` is a real value sites publish; ``urllib.robotparser`` parses the
    delay as an int and drops it, which would silently leave us fetching faster than asked."""
    robots(router, "example.com", "User-agent: *\nCrawl-delay: 2.5\nDisallow:\n")
    router.get(url__regex=r"https://example\.com/.*").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        await client.get("https://example.com/a")
        await client.get("https://example.com/b")
    assert clock.sleeps == [pytest.approx(2.5), pytest.approx(2.5)]


@pytest.mark.parametrize("status", [404, 403, 401])
async def test_robots_4xx_means_everything_is_allowed(
    router: respx.MockRouter, clock: FakeClock, status: int
) -> None:
    robots(router, "example.com", '{"message":"Forbidden"}', status=status)
    target = router.get("https://example.com/page").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        response = await client.get("https://example.com/page")
        assert response.status_code == 200
        assert client.stats.robots_denied == 0
    assert target.call_count == 1


async def test_robots_500_disallows_everything_for_this_run(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    robots_route = robots(router, "example.com", "oops", status=500)
    target = router.get("https://example.com/page").mock(return_value=httpx.Response(200))
    async with make_client(clock, max_attempts=3) as client:
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/page")
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/other")
        assert client.stats.robots_denied == 2
        assert client.stats.retries == 2  # the robots fetch ran the full retry policy once
    assert target.call_count == 0
    assert robots_route.call_count == 3  # max_attempts; then remembered, not re-fetched per page


async def test_robots_unreachable_stays_disallowed_for_the_whole_run(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """Design decision 10: a robots.txt that answers 5xx after the retry policy gave up disallows
    everything *for this run* — the outage is remembered as long as a parsed file would be
    (``ROBOTS_TTL_SECONDS``, longer than any run), not retried a few minutes later, so a long
    run cannot start fetching from the host part-way through once the file comes back."""
    robots_route = router.get("https://example.com/robots.txt").mock(
        side_effect=[httpx.Response(503), httpx.Response(503), httpx.Response(200, text=ALLOW_ALL)]
    )
    target = router.get("https://example.com/page").mock(return_value=httpx.Response(200))
    async with make_client(clock, max_attempts=2) as client:
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/page")
        assert robots_route.call_count == 2  # max_attempts, then remembered
        for hours in (1, 6, 12):  # 19 h in all: well past any "try again soon" window
            clock.advance(hours * 60 * 60)
            with pytest.raises(RobotsDisallowed):
                await client.get("https://example.com/page")
            assert robots_route.call_count == 2  # no new fetch, the page was never sent
        assert client.stats.robots_denied == 4
        # The entry expires with the same 24 h TTL as a parsed one; only then is the origin asked
        # again — a fresh client (the next run) does so at once.
        clock.advance(ROBOTS_TTL_SECONDS)
        response = await client.get("https://example.com/page")
        assert response.status_code == 200
    assert robots_route.call_count == 3
    assert target.call_count == 1


async def test_robots_transient_error_is_retried_then_recovers(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """One ReadTimeout on www.sec.gov/robots.txt must not turn into a run where every
    ``primary_doc.xml`` is refused: the robots fetch runs under the same retry policy."""
    robots_route = router.get("https://www.sec.gov/robots.txt").mock(
        side_effect=[httpx.ReadTimeout("slow"), httpx.Response(200, text=ALLOW_ALL)]
    )
    doc = "https://www.sec.gov/Archives/edgar/data/1/2/primary_doc.xml"
    target = router.get(doc).mock(return_value=httpx.Response(200, text="<edgarSubmission/>"))
    async with make_client(clock, max_attempts=5) as client:
        response = await client.get(doc)
        assert response.status_code == 200
        assert client.stats.retries == 1
        assert client.stats.robots_denied == 0
        assert client.stats.requests == 3  # two robots attempts + the page
    assert robots_route.call_count == 2
    assert target.call_count == 1
    assert 1.0 <= clock.sleeps[0] <= 2.0  # exponential backoff before the second robots attempt


async def test_robots_429_that_outlasts_the_retries_is_a_4xx(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """429 is retried like any page; when every attempt answers 429 the final status is still
    a 4xx and RFC 9309 §2.3.1.3 says "unavailable": no restrictions."""
    robots_route = robots(router, "example.com", "slow down", status=429)
    target = router.get("https://example.com/page").mock(return_value=httpx.Response(200))
    async with make_client(clock, max_attempts=2) as client:
        await client.get("https://example.com/page")
        assert client.stats.retries == 1
        assert client.stats.robots_denied == 0
    assert robots_route.call_count == 2
    assert target.call_count == 1


async def test_robots_transport_failure_disallows(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    robots_route = router.get("https://example.com/robots.txt").mock(
        side_effect=httpx.ConnectError("down")
    )
    target = router.get("https://example.com/page").mock(return_value=httpx.Response(200))
    async with make_client(clock, max_attempts=3) as client:
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/page")
        assert client.stats.retries == 2
        clock.advance(6 * 60 * 60)  # hours later, same run: still remembered, not re-fetched
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/other")
        assert client.stats.retries == 2
    assert robots_route.call_count == 3
    assert target.call_count == 0


async def test_robots_is_fetched_once_per_host(router: respx.MockRouter, clock: FakeClock) -> None:
    robots_route = robots(router, "example.com")
    router.get(url__regex=r"https://example\.com/page/\d+").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        for n in range(4):
            await client.get(f"https://example.com/page/{n}")
        assert client.stats.requests == 5
    assert robots_route.call_count == 1


async def test_robots_fetch_is_rate_limited_but_not_budgeted(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    robots(router, "example.com")
    router.get("https://example.com/a").mock(return_value=httpx.Response(200))
    router.get("https://example.com/b").mock(return_value=httpx.Response(200))
    limit = RateLimit(requests_per_second=1, max_requests_per_host_per_run=1)
    async with make_client(clock, rate_limit=limit) as client:
        await client.get("https://example.com/a")  # robots + page: page is budget item 1
        with pytest.raises(HostBudgetExceeded):
            await client.get("https://example.com/b")
    assert clock.sleeps == [pytest.approx(1.0)]  # the page waited behind the robots fetch


async def test_crawl_delay_raises_the_interval(router: respx.MockRouter, clock: FakeClock) -> None:
    robots(router, "example.com", "User-agent: *\nCrawl-delay: 5\nDisallow:\n")
    router.get("https://example.com/a").mock(return_value=httpx.Response(200))
    router.get("https://example.com/b").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        await client.get("https://example.com/a")
        await client.get("https://example.com/b")
    # The delay learned from robots.txt already spaces the *first* page after the robots fetch,
    # not just the second page after the first.
    assert clock.sleeps == [pytest.approx(5.0), pytest.approx(5.0)]


async def test_respect_robots_false_never_fetches_robots(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    robots_route = robots(router, "example.com", "User-agent: *\nDisallow: /\n")
    router.get("https://example.com/page").mock(return_value=httpx.Response(200))
    async with make_client(clock, respect_robots=False) as client:
        await client.get("https://example.com/page")
        assert client.stats.robots_denied == 0
    assert robots_route.call_count == 0


# ------------------------------------------------------------ cache (SPEC §4, design decision 9)


def _conditional_server(
    router: respx.MockRouter,
    *,
    etag: str | None = None,
    last_modified: str | None = None,
    body: str = "fresh",
) -> tuple[list[httpx.Request], respx.Route]:
    """A route that answers 200 with validators, then 304 to a matching conditional request."""
    seen: list[httpx.Request] = []
    headers = {k: v for k, v in {"ETag": etag, "Last-Modified": last_modified}.items() if v}

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        matches_etag = etag is not None and request.headers.get("if-none-match") == etag
        matches_date = (
            last_modified is not None and request.headers.get("if-modified-since") == last_modified
        )
        if matches_etag or matches_date:
            return httpx.Response(304, headers=headers)
        return httpx.Response(200, headers={**headers, "Content-Type": "text/plain"}, text=body)

    return seen, router.get("https://example.com/list").mock(side_effect=respond)


async def test_etag_flow_answers_304_from_the_cache(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    seen, route = _conditional_server(router, etag='"v1"', body="the list")
    cache = MemoryCache()
    async with make_client(clock, respect_robots=False, cache=cache) as client:
        first = await client.get("https://example.com/list")
        assert first.extensions["from_cache"] is False
        assert "if-none-match" not in seen[0].headers
        assert len(cache) == 1

        second = await client.get("https://example.com/list")
        assert seen[1].headers["if-none-match"] == '"v1"'
        assert second.status_code == 200
        assert second.text == "the list"
        assert second.headers["content-type"] == "text/plain"
        assert second.extensions["from_cache"] is True
        assert client.stats.cache_hits == 1
        assert client.stats.requests == 2  # the conditional request was still sent
    assert route.call_count == 2


async def test_last_modified_flow_sends_if_modified_since(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    stamp = "Wed, 21 Oct 2015 07:28:00 GMT"
    seen, _route = _conditional_server(router, last_modified=stamp, body="jobs")
    async with make_client(clock, respect_robots=False, cache=MemoryCache()) as client:
        await client.get("https://example.com/list")
        second = await client.get("https://example.com/list")
        assert seen[1].headers["if-modified-since"] == stamp
        assert "if-none-match" not in seen[1].headers
        assert second.text == "jobs"
        assert second.extensions["from_cache"] is True


async def test_cache_entry_older_than_24h_is_ignored(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    seen, _route = _conditional_server(router, etag='"v1"', body="today")
    cache = MemoryCache()
    async with make_client(clock, respect_robots=False, cache=cache) as client:
        await client.get("https://example.com/list")
        stored_at = cache.get("https://example.com/list")
        assert stored_at is not None
        clock.advance(CACHE_TTL_SECONDS + 1)
        second = await client.get("https://example.com/list")
        assert "if-none-match" not in seen[1].headers
        assert second.extensions["from_cache"] is False
        assert client.stats.cache_hits == 0
        replaced = cache.get("https://example.com/list")
        assert replaced is not None
        assert replaced.stored_at > stored_at.stored_at


async def test_responses_without_validators_are_not_cached(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    router.get("https://example.com/plain").mock(return_value=httpx.Response(200, text="x"))
    cache = MemoryCache()
    async with make_client(clock, respect_robots=False, cache=cache) as client:
        await client.get("https://example.com/plain")
        await client.get("https://example.com/plain")
        assert client.stats.cache_hits == 0
    assert len(cache) == 0


def test_file_cache_round_trips_and_survives_a_new_instance(tmp_path: Path) -> None:
    directory = tmp_path / "http-cache"
    cache = FileCache(directory)
    assert not directory.exists()  # created lazily
    assert cache.get("https://example.com/x") is None
    entry = CacheEntry(
        status_code=200,
        headers={"content-type": "application/json"},
        body=b'{"hits": []}\x00\xff',
        etag='"abc"',
        last_modified=None,
        stored_at=WALL_START,
    )
    cache.set("https://example.com/x", entry)
    assert directory.is_dir()
    assert cache.path_for("https://example.com/x").suffix == ".json"
    assert FileCache(directory).get("https://example.com/x") == entry
    assert FileCache(directory).get("https://example.com/other") is None


def test_file_cache_corrupt_file_is_a_miss(tmp_path: Path) -> None:
    cache = FileCache(tmp_path)
    url = "https://example.com/x"
    tmp_path.mkdir(exist_ok=True)
    cache.path_for(url).write_text("{not json", encoding="utf-8")
    assert cache.get(url) is None
    cache.path_for(url).write_text('{"status_code": 200}', encoding="utf-8")  # missing keys
    assert cache.get(url) is None
    cache.path_for(url).write_text(
        '{"status_code": 200, "headers": {}, "body": "@@@", "stored_at": 1}', encoding="utf-8"
    )
    assert cache.get(url) is None  # invalid base64


def _entry(body: bytes, etag: str) -> CacheEntry:
    return CacheEntry(
        status_code=200,
        headers={"content-type": "text/plain"},
        body=body,
        etag=etag,
        last_modified=None,
        stored_at=WALL_START,
    )


def test_file_cache_concurrent_writers_use_distinct_temp_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The scheduler and a manual ``cli.py refresh`` share the cache directory and may store the
    same URL at the same moment. Simulated by letting a second writer run to completion while
    the first is about to publish: each must use its own temp file, both renames must succeed,
    and nothing may be left behind."""
    url = "https://example.com/x"
    first, second = FileCache(tmp_path), FileCache(tmp_path)
    entry_a, entry_b = _entry(b"a", '"a"'), _entry(b"b", '"b"')
    real_replace = os.replace
    sources: list[str] = []

    def interleaved_replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        sources.append(os.fspath(src))
        if len(sources) == 1:
            second.set(url, entry_b)  # the other process publishes first
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", interleaved_replace)
    first.set(url, entry_a)
    assert len(sources) == 2
    assert sources[0] != sources[1]
    assert FileCache(tmp_path).get(url) == entry_a  # the outer writer published last, intact
    assert [p.name for p in tmp_path.iterdir()] == [first.path_for(url).name]  # no *.tmp left


def test_file_cache_failed_write_leaves_no_temp_file(tmp_path: Path) -> None:
    cache = FileCache(tmp_path)
    url = "https://example.com/x"
    cache.path_for(url).mkdir()  # the rename onto a directory fails with an OSError
    cache.set(url, _entry(b"a", '"a"'))  # logged, not raised (SPEC §4: the cache is optional)
    assert [p.name for p in tmp_path.iterdir()] == [cache.path_for(url).name]
    assert cache.get(url) is None


async def test_file_cache_makes_the_next_client_conditional(
    router: respx.MockRouter, clock: FakeClock, tmp_path: Path
) -> None:
    seen, _route = _conditional_server(router, etag='"v7"', body="persisted")
    async with make_client(clock, respect_robots=False, cache=FileCache(tmp_path)) as first:
        await first.get("https://example.com/list")
    async with make_client(clock, respect_robots=False, cache=FileCache(tmp_path)) as second:
        response = await second.get("https://example.com/list")
        assert seen[1].headers["if-none-match"] == '"v7"'
        assert response.text == "persisted"
        assert response.extensions["from_cache"] is True


# ------------------------------------------------------------ per-run host budget (SPEC §4 Tier 3)


async def test_host_budget_blocks_the_third_request(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    route = router.get(url__regex=r"https://example\.com/.*").mock(return_value=httpx.Response(200))
    limit = RateLimit(requests_per_second=1, max_requests_per_host_per_run=2)
    async with make_client(clock, rate_limit=limit, respect_robots=False) as client:
        await client.get("https://example.com/1")
        await client.get("https://example.com/2")
        with pytest.raises(HostBudgetExceeded) as excinfo:
            await client.get("https://example.com/3")
        assert excinfo.value.host == "example.com"
        assert excinfo.value.budget == 2
        assert client.stats.requests == 2
        # another host has its own budget
        router.get("https://other.example/1").mock(return_value=httpx.Response(200))
        await client.get("https://other.example/1")
    assert route.call_count == 2


async def test_host_budget_counts_retries(router: respx.MockRouter, clock: FakeClock) -> None:
    route = router.get("https://example.com/a").mock(return_value=httpx.Response(503))
    limit = RateLimit(requests_per_second=1, max_requests_per_host_per_run=2)
    async with make_client(clock, rate_limit=limit, respect_robots=False, max_attempts=5) as client:
        with pytest.raises(HostBudgetExceeded):
            await client.get("https://example.com/a")
        assert client.stats.retries == 2
    assert route.call_count == 2


# ------------------------------------------------------------------ redirects (design decision 11b)


async def test_redirect_hops_are_governed_requests(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    bare_robots = robots(router, "example.com")
    www_robots = robots(router, "www.example.com")
    first = router.get("https://example.com/a").mock(
        return_value=httpx.Response(301, headers={"Location": "https://www.example.com/b"})
    )
    second = router.get("https://www.example.com/b").mock(
        return_value=httpx.Response(200, text="landed")
    )
    limit = RateLimit(requests_per_second=1, max_requests_per_host_per_run=2)
    async with make_client(clock, rate_limit=limit) as client:
        response = await client.get("https://example.com/a")
        assert response.status_code == 200
        assert response.text == "landed"
        assert response.url == "https://www.example.com/b"
        assert response.extensions["from_cache"] is False
        assert first.call_count == 1
        assert second.call_count == 1
        assert bare_robots.call_count == 1
        assert www_robots.call_count == 1
        # one bucket for example.com and www.example.com: 4 requests, 3 waits of one interval
        assert client.stats.by_host == {"example.com": 4}
        assert clock.sleeps == [pytest.approx(1.0)] * 3
        # ... and one budget: both hops consumed it, so a third page is refused
        router.get("https://example.com/c").mock(return_value=httpx.Response(200))
        with pytest.raises(HostBudgetExceeded):
            await client.get("https://example.com/c")


async def test_redirect_target_is_checked_against_robots(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    robots(router, "example.com")
    robots(router, "cdn.example", "User-agent: *\nDisallow: /\n")
    router.get("https://example.com/a").mock(
        return_value=httpx.Response(302, headers={"Location": "https://cdn.example/b"})
    )
    target = router.get("https://cdn.example/b").mock(return_value=httpx.Response(200))
    async with make_client(clock) as client:
        with pytest.raises(RobotsDisallowed):
            await client.get("https://example.com/a")
        assert client.stats.robots_denied == 1
    assert target.call_count == 0


async def test_relative_location_is_resolved(router: respx.MockRouter, clock: FakeClock) -> None:
    router.get("https://example.com/old").mock(
        return_value=httpx.Response(307, headers={"Location": "/new?x=1"})
    )
    router.get("https://example.com/new", params={"x": "1"}).mock(
        return_value=httpx.Response(200, text="new")
    )
    async with make_client(clock, respect_robots=False) as client:
        response = await client.get("https://example.com/old")
        assert response.text == "new"
        assert response.url == "https://example.com/new?x=1"


def _chain(router: respx.MockRouter, hops: int) -> None:
    for n in range(hops):
        router.get(f"https://example.com/r{n}").mock(
            return_value=httpx.Response(301, headers={"Location": f"https://example.com/r{n + 1}"})
        )
    router.get(f"https://example.com/r{hops}").mock(return_value=httpx.Response(200, text="end"))


async def test_six_redirects_raise_too_many_redirects(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    _chain(router, 6)
    async with make_client(clock, respect_robots=False) as client:
        with pytest.raises(httpx.TooManyRedirects):
            await client.get("https://example.com/r0")
        assert client.stats.requests == 6


async def test_five_redirects_are_followed(router: respx.MockRouter, clock: FakeClock) -> None:
    _chain(router, 5)
    async with make_client(clock, respect_robots=False) as client:
        response = await client.get("https://example.com/r0")
        assert response.text == "end"
        assert client.stats.requests == 6


# ------------------------------------------------------------------ helpers and lifecycle


async def test_get_json_and_get_text(router: respx.MockRouter, clock: FakeClock) -> None:
    router.get("https://example.com/data.json").mock(
        return_value=httpx.Response(200, json={"hits": [1, 2]})
    )
    router.get("https://example.com/doc.xml").mock(
        return_value=httpx.Response(200, text="<edgarSubmission/>")
    )
    async with make_client(clock, respect_robots=False) as client:
        assert await client.get_json("https://example.com/data.json") == {"hits": [1, 2]}
        assert await client.get_text("https://example.com/doc.xml") == "<edgarSubmission/>"


async def test_context_manager_closes_the_client(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    router.get("https://example.com/a").mock(return_value=httpx.Response(200))
    client = make_client(clock, respect_robots=False)
    async with client:
        await client.get("https://example.com/a")
    with pytest.raises(RuntimeError, match="closed"):
        await client.get("https://example.com/a")
    await client.aclose()  # idempotent


async def test_stats_start_at_zero(clock: FakeClock) -> None:
    async with make_client(clock) as client:
        assert client.stats.requests == 0
        assert client.stats.retries == 0
        assert client.stats.cache_hits == 0
        assert client.stats.robots_denied == 0
        assert client.stats.by_host == {}


def test_max_attempts_must_be_positive(clock: FakeClock) -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        make_client(clock, max_attempts=0)


def test_utc_now_is_timezone_aware_utc() -> None:
    now = utc_now()
    assert now.tzinfo is UTC
    assert now.utcoffset() == timedelta(0)


# ------------------------------------------------------------ the recorded SEC robots files


async def test_recorded_sec_robots_allow_the_edgar_paths(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """The fixtures README: www.sec.gov allows ``/Archives/edgar/data`` and efts.sec.gov answers
    403 to robots.txt, which RFC 9309 treats as "no restrictions"."""
    fixtures = Path(__file__).parent / "fixtures" / "sec_edgar"
    router.get("https://www.sec.gov/robots.txt").mock(
        return_value=httpx.Response(
            200, text=fixtures.joinpath("www_sec_gov_robots.txt").read_text()
        )
    )
    router.get("https://efts.sec.gov/robots.txt").mock(
        return_value=httpx.Response(
            403, content=fixtures.joinpath("efts_sec_gov_robots_403.json").read_bytes()
        )
    )
    xml = router.get(
        "https://www.sec.gov/Archives/edgar/data/1830191/000183019126000003/primary_doc.xml"
    ).mock(return_value=httpx.Response(200, text="<edgarSubmission/>"))
    search = router.get("https://efts.sec.gov/LATEST/search-index").mock(
        return_value=httpx.Response(200, json={"hits": {"hits": []}})
    )
    blocked = router.get("https://www.sec.gov/cgi-bin/browse-edgar").mock(
        return_value=httpx.Response(200)
    )
    async with make_client(clock, rate_limit=RateLimit(requests_per_second=8)) as client:
        await client.get_text(
            "https://www.sec.gov/Archives/edgar/data/1830191/000183019126000003/primary_doc.xml"
        )
        await client.get_json(
            "https://efts.sec.gov/LATEST/search-index", params={"q": '"Palo Alto"', "forms": "D"}
        )
        with pytest.raises(RobotsDisallowed):
            await client.get("https://www.sec.gov/cgi-bin/browse-edgar")
        assert client.stats.robots_denied == 1
    assert xml.call_count == 1
    assert search.call_count == 1
    assert blocked.call_count == 0
    assert search.calls.last.request.url.params["q"] == '"Palo Alto"'


async def test_default_clock_and_sleep_really_wait(router: respx.MockRouter) -> None:
    """The injectable clock defaults to ``time.monotonic``/``asyncio.sleep``: 20 requests per
    second means at least 50 ms between two requests to one host."""
    router.get(url__regex=r"https://example\.com/.*").mock(return_value=httpx.Response(200))
    limit = RateLimit(requests_per_second=20)
    async with HttpClient(user_agent=UA, rate_limit=limit, respect_robots=False) as client:
        started = time.monotonic()
        await client.get("https://example.com/1")
        await client.get("https://example.com/2")
        elapsed = time.monotonic() - started
    assert elapsed >= 0.045
