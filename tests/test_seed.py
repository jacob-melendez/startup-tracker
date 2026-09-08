"""The seed loader and its domain validation (SPEC §10, §12 Phase 3).

``respx`` answers every probe; an un-mocked request fails the test.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
import respx
import yaml

from db import enums
from ingest.config import RegionsConfig, load_regions_config
from ingest.connectors import all_connectors
from ingest.http import HttpClient
from ingest.seed import (
    SEED_YAML,
    SeedConnector,
    SeedEntry,
    SeedFile,
    SeedResult,
    classify_response,
    load_seed_file,
)
from tests.support import FakeClock, collect, connector_config, make_client, make_ctx


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
async def client(clock: FakeClock) -> AsyncIterator[HttpClient]:
    async with make_client(connector_config("seed"), clock) as http:
        yield http


def small_seed(*entries: dict[str, str], labels: dict[str, str] | None = None) -> SeedFile:
    return SeedFile.model_validate(
        {"companies": {"test_group": list(entries)}, "sector_labels": labels or {}}
    )


def build(regions: RegionsConfig, seed: SeedFile | None = None, **options: object) -> SeedConnector:
    return SeedConnector(connector_config("seed", **options), regions, seed_file=seed)


def mock_site(router: respx.MockRouter, host: str, **kwargs: object) -> None:
    router.get(f"https://{host}/robots.txt").mock(
        httpx.Response(200, text="User-agent: *\nAllow: /\n")
    )
    router.head(f"https://{host}/").mock(**kwargs)  # type: ignore[arg-type]


# ------------------------------------------------------------------ the shipped file


def test_the_shipped_seed_file_loads() -> None:
    seed = load_seed_file()
    assert len(seed.entries) >= 55  # SPEC §10's Bay Area bootstrap set
    assert all(entry.name and entry.domain and entry.city for entry in seed.entries)


def test_no_seed_entry_carries_an_ats_token() -> None:
    """SPEC §10, CLAUDE.md: "never hardcode ATS tokens — connector discovery finds them". The
    model forbids the field outright, so a token cannot be smuggled in."""
    raw = yaml.safe_load(SEED_YAML.read_text(encoding="utf-8"))
    for entries in raw["companies"].values():
        for entry in entries:
            assert not {"ats_token", "ats_provider"} & entry.keys()
    with pytest.raises(ValueError, match="ats_token"):
        SeedEntry.model_validate(
            {"name": "X", "domain": "x.com", "city": "Berkeley", "ats_token": "x"}
        )


def test_every_shipped_seed_city_resolves_in_the_shipped_region_config(
    regions: RegionsConfig,
) -> None:
    """The two config files must agree. ``config/seed_companies.yaml`` names a city and only
    ``config/regions.yaml`` knows where it is (SPEC §11), and since §12 Phase 7 a city no
    enabled region claims is skipped rather than raised (below) — so a typo, a city dropped
    from a metro, or a metro switched off would now take companies out of the bootstrap set
    with nothing louder than a log line to say so. This is the check that says so.

    The metro is compared against the configured list instead of being named here, so the test
    keeps holding the day a metro is renamed or the bootstrap list grows past one region.
    """
    connector = build(regions)
    metros = set(regions.metros)
    unclaimed: list[str] = []
    for entry in load_seed_file().entries:
        records = list(connector.to_records(SeedResult(entry, None, None, "ok")))
        if not records:
            unclaimed.append(f"{entry.name} ({entry.city})")
            continue
        (location,) = records[0].locations  # one entry, one city, one HQ
        assert location.metro in metros, f"{entry.name}: metro {location.metro!r} is not enabled"

    assert not unclaimed, "no enabled region claims the city of: " + ", ".join(unclaimed)
    assert connector.skipped == {}  # the counter an operator reads agrees: nothing was dropped


def test_an_entry_no_enabled_region_claims_is_skipped_rather_than_failing_the_run(
    regions: RegionsConfig,
) -> None:
    """SPEC §12 Phase 7. This used to raise ``ValueError``, which the pipeline records as a
    failed item: one unclaimed city therefore turned a routine ``config/regions.yaml`` edit
    into a ``partial`` seed run, and a metro switched off with ``enabled: false`` — one line —
    made every company in it an error rather than a decision.

    The entry is dropped, counted under ``outside_region`` (the reason every other connector
    already uses for a record outside the configured regions, so a single number answers "how
    many did not land"), and the entries around it in the same file still become records.
    """
    connector = build(
        regions,
        small_seed(
            {"name": "Before", "domain": "before.com", "city": "Berkeley"},
            {"name": "Elsewhere", "domain": "elsewhere.is", "city": "Reykjavík"},
            {"name": "After", "domain": "after.com", "city": "Oakland"},
        ),
    )

    mapped = [
        record
        for entry in connector.seed.entries
        for record in connector.to_records(SeedResult(entry, None, None, "ok"))
    ]

    assert [record.name for record in mapped] == ["Before", "After"]
    assert connector.skipped == {"outside_region": 1}


def test_switching_a_region_off_skips_its_seed_entries_instead_of_failing_the_run() -> None:
    """``enabled: false`` is the one-line way to take a metro out of ingestion (SPEC §11), and
    the seed loader has to honour it the same way every connector does — the bootstrap file
    keeps naming those companies, and they simply stop loading.

    Run twice over one seed file, with the second region enabled and then disabled, because the
    ``enabled`` flag is only meaningful as a difference: a single disabled run would also pass
    if the loader had never resolved that city at all.
    """

    def two_metros(*, second_enabled: bool) -> RegionsConfig:
        return RegionsConfig.model_validate(
            {
                "regions": [
                    {"metro": "Metro One", "state": "AA", "cities": ["Alpha City"]},
                    {
                        "metro": "Metro Two",
                        "state": "BB",
                        "enabled": second_enabled,
                        "cities": ["Beta City"],
                    },
                ]
            }
        )

    seed = small_seed(
        {"name": "Alpha Co", "domain": "alpha.example", "city": "Alpha City"},
        {"name": "Beta Co", "domain": "beta.example", "city": "Beta City"},
    )

    def mapped_names(*, second_enabled: bool) -> tuple[list[str], dict[str, int]]:
        connector = build(two_metros(second_enabled=second_enabled), seed)
        names = [
            record.name
            for entry in seed.entries
            for record in connector.to_records(SeedResult(entry, None, None, "ok"))
        ]
        return names, connector.skipped

    assert mapped_names(second_enabled=True) == (["Alpha Co", "Beta Co"], {})
    assert mapped_names(second_enabled=False) == (["Alpha Co"], {"outside_region": 1})


def test_the_group_key_becomes_the_sector() -> None:
    seed = small_seed(
        {"name": "A", "domain": "a.com", "city": "Berkeley"},
        {"name": "B", "domain": "b.com", "city": "Berkeley", "sector": "Custom"},
    )
    assert seed.sector_for(seed.entries[0]) == "Test Group"
    assert seed.sector_for(seed.entries[1]) == "Custom"
    labelled = small_seed(
        {"name": "A", "domain": "a.com", "city": "Berkeley"}, labels={"test_group": "Nice Label"}
    )
    assert labelled.sector_for(labelled.entries[0]) == "Nice Label"


def test_the_shipped_labels_are_used() -> None:
    seed = load_seed_file()
    ai = next(entry for entry in seed.entries if entry.name == "OpenAI")
    assert seed.sector_for(ai) == "AI & ML"


def test_the_seed_loader_is_not_in_the_refresh_registry() -> None:
    """``refresh --all`` must not re-validate the bootstrap list every night; ``cli.py seed``
    builds the connector directly."""
    assert SeedConnector.name not in all_connectors()


# ------------------------------------------------------------------ classify_response


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (200, None),
        (204, None),
        (301, None),  # already followed by the client; a 3xx here is the settled answer
        (403, None),  # a bot-hostile WAF is a fact about us, not about the company
        (429, None),
        (500, None),
        (404, enums.CompanyStatus.DEAD),
        (410, enums.CompanyStatus.DEAD),
    ],
)
def test_only_404_and_410_mark_a_company_dead(
    status: int, expected: enums.CompanyStatus | None
) -> None:
    """SPEC §10 says to mark *unreachable* entries dead. A host that answered is reachable."""
    verdict, reason = classify_response(httpx.Response(status))
    assert verdict is expected
    assert str(status) in reason


# ------------------------------------------------------------------ validation


async def test_a_reachable_domain_is_probed_with_head_and_keeps_its_status(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    """SPEC §10: "resolve each domain (HEAD request, follow redirects)"."""
    mock_site(router, "example.com", return_value=httpx.Response(200))
    connector = build(
        regions, small_seed({"name": "Ex", "domain": "example.com", "city": "Berkeley"})
    )
    ctx = make_ctx(client)
    results = await collect(connector, ctx)

    assert [result.status for result in results] == [None]
    assert results[0].final_url == "https://example.com/"
    assert ctx.problems == []
    record = next(iter(connector.to_records(results[0])))
    assert record.status is None  # nothing to say, so nothing written (SPEC §8)
    assert record.website_url == "https://example.com/"
    assert record.domain == "example.com"
    assert record.ats_provider is None and record.ats_token is None


async def test_redirects_are_followed_and_the_final_url_is_stored(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    router.get("https://example.com/robots.txt").mock(
        httpx.Response(200, text="User-agent: *\nAllow: /\n")
    )
    router.get("https://www.example.com/robots.txt").mock(
        httpx.Response(200, text="User-agent: *\nAllow: /\n")
    )
    router.head("https://example.com/").mock(
        httpx.Response(301, headers={"Location": "https://www.example.com/"})
    )
    router.head("https://www.example.com/").mock(httpx.Response(200))
    connector = build(
        regions, small_seed({"name": "Ex", "domain": "example.com", "city": "Berkeley"})
    )
    result = (await collect(connector, make_ctx(client)))[0]
    record = next(iter(connector.to_records(result)))
    assert record.website_url == "https://www.example.com/"
    # The canonical key stays the domain the file names (SPEC §8 — the domain is never re-keyed).
    assert record.domain == "example.com"


async def test_a_404_marks_the_entry_dead_without_failing_the_run(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    """SPEC §10: "some entries may have been acquired or wound down, which the validation step
    will catch" — and it logs rather than failing."""
    mock_site(router, "gone.example", return_value=httpx.Response(404))
    connector = build(
        regions, small_seed({"name": "Gone", "domain": "gone.example", "city": "Berkeley"})
    )
    ctx = make_ctx(client)
    results = await collect(connector, ctx)
    assert [result.status for result in results] == [enums.CompanyStatus.DEAD]
    record = next(iter(connector.to_records(results[0])))
    assert record.status is enums.CompanyStatus.DEAD
    # A dead company is still stored: the database is the historical record (SPEC §2).
    assert record.name == "Gone"
    assert record.locations


async def test_an_unreachable_host_marks_the_entry_dead_and_records_a_problem(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    router.get("https://nxdomain.example/robots.txt").mock(
        side_effect=httpx.ConnectError("Name or service not known")
    )
    router.head("https://nxdomain.example/").mock(
        side_effect=httpx.ConnectError("Name or service not known")
    )
    connector = build(
        regions, small_seed({"name": "NX", "domain": "nxdomain.example", "city": "Berkeley"})
    )
    ctx = make_ctx(client)
    results = await collect(connector, ctx)
    assert results[0].status is enums.CompanyStatus.DEAD
    assert results[0].final_url is None
    assert any("marked dead" in problem for problem in ctx.problems)


async def test_a_403_does_not_mark_a_live_company_dead(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    """atom-computing.com answers 403 to non-browser clients; it is very much alive."""
    mock_site(router, "walled.example", return_value=httpx.Response(403))
    connector = build(
        regions, small_seed({"name": "Walled", "domain": "walled.example", "city": "Berkeley"})
    )
    results = await collect(connector, make_ctx(client))
    assert results[0].status is None
    assert "not marked dead" in results[0].reason


async def test_a_server_that_refuses_head_is_retried_with_get(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    router.get("https://nohead.example/robots.txt").mock(
        httpx.Response(200, text="User-agent: *\nAllow: /\n")
    )
    head = router.head("https://nohead.example/").mock(httpx.Response(405))
    get = router.get("https://nohead.example/").mock(httpx.Response(200, html="<html></html>"))
    connector = build(
        regions, small_seed({"name": "NoHead", "domain": "nohead.example", "city": "Berkeley"})
    )
    results = await collect(connector, make_ctx(client))
    assert head.called and get.called
    assert results[0].status is None


async def test_a_robots_refusal_is_not_evidence_about_the_company(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    router.get("https://closed.example/robots.txt").mock(
        httpx.Response(200, text="User-agent: *\nDisallow: /\n")
    )
    probe = router.head("https://closed.example/")
    connector = build(
        regions, small_seed({"name": "Closed", "domain": "closed.example", "city": "Berkeley"})
    )
    ctx = make_ctx(client)
    results = await collect(connector, ctx)
    assert not probe.called
    assert results[0].status is None
    assert any("not probed" in problem for problem in ctx.problems)


async def test_validation_can_be_skipped(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    connector = build(
        regions,
        small_seed({"name": "Ex", "domain": "example.com", "city": "Berkeley"}),
        validate_domains=False,
    )
    results = await collect(connector, make_ctx(client))
    assert client.stats.requests == 0
    assert results[0].reason == "domain validation disabled"
    assert next(iter(connector.to_records(results[0]))).website_url is None


async def test_every_entry_is_yielded_whatever_the_probe_says(
    router: respx.MockRouter, client: HttpClient, regions: RegionsConfig
) -> None:
    mock_site(router, "ok.example", return_value=httpx.Response(200))
    mock_site(router, "gone.example", return_value=httpx.Response(404))
    mock_site(router, "walled.example", return_value=httpx.Response(403))
    connector = build(
        regions,
        small_seed(
            {"name": "Ok", "domain": "ok.example", "city": "Berkeley"},
            {"name": "Gone", "domain": "gone.example", "city": "Palo Alto"},
            {"name": "Walled", "domain": "walled.example", "city": "Oakland"},
        ),
    )
    results = await collect(connector, make_ctx(client))
    assert [result.entry.name for result in results] == ["Ok", "Gone", "Walled"]
    assert [result.status for result in results] == [None, enums.CompanyStatus.DEAD, None]


# ------------------------------------------------------------------ to_records


def test_the_external_id_is_a_stable_slug(regions: RegionsConfig) -> None:
    """``CompanySource.external_id`` must survive a domain change, so it is built from the
    name (SPEC §8 step 0)."""
    connector = build(
        regions,
        small_seed({"name": "Anysphere (Cursor)", "domain": "cursor.com", "city": "Berkeley"}),
    )
    record = next(
        iter(connector.to_records(SeedResult(connector.seed.entries[0], None, None, "ok")))
    )
    assert record.external_id == "anysphere-cursor"


def test_a_configured_city_supplies_state_metro_and_spelling(regions: RegionsConfig) -> None:
    connector = build(
        regions, small_seed({"name": "X", "domain": "x.com", "city": "south san francisco"})
    )
    record = next(
        iter(connector.to_records(SeedResult(connector.seed.entries[0], None, None, "ok")))
    )
    location = record.locations[0]
    assert (location.city, location.state, location.metro, location.is_hq) == (
        "South San Francisco",
        "CA",
        "Bay Area",
        True,
    )


def test_a_malformed_seed_file_fails_on_load(tmp_path: Path) -> None:
    path = tmp_path / "seed.yaml"
    path.write_text(
        yaml.safe_dump({"companies": {"g": [{"name": "X", "city": "Berkeley"}]}}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="domain"):
        load_seed_file(path)
