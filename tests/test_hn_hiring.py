"""``ingest.connectors.hn_hiring`` on recorded fixtures (SPEC §4 Tier 1 #4, §12 Phase 3).

The fixtures are real September 2026 "Who is hiring?" comments, including the awkward ones: a
deleted comment, a comment whose header is wrapped in markdown asterisks, one that uses no
pipes at all, and one whose only links point at somebody else's job board.
"""

from __future__ import annotations

import glob
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from db import enums
from ingest.base import CompanyRecord
from ingest.config import RegionsConfig, load_regions_config
from ingest.connectors.hn_hiring import (
    ITEM_URL,
    USER_URL,
    HnComment,
    HnHiringConnector,
    extract_domain,
    extract_emails,
    extract_role_bullets,
    is_hiring_thread,
    leading_name,
    parse_header,
)
from ingest.http import HttpClient
from tests.support import (
    FIXTURES,
    FakeClock,
    collect,
    connector_config,
    fixture_json,
    make_client,
    make_ctx,
)

HN_DIR = FIXTURES / "hn_hiring"
STORY_ID = 49522897
DISCORD = 49525748  # San Francisco, pipe header
SWINGVISION = 49559434  # Berkeley, prose header, three role bullets
DELETED = 49524098
FASTLY = 49523835  # header wrapped in asterisks, no Bay Area city


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
def connector(regions: RegionsConfig) -> HnHiringConnector:
    return HnHiringConnector(connector_config("hn_hiring"), regions)


@pytest.fixture
async def client(clock: FakeClock) -> AsyncIterator[HttpClient]:
    async with make_client(connector_config("hn_hiring"), clock) as http:
        yield http


def item(item_id: int) -> dict[str, Any]:
    payload: dict[str, Any] = fixture_json("hn_hiring", f"item_{item_id}.json")
    return payload


def comment(item_id: int) -> HnComment:
    story = item(STORY_ID)
    return HnComment(story_id=STORY_ID, story_title=story["title"], item=item(item_id))


def records(connector: HnHiringConnector, item_id: int) -> list[CompanyRecord]:
    return list(connector.to_records(comment(item_id)))


def mock_api(router: respx.MockRouter, *, kids: list[int] | None = None) -> None:
    router.get("https://hacker-news.firebaseio.com/robots.txt").mock(
        httpx.Response(200, text=(HN_DIR / "robots.txt").read_text(encoding="utf-8"))
    )
    router.get(USER_URL).mock(
        httpx.Response(200, json=fixture_json("hn_hiring", "user_whoishiring.json"))
    )
    for path in sorted(glob.glob(str(HN_DIR / "item_*.json"))):
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if payload["id"] == STORY_ID:
            continue  # the story is registered last, so ``kids`` can be overridden
        router.get(ITEM_URL.format(item_id=payload["id"])).mock(httpx.Response(200, json=payload))
    story = item(STORY_ID)
    if kids is not None:
        story = {**story, "kids": kids}
    router.get(ITEM_URL.format(item_id=STORY_ID)).mock(httpx.Response(200, json=story))


# ------------------------------------------------------------------ fetch


async def test_the_newest_hiring_thread_and_its_comments_are_read(
    router: respx.MockRouter, connector: HnHiringConnector, client: HttpClient
) -> None:
    mock_api(router)
    comments = await collect(connector, make_ctx(client))
    assert len(comments) == 14
    assert {c.story_id for c in comments} == {STORY_ID}
    assert comments[0].story_title.startswith("Ask HN: Who is hiring?")


async def test_only_json_urls_are_fetched_which_is_what_robots_allows(
    router: respx.MockRouter, connector: HnHiringConnector, client: HttpClient
) -> None:
    """``hacker-news.firebaseio.com/robots.txt`` disallows ``/`` but allows ``/*.json$``."""
    mock_api(router)
    await collect(connector, make_ctx(client))
    assert client.stats.robots_denied == 0
    fetched = [str(call.request.url) for call in router.calls]
    assert all(url.endswith(".json") for url in fetched if "robots.txt" not in url)


async def test_the_comment_cap_bounds_a_run(
    router: respx.MockRouter, regions: RegionsConfig, client: HttpClient
) -> None:
    mock_api(router)
    connector = HnHiringConnector(connector_config("hn_hiring", max_comments_per_thread=3), regions)
    assert len(await collect(connector, make_ctx(client))) == 3


async def test_no_hiring_thread_is_reported_as_a_problem(
    router: respx.MockRouter, connector: HnHiringConnector, client: HttpClient
) -> None:
    router.get("https://hacker-news.firebaseio.com/robots.txt").mock(
        httpx.Response(200, text=(HN_DIR / "robots.txt").read_text(encoding="utf-8"))
    )
    router.get(USER_URL).mock(httpx.Response(200, json={"submitted": [1, 2, 3, 4]}))
    for item_id in (1, 2, 3, 4):
        router.get(ITEM_URL.format(item_id=item_id)).mock(
            httpx.Response(200, json={"id": item_id, "title": "Ask HN: Who wants to be hired?"})
        )
    ctx = make_ctx(client)
    assert await collect(connector, ctx) == []
    assert any("no 'Ask HN: Who is hiring?' thread" in problem for problem in ctx.problems)


async def test_a_failing_item_does_not_stop_the_run(
    router: respx.MockRouter, connector: HnHiringConnector, client: HttpClient
) -> None:
    mock_api(router, kids=[DISCORD, 999, SWINGVISION])
    router.get(ITEM_URL.format(item_id=999)).mock(httpx.Response(503))
    ctx = make_ctx(client)
    comments = await collect(connector, ctx)
    assert [c.item["id"] for c in comments] == [DISCORD, SWINGVISION]
    assert any("item 999" in problem for problem in ctx.problems)


def test_only_who_is_hiring_threads_are_read() -> None:
    assert is_hiring_thread("Ask HN: Who is hiring? (September 2026)")
    assert not is_hiring_thread("Ask HN: Who wants to be hired? (September 2026)")
    assert not is_hiring_thread("Ask HN: Freelancer? Seeking freelancer?")
    assert not is_hiring_thread(None)


# ------------------------------------------------------------------ header parsing


def test_a_pipe_header_is_classified_by_field_shape(regions: RegionsConfig) -> None:
    """Position is unreliable; each field is tested for what it is."""
    header = parse_header(
        "Acme | Senior Backend Engineer | San Francisco, CA | Part-time | https://acme.com",
        regions,
    )
    assert header.company == "Acme"
    assert header.role == "Senior Backend Engineer"
    assert header.location == "San Francisco, CA"
    assert header.commitment == "Part-time"
    assert header.url == "https://acme.com"


def test_fields_in_any_order_still_land_correctly(regions: RegionsConfig) -> None:
    header = parse_header(
        "MONUMENTAL | https://www.monumental.co/ | Palo Alto, CA | Full Time | Onsite", regions
    )
    assert (header.company, header.location, header.commitment) == (
        "MONUMENTAL",
        "Palo Alto, CA",
        "Full Time",
    )
    assert header.url == "https://www.monumental.co/"


def test_markdown_asterisks_are_not_part_of_a_field(regions: RegionsConfig) -> None:
    header = parse_header(item(FASTLY)["text"].split("<p>")[0], regions)
    assert header.company == "Fastly"


def test_a_comment_with_no_pipes_has_no_header_company(regions: RegionsConfig) -> None:
    """Otherwise the entire opening paragraph becomes the "company name"."""
    header = parse_header("SwingVision is the AI tennis app for everyone.", regions)
    assert header.company is None
    assert leading_name("SwingVision is the AI tennis app.") == "SwingVision"
    assert leading_name("we are hiring") is None


# ------------------------------------------------------------------ extraction


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # SwingVision links only to Deel; keying on ``app.deel.com`` would merge every Deel
        # customer into one company.
        ("See https://app.deel.com/job-boards/swingvision/job-details/abc.", None),
        # Greenhouse's short-link host, seen live on a real September 2026 comment.
        ("Apply at https://grnh.se/6f1a2b3c and https://boards.greenhouse.io/baton", None),
        # Press coverage a poster linked is not the poster's own site.
        ("Read https://www.mercurynews.com/2026/09/01/x then https://paramark.com", "paramark.com"),
        # A careers subdomain is the same company as its bare domain (SPEC §8 step 1).
        ("We are hiring: https://careers.snowflake.com/us/en", "snowflake.com"),
        ("Apply: https://go.trymira.com/careers", "trymira.com"),
        ("https://acme.io", "acme.io"),
        ("no links at all", None),
    ],
)
def test_only_a_plausible_company_link_becomes_the_domain(text: str, expected: str | None) -> None:
    options = connector_options()
    assert extract_domain(text, options.ignore_link_hosts, options.strip_subdomains) == expected


def test_the_first_usable_link_wins() -> None:
    options = connector_options()
    text = "https://app.deel.com/x and then our site https://swingvision.app"
    assert (
        extract_domain(text, options.ignore_link_hosts, options.strip_subdomains)
        == "swingvision.app"
    )


def connector_options() -> Any:
    from ingest.connectors.hn_hiring import HnHiringOptions

    return HnHiringOptions.model_validate(connector_config("hn_hiring").options)


def test_emails_the_poster_wrote_are_extracted() -> None:
    assert extract_emails("Mail jobs@acme.com or hiring@acme.com.") == [
        "jobs@acme.com",
        "hiring@acme.com",
    ]
    assert extract_emails("no address here") == []


def test_role_bullets_become_extra_jobs() -> None:
    text = (
        "- Data Labeling Intern: https://example.com/a\n"
        "* Head of Product — https://example.com/b\n"
        "not a bullet: https://example.com/c"
    )
    assert extract_role_bullets(text) == [
        ("Data Labeling Intern", "https://example.com/a"),
        ("Head of Product", "https://example.com/b"),
    ]


# ------------------------------------------------------------------ to_records


def test_a_bay_area_comment_becomes_a_company_with_one_job(
    connector: HnHiringConnector,
) -> None:
    record = records(connector, DISCORD)[0]
    assert record.name == "Discord"
    assert [loc.city for loc in record.locations] == ["San Francisco"]
    assert record.source_url == f"https://news.ycombinator.com/item?id={DISCORD}"
    assert len(record.jobs) == 1
    job = record.jobs[0]
    assert job.external_id == str(DISCORD)
    assert job.title == "Senior Software Engineer, Application Security"
    assert job.role_family is enums.RoleFamily.SECURITY
    assert job.seniority is enums.Seniority.SENIOR
    assert job.posted_at == datetime.fromtimestamp(item(DISCORD)["time"], tz=UTC)
    # A comment is a slice of a company's openings, never its whole board (SPEC §2, §5).
    assert record.jobs_complete is False


def test_a_prose_comment_yields_a_job_per_role_bullet(connector: HnHiringConnector) -> None:
    record = records(connector, SWINGVISION)[0]
    assert record.name == "SwingVision"
    assert [loc.city for loc in record.locations] == ["Berkeley"]
    titles = [job.title for job in record.jobs]
    assert titles[0].startswith("Role")  # the comment itself
    assert "Data Labeling Intern" in titles
    assert "Head of Product" in titles
    assert [job.external_id for job in record.jobs][:2] == [
        str(SWINGVISION),
        f"{SWINGVISION}#1",
    ]


def test_one_comment_does_not_make_every_role_in_it_an_internship(
    connector: HnHiringConnector,
) -> None:
    """The comment body advertises an intern role *and* two senior ones."""
    jobs = {job.title: job for job in records(connector, SWINGVISION)[0].jobs}
    assert jobs["Data Labeling Intern"].employment_type is enums.EmploymentType.INTERNSHIP
    assert jobs["Head of Product"].employment_type is enums.EmploymentType.UNKNOWN


def test_an_unclassifiable_title_is_other_and_still_a_job(connector: HnHiringConnector) -> None:
    """SPEC §7.1, §13: the comment-level job of a prose comment has no role in its header, so
    it is titled after the company — and is still ingested."""
    job = records(connector, SWINGVISION)[0].jobs[0]
    assert job.role_family is enums.RoleFamily.OTHER
    assert job.title and job.url


def test_a_deleted_comment_is_skipped(connector: HnHiringConnector) -> None:
    assert records(connector, DELETED) == []
    assert connector.skipped == {"empty_or_deleted": 1}


@pytest.mark.parametrize("item_id", [49523712, 49523950, 49524580, 49527388, 49559854])
def test_comments_outside_the_region_are_skipped(
    connector: HnHiringConnector, item_id: int
) -> None:
    assert records(connector, item_id) == []
    assert connector.skipped["outside_region"] == 1


def test_every_recorded_comment_is_either_mapped_or_counted(
    connector: HnHiringConnector,
) -> None:
    """No comment may silently vanish: each one becomes a record or a counted skip."""
    ids = [
        json.loads(Path(path).read_text(encoding="utf-8"))["id"]
        for path in sorted(glob.glob(str(HN_DIR / "item_*.json")))
    ]
    comment_ids = [item_id for item_id in ids if item(item_id).get("type") == "comment"]
    mapped = sum(len(records(connector, item_id)) for item_id in comment_ids)
    assert mapped + sum(connector.skipped.values()) == len(comment_ids)
    assert mapped == 2  # Discord and SwingVision are the Bay Area comments


def test_a_published_email_becomes_a_published_contact(regions: RegionsConfig) -> None:
    """SPEC §6: only addresses the company itself published, and never a guess."""
    connector = HnHiringConnector(connector_config("hn_hiring"), regions)
    raw = HnComment(
        story_id=STORY_ID,
        story_title="Ask HN: Who is hiring? (September 2026)",
        item={
            "id": 1,
            "type": "comment",
            "time": 1788286616,
            "text": "Acme | Backend Engineer | Berkeley, CA | Full-time<p>Email jobs@acme.com",
        },
    )
    record = next(iter(connector.to_records(raw)))
    assert [(c.kind, c.value, c.confidence) for c in record.contacts] == [
        (enums.ContactKind.EMAIL, "jobs@acme.com", enums.ContactConfidence.PUBLISHED)
    ]
