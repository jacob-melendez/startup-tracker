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
    find_city,
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
FASTLY = 49523835  # header wrapped in asterisks, no configured city


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
    """The connector, pinned to ONE thread whatever ``config/connectors.yaml`` ships.

    Every fetch test below mocks one thread and its comments, so reading the shipped value would
    make them fail the day that value changes — which it did, when SPEC §12 Phase 8 raised it so a
    monthly run missed to a sleeping machine is not lost for good. What the shipped number is is a
    claim about operations, and ``tests/test_docs.py`` is where that claim is checked against the
    README; what this file tests is the connector, so it fixes the input it is exercising.
    """
    return HnHiringConnector(connector_config("hn_hiring", max_threads=1), regions)


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
    connector = HnHiringConnector(
        connector_config("hn_hiring", max_comments_per_thread=3, max_threads=1), regions
    )
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


# --------------------------------------------------------- which city (find_city, SPEC §11)
#
# The three ranks of :func:`find_city`, one test each; then the two things the ranking is not —
# the aliases that decide which spellings are candidates at all, and the veto that removes a
# mention from the running before any of the ranks see it. All of them read the *shipped*
# ``config/regions.yaml``: the pairs that make each rule matter — a city name inside another,
# one name in two metros, one name in a state the file does not configure, the abbreviations
# posters actually type — are properties of the file that ships, and a fixture config of
# invented cities would prove the ranking sorts without proving it sorts the data this connector
# will really meet.


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("We are a small team hiring in South San Francisco.", id="bare"),
        pytest.param("We are a small team hiring in South San Francisco, CA.", id="with state"),
        pytest.param(
            "Not San Francisco itself — the office is in South San Francisco.", id="both named"
        ),
    ],
)
def test_a_city_whose_name_contains_another_is_not_read_as_the_shorter_one(
    text: str, regions: RegionsConfig
) -> None:
    """A bug that was live before §12 Phase 7: ``San Francisco`` is a whole-word match *inside*
    ``South San Francisco`` and stands earlier in the file, so taking the first match in file
    order filed every South San Francisco posting under San Francisco — a different city, and
    on a national list a different metro entirely.

    Three spellings, because each is defended by a different rank. In the first two the
    contained name matches only inside the longer one, so it also starts later and rank 3 would
    do; the ``", CA"`` is there because both mentions *end* at the same character, so a state
    qualifier applies to the wrong one exactly as well as to the right one and rank 1 cannot
    separate them. The third case is the one that isolates rank 2: the shorter name is written
    out on its own and written first, so only the length of the configured name can prefer the
    city the comment is actually about.
    """
    found = find_city(text, regions)
    assert found is not None
    assert found.city == "South San Francisco"


def test_a_state_qualified_mention_beats_a_bare_city_name_elsewhere_in_the_body(
    regions: RegionsConfig,
) -> None:
    """Rank 1. A bare configured name in a comment is often not a place at all — a founder's
    surname, a product, a street — and a 48-city list meets far more of them than an 18-city
    one did. A mention the poster wrote out with its state is the one they meant.

    ``Austin`` and ``Boston`` are both six letters and the bare one comes first, so ranks 2
    and 3 both favour the wrong answer here and only rank 1 can produce the right one.
    """
    found = find_city("Austin, our founder, is hiring. The team sits in Boston, MA.", regions)
    assert found is not None
    assert (found.city, found.state, found.metro) == ("Boston", "MA", "Boston")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(
            "Lucia | Director of Corp Dev | Remote (NYC / SEA / global overlap)",
            ("New York", "NY", "New York"),
            id="NYC",
        ),
        pytest.param(
            "Uncountable | NY, SF, London and Toronto (In-Person) | Full-Stack Engineering",
            ("San Francisco", "CA", "Bay Area"),
            id="SF",
        ),
    ],
)
def test_a_comment_that_only_abbreviates_its_city_is_still_in_region(
    text: str, expected: tuple[str, str, str], regions: RegionsConfig
) -> None:
    """SPEC §11: a comment naming a city only by a spelling ``config/regions.yaml`` lists under
    ``aliases`` resolves to that city, with the canonical name, state and metro.

    Both texts are real September 2026 "Who is hiring?" comments, from the 220 of 273 that named
    no configured city and were dropped as out of region before this phase — 22 of those 220
    write New York this way and 16 write San Francisco this way. That is the whole argument for
    the key: a spelling this connector cannot see is not a near miss but a company thrown away,
    so the alias list is the difference between reading a metro's comments and reading a
    fraction of them. What is asserted is the *canonical* name, because an alias is a spelling
    to match on and never a value to store.
    """
    found = find_city(text, regions)
    assert found is not None
    assert (found.city, found.state, found.metro) == expected
    # The canonical name is nowhere in the comment: only an alias can have matched it.
    assert found.city not in text


def test_a_bare_city_name_still_resolves_when_it_is_the_only_candidate(
    regions: RegionsConfig,
) -> None:
    """Rank 1 orders candidates; it is not a filter. Most posters write a bare city and nothing
    else, so a rule that *required* the state would throw away most of this connector's recall
    (SPEC §7.1's principle, applied to geography: the ranking chooses between mentions, it
    never rejects the only one there is)."""
    found = find_city("Small team, our office is in Redmond, four days a week.", regions)
    assert found is not None
    assert (found.city, found.state, found.metro) == ("Redmond", "WA", "Seattle")


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("Acme | Backend Engineer | Austin, MN | Full-time", id="header field"),
        pytest.param("We are a small team in Cambridge, MD — apply within.", id="body prose"),
        pytest.param("Hiring in Glendale, AZ (onsite four days a week).", id="parenthesised"),
        pytest.param("Newton, KS. Full-time, onsite.", id="sentence end"),
    ],
)
def test_a_configured_name_written_with_another_states_code_is_not_that_city(
    text: str, regions: RegionsConfig
) -> None:
    """The veto. A configured city name immediately followed by a comma and a two-letter code
    that is **not** that city's own state is not a mention of that city at all: it is a poster
    naming a different place that happens to share the name, and it is thrown out before the
    ranking rather than ranked (SPEC §11).

    Every text here is a real US city in a state ``config/regions.yaml`` does not configure, and
    each shares its name with one the file does. Without the veto the qualifier merely failed to
    *promote* the mention, so it still counted as a bare match and still won — filing a company
    that told you plainly where it was under a metro two time zones away, which then scopes what
    SPEC §8 step 2 will merge it with. Being wrong about the metro is worse than having no
    metro: an out-of-region comment is counted and visible, a misfiled one is neither.

    Four spellings, because the qualifier has to be read the same wherever it sits: in a pipe
    header field, mid-sentence, before a bracket, and at the end of a sentence.
    """
    assert find_city(text, regions) is None


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(
            "Acme | Chief of Staff | New York, ON-SITE (5 days) | Full-time", id="on-site"
        ),
        pytest.param("Hiring in Seattle, IN-PERSON, four days a week.", id="in-person"),
        pytest.param("Our Boston, CA-based team is growing.", id="hyphen after a real code"),
    ],
)
def test_a_hyphenated_word_after_a_city_is_not_read_as_a_state_code(
    text: str, regions: RegionsConfig
) -> None:
    """The veto reads a two-letter code, and the token it reads has to be the whole word.

    ``\\b`` is satisfied by a hyphen, so a pattern ending there cut ``ON`` out of ``", ON-SITE"``
    and ``IN`` out of ``", IN-PERSON"`` and handed each to the veto as somebody else's state
    code — dropping the company outright, since the veto discards rather than ranks. This thread
    is written in all-capitals vernacular, so those are the shapes it really contains: one lost
    record in 742 live comments on 2026-09-08, the first of them a real posting whose location
    field was exactly the first case here.

    The pattern ends on ``(?![\\w-])`` instead. The third case is the other half of that rule and
    the one that keeps it honest: ``CA-based`` is a genuine code with a hyphen behind it, and it
    must stop vetoing too — what the token test says is "these two letters are not a state code
    here", never "this is not the code it looks like".

    What is left is the same words with a space in them: ``", ON SITE"`` still reads as a code,
    and nothing short of a list of the fifty real ones could tell it from one. No comment in that
    sample wrote it, and that list is region logic ``config/regions.yaml`` owns (SPEC §11).
    """
    found = find_city(text, regions)
    assert found is not None
    assert found.city in text


def test_the_veto_drops_a_mention_the_ranking_would_otherwise_have_preferred(
    regions: RegionsConfig,
) -> None:
    """The veto has to happen *before* the ranking, not as a tie-break inside it.

    Both names here are eight letters and neither mention is state-qualified for its own city,
    so ranks 2 and 3 decide — and both favour the wrong one, which is written first. Only
    discarding the mention outright can produce the right answer, which is what makes this the
    case that distinguishes a veto from one more rank.
    """
    found = find_city("We started in Pasadena, TX and the team now sits in Berkeley.", regions)
    assert found is not None
    assert (found.city, found.state, found.metro) == ("Berkeley", "CA", "Bay Area")


def test_the_veto_throws_out_one_mention_and_not_the_city(regions: RegionsConfig) -> None:
    """What is rejected is a mention, not a name. A comment that names the same city twice — once
    as the other state's city and once as its own — is still a comment from the configured one,
    so the second mention is ranked normally and wins.

    A veto scoped to the city instead would turn every passing reference to a same-named place
    into a dropped company, which trades one misfiling for a second one that is invisible."""
    found = find_city("I grew up in Austin, MN; we are hiring in Austin, TX.", regions)
    assert found is not None
    assert (found.city, found.state, found.metro) == ("Austin", "TX", "Austin")


def test_the_code_that_vetoes_a_stranger_still_promotes_the_city_it_belongs_to(
    regions: RegionsConfig,
) -> None:
    """Rank 1 survived the veto. The two read the same two letters after a mention, and it would
    be an easy mistake for the narrowing to leave a matching code meaning nothing at all.

    ``Cambridge`` is longer and written first, so ranks 2 and 3 both prefer it; only the state
    the poster wrote out for the city they meant produces the other answer."""
    found = find_city("Cambridge is where our founder studied. We hire in Austin, TX.", regions)
    assert found is not None
    assert (found.city, found.state, found.metro) == ("Austin", "TX", "Austin")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(
            "The team sits in Austin, Texas, and our founder studied in Cambridge.",
            ("Austin", "TX", "Austin"),
            id="promotes its own state",
        ),
        pytest.param(
            "Office in Cambridge, England — remote-friendly.",
            ("Cambridge", "MA", "Boston"),
            id="never vetoes",
        ),
    ],
)
def test_a_written_out_qualifier_still_only_ever_costs_a_mention_a_rank(
    text: str, expected: tuple[str, str, str], regions: RegionsConfig
) -> None:
    """A state written out in words behaves exactly as it did before the veto: it can promote a
    mention and it can never discard one.

    The promotion is now an *equality*, not a guess. ``config/regions.yaml`` carries a
    ``state_name`` beside each ``state`` — how that code is spelled out — and ``_names_state``
    compares the text after the comma against the code and that name, case- and
    whitespace-folded. What stood here before was a letter heuristic, written precisely to avoid
    putting a table of the fifty state names inside a connector when SPEC §11 keeps every such
    literal in the config. It kept the table out of the code and got the answer wrong: it also
    matched a *neighbouring* state's written-out name, and rank 1 is what chooses between
    mentions, so a spurious promotion did not cost a rank, it picked the wrong mention outright.
    The names went into the file the codes were already in, which is where they belonged.

    The asymmetry survives, and is still the reason the veto reads two-letter codes only. The
    file names the configured states and no others, so "not this city's state, written out"
    spans every state the config omits, every country, and every capitalised word English puts
    after a comma. The second case is what that costs and it is worth paying: a place named for
    the one the config configures is filed under the configured metro, which is one wrong
    record, while vetoing on prose would drop real comments behind every "…, or remote" and
    "…, we are hiring", which is many. A region that configures no ``state_name`` simply stops
    promoting — a rank, never a record.
    """
    found = find_city(text, regions)
    assert found is not None
    assert (found.city, found.state, found.metro) == expected


def test_an_alias_matched_in_a_comment_is_stored_as_the_canonical_city(
    connector: HnHiringConnector,
) -> None:
    """SPEC §5, §11: an alias is what matches the poster's text; the canonical name is what
    reaches the database.

    The header field is the abbreviation and nothing else, so only the alias can have matched —
    and the ``LocationRecord`` still carries the one spelling this place is stored under, with
    its own state and metro. Storing what the poster wrote instead would put a second row past
    ``uq_locations_city_state`` for one city, which is the failure that makes an alias a lookup
    key rather than an extra city in the file. The job keeps the poster's own words in
    ``location_text``, since that field is what the comment said and not what it resolved to.
    """
    raw = HnComment(
        story_id=STORY_ID,
        story_title="Ask HN: Who is hiring? (September 2026)",
        item={
            "id": 3,
            "type": "comment",
            "time": 1788286616,
            "text": "Acme | Backend Engineer | NYC | Full-time",
        },
    )

    (record,) = connector.to_records(raw)

    (location,) = record.locations
    assert (location.city, location.state, location.metro, location.country) == (
        "New York",
        "NY",
        "New York",
        "US",
    )
    assert record.jobs[0].location_text == "NYC"
    assert connector.skipped == {}


def test_a_comment_whose_only_city_is_in_another_state_is_dropped_and_not_misfiled(
    connector: HnHiringConnector,
) -> None:
    """The veto end to end, and the highest-value claim in this file: a company in a same-named
    city in a state no region configures does not become a company in the configured metro.

    It is dropped with the ordinary counted reason instead, which is the honest outcome — the
    comment really is outside every configured region, and ``skipped`` is where a run reports
    what it did not keep (SPEC §7.2). The alternative is not a lost record but a silently wrong
    one: a wrong ``Location.metro`` is what the region filter reads and what SPEC §8 step 2
    scopes name matches to, so it spreads.
    """
    raw = HnComment(
        story_id=STORY_ID,
        story_title="Ask HN: Who is hiring? (September 2026)",
        item={
            "id": 4,
            "type": "comment",
            "time": 1788286616,
            "text": "Acme | Backend Engineer | Austin, MN | Full-time",
        },
    )

    assert list(connector.to_records(raw)) == []
    assert connector.skipped == {"outside_region": 1}


def test_a_comment_from_a_second_metro_carries_that_metros_own_state_and_label(
    connector: HnHiringConnector,
) -> None:
    """§12 Phase 7 end to end through the connector: a comment naming a city of a metro other
    than the first configured one becomes a record in *that* metro.

    Jersey City is deliberate. It is in the New York metro and not in New York state, so
    ``config/regions.yaml`` writes it in the ``{name, state}`` mapping form, and the record can
    only carry ``NJ`` if the state travelled with the matched entry. The connector used to
    resolve a city *name* to a state by scanning for the first configured entry with that name,
    which for any name two regions both use stored the company under the wrong state — and the
    wrong metro then scopes what SPEC §8 will merge it with.
    """
    raw = HnComment(
        story_id=STORY_ID,
        story_title="Ask HN: Who is hiring? (September 2026)",
        item={
            "id": 2,
            "type": "comment",
            "time": 1788286616,
            "text": "Acme | Backend Engineer | Jersey City, NJ | Full-time",
        },
    )

    (record,) = connector.to_records(raw)

    (location,) = record.locations
    assert (location.city, location.state, location.metro, location.country) == (
        "Jersey City",
        "NJ",
        "New York",
        "US",
    )
    assert connector.skipped == {}


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


def test_an_address_a_poster_capitalised_is_stored_in_the_one_published_form() -> None:
    """SPEC §6 allows two sources for a published address — a ``mailto:`` on the company's own
    site and an address a founder posted here — and they must store it identically.

    ``company_site`` reads ``mailto:HR@atom-computing.com`` off the careers page and stores
    ``hr@atom-computing.com``; a comment writing the same address with capitals has to land on
    that row. ``uq_contacts_company_id_kind_value`` is a plain unique on the value and
    ``_upsert_contacts`` stores it verbatim, so an address normalised on one side and not the
    other is two "Published" rows for one address in the panel — and the same inside a single
    comment that writes it twice.
    """
    assert extract_emails("Write HR@Atom-Computing.com") == ["hr@atom-computing.com"]
    assert extract_emails("Mail jobs@acme.com or JOBS@ACME.COM.") == ["jobs@acme.com"]


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
    """Berlin, remote-EU, remote-US, Switzerland and Hong Kong: none of the 48 configured
    cities appears in any of them, so each is dropped with a counted reason rather than stored
    under whatever city a loose match found (SPEC §1, §7.2)."""
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
    assert mapped == 2  # only Discord and SwingVision name a configured city


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
