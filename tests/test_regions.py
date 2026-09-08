"""SPEC §12 Phase 7's third clause: "verify no Bay-Area-specific logic exists outside that
config file" — generalized, because by the end of the phase there is no *one* region to be
specific about. ``config/regions.yaml`` is the only place a metro or a city name may appear
(SPEC §11, CLAUDE.md), and this module is what makes that a build failure rather than a wish.

Two halves, and they check the rule from opposite sides.

*The literal guard* reads the shipped config, takes every metro name and every spelling of every
city in it — canonical names and the aliases sources write them as — and scans the source tree
for them. Its needles come from the file, so it keeps holding when the file changes: rename a
metro, or teach it a spelling a source uses, and that name becomes the next thing that may not
be written in code, with no edit here.

*The alternate-config suite* drives the real code with a region file the shipped one does not
contain — Denver, and its neighbours — end to end: the pipeline adopts the metro, the location
upsert stores it, entity resolution (SPEC §8) scopes name matches to it, ``sec_edgar`` searches
its cities, ``hn_hiring`` finds them in a comment, ``seed`` resolves an entry in it. The guard
proves no shipped name is *written* in the code; this proves the code has not learnt one some
other way — a default, a fallback, a lookup table keyed on what happened to ship. Nothing below
names a shipped metro or city, deliberately: a test that passes only against the file that
happens to be checked in would be testing the file, not the code.

The loader's own rules (:mod:`ingest.config`) are checked here too, since they are the guarantee
the rest of the phase rests on: one metro per label, one metro per city, one city per spelling,
at least one region enabled, and the two forms a ``cities:`` entry may be written in.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple

import pytest
import yaml
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import web.templating
from db.models import Company, Location
from ingest.base import CompanyRecord, LocationRecord
from ingest.config import RegionsConfig, load_regions_config
from ingest.connectors.hn_hiring import HnComment, HnHiringConnector, find_city
from ingest.connectors.sec_edgar import SecEdgarConnector
from ingest.pipeline import _fill_metros, upsert_company_record
from ingest.seed import SeedConnector, SeedFile, SeedResult
from tests.support import connector_config

ROOT = Path(__file__).resolve().parent.parent
NOW = datetime(2026, 9, 8, 12, tzinfo=UTC)


# ============================================================ half one: the literal guard


#: Trees scanned whole, and the extensions taken from each. ``None`` means every file:
#: ``db/``, ``ingest/`` and ``migrations/`` contain nothing but source, ``script.py.mako``
#: included — it is the template every future migration is generated from, so a region name
#: written there would be copied into code that does not exist yet. ``web/`` is narrowed to the
#: four kinds this project writes by hand — ``.js`` among them, because SPEC §12 Phase 4 permits
#: one small glue script and a region name defaulted in it would be exactly this guard's business.
SCANNED_TREES: dict[str, frozenset[str] | None] = {
    "db": None,
    "ingest": None,
    "web": frozenset({".py", ".html", ".css", ".js"}),
    "migrations": None,
}

#: Vendored third-party code, skipped by **path** and never by file type: this project's rules do
#: not reach into somebody else's minified bundle, but the kind of file that bundle happens to be
#: is a kind the project may write itself (SPEC §12 Phase 4). Excluding the whole ``.js`` type to
#: exclude this one file would leave a permitted, hand-written file kind unscanned for ever.
VENDORED: frozenset[str] = frozenset({"web/static/htmx.min.js"})

#: Single files outside those trees. Each one is somewhere a region name has plausibly been
#: written by hand: the CLI's help and its printed sections, the scheduler's job names, the
#: settings and logging defaults, the Makefile's targets, the Compose service names and
#: environment, the packaging description, and Alembic's own configuration.
SCANNED_FILES: tuple[str, ...] = (
    "cli.py",
    "scheduler.py",
    "settings.py",
    "logging_config.py",
    "Makefile",
    "docker-compose.yml",
    "pyproject.toml",
    "alembic.ini",
)

#: Exempt, and why. ``config/`` holds the region file itself, which is the entire point.
#: ``tests/`` is data: a connector test is only honest if its fixture is a real comment naming
#: a real city, and this module's own alternate-config suite has to name *some* metro.
#: ``docs/`` and the two Markdown files at the root are prose describing the shipped config —
#: a README that cannot name a city cannot explain the file it documents — and are checked by
#: eye, plus by ``tests/test_docs.py`` for the claims that must track the code.
EXEMPT_PREFIXES: tuple[str, ...] = ("config/", "tests/", "docs/", "README.md", "CLAUDE.md")


class Mention(NamedTuple):
    """One line of one file that names a configured region literal."""

    line: int
    literal: str
    text: str


def region_literals(config: RegionsConfig) -> frozenset[str]:
    """Every metro name and every *spelling* of every city the config file writes out.

    Read off the parsed config rather than off the raw YAML so both spellings of a ``cities:``
    entry contribute the same way, and taken from ``regions``/``all_cities`` rather than from
    the enabled subset: a region switched off with ``enabled: false`` is still described in the
    file, so its names are still region literals that code may not contain. Switching a metro
    back on must never be the moment a hard-coded name starts contradicting the config.

    A city contributes every name it answers to (``RegionCity.names``: its canonical name and
    each configured alias), because an alias is not a lesser name. It is the string connectors
    match on, it is chosen from what a source was measured to write, and code spelling one out
    goes stale the day the file is edited exactly as code spelling out the canonical name does.
    """
    return frozenset(
        {region.metro for region in config.regions}
        | {name for entry in config.all_cities for name in entry.names}
    )


def literal_patterns(config: RegionsConfig) -> dict[str, re.Pattern[str]]:
    """One whole-word, case-insensitive pattern per literal.

    Case-insensitive, because a metro written in lower case in a docstring is the same drift as
    the configured spelling in a constant. Whole-word — ``(?<!\\w)``/``(?!\\w)`` rather than
    ``\\b``, which would not fire on a name whose first or last character is not a word
    character — so a configured ``Boulder`` is not matched inside ``Bouldering`` while a
    configured ``Denver`` still is inside ``Denver-based``. (The examples name the fixture
    region below rather than a shipped one, so that this docstring cannot go stale the way the
    ones it is written to catch do.)

    A name of several words is matched across *any* run of non-word characters between them,
    not only the single space the config writes it with. A metro reaches code as
    ``Foo-Bar-specific`` in a sentence, as ``foo_bar`` in a constant and as ``foo.bar`` in a
    dotted key at least as readily as it does spelled the way the file spells it, and every one
    of those is the same hard-coded region. Matching only the space would have guarded the
    single-word names and quietly exempted every other one — the majority of the needles here —
    which is the shape of blind spot this module exists to deny. The words themselves stay in
    their configured order and the outer boundaries still apply, so this widens the separator
    and nothing else.

    Three shapes still get through, and they are named rather than glossed because a guard whose
    limits are unwritten reads as a guarantee. The scan is line-by-line, so a name the formatter
    broke across two lines is missed — the alternative is normalising whitespace across a whole
    file, which turns every line number in the failure message into a guess, and that is a bad
    trade. The separator class needs at least one non-word character, so a name run together with
    no separator at all is missed, as is one whose separator is url-encoded (the encoding's own
    digits are word characters, so the words are no longer adjacent). Neither is worth widening
    for: dropping the boundary requirement would match those names inside longer words, which is
    the false-positive class the outer lookarounds exist to prevent, and each remaining shape is
    a spelling no formatter produces on its own — it has to be typed deliberately.
    """
    return {
        literal: re.compile(
            r"(?<!\w)" + r"[\W_]+".join(map(re.escape, literal.split())) + r"(?!\w)",
            re.IGNORECASE,
        )
        for literal in region_literals(config)
    }


def mentions(text: str, patterns: Mapping[str, re.Pattern[str]]) -> list[Mention]:
    """Every configured literal named in ``text``, with its line number.

    Comments and docstrings are scanned like any other line and that is deliberate: a docstring
    that explains what the code does by naming one region is exactly the drift this guard
    exists to catch, because it goes stale silently the moment the config gains a metro.
    """
    found: list[Mention] = []
    for number, line in enumerate(text.splitlines(), start=1):
        for literal in sorted(patterns):
            if patterns[literal].search(line):
                found.append(Mention(number, literal, line.strip()))
    return found


def scanned_files() -> list[Path]:
    """Every file the guard reads, absolute and sorted.

    Walked from the tree rather than listed by hand, so a module added to ``ingest/`` next week
    is covered the day it lands and not the day somebody remembers this file. :data:`VENDORED`
    is the one subtraction, and it names paths rather than types so that a file the project
    writes itself is never skipped for the company its file extension keeps.
    """
    found: list[Path] = []
    for tree, suffixes in SCANNED_TREES.items():
        for path in (ROOT / tree).rglob("*"):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            if path.relative_to(ROOT).as_posix() in VENDORED:
                continue
            if suffixes is None or path.suffix in suffixes:
                found.append(path)
    found += [ROOT / name for name in SCANNED_FILES]
    return sorted(found)


def test_no_source_file_names_a_configured_metro_or_city() -> None:
    """SPEC §11, §12 Phase 7, CLAUDE.md: ``config/regions.yaml`` is the only place
    region-specific logic may live, so no metro or city configured there may appear as a
    literal anywhere in the source tree — inside comments and docstrings included.

    Every offence is reported at once, with file, line and literal, because a guard that stops
    at the first one turns a five-minute cleanup into five runs. A false positive (a city name
    that is also an ordinary word) is still a failure to be fixed by rewording: the config can
    change under the code, and a sentence that reads as region-neutral today stops being an
    accident tomorrow.
    """
    patterns = literal_patterns(load_regions_config())
    offences = [
        f"{path.relative_to(ROOT).as_posix()}:{mention.line}: "
        f"{mention.literal!r} in: {mention.text[:80]}"
        for path in scanned_files()
        for mention in mentions(path.read_text(encoding="utf-8"), patterns)
    ]
    assert not offences, (
        "config/regions.yaml is the only place a metro or city name may appear (SPEC §11, "
        "§12 Phase 7, CLAUDE.md); these lines name one:\n" + "\n".join(offences)
    )


def test_the_guard_scans_the_source_tree_and_exempts_only_the_config_and_the_prose() -> None:
    """A guard that scans nothing, or looks for nothing, passes for ever without checking
    anything — which is worse than not having it, because it reads like coverage.

    So: the needle set is non-empty and holds both kinds of name, the scanned set is non-empty
    and reaches every corner the phase touched (a connector, the query layer, a template, the
    stylesheet, the CLI, a migration, the Makefile), and no exempt path has crept into it.

    The needle assertions check that each category of name is represented; that a *disabled*
    region's names are among them is pinned separately, by the test of that name below, because
    the shipped file enables every region and so cannot tell the two rules apart.
    """
    config = load_regions_config()
    literals = region_literals(config)
    assert {region.metro for region in config.regions} <= literals
    assert {entry.city for entry in config.all_cities} <= literals
    assert {alias for entry in config.all_cities for alias in entry.aliases} <= literals

    scanned = {path.relative_to(ROOT).as_posix() for path in scanned_files()}
    assert {
        "cli.py",
        "db/queries.py",
        "ingest/pipeline.py",
        "ingest/connectors/hn_hiring.py",
        "migrations/versions/0001_initial_schema.py",
        "web/filters.py",
        "web/static/styles.css",
        "web/templates/base.html",
        "Makefile",
        "pyproject.toml",
    } <= scanned
    # The hand-written glue script SPEC §12 Phase 4 allows is scanned like any other source
    # file; only the vendored bundle is skipped, and skipped by path, so this pair of assertions
    # says "that one file" and not "no JavaScript ever".
    web_suffixes = SCANNED_TREES["web"]
    assert web_suffixes is not None and ".js" in web_suffixes
    assert "web/static/htmx.min.js" not in scanned
    assert not [path for path in scanned if path.startswith(EXEMPT_PREFIXES)]


def test_a_configured_name_is_caught_in_a_comment_and_not_inside_a_longer_word() -> None:
    """The matcher itself, on synthetic lines — a whole-word, case-insensitive search that
    reads comments.

    The needle is taken from the config instead of being typed out, so this test cannot itself
    become the region literal it is checking for, and it keeps working when the config changes.
    """
    config = load_regions_config()
    patterns = literal_patterns(config)
    literal = sorted(region_literals(config))[0]

    assert mentions(f"# our office is in {literal} somewhere", patterns) == [
        Mention(1, literal, f"# our office is in {literal} somewhere")
    ]
    assert [
        m.literal for m in mentions(f'"""Docstring naming {literal.upper()}."""', patterns)
    ] == [literal]
    # A name that is only a substring of a longer word is not a mention of the place.
    assert mentions(f"prefix{literal}suffix", patterns) == []
    assert mentions("nothing regional on this line", patterns) == []


def test_a_multi_word_name_is_caught_whatever_separates_its_words(tmp_path: Path) -> None:
    """Most configured names are more than one word, and code almost never spells such a name
    the way the config does: it becomes ``Foo-Bar-specific`` in a sentence, ``foo_bar`` in a
    constant, ``foo.bar`` in a dotted key. All three are the hard-coded region the guard is for,
    so :func:`literal_patterns` joins the words with a separator class rather than escaping the
    name whole (which would have matched the config's own single space and nothing else).

    This case exists because the sibling above cannot cover it: its needle is whichever literal
    sorts first in the shipped file, and a single-word one matches the same either way — the
    exact reason a multi-word blind spot could sit here unnoticed. The fixture pins the
    mechanics; the sweep at the end pins that every multi-word needle the shipped file really
    ships is covered, so the hole cannot reopen for most of the names and none of the tests.
    """
    config = write_regions(
        tmp_path, "multi-word", [{"metro": "Front Range", "state": "CO", "cities": ["Lake City"]}]
    )
    patterns = literal_patterns(config)

    for spelling in ("Front Range", "Front-Range", "front_range", "FRONT.RANGE", "Front  Range"):
        assert [found.literal for found in mentions(f"# {spelling}-specific", patterns)] == [
            "Front Range"
        ], spelling
    # The outer boundaries still hold, and the words still have to be adjacent: a separator run
    # is not a licence to match two of a sentence's words that merely appear in the right order.
    assert mentions("# uptown Front Ranges are a landform", patterns) == []
    assert mentions("# the front of the range", patterns) == []

    shipped = literal_patterns(load_regions_config())
    multi_word = [literal for literal in sorted(shipped) if " " in literal]
    assert multi_word, "the sweep below checks nothing unless the shipped file has such a name"
    for literal in multi_word:
        assert shipped[literal].search(f"# {literal.replace(' ', '-')}-specific"), literal


def test_an_alias_is_a_region_literal_exactly_as_the_name_it_stands_for_is(
    tmp_path: Path,
) -> None:
    """SPEC §11, §12 Phase 7: the config now writes a place down in more than one way — a city's
    canonical ``name`` and the ``aliases`` sources spell it as — and the guard's needles are
    every one of those spellings.

    An alias is the string a text scan actually matches on, so code carrying one has the same
    two problems code carrying a canonical name has: it duplicates a decision the file owns, and
    it goes stale the moment the file is edited. Folding aliases into the needle set is what
    makes an alias written in a docstring a build failure like any other region literal.

    Driven from a fixture config, so it holds whatever the shipped file happens to alias today;
    the containment assertion then pins that the shipped aliases really are among the needles.
    """
    config = write_regions(
        tmp_path,
        "aliased",
        [
            {
                "metro": "Denver",
                "state": "CO",
                "cities": [{"name": "Denver", "aliases": ["Mile High City"]}, "Boulder"],
            }
        ],
    )

    assert region_literals(config) == {"Denver", "Boulder", "Mile High City"}
    patterns = literal_patterns(config)
    assert [found.literal for found in mentions("# posted from the Mile High City", patterns)] == [
        "Mile High City"
    ]

    shipped = load_regions_config()
    aliases = {alias for entry in shipped.all_cities for alias in entry.aliases}
    assert aliases <= region_literals(shipped)


def test_a_disabled_regions_names_are_still_region_literals(tmp_path: Path) -> None:
    """A region switched off with ``enabled: false`` is still described in the file, so its
    metro, its cities and their aliases are still names code may not contain — which is why
    :func:`region_literals` reads ``regions``/``all_cities`` and not the enabled subset.

    Flipping ``enabled: true`` must never be the moment a hard-coded name starts contradicting
    the config: by then the code that named it has shipped, and the failure arrives as wrong
    metros in the database rather than as a red build. Nothing else in this module can catch a
    narrowing to the enabled subset, because every other config it loads — the shipped one
    included — enables every region it describes, so the two rules agree everywhere but here.
    """
    config = write_regions(
        tmp_path,
        "disabled-still-guarded",
        [
            DENVER,
            {
                **PORTLAND,
                "enabled": False,
                "cities": [{"name": "Beaverton", "aliases": ["Bvtn"]}],
            },
        ],
    )

    assert [region.metro for region in config.enabled_regions] == ["Denver"]
    assert region_literals(config) == {"Denver", "Boulder", "Portland", "Beaverton", "Bvtn"}


# ================================================ half two: the whole stack on another region


def write_regions(tmp_path: Path, name: str, regions: Sequence[Mapping[str, Any]]) -> RegionsConfig:
    """Write a region file and load it through the real loader.

    ``load_regions_config`` is ``@cache``d on its path (and says so), so every case writes a
    file of its own: ``tmp_path`` separates the tests and ``name`` separates the files inside
    one test. Two cases sharing a path would silently get whichever config loaded first, and
    the second would then assert against the first one's regions.
    """
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump({"regions": list(regions)}, sort_keys=False), encoding="utf-8")
    return load_regions_config(path)


#: The fixture region: a metro the shipped ``config/regions.yaml`` does not contain, so nothing
#: below can pass by accident on a name the code was written around.
DENVER: dict[str, Any] = {"metro": "Denver", "state": "CO", "cities": ["Denver", "Boulder"]}
#: A second one, for the cases that need two metros to tell apart.
PORTLAND: dict[str, Any] = {
    "metro": "Portland",
    "state": "OR",
    "cities": ["Portland", "Beaverton"],
}


@pytest.fixture
def denver(tmp_path: Path) -> RegionsConfig:
    return write_regions(tmp_path, "denver", [DENVER])


def test_the_pipeline_adopts_a_metro_the_shipped_config_never_heard_of(
    denver: RegionsConfig,
) -> None:
    """``_fill_metros`` (design decision 5b, SPEC §11): for a configured city the config's
    spelling of city, state, country **and metro** replaces whatever the connector said, and a
    city no region claims keeps the connector's own label or is dropped for want of one.

    Driven with a region file the shipped config does not contain, so a pass means the function
    read the file it was given rather than the metros it grew up with.
    """
    filled = _fill_metros(
        [
            # A connector's own casing, and a region label of its own invention.
            LocationRecord(city="boulder", state="co", metro="Front Range", is_hq=True),
            # A city no configured region claims, with a label of its own: kept as reported.
            LocationRecord(city="Ithaca", state="NY", metro="Finger Lakes"),
            # The same, with nothing to file it under: dropped, since Location.metro is NOT NULL.
            LocationRecord(city="Ithaca", state="NY"),
        ],
        denver,
    )

    assert [(loc.city, loc.state, loc.country, loc.metro) for loc in filled] == [
        ("Boulder", "CO", "US", "Denver"),
        ("Ithaca", "NY", "US", "Finger Lakes"),
    ]


async def test_the_location_upsert_stores_the_configured_metro(
    session: AsyncSession, denver: RegionsConfig
) -> None:
    """SPEC §5, §11: the ``Location`` row a record lands in carries the metro the *config*
    names, in the config's own spelling, for a region the shipped file has never listed."""
    result = await upsert_company_record(
        session,
        CompanyRecord(
            name="Acme, Inc.",
            external_id="acme",
            locations=(LocationRecord(city="boulder", state="CO", is_hq=True),),
        ),
        "hn_hiring",
        now=NOW,
        regions=denver,
    )

    assert result.created is True
    (location,) = (await session.execute(select(Location))).scalars().all()
    assert (location.city, location.state, location.metro, location.country) == (
        "Boulder",
        "CO",
        "Denver",
        "US",
    )


async def test_entity_resolution_scopes_a_name_match_to_the_configured_metro(
    session: AsyncSession, tmp_path: Path
) -> None:
    """SPEC §8 step 2: an exact ``normalized_name`` match is scoped to the record's metro, and
    the metros come from the config.

    The two same-named companies are inserted in a deliberate order — the one in the *other*
    metro first, so it holds the lower id. Step 2 orders by ``Company.id``, so a resolution that
    had quietly stopped scoping by metro would return that first row and this test would fail;
    inserted the other way round it would pass either way and prove nothing.
    """
    denver = write_regions(tmp_path, "denver", [DENVER])
    portland = write_regions(tmp_path, "portland", [PORTLAND])
    elsewhere = await upsert_company_record(
        session,
        CompanyRecord(name="Acme, Inc.", locations=(LocationRecord(city="Portland", state="OR"),)),
        "seed",
        now=NOW,
        regions=portland,
    )
    local = await upsert_company_record(
        session,
        CompanyRecord(name="Acme, Inc.", locations=(LocationRecord(city="Boulder", state="CO"),)),
        "seed",
        now=NOW,
        regions=denver,
    )
    assert local.created is True
    assert local.company_id != elsewhere.company_id

    # No external id and no domain: only the metro-scoped name match can resolve these.
    again_local = await upsert_company_record(
        session,
        CompanyRecord(name="ACME", locations=(LocationRecord(city="Denver", state="CO"),)),
        "hn_hiring",
        now=NOW,
        regions=denver,
    )
    again_elsewhere = await upsert_company_record(
        session,
        CompanyRecord(name="ACME", locations=(LocationRecord(city="Beaverton", state="OR"),)),
        "hn_hiring",
        now=NOW,
        regions=portland,
    )

    assert (again_local.company_id, again_local.created) == (local.company_id, False)
    assert (again_elsewhere.company_id, again_elsewhere.created) == (elsewhere.company_id, False)
    # Two companies, not four: neither re-ingest created a row, and neither crossed the metro.
    assert len((await session.execute(select(Company.id))).scalars().all()) == 2


def test_sec_edgar_searches_every_city_of_the_configured_region(denver: RegionsConfig) -> None:
    """SPEC §4 Tier 1 #1: the EDGAR full-text search runs once per configured city per window,
    and the cities are the config's — which is also why the query count scales with the file
    (``docs/SOURCES.md``). Each city is searched as its own phrase, so a metro the shipped file
    does not list is searched exactly as the shipped ones are."""
    connector = SecEdgarConnector(connector_config("sec_edgar"), denver)

    assert [entry.city for entry in connector.regions.cities] == ["Denver", "Boulder"]
    queries = [connector.search_query(entry.city) for entry in connector.regions.cities]
    assert queries[0].startswith('"Denver"')
    assert queries[1].startswith('"Boulder"')


def test_hn_hiring_finds_a_configured_city_of_another_region_in_a_comment(
    denver: RegionsConfig,
) -> None:
    """SPEC §4 Tier 1 #4: a comment is kept when it names a configured city, and the city it is
    filed under carries its own state and metro. The comment names a city of the fixture region
    and nothing else, so only the config can have matched it."""
    found = find_city("Backend role, hybrid out of Boulder, CO — apply within.", denver)
    assert found is not None
    assert (found.city, found.state, found.metro) == ("Boulder", "CO", "Denver")

    connector = HnHiringConnector(connector_config("hn_hiring"), denver)
    record = next(
        iter(
            connector.to_records(
                HnComment(
                    story_id=1,
                    story_title="Ask HN: Who is hiring? (September 2026)",
                    item={
                        "id": 2,
                        "type": "comment",
                        "time": 1788286616,
                        "text": "Acme | Backend Engineer | Boulder, CO | Full-time",
                    },
                )
            )
        )
    )
    location = record.locations[0]
    assert (location.city, location.state, location.metro) == ("Boulder", "CO", "Denver")

    # A comment naming no configured city is counted out, not filed under a default region.
    assert (
        list(
            connector.to_records(
                HnComment(
                    story_id=1,
                    story_title="Ask HN: Who is hiring? (September 2026)",
                    item={
                        "id": 3,
                        "type": "comment",
                        "time": 1788286616,
                        "text": "Acme | Backend Engineer | Ithaca, NY | Full-time",
                    },
                )
            )
        )
        == []
    )
    assert connector.skipped == {"outside_region": 1}


def test_seed_resolves_an_entry_in_the_configured_region(denver: RegionsConfig) -> None:
    """SPEC §10: a seed entry names a city and the config supplies the state, the metro and the
    canonical spelling. With a region file the shipped config does not contain, an entry in it
    still loads — which is the whole promise of adding a metro by editing YAML."""
    connector = SeedConnector(
        connector_config("seed"),
        denver,
        seed_file=SeedFile.model_validate(
            {"companies": {"climate": [{"name": "Acme", "domain": "acme.com", "city": "boulder"}]}}
        ),
    )
    entry = connector.seed.entries[0]

    record = next(iter(connector.to_records(SeedResult(entry, None, None, "ok"))))
    location = record.locations[0]
    assert (location.city, location.state, location.country, location.metro) == (
        "Boulder",
        "CO",
        "US",
        "Denver",
    )
    assert location.is_hq is True
    assert connector.skipped == {}


# ======================================================== the loader's validation and API


def test_two_regions_may_not_claim_the_same_metro(tmp_path: Path) -> None:
    """The metro string is a region's whole identity downstream — the UI facet, SPEC §8's
    resolution scope, what ``sync-regions`` reconciles against — so two blocks under one label
    are two regions nothing can tell apart. Case included: two spellings differing only in case
    would otherwise ship as two facet values for one place."""
    with pytest.raises(ValueError, match="metro 'Denver' is configured twice"):
        write_regions(tmp_path, "same-label", [DENVER, {**DENVER, "cities": ["Aurora"]}])

    with pytest.raises(ValueError, match="also spelled"):
        write_regions(
            tmp_path,
            "same-label-other-case",
            [DENVER, {**DENVER, "metro": "denver", "cities": ["Aurora"]}],
        )


def test_one_city_may_not_be_configured_in_two_metros_even_a_disabled_one(
    tmp_path: Path,
) -> None:
    """``uq_locations_city_state`` gives a city exactly one row and one metro, so a file that
    maps a city into two describes a database that cannot exist. A *disabled* second region is
    still a mistake in the file: the check must not start failing on the day someone flips
    ``enabled: true``, when the error was made here."""
    with pytest.raises(ValueError, match="configured in two regions"):
        write_regions(
            tmp_path,
            "two-metros",
            [
                DENVER,
                {"metro": "Front Range", "state": "CO", "enabled": False, "cities": ["Boulder"]},
            ],
        )

    # "Anywhere in the file" includes twice inside one region; the comparison normalises case
    # and whitespace, exactly as ``lookup`` does.
    with pytest.raises(ValueError, match="configured in two regions"):
        write_regions(
            tmp_path,
            "twice-in-one",
            [{"metro": "Denver", "state": "CO", "cities": ["Boulder", "  boulder "]}],
        )


def test_a_file_with_no_enabled_region_is_rejected(tmp_path: Path) -> None:
    """Every city list would be empty, so a full ``refresh`` would fetch nothing and report
    success — the one outcome an operator cannot tell apart from working."""
    with pytest.raises(ValueError, match="at least one region must be enabled"):
        write_regions(tmp_path, "all-off", [{**DENVER, "enabled": False}])


def test_a_city_may_override_its_region_state(tmp_path: Path) -> None:
    """A real metro crosses state lines, and one region-level ``state`` cannot say so. The
    mapping form ``{name, state}`` overrides the state for one city; ``country`` stays
    region-level, because every configured metro is US (SPEC §1 scopes the expansion to other
    US startup hubs) and an override nothing needs is an override nobody maintains."""
    config = write_regions(
        tmp_path,
        "two-states",
        [
            {
                "metro": "Kansas City",
                "state": "MO",
                "cities": [
                    "Independence",
                    {"name": "Kansas City", "state": "KS"},
                    {"name": "Overland Park", "state": "KS"},
                ],
            }
        ],
    )

    assert [(entry.city, entry.state, entry.country) for entry in config.cities] == [
        ("Independence", "MO", "US"),
        ("Kansas City", "KS", "US"),
        ("Overland Park", "KS", "US"),
    ]
    overridden = config.lookup("overland park", "ks")
    assert overridden is not None
    assert (overridden.state, overridden.metro) == ("KS", "Kansas City")
    # The region default still applies to the entries that did not override it.
    assert config.metro_for("Independence", "MO") == "Kansas City"
    assert config.lookup("Independence", "KS") is None


def test_a_bare_city_name_still_takes_the_region_state(tmp_path: Path) -> None:
    """The Phase 1-6 spelling — a plain string in ``cities:`` — must keep loading unchanged, or
    generalising the file would have been a migration rather than an edit."""
    config = write_regions(tmp_path, "bare", [DENVER])

    assert [(entry.city, entry.state, entry.metro) for entry in config.cities] == [
        ("Denver", "CO", "Denver"),
        ("Boulder", "CO", "Denver"),
    ]


def test_a_misspelled_key_in_the_mapping_form_fails_on_load(tmp_path: Path) -> None:
    """``extra="forbid"`` on the mapping form, with the path to the field. Without it a
    misspelled ``stat:`` would be dropped in silence and that city filed under the region's
    default state — a wrong metro discovered a whole ingest run later.

    The path is asserted, not just the word: ``stat`` is a substring of ``state``, so a match on
    the word alone would be satisfied by almost any complaint about the region's own state.
    """
    with pytest.raises(ValueError, match=r"regions\.0\.cities\.0\.stat\b") as caught:
        write_regions(
            tmp_path,
            "typo",
            [{"metro": "Denver", "state": "CO", "cities": [{"name": "Boulder", "stat": "NE"}]}],
        )
    assert "Extra inputs are not permitted" in str(caught.value)


def test_an_alias_resolves_to_its_city_and_the_canonical_name_is_what_comes_back(
    tmp_path: Path,
) -> None:
    """SPEC §11: a city answers to every spelling ``config/regions.yaml`` records for it, and
    answers as itself. :meth:`RegionsConfig.lookup` and :meth:`lookup_city_name` match an alias
    exactly as they match the canonical name — same case- and whitespace-normalisation — and
    both hand back the entry whose ``city`` is the canonical name.

    Both halves are the point, and they are the two failures the key exists to prevent. Matching
    is what stops a source's own spelling from being thrown away by the geography filter, which
    is a data defect measured in whole companies. Returning the canonical entry is what stops
    that spelling from becoming a *second* place: the caller stores ``entry.city`` rather than
    the string it was handed, so ``uq_locations_city_state`` still holds one row per city
    (SPEC §5) and this file decides what that row is called.
    """
    config = write_regions(
        tmp_path,
        "aliases",
        [
            {
                "metro": "Denver",
                "state": "CO",
                "cities": [{"name": "Denver", "aliases": ["Mile High City", "DEN"]}, "Boulder"],
            }
        ],
    )

    found = config.lookup("mile high city", "co")
    assert found is not None
    assert (found.city, found.state, found.country, found.metro) == ("Denver", "CO", "US", "Denver")
    assert found.names == ("Denver", "Mile High City", "DEN")
    # Normalised the way a canonical name is: case, and runs of whitespace.
    assert config.lookup("  MILE   HIGH  City ", "CO") == found
    assert config.metro_for("den", "co") == "Denver"

    # A bare name with no state resolves through the aliases too, and to the same entry.
    assert config.lookup_city_name("DEN") == found

    # An alias is a spelling, never a place: it adds no city to the list connectors iterate,
    # and no state gains a city it does not configure by way of that city's other spellings.
    assert [entry.city for entry in config.cities] == ["Denver", "Boulder"]
    assert config.lookup("Mile High City", "NE") is None


def test_an_alias_may_not_be_another_citys_name_or_another_alias_in_the_same_state(
    tmp_path: Path,
) -> None:
    """An alias is a name a lookup answers to, so it is held to the rule a city's own name is
    held to: within one state, one spelling names one city, anywhere in the file.

    A spelling two cities answer to has no honest resolution — one of them would win by file
    order, which is the arbitrary tie-break :meth:`lookup_city_name` exists to refuse — so it is
    rejected on load, when a person is reading the file, rather than resolved at ingest time
    when nobody is. The message names **both** sides, since an alias collision is a statement
    about two places and a complaint about only one leaves the reader grepping for the other.

    A *disabled* region counts on either side. Its cities are still described in the file, the
    collision is still a mistake made here, and the check must not start failing on the day
    someone flips ``enabled: true`` — the same reasoning the duplicate-city rule uses.
    """
    with pytest.raises(ValueError, match="one spelling names one city") as caught:
        write_regions(
            tmp_path,
            "alias-over-name",
            [
                {
                    "metro": "Denver",
                    "state": "CO",
                    "cities": [{"name": "Denver", "aliases": ["Aurora"]}],
                },
                {"metro": "Front Range", "state": "CO", "enabled": False, "cities": ["Aurora"]},
            ],
        )
    message = str(caught.value)
    assert "alias 'Aurora' of city 'Denver' (CO) in 'Denver'" in message
    assert "city 'Aurora' (CO) in 'Front Range'" in message

    with pytest.raises(ValueError, match="one spelling names one city") as caught:
        write_regions(
            tmp_path,
            "alias-over-alias",
            [
                {
                    "metro": "Denver",
                    "state": "CO",
                    "cities": [{"name": "Denver", "aliases": ["The Front Range"]}],
                },
                {
                    "metro": "Pueblo",
                    "state": "CO",
                    "enabled": False,
                    "cities": [{"name": "Pueblo", "aliases": ["The Front Range"]}],
                },
            ],
        )
    message = str(caught.value)
    assert "alias 'The Front Range' of city 'Pueblo' (CO) in 'Pueblo'" in message
    assert "is also an alias of city 'Denver' (CO) in 'Denver'" in message

    # An alias repeating a name already known is the same collision and is caught by the same
    # rule: it can only add a second answer to one spelling, or nothing at all.
    with pytest.raises(ValueError, match="one spelling names one city"):
        write_regions(
            tmp_path,
            "alias-of-itself",
            [
                {
                    "metro": "Denver",
                    "state": "CO",
                    "cities": [{"name": "Denver", "aliases": ["denver"]}],
                }
            ],
        )

    # Scoped to a state, exactly as the duplicate-city rule is: two states may configure one
    # spelling, and a caller naming the state still gets an exact answer.
    shared = write_regions(
        tmp_path,
        "shared-alias",
        [
            {
                "metro": "Kansas City",
                "state": "MO",
                "cities": [
                    {"name": "Kansas City", "aliases": ["KC"]},
                    {"name": "Kansas City", "state": "KS", "aliases": ["KC"]},
                ],
            }
        ],
    )
    kansas = shared.lookup("KC", "KS")
    assert kansas is not None
    assert (kansas.city, kansas.state) == ("Kansas City", "KS")
    # ...and a bare one is as ambiguous as the name it stands for, so it resolves to nothing.
    assert shared.lookup_city_name("KC") is None


def test_a_city_name_two_states_configure_resolves_to_nothing(tmp_path: Path) -> None:
    """``lookup_city_name`` answers a *bare* name — a seed entry with no state, a listing
    carrying only a city — and answers ``None`` when two states configure that name.

    There is no honest tie-break: taking the first match files the company under a metro the
    source never claimed, and that metro then scopes what SPEC §8 will merge it with. The caller
    skips the record instead. A caller that has a state uses ``lookup``, which is exact.
    """
    config = write_regions(
        tmp_path,
        "ambiguous",
        [
            {
                "metro": "Kansas City",
                "state": "MO",
                "cities": ["Kansas City", {"name": "Kansas City", "state": "KS"}, "Independence"],
            }
        ],
    )

    assert config.lookup_city_name("Kansas City") is None
    unambiguous = config.lookup_city_name("independence")
    assert unambiguous is not None
    assert (unambiguous.city, unambiguous.state) == ("Independence", "MO")
    assert config.lookup_city_name("Boulder") is None  # configured nowhere: also "cannot answer"
    # Naming the state is what disambiguates, and it still does.
    kansas = config.lookup("Kansas City", "KS")
    assert kansas is not None and kansas.state == "KS"


def test_cities_longest_first_never_lets_a_shorter_name_shadow_a_longer_one(
    tmp_path: Path,
) -> None:
    """The order a text scanner must walk (``hn_hiring.find_city``): one configured city name
    can contain another as a whole-word substring — ``Lake City`` sits inside ``Salt Lake
    City`` — and taking the first match in file order would file every posting from the longer
    named city under the shorter named one, in the wrong state and the wrong metro.

    Descending length fixes that; ``sorted`` is stable, so equal-length names keep file order
    and the result never depends on the sort's internals.
    """
    config = write_regions(
        tmp_path,
        "shadowing",
        [
            {
                "metro": "Denver",
                "state": "CO",
                "cities": ["Denver", "Aurora", "Boulder", "Lake City"],
            },
            {"metro": "Salt Lake City", "state": "UT", "cities": ["Salt Lake City", "Provo"]},
        ],
    )

    assert [entry.city for entry in config.cities_longest_first] == [
        "Salt Lake City",
        "Lake City",
        "Boulder",
        "Denver",  # equal length with Aurora, and earlier in the file
        "Aurora",
        "Provo",
    ]


def test_the_needle_order_puts_a_longer_spelling_first_whether_or_not_it_is_a_city_name(
    tmp_path: Path,
) -> None:
    """``city_needles_longest_first`` is the sequence a scanner over free text walks
    (``hn_hiring.find_city``), and it orders by the length of the **spelling searched for**, not
    of the city it belongs to.

    That is what keeps the anti-shadowing rule of ``cities_longest_first`` true once a city
    answers to several names. An alias may be longer or shorter than its own city's name and
    than any other city's, so ordering by the city would let a short name shadow a longer alias
    — the very failure the ordering exists to prevent, arriving through the mechanism added to
    fix a related one. Here the longest needle in the file is an alias, and the shortest is
    another alias of the same city: one must be met before every canonical name and the other
    after all of them, which no order over the cities can express.

    ``sorted`` is stable, so equal-length needles keep file order and a city's canonical name is
    met before its own aliases — the result never depends on the sort's internals.
    """
    config = write_regions(
        tmp_path,
        "needles",
        [
            {
                "metro": "Denver",
                "state": "CO",
                "cities": [
                    {"name": "Denver", "aliases": ["Mile High City", "DEN"]},
                    "Lake City",
                    "Aurora",
                ],
            },
            {"metro": "Salt Lake City", "state": "UT", "cities": ["Salt Lake City"]},
        ],
    )

    assert [(needle, entry.city) for needle, entry in config.city_needles_longest_first] == [
        ("Mile High City", "Denver"),  # an alias, and the longest spelling in the file
        ("Salt Lake City", "Salt Lake City"),  # equal length, and later in the file
        ("Lake City", "Lake City"),  # a whole-word substring of the one above it
        ("Denver", "Denver"),
        ("Aurora", "Aurora"),  # equal length with Denver, and later in the file
        ("DEN", "Denver"),  # an alias again, and last, so it shadows nothing
    ]

    # The city sequence carries one spelling per city, so it cannot serve a text scan at all:
    # two of the needles above are missing from it, which is why the two are separate.
    assert [entry.city for entry in config.cities_longest_first] == [
        "Salt Lake City",
        "Lake City",
        "Denver",
        "Aurora",
    ]


def test_a_padded_value_is_stored_trimmed(tmp_path: Path) -> None:
    """Every configured name is trimmed on the way in, so one written with stray padding is the
    same name as the one written without it.

    It matters because these strings are compared, stored and printed as they are. An untrimmed
    metro would reach ``Location.metro`` with its padding and show up in the UI facet as a
    second region beside the tidy spelling of itself; an untrimmed city would be a needle no
    scan matches, since ``_normalize_city`` trims the text side and nothing trims this one; an
    untrimmed alias would slip past the one-spelling-one-city rule as a distinct alias.

    Only a *quoted* scalar can carry the padding this far — PyYAML strips a plain one — which is
    exactly why the validator has to: the file's own syntax hides the difference from a reader.
    """
    config = write_regions(
        tmp_path,
        "padded",
        [
            {
                "metro": "  Denver  ",
                "state": " CO ",
                "state_name": " Colorado ",
                "cities": [{"name": "  Boulder  ", "aliases": ["  The Flatirons  "]}],
            }
        ],
    )

    (region,) = config.regions
    assert (region.metro, region.state, region.state_name) == ("Denver", "CO", "Colorado")
    (city,) = config.cities
    assert (city.city, city.state, city.state_name, city.metro) == (
        "Boulder",
        "CO",
        "Colorado",
        "Denver",
    )
    assert city.aliases == ("The Flatirons",)
    # And the trimmed spelling is the one every lookup answers to.
    assert config.lookup("Boulder", "CO") == city
    assert config.lookup_city_name("The Flatirons") == city


def test_a_written_out_state_name_reaches_every_city_that_did_not_override_its_state(
    tmp_path: Path,
) -> None:
    """``state_name`` is how a region's ``state`` is spelled out, and it belongs to the state
    rather than to the region: :attr:`Region.resolved_cities` takes ``state`` and ``state_name``
    from the same side of a per-city override, never one from each.

    A city overriding its state sits in a *different* state, so the region's written-out name is
    not that city's. Inheriting it would hand ``hn_hiring._names_state`` the spelling of the
    wrong place and promote a mention that names somewhere else — which is the defect the key
    was added to fix, arriving through the key itself. A city that overrides ``state`` and gives
    no ``state_name`` therefore gets ``None``: no written-out spelling, which costs a mention a
    rank and never a record.
    """
    config = write_regions(
        tmp_path,
        "state-names",
        [
            {
                "metro": "Kansas City",
                "state": "MO",
                "state_name": "Missouri",
                "cities": [
                    "Independence",
                    {"name": "Overland Park", "state": "KS", "state_name": "Kansas"},
                    {"name": "Lenexa", "state": "KS"},
                ],
            }
        ],
    )

    assert [(entry.city, entry.state, entry.state_name) for entry in config.cities] == [
        ("Independence", "MO", "Missouri"),  # the region's pair, both halves of it
        ("Overland Park", "KS", "Kansas"),  # its own pair, overriding both
        ("Lenexa", "KS", None),  # its own state, and no name rather than the region's
    ]


def test_a_written_out_state_name_without_a_state_fails_on_load(tmp_path: Path) -> None:
    """The two keys are one fact written two ways, so a per-city ``state_name`` beside no
    per-city ``state`` cannot mean anything: the city takes the region's state, and its own name
    for that state would be silently dropped in favour of the region's.

    That is a typo rather than a choice, and it fails on load with the path to the field, when a
    person is reading the file — not at ingest time, when the only symptom would be a mention
    that quietly stopped being promoted.
    """
    with pytest.raises(ValueError, match=r"regions\.0\.cities\.0\b") as caught:
        write_regions(
            tmp_path,
            "name-without-state",
            [
                {
                    "metro": "Denver",
                    "state": "CO",
                    "cities": [{"name": "Boulder", "state_name": "Colorado"}],
                }
            ],
        )
    assert "without a state" in str(caught.value)


def test_one_enabled_region_names_itself_in_the_site_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC §12 Phase 7 C2: the application's own name is derived from the config too. A tracker
    covering exactly one metro says which one, and it reads the name off the file rather than
    carrying it in the page chrome — a hard-coded brand is the same drift in the most visible
    place there is.

    Driven against a region file the shipped one does not contain, which is what makes this a
    test of the derivation: the shipped config enables six regions, so nothing else in the suite
    ever executes this branch.
    """
    config = write_regions(tmp_path, "site-name-one", [DENVER])
    monkeypatch.setattr(web.templating, "load_regions_config", lambda: config)

    assert web.templating.site_name() == f"{DENVER['metro']} Startup Tracker"


def test_several_enabled_regions_share_no_metro_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two or more enabled metros have no honest shared label, so the name drops the geography
    rather than inventing one. Naming the first region would be arbitrary, and naming a wider
    area ("US Startup Tracker") would be a claim the config does not make — it lists the metros
    it lists, not a country."""
    config = write_regions(tmp_path, "site-name-two", [DENVER, PORTLAND])
    monkeypatch.setattr(web.templating, "load_regions_config", lambda: config)

    assert web.templating.site_name() == "Startup Tracker"


def test_the_shipped_config_names_no_metro_in_the_site_name() -> None:
    """The literal guard cannot see this one: with the shipped six regions the single-region
    branch would compose a metro name at *run time*, out of ``metros[0]``, so the name would
    never be spelled in any file the scan reads and would still put a region in every page title.

    This is the pin for that. It is one line and it is the whole reason the branch condition is
    ``== 1`` rather than anything looser.
    """
    assert web.templating.site_name() == "Startup Tracker"


def test_a_disabled_region_leaves_ingestion_but_stays_in_the_file(tmp_path: Path) -> None:
    """``enabled: false`` keeps a region's cities in the file while taking them out of
    ingestion and out of every lookup — a metro is switched off in one line.

    ``all_cities`` still sees them, and only consistency checks may read it: a disabled region's
    city reaching a connector is precisely what the flag promises will not happen. Rows already
    stored keep the metro they were ingested with (SPEC §2: the database is the historical
    record), which is why a disabled metro can still appear in the UI facet.
    """
    config = write_regions(tmp_path, "one-off", [DENVER, {**PORTLAND, "enabled": False}])

    assert [region.metro for region in config.regions] == ["Denver", "Portland"]
    assert [region.metro for region in config.enabled_regions] == ["Denver"]
    assert config.metros == ("Denver",)
    assert [entry.city for entry in config.cities] == ["Denver", "Boulder"]
    assert [entry.city for entry in config.all_cities] == [
        "Denver",
        "Boulder",
        "Portland",
        "Beaverton",
    ]
    assert config.lookup("Beaverton", "OR") is None
    assert config.lookup_city_name("Portland") is None
    assert config.metro_for("Boulder", "CO") == "Denver"
