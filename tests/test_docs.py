"""The documents track the code (SPEC §4, §13).

SPEC §13's acceptance criterion is "``docs/SOURCES.md`` documents every connector's legal basis
and rate limit". Documentation drifts silently, so the drift is a test failure: every connector
in ``config/connectors.yaml`` must have a section, every section must state whether the source
is an official API and what rate limit is applied, and the rate limit it states must be the one
the config actually configures.

The same idea has since been pointed at the two other documents that make checkable claims. The
README states the schedule, the regions and the commands; ``cli.py``'s own header enumerates a
closed roadmap of commands and counts them. Each of those is a fact stored twice, and the copy a
reader trusts is never the one that runs — so every one of them is read back out of the document
and compared with the config, the SPEC or the registry that decides it (SPEC §7.2, §11, §12
Phase 7).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from typer.core import TyperGroup
from typer.main import get_command

import cli
from db import enums, queries
from ingest.base import CompanyTarget
from ingest.config import (
    LINKEDIN_NOTE_LIMIT,
    OUTREACH_YAML,
    UNIMPLEMENTED_CONNECTORS,
    load_connectors_config,
    load_outreach_config,
    load_regions_config,
)
from ingest.connectors import all_connectors
from ingest.connectors.hn_hiring import HnHiringOptions
from ingest.seed import SeedConnector
from web.filters import shared_filters

ROOT = Path(__file__).resolve().parent.parent
SOURCES = ROOT / "docs" / "SOURCES.md"
SPEC = ROOT / "docs" / "SPEC.md"
README = ROOT / "README.md"
#: The em-dash the README's cadence column uses for an on-demand connector.
EM_DASH = "—"
#: A row of the README's scheduling table: "| `greenhouse` | `0 6 * * *` | daily 06:00 |".
_README_CADENCE_ROW = re.compile(
    r"^\|\s*`(?P<connector>[a-z_]+)`\s*\|\s*(?:`(?P<cadence>[^`]+)`|—)\s*\|"
)
#: SPEC §11's repository tree names ``cli.py``'s commands in a trailing comment:
#: "├── cli.py   # typer: migrate, seed, refresh, stats, merge-review".
_SPEC_CLI_COMMANDS = re.compile(r"cli\.py\s+#\s*typer:\s*(?P<commands>.+)$", re.MULTILINE)
#: ``cli.py``'s header opens by counting its own commands ("Seven commands.") and goes on to say
#: how many of them SPEC §11 names ("SPEC §11 names five"). Both are a number written as prose,
#: and neither is the number that decides anything — the registry and SPEC's tree are.
_CLI_HEADER_COUNT = re.compile(r"\b(?P<word>[A-Za-z]+) commands\b")
_CLI_HEADER_SPEC_COUNT = re.compile(r"SPEC §11 names (?P<word>[a-z]+)\b")
#: One command's bullet in that header: "* ``sync-regions`` — the same idea for …".
_CLI_HEADER_BULLET = re.compile(r"^\* ``(?P<name>[a-z-]+)``", re.MULTILINE)
#: Number words, indexed by the number they spell. Far enough to outlast any plausible command
#: list; a header that counted higher than this is a header worth reading by hand anyway.
NUMBER_WORDS = (
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
)
#: A section heading names one or more connectors in backticks: "## `greenhouse` — …" or
#: "## Applicant tracking systems — `greenhouse`, `lever`, `ashby`, `workable`".
_HEADING = re.compile(r"^##\s+(?P<title>.+)$", re.MULTILINE)
_BACKTICKED = re.compile(r"`([a-z_]+)`")


def sections() -> dict[str, str]:
    """Connector name → the body of the section documenting it."""
    text = SOURCES.read_text(encoding="utf-8")
    matches = list(_HEADING.finditer(text))
    found: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        body = text[match.start() : end]
        for name in _BACKTICKED.findall(match.group("title")):
            found[name] = body
    return found


def documented_connectors() -> set[str]:
    return set(sections())


def test_every_configured_connector_has_a_section() -> None:
    configured = set(load_connectors_config().connectors)
    missing = configured - documented_connectors()
    assert not missing, f"docs/SOURCES.md documents no section for: {sorted(missing)}"


def test_every_implemented_connector_has_a_section() -> None:
    implemented = set(all_connectors()) | {SeedConnector.name}
    assert implemented <= documented_connectors()


@pytest.mark.parametrize("name", sorted(all_connectors()))
def test_each_implemented_connector_states_its_legal_basis(name: str) -> None:
    """SPEC §13: "documents every connector's legal basis". Every section says in as many words
    whether the source is an official API, and cites the terms that apply."""
    body = sections()[name].lower()
    assert "official api" in body, f"{name}: no 'Official API:' statement"
    assert "robots" in body or "terms" in body, f"{name}: no terms/robots statement"


@pytest.mark.parametrize("name", sorted(all_connectors()))
def test_each_section_states_the_rate_limit_the_config_applies(name: str) -> None:
    """SPEC §13: "… and rate limit". The number in the prose must be the configured one, so a
    change to ``config/connectors.yaml`` cannot silently outdate the document."""
    body = sections()[name]
    rate = load_connectors_config().get(name).rate_limit.requests_per_second
    # A sub-1/s limit is written as an interval: "1 request per host per 2 seconds".
    needle = f"{rate:g} request" if rate >= 1 else f"per {1 / rate:g} second"
    assert needle in body, f"{name}: docs/SOURCES.md does not state '{needle}' (rate={rate})"


def test_the_unimplemented_connectors_are_marked_as_such() -> None:
    """A heading with no implementation must say so, so the document never implies more
    coverage than the code has."""
    for name in (
        set(load_connectors_config().connectors) - set(all_connectors()) - {SeedConnector.name}
    ):
        assert "not implemented" in sections()[name].lower(), name


def test_the_unimplemented_connector_list_is_the_one_the_registry_implies() -> None:
    """:data:`ingest.config.UNIMPLEMENTED_CONNECTORS` is the set ``/runs`` and ``cli.py stats``
    both mark, and it is written out rather than derived.

    It has to be: nothing under ``web/`` may import a connector module (SPEC §2, CLAUDE.md), and
    the registry is the wrong question anyway — ``seed`` is implemented yet deliberately absent
    from it. Written out means it can drift from the YAML and from the code, so the drift is a
    test failure: adding a connector block, or implementing one of these two, fails here until
    the list is updated.
    """
    derived = (
        set(load_connectors_config().connectors) - set(all_connectors()) - {SeedConnector.name}
    )
    assert set(UNIMPLEMENTED_CONNECTORS) == derived


def test_the_excluded_sources_are_named() -> None:
    """SPEC §4 "Excluded — do not build these", restated in CLAUDE.md."""
    text = SOURCES.read_text(encoding="utf-8").lower()
    for excluded in ("linkedin", "crunchbase", "wellfound"):
        assert excluded in text
    assert "guessed" in text or "smtp" in text


def registered_cli_commands() -> frozenset[str]:
    """The subcommands ``cli.py`` registers. ``tests/test_cli.py`` reads the same registry for
    its own checks; three lines of accessor are cheaper here than a shared import between two
    test modules with different subjects."""
    group = get_command(cli.app)
    assert isinstance(group, TyperGroup)
    return frozenset(group.commands)


def spelled(word: str) -> int:
    """``"Seven"`` → 7. A word that names no number fails the test that asked, naming it.

    Failing rather than returning a sentinel because there is no sensible comparison to make
    with one: a header that opens "Several commands." has stopped stating the fact this file
    exists to check, and that is the finding, not a mismatched count.
    """
    try:
        return NUMBER_WORDS.index(word.lower())
    except ValueError:
        pytest.fail(f"cli.py's header counts commands as {word!r}, which spells no number")


# ------------------------------------------------- SPEC §6 claims the code has to keep


def test_the_constructed_links_are_documented_with_the_scope_the_code_gives_them() -> None:
    """SPEC §6's two constructed links reach a company two ways and the document has to name
    both, because a reader upgrading a database is deciding whether to expect them.

    ``_ensure_constructed_contacts`` is called from ``upsert_company_record``, so a company gets
    them when it is *upserted* — not because it exists. That alone leaves a gap on any database
    carried over from an earlier phase: a company no connector re-lists (an aged-out
    ``funding_rss`` article, a Form D outside the incremental window, a seeded row — ``seed`` is
    not in ``refresh --all``) is never passed to the upsert, and a row with neither a
    ``website_url`` nor a ``domain`` — ``company_site`` works from ``CompanyTarget.home_url``,
    which falls back to ``https://{domain}/`` — is skipped by it on every run for ever.
    ``cli.py sync-contacts``
    (``sync_constructed_contacts``) is the sweep that closes it, and the document must send the
    reader to it rather than leaving them to notice the gap.
    """
    text = SOURCES.read_text(encoding="utf-8")
    assert "on **every company upsert**" in text
    assert "`python cli.py sync-contacts`" in text, "the upgrade case needs its remedy named"
    assert "Run it once after upgrading a database" in text
    assert "sync-contacts" in registered_cli_commands(), (
        "SOURCES.md names a command that must exist"
    )


def test_the_sweep_is_documented_as_writing_nothing_it_did_not_have_to() -> None:
    """``sync_constructed_contacts`` deletes rows, which is the one thing SPEC §2/§5 forbid for
    observed data. A reader deciding whether it is safe to run against their database is owed
    the two limits the code actually enforces — ``constructed`` only, and no ``FetchRun``."""
    text = SOURCES.read_text(encoding="utf-8")
    assert "writes no `fetch_runs` row" in text
    assert "touches only\n`confidence='constructed'` contacts" in text


def test_the_row_the_sweep_is_needed_for_is_named_by_the_condition_the_code_uses() -> None:
    """``company_site`` skips a company when ``CompanyTarget.home_url`` is ``None``, and that
    property falls back to ``https://{domain}/``: a row with no ``website_url`` but a domain is
    fetched every run like any other, and gains its links from the upsert that follows.

    The document justifies ``sync-contacts`` with that skip, so naming ``website_url`` alone is
    not a loose word — it points the reader at ``WHERE website_url IS NULL``, a population that
    ingest mostly does reach, and away from the rows (no website *and* no domain) that it never
    will. The assertion on ``home_url`` is here so the prose is checked against the property
    rather than against a memory of it.
    """
    with_a_domain_only = CompanyTarget(
        company_id=1,
        name="Acme",
        domain="acme.com",
        website_url=None,
        status=enums.CompanyStatus.ACTIVE,
        ats_provider=None,
        ats_token=None,
        external_id=None,
        last_seen_at=None,
    )
    assert with_a_domain_only.home_url == "https://acme.com/"

    # Whitespace collapsed: the claim must survive a reflow of the paragraph it sits in.
    text = " ".join(SOURCES.read_text(encoding="utf-8").split())
    assert "a row with neither a `website_url` nor a `domain`" in text
    assert "a row with no `website_url`" not in text


def test_cli_py_reconciles_its_command_set_with_the_spec_section_it_cites() -> None:
    """``cli.py``'s header claims SPEC §11 and then enumerates a closed roadmap of commands.
    At Phase 5 that enumeration was set-equal to SPEC §11's five; ``sync-contacts`` makes it a
    strict superset, and a superset that says nothing is a deviation recorded only in
    CLAUDE.md. The Makefile is the repo's own precedent: its first line reconciles its targets
    against the same section, down to noting the one target it deliberately lacks.

    So: every command the app registers is either named by SPEC §11 or called out in the
    docstring as an addition to it, and every command SPEC §11 names that is not registered yet
    is accounted for there too — which is what keeps "Four commands so far" honest when Phase 6
    lands ``stats`` and ``merge-review``.
    """
    match = _SPEC_CLI_COMMANDS.search(SPEC.read_text(encoding="utf-8"))
    assert match is not None, "SPEC §11's tree no longer lists cli.py's commands"
    specified = {name.strip() for name in match.group("commands").split(",")}
    registered = registered_cli_commands()
    doc = " ".join((cli.__doc__ or "").split())

    for extra in sorted(registered - specified):
        assert "SPEC §11 names" in doc and extra in doc, (
            f"cli.py registers {extra!r}, which SPEC §11 does not name, without saying so in "
            "its own header the way the Makefile does"
        )
    for deferred in sorted(specified - registered):
        assert deferred in doc, f"SPEC §11 names {deferred!r}; cli.py must say when it arrives"


def test_cli_pys_header_counts_the_commands_it_actually_registers() -> None:
    """The header opens "Seven commands." and then documents each of them, so both the number
    and the list are claims about the registry sitting a thousand lines below them.

    The number is the half that rots silently. Adding a command and its bullet leaves a header
    that reads correctly line by line and is wrong in its first sentence — which is exactly the
    state Phase 6's "Six commands" was in the moment ``sync-regions`` was registered, and the
    kind of small untruth that teaches a reader to stop trusting the header at all. The second
    number in the same sentence is checked against SPEC §11's own tree rather than against a
    memory of it, so implementing a sixth specified command moves it here too.

    The bullet list is compared as a *set*: a registered command with no bullet is undocumented,
    and a bullet for a command nobody registers is a promise the CLI does not keep.
    """
    doc = cli.__doc__ or ""
    registered = registered_cli_commands()

    counted = _CLI_HEADER_COUNT.search(doc)
    assert counted is not None, "cli.py's header no longer opens by counting its commands"
    assert spelled(counted["word"]) == len(registered), (
        f"cli.py's header says {counted['word']!r} commands; it registers "
        f"{len(registered)}: {sorted(registered)}"
    )

    named = _CLI_HEADER_SPEC_COUNT.search(doc)
    assert named is not None, "cli.py's header no longer says how many commands SPEC §11 names"
    match = _SPEC_CLI_COMMANDS.search(SPEC.read_text(encoding="utf-8"))
    assert match is not None, "SPEC §11's tree no longer lists cli.py's commands"
    specified = {name.strip() for name in match.group("commands").split(",")}
    assert spelled(named["word"]) == len(specified), (
        f"cli.py's header says SPEC §11 names {named['word']!r} commands; §11's tree names "
        f"{len(specified)}: {sorted(specified)}"
    )

    assert set(_CLI_HEADER_BULLET.findall(doc)) == set(registered)


# ------------------------------------------- README claims about the schedule (SPEC §7.2)


def readme_cadences() -> dict[str, str | None]:
    """The README's scheduling table as ``connector -> cadence``; ``None`` for an em-dash.

    The table is written by hand because it carries a plain-English "when it fires" column a
    generator could not produce, so this parses it back and the test below compares it with
    ``config/connectors.yaml``. Without that, the one document a new operator reads to learn
    when things run could quietly disagree with the file that decides.
    """
    found: dict[str, str | None] = {}
    for line in README.read_text(encoding="utf-8").splitlines():
        match = _README_CADENCE_ROW.match(line)
        if match is not None:
            cadence = match.group("cadence")
            found[match.group("connector")] = cadence if cadence != EM_DASH else None
    return found


def test_the_readme_cadence_table_is_the_configured_one() -> None:
    """SPEC §7.2 and CLAUDE.md: ``config/connectors.yaml`` is the only place a schedule is
    defined, so every other statement of the schedule has to be checked against it."""
    assert readme_cadences() == {
        name: config.cadence for name, config in load_connectors_config().connectors.items()
    }


def test_the_readme_writes_every_weekday_cadence_as_a_name() -> None:
    """APScheduler 3.x numbers weekdays Monday=0 while crontab numbers them Sunday=0, so a
    numeric weekday in ``config/connectors.yaml`` fires a day away from what SPEC §7.2's table
    says — ``0 3 * * 6`` means Sunday to the parser and Saturday to everyone else.

    Day names mean the same thing under either convention. This asserts the property on the
    config (the thing that runs) and on the README (the thing that is read), so neither can
    drift back to a number without the reason being re-litigated.
    """
    for name, config in load_connectors_config().connectors.items():
        if config.cadence is None:
            continue
        weekday = config.cadence.split()[4]
        assert weekday == "*" or not weekday.isdigit(), (
            f"{name}: cadence {config.cadence!r} uses a numeric weekday; write it as a name "
            "(mon..sun) — APScheduler numbers Monday=0, crontab numbers Sunday=0"
        )


def test_the_readme_documents_both_runs_signals_and_their_asymmetry() -> None:
    """``last_successful_runs`` counts ``ok`` alone while a ``partial`` run breaks a failure
    streak, so ``/runs`` can honestly show "last ok 9d ago" beside "0 consecutive failures".

    A reader who meets that pair with no explanation concludes the page is broken, so the
    README has to name both signals and say why they disagree (SPEC §7.2, §9).
    """
    text = " ".join(README.read_text(encoding="utf-8").split())
    assert "**Stale**" in text and "**Failing**" in text
    assert "twice the connector's cadence" in text
    assert "3 or more *finished* runs all ended `error`" in text
    assert "partial" in text and "limping" in text


def permanently_stale_connectors() -> set[str]:
    """Connectors ``/runs`` marks **Stale** for ever, however healthy the install.

    ``web.routes.runs.connector_health`` derives staleness from the cadence alone, and
    ``last_successful_runs`` counts ``ok`` runs. A connector configured with a cadence but
    absent from ``all_connectors()`` is scheduled by nothing (``scheduler.build_scheduler``
    skips it) and refused by ``cli.py refresh`` ("unknown connector"), so no code path can ever
    write it an ``ok`` row and the Stale mark can never clear.
    """
    implemented = set(all_connectors()) | {SeedConnector.name}
    return {
        name
        for name, config in load_connectors_config().connectors.items()
        if config.cadence is not None and name not in implemented
    }


def test_the_readme_first_run_check_is_one_a_healthy_install_can_pass() -> None:
    """The setup walkthrough's last paragraph is the acceptance test a first-time operator uses
    to decide whether ``make up && make migrate && make seed && make refresh`` worked, and it
    said "no row marked **Stale**" while ``product_hunt`` — cadenced by SPEC §7.2, implemented
    by nobody — is Stale on every install there has ever been.

    An unpassable check trains the reader to ignore the one signal SPEC §9 exists to make loud,
    so the claim has to name its exceptions. Asserted against the config rather than against a
    memory of it: implement ``product_hunt`` and this test goes quiet on its own.
    """
    collapsed = " ".join(README.read_text(encoding="utf-8").split())
    stale = permanently_stale_connectors()
    if not stale:
        pytest.skip("every cadenced connector is implemented; the plain claim is true again")

    assert "no row marked **Stale**" not in collapsed, (
        f"the first-run check promises no Stale row, but {sorted(stale)} always is"
    )
    marker = "permanently **Stale**"
    index = collapsed.find(marker)
    assert index != -1, f"the README has to say {sorted(stale)} is {marker}"
    # The sentence leading up to the marker, generously bounded: it must name the connectors.
    lead = collapsed[max(0, index - 400) : index]
    for name in sorted(stale):
        assert f"`{name}`" in lead, f"{name} is permanently Stale and the README does not say so"


#: The first line of the README's ``merge-review`` sample block, which ``cli.format_candidate``
#: composes: "candidate #1  similarity 0.91  <reason>".
_README_CANDIDATE_HEADER = re.compile(
    r"^candidate #(?P<id>\d+)  similarity (?P<similarity>\d\.\d\d)  (?P<reason>.+)$", re.MULTILINE
)
#: The ``merge_candidates.reason`` text ``ingest.normalize.record_merge_candidates`` writes for a
#: trigram pair (SPEC §8 step 3). ``ingest.pipeline``'s domain-conflict row is the only other
#: reason string in the system, and it is not what this sample illustrates.
_TRIGRAM_REASON = re.compile(
    r"trigram similarity \d\.\d\d on normalized_name within metro (?:'[^']*'|None)"
)


def test_the_readme_merge_review_sample_is_output_the_code_can_produce() -> None:
    """The sample block is the only documented example of what a destructive, human-in-the-loop
    command shows a reviewer, and it printed a ``reason`` no code path writes — one implying the
    column names both companies, where the real one repeats the similarity and names the metro.

    So: parse the header out of the README, check the reason against the shape
    ``record_merge_candidates`` stores, and render the parsed values back through
    ``cli.format_candidate`` — which is where the line is actually composed — and compare.
    """
    text = README.read_text(encoding="utf-8")
    match = _README_CANDIDATE_HEADER.search(text)
    assert match is not None, "the README no longer shows a merge-review candidate"
    assert _TRIGRAM_REASON.fullmatch(match["reason"]), (
        f"no connector writes a merge_candidates.reason like {match['reason']!r}"
    )

    moment = datetime(2026, 9, 5, 6, tzinfo=UTC)

    def side(company_id: int, name: str) -> queries.CompanySummary:
        return queries.CompanySummary(
            id=company_id,
            name=name,
            domain=None,
            city=None,
            stage=enums.Stage.UNKNOWN,
            status=enums.CompanyStatus.ACTIVE,
            open_job_count=0,
            funding_round_count=0,
            contact_count=0,
            first_seen_at=moment,
            last_seen_at=moment,
            connectors=(),
        )

    row = queries.MergeCandidateRow(
        id=int(match["id"]),
        similarity=float(match["similarity"]),
        reason=match["reason"],
        a=side(2, "Acme Robotics"),
        b=side(5, "Acme Robotics Inc"),
    )
    assert cli.format_candidate(row, suggested_id=row.a.id)[0] == match[0]


def test_the_readme_names_the_two_new_commands_with_what_they_are_for() -> None:
    """Both are registered and both are documented, so the "Operations" section cannot outlive
    a rename or arrive before the command does."""
    text = README.read_text(encoding="utf-8")
    for command in ("stats", "merge-review"):
        assert command in registered_cli_commands()
        assert f"`python cli.py {command}`" in text
    # Whitespace collapsed: the claim must survive a reflow of the paragraph it sits in.
    collapsed = " ".join(text.split())
    assert "the only thing in the system that deletes a company" in collapsed


def test_the_document_does_not_claim_an_unchanged_site_rewrites_nothing() -> None:
    """``_upsert_contacts`` is ``ON CONFLICT DO UPDATE SET confidence, source_id``, so a second
    visit to a byte-identical page rewrites every published row's ``source_id`` to the newer
    run. Only the *constructed* rows, inserted ``DO NOTHING``, are genuinely untouched.

    The stable contact order is real and worth documenting, but it is not the reason: each
    contact is upserted independently on ``(company_id, kind, value)``, so order decides which
    contacts survive the per-company caps, never whether a row is written.
    """
    text = SOURCES.read_text(encoding="utf-8")
    assert "rewrites nothing" not in text
    assert "refreshes that row's `source_id`" in text


# --------------------------------- README claims about the regions (SPEC §11, §12 Phase 7)

#: A run of Title-Case names read as a list: "Bay Area, New York, Seattle and Austin". Consulted
#: only when two of its items are already configured metros, which is what stops it reading a
#: list of connectors, of cities or of Compose services as a list of regions.
_TITLE_CASE_ITEM = r"[A-Z][a-z]+(?: [A-Z][a-z]+)*"
_NAME_LIST = re.compile(rf"{_TITLE_CASE_ITEM}(?:, {_TITLE_CASE_ITEM})+(?: and {_TITLE_CASE_ITEM})?")
#: The heading of the ``stats`` sample's per-metro block, matched on a wildcard word: which word
#: joins "companies" to "metro" is pinned against the command's real output in
#: ``tests/test_cli_ops.py``, and asserting it here too would mean two tests failing over one.
_README_METRO_HEADING = re.compile(r"^companies \w+ metro\b.*$", re.MULTILINE)
#: One row of that block: "  Bay Area     2087".
_README_METRO_COUNT_ROW = re.compile(r"^ {2}(?P<metro>\S.*?) {2,}\d+$")


def readme_section_text(title: str) -> str:
    """The body of one ``##`` section of the README, exactly as written.

    The slice runs to the next second-level heading, and a third-level one is not that: a
    ``###`` subsection belongs to the section it sits under, which is what lets a check scoped to
    a section reach the tables and samples inside it. Line structure is kept because a table is
    parsed by rows; :func:`readme_section` is the collapsed view for checking prose.
    """
    text = README.read_text(encoding="utf-8")
    start = text.index(f"\n## {title}\n")
    end = text.find("\n## ", start + 1)
    return text[start : len(text) if end == -1 else end]


def readme_section(title: str) -> str:
    """The body of one ``##`` section of the README, whitespace collapsed.

    Collapsed because the README is hand-wrapped at 100 columns: a needle that happened to span
    a line break would test where the wrapping fell rather than what the sentence says, and the
    claims below have to survive a reflow of the paragraph they sit in.
    """
    return " ".join(readme_section_text(title).split())


def readme_sample_metros() -> list[str]:
    """The metros named in the ``stats`` sample's per-metro block, in the order printed."""
    text = README.read_text(encoding="utf-8")
    heading = _README_METRO_HEADING.search(text)
    if heading is None:
        return []
    found: list[str] = []
    # The heading match stops before its newline, so the first element is that line's remainder.
    for line in text[heading.end() :].splitlines()[1:]:
        row = _README_METRO_COUNT_ROW.match(line)
        if row is None:
            break
        found.append(row["metro"])
    return found


def readme_listed_metros(configured: set[str]) -> list[str]:
    """Every item of every Title-Case list in the README that already names two configured
    metros — the *whole* list, including any item that is not one.

    Two known metros is what identifies a sentence as an enumeration of regions; taking the
    whole list from there is what gives the check something to catch, since the failure this
    guards against is a name left in a list after the config stopped defining it.
    """
    found: list[str] = []
    for match in _NAME_LIST.finditer(" ".join(README.read_text(encoding="utf-8").split())):
        items = re.split(r", | and ", match.group(0))
        if len(set(items) & configured) >= 2:
            found.extend(items)
    return found


def test_every_metro_the_readme_names_is_one_the_shipped_config_defines() -> None:
    """``config/regions.yaml`` is the only place a metro is defined (SPEC §11), and the README is
    the one document that names the shipped ones on purpose — the literal guard in
    ``tests/test_regions.py`` exempts prose precisely so it can.

    Exempt is not unchecked. A metro dropped from the config leaves the README promising a region
    the app has never heard of, in the two places a reader takes for current: the enumerations
    ("six metros ship — …") and the ``stats`` sample, which is a screenshot of a database that
    was configured differently. Both are read back and both must name only configured metros —
    the same reconciliation the cadence table gets against ``config/connectors.yaml``.

    The reverse direction is deliberately not asserted. Prose may name a subset — "Bay Area and
    Austin, say" is a legitimate sentence — and a sample of a real database is under no
    obligation to have ingested every region.
    """
    configured = {region.metro for region in load_regions_config().regions}
    sample = readme_sample_metros()
    listed = readme_listed_metros(configured)

    assert sample, "the README's `stats` sample no longer shows its per-metro block"
    assert listed, "the README no longer enumerates the shipped metros anywhere in its prose"
    assert set(sample) <= configured, (
        f"the README's stats sample shows {sorted(set(sample) - configured)}, which "
        "config/regions.yaml does not configure"
    )
    assert set(listed) <= configured, (
        f"the README's prose names {sorted(set(listed) - configured)}, which "
        "config/regions.yaml does not configure"
    )


def test_the_readme_regions_section_documents_the_flag_and_the_order_of_operations() -> None:
    """The two things about SPEC §11's expansion hook that a reader cannot get from the file
    format, and both of them decide what happens to data.

    ``enabled: false`` is the one-line switch, and what it conspicuously does *not* do is delete
    anything: rows keep the metro they were ingested with because the database is the historical
    record (SPEC §2), which is why a disabled metro can still appear in the Region filter. A
    reader who expected the flag to tidy up after itself files that as a bug.

    The order of operations is the other. ``sync-regions`` relabels what is stored and ``refresh``
    ingests what is new; documented the other way round, the sweep runs before the rows it would
    have relabelled exist, and the operator is left with the exact symptom the command was
    written to cure. The dry run comes first because a sweep over every stored city is the kind
    of thing you ask before you tell.
    """
    section = readme_section("Regions")

    assert "`enabled: false`" in section, "the one-line switch of SPEC §11 is undocumented"
    assert "out of ingestion" in section, "what `enabled: false` stops has to be said"
    assert "historical record" in section, (
        "`enabled: false` must be documented as changing nothing already stored (SPEC §2)"
    )

    assert "python cli.py sync-regions --dry-run" in section, "a sweep is reviewed before it runs"
    assert "in this order" in section, "the two steps are an order, not a menu"
    assert section.index("cli.py sync-regions") < section.index("make refresh"), (
        "sync-regions relabels what is stored and refresh ingests what is new; in the other "
        "order the sweep runs before the rows it would relabel exist"
    )

    # The claim the command's own docstring makes, in the document read before the docstring is.
    assert "no `fetch_runs` row is written" in section
    assert "sync-regions" in registered_cli_commands(), "the README names a command that must exist"


# ------------------------------ README claims about the outreach pitch (SPEC §6, §12 Phase 8)

#: One row of the README's ``config/outreach.yaml`` key table: "| `email.subject` | subject … |".
#: Dotted through one level of nesting, because that is how the file is written and therefore how
#: it is edited — the model that loads it is flat (``email_subject``) and nobody edits the model.
_README_OUTREACH_KEY_ROW = re.compile(r"^\|\s*`(?P<key>[a-z_]+(?:\.[a-z_]+)?)`\s*\|", re.MULTILINE)
#: The README's statement of LinkedIn's own limit on a connection-request note.
_README_NOTE_CAP = re.compile(r"caps a connection-request note at (?P<cap>\d+) characters")
#: ...and of how many "Who is hiring?" threads one ``hn_hiring`` run reads: "ships at **1** thread".
_README_HN_THREADS = re.compile(r"ships at \*\*(?P<threads>\d+)\*\* thread")


def outreach_keys() -> set[str]:
    """The keys ``config/outreach.yaml`` actually holds, dotted through one level of nesting.

    Read out of the YAML rather than off :class:`ingest.config.OutreachConfig`, whose fields are
    deliberately a different shape: the model is flat and the file nests, and the document
    describes the file, since that is the thing a reader opens and rewrites.
    """
    document = yaml.safe_load(OUTREACH_YAML.read_text(encoding="utf-8"))
    assert isinstance(document, dict), "config/outreach.yaml is no longer a mapping"
    found: set[str] = set()
    for key, value in document.items():
        if isinstance(value, dict):
            found.update(f"{key}.{nested}" for nested in value)
        else:
            found.add(key)
    return found


def readme_outreach_keys() -> set[str]:
    """The keys the README's Outreach section says that file has."""
    section = readme_section_text("Outreach")
    return {match["key"] for match in _README_OUTREACH_KEY_ROW.finditer(section)}


def test_the_readme_documents_every_outreach_key_and_only_keys_the_file_has() -> None:
    """SPEC §12 Phase 8 puts the pitch in ``config/outreach.yaml`` precisely so it can be
    rewritten without touching Python, which makes the README's key table the instructions for
    doing it — and instructions naming a key that is not there are worse than none: the model
    forbids unknown keys, so a reader who follows them gets a file the loader refuses and an app
    that will not start, with nothing but a field path to say which line to take back out.

    Checked in both directions, because both directions fail a reader. A documented key the file
    does not define is that broken edit; a defined key the table omits is a knob nobody finds —
    and the two guard lists are exactly the kind of thing nobody would think to look for.
    """
    documented = readme_outreach_keys()
    defined = outreach_keys()

    assert documented, "the README no longer lists config/outreach.yaml's keys"
    assert documented == defined, (
        f"the README documents {sorted(documented - defined)} which the file does not define, "
        f"and omits {sorted(defined - documented)} which it does"
    )
    # The file the table describes is one the loader accepts, so "these are the keys" and "this
    # is a file that starts the app" are the same claim rather than two.
    load_outreach_config()


def test_the_readme_states_the_note_cap_the_loader_enforces() -> None:
    """The 300-character cap is LinkedIn's, not this project's, and the README is where somebody
    about to rewrite ``linkedin.note`` learns the budget they are writing inside.

    A number stated in prose and enforced in code is the same fact stored twice, and the copy the
    reader trusts is never the one that runs (the premise of this whole module). It matters more
    here than for most: a template that overshoots does not render badly, it fails on load, so a
    README quoting a laxer number would send the reader to a file that stops the app from
    starting — and one quoting a stricter number would have them cutting a sentence they could
    have kept.

    The shipped note is measured too, filled in the way the panel fills it. That is what stops
    the pair of claims being true of nothing: a cap the README states correctly and the shipped
    pitch already breaks would mean the file could not be loaded at all.
    """
    # Whitespace collapsed: the claim must survive a reflow of the paragraph it sits in.
    match = _README_NOTE_CAP.search(readme_section("Outreach"))
    assert match is not None, "the README no longer states LinkedIn's cap on a connection note"
    assert int(match["cap"]) == LINKEDIN_NOTE_LIMIT, (
        f"the README says LinkedIn caps a note at {match['cap']} characters; "
        f"ingest.config enforces {LINKEDIN_NOTE_LIMIT}"
    )

    drafted = load_outreach_config().linkedin_note.format(company="Acme", person="Ada Lovelace")
    assert len(drafted) <= LINKEDIN_NOTE_LIMIT


def test_the_new_filter_is_documented_under_the_name_that_travels_on_the_wire() -> None:
    """SPEC §12 Phase 8's filter is spelled two ways on purpose — ``has_email`` in a URL, what the
    box is *for*, and ``has_published_email`` on :class:`db.queries.Filters`, what has to be true
    for it — and the README's parameter table is the one place a reader gets the first from.

    So the row is checked against the parser rather than against a memory of it: the name the
    table gives is fed to :func:`web.filters.shared_filters` and has to come back as the flag it
    promises. A table documenting the *field* name would read plausibly and produce a URL that
    filters nothing, which is the failure this catches — an ignored unknown parameter is silent
    (SPEC §9's filters are all opt-in), so nothing else would ever say so.
    """
    section = readme_section("Web interface")
    assert "`has_email=1`" in section, "SPEC §12 Phase 8's filter is missing from the table"

    assert shared_filters(has_email="1").has_published_email is True, (
        "the README tells a reader to write `has_email=1`, and shared_filters ignores it"
    )
    assert shared_filters().has_published_email is False, "it has to be off unless it is asked for"


def test_the_readme_states_the_thread_cap_hn_hiring_actually_runs_with() -> None:
    """The backfill knob decides how much of the one source that yields *people's own* addresses
    this app has ever read — 22 of the 25 addresses it found in one thread name a person, against
    1 of the 9 crawled off company sites — so the README documents raising it, what it costs and
    what to expect back.

    Every number in that paragraph is really a claim about ``config/connectors.yaml``, which is
    the only place a connector's cadence, rate limit or options are defined (SPEC §7.2,
    CLAUDE.md), so all three are read back out of the prose and compared with it, exactly as the
    cadence table above is. The estimate the paragraph gives — threads times comments, at the
    configured rate — is only as good as its inputs, and an operator budgeting half an hour off a
    stale one is the reason to check them rather than trust them.
    """
    section = readme_section("Outreach")
    config = load_connectors_config().get("hn_hiring")
    options = HnHiringOptions.model_validate(config.options)

    match = _README_HN_THREADS.search(section)
    assert match is not None, "the README no longer says how many threads one hn_hiring run reads"
    assert int(match["threads"]) == options.max_threads, (
        f"the README says hn_hiring ships reading {match['threads']} thread(s); "
        f"config/connectors.yaml configures {options.max_threads}"
    )

    assert f"{options.max_comments_per_thread} comments per thread" in section
    rate = config.rate_limit.requests_per_second
    assert f"{rate:g} requests/second" in section, (
        f"the README's run-time estimate is built on a rate the config does not set ({rate})"
    )
