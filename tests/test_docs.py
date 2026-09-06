"""``docs/SOURCES.md`` tracks the code (SPEC §4, §13).

SPEC §13's acceptance criterion is "``docs/SOURCES.md`` documents every connector's legal basis
and rate limit". Documentation drifts silently, so the drift is a test failure: every connector
in ``config/connectors.yaml`` must have a section, every section must state whether the source
is an official API and what rate limit is applied, and the rate limit it states must be the one
the config actually configures.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.core import TyperGroup
from typer.main import get_command

import cli
from db import enums, queries
from ingest.base import CompanyTarget
from ingest.config import UNIMPLEMENTED_CONNECTORS, load_connectors_config
from ingest.connectors import all_connectors
from ingest.seed import SeedConnector

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
