"""``docs/SOURCES.md`` tracks the code (SPEC §4, §13).

SPEC §13's acceptance criterion is "``docs/SOURCES.md`` documents every connector's legal basis
and rate limit". Documentation drifts silently, so the drift is a test failure: every connector
in ``config/connectors.yaml`` must have a section, every section must state whether the source
is an official API and what rate limit is applied, and the rate limit it states must be the one
the config actually configures.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ingest.config import load_connectors_config
from ingest.connectors import all_connectors
from ingest.seed import SeedConnector

ROOT = Path(__file__).resolve().parent.parent
SOURCES = ROOT / "docs" / "SOURCES.md"
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


def test_the_excluded_sources_are_named() -> None:
    """SPEC §4 "Excluded — do not build these", restated in CLAUDE.md."""
    text = SOURCES.read_text(encoding="utf-8").lower()
    for excluded in ("linkedin", "crunchbase", "wellfound"):
        assert excluded in text
    assert "guessed" in text or "smtp" in text
