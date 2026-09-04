"""``ingest.classify`` against ``config/classifiers.yaml`` (SPEC §7.1).

The rule this module exists to defend: **classification never excludes.** An unmatched title is
``role_family='other'`` and ``seniority='unknown'`` and is still a valid, storable record;
``tests/test_phase3_pipeline.py`` proves the same title survives all the way into the database
and the default views (SPEC §13).
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from db import enums
from ingest.classify import (
    CLASSIFIERS_YAML,
    Classification,
    classify,
    classify_round_type,
    is_funding_announcement,
    load_classifiers,
)

# ------------------------------------------------------------------ the file itself


def test_every_role_family_except_other_has_a_rule() -> None:
    """SPEC §7.1 lists 19 families; 18 of them are keyword-driven and ``other`` is the
    fallback, which by definition has no keywords."""
    rules = load_classifiers().role_family
    covered = {rule.value for rule in rules}
    expected = {member.value for member in enums.RoleFamily} - {enums.RoleFamily.OTHER.value}
    assert covered == expected


def test_every_seniority_except_unknown_has_a_rule() -> None:
    covered = {rule.value for rule in load_classifiers().seniority}
    expected = {member.value for member in enums.Seniority} - {enums.Seniority.UNKNOWN.value}
    assert covered == expected


def test_every_employment_type_except_unknown_is_reachable() -> None:
    """Both ways in: the ATS field (``source_values``) and the keyword rules."""
    classifiers = load_classifiers()
    expected = {member for member in enums.EmploymentType if member is not member.UNKNOWN}
    assert set(classifiers.employment_source_values.values()) == expected
    assert {enums.EmploymentType(rule.value) for rule in classifiers.employment_type} == expected


def test_the_shipped_file_parses_and_is_cached() -> None:
    assert load_classifiers() is load_classifiers(CLASSIFIERS_YAML)


def test_a_typo_in_the_yaml_fails_loudly(tmp_path: Path) -> None:
    """``extra="forbid"`` plus the enum types mean a misspelt family is a load-time error with
    a field path, not a rule that silently never fires."""
    broken = yaml.safe_load(CLASSIFIERS_YAML.read_text(encoding="utf-8"))
    broken["role_family"][0]["family"] = "machine_learning"  # the member is ``ml_ai``
    path = tmp_path / "classifiers.yaml"
    path.write_text(yaml.safe_dump(broken), encoding="utf-8")
    with pytest.raises(ValidationError, match="role_family"):
        load_classifiers(path)


# ------------------------------------------------------------------ role_family


@pytest.mark.parametrize(
    ("title", "family"),
    [
        # One title per family, so all 19 members of SPEC §7.1 are exercised.
        ("Senior Software Engineer, Backend", enums.RoleFamily.SOFTWARE),
        ("Site Reliability Engineer", enums.RoleFamily.INFRASTRUCTURE),
        ("Machine Learning Engineer, LLM Inference", enums.RoleFamily.ML_AI),
        ("Data Engineer", enums.RoleFamily.DATA),
        ("Principal Hardware Engineer: High Speed Mixed Signal", enums.RoleFamily.HARDWARE),
        ("Robotics Perception Engineer", enums.RoleFamily.ROBOTICS),
        ("Research Scientist", enums.RoleFamily.RESEARCH),
        ("Product Manager, Growth", enums.RoleFamily.PRODUCT),
        ("Senior Product Designer", enums.RoleFamily.DESIGN),
        ("Application Security Engineer", enums.RoleFamily.SECURITY),
        ("QA Automation Engineer", enums.RoleFamily.QA),
        ("Enterprise Account Executive", enums.RoleFamily.SALES),
        ("Developer Relations Engineer", enums.RoleFamily.MARKETING),
        ("Chief of Staff", enums.RoleFamily.BIZOPS),
        ("Accounts Receivable Lead", enums.RoleFamily.FINANCE),
        ("Technical Recruiter", enums.RoleFamily.PEOPLE),
        ("Customer Success Manager", enums.RoleFamily.OPERATIONS),
        ("General Counsel", enums.RoleFamily.LEGAL),
        ("Underwater Basket Weaver", enums.RoleFamily.OTHER),
    ],
)
def test_role_family_covers_all_nineteen(title: str, family: enums.RoleFamily) -> None:
    assert classify(title).role_family is family


def test_all_nineteen_families_are_reachable() -> None:
    """Belt and braces for SPEC §13 ("All 19 role families are represented"): every member is
    the answer to at least one of the titles above."""
    titles = [
        "Senior Software Engineer, Backend",
        "Site Reliability Engineer",
        "Machine Learning Engineer, LLM Inference",
        "Data Engineer",
        "Principal Hardware Engineer",
        "Robotics Perception Engineer",
        "Research Scientist",
        "Product Manager",
        "Senior Product Designer",
        "Application Security Engineer",
        "QA Automation Engineer",
        "Enterprise Account Executive",
        "Developer Relations Engineer",
        "Chief of Staff",
        "Accounts Receivable Lead",
        "Technical Recruiter",
        "Customer Success Manager",
        "General Counsel",
        "Underwater Basket Weaver",
    ]
    assert {classify(title).role_family for title in titles} == set(enums.RoleFamily)


@pytest.mark.parametrize(
    ("title", "family"),
    [
        # File order is precedence: the specific rule above wins over the generic one below.
        ("Software Engineer, Machine Learning", enums.RoleFamily.ML_AI),
        ("Security Engineer", enums.RoleFamily.SECURITY),
        ("Platform Engineer", enums.RoleFamily.INFRASTRUCTURE),
        # SPEC §7.1 puts "test engineer" under hardware and a bare "test" under QA.
        ("Test Engineer", enums.RoleFamily.HARDWARE),
        ("Software Test Lead", enums.RoleFamily.QA),
        # A business function whose title ends in "engineer" is claimed before software.
        ("Solutions Engineer", enums.RoleFamily.SALES),
        ("Customer Support Engineer", enums.RoleFamily.OPERATIONS),
        # "product analyst" and "financial analyst" beat data's generic "analyst".
        ("Product Analyst", enums.RoleFamily.PRODUCT),
        ("Financial Analyst", enums.RoleFamily.FINANCE),
        ("Marketing Analyst", enums.RoleFamily.DATA),
    ],
)
def test_precedence_between_overlapping_keywords(title: str, family: enums.RoleFamily) -> None:
    assert classify(title).role_family is family


def test_role_family_reads_the_title_not_the_description() -> None:
    """A description names every function in the company; only the title decides."""
    description = "You will work with our sales, marketing, legal and recruiting teams."
    assert classify("Backend Engineer", description=description).role_family is (
        enums.RoleFamily.SOFTWARE
    )


# ------------------------------------------------------------------ seniority


@pytest.mark.parametrize(
    ("title", "level"),
    [
        ("Software Engineering Intern", enums.Seniority.INTERN),
        ("New Grad Software Engineer", enums.Seniority.NEW_GRAD),
        ("Junior Data Analyst", enums.Seniority.JUNIOR),
        ("Mid-Level Backend Engineer", enums.Seniority.MID),
        ("Senior Backend Engineer", enums.Seniority.SENIOR),
        ("Staff Software Engineer", enums.Seniority.STAFF),
        ("Principal Designer", enums.Seniority.PRINCIPAL),
        ("Tech Lead, Payments", enums.Seniority.LEAD),
        ("Engineering Manager", enums.Seniority.MANAGER),
        ("Director of Engineering", enums.Seniority.DIRECTOR),
        ("VP of Sales", enums.Seniority.EXECUTIVE),
        ("Software Engineer", enums.Seniority.UNKNOWN),
    ],
)
def test_seniority_covers_every_member(title: str, level: enums.Seniority) -> None:
    assert classify(title).seniority is level


def test_seniority_prefers_the_more_senior_label() -> None:
    assert classify("Senior Engineering Manager").seniority is enums.Seniority.MANAGER
    assert classify("Senior Staff Engineer").seniority is enums.Seniority.STAFF


# ------------------------------------------------------------------ employment_type


@pytest.mark.parametrize(
    ("hint", "expected"),
    [
        ("FullTime", enums.EmploymentType.FULL_TIME),  # Ashby
        ("Full-time", enums.EmploymentType.FULL_TIME),  # Lever, Workable
        ("FULL_TIME", enums.EmploymentType.FULL_TIME),
        ("PartTime", enums.EmploymentType.PART_TIME),
        ("Contract", enums.EmploymentType.CONTRACT),
        ("Intern", enums.EmploymentType.INTERNSHIP),
        ("Temporary", enums.EmploymentType.TEMPORARY),
        ("Co-op", enums.EmploymentType.CO_OP),
        ("Something Else", enums.EmploymentType.UNKNOWN),
    ],
)
def test_the_ats_field_is_used_where_present(hint: str, expected: enums.EmploymentType) -> None:
    """SPEC §7.1: "Derived from the ATS field where present"; separators and case are ignored."""
    assert classify("Engineer", employment_type_hint=hint).employment_type is expected


def test_the_ats_field_beats_the_title_keywords() -> None:
    result = classify("Contract Technical Writer", employment_type_hint="Full-time")
    assert result.employment_type is enums.EmploymentType.FULL_TIME
    assert result.matched["employment_type"] == "ats:Full-time"


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Part-Time Barista", enums.EmploymentType.PART_TIME),
        ("Data Labeling Intern", enums.EmploymentType.INTERNSHIP),
        ("Software Engineering Co-op", enums.EmploymentType.CO_OP),
        ("Contract Technical Writer", enums.EmploymentType.CONTRACT),
        ("Seasonal Warehouse Associate", enums.EmploymentType.TEMPORARY),
        ("Full-Time Recruiter", enums.EmploymentType.FULL_TIME),
        ("Backend Engineer", enums.EmploymentType.UNKNOWN),
    ],
)
def test_employment_type_keywords(title: str, expected: enums.EmploymentType) -> None:
    assert classify(title).employment_type is expected


def test_the_title_beats_the_description() -> None:
    """Almost every description says "full-time" somewhere; a part-time title must survive it."""
    description = "We offer full-time benefits to everyone on the team."
    assert classify("Part-Time Tutor", description=description).employment_type is (
        enums.EmploymentType.PART_TIME
    )


def test_the_description_is_the_fallback() -> None:
    result = classify("Research Assistant", description="This is a part-time role, 20 hrs/week.")
    assert result.employment_type is enums.EmploymentType.PART_TIME
    assert result.matched["employment_type"] == "part-time"


def test_the_description_can_be_kept_out_of_the_employment_type() -> None:
    """``hn_hiring`` needs this: one comment advertises several roles, and an "Intern" further
    down must not make the whole comment an internship."""
    description = "- Data Labeling Intern: https://example.com/1\n- Head of Product: …"
    assert classify("Head of Product", description=description).employment_type is (
        enums.EmploymentType.INTERNSHIP
    )
    assert (
        classify(
            "Head of Product", description=description, employment_type_from_description=False
        ).employment_type
        is enums.EmploymentType.UNKNOWN
    )


# ------------------------------------------------------------------ flexible_signal


@pytest.mark.parametrize(
    "text",
    [
        "Students welcome!",
        "Hours are flexible around your class schedule.",
        "This is a remote-friendly role.",
        "About 20 hrs/week during the school year.",
        "Undergraduate applicants encouraged.",
    ],
)
def test_flexible_signal_fires_on_student_friendly_language(text: str) -> None:
    assert classify("Research Assistant", description=text).flexible_signal is True


def test_flexible_signal_is_off_by_default() -> None:
    result = classify("Backend Engineer", description="We are a fast-growing startup.")
    assert result.flexible_signal is False
    assert "flexible_signal" not in result.matched


def test_flexible_signal_reads_the_title_too() -> None:
    assert classify("Part-Time OK: Lab Assistant").flexible_signal is True


# ------------------------------------------------------------------ never excludes


def test_an_unmatched_title_is_other_and_still_a_valid_classification() -> None:
    """SPEC §7.1: "Any role that doesn't match a rule gets ``role_family='other'`` and still
    appears in the list." Nothing here raises, returns ``None`` or signals "drop this"."""
    result = classify("Underwater Basket Weaver")
    assert result == Classification(
        role_family=enums.RoleFamily.OTHER,
        employment_type=enums.EmploymentType.UNKNOWN,
        seniority=enums.Seniority.UNKNOWN,
        flexible_signal=False,
        matched={},
    )
    assert result.as_job_fields() == {
        "role_family": enums.RoleFamily.OTHER,
        "employment_type": enums.EmploymentType.UNKNOWN,
        "seniority": enums.Seniority.UNKNOWN,
        "flexible_signal": False,
    }


@pytest.mark.parametrize("title", ["", "   ", "!!!", "职位", "🚀", "a" * 5000])
def test_hostile_titles_classify_without_raising(title: str) -> None:
    assert classify(title).role_family is enums.RoleFamily.OTHER


# ------------------------------------------------------------------ matched keywords


def test_the_matched_keyword_is_reported_for_every_dimension() -> None:
    """SPEC §7.1: "Log the matched keyword on each classification so false positives can be
    traced.\""""
    result = classify(
        "Senior Machine Learning Engineer (Part-Time)",
        description="Students welcome — flexible hours.",
    )
    assert result.matched == {
        "role_family": "machine learning",
        "seniority": "senior",
        "employment_type": "part-time",
        "flexible_signal": "students welcome",
    }


def test_keywords_match_on_word_boundaries() -> None:
    """``intern`` must not match "internal", and ``ae`` must not match "Aegis"."""
    assert classify("Internal Communications Manager").employment_type is (
        enums.EmploymentType.UNKNOWN
    )
    assert classify("Aegis Platform Engineer").role_family is enums.RoleFamily.INFRASTRUCTURE


@pytest.mark.parametrize("spelling", ["Part-Time Tutor", "Part Time Tutor", "Part–Time Tutor"])
def test_a_hyphen_in_a_keyword_matches_spaces_and_dashes(spelling: str) -> None:
    assert classify(spelling).employment_type is enums.EmploymentType.PART_TIME


# ------------------------------------------------------------------ funding vocabulary


@pytest.mark.parametrize(
    "headline",
    [
        "Crusoe reportedly raises $3B at a $30B valuation",
        "Acme secures fresh capital",
        "Foo closes a Series B",
    ],
)
def test_funding_announcements_are_recognised(headline: str) -> None:
    assert is_funding_announcement(headline) is not None


def test_a_non_funding_headline_is_not_an_announcement() -> None:
    assert is_funding_announcement("How Sweden built a startup ecosystem") is None


@pytest.mark.parametrize(
    ("headline", "expected"),
    [
        ("Acme raises $2M pre-seed", enums.RoundType.PRE_SEED),
        ("Acme raises $2M seed round", enums.RoundType.SEED),
        ("Acme raises $12M Series A", enums.RoundType.SERIES_A),
        ("Acme raises $40M Series B", enums.RoundType.SERIES_B),
        ("Acme raises $90M Series C", enums.RoundType.SERIES_C),
        ("Acme raises $200M Series D", enums.RoundType.SERIES_D_PLUS),
        ("Acme raises $5M on a convertible note", enums.RoundType.CONVERTIBLE_NOTE),
        ("Acme lands $30M in venture debt", enums.RoundType.DEBT),
        ("Acme wins a $1M SBIR grant", enums.RoundType.GRANT),
        ("Acme raises money", enums.RoundType.UNKNOWN),
    ],
)
def test_round_types_are_classified_from_the_headline(
    headline: str, expected: enums.RoundType
) -> None:
    assert classify_round_type(headline)[0] is expected


def test_pre_seed_wins_over_seed() -> None:
    """File order again: ``pre-seed`` contains ``seed``, and the specific rule is listed first."""
    round_type, keyword = classify_round_type("Acme raises a pre-seed round")
    assert (round_type, keyword) == (enums.RoundType.PRE_SEED, "pre-seed")


# ------------------------------------------------------------------ editability


def test_precedence_can_be_changed_without_touching_python(tmp_path: Path) -> None:
    """CLAUDE.md: "edit YAML, not Python". Reordering two rules changes the answer."""
    yaml_text = textwrap.dedent(
        """
        role_family:
          - {family: qa, any: [test]}
          - {family: hardware, any: [test engineer]}
        employment_type:
          source_values: {}
          rules: []
        seniority:
          - {level: senior, any: [senior]}
        flexible_signal:
          any: [students welcome]
        funding:
          announcement: {any: [raises]}
          round_type: []
        """
    )
    path = tmp_path / "classifiers.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    reordered = load_classifiers(path)
    assert classify("Test Engineer", classifiers=reordered).role_family is enums.RoleFamily.QA
    # ... while the shipped file, which lists hardware first, answers hardware (SPEC §7.1).
    assert classify("Test Engineer").role_family is enums.RoleFamily.HARDWARE
