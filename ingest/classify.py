"""Role classification driven entirely by ``config/classifiers.yaml`` (SPEC §7.1, §11).

Every connector that produces a :class:`~ingest.base.JobRecord` calls :func:`classify` to fill
``role_family``, ``employment_type``, ``seniority`` and ``flexible_signal``. Nothing in this
module contains a keyword: the vocabulary, and the *precedence* between overlapping keywords,
live in the YAML so both can be changed without touching Python (CLAUDE.md).

**Classification never excludes.** A title that matches no rule is classified
``role_family='other'``, ``seniority='unknown'`` and is still ingested, stored and shown in the
default ``/`` and ``/roles`` views (SPEC §7.1, §13). ``flexible_signal`` is a badge and an
opt-in filter — never a default filter, and it never hides anything.

Matching rules
--------------
* Each block of the YAML is an **ordered list**; the first rule with a matching keyword wins,
  so list order is precedence. A value may appear in several rules, which is how "test
  engineer" reaches ``hardware`` (SPEC §7.1 lists it there) while a bare "test" reaches ``qa``.
* Keywords match case-insensitively on word boundaries: ``ae`` matches "AE" but not "Aegis",
  ``intern`` does not match "internal". Whitespace inside a keyword matches any run of
  whitespace, and ``-`` matches a hyphen, an en dash or a space, so "part-time", "part time"
  and "part–time" are one keyword.
* :attr:`Classification.matched` names the keyword that fired per dimension, and
  :func:`classify` logs it, so a false positive is traceable to one YAML line (SPEC §7.1).

Which text each dimension reads
-------------------------------
``role_family`` and ``seniority`` read the **title only** — a job description mentions every
function and every level in the company. ``employment_type`` prefers the ATS's own field
(SPEC §7.1 "Derived from the ATS field where present"), then the title, and only then the
description. ``flexible_signal`` reads title and description together, because that is where
"students welcome" and "flexible hours" are actually written.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any, Final

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from db import enums
from ingest.config import CONFIG_DIR
from logging_config import get_logger

log = get_logger(__name__)

CLASSIFIERS_YAML: Final = CONFIG_DIR / "classifiers.yaml"

#: How much of a description :func:`classify` scans. Descriptions run to tens of kilobytes and
#: the phrases we look for are near the top ("Part-time, 20 hrs/week", "students welcome"); the
#: cap keeps a full ATS board's classification linear and fast.
DESCRIPTION_SCAN_LIMIT: Final = 8000

# ``-`` in a keyword also matches an en dash, an em dash or a space: job titles spell
# "part-time", "part–time" and "part time" interchangeably.
_DASH_CLASS: Final = r"[-‐-―\s]"
_WHITESPACE: Final = re.compile(r"\s+")


def _compile_keyword(keyword: str) -> re.Pattern[str]:
    """One keyword as a word-boundary regex.

    ``\\b`` is only meaningful next to a word character, so it is applied conditionally: a
    keyword such as ``fp&a`` ends in a word character and gets a trailing boundary, while one
    ending in ``+`` does not (``\\b`` there would demand a following word character and never
    match).
    """
    parts = [
        _DASH_CLASS if char in "-‐‑‒–—" else re.escape(char)
        for char in _WHITESPACE.sub(" ", keyword.strip())
    ]
    body = "".join(_DASH_CLASS if part == re.escape(" ") else part for part in parts)
    prefix = r"\b" if keyword[:1].isalnum() or keyword[:1] == "_" else ""
    suffix = r"\b" if keyword[-1:].isalnum() or keyword[-1:] == "_" else ""
    return re.compile(f"{prefix}{body}{suffix}", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class _Rule:
    """One YAML rule: the value it assigns and its keywords, compiled once at load time."""

    value: str
    keywords: tuple[tuple[str, re.Pattern[str]], ...]

    def match(self, text: str) -> str | None:
        """The first keyword of this rule found in ``text``, or ``None``."""
        for keyword, pattern in self.keywords:
            if pattern.search(text):
                return keyword
        return None


def _rules(entries: Iterable[Mapping[str, Any]], key: str) -> tuple[_Rule, ...]:
    return tuple(
        _Rule(
            value=str(entry[key]),
            keywords=tuple((word, _compile_keyword(word)) for word in entry["any"]),
        )
        for entry in entries
    )


def _first_match(rules: Sequence[_Rule], text: str) -> tuple[str, str] | None:
    """``(value, keyword)`` of the first matching rule — list order is precedence."""
    for rule in rules:
        keyword = rule.match(text)
        if keyword is not None:
            return rule.value, keyword
    return None


# ------------------------------------------------------------------------------ the YAML file


class _RuleBlock(BaseModel):
    """Shared shape of every ordered rule list; the value key differs per block."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    any: tuple[str, ...] = Field(min_length=1)

    @field_validator("any")
    @classmethod
    def _non_empty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not word.strip() for word in value):
            msg = "keywords must not be blank"
            raise ValueError(msg)
        return value


class RoleFamilyRule(_RuleBlock):
    family: enums.RoleFamily


class EmploymentTypeRule(_RuleBlock):
    type: enums.EmploymentType


class SeniorityRule(_RuleBlock):
    level: enums.Seniority


class RoundTypeRule(_RuleBlock):
    type: enums.RoundType


class EmploymentTypeBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    #: Enum member -> the ATS's own spellings of it (SPEC §7.1).
    source_values: dict[enums.EmploymentType, tuple[str, ...]] = Field(default_factory=dict)
    rules: tuple[EmploymentTypeRule, ...] = ()


class FundingBlock(BaseModel):
    """Vocabulary for ``funding_rss`` (SPEC §4 Tier 2 #5), kept here so that connector has no
    keywords in Python either."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    announcement: _RuleBlock
    round_type: tuple[RoundTypeRule, ...] = ()


class ClassifiersConfig(BaseModel):
    """``config/classifiers.yaml``, validated on load so a typo fails fast with a field path."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    role_family: tuple[RoleFamilyRule, ...] = Field(min_length=1)
    employment_type: EmploymentTypeBlock
    seniority: tuple[SeniorityRule, ...] = Field(min_length=1)
    flexible_signal: _RuleBlock
    funding: FundingBlock


@dataclass(frozen=True, slots=True)
class Classifiers:
    """The compiled rule sets. Built once per file by :func:`load_classifiers`."""

    role_family: tuple[_Rule, ...]
    seniority: tuple[_Rule, ...]
    employment_type: tuple[_Rule, ...]
    employment_source_values: Mapping[str, enums.EmploymentType]
    flexible_signal: _Rule
    funding_announcement: _Rule
    round_type: tuple[_Rule, ...]


def _normalize_source_value(value: str) -> str:
    """ATS employment-type spellings differ only in separators and case: ``FullTime``,
    ``Full-time`` and ``FULL_TIME`` all reduce to ``fulltime``."""
    return re.sub(r"[\s_\-‐-―]+", "", value).casefold()


def load_classifiers(path: Path | None = None) -> Classifiers:
    """Load, validate and compile ``config/classifiers.yaml`` (cached per path).

    The default is resolved here rather than in the signature so that ``load_classifiers()``
    and ``load_classifiers(CLASSIFIERS_YAML)`` share one cache entry instead of parsing and
    compiling the file twice.
    """
    return _load_classifiers(path if path is not None else CLASSIFIERS_YAML)


@cache
def _load_classifiers(path: Path) -> Classifiers:
    with path.open(encoding="utf-8") as handle:
        config = ClassifiersConfig.model_validate(yaml.safe_load(handle))
    source_values: dict[str, enums.EmploymentType] = {}
    for member, spellings in config.employment_type.source_values.items():
        for spelling in spellings:
            source_values[_normalize_source_value(spelling)] = member
    return Classifiers(
        role_family=_rules((r.model_dump() for r in config.role_family), "family"),
        seniority=_rules((r.model_dump() for r in config.seniority), "level"),
        employment_type=_rules((r.model_dump() for r in config.employment_type.rules), "type"),
        employment_source_values=source_values,
        flexible_signal=_Rule(
            value="true",
            keywords=tuple((word, _compile_keyword(word)) for word in config.flexible_signal.any),
        ),
        funding_announcement=_Rule(
            value="true",
            keywords=tuple(
                (word, _compile_keyword(word)) for word in config.funding.announcement.any
            ),
        ),
        round_type=_rules((r.model_dump() for r in config.funding.round_type), "type"),
    )


# ------------------------------------------------------------------------------- the result


@dataclass(frozen=True, slots=True)
class Classification:
    """What :func:`classify` decided, plus the keyword that decided it.

    ``matched`` maps a dimension name (``role_family``, ``employment_type``, ``seniority``,
    ``flexible_signal``) to the keyword that fired. A dimension that fell back to its default
    is absent from the mapping — that is exactly the "unmatched title" case of SPEC §7.1, and
    the record is stored regardless.
    """

    role_family: enums.RoleFamily = enums.RoleFamily.OTHER
    employment_type: enums.EmploymentType = enums.EmploymentType.UNKNOWN
    seniority: enums.Seniority = enums.Seniority.UNKNOWN
    flexible_signal: bool = False
    matched: Mapping[str, str] = field(default_factory=dict)

    def as_job_fields(self) -> dict[str, Any]:
        """The four classification fields as ``JobRecord`` keyword arguments."""
        return {
            "role_family": self.role_family,
            "employment_type": self.employment_type,
            "seniority": self.seniority,
            "flexible_signal": self.flexible_signal,
        }


def classify(
    title: str,
    *,
    description: str | None = None,
    employment_type_hint: str | None = None,
    employment_type_from_description: bool = True,
    classifiers: Classifiers | None = None,
) -> Classification:
    """Classify one role (SPEC §7.1). Never raises and never rejects: an unmatched title is
    ``role_family='other'`` / ``seniority='unknown'`` and is still ingested.

    ``employment_type_hint`` is the ATS's own field (Lever ``categories.commitment``, Ashby
    ``employmentType``, Workable ``employment_type``); a value the YAML's ``source_values``
    knows wins over every keyword. Greenhouse publishes no such field, so its jobs fall through
    to the keyword rules.

    ``employment_type_from_description=False`` stops the description being used as the
    employment-type fallback while still letting it feed ``flexible_signal``. ``hn_hiring``
    needs that: one Hacker News comment advertises several roles, so an "Intern" further down
    the text would otherwise make every role in the comment an internship.
    """
    rules = classifiers if classifiers is not None else load_classifiers()
    haystack_title = _WHITESPACE.sub(" ", title)
    body = _WHITESPACE.sub(" ", (description or "")[:DESCRIPTION_SCAN_LIMIT])
    matched: dict[str, str] = {}

    role_family = enums.RoleFamily.OTHER
    if (hit := _first_match(rules.role_family, haystack_title)) is not None:
        role_family = enums.RoleFamily(hit[0])
        matched["role_family"] = hit[1]

    seniority = enums.Seniority.UNKNOWN
    if (hit := _first_match(rules.seniority, haystack_title)) is not None:
        seniority = enums.Seniority(hit[0])
        matched["seniority"] = hit[1]

    employment_type = enums.EmploymentType.UNKNOWN
    if employment_type_hint:
        member = rules.employment_source_values.get(_normalize_source_value(employment_type_hint))
        if member is not None:
            employment_type = member
            matched["employment_type"] = f"ats:{employment_type_hint}"
    if employment_type is enums.EmploymentType.UNKNOWN:
        # Title first, description only as a fallback: nearly every description says "full-time"
        # somewhere, which would drown out a part-time title.
        sources = (haystack_title, body) if employment_type_from_description else (haystack_title,)
        for text in sources:
            if not text:
                continue
            if (hit := _first_match(rules.employment_type, text)) is not None:
                employment_type = enums.EmploymentType(hit[0])
                matched["employment_type"] = hit[1]
                break

    flexible_keyword = rules.flexible_signal.match(f"{haystack_title}\n{body}")
    if flexible_keyword is not None:
        matched["flexible_signal"] = flexible_keyword

    result = Classification(
        role_family=role_family,
        employment_type=employment_type,
        seniority=seniority,
        flexible_signal=flexible_keyword is not None,
        matched=matched,
    )
    log.debug(
        "classified",
        title=title,
        role_family=result.role_family.value,
        employment_type=result.employment_type.value,
        seniority=result.seniority.value,
        flexible_signal=result.flexible_signal,
        matched=dict(matched),
    )
    return result


# ------------------------------------------------------------------- funding_rss vocabulary


def is_funding_announcement(text: str, *, classifiers: Classifiers | None = None) -> str | None:
    """The keyword that makes ``text`` read as a funding announcement, or ``None``
    (``config/classifiers.yaml``: ``funding.announcement``)."""
    rules = classifiers if classifiers is not None else load_classifiers()
    return rules.funding_announcement.match(_WHITESPACE.sub(" ", text))


def classify_round_type(
    text: str, *, classifiers: Classifiers | None = None
) -> tuple[enums.RoundType, str | None]:
    """``(round_type, matched keyword)`` for a funding headline; ``UNKNOWN`` when nothing
    matches (``config/classifiers.yaml``: ``funding.round_type``)."""
    rules = classifiers if classifiers is not None else load_classifiers()
    hit = _first_match(rules.round_type, _WHITESPACE.sub(" ", text))
    if hit is None:
        return enums.RoundType.UNKNOWN, None
    return enums.RoundType(hit[0]), hit[1]
