"""``config/outreach.yaml`` and :func:`ingest.config.load_outreach_config` (SPEC §6, §12 Phase 8).

The pitch is config and not code because it is the thing most worth rewriting: startups do not
advertise part-time work, they invent it for a specific person who asks, so what this app hands
over is a reachable human and a first draft, and the draft is the variable that decides whether
any of it works. CLAUDE.md's "edit YAML, not Python" is what keeps rewriting it a five-minute
edit — and it is also what puts the pitch outside every type checker and every code review.
This module is the compensating control.

Two halves, and they answer different questions.

*The shipped file* is checked as shipped: it loads, every key has the type and shape the loader
promises, and the drafts still fit the places they are pasted into. That last one is the test
worth having. A LinkedIn connection-request note is capped at 300 characters and refused one
character over, and the loader measures a template against one representative pair of names — so
an edit that adds a sentence can pass load and still be unpasteable for a real founder with a
long name. Nothing else in the suite would notice, because nothing else reads these words.

*The loader's rules* are checked against files written for the purpose, never against the shipped
one: a case that asserted on the shipped pitch would fail the day somebody rewrites it, which is
the one thing this file exists to keep easy. Each case gets a path of its own, because
``load_outreach_config`` is ``@cache``d on its path — the last section pins that, since it is the
reason for the ceremony rather than an accident of it.

Nothing here renders anything. How a door is chosen from ``contact_priority``, how an address is
turned into a ``mailto:`` and how the note reaches a ``<textarea>`` belong to
``tests/test_web_panel.py``; what is pinned here is the vocabulary those modules code against.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from db import enums
from ingest.config import (
    _SAMPLE_COMPANY,
    _SAMPLE_PERSON,
    LINKEDIN_NOTE_LIMIT,
    OutreachConfig,
    load_outreach_config,
)

#: A complete file that loads: the smallest document every rule below starts from, so a case can
#: say what it is testing by overriding one key of it.
#:
#: Deliberately *not* the shipped pitch, and deliberately bland. A rule is a rule about any pitch,
#: and a case built out of the shipped words would start failing the day those words are rewritten
#: — which is exactly the edit this whole file exists to keep cheap.
MINIMAL: dict[str, Any] = {
    "email": {
        "subject": "Ten hours a week for {company}",
        "body": "Hi - I have time this quarter and would like to spend it at {company}.",
    },
    "linkedin": {"note": "Hi {person} - I would like to help {company} with one specific thing."},
    "contact_priority": ["founder", "exec"],
    "role_address_prefixes": ["info", "jobs"],
    "entity_markers": ["LLC"],
}

#: A company and a person at the long end of what a corpus of small startups really holds — a
#: three-word product name, and a full name with a long surname. Both are longer than the pair the
#: loader measures a template against (:data:`ingest.config._SAMPLE_COMPANY`,
#: :data:`ingest.config._SAMPLE_PERSON`), which is what makes the check below stricter than the
#: one the load already performed rather than a second copy of it.
REALISTIC_COMPANY = "Helioscope Instrumentation"
REALISTIC_PERSON = "Konstantinos Papadopoulos"

#: The most email body that can survive being handed to a mail client. ``mailto:`` carries the
#: body percent-encoded, which adds roughly half again to its length, and clients start truncating
#: the URL around 2,000 characters — so the body is the half of the budget that is written by
#: hand, and this is what is left for it. A truncated draft is worse than a short one: it opens
#: looking finished and stops mid-sentence.
MAILTO_BODY_BUDGET = 1_200


def write_outreach(tmp_path: Path, name: str, document: Mapping[str, Any]) -> OutreachConfig:
    """Write an outreach file and load it through the real loader.

    ``load_outreach_config`` is ``@cache``d on its path (and says so), so every case writes a file
    of its own: ``tmp_path`` separates the tests and ``name`` separates the files inside one test.
    Two cases sharing a path would silently get whichever document loaded first, and the second
    would then assert against the first one's pitch.
    """
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(dict(document), sort_keys=False), encoding="utf-8")
    return load_outreach_config(path)


# ==================================================================== the file as it ships


def test_the_shipped_file_loads_with_the_shape_the_loader_promises() -> None:
    """SPEC §12 Phase 8: ``config/outreach.yaml`` is read while rendering a company row, so the
    shape it parses to is a contract the panel and the templating filters are written against —
    not an internal detail of the file.

    Every field is checked for the type *and* the invariant its validator establishes, because
    the two are what callers rely on and only one of them is visible to a type checker: a prefix
    is matched against the already-lower-cased local part of an address, so an unfolded ``Info``
    would type-check and simply never match, and the shared-inbox hint would go quietly missing
    for that one prefix.

    The templates are checked to be *trimmed*, since that is what makes the 300-character budget
    honest — a block scalar written ``|`` instead of ``|-`` keeps a trailing newline that nobody
    reads and LinkedIn counts.
    """
    config = load_outreach_config()

    for field, template in (
        ("email_subject", config.email_subject),
        ("email_body", config.email_body),
        ("linkedin_note", config.linkedin_note),
    ):
        assert isinstance(template, str), field
        assert template == template.strip() != "", field
    # Both drafts are about one company, and only the note is about a person as well.
    assert "{company}" in config.email_subject
    assert "{company}" in config.email_body
    assert "{company}" in config.linkedin_note
    assert "{person}" in config.linkedin_note

    assert isinstance(config.contact_priority, tuple)
    assert all(isinstance(role, enums.RoleType) for role in config.contact_priority)

    assert isinstance(config.role_address_prefixes, frozenset)
    assert config.role_address_prefixes == {
        prefix.casefold() for prefix in config.role_address_prefixes
    }
    assert not [prefix for prefix in config.role_address_prefixes if "@" in prefix or not prefix]

    assert isinstance(config.entity_markers, tuple)
    assert all(marker == marker.strip() != "" for marker in config.entity_markers)


def test_a_role_type_is_still_a_plain_string_to_a_caller_holding_one() -> None:
    """``contact_priority`` parses to :class:`db.enums.RoleType` members so a value the schema
    does not define fails on load — but the panel compares them against a stored
    ``Person.role_type`` in one place and against a label in another, and a
    :class:`~enum.StrEnum` member is both.

    Pinned because it is the only reason the field can be typed as the enum without forcing every
    caller to convert: were these plain ``str`` (or were ``RoleType`` a plain ``Enum``), each of
    these comparisons would silently be ``False`` and the panel would rank nobody.
    """
    priority = load_outreach_config().contact_priority

    assert "founder" in priority
    assert priority[0] == "founder"
    assert priority[0] == enums.RoleType.FOUNDER
    assert [str(role) for role in priority] == [role.value for role in priority]


def test_the_shipped_order_asks_a_founder_before_a_recruiter() -> None:
    """SPEC §12 Phase 8: the panel offers the founder first and the recruiter last, which inverts
    the conventional advice on purpose. Two measurements are the argument, and they are written
    down here so that a future editor who reverses the order learns why it was this way.

    *These companies have no talent function.* Measured on the live database on 2026-09-09, 866
    of them name a founder and exactly **2** of 17,172 stored people are recruiters. An order
    that led with the recruiter would, for almost every company, lead with nobody.

    *A recruiter cannot say yes to this.* A recruiter fills approved headcount, and no headcount
    plan has a line for a part-time student; only a founder can invent a role that does not exist
    yet, which is the entire premise of the phase. The recruiter stays on the list — where one
    exists they are still a way in (SPEC §7.1: classification never excludes) — and stays last.

    Asserted as an ordering rather than as the exact shipped list, so reordering the two middle
    roles is an edit to the file and reordering the two ends is a decision that has to be argued
    with here.
    """
    priority = load_outreach_config().contact_priority

    assert priority.index(enums.RoleType.FOUNDER) < priority.index(enums.RoleType.RECRUITER)
    assert priority[0] == enums.RoleType.FOUNDER
    assert priority[-1] == enums.RoleType.RECRUITER


def test_the_shipped_order_ranks_every_role_a_person_can_be_stored_as() -> None:
    """SPEC §7.1: classification never excludes. ``role_type`` is what the panel ranks people by,
    so a role missing from this list is a set of real, named, reachable people the panel has no
    place for — an omission that reads exactly like "nobody here to write to".

    The loader permits a shorter list on purpose (it validates an order, not a census); this pins
    that the *shipped* file does not take that option, and it is the assertion that fails if a
    member is added to :class:`db.enums.RoleType` and nobody teaches the pitch file about it.
    """
    assert set(load_outreach_config().contact_priority) == set(enums.RoleType)


def test_the_shipped_note_still_fits_once_a_real_company_and_person_are_written_into_it() -> None:
    """SPEC §12 Phase 8: 300 characters is LinkedIn's cap on a connection-request note, measured
    on the note **as sent**, and a draft one character over is not a slightly long draft —
    LinkedIn refuses it, so the door this tier exists to open is shut at the moment of use.

    The loader already fails a template that cannot fit, but it measures against one
    representative pair of names, and that check is a budget rather than a guarantee. This is the
    stricter half: the pair here is longer than the loader's on both sides (asserted, so it stays
    that way), which means a pitch edit that adds a sentence can pass load and fail here — which
    is the whole point, because the person it would fail for is a real founder with a long name
    and the symptom would otherwise be a paste that silently will not send.
    """
    note = load_outreach_config().linkedin_note
    assert len(REALISTIC_COMPANY) > len(_SAMPLE_COMPANY)
    assert len(REALISTIC_PERSON) > len(_SAMPLE_PERSON)

    drafted = note.format(company=REALISTIC_COMPANY, person=REALISTIC_PERSON)

    assert len(drafted) <= LINKEDIN_NOTE_LIMIT, (
        f"the shipped linkedin.note renders to {len(drafted)} characters for a company named "
        f"{REALISTIC_COMPANY!r} and a person named {REALISTIC_PERSON!r}; LinkedIn caps a "
        f"connection-request note at {LINKEDIN_NOTE_LIMIT} and refuses to send one longer"
    )
    # Both names survive into the sent note: a draft that greets nobody is the company-wide
    # pitch, and naming the person is the only thing tiers 1 and 2 have over tier 3.
    assert REALISTIC_PERSON in drafted
    assert REALISTIC_COMPANY in drafted


def test_the_shipped_email_stays_inside_what_a_mailto_url_can_carry() -> None:
    """SPEC §12 Phase 8: the email door is a ``mailto:`` the browser hands to a mail client, and
    the subject and body ride in the URL percent-encoded — which adds roughly half again to their
    length against a client that starts truncating around 2,000 characters.

    So the body has a real ceiling, and it is not a style preference: a body over it opens a draft
    that looks finished and stops mid-sentence, in the composer, after the send is already in
    motion. A subject spanning lines is the other half — a header is one line, and a newline in
    one is a draft that arrives wrong or not at all.

    The encoded length itself is ``mailto_url``'s to pin (``tests/test_web_panel.py``); what is
    checked here is the hand-written half of the budget, which is the half that gets edited.
    """
    config = load_outreach_config()

    assert len(config.email_body) <= MAILTO_BODY_BUDGET, (
        f"the shipped email.body is {len(config.email_body)} characters; percent-encoding adds "
        f"about half again and mail clients truncate the URL around 2,000, so {MAILTO_BODY_BUDGET}"
        " is the ceiling for the body"
    )
    assert "\n" not in config.email_subject
    assert len(config.email_subject) <= 120  # a subject line nobody's inbox will cut off


def test_the_loaded_pitch_cannot_be_rewritten_in_place() -> None:
    """One :class:`OutreachConfig` is cached and handed to every request that expands a company
    row, so a mutable one would let a single render edit the pitch for every render after it —
    a bug with no failing test anywhere near it.

    ``frozen=True`` is what rules that out, and it is worth pinning rather than trusting because
    it is one word in a ``model_config`` and nothing else in the suite would notice its removal.
    """
    config = load_outreach_config()

    with pytest.raises(ValidationError, match="frozen"):
        config.email_subject = "rewritten in a request handler"  # type: ignore[misc]

    assert load_outreach_config().email_subject == config.email_subject


# ============================================================ the templates the loader refuses


def test_a_note_that_fits_as_written_and_not_once_it_is_filled_in_fails_on_load(
    tmp_path: Path,
) -> None:
    """SPEC §12 Phase 8: LinkedIn's 300 characters are counted on the note as *sent*, so a
    template is only as short as its filled-in form and the loader measures it filled in.

    The note below is exactly 300 characters as it is written in the file — it passes any check
    that reads the template — and goes over the moment the two names are substituted for their
    placeholders, which is the failure a length check on the raw string would wave through. The
    only moment left to catch it after this one is a paste LinkedIn refuses, hours of drafting
    later, with nothing on screen to say why.

    The message is asserted, not just the failure: it names the key as the *file* spells it
    (``linkedin.note``, not the model's flat ``linkedin_note``), gives the overshoot as a number
    and gives the pair of names it was measured against — because "too long" without a number
    means editing and reloading until it stops failing.
    """
    lead = "Hi {person}, one specific thing I could own at {company}. "
    note = lead + "." * (LINKEDIN_NOTE_LIMIT - len(lead))
    assert len(note) == LINKEDIN_NOTE_LIMIT  # as written, it fits
    over = len(note.format(company=_SAMPLE_COMPANY, person=_SAMPLE_PERSON)) - LINKEDIN_NOTE_LIMIT
    assert over > 0  # filled in, it does not

    with pytest.raises(
        ValueError, match=rf"linkedin\.note is {over} characters too long"
    ) as caught:
        write_outreach(tmp_path, "over-the-cap", {**MINIMAL, "linkedin": {"note": note}})

    message = str(caught.value)
    assert _SAMPLE_COMPANY in message
    assert _SAMPLE_PERSON in message
    assert str(LINKEDIN_NOTE_LIMIT) in message


def test_a_note_that_only_just_fits_is_accepted(tmp_path: Path) -> None:
    """The other side of the same boundary, so the rule is a cap and not a mood: a note that
    renders to exactly :data:`ingest.config.LINKEDIN_NOTE_LIMIT` characters is a note LinkedIn
    sends, and rejecting it would quietly cost the pitch its last sentence.

    Without this case the check could be off by one in the safe direction for ever, and every
    other test in the file would still pass.
    """
    lead = "Hi {person}, one specific thing I could own at {company}. "
    filled = len(lead.format(company=_SAMPLE_COMPANY, person=_SAMPLE_PERSON))
    note = lead + "." * (LINKEDIN_NOTE_LIMIT - filled)

    config = write_outreach(tmp_path, "exactly-the-cap", {**MINIMAL, "linkedin": {"note": note}})

    drafted = config.linkedin_note.format(company=_SAMPLE_COMPANY, person=_SAMPLE_PERSON)
    assert len(drafted) == LINKEDIN_NOTE_LIMIT


def test_a_placeholder_nothing_fills_in_fails_on_load(tmp_path: Path) -> None:
    """The only place these templates are ever formatted is while serving a page, and
    ``str.format`` raises ``KeyError`` for a name it was not given — so ``{firstname}`` written
    for ``{person}`` would turn one expanded company row into a 500 rather than into an obviously
    wrong draft. The vocabulary is short and fixed, so the honest place to catch the typo is on
    load, naming the key and offering what it may use.

    An email draft is built from a company row alone and has nobody to name, which is why
    ``{person}`` is unknown *there* and known in the note: the vocabulary is per key, and a check
    that pooled both would accept a subject line addressed to a person the mail door never has.
    """
    with pytest.raises(ValueError, match=r"linkedin\.note uses \{firstname\}") as caught:
        write_outreach(
            tmp_path,
            "unknown-name",
            {**MINIMAL, "linkedin": {"note": "Hi {firstname} at {company}"}},
        )
    # The message offers the vocabulary, so the fix does not require reading the loader.
    assert "{company}, {person}" in str(caught.value)

    with pytest.raises(ValueError, match=r"email\.subject uses \{person\}") as caught:
        write_outreach(
            tmp_path,
            "person-in-the-email",
            {**MINIMAL, "email": {"subject": "Hi {person}", "body": "About {company}."}},
        )
    assert "it may use: {company}" in str(caught.value)


def test_a_positional_or_unbalanced_placeholder_fails_on_load(tmp_path: Path) -> None:
    """The two ways a template can be un-renderable rather than merely wrong.

    A positional field (``{}``, ``{0}``) is an ``IndexError`` waiting for the first company it is
    rendered for, since the panel formats by keyword only — and its "name" is not something a
    message could quote, so it is rejected as its own case rather than reported as an unknown
    name. A stray brace is not a template at all: :class:`string.Formatter` is the parser
    ``format`` itself will use, so an unmatched ``{`` raises here instead of in a request handler.
    """
    with pytest.raises(ValueError, match=r"email\.subject uses the positional placeholder"):
        write_outreach(
            tmp_path, "positional", {**MINIMAL, "email": {"subject": "Hi {}", "body": "Hi."}}
        )

    with pytest.raises(ValueError, match=r"email\.body is not a valid template"):
        write_outreach(
            tmp_path, "stray-brace", {**MINIMAL, "email": {"subject": "Hi", "body": "At {company"}}
        )


def test_a_doubled_brace_is_literal_text_and_not_a_placeholder(tmp_path: Path) -> None:
    """The escape the file documents, checked end to end: ``{{`` is a literal brace to
    :class:`string.Formatter` and to ``format`` alike, so a pitch that wants a brace in it — a
    bracketed signature line, a snippet of code — is not forced to choose between saying so and
    loading.

    Without this case the placeholder check could be tightened into rejecting every brace and the
    file's own instructions would become wrong with nothing to say so.
    """
    config = write_outreach(
        tmp_path,
        "doubled",
        {**MINIMAL, "email": {"subject": "{{draft}} for {company}", "body": "Hi."}},
    )

    assert config.email_subject.format(company=REALISTIC_COMPANY) == (
        f"{{draft}} for {REALISTIC_COMPANY}"
    )


def test_a_blank_template_fails_on_load(tmp_path: Path) -> None:
    """An empty subject, body or note is a door with nothing written on it: the panel would offer
    a button, the mail client would open an empty draft, and the one thing this phase promises —
    that you never start from a blank page — would be quietly untrue.

    Whitespace is the case that matters, because ``min_length=1`` accepts ``"   "`` and a quoted
    scalar can carry padding no reader sees. The same trim is what keeps the 300-character budget
    honest, which is why it is applied to the stored value and not merely checked.

    The key is quoted as the *file* spells it, not as the model names the field: every other
    template failure says ``email.subject``, and one message saying ``email_subject`` would send
    the reader looking for a line that is not in the file.
    """
    with pytest.raises(ValueError, match=r"email\.subject must not be blank"):
        write_outreach(
            tmp_path, "blank-subject", {**MINIMAL, "email": {"subject": "   ", "body": "Hi."}}
        )

    with pytest.raises(ValueError, match=r"linkedin\.note must not be blank"):
        write_outreach(tmp_path, "blank-note", {**MINIMAL, "linkedin": {"note": "\n"}})

    # And padding a real template survives as the trimmed text, so nothing invisible is sent or
    # counted against the cap.
    config = write_outreach(
        tmp_path, "padded", {**MINIMAL, "linkedin": {"note": "  Hi {person} at {company}.\n"}}
    )
    assert config.linkedin_note == "Hi {person} at {company}."


def test_an_unknown_key_fails_on_load(tmp_path: Path) -> None:
    """``extra="forbid"``, and the hand-written half of it.

    A key nothing reads is a pitch edit that had no effect — the worst failure this file can have,
    because the draft that goes out is the old one and nothing says so. At the top level pydantic
    catches it. Inside a block it cannot: ``email:`` and ``linkedin:`` are folded into flat fields
    by a ``mode="before"`` validator, and by the time ``extra="forbid"`` looks, the block is gone.
    Left alone, ``email: {subjekt: ...}`` would fail as "email_subject: Field required" — naming a
    key the file does not contain and sending the reader looking for the wrong thing — so the
    unknown sub-key is rejected by hand, spelled as the file spells it and with the block's own
    vocabulary beside it.
    """
    with pytest.raises(ValidationError, match="Extra inputs are not permitted") as caught:
        write_outreach(tmp_path, "extra-top-level", {**MINIMAL, "signature": "- sent from my app"})
    assert "signature" in str(caught.value)

    with pytest.raises(ValueError, match=r"unknown key email\.subjekt") as inside_a_block:
        write_outreach(
            tmp_path, "extra-sub-key", {**MINIMAL, "email": {"subjekt": "Hi", "body": "Hi."}}
        )
    assert "email takes: body, subject" in str(inside_a_block.value)


def test_an_omitted_key_fails_on_load(tmp_path: Path) -> None:
    """Nothing in the model has a default, and that is the rule rather than an oversight: a pitch,
    a priority order or a guard list carried in Python would be a second place the behaviour is
    defined — the one nobody thinks to edit — which is the mistake CLAUDE.md rules out for
    classifier keywords.

    So a file missing a key fails on load, where the message names it, instead of rendering a
    panel built on a default nobody chose.
    """
    without_linkedin = {key: value for key, value in MINIMAL.items() if key != "linkedin"}

    with pytest.raises(ValidationError, match="Field required") as caught:
        write_outreach(tmp_path, "no-linkedin", without_linkedin)
    assert "linkedin_note" in str(caught.value)


# ================================================= the priority order and the two guard lists


def test_a_role_the_schema_does_not_define_fails_on_load(tmp_path: Path) -> None:
    """``contact_priority`` is matched against ``Person.role_type`` (SPEC §5), so a value outside
    that enum can never match anybody: it is not a role the panel ranks lower, it is a line of the
    file that does nothing at all.

    Typing the field as the enum is what turns that into a load failure listing the roles that do
    exist — the alternative, ``tuple[str, ...]``, would take ``cto`` happily and rank nobody by it
    for ever. A repeat is the same class of typo: the list is an order, so a second mention can
    never be reached, and it is almost always a role that was meant to be there instead.
    """
    with pytest.raises(ValidationError, match="Input should be") as caught:
        write_outreach(
            tmp_path, "unknown-role", {**MINIMAL, "contact_priority": ["founder", "cto"]}
        )
    message = str(caught.value)
    assert "'founder'" in message and "'recruiter'" in message

    with pytest.raises(ValueError, match="lists 'founder' twice; it is an order, not a set"):
        write_outreach(tmp_path, "repeated-role", {**MINIMAL, "contact_priority": ["founder"] * 2})

    with pytest.raises(ValidationError, match="at least 1 item"):
        write_outreach(tmp_path, "no-roles", {**MINIMAL, "contact_priority": []})


def test_a_role_address_prefix_must_be_a_bare_local_part(tmp_path: Path) -> None:
    """``info@`` is how this is mistyped, and the failure it causes is invisible. A prefix is
    compared against the text before the ``@`` of an already-lower-cased address, so a stored
    ``@`` or a stray capital never matches — and a shared-inbox hint that is missing looks exactly
    like a hint that was not warranted. Nobody would ever report it.

    Case is folded rather than rejected, since only the ``@`` is a real error: a prefix written
    with a capital is a spelling, not a mistake. An empty list is allowed, and means the hint is
    switched off in the open — the guard is display-only either way (SPEC §7.1: classification
    never excludes), so removing a prefix removes a hint and never a company.
    """
    with pytest.raises(ValueError, match="must be the local part on its own"):
        write_outreach(tmp_path, "prefix-with-at", {**MINIMAL, "role_address_prefixes": ["info@"]})

    folded = write_outreach(
        tmp_path, "prefix-case", {**MINIMAL, "role_address_prefixes": ["INFO", " Jobs "]}
    )
    assert folded.role_address_prefixes == frozenset({"info", "jobs"})

    switched_off = write_outreach(tmp_path, "no-prefixes", {**MINIMAL, "role_address_prefixes": []})
    assert switched_off.role_address_prefixes == frozenset()


def test_an_entity_marker_may_not_be_blank_and_the_list_may_be_empty(tmp_path: Path) -> None:
    """The markers exist because a person taken from an SEC Form D "related persons" list is not
    always a person — EDGAR yields fund and partnership names stored with ``role_type=founder``,
    and the panel must never draft a note to an LLC.

    Blank is the failure worth catching on load: an empty marker is contained in every name, so
    every named person in the database would read as a legal entity and every company would fall
    back to the company-wide search. The guard would silently delete the two tiers it exists to
    protect, and the panel would still render — just worse, everywhere, with no error.

    An empty *list* is a different thing and is allowed: it switches the guard off in the open,
    which is a choice a reader of the file can see, and it costs a filing entity being offered as
    somebody to message rather than every real person being hidden.
    """
    with pytest.raises(ValueError, match="entity_markers must not be blank"):
        write_outreach(tmp_path, "blank-marker", {**MINIMAL, "entity_markers": ["LLC", "  "]})

    switched_off = write_outreach(tmp_path, "no-markers", {**MINIMAL, "entity_markers": []})
    assert switched_off.entity_markers == ()

    # Order is kept, because a caller scans them in file order; padding is trimmed, because a
    # marker is compared as a word and a padded one would be compared as a different word.
    kept = write_outreach(tmp_path, "markers", {**MINIMAL, "entity_markers": [" Fund ", "GP"]})
    assert kept.entity_markers == ("Fund", "GP")


# ============================================================================ the loader's cache


def test_the_loader_caches_on_the_path_so_an_alternate_file_needs_a_path_of_its_own(
    tmp_path: Path,
) -> None:
    """``load_outreach_config`` is ``@cache``d, and the cache is keyed on the path alone.

    That is what makes it safe to call from a request handler — this file is read while rendering
    an expanded company row, so without the cache every row would re-read and re-validate YAML
    inside the handler. It is also the reason :func:`write_outreach` gives every case a file of
    its own: a second document written to a path already loaded is not read, and a case that
    reused a path would assert against the previous case's pitch and pass or fail for a reason
    that has nothing to do with what it is testing.

    The practical consequence is worth stating too, since it is the one that surprises: editing
    ``config/outreach.yaml`` takes effect on the next process start, not on the next page load.
    The pitch is rewritten between sessions of writing, and a per-request stat on every row is not
    worth paying for that.

    The cache is cleared in a ``finally`` so no ``tmp_path`` file — a path that will not exist a
    moment from now — is left cached for the rest of the session.
    """
    path = tmp_path / "cached.yaml"
    path.write_text(yaml.safe_dump(MINIMAL, sort_keys=False), encoding="utf-8")
    try:
        first = load_outreach_config(path)
        assert load_outreach_config(path) is first  # read once, not once per rendered row

        rewritten = dict(MINIMAL)
        rewritten["email"] = {"subject": "Rewritten for {company}", "body": "At {company}."}
        path.write_text(yaml.safe_dump(rewritten, sort_keys=False), encoding="utf-8")
        assert load_outreach_config(path) is first
        assert load_outreach_config(path).email_subject == MINIMAL["email"]["subject"]

        load_outreach_config.cache_clear()
        assert load_outreach_config(path).email_subject == "Rewritten for {company}"
    finally:
        load_outreach_config.cache_clear()
