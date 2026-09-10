"""The Jinja2 environment: one :class:`Jinja2Templates` instance and the filters and globals
every template shares (SPEC §9).

Autoescaping is on (Jinja2Templates' default), so *every* value interpolated into a template is
HTML-escaped. The one thing escaping cannot make safe is a URL in an ``href``: an escaped
``javascript:alert(1)`` still runs when clicked. Job and company URLs come from scraped
third-party pages (SPEC §4, Tiers 1 to 3), which makes that a real injection vector, so
:func:`safe_url` exists and **every externally sourced URL goes through it** before it reaches
an attribute.

:func:`mailto_url` is the one deliberate exception to that rule, and it is an exception in the
safe direction: a ``mailto:`` is not an ``http(s)`` URL, so :func:`safe_url` refuses it **by
design**, and relaxing ``SAFE_SCHEMES`` to let it through would re-open the ``href`` for every
other scheme on every other link in the app. The address is instead percent-encoded here, which
is what an ``href`` needs anyway (SPEC §12 Phase 8).

Nothing here touches the database or the network; these are pure display helpers. The two things
any of them read off the disk are ``config/regions.yaml``, for :func:`site_name`, and
``config/outreach.yaml``, for the drafts :func:`mailto_url` and :func:`shared_inbox` work from.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

from starlette.requests import Request
from starlette.templating import Jinja2Templates

# Importing ``ingest.config`` from ``web/`` is already established — ``web.routes.runs`` reads
# the connector cadences through it — and it does not put a handler one import away from an
# outbound call (SPEC §2): that module parses local YAML and nothing else. No httpx, no
# connector, no fetch; ``tests/test_web_acceptance.py`` exempts it from FORBIDDEN_IMPORTS on
# exactly those grounds. ``load_outreach_config`` arrives through the same door and on the same
# terms: it parses ``config/outreach.yaml`` and hands back a frozen model (SPEC §12 Phase 8).
from ingest.config import LINKEDIN_NOTE_LIMIT, load_outreach_config, load_regions_config
from web import labels
from web.labels import EM_DASH

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

#: Schemes allowed in an ``href``. Everything else — ``javascript:``, ``data:``, ``file:``, a
#: protocol-relative ``//host`` — is refused by :func:`safe_url`.
SAFE_SCHEMES = frozenset({"http", "https"})
#: Characters browsers strip from a URL before parsing it, which is how ``java\nscript:`` gets
#: past a naive prefix check. They are removed before the scheme is inspected *and* from the
#: value that is returned, so what is validated is what is rendered.
_URL_STRIPPED_CHARS = str.maketrans("", "", "\t\r\n")

#: The query parameter that is dropped from every generated link — see :func:`query_string`.
CURSOR_PARAM = "cursor"

_SECONDS_PER_MINUTE = 60
_SECONDS_PER_HOUR = 3600
_SECONDS_PER_DAY = 86400
#: Beyond this, "412d ago" stops meaning anything and an absolute date reads better.
_AGO_MAX_DAYS = 90
#: A short run's duration is worth a decimal ("0.4s"); a long one is not ("93m 12.3s").
_SUB_SECOND_PRECISION_BELOW = 10

_MONEY_UNITS = ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K"))

#: The ceiling :func:`mailto_url` keeps the whole ``mailto:`` under. Mail clients start
#: truncating somewhere around 2,000 characters, they do it silently, and they do it at the
#: *end* — which is where the ask is. Cutting the draft here instead means the cut is
#: deliberate, testable, and always falls in the body rather than wherever the client's own
#: limit happens to land (SPEC §12 Phase 8).
MAILTO_MAX_URL = 1_900
#: The share of that ceiling the subject may spend, encoded. A subject is one line and no mail
#: client shows this much of it in a list view, so the cap costs nothing real — and without one a
#: single overlong company name (``{company}`` is interpolated into the subject *and* the body)
#: could spend the whole budget before the body was reached, which is the one outcome that would
#: make the ceiling above a promise this function does not keep.
_MAILTO_SUBJECT_MAX = 200

#: Characters left un-escaped in the address half of a ``mailto:``. Only ``@`` is added to what
#: :func:`urllib.parse.quote` already leaves alone: a ``?`` or ``&`` that came off a scraped page
#: must become ``%3F``/``%26`` rather than starting the query string, since an address that can
#: open a query can append its own ``bcc=`` to the draft. That is the same class of injection
#: :func:`safe_url` exists for, arriving through the one link that cannot use it.
_MAILTO_ADDRESS_SAFE = "@"


def safe_url(value: object) -> str | None:
    """The URL if it is an absolute ``http(s)`` URL, else ``None``.

    Templates render an anchor only when this returns a value::

        {% set href = job.url | safe_url %}
        {% if href %}<a href="{{ href }}">{{ job.title }}</a>{% else %}{{ job.title }}{% endif %}

    A ``netloc`` is required as well as the scheme, so ``http:evil`` and a protocol-relative
    ``//evil.example`` are refused too: an ``href`` that does not name a host is either broken
    or a trick.
    """
    if not isinstance(value, str):
        return None
    candidate = value.translate(_URL_STRIPPED_CHARS).strip()
    if not candidate:
        return None
    try:
        parts = urlsplit(candidate)
    except ValueError:
        # urlsplit raises on a malformed IPv6 literal ("http://[::1"); that is not a link.
        return None
    if parts.scheme.lower() not in SAFE_SCHEMES or not parts.netloc:
        return None
    return candidate


def mailto_url(address: object, company: object) -> str:
    """The ``mailto:`` href for one published address, subject and body already filled in from
    ``config/outreach.yaml`` — or ``""`` when ``address`` is not an address (SPEC §6, §12 Phase 8).

    The email door of the company panel, and the *only* href in the app that does not go through
    :func:`safe_url`. That is not an oversight to be fixed by widening :data:`SAFE_SCHEMES`: a
    ``mailto:`` has no host, so ``safe_url`` would have to stop requiring one, and every scraped
    ``http:evil`` link in the app would be re-admitted to buy this one anchor. The scheme is
    written here instead, where it is a constant rather than something read off a page, and the
    parts that *did* come off a page are percent-encoded (:data:`_MAILTO_ADDRESS_SAFE`).

    ``company`` fills the templates' ``{company}`` placeholder and may be given as the name or as
    the company row itself — the template that calls this filter holds the row, and a
    ``str(company)`` that quietly rendered ``<db.models.Company object at 0x…>`` into a subject
    line would be found by the recipient rather than by a test.

    The whole URL is held to :data:`MAILTO_MAX_URL` characters. The body absorbs almost all of
    any cut — the subject has its own small :data:`_MAILTO_SUBJECT_MAX` so that one absurdly long
    company name cannot spend the budget before the body is reached — and the shipped draft
    builds a URL of about 1,240, so in practice nothing is cut at all.

    ``""`` for anything that is not an address, so a template can guard with
    ``{% if href %}`` exactly as it does around :func:`safe_url`.
    """
    target = _mail_address(address)
    if target is None:
        return ""
    outreach = load_outreach_config()
    name = _company_name(company)
    subject = _quote_to_fit(outreach.email_subject.format(company=name), _MAILTO_SUBJECT_MAX)
    prefix = f"mailto:{target}?subject={subject}&body="
    return prefix + _quote_to_fit(
        outreach.email_body.format(company=name), MAILTO_MAX_URL - len(prefix)
    )


def shared_inbox(address: object) -> bool:
    """True when this address is a shared inbox rather than one person's own (SPEC §12 Phase 8).

    ``info@``/``jobs@`` is a ticket queue, and the scoped personal offer this app drafts converts
    close to zero when it lands in one; the panel says which kind an address is so the reader can
    weigh the door before spending a draft on it. **Display only** — nothing is hidden or dropped
    on the strength of this, per SPEC §7.1's rule that classification never excludes.

    The comparison is against the local part *whole*, not a prefix scan, because that is what the
    configured vocabulary is (``ingest.config.OutreachConfig._bare_local_parts`` rejects anything
    else): ``info@`` is a queue and ``information-security@`` is a team. A ``+tag`` suffix is cut
    first, since it routes to the same inbox it is a tag on.
    """
    if not isinstance(address, str):
        return False
    local, separator, _domain = address.strip().partition("@")
    if not separator:
        return False
    return local.partition("+")[0].casefold() in load_outreach_config().role_address_prefixes


def _mail_address(value: object) -> str | None:
    """``value`` percent-encoded for the path of a ``mailto:``, or ``None`` when it is not an
    address at all.

    Deliberately not a full address grammar — :func:`ingest.contacts.normalize_email` already
    applied one before the row was stored, and re-litigating it here would only mean the panel
    and the database disagreed about what a contact is. What is checked is what would break the
    *URL*: no ``@`` and there is nothing to send to, and internal whitespace (a newline most of
    all) is both impossible in an address and the classic way a header is injected into one.
    """
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if "@" not in candidate or any(character.isspace() for character in candidate):
        return None
    return quote(candidate, safe=_MAILTO_ADDRESS_SAFE)


def _company_name(value: object) -> str:
    """The name to interpolate into a draft: the string itself, or the ``name`` of a row."""
    if isinstance(value, str):
        return value
    name = getattr(value, "name", None)
    return name if isinstance(name, str) else ""


def _quote_to_fit(text: str, budget: int) -> str:
    """``text`` percent-encoded, cut to at most ``budget`` characters **of encoded output**.

    Encoded one character at a time once a cut is needed, which is what keeps the cut off the
    middle of an escape: slicing the encoded string instead can leave a trailing ``%E2`` that no
    client can decode, and the shipped drafts contain the punctuation that produces three-byte
    escapes. Per-character quoting concatenates to exactly what quoting the whole string gives,
    because :func:`urllib.parse.quote` encodes each character's UTF-8 bytes independently.
    """
    if budget <= 0:
        return ""
    encoded = quote(text)
    if len(encoded) <= budget:
        return encoded
    kept: list[str] = []
    used = 0
    for character in text:
        piece = quote(character)
        if used + len(piece) > budget:
            break
        kept.append(piece)
        used += len(piece)
    return "".join(kept)


def money(value: object) -> str:
    """``1_500_000 -> "$1.5M"``, ``900_000 -> "$900K"``, ``2_400_000_000 -> "$2.4B"``.

    ``None`` is an *undisclosed* amount (SPEC §5 allows ``funding_rounds.amount_usd`` to be
    NULL), not zero, so it renders as a dash. One decimal place, with a trailing ``.0``
    dropped, keeps a column of amounts scannable.
    """
    if not isinstance(value, int | float) or isinstance(value, bool):
        return EM_DASH
    sign = "-" if value < 0 else ""
    amount = abs(value)
    for unit, suffix in _MONEY_UNITS:
        if amount >= unit:
            scaled = f"{amount / unit:.1f}".removesuffix(".0")
            return f"{sign}${scaled}{suffix}"
    return f"{sign}${amount:,.0f}"


def ago(value: object) -> str:
    """A coarse relative age: ``"just now"``, ``"5h ago"``, ``"3d ago"``.

    Past 90 days the relative form stops being informative and :func:`day` takes over. A naive
    datetime is read as UTC — every timestamp in the database is ``timestamptz``, but a value
    that lost its tzinfo on the way here must not raise in a template.
    """
    if not isinstance(value, datetime):
        return EM_DASH
    moment = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    seconds = (datetime.now(UTC) - moment).total_seconds()
    if seconds < _SECONDS_PER_MINUTE:
        # Includes a slightly-future timestamp: a source's own clock, not something to shout about.
        return "just now"
    if seconds < _SECONDS_PER_HOUR:
        return f"{int(seconds // _SECONDS_PER_MINUTE)}m ago"
    if seconds < _SECONDS_PER_DAY:
        return f"{int(seconds // _SECONDS_PER_HOUR)}h ago"
    days = int(seconds // _SECONDS_PER_DAY)
    return f"{days}d ago" if days <= _AGO_MAX_DAYS else day(moment)


def day(value: object) -> str:
    """A ``date`` or ``datetime`` as ``"14 Feb 2026"``; ``None`` as a dash.

    Day-first and with a spelled-out month because the alternative (``02/14/26``) is ambiguous
    to half the world and this app renders dates from SEC filings and RSS feeds side by side.
    """
    if not isinstance(value, date):  # datetime is a date subclass
        return EM_DASH
    return f"{value.day} {value:%b %Y}"


def label(value: object) -> str:
    """Display label for an enum member or a raw enum value (see :mod:`web.labels`)."""
    return labels.label_for(value)


def duration(value: object, end: object = None) -> str:
    """``"1m 12s"`` for a run's elapsed time — ``/runs``.

    Accepts whichever shape the template has to hand: a number of seconds, a pair of datetimes,
    or a start datetime with the end passed as the filter argument
    (``{{ run.started_at | duration(run.finished_at) }}``). A run still in flight has no
    ``finished_at``, which is a dash rather than a made-up number.
    """
    seconds = _duration_seconds(value, end)
    if seconds is None:
        return EM_DASH
    total = max(0.0, seconds)
    if total < _SUB_SECOND_PRECISION_BELOW:
        return f"{total:.1f}s"
    if total < _SECONDS_PER_MINUTE:
        return f"{int(total)}s"
    minutes, remainder = divmod(int(total), _SECONDS_PER_MINUTE)
    if minutes < _SECONDS_PER_MINUTE:
        return f"{minutes}m {remainder}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def _duration_seconds(value: object, end: object) -> float | None:
    """Seconds between two instants, or ``None`` when the pair is incomplete."""
    if isinstance(value, tuple | list) and len(value) == 2:
        return _duration_seconds(value[0], value[1])
    if isinstance(value, datetime):
        if not isinstance(end, datetime):
            return None
        return (_as_utc(end) - _as_utc(value)).total_seconds()
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return None


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def query_string(request: Request, **overrides: Any) -> str:
    """This request's query string with ``overrides`` applied, e.g. ``"?city=<city>&sort=name"``.

    :func:`next_page_url` is the only caller: the "load more" URL is this request's URL with a
    new ``cursor``. Sort is deliberately *not* built here — it is a ``<select name="sort">``
    inside the filter form (``_filters.html``), so choosing one re-submits every active filter
    through the same GET the form already performs, and works with JavaScript off. It is also
    registered as the Jinja global ``qs`` for a link that needs to vary one parameter of the
    current request; no template needs one yet.

    Rules: an override of ``None`` removes the parameter, a list or tuple expands to repeated
    parameters (``city=<one>&city=<another>``), a bool becomes ``1``/``0``, everything else is
    ``str()``. Existing parameters keep their order and their repeats; overridden ones move to
    the end.

    ``cursor`` is **always dropped unless it is passed explicitly**. A cursor is a position in
    one specific ordering of one specific filter set (``db.queries.Cursor``), so carrying the
    current page's cursor into a link that changes the sort or a filter would either 400 or
    return a meaningless slice; every such change starts again at page 1. The pagination link
    passes ``cursor=`` back in on purpose.

    The result always begins with ``?``, so it concatenates onto a path — which is exactly what
    :func:`next_page_url` does with ``request.url.path`` — and never onto another query string.
    """
    replaced = set(overrides) | {CURSOR_PARAM}
    pairs = [
        (key, value) for key, value in request.query_params.multi_items() if key not in replaced
    ]
    for key, value in overrides.items():
        if value is None:
            continue
        if isinstance(value, list | tuple | set):
            pairs.extend((key, _param_value(item)) for item in value)
        else:
            pairs.append((key, _param_value(value)))
    return f"?{urlencode(pairs)}"


def next_page_url(request: Request, next_cursor: str | None) -> str | None:
    """The "load more" URL for a :class:`db.queries.Page`, or ``None`` on the last page.

    The current URL with ``cursor=`` swapped in, so every active filter and the sort travel to
    the next page unchanged — the keyset predicate is only valid for the query that minted it.
    """
    if next_cursor is None:
        return None
    return request.url.path + query_string(request, cursor=next_cursor)


def _param_value(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def site_name() -> str:
    """The application's own name: ``"<metro> Startup Tracker"``, or ``"Startup Tracker"``.

    Derived from ``config/regions.yaml`` rather than written out. SPEC §12 Phase 7 makes that
    file the only place region-specific logic may live, and a metro name baked into the page
    chrome is exactly the drift it forbids — with six regions configured the old title was also
    simply wrong. One enabled region names itself, because a tracker covering a single metro
    should say which one; two or more have no honest shared label, and inventing one ("US
    Startup Tracker") would be a claim the config does not make.

    Registered as a *callable* global rather than a precomputed string so a test can read the
    name against an alternate region file — a value baked into ``templates.env.globals`` at
    import time could not be swapped per case. It buys nothing at run time:
    :func:`ingest.config.load_regions_config` is cached on its path, so the file is parsed once
    per process and a page view costs a dict lookup rather than a YAML parse. An edit to
    ``config/regions.yaml`` under a running server therefore does *not* change the brand or the
    ``<title>``; a restart picks it up, exactly as it would with a constant.
    """
    metros = load_regions_config().metros
    return f"{metros[0]} Startup Tracker" if len(metros) == 1 else "Startup Tracker"


templates = Jinja2Templates(directory=TEMPLATES_DIR)
templates.env.filters["safe_url"] = safe_url
# The email door (SPEC §12 Phase 8). `mailto_url` takes the company alongside the address —
# `{{ contact.value | mailto_url(company) }}` — because the draft names the company in both the
# subject and the body, and `shared_inbox` is the hint rendered beside it.
templates.env.filters["mailto_url"] = mailto_url
templates.env.filters["shared_inbox"] = shared_inbox
templates.env.filters["money"] = money
templates.env.filters["ago"] = ago
templates.env.filters["day"] = day
templates.env.filters["label"] = label
templates.env.filters["duration"] = duration
# Kept as the extension point for a link that varies one parameter; no template needs one yet
# (sort is a form control, pagination is built in Python — see :func:`query_string`).
templates.env.globals["qs"] = query_string
# The <title> and the brand, in the one place the region name is allowed to come from (§12
# Phase 7). Called by the templates — `{{ site_name() }}` — not interpolated here.
templates.env.globals["site_name"] = site_name
# The cap the panel prints beside the drafted note ("… of 300 characters"). Exposed rather than
# written into the template because it is LinkedIn's limit, enforced in `ingest.config` when the
# pitch file loads and enforced again when the note is clamped — a third copy of the number in
# markup is the one that would go on claiming 300 after the other two had moved (§12 Phase 8).
templates.env.globals["LINKEDIN_NOTE_LIMIT"] = LINKEDIN_NOTE_LIMIT
# The filter selects need the enum vocabularies themselves, and Jinja cannot import a module.
templates.env.globals["ROLE_FAMILY_ORDER"] = labels.ROLE_FAMILY_ORDER
templates.env.globals["EMPLOYMENT_TYPE_ORDER"] = labels.EMPLOYMENT_TYPE_ORDER
templates.env.globals["SENIORITY_ORDER"] = labels.SENIORITY_ORDER
templates.env.globals["STAGE_ORDER"] = labels.STAGE_ORDER
templates.env.globals["ROUND_TYPE_ORDER"] = labels.ROUND_TYPE_ORDER
templates.env.globals["TRACKING_STATUS_ORDER"] = labels.TRACKING_STATUS_ORDER
