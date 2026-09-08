"""The Jinja2 environment: one :class:`Jinja2Templates` instance and the filters and globals
every template shares (SPEC §9).

Autoescaping is on (Jinja2Templates' default), so *every* value interpolated into a template is
HTML-escaped. The one thing escaping cannot make safe is a URL in an ``href``: an escaped
``javascript:alert(1)`` still runs when clicked. Job and company URLs come from scraped
third-party pages (SPEC §4, Tiers 1 to 3), which makes that a real injection vector, so
:func:`safe_url` exists and **every externally sourced URL goes through it** before it reaches
an attribute.

Nothing here touches the database or the network; these are pure display helpers. The one thing
any of them reads off the disk is ``config/regions.yaml``, for :func:`site_name`.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from starlette.requests import Request
from starlette.templating import Jinja2Templates

# Importing ``ingest.config`` from ``web/`` is already established — ``web.routes.runs`` reads
# the connector cadences through it — and it does not put a handler one import away from an
# outbound call (SPEC §2): that module parses local YAML and nothing else. No httpx, no
# connector, no fetch; ``tests/test_web_acceptance.py`` exempts it from FORBIDDEN_IMPORTS on
# exactly those grounds.
from ingest.config import load_regions_config
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
# The filter selects need the enum vocabularies themselves, and Jinja cannot import a module.
templates.env.globals["ROLE_FAMILY_ORDER"] = labels.ROLE_FAMILY_ORDER
templates.env.globals["EMPLOYMENT_TYPE_ORDER"] = labels.EMPLOYMENT_TYPE_ORDER
templates.env.globals["SENIORITY_ORDER"] = labels.SENIORITY_ORDER
templates.env.globals["STAGE_ORDER"] = labels.STAGE_ORDER
templates.env.globals["ROUND_TYPE_ORDER"] = labels.ROUND_TYPE_ORDER
templates.env.globals["TRACKING_STATUS_ORDER"] = labels.TRACKING_STATUS_ORDER
