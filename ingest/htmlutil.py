"""Small HTML helpers shared by the connectors that read markup (SPEC §3 ``selectolax``).

Three connectors need the same two things and nothing more: turn a job description's HTML into
readable plain text (the ATS boards and Hacker News), and read ``<meta>`` / ``<a>`` out of a
company's own page (``company_site``, and the careers-page board-token discovery in
``ingest/connectors/ats.py``). Keeping them here rather than in ``ingest/normalize.py`` — which
is about domain and name normalization and entity resolution (SPEC §8) — keeps that module's
subject intact; this file is a utility, not a fifth stage of the pipeline.

``selectolax`` is the spec's HTML parser (SPEC §3): it is a C parser that never raises on
malformed markup, which matters because every input here is somebody else's HTML.
"""

from __future__ import annotations

import html
import re
from collections.abc import Iterator

from selectolax.parser import HTMLParser

#: Elements whose text is markup furniture, never prose.
_INVISIBLE = ("script", "style", "noscript", "template", "svg")
_BLANK_RUN = re.compile(r"\n{3,}")
_SPACE_RUN = re.compile(r"[^\S\n]+")
#: Hacker News (and a few ATS fields) serve entity-escaped HTML rather than a document; a
#: paragraph break is ``<p>`` with no closing tag at all.
_BLOCK_BREAK = re.compile(r"(?i)<\s*(?:p|br|/p|/div|/li|/h[1-6])\s*/?>")
#: Markup that survived one pass, which means the input was HTML escaped *as data* — see
#: :func:`html_to_text`.
_RESIDUAL_MARKUP = re.compile(
    r"(?i)<\s*/?\s*(?:div|p|br|ul|ol|li|span|h[1-6]|strong|em|b|i|a|table|tr|td|img)\b"
)


def _extract(markup: str) -> str:
    """One parse: block tags to newlines, invisible elements dropped, entities unescaped."""
    tree = HTMLParser(_BLOCK_BREAK.sub("\n", markup))
    for node in tree.css(",".join(_INVISIBLE)):
        node.decompose()
    body = tree.body if tree.body is not None else tree.root
    raw = body.text(separator="") if body is not None else ""
    text = _SPACE_RUN.sub(" ", html.unescape(raw))
    return _BLANK_RUN.sub("\n\n", "\n".join(line.strip() for line in text.splitlines())).strip()


def html_to_text(markup: str | None, *, limit: int | None = None) -> str | None:
    """The visible text of ``markup`` as plain text, or ``None`` when there is none.

    Block-level tags become newlines so a bulleted job description does not collapse into one
    run-on line, ``<script>``/``<style>`` content is dropped, entities are unescaped, and runs
    of blank lines are squeezed. ``limit`` truncates the result (with an ellipsis) for callers
    that store a preview rather than the whole posting.

    Text that contains no markup at all is returned unescaped and otherwise untouched, so this
    is safe to call on a field that may or may not be HTML.

    **Two passes, at most.** Greenhouse serves a job's ``content`` entity-escaped in its
    entirety (``&lt;div&gt;…``), so unescaping after the parse leaves real tags behind in what
    is supposed to be plain text. When that happens the result is parsed once more. The second
    pass is bounded — never a loop — and a document that merely *mentions* a tag inside prose
    loses nothing, because parsing text with no elements returns that text unchanged.
    """
    if markup is None:
        return None
    text = _extract(markup)
    if text and _RESIDUAL_MARKUP.search(text):
        text = _extract(text) or text
    if not text:
        return None
    if limit is not None and len(text) > limit:
        return text[:limit].rstrip() + "…"
    return text


def meta_content(markup: str, *names: str) -> str | None:
    """The ``content`` of the first ``<meta>`` whose ``name`` or ``property`` is one of
    ``names`` (case-insensitive), e.g. ``meta_content(html, "og:description", "description")``.

    Order matters: the first *name* that is present anywhere in the document wins, not the
    first meta tag in document order, so a caller states its preference once.
    """
    tree = HTMLParser(markup)
    tags = tree.css("meta")
    for wanted in names:
        target = wanted.casefold()
        for tag in tags:
            attrs = tag.attributes
            key = (attrs.get("name") or attrs.get("property") or "").casefold()
            content = attrs.get("content")
            if key == target and content and content.strip():
                return html.unescape(content.strip())
    return None


def page_title(markup: str) -> str | None:
    """The document's ``<title>``, collapsed to one line."""
    tree = HTMLParser(markup)
    node = tree.css_first("title")
    if node is None:
        return None
    title = _SPACE_RUN.sub(" ", html.unescape(node.text())).strip()
    return title or None


def iter_links(markup: str) -> Iterator[str]:
    """Every non-empty ``href`` in document order, entities already unescaped."""
    for node in HTMLParser(markup).css("a"):
        href = node.attributes.get("href")
        if href and href.strip():
            yield html.unescape(href.strip())
