"""``logging_config`` — the structlog setup every process shares (SPEC §3: structlog, JSON).

Exceptions are the concern here. Console mode (``LOG_JSON=false``) must render a
``log.exception`` as a plain Python traceback: structlog's ``ConsoleRenderer`` defaults to
Rich's formatter with a locals table per frame whenever ``rich`` is importable, which turned one
refused database connection into ~1,200 lines of stderr. JSON mode must keep the traceback
inside the one-line record.

``configure_logging`` replaces the root logger's handlers — pytest's own log capture lives
there too — so every test snapshots and restores them, and resets structlog's global config.
"""

from __future__ import annotations

import io
import json
import logging
import sys
import traceback
from collections.abc import Callable, Iterator

import pytest
import structlog

from logging_config import configure_logging, get_logger

# Frames between the log call and the raise: enough that per-frame panels would dominate.
DEPTH = 10


@pytest.fixture(autouse=True)
def restore_root_logger() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    try:
        yield
    finally:
        for handler in root.handlers[:]:
            if handler not in handlers:
                root.removeHandler(handler)
                handler.close()
        for handler in handlers:
            if handler not in root.handlers:
                root.addHandler(handler)
        root.setLevel(level)
        structlog.reset_defaults()


def descend(depth: int) -> None:
    """Raise ``depth`` frames down; every frame holds a local a locals panel would print."""
    payload = list(range(depth))
    if depth == 0:
        raise RuntimeError(f"boom {payload}")
    descend(depth - 1)


def via_structlog() -> None:
    """The path ``cli.py`` and ``ingest/pipeline.py`` take (``log.exception(..., key=value)``)."""
    get_logger("tests.logging_config").exception("upsert failed", item=3)


def via_stdlib() -> None:
    """A foreign logger (alembic, sqlalchemy, ...) routed through the same formatter."""
    logging.getLogger("tests.logging_config.stdlib").exception("upsert failed")


def render(json_output: bool, emit: Callable[[], None]) -> tuple[str, str]:
    """Configure logging, log one exception raised ``DEPTH`` frames deep through ``emit``, and
    return what the handler wrote plus the stdlib's own rendering of that traceback."""
    configure_logging("INFO", json_output=json_output)
    (handler,) = logging.getLogger().handlers
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stderr  # stdout stays clean for program output
    buffer = io.StringIO()
    handler.setStream(buffer)
    try:
        descend(DEPTH)
    except RuntimeError as exc:
        expected = "".join(traceback.format_exception(exc)).rstrip("\n")
        emit()
    else:
        pytest.fail("descend() must raise")
    return buffer.getvalue(), expected


@pytest.mark.parametrize("emit", [via_structlog, via_stdlib], ids=["structlog", "stdlib"])
def test_console_exception_is_a_plain_python_traceback(emit: Callable[[], None]) -> None:
    out, expected = render(json_output=False, emit=emit)

    first, *rest = out.splitlines()
    assert "upsert failed" in first
    if emit is via_structlog:
        assert "item=3" in first
    # Exactly what Python itself would print, straight after the log line and nothing more.
    assert out.endswith("\n" + expected + "\n")
    assert len(rest) == len(expected.splitlines())
    assert "Traceback (most recent call last):" in expected
    assert expected.endswith(f"RuntimeError: boom {list(range(0))}")
    # No Rich panels: no box drawing, no per-frame locals table.
    assert "locals" not in out
    assert not any(char in out for char in "╭╰│❱")


def test_json_exception_stays_a_single_line_record() -> None:
    out, expected = render(json_output=True, emit=via_structlog)

    (line,) = out.splitlines()
    record = json.loads(line)
    assert record["event"] == "upsert failed"
    assert record["item"] == 3
    assert record["level"] == "error"
    assert record["logger"] == "tests.logging_config"
    assert record["exception"] == expected
