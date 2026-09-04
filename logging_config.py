"""structlog configuration — JSON output by default (SPEC §3).

Call :func:`configure_logging` once per process (Alembic env, CLI, scheduler, web app).
Standard-library loggers (alembic, sqlalchemy, uvicorn, ...) are routed through the same
processor chain, so every line has the same shape. Logs go to **stderr** so that stdout stays
clean for program output (``alembic upgrade head --sql``, CLI reports).
"""

from __future__ import annotations

import logging
import sys

import structlog
from structlog.typing import Processor


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    renderer: Processor
    formatter_processors: list[Processor] = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta
    ]
    if json_output:
        formatter_processors.append(structlog.processors.format_exc_info)
        renderer = structlog.processors.JSONRenderer()
    else:
        # Plain Python tracebacks, not Rich's. ConsoleRenderer defaults to
        # RichTracebackFormatter(show_locals=True) whenever ``rich`` is importable, which turns
        # one ``log.exception`` into a boxed panel per frame with a locals table — a refused
        # database connection rendered ~1,200 lines of stderr, burying the message, and the
        # ingest pipeline logs one exception per failed record (SPEC §7.2).
        # Colours only when stderr is a terminal: piped or redirected logs stay free of ANSI
        # escapes, so `make refresh 2> run.log` and `grep` see plain key=value text.
        renderer = structlog.dev.ConsoleRenderer(
            colors=sys.stderr.isatty(),
            exception_formatter=structlog.dev.plain_traceback,
        )
    formatter_processors.append(renderer)

    structlog.configure(
        processors=[*shared_processors, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processors=formatter_processors,
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]
