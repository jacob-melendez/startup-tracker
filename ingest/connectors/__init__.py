"""Connector registry (SPEC §11).

Connectors are imported lazily so that importing the registry never imports every connector
module (and so this module has no import cycle with ``ingest.base``).
"""

from __future__ import annotations

from typing import Any

from ingest.base import Connector


def all_connectors() -> dict[str, type[Connector[Any]]]:
    """Every implemented connector, keyed by ``Connector.name``."""
    from ingest.connectors.sec_edgar import SecEdgarConnector

    connectors: list[type[Connector[Any]]] = [SecEdgarConnector]
    return {cls.name: cls for cls in connectors}


def get_connector_class(name: str) -> type[Connector[Any]]:
    """Look a connector up by name; the error lists the known names."""
    connectors = all_connectors()
    try:
        return connectors[name]
    except KeyError:
        known = ", ".join(sorted(connectors))
        msg = f"unknown connector {name!r}; known connectors: {known}"
        raise KeyError(msg) from None
