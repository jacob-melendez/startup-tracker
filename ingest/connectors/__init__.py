"""Connector registry (SPEC §11).

Connectors are imported lazily so that importing the registry never imports every connector
module (and so this module has no import cycle with ``ingest.base``).

Registry order is the order ``cli.py refresh --all`` runs them in, and it follows
:data:`ingest.pipeline.CONNECTOR_PRIORITY` so the most authoritative source writes first and a
lower-priority connector never has to be trusted to fill a field the higher-priority one is
about to correct (SPEC §8). ``hn_hiring`` is the one connector placed by something other than
its priority: it is unlisted in ``CONNECTOR_PRIORITY`` (and so ranks below everything), but it
*discovers* companies, so it runs before the two connectors that can only enrich existing
ones — ``funding_rss`` and ``company_site``.

The **seed loader is deliberately absent**: it is a bootstrap, not a scheduled refresh, so
``--all`` must not re-validate 55 domains every night. ``cli.py seed`` builds
:class:`ingest.seed.SeedConnector` directly. ``product_hunt`` and ``opencorporates`` have a
block in ``config/connectors.yaml`` but no implementation yet (SPEC §4 Tier 2 #6, #7).
"""

from __future__ import annotations

from typing import Any

from ingest.base import Connector


def all_connectors() -> dict[str, type[Connector[Any]]]:
    """Every implemented connector, keyed by ``Connector.name``, in priority order."""
    from ingest.connectors.ashby import AshbyConnector
    from ingest.connectors.company_site import CompanySiteConnector
    from ingest.connectors.funding_rss import FundingRssConnector
    from ingest.connectors.greenhouse import GreenhouseConnector
    from ingest.connectors.hn_hiring import HnHiringConnector
    from ingest.connectors.lever import LeverConnector
    from ingest.connectors.sec_edgar import SecEdgarConnector
    from ingest.connectors.workable import WorkableConnector
    from ingest.connectors.ycombinator import YCombinatorConnector

    connectors: list[type[Connector[Any]]] = [
        SecEdgarConnector,
        YCombinatorConnector,
        GreenhouseConnector,
        LeverConnector,
        AshbyConnector,
        WorkableConnector,
        HnHiringConnector,
        FundingRssConnector,
        CompanySiteConnector,
    ]
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
