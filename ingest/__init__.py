"""Ingestion layer: connectors, the shared HTTP client, entity resolution, and the pipeline.

Batch-only (SPEC §2, CLAUDE.md): everything in this package runs from scheduled jobs or the
CLI. No web request handler imports it.
"""
