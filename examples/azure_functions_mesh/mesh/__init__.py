"""The discovery + interrogation + Blob-publishing aggregator — the seventh Function App.

``discovery_service.py`` holds the transport-agnostic discover -> interrogate -> publish pass
(``run_mesh_aggregation``); ``host.py`` builds the real Azure discovery + Blob store from the
environment; ``function_app.py`` is the Azure Functions Timer trigger entry point."""

from __future__ import annotations
