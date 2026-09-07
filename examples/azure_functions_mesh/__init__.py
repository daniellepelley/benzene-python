"""The Azure Functions mesh example: six chained Cloud Service Azure Functions + a discovering mesh
Azure Function.

See ``README.md`` for the full architecture; ``service/`` holds the six-domain composition root (one
Function App image, ``SERVICE_NAME`` selects the domain — mirrors ``examples/aws_lambda_mesh``'s
``SERVICE_NAME`` convention), ``mesh/`` holds the discovery + interrogation + Blob-catalog aggregator.
"""

from __future__ import annotations
