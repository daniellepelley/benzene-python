"""The AWS Lambda mesh example: six chained Cloud Service Lambdas + a discovering mesh Lambda.

See ``README.md`` for the full architecture; ``service/`` holds the six-domain composition root
(one Lambda image, ``SERVICE_NAME`` selects the domain — mirrors ``examples/k8s_mesh``'s
``MESH_SERVICE`` convention), ``mesh/`` holds the discovery + interrogation + S3-catalog aggregator.
"""

from __future__ import annotations
