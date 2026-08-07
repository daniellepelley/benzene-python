"""A multi-service Benzene mesh, in one process — the shape of the AWS mesh demo.

Three services call each other (``orders`` → ``inventory`` → ``notifications``) with W3C trace context
propagating across the hops, and each reports to a shared :class:`~benzene.mesh.MeshCollector`. The
collector then derives the fleet and the **consumer-edge topology** from the trace parentage — exactly
what a deployed mesh shows, here with no cloud so it runs as a test.

See ``docs/mesh-aws-plan.md`` for how this fleet maps onto AWS (Lambda services + a Fargate collector).
"""

from __future__ import annotations

from .fleet import Fleet, build_fleet

__all__ = ["Fleet", "build_fleet"]
