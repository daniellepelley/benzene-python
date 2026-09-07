"""A multi-service Benzene mesh, in one process — the shape of the AWS mesh demo.

Three services call each other (``orders`` → ``inventory`` → ``notifications``) with W3C trace context
propagating across the hops, and each reports to a shared :class:`~benzene.mesh.MeshCollector`. Each
service declares what it produces at registration, so the **provider-edge topology** exists before a
single call is made (mesh.md §4); the trace context propagating across the hops then feeds those
declared edges' invocation/error stats — exactly what a deployed mesh shows, here with no cloud so it
runs as a test.

See ``deploy/mesh/README.md`` for how this fleet maps onto AWS (Lambda services + a Fargate
collector); the original plan is archived at ``work/archive/mesh-aws-plan.md``.
"""

from __future__ import annotations

from .fleet import Fleet, build_fleet

__all__ = ["Fleet", "build_fleet"]
