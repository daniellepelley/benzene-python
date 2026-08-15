"""Kubernetes mesh self-discovery example — the Python counterpart of .NET's ``examples/K8sMesh``.

Three Benzene domain services (orders/payments/shipping, one shared image selected by ``MESH_SERVICE``)
running as Kubernetes pods, chaining to each other over HTTP, plus a mesh service that discovers them by
label via the Kubernetes API, interrogates each in-cluster, and serves the Mesh UI. See ``README.md``
for the full architecture.
"""

from __future__ import annotations
