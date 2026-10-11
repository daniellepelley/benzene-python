"""Outbound declarations — moved to :mod:`benzene.core.declarations`.

The events a service publishes and the sends it makes are part of its Contract Document
(contract-document §3), which ``benzene.core`` derives without the mesh module, so the registry lives
there now. These names are re-exported so ``from benzene.mesh import OutboundRegistry`` keeps working.
A collector reads both kinds off the registered document as provider edges (mesh.md §4).
"""

from __future__ import annotations

from benzene.core.declarations import (
    DuplicateOutboundRegistrationError,
    EventDeclaration,
    OutboundRegistry,
    SendDeclaration,
)

__all__ = [
    "DuplicateOutboundRegistrationError",
    "EventDeclaration",
    "OutboundRegistry",
    "SendDeclaration",
]
