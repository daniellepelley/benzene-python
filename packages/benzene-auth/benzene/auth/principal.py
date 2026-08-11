"""The authenticated principal and its attachment to a :class:`~benzene.core.Context`.

An authentication middleware verifies a credential and, on success, records *who* the caller is so
downstream handlers (and authorization checks) can read it. .NET carries this on the ambient
``ClaimsPrincipal`` / ``HttpContext.User``; here the equivalent is a small :class:`Principal` stashed
on the context.

:class:`~benzene.core.Context` has no auth slot and this package must not modify ``benzene-core``, so
the principal rides on a private attribute (``_benzene_auth_principal``) written with ``setattr`` and
read with ``getattr``. The context is typed :data:`Any` at the seam so mypy stays clean without a
core-side field. :func:`get_principal` returns ``None`` for an unauthenticated context — the absence
of the attribute and an explicit anonymous state are the same thing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: The private context attribute the principal is stashed under (see the module docstring).
_PRINCIPAL_ATTR = "_benzene_auth_principal"


@dataclass(frozen=True)
class Principal:
    """The authenticated caller: a ``name`` plus arbitrary ``claims``.

    Mirrors .NET's ``ClaimsPrincipal`` in miniature — ``name`` is the primary identity (the Basic
    username, or the ``sub`` / configured principal claim of a bearer token) and ``claims`` carries
    the rest of the decoded token or verification payload. Frozen, so a principal handed downstream
    cannot be mutated out from under the middleware that set it.
    """

    name: str
    claims: dict[str, Any] = field(default_factory=dict)

    def claim(self, key: str, default: Any = None) -> Any:
        """Read a single claim by name, or ``default`` when it is absent."""
        return self.claims.get(key, default)


def set_principal(context: Any, principal: Principal) -> None:
    """Attach ``principal`` to ``context`` (see the module docstring on the private-attribute stash)."""
    setattr(context, _PRINCIPAL_ATTR, principal)


def get_principal(context: Any) -> Principal | None:
    """Return the principal attached to ``context``, or ``None`` when the caller is unauthenticated."""
    return getattr(context, _PRINCIPAL_ATTR, None)
