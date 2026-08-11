"""HTTP Basic authentication middleware (mirrors ``Benzene.Auth.Basic``).

Reads the ``authorization`` header (``Context.headers`` is already lower-cased by the core), expects
``Basic base64(username:password)``, decodes it, and calls a caller-supplied ``verify(username,
password)``. Verification may be sync or async — exactly like ``benzene.core.health`` handles a check
that returns either a value or an awaitable — and may answer with a bool, a :class:`Principal`, or
``None``.

Authentication is interception: a good credential attaches the principal and falls through to the
rest of the pipeline; a missing, malformed, or rejected credential short-circuits with
``Result.unauthorized(...)`` and *never* calls ``next()``, so the handler does not run. Nothing here
raises for a bad credential — an unauthenticated caller is a domain outcome, not an exception.
"""

from __future__ import annotations

import base64
import binascii
import inspect
from collections.abc import Awaitable, Callable

from benzene.core import Context, Middleware, Next
from benzene.results import Result

from .principal import Principal, set_principal

#: A Basic-auth verifier: given a username and password, answer with a principal, a bool, or ``None``.
#: ``True`` authenticates as ``Principal(username)``; ``False``/``None`` rejects; a :class:`Principal`
#: is attached as-is. Sync or async.
BasicVerify = Callable[
    [str, str],
    "bool | Principal | None | Awaitable[bool | Principal | None]",
]


def basic_auth_interception(verify: BasicVerify, *, realm: str = "benzene") -> Middleware:
    """Middleware authenticating callers from the ``authorization: Basic ...`` header.

    On a valid credential the resolved :class:`Principal` is attached to the context (via
    :func:`~benzene.auth.set_principal`) and the pipeline continues. A missing, malformed, or rejected
    credential short-circuits with ``Result.unauthorized`` — naming the ``realm`` — and the handler is
    never reached. Install it ahead of the message router. Mirrors ``Benzene.Auth.Basic``.
    """

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        credentials = _read_basic_credentials(context.headers.get("authorization"))
        if credentials is None:
            context.result = Result.unauthorized(
                f"missing or malformed Basic credentials ({realm})"
            )
            return  # short-circuit: no credential to verify

        username, password = credentials
        outcome = verify(username, password)
        if inspect.isawaitable(outcome):
            outcome = await outcome

        principal = _resolve_principal(outcome, username)
        if principal is None:
            context.result = Result.unauthorized(f"invalid credentials ({realm})")
            return  # short-circuit: verification failed

        set_principal(context, principal)
        await next()

    return middleware


def _read_basic_credentials(header: str | None) -> tuple[str, str] | None:
    """Parse ``Basic base64(user:pass)`` into ``(user, pass)``, or ``None`` if absent/malformed."""
    if not header:
        return None
    scheme, _, encoded = header.partition(" ")
    if scheme.lower() != "basic" or not encoded:
        return None
    try:
        decoded = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None
    if ":" not in decoded:
        return None
    username, _, password = decoded.partition(":")
    return username, password


def _resolve_principal(outcome: bool | Principal | None, username: str) -> Principal | None:
    """Coerce a verifier outcome into a principal: ``True`` → ``Principal(username)``, else as given."""
    if isinstance(outcome, Principal):
        return outcome
    if outcome is True:
        return Principal(username)
    return None
