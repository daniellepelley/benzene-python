"""Bearer-token authentication middleware and a lazy JWT validator (mirrors ``Benzene.Auth.OAuth2``).

Reads the ``authorization: Bearer <token>`` header, hands the token to a caller-supplied
``validate(token)``, and treats the answer as the caller's claims. Validation may be sync or async
(like ``benzene.core.health``'s check handling) and may return a claims ``dict``, a
:class:`Principal`, or ``None``/``False`` to reject.

:class:`JwtValidator` is a ready-made ``validate`` that decodes a JWT with PyJWT — imported *lazily*
inside the decode call so the package (and its tests) need no ``PyJWT`` unless the real validator is
used. An invalid, expired, or mis-audienced token makes it return ``None`` rather than raise, so a bad
token is an ``unauthorized`` outcome, never an exception. :func:`static_token_validator` builds a
``validate`` from an in-memory token→principal map — the seam the tests drive without any JWT library.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from benzene.core import Context, Middleware, Next
from benzene.results import Result

from .principal import Principal, set_principal

#: A bearer validator: given the raw token, answer with claims, a principal, or ``None``. A ``dict`` is
#: read as claims (its ``sub`` seeds the principal name); a :class:`Principal` is attached as-is;
#: ``None``/``False`` rejects. Sync or async.
BearerValidate = Callable[
    [str],
    "dict[str, Any] | Principal | None | bool | Awaitable[dict[str, Any] | Principal | None | bool]",
]

#: A synchronous JWT decode step — ``decode(token) -> claims`` — raising on any invalid token. The
#: seam :class:`JwtValidator` calls; injectable so tests supply a fake decoder without PyJWT.
Decoder = Callable[[str], "dict[str, Any]"]

#: The claim :class:`JwtValidator` reads for the principal name when none is configured.
DEFAULT_PRINCIPAL_CLAIM = "sub"


def bearer_token_interception(validate: BearerValidate, *, scheme: str = "Bearer") -> Middleware:
    """Middleware authenticating callers from the ``authorization: Bearer <token>`` header.

    A token ``validate`` accepts attaches the resolved :class:`Principal` and continues the pipeline;
    a missing/malformed header or a rejected token short-circuits with ``Result.unauthorized`` and the
    handler never runs. ``scheme`` is the credential scheme to accept (default ``Bearer``, matched
    case-insensitively). Mirrors ``Benzene.Auth.OAuth2``.
    """

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        token = _read_bearer_token(context.headers.get("authorization"), scheme)
        if token is None:
            context.result = Result.unauthorized(f"missing or malformed {scheme} token")
            return  # short-circuit: no token to validate

        outcome = validate(token)
        if inspect.isawaitable(outcome):
            outcome = await outcome

        principal = _resolve_principal(outcome)
        if principal is None:
            context.result = Result.unauthorized("invalid token")
            return  # short-circuit: validation failed

        set_principal(context, principal)
        await next()

    return middleware


def _read_bearer_token(header: str | None, scheme: str) -> str | None:
    """Parse ``<scheme> <token>`` into the token, or ``None`` if absent or the scheme mismatches."""
    if not header:
        return None
    prefix, _, token = header.partition(" ")
    if prefix.lower() != scheme.lower() or not token.strip():
        return None
    return token.strip()


def _resolve_principal(outcome: dict[str, Any] | Principal | None | bool) -> Principal | None:
    """Coerce a validator outcome into a principal: claims ``dict`` → ``Principal(sub, claims)``."""
    if isinstance(outcome, Principal):
        return outcome
    if isinstance(outcome, Mapping):
        claims = dict(outcome)
        return Principal(str(claims.get(DEFAULT_PRINCIPAL_CLAIM, "")), claims)
    return None


class JwtValidator:
    """A bearer ``validate`` that decodes and verifies a JWT with PyJWT, returning claims or ``None``.

    Configure it with the verification ``key`` (an HMAC secret or an RSA/EC public key), the accepted
    ``algorithms``, and optional ``audience``/``issuer`` constraints; ``principal_claim`` names the
    claim used for :attr:`Principal.name` (default ``sub``). PyJWT is imported lazily on first decode,
    so importing this class costs nothing and the ``[jwt]`` extra is only needed to actually validate.

    Any token PyJWT rejects — bad signature, expired, wrong audience/issuer, malformed — yields
    ``None`` rather than an exception, so it drops straight into ``unauthorized``. Pass ``decode`` to
    inject a decoder (the tests do this to exercise the validator without installing PyJWT). Mirrors
    ``Benzene.Auth.OAuth2``'s JWT bearer handler.
    """

    def __init__(
        self,
        *,
        key: Any = None,
        algorithms: tuple[str, ...] = ("HS256",),
        audience: str | None = None,
        issuer: str | None = None,
        principal_claim: str = DEFAULT_PRINCIPAL_CLAIM,
        decode: Decoder | None = None,
    ) -> None:
        self._key = key
        self._algorithms = list(algorithms)
        self._audience = audience
        self._issuer = issuer
        self._principal_claim = principal_claim
        self._decode = decode

    def __call__(self, token: str) -> Principal | None:
        return self.validate(token)

    def validate(self, token: str) -> Principal | None:
        """Decode ``token`` and build a :class:`Principal`, or return ``None`` for any invalid token."""
        decoder = self._decode if self._decode is not None else self._decode_with_pyjwt
        try:
            claims = decoder(token)
        except ImportError:
            raise  # a missing PyJWT is a deployment error, not a token outcome — surface it
        except Exception:
            return None  # any token PyJWT (or the injected decoder) rejects → unauthenticated
        name = str(claims.get(self._principal_claim, ""))
        return Principal(name, dict(claims))

    def _decode_with_pyjwt(self, token: str) -> dict[str, Any]:
        import jwt  # optional [jwt] extra — imported lazily so the package works without PyJWT

        kwargs: dict[str, Any] = {"algorithms": self._algorithms}
        if self._audience is not None:
            kwargs["audience"] = self._audience
        if self._issuer is not None:
            kwargs["issuer"] = self._issuer
        decoded: dict[str, Any] = jwt.decode(token, self._key, **kwargs)
        return decoded


def static_token_validator(
    tokens: Mapping[str, Principal | dict[str, Any]],
) -> Callable[[str], Principal | None]:
    """Build a ``validate`` from a static ``token → principal|claims`` map (a test seam, no JWT).

    Each mapped value may be a :class:`Principal` (attached as-is) or a claims ``dict`` (wrapped like
    :func:`bearer_token_interception` wraps a validator's dict). An unmapped token returns ``None``.
    """

    def validate(token: str) -> Principal | None:
        entry = tokens.get(token)
        if entry is None:
            return None
        return _resolve_principal(entry)

    return validate
