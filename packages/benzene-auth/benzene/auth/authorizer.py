"""AWS API Gateway custom (Lambda) authorizer adapter.

Mirrors ``Benzene.Aws.Lambda.ApiGateway.ApiGatewayCustomAuthorizer``: it adapts the same
``validate(token)`` seam the :mod:`~benzene.auth.bearer` middleware uses into the shape API Gateway's
custom authorizer expects — a Lambda ``handler(event, context)`` that returns an IAM policy document
allowing or denying ``execute-api:Invoke`` on the invoked method ARN.

The token is pulled from either authorizer flavour: a TOKEN authorizer passes it in
``event["authorizationToken"]`` (usually ``Bearer <token>``); a REQUEST authorizer passes the request,
so the token is read from the ``authorization`` header. A token ``validate`` accepts yields an *Allow*
policy (with the decoded claims echoed in the authorizer ``context``); anything else yields *Deny*.
``validate`` may be sync or async. Nothing raises for a rejected token — an unauthenticated caller is
a Deny policy, not an exception.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from .bearer import DEFAULT_PRINCIPAL_CLAIM, _resolve_principal
from .principal import Principal

#: Same contract as :data:`~benzene.auth.bearer.BearerValidate` — the adapter reuses one seam.
AuthorizerValidate = Callable[
    [str],
    "dict[str, Any] | Principal | None | bool | Awaitable[dict[str, Any] | Principal | None | bool]",
]


def api_gateway_authorizer(
    validate: AuthorizerValidate,
    *,
    principal_id_claim: str = DEFAULT_PRINCIPAL_CLAIM,
    scheme: str = "Bearer",
) -> Callable[..., dict[str, Any]]:
    """Adapt ``validate`` into an API Gateway custom-authorizer Lambda handler.

    Returns ``handler(event, context=None) -> dict``: it extracts the token (``authorizationToken``
    for a TOKEN authorizer, or the ``authorization`` header for a REQUEST authorizer), validates it,
    and returns an IAM policy — *Allow* on success with the claims echoed under ``context``, *Deny*
    otherwise — scoped to ``event["methodArn"]``. ``principal_id_claim`` selects the claim used for the
    policy's ``principalId``. Mirrors ``ApiGatewayCustomAuthorizer``.
    """

    def handler(event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
        method_arn = event.get("methodArn") or event.get("routeArn") or "*"
        token = _extract_token(event, scheme)
        principal = _validate(validate, token) if token is not None else None
        if principal is None:
            return _policy("unauthorized", "Deny", method_arn, {})
        principal_id = str(principal.claim(principal_id_claim, principal.name) or principal.name)
        return _policy(principal_id, "Allow", method_arn, _auth_context(principal.claims))

    return handler


async def _await(awaitable: Awaitable[Any]) -> Any:
    return await awaitable


def _validate(validate: AuthorizerValidate, token: str) -> Principal | None:
    """Run ``validate`` (awaiting an async one) and coerce the outcome into a principal or ``None``."""
    outcome = validate(token)
    if inspect.isawaitable(outcome):
        outcome = asyncio.run(_await(outcome))
    return _resolve_principal(outcome)


def _extract_token(event: Mapping[str, Any], scheme: str) -> str | None:
    """Read the token from a TOKEN (``authorizationToken``) or REQUEST (header) authorizer event."""
    raw = event.get("authorizationToken")
    if not raw:
        headers = {str(k).lower(): v for k, v in (event.get("headers") or {}).items()}
        raw = headers.get("authorization")
    if not raw:
        return None
    prefix, sep, rest = str(raw).partition(" ")
    if sep and prefix.lower() == scheme.lower():
        return rest.strip() or None
    return str(raw).strip() or None


def _auth_context(claims: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten claims into the authorizer ``context`` (values must be primitives, so stringify others)."""
    context: dict[str, Any] = {}
    for key, value in claims.items():
        context[str(key)] = value if isinstance(value, (str, int, float, bool)) else str(value)
    return context


def _policy(
    principal_id: str, effect: str, resource: str, context: dict[str, Any]
) -> dict[str, Any]:
    """Build the API Gateway authorizer response — an IAM policy document plus the auth context."""
    return {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": effect,
                    "Resource": resource,
                }
            ],
        },
        "context": context,
    }
