"""``benzene.auth`` — authentication as pipeline middleware.

Authentication in Benzene is an interception concern: a middleware verifies a credential before the
message router runs, attaches the authenticated :class:`Principal` to the context on success, and
short-circuits with ``Result.unauthorized`` on failure so the handler never sees an unauthenticated
call. That is the same short-circuit the core's health interceptor relies on — a middleware that does
not ``await next()`` ends the pipeline. This distribution (``benzene-auth``) adds the auth surface the
.NET port carries:

* **Basic** — ``basic_auth_interception`` decodes ``authorization: Basic ...`` and verifies it.
* **Bearer/OAuth2** — ``bearer_token_interception`` reads ``authorization: Bearer <token>`` and
  validates it; :class:`JwtValidator` decodes a JWT with PyJWT (the optional ``[jwt]`` extra, imported
  lazily), returning ``None`` for any invalid token rather than raising.
* **API Gateway authorizer** — ``api_gateway_authorizer`` adapts the *same* ``validate`` seam into an
  AWS Lambda custom authorizer handler that emits an Allow/Deny IAM policy.

Because :class:`~benzene.core.Context` has no auth slot and this package does not modify
``benzene-core``, the principal is stashed on the context via a private attribute — see
:mod:`~benzene.auth.principal`. Verifiers and validators may be sync or async, and none of them raise
for a bad credential: an unauthenticated caller is a ``Result``/``None``, never an exception.

Mirrors .NET's ``Benzene.Auth.Basic`` and ``Benzene.Auth.OAuth2``, plus
``Benzene.Aws.Lambda.ApiGateway.ApiGatewayCustomAuthorizer``. Contributes the ``benzene.auth``
subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .authorizer import api_gateway_authorizer
from .basic import BasicVerify, basic_auth_interception
from .bearer import (
    DEFAULT_PRINCIPAL_CLAIM,
    BearerValidate,
    Decoder,
    JwtValidator,
    bearer_token_interception,
    static_token_validator,
)
from .principal import Principal, get_principal, set_principal

__all__ = [
    # principal
    "Principal",
    "get_principal",
    "set_principal",
    # basic
    "BasicVerify",
    "basic_auth_interception",
    # bearer / oauth2
    "BearerValidate",
    "DEFAULT_PRINCIPAL_CLAIM",
    "Decoder",
    "JwtValidator",
    "bearer_token_interception",
    "static_token_validator",
    # aws api gateway authorizer
    "api_gateway_authorizer",
]
