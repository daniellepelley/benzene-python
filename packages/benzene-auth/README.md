# benzene-auth

Authentication middleware for [Benzene Python](https://github.com/daniellepelley/benzene-python) —
**Basic** auth, **JWT/OAuth2 bearer-token** validation (against a static key *or* an identity
provider's rotating **JWKS**), and an AWS **API Gateway custom authorizer** adapter. Depends only on
`benzene-core` (PyJWT is an optional extra, only for the real JWT decode).

```bash
pip install benzene-auth         # middleware only
pip install benzene-auth[jwt]    # + PyJWT, for JwtValidator's real decode (HMAC)
pip install benzene-auth[jwks]   # + PyJWT[crypto], for JwksValidator against a real IdP
```

Authentication is an interception concern, like the core's health endpoint: a middleware verifies the
credential ahead of the message router, attaches the authenticated `Principal` to the context on
success, and short-circuits with `Result.unauthorized` on failure — a middleware that does not
`await next()` ends the pipeline, so the handler never sees an unauthenticated call. Verifiers and
validators may be sync or async, and none of them raise for a bad credential.

```python
from benzene.auth import (
    basic_auth_interception,
    bearer_token_interception,
    JwtValidator,
    api_gateway_authorizer,
    get_principal,
)


# Basic auth — verify(username, password) → bool | Principal | None.
def verify(username: str, password: str) -> bool:
    return password == secrets.get(username)


definition.middleware += [basic_auth_interception(verify, realm="orders")]

# Bearer/OAuth2 — a validator that decodes a JWT (None on any invalid token, never raises).
definition.middleware += [
    bearer_token_interception(
        JwtValidator(key=signing_secret, algorithms=("HS256",), audience="orders-api")
    )
]

# Downstream, read who the caller is:
principal = get_principal(context)  # None when unauthenticated
```

```python
# JWKS / OIDC discovery — key-rotation-aware validation against an identity provider. The key is
# resolved per token by the header's `kid` from the IdP's cached JWKS, so a rotation is a non-event.
from benzene.auth import JwksValidator

definition.middleware += [
    bearer_token_interception(
        JwksValidator(issuer="https://login.example.com/", audience="orders-api", algorithms=("RS256",))
    )
]
```

```python
# AWS API Gateway custom authorizer — adapts the same validate seam into a Lambda handler
# emitting an Allow/Deny IAM policy scoped to the invoked methodArn.
handler = api_gateway_authorizer(JwtValidator(key=signing_secret))
```

- **Basic** — `basic_auth_interception(verify, *, realm=...)` decodes `authorization: Basic
  base64(user:pass)` and calls `verify`; `True` authenticates as `Principal(username)`, a `Principal`
  is attached as-is, `False`/`None` (or a missing/malformed header) rejects with `unauthorized`.
- **Bearer/OAuth2** — `bearer_token_interception(validate, *, scheme="Bearer")` reads the bearer token
  and calls `validate(token) → claims | Principal | None`. `JwtValidator` is a ready-made validator
  that decodes a JWT with PyJWT (imported lazily), constrained by key, algorithms, audience, and
  issuer, returning `None` for any token it rejects. `static_token_validator({token: principal})`
  builds an in-memory validator for tests.
- **JWKS / OIDC discovery** — `JwksValidator(issuer=..., algorithms=(...), audience=...)` resolves the
  verification key per token by the header's `kid` from the IdP's JWKS (found via
  `/.well-known/openid-configuration`, or set directly with `jwks_uri=`), so a rotated signing key is
  picked up instead of breaking the service. Its refresh policy is deliberately bounded — an unknown
  `kid` buys **at most one** refetch per `min_refresh_interval` (300s), single-flighted, keeping the
  last known good keys when the IdP is unreachable — so nobody can spray random `kid`s to drive
  unbounded outbound requests. The `algorithms` allowlist is the only source of algorithm truth: the
  header's `alg` is checked before any key is resolved, a JWKS entry declaring an `alg` is usable for
  that algorithm only, and the decoder is handed exactly one allowlisted algorithm (RFC 8725 §3.1).
  Metadata must be HTTPS unless `require_https=False`.
- **API Gateway authorizer** — `api_gateway_authorizer(validate, *, principal_id_claim=...)` returns a
  Lambda `handler(event, context=None) → dict`. It pulls the token from `authorizationToken` (TOKEN
  authorizer) or the `authorization` header (REQUEST authorizer), and returns an IAM policy document
  allowing or denying `execute-api:Invoke` on `event["methodArn"]`, echoing the claims under
  `context`.

Nothing here needs PyJWT installed to run or test: `JwtValidator` and `JwksValidator` accept an
injected `decode` function (and `JwksValidator` an injected `fetch` and `clock`, so key rotation and
cache expiry are tested without a socket or a sleep), and `static_token_validator` needs no JWT
library at all. Mirrors .NET's `Benzene.Auth.Basic`
and `Benzene.Auth.OAuth2`, plus `Benzene.Aws.Lambda.ApiGateway.ApiGatewayCustomAuthorizer`, and
contributes the `benzene.auth` subpackage to the shared `benzene` namespace.
