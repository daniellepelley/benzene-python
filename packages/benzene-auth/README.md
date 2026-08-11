# benzene-auth

Authentication middleware for [Benzene Python](https://github.com/daniellepelley/benzene-python) —
**Basic** auth, **JWT/OAuth2 bearer-token** validation, and an AWS **API Gateway custom authorizer**
adapter. Depends only on `benzene-core` (PyJWT is an optional extra, only for the real JWT decode).

```bash
pip install benzene-auth        # middleware only
pip install benzene-auth[jwt]   # + PyJWT, for JwtValidator's real decode
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
- **API Gateway authorizer** — `api_gateway_authorizer(validate, *, principal_id_claim=...)` returns a
  Lambda `handler(event, context=None) → dict`. It pulls the token from `authorizationToken` (TOKEN
  authorizer) or the `authorization` header (REQUEST authorizer), and returns an IAM policy document
  allowing or denying `execute-api:Invoke` on `event["methodArn"]`, echoing the claims under
  `context`.

Nothing here needs PyJWT installed to run or test: `JwtValidator` accepts an injected `decode`
function and `static_token_validator` needs no JWT library at all. Mirrors .NET's `Benzene.Auth.Basic`
and `Benzene.Auth.OAuth2`, plus `Benzene.Aws.Lambda.ApiGateway.ApiGatewayCustomAuthorizer`, and
contributes the `benzene.auth` subpackage to the shared `benzene` namespace.
