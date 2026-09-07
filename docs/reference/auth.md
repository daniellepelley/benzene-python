# `benzene.auth`

Authentication as pipeline middleware — **Basic** auth, **JWT/OAuth2 bearer** validation (against a
static key *or* an identity provider's rotating **JWKS**), and an AWS **API Gateway custom
authorizer** adapter. **Distribution: `benzene-auth` (depends only on `benzene-core`; PyJWT is an
optional extra).**

```bash
pip install benzene-auth          # middleware only
pip install "benzene-auth[jwt]"   # + PyJWT, for JwtValidator's real decode (HMAC)
pip install "benzene-auth[jwks]"  # + PyJWT[crypto], for JwksValidator against a real IdP
```

## Overview

Authentication in Benzene is an **interception** concern, exactly like the core's health endpoint: a
middleware verifies the credential ahead of the message router, attaches the authenticated `Principal`
to the context on success, and short-circuits with `Result.unauthorized(...)` on failure. That
short-circuit is the same one every interceptor uses — a middleware that does not `await next()` ends
the pipeline — so the handler never sees an unauthenticated call.

Verifiers and validators may be **sync or async** (the package awaits either, like the core's health
checks), and none of them **raise** for a bad credential: an unauthenticated caller is a
`Result`/`None`, never an exception. A missing or malformed header is treated the same as a rejected
credential.

The package mirrors .NET's `Benzene.Auth.Basic` and `Benzene.Auth.OAuth2`, plus
`Benzene.Aws.Lambda.ApiGateway.ApiGatewayCustomAuthorizer`. Nothing here needs PyJWT installed to run
or test — `JwtValidator` and `JwksValidator` both accept an injected decoder (and `JwksValidator` an
injected fetcher and clock), and `static_token_validator` needs no JWT library at all.

## The principal

`Principal` is the authenticated caller — a `name` plus arbitrary `claims`. It mirrors .NET's
`ClaimsPrincipal` in miniature: `name` is the primary identity (the Basic username, or the `sub` /
configured claim of a bearer token) and `claims` carries the rest of the decoded token. It is frozen,
so a principal handed downstream can't be mutated out from under the middleware that set it.

```python
from benzene.auth import Principal

principal = Principal("alice", {"sub": "alice", "scope": "orders:write"})
principal.name                    # "alice"
principal.claim("scope")          # "orders:write"
principal.claim("role", "user")   # "user" — the default when the claim is absent
```

`benzene.core.Context` has no auth slot and this package must not modify the core, so the principal
rides on a private context attribute. Two free functions are the seam:

- `set_principal(context, principal)` — attach a principal (the middlewares call this on success).
- `get_principal(context) -> Principal | None` — read who the caller is downstream; `None` when the
  context is unauthenticated.

```python
from benzene.auth import get_principal

async def handle(context):
    principal = get_principal(context)     # None when unauthenticated
    if principal is None or not principal.claim("scope"):
        return Result.forbidden("scope required")
    ...
```

## Basic auth

`basic_auth_interception` decodes the `authorization: Basic base64(user:pass)` header and calls a
caller-supplied `verify(username, password)`.

```python
from benzene.auth import basic_auth_interception, Principal

secrets = {"alice": "s3cret"}

def verify(username: str, password: str) -> bool:      # sync or async; bool | Principal | None
    return secrets.get(username) == password

definition.middleware += [basic_auth_interception(verify, realm="orders")]
```

```python
basic_auth_interception(verify: BasicVerify, *, realm: str = "benzene") -> Middleware
```

The `verify` outcome (a `BasicVerify`) is coerced to a principal:

- `True` authenticates as `Principal(username)`;
- a returned `Principal` is attached as-is (use this to carry claims);
- `False` / `None` — or a missing or malformed header — rejects with `Result.unauthorized`, naming the
  `realm`, and the handler is never reached.

Install it ahead of the message router.

## Bearer / OAuth2

`bearer_token_interception` reads the `authorization: Bearer <token>` header and hands the token to a
`validate(token)`.

```python
from benzene.auth import bearer_token_interception, JwtValidator

definition.middleware += [
    bearer_token_interception(
        JwtValidator(key=signing_secret, algorithms=("HS256",), audience="orders-api")
    )
]
```

```python
bearer_token_interception(validate: BearerValidate, *, scheme: str = "Bearer") -> Middleware
```

The `validate` outcome (a `BearerValidate`) is coerced to a principal:

- a claims **`dict`** becomes `Principal(str(claims["sub"]), claims)` — the `DEFAULT_PRINCIPAL_CLAIM`
  (`"sub"`) seeds the name;
- a returned `Principal` is attached as-is;
- `None` / `False` — or a missing/malformed header — rejects with `Result.unauthorized("invalid
  token")`.

`scheme` is the credential scheme to accept (default `Bearer`, matched case-insensitively).

### `JwtValidator`

A ready-made `validate` that decodes and verifies a JWT with PyJWT, **returning `None` for any invalid
token** (bad signature, expired, wrong audience/issuer, malformed) rather than raising — so a bad token
drops straight into `unauthorized`.

```python
JwtValidator(
    *,
    key: Any = None,                                # HMAC secret or an RSA/EC public key
    algorithms: tuple[str, ...] = ("HS256",),
    audience: str | None = None,
    issuer: str | None = None,
    principal_claim: str = DEFAULT_PRINCIPAL_CLAIM, # "sub" — the claim used for Principal.name
    decode: Decoder | None = None,                  # inject a decoder to test without PyJWT
)
```

PyJWT is imported **lazily** on first decode, so importing this class costs nothing and the `[jwt]`
extra is only needed to actually validate. A genuinely *missing* PyJWT is a deployment error, not a
token outcome — it surfaces as `ImportError` rather than being swallowed into `None`. Pass `decode` (a
`Decoder`, `token -> claims`, raising on an invalid token) to inject a fake decoder in tests.
`JwtValidator` is callable, so it is a `validate` directly.

### `JwksValidator` — rotating keys, JWKS and OIDC discovery

`JwtValidator` verifies against **one configured key**, which breaks the moment the identity provider
rotates its signing key. `JwksValidator` is the rotation-aware counterpart: it resolves the
verification key **per token**, by the `kid` in the token header, from an IdP's cached **JWKS**
document — discovered from the issuer's `/.well-known/openid-configuration`, or configured directly.
It mirrors .NET's `Benzene.Auth.OAuth2` (`Authority` / `JwksUri` + a JWKS-caching
`ConfigurationManager`).

```python
from benzene.auth import JwksValidator, bearer_token_interception

definition.middleware += [
    bearer_token_interception(
        JwksValidator(
            issuer="https://login.example.com/",   # OIDC discovery finds the jwks_uri
            audience="orders-api",
            algorithms=("RS256",),                 # the allowlist — see below
        )
    )
]
```

```python
JwksValidator(
    *,
    issuer: str | None = None,          # OIDC issuer: the discovery root *and* the `iss` constraint
    jwks_uri: str | None = None,        # or a bare JWKS URL, for IdPs without discovery (wins if both)
    algorithms: tuple[str, ...] = ("RS256",),
    audience: str | None = None,
    principal_claim: str = DEFAULT_PRINCIPAL_CLAIM,
    leeway: float = 0.0,                # clock-skew tolerance handed to PyJWT
    fetch: JwksFetch | None = None,     # async (url) -> body text; default: urllib on a worker thread
    clock: Clock = time.monotonic,
    cache_ttl: float = DEFAULT_CACHE_TTL,                       # 3600s
    min_refresh_interval: float = DEFAULT_MIN_REFRESH_INTERVAL, # 300s
    require_https: bool = True,
    decode: JwksDecoder | None = None,  # (token, jwk, algorithm) -> claims; inject to test w/o PyJWT
    client: JwksClient | None = None,   # share one client (hence one cache and budget) if you like
)
```

`validate` is **async** here (the bearer middleware awaits either shape). Every token it rejects — bad
signature, expired, wrong audience/issuer, unknown `kid`, non-allowlisted `alg`, an unreachable IdP —
is `None`, never an exception, so it drops straight into `unauthorized`. A *missing PyJWT* still
raises `ImportError`, because that is a deployment fault rather than a token outcome.

**The refresh policy, and why it is bounded.** An unknown `kid` is what a key rotation looks like — and
also what an attacker sending random `kid`s looks like. A validator that refetches on every miss lets
an unauthenticated caller drive one outbound request per token, aiming this service at its own IdP. So
every fetch, scheduled or forced, passes one gate:

| Bound | What it buys |
|---|---|
| **Floor between attempts** (`min_refresh_interval`, 300s) | At most **one** outbound fetch per document per interval, however many unknown `kid`s arrive. Failed attempts spend the budget too, so a down IdP is not hammered. |
| **Single-flight** | Concurrent misses coalesce behind one lock — eight simultaneous requests for a just-rotated key cause one fetch, not eight. |
| **Last known good** | A failed refresh keeps the keys already fetched: an IdP outage stops rotation, it does not reject every token in flight. |
| **TTL** (`cache_ttl`, 3600s) | After it, the document is refreshed on next use (subject to the same floor). |

The cost is bounded *latency* on rotation, not correctness: a token signed with a brand-new `kid` can
be rejected for up to `min_refresh_interval`. That is the deliberate trade — IdPs publish a new key
before signing with it — and both numbers are per-validator arguments.

**Algorithms are never taken from the key or the token.** The `algorithms` allowlist is the only
source of algorithm truth, exactly as with `JwtValidator`:

- the header's `alg` must be in the allowlist **before** any key is resolved (so a junk `alg` cannot
  even cost a fetch); `algorithms=()` is a `ValueError` at construction, and so is `"none"` in it (an
  unsecured JWT carries no signature at all — RFC 8725 §3.2);
- a JWKS entry that declares an `alg` of its own is usable for **that algorithm only** — an entry
  advertising `HS256` (an RSA public key republished as an `oct` secret, say) never becomes usable
  because the service allowlists `RS256` and a token asks for it;
- entries with `use` other than `sig`, or `key_ops` without `verify`, are skipped;
- the decoder is handed exactly **one** already-allowlisted algorithm, never a list the JWKS or the
  token got to widen. This is what closes RFC 8725 §3.1 algorithm confusion.

**Metadata is fetched over HTTPS.** The JWKS is what establishes which keys are trusted, so a plain
`http://` issuer or `jwks_uri` is a `ValueError` at construction, and a discovered `jwks_uri` that is
not HTTPS is ignored. `require_https=False` is the local-dev/test escape hatch only — the same one
.NET's `RequireHttpsMetadata` provides. The discovery document's own `issuer` must equal the
configured one (OIDC Discovery §4.3), so a substituted document cannot point the validator at someone
else's keys.

`JwksClient` is the key-resolution half on its own (`key_for(kid=..., alg=...) -> jwk | None`,
`jwks_uri()`), if you want to share one cache and one refresh budget across several validators, and
`unverified_header(token)` decodes a JWT header without verifying anything (only `alg`/`kid` routing
hints are ever read from it).

### `static_token_validator`

An in-memory `token -> principal|claims` map — the test seam, no JWT library needed. An unmapped token
returns `None`; a mapped `dict` is wrapped exactly as `bearer_token_interception` wraps a validator's
dict.

```python
from benzene.auth import static_token_validator, bearer_token_interception, Principal

validate = static_token_validator({
    "tok-alice": Principal("alice", {"scope": "orders:write"}),
    "tok-bob": {"sub": "bob"},
})
definition.middleware += [bearer_token_interception(validate)]
```

## AWS API Gateway custom authorizer

`api_gateway_authorizer` adapts the *same* `validate` seam into an AWS Lambda custom-authorizer
handler that returns an IAM policy document allowing or denying `execute-api:Invoke`. It mirrors
`Benzene.Aws.Lambda.ApiGateway.ApiGatewayCustomAuthorizer`.

```python
from benzene.auth import api_gateway_authorizer, JwtValidator

handler = api_gateway_authorizer(JwtValidator(key=signing_secret))

# AWS invokes this synchronously:
response = handler(event, context)
```

```python
api_gateway_authorizer(
    validate: AuthorizerValidate,
    *,
    principal_id_claim: str = DEFAULT_PRINCIPAL_CLAIM,   # "sub" — the policy's principalId
    scheme: str = "Bearer",
) -> Callable[..., dict[str, Any]]
```

The returned `handler(event, context=None) -> dict` extracts the token from either authorizer flavour:

- a **TOKEN** authorizer passes it in `event["authorizationToken"]` (usually `Bearer <token>`);
- a **REQUEST** authorizer passes the request, so the token is read from the `authorization` header.

A token `validate` accepts yields an **Allow** policy scoped to `event["methodArn"]`, with the decoded
claims echoed under `context` (values coerced to primitives); anything else yields **Deny**. The
emitted shape is the standard authorizer response:

```jsonc
{
  "principalId": "alice",
  "policyDocument": {
    "Version": "2012-10-17",
    "Statement": [
      { "Action": "execute-api:Invoke", "Effect": "Allow", "Resource": "<methodArn>" }
    ]
  },
  "context": { "sub": "alice", "scope": "orders:write" }
}
```

**The synchronous-handler contract.** The returned `handler` is **synchronous** — the shape AWS Lambda
invokes — so an async `validate` is driven to completion internally (`asyncio.run`). Call it from a
synchronous context, as the Lambda runtime does. Invoking it from inside a *running* event loop with an
async `validate` will raise, because a coroutine cannot be driven synchronously from within one; a sync
`validate` (such as `JwtValidator`) has no such constraint.

## Troubleshooting

- **`JwtValidator` raises `ImportError` on validate** — PyJWT is not installed. Run
  `pip install "benzene-auth[jwt]"`, or inject a `decode` function. This is deliberately *not*
  swallowed into `None`, because a missing library is a deployment fault, not a bad token.
- **Every request is `unauthorized` even with a good token** — check the header name and casing;
  `Context.headers` is already lower-cased by the core, and the middleware reads `authorization`.
  Confirm the `scheme` matches (`Bearer` by default) and that `JwtValidator`'s `algorithms` /
  `audience` / `issuer` match the token's.
- **The Lambda authorizer raises about a running event loop** — you called the synchronous `handler`
  with an async `validate` from inside an event loop. Use a sync `validate` (e.g. `JwtValidator`) or
  invoke the handler from a synchronous context. `JwksValidator` is async, so it belongs in
  `bearer_token_interception`, not in an authorizer invoked from inside a loop.
- **`JwksValidator` rejects tokens right after a key rotation, then starts accepting them** — that is
  the refresh floor doing its job (`min_refresh_interval`, 300s by default): an unknown `kid` buys at
  most one refetch per interval, so an attacker cannot use it to spray requests at your IdP. Lower it
  if your rotation cadence needs to, knowing what it buys.
- **`JwksValidator` raises `ImportError` naming a crypto backend** — PyJWT is installed without it, so
  it cannot verify `RS256`/`ES256`. Install `"benzene-auth[jwks]"`.
- **`JwksValidator` raises `ValueError` about https at start-up** — the issuer/`jwks_uri` is `http://`.
  Fetching the document that decides which keys are trusted over plain HTTP lets a MITM substitute a
  key; use HTTPS, or `require_https=False` for a local fake endpoint only.

## Exports

`Principal`, `get_principal`, `set_principal`; `BasicVerify`, `basic_auth_interception`;
`BearerValidate`, `Decoder`, `DEFAULT_PRINCIPAL_CLAIM`, `JwtValidator`, `bearer_token_interception`,
`static_token_validator`; `JwksValidator`, `JwksClient`, `JwksFetch`, `JwksDecoder`,
`DEFAULT_CACHE_TTL`, `DEFAULT_MIN_REFRESH_INTERVAL`, `OPENID_CONFIGURATION_PATH`,
`openid_configuration_url`, `stdlib_fetch`, `unverified_header` (the `Clock` alias stays in
`benzene.auth.jwks`, like the other packages' clock seams); `api_gateway_authorizer`.

## See also

- [`benzene.core`](core.md) — the `Context`, `Middleware`, and pipeline these interceptions plug into.
- [`benzene.results`](results.md) — the `Result.unauthorized` short-circuit and the status vocabulary.
