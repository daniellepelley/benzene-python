"""Authentication middleware — Basic, Bearer/JWT, JWKS/OIDC, and the API Gateway custom authorizer.

Every check runs in memory: the middleware tests drive a real ``MiddlewarePipeline`` + ``message_router``
so a rejection is asserted to *short-circuit* (the handler never runs), the JWT validator is exercised
through an injected fake decoder, and the authorizer is called with hand-built events. The JWKS suite
drives a fake in-memory identity provider through the injected fetcher and clock, so key rotation,
the bounded refresh policy, and the algorithm allowlist are all asserted without a socket or a sleep.
No PyJWT, no broker, no network.
"""

from __future__ import annotations

import asyncio
import base64
import json
import sys
import types
from typing import Any

import pytest
from benzene.auth import (
    JwksValidator,
    JwtValidator,
    Principal,
    api_gateway_authorizer,
    basic_auth_interception,
    bearer_token_interception,
    get_principal,
    static_token_validator,
)
from benzene.core import Context, MiddlewarePipeline, Registry, message_router
from benzene.results import Result, Status


def run(coro):
    return asyncio.run(coro)


def _basic_header(username: str, password: str) -> str:
    encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {encoded}"


class RecordingHandler:
    """A handler that records how often it ran (the router hands it the mapped request, not context)."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, _request) -> Result:
        self.calls += 1
        return Result.ok({"ran": True})


def _pipeline(middleware) -> tuple[MiddlewarePipeline, RecordingHandler]:
    handler = RecordingHandler()
    registry = Registry().register("t", handler)
    pipeline = MiddlewarePipeline([middleware]).use(message_router(registry))
    return pipeline, handler


# --- basic auth --------------------------------------------------------------------------------


def test_basic_auth_accepts_valid_credentials_and_attaches_principal() -> None:
    def verify(username: str, password: str) -> bool:
        return username == "alice" and password == "s3cret"

    pipeline, handler = _pipeline(basic_auth_interception(verify))
    ctx = Context("t", {}, headers={"authorization": _basic_header("alice", "s3cret")})
    run(pipeline.handle(ctx))

    assert handler.calls == 1  # the handler ran
    assert ctx.result is not None and ctx.result.is_successful
    principal = get_principal(ctx)
    assert principal is not None and principal.name == "alice"


def test_basic_auth_attaches_a_returned_principal() -> None:
    def verify(username: str, password: str) -> Principal:
        return Principal(username, {"role": "admin"})

    pipeline, handler = _pipeline(basic_auth_interception(verify))
    ctx = Context("t", {}, headers={"authorization": _basic_header("bob", "pw")})
    run(pipeline.handle(ctx))

    principal = get_principal(ctx)
    assert principal is not None and principal.claim("role") == "admin"


def test_basic_auth_rejects_missing_header_without_running_handler() -> None:
    pipeline, handler = _pipeline(basic_auth_interception(lambda u, p: True))
    ctx = Context("t", {})  # no authorization header
    run(pipeline.handle(ctx))

    assert handler.calls == 0  # short-circuit
    assert ctx.result is not None and ctx.result.status == Status.UNAUTHORIZED


def test_basic_auth_rejects_malformed_header_without_running_handler() -> None:
    pipeline, handler = _pipeline(basic_auth_interception(lambda u, p: True))
    ctx = Context("t", {}, headers={"authorization": "Basic not-base64!!"})
    run(pipeline.handle(ctx))

    assert handler.calls == 0
    assert ctx.result is not None and ctx.result.status == Status.UNAUTHORIZED


def test_basic_auth_rejects_invalid_credentials_without_running_handler() -> None:
    async def verify(username: str, password: str) -> bool:  # async verifier is supported too
        return False

    pipeline, handler = _pipeline(basic_auth_interception(verify))
    ctx = Context("t", {}, headers={"authorization": _basic_header("alice", "wrong")})
    run(pipeline.handle(ctx))

    assert handler.calls == 0
    assert ctx.result is not None and ctx.result.status == Status.UNAUTHORIZED


# --- bearer ------------------------------------------------------------------------------------


def test_bearer_accepts_a_valid_token_and_attaches_claims() -> None:
    validate = static_token_validator({"good-token": Principal("carol", {"scope": "read"})})
    pipeline, handler = _pipeline(bearer_token_interception(validate))
    ctx = Context("t", {}, headers={"authorization": "Bearer good-token"})
    run(pipeline.handle(ctx))

    assert handler.calls == 1
    principal = get_principal(ctx)
    assert principal is not None and principal.name == "carol"
    assert principal.claim("scope") == "read"


def test_bearer_reads_claims_dict_from_validator() -> None:
    def validate(token: str) -> dict | None:
        return {"sub": "dave", "scope": "write"} if token == "t0k" else None

    pipeline, handler = _pipeline(bearer_token_interception(validate))
    ctx = Context("t", {}, headers={"authorization": "Bearer t0k"})
    run(pipeline.handle(ctx))

    principal = get_principal(ctx)
    assert principal is not None and principal.name == "dave"


def test_bearer_rejects_an_invalid_token_without_running_handler() -> None:
    validate = static_token_validator({"good-token": Principal("carol")})
    pipeline, handler = _pipeline(bearer_token_interception(validate))
    ctx = Context("t", {}, headers={"authorization": "Bearer nope"})
    run(pipeline.handle(ctx))

    assert handler.calls == 0
    assert ctx.result is not None and ctx.result.status == Status.UNAUTHORIZED


def test_bearer_rejects_missing_header_without_running_handler() -> None:
    validate = static_token_validator({"good-token": Principal("carol")})
    pipeline, handler = _pipeline(bearer_token_interception(validate))
    ctx = Context("t", {})
    run(pipeline.handle(ctx))

    assert handler.calls == 0
    assert ctx.result is not None and ctx.result.status == Status.UNAUTHORIZED


# --- JwtValidator (via an injected decoder — no PyJWT) ------------------------------------------


class FakeExpiredError(Exception):
    """Stands in for ``jwt.ExpiredSignatureError`` so the validator is tested without PyJWT."""


def test_jwt_validator_decodes_a_valid_token() -> None:
    def decode(token: str) -> dict:
        assert token == "header.payload.sig"
        return {"sub": "erin", "aud": "orders"}

    validator = JwtValidator(decode=decode, principal_claim="sub")
    principal = validator.validate("header.payload.sig")

    assert principal is not None and principal.name == "erin"
    assert principal.claim("aud") == "orders"


def test_jwt_validator_returns_none_on_bad_or_expired_token() -> None:
    def decode(token: str) -> dict:
        raise FakeExpiredError("token expired")

    validator = JwtValidator(decode=decode)
    assert validator.validate("whatever") is None  # never raises out — a bad token is None


# --- API Gateway custom authorizer -------------------------------------------------------------


def test_authorizer_allows_a_valid_token_token_authorizer() -> None:
    validate = static_token_validator({"good": Principal("frank", {"scope": "read"})})
    handler = api_gateway_authorizer(validate)
    arn = "arn:aws:execute-api:us-east-1:123:api/prod/GET/orders"
    response = handler({"authorizationToken": "Bearer good", "methodArn": arn})

    statement = response["policyDocument"]["Statement"][0]
    assert statement["Effect"] == "Allow"
    assert statement["Action"] == "execute-api:Invoke"
    assert statement["Resource"] == arn
    assert response["principalId"] == "frank"
    assert response["context"]["scope"] == "read"


def test_authorizer_allows_from_request_authorizer_header() -> None:
    validate = static_token_validator({"good": Principal("frank")})
    handler = api_gateway_authorizer(validate)
    arn = "arn:aws:execute-api:us-east-1:123:api/prod/GET/orders"
    response = handler({"headers": {"Authorization": "Bearer good"}, "methodArn": arn})

    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"
    assert response["policyDocument"]["Statement"][0]["Resource"] == arn


def test_authorizer_denies_an_invalid_token() -> None:
    validate = static_token_validator({"good": Principal("frank")})
    handler = api_gateway_authorizer(validate)
    arn = "arn:aws:execute-api:us-east-1:123:api/prod/GET/orders"
    response = handler({"authorizationToken": "Bearer bad", "methodArn": arn})

    statement = response["policyDocument"]["Statement"][0]
    assert statement["Effect"] == "Deny"
    assert statement["Resource"] == arn


# --- bearer scheme edges -------------------------------------------------------------------------


def test_bearer_rejects_a_non_bearer_scheme_without_running_handler() -> None:
    validate = static_token_validator({"abc": Principal("carol")})
    pipeline, handler = _pipeline(bearer_token_interception(validate))
    # A Basic credential offered where Bearer is expected is not a bearer token, even though the
    # token part alone would have validated.
    ctx = Context("t", {}, headers={"authorization": "Basic abc"})
    run(pipeline.handle(ctx))

    assert handler.calls == 0
    assert ctx.result is not None and ctx.result.status == Status.UNAUTHORIZED


def test_custom_scheme_is_matched_case_insensitively() -> None:
    validate = static_token_validator({"t0k": Principal("erin")})
    pipeline, handler = _pipeline(bearer_token_interception(validate, scheme="Token"))
    ctx = Context("t", {}, headers={"authorization": "token t0k"})  # lowercased scheme still matches
    run(pipeline.handle(ctx))

    assert handler.calls == 1
    principal = get_principal(ctx)
    assert principal is not None and principal.name == "erin"


# --- JwtValidator's real PyJWT path (a stubbed `jwt` module, still no PyJWT installed) -----------


def test_decode_with_pyjwt_passes_key_algorithms_audience_and_issuer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple, dict]] = []

    def decode(token, key, **kwargs):
        calls.append(((token, key), kwargs))
        return {"sub": "erin", "aud": "orders", "iss": "https://issuer.example"}

    monkeypatch.setitem(sys.modules, "jwt", types.SimpleNamespace(decode=decode))

    validator = JwtValidator(
        key="k", algorithms=("RS256",), audience="orders", issuer="https://issuer.example"
    )
    principal = validator.validate("header.payload.sig")  # no injected decode → the real path

    # The kwargs assembly is the whole point: a typo here would only ever fail in production.
    assert calls == [
        (
            ("header.payload.sig", "k"),
            {
                "algorithms": ["RS256"],
                "audience": "orders",
                "issuer": "https://issuer.example",
            },
        )
    ]
    assert principal is not None and principal.name == "erin"


def test_decode_with_pyjwt_omits_audience_and_issuer_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict] = []

    def decode(token, key, **kwargs):
        calls.append(kwargs)
        return {"sub": "frank"}

    monkeypatch.setitem(sys.modules, "jwt", types.SimpleNamespace(decode=decode))

    assert JwtValidator(key="k").validate("t") is not None
    # Passing audience=None/issuer=None would make PyJWT *verify* against None — they must be absent.
    assert calls == [{"algorithms": ["HS256"]}]


def test_missing_pyjwt_surfaces_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "jwt", None)
    validator = JwtValidator(key="k")
    # A missing PyJWT is a deployment error, not an "invalid token": it must escape `validate`'s
    # rejection mapper, naming the extra (and the injectable decode seam) rather than returning None.
    with pytest.raises(ImportError, match=r"benzene-auth\[jwt\]") as raised:
        validator.validate("header.payload.sig")
    assert "decode" in str(raised.value)


# --- JWKS / OIDC discovery: key-rotation-aware validation (no PyJWT, no network) ----------------
#
# The fixtures below stand in for a real identity provider: `FakeIdp` serves an OIDC discovery
# document and a JWKS document and counts every outbound fetch, `FakeClock` is the injected clock,
# and `_fake_decode` is the injected decoder — its "signature check" is that the token's
# `signed_with` claim names the JWK it was handed, so a key rotation genuinely breaks validation
# until the new key is fetched. Nothing here touches the network or needs PyJWT.

ISSUER = "https://issuer.example"
JWKS_URI = "https://issuer.example/keys"
DISCOVERY_URL = "https://issuer.example/.well-known/openid-configuration"


def _b64(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _jwt(
    *, kid: str | None, alg: str = "RS256", signed_with: str | None = None, sub: str = "erin"
) -> str:
    """A JWT-shaped token: a real base64url header (which the validator parses) and a fake signature."""
    header: dict[str, Any] = {"alg": alg, "typ": "JWT"}
    if kid is not None:
        header["kid"] = kid
    claims = {"sub": sub, "signed_with": signed_with if signed_with is not None else kid}
    return f"{_b64(header)}.{_b64(claims)}.signature"


def _jwk(kid: str, *, alg: str | None = "RS256", **extra: Any) -> dict[str, Any]:
    jwk: dict[str, Any] = {"kty": "RSA", "use": "sig", "kid": kid, "n": f"n-{kid}", "e": "AQAB"}
    if alg is not None:
        jwk["alg"] = alg
    jwk.update(extra)
    return jwk


class FakeIdp:
    """An in-memory OIDC/JWKS endpoint pair that counts every fetch and can rotate or go down."""

    def __init__(self, *keys: dict[str, Any]) -> None:
        self.keys: list[dict[str, Any]] = list(keys)
        self.requests: list[str] = []
        self.down = False
        self.discovery_issuer = ISSUER

    async def fetch(self, url: str) -> str:
        self.requests.append(url)
        await asyncio.sleep(0)  # a real fetch yields — this is what makes single-flight observable
        if self.down:
            raise OSError("jwks endpoint unreachable")
        if url == DISCOVERY_URL:
            return json.dumps({"issuer": self.discovery_issuer, "jwks_uri": JWKS_URI})
        if url == JWKS_URI:
            return json.dumps({"keys": self.keys})
        raise AssertionError(f"unexpected fetch: {url}")

    @property
    def jwks_fetches(self) -> int:
        return sum(1 for url in self.requests if url == JWKS_URI)

    @property
    def discovery_fetches(self) -> int:
        return sum(1 for url in self.requests if url == DISCOVERY_URL)


class FakeClock:
    """An injected clock — no test sleeps to cross a cache TTL or a refresh floor."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _fake_decode(token: str, jwk: dict[str, Any], algorithm: str) -> dict[str, Any]:
    """Stands in for PyJWT: the token only "verifies" against the very JWK that signed it."""
    segment = token.split(".")[1]
    claims: dict[str, Any] = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
    if claims.get("signed_with") != jwk.get("kid"):
        raise ValueError("signature does not verify against this key")
    return claims


def _validator(idp: FakeIdp, clock: FakeClock, **kwargs: Any) -> JwksValidator:
    options: dict[str, Any] = {
        "issuer": ISSUER,
        "algorithms": ("RS256",),
        "fetch": idp.fetch,
        "clock": clock,
        "decode": _fake_decode,
    }
    options.update(kwargs)
    return JwksValidator(**options)


def test_jwks_validator_resolves_a_key_by_kid_through_oidc_discovery() -> None:
    idp = FakeIdp(_jwk("k1"))
    validator = _validator(idp, FakeClock())

    principal = run(validator.validate(_jwt(kid="k1")))

    assert principal is not None and principal.name == "erin"
    assert idp.requests == [DISCOVERY_URL, JWKS_URI]  # discovery, then the JWKS it points at


def test_a_rotated_signing_key_is_picked_up_after_the_refresh_floor() -> None:
    idp = FakeIdp(_jwk("k1"))
    clock = FakeClock()
    validator = _validator(idp, clock)

    assert run(validator.validate(_jwt(kid="k1"))) is not None  # warms the cache on k1

    idp.keys = [_jwk("k2")]  # the IdP rotates: k1 retired, k2 published
    # Immediately after a fetch the refresh floor is closed, so the unknown kid is simply rejected —
    # this is the bound, not a bug (see the unbounded-fetch test below).
    assert run(validator.validate(_jwt(kid="k2"))) is None
    assert idp.jwks_fetches == 1

    clock.advance(301.0)  # past the default 300s floor between outbound fetch attempts
    principal = run(validator.validate(_jwt(kid="k2")))

    assert principal is not None and principal.name == "erin"  # the rotated key now validates
    assert idp.jwks_fetches == 2
    # ...and the retired key stops validating, which is the point of rotation.
    assert run(validator.validate(_jwt(kid="k1"))) is None


def test_unknown_kids_cannot_force_unbounded_jwks_fetches() -> None:
    idp = FakeIdp(_jwk("k1"))
    clock = FakeClock()
    validator = _validator(idp, clock)

    assert run(validator.validate(_jwt(kid="k1"))) is not None
    baseline = idp.jwks_fetches

    for index in range(200):  # an attacker spraying random kids inside one refresh window
        clock.advance(1.0)
        assert run(validator.validate(_jwt(kid=f"attacker-{index}"))) is None
    assert idp.jwks_fetches == baseline  # 200 unknown kids, not one extra outbound request

    clock.advance(200.0)  # now past the 300s floor: the *next* miss buys exactly one refetch
    assert run(validator.validate(_jwt(kid="attacker-again"))) is None
    assert idp.jwks_fetches == baseline + 1
    for index in range(100):  # ...and that one refetch is the whole budget for this window too
        clock.advance(1.0)
        assert run(validator.validate(_jwt(kid=f"more-{index}"))) is None
    assert idp.jwks_fetches == baseline + 1


def test_concurrent_unknown_kids_share_a_single_refetch() -> None:
    idp = FakeIdp(_jwk("k1"), _jwk("k2"))
    validator = _validator(idp, FakeClock())

    async def race() -> list[Principal | None]:
        return list(
            await asyncio.gather(*(validator.validate(_jwt(kid="k2")) for _ in range(8)))
        )

    results = run(race())

    assert all(result is not None for result in results)
    assert idp.jwks_fetches == 1  # single-flight: eight concurrent misses, one outbound fetch
    assert idp.discovery_fetches == 1


def test_a_jwks_entry_claiming_another_alg_is_never_used() -> None:
    decoded: list[str] = []

    def decode(token: str, jwk: dict[str, Any], algorithm: str) -> dict[str, Any]:
        decoded.append(algorithm)
        return _fake_decode(token, jwk, algorithm)

    # A hostile/compromised JWKS entry advertising HS256 while the service allowlists RS256 only:
    # the classic algorithm-confusion setup (RFC 8725 §3.1).
    idp = FakeIdp(_jwk("evil", alg="HS256", kty="oct", k="cHVibGljLWtleQ"))
    validator = _validator(idp, FakeClock(), decode=decode)

    # The token claiming the entry's own alg is rejected: HS256 is not in the allowlist.
    assert run(validator.validate(_jwt(kid="evil", alg="HS256"))) is None
    # And the entry cannot be borrowed for an allowlisted alg it does not declare either.
    assert run(validator.validate(_jwt(kid="evil", alg="RS256"))) is None
    assert decoded == []  # the decoder never saw that key material at all


def test_a_token_alg_outside_the_allowlist_is_rejected_before_any_fetch() -> None:
    idp = FakeIdp(_jwk("k1", alg=None))  # a key that names no alg of its own
    validator = _validator(idp, FakeClock(), algorithms=("RS256", "ES256"))

    assert run(validator.validate(_jwt(kid="k1", alg="none"))) is None
    assert run(validator.validate(_jwt(kid="k1", alg="HS256"))) is None
    assert idp.requests == []  # rejected on the header alone — no key resolution, no outbound call
    assert run(validator.validate(_jwt(kid="k1", alg="ES256"))) is not None


def test_the_decoder_is_handed_the_headers_alg_only_when_allowlisted() -> None:
    seen: list[tuple[str, str]] = []

    def decode(token: str, jwk: dict[str, Any], algorithm: str) -> dict[str, Any]:
        seen.append((algorithm, jwk["kid"]))
        return _fake_decode(token, jwk, algorithm)

    idp = FakeIdp(_jwk("k1"))
    validator = _validator(idp, FakeClock(), decode=decode)

    assert run(validator.validate(_jwt(kid="k1"))) is not None
    assert seen == [("RS256", "k1")]


def test_oidc_discovery_is_cached_and_its_issuer_must_match() -> None:
    idp = FakeIdp(_jwk("k1"))
    clock = FakeClock()
    validator = _validator(idp, clock)

    assert run(validator.validate(_jwt(kid="k1"))) is not None
    assert run(validator.validate(_jwt(kid="k1"))) is not None
    assert idp.discovery_fetches == 1  # the discovery document is cached across validations

    mixed_up = FakeIdp(_jwk("k1"))
    mixed_up.discovery_issuer = "https://attacker.example"  # issuer mix-up
    assert run(_validator(mixed_up, FakeClock()).validate(_jwt(kid="k1"))) is None
    assert mixed_up.jwks_fetches == 0  # never even followed the jwks_uri it advertised


def test_an_explicit_jwks_uri_skips_discovery() -> None:
    idp = FakeIdp(_jwk("k1"))
    validator = _validator(idp, FakeClock(), issuer=None, jwks_uri=JWKS_URI)

    assert run(validator.validate(_jwt(kid="k1"))) is not None
    assert idp.requests == [JWKS_URI]


def test_last_known_good_keys_survive_an_idp_outage() -> None:
    idp = FakeIdp(_jwk("k1"))
    clock = FakeClock()
    validator = _validator(idp, clock)

    assert run(validator.validate(_jwt(kid="k1"))) is not None

    idp.down = True
    clock.advance(4_000.0)  # past the cache TTL, so a refresh is attempted and fails
    assert run(validator.validate(_jwt(kid="k1"))) is not None  # served from last known good
    attempts = idp.jwks_fetches
    clock.advance(1.0)
    assert run(validator.validate(_jwt(kid="k1"))) is not None
    assert idp.jwks_fetches == attempts  # a down IdP is not hammered — the floor applies to retries


def test_https_is_required_for_metadata_unless_explicitly_relaxed() -> None:
    with pytest.raises(ValueError, match="https"):
        JwksValidator(issuer="http://issuer.example", algorithms=("RS256",))
    with pytest.raises(ValueError, match="https"):
        JwksValidator(jwks_uri="http://issuer.example/keys", algorithms=("RS256",))
    # The local-dev/test escape hatch, the same one .NET's RequireHttpsMetadata provides.
    JwksValidator(jwks_uri="http://localhost:8080/keys", algorithms=("RS256",), require_https=False)


def test_jwks_validator_requires_an_issuer_or_jwks_uri_and_a_non_empty_allowlist() -> None:
    with pytest.raises(ValueError):
        JwksValidator(algorithms=("RS256",))
    with pytest.raises(ValueError, match="algorithm"):
        JwksValidator(issuer=ISSUER, algorithms=())
    with pytest.raises(ValueError, match="none"):  # an unsecured JWT is not an algorithm choice
        JwksValidator(issuer=ISSUER, algorithms=("RS256", "none"))


def test_jwks_validator_drops_into_the_bearer_pipeline() -> None:
    idp = FakeIdp(_jwk("k1"))
    validator = _validator(idp, FakeClock())
    pipeline, handler = _pipeline(bearer_token_interception(validator))

    ctx = Context("t", {}, headers={"authorization": f"Bearer {_jwt(kid='k1')}"})
    run(pipeline.handle(ctx))
    assert handler.calls == 1
    principal = get_principal(ctx)
    assert principal is not None and principal.name == "erin"

    rejected = Context("t", {}, headers={"authorization": f"Bearer {_jwt(kid='unknown')}"})
    run(pipeline.handle(rejected))
    assert handler.calls == 1  # short-circuit
    assert rejected.result is not None and rejected.result.status == Status.UNAUTHORIZED


def test_jwks_decode_without_pyjwt_surfaces_the_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "jwt", None)
    idp = FakeIdp(_jwk("k1"))
    validator = JwksValidator(
        issuer=ISSUER, algorithms=("RS256",), fetch=idp.fetch, clock=FakeClock()
    )
    # A missing PyJWT is a deployment error, not an invalid token — it must escape the None mapper.
    with pytest.raises(ImportError, match=r"benzene-auth\[jwt\]") as raised:
        run(validator.validate(_jwt(kid="k1")))
    assert "decode" in str(raised.value)


def test_jwks_decode_with_pyjwt_passes_the_allowlisted_algorithm_and_constraints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, dict[str, Any]]] = []

    class FakeAlgorithm:
        def from_jwk(self, jwk_json: str) -> str:
            return f"key({json.loads(jwk_json)['kid']})"

    def decode(token: str, key: str, **kwargs: Any) -> dict[str, Any]:
        calls.append((token, key, kwargs))
        return {"sub": "erin"}

    monkeypatch.setitem(sys.modules, "jwt", types.SimpleNamespace(decode=decode))
    monkeypatch.setitem(
        sys.modules,
        "jwt.algorithms",
        types.SimpleNamespace(get_default_algorithms=lambda: {"RS256": FakeAlgorithm()}),
    )

    idp = FakeIdp(_jwk("k1"))
    validator = JwksValidator(
        issuer=ISSUER,
        algorithms=("RS256", "ES256"),
        audience="orders",
        fetch=idp.fetch,
        clock=FakeClock(),
    )
    token = _jwt(kid="k1")
    principal = run(validator.validate(token))

    assert principal is not None and principal.name == "erin"
    # The key comes from the JWKS entry; the algorithm list is the *header's* alg after the
    # allowlist check — never a list the JWKS entry or the token got to widen.
    assert calls == [
        (
            token,
            "key(k1)",
            {"algorithms": ["RS256"], "audience": "orders", "issuer": ISSUER, "leeway": 0.0},
        )
    ]


def test_jwks_decode_without_a_crypto_backend_names_the_missing_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "jwt", types.SimpleNamespace(decode=lambda *a, **k: {}))
    monkeypatch.setitem(
        sys.modules, "jwt.algorithms", types.SimpleNamespace(get_default_algorithms=lambda: {})
    )
    idp = FakeIdp(_jwk("k1"))
    validator = JwksValidator(
        issuer=ISSUER, algorithms=("RS256",), fetch=idp.fetch, clock=FakeClock()
    )
    with pytest.raises(ImportError, match="crypto"):
        run(validator.validate(_jwt(kid="k1")))
