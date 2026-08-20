"""Authentication middleware — Basic, Bearer/JWT, and the API Gateway custom authorizer.

Every check runs in memory: the middleware tests drive a real ``MiddlewarePipeline`` + ``message_router``
so a rejection is asserted to *short-circuit* (the handler never runs), the JWT validator is exercised
through an injected fake decoder, and the authorizer is called with hand-built events. No PyJWT, no
broker, no network.
"""

from __future__ import annotations

import asyncio
import base64
import sys
import types

import pytest
from benzene.auth import (
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
