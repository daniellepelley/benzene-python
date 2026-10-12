"""Outbound-client decorators: retry and correlation-id, over any MessageSender."""

from __future__ import annotations

import asyncio

from benzene.core import (
    MessageSender,
    with_correlation_id,
    with_retry,
)
from benzene.results import Result, Status


class _ScriptedSender:
    """A MessageSender that returns a scripted sequence of results and records each call's headers."""

    def __init__(self, *results: Result) -> None:
        self._results = list(results)
        self.calls: list[dict] = []

    async def send_message(self, topic, message, headers=None) -> Result:
        self.calls.append(dict(headers or {}))
        return self._results[min(len(self.calls) - 1, len(self._results) - 1)]


def test_retry_gives_up_returns_last_on_persistent_transient_failure() -> None:
    inner = _ScriptedSender(Result.service_unavailable("down"))
    result = asyncio.run(with_retry(inner, attempts=3).send_message("t", {}))
    assert result.status == "service-unavailable"
    assert len(inner.calls) == 3  # tried the full budget


def test_retry_returns_on_first_success() -> None:
    inner = _ScriptedSender(Result.service_unavailable("down"), Result.ok({"n": 1}))
    result = asyncio.run(with_retry(inner, attempts=5).send_message("t", {}))
    assert result.status == "ok"
    assert len(inner.calls) == 2  # stopped as soon as it succeeded


def test_retry_does_not_retry_a_non_transient_failure() -> None:
    inner = _ScriptedSender(Result.not_found("nope"))
    result = asyncio.run(with_retry(inner).send_message("t", {}))
    assert result.status == "not-found"
    assert len(inner.calls) == 1  # not-found won't get better by retrying


def test_retry_backoff_runs_between_attempts() -> None:
    delays: list[int] = []

    async def backoff(attempt: int) -> None:
        delays.append(attempt)

    inner = _ScriptedSender(
        Result.failure(Status.TIMEOUT, "slow"), Result.failure(Status.TIMEOUT, "slow"), Result.ok()
    )
    asyncio.run(with_retry(inner, attempts=3, backoff=backoff).send_message("t", {}))
    assert delays == [1, 2]  # once before each re-attempt


def test_it_is_a_message_sender() -> None:
    assert isinstance(with_retry(_ScriptedSender(Result.ok())), MessageSender)
    assert isinstance(with_correlation_id(_ScriptedSender(Result.ok())), MessageSender)


def test_correlation_id_injected_when_absent() -> None:
    inner = _ScriptedSender(Result.ok())
    asyncio.run(with_correlation_id(inner).send_message("t", {}))
    assert inner.calls[0]["x-correlation-id"]  # a fresh id was added


def test_correlation_id_preserved_when_present() -> None:
    inner = _ScriptedSender(Result.ok())
    asyncio.run(with_correlation_id(inner).send_message("t", {}, headers={"x-correlation-id": "mine"}))
    assert inner.calls[0]["x-correlation-id"] == "mine"  # caller's id left untouched


def test_decorators_compose() -> None:
    inner = _ScriptedSender(Result.service_unavailable("x"), Result.ok())
    sender = with_retry(with_correlation_id(inner), attempts=3)
    result = asyncio.run(sender.send_message("t", {}))
    assert result.status == "ok"
    assert len(inner.calls) == 2
    assert all("x-correlation-id" in call for call in inner.calls)  # id on every attempt


# --- a call that fails before any reply (transport-bindings §1, spec 1.1) -------------------------


def test_failed_call_result_maps_a_deadline_to_timeout_and_anything_else_to_service_unavailable() -> None:
    import urllib.error

    from benzene.core import failed_call_result

    refused = failed_call_result(ConnectionRefusedError(111, "Connection refused"), "POST http://x")
    assert refused.status == Status.SERVICE_UNAVAILABLE
    assert refused.messages == ("POST http://x: [Errno 111] Connection refused",)
    assert not refused.is_successful

    assert failed_call_result(TimeoutError("timed out")).status == Status.TIMEOUT
    # urllib wraps a connect timeout in URLError, keeping the socket timeout as its reason.
    assert failed_call_result(urllib.error.URLError(TimeoutError("timed out"))).status == Status.TIMEOUT
    assert failed_call_result(urllib.error.URLError(ConnectionRefusedError())).status == (
        Status.SERVICE_UNAVAILABLE
    )
    # A message-less error still says what happened.
    assert failed_call_result(ConnectionResetError()).messages == ("ConnectionResetError",)


def test_is_timeout_recognises_sdk_timeouts_it_cannot_import_by_name_and_by_cause() -> None:
    from benzene.core import is_timeout

    class ReadTimeoutError(Exception):  # botocore's name
        pass

    class ConnectTimeout(Exception):  # httpx / requests style
        pass

    class DeadlineExceeded(Exception):  # google-api-core's name
        pass

    class SdkError(Exception):
        pass

    assert is_timeout(ReadTimeoutError())
    assert is_timeout(ConnectTimeout())
    assert is_timeout(DeadlineExceeded())
    assert is_timeout(asyncio.TimeoutError())
    wrapped = SdkError("request failed")
    wrapped.__cause__ = TimeoutError()
    assert is_timeout(wrapped)
    assert not is_timeout(SdkError("connection refused"))
    assert not is_timeout(ValueError("bad"))


def test_retry_retries_a_dead_peer_now_that_it_is_a_result() -> None:
    # The point of a Result over an exception: the transient-failure decorators see it.
    from benzene.core import failed_call_result

    inner = _ScriptedSender(
        failed_call_result(ConnectionRefusedError()), Result.ok({"id": 1})
    )
    result = asyncio.run(with_retry(inner, attempts=3).send_message("t", {}))
    assert result.is_successful
    assert len(inner.calls) == 2
