"""Outbound-client decorators: retry and correlation-id, over any MessageSender."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from benzene.core import (
    BatchMessageSender,
    BatchResult,
    FailedMessage,
    MessageSender,
    chunked,
    send_batch_sequentially,
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


# --- batch sends (T2.1): per-message outcomes, chunking, and decorator composition -------------


def _failure(result: BatchResult, index: int) -> FailedMessage:
    """The failure recorded at ``index``, asserting there is one (and narrowing it for mypy)."""
    failure = result.failure_for(index)
    assert failure is not None, f"expected message {index} to have failed"
    return failure


class _ScriptedBatchSender:
    """A BatchMessageSender that fails whichever caller indices each scripted attempt names."""

    def __init__(self, *attempts: dict[int, str]) -> None:
        self._attempts = list(attempts)
        self.batches: list[list[tuple[str, Any]]] = []
        self.headers: list[dict[str, str]] = []

    async def send_message(self, topic, message, headers=None) -> Result:
        return Result.ok()

    async def send_batch(self, messages, headers=None) -> BatchResult:
        self.batches.append(list(messages))
        self.headers.append(dict(headers or {}))
        script = self._attempts[min(len(self.batches) - 1, len(self._attempts) - 1)]
        # The script names *caller* indices; map them onto this call's positions by payload id.
        failures = [
            FailedMessage(position, script[message["n"]])
            for position, (_topic, message) in enumerate(messages)
            if message["n"] in script
        ]
        return BatchResult(tuple(failures))


def _pairs(count: int) -> list[tuple[str, Any]]:
    return [("orders:created", {"n": n}) for n in range(count)]


def test_a_partial_failure_reports_which_messages_failed_and_which_succeeded() -> None:
    # The whole point of the seam: a batch API that fails entry 1 and 3 of 5 must say *which*,
    # because the caller resends exactly those. A single collapsed status would hide it.
    inner = _ScriptedBatchSender({1: Status.SERVICE_UNAVAILABLE, 3: Status.BAD_REQUEST})
    result = asyncio.run(send_batch_sequentially(_ScriptedSender(Result.ok()), []))
    assert result.all_succeeded

    batch = asyncio.run(inner.send_batch(_pairs(5)))
    assert not batch.all_succeeded
    assert not batch  # falsy when anything failed
    assert batch.failed_indexes == (1, 3)
    assert [failure.status for failure in batch.failures] == [
        Status.SERVICE_UNAVAILABLE,
        Status.BAD_REQUEST,
    ]
    assert _failure(batch, 3).status == Status.BAD_REQUEST
    assert batch.failure_for(0) is None  # 0, 2 and 4 succeeded


def test_an_all_succeeded_batch_is_truthy_and_empty() -> None:
    assert BatchResult().all_succeeded
    assert BatchResult()
    assert BatchResult().failed_indexes == ()


def test_chunked_pairs_every_item_with_its_original_index() -> None:
    chunks = list(chunked(list("abcde"), 2))
    assert chunks == [[(0, "a"), (1, "b")], [(2, "c"), (3, "d")], [(4, "e")]]
    assert list(chunked([], 10)) == []  # nothing to send is no API calls at all


def test_chunked_rejects_a_non_positive_size() -> None:
    with pytest.raises(ValueError):
        list(chunked([1, 2, 3], 0))


def test_a_batch_sender_is_a_batch_message_sender() -> None:
    assert isinstance(_ScriptedBatchSender({}), BatchMessageSender)
    assert not isinstance(_ScriptedSender(Result.ok()), BatchMessageSender)


def test_sequential_fallback_reports_per_message_outcomes() -> None:
    # A transport with no batch API still owes the caller per-message outcomes.
    class _Alternating:
        def __init__(self) -> None:
            self.sent: list[str] = []

        async def send_message(self, topic, message, headers=None) -> Result:
            self.sent.append(topic)
            return Result.ok() if message["n"] % 2 == 0 else Result.not_found("no such thing")

    inner = _Alternating()
    result = asyncio.run(send_batch_sequentially(inner, _pairs(4)))
    assert result.failed_indexes == (1, 3)
    assert result.failures[0].status == "not-found"
    assert "no such thing" in (result.failures[0].detail or "")
    assert len(inner.sent) == 4  # every message was attempted; one failure aborts nothing


def test_retry_resends_only_the_retryable_failures_and_keeps_caller_indices() -> None:
    inner = _ScriptedBatchSender(
        {1: Status.SERVICE_UNAVAILABLE, 2: Status.BAD_REQUEST, 4: Status.TIMEOUT},
        {4: Status.TIMEOUT},
        {},
    )
    result = asyncio.run(with_retry(inner, attempts=3).send_batch(_pairs(5)))
    assert result.failed_indexes == (2,)  # 1 and 4 eventually went through; 2 is the caller's fault
    assert [len(batch) for batch in inner.batches] == [5, 2, 1]  # only the transient ones resent
    assert [message["n"] for _topic, message in inner.batches[1]] == [1, 4]
    assert [message["n"] for _topic, message in inner.batches[2]] == [4]


def test_retry_keeps_a_non_retryable_failure_at_its_original_caller_index() -> None:
    inner = _ScriptedBatchSender({2: Status.BAD_REQUEST, 3: Status.SERVICE_UNAVAILABLE}, {})
    result = asyncio.run(with_retry(inner, attempts=3).send_batch(_pairs(5)))
    assert result.failed_indexes == (2,)  # 3 recovered; 2 was never retried
    assert result.failures[0].index == 2
    assert len(inner.batches) == 2


def test_retry_gives_up_on_a_persistently_failing_entry() -> None:
    inner = _ScriptedBatchSender({0: Status.SERVICE_UNAVAILABLE})
    result = asyncio.run(with_retry(inner, attempts=3).send_batch(_pairs(2)))
    assert result.failed_indexes == (0,)
    assert len(inner.batches) == 3  # the full budget, then the caller is told


def test_retry_falls_back_to_sequential_for_a_sender_with_no_batch_api() -> None:
    inner = _ScriptedSender(Result.service_unavailable("down"), Result.ok())
    result = asyncio.run(with_retry(inner, attempts=2).send_batch(_pairs(1)))
    assert result.all_succeeded
    assert len(inner.calls) == 2


def test_correlation_id_is_injected_on_a_batch_too() -> None:
    inner = _ScriptedBatchSender({})
    asyncio.run(with_correlation_id(inner).send_batch(_pairs(3)))
    assert inner.headers[0]["x-correlation-id"]  # one id for the batch, as for one send


def test_batch_decorators_compose() -> None:
    inner = _ScriptedBatchSender({0: Status.SERVICE_UNAVAILABLE}, {})
    sender = with_retry(with_correlation_id(inner), attempts=2)
    result = asyncio.run(sender.send_batch(_pairs(2), headers={"x-correlation-id": "mine"}))
    assert result.all_succeeded
    assert [headers["x-correlation-id"] for headers in inner.headers] == ["mine", "mine"]


def test_failures_are_reported_in_caller_order_however_the_provider_listed_them() -> None:
    # SQS's ``Failed`` list has no defined order, and a size-bounded transport finds an oversized
    # message before the batch around it fails; the caller still reads one deterministic order.
    result = BatchResult(
        (FailedMessage(7, Status.SERVICE_UNAVAILABLE), FailedMessage(2, Status.BAD_REQUEST))
    )
    assert result.failed_indexes == (2, 7)
