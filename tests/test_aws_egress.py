"""AWS outbound clients (SNS, SQS) — the egress wire contract, the mirror of the inbound decode.

Each sender forwards the Benzene topic + headers onto the native message-attribute channel (so
correlation/trace propagation survives the hop) and serializes the body through the shared wire
policy. The harness's ``FakeMessageSender`` bypasses these publish paths, so they need direct cover:
the native call shape, the topic/header tagging, and the ``except -> service-unavailable`` mapping.
Each client takes an injected fake, so this is credential-free — no boto3.
"""

from __future__ import annotations

import asyncio
import io
import json
import sys

import pytest

pytest.importorskip("benzene.aws")

from benzene.aws import (
    TOPIC_ATTRIBUTE,
    EventBridgeMessageSender,
    KinesisMessageSender,
    LambdaMessageSender,
    SnsMessageSender,
    SqsMessageSender,
)
from benzene.core import BatchResult, FailedMessage, encode_body
from benzene.results import Status, is_successful


class _FakeSns:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def publish(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return {"MessageId": "m1"}


class _FakeSqs:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def send_message(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return {"MessageId": "m1"}


def test_sns_sender_tags_topic_propagates_headers_and_serializes_body() -> None:
    fake = _FakeSns()
    result = asyncio.run(
        SnsMessageSender("arn:topic", client=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    call = fake.calls[0]
    assert call["TopicArn"] == "arn:topic"
    assert call["Message"] == encode_body({"id": "1"})  # the shared wire policy, not asdict
    attrs = call["MessageAttributes"]
    assert attrs[TOPIC_ATTRIBUTE] == {"DataType": "String", "StringValue": "orders:created"}
    assert attrs["traceparent"] == {"DataType": "String", "StringValue": "tp"}


def test_sqs_sender_tags_topic_propagates_headers_and_serializes_body() -> None:
    fake = _FakeSqs()
    result = asyncio.run(
        SqsMessageSender("q-url", client=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    call = fake.calls[0]
    assert call["QueueUrl"] == "q-url"
    assert call["MessageBody"] == encode_body({"id": "1"})
    attrs = call["MessageAttributes"]
    assert attrs[TOPIC_ATTRIBUTE]["StringValue"] == "orders:created"
    assert attrs["traceparent"]["StringValue"] == "tp"


def test_sns_sender_maps_a_publish_failure_to_service_unavailable() -> None:
    class Boom:
        def publish(self, **kwargs):
            raise RuntimeError("sns down")

    result = asyncio.run(SnsMessageSender("arn", client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "sns down" in " ".join(result.messages)


def test_sqs_sender_maps_a_send_failure_to_service_unavailable() -> None:
    class Boom:
        def send_message(self, **kwargs):
            raise RuntimeError("sqs down")

    result = asyncio.run(SqsMessageSender("q", client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "sqs down" in " ".join(result.messages)


# --- a missing SDK is a deployment error, not a message outcome (D1) ----------------------------


@pytest.mark.parametrize(
    ("name", "make_sender"),
    [
        ("SnsMessageSender", lambda: SnsMessageSender("arn:topic")),
        ("SqsMessageSender", lambda: SqsMessageSender("q-url")),
        ("EventBridgeMessageSender", lambda: EventBridgeMessageSender("bus")),
        ("KinesisMessageSender", lambda: KinesisMessageSender("stream")),
        ("LambdaMessageSender", lambda: LambdaMessageSender("fn")),
    ],
)
def test_a_missing_boto3_raises_a_teaching_import_error_out_of_send_message(
    monkeypatch: pytest.MonkeyPatch, name: str, make_sender
) -> None:
    # Without the guard the lazy ``import boto3`` is swallowed by the sender's ``except Exception``
    # mapper and every publish quietly becomes service-unavailable — which retry middleware and
    # circuit breakers then hammer. A missing extra must escape as a teaching ImportError instead.
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(ImportError) as excinfo:
        asyncio.run(make_sender().send_message("orders:created", {"id": "1"}))
    message = str(excinfo.value)
    assert name in message
    assert "boto3" in message
    assert "pip install benzene-aws[boto3]" in message


# --- batch sends (T2.1) -------------------------------------------------------------------------
#
# Every fake below models the *real* response shape of its API: SQS/SNS answer with ``Successful``
# and ``Failed`` lists keyed by the request's ``Id``; EventBridge/Kinesis answer positionally with a
# per-entry ``ErrorCode``/``ErrorMessage``. Partial failure is the normal case for all four, so
# these pin that the caller is told exactly which of *its* messages failed.


def _failure(result: BatchResult, index: int) -> FailedMessage:
    """The failure recorded at ``index``, asserting there is one (and narrowing it for mypy)."""
    failure = result.failure_for(index)
    assert failure is not None, f"expected message {index} to have failed"
    return failure


class _FakeBatchSqs:
    """``send_message_batch``: fails whichever caller indices ``fail`` names (Id carries the index)."""

    def __init__(self, fail: dict[int, tuple[str, bool]] | None = None) -> None:
        self.calls: list[dict] = []
        self._fail = fail or {}

    def send_message_batch(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        successful, failed = [], []
        for entry in kwargs["Entries"]:
            index = int(entry["Id"])
            if index in self._fail:
                code, sender_fault = self._fail[index]
                failed.append(
                    {
                        "Id": entry["Id"],
                        "SenderFault": sender_fault,
                        "Code": code,
                        "Message": f"{code} for {index}",
                    }
                )
            else:
                successful.append({"Id": entry["Id"], "MessageId": f"m{index}"})
        return {"Successful": successful, "Failed": failed}


class _FakeBatchSns(_FakeBatchSqs):
    def publish_batch(self, **kwargs) -> dict:
        entries = kwargs["PublishBatchRequestEntries"]
        return self.send_message_batch(TopicArn=kwargs["TopicArn"], Entries=entries)


class _FakeBatchEventBridge:
    """``put_events``: positional per-entry results, exactly as EventBridge answers."""

    def __init__(self, fail: set[int] | None = None) -> None:
        self.calls: list[dict] = []
        self._fail = fail or set()
        self._seen = 0

    def put_events(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        entries, failed = [], 0
        for offset, _entry in enumerate(kwargs["Entries"]):
            index = self._seen + offset
            if index in self._fail:
                failed += 1
                entries.append(
                    {"ErrorCode": "ThrottlingException", "ErrorMessage": f"slow down {index}"}
                )
            else:
                entries.append({"EventId": f"e{index}"})
        self._seen += len(kwargs["Entries"])
        return {"FailedEntryCount": failed, "Entries": entries}


class _FakeBatchKinesis:
    """``put_records``: positional per-record results, ``FailedRecordCount`` gating the walk."""

    def __init__(self, fail: set[int] | None = None) -> None:
        self.calls: list[dict] = []
        self._fail = fail or set()
        self._seen = 0

    def put_records(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        records, failed = [], 0
        for offset, _record in enumerate(kwargs["Records"]):
            index = self._seen + offset
            if index in self._fail:
                failed += 1
                records.append(
                    {
                        "ErrorCode": "ProvisionedThroughputExceededException",
                        "ErrorMessage": f"throttled {index}",
                    }
                )
            else:
                records.append({"SequenceNumber": str(index), "ShardId": "shard-0"})
        self._seen += len(kwargs["Records"])
        return {"FailedRecordCount": failed, "Records": records}


def _batch(count: int) -> list[tuple[str, dict]]:
    return [("orders:created", {"id": str(n)}) for n in range(count)]


def test_sqs_batch_reports_which_messages_failed_and_which_succeeded() -> None:
    # The headline: a partial failure names the caller's own indices, so the caller resends
    # exactly those. Collapsing this to one status would hide the two that need resending.
    fake = _FakeBatchSqs({1: ("InternalError", False), 3: ("InvalidParameterValue", True)})
    result = asyncio.run(SqsMessageSender("q-url", client=fake).send_batch(_batch(5)))

    assert not result.all_succeeded
    assert result.failed_indexes == (1, 3)
    assert _failure(result, 1).status == Status.SERVICE_UNAVAILABLE  # AWS's fault: retryable
    assert _failure(result, 3).status == Status.BAD_REQUEST  # SenderFault: retrying won't help
    assert "InvalidParameterValue" in (_failure(result, 3).detail or "")
    assert result.failure_for(0) is None and result.failure_for(4) is None


def test_sqs_batch_chunks_to_ten_and_keeps_caller_indices_across_chunks() -> None:
    fake = _FakeBatchSqs({0: ("InternalError", False), 24: ("InternalError", False)})
    result = asyncio.run(SqsMessageSender("q-url", client=fake).send_batch(_batch(25)))

    assert [len(call["Entries"]) for call in fake.calls] == [10, 10, 5]  # SQS's documented cap
    assert result.failed_indexes == (0, 24)  # the *caller's* numbering, not the chunk's


def test_sqs_batch_entry_matches_what_a_single_send_puts_on_the_wire() -> None:
    single, batch = _FakeSqs(), _FakeBatchSqs()
    asyncio.run(
        SqsMessageSender("q-url", client=single).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    asyncio.run(
        SqsMessageSender("q-url", client=batch).send_batch(
            [("orders:created", {"id": "1"})], headers={"traceparent": "tp"}
        )
    )
    entry = batch.calls[0]["Entries"][0]
    assert entry["MessageBody"] == single.calls[0]["MessageBody"]
    assert entry["MessageAttributes"] == single.calls[0]["MessageAttributes"]


def test_a_raising_chunk_fails_only_its_own_indices() -> None:
    class _FailsSecondChunk(_FakeBatchSqs):
        def send_message_batch(self, **kwargs):
            if len(self.calls) == 1:
                self.calls.append(kwargs)
                raise RuntimeError("throttled")
            return super().send_message_batch(**kwargs)

    fake = _FailsSecondChunk()
    result = asyncio.run(SqsMessageSender("q", client=fake).send_batch(_batch(25)))
    assert result.failed_indexes == tuple(range(10, 20))  # chunk 2 only; 0-9 and 20-24 stand
    assert "throttled" in (_failure(result, 10).detail or "")


def test_an_empty_batch_makes_no_api_call() -> None:
    fake = _FakeBatchSqs()
    result = asyncio.run(SqsMessageSender("q", client=fake).send_batch([]))
    assert result.all_succeeded
    assert fake.calls == []


def test_sns_batch_maps_per_entry_failures_and_chunks_to_ten() -> None:
    fake = _FakeBatchSns({11: ("InternalError", False)})
    result = asyncio.run(SnsMessageSender("arn:topic", client=fake).send_batch(_batch(12)))
    assert [len(call["Entries"]) for call in fake.calls] == [10, 2]
    assert result.failed_indexes == (11,)
    assert _failure(result, 11).status == Status.SERVICE_UNAVAILABLE


def test_eventbridge_batch_maps_positional_failures_and_chunks_to_ten() -> None:
    # PutEvents has no per-entry id: the i-th response entry pairs with the i-th request entry.
    fake = _FakeBatchEventBridge({2, 14})
    result = asyncio.run(EventBridgeMessageSender("bus", client=fake).send_batch(_batch(20)))
    assert [len(call["Entries"]) for call in fake.calls] == [10, 10]
    assert result.failed_indexes == (2, 14)
    assert "ThrottlingException" in (_failure(result, 14).detail or "")


def test_kinesis_batch_maps_positional_failures_and_chunks_to_five_hundred() -> None:
    fake = _FakeBatchKinesis({7})
    result = asyncio.run(KinesisMessageSender("stream", client=fake).send_batch(_batch(600)))
    assert [len(call["Records"]) for call in fake.calls] == [500, 100]
    assert result.failed_indexes == (7,)
    assert fake.calls[0]["Records"][0]["PartitionKey"] == "orders:created"


def test_a_missing_boto3_raises_out_of_send_batch_too(monkeypatch: pytest.MonkeyPatch) -> None:
    # Same rule as send_message: a forgotten extra is a deployment error, never a failure entry
    # that retry middleware would hammer.
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(ImportError) as excinfo:
        asyncio.run(SqsMessageSender("q-url").send_batch(_batch(2)))
    assert "pip install benzene-aws[boto3]" in str(excinfo.value)


def test_lambda_has_no_batch_invoke_so_it_falls_back_to_sequential_sends() -> None:
    # Documented, not faked: Invoke has no batch API, so send_batch is N invokes whose per-message
    # outcomes are still reported individually.
    class _FakeLambda:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def invoke(self, **kwargs):
            self.calls.append(kwargs)
            index = len(self.calls) - 1
            body = (
                {"statusCode": "not-found", "isSuccessful": False, "body": "{}"}
                if index == 1
                else {"statusCode": "ok", "body": "{}"}
            )
            return {"Payload": io.BytesIO(json.dumps(body).encode("utf-8"))}

    fake = _FakeLambda()
    result = asyncio.run(LambdaMessageSender("fn", client=fake).send_batch(_batch(3)))
    assert len(fake.calls) == 3
    assert result.failed_indexes == (1,)
    assert _failure(result, 1).status == "not-found"


def test_an_unserializable_entry_fails_alone_and_is_reported_once() -> None:
    # A serialization failure is that entry's `bad-request`, never an abort of the batch — and when
    # the call around it also fails, the entry must not be reported twice at two different statuses.
    def picky(message):
        if message["id"] == "1":
            raise TypeError("not JSON serializable")
        return encode_body(message)

    fake = _FakeBatchSqs()
    result = asyncio.run(SqsMessageSender("q", client=fake, serializer=picky).send_batch(_batch(3)))
    assert result.failed_indexes == (1,)
    assert _failure(result, 1).status == Status.BAD_REQUEST
    assert [entry["Id"] for entry in fake.calls[0]["Entries"]] == ["0", "2"]  # the rest still went

    class Boom(_FakeBatchSqs):
        def send_message_batch(self, **kwargs):
            raise RuntimeError("sqs down")

    boom = asyncio.run(SqsMessageSender("q", client=Boom(), serializer=picky).send_batch(_batch(3)))
    assert boom.failed_indexes == (0, 1, 2)  # each exactly once
    assert (
        _failure(boom, 1).status == Status.BAD_REQUEST
    )  # still the caller's fault, not the call's
    assert _failure(boom, 0).status == Status.SERVICE_UNAVAILABLE
