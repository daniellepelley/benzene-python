"""The claim check — sending a payload the transport is too small to carry.

The tests are written as the story the package exists to tell:

1. **The gap.** Every transport caps a message (SQS/SNS/EventBridge 256 KB, Azure Queue Storage
   64 KB). A handler with a legitimately larger message cannot send it at all: the publish is
   refused, loudly and immediately, and there is nothing the application can do about it inside
   Benzene.
2. **The fix.** ``with_claim_check(sender, store)`` in front of the same transport: the body goes to
   a blob store, a tiny placeholder goes on the wire, and the reference travels in the
   ``benzene-claim-check`` header. On the far side one middleware puts the real body back before the
   router maps it, so the handler sees exactly the request that was sent.
3. **The wire surface.** The header name and the placeholder shape are a *cross-port* contract with
   the .NET port (``Benzene.ClaimCheck``): ``benzene-claim-check`` and ``{"_benzeneClaimCheck":
   "<ref>"}``, the key verbatim and never camel-cased by the wire-naming encoder. These assertions
   are the byte-for-byte guard.
4. **Failing loud.** A reference that resolves to nothing raises ``ClaimCheckNotFound``; a reference
   that belongs to somebody else's store raises ``ClaimCheckStoreMismatch`` *without* touching the
   backing client — two different failures, deliberately not merged, because the second one is a
   security boundary and the first one is an expiry.

Everything runs in memory on a manual clock, with fake S3/Blob clients — no cloud SDK, no sleeping.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from benzene.claim_check import (
    CLAIM_CHECK_HEADER,
    DEFAULT_THRESHOLD_BYTES,
    PLACEHOLDER_KEY,
    BlobClaimCheckStore,
    ClaimCheckMessageSender,
    ClaimCheckNotFound,
    ClaimCheckStore,
    ClaimCheckStoreMismatch,
    InMemoryClaimCheckStore,
    S3ClaimCheckStore,
    claim_check_interception,
    claim_check_placeholder,
    with_claim_check,
)
from benzene.core import (
    Context,
    MessageSender,
    MiddlewarePipeline,
    Registry,
    encode_body,
    message_router,
)
from benzene.results import Result, Status

from ._async import run

TOPIC = "documents:ingest"

# A payload that is genuinely over every transport limit in play: ~300 KB of JSON.
BIG = {"documentId": "doc-1", "pages": ["x" * 1024 for _ in range(300)]}
SMALL = {"documentId": "doc-1", "pages": ["one page"]}


class SizeCappedSender:
    """A transport that refuses an oversized message, the way SQS/SNS/EventBridge actually do.

    ``sent`` records ``(topic, message, headers)`` for every accepted publish.
    """

    #: SQS/SNS/EventBridge. Azure Queue Storage is 64 KB; the shape of the failure is identical.
    LIMIT = 256 * 1024

    def __init__(self, limit: int | None = None) -> None:
        self.limit = limit if limit is not None else self.LIMIT
        self.sent: list[tuple[str, Any, dict[str, str] | None]] = []

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        size = len(encode_body(message).encode("utf-8"))
        if size > self.limit:
            return Result.failure(
                Status.BAD_REQUEST, f"message of {size} bytes exceeds the {self.limit} byte limit"
            )
        self.sent.append((topic, message, headers))
        return Result.ok()


class RecordingStore:
    """A :class:`ClaimCheckStore` that counts calls, for "never touched" assertions."""

    def __init__(self, *, fail_on_put: Exception | None = None) -> None:
        self.puts: list[tuple[str, str]] = []
        self.gets: list[str] = []
        self.deletes: list[str] = []
        self._bodies: dict[str, str] = {}
        self._fail_on_put = fail_on_put

    async def put(self, body: str, topic: str) -> str:
        if self._fail_on_put is not None:
            raise self._fail_on_put
        self.puts.append((body, topic))
        reference = f"memory://{topic}/{len(self.puts)}"
        self._bodies[reference] = body
        return reference

    async def get(self, reference: str) -> str | None:
        self.gets.append(reference)
        return self._bodies.get(reference)

    async def delete(self, reference: str) -> None:
        self.deletes.append(reference)
        self._bodies.pop(reference, None)


def recording_registry(seen: list[Any]) -> Registry:
    """A registry whose one handler records the request it was given."""

    async def handle(request: Any) -> Result:
        seen.append(request)
        return Result.ok()

    return Registry().register(TOPIC, handle)


def refusing_registry() -> Registry:
    """A registry whose handler must never run — the guard for "never a silent skip"."""

    async def handle(request: Any) -> Result:  # pragma: no cover - must never run
        raise AssertionError("a placeholder must never reach a handler")

    return Registry().register(TOPIC, handle)


def a_pipeline(store: ClaimCheckStore, registry: Registry, **options: Any) -> MiddlewarePipeline:
    """The receiving side: hydrate, then route — hydration ahead of the deserialisation boundary."""
    return (
        MiddlewarePipeline()
        .use(claim_check_interception(store, **options))
        .use(message_router(registry))
    )


# --------------------------------------------------------------------------------------------
# 1. The gap, and the fix
# --------------------------------------------------------------------------------------------


def test_an_oversized_payload_is_simply_unsendable_without_a_claim_check() -> None:
    transport = SizeCappedSender()

    result = run(transport.send_message(TOPIC, BIG))

    assert not result.is_successful
    assert "exceeds the 262144 byte limit" in result.messages[0]
    assert transport.sent == []  # nothing went anywhere


def test_the_same_payload_sends_once_the_claim_check_is_in_front() -> None:
    transport = SizeCappedSender()
    sender = with_claim_check(transport, InMemoryClaimCheckStore())

    result = run(sender.send_message(TOPIC, BIG))

    assert result.is_successful
    topic, body, headers = transport.sent[0]
    assert topic == TOPIC
    assert headers is not None and headers[CLAIM_CHECK_HEADER].startswith("memory://")
    assert len(encode_body(body).encode("utf-8")) < 200  # a placeholder, not a document


def test_the_payload_round_trips_end_to_end_through_a_real_pipeline() -> None:
    store = InMemoryClaimCheckStore()
    transport = SizeCappedSender()
    sender = with_claim_check(transport, store)
    seen: list[Any] = []
    registry = recording_registry(seen)

    assert run(sender.send_message(TOPIC, BIG)).is_successful
    _, placeholder, headers = transport.sent[0]

    # The receiving side gets only what crossed the wire: the placeholder body and the headers.
    context = Context(TOPIC, json.loads(encode_body(placeholder)), headers)
    run(a_pipeline(store, registry).handle(context))

    assert context.result is not None and context.result.is_successful
    assert seen == [BIG]  # the handler saw the original request, not the placeholder


# --------------------------------------------------------------------------------------------
# 2. The wire surface — matched to .NET byte-for-byte
# --------------------------------------------------------------------------------------------


def test_the_header_name_is_the_dotnet_literal() -> None:
    # Benzene.ClaimCheck/ClaimCheckHeaders.cs:
    #     public const string ClaimCheck = "benzene-claim-check";
    assert CLAIM_CHECK_HEADER == "benzene-claim-check"


def test_the_placeholder_key_is_the_dotnet_literal_and_is_never_camel_cased() -> None:
    # Benzene.ClaimCheck/ClaimCheckPlaceholder.cs:
    #     public string _benzeneClaimCheck { get; set; }
    assert PLACEHOLDER_KEY == "_benzeneClaimCheck"
    assert claim_check_placeholder("s3://b/k") == {"_benzeneClaimCheck": "s3://b/k"}
    # The wire-naming encoder writes application dict keys verbatim: the leading underscore has no
    # case, so the key survives every serializer's naming policy unchanged (.NET's own rationale).
    assert encode_body(claim_check_placeholder("s3://b/k")) == '{"_benzeneClaimCheck": "s3://b/k"}'


def test_the_body_on_the_wire_is_exactly_the_placeholder() -> None:
    transport = SizeCappedSender()
    store = InMemoryClaimCheckStore()

    run(with_claim_check(transport, store).send_message(TOPIC, BIG))

    _, body, headers = transport.sent[0]
    assert headers is not None
    reference = headers[CLAIM_CHECK_HEADER]
    assert body == {"_benzeneClaimCheck": reference}
    assert json.loads(encode_body(body)) == {"_benzeneClaimCheck": reference}


def test_the_reference_is_uri_shaped_and_the_stored_body_is_the_wire_body() -> None:
    store = InMemoryClaimCheckStore()
    transport = SizeCappedSender()

    run(with_claim_check(transport, store).send_message(TOPIC, BIG))
    reference = transport.sent[0][2][CLAIM_CHECK_HEADER]  # type: ignore[index]

    assert reference.startswith("memory://documents%3Aingest/")
    assert run(store.get(reference)) == encode_body(BIG)


def test_caller_headers_survive_alongside_the_claim_check_header() -> None:
    transport = SizeCappedSender()
    sender = with_claim_check(transport, InMemoryClaimCheckStore())

    run(sender.send_message(TOPIC, BIG, {"idempotency-key": "abc"}))

    headers = transport.sent[0][2]
    assert headers is not None
    assert headers["idempotency-key"] == "abc"
    assert CLAIM_CHECK_HEADER in headers


# --------------------------------------------------------------------------------------------
# 3. The threshold
# --------------------------------------------------------------------------------------------


def test_the_default_threshold_matches_dotnet() -> None:
    # ClaimCheckOptions.DefaultThresholdBytes = 192 * 1024
    assert DEFAULT_THRESHOLD_BYTES == 192 * 1024


def test_a_small_payload_is_sent_inline_and_the_store_is_never_touched() -> None:
    store = RecordingStore()
    transport = SizeCappedSender()

    result = run(with_claim_check(transport, store).send_message(TOPIC, SMALL))

    assert result.is_successful
    assert store.puts == []
    # Untouched means untouched: the same object, and the caller's own headers (here, none at all).
    assert transport.sent == [(TOPIC, SMALL, None)]


def test_the_threshold_is_configurable() -> None:
    store = RecordingStore()
    transport = SizeCappedSender()
    sender = ClaimCheckMessageSender(transport, store, threshold_bytes=8)

    run(sender.send_message(TOPIC, SMALL))

    assert len(store.puts) == 1


def test_always_offload_ignores_the_threshold() -> None:
    store = RecordingStore()
    transport = SizeCappedSender()
    sender = ClaimCheckMessageSender(transport, store, always_offload=True)

    run(sender.send_message(TOPIC, {"a": 1}))

    assert len(store.puts) == 1
    assert transport.sent[0][1] == {PLACEHOLDER_KEY: f"memory://{TOPIC}/1"}


def test_the_threshold_counts_utf8_bytes_not_characters() -> None:
    """.NET measures ``Encoding.UTF8.GetByteCount``; a character count would under-read the wire.

    Invisible with the default encoder (``json.dumps`` escapes non-ASCII to an ASCII escape), and
    very visible with a serializer that does not — the sort a service picks to keep a
    document readable, and exactly where a naive ``len(body)`` would send an oversized message.
    """
    store = RecordingStore()
    transport = SizeCappedSender()

    def unescaped(payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False)

    payload = {"text": "三" * 40}  # 40 characters, 120 UTF-8 bytes
    sender = ClaimCheckMessageSender(transport, store, threshold_bytes=100, serializer=unescaped)

    run(sender.send_message(TOPIC, payload))

    body = unescaped(payload)
    assert len(body) < 100 < len(body.encode("utf-8"))  # under by characters, over by bytes
    assert len(store.puts) == 1  # measured in bytes, so it offloaded


# --------------------------------------------------------------------------------------------
# 4. Offload-then-send is not atomic — and fails in the safe direction
# --------------------------------------------------------------------------------------------


def test_a_store_failure_propagates_and_the_transport_is_never_called() -> None:
    store = RecordingStore(fail_on_put=RuntimeError("bucket is on fire"))
    transport = SizeCappedSender()

    with pytest.raises(RuntimeError, match="bucket is on fire"):
        run(with_claim_check(transport, store).send_message(TOPIC, BIG))

    assert transport.sent == []  # a failed put means the send never happened


# --------------------------------------------------------------------------------------------
# 5. Hydration
# --------------------------------------------------------------------------------------------


def test_a_message_without_the_header_passes_through_without_touching_the_store() -> None:
    store = RecordingStore()
    seen: list[Any] = []
    registry = recording_registry(seen)

    context = Context(TOPIC, SMALL, {"idempotency-key": "abc"})
    run(a_pipeline(store, registry).handle(context))

    assert store.gets == []
    assert seen == [SMALL]


def test_hydration_replaces_the_request_before_the_router_maps_it() -> None:
    store = InMemoryClaimCheckStore()
    reference = run(store.put(encode_body(BIG), TOPIC))
    seen: list[Any] = []
    registry = recording_registry(seen)

    context = Context(TOPIC, claim_check_placeholder(reference), {CLAIM_CHECK_HEADER: reference})
    run(a_pipeline(store, registry).handle(context))

    assert seen == [BIG]


def test_a_missing_reference_raises_claim_check_not_found() -> None:
    store = InMemoryClaimCheckStore()

    async def next_() -> None:  # pragma: no cover - must never run
        raise AssertionError("the pipeline must not continue past an unresolvable claim check")

    middleware = claim_check_interception(store)
    context = Context(TOPIC, {}, {CLAIM_CHECK_HEADER: "memory://documents/never-stored"})

    with pytest.raises(ClaimCheckNotFound) as raised:
        run(middleware(context, next_))

    assert raised.value.reference == "memory://documents/never-stored"
    assert "never-stored" in str(raised.value)


def test_an_unresolvable_claim_check_fails_the_message_through_the_pipeline() -> None:
    """Never a silent skip: the transport's nack → redelivery → DLQ path applies."""
    context = Context(TOPIC, {}, {CLAIM_CHECK_HEADER: "memory://documents/gone"})
    run(a_pipeline(InMemoryClaimCheckStore(), refusing_registry()).handle(context))

    assert context.result is not None
    assert not context.result.is_successful
    assert "memory://documents/gone" in context.result.messages[0]


def test_a_foreign_reference_is_a_mismatch_not_a_not_found() -> None:
    store = InMemoryClaimCheckStore()

    with pytest.raises(ClaimCheckStoreMismatch) as raised:
        run(store.get("s3://someone-elses-bucket/key"))

    assert raised.value.reference == "s3://someone-elses-bucket/key"
    # The two failures are deliberately not merged: one is expiry, one is a security boundary.
    assert not isinstance(raised.value, ClaimCheckNotFound)
    assert not issubclass(ClaimCheckStoreMismatch, ClaimCheckNotFound)


def test_a_body_that_is_not_json_names_the_serializer_coupling() -> None:
    store = InMemoryClaimCheckStore()
    reference = run(store.put("<order><id>1</id></order>", TOPIC))

    async def next_() -> None:  # pragma: no cover - must never run
        raise AssertionError("a body that cannot be decoded must not reach the handler")

    context = Context(TOPIC, {}, {CLAIM_CHECK_HEADER: reference})
    with pytest.raises(ValueError, match="serializer"):
        run(claim_check_interception(store)(context, next_))


# --------------------------------------------------------------------------------------------
# 6. Retention — no delete-on-consume, ever
# --------------------------------------------------------------------------------------------


def test_hydration_never_deletes_the_stored_payload() -> None:
    """Fan-out and redelivery both re-read the same reference; a read-time delete would starve them."""
    store = InMemoryClaimCheckStore()
    reference = run(store.put(encode_body(BIG), TOPIC))
    seen: list[Any] = []
    registry = recording_registry(seen)

    for _ in range(3):  # two fan-out siblings and a redelivery
        context = Context(TOPIC, {}, {CLAIM_CHECK_HEADER: reference})
        run(a_pipeline(store, registry).handle(context))

    assert seen == [BIG, BIG, BIG]
    assert run(store.get(reference)) == encode_body(BIG)


def test_delete_is_an_operator_tool_not_part_of_the_read_path() -> None:
    store = InMemoryClaimCheckStore()
    reference = run(store.put("body", TOPIC))

    run(store.delete(reference))

    assert run(store.get(reference)) is None


def test_the_in_memory_store_expires_on_its_ttl() -> None:
    now = [1000.0]
    store = InMemoryClaimCheckStore(ttl=60.0, clock=lambda: now[0])
    reference = run(store.put("body", TOPIC))

    now[0] += 59.0
    assert run(store.get(reference)) == "body"

    now[0] += 2.0
    assert run(store.get(reference)) is None  # expired: a miss, not a mismatch


def test_in_memory_references_are_independent() -> None:
    store = InMemoryClaimCheckStore()

    first = run(store.put("one", TOPIC))
    second = run(store.put("two", TOPIC))

    assert first != second
    assert run(store.get(first)) == "one"
    assert run(store.get(second)) == "two"


# --------------------------------------------------------------------------------------------
# 7. The cloud stores, against fakes
# --------------------------------------------------------------------------------------------


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[dict[str, Any]] = []
        self.thread: list[str] = []

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.thread.append(_current_thread())
        self.puts.append(kwargs)
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]
        return {}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.thread.append(_current_thread())
        try:
            body = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError:
            raise _client_error("NoSuchKey") from None
        return {"Body": _Streaming(body)}

    def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        self.objects.pop((kwargs["Bucket"], kwargs["Key"]), None)
        return {}


class _Streaming:
    """Stands in for a boto3 ``StreamingBody`` / an Azure ``StorageStreamDownloader``."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def readall(self) -> bytes:
        return self._body


class _FakeClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _client_error(code: str) -> Exception:
    return _FakeClientError(code)


def _current_thread() -> str:
    import threading

    return threading.current_thread().name


def test_the_s3_store_key_shape_and_reference() -> None:
    client = FakeS3()
    store = S3ClaimCheckStore("payload-bucket", client=client, key=lambda: "abc123")

    reference = run(store.put("body", TOPIC))

    assert reference == "s3://payload-bucket/claim-checks/documents:ingest/abc123"
    put = client.puts[0]
    assert put["Bucket"] == "payload-bucket"
    assert put["Key"] == "claim-checks/documents:ingest/abc123"
    assert put["Body"] == b"body"
    assert run(store.get(reference)) == "body"


def test_the_s3_store_maps_a_missing_object_to_none() -> None:
    store = S3ClaimCheckStore("payload-bucket", client=FakeS3())

    assert run(store.get("s3://payload-bucket/claim-checks/documents:ingest/gone")) is None


def test_the_s3_store_refuses_a_foreign_reference_without_calling_s3() -> None:
    client = FakeS3()
    store = S3ClaimCheckStore("payload-bucket", client=client)

    for foreign in (
        "s3://another-bucket/claim-checks/x",  # a bucket this store was not configured for
        "s3://payload-bucket/elsewhere/x",  # outside the configured prefix
        "memory://documents/x",  # a foreign scheme entirely
        "https://evil.example/payload",  # an attacker-supplied location
    ):
        with pytest.raises(ClaimCheckStoreMismatch):
            run(store.get(foreign))

    assert client.thread == []  # the client was never touched


def test_the_s3_store_runs_the_blocking_sdk_off_the_event_loop() -> None:
    client = FakeS3()
    store = S3ClaimCheckStore("payload-bucket", client=client)

    async def exercise() -> None:
        reference = await store.put("body", TOPIC)
        await store.get(reference)

    asyncio.run(exercise())

    assert client.thread and all(name != "MainThread" for name in client.thread)


class FakeContainer:
    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self.uploads: list[tuple[str, bytes]] = []

    def upload_blob(self, name: str, data: bytes, **kwargs: Any) -> None:
        self.uploads.append((name, data))
        self.blobs[name] = data

    def download_blob(self, name: str) -> Any:
        if name not in self.blobs:
            raise ResourceNotFoundError(name)
        return _Streaming(self.blobs[name])

    def delete_blob(self, name: str) -> None:
        self.blobs.pop(name, None)


class ResourceNotFoundError(Exception):
    """Stands in for ``azure.core.exceptions.ResourceNotFoundError``, matched by type name.

    The store matches the SDK's exception by name rather than importing it, so this branch stays
    testable with no Azure SDK installed — which is the whole point of the injected-client seam.
    """


def test_the_blob_store_key_shape_and_reference() -> None:
    container = FakeContainer()
    store = BlobClaimCheckStore("payloads", client=container, key=lambda: "abc123")

    reference = run(store.put("body", TOPIC))

    assert reference == "azblob://payloads/claim-checks/documents:ingest/abc123"
    assert container.uploads == [("claim-checks/documents:ingest/abc123", b"body")]
    assert run(store.get(reference)) == "body"


def test_the_blob_store_refuses_a_foreign_reference() -> None:
    store = BlobClaimCheckStore("payloads", client=FakeContainer())

    with pytest.raises(ClaimCheckStoreMismatch):
        run(store.get("azblob://another-container/claim-checks/x"))


def test_the_blob_store_maps_a_missing_blob_to_none() -> None:
    store = BlobClaimCheckStore("payloads", client=FakeContainer())

    assert run(store.get("azblob://payloads/claim-checks/documents:ingest/gone")) is None


# --------------------------------------------------------------------------------------------
# 8. The protocol
# --------------------------------------------------------------------------------------------


def test_the_offload_decorator_is_a_drop_in_message_sender() -> None:
    """The whole point of the decorator idiom: nothing downstream of the call site changes."""
    sender = with_claim_check(SizeCappedSender(), InMemoryClaimCheckStore())
    as_the_port: MessageSender = sender  # a ClaimCheckMessageSender *is* a MessageSender

    assert isinstance(as_the_port, MessageSender)
    assert isinstance(sender.inner, SizeCappedSender)  # and it still names the transport it wraps


def test_every_shipped_store_satisfies_the_protocol() -> None:
    assert isinstance(InMemoryClaimCheckStore(), ClaimCheckStore)
    assert isinstance(S3ClaimCheckStore("b", client=FakeS3()), ClaimCheckStore)
    assert isinstance(BlobClaimCheckStore("c", client=FakeContainer()), ClaimCheckStore)
    assert isinstance(RecordingStore(), ClaimCheckStore)
