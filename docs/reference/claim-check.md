# `benzene.claim_check`

The **claim check**: when a message is too big for the transport, put the body in a blob store and
send a reference in its place. **Distribution: `benzene-claim-check` (depends only on
`benzene-core`).**

```bash
pip install benzene-claim-check              # in-memory store + both middleware halves
pip install "benzene-claim-check[s3]"        # + boto3, for the S3 store
pip install "benzene-claim-check[azure]"     # + azure-storage-blob, for the Blob store
```

## Overview

Every transport caps a message:

| Transport | Limit |
|---|---|
| SQS, SNS, EventBridge, Service Bus standard | 256 KB |
| Event Grid | 1 MB |
| Azure Queue Storage | 64 KB |

A handler with a legitimately larger message — a document, an image manifest, a batch of a few
thousand rows — cannot send it at all. The publish is refused, loudly and immediately, and there is
nothing the application can do about it from inside Benzene:

```python
await sender.send_message("documents:ingest", a_300kb_document)
# → bad-request: message of 308434 bytes exceeds the 262144 byte limit
```

The claim check is the standard answer: the body goes somewhere with no size limit, and a small,
opaque reference to it goes on the wire. This package is a **pair** — an outbound half that offloads
and an inbound half that hydrates — plus a pluggable store.

| Part | What it is | Where it lives |
|---|---|---|
| **Offload** | `with_claim_check(sender, store)` — a decorator over the one `MessageSender` seam. Stores the body, sends a placeholder. | `sender.py` |
| **Hydrate** | `claim_check_interception(store)` — middleware ahead of the message router. Resolves the reference, restores `context.request`. | `interception.py` |
| **Store** | `ClaimCheckStore` — a Protocol. `InMemoryClaimCheckStore` always; `S3ClaimCheckStore` / `BlobClaimCheckStore` behind extras. | `store.py`, `memory.py`, `s3.py`, `blob.py` |

## Wiring it

```python
from benzene.claim_check import S3ClaimCheckStore, claim_check_interception, with_claim_check

store = S3ClaimCheckStore("my-payload-bucket")        # both sides need the same store

# Sending: wrap the transport. The call site never changes.
sender = with_correlation_id(with_claim_check(with_retry(sqs_sender), store))

# Receiving: before the message router, after the observability prelude.
pipeline.use(tracing_interception(...))
pipeline.use(claim_check_interception(store))
pipeline.use(message_router(registry))
```

**Composition order is load-bearing on the sending side.** Anything *outside* the claim check sees
the real message — which is what header-stampers (correlation id) and the outbox's capture want.
Anything *inside* it sees the placeholder — which is what retry wants: a retried send re-sends the
same tiny reference instead of uploading the payload again and orphaning the first copy.

## What crosses the wire

This is a **cross-port contract**, matched byte-for-byte to .NET's `Benzene.ClaimCheck` so that a
payload a Python service offloads is hydratable by a .NET consumer and vice versa.

| | Value | Where it comes from |
|---|---|---|
| Header | `benzene-claim-check` | .NET `ClaimCheckHeaders.ClaimCheck`; specified in `wire-contracts.md` §2 as a **Tier C** (add-on) header |
| Placeholder body | `{"_benzeneClaimCheck": "<ref>"}` | .NET `ClaimCheckPlaceholder._benzeneClaimCheck` |
| Reference | `scheme://location/key` — `memory://…`, `s3://bucket/key`, `azblob://container/key` | `wire-contracts.md` §2.1 |

The leading underscore in the placeholder key is deliberate and is .NET's own reasoning: an
underscore has no upper or lower case, so the key round-trips identically through any serializer's
naming policy. Benzene's wire encoder writes application `dict` keys verbatim, so the placeholder is
built as a plain dict and never passed through a field-name camel-caser.

**The header is authoritative.** A consumer with the add-on wired never reads the placeholder body;
it is there for a human looking at a raw queue message, and for a non-Benzene consumer that at least
learns *why* the body it expected is not there.

### Where the canonical specification stands

The **header is specified**: `docs/specification/wire-contracts.md` §2 lists `benzene-claim-check` as
a Tier C add-on header, and §2.1 gives the reference shape, the fail-loud rule, the store-boundary
rule and the ban on delete-on-consume. This package implements that section.

The **placeholder body is not specified** — §2.1 states in as many words that the body of an
offloaded message is unspecified and that a consumer must not interpret it. Matching .NET's
`{"_benzeneClaimCheck": …}` is therefore **this port moving with .NET slightly ahead of the written
spec**: deliberate, so that the bytes on the wire are identical between the two ports, and recorded
here (and in `benzene/claim_check/wire.py`) so it can be proposed upstream as a SHOULD rather than
becoming an unwritten habit shared by two ports. Nothing in either port *reads* the placeholder, which
is what keeps the unwritten part harmless in the meantime.

Tier C also means this is an **explicit deployment agreement**, not a universal capability: a service
that offloads is interoperable only with consumers that have wired the add-on *and* share access to
the same store.

## The threshold

`DEFAULT_THRESHOLD_BYTES` is **192 KiB (196,608 bytes)**, matching .NET's
`ClaimCheckOptions.DefaultThresholdBytes`: the smallest limit in the 256 KB family, less headroom for
message attributes and the envelope, which count against the same limit. A message whose serialized
body is **at or above** the threshold is offloaded; below it, the message is passed to the transport
untouched and the store is never contacted.

Two knobs, both per-sender:

```python
with_claim_check(sqs, store, threshold_bytes=48 * 1024)   # Azure Queue Storage is 64 KB
with_claim_check(sqs, store, always_offload=True)         # this topic is always large
```

There is no single number that fits every transport, which is why it is a parameter rather than a
constant in the code path. A service on Azure Queue Storage **must** lower it.

## Serializer consistency

`serializer` measures *and* stores the body, and defaults to `benzene.core.encode_body` — the single
wire-encoding entry point every outbound transport in this port uses. So by default the bytes measured
and stored are exactly the bytes an inline send would have produced.

If you gave the transport a custom serializer (`SnsMessageSender(arn, serializer=my_dumps)`), pass the
same one to the claim check, or the size decision is made against a body the transport would never
have sent.

.NET documents this as a real, load-bearing coupling, and it is — but **Python is safer on the other
half of it**. In .NET the stored body is handed back to the receiving transport's own deserializer, so
a serializer mismatch can produce a payload that deserializes to the wrong thing. Here the stored body
is decoded by this package's own hydrate middleware (`decoder`, `json.loads` by default), so a
mismatch cannot silently corrupt a payload: it is a loud `ValueError` whose message names the coupling
and tells you which two arguments have to agree.

## Retention: no delete-on-consume, ever

**Nothing in this package deletes a stored payload on the read path, and that is not negotiable.**
`wire-contracts.md` §2.1 states it as a prohibition, and the reasons are concrete:

- a fan-out transport (SNS, Pub/Sub) delivers one offloaded message to several independent consumers
  — the first one to finish would starve its siblings;
- every at-least-once transport redelivers, and this port spends real effort on at-least-once
  semantics (the outbox, idempotency) — a read-time delete would turn a transient handler failure
  into a *permanently* unhydratable message, i.e. it would convert a retryable failure into a poison
  one. That is strictly worse than the leak it was trying to fix.

So retention is **store-side expiry owned by infrastructure**: an S3 Lifecycle rule or an Azure Blob
lifecycle-management policy on the store's prefix, exactly the posture the outbox takes to its own
retention. Benzene does not create that rule, and this is the honest cost of the decision: **a bucket
with no expiration rule grows forever.** Both cloud store docstrings say so, and both stores use a
dedicated key prefix (`claim-checks/`) precisely so that one rule can target claim-check objects and
nothing else.

Sizing rule: **the retention window must outlive the longest path from send to last possible
consumption** — queue retention plus every dead-letter redrive window a consumer might use. Too short
and a legitimate redelivery arrives to find its payload gone; too long only costs storage.

`ClaimCheckStore.delete` exists, and is **never called by either middleware half**. It is for the
operator and for tests: cleaning up after a load test, purging a payload on a data-subject request,
removing an orphan a failed send left behind.

## Failing loud, and the two failure modes

A message that carries the header but cannot be hydrated **fails**. The exception is not caught by
this package; the pipeline turns it into an unsuccessful result, so the transport's normal semantics
— nack, redelivery, eventually a dead-letter — apply exactly as they would for any other
unprocessable message. There is no silent skip, and a placeholder is never handed to a handler.

The two failures are deliberately **separate types**, and merging them would be a real mistake:

| | Raised when | Meaning |
|---|---|---|
| `ClaimCheckNotFound` | the store returned `None` for a reference it *could* have issued | expired, or never stored |
| `ClaimCheckStoreMismatch` | the reference is outside this store's own scheme / bucket / container / prefix | **not this store's to resolve** |

They have different causes (an expiry versus a wrong or hostile reference) and different operator
responses (widen the retention window versus investigate who sent that). More importantly, the second
is a **security boundary**. The reference arrives on a wire header, so it is attacker-controllable. A
store that treated "not mine" as "not found" would have to *attempt* the fetch to find out — which is
precisely the fetch it must never make (§2.1: "a consumer MUST NOT fetch an attacker-supplied
arbitrary location"). Checking the reference against the store's own configuration **before** any
client call, and refusing loudly, is the whole mechanism. Both cloud stores do it with a single prefix
comparison rather than a URL parse, because `urllib.parse` would not round-trip the `:` a Benzene
topic carries.

`ClaimCheckError` is the shared base, for a caller that genuinely wants to catch both.

## Offload-then-send is not atomic

The put happens before the send:

- **A failed put raises, and the send never happens.** The caller learns the message did not go, which
  is the safe direction.
- **A successful put followed by a failed send leaves an orphan** in the store until its retention rule
  expires it.

There is no two-phase commit here and pretending otherwise would be worse. If you need the send itself
to be durable, that is the [outbox](outbox.md)'s job, and the two compose: put the outbox *outside* the
claim check so the captured envelope holds the real message and the dispatcher's send is what offloads.

## The stores

| Store | Reference | Extra | Use it for |
|---|---|---|---|
| `InMemoryClaimCheckStore` | `memory://{topic}/{uuid}` | — | tests, local development, a genuinely single-instance service |
| `S3ClaimCheckStore` | `s3://{bucket}/{prefix}{topic}/{date}/{uuid}` | `[s3]` | an AWS deployment |
| `BlobClaimCheckStore` | `azblob://{container}/{prefix}{topic}/{date}/{uuid}` | `[azure]` | an Azure deployment |

**`InMemoryClaimCheckStore` is single-process, and getting this wrong is silent.** On more than one
instance, the pod that offloaded a payload and the pod that receives the message are usually not the
same one, so every hydration raises `ClaimCheckNotFound` and the message dead-letters. Anything real
needs a shared, durable store.

Both cloud stores:

- import their SDK **lazily**, with a teaching `ImportError` naming the exact extra — a missing SDK is
  a deployment error, never a message outcome;
- accept an injected `client`, so they are fully exercisable with no cloud SDK installed;
- run every blocking SDK call through `asyncio.to_thread`, so a hydration never stalls the event loop;
- key objects as `{prefix}{topic}/{date}/{uuid}` — the topic verbatim (S3 keys and blob names both
  permit the `:` a Benzene topic carries), the date segment so an operator can see by eye how far back
  objects go and whether the lifecycle rule is firing;
- map a 404 to `None` (a miss) and refuse a foreign reference (a mismatch) — never the other way round.

Writing your own is three methods (`put`, `get`, `delete`) and two rules: **refuse a foreign
reference**, and **never delete on read**.

## Relationship to .NET

Same semantics, Python's idiom, and one place where Python is structurally simpler.

| | .NET | Python |
|---|---|---|
| Offload | `ClaimCheckOffloadMiddleware` on the outbound route pipeline | `ClaimCheckMessageSender`, a decorator over `MessageSender` — because Python's outbound side *is* decorators (`with_retry`, `with_correlation_id`, `with_outbox`) |
| Hydrate | `ClaimCheckHydrateMiddleware<TContext>` + an `IMessageBodySetter<TContext>` **per transport** | one `claim_check_interception(store)` for every transport |
| Replacement point | the raw body, before deserialization | the already-parsed `context.request` |
| Service Bus | **blocked** — `ServiceBusReceivedMessage.Body` has no setter | works, like every other transport |

The body-setter abstraction is deliberately **not** ported. Every Python host funnels through
`BenzeneMessageApplication`, which parses the body once and hands the pipeline a `context.request`, so
one middleware hydrates every transport. The behavioural consequence — the fetched body is decoded
here rather than by the transport's deserializer — is the "serializer consistency" section above, and
it is the safer end of that trade.
