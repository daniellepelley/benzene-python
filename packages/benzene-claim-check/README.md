# benzene-claim-check

The **claim check** for [Benzene Python](https://github.com/daniellepelley/benzene-python): offload an
oversized message body to a blob store, send a small reference in its place, and put the real payload
back on the receiving side. Depends only on `benzene-core`.

```bash
pip install benzene-claim-check                # in-memory store + both middleware halves
pip install "benzene-claim-check[s3]"          # + boto3, for the S3 store
pip install "benzene-claim-check[azure]"       # + azure-storage-blob, for the Blob store
```

## The problem it removes

Every transport caps a message — SQS, SNS, EventBridge and Service Bus standard at 256 KB, Azure
Queue Storage at 64 KB. A handler with a legitimately larger message cannot send it at all:

```python
await sender.send_message("documents:ingest", a_300kb_document)
# → bad-request: message of 308434 bytes exceeds the 262144 byte limit
```

With the claim check in front of the same transport, the body goes to a blob store and a reference
goes on the wire:

```python
from benzene.claim_check import S3ClaimCheckStore, with_claim_check

store  = S3ClaimCheckStore("my-payload-bucket")
sender = with_claim_check(sqs_sender, store)          # the call site never changes

await sender.send_message("documents:ingest", a_300kb_document)   # ok
```

On the receiving side, one middleware puts it back before the router maps it:

```python
from benzene.claim_check import claim_check_interception

pipeline.use(claim_check_interception(store))         # before the message router
```

The handler sees exactly the request that was sent. Neither side's code knows anything happened.

## What crosses the wire

A **cross-port contract**, taken byte-for-byte from .NET's `Benzene.ClaimCheck`, so a Python offload
is hydratable by a .NET consumer and vice versa:

| | Value | Source |
|---|---|---|
| Header | `benzene-claim-check` | `ClaimCheckHeaders.ClaimCheck`; `wire-contracts.md` §2 (Tier C) |
| Placeholder body | `{"_benzeneClaimCheck": "<ref>"}` | `ClaimCheckPlaceholder._benzeneClaimCheck` |
| Reference | `scheme://location/key` (`memory://…`, `s3://bucket/key`, `azblob://container/key`) | `wire-contracts.md` §2.1 |

The header is authoritative — a consumer never interprets the placeholder body.

## What it guarantees, and what it does not

- **Offload-then-send is not atomic.** A failed put raises and the send never happens; a successful
  put followed by a failed send orphans the object until retention expires it.
- **A missing payload fails loud** (`ClaimCheckNotFound`) so the transport's nack → redelivery →
  dead-letter path applies. Never a silent skip.
- **Nothing is deleted on read** — fan-out siblings and redeliveries re-read the same reference.
  Retention is a bucket/container lifecycle rule you own, sized to outlive queue retention plus every
  dead-letter redrive window. **A store with no such rule grows forever.**
- **A store resolves only its own references** (`ClaimCheckStoreMismatch`, raised before any fetch).
  A reference off a wire header is attacker-controllable; this is a security boundary.
- **`InMemoryClaimCheckStore` is single-process** — tests and local development only.

Full documentation: [`docs/reference/claim-check.md`](https://github.com/daniellepelley/benzene-python/blob/main/docs/reference/claim-check.md).
