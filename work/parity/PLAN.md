# Aligning with the .NET port — the work order

Synthesised from six domain parity analyses (`work/parity/*.md`), `docs/capability-matrix.md` on
`main`, and the canonical spec at `/workspace/benzene`. Sequenced by production impact, not by
which .NET project it came from.

## The three sources of truth, and how they disagree

1. **`docs/capability-matrix.md`** (the maintainer's own statement) names four unbuilt gaps versus
   .NET: outbox, claim check, schema registry, and JWKS/OIDC in auth. It also marks several things
   as *deliberate* refusals — no database/state abstraction, no policy-toolkit (Polly analogue), no
   durable saga, no transport-hiding queue interface. **Those refusals are not gaps and must not be
   "fixed".**
2. **The parity analyses** found defects the matrix does not cover, several of which bite every
   deployment rather than only the services that would have used the missing feature.
3. **The canonical spec** (`/workspace/benzene/docs/specification`) reserves exactly seven topics:
   `healthcheck`, `heartbeat`, `issues`, `mesh`, `register`, `spec`, `traces`. .NET additionally
   defines `benzene:liveness` and `benzene:readiness` in `BenzeneTopic.cs`, **which the spec does not
   reserve** — that is .NET ahead of the contract, not Python behind it. Python must not follow
   unilaterally; the same capability is delivered here through the existing health surface.

Where these disagree, production impact wins, and the frozen wire contract always wins over both.

## Tier 0 — correctness, data loss, and leaks

Small diffs, severe consequences. Three of these finish work this branch already started: a fix that
was right but incomplete is still a bug.

- **T0.1 RabbitMQ publishes non-persistent.** `producer.py` builds
  `pika.BasicProperties(headers=headers)` with no `delivery_mode`, so AMQP defaults to transient and
  every published message is lost when the broker restarts. .NET made persistent the deliberate
  default. Fix: `delivery_mode=2` by default, overridable. *Critical, ~10 lines.*
- **T0.2 A poison Kafka record wedges its partition forever.** This branch's C1 fix correctly stopped
  the loop committing past a failure — but with no dead-letter bound, a record that can never succeed
  is re-served indefinitely and the partition stops advancing. .NET has `KafkaDeadLetterOptions`.
  Fix: a bounded attempt count per `(topic, partition, offset)` and a dead-letter seam; after N
  attempts, route the record and let the partition advance. *Critical — C1 is not safe without it.*
- **T0.3 The collector still leaks.** C8 bounded `_events`, but `_span_owner` and the merged `_issues`
  map are never pruned, so a long-lived collector still grows without limit. Fix: prune both against
  the retained window. *Critical.*
- **T0.4 Schema derivation is blind to pydantic.** `schema.py` returns `{}` for any `BaseModel`, so
  `/benzene/spec`, the Contract Document, the mesh descriptor and the OpenAPI document all publish an
  empty contract for the exact model type this port ships an adapter for. Fix: a provider hook in
  core plus `model_json_schema(by_alias=True)` in `benzene-pydantic`, with `$defs` inlined so
  Contract Document §4's ref rule still holds. *Critical, and it silently degrades four surfaces.*
- **T0.6 A raising middleware escapes the pipeline.** Only the terminal router catches, so an
  exception thrown by any middleware propagates out into the transport adapter — where each host
  handles it differently, or not at all. The host is supposed to be uncrashable by request content.
  Fix: contain exceptions at the pipeline boundary, mapping to `service-unavailable` exactly as the
  router already does. *High.*
- **T0.5 Nothing drains on SIGTERM.** `WorkerHost` installs no signal handlers and uvicorn is the only
  signal source, so a queue-only pod is killed mid-handler with work uncommitted. Paired with it:
  there is no way to tell an orchestrator "stop routing to me, I am draining". Fix: signal handling
  that trips the stop signal, and a shutdown latch exposed through the **existing**
  `benzene:healthcheck` surface — no new reserved topics. *Critical.*

## Tier 1 — the capability-matrix gaps (true .NET alignment)

- **T1.1 Durable `IdempotencyStore` backends.** Only the in-memory store ships, so dedupe is a silent
  no-op on any multi-instance deployment. The protocol is already the right shape
  (`put_if_absent`/`delete`). Ship Redis (`SET NX`) first — `benzene-cache` already carries the client
  — then DynamoDB (conditional put). *This is the matrix's own recommended answer, shipped.*
- **T1.2 Transactional outbox** (`benzene-outbox`): store-and-forward staging so a state write and a
  send cannot diverge. Stores: in-memory, DynamoDB, and a DB-API/SQLAlchemy store (**not** an
  EntityFramework port — that is a .NET-ecosystem adapter).
- **T1.3 Claim check** (`benzene-claim-check`): offload/hydrate middleware pair with S3 and Blob
  stores. ⚠️ **New cross-port wire surface** — the `benzene-claim-check` header and placeholder shape
  must match .NET byte-for-byte, and it belongs in the spec repo. Implement to .NET's constants and
  flag it upstream rather than inventing a Python-only shape.
- **T1.4 JWKS / OIDC discovery** in `benzene-auth`: key-rotation-aware validation against an IdP's
  metadata. The matrix explicitly calls this unbuilt, not declined, and notes .NET and Go ship it.
- **T1.5 Schema registry / Confluent wire codec** for Kafka, so Benzene payloads interoperate with
  non-Benzene consumers.

## Tier 2 — depth and throughput

- **T2.1 Batch producers**: a shared `BatchResult`/chunking seam plus `send_batch` on the senders
  (EventBridge ≤10, SNS, SQS, Event Hub, Event Grid, Service Bus). Purely additive, no frozen shape.
- **T2.2 Health-check family**: TCP/HTTP/disk checks, a per-check timeout (one hanging check currently
  hangs `/benzene/health`), and stop leaking `str(exc)` into an unauthenticated payload.
- **T2.3 Mesh live-plane**: cumulative counters that survive ring eviction, and ingest of `durationMs`
  / `exceptionType` (every latency field in the pinned artifact set is null today despite the wire
  carrying the data).
- **T2.4 Serializer seam** for the message *body* only — never the envelope, descriptor or hashed
  documents; failure bodies stay JSON.
- **T2.6 RabbitMQ publish is fire-and-forget.** The channel never calls `confirm_delivery()`, so an
  `ok` Result means the frame was written, not that the broker accepted it. Persistence (T0.1) is
  necessary but not sufficient for delivery; confirms are the other half. Only an injected
  pre-confirming channel can get this today.
- **T2.5 Timeout/deadline policy** (a hanging dependency never trips the breaker) and cache
  degrade-to-miss instead of failing the request.

## Already closed by `main` — do not re-implement

The six analyses ran against a tree that was 36 commits behind `origin/main`, so some findings were
already fixed. Confirmed stale, and dropped from this plan:

- **RFC 9457 problem details, structured `errors`, envelope `isSuccessful`** — the core-tooling
  analysis ranked this its #1 critical gap (a .NET client throws on a Python failure response because
  `status` was a string where it expects an int). `main` shipped it; the merge brings it in.
- **A Contract Document producer** — `main` added `contract.py` with `ContractDocument.derive`, served
  at `GET /benzene/spec`.
- **A worker host** — `main`'s `WorkerHost`/`StopSignal` supersedes the `run_legs` helper this branch
  added to `examples/k8s_orders`, and is the thing T0.5's signal handling should attach to.

**Rule: re-validate every item below against the merged tree before implementing it.** An analysis
finding is a hypothesis about code that has since moved.

## Tier 3 — the refining pass (run after the capabilities land, not before)

The repo vendors specialist agents for exactly this, and they are the difference between "the code
exists" and "it is production grade":

- **`capability-scribe`** — **mandatory, not optional.** `docs/capability-matrix.md` currently states
  outbox, claim check and schema registry as *"Not implemented"* and auth's JWKS as *"Partial"*.
  Shipping those without updating it converts the port's most honest document into a false one. The
  matrix must move in the same change as the capability.
- **`ergonomics-champion`** — boilerplate-versus-magic on every new public API (outbox, claim check,
  the stores). New packages are exactly where ceremony accumulates.
- **`python-dx-champion`** — does the new surface feel like Python, and do its errors teach?
- **`python-test-champion`** — is each new capability reachable through the shared harness, with the
  failure paths (not just the happy path) covered?
- **`docs-archivist`** — `work/` has accumulated an audit, six parity analyses and this plan; once
  actioned they belong in `work/archive/` with an index, not in the way.

## Rules for every workstream

1. **Failing test first.** If it cannot be made to fail, the finding is wrong — report, don't "fix".
2. **Gates stay green**: `ruff check .`, `python -m mypy` (now covers `tests` and `examples` too),
   `python -m pytest -q`, `python -m tests.conformance_runner`.
3. **The wire contract is frozen.** No new reserved topics, no envelope/descriptor/hash changes.
   Anything wire-adjacent re-runs the conformance runner and says so.
4. **Don't port .NET ecosystem adapters** (EntityFramework, Autofac, SourceGenerators, Newtonsoft,
   TestHelpers-as-packages). Port *capabilities*, in Python idiom: async/await, Protocols,
   dataclasses, injectable clocks/clients, duck-typed fakes, optional extras, no new required deps.
5. **Respect the matrix's deliberate refusals.** A documented "no" is a design decision, not a gap.
