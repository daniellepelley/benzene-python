# Parity gap analysis — reliability & messaging patterns

**Reference:** `/workspace/benzene-dotnet` (.NET port — widest)
**Target:** `/home/user/benzene-python` (Python port — conformant, thinner)
**Domain:** outbox, claim check, idempotency, saga, resilience, rate limiting, cache.
**Date:** 2026-09-07. Analysis only — no code was changed.

## How to read this

Severity answers one question: **would a real service running this in production be hurt by its
absence?** It is not a measure of implementation size. A missing capability that silently degrades a
guarantee the port already advertises scores higher than a large missing feature that nobody is
currently relying on.

Every spec below is written to be implementable without re-reading the .NET source. Where the .NET
shape is a C#/ecosystem artefact (EF Core, Polly, DI extension methods, `IMessageBodySetter`), the
spec gives the *Python* answer instead and says why — see [§13 Not worth porting](#13-not-worth-porting).

### Frozen-contract flags (read before implementing)

The cross-language wire contract and `conformance/*.json` fixtures are frozen. Nothing in this
document requires changing an existing fixture, but three items **touch or create cross-language wire
surface** and need a deliberate decision before code lands:

| Item | Status | Rule |
|---|---|---|
| `idempotency-key` header | **Already shared.** .NET `IdempotencyDefaults.HeaderName` and `OutboxDefaults.IdempotencyKeyHeaderName` are both the literal `idempotency-key`; Python's `DEFAULT_KEY_HEADERS` is `("idempotency-key", "message-id")`, a compatible superset. | The outbox capture point (§2) MUST stamp exactly `idempotency-key`, lower-case. Do not invent a new header. |
| `benzene-claim-check` header + `{"_benzeneClaimCheck": "<ref>"}` placeholder body | **New cross-port wire surface** (§3). Not in any frozen fixture today. | Match .NET byte-for-byte: header name `benzene-claim-check`, placeholder key `_benzeneClaimCheck` verbatim (not camel-cased by the wire-naming encoder). A Python offload must be hydratable by a .NET consumer and vice versa. Propose the fixture upstream in the spec repo; do not add a local fixture. |
| Outbox store item/row schema | **Not wire** — only matters if a .NET and a Python service share one outbox table. | Mirror .NET's DynamoDB attribute names and status strings (§2.6) so a shared table stays legible; treat `payloadType` as advisory (§13). |

The status vocabulary is unchanged throughout: outbox capture answers `accepted`, claim-check misses
raise (not a new status), timeouts answer `timeout`, rate-limit rejections answer `too-many-requests`
— all already in `conformance/status-vocabulary.json`.

---

## Gap index

| # | Gap | Severity |
|---|---|---|
| 1 | Idempotency has no shared store — dedup is a silent no-op on every multi-instance deployment | **critical** |
| 2 | No transactional outbox at all | **critical** |
| 3 | No claim check — oversized payloads are unsendable on SQS/SNS/EventBridge/Service Bus | **high** |
| 4 | No timeout/deadline policy — a hung dependency hangs the worker forever | **high** |
| 5 | Cache errors fail the request (no degrade-to-miss), and no cache health check | **high** |
| 6 | Retry is status-only, with no backoff/jitter helpers | **medium** |
| 7 | Idempotency key derivation is fixed — no pluggable strategy, body-hash fallback, or prefix | **medium** |
| 8 | Saga has no state store and no whole-saga retry | **medium** |
| 9 | Rate limiting is one algorithm with a fixed cost of 1 | **medium** |
| 10 | Cache has no write-through / invalidate-on-success actions | **medium** |
| 11 | Saga has no concurrent stages | **low** |
| 12 | Idempotency in-flight behaviour is not configurable | **low** |

---

## 1. Idempotency has no shared store — dedup is a silent no-op across instances

**Severity: critical.** This is the only gap where the port ships a *guarantee-shaped* feature that
quietly does not hold in the deployment shape the framework targets. `idempotency_interception` is
documented as making a handler run at most once per key; with the only shipped store being
process-local, two Lambda invocations, two SQS consumer replicas, or two pods dedupe against
*different dictionaries* and both run the handler. Nothing errors, nothing logs; the card is charged
twice. Every other gap here is an absent capability the operator can see is absent.

**What .NET has**
- `src/Benzene.Idempotency/IIdempotencyStore.cs` — the pluggable contract, with the atomicity
  requirement stated as a hard rule.
- `src/Benzene.Idempotency.DynamoDb/DynamoDbIdempotencyStore.cs` — the production store: conditional
  `PutItem` with `attribute_not_exists(#pk) OR expiresAt < :now`, read-back on
  `ConditionalCheckFailedException`, native TTL on `expiresAt`, and a lapsed-but-not-yet-deleted
  record treated as absent so an expired key is reclaimable the instant it lapses.
- `src/Benzene.Idempotency/CLAUDE.md` "Capability boundary" — names Redis `SET NX` and a unique-key
  insert as equally valid backings.

**What Python has**
- `packages/benzene-resilience/benzene/resilience/idempotency.py` — `IdempotencyStore` Protocol
  (`get`/`put`/`put_if_absent`/`delete`) and `InMemoryIdempotencyStore`. The protocol is *better
  shaped* than .NET's: `put_if_absent` is an explicit atomic reservation, and the store holds the
  actual first `Result` so a duplicate replays the real answer rather than .NET's synthetic
  `BenzeneResult.Ok()`. The seam is right; only the implementations are missing.
- `packages/benzene-cache/benzene/cache/redis.py` already carries a lazily-imported `redis.asyncio`
  client and the `[redis]` extra — the SDK plumbing for a Redis store is already solved in this repo.

### Implementation spec

**Package:** existing `packages/benzene-resilience`. Two new modules beside `idempotency.py`, keeping
protocol and implementations together exactly as `benzene-cache` keeps `Cache` beside `RedisCache`.

**Extras:** add to `packages/benzene-resilience/pyproject.toml`:
```toml
[project.optional-dependencies]
redis    = ["redis>=5.0"]
dynamodb = ["boto3>=1.34"]
```

**New: `benzene/resilience/result_codec.py`** (shared by both stores)
```python
def encode_result(result: Result) -> str      # json.dumps({"status", "payload", "errors": [...]})
def decode_result(raw: str | bytes) -> Result # Result(status, payload, tuple(errors))
```
Load-bearing detail: `Result.errors` is a **tuple** on a frozen dataclass, and the middleware
recognises the in-flight marker by `settled == IN_PROGRESS` (equality, not identity). A codec that
restores `errors` as a `list` breaks that comparison and every concurrent-duplicate test passes
locally while failing on Redis. Restore a tuple. Do **not** reuse `encode_response`/`decode_response`
for this: they join multiple errors into one `detail` string and are lossy for a multi-error result.

**New: `benzene/resilience/redis_store.py`**
```python
class RedisIdempotencyStore:
    """A shared IdempotencyStore over redis.asyncio — the cross-instance dedup backing."""
    def __init__(
        self,
        url: str | None = None,
        *,
        client: Any | None = None,          # duck-typed: get/set/delete only
        ttl: float = 86_400.0,              # must exceed the transport's max redelivery window
        prefix: str = "benzene:idem:",
    ) -> None: ...

    async def get(self, key: str) -> Result | None
    async def put(self, key: str, result: Result) -> None          # SET k v EX ttl
    async def put_if_absent(self, key: str, result: Result) -> bool # SET k v NX EX ttl -> bool(reply)
    async def delete(self, key: str) -> None
```
- `put_if_absent` is **one** command — `await client.set(k, v, nx=True, ex=ceil(ttl))` — never
  `exists()`-then-`set()`. Redis returns `True`/`None`; normalise with `bool(reply)`.
- Sub-second TTLs use `px=max(1, ceil(ttl*1000))`, matching the rounding rule already established and
  regression-tested in `packages/benzene-cache/benzene/cache/redis.py`.
- Missing SDK → `ImportError` naming `benzene-resilience[redis]`, following the existing
  "a missing extra is a deployment error, never a `service-unavailable` result" rule in
  `packages/benzene-aws/benzene/aws/clients.py`.

**New: `benzene/resilience/dynamodb_store.py`**
```python
class DynamoDbIdempotencyStore:
    def __init__(
        self,
        table_name: str,
        *,
        client: Any | None = None,          # duck-typed boto3 dynamodb client
        ttl: float = 86_400.0,
        partition_key: str = "pk",
        clock: Callable[[], float] = time.time,   # epoch seconds, UTC
    ) -> None: ...
```
- `put_if_absent` → `put_item` with
  `ConditionExpression="attribute_not_exists(#pk) OR expiresAt < :now"`,
  `ExpressionAttributeNames={"#pk": partition_key}`; catch
  `client.exceptions.ConditionalCheckFailedException` → `False`.
- `get` → `get_item(ConsistentRead=True)`; a record whose `expiresAt <= now` reads as **absent**
  (DynamoDB TTL deletion lags by up to 48h; without this an expired key is unreclaimable).
- Item attributes, mirroring .NET so a mixed-language fleet can share one table:
  `pk` (S), `status` (S: `"InProgress"` | `"Completed"`), `wasSuccessful` (BOOL),
  `expiresAt` (N, epoch seconds), plus Python-only `result` (S, `encode_result` output) which .NET
  ignores. Python ignores `wasSuccessful` on read except to synthesise a `Result` when `result` is
  absent (i.e. the record was written by a .NET service).
- Every blocking boto3 call goes through `asyncio.to_thread`, the rule already enforced by
  `tests/test_egress_offloads_the_event_loop.py`.
- The store **never creates the table**; TTL on `expiresAt` is the consumer's infra. Say so in the
  docstring, as every Benzene store does.

**Tests** — new `tests/test_resilience_stores.py`:
1. Redis: `put_if_absent` issues exactly one `set(..., nx=True, ex=...)`; a second caller gets `False`.
2. Redis: a `Result` with a payload and two errors round-trips identically, `errors` still a tuple.
3. Redis: `IN_PROGRESS` survives a store round-trip and still compares `== IN_PROGRESS`.
4. Redis: sub-second ttl → `px=1`; `ttl=1.9` → `ex=2`.
5. Redis: missing SDK path raises `ImportError` naming the extra.
6. Dynamo (fake client): the exact condition expression is sent; `ConditionalCheckFailedException` →
   `False`; a lapsed `expiresAt` reads as absent; `delete` issues `delete_item`.
7. Dynamo: the boto3 call runs off the event loop (assert via a client that records the thread id, as
   `tests/test_egress_offloads_the_event_loop.py` does).
8. **The integration test that matters**: drive the existing `idempotency_interception` with each new
   store via two `asyncio.gather`ed duplicate invocations and assert the handler ran once — the same
   assertion `tests/test_resilience.py::test_idempotency_runs_the_handler_once_for_concurrent_duplicates`
   makes for the in-memory store, so a store swap is proven behaviour-preserving.

Also update `packages/benzene-resilience/benzene/resilience/idempotency.py`'s module docstring (it
still says "a shared backend (e.g. Redis, once `benzene-cache` lands)") and
`docs/reference/resilience.md` to state plainly: **the in-memory store is single-process; a
multi-instance service must configure a shared store or it is not deduplicating.**

---

## 2. No transactional outbox

**Severity: critical.** A handler that writes state and then sends a message does two independent
operations. A crash or a transport outage between them loses one side, silently. This is the single
largest *capability* the Python port lacks: today the only remedies available to a Python Benzene
service are try/catch-and-log (loses the message) or send-before-commit (sends a message about state
that was never written). `benzene-resilience`'s retry decorator does not help — it retries within one
process lifetime and its state dies with the process.

**What .NET has** — `src/Benzene.Outbox/` (engine, host-agnostic, no third-party deps):
- `OutboxEnvelope.cs`, `OutboxStatus.cs` (`Pending`/`Dispatched`/`Parked`), `OutboxWriteMode.cs`
  (`Immediate`/`Transactional`), `OutboxOptions.cs` (defaults: `MaxAttempts` 10, `BackoffBase` 30s,
  `BackoffCap` 1h, `RetentionPeriod` 7d, `BatchSize` 25, `ClaimLease` 2m, `PollInterval` 5s,
  `StampIdempotencyKey` true).
- `IOutboxStore.cs` — `AddAsync` / `ClaimDueAsync` / `ClaimAsync` / `MarkDispatchedAsync` /
  `RescheduleAsync` / `ParkAsync` / `DeleteDispatchedBeforeAsync`, with claim atomicity as a hard
  contract requirement.
- `OutboxMiddleware.cs` — the capture point: terminal on a fresh send (build envelope, write or
  stage, answer `Accepted`, never call `next`), pass-through on a relay dispatch (replay the
  envelope's stored headers, then `next`).
- `OutboxDispatcher.cs` — `RunOnceAsync` (claim a due batch, dispatch each, then retention-delete)
  and `DispatchOneAsync(id)` (the stream-triggered path); backoff
  `min(base * 2^(attempt-1), cap)`; park at `MaxAttempts`.
- `OutboxDispatcherWorker.cs` — the poll loop; `IOutboxStage`/`BufferedOutboxStage` — the staging
  seam for `Transactional` mode.
- Stores: `src/Benzene.Outbox.DynamoDb/` (sparse `pending-index` GSI projecting ALL, conditional
  `UpdateItem` claim, native TTL on `expiresAt`, plus `IDynamoDbOutboxTransaction` committing app
  items + staged envelopes in one `TransactWriteItems`) and `src/Benzene.Outbox.EntityFramework/`.
- `src/Benzene.Outbox/CLAUDE.md` "Capability boundary" — the honesty that must be carried over
  verbatim: neither write mode is exactly-once; delivery is at-least-once; no ordering guarantee;
  poison envelopes are **parked, not dead-lettered**; the in-memory store is single-process.

**What Python has** — nothing. `grep -ril outbox packages/` returns only an unrelated
`benzene-otel/response_events.py` hit.

### Implementation spec

**New package:** `packages/benzene-outbox`, importing `benzene.outbox`, depending on `benzene-core`
(it needs `MessageSender` and `encode_body`). Extras: `dynamodb = ["boto3>=1.34"]`,
`sql = ["sqlalchemy>=2.0"]`. This mirrors the repo's one-package-per-adoption-level thesis and keeps
boto3/SQLAlchemy out of a service that only wants the in-memory engine.

**2.1 `benzene/outbox/envelope.py`**
```python
class OutboxStatus(str, Enum):
    PENDING = "pending"; DISPATCHED = "dispatched"; PARKED = "parked"

@dataclass(frozen=True)
class OutboxEnvelope:
    id: str
    topic: str
    payload: str                       # the JSON wire body (encode_body output)
    headers: Mapping[str, str]         # post-stamping snapshot, lower-cased keys
    created_at: float                  # epoch seconds, UTC
    attempt_count: int = 0
    next_attempt_at: float | None = None
    status: OutboxStatus = OutboxStatus.PENDING
    last_error: str | None = None
```
No `payload_type`: Python re-sends the parsed payload through `send_message(topic, dict)` — see §13.

**2.2 `benzene/outbox/options.py`** — `@dataclass OutboxOptions` with the .NET defaults above,
expressed in seconds as floats, plus `write_mode: Literal["immediate", "transactional"] = "immediate"`
and `stamp_idempotency_key: bool = True`.

**2.3 `benzene/outbox/store.py`**
```python
class OutboxStore(Protocol):
    async def add(self, envelopes: Sequence[OutboxEnvelope]) -> None: ...
    async def claim_due(self, batch_size: int, lease: float) -> list[OutboxEnvelope]: ...
    async def claim(self, envelope_id: str, lease: float) -> OutboxEnvelope | None: ...
    async def mark_dispatched(self, envelope_id: str) -> None: ...
    async def reschedule(self, envelope_id: str, attempt_count: int, delay: float, error: str) -> None: ...
    async def park(self, envelope_id: str, error: str) -> None: ...
    async def delete_dispatched_before(self, cutoff: float) -> int: ...
```
Docstring must state, as .NET's does: **`claim`/`claim_due` MUST be atomic per envelope** — a
sweeper and a stream-triggered relay racing the same envelope cannot both win it. Lifecycle methods
on a missing envelope are a no-op, not an error.

**2.4 `benzene/outbox/memory.py`** — `InMemoryOutboxStore(clock=time.time)`: dict + lease +
`dispatched_at`, due = `PENDING and (next_attempt_at is None or <= now) and (lease_until is None or
<= now)`, `claim_due` ordered by `created_at`. Atomic by construction on the single-threaded event
loop provided no `await` sits between the read and the write — the same argument
`InMemoryIdempotencyStore.put_if_absent` already documents. Single-process; say so.

**2.5 `benzene/outbox/sender.py` — the capture point (the key design decision)**

.NET captures with an outbound-route *middleware* because its outbound pipeline is a middleware
chain. Python's outbound side is a **decorator over `MessageSender`** (`benzene/core/outbound.py`:
`with_retry`, `with_correlation_id`; `benzene/resilience/rate_limit.py`:
`RateLimitingMessageSender`). The outbox belongs in that idiom, not as a transplanted middleware:

```python
class OutboxMessageSender:
    """Captures a send into the outbox instead of performing it. Answers `accepted`."""
    def __init__(
        self,
        inner: MessageSender,               # kept only for the relay path (see dispatcher)
        store: OutboxStore,
        *,
        options: OutboxOptions | None = None,
        stage: OutboxStage | None = None,
        new_id: Callable[[], str] = lambda: uuid.uuid4().hex,
        clock: Callable[[], float] = time.time,
    ) -> None: ...

    async def send_message(self, topic, message, headers=None) -> Result: ...

def with_outbox(inner: MessageSender, store: OutboxStore, **options: Any) -> OutboxMessageSender: ...
```
Behaviour:
- Build `id = new_id()`; `headers = {k.lower(): v for ...}`; if `options.stamp_idempotency_key` and
  `"idempotency-key"` is absent, set it to `id` (**the frozen header name** — this is what makes an
  outbox producer and an idempotency consumer click together with no configuration, exactly as .NET
  documents).
- `payload = encode_body(message)` — the same wire-naming entry point every outbound client uses, so
  the stored body is byte-identical to what an inline send would have produced.
- `immediate` → `await store.add([envelope])`; `transactional` → `await stage.stage(envelope)`.
- Return `Result.accepted()`. Never call `inner.send_message` — capture is terminal.
- **Composition order is load-bearing and must be documented**: `OutboxMessageSender` is the
  *innermost* decorator, so ambient stamping still happens before capture:
  `with_retry(with_correlation_id(with_outbox(real_sender, store)))` is wrong (retry/correlation would
  wrap a call that never leaves the process); the correct shape is
  `with_outbox(with_correlation_id(real_sender), store)` — correlation stamps into `headers` on the
  way in, and the retry/circuit-breaker decorators belong on the sender the **dispatcher** uses, not
  the capture one. Give this its own docstring section and a test.
- **Constraint to carry over**: an outboxed send is fire-and-forget. The caller gets `accepted`, never
  the downstream's real answer. Request/response topics must not be outboxed.

**2.6 `benzene/outbox/stage.py` — transactional mode**
```python
class OutboxStage(Protocol):
    async def stage(self, envelope: OutboxEnvelope) -> None: ...

class BufferedOutboxStage:
    def drain(self) -> list[OutboxEnvelope]: ...   # returns and clears

@asynccontextmanager
async def outbox_transaction(commit: Callable[[list[OutboxEnvelope]], Awaitable[None]]) -> AsyncIterator[BufferedOutboxStage]: ...
```
Use a module-level `contextvars.ContextVar[BufferedOutboxStage | None]` as the ambient scope — the
exact Python analogue of .NET's scoped `IOutboxStage`, and correct under `asyncio` concurrency
(`ContextVar` is per-task). `outbox_transaction` sets it, yields the stage, and on clean exit calls
`commit(stage.drain())`; on an exception it discards (consistent by construction — nothing was
committed). **Log a warning if the stage is disposed with undrained envelopes**, as
`BufferedOutboxStage.Dispose` does: staging without a committer is a misconfiguration where the write
never happens, not a smaller guarantee.

**2.7 `benzene/outbox/dispatcher.py`**
```python
@dataclass(frozen=True)
class OutboxDispatchResult:
    dispatched: int; rescheduled: int; parked: int; deleted_retired: int

class OutboxDispatchOutcome(str, Enum):
    DISPATCHED = "dispatched"; RESCHEDULED = "rescheduled"; PARKED = "parked"; CLAIM_REFUSED = "claim-refused"

class OutboxDispatcher:
    def __init__(
        self,
        store: OutboxStore,
        sender: MessageSender,               # the REAL transport sender, not the OutboxMessageSender
        *,
        options: OutboxOptions | None = None,
        clock: Callable[[], float] = time.time,
        logger: logging.Logger | None = None,
    ) -> None: ...

    async def run_once(self) -> OutboxDispatchResult: ...
    async def dispatch_one(self, envelope_id: str) -> OutboxDispatchOutcome: ...
```
- Relay send: `await sender.send_message(envelope.topic, json.loads(envelope.payload), dict(envelope.headers))`
  — the envelope's **stored headers win** over any ambient stamping the relay host would apply.
- A send that raises *or* returns an unsuccessful `Result` is a failed attempt (this is a genuine
  divergence from .NET, whose dispatcher only sees exceptions because its sender throws; Python
  senders return `Result`, so the dispatcher must check `result.is_successful`. Document it).
- Failure → `attempt = envelope.attempt_count + 1`; if `attempt >= max_attempts` → `park`, else
  `reschedule(delay=min(backoff_base * 2**min(attempt-1, 32), backoff_cap))`.
- `run_once` finishes with `deleted = await store.delete_dispatched_before(now - retention)`.
- **Parked is the terminal state, not a dead-letter.** Never auto-delete or auto-retry a parked
  envelope: it is the operator's evidence. Carry .NET's wording.

**2.8 `benzene/outbox/worker.py`** — `OutboxDispatcherWorker(dispatcher, *, options)` with
`async def run(self)` and `def stop(self)`, modelled on
`packages/benzene-aws/benzene/aws/sqs_consumer.py`'s poll loop: an `asyncio.Event` stop signal, a
`run_once` per `poll_interval`, and a failing run logged and survived rather than killing the loop.
Note in the docstring that this loop does not apply on Lambda (no background thread); the Lambda
pattern is a DynamoDB Streams relay calling `dispatch_one` **plus** a low-frequency scheduled sweep
calling `run_once` — streams alone fire once per INSERT and cannot drive retries.

**2.9 `benzene/outbox/dynamodb.py`** (extra `[dynamodb]`)
```python
class DynamoDbOutboxStore:
    def __init__(self, table_name: str, *, client=None, partition_key="id",
                 pending_index="pending-index", retention: float = 604_800.0,
                 clock: Callable[[], float] = time.time) -> None: ...

def outbox_stream_ids(event: Mapping[str, Any]) -> list[str]:
    """Envelope ids from a DynamoDB Streams INSERT event — feed each to dispatcher.dispatch_one."""
```
Table shape, mirroring .NET so a shared table stays legible: partition key `id`; sparse GSI
`pending-index` with `gsiPk` = constant `"pending"` and `gsiSk` = the due time as an ISO-8601 UTC
string (lexical order == chronological order), both written only while `Pending` and removed by
`mark_dispatched`/`park`; **the GSI must project ALL**; `expiresAt` (epoch seconds) set only by
`mark_dispatched`, so `delete_dispatched_before` is a no-op returning `0` and parked envelopes never
self-delete. Claim = one conditional `update_item` setting `leaseUntil`:
```
attribute_exists(#pk) AND #status = :pending
  AND (attribute_not_exists(nextAttemptAtUtc) OR nextAttemptAtUtc <= :now)
  AND (attribute_not_exists(leaseUntil)       OR leaseUntil       <  :now)
```
`ConditionalCheckFailedException` → excluded from `claim_due` / `None` from `claim`. Store status
strings in .NET's casing (`"Pending"`/`"Dispatched"`/`"Parked"`) while the Python enum stays
lower-case, so a shared table reads the same from both ports. All boto3 calls via `asyncio.to_thread`.
The store never creates the table, GSI, or TTL config.

**2.10 `benzene/outbox/sql.py`** (extra `[sql]`) — the relational store. **Not** an EF Core port; see
§13.
```python
class SqlOutboxStore:
    def __init__(self, engine: Any, *, table: str = "benzene_outbox",
                 clock: Callable[[], float] = time.time) -> None: ...

class SqlOutboxStage:
    """Stages envelopes onto the caller's OWN connection/transaction; never commits."""
    def __init__(self, connection: Any, *, table: str = "benzene_outbox") -> None: ...

CREATE_TABLE_SQL: str   # exported DDL, for the app's own migration tool — the store never migrates
```
- Accepts a SQLAlchemy 2.x `AsyncEngine` (or anything exposing `begin()` → an async connection with
  `execute`), so asyncpg/aiosqlite/psycopg all work without the package choosing a driver.
- Claim is **one statement**, so atomicity needs no optimistic-concurrency fallback (.NET only needs
  one because the EF Core InMemory provider lacks `ExecuteUpdate`):
  `UPDATE {table} SET lease_until=:lease WHERE id=:id AND status='pending' AND (next_attempt_at IS NULL OR next_attempt_at<=:now) AND (lease_until IS NULL OR lease_until<=:now)`
  — `rowcount == 1` *is* the claim decision; follow with a `SELECT` only on a win.
- `claim_due` selects candidate ids ordered by `created_at` (index on `(status, next_attempt_at)`),
  then claims each through the same conditional `UPDATE`.
- `delete_dispatched_before` performs a real `DELETE` of `dispatched` rows past retention and
  **never touches parked rows**.
- The transactional story: `SqlOutboxStage` holds the *caller's* connection and only issues the
  `INSERT`; the handler's own `commit()` commits state + envelope in one database transaction. This is
  the Python equivalent of "the shared `DbContext` instance IS the unit of work", with no ORM required.

**Tests** — new `tests/test_outbox.py`:
1. Capture answers `accepted`, writes exactly one envelope, and the inner sender is never called.
2. `idempotency-key` stamped with the envelope id only when absent; a caller-supplied one is untouched.
3. Stored payload equals `encode_body(message)`; headers are lower-cased and snapshot at capture.
4. Transactional mode stages and does not write; `outbox_transaction` drains and commits; an
   exception inside the block discards; an undrained stage logs a warning.
5. In-memory store: due/lease semantics, reschedule, park, retention delete count, id independence.
6. In-memory store: two `asyncio.gather`ed `claim` calls for one id — exactly one wins.
7. Dispatcher: success → `dispatched` + `mark_dispatched`; a raising sender → `rescheduled` with the
   exact computed backoff (manual clock, no sleeping); an unsuccessful `Result` → also rescheduled;
   the final attempt → `parked`; `run_once` returns the retention delete count;
   `dispatch_one` on a leased envelope → `claim-refused`.
8. Dispatcher replays stored headers over ambient ones.
9. Worker: polls on the interval (manual clock / injected sleep), stops gracefully mid-flight,
   survives a `run_once` that raises.
10. DynamoDB store against a fake client: the GSI query, the exact claim condition expression,
    `expiresAt` set only on `mark_dispatched`, GSI attributes removed on dispatch/park,
    `delete_dispatched_before` never calls DynamoDB.
11. SQL store against in-memory SQLite: full lifecycle; two `SqlOutboxStore` instances over one
    database cannot both claim the same envelope; parked rows survive retention cleanup; the stage
    row is invisible until the caller commits and vanishes on rollback.
12. End-to-end: capture → `run_once` → the fake transport received the message with its stamped
    `idempotency-key`, and feeding that message to `idempotency_interception` dedupes a replay.

**Docs:** `docs/reference/outbox.md` + a `docs/cookbooks/` entry. Carry the capability boundary
verbatim: at-least-once, no ordering, parked-not-dead-lettered, in-memory is single-process, and
`transactional` mode without a committer means the write never happens.

---

## 3. No claim check — oversized payloads are unsendable

**Severity: high.** Python Benzene targets SQS, SNS, EventBridge (256 KB), Service Bus standard
(256 KB) and Azure Queue Storage (64 KB). A payload over the limit fails the publish outright; the
service has no framework-supported way to send it. Every serious event-driven service eventually
meets this (a document, an image manifest, a batch). Below critical only because the failure is loud
and immediate rather than silent.

**What .NET has** — `src/Benzene.ClaimCheck/`: `ClaimCheckOffloadMiddleware.cs` (serialize, measure
UTF-8 bytes, store, stamp `benzene-claim-check`, swap the request for `ClaimCheckPlaceholder`),
`ClaimCheckHydrateMiddleware.cs` (read the header, fetch, set the body, fail loud on a miss),
`IClaimCheckStore.cs`, `InMemoryClaimCheckStore.cs`, `ClaimCheckOptions.cs`
(`ThresholdBytes` 192 KiB, `AlwaysOffload`, `HeaderName`), `ClaimCheckNotFoundException` /
`ClaimCheckStoreMismatchException`; stores in `src/Benzene.ClaimCheck.Aws.S3/S3ClaimCheckStore.cs`
and `src/Benzene.ClaimCheck.Azure.Blob/BlobClaimCheckStore.cs`.

**What Python has** — nothing.

### Implementation spec

**New package:** `packages/benzene-claimcheck`, importing `benzene.claimcheck`, depending on
`benzene-core`. Extras: `s3 = ["boto3>=1.34"]`, `azure = ["azure-storage-blob>=12", "azure-identity>=1.13"]`.
(If the maintainers prefer fewer distributions, folding this into `benzene-outbox` as a
`benzene.messaging` package is defensible — they are both "the message is too big / the message must
survive" family. Two packages matches .NET and the repo's existing layering; call it either way, but
do not fold claim check into `benzene-resilience`, which is policies, not payload plumbing.)

**3.1 `benzene/claimcheck/store.py`**
```python
class ClaimCheckStore(Protocol):
    async def put(self, body: str, topic: str) -> str: ...       # returns scheme://location/key
    async def get(self, reference: str) -> str | None: ...       # None = missing/expired

class ClaimCheckNotFound(Exception): ...        # raised by hydration when get() returns None
class ClaimCheckStoreMismatch(Exception): ...   # a reference this store did not (or could not) issue
```
The mismatch/not-found split is a **security boundary**, not a convenience: a store must refuse to
fetch a foreign scheme/bucket/prefix rather than attempting it. State that in the Protocol docstring,
as .NET does.

**3.2 `benzene/claimcheck/memory.py`** — `InMemoryClaimCheckStore(ttl=86_400.0, clock=time.monotonic)`
issuing `memory://{quote(topic, safe='')}/{uuid4().hex}`; `get` raises `ClaimCheckStoreMismatch` for
any non-`memory://` reference. Single-process, with the same caveat as the other in-memory stores.

**3.3 `benzene/claimcheck/sender.py` — offload (outbound)**
```python
CLAIM_CHECK_HEADER = "benzene-claim-check"     # FROZEN — matches .NET ClaimCheckHeaders.ClaimCheck
PLACEHOLDER_KEY   = "_benzeneClaimCheck"       # FROZEN — the literal wire key, never camel-cased

class ClaimCheckMessageSender:
    def __init__(self, inner: MessageSender, store: ClaimCheckStore, *,
                 threshold_bytes: int = 192 * 1024, always_offload: bool = False,
                 header: str = CLAIM_CHECK_HEADER) -> None: ...
    async def send_message(self, topic, message, headers=None) -> Result: ...

def with_claim_check(inner, store, **options) -> ClaimCheckMessageSender: ...
```
Serialize with `benzene.core.encode_body` (the same encoder the transports use, so the measured bytes
are the bytes that would have been sent), measure `len(body.encode("utf-8"))`, and below threshold
delegate untouched. At or over: `reference = await store.put(body, topic)`, then delegate with
`headers | {header: reference}` and the message replaced by `{PLACEHOLDER_KEY: reference}`.
**Do not** pass the placeholder through the wire-naming encoder — the key must survive verbatim.

Honesty to document, from .NET: offload-then-send is **not atomic**. A failed put raises and the send
never happens; a successful put followed by a failed send orphans the object until its TTL.

**3.4 `benzene/claimcheck/interception.py` — hydrate (inbound)**
```python
def claim_check_interception(store: ClaimCheckStore, *,
                             header: str = CLAIM_CHECK_HEADER) -> Middleware: ...
```
Read `context.headers.get(header)` (already lower-cased by `Context.__init__`); absent → `await next()`
untouched. Present → `body = await store.get(reference)`; `None` → raise `ClaimCheckNotFound(reference)`
so the transport's normal nack → redelivery → DLQ path applies (**never** a silent skip, never
processing the placeholder). Otherwise `context.request = json.loads(body)` and continue.

**This is where Python is structurally simpler than .NET, and the spec should say so.** .NET needs an
`IMessageBodySetter<TContext>` per transport, resolved with `TryGetService`, and Azure Service Bus
hydration is *blocked outright* because `ServiceBusReceivedMessage.Body` has no setter. Python has no
such problem: every host funnels through
`BenzeneMessageApplication.handle({topic, headers, body})` (`packages/benzene-core/benzene/core/envelope.py:66`),
which parses the body once and hands the pipeline `context.request`. One middleware hydrates every
transport, including Service Bus. Do not port the body-setter abstraction. The one behavioural note
to document: .NET replaces the raw body *before* deserialization, Python replaces the already-parsed
request — same wire outcome, and the fetched body is parsed with `json.loads` here.

**3.5 `benzene/claimcheck/s3.py`** (extra `[s3]`)
```python
class S3ClaimCheckStore:
    def __init__(self, bucket: str, *, client=None, prefix: str = "claim-checks/") -> None: ...
```
Key `{prefix}{topic}/{yyyy/MM/dd}/{uuid4().hex}` (topic verbatim — S3 keys permit the `:` a Benzene
topic carries; the date segment makes a lifecycle rule auditable by eye), `ContentType`
`application/octet-stream`, reference `s3://{bucket}/{key}`. `get` validates with a **single prefix
comparison** against `s3://{bucket}/{prefix}` — not `urllib.parse.urlparse`, whose quoting would not
round-trip a topic's `:` — raising `ClaimCheckStoreMismatch` before touching S3; a 404
(`ClientError` with `NoSuchKey`/`404`) maps to `None`. boto3 via `asyncio.to_thread`; lazy import
naming `benzene-claimcheck[s3]`.

**3.6 `benzene/claimcheck/blob.py`** (extra `[azure]`, low priority) — `azblob://{container}/{key}`,
same key layout, 404 → `None`, mismatch on foreign container/scheme/prefix. Ship after S3; the
Python AWS host is the more complete of the two.

**Retention:** no delete-on-consume, ever (SNS fan-out delivers one offloaded message to several
consumers and at-least-once transports redeliver; deleting at read time starves siblings and makes a
retry permanently unhydratable). Retention is a bucket/container lifecycle rule owned by infra, and
the TTL must exceed queue retention plus the DLQ redrive window. Put the sizing rule in the store
docstring *and* `docs/reference/claim-check.md`.

**Tests** — new `tests/test_claimcheck.py`: under/over threshold; `always_offload`; a store failure
propagates and the inner sender is never called; the header and placeholder are byte-exact
(`{"_benzeneClaimCheck": ref}`, key not camel-cased); hydration passes a header-less message through
without touching the store; hydration replaces `context.request`; a `None` from the store raises
`ClaimCheckNotFound`; a foreign reference raises `ClaimCheckStoreMismatch` **without** calling the
client; in-memory TTL expiry; a real round trip — offload through `ClaimCheckMessageSender` into
`InMemoryClaimCheckStore`, then hydrate through a real `MiddlewarePipeline` and assert the handler saw
the original request object; S3 store against a fake client (key shape, reference, 404 → `None`,
mismatch, off-loop execution).

---

## 4. No timeout / deadline policy

**Severity: high.** Python's resilience set covers *failing* dependencies (circuit breaker) and
*saturated* ones (bulkhead, rate limiter) but not *hanging* ones. A downstream that accepts a
connection and never answers is the worst case for all three: no failure is recorded, so the breaker
never trips; the bulkhead fills with stuck callers and sheds healthy traffic; on Lambda the invocation
burns to its own timeout and bills for it. `RetryingMessageSender` makes it worse — it re-issues a
call that can hang again.

**What .NET has** — `src/Benzene.Resilience/TimeoutMiddleware.cs` and `.UseTimeout(...)`: link a
`CancellationTokenSource` to the ambient token, `CancelAfter(timeout)`, restore in `finally`, and —
the load-bearing part — translate the **timer's** cancellation into a `TimeoutException` (→
`BenzeneResultStatus.Timeout`) while letting a **host** cancellation propagate untouched, so
queue/settle/ack transports still redeliver interrupted work.

**What Python has** — nothing. `grep -n timeout` across `benzene-core` and `benzene-resilience` finds
only the circuit breaker's `reset_timeout`.

### Implementation spec

**Package:** existing `packages/benzene-resilience`. New module `benzene/resilience/timeout.py`.
```python
def timeout_interception(seconds: float) -> Middleware:
    """Bound the downstream pipeline; on expiry set `Result.timeout(...)` and do not run further."""

class TimeoutMessageSender:
    def __init__(self, inner: MessageSender, *, seconds: float) -> None: ...
    async def send_message(self, topic, message, headers=None) -> Result: ...

def with_timeout(inner: MessageSender, seconds: float) -> TimeoutMessageSender: ...
```
- Implement with `asyncio.timeout(seconds)` on 3.11+ and `asyncio.wait_for` on 3.10 (the repo's
  `requires-python` is `>=3.10`); a small `_deadline(seconds)` async-context-manager helper keeps the
  version check in one place.
- **The load-bearing rule, and the Python analogue of .NET's timeout-vs-cancellation filter:** catch
  only the deadline's own expiry (`TimeoutError` — note that on 3.11+ `asyncio.TimeoutError` *is*
  `TimeoutError`) and answer `Result.timeout(f"exceeded {seconds}s")`. Let `asyncio.CancelledError`
  propagate **untouched**. Swallowing `CancelledError` breaks worker shutdown
  (`benzene.aws.sqs_consumer`, `benzene.kafka`) and converts an interrupted delivery into a "handled"
  one that the transport then deletes. Guard this with a dedicated test; it is the single most likely
  thing to be got wrong.
- Nested timeouts compose naturally (innermost deadline governs) — assert it.
- Document the same caveat .NET does: the deadline only interrupts *cooperative* work. A handler that
  blocks the event loop (a sync DB driver called without `asyncio.to_thread`) is not interruptible.
- The outbound decorator is the higher-value half here, matching Benzene's "wrap the port call"
  thesis; ship both, since the inbound middleware is four extra lines.

**Tests** — extend `tests/test_resilience.py`: fast path returns the handler's result untouched; a
slow handler yields `timeout` and the handler task is actually cancelled (assert a `CancelledError`
was delivered into it); an *outer* cancellation propagates as `CancelledError`, never as a `timeout`
result; nested timeouts — the inner one wins; the sender decorator returns `timeout` and composes with
`with_retry` (a timed-out attempt is retried, since `timeout` is already in `DEFAULT_RETRYABLE`).

---

## 5. Cache errors fail the request; no cache health check

**Severity: high.** `RedisCache.get/set/delete`
(`packages/benzene-cache/benzene/cache/redis.py`) call the client directly and let every SDK
exception propagate. Through `get_or_load` that exception reaches the handler, so a Redis blip turns
a cacheable read into an `unexpected-error` — the cache becomes a hard dependency of a path it exists
to make *faster*. .NET makes the opposite promise explicitly:
`src/Benzene.Cache.Core/CLAUDE.md` — "Read failures degrade to a miss: `CacheEntry<T>.GetValueAsync`
swallows and logs a read exception rather than propagating, so a cache outage doesn't fail the
request" — and `Benzene.Cache.Redis` catches get/set/invalidate errors too. Small fix, real incident
class.

Secondly, .NET ships `CacheHealthCheck<TCacheService>` + `ICacheService.CanConnectAsync`
(`src/Benzene.Cache.Core/CacheHealthCheck.cs`) so a cache outage is visible on
`benzene:healthcheck`. Python's cache contributes nothing to health, though
`packages/benzene-core/benzene/core/health.py` already defines the trivially-satisfiable
`HealthCheck = Callable[[], HealthCheckResult | bool | Awaitable[...]]`.

### Implementation spec

**Package:** existing `packages/benzene-cache`.

**5.1 Fail-open, in `benzene/cache/redis.py`**
```python
class RedisCache:
    def __init__(self, url=None, *, client=None, fail_open: bool = True,
                 logger: logging.Logger | None = None) -> None: ...
```
Wrap each of `get`/`set`/`delete` so that when `fail_open` is true a client exception is logged at
`warning` (with the exception *type*, never the message — a connection string or key can appear in a
Redis error, the same discipline `CacheHealthCheck` uses for its `Error` datum) and the call degrades:
`get` → `None` (a miss), `set`/`delete` → no-op. `fail_open=False` re-raises, for a caller who needs
write failures surfaced. **Never** swallow `asyncio.CancelledError` (same rule as §4). Apply the same
treatment to `InMemoryCache` for symmetry — it has no I/O, so it is a no-op guard, but it keeps the
two backends behaviourally identical, which is the point of the port.

Note that `get_or_load` (`benzene/cache/aside.py`) then behaves correctly with no change: a degraded
`get` is a miss, so the loader runs; a degraded `set` means the next call reloads. Add one test that
asserts exactly that, because it is the whole value of the change.

**5.2 `benzene/cache/health.py`**
```python
def cache_health_check(cache: Cache, *, name: str = "cache",
                       probe_key: str = "benzene:cache:health") -> HealthCheck:
    """A HealthCheck that round-trips a probe value through `cache`, reporting the failure type only."""
```
Returns an async zero-arg callable → `HealthCheckResult.healthy()` /
`HealthCheckResult.unhealthy(type(exc).__name__)`. Register with the core `HealthChecks` registry.
**Do not** port `ICacheService`, `CacheHealthCheckFactory<T>`, or the generic-parameterised
health-check machinery — a plain callable is what `benzene.core.health` already takes.
Important interaction: build the probe against a `fail_open=False` view (or call the underlying
client directly), otherwise the fail-open wrapper makes the health check permanently green.

**Tests** — extend `tests/test_cache.py`: a raising fake client → `get` returns `None`, `set`/`delete`
are no-ops, a warning is logged and the message contains no key material; `fail_open=False` re-raises;
`get_or_load` still returns the loaded value when the cache is down; `CancelledError` propagates;
health check healthy and unhealthy paths and that the detail is a type name.

---

## 6. Retry is status-only, with no backoff or jitter helpers

**Severity: medium.** `RetryingMessageSender`
(`packages/benzene-core/benzene/core/outbound.py`) retries only on a *transient failure status*
(`service-unavailable` / `timeout` / `too-many-requests`) and takes `backoff` as a bare
`async (attempt) -> None` hook with no supplied implementation — so a caller who does not write their
own backoff gets a **tight retry loop**, which is worse than not retrying under load. It also never
retries a raised exception, so a `ConnectionResetError` out of a transport client is not retried at
all. .NET's `RetryMiddleware` (`src/Benzene.Resilience/RetryMiddleware.cs`) has `initialDelay`,
`backoffFactor`, `maxDelay`, an exception predicate, and `RetryMiddleware.FullJitter` — full jitter
being what stops a fleet from retrying in lockstep after a shared outage.

### Implementation spec

**Package:** `packages/benzene-core` (extend `outbound.py` — purely additive, no API break), with the
helpers re-exported from `benzene.resilience` for discoverability.
```python
def exponential_backoff(*, initial: float = 0.2, factor: float = 2.0,
                        cap: float | None = None,
                        jitter: Callable[[float], float] | None = None,
                        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                        ) -> Callable[[int], Awaitable[None]]:
    """A `backoff` hook: sleep min(cap, initial * factor**(attempt-1)), optionally jittered."""

def full_jitter(rng: random.Random | None = None) -> Callable[[float], float]:
    """AWS full jitter: uniform(0, delay). Spreads a fleet's retries after a shared outage."""
```
Mirror .NET's documented curve exactly: the **cap applies to the sleep**, while the underlying
exponential keeps compounding off the uncapped curve; jitter is applied after the cap
(`sleep = uniform(0, min(cap, initial * factor**n))`).

Add to `RetryingMessageSender.__init__`: `retry_on_exception: Callable[[BaseException], bool] | None = None`
(default `None` = today's behaviour, exceptions propagate). When supplied, a raised exception matching
the predicate consumes an attempt and is retried; `asyncio.CancelledError` is **never** retried.

Also change the default: `with_retry(sender)` with no `backoff` should default to
`exponential_backoff()` rather than no delay. That is a behaviour change to an existing API — flag it
in the changelog; the current no-delay default is a footgun, not a feature.

**Judgement — do not port the inbound `RetryMiddleware`.** .NET ships retry as pipeline middleware and
then has to warn "do not place it on an inbound context that has already written a response". On the
inbound side of a Python Benzene service the transport already redelivers (SQS visibility timeout,
Kafka offset, Service Bus lock), so an in-process handler retry mostly burns the visibility window and
duplicates side effects. The outbound decorator is the correct and sufficient home.

**Tests** — extend `tests/test_outbound.py`: the curve is exact under a recording fake `sleep`
(0.2, 0.4, 0.8 …); `cap` bounds the sleep but not the next computation; `full_jitter` with a seeded
`Random` is deterministic and always within `[0, delay]`; exception retry honours the predicate;
`CancelledError` is never retried; the no-arg default now delays.

---

## 7. Idempotency key derivation is fixed

**Severity: medium.** Python reads the key from `("idempotency-key", "message-id")` and nothing else:
a message with neither header passes straight through **undeduplicated**. .NET's
`HeaderOrBodyHashIdempotencyKeyStrategy<TContext>` falls back to a SHA-256 over the topic, version and
body, and its `IIdempotencyKeyStrategy<TContext>` seam lets a service key on a *business* identifier
(the order id in the payload) — which is the shape most real services actually want, since a
transport-generated `message-id` differs per redelivery on several brokers and dedupes nothing.

### Implementation spec

**Package:** `packages/benzene-resilience`, in `idempotency.py`.
```python
KeyStrategy = Callable[[Context], str | None]

def header_or_body_hash_key(*, headers: Sequence[str] = DEFAULT_KEY_HEADERS,
                            hash_body: bool = True, prefix: str = "") -> KeyStrategy: ...

def body_field_key(*path: str, prefix: str = "") -> KeyStrategy:
    """Key on a business identifier in the request, e.g. body_field_key("order", "id")."""
```
Add `key_strategy: KeyStrategy | None = None` to `idempotency_interception`, keeping `key_headers` for
compatibility (when `key_strategy` is given, `key_headers` is ignored). Default stays today's
header-only behaviour so no existing service changes silently; `header_or_body_hash_key()` is opt-in.

Hash construction, mirroring .NET's **length-prefixed** framing (its comment explains the real bug it
fixes: `id="order"/version="v2:create"` and `id="order:v2"/version="create"` otherwise flatten to the
same string and drop a message as a false duplicate):
`sha256(f"{len(topic)}:{topic}|{len(version)}:{version}|{len(body)}:{body}").hexdigest()`, where
`body = encode_body(context.request)`.

**Honesty note to put in the docstring:** this hash is **not** cross-language identical to .NET's.
.NET hashes the raw received body string; Python hashes a re-encoded canonical JSON of the parsed
request, and produces lower-case hex where .NET produces upper-case. That is fine — the key never
crosses the wire and each service dedupes against its own store — but nobody should claim parity here,
and a shared idempotency table between a .NET and a Python consumer must use the *header* key, not the
body hash.

**Tests** — extend `tests/test_resilience.py`: header wins over body hash; prefix applied; the hash is
deterministic across two invocations with equal bodies and differs for different topics/versions;
`hash_body=False` returns `None` (pass-through); the length-prefix case (the two colliding
topic/version splits above hash differently); `body_field_key` dedupes two deliveries carrying the
same order id under different message ids.

---

## 8. Saga has no state store and no whole-saga retry

**Severity: medium.** Python's `Saga.execute` returns a rich `SagaResult` but writes nothing anywhere.
When a process dies mid-saga, there is **no record of which steps had completed** — and since neither
port resumes a crashed saga (both are explicitly in-process), that record is the only thing that makes
manual reconciliation possible. .NET treats this as the point of `ISagaStateStore`: observability and
operational recovery, not recovery-by-replay. .NET also ships `SagaRetryPolicy` — whole-saga retry
that fires **only** on a clean rollback, never on a partial one (re-running after a partial rollback
double-applies orphaned effects).

**What .NET has** — `src/Benzene.Saga/ISagaStateStore.cs`, `SagaRetryPolicy.cs`, `SagaRunOptions.cs`,
`SagaStateEvent.cs`, `SagaRunInfo.cs`, `InMemorySagaStateStore.cs`, `SagaOutcome.cs`.

**What Python has** — `packages/benzene-resilience/benzene/resilience/saga.py`: `Saga`, `SagaStep`,
`SagaResult` (with `completed`/`compensated`/`compensation_failures`/`failed_step` — a good, honest
model), no store, no retry, no outcome enum.

### Implementation spec

**Package:** `packages/benzene-resilience`, extending `saga.py` (all additive).
```python
@dataclass(frozen=True)
class SagaRunInfo:
    saga_id: str; name: str; attempt: int; step_count: int

class SagaStateStore(Protocol):
    async def record_started(self, run: SagaRunInfo) -> None: ...
    async def record_step_completed(self, saga_id: str, attempt: int, step: str) -> None: ...
    async def record_finished(self, saga_id: str, attempt: int, result: SagaResult) -> None: ...

class InMemorySagaStateStore:
    def events_for(self, saga_id: str) -> list[SagaStateEvent]: ...

@dataclass(frozen=True)
class SagaRetryPolicy:
    max_attempts: int = 3
    initial_delay: float = 0.0
    backoff_factor: float = 2.0
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep   # injectable for tests
```
Extend the runner (keyword-only, so `execute(state)` is unchanged):
```python
async def execute(self, state: State | None = None, *, saga_id: str | None = None,
                  name: str = "saga", state_store: SagaStateStore | None = None,
                  retry: SagaRetryPolicy | None = None) -> SagaResult: ...
```
- `saga_id` defaults to `uuid4().hex`; one `record_started` **per attempt**;
  `record_step_completed` per completed step; one `record_finished` per attempt.
- Add a derived `SagaResult.outcome -> Literal["succeeded", "rolled-back", "partially-rolled-back"]`
  (`partially-rolled-back` iff `compensation_failures` is non-empty), matching .NET's `SagaOutcome`.
- Retry fires **only** when `outcome == "rolled-back"`. Never retry `partially-rolled-back` — the
  orphaned effects would be applied twice. This rule is the whole safety argument; assert it in a test
  and state it in the docstring.
- .NET's note that a durable adapter is "a 3-method copy-paste" holds here: a DynamoDB/SQL
  `SagaStateStore` is left to the application, not shipped.

**Tests** — extend `tests/test_resilience.py`: the store records start → per-step → finish in order;
only *completed* steps are recorded on a failing run; one `Started` per retry attempt; retry recovers
a flaky step; retry exhausts to a rolled-back result; retry **refuses** to re-run a
partially-rolled-back saga; an id is generated when none is given; the injected `sleep` shows the
backoff curve without real time passing.

---

## 9. Rate limiting is one algorithm with a fixed cost of 1

**Severity: medium.** Python's `RateLimiter` is a token bucket where every message costs exactly one
token. .NET's `Benzene.RateLimiting` additionally offers a fixed window, a caller-supplied
per-message cost, and — the genuinely valuable one —
`UsePayloadSizeRateLimiting(maxBurstBytes, bytesPerPeriod, replenishmentPeriod)`, a *bytes-per-second*
budget where each message costs its UTF-8 body size and a single oversized payload is always
rejected. Message count is a poor proxy for the resource a public endpoint actually burns; on
serverless the cost-amplification vector is bytes, not calls.

### Implementation spec

**Package:** `packages/benzene-resilience`, extending `rate_limit.py` (additive).
```python
class RateLimiter:                              # existing
    def try_acquire(self, cost: int = 1) -> bool: ...
    async def execute(self, run: Run, *, cost: int = 1) -> Result: ...

class FixedWindowRateLimiter:                   # new, same execute() seam
    def __init__(self, *, permits: int, window: float, clock: Clock = time.monotonic) -> None: ...

def payload_size_cost(context: Context) -> int:
    """UTF-8 byte size of the request body (min 1) — the cost function for a bytes/second budget."""

def rate_limit_interception(limiter, *, cost: int | Callable[[Context], int] = 1) -> Middleware: ...
```
- A `cost` the bucket could never grant (greater than `burst`) is a **rejection**, not an error —
  .NET is explicit about this, and the alternative (an exception for an oversized payload) is a crash
  vector on a public endpoint.
- Include a retry-after hint in the rejection: `Result.too_many_requests("rate limit exceeded",
  f"retry-after={seconds_until_a_token:.2f}")`, as .NET surfaces the limiter's retry-after metadata.
  Keep the first error string exactly as it is today so existing assertions hold.
- **Carry the honesty rule verbatim into the docstring and `docs/reference/resilience.md`**: this is a
  *per-instance* limiter. A fleet of N instances admits up to N× the configured rate and serverless
  scale-out multiplies it further; authoritative rate limiting belongs at the gateway. Python's
  current docstring does not say this, and .NET flags it as a must-keep-in-every-doc rule.

**Tests** — extend `tests/test_resilience.py`: a cost of 3 spends three tokens; a cost above `burst`
is rejected without draining the bucket; the payload-size cost function on a real `Context`;
the fixed-window limiter admits N then rejects and resets at the boundary (manual clock);
the retry-after hint appears and is roughly right.

---

## 10. Cache has no write-through or invalidate-on-success actions

**Severity: medium.** Python ships cache-aside only (`get_or_load`). .NET's `CacheWriteActions<T>` /
`CacheInvalidateActions` layering gives the *write* side: run the database mutation, then set or
invalidate the cache **only when the mutation succeeded**, with the action defaulting from the
result's status (`ok`/`created`/`accepted`/`updated` → set, `deleted` → invalidate, anything else →
no cache change). Without it, every service writes that `if result.is_successful: await cache.delete(...)`
by hand and some of them get the failure branch wrong, leaving a stale entry behind a failed write.

### Implementation spec

**Package:** `packages/benzene-cache`, new module `benzene/cache/write.py`.
```python
class CacheAction(str, Enum):
    NONE = "none"; SET = "set"; INVALIDATE = "invalidate"

def action_for(status: str) -> CacheAction:
    """ok/created/accepted/updated -> SET; deleted -> INVALIDATE; anything else -> NONE."""

async def write_through(cache: Cache, key: str,
                        modify: Callable[[], Awaitable[Result]], *,
                        value: Callable[[Result], Any] | None = None,   # default: result.payload
                        action: Callable[[Result], CacheAction] = lambda r: action_for(r.status),
                        ttl: float | None = None) -> Result: ...

async def invalidate_on_success(cache: Cache, keys: Sequence[str],
                                modify: Callable[[], Awaitable[Result]]) -> Result: ...
```
Add `CacheAside.write_through(...)` bound to the instance's cache and default TTL, for symmetry with
its `get_or_load`.

**Explicitly do not port** `RedisWildcardActions` (`KEYS <pattern>` then batched delete). .NET's own
doc warns it scans the keyspace. If pattern invalidation is ever wanted, implement it over `SCAN` with
a cursor and never `KEYS` — that is an improvement on the reference, not a port of it. Multi-key
write/invalidate (`RedisMultiKeyActions`) reduces in Python to `asyncio.gather` over the same three
methods; `invalidate_on_success(keys=[...])` covers it without a new abstraction.

**Tests** — extend `tests/test_cache.py`: each status maps to the documented action; a failed mutation
leaves the cache untouched; a custom value mapper; a custom action mapper; `invalidate_on_success`
deletes every key only on success; write-through honours the bound default TTL.

---

## 11. Saga has no concurrent stages

**Severity: low.** .NET's model is `Saga → Stage (concurrent group) → Step`, with await-all (not
fail-fast) semantics inside a stage, compensation of a failed stage's succeeded members first, then
completed stages newest-first. Python's saga is a flat sequential list. This matters only for sagas
whose steps are genuinely independent and latency-sensitive; the sequential shape is correct for
everything else, and the reverse-order compensation Python already implements is the part that carries
the safety.

**Spec, if picked up:** add `Saga.stage(name: str, *steps: SagaStep) -> Saga` grouping steps run with
`asyncio.gather(..., return_exceptions=True)` (**await-all**, deterministic — every step's outcome is
known before deciding to compensate); a stage succeeds only if every step does; on failure compensate
that stage's succeeded steps first, then earlier stages in reverse. Keep `step()` as sugar for a
one-step stage so the existing API and its tests are untouched. Tests: a stage runs its steps
concurrently (assert overlap via an event), one failure compensates its siblings, cross-stage LIFO
order is preserved.

---

## 12. Idempotency in-flight behaviour is not configurable

**Severity: low.** A duplicate arriving while the first delivery is still running always gets
`conflict` in Python (equivalent to .NET's `InProgressBehavior.Throw`), so the transport redelivers
later. .NET also offers `Skip` — drop the duplicate silently. Python's default is the **safer** of the
two (`Skip` loses a message whenever the sibling later fails before releasing the key), so this is a
knob, not a gap.

**Spec, if picked up:** `idempotency_interception(..., on_in_flight: Literal["conflict", "skip"] = "conflict")`;
`"skip"` sets `Result.ok()` and does not run the handler. Document the loss window on `"skip"`. Do not
change the default.

---

## 13. Not worth porting

Each of these is a .NET-ecosystem artefact, a shape Python already improves on, or a deliberate
boundary the Python port should keep.

| .NET thing | Verdict | Why / the Python answer |
|---|---|---|
| `Benzene.Outbox.EntityFramework` | **Not applicable as such** — port the *capability*, not the shape | There is no `DbContext`, change tracker, or `IDbContextFactory` in Python. The whole "why the store uses a factory, not a scoped `DbContext`" section is reasoning about .NET DI lifetimes and thread-unsafe contexts; it evaporates. The Python equivalent is §2.10's `SqlOutboxStore` over a SQLAlchemy `AsyncEngine` (or a DB-API `connect` callable), plus a stage that writes on the *caller's own* connection and never commits. Notably the optimistic-concurrency fallback also evaporates: EF needs it only because the EF InMemory provider lacks `ExecuteUpdate`; a real database claims with one conditional `UPDATE` and a `rowcount` check. |
| `Benzene.Resilience.Polly` (whole package) | **Not applicable** | Polly is a .NET library. Python already ships native `Result`-aware circuit breaker, bulkhead and rate limiter in `benzene-resilience`, plus the retry decorator in core. Do **not** wrap `tenacity`/`pybreaker` to mirror the package list — that would add a dependency to re-expose policies the port already owns, in a less `Result`-native form. `BenzeneFailureResultException` exists purely to bridge Benzene's result-on-context model into Polly's exception-shaped outcome model; where the policies are already result-native there is nothing to bridge. The genuinely missing policy from that package's surface is **timeout** (§4) and it is 30 lines of `asyncio`. |
| `IMessageBodySetter<TContext>` + per-transport hydration adapters | **Not needed** | Python parses the body once in `BenzeneMessageApplication.handle` and hands the pipeline `context.request`, so one hydrate middleware serves every transport. This also removes .NET's outright *blocker* — `ServiceBusReceivedMessage.Body` has no public setter, so Azure Service Bus hydration is unimplemented there. Python gets it for free. Say so in the claim-check docs; it is a genuine advantage of the port, not an omission. |
| `OutboxEnvelope.PayloadType` (assembly-qualified type name) | **Do not require** | .NET needs `Type.GetType(payloadType)` to rehydrate a typed request. Python re-sends the parsed JSON through `send_message(topic, dict)` — the type is a handler-side concern resolved by the receiving service's registry. Write the attribute in the DynamoDB/SQL store (a constant such as `"application/json"`) purely so a shared table stays readable by a .NET dispatcher; never read it. |
| `OutboxStreamImage` POCO + `ToEnvelope()` | **Reduce to a helper** | `benzene-aws` already decodes DynamoDB Streams events. A relay only needs the envelope id, so §2.9's `outbox_stream_ids(event) -> list[str]` replaces the whole type. |
| `ClaimResult` / `IdempotencyRecord` / `IdempotencyStatus` | **Already superseded** | Python's `put_if_absent -> bool` plus `get -> Result \| None` is simpler *and* strictly more capable: it replays the real first result, where .NET can only synthesize a fresh `BenzeneResult.Ok()` and explicitly documents that "the original first-attempt response/payload is not stored or replayed". Do not add the value objects to regain .NET's shape. |
| `RedisCacheService` (abstract, subclass-and-override `GetConfigurationOptionsAsync`), `IRedisConnectionFactory`, `ConfigurationOptions` | **Already superseded** | Python injects a duck-typed async client (`RedisCache(client=...)`) or a URL. That is the idiomatic seam and it is what makes the tests run with a dict-backed fake and no SDK. Do not introduce a connection-factory interface or an abstract base class to mirror the layering. |
| `ICacheService` + `CacheHealthCheckFactory<TCacheService>` | **Reduce** | Keep the *health check* (§5.2) but as a plain callable — `benzene.core.health.HealthCheck` is already `Callable[[], HealthCheckResult \| bool \| Awaitable[...]]`. The generic factory and marker interface are DI-container machinery with no Python counterpart. |
| `RedisWildcardActions` (`KEYS`-based pattern invalidation) | **Do not port** | .NET's own doc warns it scans the keyspace. If needed, build it on `SCAN`; see §10. |
| `AddX(...)` / `UseX(...)` DI + pipeline extension pairs, per-route `OutboxOptions.Clone()` | **Not applicable** | Python composes decorators and passes objects to constructors. Per-route option divergence is just constructing a second `OutboxMessageSender` with different options. No service-container extension surface is needed, and adding one would fight the port's existing style. |
| `Benzene.ClaimCheck.Azure.Blob` | **Defer, not decline** | A real store with a real audience, but ship S3 first: the Python AWS host is the more complete of the two, and .NET's own Azure story is offload-only (hydration is blocked there — a blocker Python does not have, §13 row 3). Low priority. |
| Outbox dead-letter forwarding | **Keep the boundary** | .NET deliberately parks poison envelopes rather than dead-lettering, as the operator's evidence. Python should keep the identical boundary and the identical wording. Not a gap. |
| Saga durable crash-resume | **Keep the boundary** | Both ports are explicitly in-process; step closures cannot be serialized. The honest guidance — reach for Step Functions / Durable Functions / Temporal for crash-durable orchestration — should be carried into the Python docs verbatim. §8's state store is for observability and manual reconciliation, and must not be described as resume. |
| Cache stampede / single-flight protection | **Neither has it; matching is correct** | .NET lists it as a deliberate non-goal (`CacheEntry<T>.LazyLoadAsync` is plain cache-aside). If Python ever adds it, an `asyncio` single-flight over `get_or_load` (a per-key `asyncio.Future` map) is a genuine improvement — but it would be *ahead* of the reference, not parity, and should be decided on its own merits. |

---

## Suggested sequencing

1. **§1 shared idempotency stores** — days, and it closes a hole that is silently open in production today.
2. **§4 timeout** and **§5 cache fail-open** — each is a small, self-contained module in an existing package, and both are incident-class fixes.
3. **§2 outbox** — the large one. Core engine + in-memory store + dispatcher + worker first (usable end-to-end in one process), then the DynamoDB store and the Streams relay helper, then the SQL store.
4. **§3 claim check** — core + in-memory + S3. Settle the frozen-header decision with the spec repo before writing code.
5. **§6–§10** — the depth items, in any order.
6. **§11–§12** — only on demand.
