# `benzene.resilience`

Resilience policies **beyond retry** — a circuit breaker, a bulkhead, a rate limiter, idempotent
dedupe, and an in-process saga. **Distribution: `benzene-resilience` (depends only on `benzene-core`).**

```bash
pip install benzene-resilience               # every policy, no third-party SDK
pip install benzene-resilience[redis]        # + the Redis idempotency store
pip install benzene-resilience[dynamodb]     # + the DynamoDB idempotency store
```

Both extras are optional and imported lazily: the engine, and every test, runs with neither
installed.

## Overview

The core already ships retry as an outbound decorator (`benzene.core.with_retry`); this distribution
adds the rest of the resilience surface the .NET port carries, mirroring `Benzene.Resilience.Polly`
(circuit breaker + bulkhead), `Benzene.RateLimiting`, `Benzene.Idempotency`, and `Benzene.Saga`.

The three **gating** policies — circuit breaker, bulkhead, rate limiter — share one design. Each is a
plain object with a single `execute(run)` seam that wraps a `Result`-returning unit of work, and each
ships in **two shapes off that seam**:

- an inbound **`*_interception` middleware** you install ahead of the message router, and
- an outbound **`with_*` decorator** over a `benzene.core.MessageSender`, exactly the shape of
  `with_retry`.

So the *same* policy object can guard a handler pipeline and an outbound client, and the policies
compose freely with the core decorators — `with_retry(with_circuit_breaker(sender))`. Every gating
policy takes an injectable `clock`, and the idempotency store an injectable one too, so the whole
surface is exercised deterministically in memory — no broker, no sleeping, no third-party package.

The two remaining pieces are not gates: **idempotency** is dedupe middleware over a pluggable store,
and **saga** is a compensating in-process sequence.

## The one seam, two shapes

Every gating policy exposes:

```python
async def execute(self, run: Run) -> Result: ...   # Run = Callable[[], Awaitable[Result]]
```

The inbound middleware wraps the pipeline's `next()` in a `run` that returns the context result; the
outbound decorator wraps the client's `send_message`. When a policy rejects, the middleware **does not
call `next()`** (the handler never runs) and the decorator **does not call the inner sender** — the
caller gets the rejection status directly.

## Circuit breaker

Stops a service from hammering a dependency that is already failing: after `failure_threshold`
consecutive *tripping* failures the circuit **opens** and rejects fast with `service-unavailable` for
`reset_timeout` seconds, then admits a single probe (**half-open**) — a success closes it, a failure
re-opens it.

```python
from benzene.resilience import CircuitBreaker, circuit_breaker_interception, with_circuit_breaker

breaker = CircuitBreaker(failure_threshold=5, reset_timeout=30.0)

# Inbound: guard the pipeline (install ahead of the router, typically ahead of retry).
definition.middleware += [circuit_breaker_interception(breaker)]

# Outbound: guard a client. Pass a shared breaker, or keyword options to build a fresh one.
sender = with_circuit_breaker(orders_client, failure_threshold=3, reset_timeout=15)
```

### `CircuitBreaker`

```python
CircuitBreaker(
    *,
    failure_threshold: int = 5,          # consecutive tripping failures that open the circuit
    reset_timeout: float = 30.0,         # seconds open before one probe is admitted
    trip_on: Iterable[str] = DEFAULT_TRIP_ON,
    clock: Clock = time.monotonic,       # injectable monotonic clock (seconds)
)
```

- `state` — a read-only `CircuitState` (`CLOSED` / `OPEN` / `HALF_OPEN`), resolving an elapsed open
  window to `HALF_OPEN` on read.
- `execute(run)` — runs `run` under the breaker, or returns `Result.service_unavailable(...)` when
  open. A tripping failure (or a raised exception) advances toward open; anything else resets the
  consecutive count. An exception is **recorded and re-raised** — the breaker observes faults, it does
  not swallow them — so the surrounding envelope still maps it.
- `trip_on` — the statuses that count as failures. The default `DEFAULT_TRIP_ON` is the server-side /
  transient set (`service-unavailable`, `timeout`, `too-many-requests`, `unexpected-error`). A client
  error like `bad-request` or `not-found` is a handled outcome the dependency answered correctly, so it
  **resets** the breaker rather than tripping it.

`CircuitOpenError` is exported as a named type for callers that branch on the reason string
(`"circuit breaker is open"`); the breaker itself never raises it — a rejection is always a failure
`Result`.

## Bulkhead

Caps how many invocations run *at once* so one slow dependency can't exhaust the whole service. At most
`max_concurrency` calls run concurrently, up to `max_queue` more may wait for a slot, and the next call
beyond that is **shed immediately** with `too-many-requests` rather than piling up unbounded.

```python
from benzene.resilience import Bulkhead, bulkhead_interception, with_bulkhead

bulkhead = Bulkhead(max_concurrency=20, max_queue=40)

definition.middleware += [bulkhead_interception(bulkhead)]        # inbound
sender = with_bulkhead(inventory_client, max_concurrency=10)      # outbound
```

### `Bulkhead`

```python
Bulkhead(max_concurrency: int, *, max_queue: int = 0)
```

- `in_flight` — admitted calls currently running or queued (excludes shed calls).
- `execute(run)` — runs `run` when a slot or a queue place is free, else returns
  `Result.too_many_requests("bulkhead is full")`. The admission check is a synchronous
  compare-and-increment before any `await`, so the shed decision is race-free under cooperative asyncio
  scheduling.

## Rate limiter

A continuous-refill **token bucket** that enforces `too-many-requests` at the edge — the concrete
producer of that back-pressure status the port was missing before this package. `refill_rate` tokens
are added per second up to a ceiling of `burst` (which also seeds the bucket full, so an idle service
absorbs a burst immediately); each call spends one token, and a call that finds the bucket empty is
rejected.

```python
from benzene.resilience import RateLimiter, rate_limit_interception, with_rate_limit

limiter = RateLimiter(refill_rate=100, burst=200)

definition.middleware += [rate_limit_interception(limiter)]       # inbound
sender = with_rate_limit(notifications_client, refill_rate=50)    # outbound
```

### `RateLimiter`

```python
RateLimiter(
    *,
    refill_rate: float,                  # tokens added per second (must be > 0)
    burst: int = 1,                      # bucket ceiling; also the initial fill
    clock: Clock = time.monotonic,       # injectable so tests drive refill without sleeping
)
```

- `available` — tokens available right now (after refilling for the elapsed time).
- `execute(run)` — spends a token and runs, or returns `Result.too_many_requests("rate limit
  exceeded")` when the bucket is empty.

A non-positive `refill_rate` raises `ValueError` at construction.

## Idempotency

At-least-once transports (SQS, Pub/Sub, Kafka, Service Bus) redeliver: the same logical message can
reach the handler twice. The `idempotency_interception` middleware makes the *second* delivery a
no-op by keying each invocation on a caller-supplied idempotency header and **replaying the first
result** — so "charge the card" happens once even though the message arrived twice.

```python
from benzene.resilience import InMemoryIdempotencyStore, idempotency_interception

definition.middleware += [idempotency_interception(InMemoryIdempotencyStore(ttl=3600))]
```

### `idempotency_interception`

```python
idempotency_interception(         # the older name `idempotency` remains a working alias
    store: IdempotencyStore,
    *,
    key_headers: Sequence[str] = DEFAULT_KEY_HEADERS,   # ("idempotency-key", "message-id")
    remember_when: Callable[[Result], bool] | None = None,
) -> Middleware
```

- The key is read from the first present of `key_headers`. A **keyless** message passes straight
  through — there is nothing to dedupe on.
- A first delivery runs the handler; if its result satisfies `remember_when` (default: the result is
  successful) it is stored. A repeat delivery short-circuits with the stored result and **never touches
  the handler**.
- Only remembered outcomes replay. Because the default only remembers successes, a transient failure is
  *not* pinned in place — a redelivery is free to retry it. Override `remember_when` to change that
  policy.
- Install it ahead of the message router.

### `IdempotencyStore` and the stores that implement it

`IdempotencyStore` is the pluggable seam — a `Protocol`, so a network-backed store is a drop-in and
nothing needs to inherit from anything:

```python
class IdempotencyStore(Protocol):
    async def get(self, key: str) -> Result | None: ...
    async def put(self, key: str, result: Result) -> None: ...
    async def put_if_absent(self, key: str, result: Result) -> bool: ...
    async def delete(self, key: str) -> None: ...
```

`put_if_absent` **must be atomic** — it is what stops two overlapping deliveries of the same key from
both running the handler, so implement it with `SET NX` on Redis or a conditional put on DynamoDB,
never as a read followed by a write. It returns `True` when this caller won the reservation.

The middleware reserves the key before running the handler. A delivery whose twin is still in flight
is answered `conflict` rather than being run a second time or made to wait; a settled result is
replayed as before; and a handler that raises releases its reservation, so a redelivery is not locked
out.

Three stores ship:

| Store | Reservation | Use it for |
|---|---|---|
| `InMemoryIdempotencyStore` | a `dict` write with no `await` between read and write | tests, and services that genuinely run **one** instance |
| `RedisIdempotencyStore` | `SET key value NX EX ttl` — one command | anything with a Redis, containers and pods |
| `DynamoDbIdempotencyStore` | `PutItem` with `attribute_not_exists(#pk) OR expiresAt <= :now` | Lambda and other serverless shapes |

> **The in-memory store is single-process, and a multi-instance service using it is not
> deduplicating.** Two pods, two Lambda invocations or two SQS consumer replicas dedupe against two
> different dictionaries, so both run the handler. Nothing errors and nothing logs — the card is
> charged twice. Configure a shared store, or accept that dedupe is off.

And the honest limit on the shared stores: **a shared store relocates the race, it does not remove
it.** Independent processes cannot coordinate at runtime; what a conditional write buys you is that
the *store* orders the two deliveries, so exactly one reservation wins. Everything outside that one
command is still at-least-once — a key that lapsed between deliveries, a store outage, a redelivery
beyond `ttl` — so handlers should still be designed to tolerate running twice. Dedupe middleware is
a large improvement on nothing; it is not a distributed transaction.

#### `InMemoryIdempotencyStore`

```python
InMemoryIdempotencyStore(*, ttl: float | None = None, clock: Clock = time.monotonic)
```

The process-local implementation. `ttl` (seconds) bounds how long a key is remembered (`None` keeps
it for the process lifetime); `clock` is injectable so a test expires entries without sleeping.

#### `RedisIdempotencyStore`

```bash
pip install benzene-resilience[redis]
```

```python
from benzene.resilience import RedisIdempotencyStore, idempotency_interception

store = RedisIdempotencyStore("redis://cache:6379", ttl=3600)
definition.middleware += [idempotency_interception(store)]
```

```python
RedisIdempotencyStore(
    url: str | None = None,
    *,
    client: Any | None = None,          # an already-built redis.asyncio client (or a fake)
    ttl: float | None = 86_400.0,       # seconds; None = no expiry
    prefix: str = "benzene:idem:",
)
```

`put_if_absent` is **one** command, `SET ... NX`, with the TTL in the same command — never `EXISTS`
then `SET` (both callers would read "absent" across the await between them), and never a follow-up
`EXPIRE` (a crash in between pins the key forever). Redis answers nil when `NX` finds the key
present, so the reply *is* the answer.

Only the client's `get` / `set` / `delete` are used, so any duck-typed stand-in works and the tests
need no Redis. Sub-second TTLs go out as `px` milliseconds, whole seconds as `ex`, both rounded
**up** — the same rule `RedisCache` follows. The `redis` SDK is imported lazily; a missing `[redis]`
extra raises an `ImportError` naming it, at construction, rather than silently disabling dedupe.

#### `DynamoDbIdempotencyStore`

```bash
pip install benzene-resilience[dynamodb]
```

```python
DynamoDbIdempotencyStore(
    table_name: str,
    *,
    client: Any | None = None,          # an already-built boto3 dynamodb client (or a fake)
    ttl: float | None = 86_400.0,
    partition_key: str = "pk",
    clock: Callable[[], float] = time.time,   # epoch seconds, UTC
)
```

`put_if_absent` is one conditional `PutItem`; DynamoDB evaluates the condition and writes the item
atomically, and the loser's `ConditionalCheckFailedException` is mapped to `False` rather than
raised. Any *other* error (throttling, access denied) propagates — a store you cannot reach must not
be mistaken for a key that is already taken.

Two things are worth knowing before you deploy it:

- **A lapsed record reads as absent.** DynamoDB's TTL sweeper lags by up to 48 hours, so both `get`
  and the condition compare `expiresAt` against the clock themselves. Waiting for the physical delete
  would make an expired key unreclaimable for two days.
- **Reads are strongly consistent** (`ConsistentRead=True`). An eventually-consistent read could miss
  the reservation another instance wrote a millisecond ago — which is exactly the read being made.

The store **never creates the table** and never enables TTL: a single string partition key
(`partition_key`, default `pk`) and TTL on `expiresAt` are your infrastructure. Every blocking
`boto3` call goes through `asyncio.to_thread`, so dedupe never stalls a co-hosted ASGI server. The
item mirrors .NET's — `pk`, `status` (`"InProgress"` / `"Completed"`), `wasSuccessful`, `expiresAt`,
plus a Python-only `result` .NET ignores — so a mixed-language fleet can share one table, and a
record written by a .NET service (no `result` attribute) still reads as "this key is taken".

#### `ttl` is a correctness setting, not a tidiness one

It must **exceed the transport's maximum redelivery window**. A redelivery arriving after the key
lapsed finds nothing and runs the handler again — SQS's 14-day retention, not its 30-second
visibility timeout, is the number to size against. Too long instead of too short: the cost is
storage.

#### Writing your own store

Implement the four methods (no inheritance — it is a `Protocol`) over anything offering an **atomic
conditional write**: a unique-key `INSERT`, an etag-conditional blob write, a Postgres
`INSERT ... ON CONFLICT DO NOTHING`. `encode_result` / `decode_result` are exported for serialising
the stored `Result`, and their contract is worth reading before rolling your own: `errors` must come
back a **tuple** (`Result` is a frozen dataclass compared field-by-field, and the middleware
recognises its reservation marker with `settled == IN_PROGRESS`), and `benzene.core.encode_response`
is *not* a substitute — it collapses several errors into one `detail` string.

They persist the status, the payload in its wire form, every error whole (with `field` and `code`),
an explicit `successful` classification, and an application-authored problem document. What they
deliberately drop is the payload's Python *type*: a replayed payload is JSON data — a `dict`, not the
dataclass or model the handler returned — which is what a durable store can honestly hold and what
the wire edge would have produced anyway. Nothing is added: no exception text, no traceback, no host
identity. A shared store is a new home for whatever your remembered results carry, so scope its
credentials and its TTL accordingly.

## Saga

An **in-process** compensating sequence: steps that each change state and each know how to *undo* that
change. If any step fails, the steps that already ran are compensated in reverse order, so a multi-step
operation that can't complete leaves no half-finished trail. It is not durable — orchestration and
compensation live in one process — and it is `Result`-shaped: `execute` never raises for a step
failure, it returns a `SagaResult`.

```python
from benzene.resilience import Saga

async def reserve_stock(state):   # a SagaAction: mutate state, return a Result or None
    state["reserved"] = await inventory.reserve(state["sku"])
    return None                    # None means "ok, continue"

async def release_stock(state):   # a SagaCompensation: undo the action
    await inventory.release(state["reserved"])

async def take_payment(state):
    charge = await payments.charge(state["amount"])
    if charge is None:
        return Result.service_unavailable("payment declined")
    state["charge"] = charge
    return None

saga = (
    Saga()
    .step("reserve", reserve_stock, release_stock)
    .step("pay", take_payment)          # no compensation needed for a step that took no effect
)

outcome = await saga.execute({"sku": "ABC", "amount": 999})
if outcome.is_successful:
    ...                                 # outcome.result is ok(final_state)
```

### `Saga`

- `step(name, action, compensation=None) -> Saga` — append a step (chainable). `action` is a
  `SagaAction` (`async (state) -> Result | None`; return `None` or a successful `Result` to continue,
  a failure `Result` to trigger rollback). `compensation` is a `SagaCompensation`
  (`async (state) -> None`) that undoes the action.
- `add(step: SagaStep) -> Saga` — append a pre-built step (chainable).
- `execute(state=None) -> SagaResult` — run the steps in order over a copy of `state`. A step "fails"
  when its action returns a failure `Result` or raises (a raise becomes an `unexpected-error` result).
  On the first failure, forward progress stops and the completed steps are compensated in reverse.
  Compensation is best-effort: every completed step's compensation is attempted even if an earlier one
  raised, and any raise is recorded, never propagated.

### `SagaStep` and `SagaResult`

```python
@dataclass(frozen=True)
class SagaStep:
    name: str
    action: SagaAction
    compensation: SagaCompensation | None = None

@dataclass(frozen=True)
class SagaResult:
    result: Result                       # ok(final_state) on success, else the failing step's result
    state: State                         # the working state (dict[str, Any])
    completed: list[str] = ...            # steps whose action succeeded, in run order
    compensated: list[str] = ...          # steps successfully rolled back, in the order they were undone
    compensation_failures: list[str] = ...# steps whose compensation itself raised (surfaced, not hidden)
    failed_step: str | None = None
    # .is_successful -> self.result.is_successful
```

## Composing with the core

Every gating policy is a decorator over the one `Result`-returning contract, so it stacks with the core
outbound decorators. The order encodes intent:

```python
from benzene.core import with_retry, with_correlation_id

# Breaker inside retry: a fast rejection short-circuits before retrying a known-open dependency.
sender = with_retry(with_circuit_breaker(orders_client, failure_threshold=3))

# Or breaker outside retry: a whole retried burst counts as one failure against the breaker.
sender = with_circuit_breaker(with_retry(orders_client))
```

Inbound, list the interceptions on the `AppDefinition.middleware` ahead of the message router — rate
limit and bulkhead at the edge to shed load early, idempotency to dedupe before the handler runs.

## Exports

`CircuitBreaker`, `CircuitBreakingMessageSender`, `CircuitOpenError`, `CircuitState`,
`DEFAULT_TRIP_ON`, `circuit_breaker_interception`, `with_circuit_breaker`; `Bulkhead`,
`BulkheadMessageSender`, `bulkhead_interception`, `with_bulkhead`; `RateLimiter`,
`RateLimitingMessageSender`, `rate_limit_interception`, `with_rate_limit`; `IdempotencyStore`,
`InMemoryIdempotencyStore`, `RedisIdempotencyStore`, `DynamoDbIdempotencyStore`, `encode_result`,
`decode_result`, `IN_PROGRESS`, `DEFAULT_KEY_HEADERS`, `idempotency_interception` (alias
`idempotency`); `Saga`, `SagaStep`, `SagaResult`,
`SagaAction`, `SagaCompensation`.

## See also

- [`benzene.core`](core.md) — the pipeline, the `MessageSender` port, and `with_retry` /
  `with_correlation_id` these policies compose with.
- [`benzene.results`](results.md) — the `Result` and `Status` vocabulary (`service-unavailable`,
  `too-many-requests`) these policies produce.
