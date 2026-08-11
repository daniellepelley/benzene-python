# `benzene.resilience`

Resilience policies **beyond retry** — a circuit breaker, a bulkhead, a rate limiter, idempotent
dedupe, and an in-process saga. **Distribution: `benzene-resilience` (depends only on `benzene-core`).**

```bash
pip install benzene-resilience
```

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
reach the handler twice. The `idempotency` middleware makes the *second* delivery a no-op by keying
each invocation on a caller-supplied idempotency header and **replaying the first result** — so "charge
the card" happens once even though the message arrived twice.

```python
from benzene.resilience import InMemoryIdempotencyStore, idempotency

definition.middleware += [idempotency(InMemoryIdempotencyStore(ttl=3600))]
```

### `idempotency`

```python
idempotency(
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

### `IdempotencyStore` and `InMemoryIdempotencyStore`

`IdempotencyStore` is the pluggable seam — a runtime `Protocol` of two async methods, so a
network-backed store (Redis, DynamoDB) is a drop-in:

```python
class IdempotencyStore(Protocol):
    async def get(self, key: str) -> Result | None: ...
    async def put(self, key: str, result: Result) -> None: ...
```

`InMemoryIdempotencyStore(*, ttl=None, clock=time.monotonic)` is the process-local implementation for
tests and single-instance services. `ttl` (seconds) bounds how long a key is remembered (`None` keeps
it for the process lifetime); `clock` is injectable so a test expires entries without sleeping.

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
`InMemoryIdempotencyStore`, `DEFAULT_KEY_HEADERS`, `idempotency`; `Saga`, `SagaStep`, `SagaResult`,
`SagaAction`, `SagaCompensation`.

## See also

- [`benzene.core`](core.md) — the pipeline, the `MessageSender` port, and `with_retry` /
  `with_correlation_id` these policies compose with.
- [`benzene.results`](results.md) — the `Result` and `Status` vocabulary (`service-unavailable`,
  `too-many-requests`) these policies produce.
