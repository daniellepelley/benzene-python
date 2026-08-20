# benzene-resilience

Resilience policies for [Benzene Python](https://github.com/daniellepelley/benzene-python) beyond the
retry that ships in the core — **circuit breaker**, **bulkhead**, **rate limiting**, **idempotency**,
and an in-process **saga**. Depends only on `benzene-core`.

```bash
pip install benzene-resilience
```

Every gating policy has one `execute(run)` seam and ships in two shapes off it — an inbound
`*_interception` middleware and an outbound `MessageSender` decorator — so it guards a handler
pipeline and an outbound client with the same object, composing with the core's `with_retry` /
`with_correlation_id`.

```python
from benzene.resilience import (
    CircuitBreaker,
    circuit_breaker_interception,
    with_circuit_breaker,
    Bulkhead,
    bulkhead_interception,
    RateLimiter,
    rate_limit_interception,
    InMemoryIdempotencyStore,
    idempotency_interception,
    Saga,
)

# Inbound: cross-cutting middleware, installed ahead of the message router in the AppDefinition.
definition.middleware += [
    rate_limit_interception(
        RateLimiter(refill_rate=100, burst=200)
    ),  # too-many-requests at the edge
    bulkhead_interception(Bulkhead(max_concurrency=20, max_queue=40)),  # shed load past capacity
    idempotency_interception(InMemoryIdempotencyStore(ttl=3600)),  # dedupe redeliveries
]

# Outbound: decorate a MessageSender, same shape as with_retry.
sender = with_circuit_breaker(orders_client, failure_threshold=5, reset_timeout=30)
```

- **Circuit breaker** — after `failure_threshold` consecutive *server-side* failures the circuit
  opens and rejects with `service-unavailable` for `reset_timeout` seconds, then admits one probe;
  a success closes it, a failure re-opens it. Client errors (`bad-request`, `not-found`) are handled
  outcomes and don't trip it.
- **Bulkhead** — at most `max_concurrency` invocations run at once, `max_queue` more may wait, and the
  next is shed immediately with `too-many-requests` — one slow dependency can't exhaust the service.
- **Rate limiting** — a continuous-refill token bucket (`refill_rate`/sec, tolerating a `burst`) that
  enforces `too-many-requests` at the edge; the concrete producer of that status the port was missing.
- **Idempotency** — keys each invocation on an idempotency header and replays the first result for a
  redelivery, so at-least-once transports don't run a handler twice. The key is *reserved* atomically
  before the handler runs, so two deliveries that overlap can't both run it: the duplicate gets
  `conflict` ("duplicate delivery is already in flight") and is redelivered once the first finishes.
  Only successes are remembered by default, so a transient failure — or a handler that raises —
  releases the key and stays retryable. The store is a pluggable async port (in-memory impl included).
  (`idempotency_interception` is the preferred name; the original `idempotency` still works.)
- **Saga** — sequence steps that each know how to undo themselves; a later failure compensates the
  completed steps in reverse. In-process (not durable) and `Result`-shaped — `execute` returns a
  `SagaResult`, it never raises for a step failure.

Nothing here needs a broker, a clock you can't control, or any third-party package: the gating
policies take an injectable `clock`, the idempotency store an injectable one too, so every policy is
exercised deterministically in memory. Mirrors .NET's `Benzene.Resilience.Polly`,
`Benzene.RateLimiting`, `Benzene.Idempotency`, and `Benzene.Saga`, and contributes the
`benzene.resilience` subpackage to the shared `benzene` namespace.
