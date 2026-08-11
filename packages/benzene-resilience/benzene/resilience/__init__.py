"""``benzene.resilience`` — resilience policies beyond retry.

The Benzene port already ships retry as an outbound decorator (``benzene.core.with_retry``); this
distribution (``benzene-resilience``) adds the rest of the resilience surface the .NET port carries:

* **Circuit breaker** — stop calling a failing dependency; fail fast, then probe for recovery.
* **Bulkhead** — cap concurrent invocations so one slow dependency can't exhaust the service.
* **Rate limiting** — a token bucket that enforces ``too-many-requests`` at the edge.
* **Idempotency** — dedupe redelivered messages by replaying the first result.
* **Saga** — an in-process compensating sequence (undo completed steps when a later one fails).

Every gating policy (circuit breaker, bulkhead, rate limiter) exposes one ``execute(run)`` seam and
ships in two shapes off it — an inbound ``*_interception`` middleware and an outbound ``MessageSender``
decorator — so resilience is a decorator over the one ``Result``-returning contract, composing freely
with the core's ``with_retry`` / ``with_correlation_id``. Depends only on ``benzene-core``.

Mirrors .NET's ``Benzene.Resilience.Polly`` (circuit breaker + bulkhead), ``Benzene.RateLimiting``,
``Benzene.Idempotency``, and ``Benzene.Saga``. Contributes the ``benzene.resilience`` subpackage to
the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .bulkhead import (
    Bulkhead,
    BulkheadMessageSender,
    bulkhead_interception,
    with_bulkhead,
)
from .circuit_breaker import (
    DEFAULT_TRIP_ON,
    CircuitBreaker,
    CircuitBreakingMessageSender,
    CircuitOpenError,
    CircuitState,
    circuit_breaker_interception,
    with_circuit_breaker,
)
from .idempotency import (
    DEFAULT_KEY_HEADERS,
    IdempotencyStore,
    InMemoryIdempotencyStore,
    idempotency,
)
from .rate_limit import (
    RateLimiter,
    RateLimitingMessageSender,
    rate_limit_interception,
    with_rate_limit,
)
from .saga import (
    Saga,
    SagaAction,
    SagaCompensation,
    SagaResult,
    SagaStep,
)

__all__ = [
    # circuit breaker
    "CircuitBreaker",
    "CircuitBreakingMessageSender",
    "CircuitOpenError",
    "CircuitState",
    "DEFAULT_TRIP_ON",
    "circuit_breaker_interception",
    "with_circuit_breaker",
    # bulkhead
    "Bulkhead",
    "BulkheadMessageSender",
    "bulkhead_interception",
    "with_bulkhead",
    # rate limiting
    "RateLimiter",
    "RateLimitingMessageSender",
    "rate_limit_interception",
    "with_rate_limit",
    # idempotency
    "DEFAULT_KEY_HEADERS",
    "IdempotencyStore",
    "InMemoryIdempotencyStore",
    "idempotency",
    # saga
    "Saga",
    "SagaAction",
    "SagaCompensation",
    "SagaResult",
    "SagaStep",
]
