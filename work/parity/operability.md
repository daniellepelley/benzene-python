# Parity gap analysis — health checks, diagnostics, configuration, hosting & operability

**Reference:** `/workspace/benzene-dotnet` (the .NET port — widest surface)
**Target:** `/home/user/benzene-python` (this repo)
**Scope:** `Benzene.HealthChecks[.Core|.Http|.Tcp|.Disk|.DynamoDb|.EntityFramework|.Schema|.Azure.ServiceBus]`,
`Benzene.Clients.HealthChecks`, `Benzene.Diagnostics`, `Benzene.Configuration.Core`,
`Benzene.HostedService`, `Benzene.SelfHost`, `Benzene.ResponseEvents` — against
`benzene/core/health.py`, `benzene/http/standard.py`, the three consumer loops, and `benzene-otel`.

## State-of-the-tree note (read this first)

At the time of writing, this working tree is on `claude/roadmap-implementation-z5k8zw`, which is
**behind `origin/main`**. `origin/main` already contains a hosting layer this branch does not:

- `packages/benzene-core/benzene/core/worker.py` — `StopSignal`, `Worker`, `WorkerHost`,
  `background_worker`
- `packages/benzene-http/benzene/http/serving.py` — `asgi_server_worker`, `uvicorn_worker`
- `sqs_consumer_worker` (`benzene/aws/sqs_consumer.py`) and `kafka_consumer_worker`
  (`benzene/kafka/consumer.py`)

That work closes the *multi-leg supervision* half of the .NET `Benzene.SelfHost` /
`Benzene.HostedService` story. **Every gap below is stated relative to `origin/main`**, i.e. it is
what is still missing once that lands. Where a gap would be worse on this branch, it is called out.

Verified absent on `origin/main` as well as here: any `signal.SIGTERM` / `add_signal_handler` use;
any liveness/readiness topic; any shutdown latch; any concrete health check implementation; any
metrics instrument; any secrets/configuration abstraction.

## Frozen contracts — do not touch

- The aggregate wire shape `{"isHealthy": bool, "healthChecks": {name: {"isHealthy": bool, ...}}}`
  (`health.py:HealthReport.to_payload`, `mesh/feeds.py:Heartbeat`, and
  `conformance/mesh-collector-cases.json` lines 35/81/89).
- The reserved topic `benzene:healthcheck` (`health.py:HEALTH_TOPIC`) and the HTTP surface
  `/benzene/health` returning 200/503 (`http/app.py` ~line 202, profile R3).
- The `service-unavailable` + `successful=True` result override in `health_interception` — that is
  what keeps the report in the body instead of a problem document, and mirrors
  `HealthCheckProcessor.PerformHealthChecksAsync`. Keep it.

Conformance assertions are **JSON-subset** on bodies, so *adding* keys to a per-check payload
(`status`, `durationMs`, `data`) is contract-safe. Renaming or removing `isHealthy`/`healthChecks`
is not. Every recommendation below is additive.

---

# Gap 1 — A worker-only process does not drain on SIGTERM

**Severity: CRITICAL**

## What .NET has
- `Benzene.SelfHost/CLAUDE.md`, `Benzene.Abstractions.Pipelines/Hosting/IBenzeneWorker.cs` —
  `StartAsync(CancellationToken)` / `StopAsync(CancellationToken)`; `StopAsync` is a **bounded
  drain**, not an abort.
- `Benzene.SelfHost/BoundedConcurrentDispatcher.cs:166` — `DrainAsync(TimeSpan drainTimeout)`
  completes every lane's writer and awaits the in-flight handlers up to a deadline. The CLAUDE.md
  calls this out as "the mechanism that makes `StopAsync` on both workers actually graceful now,
  instead of abandoning in-flight work."
- `Benzene.HostedService/CLAUDE.md` — the generic host delivers the OS signal; `IHostedService.StopAsync`
  is invoked, which delegates to the worker's drain.

## What Python has / lacks
- `origin/main`'s `benzene/core/worker.py` supervises legs and wires a shared `StopSignal`, but its
  own module docstring states the boundary explicitly: *"It starts no threads and **installs no
  signal handlers**. `uvicorn.Server.serve()` installs its own SIGINT/SIGTERM handling."*
- So the **only** source of a shutdown signal in the whole port is uvicorn. A process whose legs are
  a Kafka consumer and an SQS consumer and nothing else — the canonical `Deployment`/`Job` worker —
  has no signal handler at all. Python's default SIGTERM disposition terminates the interpreter
  immediately: no `finally`, no `consumer.close()`, no offset commit, no `delete_message`.
- Consequences with the loops as written
  (`benzene/kafka/consumer.py:run_consumer_loop`, `benzene/aws/sqs_consumer.py:run_consumer_loop`,
  `benzene/rabbitmq/consumer.py:run_consumer_loop`): the in-flight message is killed mid-handler.
  Kafka never commits, so the record is re-served to the next pod — at-least-once holds, but a
  side-effecting handler is re-run. SQS never deletes, so the message reappears after the visibility
  timeout. RabbitMQ never acks. Every rolling deploy therefore duplicates work proportional to the
  in-flight set.
- `examples/k8s_orders/app.py` documents the workaround, and it only works because that example
  happens to have an HTTP leg.

## Implementation spec

**Package:** `benzene-core` (no new dependency — `signal` and `asyncio` are stdlib).
**Module:** extend `benzene/core/worker.py`.

```python
# benzene/core/worker.py

DEFAULT_SHUTDOWN_SIGNALS: tuple[signal.Signals, ...] = (signal.SIGTERM, signal.SIGINT)

class WorkerHost:
    def __init__(
        self,
        *,
        shutdown_timeout: float | None = 30.0,
        signals: Sequence[signal.Signals] | None = DEFAULT_SHUTDOWN_SIGNALS,
    ) -> None: ...
```

`run()` installs the handlers via `asyncio.get_running_loop().add_signal_handler(sig, self._stop.set)`
inside a `try` block, and **removes them in a `finally`**. Rules the implementation must honour:

1. `add_signal_handler` raises `NotImplementedError` on Windows and `RuntimeError`/`ValueError` off
   the main thread. Catch both, log at `INFO` ("no signal handler installed on this platform/thread;
   set `WorkerHost.stop` yourself"), and continue — never fail to start because of it.
2. **Do not fight uvicorn.** If a leg installs its own handlers (uvicorn does, on `serve()`), the
   last registration wins in asyncio's loop-level handler table, and uvicorn re-installs on entry
   and restores on exit. Setting `stop` from the host's handler is still correct, because the
   `asgi_server_worker` watcher already translates `stop` → `server.should_exit`. Document that both
   paths converge; a test must prove SIGTERM stops a host with *and* without an ASGI leg.
3. `signals=None` opts out entirely (an embedding host owns signals).
4. A **second** signal while draining should escalate: set a `_force` flag and cancel the tasks
   immediately, so an operator's second Ctrl-C is not ignored.

Then add a graceful-drain seam to the loops so the deadline is meaningful:

```python
# benzene/aws/sqs_consumer.py
async def run_consumer_loop(..., should_continue=..., drain_batch: bool = True) -> None: ...
```

The SQS loop currently pulls up to 10 messages and processes the whole batch without re-checking
`should_continue`. With `drain_batch=True` (default, and the safe choice) it keeps finishing the
batch it already received — those messages are invisible to other consumers anyway; abandoning them
just burns the visibility timeout. Add an explicit `break` out of the *outer* poll loop only. The
per-message dispatch must never be cancelled mid-handler; that is what `shutdown_timeout` is for.

**Tests** (`tests/test_worker_host.py`, new):
- `test_sigterm_stops_a_host_with_no_asgi_leg` — a fake worker that loops on
  `stop.should_continue()`; `os.kill(os.getpid(), signal.SIGTERM)` from a scheduled task; assert
  `run()` returns and the worker recorded a clean exit (not a cancellation).
- `test_the_in_flight_message_completes_before_the_loop_exits` — a fake SQS client returning one
  message whose handler sleeps; signal mid-handler; assert `delete_message` was still called.
- `test_a_second_signal_escalates_to_cancellation`.
- `test_signal_handlers_are_removed_after_run` — assert the loop's handler table is restored, so a
  second `WorkerHost` in the same process (tests!) is unaffected.
- `test_unsupported_platform_degrades_to_no_handler` — monkeypatch `add_signal_handler` to raise
  `NotImplementedError`; assert `run()` still works.

---

# Gap 2 — No liveness / readiness separation, and no shutdown-readiness latch

**Severity: CRITICAL** (this is the other half of Gap 1 — without it, Kubernetes keeps routing
traffic at a pod that has begun draining)

## What .NET has
- `Benzene.Abstractions/BenzeneTopic.cs` reserves **three** health topics, not one:
  `benzene:healthcheck` (deep, dependencies included), `benzene:liveness` ("is the process up? Never
  gated on dependencies"), `benzene:readiness` ("should this instance receive traffic?"), plus
  `contracts` (`Benzene.HealthChecks/Constants.cs`).
- The **harvest matrix** (`Benzene.HealthChecks/CLAUDE.md`): only `.UseHealthCheck` includes
  dependency-category checks. `.UseLivenessCheck`, `.UseReadinessCheck` and `.UseContractsCheck`
  exclude them. The reasoning is shared-fate: every replica probes the same downstream, so gating a
  k8s probe on it fails every replica at once — liveness restart-storms the fleet, readiness empties
  the Service. A degradation becomes an outage.
- `.UseLivenessCheck`/`.UseReadinessCheck` deliberately do **not** also answer the default healthcheck
  topic, so registering both in one pipeline can't have one shadow the other.
- `Benzene.HealthChecks/ShutdownState.cs` — a one-way latch, `MarkShuttingDown()` /
  `LinkTo(CancellationToken)`; `ShutdownReadinessHealthCheck.cs` returns `Failed` once tripped, with
  the docstring rule: **readiness only, never liveness** — "a draining instance is healthy, not
  broken, and failing liveness would trigger a restart mid-drain."

## What Python has / lacks
- One topic (`health.py:HEALTH_TOPIC`), one middleware (`health_interception`), one HTTP path
  (`http/standard.py:StandardPaths.health_path`, `http/app.py` ~line 204). There is no way to
  express "this check is safe for a probe" vs "this check calls a downstream".
- No shutdown latch anywhere. During the drain that Gap 1 adds, `/benzene/health` keeps returning
  200, so the k8s Service keeps sending new requests to a pod that is winding down.
- The example manifests show the consequence: `examples/k8s_orders/k8s/app.yaml:38` falls back to a
  bare `tcpSocket` readiness probe, and `examples/k8s_mesh/k8s/services.yaml:31` points readiness at
  the deep `/benzene/health`. Neither has a liveness probe at all.

## Implementation spec

**Package:** `benzene-core` (`benzene/core/health.py`) and `benzene-http`
(`benzene/http/standard.py`, `benzene/http/app.py`).

### 1. Reserved topics and probe scope

```python
# benzene/core/health.py
HEALTH_TOPIC = "benzene:healthcheck"      # unchanged, frozen
LIVENESS_TOPIC = "benzene:liveness"       # new
READINESS_TOPIC = "benzene:readiness"     # new

class Probe(str, Enum):
    LIVENESS = "liveness"
    READINESS = "readiness"
    DEEP = "deep"
```

`HealthChecks.add` gains a keyword, defaulting to the current behaviour:

```python
def add(
    self,
    name: str,
    check: HealthCheck,
    *,
    probes: Collection[Probe] = (Probe.LIVENESS, Probe.READINESS, Probe.DEEP),
    dependency: bool = False,
) -> HealthChecks: ...
```

`dependency=True` is the .NET `IDependencyHealthCheck` category expressed as a flag rather than a
type — Python has no DI service-type registry to key it off, and a flag reads better than a marker
Protocol. Setting it **forces** `probes={Probe.DEEP}` and `critical=False` (see Gap 5), and the
setter must reject an explicit `probes=` alongside `dependency=True` with a `ValueError` naming the
shared-fate reasoning. That is the one-way door from the .NET design, and it is the whole point:
make it hard to put a downstream check on a probe by accident.

```python
async def run(self, probe: Probe = Probe.DEEP) -> HealthReport: ...
```

`run()` keeps its current no-arg signature (defaulting to `DEEP`) so nothing existing breaks.

### 2. Middleware

```python
def health_interception(
    checks: HealthChecks,
    *,
    aliases: Iterable[str] = (),
    probe: Probe = Probe.DEEP,
    topics: Iterable[str] | None = None,
) -> Middleware: ...

def liveness_interception(checks, *, aliases=()) -> Middleware:   # LIVENESS_TOPIC only
def readiness_interception(checks, *, aliases=()) -> Middleware:  # READINESS_TOPIC only
```

Match .NET exactly on the shadowing rule: the liveness/readiness middlewares answer **only** their
own topic, never `benzene:healthcheck`. The response shape is identical for all three (frozen
aggregate) — only the *set of checks run* differs.

### 3. Shutdown latch

```python
# benzene/core/health.py
class ShutdownState:
    """One-way 'we are draining' latch. Read by shutdown_readiness_check; never reverts."""
    def mark_shutting_down(self) -> None: ...
    @property
    def is_shutting_down(self) -> bool: ...
    def link_to(self, stop: StopSignal) -> ShutdownState: ...   # spawns a task awaiting stop.wait()

def shutdown_readiness_check(state: ShutdownState) -> HealthCheck:
    """Unhealthy once draining. READINESS ONLY — a draining pod is not broken."""
```

`link_to` takes the `StopSignal` from `benzene.core.worker`, which is the exact analogue of .NET's
`LinkTo(IHostApplicationLifetime.ApplicationStopping)`. `WorkerHost` should expose a convenience:
`host.shutdown_state` (lazily created, pre-linked), so a startup writes
`checks.add("shutdown", shutdown_readiness_check(host.shutdown_state), probes={Probe.READINESS})`.

### 4. HTTP surfaces

```python
@dataclass
class StandardPaths:
    prefix: str = DEFAULT_PREFIX
    invoke: bool = True
    health: HealthChecks | None = None
    spec: SpecSource | None = None
    probes: bool = True            # new: serve /benzene/live and /benzene/ready off `health`

    @property
    def liveness_path(self) -> str: return f"{self.prefix}/live"
    @property
    def readiness_path(self) -> str: return f"{self.prefix}/ready"
```

`http/app.py` serves them next to the existing `/benzene/health` branch, same 200/503 mapping,
running `std.health.run(Probe.LIVENESS)` / `run(Probe.READINESS)` respectively. `/benzene/health`
keeps running `Probe.DEEP` — unchanged, so R3 and the live probe in `mesh/probe.py` are untouched.

**Note for the profile:** `docs/cloud-service-profile.md` and `mesh/profile.py:170` describe R3 in
terms of the single health surface. Adding `/benzene/live` and `/benzene/ready` is additive and must
not change R3's verdict logic in `mesh/probe.py` — a service that serves only `/benzene/health` is
still R3-conformant. Do not add a new requirement id without a spec change.

**Tests** (extend `tests/test_health.py`, and `tests/test_wellknown.py` for the HTTP paths):
- `test_a_dependency_check_never_reaches_liveness_or_readiness`
- `test_liveness_does_not_answer_the_healthcheck_topic` (and the converse)
- `test_registering_liveness_and_readiness_in_one_pipeline_does_not_shadow`
- `test_shutdown_latch_flips_readiness_but_not_liveness`
- `test_shutdown_state_links_to_a_stop_signal`
- `test_probe_paths_map_200_and_503`
- A regression test asserting the deep aggregate payload is byte-identical to today's for a service
  that registers no probe scopes — the frozen-shape guard.

---

# Gap 3 — The port ships zero real health checks

**Severity: HIGH**

## What .NET has
Eight check packages. The ones with genuine Python meaning:
- `Benzene.HealthChecks.Http/HttpPingHealthCheck` — GET a URL, healthy only on exact 200; strips
  basic-auth userinfo from the reported URL; reports status code in `Data`.
- `Benzene.HealthChecks.Tcp/TcpHealthCheck.cs` — `TcpClient.ConnectAsync(host, port)`; the
  lowest-common-denominator reachability check for anything without a first-class client.
- `Benzene.HealthChecks.Disk/DiskHealthCheck.cs` — free space on the drive containing a path, with a
  hard `minimumFreeBytes` and a soft `warningFreeBytes`.
- `Benzene.HealthChecks/MemoryHealthCheck.cs` — process working set against a ceiling, with a soft
  warning tier; injectable measurement seam for tests.
All four report the **exception type name, never the message** — a health report flows out with no
authorization, and provider exception messages carry connection strings.

## What Python has / lacks
`health.py` is a registry and nothing else. Every single call site in this repo registers
`lambda: True`:
`deploy/mesh/collector/host.py:88`, `deploy/mesh/fleet/service.py:139`,
`examples/aws_lambda_mesh/service/domain.py:145`, `examples/k8s_mesh/service/startup.py:70`,
`examples/mesh_dashboard/profile.py:46`, `examples/k8s_mesh/mesh/host.py:73`.
A `/benzene/health` that returns 200 unconditionally is worse than none: it makes a readiness probe
that cannot fail, so Kubernetes will happily route to a pod whose database is gone.

## Implementation spec

**Package:** `benzene-core`, new module `benzene/core/checks.py`, re-exported from
`benzene.core`. All four are stdlib-only — **no optional extra**, which is why they belong in core
rather than four satellite distributions (the .NET split exists because `HttpClient` registration
and `DriveInfo` sit in different assemblies; Python has no such pressure).

```python
# benzene/core/checks.py

def tcp_check(host: str, port: int, *, timeout: float = 5.0) -> HealthCheck:
    """L4 reachability via asyncio.open_connection, closed immediately. Unhealthy on any
    OSError/TimeoutError; detail is the exception *type name*, never its message."""

def http_check(
    url: str,
    *,
    timeout: float = 5.0,
    expect_status: int | Collection[int] = 200,
    fetch: Callable[[str, float], Awaitable[int]] | None = None,
) -> HealthCheck:
    """GET `url`; healthy only on `expect_status`. `fetch` is the injectable seam (a duck-typed
    fake in tests, `httpx`/`aiohttp` in production); the default runs urllib.request through
    asyncio.to_thread. The reported URL has userinfo stripped (https://u:p@h -> https://h)."""

def disk_check(
    path: str = "/",
    *,
    minimum_free_bytes: int,
    warning_free_bytes: int | None = None,
    usage: Callable[[str], tuple[int, int]] | None = None,
) -> HealthCheck:
    """shutil.disk_usage(path) -> (free, total). Unhealthy below the minimum, degraded (Gap 5)
    below the warning. `usage` is the injectable seam so a test asserts the boundary without
    depending on the runner's real disk."""

def memory_check(
    *,
    maximum_bytes: int,
    warning_bytes: int | None = None,
    measure: Callable[[], int] | None = None,
) -> HealthCheck:
    """Resident set size against a ceiling. Default `measure` reads
    /sys/fs/cgroup/memory.current (cgroup v2), falling back to
    resource.getrusage(RUSAGE_SELF).ru_maxrss * 1024, falling back to psutil if importable.
    On a container this is what the OOM-killer watches — the point of the check."""
```

Python idiom over .NET shape, deliberately:
- **Closures returning `HealthCheck`, not classes.** The existing `HealthCheck` type alias is
  `Callable[[], HealthCheckResult | bool | Awaitable[...]]`; a factory function fits it exactly and
  needs no base class, no `Type` property, and no registration extension method.
- **Injectable seams (`fetch`, `usage`, `measure`) instead of DI resolution.** This is the port's
  standing pattern (`mesh/probe.py:HttpCall`, `otel/exporter.py:OtelTracer`) and it removes the
  .NET requirement that "the consumer must register an `HttpClient` or the check throws on first run."
- **Do not port `ICancellationTokenAccessor`.** Python's `asyncio.timeout`/`wait_for` is the
  ambient-cancellation mechanism; each check takes its own `timeout` float.

**Secret-safety rule to enforce in review and in a test:** the `detail` string must be
`f"{type(exc).__name__}"` plus safe structured facts (host, port, status code, byte counts), never
`str(exc)`. `HealthChecks.run()` currently violates this — `health.py:106` formats
`f"raised {type(exc).__name__}: {exc}"`, which puts an arbitrary exception message into a payload
that leaves the process unauthenticated. **Change that line to the type name only** and put the full
exception on the module logger with `exc_info=True`. This is a small, high-value fix.

**Tests** (`tests/test_health_checks.py`, new):
- tcp: healthy against an `asyncio.start_server` on port 0; unhealthy against a closed port;
  unhealthy (not raising) on timeout; detail carries no exception message.
- http: 200 healthy / 204 and 500 unhealthy via an injected `fetch`; `expect_status={200, 204}`;
  `https://user:pass@host/x` reports `https://host/x` — assert the password does not appear anywhere
  in the payload.
- disk/memory: injected measurement, all three boundary cases (below min, between, above), plus the
  degraded tier once Gap 5 lands.
- One integration test wiring all four into `HealthChecks` and asserting the aggregate payload keeps
  the frozen shape.

---

# Gap 4 — `HealthChecks.run()` is sequential, unbounded and unisolated

**Severity: HIGH**

## What .NET has
`Benzene.HealthChecks/HealthCheckProcessor.cs`:
- Every check runs **concurrently** — one `Task.WhenAll` — so N slow dependencies cost `max`, not `sum`.
- Every check is wrapped `TimeOutHealthCheck(ExceptionHandlingHealthCheck(check))` with a
  configurable per-check timeout (default 10s), overridable per check via `IHealthCheck.Timeout`.
- Every check is **timed**, and the duration is stamped onto the result.
- `CachingHealthCheckProcessor` caches the aggregate for a TTL, keyed by the check set, so a probe
  polling every 5 seconds does not re-hit every dependency.

## What Python has / lacks
`health.py:96-108`: a `for` loop, `await`ing each check in turn, with no timeout of any kind. A
single check that hangs — exactly what a TCP check against a black-holed dependency does — hangs
`/benzene/health` forever. Kubernetes' probe `timeoutSeconds` then fires, the probe fails, and the
pod is restarted or de-routed **because the health endpoint hung**, not because anything was wrong.
That is a self-inflicted outage, and it is the single most likely way the current code hurts a
production service once Gap 3's real checks exist.

## Implementation spec

**Package:** `benzene-core`, rewrite `HealthChecks.run` in `benzene/core/health.py`.

```python
class HealthChecks:
    def __init__(self, *, timeout: float | None = 10.0) -> None: ...

    def add(self, name, check, *, timeout: float | None = _UNSET, ...) -> HealthChecks:
        """`timeout` overrides the registry-wide default for this check; None disables it."""

    async def run(self, probe: Probe = Probe.DEEP) -> HealthReport: ...
```

`run` becomes:

```python
results = await asyncio.gather(*(self._run_one(name, check) for name, check in selected))
```

`_run_one` wraps each call in `asyncio.wait_for(..., timeout)` (use `asyncio.timeout` where the
floor is 3.11; `wait_for` keeps 3.10 support, which `requires-python = ">=3.10"` mandates), catches
`TimeoutError` into `HealthCheckResult.unhealthy("timed out after 10.0s")`, catches every other
`Exception` into `unhealthy(type(exc).__name__)`, and measures `time.perf_counter()` around it.

`HealthCheckResult` gains an optional `duration_ms: float | None = None`, emitted as `durationMs` in
`to_payload()` **only when set** — additive, and the conformance subset assertions tolerate it.

Ordering: `asyncio.gather` preserves argument order, and `run` must build the result dict in
registration order so the payload is stable across runs (a changing key order in a health payload
makes diffing reports miserable).

**Caching:** add `HealthChecks.cached(ttl: float, *, clock: Callable[[], float] = time.monotonic)`
returning a wrapper with the same `run` signature, memoising per `Probe`. Injectable clock, matching
the port's convention. Document the .NET warning verbatim: **do not cache a liveness probe** — it
must reflect the instant state.

**Tests** (extend `tests/test_health.py`):
- `test_checks_run_concurrently` — three checks each sleeping 0.1s; assert wall time < 0.2s.
- `test_a_hanging_check_times_out_and_is_unhealthy_not_a_hang` — the important one.
- `test_a_per_check_timeout_overrides_the_registry_default`
- `test_durations_are_stamped_and_the_payload_stays_a_superset_of_the_frozen_shape`
- `test_the_cached_wrapper_reruns_after_the_ttl_with_an_injected_clock`
- `test_result_order_follows_registration_order`

---

# Gap 5 — No degraded ("warning") tier, and no non-critical checks

**Severity: MEDIUM**

## What .NET has
- `Benzene.HealthChecks.Core/HealthCheckStatus` — three statuses: `"ok"`, `"warning"`, `"failed"`.
  A `Warning` is a real, distinct outcome that **does not flip aggregate `IsHealthy`**.
- `IHealthCheck.IsNonCritical` (note the polarity — default `false` = critical, chosen so a mock
  returning `default(bool)` fails *safe*): a failing non-critical check is downgraded to `Warning`.
- The `IsPersistent` carve-out (`HealthCheckError.Classify`): an authorization/permission denial is
  a deterministic misconfiguration that will not self-heal, so it escapes the downgrade and stays
  `Failed`.
- `DiskHealthCheck`/`MemoryHealthCheck` are the canonical two-threshold users: surface low disk
  before it is fatal, without failing the aggregate.

## What Python has / lacks
`HealthCheckResult` is a bool plus a detail string. There is no way to say "Redis is down, we're
serving from origin, don't take me out of the Service."

## Implementation spec

Additive, and wire-safe because `isHealthy` keeps its meaning:

```python
@dataclass(frozen=True)
class HealthCheckResult:
    is_healthy: bool
    detail: str | None = None
    status: str = "ok"          # "ok" | "warning" | "failed" — matches .NET's HealthCheckStatus
    duration_ms: float | None = None   # from Gap 4

    @classmethod
    def degraded(cls, detail: str | None = None) -> HealthCheckResult:
        return cls(True, detail, status="warning")     # is_healthy stays True — that is the point
```

`to_payload()` emits `status` **only when it is not `"ok"`**, so today's payloads are byte-identical
and the conformance fixtures are untouched. `HealthReport.is_healthy` stays `all(r.is_healthy)`,
which now naturally means "no `failed`" — the same rule as `HealthCheckProcessor`.

`HealthChecks.add(..., critical: bool = True)`: when `critical=False`, a failing check's result is
rewritten to `degraded(...)` during aggregation.

**Do not port `IsPersistent` yet.** Its value in .NET comes entirely from `HealthCheckError.Classify`
knowing about `AmazonServiceException` / `RequestFailedException` status codes, and this port has no
cloud-SDK error classification layer. Reintroduce it *with* the cloud checks, if ever. Note it here
so the next agent does not silently drop the concept: a permanently-broken IAM grant sitting yellow
forever is the failure mode it prevents.

**Tests:** a degraded check keeps the aggregate healthy but appears with `"status": "warning"`;
`critical=False` downgrades a failure; a payload with no degraded check is exactly today's payload.

---

# Gap 6 — No downstream-service health check, and no contract-drift check

**Severity: MEDIUM**

## What .NET has
- `Benzene.Clients.HealthChecks/ServiceHealthCheckClient` — sends `benzene:healthcheck` with a `Void`
  body over `IBenzeneMessageSender` and expects a `HealthCheckResponse` back. The payload is fixed by
  the framework, which is exactly why this is *not* generated client code.
- `ClientHealthCheck` folds that into one result: reachable + matching contract → `Ok`, reachable +
  drift → `Warning`, unreachable → `Failed`.
- `Benzene.HealthChecks.Schema/SchemaHealthCheck` is the provider half: publishes
  `ContractHash.Compute(all handlers)` as a `"schema"`-typed check;
  `ClientHealthCheckProcessor` compares it to the hash baked into the consumer's generated client.
- Wired to the dedicated `contracts` topic — **never a probe**, for the shared-fate reason in Gap 2.

## What Python has / lacks
- `benzene/core/clients.py:MessageSender` is the right seam and already exists.
- `benzene/codegen_client/contract_hash.py:compute(document, *, topic_scoped=False)` is the same
  spec-pinned hash, already implemented and conformance-tested
  (`conformance/contract-hash-cases.json`). Both halves exist; nothing joins them.
- `examples/aws_lambda_mesh/mesh/discovery_service.py:82` hand-rolls the downstream health call
  (`sender.send_message(HEALTH_TOPIC, None)`) — evidence the capability is wanted.

## Implementation spec

**Package:** `benzene-core`, in `benzene/core/checks.py` (alongside Gap 3's checks; it depends only
on the `MessageSender` Protocol already in core).

```python
def service_check(
    sender: MessageSender,
    service: str,
    *,
    expected_contract_hash: str | None = None,
    timeout: float = 5.0,
) -> HealthCheck:
    """Call a downstream's `benzene:healthcheck` and fold the answer into one result.

    Unreachable / non-success status -> unhealthy.
    Reachable, no expected hash -> healthy (reachability only; never a fabricated drift verdict).
    Reachable + hash matches -> healthy.
    Reachable + hash differs -> degraded (Gap 5) with detail naming both hashes.
    """
```

Register it with `dependency=True` (Gap 2), which forces it onto the deep probe only. That is the
Python expression of .NET's separate `contracts` topic — a fourth reserved topic would need a
cross-language spec change and buys nothing here, since the deep layer already triggers no automated
Kubernetes action. **Record that as a deliberate divergence** in `docs/reference/core.md`.

Provider half:

```python
def schema_check(spec: SpecSource, *, service: str) -> HealthCheck:
    """Publish this service's contract hash so consumers can detect drift.
    detail = the hash; payload carries {"hashCode": ...} under the check's data."""
```

This needs `HealthCheckResult` to carry structured data. Add `data: Mapping[str, Any] | None = None`,
emitted in `to_payload()` only when set (additive, subset-safe) — the .NET `Data` dictionary, and
the mechanism the Mesh UI's root-cause block reads.

**Note the dependency direction:** `contract_hash.compute` lives in `benzene-codegen-client`, which
depends on `benzene-core`. `schema_check` must therefore either live in `benzene-codegen-client`
(cleanest) or take the hash as a plain string computed by the caller (loosest). **Prefer the latter
in core** (`schema_check(hash_code: str)`) with a one-line helper in `benzene-codegen-client` that
computes it from a `ServiceSpec` — no new dependency edge, and it keeps `benzene-core` free of the
`rfc8785` requirement.

**Tests** (`tests/test_health_checks.py`): a fake `MessageSender` returning ok/failure/raising;
matching and drifting hashes; the no-expected-hash pass-through (assert it does *not* report drift
against an empty string — the .NET bug this design avoids).

---

# Gap 7 — No metrics surface (RED signals)

**Severity: MEDIUM-HIGH**

## What .NET has
`Benzene.Diagnostics/MetricsExtensions.cs` — `UseBenzeneMetrics<TContext>()`, explicit opt-in,
recording **once per message**, on the shared `"Benzene"` `Meter`:
- `benzene.messages.processed` — `Counter<long>`
- `benzene.message.duration` — `Histogram<double>` (ms)
tagged `topic` / `transport` / `result`, where `result` **collapses success and itemizes failure**:
`"success"` for any successful outcome, the failure's status string verbatim
(`not-found`/`unauthorized`/…), `"exception"` if the pipeline threw. Rationale in the CLAUDE.md:
nobody wants `ok`-vs-`created`, but a failure mix that is mostly `not-found` reads very differently
from mostly `unauthorized`. The CLAUDE.md is explicit that these instrument names and tag keys are a
**published contract** — `docs/mesh-usage-feed.md` — not internals, because mesh backend adapters
read them back out of whatever backend they were exported to.

## What Python has / lacks
Nothing. `benzene-otel` exports **traces** only (`otel/exporter.py:OtelTraceExporter` over
`mesh/trace.py:TraceEvent`). `mesh/artifacts.py:18` documents the consequence: "latency/rate metrics
… stays `null` rather than being invented", and line 137 notes the mesh edge "rate/latency need a
metrics source". The mesh UI is structurally unable to show throughput or latency for a Python
service, and any operator wanting a RED dashboard writes it themselves.

Note that `mesh/trace.py:trace_middleware` **already measures** `duration_ms` per invocation
(line ~208) and already knows the topic and status. The metrics middleware is a small amount of new
code over a signal that is already computed.

## Implementation spec

**Package:** `benzene-otel`, new module `benzene/otel/metrics.py`. Optional extra `[otel]` (already
declared). The OTel metrics API must be imported lazily and the meter must be **injectable**, exactly
as `OtelTracer` is, so the whole module is testable with no `opentelemetry` installed.

```python
# benzene/otel/metrics.py

MESSAGES_PROCESSED = "benzene.messages.processed"   # published contract — do not rename
MESSAGE_DURATION   = "benzene.message.duration"     # published contract — do not rename

@runtime_checkable
class Counter(Protocol):
    def add(self, amount: int, attributes: Mapping[str, str] | None = None) -> None: ...

@runtime_checkable
class Histogram(Protocol):
    def record(self, amount: float, attributes: Mapping[str, str] | None = None) -> None: ...

@dataclass
class BenzeneInstruments:
    processed: Counter
    duration: Histogram

    @classmethod
    def from_meter(cls, meter: Any) -> BenzeneInstruments: ...
    @classmethod
    def default(cls) -> BenzeneInstruments:
        """Lazily builds them from opentelemetry.metrics.get_meter('benzene')."""

class RecordingInstruments:
    """In-memory fake: `.processed_calls` / `.duration_calls` lists. The metrics analogue of
    RecordingSink."""

def metrics_interception(
    instruments: BenzeneInstruments | None = None,
    *,
    transport: str = "<missing>",
) -> Middleware:
    """Record one counter increment and one duration sample per invocation, after next()."""
```

The `result` tag must reproduce .NET's rule exactly, because it is a cross-port contract:
`"success"` when `result.is_successful`; otherwise `result.status` verbatim; `"exception"` if
`next()` raised (re-raise after recording); `"<missing>"` when no result was produced. Note the
subtlety the .NET doc flags: a health check's `service-unavailable` result is constructed
`successful=True` (`health.py:137`), so it tags `success` — that is correct and intentional, and a
test should pin it so nobody "fixes" it later.

`transport` is a plain string the host binding passes (`"http"`, `"sqs"`, `"kafka"`, `"rabbitmq"`,
`"lambda"`), because Python has no `ICurrentTransport` scoped service.

**Tests** (`tests/test_otel_metrics.py`, new): one increment + one sample per invocation, never
per-middleware; the four `result` tag cases; the health-check `success` case; an exception is
recorded and re-raised; everything runs with `RecordingInstruments` and no `opentelemetry` import.

---

# Gap 8 — No configuration / secrets seam, and no fail-fast startup validation

**Severity: MEDIUM**

## What .NET has
`Benzene.Configuration.Core` (BCL-only, deliberately no cloud SDK):
- `ISecretStore` — one method, `GetSecretAsync(name, ct) -> string?`, `null` when this store does not
  have the name so a composite can fall through.
- `InMemorySecretStore`, `EnvironmentVariableSecretStore(prefix)` (maps `Db:Password` → `DB_PASSWORD`),
  `FileSecretStore(directory)` (the `/run/secrets/<name>` Docker/K8s mount convention, trailing
  newline trimmed), `CompositeSecretStore` (ordered, first non-null wins),
  `CachingSecretStore(inner, ttl, now)` with `Invalidate`/`InvalidateAll` for rotation.
- `SecretResolver` — `RequireAsync` (throws `MissingSecretException`), `GetAsync(name, default)`,
  `RequireIntAsync`/`RequireBoolAsync`/`RequireUriAsync`.
- `SecretValidation.EnsureRequiredAsync(store, names...)` — throws listing **all** missing names at
  once, so a misconfigured deploy fails before serving traffic rather than one redeploy at a time.

## What Python has / lacks
Nothing. `benzene/core/startup.py:build_application` takes `config: Mapping[str, str] | None` and
every host fills it from bare `os.environ` reads scattered through the composition root
(`examples/k8s_orders/app.py`, `examples/k8s_mesh/service/startup.py`). A missing variable surfaces
as a `KeyError` at whatever point of startup happens to read it, or — worse — as a `None` that only
fails when the first message arrives.

## Judgement
**Port the mechanism, not the framework.** Python already has excellent config libraries
(`pydantic-settings`), and this port must not grow a config-binding system. But three specific
things are genuinely missing and are cheap:
1. The **file store** — `/run/secrets` is how Kubernetes and Docker Swarm mount secrets, and reading
   it correctly (trailing-newline trim, other whitespace preserved) is a footgun people get wrong.
2. The **composite + caching + invalidate** trio — the rotation story.
3. **Fail-fast validation listing every missing name at once** — the highest value item in the whole
   package, and the one no library gives you by default.

Do **not** port `SecretResolver`'s typed accessors as a new API; point users at
`pydantic-settings`/`os.environ` + `int()` and their own typed options object, which is the
idiomatic Python answer. One exception: keep `require`, because it is the thing that raises the good
error message.

## Implementation spec

**Package:** `benzene-core`, new module `benzene/core/config.py`. Stdlib-only, no extra.

```python
@runtime_checkable
class SecretStore(Protocol):
    async def get(self, name: str) -> str | None: ...

class MappingSecretStore:       # dict-backed; tests, defaults, a composite's bottom layer
    def __init__(self, values: Mapping[str, str]) -> None: ...

class EnvironmentSecretStore:
    """`Db:Password` -> `DB_PASSWORD`: upper-case, and `: . -` and spaces -> `_`, plus an
    optional prefix. Exposes the mapping as a module function `env_key(name, prefix="")` so a
    caller can report which variable it actually looked for."""
    def __init__(self, *, prefix: str = "", environ: Mapping[str, str] | None = None) -> None: ...

class FileSecretStore:
    """One file per secret under `directory` — the /run/secrets convention. Strips exactly one
    trailing newline; preserves all other whitespace. A missing file is None, not an error."""

class CompositeSecretStore:
    """Ordered; first non-None wins, so env overrides a mounted file overrides a default."""

class CachingSecretStore:
    """Caches hits *and* misses for `ttl`; `invalidate(name)` / `invalidate_all()` force a
    re-fetch after rotation. `clock` is injectable (default time.monotonic)."""

class MissingSecretsError(Exception):
    missing: tuple[str, ...]

async def require(store: SecretStore, name: str) -> str: ...
async def require_all(store: SecretStore, *names: str) -> dict[str, str]:
    """Fetch every name concurrently; raise MissingSecretsError listing ALL missing ones at once."""
```

Wire it to the existing seam by documenting the pattern rather than adding API: resolve at startup,
`build_application(MyStartUp, config=await require_all(store, "DB_URL", "QUEUE_URL", ...))`. The
`config: Mapping[str, str]` parameter already there is the injection point, so nothing in
`startup.py` changes.

Cloud adapters (Secrets Manager, Key Vault, SSM) are **one `async def get` each** and belong in the
cookbook, exactly as .NET decided — do not take a boto3/azure dependency in `benzene-core`. Add
`docs/cookbooks/secrets-configuration.md` with copy-paste versions.

**Tests** (`tests/test_config.py`, new): env-key mapping table; file read + newline trim + missing;
composite precedence; caching TTL and both invalidate paths with an injected clock; `require_all`
raising with the **complete** list, not just the first.

---

# Gap 9 — `benzene-otel`'s "response events" is not .NET's response-as-event

**Severity: MEDIUM** (capability gap, and a naming collision worth resolving deliberately)

## What .NET has
`Benzene.ResponseEvents` republishes a request/response handler's **response payload as a new event
on a fire-and-forget transport**: a handler on SQS `order:create` returns a payload the transport
cannot deliver, so a per-pipeline mapping publishes it as `order:created` instead. It is a messaging
pattern, not an observability one:
- `ResponseEventsBuilder` — `Map(source, event, when?)`, `Map<TPayload>(..., project?)`,
  `MapCrudConvention()` (`X:create` + `Created` → `X:created`), `OnPublishFailure(mode)`.
- `IResponseEventPublisher`, default `BenzeneMessageSenderResponseEventPublisher` — publishes through
  `IBenzeneMessageSender`, so the outbound pipeline's correlation/W3C-trace/retry middleware applies.
  "Publishing rides the outbound pipeline, never a bespoke send path."
- `PublishFailureMode.FailMessage` (default) replaces the response with `UnexpectedError` so a queue
  transport nacks and redelivers; `LogAndContinue` keeps the handler's response.
- `IResponseEventCatalog` + `AddResponseEventDeclarations` — the published-event contracts appear in
  generated AsyncAPI/event-service specs.
- An advisory `FindUnmappedResponseHandlers()` diagnostic listing response-returning handlers no
  mapping covers.

## What Python has
`packages/benzene-otel/benzene/otel/response_events.py` — `response_event_interception(sink, ...)`
emits a `ResponseEvent` (topic, status, correlation id, payload) to a `ResponseEventSink` after each
invocation, **lossy by contract** (a raising sink is swallowed). Its own docstring claims it
"mirrors .NET's `Benzene.ResponseEvents`". It does not: it is an audit/outcome stream — closer to
.NET's metrics middleware than to response-as-event. Nothing is republished on a topic, nothing is
mapped, nothing appears in a spec, and a publish failure cannot nack the message.

Both are legitimate capabilities. The Python one is genuinely useful and should stay.

## Implementation spec

Two actions, in this order:

**(a) Fix the claim (do this even if (b) never happens).** Amend the module and package docstrings
(`otel/response_events.py:16`, `otel/__init__.py`) to say it is the *outcome-audit* counterpart, and
that .NET's response-as-event republishing is a distinct, unported capability. A false "mirrors X"
claim in a port is worse than an admitted gap — it stops anyone from noticing.

**(b) Port the real thing** as `benzene-core`'s `benzene/core/response_events.py` (it needs
`MessageSender` and `Registry`, both in core; it must not live in `benzene-otel`, which is an
observability distribution).

```python
@dataclass(frozen=True)
class EventMapping:
    source_topic: str
    event_topic: str
    when: Callable[[Result], bool] | None = None      # default: successful and payload is not None
    project: Callable[[Any], Any] | None = None       # None from the projector -> skip publishing

class PublishFailure(str, Enum):
    FAIL_MESSAGE = "fail-message"     # default: replace the result with unexpected-error -> nack
    LOG_AND_CONTINUE = "log-and-continue"

def crud_convention(*, suffixes: Mapping[str, str] = CRUD_SUFFIXES) -> EventMappingRule:
    """`x:create` + a successful `created` result -> `x:created`; update/delete likewise."""

def response_event_publication(
    sender: MessageSender,
    mappings: Sequence[EventMapping | EventMappingRule],
    *,
    on_failure: PublishFailure = PublishFailure.FAIL_MESSAGE,
) -> Middleware:
    """After next(), publish every matching mapping's event through `sender`. Fan-out is allowed."""
```

Keep .NET's two load-bearing decisions: publishing goes through the ordinary `MessageSender` (so
`mesh.with_trace_propagation` and the resilience middleware apply), and the default failure mode
fails the message so a queue transport redelivers — at-least-once, handlers must be idempotent.

The catalog/spec-generation half (`IResponseEventCatalog`, `AddResponseEventDeclarations`) is worth
a follow-up once `benzene-openapi`/`ServiceSpec` grows an "events produced" section; do not block
(b) on it, but leave `EventMapping` as a frozen dataclass with `source_topic`/`event_topic` public so
a catalog can be derived later without an API change.

**Tests** (`tests/test_response_events.py`, new): explicit map publishes with the projected payload;
a `when` that declines publishes nothing; CRUD convention across create/update/delete; fan-out to two
events; a raising sender under `FAIL_MESSAGE` rewrites the result to `unexpected-error`; under
`LOG_AND_CONTINUE` the handler's result survives; a failed handler result publishes nothing.

---

# Gap 10 — No wiring diagnostics

**Severity: LOW-MEDIUM**

## What .NET has
Three advisory, opt-in, never-throwing checks, all called once after wiring:
- `Benzene.Diagnostics/PipelineOrderingDiagnosticsExtensions` — `FindPipelineOrderingIssues(builder)`
  warns when `UseW3CTraceContext()` is present but not at index 0.
- `Benzene.ResponseEvents/ResponseEventDiagnosticsExtensions` — `FindUnmappedResponseHandlers()`.
- `Benzene.Clients`' `ValidateOutboundRouting`.

## What Python has / lacks
`mesh/profile.py` is a wiring-time self-check for the Cloud Service Profile requirements, which is
the same *idea* and proves the pattern is welcome here. There is no pipeline-ordering equivalent.

## Judgement and spec
Port the one rule that is machine-checkable and matters: **middleware order**. In this port the
ordering footguns are concrete and enumerable, so the check can be genuinely useful rather than a
lint gesture:
- `health_interception` / `spec_interception` / `mesh_interception` must precede the router (they
  short-circuit; behind the router they never fire).
- `trace_middleware` should be first, so every later stage is inside its span.
- `response_event_publication` (Gap 9) must be *after* whatever produces the result.

```python
# benzene/core/pipeline.py (or a new benzene/core/diagnostics.py)

@dataclass(frozen=True)
class OrderingIssue:
    middleware: str
    detail: str

def find_ordering_issues(middleware: Sequence[Middleware]) -> list[OrderingIssue]: ...
def log_ordering_issues(middleware, logger=None) -> None: ...
```

Identification is the hard part — Python middlewares are closures. Have the interception factories
stamp `middleware.__benzene_name__ = "health"` on the returned function (a one-line addition in each
factory, invisible to callers), and have `find_ordering_issues` read it with `getattr(..., None)` and
**skip anything unnamed**. Advisory, never raises, mirroring .NET.

**Tests:** a correctly-ordered pipeline yields no issues; health-after-router is reported; unnamed
middleware is silently skipped; the function never raises on an arbitrary callable.

---

# Gap 11 — Consumer loops process strictly one message at a time

**Severity: MEDIUM**

## What .NET has
`Benzene.SelfHost/BoundedConcurrentDispatcher.cs` — the shared primitive both `BenzeneKafkaWorker`
and `RabbitMqWorker` use: `laneCount` independent single-consumer `Channel<T>` lanes, each bounded
at capacity 1 so `EnqueueAsync` gives the poll loop **real backpressure**; an optional `keySelector`
routes same-key items to the same lane so per-key (per-partition) order is preserved while different
keys run concurrently; faults are logged per item and isolated to their lane;
`DrainAsync(timeout)`/`DrainLanesAsync(keys, timeout)` for shutdown and rebalance quiesce. It is
unit-tested in isolation because the worker itself needs a live broker.

## What Python has / lacks
All three loops (`benzene/kafka/consumer.py`, `benzene/aws/sqs_consumer.py`,
`benzene/rabbitmq/consumer.py`) `await app.handle_message(...)` inline. Throughput is
`1 / mean_handler_latency`, so an I/O-bound handler at 50ms caps a pod at ~20 msg/s regardless of
how much headroom the event loop has. Since every handler is already `async`, this is leaving most
of the port's natural concurrency on the floor. SQS receives batches of 10 and then processes them
strictly serially.

## Judgement
Worth porting, but **not as a channels-and-lanes engine** — that shape exists in .NET because
`Task`s are not cheap to coordinate and `SemaphoreSlim` + `ContinueWith` was the pattern it replaced.
Python's asyncio gives the same guarantees with far less machinery.

## Implementation spec

**Package:** `benzene-core`, `benzene/core/worker.py` (next to `WorkerHost`).

```python
async def dispatch_bounded(
    items: Iterable[T],
    handle: Callable[[T], Awaitable[R]],
    *,
    limit: int = 1,
    key: Callable[[T], Any] | None = None,
) -> list[R | BaseException]:
    """Run `handle` over `items` with at most `limit` concurrently.

    With `key`, items sharing a key are serialized relative to one another (a per-key
    asyncio.Lock) so per-partition / per-entity order is preserved while different keys
    overlap — the asyncio equivalent of .NET's lane routing.

    Returns one entry per item, in input order, exceptions included (never raised) — the caller
    (the ack/commit logic) decides what a failure means.
    """
```

Then thread it into the loops as an **opt-in** parameter defaulting to today's behaviour:
- `run_consumer_loop(..., concurrency: int = 1)` on all three.
- SQS: `dispatch_bounded(messages, handle, limit=concurrency)` over the received batch, then delete
  the successes. Safe — SQS has no ordering guarantee on a standard queue. On a **FIFO** queue pass
  `key=lambda m: m["Attributes"]["MessageGroupId"]`; document that.
- Kafka: `concurrency > 1` is **only** safe with `key=partition`, and even then the existing
  seek-back/blocked-offset logic must be re-derived over a batch. Recommend gating it: raise a
  `ValueError` if `concurrency > 1` and `commit=True` until that interaction is worked through, and
  say so in the docstring. Getting this wrong silently commits past a failure — worse than being slow.
- RabbitMQ: `basic_get` is one-at-a-time by construction; concurrency here needs `basic_consume`
  instead, which is a separate piece of work. Leave it at 1 and note why.

**Tests** (`tests/test_worker_host.py`): the limit is respected (a counter of in-flight handlers
never exceeds it); same-key items never overlap; different-key items do; results come back in input
order; a raising handler yields an exception entry and does not stop the rest; per-transport tests
that `concurrency=1` is byte-identical to today's behaviour.

---

# Gap 12 — RabbitMQ has no worker factory

**Severity: LOW**

`origin/main` adds `sqs_consumer_worker` and `kafka_consumer_worker` but no
`rabbitmq_consumer_worker`, so a process combining HTTP + RabbitMQ has to hand-roll the closure the
other two ship. Spec: mirror `sqs_consumer_worker` exactly —

```python
# benzene/rabbitmq/consumer.py
def rabbitmq_consumer_worker(app, channel, *, queue: str, **loop_options) -> Worker: ...
```

Same `should_continue`-is-refused `TypeError`, same pass-through of `**loop_options`. Consider a
`close: bool = True` finally-clause on the channel, matching `kafka_consumer_worker`'s rationale
(release the connection promptly rather than waiting for a heartbeat timeout). Test: the factory
refuses `should_continue`; the stop signal ends the loop; the channel is closed on exit.

---

# Not worth porting

**`Benzene.HealthChecks.EntityFramework`** — an EF Core `DbContext` adapter. There is no EF Core in
Python and no ORM the port depends on. `DatabaseConnectionHealthCheck` reduces to Gap 3's
`tcp_check(host, 5432)` or a three-line user-written check calling their own
`SELECT 1`/`conn.execute("SELECT 1")`. The *migration-drift* idea
(`DatabaseHealthCheck<T>`: healthy only if the configured target migration is the **last** applied
one, so a rolled-out pod does not serve against an un-migrated database) is a genuinely good idea
worth a cookbook paragraph showing it against Alembic's `alembic_version` table — but it must not be
a framework package, because it would pin an ORM.

**`Benzene.HostedService`** — pure `Microsoft.Extensions.Hosting` glue (`IHostedService`,
`IHostBuilder.UseBenzene<T>`). It has no Python meaning: there is no generic host to bridge onto.
The Python equivalent of "the hosted service concept" is precisely the consumer loop plus
`WorkerHost` plus the SIGTERM handling in Gap 1 — that *is* the story, and once Gap 1 lands it is
complete. Porting `IBenzeneWorker`'s `start_async`/`stop_async` class shape on top of it would add a
base class to inherit for no gain; `Worker = Callable[[StopSignal], Awaitable[None]]` is the better
Python.

**`Microsoft.Extensions.Configuration` binding** — deliberately not ported even in .NET
(`Benzene.Configuration.Core` is BCL-only by design). Gap 8's `SecretStore` is the whole seam;
typed binding is `pydantic-settings`' job.

**`Benzene.HealthChecks.DynamoDb` / `.Azure.ServiceBus` as dedicated distributions** — the
*capability* (a read-only, non-side-effecting reachability probe: `DescribeTable`, `PeekMessage`) is
right, but four extra distributions for one closure each is not. Put them in
`benzene/aws/checks.py` and `benzene/azure/checks.py` behind the existing `[boto3]`/`[azure]`
extras, as `dynamodb_check(client, table)` / `service_bus_check(client, queue)`, duck-typed against
the SDK client so they test with fakes like every other binding in this port. Keep .NET's two hard
rules: **read-only and non-side-effecting** (never send a probe message), and **exception type name
only** in the detail. Lower priority than Gaps 1-4.

**`ICancellationTokenAccessor`** — a scoped accessor threading an ambient `CancellationToken`
through a pipeline that does not carry one. Python's answer is `asyncio.timeout` / `wait_for` and
task cancellation, which is ambient already. Gap 4's per-check timeout covers the real use.

**`IProcessTimer` / `IProcessTimerFactory` / `UseTimer("name")`** — .NET's own CLAUDE.md says it is
kept only for source-compat and that new code should use `Activity` directly. Do not port a
deprecated seam into a new port.

**`DebugMiddlewareWrapper`** — `Debug.WriteLine` per middleware stage, visible only under a debugger
in a `DEBUG` build. Python's equivalent is a `logging` call at `DEBUG` level, which anyone can add;
a framework wrapper for it is noise.

**`ActivityMiddlewareWrapper`'s span-per-middleware** — .NET wraps *every* middleware in its own
`Activity`. That is defensible there because `ActivitySource.StartActivity` no-ops when nothing
listens (and they measured the fast path). In Python, `mesh/trace.py:trace_middleware` already emits
one span per *invocation*, which is the useful granularity; a span per middleware stage would
multiply trace volume for little insight. Keep the current design. **Do** consider porting the
narrower, high-value pieces of `Benzene.Diagnostics`' tag set that the mesh reads back:
`benzene.service` (from the service name) and `benzene.exception.type` (the failure's *why*, type
name only, span-only and never a metric tag for cardinality reasons) — both are additive attributes
on the existing span in `otel/exporter.py`, and both are cheap.

**`HealthCheckNamer`** — dedupes result keys when several checks share a `Type`. Python's
`HealthChecks.add` is already keyed by a caller-supplied unique name and raises
`DuplicateHealthCheckError` on collision, which is strictly better. Nothing to do.

**A fourth reserved `contracts` topic** — see Gap 6. The separation .NET buys with a topic, Python
buys with `dependency=True` forcing deep-probe-only. Adding a reserved topic id would require a
cross-language spec change for no behavioural gain.

---

# Suggested order of work

| # | Gap | Severity | Rough size |
|---|-----|----------|-----------|
| 1 | SIGTERM handling + bounded drain | critical | S |
| 2 | Liveness/readiness split + shutdown latch | critical | M |
| 4 | Concurrent, per-check-timeout `run()` (+ the `str(exc)` leak fix) | high | S |
| 3 | Real checks: tcp / http / disk / memory | high | M |
| 5 | Degraded tier + non-critical | medium | S |
| 7 | Metrics instruments | medium-high | M |
| 8 | Secret store + fail-fast validation | medium | M |
| 6 | Downstream service check + contract drift | medium | M |
| 11 | Bounded concurrent dispatch | medium | M |
| 9 | Response-as-event republishing (and fix the docstring claim now) | medium | M |
| 10 | Ordering diagnostics | low-medium | S |
| 12 | RabbitMQ worker factory | low | XS |

Gaps 1 and 2 are one deploy-safety story and should ship together: signal handling without a
readiness flip still drops requests that arrive during the drain, and a readiness flip with no signal
to trip it never fires. Gap 4 should land before Gap 3, so the first real check that can hang is
already bounded when it arrives.
