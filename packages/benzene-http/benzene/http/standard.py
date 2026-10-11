"""The Cloud Service Profile's well-known HTTP surfaces (design-principles §5.2, profile R3/R4/R5/R7).

A conforming service exposes a small set of well-known HTTP paths under a ``/benzene/`` prefix:

- ``/benzene/invoke`` (R4) — the **envelope endpoint**: POST a ``{topic, headers, body}`` message
  envelope and get the response envelope back, so the service is invokable uniformly across transports.
  HTTP 200 for every envelope it processed, whatever its status — the envelope carries the outcome —
  and 400 only for a body that is not an envelope (transport-bindings §1). It reaches every registered
  handler, so ``invoke_topics`` restricts which topics it serves.
- ``/benzene/health`` (R3) — the **health report**: the ``{isHealthy, healthChecks}`` report, 200
  when healthy and 503 when not, the report as the body either way.
- ``/benzene/spec`` (R5) — the HTTP route for the ``benzene:spec`` topic: ``GET`` copies its ``type``
  and ``format`` query parameters onto a ``benzene:spec`` request and runs it **through the
  pipeline**, so it answers exactly what the topic answers — the Contract Document, or
  ``501 not-implemented`` for a type or format the service does not serve — under the same
  authentication as every other topic (contract-document §7). The document itself is derived at the
  composition root (``AppDefinition(contract=ContractInfo(...))``); a service that declares none has
  no ``benzene:spec`` handler, and the route answers 404.

:class:`StandardPaths` is the one config object that turns these on; :class:`~benzene.http.BenzeneHttpApp`
reads it and serves the surfaces ahead of ordinary routing. The prefix is configurable per deployment
(R7 — "the prefix is the steer, not a cage"); relocate it and clients must be told the new base, so a
service that moves these paths documents the move.
"""

from __future__ import annotations

from collections.abc import Container
from dataclasses import dataclass

from benzene.core import HealthChecks

#: The default well-known path prefix (design-principles §5.2).
DEFAULT_PREFIX = "/benzene"


@dataclass
class StandardPaths:
    """Which well-known surfaces to expose, and under which prefix.

    ``invoke`` and ``spec`` need nothing but the application (both are on by default); ``health`` is
    enabled by supplying a :class:`~benzene.core.HealthChecks`, and is simply absent (its path 404s)
    when not supplied, matching the spec's "declined → no output".

    ``spec`` routes ``GET /benzene/spec`` to the ``benzene:spec`` topic. What answers it is the
    pipeline's: :func:`~benzene.core.application_from` installs the answer for an
    :class:`~benzene.core.AppDefinition` that declares ``contract=ContractInfo(...)``, and the
    document then carries this host's ``messageEndpoint`` (:attr:`message_endpoint`) and each
    topic's HTTP routes.

    ``invoke_topics`` restricts which topics ``/benzene/invoke`` reaches (transport-bindings §1, D21):
    anything supporting ``in`` — a set of topic ids, or your own object whose ``__contains__`` applies
    a rule. A topic outside it is answered ``not-found``, exactly as if no handler matched. ``None``
    (the default) reaches every registered topic, reserved ones included — the endpoint runs the same
    pipeline, so the same authentication middleware, as every other transport; put that in front of
    it before exposing it.

    Declare it on :class:`~benzene.core.AppDefinition` (``standard_paths=StandardPaths(...)``) to
    expose the surfaces from every HTTP-capable host *and* the test harness off one declaration, or
    pass it straight to :class:`BenzeneHttpApp` to turn them on for a single host.
    """

    prefix: str = DEFAULT_PREFIX
    invoke: bool = True
    invoke_topics: Container[str] | None = None
    health: HealthChecks | None = None
    spec: bool = True

    @property
    def invoke_path(self) -> str:
        return f"{self.prefix}/invoke"

    @property
    def health_path(self) -> str:
        return f"{self.prefix}/health"

    @property
    def spec_path(self) -> str:
        return f"{self.prefix}/spec"

    @property
    def message_endpoint(self) -> str | None:
        """The Contract Document's ``messageEndpoint``: the envelope endpoint, when it is on.

        Absent when ``invoke`` is off — a consumer feature-detects send capability on its presence
        (contract-document §1). Read by :func:`~benzene.core.derive_contract`.
        """
        return self.invoke_path if self.invoke else None
