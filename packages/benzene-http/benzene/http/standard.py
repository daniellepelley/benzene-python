"""The Cloud Service Profile's well-known HTTP surfaces (design-principles §5.2, profile R3/R4/R5/R7).

A conforming service exposes a small set of well-known HTTP paths under a ``/benzene/`` prefix:

- ``/benzene/invoke`` (R4) — the **wire-envelope endpoint**: POST a ``{topic, headers, body}`` message
  envelope and get the response envelope back, so the service is invokable uniformly across transports.
- ``/benzene/health`` (R3) — the **health aggregate**: the ``{isHealthy, healthChecks}`` report, 200
  when healthy and 503 when not.
- ``/benzene/spec`` (R5) — the **derived spec document** (:class:`~benzene.core.ServiceSpec`): what the
  service serves, projected from its registry.

:class:`StandardPaths` is the one config object that turns these on; :class:`~benzene.http.BenzeneHttpApp`
reads it and serves the surfaces ahead of ordinary routing. The prefix is configurable per deployment
(R7 — "the prefix is the steer, not a cage"); relocate it and clients must be told the new base, so a
service that moves these paths documents the move.
"""

from __future__ import annotations

from dataclasses import dataclass

from benzene.core import HealthChecks, ServiceSpec, SpecSource

#: The default well-known path prefix (design-principles §5.2).
DEFAULT_PREFIX = "/benzene"


@dataclass
class StandardPaths:
    """Which well-known surfaces to expose, and under which prefix.

    ``invoke`` needs nothing but the application (it is on by default). ``health`` and ``spec`` are
    each enabled by supplying their source — a :class:`~benzene.core.HealthChecks` and a
    :class:`~benzene.core.ServiceSpec` (or a callable returning one, re-derived per request) — and are
    simply absent (their path 404s) when not supplied, matching the spec's "declined → no output".

    Declare it on :class:`~benzene.core.AppDefinition` (``standard_paths=StandardPaths(...)``) to
    expose the surfaces from every HTTP-capable host *and* the test harness off one declaration, or
    pass it straight to :class:`BenzeneHttpApp` to turn them on for a single host.
    """

    prefix: str = DEFAULT_PREFIX
    invoke: bool = True
    health: HealthChecks | None = None
    spec: SpecSource | None = None

    @property
    def invoke_path(self) -> str:
        return f"{self.prefix}/invoke"

    @property
    def health_path(self) -> str:
        return f"{self.prefix}/health"

    @property
    def spec_path(self) -> str:
        return f"{self.prefix}/spec"

    def resolved_spec(self) -> ServiceSpec | None:
        """The current spec document, calling the source if it is a callable."""
        return self.spec() if callable(self.spec) else self.spec
