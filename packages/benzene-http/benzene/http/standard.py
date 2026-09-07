"""The Cloud Service Profile's well-known HTTP surfaces (design-principles §5.2, profile R3/R4/R5/R7).

A conforming service exposes a small set of well-known HTTP paths under a ``/benzene/`` prefix:

- ``/benzene/invoke`` (R4) — the **wire-envelope endpoint**: POST a ``{topic, headers, body}`` message
  envelope and get the response envelope back, so the service is invokable uniformly across transports.
- ``/benzene/health`` (R3) — the **health aggregate**: the ``{isHealthy, healthChecks}`` report, 200
  when healthy and 503 when not.
- ``/benzene/spec`` (R5) — the **derived spec document**: what the service serves, projected from its
  registry. It answers the Contract Document (contract-document.md — the format every language's
  client generator parses) by default, and this port's native
  :class:`~benzene.core.ServiceSpec` payload under ``?type=native``.

:class:`StandardPaths` is the one config object that turns these on; :class:`~benzene.http.BenzeneHttpApp`
reads it and serves the surfaces ahead of ordinary routing. The prefix is configurable per deployment
(R7 — "the prefix is the steer, not a cage"); relocate it and clients must be told the new base, so a
service that moves these paths documents the move.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from benzene.core import (
    ContractDocument,
    ContractSource,
    HealthChecks,
    HttpMapping,
    ServiceSpec,
    SpecSource,
    resolve_contract,
)

#: The default well-known path prefix (design-principles §5.2).
DEFAULT_PREFIX = "/benzene"

#: ``?type=`` on ``/benzene/spec`` selecting this port's native ``{service, topics}`` payload. Any
#: other value — including none at all — selects the Contract Document, which is what R5 names
#: (``?type=benzene&format=json``) and what the .NET reference falls through to for an absent or
#: unrecognised type. A generator that follows the profile's documented path and asks for nothing
#: must get the parseable document, not a port-native shape only this port can read.
NATIVE_SPEC_TYPE = "native"

#: The ``?type=`` value that names the Contract Document explicitly (the R5 spelling).
CONTRACT_SPEC_TYPE = "benzene"


@dataclass
class StandardPaths:
    """Which well-known surfaces to expose, and under which prefix.

    ``invoke`` needs nothing but the application (it is on by default). ``health`` and ``spec`` are
    each enabled by supplying their source — a :class:`~benzene.core.HealthChecks` and a
    :class:`~benzene.core.ServiceSpec` (or a callable returning one, re-derived per request) — and are
    simply absent (their path 404s) when not supplied, matching the spec's "declined → no output".

    ``spec`` enables the surface; what it *answers with* depends on ``?type=``. Left alone, the
    Contract Document is projected from the ``ServiceSpec`` (schemas inline, no named catalogue) with
    this host's own knowledge folded in — the message endpoint and each topic's HTTP routes. Supply
    ``contract`` — most usefully ``ContractDocument.derive(registry, ...)``, which sees the handlers'
    declared types and so can *name* each payload in ``components.schemas`` — to serve an authored
    document instead; it is then emitted exactly as given, message endpoint and HTTP mappings
    included, because a document you built is not one this host should be editing.

    Declare it on :class:`~benzene.core.AppDefinition` (``standard_paths=StandardPaths(...)``) to
    expose the surfaces from every HTTP-capable host *and* the test harness off one declaration, or
    pass it straight to :class:`BenzeneHttpApp` to turn them on for a single host.
    """

    prefix: str = DEFAULT_PREFIX
    invoke: bool = True
    health: HealthChecks | None = None
    spec: SpecSource | None = None
    contract: ContractSource | None = None
    #: Every transport this host receives messages over (contract-document.md §1's ``transports``).
    #: Declared, not detected: an HTTP host cannot see the queues and topics the same service is also
    #: wired to, and a half-true list is worse than the absent one a consumer knows to feature-detect.
    transports: tuple[str, ...] = ()

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

    def resolved_contract(
        self, http_mappings: Mapping[tuple[str, str], Sequence[HttpMapping]] | None = None
    ) -> ContractDocument | None:
        """The Contract Document to serve at ``/benzene/spec``, or ``None`` when R5 is not wired.

        An authored ``contract`` wins as-is. Otherwise the document is projected from the
        ``ServiceSpec`` — one source of truth, so ``?type=benzene`` and ``?type=native`` can never
        describe two different topic sets — with ``messageEndpoint``, ``transports`` and the caller's
        ``http_mappings`` folded in, since those are the host's knowledge and not the registry's.
        """
        authored = resolve_contract(self.contract)
        if authored is not None:
            return authored
        spec = self.resolved_spec()
        if spec is None:
            return None
        return ContractDocument.from_spec(
            spec,
            message_endpoint=self.invoke_path if self.invoke else None,
            transports=self.transports,
            http_mappings=http_mappings,
        )
