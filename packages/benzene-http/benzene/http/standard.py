"""The Cloud Service Profile's well-known HTTP surfaces (design-principles §5.2, profile R3/R4/R5/R7).

A conforming service exposes a small set of well-known HTTP paths under a ``/benzene/`` prefix:

- ``/benzene/invoke`` (R4) — the **envelope endpoint**: POST a ``{topic, headers, body}`` message
  envelope and get the response envelope back, so the service is invokable uniformly across transports.
  HTTP 200 for every envelope it processed, whatever its status — the envelope carries the outcome —
  and 400 only for a body that is not an envelope (transport-bindings §1). It reaches every registered
  handler, so ``invoke_topics`` restricts which topics it serves.
- ``/benzene/health`` (R3) — the **health report**: the ``{isHealthy, healthChecks}`` report, 200
  when healthy and 503 when not, the report as the body either way.
- ``/benzene/spec`` (R5) — the **derived spec document**: what the service serves, projected from its
  registry. ``?type=``/``?format=`` are the ``benzene:spec`` request (wire-contracts §7): the Contract
  Document (contract-document.md — the format every language's client generator parses) by default or
  for ``type=benzene``, this port's native :class:`~benzene.core.ServiceSpec` payload for
  ``type=native``, and ``501 not-implemented`` for any other type or a format other than ``json``.

:class:`StandardPaths` is the one config object that turns these on; :class:`~benzene.http.BenzeneHttpApp`
reads it and serves the surfaces ahead of ordinary routing. The prefix is configurable per deployment
(R7 — "the prefix is the steer, not a cage"); relocate it and clients must be told the new base, so a
service that moves these paths documents the move.
"""

from __future__ import annotations

from collections.abc import Container, Mapping, Sequence
from dataclasses import dataclass

from benzene.core import (
    SPEC_TYPE_BENZENE,
    SPEC_TYPE_NATIVE,
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

#: ``?type=`` on ``/benzene/spec`` selecting this port's native ``{service, topics}`` payload — a port
#: extension type (wire-contracts §7). Absent or ``benzene`` selects the Contract Document, which is
#: what R5 names (``?type=benzene&format=json``): a generator that follows the profile's documented
#: path and asks for nothing gets the parseable document, not a port-native shape only this port can
#: read. Any other type is answered ``not-implemented``, never with a different document.
NATIVE_SPEC_TYPE = SPEC_TYPE_NATIVE

#: The ``?type=`` value that names the Contract Document explicitly (the R5 spelling).
CONTRACT_SPEC_TYPE = SPEC_TYPE_BENZENE


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
