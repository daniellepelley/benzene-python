"""Composition root — the ``BenzeneStartUp`` seam every host (and the test harness) boots from.

A ``BenzeneStartUp`` is the single place an application declares *what it is*, independent of where
it is hosted: it **registers its services** into a :class:`~benzene.core.Container` and then
**configures its routes and topics** by resolving those services and wiring handlers. Every host —
GCP Functions, AWS Lambda, Azure Functions — and the test harness build the app from the *same*
startup, so a test exercises exactly what deploys. Mirrors .NET's ``BenzeneStartUp`` +
``BenzeneTestHost``'s ``Build<THost>(factory)`` seam.

``build_application`` is the neutral runtime plumbing (core-only — it carries the HTTP router
opaquely so ``benzene.core`` need not depend on ``benzene.http``): run the startup's registrations,
apply any overrides (last-registration-wins — the seam a test uses to substitute a fake), then let
the startup configure the app against a resolved scope.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .contract import ContractDocument, ContractInfo, derive_contract, spec_interception
from .declarations import OutboundRegistry
from .dependencies import Container, Scope, ServiceOverride
from .envelope import BenzeneMessageApplication
from .pipeline import Middleware, MiddlewarePipeline
from .registry import Registry


@dataclass
class AppDefinition:
    """What a startup produces: the message registry, an optional HTTP router, and any middleware.

    ``router`` is typed ``Any`` so this stays a pure ``benzene.core`` type — a message-only app
    leaves it ``None``; an HTTP-capable host receives whatever ``benzene.http`` router the startup
    built and passes it straight to the host binding. ``middleware`` is the pipeline the startup
    wants installed ahead of the message router (cross-cutting concerns such as mesh interception,
    tracing, or auth), so a host and a test boot the *same* pipeline, not just the same handlers.

    ``standard_paths`` (typed ``Any``, the same trick as ``router`` — it holds a
    ``benzene.http.StandardPaths``) declares the Cloud Service Profile's well-known HTTP surfaces
    (``/benzene/invoke|health|spec``) as part of the composition root, so every HTTP-capable host and
    the test harness expose them from one declaration rather than re-wiring them per host.

    ``contract`` turns the service's self-description on (contract-document.md): the one
    :class:`~benzene.core.ContractDocument` is derived from the registry, ``outbound``'s event and
    send declarations, the router's HTTP routes and the standard paths' envelope endpoint, and the
    ``benzene:spec`` topic answers with it — over every transport, ``GET /benzene/spec`` included.
    """

    registry: Registry | None = None
    router: Any = None
    middleware: list[Middleware] = field(default_factory=list)
    standard_paths: Any = None
    outbound: OutboundRegistry | None = None
    contract: ContractInfo | None = None

    def handler_registry(self) -> Registry:
        """The registry the app serves: ``registry``, else one built from the router's handlers."""
        if self.registry is not None:
            return self.registry
        registry = Registry()
        if self.router is not None:
            for handler_definition in self.router.definitions():
                registry.add_definition(handler_definition)
        return registry

    def contract_document(self) -> ContractDocument | None:
        """The service's Contract Document, or ``None`` when ``contract`` is not declared.

        Deterministic: every call derives the identical document. A component-name collision
        raises :class:`~benzene.core.ComponentNameCollisionError` here.
        """
        if self.contract is None:
            return None
        return derive_contract(
            self.contract,
            self.handler_registry(),
            outbound=self.outbound,
            router=self.router,
            standard_paths=self.standard_paths,
        )


class BenzeneStartUp:
    """Base class for an application's composition root. Subclass and override both methods."""

    def configure_services(self, services: Container, config: Mapping[str, str]) -> None:
        """Register the application's services (stores, outbound clients, clock, …)."""

    def configure(self, services: Scope, config: Mapping[str, str]) -> AppDefinition:
        """Resolve services and wire routes/topics, returning the :class:`AppDefinition`."""
        raise NotImplementedError


def build_application(
    startup: BenzeneStartUp | type[BenzeneStartUp],
    *,
    overrides: Sequence[ServiceOverride] = (),
    config: Mapping[str, str] | None = None,
) -> tuple[AppDefinition, Scope]:
    """Boot an app from its startup: register services, apply overrides, then configure.

    ``overrides`` run **after** the startup's own ``configure_services`` (last-registration-wins), so
    a caller — a test, or a host supplying its cloud's real outbound client — can replace any
    registration before the app is wired. Returns the :class:`AppDefinition` and the root
    :class:`~benzene.core.Scope` (so a caller can resolve services for assertions).

    An override's return value is ignored, so ``lambda c: c.add_instance(...)`` — the shortest way to
    say it, and the form this module's own docstrings use — is accepted. See :data:`ServiceOverride`.
    """
    instance = startup() if isinstance(startup, type) else startup
    resolved_config = dict(config or {})

    container = Container()
    instance.configure_services(container, resolved_config)
    for override in overrides:
        override(container)

    scope = container.create_scope()
    return instance.configure(scope, resolved_config), scope


def application_from(definition: AppDefinition) -> BenzeneMessageApplication:
    """Build the runnable :class:`~benzene.core.BenzeneMessageApplication` from an :class:`AppDefinition`.

    Installs the startup's ``middleware`` ahead of the message router. Every host (and the test
    harness) builds the app this way, so a startup's cross-cutting middleware — mesh interception,
    tracing, auth — is exercised identically in deployment and in tests. When only a ``router`` is
    present, its handler definitions become the registry.

    When the definition declares a ``contract``, the Contract Document is derived **here, once** —
    so a component-name collision fails the build of the app, before the first message — and
    ``benzene:spec`` is answered by :func:`~benzene.core.spec_interception`, installed after the
    startup's own middleware: the topic runs the same pipeline, authentication included, as every
    other topic (contract-document §7).
    """
    registry = definition.handler_registry()
    middleware = list(definition.middleware)
    document = definition.contract_document()
    if document is not None:
        middleware.append(spec_interception(document))
    return BenzeneMessageApplication(registry, MiddlewarePipeline(middleware))
