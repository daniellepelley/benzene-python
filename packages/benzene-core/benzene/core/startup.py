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

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from .dependencies import Container, Scope
from .registry import Registry


@dataclass
class AppDefinition:
    """What a startup produces: the message topic registry and (optionally) an HTTP router.

    ``router`` is typed ``Any`` so this stays a pure ``benzene.core`` type — a message-only app
    leaves it ``None``; an HTTP-capable host receives whatever ``benzene.http`` router the startup
    built and passes it straight to the host binding.
    """

    registry: Registry | None = None
    router: Any = None


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
    overrides: Sequence[Callable[[Container], None]] = (),
    config: Mapping[str, str] | None = None,
    check_packages: Sequence[str] = (),
) -> tuple[AppDefinition, Scope]:
    """Boot an app from its startup: register services, apply overrides, then configure.

    ``overrides`` run **after** the startup's own ``configure_services`` (last-registration-wins), so
    a caller — a test, or a host supplying its cloud's real outbound client — can replace any
    registration before the app is wired. Returns the :class:`AppDefinition` and the root
    :class:`~benzene.core.Scope` (so a caller can resolve services for assertions).

    ``check_packages`` names the package(s) the application keeps its handlers in; each is scanned
    and any ``@message`` handler missing from the registry is warned about. Opt-in, because only the
    application knows where its handlers live, and because scanning everything imported would flag
    handlers a sibling deployable legitimately owns. Applications that build their registry with
    :meth:`Registry.from_package` have no use for it — nothing in a scanned package can be missed.
    """
    instance = startup() if isinstance(startup, type) else startup
    resolved_config = dict(config or {})

    container = Container()
    instance.configure_services(container, resolved_config)
    for override in overrides:
        override(container)

    scope = container.create_scope()
    definition = instance.configure(scope, resolved_config)

    if check_packages and definition.registry is not None:
        from .discovery import warn_unregistered_handlers

        warn_unregistered_handlers(definition.registry, *check_packages, stacklevel=3)

    return definition, scope
