"""Handler discovery — producing registration records without hand-listing them (core-concepts §9).

The spec is explicit that *explicit registration is the concept* and discovery is an **idiom**:
scanning must be sugar over the records an application could have handed the framework itself.
:class:`HandlerSource` is that seam — the Python counterpart of .NET's ``IMessageHandlersFinder``,
where reflection scanning is merely the default implementation and an explicit list or a
container-backed lookup are equally first-class.

**Python cannot scan the way .NET does, and the difference matters.** .NET enumerates the types in
a compiled assembly without running application code; Python can only discover a decorated function
by *importing the module that defines it*. So :class:`PackageHandlers` walks a package and imports
every submodule — that import **is** the discovery mechanism, not an incidental side effect. The
practical consequence is the rule to give users: a handler is found if it lives under a package you
name, and is invisible otherwise. That is a rule someone can hold in their head, which is the point.

Two failure modes this module exists to remove:

* *"I wrote a handler and it never runs."* Naming a package registers everything in it, so there is
  no per-handler line to forget.
* *"I use explicit registration and missed one."* :func:`warn_unregistered_handlers` scans a package
  and reports anything tagged but unregistered. Because it imports the package tree, it sees
  handlers in modules nothing else imported — the exact blind spot a scan of ``sys.modules`` has.
"""

from __future__ import annotations

import difflib
import importlib
import pkgutil
import sys
import warnings
from types import ModuleType
from typing import Iterable, Protocol, Sequence, runtime_checkable

from .handler import Handler, HandlerDefinition, definition_of


class UnregisteredHandlerWarning(UserWarning):
    """A ``@message``-tagged handler was found that no registry will route to."""


@runtime_checkable
class HandlerSource(Protocol):
    """Anything that can produce registration records.

    Deliberately one method: it is the whole contract, so an application can supply its own source
    (a plugin loader, a container query, a config file) without the framework knowing about it.
    :class:`~benzene.core.Registry` satisfies this protocol too, so registries compose as sources.
    """

    def definitions(self) -> list[HandlerDefinition]:  # pragma: no cover - protocol
        ...


def _definitions_in(modules: Iterable[ModuleType]) -> list[HandlerDefinition]:
    """Collect the tagged handlers reachable as attributes of ``modules``, de-duplicated.

    De-duplication is by function identity, not by topic, and it is load-bearing: re-exporting a
    handler (``from .orders import create`` in a package ``__init__``) makes the same function an
    attribute of two modules, and registering it twice would raise ``DuplicateHandlerError`` — a
    scan would then break the very layout that packages are usually organised in.
    """
    found: list[HandlerDefinition] = []
    seen: set[int] = set()
    for module in modules:
        if module is None:
            continue
        try:
            members = list(vars(module).values())
        except Exception:  # a module object without a usable __dict__
            continue
        for value in members:
            if not callable(value) or id(value) in seen:
                continue
            definition = definition_of(value)
            if definition is None:
                continue
            seen.add(id(value))
            found.append(definition)
    # Stable order so registration (and any error about it) is reproducible run to run.
    return sorted(found, key=lambda d: (d.topic, d.version))


def handlers_in_module(module: ModuleType | str) -> list[HandlerDefinition]:
    """The tagged handlers in a single module. Importing it is what makes them visible."""
    resolved = importlib.import_module(module) if isinstance(module, str) else module
    return _definitions_in([resolved])


def handlers_in_package(package: ModuleType | str) -> list[HandlerDefinition]:
    """The tagged handlers anywhere under ``package``, importing every submodule to find them.

    A plain module (no ``__path__``) is accepted and treated as a package of one, so callers need
    not care which they were given.
    """
    resolved = importlib.import_module(package) if isinstance(package, str) else package
    modules = [resolved]
    path = getattr(resolved, "__path__", None)
    if path is not None:
        for _finder, name, _ispkg in pkgutil.walk_packages(path, resolved.__name__ + "."):
            modules.append(importlib.import_module(name))
    return _definitions_in(modules)


class ExplicitHandlers:
    """The explicit path as a source: hand it the ``@message``-tagged functions themselves."""

    def __init__(self, *handlers: Handler) -> None:
        self._handlers = handlers

    def definitions(self) -> list[HandlerDefinition]:
        definitions = []
        for handler in self._handlers:
            definition = definition_of(handler)
            if definition is None:
                raise ValueError(
                    f"{getattr(handler, '__name__', handler)!r} is not a @message-tagged handler."
                )
            definitions.append(definition)
        return definitions


class ModuleHandlers:
    """Scans the named modules only — no submodule walk."""

    def __init__(self, *modules: ModuleType | str) -> None:
        self._modules = modules

    def definitions(self) -> list[HandlerDefinition]:
        return _definitions_in(
            [importlib.import_module(m) if isinstance(m, str) else m for m in self._modules]
        )


class PackageHandlers:
    """Scans whole package trees. The default discovery idiom for this port."""

    def __init__(self, *packages: ModuleType | str) -> None:
        self._packages = packages

    def definitions(self) -> list[HandlerDefinition]:
        return _definitions_in_order(
            [handlers_in_package(package) for package in self._packages]
        )


class CompositeHandlers:
    """Chains sources in order — the counterpart of .NET's ``CompositeMessageHandlersFinder``.

    Lets a deployment combine idioms (scan the app's package, then add a handler supplied by a
    plugin) without either source knowing about the other.
    """

    def __init__(self, *sources: HandlerSource) -> None:
        self._sources = sources

    def definitions(self) -> list[HandlerDefinition]:
        return _definitions_in_order([source.definitions() for source in self._sources])


def _definitions_in_order(groups: Sequence[Sequence[HandlerDefinition]]) -> list[HandlerDefinition]:
    """Flatten groups, dropping records for a handler already contributed by an earlier group."""
    flattened: list[HandlerDefinition] = []
    seen: set[int] = set()
    for group in groups:
        for definition in group:
            if id(definition.handler) in seen:
                continue
            seen.add(id(definition.handler))
            flattened.append(definition)
    return flattened


def unregistered_handlers(registry, *packages: ModuleType | str) -> list[HandlerDefinition]:
    """Tagged handlers that ``registry`` will never route to.

    With ``packages``, those trees are imported and scanned — this catches a handler in a module
    nothing else imported. Without them, it falls back to the modules already in ``sys.modules``,
    which is cheaper but can only see what something has already imported.
    """
    if packages:
        candidates = _definitions_in_order(
            [handlers_in_package(package) for package in packages]
        )
    else:
        candidates = _definitions_in(list(sys.modules.values()))
    return [d for d in candidates if registry.find(d.topic, d.version) is None]


def warn_unregistered_handlers(
    registry, *packages: ModuleType | str, stacklevel: int = 2
) -> list[HandlerDefinition]:
    """Warn (never raise) about tagged handlers missing from ``registry``; return them.

    A warning rather than an error on purpose: one codebase commonly builds several deployables,
    and an orders host legitimately does not mount the payments handlers. Failing startup would
    make that normal layout impossible, so this reports and leaves the decision to the application.
    """
    missing = unregistered_handlers(registry, *packages)
    for definition in missing:
        location = f"{definition.handler.__module__}.{getattr(definition.handler, '__qualname__', '?')}"
        version = f" version {definition.version!r}" if definition.version else ""
        warnings.warn(
            f"{location} is tagged @message({definition.topic!r}){version} but is not registered, "
            "so it will not receive messages. Register it, or scan its package with "
            "Registry.from_package(...).",
            UnregisteredHandlerWarning,
            stacklevel=stacklevel,
        )
    return missing


def suggest_topic(topic: str, known: Sequence[str]) -> str | None:
    """The closest registered topic to ``topic``, if one is close enough to be worth suggesting."""
    matches = difflib.get_close_matches(topic, list(known), n=1, cutoff=0.7)
    return matches[0] if matches else None
