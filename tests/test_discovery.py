"""Handler discovery, and the diagnostics for when a topic does not resolve.

Both exist to kill the same class of bug: a handler that was written but never registered, which
otherwise surfaces only as a ``not-found`` at runtime and looks like a routing or deployment fault.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from benzene.core import (
    BenzeneMessageApplication,
    CompositeHandlers,
    ExplicitHandlers,
    HandlerSource,
    ModuleHandlers,
    PackageHandlers,
    Registry,
    UnregisteredHandlerWarning,
    handlers_in_package,
    message,
    unregistered_handlers,
    warn_unregistered_handlers,
)
from benzene.results import Result, Status

SAMPLE_TOPICS = {
    "sample:order:create",
    "sample:order:cancel",
    "sample:payment:capture",
    "sample:deep",
}


# --- scanning ---------------------------------------------------------------------------------


def test_from_package_registers_every_handler_in_the_tree() -> None:
    registry = Registry.from_package("tests.sample_app")
    assert set(registry.topics()) == SAMPLE_TOPICS
    # Both versions of the versioned topic, not just one.
    assert registry.versions_for("sample:payment:capture") == ["", "2"]


def test_scan_reaches_a_module_nothing_else_imports() -> None:
    """The point of walking the package: import *is* the discovery mechanism in Python."""
    sys.modules.pop("tests.sample_app.nested.deep", None)
    registry = Registry.from_package("tests.sample_app")
    assert registry.find("sample:deep") is not None


def test_re_exported_handler_is_registered_once() -> None:
    """``from .orders import create_order`` in ``__init__`` must not double-register."""
    registry = Registry.from_package("tests.sample_app")  # would raise DuplicateHandlerError
    assert len([d for d in registry.definitions() if d.topic == "sample:order:create"]) == 1


def test_scanned_handlers_actually_route() -> None:
    app = BenzeneMessageApplication(Registry.from_package("tests.sample_app"))
    response = asyncio.run(
        app.handle_async({"topic": "sample:deep", "headers": {}, "body": "{}"})
    )
    assert response["statusCode"] == Status.OK


# --- the HandlerSource seam -------------------------------------------------------------------


def test_registry_satisfies_the_handler_source_protocol() -> None:
    assert isinstance(Registry(), HandlerSource)
    assert isinstance(PackageHandlers("tests.sample_app"), HandlerSource)


def test_explicit_source_is_equivalent_to_hand_registration() -> None:
    from tests.sample_app.orders import cancel_order, create_order

    scanned = Registry().add_source(ExplicitHandlers(create_order, cancel_order))
    by_hand = Registry().add(create_order).add(cancel_order)
    assert scanned.topics() == by_hand.topics()


def test_explicit_source_rejects_an_untagged_function() -> None:
    async def untagged(_request: dict) -> Result:
        return Result.ok()

    with pytest.raises(ValueError):
        Registry().add_source(ExplicitHandlers(untagged))


def test_module_source_does_not_walk_submodules() -> None:
    topics = {d.topic for d in ModuleHandlers("tests.sample_app.orders").definitions()}
    assert topics == {"sample:order:create", "sample:order:cancel"}


def test_composite_chains_sources_and_drops_repeats() -> None:
    """A handler contributed by an earlier source is not contributed again by a later one."""
    from tests.sample_app.orders import create_order

    composite = CompositeHandlers(
        PackageHandlers("tests.sample_app"),
        ExplicitHandlers(create_order),  # already contributed by the scan above
    )
    registry = Registry().add_source(composite)  # would raise DuplicateHandlerError
    assert set(registry.topics()) == SAMPLE_TOPICS


# --- the safety net ---------------------------------------------------------------------------


def test_unregistered_handler_is_reported() -> None:
    registry = Registry.from_package("tests.sample_app")
    missing = unregistered_handlers(registry, "tests.sample_stray")
    assert [d.topic for d in missing] == ["sample:stray"]


def test_unregistered_handler_warns_with_a_usable_location() -> None:
    registry = Registry()
    with pytest.warns(UnregisteredHandlerWarning) as caught:
        warn_unregistered_handlers(registry, "tests.sample_stray")
    text = str(caught[0].message)
    assert "sample:stray" in text
    assert "tests.sample_stray.handlers" in text


def test_no_warning_when_everything_is_registered() -> None:
    registry = Registry.from_package("tests.sample_app")
    with warnings_as_errors():
        assert warn_unregistered_handlers(registry, "tests.sample_app") == []


class warnings_as_errors:
    """Any warning at all inside this block fails the test."""

    def __enter__(self):
        import warnings

        self._ctx = warnings.catch_warnings()
        self._ctx.__enter__()
        warnings.simplefilter("error")
        return self

    def __exit__(self, *exc):
        return self._ctx.__exit__(*exc)


# --- unresolved-topic diagnostics -------------------------------------------------------------


def _not_found_message(registry: Registry, topic: str, version: str = "") -> str:
    headers = {"benzene-version": version} if version else {}
    app = BenzeneMessageApplication(registry)
    response = asyncio.run(app.handle_async({"topic": topic, "headers": headers, "body": "{}"}))
    assert response["statusCode"] == Status.NOT_FOUND
    return response["body"]


def test_empty_registry_says_so() -> None:
    message_text = _not_found_message(Registry(), "anything")
    assert "registry is empty" in message_text
    assert "startup wiring" in message_text


def test_known_topic_at_an_unregistered_version_is_distinguished() -> None:
    registry = Registry.from_package("tests.sample_app")
    message_text = _not_found_message(registry, "sample:payment:capture", version="9")
    assert "the topic is registered" in message_text
    assert "'2'" in message_text  # the versions that *are* registered
    assert "Did you mean" not in message_text  # not a typo — do not send someone down that path


def test_a_typo_gets_a_suggestion() -> None:
    registry = Registry.from_package("tests.sample_app")
    message_text = _not_found_message(registry, "sample:order:creat")
    assert "Did you mean 'sample:order:create'?" in message_text


def test_an_unrelated_topic_gets_no_misleading_suggestion() -> None:
    registry = Registry.from_package("tests.sample_app")
    message_text = _not_found_message(registry, "completely:different")
    assert "Did you mean" not in message_text
    assert "handler(s) are registered" in message_text


def test_the_message_does_not_enumerate_the_registry() -> None:
    """A failed request should not hand a caller the service's full topic inventory."""
    registry = Registry.from_package("tests.sample_app")
    message_text = _not_found_message(registry, "completely:different")
    assert "sample:payment:capture" not in message_text
    assert "sample:deep" not in message_text


def test_empty_topic_is_still_a_validation_error() -> None:
    @message("t")
    async def handler(_request: dict) -> Result:
        return Result.ok()

    app = BenzeneMessageApplication(Registry().add(handler))
    response = asyncio.run(app.handle_async({"topic": "", "headers": {}, "body": "{}"}))
    assert response["statusCode"] == Status.VALIDATION_ERROR


# --- configurable wire names ------------------------------------------------------------------


def test_default_topic_key_is_the_plain_name() -> None:
    from benzene.core import DEFAULT_WIRE_NAMES

    assert DEFAULT_WIRE_NAMES.topic == "topic"


def test_use_wire_names_overrides_for_the_whole_service() -> None:
    from benzene.core import Container, WireNames, use_wire_names, wire_names

    services = Container()
    use_wire_names(services, WireNames(topic="x-my-topic"))
    assert wire_names(services.create_scope()).topic == "x-my-topic"


def test_a_service_that_registers_nothing_gets_the_defaults() -> None:
    from benzene.core import Container, DEFAULT_WIRE_NAMES, wire_names

    assert wire_names(Container().create_scope()) is DEFAULT_WIRE_NAMES
    assert wire_names(None) is DEFAULT_WIRE_NAMES


def test_the_application_exposes_the_registered_names() -> None:
    """Hosts read the names from here, so one registration reaches every binding."""
    from benzene.core import Container, WireNames, use_wire_names

    services = Container()
    use_wire_names(services, WireNames(topic="x-my-topic"))
    app = BenzeneMessageApplication(Registry(), container=services)
    assert app.wire_names.topic == "x-my-topic"


def test_take_topic_reads_case_insensitively_and_consumes_the_key() -> None:
    from benzene.core import take_topic

    topic, headers = take_topic({"Topic": "order:create", "tenant": "acme"})
    assert topic == "order:create"
    assert headers == {"tenant": "acme"}  # the routing key must not also be a header


def test_take_topic_ignores_a_key_that_is_not_the_configured_one() -> None:
    from benzene.core import WireNames, take_topic

    topic, headers = take_topic({"topic": "order:create"}, WireNames(topic="x-my-topic"))
    assert topic == ""
    assert headers == {"topic": "order:create"}  # now just an application header
