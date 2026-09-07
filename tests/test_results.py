"""The Result factories — one per framework status, and the success/failure classification.

Every framework-defined status has a matching factory (the success set and the failure set both
covered), so an application never has to drop to ``Result.failure(Status.X, ...)`` for a standard
status. These pin each factory to its status and confirm the success/failure split.
"""

from __future__ import annotations

import pytest
from benzene.results import Result, Status
from benzene.results.status import FAILURE_STATUSES, SUCCESS_STATUSES

SUCCESS_FACTORIES = {
    Status.OK: Result.ok,
    Status.CREATED: Result.created,
    Status.ACCEPTED: Result.accepted,
    Status.UPDATED: Result.updated,
    Status.DELETED: Result.deleted,
    Status.IGNORED: Result.ignored,
}

FAILURE_FACTORIES = {
    Status.BAD_REQUEST: Result.bad_request,
    Status.VALIDATION_ERROR: Result.validation_error,
    Status.UNAUTHORIZED: Result.unauthorized,
    Status.FORBIDDEN: Result.forbidden,
    Status.NOT_FOUND: Result.not_found,
    Status.CONFLICT: Result.conflict,
    Status.TOO_MANY_REQUESTS: Result.too_many_requests,
    Status.TIMEOUT: Result.timeout,
    Status.NOT_IMPLEMENTED: Result.not_implemented,
    Status.SERVICE_UNAVAILABLE: Result.service_unavailable,
    Status.UNEXPECTED_ERROR: Result.unexpected_error,
}


@pytest.mark.parametrize("status,factory", SUCCESS_FACTORIES.items(), ids=SUCCESS_FACTORIES.keys())
def test_success_factory_sets_status_and_payload(status: str, factory) -> None:
    result = factory({"v": 1})
    assert result.status == status
    assert result.is_successful
    assert result.payload == {"v": 1}


@pytest.mark.parametrize("status,factory", FAILURE_FACTORIES.items(), ids=FAILURE_FACTORIES.keys())
def test_failure_factory_sets_status_and_errors(status: str, factory) -> None:
    result = factory("boom")
    assert result.status == status
    assert not result.is_successful
    assert result.messages == ("boom",)
    assert result.payload is None


def test_every_framework_status_has_a_factory() -> None:
    # The symmetry guarantee: no standard status is missing a shortcut.
    assert set(SUCCESS_FACTORIES) == set(SUCCESS_STATUSES)
    assert set(FAILURE_FACTORIES) == set(FAILURE_STATUSES)
