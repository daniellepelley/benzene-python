"""In-process saga — compensating rollback across steps (mirrors ``Benzene.Saga``).

A saga sequences steps that each change state and each know how to *undo* that change. If any step
fails, the steps that already ran are compensated in reverse order, so a multi-step operation that
can't complete leaves no half-finished trail — the pattern you reach for when a single transaction
can't span the resources involved (reserve stock, take payment, book courier: fail at courier and you
must refund and release the stock).

This is the **in-process** saga the roadmap scopes: orchestration and compensation held in one
process, not a durable/distributed coordinator. Nothing here persists across a crash — it is the
"undo the work I just did" primitive, deliberately small. Like the rest of the port it speaks
:class:`~benzene.results.Result`: ``execute`` never raises for a step failure, it returns a
:class:`SagaResult` describing what ran and what was rolled back.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from benzene.results import Result, Status

#: A saga's shared, mutable working state — steps read and write it as they run.
State = dict[str, Any]

#: A step's forward action: mutate the state, return a Result (or ``None`` for "ok, continue").
SagaAction = Callable[[State], Awaitable[Result | None]]

#: A step's compensation: undo the action's effect. Best-effort; a raise is recorded, not propagated.
SagaCompensation = Callable[[State], Awaitable[None]]


@dataclass(frozen=True)
class SagaStep:
    """One forward action and its optional compensation."""

    name: str
    action: SagaAction
    compensation: SagaCompensation | None = None


@dataclass(frozen=True)
class SagaResult:
    """The outcome of running a saga.

    ``result`` is ``ok`` (payload: the final state) when every step succeeded, otherwise the failing
    step's failure result. ``completed`` lists the steps whose action succeeded, in run order;
    ``compensated`` lists the steps successfully rolled back, in the reverse order they were undone;
    ``compensation_failures`` names any step whose compensation itself raised (surfaced, never hidden).
    """

    result: Result
    state: State
    completed: list[str] = field(default_factory=list)
    compensated: list[str] = field(default_factory=list)
    compensation_failures: list[str] = field(default_factory=list)
    failed_step: str | None = None

    @property
    def is_successful(self) -> bool:
        return self.result.is_successful


class Saga:
    """A builder-and-runner for a compensating sequence of steps.

    Add steps with :meth:`step` (chainable), then :meth:`execute`. Steps run in the order added; the
    first failure stops forward progress and triggers reverse-order compensation of the steps that
    already completed.
    """

    def __init__(self) -> None:
        self._steps: list[SagaStep] = []

    def step(
        self,
        name: str,
        action: SagaAction,
        compensation: SagaCompensation | None = None,
    ) -> Saga:
        """Append a step (its forward ``action`` and optional ``compensation``). Chainable."""
        self._steps.append(SagaStep(name, action, compensation))
        return self

    def add(self, step: SagaStep) -> Saga:
        """Append a pre-built :class:`SagaStep`. Chainable."""
        self._steps.append(step)
        return self

    async def execute(self, state: State | None = None) -> SagaResult:
        """Run the steps; on the first failure, compensate the completed ones in reverse.

        A step "fails" when its action returns a failure :class:`~benzene.results.Result` or raises
        (a raise becomes an ``unexpected-error`` result). Compensation is best-effort: every
        completed step's compensation is attempted even if an earlier one raised, and any raise is
        recorded in ``compensation_failures`` rather than propagated.
        """
        working: State = dict(state or {})
        completed: list[SagaStep] = []

        for step in self._steps:
            try:
                outcome = await step.action(working)
            except Exception as exc:
                failure = Result.failure(Status.UNEXPECTED_ERROR, f"{step.name}: {exc}")
                return await self._rollback(step.name, failure, working, completed)
            if outcome is not None and not outcome.is_successful:
                return await self._rollback(step.name, outcome, working, completed)
            completed.append(step)

        return SagaResult(
            result=Result.ok(working),
            state=working,
            completed=[s.name for s in completed],
        )

    async def _rollback(
        self,
        failed_step: str,
        failure: Result,
        state: State,
        completed: list[SagaStep],
    ) -> SagaResult:
        compensated: list[str] = []
        compensation_failures: list[str] = []
        for step in reversed(completed):
            if step.compensation is None:
                continue
            # A failed compensation is surfaced, never allowed to mask the original failure.
            try:
                await step.compensation(state)
                compensated.append(step.name)
            except Exception:
                compensation_failures.append(step.name)
        return SagaResult(
            result=failure,
            state=state,
            completed=[s.name for s in completed],
            compensated=compensated,
            compensation_failures=compensation_failures,
            failed_step=failed_step,
        )
