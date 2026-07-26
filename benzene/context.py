"""The pipeline context (core-concepts.md section 6).

A transport-neutral context carrying the resolved topic, the native request, headers, a slot for
the handler's result, and the per-invocation resolution scope. Concrete transport adapters may
subclass this to add invocation-scoped facts (a native call object, a deadline, …).
"""

from __future__ import annotations

from typing import Any

from .container import Scope
from .result import Result


class Context:
    def __init__(
        self,
        topic: str,
        request: Any,
        headers: dict[str, str] | None = None,
        scope: Scope | None = None,
        version: str = "",
    ) -> None:
        self.topic = topic
        self.version = version
        self.request = request
        # Headers are a flat, case-insensitive string→string channel; normalise keys to lower-case.
        self.headers: dict[str, str] = {k.lower(): v for k, v in (headers or {}).items()}
        self.scope = scope
        #: Set by the message-router middleware once the handler runs.
        self.result: Result[Any] | None = None
