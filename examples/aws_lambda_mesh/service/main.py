"""Lambda entry point for a service Lambda — ``SERVICE_NAME`` (env) picks the domain.

A managed Python runtime needs no custom bootstrap: ``to_lambda_handler`` wraps the app into the
``handler(event, context)`` callable Lambda invokes directly. Terraform points every one of the six
service functions' ``handler`` at ``service.main.handler`` (one shared zip — see ``deploy/main.tf``
and ``deploy/build_service.sh``), differing only in the ``SERVICE_NAME`` (and outbound target) env vars.

After each invocation, drains the trace exporter and pushes the batch into the mesh's
:class:`~benzene.mesh.S3TraceInbox` (when ``MESH_ARTIFACT_BUCKET`` is set) — best-effort, mirrors
``deploy/mesh/fleet/service.py``'s ``lambda_handler_for`` exactly (a Lambda only runs *during* an
invocation, so this pushes once per invocation rather than on a background loop a long-lived host would
use). A push never raises (every :class:`~benzene.core.MessageSender` in this port, including
:class:`~benzene.mesh.S3TraceInbox`, catches its own exceptions and returns a failed
:class:`~benzene.results.Result` instead), so a genuine failure (e.g. IAM denied, wrong bucket) is
logged rather than silently discarded: check this Lambda's CloudWatch logs for a "trace push failed"
warning before assuming a missing consumer edge means no traffic has flowed yet.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from benzene.aws import to_lambda_handler

from .host import ServiceLambda, build_service

logger = logging.getLogger(__name__)


def lambda_handler_for(service: ServiceLambda):
    """Return the ``handler(event, context)`` Lambda invokes, pushing traces after each invocation."""
    base = to_lambda_handler(service.app)

    def handler(event: dict[str, Any], context: Any = None) -> Any:
        result = base(event, context)
        if service.feeds is not None:
            events = service.exporter.drain()
            if events:
                try:
                    push = asyncio.run(service.feeds.publish_traces(events))
                except Exception:  # noqa: BLE001 - best-effort: never fails the request
                    logger.warning("trace push to the mesh's S3 inbox raised", exc_info=True)
                else:
                    if not push.is_successful:
                        logger.warning(
                            "trace push to the mesh's S3 inbox failed: %s (%s)",
                            push.status,
                            "; ".join(push.messages) or "no detail",
                        )
        return result

    return handler


handler = lambda_handler_for(build_service())
