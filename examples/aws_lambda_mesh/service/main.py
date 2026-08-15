"""Lambda entry point for a service Lambda — ``SERVICE_NAME`` (env) picks the domain.

A managed Python runtime needs no custom bootstrap: ``to_lambda_handler`` returns the
``handler(event, context)`` callable Lambda invokes directly. Terraform points every one of the six
service functions' ``handler`` at ``service.main.handler`` (one shared zip — see ``deploy/main.tf``
and ``deploy/build_service.sh``), differing only in the ``SERVICE_NAME`` (and outbound target) env vars.
"""

from __future__ import annotations

from benzene.aws import to_lambda_handler

from .host import build_service_app

handler = to_lambda_handler(build_service_app())
