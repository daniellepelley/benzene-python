"""Azure Functions entry point for the mesh Function App — a single **Timer trigger** running the
discover -> interrogate -> publish pass (``discovery_service.run_mesh_aggregation``) on a schedule
(default every 5 minutes, matching ``examples/aws_lambda_mesh``'s EventBridge schedule; see
``deploy/main.tf``'s ``var.aggregate_schedule``). This Function App is deliberately **not** tagged for
discovery (``deploy/main.tf``), so it never interrogates itself.

Local run:  ``MESH_SUBSCRIPTION_ID=... MESH_BLOB_ACCOUNT_URL=... func start`` (needs Azure credentials
the ARM client can use — e.g. ``az login`` locally — for real discovery + live interrogation; like the
sibling mesh examples, the value of a local run is that the whole thing builds and wires up).
"""

from __future__ import annotations

import logging

import azure.functions as func

from .host import run_aggregation_from_env

app = func.FunctionApp()


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=False)
def aggregate(timer: func.TimerRequest) -> None:
    summary = run_aggregation_from_env()
    logging.info("mesh aggregation: discovered %d service(s)", summary.discovered)
