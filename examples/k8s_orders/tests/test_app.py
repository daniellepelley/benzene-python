"""The multi-transport entry point actually builds — three named legs, no cloud, no broker, no server.

The one thing this example claims is that HTTP + SQS + Kafka run in one process off one composition
root. That claim is only worth anything if the entry point is executed, so this builds the real
``WorkerHost`` with the SDKs stubbed out (see ``conftest.py``) and then runs it to completion to
prove the legs really do wind each other down.
"""

from __future__ import annotations

import asyncio
from typing import Any

from benzene.core import StopSignal, WorkerHost


def test_the_entry_point_builds_all_three_transports_off_one_composition_root(
    stubbed_sdks: dict[str, Any],
) -> None:
    from k8s_orders.app import build_orders_worker_host

    host = build_orders_worker_host()

    assert isinstance(host, WorkerHost)
    assert host.names == ("http", "sqs", "kafka")
    assert stubbed_sdks["uvicorn_kwargs"]["port"] == 8081
    assert stubbed_sdks["kafka_topics"] == ["orders-in"]
    # The consumer builder, not the example, is what keeps at-least-once honest.
    assert stubbed_sdks["kafka_config"]["enable.auto.commit"] is False


def test_one_leg_stopping_winds_the_whole_process_down(stubbed_sdks: dict[str, Any]) -> None:
    from k8s_orders.app import build_orders_worker_host

    host = build_orders_worker_host()

    async def sigterm_after_a_moment(stop: StopSignal) -> None:
        await asyncio.sleep(0.02)
        stubbed_sdks["server"].should_exit = True  # stands in for uvicorn's SIGTERM handling

    host.add("fake-sigterm", sigterm_after_a_moment)
    asyncio.run(asyncio.wait_for(host.run(), timeout=10))  # returns => every leg wound down
    assert stubbed_sdks["kafka_closed"]  # the Kafka worker released its partition assignment
