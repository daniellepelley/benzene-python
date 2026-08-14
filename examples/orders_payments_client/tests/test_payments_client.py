"""Dogfoods the generated ``payments:capture`` client against a real .NET-produced Contract Document.

Proves ingress -> generated client -> egress the same way the cloud examples' tests dogfood the
real pipeline and fake only the outbound edge (port-quality-standards §4): the generated method
calls through to :class:`FakeMessageSender`, which records exactly what was sent.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from benzene.codegen_client import compute_contract_hash, generate_service_client, parse_document
from benzene.codegen_client.document import ContractDocument, to_raw_document
from benzene.codegen_client.schema_closure import reachable_schemas
from benzene.testing import FakeMessageSender

from orders_payments_client.generated.payments_capture_client import (
    CONTRACT_HASH,
    REQUIRED_TOPICS,
    CapturePayment,
    PaymentDto,
    create_payments_capture_client,
)

_SPEC_PATH = Path(__file__).resolve().parent.parent / "contracts" / "payments.spec.json"


def _document():
    return parse_document(json.loads(_SPEC_PATH.read_text()))


def test_required_topics_is_exactly_the_one_topic() -> None:
    assert REQUIRED_TOPICS == ("payments:capture",)


def test_calling_the_generated_method_sends_the_typed_payload() -> None:
    sender = FakeMessageSender()
    client = create_payments_capture_client(sender)

    request = CapturePayment(order_id="ord-1", amount=42.42, currency="GBP")
    result = asyncio.run(client.capture_payments(request))

    assert result.is_successful
    assert sender.last_topic == "payments:capture"
    assert sender.last_message is request  # the typed dataclass instance was sent, unmodified
    assert isinstance(sender.last_message, CapturePayment)


def test_contract_hash_matches_the_expected_topic_scoped_projection() -> None:
    # Recomputed independently from the same source document (not read back off the generated
    # module) so this proves the embedded CONTRACT_HASH is correct, not merely self-consistent.
    document = _document()
    request = document.find_request("payments:capture")
    assert request is not None

    closure = reachable_schemas(document.schemas, request.request, request.response)
    projected = ContractDocument(
        openapi=document.openapi,
        info=document.info,
        requests=(request,),
        events=(),
        schemas=closure,
        message_endpoint=document.message_endpoint,
        transports=document.transports,
    )
    expected_hash = compute_contract_hash(to_raw_document(projected), topic_scoped=True)

    assert CONTRACT_HASH == expected_hash


def test_no_reserved_benzene_topic_appears_anywhere_in_the_generated_client() -> None:
    source = (Path(__file__).resolve().parent.parent / "generated" / "payments_capture_client.py").read_text()
    assert "benzene:" not in source


def test_no_reserved_benzene_topic_appears_in_a_whole_service_client_either() -> None:
    # payments.spec.json's requests[] includes benzene:spec (reserved: true) alongside the two
    # domain topics — the domain-only default (contract-document.md §5.1) must exclude it even
    # from a service-level (not topic-scoped) generation over the whole document.
    document = _document()
    generated = generate_service_client(document, service_name="Payments")
    assert "benzene:" not in generated.source
    assert "benzene:spec" not in generated.required_topics
    assert set(generated.required_topics) == {"payments:capture", "payments:get-all"}


@pytest.mark.parametrize("field_name,response_type", [("order_id", str), ("amount", float)])
def test_response_type_is_the_generated_payment_dto(field_name: str, response_type: type) -> None:
    dto = PaymentDto(id="pay-1", order_id="ord-1", amount=42.42, currency="GBP", status="captured")
    assert isinstance(getattr(dto, field_name), response_type)
