"""Regenerates ``generated/payments_capture_client.py`` from ``contracts/payments.spec.json``.

Run this whenever the contract changes (see ``docs/codegen-client.md``'s build-integration note):

    python examples/orders_payments_client/generate.py

CI runs this and then ``git diff --exit-code`` on the ``generated/`` directory, so a contract change
whose regenerated client wasn't committed fails the build rather than drifting silently.
"""

from __future__ import annotations

import json
from pathlib import Path

from benzene.codegen_client import generate_topic_client, parse_document

_HERE = Path(__file__).resolve().parent
_SPEC = _HERE / "contracts" / "payments.spec.json"
_OUT = _HERE / "generated" / "payments_capture_client.py"


def main() -> None:
    document = parse_document(json.loads(_SPEC.read_text()))
    generated = generate_topic_client(document, topic="payments:capture")
    _OUT.write_text(generated.source)
    print(f"Wrote {_OUT} (contractHash={generated.contract_hash})")


if __name__ == "__main__":
    main()
