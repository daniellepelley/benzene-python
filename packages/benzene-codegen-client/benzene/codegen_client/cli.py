"""``benzene-codegen`` — the console-script entry point (parity checklist row 12).

    benzene-codegen service --spec payments.spec.json --service Payments --out payments_client.py
    benzene-codegen topic   --spec payments.spec.json --topic payments:capture --out payments_capture_client.py

Exits non-zero on an unknown flag (argparse's own behaviour), an unknown topic (§5.2 fail-loud), or
an unparseable Contract Document.

**Registration/wiring (parity checklist row 9):** this port has no DI-container convention the way
.NET's ``Add{Service}ServiceClient()`` extension has one to hook into (``dependencies.py`` is a
hand-rolled ``Container``/``Scope``, composition-root style, with no reflective registration). The
generated ``create_{name}_client(sender)`` factory function is the idiomatic equivalent: a caller's
own ``configure_services`` does ``services.try_add_singleton(FooClient, lambda scope:
create_foo_client(scope.get_service(MessageSender)))``, exactly like every other collaborator in
this port's composition root — see ``examples/orders_payments_client``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .document import ContractDocument, ContractDocumentError, parse_document
from .generator import (
    GeneratedClient,
    TopicNotFoundError,
    generate_service_client,
    generate_topic_client,
)
from .topic_scope import UnknownTopicsError


def _load_document(spec_path: str) -> ContractDocument:
    try:
        raw = json.loads(Path(spec_path).read_text())
    except FileNotFoundError as exc:
        raise SystemExit(f"benzene-codegen: no such file: {spec_path!r}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"benzene-codegen: {spec_path!r} is not valid JSON: {exc}") from exc
    try:
        return parse_document(raw)
    except ContractDocumentError as exc:
        raise SystemExit(
            f"benzene-codegen: {spec_path!r} is not a valid Contract Document: {exc}"
        ) from exc


def _write(out_path: str | None, generated: GeneratedClient) -> None:
    if out_path is None:
        sys.stdout.write(generated.source)
        return
    Path(out_path).write_text(generated.source)
    print(
        f"Wrote {out_path} — {generated.class_name} "
        f"(required topics: {', '.join(generated.required_topics)}; contractHash={generated.contract_hash})"
    )


def _cmd_service(args: argparse.Namespace) -> int:
    document = _load_document(args.spec)
    topics = tuple(t.strip() for t in args.topics.split(",")) if args.topics else None
    try:
        generated = generate_service_client(
            document,
            service_name=args.service,
            topics=topics,
            include_reserved=args.include_reserved,
        )
    except UnknownTopicsError as exc:
        print(f"benzene-codegen: {exc}", file=sys.stderr)
        return 1
    _write(args.out, generated)
    return 0


def _cmd_topic(args: argparse.Namespace) -> int:
    document = _load_document(args.spec)
    try:
        generated = generate_topic_client(document, topic=args.topic)
    except TopicNotFoundError as exc:
        print(f"benzene-codegen: {exc}", file=sys.stderr)
        return 1
    _write(args.out, generated)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="benzene-codegen",
        description="Generate a typed Python client from a Benzene Contract Document (.spec.json).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    service = subparsers.add_parser("service", help="Generate a service-level client (one class, one method per topic).")
    service.add_argument("--spec", required=True, help="Path to the Contract Document (.spec.json).")
    service.add_argument("--service", required=True, help="Service name, used exactly for the client class name.")
    service.add_argument("--topics", default=None, help="Comma-separated topic include-list (default: every domain topic).")
    service.add_argument("--include-reserved", action="store_true", help="Include reserved benzene:* topics in the default scope.")
    service.add_argument("--out", default=None, help="Output file path (default: stdout).")
    service.set_defaults(func=_cmd_service)

    topic = subparsers.add_parser("topic", help="Generate a self-contained client for exactly one topic.")
    topic.add_argument("--spec", required=True, help="Path to the Contract Document (.spec.json).")
    topic.add_argument("--topic", required=True, help="The topic id to generate a client for.")
    topic.add_argument("--out", default=None, help="Output file path (default: stdout).")
    topic.set_defaults(func=_cmd_topic)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    exit_code: int = args.func(args)
    return exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
