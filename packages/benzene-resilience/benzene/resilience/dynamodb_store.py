"""A shared :class:`~benzene.resilience.IdempotencyStore` over DynamoDB — cross-instance dedupe.

The serverless half of the answer :mod:`~benzene.resilience.redis_store` gives: a Lambda-based
service has no connection pool to keep warm and usually already has a DynamoDB table, so a
conditional ``PutItem`` is the cheapest atomic conditional write it can reach.

The reservation is one call::

    PutItem  ConditionExpression = attribute_not_exists(#pk) OR expiresAt <= :now

DynamoDB evaluates that condition and writes the item as a single atomic operation on one item, so
of two invocations racing for the same key exactly one succeeds and the loser gets
``ConditionalCheckFailedException`` — which this store maps to ``False``, not to an error. A
``GetItem`` followed by a plain ``PutItem`` would reintroduce the race the middleware reserves
against and must never be written here.

A shared store **relocates** the race; it does not remove it. Handlers should still tolerate running
twice: a lapsed key, a throttled table, or a redelivery beyond ``ttl`` all end with a second run.

Two details are load-bearing and neither is guessable from the API:

* **A lapsed record reads as absent.** DynamoDB's TTL sweeper is best-effort and lags by up to 48
  hours, so a store that waited for the delete would leave an expired key unreclaimable for two
  days. :meth:`~DynamoDbIdempotencyStore.get` and the condition expression both compare ``expiresAt``
  against the clock themselves, and the record's physical presence is irrelevant.
* **Reads are strongly consistent.** An eventually-consistent read could miss the reservation
  another instance wrote a millisecond ago — which is precisely the read this store makes.

The item mirrors .NET's ``DynamoDbIdempotencyStore`` so a mixed-language fleet can share one table:
``pk`` (S), ``status`` (S, ``"InProgress"`` / ``"Completed"``), ``wasSuccessful`` (BOOL),
``expiresAt`` (N, epoch seconds), plus a Python-only ``result`` (S) that .NET ignores. Reading back a
record with no ``result`` — one a .NET service wrote — synthesises a Result from ``status`` and
``wasSuccessful`` rather than looking absent. The one deliberate divergence is ``<=`` where .NET
writes ``expiresAt < :now``: it makes reclaim and expiry the same instant, so this store, the Redis
store and :class:`~benzene.resilience.InMemoryIdempotencyStore` share one expiry rule. The
expression is per-request, never stored, so a shared table is unaffected.

The store **never creates the table**, and does not enable TTL on ``expiresAt`` — both are the
consumer's infrastructure, as with every Benzene store. ``boto3`` is an optional ``[dynamodb]``
extra imported lazily, so this module and its tests need no SDK and no AWS.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable
from typing import Any

from benzene.results import Result, Status

from .idempotency import IN_PROGRESS
from .result_codec import decode_result, encode_result

#: One day — comfortably past the redelivery window of every at-least-once transport this port binds.
DEFAULT_TTL = 86_400.0

#: The atomic reservation. ``attribute_not_exists`` claims a free key; the ``expiresAt`` arm reclaims
#: a lapsed one the TTL sweeper has not got to yet.
CONDITION_EXPRESSION = "attribute_not_exists(#pk) OR expiresAt <= :now"

_IN_PROGRESS = "InProgress"
_COMPLETED = "Completed"


def _boto3() -> Any:
    """Import ``boto3`` lazily, turning a missing optional dependency into a teaching error.

    A missing SDK is a *deployment* error, not a dedupe outcome — surfacing it as an ImportError
    naming the exact extra fails fast and says what to install, rather than leaving a service that
    has quietly stopped deduplicating.
    """
    try:
        import boto3  # noqa: PLC0415 - lazy: optional [dynamodb] extra
    except ImportError as exc:
        raise ImportError(
            "DynamoDbIdempotencyStore requires boto3 — install it with "
            "'pip install benzene-resilience[dynamodb]'."
        ) from exc
    return boto3


def _number(value: float) -> str:
    """A DynamoDB ``N`` attribute value — integral floats without a trailing ``.0``."""
    return str(int(value)) if float(value).is_integer() else repr(float(value))


class DynamoDbIdempotencyStore:
    """An :class:`~benzene.resilience.IdempotencyStore` over a DynamoDB table::

        from benzene.resilience import DynamoDbIdempotencyStore, idempotency_interception

        store = DynamoDbIdempotencyStore("orders-idempotency", ttl=3600)
        definition.middleware += [idempotency_interception(store)]

    The table needs a single partition key (``partition_key``, default ``pk``, a string) and, to
    stop growing, TTL enabled on ``expiresAt``. Pass ``client=`` an already-built ``boto3`` DynamoDB
    client to control the session or region — or, in tests, a duck-typed fake: only ``put_item`` /
    ``get_item`` / ``delete_item`` and ``client.exceptions.ConditionalCheckFailedException`` are
    used, and every blocking call goes through :func:`asyncio.to_thread` so a send never stalls an
    event loop an ASGI server is sharing.

    ``ttl`` (seconds, default one day) must **exceed the transport's maximum redelivery window** — a
    redelivery arriving after the key lapsed finds nothing and runs the handler again. It rounds up
    to whole epoch seconds, which is what DynamoDB's TTL attribute requires. ``None`` writes no
    ``expiresAt``, so nothing ever expires and the table grows without bound; pass it only for a
    table you prune yourself. ``clock`` is injectable (epoch seconds, UTC) so a test expires a
    record without sleeping.
    """

    def __init__(
        self,
        table_name: str,
        *,
        client: Any | None = None,
        ttl: float | None = DEFAULT_TTL,
        partition_key: str = "pk",
        clock: Callable[[], float] = time.time,
    ) -> None:
        if ttl is not None and ttl <= 0:
            raise ValueError("DynamoDbIdempotencyStore ttl must be positive (or None for no expiry)")
        self._table_name = table_name
        self._client = client
        self._ttl = ttl
        self._partition_key = partition_key
        self._clock = clock

    def _dynamodb(self) -> Any:
        if self._client is None:
            self._client = _boto3().client("dynamodb")
        return self._client

    async def get(self, key: str) -> Result | None:
        client = self._dynamodb()
        response = await asyncio.to_thread(
            client.get_item,
            TableName=self._table_name,
            Key={self._partition_key: {"S": key}},
            ConsistentRead=True,  # a stale read would miss the reservation just written elsewhere
        )
        item = response.get("Item")
        if item is None or self._is_lapsed(item):
            return None
        return self._result_of(item)

    async def put(self, key: str, result: Result) -> None:
        """Store ``result`` under ``key``, overwriting the reservation and restarting the TTL."""
        client = self._dynamodb()
        await asyncio.to_thread(
            client.put_item, TableName=self._table_name, Item=self._item(key, result)
        )

    async def put_if_absent(self, key: str, result: Result) -> bool:
        """Claim ``key`` with one conditional ``PutItem``; ``True`` when this caller won it.

        The failed condition is the *expected* outcome for a duplicate delivery, so it is mapped to
        ``False`` rather than raised; any other DynamoDB error (throttling, access denied) still
        propagates, because a store that cannot be reached must not be mistaken for a key that is
        already taken.
        """
        client = self._dynamodb()
        try:
            await asyncio.to_thread(
                client.put_item,
                TableName=self._table_name,
                Item=self._item(key, result),
                ConditionExpression=CONDITION_EXPRESSION,
                ExpressionAttributeNames={"#pk": self._partition_key},
                ExpressionAttributeValues={":now": {"N": _number(self._clock())}},
            )
        except Exception as exc:  # noqa: BLE001 - narrowed by _is_conditional_check_failure
            if _is_conditional_check_failure(client, exc):
                return False
            raise
        return True

    async def delete(self, key: str) -> None:
        client = self._dynamodb()
        await asyncio.to_thread(
            client.delete_item,
            TableName=self._table_name,
            Key={self._partition_key: {"S": key}},
        )

    def _item(self, key: str, result: Result) -> dict[str, dict[str, Any]]:
        """The stored record. ``status`` mirrors .NET's; ``result`` is this port's own replay data.

        The in-flight marker is recognised the way the middleware recognises it — by **equality**
        with :data:`~benzene.resilience.IN_PROGRESS`, never by identity — so a marker that has been
        through this codec once is still classified as a reservation.
        """
        item: dict[str, dict[str, Any]] = {
            self._partition_key: {"S": key},
            "status": {"S": _IN_PROGRESS if result == IN_PROGRESS else _COMPLETED},
            "wasSuccessful": {"BOOL": result.is_successful},
            "result": {"S": encode_result(result)},
        }
        if self._ttl is not None:
            # Whole epoch seconds, rounded up: DynamoDB's TTL attribute is defined in seconds, and
            # rounding down would forget a key sooner than the caller asked for.
            item["expiresAt"] = {"N": str(math.ceil(self._clock() + self._ttl))}
        return item

    def _is_lapsed(self, item: dict[str, Any]) -> bool:
        expires_at = item.get("expiresAt")
        if not isinstance(expires_at, dict) or "N" not in expires_at:
            return False  # no TTL attribute: the record never lapses on its own
        return float(expires_at["N"]) <= self._clock()

    def _result_of(self, item: dict[str, Any]) -> Result:
        """The stored Result, or one synthesised from a record another port wrote.

        A .NET service sharing this table writes ``status``/``wasSuccessful`` and no ``result``. Its
        record still has to read as "this key is taken" — treating it as absent would let the very
        duplicate the shared table exists to stop run the handler again.
        """
        stored = item.get("result")
        if isinstance(stored, dict) and isinstance(stored.get("S"), str):
            return decode_result(stored["S"])
        status = item.get("status", {}).get("S")
        if status == _IN_PROGRESS:
            return IN_PROGRESS
        if item.get("wasSuccessful", {}).get("BOOL", True):
            return Result.ok()
        return Result.failure(
            Status.UNEXPECTED_ERROR, "a peer recorded this idempotency key as a failed delivery"
        )


def _is_conditional_check_failure(client: Any, exc: Exception) -> bool:
    """Whether ``exc`` is DynamoDB's "the condition did not hold" — i.e. the key was already taken.

    ``client.exceptions.ConditionalCheckFailedException`` is generated per-session by botocore, so
    it is read off the *injected* client rather than imported; the error-code and class-name
    fallbacks keep a duck-typed fake (and a client built by another session) working.
    """
    expected = getattr(getattr(client, "exceptions", None), "ConditionalCheckFailedException", None)
    if isinstance(expected, type) and isinstance(exc, expected):
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
        if code == "ConditionalCheckFailedException":
            return True
    return type(exc).__name__ == "ConditionalCheckFailedException"
