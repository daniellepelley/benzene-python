"""The relational store — the natural home of "in the same transaction as my state write".

This is **not** a database abstraction, and deliberately so: the capability matrix's standing "no" to
any state-store abstraction applies here too. Nothing in this module opens a transaction, commits,
rolls back, creates a table or chooses a driver. It knows one table's shape and issues plain SQL
against a connection *you* hand it — which is exactly what makes the transactional story true rather
than magical: :class:`SqlOutboxStage` inserts the envelope on your connection, inside your
transaction, and your ``commit()`` is what makes both the state write and the send real.

**What it talks to.** Anything with SQLAlchemy 2.x's async shape — ``async with engine.begin() as
conn`` and ``await conn.execute(statement, parameters)`` — so asyncpg, psycopg, aiosqlite and
aiomysql all work without this package depending on any of them. That shape is small enough that a
test can implement it over stdlib ``sqlite3`` in thirty lines, which is how this module's own tests
run with no SQLAlchemy installed at all.

**The DDL is yours.** :data:`CREATE_TABLE_SQL` (or :func:`create_table_sql` for a non-default table
name) is exported for your migration tool to run. The store never migrates anything: silently
creating tables in someone's production database is not a framework's business.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .envelope import OutboxEnvelope, OutboxStatus

#: The default table name. Override it per-store with ``table=``; the DDL follows via
#: :func:`create_table_sql`.
DEFAULT_TABLE = "benzene_outbox"

_COLUMNS = (
    "id, topic, payload, headers, created_at, attempt_count, next_attempt_at, status, "
    "last_error, dispatched_at"
)

_INSERT = (
    "INSERT INTO {table} (id, topic, payload, headers, created_at, attempt_count, "
    "next_attempt_at, status, last_error, lease_until, dispatched_at) VALUES "
    "(:id, :topic, :payload, :headers, :created_at, :attempt_count, :next_attempt_at, :status, "
    ":last_error, NULL, NULL)"
)

#: The claim: one conditional ``UPDATE`` whose ``rowcount`` *is* the decision — pending, due, and
#: nobody else's live lease. There is no read-then-write to race, so no optimistic-concurrency
#: retry loop is needed on top.
_CLAIM = (
    "UPDATE {table} SET lease_until = :lease WHERE id = :id AND status = 'pending' "
    "AND (next_attempt_at IS NULL OR next_attempt_at <= :now) "
    "AND (lease_until IS NULL OR lease_until <= :now)"
)


def create_table_sql(table: str = DEFAULT_TABLE) -> str:
    """The DDL for the outbox table (and its due-scan index), for your own migration tool.

    Types are chosen to be portable rather than clever: timestamps are epoch seconds as
    ``DOUBLE PRECISION`` (no timezone to get wrong, and the same value the injectable clock hands
    out), and everything else is text. The index on ``(status, next_attempt_at)`` is what keeps the
    due-scan cheap once the table has a retention window's worth of dispatched rows in it.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    id              TEXT NOT NULL PRIMARY KEY,
    topic           TEXT NOT NULL,
    payload         TEXT NOT NULL,
    headers         TEXT NOT NULL,
    created_at      DOUBLE PRECISION NOT NULL,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at DOUBLE PRECISION,
    status          TEXT NOT NULL DEFAULT 'pending',
    last_error      TEXT,
    lease_until     DOUBLE PRECISION,
    dispatched_at   DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS ix_{table}_due ON {table} (status, next_attempt_at);
""".strip()


#: The DDL for the default table name — ``engine.execute(CREATE_TABLE_SQL)`` in your migration.
CREATE_TABLE_SQL = create_table_sql()


def _default_statement() -> Callable[[str], Any]:
    """How raw SQL is handed to the connection.

    SQLAlchemy requires textual SQL to be wrapped in ``sqlalchemy.text``; a plain DB-API-shaped
    connection wants the string itself. Resolving it here keeps both callers working with no
    configuration, and keeps SQLAlchemy an optional extra rather than an import this module needs.
    """
    try:
        from sqlalchemy import text  # noqa: PLC0415 - lazy: optional [sql] extra
    except ImportError:
        return lambda sql: sql
    resolved: Callable[[str], Any] = text
    return resolved


def _row_to_envelope(row: Sequence[Any]) -> OutboxEnvelope:
    """One ``_COLUMNS`` row (a SQLAlchemy ``Row`` or a DB-API tuple) as an envelope."""
    return OutboxEnvelope(
        id=str(row[0]),
        topic=str(row[1]),
        payload=str(row[2]),
        headers=json.loads(row[3]) if row[3] else {},
        created_at=float(row[4]),
        attempt_count=int(row[5]),
        next_attempt_at=None if row[6] is None else float(row[6]),
        status=OutboxStatus(str(row[7])),
        last_error=None if row[8] is None else str(row[8]),
        dispatched_at=None if row[9] is None else float(row[9]),
    )


def _insert_parameters(envelope: OutboxEnvelope) -> dict[str, Any]:
    return {
        "id": envelope.id,
        "topic": envelope.topic,
        "payload": envelope.payload,
        "headers": json.dumps(dict(envelope.headers)),
        "created_at": envelope.created_at,
        "attempt_count": envelope.attempt_count,
        "next_attempt_at": envelope.next_attempt_at,
        "status": OutboxStatus(envelope.status).value,
        "last_error": envelope.last_error,
    }


class SqlOutboxStage:
    """Stages envelopes onto **your** connection, inside **your** transaction. Never commits.

    This is the whole transactional story, and it is deliberately this small::

        connection = await engine.connect()          # yours
        async with connection.begin():               # your transaction
            await connection.execute(insert_order)   # your state write
            async with outbox_transaction(stage=SqlOutboxStage(connection)):
                await sender.send_message("orders:placed", order)   # captured, same transaction

    The ``INSERT`` for the envelope lands in the transaction you already had open, so the state write
    and the recorded send commit together or roll back together. Benzene never begins, commits or
    rolls anything back, and has no opinion about the rest of your schema — the only thing shared
    between your data and the outbox is the connection you passed in.
    """

    def __init__(
        self,
        connection: Any,
        *,
        table: str = DEFAULT_TABLE,
        statement: Callable[[str], Any] | None = None,
    ) -> None:
        self._connection = connection
        self._statement = statement or _default_statement()
        self._insert = _INSERT.format(table=table)

    async def stage(self, envelope: OutboxEnvelope) -> None:
        await self._connection.execute(
            self._statement(self._insert), _insert_parameters(envelope)
        )


class SqlOutboxStore:
    """An :class:`~benzene.outbox.OutboxStore` over one relational table.

    ``engine`` is anything exposing SQLAlchemy 2.x's ``async with engine.begin() as conn`` /
    ``await conn.execute(statement, parameters)`` — inject your application's own ``AsyncEngine``, or
    build one from a URL with :meth:`from_url`. ``clock`` is injectable so a test drives leases,
    backoff and retention without sleeping.

    Every claim is a single conditional ``UPDATE`` (see :data:`_CLAIM`), so two dispatchers over one
    database cannot both take an envelope — the database, not this code, is the arbiter.
    """

    def __init__(
        self,
        engine: Any,
        *,
        table: str = DEFAULT_TABLE,
        clock: Callable[[], float] = time.time,
        statement: Callable[[str], Any] | None = None,
    ) -> None:
        self._engine = engine
        self._table = table
        self._clock = clock
        self._statement = statement or _default_statement()

    @classmethod
    def from_url(cls, url: str, **options: Any) -> SqlOutboxStore:
        """Build a store over a SQLAlchemy async engine for ``url`` (needs the ``[sql]`` extra)."""
        try:
            from sqlalchemy.ext.asyncio import (  # noqa: PLC0415 - lazy: optional [sql] extra
                create_async_engine,
            )
        except ImportError as exc:
            raise ImportError(
                "SqlOutboxStore.from_url requires SQLAlchemy — install it with "
                "'pip install benzene-outbox[sql]', or pass an engine you built yourself."
            ) from exc
        return cls(create_async_engine(url), **options)

    async def add(self, envelopes: Sequence[OutboxEnvelope]) -> None:
        if not envelopes:
            return
        sql = self._statement(_INSERT.format(table=self._table))
        async with self._engine.begin() as connection:
            for envelope in envelopes:
                await connection.execute(sql, _insert_parameters(envelope))

    async def claim_due(self, batch_size: int, lease: float) -> list[OutboxEnvelope]:
        now = self._clock()
        select = self._statement(
            f"SELECT id FROM {self._table} WHERE status = 'pending' "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= :now) "
            "AND (lease_until IS NULL OR lease_until <= :now) "
            "ORDER BY created_at LIMIT :limit"
        )
        async with self._engine.begin() as connection:
            result = await connection.execute(select, {"now": now, "limit": max(0, batch_size)})
            candidates = [str(row[0]) for row in result.fetchall()]
        # Selecting is not claiming: each candidate still has to win the conditional UPDATE, so a
        # racing dispatcher that got there first simply drops out of this batch.
        claimed = []
        for envelope_id in candidates:
            envelope = await self.claim(envelope_id, lease)
            if envelope is not None:
                claimed.append(envelope)
        return claimed

    async def claim(self, envelope_id: str, lease: float) -> OutboxEnvelope | None:
        now = self._clock()
        async with self._engine.begin() as connection:
            claim = await connection.execute(
                self._statement(_CLAIM.format(table=self._table)),
                {"id": envelope_id, "lease": now + lease, "now": now},
            )
            if claim.rowcount != 1:
                return None
            return await self._select(connection, envelope_id)

    async def get(self, envelope_id: str) -> OutboxEnvelope | None:
        async with self._engine.begin() as connection:
            return await self._select(connection, envelope_id)

    async def mark_dispatched(self, envelope_id: str) -> None:
        await self._settle(
            "SET status = 'dispatched', dispatched_at = :now, lease_until = NULL",
            {"id": envelope_id, "now": self._clock()},
        )

    async def reschedule(
        self, envelope_id: str, attempt_count: int, delay: float, error: str
    ) -> None:
        await self._settle(
            "SET status = 'pending', attempt_count = :attempt_count, "
            "next_attempt_at = :next_attempt_at, last_error = :last_error, lease_until = NULL",
            {
                "id": envelope_id,
                "attempt_count": attempt_count,
                "next_attempt_at": self._clock() + delay,
                "last_error": error,
            },
        )

    async def park(self, envelope_id: str, error: str) -> None:
        await self._settle(
            "SET status = 'parked', last_error = :last_error, lease_until = NULL",
            {"id": envelope_id, "last_error": error},
        )

    async def delete_dispatched_before(self, cutoff: float) -> int:
        """Delete dispatched rows past retention. ``parked`` rows are never in scope of this DELETE."""
        sql = self._statement(
            f"DELETE FROM {self._table} WHERE status = 'dispatched' "
            "AND dispatched_at IS NOT NULL AND dispatched_at <= :cutoff"
        )
        async with self._engine.begin() as connection:
            result = await connection.execute(sql, {"cutoff": cutoff})
            return max(0, int(result.rowcount))

    async def _select(self, connection: Any, envelope_id: str) -> OutboxEnvelope | None:
        result = await connection.execute(
            self._statement(f"SELECT {_COLUMNS} FROM {self._table} WHERE id = :id"),
            {"id": envelope_id},
        )
        rows = result.fetchall()
        return _row_to_envelope(rows[0]) if rows else None

    async def _settle(self, assignment: str, parameters: Mapping[str, Any]) -> None:
        """A lifecycle transition. An id that no longer exists updates no rows — a no-op, as specified."""
        sql = self._statement(f"UPDATE {self._table} {assignment} WHERE id = :id")
        async with self._engine.begin() as connection:
            await connection.execute(sql, dict(parameters))
