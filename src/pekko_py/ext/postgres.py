"""PostgresStore: durable states in a Postgres table every node can reach, for persisted actors on a cluster.

    async with PostgresStore[Cart]("postgresql://user:pw@db/app") as store:
        cart = Behaviors.persisted(store)(cart_factory)

The contract is SqliteStore's: ``save`` commits only revision 1 for a new id, or exactly the stored revision
plus one, so two incarnations of one entity on two nodes cannot both write. The table follows Pekko's JDBC
durable state names (``durable_state``: ``persistence_id``, ``revision``, ``state_payload``), without the JVM
serializer columns. Requires the ``postgres`` extra.
"""
from __future__ import annotations

import pickle
import re
from collections.abc import Callable
from types import TracebackType
from typing import Self

from psycopg import AsyncConnection, sql
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool

from ..persistence import RevisionConflictError

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?")


class PostgresStore[S]:
    """A connection pool owned by ``async with``. The default codec is pickle: trusted storage only; pass
    (dumps, loads) for anything else. ``table`` may be schema-qualified, as in ``"app.durable_state"``."""

    def __init__(
        self,
        dsn: str,
        table: str = "durable_state",
        codec: tuple[Callable[[S], bytes], Callable[[bytes], S]] = (pickle.dumps, pickle.loads),
    ) -> None:
        if not _IDENTIFIER.fullmatch(table):
            raise ValueError(f"table must be a name or schema.name, not {table!r}")
        self._dsn = dsn
        self._table = sql.Identifier(*table.split("."))
        self._dumps, self._loads = codec
        self._pool: AsyncConnectionPool[AsyncConnection[TupleRow]] | None = None

    async def __aenter__(self) -> Self:
        pool = AsyncConnectionPool[AsyncConnection[TupleRow]](self._dsn, open=False, kwargs={"autocommit": True})
        await pool.open(wait=True)
        try:
            async with pool.connection() as conn, conn.transaction():
                # Concurrent CREATE TABLE IF NOT EXISTS can fail on the catalog's unique index when pods start
                # together; a transaction-scoped advisory lock lets one create while the others wait.
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext('pekko_py.durable_state'))")
                await conn.execute(
                    sql.SQL(
                        "CREATE TABLE IF NOT EXISTS {} (persistence_id TEXT PRIMARY KEY, "
                        "revision BIGINT NOT NULL, state_payload BYTEA NOT NULL)"
                    ).format(self._table)
                )
        except BaseException:
            await pool.close()
            raise
        self._pool = pool
        return self

    async def __aexit__(self, *_: type[BaseException] | BaseException | TracebackType | None) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            await pool.close()

    def _opened(self) -> AsyncConnectionPool[AsyncConnection[TupleRow]]:
        if self._pool is None:
            raise RuntimeError("PostgresStore is not open: use it inside `async with PostgresStore(...) as store`")
        return self._pool

    async def load(self, persistence_id: str) -> tuple[S, int] | None:
        async with self._opened().connection() as conn:
            cur = await conn.execute(
                sql.SQL("SELECT state_payload, revision FROM {} WHERE persistence_id = %s").format(self._table),
                (persistence_id,),
            )
            row = await cur.fetchone()
        return None if row is None else (self._loads(row[0]), row[1])

    async def save(self, persistence_id: str, state: S, revision: int) -> None:
        data = self._dumps(state)
        async with self._opened().connection() as conn:
            if revision == 1:
                cur = await conn.execute(
                    sql.SQL(
                        "INSERT INTO {} (persistence_id, revision, state_payload) VALUES (%s, 1, %s) "
                        "ON CONFLICT (persistence_id) DO NOTHING"
                    ).format(self._table),
                    (persistence_id, data),
                )
            else:
                cur = await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET revision = %s, state_payload = %s WHERE persistence_id = %s AND revision = %s"
                    ).format(self._table),
                    (revision, data, persistence_id, revision - 1),
                )
            if cur.rowcount != 1:
                raise RevisionConflictError(f"{persistence_id}: revision {revision} is not the next revision")

    async def delete(self, persistence_id: str) -> None:
        async with self._opened().connection() as conn:
            await conn.execute(sql.SQL("DELETE FROM {} WHERE persistence_id = %s").format(self._table), (persistence_id,))
