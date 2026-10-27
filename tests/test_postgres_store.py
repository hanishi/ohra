r"""PostgresStore: the SqliteStore contract on a database every node can reach.

Runs against the server in OHRA_POSTGRES_DSN, for example
    docker run -d --rm -p 55432:5432 -e POSTGRES_PASSWORD=pg postgres:17
    OHRA_POSTGRES_DSN=postgresql://postgres:pg@localhost:55432/postgres \
        pytest tests/test_postgres_store.py
and is skipped without it.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import AsyncIterator

import pytest

from ohra import ActorSystem
from ohra.persistence import RevisionConflictError

from .test_persistence import Add, Cmd, Get, counter_on

DSN = os.environ.get("OHRA_POSTGRES_DSN", "")
pytestmark = pytest.mark.skipif(not DSN, reason="OHRA_POSTGRES_DSN is not set")

if DSN:
    import psycopg

    from ohra.ext.postgres import PostgresStore


@pytest.fixture
async def table() -> AsyncIterator[str]:
    """A fresh table per test, dropped afterwards."""
    name = f"durable_state_{uuid.uuid4().hex[:12]}"
    yield name
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {name}")


async def test_postgres_store_round_trip_and_conflicts(table: str):
    async with PostgresStore[int](DSN, table=table) as store:
        assert await store.load("a") is None
        with pytest.raises(RevisionConflictError):
            await store.save("a", 5, 2)  # nothing stored: the first revision is 1
        await store.save("a", 5, 1)
        assert await store.load("a") == (5, 1)
        with pytest.raises(RevisionConflictError):
            await store.save("a", 6, 1)
        await store.save("a", 6, 2)
        assert await store.load("a") == (6, 2)
        await store.delete("a")
        assert await store.load("a") is None
        await store.save("a", 7, 1)  # after a delete the id starts over
        assert await store.load("a") == (7, 1)


async def test_postgres_store_custom_codec(table: str):
    codec = (lambda s: json.dumps(s).encode(), lambda b: json.loads(b))
    async with PostgresStore[dict[str, int]](DSN, table=table, codec=codec) as store:
        await store.save("a", {"x": 1}, 1)
        assert await store.load("a") == ({"x": 1}, 1)
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        cur = await conn.execute(f"SELECT state_payload FROM {table} WHERE persistence_id = 'a'")
        assert await cur.fetchone() == (b'{"x": 1}',)


async def test_two_nodes_racing_for_the_first_revision_one_wins(table: str):
    # Two incarnations of one entity, on two nodes, both writing revision 1: exactly one commits.
    async with (
        PostgresStore[int](DSN, table=table) as a,
        PostgresStore[int](DSN, table=table) as b,
    ):
        results = await asyncio.gather(
            a.save("e", 1, 1), b.save("e", 2, 1), return_exceptions=True
        )
        assert sorted(type(r).__name__ for r in results) == ["NoneType", "RevisionConflictError"]
        state, revision = await a.load("e") or (None, 0)
        assert revision == 1 and state in (1, 2)


async def test_state_survives_a_restart_on_another_node(table: str):
    log: list[str] = []

    def system_on(store: PostgresStore[int]) -> ActorSystem[Cmd]:
        return ActorSystem(counter_on(store, log).recover("c", empty=0), "s")

    async with PostgresStore[int](DSN, table=table) as store, system_on(store) as system:
        system.tell(Add(4))
        assert await system.ask(Get, timeout=1) == 4
    # a new pool, as on another pod
    async with PostgresStore[int](DSN, table=table) as store, system_on(store) as system:
        assert await system.ask(Get, timeout=1) == 4


async def test_postgres_store_must_be_opened_first(table: str):
    store = PostgresStore[int](DSN, table=table)
    with pytest.raises(RuntimeError, match="async with"):
        await store.load("a")


def test_postgres_store_rejects_a_table_name_that_is_not_an_identifier():
    with pytest.raises(ValueError):
        PostgresStore[int](DSN, table="state; DROP TABLE users")


async def test_nodes_starting_together_create_the_table_once(table: str):
    # Pods starting at once: concurrent CREATE TABLE IF NOT EXISTS can fail in Postgres on the
    # catalog's unique index.
    stores = [PostgresStore[int](DSN, table=table) for _ in range(8)]
    try:
        await asyncio.gather(*(s.__aenter__() for s in stores))
        await stores[0].save("a", 1, 1)
        assert await stores[-1].load("a") == (1, 1)
    finally:
        await asyncio.gather(*(s.__aexit__(None, None, None) for s in stores))


async def test_the_store_reconnects_after_the_database_restarts(table: str):
    # A restarted server leaves the pool holding closed connections; the next calls must still
    # succeed.
    async with PostgresStore[int](DSN, table=table) as store:
        await store.save("a", 1, 1)
        async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as admin:
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> "
                "pg_backend_pid() AND datname = current_database()"
            )
        started = time.monotonic()
        for revision in range(2, 8):  # more calls than the pool has connections
            assert await store.load("a") == (revision - 1, revision - 1)
            await store.save("a", revision, revision)
        # every broken connection dropped at once, not one per backoff step
        assert time.monotonic() - started < 2
