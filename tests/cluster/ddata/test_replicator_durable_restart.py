"""The "work in single node cluster" case of Pekko's ``DurableDataSpec``, on one node: a
Replicator with a real SQLite store is stopped and a new one is started on the same directory."""

from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any

from ohra.cluster.ddata import GCounter, GCounterKey, Replicator, ReplicatorSettings
from ohra.cluster.ddata import sqlite_durable_store
from ohra.testkit import TestProbe

from .test_replicator_local import Node
from .test_replicator_subscriptions import manual_node

DURABLE = GCounterKey("durable-a")
PLAIN = GCounterKey("plain")


def settings(tmp_path: Path) -> ReplicatorSettings:
    return ReplicatorSettings(
        durable_keys=frozenset({"durable*"}),
        durable_store=partial(sqlite_durable_store, tmp_path / "store"),
    )


async def increment(node: Node, key: GCounterKey) -> None:
    probe: TestProbe[Any] = node.probe()
    node.replicator.tell(
        Replicator.Update(
            key,
            GCounter(),
            Replicator.WriteLocal(),
            probe.ref,
            lambda c: c.increment(node.self_address, 1),
        )
    )
    await probe.expect_message(Replicator.UpdateSuccess(key))


async def get(node: Node, key: GCounterKey) -> Any:
    probe: TestProbe[Any] = node.probe()
    node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), probe.ref))
    return await probe.receive_message()


async def test_a_durable_counter_survives_a_restart(tmp_path: Path) -> None:
    async with manual_node(settings(tmp_path)) as (node, _):
        for _ in range(3):
            await increment(node, DURABLE)
    async with manual_node(settings(tmp_path)) as (node, _):
        got = await get(node, DURABLE)
        assert isinstance(got, Replicator.GetSuccess)
        assert got.get(DURABLE).value == 3


async def test_a_deleted_key_stays_deleted_after_a_restart(tmp_path: Path) -> None:
    async with manual_node(settings(tmp_path)) as (node, _):
        await increment(node, DURABLE)
        probe: TestProbe[Any] = node.probe()
        node.replicator.tell(Replicator.Delete(DURABLE, Replicator.WriteLocal(), probe.ref))
        await probe.expect_message(Replicator.DeleteSuccess(DURABLE))
    async with manual_node(settings(tmp_path)) as (node, _):
        assert await get(node, DURABLE) == Replicator.GetDataDeleted(DURABLE)


async def test_a_non_durable_key_is_gone_after_a_restart(tmp_path: Path) -> None:
    s = replace(settings(tmp_path))
    async with manual_node(s) as (node, _):
        await increment(node, PLAIN)
    async with manual_node(s) as (node, _):
        assert await get(node, PLAIN) == Replicator.NotFound(PLAIN)
