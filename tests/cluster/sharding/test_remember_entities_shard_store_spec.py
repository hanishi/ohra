"""Pekko's RememberEntitiesShardStoreSpec, as DDataRememberEntitiesShardStoreSpec: the shard store
on a one-node cluster with a real Replicator. Each test runs its own system, so the late request
writes its own entity first instead of reading what the previous case left."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest

from ohra import ActorRef, ActorSystem
from ohra.cluster.ddata import DistributedData, Replicator
from ohra.cluster.sharding import ClusterShardingSettings
from ohra.cluster.sharding.remember_entities import RememberEntitiesShardStore, ddata_shard_store
from ohra.testkit import TestKitSettings, TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node

Store = RememberEntitiesShardStore


@dataclass
class Node:
    system: ActorSystem[Any]
    replicator: ActorRef[Replicator.Command]
    settings: TestKitSettings = field(default_factory=TestKitSettings)

    def probe(self) -> TestProbe[Any]:
        return TestProbe[Any](self)

    def spawn(self, behavior: Any, name: str) -> ActorRef[Any]:
        return self.system._system_root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]

    def store(self, shard_id: str, type_name: str, name: str) -> ActorRef[Store.Command]:
        settings = ClusterShardingSettings()
        return self.spawn(
            ddata_shard_store(type_name, shard_id, self.replicator, settings, 1), name
        )


@asynccontextmanager
async def one_node() -> AsyncGenerator[Node]:
    """A one-node cluster, as the spec's atStartup forms."""
    port = free_port()
    async with cluster_node("re", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=3)
        yield Node(system, DistributedData(system).replicator)


async def remembered(store: ActorRef[Store.Command], probe: TestProbe[Any]) -> frozenset[str]:
    store.tell(Store.GetEntities(probe.ref))
    return (await probe.expect_message_type(Store.RememberedEntities, 3)).entities


def ids(first: int, last: int) -> frozenset[str]:
    return frozenset(str(i) for i in range(first, last + 1))


async def test_the_store_starts_and_stops_and_lists_remembered_entity_ids():
    async with one_node() as node:
        probe = node.probe()
        store = node.store("FakeShardId", "FakeTypeName", "store-1")
        assert await remembered(store, probe) == frozenset()

        for started, stopped in [
            (frozenset({"1", "2", "3"}), frozenset[str]()),
            (frozenset({"4", "5", "6"}), frozenset({"2", "3"})),
            (frozenset[str](), frozenset({"6"})),
            (frozenset({"2"}), frozenset[str]()),
        ]:
            store.tell(Store.Update(started, stopped, probe.ref))
            await probe.expect_message(Store.UpdateDone(started, stopped), 3)

        # the store does not support get after update
        incarnation2 = node.store("FakeShardId", "FakeTypeName", "store-2")
        assert await remembered(incarnation2, probe) == frozenset({"1", "2", "4", "5"})


async def test_a_late_request_is_answered(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    async with one_node() as node:
        probe = node.probe()
        store = node.store("FakeShardId", "FakeTypeName", "store-1")
        assert await remembered(store, probe) == frozenset()
        store.tell(Store.Update(frozenset({"1"}), frozenset(), probe.ref))
        await probe.expect_message(Store.UpdateDone(frozenset({"1"}), frozenset()), 3)

        caplog.clear()
        incarnation2 = node.store("FakeShardId", "FakeTypeName", "store-2")
        await eventually(
            lambda: "Got remembered entities, waiting for shard to request them" in caplog.text,
            timeout=3,
        )
        assert await remembered(incarnation2, probe) == frozenset({"1"})


async def test_a_large_batch():
    """The Scala stops the first incarnation before it starts the second; here it is left
    running, which changes nothing the test observes."""
    async with one_node() as node:
        probe = node.probe()
        store = node.store("FakeShardIdLarge", "FakeTypeNameLarge", "store-1")
        assert await remembered(store, probe) == frozenset()

        store.tell(Store.Update(ids(1, 1000), ids(1001, 2000), probe.ref))
        response = await probe.expect_message_type(Store.UpdateDone, 3)
        assert len(response.started) == 1000
        assert len(response.stopped) == 1000

        incarnation2 = node.store("FakeShardIdLarge", "FakeTypeNameLarge", "store-2")
        assert len(await remembered(incarnation2, probe)) == 1000
