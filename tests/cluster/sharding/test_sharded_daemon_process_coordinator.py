"""Pekko's scaladsl ShardedDaemonProcessSpec, "Coordinator" (126-183): the coordinator reads its
state from Distributed Data and keeps every worker alive with StartEntity, throttled. One joined
node on ManualTime, with a TestProbe standing in for the sharding region."""

from __future__ import annotations

from typing import Any

from ohra import ActorRef
from ohra.cluster.ddata import DistributedData, Replicator
from ohra.cluster.sharding import ClusterShardingSettings, StartEntity
from ohra.cluster.sharding.sharded_daemon_process import (
    ShardedDaemonProcessSettings,
    ShardedDaemonProcessState,
    ShardedDaemonProcessStateKey,
)
from ohra.cluster.sharding.sharded_daemon_process_coordinator import (
    sharded_daemon_process_coordinator,
)
from ohra.testkit import TestProbe

from .test_ddata_shard_coordinator import Node, manual_node

SETTINGS = ShardedDaemonProcessSettings(keep_alive_interval=1.0, keep_alive_throttle_interval=0.02)
"""Pekko's test config: ping often, start fast."""


def pinger(
    node: Node,
    name: str,
    sharding: TestProbe[Any],
    settings: ShardedDaemonProcessSettings = SETTINGS,
) -> ActorRef[Any]:
    behavior = sharded_daemon_process_coordinator(
        settings, ClusterShardingSettings(), 3, name, sharding.ref
    )
    return node.system._system_root.spawn_child(behavior, f"pinger-{name}", None).ref  # pyright: ignore[reportPrivateUsage]


async def test_keep_alive_uses_the_original_id_scheme_at_revision_0() -> None:
    async with manual_node() as node:
        assert node.manual is not None
        sharding = node.probe()
        pinger(node, "throttle-a", sharding)
        await sharding.expect_message(StartEntity("0"))
        await sharding.expect_message(StartEntity("1"))
        await sharding.expect_message(StartEntity("2"))
        await node.manual.time_passes(1.0)
        await sharding.expect_message(StartEntity("0"))


async def test_keep_alive_uses_the_new_scheme_at_a_revision_above_0() -> None:
    async with manual_node() as node:
        assert node.manual is not None
        name = "throttle-b"
        key = ShardedDaemonProcessStateKey(name)
        ddata = node.probe()
        DistributedData(node.system).replicator.tell(
            Replicator.Update(
                key,
                ShardedDaemonProcessState.initial_state(2),
                Replicator.WriteLocal(),
                ddata.ref,
                lambda s: s.start_scaling_to(3).complete_scaling(),
            )
        )
        await ddata.expect_message_type(Replicator.UpdateSuccess)

        sharding = node.probe()
        pinger(node, name, sharding)
        await sharding.expect_message(StartEntity("1|3|0"))
        await sharding.expect_message(StartEntity("1|3|1"))
        await sharding.expect_message(StartEntity("1|3|2"))
        await node.manual.time_passes(1.0)
        await sharding.expect_message(StartEntity("1|3|0"))


async def test_keep_alive_is_throttled() -> None:
    async with manual_node() as node:
        manual = node.manual
        assert manual is not None
        sharding = node.probe()
        pinger(node, "throttle-c", sharding, SETTINGS.with_keep_alive_throttle_interval(1.0))
        await sharding.expect_message(StartEntity("0"))
        for entity_id in ("1", "2", "0"):  # "0" comes a keep-alive interval after "2"
            await manual.expect_no_message_for(0.9, sharding)
            await manual.time_passes(0.1)
            await sharding.expect_message(StartEntity(entity_id))


async def test_an_unthrottled_keep_alive_sends_every_id_at_once() -> None:
    async with manual_node() as node:
        manual = node.manual
        assert manual is not None
        sharding = node.probe()
        pinger(node, "throttle-d", sharding, SETTINGS.with_keep_alive_throttle_interval(0.0))
        for entity_id in ("0", "1", "2"):
            await sharding.expect_message(StartEntity(entity_id))
        await manual.expect_no_message_for(0.9, sharding)
        await manual.time_passes(0.1)
        for entity_id in ("0", "1", "2"):
            await sharding.expect_message(StartEntity(entity_id))
