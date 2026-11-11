"""Full cluster shutdown in sharding: ohra's own tests of B6. While the cluster prepares for a
full shutdown, a shard hands off at once (Shard.scala 474, 498-505, 546-553, 879-883), a region
acks no ``BeginHandOff`` and stops at its graceful shutdown without handing off
(ShardRegion.scala 655, 801-805, 855-868, 926-933), and the coordinator stops rebalancing
(ShardCoordinator.scala 695, 828, 949-956). The multi-node spec is ported in
test_cluster_sharding_preparing_for_shutdown_spec.py.

Every test runs on ``manual_node()``. Each docstring says whether the member events are told
directly or come from ``Cluster.prepare_for_full_cluster_shutdown()``.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

import pytest

from ohra import (
    ActorRef,
    CurrentShardRegionState,
    Entity,
    GetShardRegionState,
    ShardingEnvelope,
)
from ohra.cluster.core import Cluster, MemberPreparingForShutdown, MemberReadyForShutdown
from ohra.cluster.ddata import Replicator
from ohra.cluster.sharding.coordinator import (
    BeginHandOff,
    CoordinatorState,
    HostShard,
    ShardHomeAllocated,
    ShardRegionRegistered,
    ShardStarted,
)
from ohra.cluster.sharding.region import (
    _ExitingProgress,  # pyright: ignore[reportPrivateUsage]
    _GracefulShutdown,  # pyright: ignore[reportPrivateUsage]
    region_behavior,
)
from ohra.cluster.sharding.shard import (
    ClusterShardingSettings,
    HandOff,
    ShardInitialized,
    ShardStopped,
    shard_behavior,
)
from ohra.testkit import FishingOutcomes, TestProbe

from ...util import eventually
from .test_ddata_shard_coordinator import (
    SETTINGS,
    Node,
    active,
    expect_get,
    manual_node,
    registered,
    settle,
)
from .test_shard import Add, Cmd, Counter, Halt, counter


def ignoring_halt(events: TestProbe[str]) -> Entity[Cmd]:
    """A counter that reports its stop message ``Halt`` and ignores it."""
    return Entity(Counter, counter(events.ref, halts=False, reports_start=True)).with_stop_message(
        Halt()
    )


def spawn(n: Node, behavior: Any, name: str) -> ActorRef[Any]:
    return n.system._system_root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]


async def hands_off_at_once(n: Node, shard: ActorRef[Any], events: TestProbe[str]) -> None:
    """``HandOff`` gets ``ShardStopped`` before any time passes, though the entity ignores its
    stop message, and the shard stops with its entity. The shard stops its children at once, so
    whether the entity reads the stop message first is a race, as in Pekko."""
    stopped = n.probe()
    shard.tell(HandOff("7", stopped.ref))
    await stopped.expect_message(ShardStopped("7"))
    await stopped.expect_terminated(shard)
    await events.fish_for_message(
        3,
        lambda m: (
            FishingOutcomes.complete()
            if m == "stopping a"
            else FishingOutcomes.continue_and_ignore()
        ),
    )


async def test_a_shard_preparing_for_shutdown_hands_off_at_once(
    caplog: pytest.LogCaptureFixture,
):
    """The events come from ``prepare_for_full_cluster_shutdown()``."""
    caplog.set_level(logging.INFO)
    async with manual_node() as n:
        events, region = n.probe(), n.probe()
        shard = spawn(n, shard_behavior(ignoring_halt(events), "7", region.ref), "shard")
        await region.expect_message(ShardInitialized("7"))
        shard.tell(ShardingEnvelope[Cmd]("a", Add(1)))
        await events.expect_message("started a")
        Cluster(n.system).prepare_for_full_cluster_shutdown()
        await eventually(lambda: "Counter: Preparing for shutdown" in caplog.messages)
        await hands_off_at_once(n, shard, events)
        assert "Counter: HandOff shard [7] while preparing for shutdown. Stopping right away." in (
            caplog.messages
        )


async def test_a_shard_learns_of_an_earlier_preparation_from_the_replayed_state(
    caplog: pytest.LogCaptureFixture,
):
    """The events come from ``prepare_for_full_cluster_shutdown()``, before the shard starts: its
    subscription replays the member as ``MemberReadyForShutdown``."""
    caplog.set_level(logging.INFO)
    async with manual_node() as n:
        assert n.manual is not None
        cluster = Cluster(n.system)
        cluster.prepare_for_full_cluster_shutdown()
        while cluster.self_member.status != "ready_for_shutdown":
            await n.manual.time_passes(0.02)
        events, region = n.probe(), n.probe()
        shard = spawn(n, shard_behavior(ignoring_halt(events), "7", region.ref), "shard")
        await region.expect_message(ShardInitialized("7"))
        shard.tell(ShardingEnvelope[Cmd]("a", Add(1)))
        await events.expect_message("started a")
        await eventually(lambda: "Counter: Preparing for shutdown" in caplog.messages)
        await hands_off_at_once(n, shard, events)


async def test_a_region_preparing_for_shutdown_does_not_ack_begin_hand_off(
    caplog: pytest.LogCaptureFixture,
):
    """The events come from ``prepare_for_full_cluster_shutdown()``. The region answers a
    ``GetShardRegionState`` sent after the ``BeginHandOff``, so an ack would have come first."""
    caplog.set_level(logging.INFO)
    async with manual_node() as n:
        events, probe = n.probe(), n.probe()
        settings = ClusterShardingSettings()
        entity = ignoring_halt(events).with_settings(settings)
        region = spawn(n, region_behavior(entity, settings, _ExitingProgress()), "Counter")
        Cluster(n.system).prepare_for_full_cluster_shutdown()
        await eventually(lambda: "Counter. preparing for shutdown" in caplog.messages)
        region.tell(BeginHandOff("1", probe.ref))
        region.tell(GetShardRegionState(probe.ref))
        assert await probe.receive_message() == CurrentShardRegionState(frozenset())


async def test_a_region_preparing_for_shutdown_stops_at_graceful_shutdown(
    caplog: pytest.LogCaptureFixture,
):
    """The events are told directly, both of them, and the region logs once. It hosts a shard,
    so without the preparation it would wait for the coordinator to hand it off."""
    caplog.set_level(logging.INFO)
    async with manual_node() as n:
        events, probe = n.probe(), n.probe()
        settings = ClusterShardingSettings()
        entity = ignoring_halt(events).with_settings(settings)
        progress = _ExitingProgress()
        region = spawn(n, region_behavior(entity, settings, progress), "Counter")
        region.tell(HostShard("1", probe.ref))
        await probe.expect_message(ShardStarted("1"))
        me = Cluster(n.system).self_member
        region.tell(MemberPreparingForShutdown(me))
        region.tell(MemberReadyForShutdown(me))
        region.tell(_GracefulShutdown())
        await probe.expect_terminated(region)
        assert progress.future is not None and progress.future.done()
        assert caplog.messages.count("Counter. preparing for shutdown") == 1


@pytest.mark.parametrize("preparing", [True, False], ids=["preparing", "control"])
async def test_the_coordinator_stops_rebalancing_while_preparing(preparing: bool):
    """The event is told directly. One region holds 4 shards and the other none, so the
    rebalance every second would hand one off."""
    async with manual_node() as n:
        assert n.manual is not None
        replicator, full, empty = n.probe(), n.probe(), n.probe()
        me = n.unique_address
        state = CoordinatorState.EMPTY
        for region in (full, empty):
            state = state.updated(ShardRegionRegistered(region.ref.path, me))
        for shard in "1234":
            state = state.updated(ShardHomeAllocated(shard, full.ref.path))
        coordinator = n.coordinator(replicator, settings=replace(SETTINGS, rebalance_interval=1.0))
        get = await expect_get(replicator)
        get.reply_to.tell(Replicator.GetSuccess(get.key, n.loaded(state)))
        await n.manual.time_passes(0.6)
        assert {m.shard for m in await full.receive_messages(4)} == set("1234")
        if preparing:
            coordinator.tell(MemberPreparingForShutdown(Cluster(n.system).self_member))
            await settle(n)
            await n.manual.expect_no_message_for(3.0, full, empty)
        else:
            await n.manual.time_passes(3.0)
            await full.expect_message_type(BeginHandOff)
            await empty.expect_message_type(BeginHandOff)


async def test_a_region_on_a_preparing_member_may_register():
    """The events come from ``prepare_for_full_cluster_shutdown()``; no time passes, so the
    member stays ``preparing_for_shutdown``."""
    async with manual_node() as n:
        replicator, region = n.probe(), n.probe()
        coordinator = await active(n, replicator)
        cluster = Cluster(n.system)
        cluster.prepare_for_full_cluster_shutdown()
        await eventually(lambda: cluster.self_member.status == "preparing_for_shutdown")
        await registered(coordinator, replicator, region)
