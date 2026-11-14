"""Pekko's scaladsl ShardedDaemonProcessSpec, "Coordinator" (126-183): the coordinator reads its
state from Distributed Data and keeps every worker alive with StartEntity, throttled. Then the
rescale of ShardedDaemonProcessCoordinator.scala (prepareRescale, stopAllShards,
waitForShardsStopping, rescalingComplete), which has no Pekko spec. One joined node on
ManualTime, with a TestProbe standing in for the sharding region, and for the Replicator where
only a fake one can time out."""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, cast

import pytest

from ohra import ActorRef
from ohra.cluster.ddata import DistributedData, Replicator
from ohra.cluster.sharding import ClusterShardingSettings, ShardStopped, StartEntity
from ohra.cluster.sharding.coordinator import StopShards
from ohra.cluster.sharding.sharded_daemon_process import (
    ChangeNumberOfProcesses,
    GetNumberOfProcesses,
    NumberOfProcesses,
    ShardedDaemonProcessSettings,
    ShardedDaemonProcessState,
    ShardedDaemonProcessStateKey,
)
from ohra.cluster.sharding.sharded_daemon_process_coordinator import (
    sharded_daemon_process_coordinator,
)
from ohra.patterns import StatusReply
from ohra.testkit import FishingOutcomes, TestProbe

from .test_ddata_shard_coordinator import Node, expect_get, expect_update, levels, manual_node

SETTINGS = ShardedDaemonProcessSettings(keep_alive_interval=1.0, keep_alive_throttle_interval=0.02)
"""Pekko's test config: ping often, start fast."""

UNTHROTTLED = SETTINGS.with_keep_alive_throttle_interval(0.0)

SHARDING = replace(ClusterShardingSettings(), handoff_timeout=4.0)
"""So the coordinator asks again for shards that have not stopped after 1 s."""


def pinger(
    node: Node,
    name: str,
    sharding: TestProbe[Any],
    settings: ShardedDaemonProcessSettings = SETTINGS,
) -> ActorRef[Any]:
    behavior = sharded_daemon_process_coordinator(settings, SHARDING, 3, name, sharding.ref)
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


# --- the rescale ---------------------------------------------------------------------------------


async def stop_shards(probe: TestProbe[Any], expected: frozenset[str]) -> StopShards:
    """The next StopShards, past any keep-alive."""
    fished = await probe.fish_for_message(
        10,
        lambda m: (
            FishingOutcomes.complete()
            if isinstance(m, StopShards)
            else FishingOutcomes.continue_and_ignore()
            if isinstance(m, StartEntity)
            else FishingOutcomes.fail(f"unexpected {m!r}")
        ),
    )
    stop = fished[-1]
    assert isinstance(stop, StopShards)
    assert stop.shards == expected
    return stop


async def number_of_processes(node: Node, coordinator: ActorRef[Any]) -> NumberOfProcesses:
    reply = node.probe()
    coordinator.tell(GetNumberOfProcesses(reply.ref))
    return await reply.expect_message_type(NumberOfProcesses)


async def expect_ids(probe: TestProbe[Any], *ids: str) -> None:
    for entity_id in ids:
        await probe.expect_message(StartEntity(entity_id), 10)


async def test_a_rescale_stops_the_old_shards_and_starts_the_new_ids() -> None:
    async with manual_node() as node:
        assert node.manual is not None
        sharding, reply = node.probe(), node.probe()
        coordinator = pinger(node, "rescale-a", sharding, UNTHROTTLED)
        await expect_ids(sharding, "0", "1", "2")
        coordinator.tell(ChangeNumberOfProcesses(4, reply.ref))
        stop = await stop_shards(sharding, frozenset({"0", "1", "2"}))
        await reply.expect_no_message(0.1)
        for shard in stop.shards:
            stop.reply_to.tell(ShardStopped(shard))
        await reply.expect_message(StatusReply.ACK)
        await expect_ids(sharding, "1|4|0", "1|4|1", "1|4|2", "1|4|3")
        got = await number_of_processes(node, coordinator)
        assert (got.number_of_processes, got.rescale_in_progress, got.revision) == (4, False, 1)


async def test_a_rescale_to_the_same_count_is_acked_without_a_new_revision() -> None:
    async with manual_node() as node:
        assert node.manual is not None
        sharding, reply = node.probe(), node.probe()
        coordinator = pinger(node, "rescale-b", sharding, UNTHROTTLED)
        await expect_ids(sharding, "0", "1", "2")
        coordinator.tell(ChangeNumberOfProcesses(3, reply.ref))
        await reply.expect_message(StatusReply.ACK)
        await node.manual.time_passes(1.0)
        await expect_ids(sharding, "0", "1", "2")  # a StopShards would come first
        await sharding.expect_no_message(0)
        got = await number_of_processes(node, coordinator)
        assert (got.number_of_processes, got.rescale_in_progress, got.revision) == (3, False, 0)


async def test_a_rescale_to_no_processes_is_refused() -> None:
    async with manual_node() as node:
        assert node.manual is not None
        sharding, reply = node.probe(), node.probe()
        coordinator = pinger(node, "rescale-c", sharding, UNTHROTTLED)
        await expect_ids(sharding, "0", "1", "2")
        for n in (0, -1):
            coordinator.tell(ChangeNumberOfProcesses(n, reply.ref))
            refused = cast(StatusReply[Any], await reply.expect_message_type(StatusReply))
            assert refused.error_message == f"number_of_processes must be >= 1, got {n}"
        await node.manual.time_passes(1.0)
        await expect_ids(sharding, "0", "1", "2")
        await sharding.expect_no_message(0)


async def test_a_second_rescale_during_one_is_refused() -> None:
    async with manual_node() as node:
        sharding, reply, second = node.probe(), node.probe(), node.probe()
        coordinator = pinger(node, "rescale-d", sharding, UNTHROTTLED)
        coordinator.tell(ChangeNumberOfProcesses(4, reply.ref))
        await stop_shards(sharding, frozenset({"0", "1", "2"}))
        coordinator.tell(ChangeNumberOfProcesses(5, second.ref))
        refused = cast(StatusReply[Any], await second.expect_message_type(StatusReply))
        assert refused.error_message == "Rescale in progress, retry later"
        got = await number_of_processes(node, coordinator)
        assert (got.number_of_processes, got.rescale_in_progress, got.revision) == (4, True, 1)


async def test_shards_that_do_not_stop_are_asked_again(caplog: pytest.LogCaptureFixture) -> None:
    async with manual_node() as node:
        manual = node.manual
        assert manual is not None
        sharding, reply = node.probe(), node.probe()
        with caplog.at_level(logging.DEBUG):
            coordinator = pinger(node, "rescale-e", sharding, UNTHROTTLED)
            await expect_ids(sharding, "0", "1", "2")  # the tick timer runs
            coordinator.tell(ChangeNumberOfProcesses(4, reply.ref))
            stop = await stop_shards(sharding, frozenset({"0", "1", "2"}))
            stop.reply_to.tell(ShardStopped("0"))
            await manual.expect_no_message_for(0.9, sharding, reply)  # no keep-alive meanwhile
            await manual.time_passes(0.1)
            again = await stop_shards(sharding, frozenset({"0", "1", "2"}))
            # Pekko waits on every shard again; the shard coordinator answers a stopped one at once
            again.reply_to.tell(ShardStopped("1"))
            again.reply_to.tell(ShardStopped("2"))
            await reply.expect_no_message(0.1)
            again.reply_to.tell(ShardStopped("0"))
            await reply.expect_message(StatusReply.ACK)
            await expect_ids(sharding, "1|4|0", "1|4|1", "1|4|2", "1|4|3")
            await manual.time_passes(1.0)  # past where an uncancelled timer would fire
            await expect_ids(sharding, "1|4|0", "1|4|1", "1|4|2", "1|4|3")
        assert levels(caplog, "Stopping shards timed out") == ["DEBUG"]
        assert levels(caplog, "Unexpected message") == []  # no keep-alive tick while rescaling
        assert levels(caplog, "ShardStopTimeout") == []  # nor a stop timeout after it


async def test_an_interrupted_rescale_resumes_when_the_coordinator_starts() -> None:
    async with manual_node() as node:
        name = "rescale-f"
        key = ShardedDaemonProcessStateKey(name)
        ddata = node.probe()
        replicator = DistributedData(node.system).replicator
        replicator.tell(
            Replicator.Update(
                key,
                ShardedDaemonProcessState.initial_state(3),
                Replicator.WriteLocal(),
                ddata.ref,
                lambda s: s.start_scaling_to(4),
            )
        )
        await ddata.expect_message_type(Replicator.UpdateSuccess)

        sharding = node.probe()
        pinger(node, name, sharding, UNTHROTTLED)
        stop = await stop_shards(sharding, frozenset({"0", "1", "2"}))
        for shard in stop.shards:
            stop.reply_to.tell(ShardStopped(shard))
        await expect_ids(sharding, "1|4|0", "1|4|1", "1|4|2", "1|4|3")
        replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), ddata.ref))
        got = cast(
            Replicator.GetSuccess[ShardedDaemonProcessState],
            await ddata.expect_message_type(Replicator.GetSuccess),
        )
        assert (got.data.revision, got.data.completed) == (1, True)


async def test_a_rescale_during_a_throttled_round_stops_the_round() -> None:
    async with manual_node() as node:
        assert node.manual is not None
        sharding, reply = node.probe(), node.probe()
        coordinator = pinger(
            node, "rescale-g", sharding, SETTINGS.with_keep_alive_throttle_interval(1.0)
        )
        await sharding.expect_message(StartEntity("0"), 10)
        coordinator.tell(ChangeNumberOfProcesses(4, reply.ref))
        stop = await sharding.expect_message_type(StopShards)
        for shard in stop.shards:
            stop.reply_to.tell(ShardStopped(shard))
        await reply.expect_message(StatusReply.ACK)
        await expect_ids(sharding, "1|4|0")
        await node.manual.time_passes(2.0)
        await expect_ids(sharding, "1|4|1", "1|4|2")  # and no "1" or "2" of the old round
        await sharding.expect_no_message(0)


async def test_update_timeouts_are_retried_with_pekkos_log_levels(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with manual_node() as node:
        replicator, sharding, reply = node.probe(), node.probe(), node.probe()
        monkeypatch.setattr(DistributedData(node.system), "replicator", replicator.ref)
        with caplog.at_level(logging.DEBUG, logger="ohra.actor"):
            coordinator = pinger(node, "rescale-h", sharding, UNTHROTTLED)
            get = await expect_get(replicator)
            get.reply_to.tell(Replicator.NotFound(get.key))
            await expect_ids(sharding, "0", "1", "2")
            coordinator.tell(ChangeNumberOfProcesses(4, reply.ref))
            update = await expect_update(replicator)
            for _ in range(7):
                update.reply_to.tell(Replicator.UpdateTimeout(update.key))
                update = await expect_update(replicator)
            update.reply_to.tell(Replicator.UpdateSuccess(update.key))
            stop = await stop_shards(sharding, frozenset({"0", "1", "2"}))
            for shard in stop.shards:
                stop.reply_to.tell(ShardStopped(shard))
            await reply.expect_message(StatusReply.ACK)
            update = await expect_update(replicator)
            for _ in range(6):
                update.reply_to.tell(Replicator.UpdateTimeout(update.key))
                update = await expect_update(replicator)
        assert levels(caplog, "sharded daemon process, retrying") == ["DEBUG"] * 6 + ["WARNING"]
        assert levels(caplog, "after completing rescale") == ["DEBUG"] * 5 + ["WARNING"]


async def test_a_get_failure_is_retried(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with manual_node() as node:
        replicator, sharding = node.probe(), node.probe()
        monkeypatch.setattr(DistributedData(node.system), "replicator", replicator.ref)
        with caplog.at_level(logging.INFO, logger="ohra.actor"):
            pinger(node, "rescale-i", sharding)
            get = await expect_get(replicator)
            get.reply_to.tell(Replicator.GetFailure(get.key))
            await expect_get(replicator)
        assert levels(caplog, "Failed fetching initial state") == ["INFO"]
