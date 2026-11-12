"""The coordinator with remembered entities: DDataShardCoordinator (ShardCoordinator.scala
1510-1976) with its remember-entities store, over the base ShardCoordinator's
allocateShardHomesForRememberEntities (1269-1274). Pekko has no spec of its own for these steps.
The unit tests run one coordinator on ManualTime, with a TestProbe standing in for the
coordinator's Replicator and another for its store's:

- the remembered shards are asked for at start, again on the load timeout, and allocated once a
  region can host them, also when they arrive late;
- a new shard's allocation waits for both the state write and the remember write, and the remember
  write is retried on UpdateFailed and on its timeout, but not while terminating;
- the store stopping stops the coordinator, and a restarted coordinator starts a new store."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest

from ohra import ActorRef, Behaviors
from ohra.behavior import Behavior
from ohra.cluster.sharding.coordinator import (
    CoordinatorState,
    GetShardHome,
    RegionStopped,
    Register,
    ShardHome,
    ShardRegionRegistered,
    _Terminate,  # pyright: ignore[reportPrivateUsage]
    coordinator_behavior,
)
from ohra.cluster.sharding.remember_entities import (
    DDataRememberEntitiesProvider,
    RememberEntitiesCoordinatorStore,
)
from ohra.cluster.ddata import Replicator
from ohra.supervision import SupervisorStrategy
from ohra.testkit import TestProbe

from .test_ddata_shard_coordinator import (
    SETTINGS,
    Node,
    expect_get,
    expect_update,
    manual_node,
    registered,
    settle,
)

Coord = RememberEntitiesCoordinatorStore

REMEMBER = replace(SETTINGS, remember_entities=True)


@dataclass(frozen=True)
class StopStore:
    """Stops the store a ``ProbeStoreProvider`` started."""


@dataclass(frozen=True)
class ProbeStoreProvider(DDataRememberEntitiesProvider):
    """A provider whose coordinator store forwards every message to ``store``."""

    store: ActorRef[Any]

    def coordinator_store_behavior(self) -> Behavior[RememberEntitiesCoordinatorStore.Command]:
        store = self.store

        @Behaviors.receive_message
        def forward(msg: Any) -> Behavior[Any]:
            if isinstance(msg, StopStore):
                return Behaviors.stopped()
            store.tell(msg)
            return Behaviors.same

        return cast(Behavior[RememberEntitiesCoordinatorStore.Command], forward)


@dataclass
class Harness:
    node: Node
    coordinator: ActorRef[Any]
    replicator: TestProbe[Any]
    store: TestProbe[Any]

    async def time_passes(self, seconds: float) -> None:
        assert self.node.manual is not None
        await self.node.manual.time_passes(seconds)

    def stop_store(self) -> None:
        self.node.system.resolve(f"{self.coordinator.path}/RememberEntitiesStore").tell(
            StopStore()
        )

    async def expect_add_shard(self, shard: str) -> None:
        await self.store.expect_message(Coord.AddShard(shard, self.coordinator), 3)


def spawn(
    node: Node,
    replicator: TestProbe[Any],
    store: TestProbe[Any],
    supervised: bool = False,
) -> ActorRef[Any]:
    """A coordinator with remember entities on, whose store forwards to ``store``; the provider
    has a replicator probe of its own."""
    provider = ProbeStoreProvider("Counter", REMEMBER, 5, node.probe().ref, store.ref)
    behavior = coordinator_behavior("Counter", REMEMBER, replicator.ref, provider)
    if supervised:
        behavior = Behaviors.supervise(behavior).on_failure(SupervisorStrategy.restart())
    return node.system._system_root.spawn_child(behavior, "coordinator", None).ref  # pyright: ignore[reportPrivateUsage]


@asynccontextmanager
async def started(supervised: bool = False) -> AsyncGenerator[Harness]:
    """A coordinator that has asked its store for the remembered shards, its state unanswered."""
    async with manual_node() as node:
        replicator, store = node.probe(), node.probe()
        coordinator = spawn(node, replicator, store, supervised)
        await store.expect_message(Coord.GetShards(coordinator), 3)
        yield Harness(node, coordinator, replicator, store)


async def to_active(h: Harness, remembered: frozenset[str] = frozenset()) -> None:
    """NotFound, then the remembered shards."""
    get = await expect_get(h.replicator)
    get.reply_to.tell(Replicator.NotFound(get.key))
    await settle(h.node)
    h.coordinator.tell(Coord.RememberedShards(remembered))
    await settle(h.node)


def allocated(update: Replicator.Update[Any]) -> dict[str, str]:
    return update.modify(update.initial).value.shards


def logged(caplog: pytest.LogCaptureFixture, level: int, text: str) -> int:
    return sum(1 for r in caplog.records if r.levelno == level and text in r.getMessage())


async def test_the_coordinator_asks_its_store_for_the_remembered_shards_at_start():
    async with started() as h:
        await h.store.expect_no_message(0.1)  # once, the GetShards ``started`` saw


async def test_the_load_timeout_asks_again_until_answered():
    async with started() as h:
        await h.time_passes(REMEMBER.waiting_for_state_timeout)
        await h.store.expect_message(Coord.GetShards(h.coordinator), 3)
        h.coordinator.tell(Coord.RememberedShards(frozenset()))
        await settle(h.node)
        await h.time_passes(REMEMBER.waiting_for_state_timeout)
        await h.store.expect_no_message(0.1)


async def test_remembered_shards_are_allocated_when_the_first_region_registers():
    async with started() as h:
        region = h.node.probe()
        await to_active(h, frozenset({"1", "2"}))
        await h.replicator.expect_no_message(0.1)  # no region to allocate to
        h.coordinator.tell(Register(region.ref))
        update = await expect_update(h.replicator)
        assert update.modify(update.initial).value.regions == {region.ref.path: ()}
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        for shard in ("1", "2"):
            update = await expect_update(h.replicator)
            assert allocated(update)[shard] == region.ref.path
            await h.expect_add_shard(shard)
            update.reply_to.tell(Replicator.UpdateSuccess(update.key))
            h.coordinator.tell(Coord.UpdateDone(shard))
        await h.replicator.expect_no_message(0.1)


@pytest.mark.parametrize("state_write_first", [True, False])
async def test_shard_home_waits_for_both_the_state_write_and_the_remember_write(
    state_write_first: bool,
):
    async with started() as h:
        region, asker = h.node.probe(), h.node.probe()
        await to_active(h)
        await registered(h.coordinator, h.replicator, region)
        h.coordinator.tell(GetShardHome("1", asker.ref))
        update = await expect_update(h.replicator)
        await h.expect_add_shard("1")
        state_written = Replicator.UpdateSuccess(update.key)
        if state_write_first:
            update.reply_to.tell(state_written)
        else:
            h.coordinator.tell(Coord.UpdateDone("1"))
        await asker.expect_no_message(0.1)
        if state_write_first:
            h.coordinator.tell(Coord.UpdateDone("1"))
        else:
            update.reply_to.tell(state_written)
        await asker.expect_message(ShardHome("1", region.ref), 3)


async def active_with_pending_remember_write(h: Harness, asker: TestProbe[Any]) -> None:
    """A registered region, and shard "1" allocated, its remember write unanswered."""
    region = h.node.probe()
    await to_active(h)
    await registered(h.coordinator, h.replicator, region)
    h.coordinator.tell(GetShardHome("1", asker.ref))
    update = await expect_update(h.replicator)
    await h.expect_add_shard("1")
    update.reply_to.tell(Replicator.UpdateSuccess(update.key))
    await settle(h.node)


async def test_an_update_failed_retries_the_remember_write(caplog: pytest.LogCaptureFixture):
    async with started() as h:
        await active_with_pending_remember_write(h, h.node.probe())
        h.coordinator.tell(Coord.UpdateFailed("1"))
        await h.expect_add_shard("1")
        text = (
            "Counter: The ShardCoordinator was unable to update remembered shard [1] within "
            "'updating-state-timeout': 5000 millis, retrying"
        )
        assert logged(caplog, logging.ERROR, text) == 1


async def test_the_remember_timeout_retries_the_remember_write():
    async with started() as h:
        await active_with_pending_remember_write(h, h.node.probe())
        await h.time_passes(REMEMBER.updating_state_timeout)
        await h.expect_add_shard("1")


async def test_an_update_done_for_another_shard_is_warned_and_ignored(
    caplog: pytest.LogCaptureFixture,
):
    async with started() as h:
        asker = h.node.probe()
        await active_with_pending_remember_write(h, asker)
        h.coordinator.tell(Coord.UpdateDone("2"))
        await asker.expect_no_message(0.1)
        text = (
            "Counter: Saw remember entities update complete for shard id [2], while waiting "
            "for [1]"
        )
        assert logged(caplog, logging.WARNING, text) == 1
        h.coordinator.tell(Coord.UpdateDone("1"))
        assert (await asker.expect_message_type(ShardHome, 3)).shard == "1"


async def test_terminate_while_the_remember_write_is_pending_stops_on_its_failure():
    async with started() as h:
        await active_with_pending_remember_write(h, h.node.probe())
        h.coordinator.tell(_Terminate())
        await settle(h.node)
        h.coordinator.tell(Coord.UpdateFailed("1"))
        await h.replicator.expect_terminated(h.coordinator, 3)
        await h.store.expect_no_message(0.1)  # no retry


@pytest.mark.parametrize(
    "phase", ["waiting for initial state", "waiting for state initialized", "active", "update"]
)
async def test_the_store_stopping_stops_the_coordinator(
    phase: str, caplog: pytest.LogCaptureFixture
):
    async with started() as h:
        region = h.node.probe()
        if phase == "waiting for state initialized":
            state = CoordinatorState.EMPTY.updated(
                ShardRegionRegistered(region.ref.path, h.node.unique_address)
            )
            get = await expect_get(h.replicator)
            get.reply_to.tell(Replicator.GetSuccess(get.key, h.node.loaded(state)))
            await settle(h.node)
        elif phase == "active":
            await to_active(h)
        elif phase == "update":
            await to_active(h)
            h.coordinator.tell(Register(region.ref))
            await expect_update(h.replicator)
        h.stop_store()
        await h.replicator.expect_terminated(h.coordinator, 3)
        text = "Counter: The ShardCoordinator stopping because the remember entities store stopped"
        assert logged(caplog, logging.ERROR, text) == 1


async def test_late_remembered_shards_while_waiting_for_state_initialized_are_allocated():
    """Pekko's waitingForStateInitialized (ShardCoordinator.scala 1686-1687) builds the state with
    the remembered shards and discards the copy, so stateInitialized would allocate nothing and
    no write would follow. ohra keeps them."""
    async with started() as h:
        region = h.node.probe()
        path = region.ref.path
        state = CoordinatorState.EMPTY.updated(ShardRegionRegistered(path, h.node.unique_address))
        get = await expect_get(h.replicator)
        get.reply_to.tell(Replicator.GetSuccess(get.key, h.node.loaded(state)))
        await settle(h.node)
        h.coordinator.tell(Coord.RememberedShards(frozenset({"7"})))
        await settle(h.node)
        await h.replicator.expect_no_message(0.1)
        await h.time_passes(0.5)  # _StateInitialized
        update = await expect_update(h.replicator)
        assert allocated(update) == {"7": path}
        await h.expect_add_shard("7")


async def test_late_remembered_shards_while_waiting_for_an_update_are_stashed_and_applied():
    async with started() as h:
        region, other = h.node.probe(), h.node.probe()
        await to_active(h)
        await registered(h.coordinator, h.replicator, region)
        h.coordinator.tell(Register(other.ref))
        update = await expect_update(h.replicator)
        h.coordinator.tell(Coord.RememberedShards(frozenset({"7"})))
        await settle(h.node)
        await h.store.expect_no_message(0.1)
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        update = await expect_update(h.replicator)
        assert "7" in allocated(update)
        await h.expect_add_shard("7")


async def test_late_remembered_shards_in_active_are_allocated():
    async with started() as h:
        region = h.node.probe()
        await to_active(h)
        await registered(h.coordinator, h.replicator, region)
        h.coordinator.tell(Coord.RememberedShards(frozenset({"7"})))
        update = await expect_update(h.replicator)
        assert allocated(update) == {"7": region.ref.path}
        await h.expect_add_shard("7")


async def test_a_terminated_region_s_shards_are_allocated_again():
    async with started() as h:
        r1, r2, asker = h.node.probe(), h.node.probe(), h.node.probe()
        await to_active(h)
        await registered(h.coordinator, h.replicator, r1)
        for shard in ("1", "2"):
            h.coordinator.tell(GetShardHome(shard, asker.ref))
            update = await expect_update(h.replicator)
            await h.expect_add_shard(shard)
            update.reply_to.tell(Replicator.UpdateSuccess(update.key))
            h.coordinator.tell(Coord.UpdateDone(shard))
            await asker.expect_message(ShardHome(shard, r1.ref), 3)
        await registered(h.coordinator, h.replicator, r2)

        h.coordinator.tell(RegionStopped(r1.ref.path))
        terminated = await expect_update(h.replicator)
        assert terminated.modify(terminated.initial).value.shards == {}
        terminated.reply_to.tell(Replicator.UpdateSuccess(terminated.key))
        for shard in ("1", "2"):
            update = await expect_update(h.replicator)
            assert allocated(update)[shard] == r2.ref.path
            await h.expect_add_shard(shard)
            update.reply_to.tell(Replicator.UpdateSuccess(update.key))
            h.coordinator.tell(Coord.UpdateDone(shard))
        await h.replicator.expect_no_message(0.1)


async def test_a_restarted_coordinator_asks_a_new_store_again(caplog: pytest.LogCaptureFixture):
    async with started(supervised=True) as h:
        region = h.node.probe()
        await to_active(h)
        h.coordinator.tell(Register(region.ref))
        update = await expect_update(h.replicator)
        update.reply_to.tell(Replicator.ModifyFailure(update.key, "boom", ValueError("boom")))
        await h.store.expect_message(Coord.GetShards(h.coordinator), 3)
        assert not [r for r in caplog.records if "already has a child named" in r.getMessage()]

        await to_active(h, frozenset({"1"}))
        await registered(h.coordinator, h.replicator, region)
        update = await expect_update(h.replicator)
        assert allocated(update) == {"1": region.ref.path}
        await h.expect_add_shard("1")


async def test_without_remember_entities_no_store_is_spawned():
    async with manual_node() as node:
        replicator, region, asker = node.probe(), node.probe(), node.probe()
        behavior = coordinator_behavior("Counter", SETTINGS, replicator.ref)
        coordinator = node.system._system_root.spawn_child(behavior, "coordinator", None).ref  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(LookupError):
            node.system.resolve(f"{coordinator.path}/RememberEntitiesStore")
        get = await expect_get(replicator)
        get.reply_to.tell(Replicator.NotFound(get.key))
        await settle(node)
        await registered(coordinator, replicator, region)
        coordinator.tell(GetShardHome("1", asker.ref))
        update = await expect_update(replicator)
        assert allocated(update) == {"1": region.ref.path}
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        await asker.expect_message(ShardHome("1", region.ref), 3)
        await replicator.expect_no_message(0.1)
