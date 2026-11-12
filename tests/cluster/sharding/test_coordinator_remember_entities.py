"""The coordinator with remembered entities: DDataShardCoordinator (ShardCoordinator.scala
1510-1976) with its remember-entities store, over the base ShardCoordinator's
allocateShardHomesForRememberEntities (1269-1274). Pekko has no spec of its own for these steps.
The unit tests run one coordinator on ManualTime, with a TestProbe standing in for the
coordinator's Replicator and another for its store's:

- the remembered shards are asked for at start, again on the load timeout, and allocated once a
  region can host them, also when they arrive late;
- a new shard's allocation waits for both the state write and the remember write, and the remember
  write is retried on UpdateFailed and on its timeout, but not while terminating;
- the store stopping stops the coordinator, and a restarted coordinator starts a new store;
- under the on-stop backoff supervisor, a coordinator stopped by its store comes back after the
  backoff, and ``_Terminate`` stops it for good.

The coordinator side of Pekko's ``ClusterShardingRememberEntitiesSpec`` runs on real nodes: the
remembered shards start again after a coordinator fail-over and in a new cluster on the same
durable store. The region does not start remembered entities yet, so the tests observe shards."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, cast

import pytest

from ohra import ActorRef, Behaviors
from ohra.behavior import Behavior
from ohra.cluster.sharding import ClusterShardingSettings
from ohra.cluster.sharding.coordinator import (
    BeginHandOff,
    BeginHandOffAck,
    CoordinatorState,
    GetShardHome,
    GracefulShutdownReq,
    RegionStopped,
    Register,
    ShardHome,
    ShardHomeAllocated,
    ShardRegionRegistered,
    _Terminate,  # pyright: ignore[reportPrivateUsage]
    coordinator_behavior,
)
from ohra.cluster.sharding.backoff_supervisor import backoff_on_stop_supervisor
from ohra.cluster.sharding.shard import HandOff, ShardStopped
from ohra.cluster.sharding.remember_entities import (
    DDataRememberEntitiesProvider,
    RememberEntitiesCoordinatorStore,
)
from ohra.cluster.core import Cluster, Down
from ohra.cluster.ddata import Replicator, ReplicatorSettings, sqlite_durable_store
from ohra.remote import Address
from ohra.supervision import SupervisorStrategy
from ohra.testkit import FishingOutcomes, TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up, unreachable_ports
from ..test_cluster_sharding import FAST_SHARDING, crash, ids_hosted_by, node, shards_on, start
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
    settings: ClusterShardingSettings = REMEMBER,
) -> ActorRef[Any]:
    """A coordinator with remember entities on, whose store forwards to ``store``; the provider
    has a replicator probe of its own."""
    provider = ProbeStoreProvider("Counter", settings, 5, node.probe().ref, store.ref)
    behavior = coordinator_behavior("Counter", settings, replicator.ref, provider)
    if supervised:
        behavior = Behaviors.supervise(behavior).on_failure(SupervisorStrategy.restart())
    return node.system._system_root.spawn_child(behavior, "coordinator", None).ref  # pyright: ignore[reportPrivateUsage]


@asynccontextmanager
async def started(
    supervised: bool = False, settings: ClusterShardingSettings = REMEMBER
) -> AsyncGenerator[Harness]:
    """A coordinator that has asked its store for the remembered shards, its state unanswered."""
    async with manual_node() as node:
        replicator, store = node.probe(), node.probe()
        coordinator = spawn(node, replicator, store, supervised, settings)
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


@pytest.mark.parametrize("failure", ["update failed", "remember timeout"])
async def test_terminate_while_the_remember_write_is_pending_stops_on_its_failure(failure: str):
    async with started() as h:
        await active_with_pending_remember_write(h, h.node.probe())
        h.coordinator.tell(_Terminate())
        await settle(h.node)
        if failure == "update failed":
            h.coordinator.tell(Coord.UpdateFailed("1"))
        else:
            await h.time_passes(REMEMBER.updating_state_timeout)
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


async def test_remembered_shards_before_an_empty_state_are_allocated_when_a_region_registers():
    """waitingForInitialState (ShardCoordinator.scala 1625-1628) keeps the remembered shards that
    arrive before the state, and onInitialState (1650-1654) makes them unallocated."""
    async with started() as h:
        region = h.node.probe()
        h.coordinator.tell(Coord.RememberedShards(frozenset({"1"})))
        await settle(h.node)
        get = await expect_get(h.replicator)
        get.reply_to.tell(Replicator.NotFound(get.key))
        await settle(h.node)
        await registered(h.coordinator, h.replicator, region)
        update = await expect_update(h.replicator)
        assert allocated(update) == {"1": region.ref.path}
        await h.expect_add_shard("1")


async def test_remembered_shards_before_a_loaded_state_are_allocated_once_it_is_initialized():
    async with started() as h:
        region = h.node.probe()
        path = region.ref.path
        h.coordinator.tell(Coord.RememberedShards(frozenset({"7"})))
        await settle(h.node)
        state = CoordinatorState.EMPTY.updated(ShardRegionRegistered(path, h.node.unique_address))
        get = await expect_get(h.replicator)
        get.reply_to.tell(Replicator.GetSuccess(get.key, h.node.loaded(state)))
        await settle(h.node)
        await h.time_passes(0.5)  # _StateInitialized
        update = await expect_update(h.replicator)
        assert allocated(update) == {"7": path}
        await h.expect_add_shard("7")


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


HANDOFF = replace(REMEMBER, handoff_timeout=1.0)
"""A hand-off that times out well before the remember load timeout and the rebalance tick."""


async def loaded_active(h: Harness, homes: list[tuple[TestProbe[Any], tuple[str, ...]]]) -> None:
    """The state loaded with each region and its shards, then stateInitialized."""
    state = CoordinatorState.EMPTY
    for region, shards in homes:
        state = state.updated(ShardRegionRegistered(region.ref.path, h.node.unique_address))
        for shard in shards:
            state = state.updated(ShardHomeAllocated(shard, region.ref.path))
    get = await expect_get(h.replicator)
    get.reply_to.tell(Replicator.GetSuccess(get.key, h.node.loaded(state)))
    await settle(h.node)
    await h.time_passes(0.5)  # _StateInitialized


async def fish[M](probe: TestProbe[Any], cls: type[M], shard: str) -> M:
    """The next ``cls`` message for ``shard``, past the others."""
    got = await probe.fish_for_message(
        3,
        lambda m: (
            FishingOutcomes.complete()
            if isinstance(m, cls) and getattr(m, "shard") == shard
            else FishingOutcomes.continue_and_ignore()
        ),
    )
    return cast(M, got[-1])


async def test_a_known_region_registering_again_gets_the_remembered_shards_allocated():
    """A known region's Register allocates the remembered shards (ShardCoordinator.scala 749).
    Shard "7" arrives while the only region shuts down gracefully, so no region takes it; the
    failed hand-off ends the shutdown without an allocation, and the region's Register makes
    one."""
    async with started(settings=HANDOFF) as h:
        region = h.node.probe()
        await loaded_active(h, [(region, ("1",))])
        h.coordinator.tell(GracefulShutdownReq(region.ref))
        h.coordinator.tell(Coord.RememberedShards(frozenset({"7"})))
        await settle(h.node)
        await h.time_passes(HANDOFF.handoff_timeout)  # the hand-off of "1" fails
        await h.replicator.expect_no_message(0.1)
        h.coordinator.tell(Register(region.ref))
        update = await expect_update(h.replicator)
        assert allocated(update) == {"1": region.ref.path, "7": region.ref.path}
        await h.expect_add_shard("7")


async def test_a_rebalanced_shard_s_deallocation_allocates_the_unallocated_shards():
    """A completed hand-off's deallocation allocates the remembered shards (ShardCoordinator.scala
    869). Shard "7" arrives while both regions shut down gracefully, so no region takes it; r2's
    hand-off fails, which frees r2 without an allocation, and the deallocation of r1's shard "1"
    allocates "1" and "7" to r2."""
    async with started(settings=HANDOFF) as h:
        r1, r2 = h.node.probe(), h.node.probe()
        await loaded_active(h, [(r1, ("1",)), (r2, ("2",))])
        h.coordinator.tell(GracefulShutdownReq(r2.ref))
        await h.time_passes(HANDOFF.handoff_timeout / 2)
        h.coordinator.tell(GracefulShutdownReq(r1.ref))
        h.coordinator.tell(Coord.RememberedShards(frozenset({"7"})))
        await settle(h.node)
        await h.time_passes(HANDOFF.handoff_timeout / 2)  # the hand-off of "2" fails
        await h.replicator.expect_no_message(0.1)
        for region in (r1, r2):
            begin = await fish(region, BeginHandOff, "1")
            begin.reply_to.tell(BeginHandOffAck("1", region.ref))
        (await fish(r1, HandOff, "1")).reply_to.tell(ShardStopped("1"))
        deallocated = await expect_update(h.replicator)
        assert "1" not in allocated(deallocated)
        deallocated.reply_to.tell(Replicator.UpdateSuccess(deallocated.key))
        for shard in ("1", "7"):
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


def backed_off(node: Node, replicator: TestProbe[Any], store: TestProbe[Any]) -> ActorRef[Any]:
    """A coordinator with remember entities on as ``ClusterSharding.init`` wires it: under the
    on-stop backoff supervisor, as its child ``coordinator``."""
    provider = ProbeStoreProvider("Counter", REMEMBER, 5, node.probe().ref, store.ref)
    supervised = Behaviors.supervise(
        coordinator_behavior("Counter", REMEMBER, replicator.ref, provider)
    ).on_failure(SupervisorStrategy.restart())
    backoff = REMEMBER.coordinator_failure_backoff
    behavior = backoff_on_stop_supervisor(
        supervised, "coordinator", backoff, backoff * 5, 0.2, lambda m: isinstance(m, _Terminate)
    )
    return node.system._system_root.spawn_child(behavior, "singleton", None).ref  # pyright: ignore[reportPrivateUsage]


async def test_the_store_stopping_restarts_the_coordinator_after_the_backoff():
    async with manual_node() as node:
        replicator, store = node.probe(), node.probe()
        singleton = backed_off(node, replicator, store)
        coordinator = f"{singleton.path}/coordinator"
        assert (await store.expect_message_type(Coord.GetShards, 3)).reply_to.path == coordinator
        node.system.resolve(f"{coordinator}/RememberEntitiesStore").tell(StopStore())
        await replicator.expect_terminated(node.system.resolve(coordinator), 3)
        assert node.manual is not None
        backoff = REMEMBER.coordinator_failure_backoff
        await node.manual.expect_no_message_for(backoff - 0.01, store)
        await node.manual.time_passes(backoff * 0.2 + 0.01)  # the random factor at most
        assert (await store.expect_message_type(Coord.GetShards, 3)).reply_to.path == coordinator


async def test_terminate_stops_the_supervised_coordinator_for_good():
    async with manual_node() as node:
        replicator, store = node.probe(), node.probe()
        singleton = backed_off(node, replicator, store)
        await store.expect_message_type(Coord.GetShards, 3)
        singleton.tell(_Terminate())
        await replicator.expect_terminated(singleton, 3)
        assert node.manual is not None
        await node.manual.expect_no_message_for(REMEMBER.coordinator_failure_backoff * 6, store)


# --- ClusterShardingRememberEntitiesSpec, the coordinator side, on real nodes -------------------

IN_MEMORY = replace(
    FAST_SHARDING,
    remember_entities=True,
    distributed_data=ReplicatorSettings(prefer_oldest=True, max_delta_elements=5),
)
"""Remember entities on, with sharding's Replicator keeping nothing on disk."""


async def test_remembered_shards_start_again_when_the_coordinator_fails_over():
    """ "start remembered entities when coordinator fail over": the new coordinator allocates the
    shards of the crashed node's region with no message sent. The region does not start
    remembered entities yet, so the shards are what shows."""
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start(n, settings=IN_MEMORY) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 12)
        assert {1, 2} <= set(where.values())
        on_first = await shards_on(regions[0])
        assert on_first
        await crash(nodes[0])
        await eventually(lambda: unreachable_ports(nodes[1]) == {p1}, timeout=5)
        Cluster(nodes[1]).manager.tell(Down(Address("c", "127.0.0.1", p1)))
        await eventually(lambda: all_up(nodes[1], 2) and all_up(nodes[2], 2), timeout=5)

        async def all_started() -> None:
            surviving = await shards_on(regions[1]) | await shards_on(regions[2])
            assert on_first <= surviving

        await Node(nodes[1]).probe().await_assert(all_started, max=10)


async def test_remembered_shards_start_in_a_new_cluster(tmp_path: Path):
    """ "start remembered entities in new cluster": a new system on the same durable store hosts
    the shards the old one remembered, with no message sent."""
    settings = replace(
        FAST_SHARDING,
        remember_entities=True,
        distributed_data=ReplicatorSettings(
            durable_keys=frozenset({"shard-*"}),
            durable_store=lambda: sqlite_durable_store(tmp_path / "ddata"),
        ),
    )
    p = free_port()
    async with node(p, [p]) as first:
        await eventually(lambda: all_up(first, 1), timeout=5)
        region = start(first, settings=settings)
        await ids_hosted_by([region], [first], 6)
        remembered = await shards_on(region)
        assert remembered
    async with node(p, [p]) as second:
        await eventually(lambda: all_up(second, 1), timeout=5)
        region = start(second, settings=settings)

        async def all_started() -> None:
            assert await shards_on(region) == remembered

        await Node(second).probe().await_assert(all_started, max=10)
