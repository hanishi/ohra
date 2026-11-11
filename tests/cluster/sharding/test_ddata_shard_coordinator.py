"""The sharding coordinator on Distributed Data: Pekko's DDataShardCoordinator
(ShardCoordinator.scala 1535-1976) over the base ShardCoordinator (600-1385). Pekko has no spec of
its own for these steps. The unit tests run one coordinator on ManualTime, with a TestProbe
standing in for the Replicator:

- the initial Get, its consistency and its retries (getCoordinatorState, waitingForInitialState);
- NotFound goes straight to active, and nothing is answered before the initial state;
- update and waitingForUpdate: an ack only after its write, the GetShardHome fast path, the
  UpdateTimeout retry, ModifyFailure's restart, and Terminate during an update;
- watchStateActors with D5 (a region of a gone node incarnation is terminated), and
  stateInitialized 500 ms later.

The review-focus tests run several real nodes with sharding's own Replicator."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from typing import Any, cast

import pytest

from ohra import ActorRef, ActorSystem, Behaviors, ask
from ohra.cluster import ClusterSettings
from ohra.cluster.core import Cluster, Down
from ohra.cluster.ddata import LWWRegister, LWWRegisterKey, Replicator, SelfUniqueAddress
from ohra.cluster.sharding import ClusterSharding, ClusterShardingSettings, shard_id_of
from ohra.cluster.sharding.coordinator import (
    CoordinatorState,
    GetShardHome,
    HostShard,
    RegionStopped,
    Register,
    RegisterAck,
    ShardHome,
    ShardHomeAllocated,
    ShardHomes,
    ShardRegionRegistered,
    ShardStarted,
    _Terminate,  # pyright: ignore[reportPrivateUsage]
    coordinator_behavior,
)
from ohra.remote import Address, UniqueAddress
from ohra.supervision import SupervisorStrategy
from ohra.testkit import FishingOutcomes, ManualTime, TestProbe
from ohra.testkit.manual_time import ExplicitlyTriggeredScheduler
from ohra.testkit.settings import TestKitSettings

from ...util import eventually, free_port
from ..test_cluster import FAST, FAST_SHUTDOWN, all_up, unreachable_ports
from ..test_cluster_sharding import (
    COUNTER2,
    FAST_SHARDING,
    Count,
    crash,
    ids_hosted_by,
    ids_on_new_shards,
    node,
    ref,
    start,
)

SETTINGS = ClusterShardingSettings(rebalance_interval=60.0, shard_start_timeout=60.0)
"""Pekko's coordinator-state defaults, with no rebalance or HostShard resend in the way."""

type Register_ = LWWRegister[CoordinatorState]


@dataclass
class Node:
    system: ActorSystem[Any]
    manual: ManualTime | None = None
    settings: TestKitSettings = field(default_factory=TestKitSettings)

    def probe(self) -> TestProbe[Any]:
        return TestProbe[Any](self)

    @property
    def unique_address(self) -> UniqueAddress:
        return Cluster(self.system).self_unique_address

    def coordinator(
        self,
        replicator: TestProbe[Any],
        supervised: bool = False,
        settings: ClusterShardingSettings = SETTINGS,
    ) -> ActorRef[Any]:
        behavior = coordinator_behavior("Counter", settings, replicator.ref)
        if supervised:
            behavior = Behaviors.supervise(behavior).on_failure(SupervisorStrategy.restart())
        return self.system._system_root.spawn_child(behavior, "coordinator", None).ref  # pyright: ignore[reportPrivateUsage]

    def loaded(self, state: CoordinatorState) -> Register_:
        return LWWRegister.create(SelfUniqueAddress(self.unique_address), state)


@asynccontextmanager
async def manual_node() -> AsyncGenerator[Node]:
    """One joined node on ManualTime."""
    port = free_port()
    system = ActorSystem(
        Behaviors.ignore,
        "c",
        address=("127.0.0.1", port),
        cluster=ClusterSettings(seed_nodes=(Address("c", "127.0.0.1", port),), **FAST),
        shutdown=FAST_SHUTDOWN,
        scheduler=ExplicitlyTriggeredScheduler(),
    )
    async with system:
        manual = ManualTime(system)
        while not all_up(system, 1):
            await manual.time_passes(0.02)
        yield Node(system, manual)


async def expect_get(replicator: TestProbe[Any]) -> Replicator.Get[Register_]:
    return cast(Replicator.Get[Register_], await replicator.expect_message_type(Replicator.Get))


async def expect_update(replicator: TestProbe[Any]) -> Replicator.Update[Register_]:
    return cast(
        Replicator.Update[Register_], await replicator.expect_message_type(Replicator.Update)
    )


async def settle(node: Node) -> None:
    """Let every mailbox empty, so a reply through the ask's adapter has arrived."""
    assert node.manual is not None
    await node.manual.time_passes(0)


async def active(
    node: Node, replicator: TestProbe[Any], supervised: bool = False
) -> ActorRef[Any]:
    """A coordinator past its initial Get, with nothing stored."""
    coordinator = node.coordinator(replicator, supervised)
    get = await expect_get(replicator)
    get.reply_to.tell(Replicator.NotFound(get.key))
    await settle(node)
    return coordinator


async def registered(
    coordinator: ActorRef[Any], replicator: TestProbe[Any], region: TestProbe[Any]
) -> None:
    coordinator.tell(Register(region.ref))
    update = await expect_update(replicator)
    update.reply_to.tell(Replicator.UpdateSuccess(update.key))
    await region.fish_for_message(
        3,
        lambda m: (
            FishingOutcomes.complete()
            if isinstance(m, RegisterAck)
            else FishingOutcomes.continue_and_ignore()
        ),
    )


def levels(caplog: pytest.LogCaptureFixture, text: str) -> list[str]:
    return [r.levelname for r in caplog.records if text in r.getMessage()]


# --- the unit tests, on ManualTime --------------------------------------------------------------


async def test_the_coordinator_reads_its_state_with_read_majority_plus():
    async with manual_node() as node:
        replicator = node.probe()
        node.coordinator(replicator)
        get = await expect_get(replicator)
        assert get.key.id == "CounterCoordinatorState"
        assert get.consistency == Replicator.ReadMajorityPlus(2.0, 5, 5)


async def test_a_get_failure_is_retried_with_pekkos_log_levels(caplog: pytest.LogCaptureFixture):
    async with manual_node() as node:
        replicator = node.probe()
        with caplog.at_level(logging.INFO, logger="ohra.actor"):
            node.coordinator(replicator)
            get = await expect_get(replicator)
            for _ in range(5):
                get.reply_to.tell(Replicator.GetFailure(get.key))
                get = await expect_get(replicator)
        assert levels(caplog, "unable to get an initial state") == [
            "INFO",
            "WARNING",
            "WARNING",
            "WARNING",
            "ERROR",
        ]


async def test_not_found_goes_straight_to_active():
    async with manual_node() as node:
        replicator, region = node.probe(), node.probe()
        coordinator = await active(node, replicator)
        coordinator.tell(Register(region.ref))
        update = await expect_update(replicator)
        assert update.key.id == "CounterCoordinatorState"


async def test_nothing_is_answered_before_the_initial_state():
    async with manual_node() as node:
        replicator, region = node.probe(), node.probe()
        coordinator = node.coordinator(replicator)
        get = await expect_get(replicator)
        coordinator.tell(Register(region.ref))
        coordinator.tell(GetShardHome("1", region.ref))
        await region.expect_no_message(0.1)
        get.reply_to.tell(Replicator.NotFound(get.key))
        # both were dropped, not stashed: the region retries
        await replicator.expect_no_message(0.1)
        await region.expect_no_message(0.1)
        await registered(coordinator, replicator, region)
        await replicator.expect_no_message(0.1)  # no allocation for the early GetShardHome
        await region.expect_no_message(0.1)


async def test_a_registration_is_acked_only_after_its_write():
    async with manual_node() as node:
        replicator, region = node.probe(), node.probe()
        coordinator = await active(node, replicator)
        coordinator.tell(Register(region.ref))
        update = await expect_update(replicator)
        written = update.modify(update.initial).value
        assert written.region_nodes == {region.ref.path: node.unique_address}
        await region.expect_no_message(0.1)
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        await region.expect_message(RegisterAck(coordinator))


async def test_get_shard_home_for_a_known_shard_is_answered_during_an_update():
    async with manual_node() as node:
        replicator, r1, r2, asker = node.probe(), node.probe(), node.probe(), node.probe()
        coordinator = await active(node, replicator)
        await registered(coordinator, replicator, r1)
        coordinator.tell(GetShardHome("1", asker.ref))
        allocated = await expect_update(replicator)
        allocated.reply_to.tell(Replicator.UpdateSuccess(allocated.key))
        await asker.expect_message(ShardHome("1", r1.ref))
        await r1.expect_message(HostShard("1", coordinator))

        coordinator.tell(Register(r2.ref))
        in_flight = await expect_update(replicator)
        await r2.expect_message(ShardHomes(((r1.ref, ("1",)),)))
        coordinator.tell(GetShardHome("1", asker.ref))
        await asker.expect_message(ShardHome("1", r1.ref))
        coordinator.tell(GetShardHome("2", asker.ref))
        await asker.expect_no_message(0.1)
        await replicator.expect_no_message(0.1)  # one update at a time

        in_flight.reply_to.tell(Replicator.UpdateSuccess(in_flight.key))
        await r2.expect_message(RegisterAck(coordinator))
        second = await expect_update(replicator)
        second.reply_to.tell(Replicator.UpdateSuccess(second.key))
        await asker.expect_message(ShardHome("2", r2.ref))


async def test_an_update_timeout_is_retried_with_pekkos_log_levels(
    caplog: pytest.LogCaptureFixture,
):
    async with manual_node() as node:
        replicator, region = node.probe(), node.probe()
        coordinator = await active(node, replicator)
        with caplog.at_level(logging.INFO, logger="ohra.actor"):
            coordinator.tell(Register(region.ref))
            update = await expect_update(replicator)
            for _ in range(5):
                update.reply_to.tell(Replicator.UpdateTimeout(update.key))
                update = await expect_update(replicator)
        assert levels(caplog, "unable to update a distributed state") == [
            "WARNING",
            "WARNING",
            "WARNING",
            "WARNING",
            "ERROR",
        ]
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        await region.expect_message(RegisterAck(coordinator))


async def test_a_modify_failure_restarts_the_coordinator():
    async with manual_node() as node:
        replicator, region = node.probe(), node.probe()
        coordinator = await active(node, replicator, supervised=True)
        coordinator.tell(Register(region.ref))
        update = await expect_update(replicator)
        update.reply_to.tell(Replicator.ModifyFailure(update.key, "boom", ValueError("boom")))
        await expect_get(replicator)  # the restarted coordinator reads its state again


async def test_a_restarted_coordinator_starts_from_the_replicated_state():
    """A restart starts from scratch, as a new DDataShardCoordinator: when the Replicator holds
    nothing, the restarted coordinator does not know the shard the failed one had allocated."""
    async with manual_node() as node:
        replicator, region, home = node.probe(), node.probe(), node.probe()
        coordinator = await active(node, replicator, supervised=True)
        coordinator.tell(Register(region.ref))
        update = await expect_update(replicator)
        stored = update.modify(update.initial)
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        coordinator.tell(GetShardHome("a", home.ref))
        update = await expect_update(replicator)
        update.modify(stored)
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        assert await home.receive_message(3) == ShardHome("a", region.ref)
        coordinator.tell(GetShardHome("b", home.ref))
        update = await expect_update(replicator)
        update.reply_to.tell(Replicator.ModifyFailure(update.key, "boom", ValueError("boom")))
        get = await expect_get(replicator)
        get.reply_to.tell(Replicator.NotFound(get.key))
        await settle(node)
        coordinator.tell(GetShardHome("a", home.ref))
        assert node.manual is not None
        await node.manual.time_passes(0.5)  # stateInitialized
        await home.expect_no_message(0.1)  # no region is registered, and "a" is unknown


async def test_terminate_during_an_update_stops_after_it():
    async with manual_node() as node:
        replicator, region = node.probe(), node.probe()
        coordinator = await active(node, replicator)
        coordinator.tell(Register(region.ref))
        update = await expect_update(replicator)
        coordinator.tell(_Terminate())
        await settle(node)
        update.reply_to.tell(Replicator.UpdateTimeout(update.key))
        await replicator.expect_terminated(coordinator)
        await replicator.expect_no_message(0.1)  # no retry


async def test_nothing_stashed_is_processed_after_the_local_region_lets_terminate_stop():
    """Terminate waits for the local region; its termination's write completes the stop, and a
    Register stashed during that write is not processed (Pekko's context.stop)."""
    async with manual_node() as node:
        replicator, region, late = node.probe(), node.probe(), node.probe()
        coordinator = await active(node, replicator)
        await registered(coordinator, replicator, region)
        coordinator.tell(_Terminate())  # deferred: the local region is alive
        region.stop()
        terminated = await expect_update(replicator)
        coordinator.tell(Register(late.ref))
        await settle(node)
        terminated.reply_to.tell(Replicator.UpdateSuccess(terminated.key))
        await replicator.expect_terminated(coordinator)
        await replicator.expect_no_message(0.1)


async def test_the_get_waits_out_a_long_waiting_for_state_timeout():
    """The adapter's ask timeout covers the Get's own timeout, so a late answer is taken."""
    async with manual_node() as node:
        assert node.manual is not None
        replicator, region = node.probe(), node.probe()
        settings = replace(SETTINGS, waiting_for_state_timeout=10.0)
        coordinator = node.coordinator(replicator, settings=settings)
        get = await expect_get(replicator)
        await node.manual.time_passes(9.0)
        await replicator.expect_no_message(0.1)  # no retry from the ask's own timeout
        get.reply_to.tell(Replicator.NotFound(get.key))
        await settle(node)
        coordinator.tell(Register(region.ref))
        await expect_update(replicator)


async def test_a_loaded_region_of_a_gone_incarnation_is_terminated():
    """D5: the path is at this node's address, a member, but its uid is another incarnation's."""
    async with manual_node() as node:
        replicator = node.probe()
        me = node.unique_address
        gone = UniqueAddress(me.host, me.port, me.uid + 1, me.name)
        path = f"ohra://c@127.0.0.1:{me.port}/system/sharding/Counter"
        state = CoordinatorState.EMPTY.updated(ShardRegionRegistered(path, gone)).updated(
            ShardHomeAllocated("1", path)
        )
        loaded = node.loaded(state)
        node.coordinator(replicator)
        get = await expect_get(replicator)
        get.reply_to.tell(Replicator.GetSuccess(get.key, loaded))
        update = await expect_update(replicator)
        after = update.modify(loaded).value
        assert after.regions == {} and after.shards == {} and after.region_nodes == {}


async def test_two_gone_regions_are_written_one_update_after_the_other():
    """watchStateActors terminates both; the second write waits for the first, so it holds both
    changes."""
    async with manual_node() as node:
        replicator = node.probe()
        me = node.unique_address
        gone = UniqueAddress(me.host, me.port, me.uid + 1, me.name)
        root = f"ohra://c@127.0.0.1:{me.port}/system/sharding"
        state = CoordinatorState.EMPTY
        for path in (f"{root}/A", f"{root}/B"):
            state = state.updated(ShardRegionRegistered(path, gone))
        loaded = node.loaded(state)
        node.coordinator(replicator)
        get = await expect_get(replicator)
        get.reply_to.tell(Replicator.GetSuccess(get.key, loaded))
        first = await expect_update(replicator)
        assert set(first.modify(loaded).value.regions) == {f"{root}/B"}
        await replicator.expect_no_message(0.1)
        first.reply_to.tell(Replicator.UpdateSuccess(first.key))
        second = await expect_update(replicator)
        assert second.modify(loaded).value == CoordinatorState.EMPTY


async def test_state_initialized_sends_host_shard_for_every_allocated_shard():
    async with manual_node() as node:
        replicator, region = node.probe(), node.probe()
        path = region.ref.path
        state = CoordinatorState.EMPTY.updated(ShardRegionRegistered(path, node.unique_address))
        state = state.updated(ShardHomeAllocated("1", path)).updated(ShardHomeAllocated("2", path))
        coordinator = node.coordinator(replicator)
        get = await expect_get(replicator)
        get.reply_to.tell(Replicator.GetSuccess(get.key, node.loaded(state)))
        await replicator.expect_no_message(0.1)  # the live region is kept
        await region.expect_no_message(0.1)
        assert node.manual is not None
        await node.manual.time_passes(0.5)
        homes = await region.receive_messages(2)
        assert set(homes) == {HostShard("1", coordinator), HostShard("2", coordinator)}


async def test_state_initialized_stashed_behind_an_update_unstashes_without_a_restart():
    """_StateInitialized is replayed from the stash and unstashes again: the outer replay must
    not fail on the emptied stash, which would restart the coordinator."""
    async with manual_node() as node:
        replicator, region, home = node.probe(), node.probe(), node.probe()
        me = node.unique_address
        gone = UniqueAddress(me.host, me.port, me.uid + 1, me.name)
        gone_path = f"ohra://c@127.0.0.1:{me.port}/system/sharding/Gone"
        live = region.ref.path
        state = (
            CoordinatorState.EMPTY.updated(ShardRegionRegistered(gone_path, gone))
            .updated(ShardRegionRegistered(live, me))
            .updated(ShardHomeAllocated("1", live))
        )
        coordinator = node.coordinator(replicator, supervised=True)
        get = await expect_get(replicator)
        get.reply_to.tell(Replicator.GetSuccess(get.key, node.loaded(state)))
        update = await expect_update(replicator)  # the gone region's termination
        assert node.manual is not None
        await node.manual.time_passes(0.6)  # _StateInitialized is stashed behind the update
        coordinator.tell(ShardStarted("1"))
        coordinator.tell(Register(region.ref))
        await settle(node)
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        await region.fish_for_message(
            3,
            lambda m: (
                FishingOutcomes.complete()
                if isinstance(m, RegisterAck)
                else FishingOutcomes.continue_and_ignore()
            ),
        )
        coordinator.tell(GetShardHome("1", home.ref))
        assert await home.receive_message(1) == ShardHome("1", region.ref)
        await replicator.expect_no_message(0.1)  # no restart, so no second Get


async def test_after_a_slow_first_write_the_stash_replays_in_arrival_order():
    """Pekko's unbecomeAfterUpdate unstashAll()s, which prepends the classic stash to the mailbox:
    what a replayed Register's own update stashes again keeps its place ahead of later messages."""
    async with manual_node() as node:
        replicator = node.probe()
        a, b, c, d = (node.probe() for _ in range(4))
        names = {a.ref.path: "A", b.ref.path: "B", c.ref.path: "C", d.ref.path: "D"}
        coordinator = await active(node, replicator)
        coordinator.tell(Register(a.ref))
        update = await expect_update(replicator)  # held
        coordinator.tell(Register(b.ref))
        coordinator.tell(RegionStopped(b.ref.path))
        coordinator.tell(Register(c.ref))
        await settle(node)
        updates = [update]
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        update = await expect_update(replicator)  # the replayed Register(B)'s, held
        updates.append(update)
        coordinator.tell(Register(d.ref))
        await settle(node)
        for _ in range(3):
            update.reply_to.tell(Replicator.UpdateSuccess(update.key))
            update = await expect_update(replicator)
            updates.append(update)
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        await replicator.expect_no_message(0.1)

        events: list[str] = []
        register = updates[0].initial
        for u in updates:
            before = set(register.value.regions)
            register = u.modify(register)
            after = set(register.value.regions)
            events += [f"{names[p]} registered" for p in after - before]
            events += [f"{names[p]} terminated" for p in before - after]
        assert events == [
            "A registered",
            "B registered",
            "B terminated",
            "C registered",
            "D registered",
        ]


# --- the review focus, on real nodes -------------------------------------------------------------

DDATA = replace(FAST_SHARDING, waiting_for_state_timeout=0.3, updating_state_timeout=0.3)
KEY = "Counter"


async def stored_state(system: ActorSystem[Any], type_name: str) -> CoordinatorState:
    """What the coordinator wrote, read from sharding's own Replicator."""
    key = LWWRegisterKey[CoordinatorState](f"{type_name}CoordinatorState")
    replicator = ClusterSharding(system)._replicator  # pyright: ignore[reportPrivateUsage]
    assert replicator is not None
    reply: Replicator.GetResponse[Register_] = await ask(
        replicator, lambda r: Replicator.Get(key, Replicator.ReadLocal(), r), timeout=2
    )
    assert isinstance(reply, Replicator.GetSuccess)
    return reply.get(key).value


async def test_an_unreachable_node_stalls_new_allocations_until_it_is_removed():
    """Pekko's write-majority-plus 3 needs every node of three (a majority of two plus three): a
    known shard is still answered, a new one waits until the unreachable node is removed."""
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start(n, settings=DDATA) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 6)
        known = shard_id_of(next(e for e, i in where.items() if i != 2), 10)
        [fresh] = ids_on_new_shards(1, {shard_id_of(e, 10) for e in where})
        fresh_shard = shard_id_of(fresh, 10)
        coordinator = nodes[0].resolve(
            f"ohra://c@127.0.0.1:{p1}/system/singletonManagerCounterCoordinator/singleton"
        )
        home = TestProbe[ShardHome](Node(nodes[0]))
        await crash(nodes[2])
        await eventually(lambda: unreachable_ports(nodes[0]) == {p3}, timeout=5)
        coordinator.tell(GetShardHome(fresh_shard, home.ref))
        coordinator.tell(GetShardHome(known, home.ref))
        assert (await home.receive_message(2)).shard == known
        await home.expect_no_message(1.0)
        Cluster(nodes[0]).manager.tell(Down(Address("c", "127.0.0.1", p3)))
        await eventually(lambda: all_up(nodes[0], 2) and all_up(nodes[1], 2), timeout=5)
        assert (await home.receive_message(5)).shard == fresh_shard


async def test_a_hundred_new_shards_at_once_are_each_allocated_once():
    settings = replace(DDATA, number_of_shards=100)
    ids: list[str] = []
    taken: set[str] = set()
    i = 0
    while len(ids) < 100:
        shard = shard_id_of(f"e{i}", 100)
        if shard not in taken:
            taken.add(shard)
            ids.append(f"e{i}")
        i += 1
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        start(a, settings=settings)
        start(b, settings=settings)
        replies = await asyncio.gather(
            *(ask(ref(a, e), lambda r: Count(1, r), timeout=15) for e in ids)
        )
        assert replies == [1] * 100
        state = await stored_state(a, KEY)
        assert len(state.shards) == 100
        listed = [s for shards in state.regions.values() for s in shards]
        assert sorted(listed) == sorted(state.shards)
        assert all(state.shards[s] == r for r, shards in state.regions.items() for s in shards)


async def test_a_rejoined_node_at_the_same_address_serves_its_entities_again():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [
            await stack.enter_async_context(node(p, [p1], retry_unsuccessful_join_after=0.1))
            for p in (p1, p2)
        ]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        regions = [start(a, settings=DDATA), start(b, settings=DDATA)]
        where = await ids_hosted_by(regions, [a, b], 12)
        on_b = [e for e, i in where.items() if i == 1]
        assert on_b
        await crash(b)
        await eventually(lambda: unreachable_ports(a) == {p2}, timeout=5)
        Cluster(a).manager.tell(Down(Address("c", "127.0.0.1", p2)))
        await eventually(lambda: all_up(a, 1), timeout=5)
        b2 = await stack.enter_async_context(node(p2, [p1], retry_unsuccessful_join_after=0.1))
        await eventually(lambda: all_up(a, 2) and all_up(b2, 2), timeout=8)
        start(b2, settings=DDATA)
        coordinator = a.resolve(
            f"ohra://c@127.0.0.1:{p1}/system/singletonManagerCounterCoordinator/singleton"
        )
        coordinator.tell(Register(cast(Any, "not a region")))  # fails it: it restarts
        for e in on_b:
            assert await ask(ref(a, e), lambda r: Count(1, r), timeout=5) == 1


async def test_two_entity_types_keep_separate_states():
    p = free_port()
    async with node(p, [p]) as a:
        await eventually(lambda: all_up(a, 1), timeout=5)
        start(a, settings=DDATA)
        start(a, COUNTER2, DDATA)
        assert await ask(ref(a, "x"), lambda r: Count(1, r), timeout=5) == 1
        assert await ask(ref(a, "y", COUNTER2), lambda r: Count(1, r), timeout=5) == 1
        first, second = await stored_state(a, KEY), await stored_state(a, "Counter2")
        assert set(first.shards) == {shard_id_of("x", 10)}
        assert set(second.shards) == {shard_id_of("y", 10)}
        assert all(r.endswith("/Counter") for r in first.regions)
        assert all(r.endswith("/Counter2") for r in second.regions)
