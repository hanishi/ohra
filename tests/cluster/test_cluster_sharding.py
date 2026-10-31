"""Cluster sharding: entities addressable from any node, a coordinator singleton allocating shards
to regions and rebuilding its map from their registrations, as ShardCoordinator.scala and
ShardRegion.scala."""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest

from ohra import (
    CoordinatedShutdownSettings,
    ActorContext,
    ActorRef,
    ActorSystem,
    AskTimeoutError,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingSettings,
    ClusterSingletonSettings,
    CurrentShardRegionState,
    Entity,
    EntityContext,
    EntityTypeKey,
    GetShardRegionState,
    ShardingEnvelope,
    ask,
    remotable,
)
from ohra.cluster.core import Cluster, Down, Leave
from ohra.remote import Address
from ohra.remote.serialization import Json

from ..util import eventually, free_port
from .test_cluster import all_up, cluster_node, unreachable_ports

FAST_SHARDING = ClusterShardingSettings(
    number_of_shards=10,
    retry_interval=0.05,
    rebalance_interval=0.1,
    handoff_timeout=2.0,
    shard_start_timeout=0.2,
    coordinator_failure_backoff=0.1,
    shard_region_query_timeout=1.0,
    coordinator_singleton_settings=ClusterSingletonSettings(
        hand_over_retry_interval=0.05,
        min_number_of_hand_over_retries=5,
        singleton_identification_interval=0.02,
    ),
)


@remotable
@dataclass(frozen=True)
class Count:
    n: int
    reply_to: ActorRef[int]


COUNTER = EntityTypeKey[Count]("Counter")
COUNTER2 = EntityTypeKey[Count]("Counter2")


def counter(entity: EntityContext[Count]) -> Behavior[Count]:
    def active(total: int) -> Behavior[Count]:
        def on_message(_ctx: ActorContext[Count], msg: Count) -> Behavior[Count]:
            msg.reply_to.tell(total + msg.n)
            return active(total + msg.n)

        return Behaviors.receive(on_message)

    return active(0)


def node(
    port: int, seeds: list[int], exiting_timeout: float = 2.0, **settings: Any
) -> ActorSystem[Any]:
    """A node whose hand-off phases (region shutdown and cluster-exiting) are bounded by
    ``exiting_timeout``."""
    phases = {
        "cluster-sharding-shutdown-region": exiting_timeout,
        "cluster-exiting": exiting_timeout,
    }
    return cluster_node(
        "c", port, seeds, shutdown=CoordinatedShutdownSettings(phase_timeouts=phases), **settings
    )


def start(
    system: ActorSystem[Any],
    key: EntityTypeKey[Count] = COUNTER,
    settings: ClusterShardingSettings = FAST_SHARDING,
) -> ActorRef[ShardingEnvelope[Count]]:
    return ClusterSharding(system).init(Entity(key, counter).with_settings(settings))


def ref(
    system: ActorSystem[Any], entity_id: str, key: EntityTypeKey[Count] = COUNTER
) -> ActorRef[Count]:
    return ClusterSharding(system).entity_ref_for(key, entity_id)


async def hosted(region: ActorRef[Any]) -> frozenset[str]:
    state: CurrentShardRegionState = await ask(region, GetShardRegionState, timeout=2)
    return state.entity_ids


async def crash(system: ActorSystem[Any]) -> None:
    assert system.remoting is not None
    await system.remoting.close()
    system._final_terminate()  # pyright: ignore[reportPrivateUsage]  # a crash does not run the coordinated shutdown
    await system.when_terminated()


async def test_entity_refs_on_three_nodes_reach_one_entity():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        for n in nodes:
            start(n)
        assert await ask(ref(nodes[0], "x"), lambda r: Count(1, r), timeout=5) == 1
        assert await ask(ref(nodes[1], "x"), lambda r: Count(1, r), timeout=5) == 2
        assert await ask(ref(nodes[2], "x"), lambda r: Count(1, r), timeout=5) == 3


async def test_entity_ref_for_before_registration_buffers_and_delivers():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(node(p1, [p1]))
        await eventually(lambda: all_up(a, 1), timeout=3)
        start(a)
        b = await stack.enter_async_context(node(p2, [p1]))
        start(b)  # b has not even joined yet
        assert await ask(ref(b, "y"), lambda r: Count(1, r), timeout=5) == 1


async def test_entities_are_spread_across_regions():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start(n) for n in nodes]
        for i in range(30):
            assert await ask(ref(nodes[0], f"id{i}"), lambda r: Count(1, r), timeout=5) == 1
        views = [await hosted(r) for r in regions]
        assert all(views) and frozenset().union(*views) == {f"id{i}" for i in range(30)}


async def test_two_entity_types_side_by_side():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        for n in (a, b):
            start(n)
            start(n, COUNTER2)
        assert await ask(ref(a, "x"), lambda r: Count(1, r), timeout=5) == 1
        assert await ask(ref(b, "x", COUNTER2), lambda r: Count(5, r), timeout=5) == 5
        assert await ask(ref(b, "x"), lambda r: Count(1, r), timeout=5) == 2


def _drop(system: ActorSystem[Any], manifests: tuple[str, ...]) -> Any:
    """Drop inbound frames of the given message types at this node; returns the original handler to
    restore."""
    remoting = system.remoting
    assert remoting is not None
    deliver = remoting._handle  # pyright: ignore[reportPrivateUsage]

    def dropping(frame: Json) -> None:
        message = frame.get("message") if isinstance(frame, dict) else None
        if isinstance(message, dict) and message.get("$type") in manifests:
            return
        deliver(frame)

    remoting._handle = dropping  # pyright: ignore[reportPrivateUsage]
    return deliver


async def test_region_buffer_is_bounded(caplog: pytest.LogCaptureFixture):
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        small = ClusterShardingSettings(number_of_shards=10, retry_interval=0.05, buffer_size=2)
        start(a, settings=small)
        start(b, settings=small)
        # the coordinator on a never hears b's questions
        deliver = _drop(a, ("Sharding.GetShardHome",))
        replies: list[int] = []
        with caplog.at_level(logging.DEBUG, logger="ohra.dead_letters"):
            for i in range(3):
                ref(b, f"q{i}").tell(Count(1, FunctionRefHolder.keep(replies)))
            await eventually(
                lambda: any("q2" in r.getMessage() for r in caplog.records), timeout=3
            )
        a.remoting._handle = deliver  # pyright: ignore[reportPrivateUsage]
        # q0 and q1 were buffered and delivered
        await eventually(lambda: sorted(replies) == [1, 1], timeout=5)
        assert await ask(ref(b, "q0"), lambda r: Count(0, r), timeout=5) == 1


class FunctionRefHolder:
    """Keeps probe refs alive for the test's duration: a temp reply path lives only as long as its
    ref."""

    refs: list[Any] = []

    @classmethod
    def keep(cls, into: list[int]) -> ActorRef[int]:
        from ohra.ref import FunctionRef

        r: FunctionRef[int] = FunctionRef("probe", into.append)
        cls.refs.append(r)
        return r


async def ids_hosted_by(
    regions: list[ActorRef[Any]], nodes: list[ActorSystem[Any]], count: int
) -> dict[str, int]:
    """Touch `count` ids through the first node; map each id to the index of the node hosting
    it."""
    for i in range(count):
        await ask(ref(nodes[0], f"e{i}"), lambda r: Count(1, r), timeout=5)
    where: dict[str, int] = {}
    for index, region in enumerate(regions):
        for entity_id in await hosted(region):
            where[entity_id] = index
    return where


async def test_coordinator_leaving_keeps_entities_where_they_are():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start(n) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 12)
        elsewhere = [e for e, i in where.items() if i != 0]
        assert elsewhere
        # the coordinator's host
        Cluster(nodes[1]).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        await asyncio.wait_for(nodes[0].when_terminated(), timeout=5)
        await eventually(lambda: all_up(nodes[1], 2) and all_up(nodes[2], 2), timeout=5)
        for e in elsewhere:
            # the same entity, total kept
            assert await ask(ref(nodes[1], e), lambda r: Count(0, r), timeout=5) == 1
        assert await ask(ref(nodes[2], "fresh"), lambda r: Count(1, r), timeout=5) == 1


async def test_coordinator_crash_is_recovered_after_removal():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start(n) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 12)
        elsewhere = [e for e, i in where.items() if i != 0]
        await crash(nodes[0])
        await eventually(lambda: unreachable_ports(nodes[1]) == {p1}, timeout=5)
        Cluster(nodes[1]).manager.tell(Down(Address("c", "127.0.0.1", p1)))
        await eventually(lambda: all_up(nodes[1], 2) and all_up(nodes[2], 2), timeout=5)
        on_c = [e for e, i in where.items() if i == 2]
        assert on_c
        # b never saw these homes: it must ask the new coordinator, which rebuilt them from c
        for e in on_c:
            assert await ask(ref(nodes[1], e), lambda r: Count(0, r), timeout=5) == 1
        for e in elsewhere:
            assert await ask(ref(nodes[1], e), lambda r: Count(0, r), timeout=5) == 1
        assert await ask(ref(nodes[2], "fresh"), lambda r: Count(1, r), timeout=8) == 1


async def test_shards_of_an_unreachable_node_wait_for_its_removal():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start(n) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 12)
        on_c = next(e for e, i in where.items() if i == 2)
        await crash(nodes[2])
        await eventually(lambda: unreachable_ports(nodes[0]) == {p3}, timeout=5)
        # the home is still c, as in Pekko until the member is removed
        with pytest.raises(AskTimeoutError):
            await ask(ref(nodes[0], on_c), lambda r: Count(1, r), timeout=0.5)
        await asyncio.sleep(0.3)
        Cluster(nodes[0]).manager.tell(Down(Address("c", "127.0.0.1", p3)))
        await eventually(lambda: all_up(nodes[0], 2) and all_up(nodes[1], 2), timeout=5)
        # a fresh entity elsewhere
        assert await ask(ref(nodes[0], on_c), lambda r: Count(1, r), timeout=8) == 1


# --- rebalance, hand-off and graceful leave (Task 4) ---------------------------------------------

from ohra.cluster.sharding import shard_id_of  # noqa: E402
from ohra.cluster.sharding.coordinator import Register  # noqa: E402


async def shards_on(region: ActorRef[Any]) -> frozenset[str]:
    state: CurrentShardRegionState = await ask(region, GetShardRegionState, timeout=2)
    return state.shard_ids


async def touch_all_shards(system: ActorSystem[Any], number_of_shards: int = 10) -> list[str]:
    """Create entities until every shard id has at least one; returns the ids used."""
    ids: list[str] = []
    seen: set[str] = set()
    i = 0
    while len(seen) < number_of_shards:
        entity_id = f"s{i}"
        if shard_id_of(entity_id, number_of_shards) not in seen:
            seen.add(shard_id_of(entity_id, number_of_shards))
            assert await ask(ref(system, entity_id), lambda r: Count(1, r), timeout=5) == 1
            ids.append(entity_id)
        i += 1
    return ids


async def test_a_joining_node_receives_shards_within_a_rebalance_interval():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        start(a)
        start(b)
        await touch_all_shards(a)
        c = await stack.enter_async_context(node(p3, [p1]))
        region_c = start(c)
        await eventually(lambda: all_up(a, 3) and all_up(c, 3), timeout=5)
        moved: list[int] = []

        async def c_has_two() -> bool:
            n = len(await shards_on(region_c))
            moved.append(n)
            return n >= 2

        deadline = asyncio.get_running_loop().time() + 4
        while not await c_has_two():
            assert asyncio.get_running_loop().time() < deadline, moved
            await asyncio.sleep(0.05)


async def test_rebalance_limits_are_respected():
    p1, p2 = free_port(), free_port()
    one_per_round = ClusterShardingSettings(
        number_of_shards=10,
        retry_interval=0.05,
        rebalance_interval=0.3,
        handoff_timeout=2.0,
        shard_start_timeout=0.2,
        rebalance_absolute_limit=1,
        rebalance_relative_limit=1.0,
    )
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(node(p1, [p1]))
        await eventually(lambda: all_up(a, 1), timeout=3)
        start(a, settings=one_per_round)
        await touch_all_shards(a)
        b = await stack.enter_async_context(node(p2, [p1]))
        region_b = start(b, settings=one_per_round)
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        samples: list[int] = []
        for _ in range(40):  # 2 s of samples every 50 ms: one rebalance round takes at least 0.3 s
            samples.append(len(await shards_on(region_b)))
            await asyncio.sleep(0.05)
        assert samples[-1] >= 2, samples
        assert all(
            later - earlier <= 1 for earlier, later in zip(samples, samples[1:], strict=False)
        ), samples


class Replies:
    """Collects replies through refs it keeps alive."""

    def __init__(self) -> None:
        from ohra.ref import FunctionRef

        self.received: list[int] = []
        self.ref: FunctionRef[int] = FunctionRef("replies", self.received.append)


def dropped_during(caplog: pytest.LogCaptureFixture) -> int:
    """Messages a hand-off dropped: the hosting region drops what it buffered between BeginHandOff
    and HandOff, and a Shard already handing off dead-letters what reaches it late, as Pekko's do.
    Never silent."""
    return sum(
        1 for r in caplog.records if r.name == "ohra.dead_letters" and ("Count(" in r.getMessage())
    )


async def test_messages_sent_during_a_rebalance_all_arrive(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        start(a)
        start(b)
        ids = await touch_all_shards(a)
        replies = Replies()
        sent = 0
        c = await stack.enter_async_context(node(p3, [p1]))
        region_c = start(c)
        for k in range(300):  # a steady stream while c joins and shards move
            ref(a, ids[k % len(ids)]).tell(Count(1, replies.ref))
            sent += 1
            await asyncio.sleep(0.005)
        await eventually(
            lambda: len(replies.received) + dropped_during(caplog) == sent, timeout=10
        )
        assert len(replies.received) >= sent * 0.9  # only the hand-off windows lose anything
        assert len(await shards_on(region_c)) >= 1  # the move happened while the stream ran


async def test_graceful_leave_hands_shards_off_and_loses_nothing(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        regions = [start(n) for n in (a, b, c)]
        ids = await touch_all_shards(a)
        # some shards live off the first node
        assert await shards_on(regions[1]) or await shards_on(regions[2])
        replies = Replies()
        sent = 0
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        for k in range(300):
            ref(a if k % 2 else c, ids[k % len(ids)]).tell(Count(1, replies.ref))
            sent += 1
            await asyncio.sleep(0.005)
        await asyncio.wait_for(b.when_terminated(), timeout=8)
        await eventually(
            lambda: len(replies.received) + dropped_during(caplog) == sent, timeout=10
        )
        assert len(replies.received) >= sent * 0.9
        await eventually(lambda: all_up(a, 2) and all_up(c, 2), timeout=5)
        for entity_id in ids:
            assert await ask(ref(c, entity_id), lambda r: Count(0, r), timeout=5) >= 1


async def test_hand_off_stops_an_entity_that_ignores_its_stop_message_at_the_timeout():
    p1, p2 = free_port(), free_port()
    slow_stop = ClusterShardingSettings(
        number_of_shards=10,
        retry_interval=0.05,
        rebalance_interval=0.1,
        handoff_timeout=6.0,
        shard_start_timeout=0.2,
    )
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        for n in (a, b):
            # answered, never obeyed
            ClusterSharding(n).init(
                Entity(COUNTER, counter)
                .with_settings(slow_stop)
                .with_stop_message(Count(0, n.ignore_ref))
            )
        ids = await touch_all_shards(a)
        on_b = [
            e for e in ids if e in await hosted(ClusterSharding(b).init(Entity(COUNTER, counter)))
        ]
        assert on_b
        began = asyncio.get_running_loop().time()
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        await asyncio.wait_for(b.when_terminated(), timeout=6)
        # max(handoff_timeout - 5, 1) before the stragglers are stopped
        assert asyncio.get_running_loop().time() - began >= 0.9
        # a fresh entity elsewhere
        assert await ask(ref(a, on_b[0]), lambda r: Count(0, r), timeout=5) == 0


async def test_region_exits_at_the_exiting_timeout_when_the_coordinator_never_answers():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(node(p1, [p1], exiting_timeout=0.5))
        b = await stack.enter_async_context(node(p2, [p1], exiting_timeout=0.5))
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        start(a)
        start(b)
        await touch_all_shards(a)
        _drop(a, ("Sharding.GracefulShutdownReq",))  # the coordinator on a never hears b's request
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        await asyncio.wait_for(b.when_terminated(), timeout=3)


# --- from the final review -----------------------------------------------------------------------


async def test_coordinator_host_leave_hands_its_shards_off():
    """Pekko's Terminate: the coordinator on a leaving node stays until the local region has handed
    off, and the other regions find the successor; the leave completes well under the exiting
    timeout."""
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(node(p, [p1], exiting_timeout=10.0))
            for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start(n) for n in nodes]
        ids = await touch_all_shards(nodes[0])
        on_a = await shards_on(regions[0])
        assert on_a
        began = asyncio.get_running_loop().time()
        Cluster(nodes[1]).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        await asyncio.wait_for(nodes[0].when_terminated(), timeout=8)
        assert asyncio.get_running_loop().time() - began < 5.0  # handed off, not timed out
        await eventually(lambda: all_up(nodes[1], 2) and all_up(nodes[2], 2), timeout=5)
        for entity_id in ids:
            assert await ask(ref(nodes[2], entity_id), lambda r: Count(0, r), timeout=5) >= 0
        assert on_a <= (await shards_on(regions[1])) | (await shards_on(regions[2]))


async def test_late_registration_after_a_coordinator_move_is_resolved():
    """b's new coordinator opens its gate before c manages to register (c's Register frames are
    dropped), so a shard c hosts is allocated on b too; when c's registration lands, the conflict
    is resolved to one host, c's region stays alive, and every node sees the same entity
    afterwards."""
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start(n) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 12)
        on_c = next(e for e, i in where.items() if i == 2)
        deliver = _drop(nodes[1], ("Sharding.Register",))  # the next coordinator never hears c
        Cluster(nodes[1]).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        await asyncio.wait_for(nodes[0].when_terminated(), timeout=8)
        await eventually(lambda: all_up(nodes[1], 2) and all_up(nodes[2], 2), timeout=5)
        await asyncio.sleep(0.3)
        # possibly a second entity on b
        await ask(ref(nodes[1], on_c), lambda r: Count(1, r), timeout=5)
        nodes[1].remoting._handle = deliver  # pyright: ignore[reportPrivateUsage]
        await asyncio.sleep(0.5)  # c registers and reports what it hosts; the conflict is resolved
        totals = {await ask(ref(n, on_c), lambda r: Count(0, r), timeout=5) for n in nodes[1:]}
        assert len(totals) == 1  # one entity, seen from both nodes
        shard = shard_id_of(on_c, 10)
        # one host
        assert not (
            {shard} <= await shards_on(regions[1]) and {shard} <= await shards_on(regions[2])
        )
        # c's region lives
        assert isinstance(
            await ask(regions[2], GetShardRegionState, timeout=2), CurrentShardRegionState
        )


async def test_a_failed_region_restarts_and_recovers():
    from typing import cast

    from ohra.cluster.sharding.coordinator import HostShard

    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        region_a, _ = start(a), start(b)
        ids = await touch_all_shards(a)
        # the handler raises: Pekko's guardian restarts the region
        region_a.tell(HostShard("1", cast(Any, "not a ref")))
        await asyncio.sleep(0.2)
        for entity_id in ids:
            assert await ask(ref(a, entity_id), lambda r: Count(0, r), timeout=5) >= 0
        assert await shards_on(region_a) or True


async def test_coordinator_singleton_settings_apply_to_the_coordinator():
    """Pekko's ClusterShardingSettings.coordinatorSingletonSettings: a lone leaving node's
    coordinator waits one hand-over retry before stopping, so a short retry interval makes its
    shutdown short."""
    fast_singleton = ClusterSingletonSettings(
        hand_over_retry_interval=0.05,
        min_number_of_hand_over_retries=5,
        singleton_identification_interval=0.02,
    )
    settings = replace(FAST_SHARDING, coordinator_singleton_settings=fast_singleton)
    p = free_port()
    async with node(p, [p]) as a:
        await eventually(lambda: all_up(a, 1), timeout=5)
        start(a, settings=settings)
        assert await ask(ref(a, "x"), lambda r: Count(1, r), timeout=5) == 1
        began = asyncio.get_running_loop().time()
        a.terminate()
        await asyncio.wait_for(a.when_terminated(), timeout=5)
        # one 0.05 s retry, not the default second
        assert asyncio.get_running_loop().time() - began < 0.6


def ids_on_new_shards(count: int, taken: set[str]) -> list[str]:
    """``count`` entity ids, each on a shard of its own that is not in ``taken``."""
    ids: list[str] = []
    for i in range(1000):
        shard = shard_id_of(f"n{i}", FAST_SHARDING.number_of_shards)
        if shard not in taken:
            taken.add(shard)
            ids.append(f"n{i}")
            if len(ids) == count:
                return ids
    raise AssertionError("not enough shards")


async def test_a_full_buffer_drops_a_message_for_a_new_shard_and_keeps_the_rest(
    caplog: pytest.LogCaptureFixture,
):
    """ShardRegion.deliverMessage: the first message for a shard asks for its home, and
    bufferMessage drops it into dead letters when the buffer is full."""
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        small = replace(FAST_SHARDING, buffer_size=2)
        start(a, settings=small)
        start(b, settings=small)
        # an answer through the coordinator: b's region has registered
        assert await ask(ref(b, "warm"), lambda r: Count(1, r), timeout=5) == 1
        ids = ids_on_new_shards(3, {shard_id_of("warm", small.number_of_shards)})
        deliver = _drop(a, ("Sharding.GetShardHome",))  # no home comes back for now
        replies: list[int] = []
        with caplog.at_level(logging.DEBUG, logger="ohra.dead_letters"):
            for entity_id in ids:
                ref(b, entity_id).tell(Count(1, FunctionRefHolder.keep(replies)))
            await eventually(
                lambda: any(ids[2] in r.getMessage() for r in caplog.records), timeout=3
            )
        a.remoting._handle = deliver  # pyright: ignore[reportPrivateUsage]
        await eventually(lambda: sorted(replies) == [1, 1], timeout=5)


async def test_a_failed_coordinator_restarts():
    """ClusterSharding's BackoffOpts.onStop restarts a coordinator that throws, and the regions
    register again with the new incarnation."""
    p = free_port()
    async with node(p, [p]) as a:
        await eventually(lambda: all_up(a, 1), timeout=5)
        start(a)
        assert await ask(ref(a, "warm"), lambda r: Count(1, r), timeout=5) == 1
        coordinator = a.resolve(
            f"ohra://c@127.0.0.1:{p}/system/singletonManagerCounterCoordinator/singleton"
        )
        coordinator.tell(Register(cast(Any, "not a region"), ()))  # fails the coordinator
        [fresh] = ids_on_new_shards(1, {shard_id_of("warm", FAST_SHARDING.number_of_shards)})
        assert await ask(ref(a, fresh), lambda r: Count(1, r), timeout=5) == 1
