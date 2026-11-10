"""The cluster receptionist actor on a single node (and two for the non-leader), after Pekko's
ClusterReceptionistSpec and the review focus of the port. Timers run on ManualTime where one is
involved; the registry changes come back through the Replicator, so ``sync`` lets its notify
interval pass."""

import asyncio
import gc
import pytest
import time
import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager, AsyncExitStack
from dataclasses import dataclass, field
from typing import Any

from ohra.behavior import Behavior, Behaviors
from ohra.context import ActorContext
from ohra.cluster import ClusterSettings
from ohra.cluster.core import Cluster, Leave
from ohra.cluster.ddata import ORMultiMap, ORMultiMapKey, Replicator, ReplicatorSettings
from ohra.cluster.receptionist import ClusterReceptionistSettings
from ohra.cluster.receptionist.cluster_receptionist import NAME
from ohra.cluster.receptionist.registry import (
    DDataKey,
    Entry,
    ServiceRegistry,
    ShardedServiceRegistry,
)
from ohra.eventstream import DeadLetter, EventStream
from ohra.receptionist import (
    Deregister,
    Deregistered,
    Find,
    Listing,
    Receptionist,
    Register,
    Registered,
    ServiceKey,
    Subscribe,
)
from ohra.ref import ActorRef
from ohra.remote import Address
from ohra.remote.remoting import EmptyLocalActorRef
from ohra.system import ActorSystem
from ohra.testkit import ManualTime, TestProbe
from ohra.testkit.manual_time import ExplicitlyTriggeredScheduler
from ohra.testkit.settings import TestKitSettings

from ...util import errors_logged, eventually, free_port
from ..test_cluster import FAST, FAST_SHUTDOWN, all_up

NOTIFY = 0.05
KEY = ServiceKey[str]("svc")
OTHER = ServiceKey[str]("other")
DDATA_KEYS = ShardedServiceRegistry.create(5)


@dataclass
class Node:
    system: ActorSystem[Any]
    receptionist: ActorRef[Any]
    manual: ManualTime | None
    settings: TestKitSettings = field(default_factory=TestKitSettings)

    def probe[M](self) -> TestProbe[M]:  # pyright: ignore[reportInvalidTypeVarUse]
        return TestProbe[M](self)

    @property
    def cluster(self) -> Cluster:
        return Cluster(self.system)

    @property
    def replicator(self) -> ActorRef[Replicator.Command]:
        cell = self.system._system_root.children[NAME].children["replicator"]  # pyright: ignore[reportPrivateUsage]
        return cell.ref

    def spawn(self, behavior: Behavior[Any], name: str) -> ActorRef[Any]:
        return self.system._root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]

    async def sync(self) -> None:
        """Let the Replicator's notify interval pass, so its changes reach the receptionist."""
        if self.manual is not None:
            await self.manual.time_passes(NOTIFY)
        else:
            await asyncio.sleep(NOTIFY)

    async def until(self, cond: Callable[[], Any], attempts: int = 100) -> None:
        for _ in range(attempts):
            if await cond():
                return
            await self.sync()
        raise AssertionError("condition not met")

    async def find(self, key: ServiceKey[Any] = KEY) -> Listing[Any]:
        probe: TestProbe[Listing[Any]] = self.probe()
        self.receptionist.tell(Find(key, probe.ref))
        return await probe.receive_message()

    async def listed(self, key: ServiceKey[Any], refs: set[ActorRef[Any]]) -> None:
        async def cond() -> bool:
            return (await self.find(key)).service_instances == refs

        await self.until(cond)

    async def ddata(self, key: ServiceKey[Any]) -> ServiceRegistry:
        ddata_key = DDATA_KEYS.ddata_key_for(key)
        probe: TestProbe[Replicator.GetResponse[Any]] = self.probe()
        self.replicator.tell(Replicator.Get(ddata_key, Replicator.ReadLocal(), probe.ref))
        match await probe.receive_message():
            case Replicator.GetSuccess() as ok:
                return ServiceRegistry(ok.get(ddata_key))
            case _:
                return ServiceRegistry(ORMultiMap.empty)

    def update(self, key: ServiceKey[Any], modify: Callable[[ServiceRegistry], ServiceRegistry]):
        """Write to the receptionist's Replicator directly, as another node's gossip would."""
        self.replicator.tell(
            Replicator.Update(
                DDATA_KEYS.ddata_key_for(key),
                ORMultiMap.empty,
                Replicator.WriteLocal(),
                self.system.ignore_ref,
                lambda m: modify(ServiceRegistry(m)).to_or_multi_map(),
            )
        )

    @property
    def self_node(self) -> Any:
        from ohra.cluster.ddata import SelfUniqueAddress

        return SelfUniqueAddress(self.cluster.self_unique_address)


@dataclass(frozen=True)
class Stop: ...


@Behaviors.receive_message
def service(msg: Any) -> Behavior[Any]:
    return Behaviors.stopped() if isinstance(msg, Stop) else Behaviors.same


def forwarder(to: ActorRef[Any]) -> Behavior[Any]:
    @Behaviors.receive_message
    def on_message(msg: Any) -> Behavior[Any]:
        if isinstance(msg, Stop):
            return Behaviors.stopped()
        to.tell(msg)
        return Behaviors.same

    return on_message


def settings_for(**receptionist: Any) -> dict[str, Any]:
    return dict(
        distributed_data=ReplicatorSettings(notify_subscribers_interval=NOTIFY),
        receptionist=ClusterReceptionistSettings(**receptionist),
    )


@asynccontextmanager
async def manual_node(
    prune_gossip_tombstones_after: float = 86400.0, **receptionist: Any
) -> AsyncGenerator[Node]:
    """One joined node on ManualTime, with the system's own cluster receptionist."""
    port = free_port()
    system = ActorSystem(
        Behaviors.ignore,
        "r",
        address=("127.0.0.1", port),
        cluster=ClusterSettings(
            seed_nodes=(Address("r", "127.0.0.1", port),),
            prune_gossip_tombstones_after=prune_gossip_tombstones_after,
            **FAST,
            **settings_for(**receptionist),
        ),
        shutdown=FAST_SHUTDOWN,
        scheduler=ExplicitlyTriggeredScheduler(),
    )
    async with system:
        manual = ManualTime(system)
        while not all_up(system, 1):
            await manual.time_passes(0.02)
        ref = Receptionist(system).ref
        node = Node(system, ref, manual)
        while "replicator" not in system._system_root.children[NAME].children:  # pyright: ignore[reportPrivateUsage]
            await manual.time_passes(0.01)
        yield node


async def test_a_cluster_system_runs_the_cluster_receptionist_and_its_replicator():
    async with manual_node() as node:
        system = node.system
        assert Receptionist(system).ref.path.endswith("/system/clusterReceptionist")
        assert system.resolve(Receptionist(system).ref.path) is Receptionist(system).ref
        assert node.replicator.path.endswith("/system/clusterReceptionist/replicator")
        assert system.resolve(node.replicator.path) is node.replicator
        assert "receptionist" not in system._system_root.children  # pyright: ignore[reportPrivateUsage]


async def test_register_find_and_subscribe_on_one_node():
    async with manual_node() as node:
        registered: TestProbe[Registered[str]] = node.probe()
        a, b = node.spawn(service, "a"), node.spawn(service, "b")
        node.receptionist.tell(Register(KEY, a, registered.ref))
        await registered.expect_message(Registered(KEY, a))
        await node.listed(KEY, {a})
        assert (await node.find(KEY)) == Listing(KEY, frozenset({a}), frozenset({a}), True)

        events: TestProbe[Listing[str]] = node.probe()
        node.receptionist.tell(Subscribe(KEY, events.ref))
        await events.expect_message(Listing(KEY, frozenset({a}), frozenset({a}), True))
        node.receptionist.tell(Register(KEY, b))
        await node.sync()
        await events.expect_message(Listing(KEY, frozenset({a, b}), frozenset({a, b}), True))


async def test_a_deregister_of_a_never_registered_ref_is_logged_and_ignored():
    async with manual_node() as node:
        a = node.spawn(service, "a")
        replies: TestProbe[Deregistered[str]] = node.probe()
        with errors_logged("ohra.actor") as records:
            node.receptionist.tell(Deregister(KEY, a, replies.ref))
            found = await node.find()
        assert found.service_instances == frozenset()
        assert any("never been registered" in r.getMessage() for r in records)
        await replies.expect_no_message(0)


async def test_a_remote_register_is_logged_and_ignored():
    async with manual_node() as node:
        remote = node.system.resolve("ohra://r@127.0.0.1:1/user/elsewhere")
        with errors_logged("ohra.actor") as records:
            node.receptionist.tell(Register(KEY, remote))
            await node.sync()
            found = await node.find()
        assert found.all_service_instances == frozenset()
        assert any("Register of non-local" in r.getMessage() for r in records)


async def test_a_ref_under_two_keys_that_stops_leaves_both_listings():
    async with manual_node() as node:
        a = node.spawn(service, "a")
        node.receptionist.tell(Register(KEY, a))
        node.receptionist.tell(Register(OTHER, a))
        await node.listed(KEY, {a})
        await node.listed(OTHER, {a})
        a.tell(Stop())
        await node.listed(KEY, set())
        await node.listed(OTHER, set())


async def test_a_stopped_subscriber_gets_nothing_more():
    async with manual_node() as node:
        dead: TestProbe[DeadLetter] = node.probe()
        node.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        seen: TestProbe[Listing[str]] = node.probe()
        subscriber = node.spawn(forwarder(seen.ref), "sub")
        node.receptionist.tell(Subscribe(KEY, subscriber))
        await seen.expect_message(Listing(KEY, frozenset(), frozenset(), True))
        subscriber.tell(Stop())
        await dead.expect_no_message(0)
        a = node.spawn(service, "a")
        node.receptionist.tell(Register(KEY, a))
        await node.listed(KEY, {a})
        await seen.expect_no_message(0)
        await dead.expect_no_message(0)


async def test_deregister_then_register_lists_the_ref_again():
    async with manual_node() as node:
        a = node.spawn(service, "a")
        node.receptionist.tell(Register(KEY, a))
        await node.listed(KEY, {a})
        node.receptionist.tell(Deregister(KEY, a))
        await node.listed(KEY, set())
        node.receptionist.tell(Register(KEY, a))
        await node.listed(KEY, {a})


async def test_find_before_joining_lists_local_refs():
    port = free_port()
    system = ActorSystem(
        Behaviors.ignore,
        "r",
        address=("127.0.0.1", port),
        cluster=ClusterSettings(
            seed_nodes=(Address("r", "127.0.0.1", free_port()),), **FAST, **settings_for()
        ),
        shutdown=FAST_SHUTDOWN,
    )
    async with system:
        ref = Receptionist(system).ref
        node = Node(system, ref, None)
        assert not all_up(system, 1)
        a = node.spawn(service, "a")
        ref.tell(Register(KEY, a))
        await node.listed(KEY, {a})


async def test_a_tombstoned_ref_merged_back_is_removed_again():
    async with manual_node() as node:
        a = node.spawn(service, "a")
        node.receptionist.tell(Register(KEY, a))
        await node.listed(KEY, {a})
        node.receptionist.tell(Deregister(KEY, a))

        async def gone() -> bool:
            return not (await node.ddata(KEY)).entries_for(KEY)

        await node.until(gone)
        await node.sync()  # the removal reaches the receptionist's registry before the re-add
        await node.sync()
        uid = node.cluster.self_unique_address.uid
        entry = Entry(a, uid, int(time.time() * 1000))
        # a concurrent registration for the same ref, merged in from elsewhere
        node.update(KEY, lambda r: r.add_binding(KEY, entry, node.self_node))
        await node.until(lambda: _has(node, entry)())
        await node.until(gone)
        assert (await node.find()).service_instances == frozenset()


async def test_a_tombstoned_ref_merged_back_as_a_dead_letter_ref_is_removed_again():
    # a peer's copy of the entry decodes here as EmptyLocalActorRef, which is not the LocalRef
    # the tombstone was made for: tombstones match by path
    async with manual_node() as node:
        a = node.spawn(service, "a")
        node.receptionist.tell(Register(KEY, a))
        await node.listed(KEY, {a})
        node.receptionist.tell(Deregister(KEY, a))

        async def gone() -> bool:
            return not (await node.ddata(KEY)).entries_for(KEY)

        await node.until(gone)
        await node.sync()
        await node.sync()
        uid = node.cluster.self_unique_address.uid
        entry = Entry(EmptyLocalActorRef(node.system, a.path), uid, int(time.time() * 1000))
        node.update(KEY, lambda r: r.add_binding(KEY, entry, node.self_node))
        await node.until(lambda: _has(node, entry)())
        await node.until(gone)
        assert (await node.find()).service_instances == frozenset()


def adapter_owner(to: ActorRef[Any]) -> Behavior[Any]:
    """Hands two message adapters of itself to ``to``; they share one path."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        to.tell((ctx.message_adapter(lambda m: m), ctx.message_adapter(lambda m: m)))
        return service

    return setup


async def test_deregistering_one_message_adapter_keeps_the_actors_other_adapter_listed():
    async with manual_node() as node:
        probe: TestProbe[Any] = node.probe()
        node.spawn(adapter_owner(probe.ref), "owner")
        ad1, ad2 = await probe.receive_message()
        assert ad1.path == ad2.path
        node.receptionist.tell(Register(KEY, ad1))
        node.receptionist.tell(Register(KEY, ad2))
        await node.listed(KEY, {ad1, ad2})
        node.receptionist.tell(Deregister(KEY, ad1))
        await node.listed(KEY, {ad2})
        for _ in range(5):  # a wrong tombstone would hide ad2 and re-remove its binding
            await node.sync()
        assert (await node.find()).service_instances == {ad2}


async def test_a_stopped_adapter_service_merged_back_as_a_dead_letter_ref_is_removed_again():
    async with manual_node() as node:
        probe: TestProbe[Any] = node.probe()
        owner = node.spawn(adapter_owner(probe.ref), "owner")
        ad1, _ = await probe.receive_message()
        wire = node.system.remoting.ref_path(ad1)  # pyright: ignore[reportOptionalMemberAccess]
        node.receptionist.tell(Register(KEY, ad1))
        await node.listed(KEY, {ad1})
        owner.tell(Stop())
        await node.listed(KEY, set())
        await node.sync()
        await node.sync()
        del ad1, _
        gc.collect()  # the peer's copy of the entry decodes to a dead-letter ref
        uid = node.cluster.self_unique_address.uid
        entry = Entry(EmptyLocalActorRef(node.system, wire), uid, int(time.time() * 1000))
        node.update(KEY, lambda r: r.add_binding(KEY, entry, node.self_node))
        await node.until(lambda: _has(node, entry)())
        for _ in range(5):
            await node.sync()
        assert (await node.find()).service_instances == frozenset()


async def test_prune_tombstones_tick_drops_expired_tombstones():
    # a tombstone lives prune_gossip_tombstones_after; the tick runs every 1/24th of that
    async with manual_node(prune_gossip_tombstones_after=2.4) as node:
        a = node.spawn(service, "a")
        node.receptionist.tell(Register(KEY, a))
        await node.listed(KEY, {a})
        node.receptionist.tell(Deregister(KEY, a))

        async def gone() -> bool:
            return not (await node.ddata(KEY)).entries_for(KEY)

        await node.until(gone)
        await node.sync()  # the removal reaches the receptionist's registry before the re-add
        await node.sync()
        entry = Entry(a, node.cluster.self_unique_address.uid, 1)
        node.update(KEY, lambda r: r.add_binding(KEY, entry, node.self_node))
        await node.until(lambda: _has(node, entry)())
        await node.until(gone)  # tombstoned: the receptionist removes it again
        await node.sync()
        await node.sync()
        await node.manual.time_passes(2.5)  # type: ignore[union-attr]
        node.update(KEY, lambda r: r.add_binding(KEY, entry, node.self_node))
        await node.listed(KEY, {a})  # the tombstone is pruned: this one stays


async def test_a_removed_self_member_stops_the_receptionist():
    port = free_port()
    system = ActorSystem(
        Behaviors.ignore,
        "r",
        address=("127.0.0.1", port),
        cluster=ClusterSettings(
            seed_nodes=(Address("r", "127.0.0.1", port),), **FAST, **settings_for()
        ),
        shutdown=FAST_SHUTDOWN,
    )
    async with system:
        await eventually(lambda: all_up(system, 1), timeout=3)
        ref = Receptionist(system).ref
        node = Node(system, ref, None)
        watcher: TestProbe[Any] = node.probe()
        cluster = node.cluster
        cluster.manager.tell(Leave(cluster.self_address))
        await watcher.expect_terminated(ref, 5)


def foreign_entry(node: Node, created: int, name: str = "x") -> Entry:
    """An entry of a node that is not in the cluster."""
    remote = node.system.resolve(f"ohra://r@127.0.0.1:1/user/{name}")
    return Entry(remote, 999, created)


async def test_the_leader_removes_entries_of_nodes_not_in_the_cluster_after_older_than():
    async with manual_node(pruning_interval=1.0, prune_removed_older_than=60.0) as node:
        young = foreign_entry(node, int(time.time() * 1000), "young")
        old = foreign_entry(node, 0, "old")
        node.update(KEY, lambda r: r.add_binding(KEY, young, node.self_node))
        node.update(OTHER, lambda r: r.add_binding(OTHER, old, node.self_node))
        await node.sync()
        await node.sync()
        await node.manual.time_passes(1.0)  # type: ignore[union-attr]
        await node.sync()

        async def old_gone() -> bool:
            return not (await node.ddata(OTHER)).entries_for(OTHER)

        await node.until(old_gone)
        assert (await node.ddata(KEY)).entries_for(KEY) == {young}


async def test_the_leader_does_not_prune_this_nodes_own_entries():
    async with manual_node(pruning_interval=1.0, prune_removed_older_than=0.0) as node:
        a = node.spawn(service, "a")
        node.receptionist.tell(Register(KEY, a))
        await node.listed(KEY, {a})
        for _ in range(3):  # every entry is old enough: only a node not in the cluster loses its
            await node.manual.time_passes(1.0)  # type: ignore[union-attr]
            await node.sync()
        assert (await node.find()).service_instances == {a}
        assert len((await node.ddata(KEY)).entries_for(KEY)) == 1


async def test_a_non_leader_does_not_prune():
    # The lower port leads. Only the follower prunes eagerly: the leader would prune the entry
    # too, and its removal would replicate to the follower.
    ports = sorted([free_port(), free_port()])
    seeds = tuple(Address("r", "127.0.0.1", p) for p in ports[:1])
    async with AsyncExitStack() as stack:
        systems = [
            await stack.enter_async_context(
                ActorSystem(
                    Behaviors.ignore,
                    "r",
                    address=("127.0.0.1", p),
                    cluster=ClusterSettings(
                        seed_nodes=seeds,
                        **FAST,
                        **settings_for(
                            **(
                                {}
                                if i == 0
                                else dict(pruning_interval=0.05, prune_removed_older_than=0.0)
                            )
                        ),
                    ),
                    shutdown=FAST_SHUTDOWN,
                )
            )
            for i, p in enumerate(ports)
        ]
        await eventually(lambda: all(all_up(s, 2) for s in systems), timeout=5)
        leader = Cluster(systems[0]).state.leader
        (follower,) = [s for s in systems if Cluster(s).self_address != leader]
        assert follower is systems[1]
        ref = Receptionist(follower).ref
        node = Node(follower, ref, None)
        await eventually(lambda: "replicator" in follower._system_root.children[NAME].children)  # pyright: ignore[reportPrivateUsage]
        entry = foreign_entry(node, 0)
        node.update(KEY, lambda r: r.add_binding(KEY, entry, node.self_node))
        await node.until(_has(node, entry))
        for _ in range(40):  # many RemoveTicks (0.05 s) pass
            await node.sync()
        assert (await node.ddata(KEY)).entries_for(KEY) == {entry}


def _has(node: Node, entry: Entry) -> Callable[[], Any]:
    async def cond() -> bool:
        return entry in (await node.ddata(KEY)).entries_for(KEY)

    return cond


async def test_cluster_shutting_down_alone_stops_the_receptionist(
    caplog: pytest.LogCaptureFixture,
):
    # a node that never joined has no MemberRemoved(self) to come: only ClusterShuttingDown
    caplog.set_level(logging.DEBUG)
    system = ActorSystem(
        Behaviors.ignore,
        "r",
        address=("127.0.0.1", free_port()),
        cluster=ClusterSettings(
            seed_nodes=(Address("r", "127.0.0.1", free_port()),), **FAST, **settings_for()
        ),
        shutdown=FAST_SHUTDOWN,
    )
    async with system:
        ref = Receptionist(system).ref
        await Node(system, ref, None).find()
        assert not all_up(system, 1)
    # only the NodeRemoved(self) branch logs this
    assert any("terminated/removed" in r.getMessage() for r in caplog.records)


async def test_an_actor_that_is_service_and_subscriber_leaves_everything_when_it_stops():
    async with manual_node() as node:
        seen: TestProbe[Listing[str]] = node.probe()
        x = node.spawn(forwarder(seen.ref), "x")
        node.receptionist.tell(Subscribe(KEY, x))
        node.receptionist.tell(Register(KEY, x))
        node.receptionist.tell(Register(OTHER, x))
        await node.listed(KEY, {x})
        await node.listed(OTHER, {x})
        x.tell(Stop())
        await node.listed(KEY, set())
        await node.listed(OTHER, set())
        assert not (await node.ddata(KEY)).entries_for(KEY)
        assert not (await node.ddata(OTHER)).entries_for(OTHER)
        dead: TestProbe[DeadLetter] = node.probe()
        node.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        b = node.spawn(service, "b")
        node.receptionist.tell(Register(KEY, b))
        await node.listed(KEY, {b})
        for _ in range(5):
            await node.sync()
        await dead.expect_no_message(0)  # the subscription went with the actor


async def test_a_subscriber_that_deregisters_its_last_service_stays_watched():
    async with manual_node() as node:
        dead: TestProbe[DeadLetter] = node.probe()
        node.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        seen: TestProbe[Listing[str]] = node.probe()
        x = node.spawn(forwarder(seen.ref), "x")
        node.receptionist.tell(Subscribe(KEY, x))
        node.receptionist.tell(Register(KEY, x))
        await node.listed(KEY, {x})
        node.receptionist.tell(Deregister(KEY, x))
        await node.listed(KEY, set())
        x.tell(Stop())
        for _ in range(3):
            await node.sync()
        b = node.spawn(service, "b")
        node.receptionist.tell(Register(KEY, b))
        await node.listed(KEY, {b})
        for _ in range(5):
            await node.sync()
        await dead.expect_no_message(0)  # still watched: its subscription was removed
