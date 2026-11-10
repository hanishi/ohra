"""Pekko's multi-node ``DurablePruningSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/DurablePruningSpec.scala``),
as one ``@multi_node_spec`` test with the spec's own barrier names (``first-joined``,
``second-joined``, ``joined``, ``updates-done``, ``get1``, ``removed``, ``pruned``,
``sys3-started``, ``after-1``) and its one case, "move data from removed node" (:81). Each of the
two nodes runs a second system; the second system of ``first`` leaves, is stopped, and is
started again on its old port and directory as ``sys3``. The counter keeps the value 10 through
the pruning, and the removed node's slot is not counted again once ``sys3`` has loaded its old
data (state size 3).

Differences from Pekko:

- The store directory is a per-run pytest temp directory ending in ``ddata``, where Pekko's is
  ``target/DurablePruningSpec-<millis>-ddata``. Pekko's rule for such a directory (system name,
  replicator name and port appended) gives each system its own, and ``sys3`` the directory of
  ``sys2``. ``durable_keys={"*"}`` is in the Replicators' settings, built here instead of read
  from ``ReplicatorSettings(sys)``; LMDB's ``map-size`` has no SQLite counterpart.
- ``startReplicator`` spawns ``Replicator.behavior(settings)`` as ``replicator``, with gossip
  interval 1 s, ``pruning_interval=1.0`` and ``max_pruning_dissemination=3.0``.
- The typed protocol has no request context, so Pekko's ``None`` request is dropped; each reply
  goes to a new ``TestProbe``, so the reply to a try that ``await_assert`` repeats cannot reach the
  next one.
- ``MultiNodeSpec.configureNextPortIfFixed`` is ``free_port()``. ``sys2`` is named as the spec's
  system, as in Pekko, and ``sys3`` takes ``sys2``'s port and name.
- ``cluster.leave(cluster2.selfAddress)`` is the typed ``cluster.manager ! Leave(...)``. Pekko's
  ``sys2.close()`` on ``first`` is ``terminate()`` and a wait for the termination; ``sys2`` on
  ``second`` and ``sys3`` are stopped when the test ends (Pekko leaves them to the JVM exit).
- ``remainingOrDefault`` is ``single_expect_default`` (3 s).
- The cluster runs with ``FAST`` intervals and ``FAST_SHUTDOWN``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, cast

from ohra import Behaviors
from ohra.cluster import Cluster, ClusterSettings, Join, Leave
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    Replicator,
    ReplicatorSettings,
    sqlite_durable_store,
)
from ohra.ref import ActorRef
from ohra.remote import Address
from ohra.system import ActorSystem
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, RoleName, TestProbe, multi_node_spec

from ...test_cluster import FAST, FAST_SHUTDOWN
from ....util import free_port

MAX_PRUNING_DISSEMINATION = 3.0
KeyA = GCounterKey("A")
READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()


@dataclass
class Host:
    """What a probe needs of a system: the settings and the system."""

    settings: Any
    system: ActorSystem[Any]


async def durable_pruning_spec(spec: MultiNodeSpec, base: Path) -> None:
    first, second = spec.roles
    cluster = Cluster(spec.system)
    self_address = DistributedData(spec.system).self_unique_address

    def cluster_settings() -> ClusterSettings:
        return ClusterSettings(**FAST, distributed_data=ReplicatorSettings(gossip_interval=1.0))

    def start_replicator(sys: ActorSystem[Any] | None = None) -> ActorRef[Replicator.Command]:
        behavior = Replicator.behavior(
            ReplicatorSettings(
                gossip_interval=1.0,
                pruning_interval=1.0,
                max_pruning_dissemination=MAX_PRUNING_DISSEMINATION,
                durable_keys=frozenset({"*"}),
                durable_store=partial(sqlite_durable_store, base / "ddata"),
            )
        )
        if sys is None:
            return spec.spawn(behavior, "replicator")
        return sys._root.spawn_child(behavior, "replicator", None).ref  # pyright: ignore[reportPrivateUsage]

    @asynccontextmanager
    async def additional(port: int) -> AsyncGenerator[ActorSystem[Any]]:
        async with ActorSystem(
            Behaviors.empty,
            spec.system.name,
            address=("127.0.0.1", port),
            cluster=cluster_settings(),
            shutdown=FAST_SHUTDOWN,
        ) as sys:
            yield sys

    async def await_assert(fn: Callable[[], Awaitable[None]], max: float) -> None:
        async with spec.within(max):
            await spec.await_assert(fn)

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    async def get_counter(
        r: ActorRef[Replicator.Command],
        consistency: Replicator.ReadConsistency,
        host: Any = spec,
    ) -> GCounter:
        probe: TestProbe[Any] = TestProbe(host)
        r.tell(Replicator.Get(KeyA, consistency, probe.ref))
        reply = await probe.receive_message()
        assert isinstance(reply, Replicator.GetSuccess), reply
        return cast(GCounter, cast(Any, reply).data)

    async def update(
        r: ActorRef[Replicator.Command], modify: Callable[[GCounter], GCounter], host: Any = spec
    ) -> None:
        probe: TestProbe[Any] = TestProbe(host)
        r.tell(Replicator.Update(KeyA, GCounter(), WRITE_LOCAL, probe.ref, modify))
        await probe.expect_message(Replicator.UpdateSuccess(KeyA))

    async def replica_count(r: ActorRef[Replicator.Command], n: int, host: Any = spec) -> None:
        probe: TestProbe[Replicator.ReplicaCount] = TestProbe(host)
        r.tell(Replicator.GetReplicaCount(probe.ref))
        await probe.expect_message(Replicator.ReplicaCount(n))

    def all_up(c: Cluster, n: int) -> None:
        members = c.state.members
        assert len(members) == n, members
        assert {m.status for m in members} == {"up"}, members

    replicator = start_replicator()

    async with AsyncExitStack() as stack:
        # "move data from removed node"
        await join(first, first)
        await join(second, first)

        port2 = free_port()
        sys2 = await stack.enter_async_context(additional(port2))
        cluster2 = Cluster(sys2)
        distributed_data2 = DistributedData(sys2)
        replicator2 = start_replicator(sys2)
        host2 = Host(spec.settings, sys2)
        cluster2.manager.tell(Join(Address.parse(spec.node(first))))

        async def joined() -> None:
            all_up(cluster, 4)
            all_up(cluster2, 4)

        await await_assert(joined, 10.0)
        await spec.enter_barrier("joined")

        async def four_replicas() -> None:
            await replica_count(replicator, 4)
            await replica_count(replicator2, 4, host2)

        await await_assert(four_replicas, 5.0)

        await update(replicator, lambda c: c.increment(self_address, 3))
        await update(
            replicator2,
            lambda c: c.increment(distributed_data2.self_unique_address, 2),
            host2,
        )

        await spec.enter_barrier("updates-done")

        async def counter1() -> None:
            counter = await get_counter(replicator, Replicator.ReadAll(1.0))
            assert counter.value == 10
            assert len(counter.state) == 4

        await await_assert(counter1, 10.0)

        async def counter2() -> None:
            counter = await get_counter(replicator2, Replicator.ReadAll(1.0), host2)
            assert counter.value == 10
            assert len(counter.state) == 4

        await await_assert(counter2, 10.0)
        await spec.enter_barrier("get1")

        if spec.is_node(first):
            cluster.manager.tell(Leave(cluster2.self_address))

        async def three_replicas() -> None:
            await replica_count(replicator, 3)

        await await_assert(three_replicas, 15.0)
        await spec.enter_barrier("removed")
        if spec.is_node(first):
            sys2.terminate()
            await sys2.when_terminated()

        values: set[int] = set()

        async def counter3() -> None:
            counter = await get_counter(replicator, READ_LOCAL)
            values.add(counter.value)
            assert counter.value == 10
            assert len(counter.state) == 3

        await await_assert(counter3, 15.0)
        assert values == {10}
        await spec.enter_barrier("pruned")

        if spec.is_node(first):
            address = cluster2.self_address
            sys3 = await stack.enter_async_context(additional(address.port))
            cluster3 = Cluster(sys3)
            replicator3 = start_replicator(sys3)
            host3 = Host(spec.settings, sys3)
            cluster3.manager.tell(Join(Address.parse(spec.node(first))))

            async def sys3_up() -> None:
                assert any(
                    m.unique_address == cluster3.self_unique_address and m.status == "up"
                    for m in cluster.state.members
                )

            await await_assert(sys3_up, 10.0)

            values = set()

            async def counter4() -> None:
                counter = await get_counter(replicator3, READ_LOCAL, host3)
                values.add(counter.value)
                assert counter.value == 10
                assert len(counter.state) == 3

            await await_assert(counter4, 10.0)
            assert values == {10}

            # all must at least have seen it as joining
            async def sys3_sees_all() -> None:
                all_up(cluster3, 4)

            await await_assert(sys3_sees_all, 10.0)

            # after merging with others
            counter5 = await get_counter(
                replicator3, Replicator.ReadAll(spec.settings.single_expect_default_dilated), host3
            )
            assert counter5.value == 10
            assert len(counter5.state) == 3

        await spec.enter_barrier("sys3-started")
        counter6 = await get_counter(
            replicator, Replicator.ReadAll(spec.settings.single_expect_default_dilated)
        )
        assert counter6.value == 10
        assert len(counter6.state) == 3

        await spec.enter_barrier("after-1")


async def test_durable_pruning_spec(tmp_path: Path) -> None:
    config = MultiNodeConfig()
    config.role("first")
    config.role("second")
    config.common_config(
        cluster=ClusterSettings(**FAST, distributed_data=ReplicatorSettings(gossip_interval=1.0)),
        shutdown=FAST_SHUTDOWN,
    )

    @multi_node_spec(config)
    async def durable_pruning(spec: MultiNodeSpec) -> None:
        await durable_pruning_spec(spec, tmp_path)

    await durable_pruning()
