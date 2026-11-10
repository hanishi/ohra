"""Pekko's multi-node ``DurableDataSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/DurableDataSpec.scala``),
twice, as Pekko does: once with the store writing at once (``DurableDataSpec``) and once with a
write-behind interval of 200 ms (``DurableDataWriteBehindSpec``). One body runs both, with the
spec's six cases in its order and its barrier names (``<role>-joined``, ``both-initialized``,
``update-done-N``, ``values-verified-N``, ``after-N``):

- "work in single node cluster" (:130): three durable updates, stop, a new Replicator on the same
  name sees 3, the commands wait for the load.
- "work in multi node cluster" (:174): writes and reads to two nodes, stop, restart, the values
  are back.
- "be durable after gossip update" (:222): a value that arrived by gossip is stored.
- "handle Update before load" (:258): a second system on the same port and directory takes an
  Update while it loads.
- "stop Replicator if Load fails" (:334): a store that throws ``LoadFailed``.
- "reply with StoreFailure if store fails" (:345): a store that answers the failure message.

Differences from Pekko:

- The store directory is a per-run pytest temp directory ending in ``ddata``, one for each of the
  two runs, where Pekko's is ``target/DurableDataSpec-<millis>-ddata`` from the common config.
  Pekko's rule for such a directory (system name, replicator name and port appended) gives each
  node, and each ``replicator-N``, its own directory under it.
- Pekko's ``ReplicatorSettings(system)`` reads the config; here the settings are
  ``ReplicatorSettings(gossip_interval=1.0, durable_keys={"durable*"}, durable_store=...)`` where
  the store is ``sqlite_durable_store`` (LMDB's ``map-size`` has no SQLite counterpart). The
  ``TestDurableStore`` cases give the same keys and a store behavior of the test's own.
- ``withDurableStoreProps(props)`` is the ``durable_store`` factory; ``TestDurableStore`` is a
  behavior, and its ``LoadFailed`` is raised in the handler, which stops the store.
- The typed protocol has no request context, so Pekko's ``Some("a")`` request is dropped:
  ``StoreFailure(KeyA)``.
- Pekko's ``expectMsg`` on the implicit sender is a ``TestProbe`` as ``reply_to``, a new one for
  each request, so a reply to a try that ``await_assert`` repeats cannot reach the next one. The
  three updates of the first case share one probe and are answered in order.
- ``system.stop(r)`` is ``stop_child`` on the guardian, and ``watch`` plus ``expectTerminated``
  is ``expect_terminated`` on a probe.
- ``MultiNodeSpec.configureNextPortIfFixed`` is ``free_port()``; the second incarnation of the
  additional system reuses the first's port, as Pekko's saved ``address.port``. Pekko's first
  incarnation is closed in a ``finally`` and its second is never terminated; here both are
  ``async with``, so the second stops at the end of the case, as a test must leave nothing
  running, and the "Await.ready(sys1.terminate())" in Pekko's ``finally`` is the exit of the
  first block.
- The cluster runs with ``FAST`` intervals and ``FAST_SHUTDOWN``; the gossip interval of the
  Replicators stays at Pekko's one second. ``single-expect-default`` is Pekko's 15 s.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, cast

import pytest

from ohra import Behavior, Behaviors
from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    DurableStore,
    GCounter,
    GCounterKey,
    Key,
    ORSet,
    ORSetKey,
    ReplicatedData,
    Replicator,
    ReplicatorSettings,
    sqlite_durable_store,
)
from ohra.ref import ActorRef
from ohra.remote import Address
from ohra.system import ActorSystem
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, RoleName, TestProbe, multi_node_spec
from ohra.testkit.settings import TestKitSettings

from ...test_cluster import FAST, FAST_SHUTDOWN
from ....util import free_port

CLUSTER = ClusterSettings(**FAST, distributed_data=ReplicatorSettings(gossip_interval=1.0))

READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()

KeyA = GCounterKey("durable-A")
KeyB = GCounterKey("durable-B")
KeyC: ORSetKey[str] = ORSetKey("durable-C")


def test_durable_store(
    fail_load: bool = False, fail_store: bool = False
) -> Callable[[], Behavior[DurableStore.Command]]:
    """Pekko's ``TestDurableStore`` (:44)."""

    def receive(msg: DurableStore.Command) -> Behavior[DurableStore.Command]:
        match msg:
            case DurableStore.LoadAll(reply_to):
                if fail_load:
                    raise DurableStore.LoadFailed(
                        "TestDurableStore: failed to load durable distributed-data"
                    )
                reply_to.tell(DurableStore.LoadAllCompleted())
            case DurableStore.Store(_, _, reply):
                if reply is not None:
                    reply.reply_to.tell(reply.failure if fail_store else reply.success)
            case _:
                pass
        return Behaviors.same

    return lambda: Behaviors.receive_message(receive)


test_durable_store.__test__ = False  # pyright: ignore[reportFunctionMemberAccess]  # not a test


@dataclass
class Host:
    """What a probe needs of a node: the settings and the system."""

    settings: TestKitSettings
    system: ActorSystem[Any]


def make_config() -> MultiNodeConfig:
    config = MultiNodeConfig()
    config.role("first")
    config.role("second")
    config.common_config(cluster=CLUSTER, shutdown=FAST_SHUTDOWN)
    return config


async def durable_data_spec(spec: MultiNodeSpec, base: Path, write_behind: float) -> None:
    first, second = spec.roles
    cluster = Cluster(spec.system)
    self_address = DistributedData(spec.system).self_unique_address
    timeout = spec.settings.dilated(14.0)  # initialization of the store can be slow in CI
    write_two = Replicator.WriteTo(2, timeout)
    read_two = Replicator.ReadFrom(2, timeout)
    step = 0

    def settings() -> ReplicatorSettings:
        return ReplicatorSettings(
            gossip_interval=1.0,
            durable_keys=frozenset({"durable*"}),
            durable_store=partial(
                sqlite_durable_store, base / "ddata", write_behind_interval=write_behind
            ),
        )

    async def enter_barrier_after_test_step() -> None:
        nonlocal step
        step += 1
        await spec.enter_barrier("after-" + str(step))

    def new_replicator(
        sys: ActorSystem[Any] | None = None,
        durable_store: Callable[[], Behavior[DurableStore.Command]] | None = None,
    ) -> ActorRef[Replicator.Command]:
        s = settings() if durable_store is None else _with_store(settings(), durable_store)
        behavior = Replicator.behavior(s)
        name = "replicator-" + str(step)
        if sys is None:
            return spec.spawn(behavior, name)
        return sys._root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]

    async def new_replicator_when_free() -> ActorRef[Replicator.Command]:
        refs: list[ActorRef[Replicator.Command]] = []

        def spawn() -> None:
            refs.append(new_replicator())

        await spec.await_assert(spawn)  # try until name is free
        return refs[0]

    async def stop(ref: ActorRef[Any], sys: ActorSystem[Any] | None = None) -> None:
        probe: TestProbe[Any] = spec.create_test_probe()
        (sys or spec.system)._root.stop_child(ref)  # pyright: ignore[reportPrivateUsage]
        await probe.expect_terminated(ref)

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    async def await_assert(fn: Callable[[], Awaitable[None]], max: float) -> None:
        async with spec.within(max):
            await spec.await_assert(fn)

    def probe_of(host: Host | None = None) -> TestProbe[Any]:
        return TestProbe(host or spec)

    async def await_replica_count(
        r: ActorRef[Replicator.Command], n: int, host: Host | None = None
    ) -> None:
        async def replica_count() -> None:
            probe: TestProbe[Replicator.ReplicaCount] = probe_of(host)
            r.tell(Replicator.GetReplicaCount(probe.ref))
            await probe.expect_message(Replicator.ReplicaCount(n))

        await await_assert(replica_count, 10.0)

    async def get_response(
        r: ActorRef[Replicator.Command],
        key: Key[Any],
        consistency: Replicator.ReadConsistency,
        host: Host | None = None,
    ) -> Any:
        probe: TestProbe[Any] = probe_of(host)
        r.tell(Replicator.Get(key, consistency, probe.ref))
        return await probe.receive_message()

    async def get[A: ReplicatedData](
        r: ActorRef[Replicator.Command],
        key: Key[A],
        consistency: Replicator.ReadConsistency = READ_LOCAL,
        host: Host | None = None,
    ) -> A:
        reply = await get_response(r, key, consistency, host)
        assert isinstance(reply, Replicator.GetSuccess), reply
        return cast(A, cast(Any, reply).data)

    def tell_update[A: ReplicatedData](
        r: ActorRef[Replicator.Command],
        probe: TestProbe[Any],
        key: Key[A],
        initial: A,
        write: Replicator.WriteConsistency,
        modify: Callable[[A], A],
    ) -> None:
        r.tell(Replicator.Update(key, initial, write, probe.ref, modify))

    async def update[A: ReplicatedData](
        r: ActorRef[Replicator.Command],
        key: Key[A],
        initial: A,
        write: Replicator.WriteConsistency,
        modify: Callable[[A], A],
        host: Host | None = None,
    ) -> None:
        probe = probe_of(host)
        tell_update(r, probe, key, initial, write, modify)
        await probe.expect_message(Replicator.UpdateSuccess(key))

    async def key_ids(r: ActorRef[Replicator.Command], host: Host | None = None) -> frozenset[str]:
        probe: TestProbe[Replicator.GetKeyIdsResult] = probe_of(host)
        r.tell(Replicator.GetKeyIds(probe.ref))
        return (await probe.receive_message()).key_ids

    async def await_key_ids_not_empty(r: ActorRef[Replicator.Command]) -> None:
        async def not_empty() -> None:
            assert await key_ids(r) != frozenset()

        await spec.await_assert(not_empty)

    # "work in single node cluster"
    await join(first, first)

    if spec.is_node(first):
        r = new_replicator()
        await await_replica_count(r, 1)

        assert await get_response(r, KeyA, READ_LOCAL) == Replicator.NotFound(KeyA)

        probe = probe_of()
        for _ in range(3):
            tell_update(
                r, probe, KeyA, GCounter(), WRITE_LOCAL, lambda c: c.increment(self_address, 1)
            )
        for _ in range(3):
            await probe.expect_message(Replicator.UpdateSuccess(KeyA))

        await stop(r)

        r2 = await new_replicator_when_free()

        # note that it will stash the commands until loading completed
        assert (await get(r2, KeyA)).value == 3

        await stop(r2)

    await enter_barrier_after_test_step()

    # "work in multi node cluster"
    await join(second, first)

    r = new_replicator()
    await await_replica_count(r, 2)
    await spec.enter_barrier("both-initialized")

    await update(r, KeyA, GCounter(), write_two, lambda c: c.increment(self_address, 1))
    await update(r, KeyC, ORSet[str](), write_two, lambda s: s.add(self_address, spec.myself.name))

    await spec.enter_barrier("update-done-" + str(step))

    assert (await get(r, KeyA, read_two)).value == 2
    assert (await get(r, KeyC, read_two)).elements == {first.name, second.name}

    await spec.enter_barrier("values-verified-" + str(step))

    await stop(r)

    r2 = await new_replicator_when_free()
    await await_key_ids_not_empty(r2)

    assert (await get(r2, KeyA)).value == 2
    assert (await get(r2, KeyC)).elements == {first.name, second.name}

    await enter_barrier_after_test_step()

    # "be durable after gossip update"
    r = new_replicator()

    if spec.is_node(first):
        await update(r, KeyC, ORSet[str](), WRITE_LOCAL, lambda s: s.add(self_address, "first"))

    if spec.is_node(second):
        changed: TestProbe[Any] = probe_of()
        r.tell(Replicator.Subscribe(KeyC, changed.ref))
        msg = await changed.receive_message()
        assert isinstance(msg, Replicator.Changed)
        assert cast(ORSet[str], msg.data).elements == {first.name}  # pyright: ignore[reportUnknownMemberType]

        # must do one more roundtrip to be sure that it keyB is stored, since Changed might have
        # been sent out before storage
        await update(r, KeyA, GCounter(), WRITE_LOCAL, lambda c: c.increment(self_address, 1))

        await stop(r)

        r2 = await new_replicator_when_free()
        await await_key_ids_not_empty(r2)

        assert (await get(r2, KeyC)).elements == {first.name}

    await enter_barrier_after_test_step()

    # "handle Update before load"
    if spec.is_node(first):
        port = free_port()

        def additional() -> ActorSystem[Any]:
            return ActorSystem(
                Behaviors.empty,
                "AdditionalSys",
                address=("127.0.0.1", port),
                cluster=CLUSTER,
                shutdown=FAST_SHUTDOWN,
            )

        async with additional() as sys1:
            address = Cluster(sys1).self_address
            Cluster(sys1).manager.tell(Join(address))
            host = Host(spec.settings, sys1)

            r = new_replicator(sys1)
            await await_replica_count(r, 1, host)

            assert await get_response(r, KeyA, READ_LOCAL, host) == Replicator.NotFound(KeyA)

            probe = probe_of(host)
            for _ in range(3):
                tell_update(
                    r, probe, KeyA, GCounter(), WRITE_LOCAL, lambda c: c.increment(self_address, 1)
                )
            tell_update(
                r, probe, KeyB, GCounter(), WRITE_LOCAL, lambda c: c.increment(self_address, 1)
            )

            for _ in range(3):
                await probe.expect_message(Replicator.UpdateSuccess(KeyA))
            await probe.expect_message(Replicator.UpdateSuccess(KeyB))

            await stop(r, sys1)

        # use the same port
        async with additional() as sys2:
            Cluster(sys2).manager.tell(Join(address))
            host = Host(spec.settings, sys2)

            r2 = new_replicator(sys2)

            # it should be possible to update while loading is in progress
            await update(
                r2, KeyB, GCounter(), WRITE_LOCAL, lambda c: c.increment(self_address, 1), host
            )

            # wait until all loaded
            async def all_loaded() -> None:
                assert await key_ids(r2, host) == {KeyA.id, KeyB.id}

            await spec.await_assert(all_loaded)
            assert (await get(r2, KeyA, host=host)).value == 3
            assert (await get(r2, KeyB, host=host)).value == 2
    await enter_barrier_after_test_step()

    # "stop Replicator if Load fails"
    if spec.is_node(first):
        r = new_replicator(durable_store=test_durable_store(fail_load=True))
        await probe_of().expect_terminated(r)
    await enter_barrier_after_test_step()

    # "reply with StoreFailure if store fails"
    if spec.is_node(first):
        r = new_replicator(durable_store=test_durable_store(fail_store=True))
        probe = probe_of()
        tell_update(
            r, probe, KeyA, GCounter(), WRITE_LOCAL, lambda c: c.increment(self_address, 1)
        )
        await probe.expect_message(Replicator.StoreFailure(KeyA))
    await enter_barrier_after_test_step()


def _with_store(
    s: ReplicatorSettings, store: Callable[[], Behavior[DurableStore.Command]]
) -> ReplicatorSettings:
    from dataclasses import replace

    return replace(s, durable_store=store)


@pytest.mark.parametrize("write_behind", [0.0, 0.2], ids=["DurableDataSpec", "WriteBehind"])
async def test_durable_data_spec(tmp_path: Path, write_behind: float) -> None:
    @multi_node_spec(make_config(), TestKitSettings(single_expect_default=15.0))
    async def durable_data(spec: MultiNodeSpec) -> None:
        await durable_data_spec(spec, tmp_path, write_behind)

    await durable_data()
