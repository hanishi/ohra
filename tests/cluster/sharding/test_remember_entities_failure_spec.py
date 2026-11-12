"""RememberEntitiesFailureSpec, ported from Apache Pekko's cluster-sharding/src/test/scala/org/
apache/pekko/cluster/sharding/RememberEntitiesFailureSpec.scala.

Remembered entities recover from every way a shard or coordinator store can fail. Five cases
(217-410), each run for the five ways to fail, in Scala order (``NoResponse``, ``CrashStore``,
``StopStore``, ``Delay(500 ms)``, ``Delay(1 s)``):

- "recover when initial remember entities load fails" (220);
- "recover when shard storing a start event fails" (250);
- "recover on abrupt entity stop when storing a stop event fails" (291);
- "recover on graceful entity stop when storing a stop event fails" (326);
- "recover when coordinator storing shard start fails" (369).

Differences from the Scala spec:

- each test runs its own one-node cluster, which joins itself, in place of the spec's one system;
  so the Scala's ``system.stop(sharding)`` at the end of each case is the node's stop;
- the settings are ``FAST_SHARDING``'s (Pekko's retry interval, shard start timeout and
  rebalance interval are shortened), with remember entities on, ``verbose_debug_logging`` and
  ``fail_on_invalid_entity_state_transition`` on, and sharding's Replicator in memory, as the
  spec's ``durable.keys = []``;
- the times are halved, and Pekko's ``waiting-for-state-timeout`` (2 s by default) is the
  ``updating_state_timeout`` too: both timeouts are 0.5 s, the three backoffs 0.2 s, the ways
  ``Delay(0.25)`` and ``Delay(0.5)``, ``expectNoMessage(1.second)`` is
  ``expect_no_message(0.5)`` and ``Thread.sleep(250)`` is ``asyncio.sleep(0.125)``; the
  ``awaitAssert`` maxima stay 10 s and 5 s, the first case's is the probe's default, as are the
  ``expectMsg`` waits (3 s);
- the bare ``expectNoMessage()`` waits Pekko's ``expect-no-message-default``, 100 ms. In the
  start-event case it is ``expect_no_message(0.05)``, so the store's failure is cleared before
  the shard's write times out, as in the Scala ("timeout hits and then backoff"): a wait past
  the timeout would restart the shard, and with it the store the test then clears. In the
  initial-load case it is ``expect_no_message(0.5)``: ohra's shard starts more than 50 ms after
  the region gets the message, so the shorter wait would reset the failure before the shard
  asks its store, and the case would test nothing;
- the custom store config is a ``FakeStore`` provider, a subclass of
  ``DDataRememberEntitiesProvider``, which the test puts in place of
  ``ohra.cluster.sharding._remember_entities_provider``; the stores are typed behaviors, so their
  messages carry ``reply_to`` where the Scala answers ``sender()``, and ``CrashStore`` raises
  ``RuntimeError`` for ``TestException``;
- the stores announce ``ShardStoreCreated`` and ``CoordinatorStoreCreated`` to a probe the
  provider holds, in place of the event stream; as the Scala's subscriptions, a test gets only
  the shard stores' or only the coordinator store's;
- ``Delay`` answers, as the Scala's ``Delayed(sender(), Set.empty)``, an empty set in place of
  the store's reply, so a delayed store never answers: the shard or coordinator times out;
- ``EntityActor`` is a typed entity: its ``Msg`` carries the text and the ``reply_to`` it is
  echoed to, ``"stop"`` returns ``Behaviors.stopped()``, and ``"graceful-stop"`` sends
  ``Passivate`` to its shard; Typed's one stop message is ``Msg("stop")``, for both the Scala's
  ``Passivate("stop")`` and its hand-off stop message ``"graceful-stop"``, in the two cases that
  set them;
- Pekko's shard ids ``id % 10`` are ``number_of_shards=10``: entity ``"1"`` is in the shard whose
  store fails, and the Scala's entity 11, in the same shard, is the first ``str(n)``, ``n > 1``,
  of the same shard; ``failShardGetEntities = Map("1" -> ...)`` keys on that shard;
- ``leastShardAllocationStrategy(absoluteLimit = 1, relativeLimit = 0.1)`` is
  ``rebalance_absolute_limit=1`` and ``rebalance_relative_limit=0.1``;
- the type names are the Scala's with the ways' ``str``: ``initial-NoResponse``,
  ``initial-Delay(0.25 s)``, ...
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest

from ohra import (
    DONE,
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ClusterSharding,
    Done,
    Entity,
    EntityContext,
    EntityTypeKey,
    Passivate,
    ShardingEnvelope,
)
from ohra.cluster.ddata import ReplicatorSettings
from ohra.cluster.sharding import shard_id_of
from ohra.cluster.sharding.remember_entities import (
    DDataRememberEntitiesProvider,
    RememberEntitiesCoordinatorStore,
    RememberEntitiesShardStore,
)
from ohra.testkit import TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING
from .test_ddata_shard_coordinator import Node

SETTINGS = replace(
    FAST_SHARDING,
    number_of_shards=10,
    remember_entities=True,
    verbose_debug_logging=True,
    fail_on_invalid_entity_state_transition=True,
    distributed_data=ReplicatorSettings(durable_keys=frozenset()),
    entity_restart_backoff=0.2,
    shard_failure_backoff=0.2,
    coordinator_failure_backoff=0.2,
    updating_state_timeout=0.5,
    waiting_for_state_timeout=0.5,
)

GRACEFUL = replace(SETTINGS, rebalance_absolute_limit=1, rebalance_relative_limit=0.1)

SHARD_1 = shard_id_of("1", SETTINGS.number_of_shards)

ID_11 = next(
    str(n) for n in range(2, 1000) if shard_id_of(str(n), SETTINGS.number_of_shards) == SHARD_1
)
"""The Scala's entity 11: another entity of shard 1."""


# --- the entity (RememberEntitiesFailureSpec.scala 64-74) ----------------------------------------


@dataclass(frozen=True)
class Msg:
    text: str
    reply_to: ActorRef[str] | None = None


def entity_actor(entity: EntityContext[Msg]) -> Behavior[Msg]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Msg]) -> Behavior[Msg]:
        ctx.log.info("Entity actor [%s] starting up", entity.entity_id)

        def on_message(ctx: ActorContext[Msg], msg: Msg) -> Behavior[Msg]:
            match msg.text:
                case "stop":
                    ctx.log.info("Stopping myself!")
                    return Behaviors.stopped()
                case "graceful-stop":
                    entity.shard.tell(Passivate(ctx.self))
                case _:
                    if msg.reply_to is not None:
                        msg.reply_to.tell(msg.text)
            return Behaviors.same

        return Behaviors.receive(on_message)

    return setup


# --- the ways to fail (RememberEntitiesFailureSpec.scala 86-95) ----------------------------------


@dataclass(frozen=True)
class NoResponse:
    def __str__(self) -> str:
        return "NoResponse"


@dataclass(frozen=True)
class CrashStore:
    def __str__(self) -> str:
        return "CrashStore"


@dataclass(frozen=True)
class StopStore:
    def __str__(self) -> str:
        return "StopStore"


@dataclass(frozen=True)
class Delay:
    """Not really a failure but close enough."""

    how_long: float

    def __str__(self) -> str:
        return f"Delay({self.how_long:g} s)"


type Fail = NoResponse | CrashStore | StopStore | Delay

WAYS: list[Fail] = [NoResponse(), CrashStore(), StopStore(), Delay(0.25), Delay(0.5)]

# outside store since we need to be able to set them before sharding initializes
fail_shard_get_entities: dict[str, Fail] = {}
fail_coordinator_get_shards: Fail | None = None


@dataclass(frozen=True)
class ShardStoreCreated:
    store: ActorRef[Any]
    shard_id: str


@dataclass(frozen=True)
class CoordinatorStoreCreated:
    store: ActorRef[Any]


# --- the fake stores (RememberEntitiesFailureSpec.scala 100-213) ---------------------------------


@dataclass(frozen=True)
class FailUpdateEntity:
    which_way: Fail
    reply_to: ActorRef[Done]


@dataclass(frozen=True)
class ClearFail:
    reply_to: ActorRef[Done]


@dataclass(frozen=True)
class Delayed:
    reply_to: ActorRef[Any]
    msg: object


@dataclass(frozen=True)
class FailAddShard:
    shard_id: str
    way_to_fail: Fail
    reply_to: ActorRef[Done]


@dataclass(frozen=True)
class ClearFailShard:
    shard_id: str
    reply_to: ActorRef[Done]


ShardStore = RememberEntitiesShardStore
CoordinatorStore = RememberEntitiesCoordinatorStore


def fake_shard_store(shard_id: str, created: ActorRef[Any] | None) -> Behavior[Any]:
    """FakeShardStoreActor (RememberEntitiesFailureSpec.scala 112-150)."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        fail_update: Fail | None = None
        if created is not None:
            created.tell(ShardStoreCreated(ctx.self, shard_id))

        def on_message(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            nonlocal fail_update
            match msg:
                case ShardStore.GetEntities(reply_to):
                    match fail_shard_get_entities.get(shard_id):
                        case None:
                            reply_to.tell(ShardStore.RememberedEntities(frozenset()))
                        case NoResponse():
                            ctx.log.debug("Sending no response for GetEntities")
                        case CrashStore():
                            raise RuntimeError("store crash on GetEntities")
                        case StopStore():
                            return Behaviors.stopped()
                        case Delay(how_long):
                            ctx.log.debug(
                                "Delaying initial entities listing with %s", f"{how_long:g} s"
                            )
                            ctx.timers.start_single_timer(
                                "get-entities-delay", Delayed(reply_to, frozenset[str]()), how_long
                            )
                case ShardStore.Update(started, stopped, reply_to):
                    match fail_update:
                        case None:
                            reply_to.tell(ShardStore.UpdateDone(started, stopped))
                        case NoResponse():
                            ctx.log.debug("Sending no response for AddEntity")
                        case CrashStore():
                            raise RuntimeError("store crash on AddEntity")
                        case StopStore():
                            return Behaviors.stopped()
                        case Delay(how_long):
                            ctx.log.debug(
                                "Delaying response for AddEntity with %s", f"{how_long:g} s"
                            )
                            ctx.timers.start_single_timer(
                                "add-entity-delay", Delayed(reply_to, frozenset[str]()), how_long
                            )
                case FailUpdateEntity(which_way, reply_to):
                    fail_update = which_way
                    reply_to.tell(DONE)
                case ClearFail(reply_to):
                    fail_update = None
                    reply_to.tell(DONE)
                case Delayed(to, delayed):
                    to.tell(delayed)
                case _:
                    return Behaviors.unhandled
            return Behaviors.same

        return Behaviors.receive(on_message)

    return setup


def fake_coordinator_store(created: ActorRef[Any] | None) -> Behavior[Any]:
    """FakeCoordinatorStoreActor (RememberEntitiesFailureSpec.scala 157-211)."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        if created is not None:
            created.tell(CoordinatorStoreCreated(ctx.self))
        fail_add_shard: dict[str, Fail] = {}

        def on_message(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            nonlocal fail_add_shard
            match msg:
                case CoordinatorStore.GetShards(reply_to):
                    match fail_coordinator_get_shards:
                        case None:
                            reply_to.tell(CoordinatorStore.RememberedShards(frozenset()))
                        case NoResponse():
                            pass
                        case CrashStore():
                            raise RuntimeError("store crash on load")
                        case StopStore():
                            return Behaviors.stopped()
                        case Delay(how_long):
                            ctx.log.debug(
                                "Delaying initial shard listing with %s", f"{how_long:g} s"
                            )
                            ctx.timers.start_single_timer(
                                "list-shards-delay", Delayed(reply_to, frozenset[str]()), how_long
                            )
                case CoordinatorStore.AddShard(shard_id, reply_to):
                    match fail_add_shard.get(shard_id):
                        case None:
                            reply_to.tell(CoordinatorStore.UpdateDone(shard_id))
                        case NoResponse():
                            pass
                        case CrashStore():
                            raise RuntimeError("store crash on add")
                        case StopStore():
                            return Behaviors.stopped()
                        case Delay(how_long):
                            ctx.log.debug("Delaying adding shard with %s", f"{how_long:g} s")
                            ctx.timers.start_single_timer(
                                "add-shard-delay", Delayed(reply_to, frozenset[str]()), how_long
                            )
                case FailAddShard(shard_id, way_to_fail, reply_to):
                    ctx.log.debug("Failing store of %s with %s", shard_id, way_to_fail)
                    fail_add_shard = {**fail_add_shard, shard_id: way_to_fail}
                    reply_to.tell(DONE)
                case ClearFailShard(shard_id, reply_to):
                    ctx.log.debug("No longer failing store of %s", shard_id)
                    fail_add_shard = {k: v for k, v in fail_add_shard.items() if k != shard_id}
                    reply_to.tell(DONE)
                case Delayed(to, delayed):
                    to.tell(delayed)
                case _:
                    return Behaviors.unhandled
            return Behaviors.same

        return Behaviors.receive(on_message)

    return setup


@dataclass(frozen=True)
class FakeStore(DDataRememberEntitiesProvider):
    """FakeStore (RememberEntitiesFailureSpec.scala 104-108): the fake stores in place of the
    DData ones. They announce themselves to ``shard_stores`` and ``coordinator_stores``."""

    shard_stores: ActorRef[Any] | None
    coordinator_stores: ActorRef[Any] | None

    def shard_store_behavior(self, shard_id: str) -> Behavior[ShardStore.Command]:
        return cast(Behavior[ShardStore.Command], fake_shard_store(shard_id, self.shard_stores))

    def coordinator_store_behavior(self) -> Behavior[CoordinatorStore.Command]:
        return cast(
            Behavior[CoordinatorStore.Command], fake_coordinator_store(self.coordinator_stores)
        )


def start(
    monkeypatch: pytest.MonkeyPatch,
    system: Any,
    type_name: str,
    shard_stores: ActorRef[Any] | None = None,
    coordinator_stores: ActorRef[Any] | None = None,
    graceful: bool = False,
) -> ActorRef[ShardingEnvelope[Msg]]:
    """``ClusterSharding(system).start(...)`` on the fake stores; ``graceful`` for the two cases
    that pass the allocation strategy and the hand-off stop message."""

    def provider(name: str, settings: Any, replicator: Any) -> DDataRememberEntitiesProvider:
        return FakeStore(
            name,
            settings,
            settings.majority_min_cap,
            replicator,
            shard_stores,
            coordinator_stores,
        )

    monkeypatch.setattr("ohra.cluster.sharding._remember_entities_provider", provider)
    entity = Entity(EntityTypeKey[Msg](type_name), entity_actor)
    if graceful:
        entity = entity.with_stop_message(Msg("stop")).with_settings(GRACEFUL)
    else:
        entity = entity.with_settings(SETTINGS)
    return ClusterSharding(system).init(entity)


async def one_node(system: Any) -> None:
    await eventually(lambda: all_up(system, 1), timeout=5)


# --- the cases (RememberEntitiesFailureSpec.scala 217-410) ---------------------------------------


@pytest.mark.parametrize("way", WAYS, ids=str)
async def test_recover_when_initial_remember_entities_load_fails(
    monkeypatch: pytest.MonkeyPatch, way: Fail
):
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await one_node(system)
        # Getting entities for shard 1 will fail
        fail_shard_get_entities.clear()
        fail_shard_get_entities[SHARD_1] = way
        try:
            probe: TestProbe[str] = Node(system).probe()
            sharding = start(monkeypatch, system, f"initial-{way}")

            sharding.tell(ShardingEnvelope("1", Msg("hello-1", probe.ref)))
            await probe.expect_no_message(0.5)  # message is lost because shard crashes

            # Resetting initial fail
            fail_shard_get_entities.clear()

            # shard should be restarted and eventually succeed
            async def answers() -> None:
                sharding.tell(ShardingEnvelope("1", Msg("hello-1", probe.ref)))
                await probe.expect_message("hello-1")

            await probe.await_assert(answers)
        finally:
            fail_shard_get_entities.clear()


@pytest.mark.parametrize("way", WAYS, ids=str)
async def test_recover_when_shard_storing_a_start_event_fails(
    monkeypatch: pytest.MonkeyPatch, way: Fail
):
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await one_node(system)
        store_probe: TestProbe[Any] = Node(system).probe()
        sharding = start(monkeypatch, system, f"shardStoreStart-{way}", store_probe.ref)

        # trigger shard start and store creation
        probe: TestProbe[str] = Node(system).probe()
        sharding.tell(ShardingEnvelope("1", Msg("hello-1", probe.ref)))
        shard_store = (await store_probe.expect_message_type(ShardStoreCreated)).store
        await probe.expect_message("hello-1")

        # hit shard with other entity that will fail
        shard_store.tell(FailUpdateEntity(way, store_probe.ref))
        await store_probe.expect_message(DONE)

        sharding.tell(ShardingEnvelope(ID_11, Msg("hello-11", probe.ref)))

        # do we get an answer here? shard crashes
        await probe.expect_no_message(0.05)
        if isinstance(way, StopStore | CrashStore):
            # a new store should be started
            shard_store = (await store_probe.expect_message_type(ShardStoreCreated)).store

        stop_failing_probe: TestProbe[Done] = Node(system).probe()
        shard_store.tell(ClearFail(stop_failing_probe.ref))
        await stop_failing_probe.expect_message(DONE)

        # it takes a while - timeout hits and then backoff
        async def answers() -> None:
            sharding.tell(ShardingEnvelope(ID_11, Msg("hello-11-2", probe.ref)))
            await probe.expect_message("hello-11-2")

        await probe.await_assert(answers, max=10)


@pytest.mark.parametrize("way", WAYS, ids=str)
async def test_recover_on_abrupt_entity_stop_when_storing_a_stop_event_fails(
    monkeypatch: pytest.MonkeyPatch, way: Fail
):
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await one_node(system)
        store_probe: TestProbe[Any] = Node(system).probe()
        sharding = start(monkeypatch, system, f"shardStoreStopAbrupt-{way}", store_probe.ref)
        probe: TestProbe[str] = Node(system).probe()

        # trigger shard start and store creation
        sharding.tell(ShardingEnvelope("1", Msg("hello-1", probe.ref)))
        shard1_store = (await store_probe.expect_message_type(ShardStoreCreated)).store
        await probe.expect_message("hello-1")

        # fail it when stopping
        shard1_store.tell(FailUpdateEntity(way, store_probe.ref))
        await store_probe.expect_message(DONE)

        # FIXME restart without passivating is not saved and re-started again without storing
        # the stop so this isn't testing anything
        sharding.tell(ShardingEnvelope("1", Msg("stop")))

        shard1_store.tell(ClearFail(store_probe.ref))
        await store_probe.expect_message(DONE)

        # it takes a while - timeout hits and then backoff
        async def answers() -> None:
            sharding.tell(ShardingEnvelope("1", Msg("hello-2", probe.ref)))
            await probe.expect_message("hello-2")

        await probe.await_assert(answers, max=10)


@pytest.mark.parametrize("way", WAYS, ids=str)
async def test_recover_on_graceful_entity_stop_when_storing_a_stop_event_fails(
    monkeypatch: pytest.MonkeyPatch, way: Fail
):
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await one_node(system)
        store_probe: TestProbe[Any] = Node(system).probe()
        sharding = start(
            monkeypatch, system, f"shardStoreStopGraceful-{way}", store_probe.ref, graceful=True
        )
        probe: TestProbe[Any] = Node(system).probe()

        # trigger shard start and store creation
        sharding.tell(ShardingEnvelope("1", Msg("hello-1", probe.ref)))
        shard1_store = (await store_probe.expect_message_type(ShardStoreCreated)).store
        await probe.expect_message("hello-1")

        # fail it when stopping
        shard1_store.tell(FailUpdateEntity(way, store_probe.ref))
        await store_probe.expect_message(DONE)

        sharding.tell(ShardingEnvelope("1", Msg("graceful-stop")))

        if not isinstance(way, CrashStore | StopStore):
            # race, give the shard some time to see the passivation before restoring the fake
            # shard store
            await asyncio.sleep(0.125)
            shard1_store.tell(ClearFail(probe.ref))
            await probe.expect_message(DONE)

        # it takes a while?
        async def answers() -> None:
            sharding.tell(ShardingEnvelope("1", Msg("hello-2", probe.ref)))
            await probe.expect_message("hello-2")

        await probe.await_assert(answers, max=5)


@pytest.mark.parametrize("way", WAYS, ids=str)
async def test_recover_when_coordinator_storing_shard_start_fails(
    monkeypatch: pytest.MonkeyPatch, way: Fail
):
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await one_node(system)
        store_probe: TestProbe[Any] = Node(system).probe()
        sharding = start(
            monkeypatch,
            system,
            f"coordinatorStoreStopGraceful-{way}",
            coordinator_stores=store_probe.ref,
            graceful=True,
        )
        probe: TestProbe[Any] = Node(system).probe()

        # coordinator store is triggered by coordinator starting up
        coordinator_store = (await store_probe.expect_message_type(CoordinatorStoreCreated)).store
        coordinator_store.tell(FailAddShard(SHARD_1, way, probe.ref))
        await probe.expect_message(DONE)

        sharding.tell(ShardingEnvelope("1", Msg("hello-1", probe.ref)))
        await probe.expect_no_message(0.5)  # because shard cannot start while store failing

        if isinstance(way, StopStore | CrashStore):
            # a new store should be started
            coordinator_store = (
                await store_probe.expect_message_type(CoordinatorStoreCreated)
            ).store

        # fail it when stopping
        coordinator_store.tell(ClearFailShard(SHARD_1, store_probe.ref))
        await store_probe.expect_message(DONE)

        async def answers() -> None:
            sharding.tell(ShardingEnvelope("1", Msg("hello-2", probe.ref)))
            await probe.expect_message("hello-2")  # should now work again

        await probe.await_assert(answers, max=5)
