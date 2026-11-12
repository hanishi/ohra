"""EntityTerminationSpec, ported from Apache Pekko's cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/EntityTerminationSpec.scala.

The spec checks that the automatic restart of an entity that stopped, which remembered entities
bring, does not apply when they are off. Cases, in Scala order:

- "allow stop without passivation if not remembering entities" (89): ported here.
- "automatically restart a terminating entity (not passivating) if remembering entities" (119):
  ported here.
- "allow terminating entity to passivate if remembering entities" (144): ported here.

Differences from the Scala spec:

- one real cluster node that joins itself, with ``verbose_debug_logging`` and
  ``fail_on_invalid_entity_state_transition`` on, as the spec's config;
- the ``StoppingActor`` is a typed entity: ``Stop`` returns ``Behaviors.stopped()``, ``Ping``
  replies ``Pong("pong-<n>", entity)`` to its ``reply_to``, as the classic test takes the
  entity from ``lastSender``, and ``PassivateMe`` sends ``Passivate`` to its shard;
- the remembering cases keep sharding's Replicator in memory, as the spec's "no leaks between
  test runs";
- the spec's single shard ``"1"`` is ``number_of_shards=1``, so ohra's shard is ``"0"``;
- ``Thread.sleep(400)`` is ``asyncio.sleep(0.4)``: Pekko waits out a restart backoff that must
  not happen, and no condition can show a non-event;
- the ``GetShardRegionState`` reply is ``CurrentShardRegionState(frozenset({"2"}),
  frozenset({"0"}))``: the live entity ids, and the shards; ``awaitAssert(..., 2.seconds)`` is
  ``await_assert(..., max=2)``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any

from ohra import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ClusterSharding,
    Entity,
    EntityContext,
    EntityTypeKey,
    Passivate,
    ShardingEnvelope,
    ask,
)
from ohra.cluster.ddata import ReplicatorSettings
from ohra.cluster.sharding.shard import CurrentShardRegionState, GetShardRegionState
from ohra.testkit import TestProbe
from ohra.testkit.settings import TestKitSettings

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[Pong]


@dataclass(frozen=True)
class Pong:
    text: str
    entity: ActorRef[Any]


@dataclass(frozen=True)
class Stop:
    pass


@dataclass(frozen=True)
class PassivateMe:
    pass


type Command = Ping | Stop | PassivateMe

STOPPING = EntityTypeKey[Command]("regular")


def stopping_actor(entity: EntityContext[Command]) -> Behavior[Command]:
    def active(counter: int) -> Behavior[Command]:
        def on_message(ctx: ActorContext[Command], msg: Command) -> Behavior[Command]:
            match msg:
                case Stop():
                    return Behaviors.stopped()
                case Ping(reply_to):
                    reply_to.tell(Pong(f"pong-{counter + 1}", ctx.self))
                    return active(counter + 1)
                case PassivateMe():
                    entity.shard.tell(Passivate(ctx.self))
                    return Behaviors.same

        return Behaviors.receive(on_message)

    return active(0)


@dataclass
class Host:
    system: Any
    settings: TestKitSettings

    @staticmethod
    def of(system: Any) -> Host:
        return Host(system, TestKitSettings())


async def test_allow_stop_without_passivation_if_not_remembering_entities():
    # "allow stop without passivation if not remembering entities" (89)
    port = free_port()
    settings = replace(
        FAST_SHARDING,
        number_of_shards=1,
        verbose_debug_logging=True,
        fail_on_invalid_entity_state_transition=True,
        entity_restart_backoff=0.25,
    )
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        probe: TestProbe[Pong] = TestProbe(Host.of(system))
        sharding = ClusterSharding(system).init(
            Entity(STOPPING, stopping_actor).with_settings(settings)
        )

        sharding.tell(ShardingEnvelope("1", Ping(probe.ref)))
        first = await probe.receive_message(5)
        assert first.text == "pong-1"
        entity = first.entity

        sharding.tell(ShardingEnvelope("2", Ping(probe.ref)))
        assert (await probe.receive_message(5)).text == "pong-1"

        sharding.tell(ShardingEnvelope("1", Stop()))
        await probe.expect_terminated(entity, 5)

        await asyncio.sleep(0.4)  # restart backoff is 250 ms
        state: CurrentShardRegionState = await ask(sharding, GetShardRegionState, timeout=3)
        assert state == CurrentShardRegionState(frozenset({"2"}), frozenset({"0"}))

        # make sure the shard didn't crash (coverage for regression bug #29383)
        sharding.tell(ShardingEnvelope("2", Ping(probe.ref)))
        # if it lost state we know it restarted
        assert (await probe.receive_message(5)).text == "pong-2"


REMEMBERING = replace(
    FAST_SHARDING,
    number_of_shards=1,
    verbose_debug_logging=True,
    fail_on_invalid_entity_state_transition=True,
    entity_restart_backoff=0.25,
    remember_entities=True,
    distributed_data=ReplicatorSettings(durable_keys=frozenset()),
)


async def test_automatically_restart_a_terminating_entity_if_remembering_entities():
    # "automatically restart a terminating entity (not passivating) if remembering entities" (119)
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        probe: TestProbe[Pong] = TestProbe(Host.of(system))
        sharding = ClusterSharding(system).init(
            Entity(EntityTypeKey[Command]("remembering"), stopping_actor).with_settings(
                REMEMBERING
            )
        )

        sharding.tell(ShardingEnvelope("1", Ping(probe.ref)))
        first = await probe.receive_message(5)
        assert first.text == "pong-1"

        sharding.tell(ShardingEnvelope("1", Stop()))
        await probe.expect_terminated(first.entity, 5)

        await asyncio.sleep(0.4)  # restart backoff is 250 ms

        async def restarted() -> None:
            state: CurrentShardRegionState = await ask(sharding, GetShardRegionState, timeout=3)
            assert state.shard_ids == frozenset({"0"})
            assert len(state.entity_ids) == 1

        await probe.await_assert(restarted, max=2)


async def test_allow_terminating_entity_to_passivate_if_remembering_entities():
    # "allow terminating entity to passivate if remembering entities" (144)
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        probe: TestProbe[Pong] = TestProbe(Host.of(system))
        sharding = ClusterSharding(system).init(
            Entity(EntityTypeKey[Command]("remembering"), stopping_actor).with_settings(
                REMEMBERING
            )
        )

        sharding.tell(ShardingEnvelope("1", Ping(probe.ref)))
        first = await probe.receive_message(5)
        assert first.text == "pong-1"

        sharding.tell(ShardingEnvelope("1", PassivateMe()))
        await probe.expect_terminated(first.entity, 5)
        await asyncio.sleep(0.4)  # restart backoff is 250 ms

        state: CurrentShardRegionState = await ask(sharding, GetShardRegionState, timeout=3)
        assert state == CurrentShardRegionState(frozenset(), frozenset({"0"}))
