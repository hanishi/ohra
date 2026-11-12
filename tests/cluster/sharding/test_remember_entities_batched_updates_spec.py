"""RememberEntitiesBatchedUpdatesSpec, ported from Apache Pekko's cluster-sharding/src/test/scala/
org/apache/pekko/cluster/sharding/RememberEntitiesBatchedUpdatesSpec.scala.

Cases, in Scala order:

- "Batching of starts and stops" "work": ported here.

Differences from the Scala spec:

- one real cluster node that joins itself, with ``remember_entities``, ``verbose_debug_logging``
  and ``fail_on_invalid_entity_state_transition`` on, and sharding's Replicator in memory, as the
  spec's "no leaks between test runs";
- the spec's single shard ``"1"`` is ``number_of_shards=1``;
- the classic ``EntityActor`` is a typed entity: it reports ``Started`` when it is set up and
  ``Stopped`` on ``PostStop``, and ``GracefulStop`` sends ``Passivate`` to its shard;
- ``receiveN`` is ``receive_messages``, and the reports are counted: the starts of the batch, and
  the stops of the entities passivated.
"""

from __future__ import annotations

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
    Signal,
)
from ohra.cluster.ddata import ReplicatorSettings
from ohra.signals import PostStop

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING
from .test_ddata_shard_coordinator import Node

SETTINGS = replace(
    FAST_SHARDING,
    number_of_shards=1,
    remember_entities=True,
    verbose_debug_logging=True,
    fail_on_invalid_entity_state_transition=True,
    distributed_data=ReplicatorSettings(durable_keys=frozenset()),
)


@dataclass(frozen=True)
class Started:
    id: int


@dataclass(frozen=True)
class Stopped:
    id: int


@dataclass(frozen=True)
class GracefulStop:
    pass


@dataclass(frozen=True)
class Start:
    pass


@dataclass(frozen=True)
class Ping:
    pass


type Command = GracefulStop | Start | Ping

KEY = EntityTypeKey[Command]("batching")


def entity_actor(entity: EntityContext[Command], probe: ActorRef[Any]) -> Behavior[Command]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Command]) -> Behavior[Command]:
        probe.tell(Started(int(entity.entity_id)))

        def on_message(ctx: ActorContext[Command], msg: Command) -> Behavior[Command]:
            match msg:
                case GracefulStop():
                    ctx.log.debug("Got a graceful stop, requesting passivation")
                    entity.shard.tell(Passivate(ctx.self))
                case Start():
                    ctx.log.debug("Got a start")
                case Ping():
                    pass
            return Behaviors.same

        def on_signal(_ctx: ActorContext[Command], sig: Signal) -> Behavior[Command]:
            if sig is PostStop:
                probe.tell(Stopped(int(entity.entity_id)))
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    return setup


async def test_batching_of_starts_and_stops_work():
    # "Batching of starts and stops" "work"
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        probe = Node(system).probe()
        sharding = ClusterSharding(system).init(
            Entity(KEY, lambda e: entity_actor(e, probe.ref)).with_settings(SETTINGS)
        )

        # make sure that sharding is up and running
        sharding.tell(ShardingEnvelope("0", Ping()))
        await probe.expect_message(Started(0), 5)

        # start 20, should write first and batch the rest
        for i in range(1, 21):
            sharding.tell(ShardingEnvelope(str(i), Start()))
        reports = await probe.receive_messages(20, 5)
        assert set(reports) == {Started(i) for i in range(1, 21)}

        # start 20 more, and stop the previous ones that are already running,
        # should create a mixed batch of start + stops
        for i in range(21, 41):
            sharding.tell(ShardingEnvelope(str(i), Start()))
            sharding.tell(ShardingEnvelope(str(i - 20), GracefulStop()))
        reports = await probe.receive_messages(40, 5)
        assert set(reports) == {Started(i) for i in range(21, 41)} | {
            Stopped(i) for i in range(1, 21)
        }

        # stop the last 20, should batch stops only
        for i in range(21, 41):
            sharding.tell(ShardingEnvelope(str(i), GracefulStop()))
        reports = await probe.receive_messages(20, 5)
        assert set(reports) == {Stopped(i) for i in range(21, 41)}
