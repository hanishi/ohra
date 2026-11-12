"""RememberEntitiesAndStartEntitySpec, ported from Apache Pekko's cluster-sharding/src/test/scala/
org/apache/pekko/cluster/sharding/RememberEntitiesAndStartEntitySpec.scala.

Cases, in Scala order:

- "remember entities started with StartEntity": ported here.

Differences from the Scala spec:

- one real cluster node that joins itself, with ``remember_entities``, ``verbose_debug_logging``
  and ``fail_on_invalid_entity_state_transition`` on, and sharding's Replicator in memory, as the
  spec's "no leaks between test runs";
- Pekko's ``StartEntity`` with its sender is the internal ``_StartEntity(id, ack_to)``; the
  public ``StartEntity`` is acked by no one, as in Pekko Typed;
- the classic test takes the shard as the ack's ``lastSender``; a typed message has no sender,
  so the entity reports its ``EntityContext.shard`` on ``GiveMeShard``;
- ``PoisonPill`` is ``_LeaseLost(None)``, which stops the shard at once as ``PoisonPill`` does;
- Pekko's ``id % 10`` shard ids are ``shard_id_of``: the second id is the first that shares the
  shard of ``"1"``, as Pekko's 11 does.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, cast

from ohra import (
    ActorRef,
    Behavior,
    Behaviors,
    ClusterSharding,
    Entity,
    EntityContext,
    EntityTypeKey,
    ShardingEnvelope,
)
from ohra.cluster.ddata import ReplicatorSettings
from ohra.cluster.sharding import shard_id_of
from ohra.cluster.sharding.shard import (
    GetShardStats,
    ShardStats,
    StartEntityAck,
    _LeaseLost,  # pyright: ignore[reportPrivateUsage]
    _StartEntity,  # pyright: ignore[reportPrivateUsage]
)

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING
from .test_ddata_shard_coordinator import Node

SETTINGS = replace(
    FAST_SHARDING,
    remember_entities=True,
    verbose_debug_logging=True,
    fail_on_invalid_entity_state_transition=True,
    distributed_data=ReplicatorSettings(durable_keys=frozenset()),
)


@dataclass(frozen=True)
class GiveMeShard:
    reply_to: ActorRef[ActorRef[Any]]


KEY = EntityTypeKey[GiveMeShard]("startEntity")


def entity_actor(entity: EntityContext[GiveMeShard]) -> Behavior[GiveMeShard]:
    @Behaviors.receive_message
    def receive(msg: GiveMeShard) -> Behavior[GiveMeShard]:
        msg.reply_to.tell(entity.shard)
        return Behaviors.same

    return receive


async def test_remember_entities_started_with_start_entity():
    # "remember entities started with StartEntity"
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        sharding = ClusterSharding(system).init(Entity(KEY, entity_actor).with_settings(SETTINGS))
        probe, shards = Node(system).probe(), Node(system).probe()
        shard_id = shard_id_of("1", SETTINGS.number_of_shards)

        cast(ActorRef[Any], sharding).tell(_StartEntity("1", probe.ref))
        await probe.expect_message(StartEntityAck("1", shard_id), 5)
        sharding.tell(ShardingEnvelope("1", GiveMeShard(shards.ref)))
        shard: ActorRef[Any] = await shards.receive_message(5)

        shard.tell(_LeaseLost(None))
        await probe.expect_terminated(shard, 5)

        # trigger shard start by messaging other actor in it
        other = next(
            str(i)
            for i in range(2, 1000)
            if shard_id_of(str(i), SETTINGS.number_of_shards) == shard_id
        )

        # race condition between this message and region getting the termination message, we
        # may need to retry
        async def second_shard_incarnation() -> ActorRef[Any]:
            sharding.tell(ShardingEnvelope(other, GiveMeShard(shards.ref)))
            return await shards.receive_message(1)

        second = await shards.await_assert(second_shard_incarnation, max=10)

        async def remembered_and_started() -> None:
            second.tell(GetShardStats(probe.ref))
            # the remembered 1 and the other which we just triggered start of
            assert await probe.receive_message(1) == ShardStats(shard_id, 2)

        await probe.await_assert(remembered_and_started, max=10)
