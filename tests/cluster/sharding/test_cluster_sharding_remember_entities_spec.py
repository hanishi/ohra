"""ClusterShardingRememberEntitiesSpec, ported from Apache Pekko's cluster-sharding/src/multi-jvm/
scala/org/apache/pekko/cluster/sharding/ClusterShardingRememberEntitiesSpec.scala, the DData mode
with remember entities on and the DData store (``DDataClusterShardingRememberEntitiesEnabled``):

- "start remembered entities when coordinator fail over" (164-205);
- "start remembered entities in new cluster" (207-230).

Each entity reports ``Started`` to a probe of the node it starts on, as ``EntityActor``
(MultiNodeClusterShardingSpec.scala), and answers ``Echo`` with its number. With remember
entities on, ``expectEntityRestarted`` (149-160) sends nothing: it expects ``Started`` only.

Differences from the Scala spec:

- the nodes run on one event loop, without the role-less node ``first``, which only conducts the
  test there; ``testConductor.exit(second)`` is a crash, and the Scala's auto-downing
  (``auto-down-unreachable-after = 0s``) is a ``Down`` from the surviving node;
- Pekko's ``Thread.sleep(5000)`` before the crash, which gives the gossip time to bring entity 1's
  remembered state to ``third``, is a wait until ``third``'s Replicator holds it: the shard in
  ``shard-Entity-all``, the entity in its shard's keys;
- the fail-over case keeps sharding's Replicator in memory, as ``third``'s durable directory
  would not hold what ``second`` wrote either;
- the cases run on clusters of their own: the new-cluster case first remembers entity 1 on a
  one-node cluster with a SQLite durable directory under ``tmp_path``, in place of the fail-over
  case's ``third``; that node leaves, and a new system on the same port and directory starts it;
- the settings are ``FAST_SHARDING``'s, with remember entities and
  ``fail_on_invalid_entity_state_transition`` on."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    ShardingEnvelope,
    ask,
)
from ohra.cluster.core import Cluster, Down, Leave
from ohra.cluster.ddata import Replicator, ReplicatorSettings, sqlite_durable_store
from ohra.cluster.ddata.gset import GSetKey
from ohra.cluster.ddata.orset import ORSetKey
from ohra.cluster.sharding import shard_id_of
from ohra.remote import Address

from ...util import eventually, free_port
from ..test_cluster import all_up, unreachable_ports
from ..test_cluster_sharding import FAST_SHARDING, crash, node
from .test_ddata_shard_coordinator import Node

REMEMBER = replace(
    FAST_SHARDING, remember_entities=True, fail_on_invalid_entity_state_transition=True
)

IN_MEMORY = replace(
    REMEMBER, distributed_data=ReplicatorSettings(prefer_oldest=True, max_delta_elements=5)
)


def durable(dir: Path) -> ClusterShardingSettings:
    """Sharding's remember-entities data in a SQLite durable directory, as ``shard-*``."""
    return replace(
        REMEMBER,
        distributed_data=ReplicatorSettings(
            prefer_oldest=True,
            max_delta_elements=5,
            durable_keys=frozenset({"shard-*"}),
            durable_store=lambda: sqlite_durable_store(dir),
        ),
    )


@dataclass(frozen=True)
class Echo:
    n: int
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Started:
    entity_id: str


KEY = EntityTypeKey[Echo]("Entity")


def entity_actor(entity: EntityContext[Echo], probe: ActorRef[Started]) -> Behavior[Echo]:
    """MultiNodeClusterShardingSpec's ``EntityActor``: ``Started`` to ``probe``, then echoes."""

    @Behaviors.setup
    def setup(_ctx: ActorContext[Echo]) -> Behavior[Echo]:
        probe.tell(Started(entity.entity_id))

        @Behaviors.receive_message
        def receive(msg: Echo) -> Behavior[Echo]:
            msg.reply_to.tell(msg.n)
            return Behaviors.same

        return receive

    return setup


def start_sharding(
    system: ActorSystem[Any], probe: ActorRef[Started], settings: ClusterShardingSettings
) -> ActorRef[ShardingEnvelope[Echo]]:
    """133-141."""
    entity = Entity(KEY, lambda e: entity_actor(e, probe))
    return ClusterSharding(system).init(entity.with_settings(settings))


async def remembered_by(system: ActorSystem[Any], region: ActorRef[Any], entity_id: str) -> None:
    """The sharding Replicator beside ``region`` holds ``entity_id``'s shard in the coordinator's
    remembered shards and the entity in its shard's keys."""
    replicator = system.resolve(f"{region.path.rsplit('/', 1)[0]}/replicator")
    shard = shard_id_of(entity_id, REMEMBER.number_of_shards)
    shards_key = GSetKey[str](f"shard-{KEY.name}-all")
    got = await ask(replicator, lambda r: Replicator.Get(shards_key, Replicator.ReadLocal(), r), 3)
    assert isinstance(got, Replicator.GetSuccess)
    assert got.data.contains(shard)
    ids = frozenset[str]()
    for i in range(5):
        key = ORSetKey[str](f"shard-{KEY.name}-{shard}-{i}")
        got = await ask(replicator, lambda r: Replicator.Get(key, Replicator.ReadLocal(), r), 3)
        if isinstance(got, Replicator.GetSuccess):
            ids |= got.data.elements
    assert entity_id in ids


async def test_start_remembered_entities_when_coordinator_fail_over():
    """164-205: entity 1, remembered on ``second``, starts on ``third`` when ``second`` crashes and
    the coordinator fails over, with no message sent."""
    p2, p3 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        second = await stack.enter_async_context(node(p2, [p2]))
        await eventually(lambda: all_up(second, 1), timeout=5)
        entity_probe = Node(second).probe()
        start_sharding(second, entity_probe.ref, IN_MEMORY)
        entity = ClusterSharding(second).entity_ref_for(KEY, "1")
        assert await ask(entity, lambda r: Echo(1, r), timeout=5) == 1
        await entity_probe.expect_message(Started("1"), 3)

        third = await stack.enter_async_context(node(p3, [p2]))
        third_probe = Node(third).probe()
        third_region = start_sharding(third, third_probe.ref, IN_MEMORY)
        await eventually(lambda: all_up(second, 2) and all_up(third, 2), timeout=5)

        # Pekko's Thread.sleep(5000): entity 1's remembered state reaches third by gossip
        await third_probe.await_assert(lambda: remembered_by(third, third_region, "1"), max=10)
        await crash(second)
        await eventually(lambda: unreachable_ports(third) == {p2}, timeout=5)
        Cluster(third).manager.tell(Down(Address("c", "127.0.0.1", p2)))

        await third_probe.expect_message(Started("1"), 30)


async def test_start_remembered_entities_in_new_cluster(tmp_path: Path):
    """207-230: a new system on the durable directory of a cluster that is gone starts entity 1,
    with no message sent."""
    settings = durable(tmp_path / "sharding-third")
    p = free_port()
    async with node(p, [p]) as third:
        await eventually(lambda: all_up(third, 1), timeout=5)
        entity_probe = Node(third).probe()
        start_sharding(third, entity_probe.ref, settings)
        entity = ClusterSharding(third).entity_ref_for(KEY, "1")
        assert await ask(entity, lambda r: Echo(1, r), timeout=5) == 1
        await entity_probe.expect_message(Started("1"), 3)

        # the last node leaves: no nodes are left of the original cluster
        cluster = Cluster(third)
        cluster.manager.tell(Leave(cluster.self_unique_address.address))
        await asyncio.wait_for(third.when_terminated(), timeout=5)

    async with node(p, [p]) as sys2:
        entity_probe2 = Node(sys2).probe()
        await eventually(lambda: all_up(sys2, 1), timeout=5)
        start_sharding(sys2, entity_probe2.ref, settings)
        await entity_probe2.expect_message(Started("1"), 30)
