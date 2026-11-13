"""ProxyShardingSpec, ported from Apache Pekko's cluster-sharding/src/test/scala/org/apache/pekko/
cluster/sharding/ProxyShardingSpec.scala, with what ``ClusterSharding.init`` does on a node
without the shard role (ClusterSharding.scala 300-334, ``internalStart``): a proxy-only region,
the coordinator only where its role is, and one Replicator per role (720-760).

The Scala's three cases:

- "Proxy should be found": ``init`` on a node without the role starts the proxy
  ``/system/sharding/<type>Proxy``;
- "Shard region should be found": on a node with the role, the region ``/system/sharding/<type>``;
- "Shard coordinator should be found": ohra's coordinator manager is
  ``/system/singletonManager<type>Coordinator`` (appendix B), and only a node with the
  coordinator's role runs it.

Differences from the Scala spec: Pekko starts the proxy (``startProxy``) and the region
(``start``) on one system by name; ohra's one API is ``init``, so the proxy is on a second node,
without the role. The rest of the tests are this port's own: what the proxy delivers, that it
hosts nothing, that it follows a coordinator that moves, the Replicator names, and a coordinator
role apart from the shard role (``coordinator_singleton_override_role=False``)."""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass, replace
from typing import Any

import pytest

from ohra import (
    ActorRef,
    ActorSystem,
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
    ask,
    remotable,
)
from ohra.cluster.core import Cluster, Down
from ohra.remote import Address

from ...util import eventually, free_port
from ..test_cluster import all_up, unreachable_ports
from ..test_cluster_sharding import FAST_SHARDING, crash, node


@remotable
@dataclass(frozen=True)
class ProxyWhere:
    reply_to: ActorRef[int]


KEY = EntityTypeKey[ProxyWhere]("myType")

BACKEND = replace(FAST_SHARDING, role="backend")


def where(_entity: EntityContext[ProxyWhere], port: int) -> Behavior[ProxyWhere]:
    """Answers with the port of its node."""

    @Behaviors.receive_message
    def receive(msg: ProxyWhere) -> Behavior[ProxyWhere]:
        msg.reply_to.tell(port)
        return Behaviors.same

    return receive


def init(system: ActorSystem[Any], settings: ClusterShardingSettings = BACKEND) -> ActorRef[Any]:
    port = Cluster(system).self_address.port
    entity = Entity(KEY, lambda e: where(e, port)).with_settings(settings)
    return ClusterSharding(system).init(entity)


async def ask_where(system: ActorSystem[Any], entity_id: str, timeout: float = 5) -> int:
    entity = ClusterSharding(system).entity_ref_for(KEY, entity_id)
    return await ask(entity, ProxyWhere, timeout)


def root(system: ActorSystem[Any]) -> str:
    return Cluster(system).self_address.root


def backend(port: int, seed: int) -> Any:
    return node(port, [seed], roles=frozenset({"backend"}))


async def test_proxy_should_be_found():
    pb, pf = free_port(), free_port()
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(backend(pb, pb))
        f = await stack.enter_async_context(node(pf, [pb]))
        await eventually(lambda: all_up(b, 2) and all_up(f, 2), timeout=5)
        init(b)
        proxy = init(f)
        assert proxy.path == f"{root(f)}/system/sharding/myTypeProxy"
        assert f.resolve(f"{root(f)}/system/sharding/myTypeProxy").path.endswith("Proxy")


async def test_shard_region_and_coordinator_should_be_found():
    pb, pf = free_port(), free_port()
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(backend(pb, pb))
        f = await stack.enter_async_context(node(pf, [pb]))
        await eventually(lambda: all_up(b, 2) and all_up(f, 2), timeout=5)
        region = init(b)
        init(f)
        assert region.path.endswith("/system/sharding/myType")
        manager = "/system/singletonManagermyTypeCoordinator"
        assert b.resolve(f"{root(b)}{manager}").path.endswith("Coordinator")
        with pytest.raises(LookupError):
            f.resolve(f"{root(f)}{manager}")


async def test_a_proxy_delivers_to_an_entity_on_a_node_with_the_role():
    pb, pf = free_port(), free_port()
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(backend(pb, pb))
        f = await stack.enter_async_context(node(pf, [pb]))
        await eventually(lambda: all_up(b, 2) and all_up(f, 2), timeout=5)
        init(b)
        init(f)
        assert await ask_where(f, "1") == pb


async def test_a_proxy_hosts_no_shards():
    pb, pf = free_port(), free_port()
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(backend(pb, pb))
        f = await stack.enter_async_context(node(pf, [pb]))
        await eventually(lambda: all_up(b, 2) and all_up(f, 2), timeout=5)
        init(b)
        proxy = init(f)
        for i in range(5):
            assert await ask_where(f, str(i)) == pb
        state: CurrentShardRegionState = await ask(proxy, GetShardRegionState, 3)
        assert state.shard_ids == frozenset()
        assert state.entity_ids == frozenset()


async def test_a_proxy_registers_again_when_the_coordinator_moves():
    p1, p2, pf = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        b1 = await stack.enter_async_context(backend(p1, p1))
        b2 = await stack.enter_async_context(backend(p2, p1))
        f = await stack.enter_async_context(node(pf, [p1]))
        nodes = [b1, b2, f]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        for n in nodes:
            init(n)
        assert await ask_where(f, "before") in (p1, p2)
        await crash(b1)
        await eventually(lambda: unreachable_ports(f) == {p1}, timeout=5)
        Cluster(f).manager.tell(Down(Address("c", "127.0.0.1", p1)))
        await eventually(lambda: all_up(b2, 2) and all_up(f, 2), timeout=5)
        assert await ask_where(f, "after", timeout=8) == p2


async def test_the_replicators_are_one_per_role():
    pb, pr = free_port(), free_port()
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(backend(pb, pb))
        await eventually(lambda: all_up(b, 1), timeout=5)
        init(b)
        assert b.resolve(f"{root(b)}/system/sharding/backendReplicator")
        with pytest.raises(LookupError):
            b.resolve(f"{root(b)}/system/sharding/replicator")
        spaced = await stack.enter_async_context(node(pr, [pr], roles=frozenset({"back end"})))
        await eventually(lambda: all_up(spaced, 1), timeout=5)
        init(spaced, replace(FAST_SHARDING, role="back end"))
        # Java's URLEncoder, as Pekko's replicator name
        assert spaced.resolve(f"{root(spaced)}/system/sharding/back+endReplicator")


SPLIT = replace(
    FAST_SHARDING,
    role="shard",
    coordinator_singleton_override_role=False,
    coordinator_singleton_settings=ClusterSingletonSettings(role="coordinator"),
)
"""ClusterShardingCoordinatorRoleSpec's settings: the coordinator on its own role."""


async def test_a_node_without_the_coordinator_role_runs_no_coordinator():
    pc, ps = free_port(), free_port()
    async with AsyncExitStack() as stack:
        c = await stack.enter_async_context(node(pc, [pc], roles=frozenset({"coordinator"})))
        s = await stack.enter_async_context(node(ps, [pc], roles=frozenset({"shard"})))
        await eventually(lambda: all_up(c, 2) and all_up(s, 2), timeout=5)
        proxy = init(c, SPLIT)
        init(s, SPLIT)
        manager = "/system/singletonManagermyTypeCoordinator"
        assert proxy.path.endswith("myTypeProxy")
        assert c.resolve(f"{root(c)}{manager}")
        with pytest.raises(LookupError):
            s.resolve(f"{root(s)}{manager}")
        assert await ask_where(s, "1") == ps
        assert await ask_where(c, "2") == ps
