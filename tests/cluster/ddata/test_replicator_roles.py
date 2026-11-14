"""The role parts of Pekko's ``Replicator.scala`` (ReplicatorSettings roles, the require in the
constructor, ``matchingRole``) and typed ``DistributedData.scala`` (dead letters for a node
without the roles). No Pekko spec covers them."""

from __future__ import annotations

import logging
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any

import pytest

from ohra.cluster import Cluster, ClusterSettings
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.cluster.receptionist.settings import ClusterReceptionistSettings
from ohra.cluster.sharding.shard import ClusterShardingSettings
from ohra.system import ActorSystem
from ohra.testkit import TestProbe
from ohra.testkit.settings import TestKitSettings

from ...util import errors_logged, eventually, free_port
from ..test_cluster import cluster_node

BACKEND = frozenset({"backend"})
KEY = GCounterKey("roles")


def test_replicator_settings_roles():
    s = ReplicatorSettings()
    assert s.role is None and s.roles == frozenset()
    assert s.with_role("a").roles == {"a"} and s.with_role("a").role == "a"
    assert s.with_role(None).roles == frozenset()
    assert s.with_role("").roles == frozenset()
    assert s.with_roles("a", "b").roles == {"a", "b"}


def test_replicator_settings_roles_must_be_a_frozenset():
    with pytest.raises(TypeError):
        ReplicatorSettings(roles="backend")  # type: ignore[arg-type]


@dataclass
class Host:
    """What a TestProbe needs of its host."""

    system: ActorSystem[Any]
    settings: TestKitSettings = field(default_factory=TestKitSettings)


def probe_on[M](system: ActorSystem[Any]) -> TestProbe[M]:  # pyright: ignore[reportInvalidTypeVarUse]
    return TestProbe[M](Host(system))


async def three_nodes(stack: AsyncExitStack):
    ports = sorted(free_port() for _ in range(3))
    settings = ReplicatorSettings(roles=BACKEND, gossip_interval=0.2)
    nodes = [
        await stack.enter_async_context(
            cluster_node("roles", p, [ports[0]], roles=roles, distributed_data=settings)
        )
        for p, roles in zip(ports, [BACKEND, BACKEND, frozenset()])
    ]

    def all_up() -> bool:
        return all(
            len(Cluster(n).state.members) == 3
            and all(m.status == "up" for m in Cluster(n).state.members)
            for n in nodes
        )

    await eventually(all_up, timeout=10.0)
    return nodes


async def test_replicators_with_a_role_write_all_and_read_all_between_the_members_with_the_role():
    async with AsyncExitStack() as stack:
        one, two, _ = await three_nodes(stack)
        d1, d2 = DistributedData(one), DistributedData(two)
        updated: TestProbe[Replicator.UpdateResponse[GCounter]] = probe_on(one)
        d1.replicator.tell(
            Replicator.Update(
                KEY,
                GCounter(),
                Replicator.WriteAll(3.0),
                updated.ref,
                lambda c: c.increment(d1.self_unique_address),
            )
        )
        assert isinstance(await updated.receive_message(5.0), Replicator.UpdateSuccess)
        got: TestProbe[Replicator.GetResponse[GCounter]] = probe_on(two)
        d2.replicator.tell(Replicator.Get(KEY, Replicator.ReadAll(3.0), got.ref))
        reply = await got.receive_message(5.0)
        assert isinstance(reply, Replicator.GetSuccess) and reply.data.value == 1


async def test_a_node_without_the_role_gets_dead_letters(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARNING, logger="ohra.cluster.ddata.distributed_data"):
        async with AsyncExitStack() as stack:
            *_, three = await three_nodes(stack)
            dd = DistributedData(three)
            assert dd.is_terminated
            assert dd.replicator.path == "/deadLetters"
    assert (
        "Replicator points to dead letters. Make sure the cluster node has the proper role. "
        "Node has roles [dc-default], Distributed Data is configured for roles [backend]."
    ) in caplog.text


async def test_a_replicator_spawned_without_its_role_stops():
    async with AsyncExitStack() as stack:
        *_, three = await three_nodes(stack)
        probe: TestProbe[object] = probe_on(three)
        with errors_logged("ohra") as errors:
            ref = three._system_root.spawn_child(  # pyright: ignore[reportPrivateUsage]
                Replicator.behavior(ReplicatorSettings(roles=BACKEND)), "noRole", None
            ).ref
            await probe.expect_terminated(ref)
        assert any("doesn't have all the roles [backend]" in r.getMessage() for r in errors)


def test_the_receptionist_and_sharding_replicators_ignore_the_configured_roles():
    """Sharding's Replicator takes its data center's role and sharding's own role
    (ClusterSharding.scala 739-742), never ``distributed_data.roles``."""
    cs = ClusterSettings(distributed_data=ReplicatorSettings(roles=BACKEND))
    assert ClusterReceptionistSettings().effective_replicator_settings(cs).roles == frozenset()
    sharding = ClusterShardingSettings().effective_replicator_settings(cs, None)
    assert sharding.roles == frozenset({"dc-default"})
