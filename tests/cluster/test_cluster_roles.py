"""Cluster roles on the settings, members and the join, as Pekko's ClusterConfigSpec and
ClusterReadView (a member not in the state is a removed one with the self roles)."""

from __future__ import annotations

import json
import re

import pytest

from contextlib import AsyncExitStack

from ohra.cluster import (
    Cluster,
    ClusterDomainEvent,
    ClusterSettings,
    LeaderChanged,
    RoleLeaderChanged,
    Subscribe,
)
from ohra.cluster.core import JoinNode
from ohra.cluster.gossip import Member, removed_placeholder
from ohra.remote import Address, UniqueAddress
from ohra.remote.serialization import Serialization

from ..util import eventually, free_port
from .test_cluster import cluster_node, probe

MSG = (
    "Roles must not start with 'dc-' as that is reserved for the cluster self-data-center setting"
)
NODE = UniqueAddress("127.0.0.1", 1, 11, "s")


def test_roles_default_to_the_data_center_role():
    s = ClusterSettings()
    assert s.roles == frozenset() and s.self_roles == frozenset({"dc-default"})
    assert s.min_nr_of_members_of_role == {}


def test_configured_roles_are_kept_with_the_data_center_role():
    s = ClusterSettings(roles=frozenset({"hamlet"}))
    assert s.self_roles == frozenset({"hamlet", "dc-default"})


@pytest.mark.parametrize("role", ["dc-default", "dc-blue"])
def test_a_role_starting_with_dc_is_rejected(role: str):
    with pytest.raises(ValueError, match=re.escape(MSG)):
        ClusterSettings(roles=frozenset({role}))


def test_roles_given_as_a_string_are_rejected():
    with pytest.raises(TypeError):
        ClusterSettings(roles="backend")  # type: ignore[arg-type]


@pytest.mark.parametrize("n", [0, -1])
def test_min_nr_of_members_of_role_must_be_positive(n: int):
    with pytest.raises(ValueError):
        ClusterSettings(min_nr_of_members_of_role={"backend": n})


def test_member_has_role_and_data_center():
    m = Member(NODE, 1, "up", frozenset({"backend", "dc-default"}))
    assert m.has_role("backend") and not m.has_role("frontend") and m.data_center == "default"


def test_member_without_a_data_center_role_has_no_data_center():
    with pytest.raises(ValueError):
        Member(NODE, 1, "up", frozenset({"backend"})).data_center


def test_removed_placeholder_has_the_unknown_data_center():
    assert removed_placeholder(NODE).roles == frozenset({"dc-N/A"})


def test_roles_cross_the_wire():
    roles = frozenset({"backend", "dc-default"})
    member = Member(NODE, 1, "up", roles)
    join = JoinNode(NODE, roles, None)  # type: ignore[arg-type]
    for value in (member, join):
        data = json.loads(json.dumps(Serialization.encode(value, lambda r: r.path)))
        decoded = Serialization.decode(data, lambda p: pytest.fail(p))
        assert decoded.roles == roles


async def test_members_carry_their_roles_on_every_node():
    p1, p2 = free_port(), free_port()
    async with (
        cluster_node("roles", p1, [p1], roles=frozenset({"frontend"})) as first,
        cluster_node("roles", p2, [p1], roles=frozenset({"backend"})) as second,
    ):

        def roles_by_port(system: object) -> dict[int, frozenset[str]]:
            return {m.unique_address.port: m.roles for m in Cluster(system).state.members}  # type: ignore[arg-type]

        expected = {
            p1: frozenset({"frontend", "dc-default"}),
            p2: frozenset({"backend", "dc-default"}),
        }
        for system in (first, second):
            await eventually(lambda s=system: roles_by_port(s) == expected, timeout=10.0)


async def test_self_member_before_joining_carries_the_self_roles():
    async with cluster_node("roles", free_port(), [], roles=frozenset({"backend"})) as system:
        cluster = Cluster(system)
        want = frozenset({"backend", "dc-default"})
        assert cluster.self_member.status == "removed" and cluster.self_member.roles == want
        assert cluster.self_roles == want


async def test_role_leader_map_agrees_on_every_node():
    ports = sorted(free_port() for _ in range(3))
    roles = [{"frontend"}, {"backend"}, {"backend"}]
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(cluster_node("c", p, [ports[0]], roles=frozenset(r)))
            for p, r in zip(ports, roles)
        ]
        await eventually(lambda: all(len(Cluster(n).state.members) == 3 for n in nodes), timeout=5)
        first, second = (Address("c", "127.0.0.1", p) for p in ports[:2])
        want = {"dc-default": first, "frontend": first, "backend": second}
        await eventually(
            lambda: all(Cluster(n).state.role_leader_map == want for n in nodes), timeout=5
        )


async def test_role_leader_map_holds_the_data_center_role_without_configured_roles():
    port = free_port()
    async with cluster_node("c", port, [port]) as a:
        await eventually(lambda: Cluster(a).self_member.status == "up", timeout=3)
        state = Cluster(a).state
        assert state.role_leader_map == {"dc-default": Address("c", "127.0.0.1", port)}
        assert state.all_roles == {"dc-default"} and state.role_leader("nope") is None


async def test_role_leader_changed_follows_leader_changed():
    port = free_port()
    events: list[ClusterDomainEvent] = []
    async with cluster_node("c", port, [port], roles=frozenset({"backend"})) as a:
        Cluster(a).subscriptions.tell(Subscribe(probe(events), ClusterDomainEvent))
        await eventually(lambda: Cluster(a).self_member.status == "up", timeout=3)
        me = Address("c", "127.0.0.1", port)
        want = {RoleLeaderChanged("backend", me), RoleLeaderChanged("dc-default", me)}
        await eventually(lambda: want <= {e for e in events if isinstance(e, RoleLeaderChanged)})
        first_leader = next(i for i, e in enumerate(events) if isinstance(e, LeaderChanged))
        assert all(
            i > first_leader for i, e in enumerate(events) if isinstance(e, RoleLeaderChanged)
        )
