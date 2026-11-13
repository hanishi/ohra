"""Pekko's MinMembersBeforeUpSpec: the leader keeps members Joining until the minimum number of
members, and of members with a role, has joined. All three configs of the spec are here.

``registerOnMemberUp`` has no ohra counterpart; a ``SelfUp`` subscriber stands in for it. Pekko
checks five times, a second apart, that the members stay Joining; here the checks are 0.1 s apart,
five rounds of ``FAST``'s leader interval.
"""

import asyncio
from typing import Any

from ohra.cluster import Cluster, ClusterSettings, Join, SelfUp, Subscribe
from ohra.remote import Address
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, RoleName, multi_node_spec

from .test_cluster import FAST, FAST_SHUTDOWN


type Nodes = tuple[MultiNodeConfig, RoleName, RoleName, RoleName]


def make_config(settings: dict[str, Any], roles: tuple[str, str, str] | None = None) -> Nodes:
    config = MultiNodeConfig()
    first, second, third = (config.role(r) for r in ("first", "second", "third"))
    config.common_config(cluster=ClusterSettings(**{**FAST, **settings}), shutdown=FAST_SHUTDOWN)
    for role, name in zip((first, second, third), roles or ()):
        config.node_config(
            role,
            cluster=ClusterSettings(**{**FAST, **settings}, roles=frozenset({name})),
        )
    return config, first, second, third


async def wait_moving_members_to_up(spec: MultiNodeSpec, nodes: Nodes) -> None:
    _, first, second, third = nodes
    cluster = Cluster(spec.system)
    self_up = spec.create_test_probe()
    cluster.subscriptions.tell(Subscribe(self_up.ref, SelfUp))

    def statuses() -> set[str]:
        return {m.status for m in cluster.state.members}

    def addresses() -> set[Address]:
        return {m.unique_address.address for m in cluster.state.members}

    if spec.is_node(first):
        cluster.manager.tell(Join(Address.parse(spec.node(first))))
        await spec.await_assert(lambda: _eq(statuses(), {"joining"}))
    await spec.enter_barrier("first-started")

    await self_up.expect_no_message(0.0)

    if spec.is_node(second):
        cluster.manager.tell(Join(Address.parse(spec.node(first))))
    if spec.is_node(first) or spec.is_node(second):
        expected = {Address.parse(spec.node(r)) for r in (first, second)}
        await spec.await_assert(lambda: _eq(addresses(), expected))
        assert statuses() == {"joining"}
        for _ in range(5):  # the negative check: a sleep is the assertion itself
            await asyncio.sleep(0.1)
            assert addresses() == expected
            assert statuses() == {"joining"}
    await spec.enter_barrier("second-joined")

    if spec.is_node(third):
        cluster.manager.tell(Join(Address.parse(spec.node(first))))
    await spec.await_assert(lambda: _eq(statuses(), {"up"}) and _eq(len(cluster.state.members), 3))
    await self_up.expect_message_type(SelfUp)
    await spec.enter_barrier("after-1")


def _eq(actual: object, expected: object) -> bool:
    assert actual == expected, f"{actual} != {expected}"
    return True


MIN_3 = make_config({"min_nr_of_members": 3})


@multi_node_spec(MIN_3[0])
async def test_min_members_before_up(spec: MultiNodeSpec) -> None:
    await wait_moving_members_to_up(spec, MIN_3)


WEAKLY_UP = make_config({"min_nr_of_members": 3, "allow_weakly_up_members": 3.0})


@multi_node_spec(WEAKLY_UP[0])
async def test_min_members_before_up_with_weakly_up(spec: MultiNodeSpec) -> None:
    await wait_moving_members_to_up(spec, WEAKLY_UP)


OF_ROLE = make_config(
    {"min_nr_of_members_of_role": {"backend": 2}}, ("frontend", "backend", "backend")
)


@multi_node_spec(OF_ROLE[0])
async def test_min_members_of_role_before_up(spec: MultiNodeSpec) -> None:
    await wait_moving_members_to_up(spec, OF_ROLE)
