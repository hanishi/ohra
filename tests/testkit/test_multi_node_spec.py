"""MultiNodeSpec: one body per role on one event loop, with barriers between them.

Ported from Pekko's ``MultiNodeSpecSpec`` (the after-all waits for every node to remove itself)
and ``MultiNodeSample`` (send to and receive from a remote node), plus ohra's own cases for the
runner: a failing body, a body that returns while others wait, and the config.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any

import pytest

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.coordinated_shutdown import CoordinatedShutdownSettings
from ohra.remote import Serialization
from ohra.testkit import (
    ClientLost,
    MultiNodeConfig,
    MultiNodeSpec,
    RoleName,
    TestProbe,
    multi_node_spec,
)

# --- MultiNodeSpecSpec ---------------------------------------------------------------------------

four = MultiNodeConfig()
n1, n2, n3, n4 = (four.role(f"node{i}") for i in range(1, 5))


@multi_node_spec(four)
async def test_waits_for_all_nodes_to_remove_themselves_before_the_conductor_shuts_down(
    spec: MultiNodeSpec,
) -> None:
    await spec.enter_barrier("startup")
    # this test is empty here since it only exercises the shutdown code in the MultiNodeSpec


# --- MultiNodeSample -----------------------------------------------------------------------------

sample = MultiNodeConfig()
node1, node2 = sample.role("node1"), sample.role("node2")


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


Serialization.register(Ping, "MultiNodeSample.Ping")  # tests/test_remote.py has a Ping too


@Behaviors.receive
def ponger(_ctx: ActorContext[Ping], msg: Ping) -> Behavior[Ping]:
    msg.reply_to.tell("pong")
    return Behaviors.same


@multi_node_spec(sample)
async def test_sends_to_and_receives_from_a_remote_node(spec: MultiNodeSpec) -> None:
    if spec.is_node(node1):
        await spec.enter_barrier("deployed")
        probe: TestProbe[str] = spec.create_test_probe()
        spec.system.resolve(spec.node(node2) + "/user/ponger").tell(Ping(probe.ref))
        await probe.expect_message("pong", 10.0)

    if spec.is_node(node2):
        spec.spawn(ponger, "ponger")
        await spec.enter_barrier("deployed")

    await spec.enter_barrier("finished")


def test_a_decorated_spec_takes_no_parameters():
    assert inspect.signature(test_sends_to_and_receives_from_a_remote_node).parameters == {}
    assert (
        test_sends_to_and_receives_from_a_remote_node.__name__
        == "test_sends_to_and_receives_from_a_remote_node"
    )


# --- the runner ----------------------------------------------------------------------------------


def _role_failure(group: BaseExceptionGroup[Any], role: str) -> BaseException:
    assert group.message == f"role {role} failed"
    assert len(group.exceptions) == 1
    return group.exceptions[0]


async def test_a_failing_body_fails_the_spec_at_once_naming_its_role():
    config = MultiNodeConfig()
    first, second = config.role("first"), config.role("second")

    @multi_node_spec(config)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(second):
            raise ValueError("boom")
        assert spec.myself == first
        await spec.enter_barrier("b1")

    with pytest.raises(ExceptionGroup) as info:
        await asyncio.wait_for(body(), 2.0)  # the barrier timeout is 30s
    e = _role_failure(info.value, "second")
    assert isinstance(e, ValueError) and e.args == ("boom",)


async def test_a_body_that_returns_while_others_wait_fails_them_with_client_lost():
    config = MultiNodeConfig()
    a, b = config.role("a"), config.role("b")

    @multi_node_spec(config)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(a):
            await spec.enter_barrier("x")

    with pytest.raises(ExceptionGroup) as info:
        await asyncio.wait_for(body(), 2.0)
    e = _role_failure(info.value, "a")
    assert isinstance(e, ClientLost) and e.client == b


def test_a_duplicate_role_is_refused():
    config = MultiNodeConfig()
    config.role("a")
    with pytest.raises(ValueError):
        config.role("a")
    assert config.roles == (RoleName("a"),)


def test_address_and_name_are_not_config():
    config = MultiNodeConfig()
    r = config.role("a")
    for key in ("address", "name"):
        with pytest.raises(ValueError):
            config.common_config(**{key: "x"})
        with pytest.raises(ValueError):
            config.node_config(r, **{key: "x"})


async def test_node_gives_each_roles_root_path_and_is_node_picks_myself():
    config = MultiNodeConfig()
    a, b = config.role("a"), config.role("b")
    seen: dict[RoleName, tuple[str, str]] = {}

    @multi_node_spec(config)
    async def paths(spec: MultiNodeSpec) -> None:
        assert spec.is_node(spec.myself) and spec.is_node(a, b)
        assert not spec.is_node(*(r for r in spec.roles if r != spec.myself))
        assert spec.system.address is not None
        assert spec.node(spec.myself) == spec.system.address.root
        seen[spec.myself] = (spec.node(a), spec.node(b))

    await paths()
    assert seen[a] == seen[b]
    for path in seen[a]:
        assert path.startswith("ohra://paths@127.0.0.1:")
    assert seen[a][0] != seen[a][1]


async def test_run_on_runs_only_on_the_given_nodes():
    config = MultiNodeConfig()
    a, _b, c = config.role("a"), config.role("b"), config.role("c")
    ran: list[RoleName] = []

    @multi_node_spec(config)
    async def body(spec: MultiNodeSpec) -> None:
        async def thunk() -> None:
            ran.append(spec.myself)

        await spec.run_on(a, c, thunk=thunk)
        await spec.run_on(a, thunk=lambda: ran.append(spec.myself))

    await body()
    assert sorted(ran, key=lambda r: r.name) == [a, a, c]


async def test_node_config_overrides_common_config_for_its_roles():
    common = CoordinatedShutdownSettings(default_phase_timeout=2.0)
    override = CoordinatedShutdownSettings(default_phase_timeout=3.0)
    config = MultiNodeConfig()
    r1, r2 = config.role("r1"), config.role("r2")
    config.common_config(shutdown=common)
    config.node_config(r2, shutdown=override)
    seen: dict[RoleName, CoordinatedShutdownSettings] = {}

    @multi_node_spec(config)
    async def body(spec: MultiNodeSpec) -> None:
        seen[spec.myself] = spec.system.coordinated_shutdown.settings

    await body()
    assert seen == {r1: common, r2: override}
