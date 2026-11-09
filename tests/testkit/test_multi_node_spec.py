"""MultiNodeSpec: one body per role on one event loop, with barriers between them.

Ported from Pekko's ``MultiNodeSpecSpec`` (the after-all waits for every node to remove itself)
and ``MultiNodeSample`` (send to and receive from a remote node), plus ohra's own cases for the
runner: a failing body, a body that returns while others wait, a node that does not stop, a
barrier inside ``within``, and the config.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass

import pytest

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.coordinated_shutdown import CoordinatedShutdownSettings
from ohra.remote import Serialization
from ohra.system import ActorSystem
from ohra.testkit import (
    BarrierTimeout,
    ClientLost,
    MultiNodeConfig,
    MultiNodeSpec,
    RoleName,
    TestKitSettings,
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
    assert info.group_contains(ValueError, match="boom")
    (member,) = info.value.exceptions
    assert isinstance(member, ExceptionGroup)
    assert member.message == "role second failed"


async def test_a_body_that_returns_while_others_wait_fails_them_with_client_lost():
    config = MultiNodeConfig()
    a, b = config.role("a"), config.role("b")

    @multi_node_spec(config)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(a):
            await spec.enter_barrier("x")

    with pytest.raises(ExceptionGroup) as info:
        await asyncio.wait_for(body(), 2.0)
    assert info.group_contains(ClientLost)
    (member,) = info.value.exceptions
    assert isinstance(member, ExceptionGroup)
    assert member.message == "role a failed"
    (lost,) = member.exceptions
    assert isinstance(lost, ClientLost) and lost.client == b


async def test_a_node_that_does_not_stop_is_left_after_the_shutdown_timeout(
    caplog: pytest.LogCaptureFixture,
):
    config = MultiNodeConfig()
    _a, b = config.role("a"), config.role("b")
    started, release = asyncio.Event(), asyncio.Event()
    stuck: list[ActorSystem[object]] = []

    async def hold(_ctx: ActorContext[str], _msg: str) -> Behavior[str]:
        started.set()
        await release.wait()  # holds the actor, its stop included
        return Behaviors.same

    @multi_node_spec(config, settings=TestKitSettings(system_shutdown_default=0.2))
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(b):
            stuck.append(spec.system)
            spec.spawn(Behaviors.receive(hold)).tell("hold")
            await started.wait()

    try:
        start = time.monotonic()
        with caplog.at_level(logging.WARNING):
            await asyncio.wait_for(body(), 2.0)  # the default bound is 10s
        took = time.monotonic() - start
        root = stuck[0].path.removesuffix("/user")
        warnings = [
            r for r in caplog.records if r.getMessage().startswith(f"Failed to stop [{root}]")
        ]
        assert len(warnings) == 1  # shut down once, by its after-all, not again at the end
        assert took < 0.4  # one 0.2s bound, not two
    finally:
        release.set()
        for system in stuck:
            await system.when_terminated()


async def test_a_node_whose_shutdown_is_cut_short_is_shut_down_at_the_end():
    config = MultiNodeConfig()
    a, b = config.role("a"), config.role("b")
    terminating = asyncio.Event()
    systems: list[ActorSystem[object]] = []

    @multi_node_spec(config)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(b):
            systems.append(spec.system)

            async def slow() -> None:
                terminating.set()
                await asyncio.sleep(0.5)  # the shutdown is still running when a fails

            spec.system.coordinated_shutdown.add_task("before-service-unbind", "slow", slow)
            return  # its after-all starts the shutdown
        assert spec.is_node(a)
        await terminating.wait()
        raise ValueError("boom")  # cancels b inside its after-all's shutdown

    with pytest.raises(ExceptionGroup) as info:
        await asyncio.wait_for(body(), 5.0)
    assert info.group_contains(ValueError, match="boom")
    assert systems[0].is_terminated


async def test_an_enclosing_within_shortens_a_barrier():
    config = MultiNodeConfig()
    a, _b = config.role("a"), config.role("b")
    done = asyncio.Event()

    @multi_node_spec(config)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(a):
            with pytest.raises(BarrierTimeout):
                async with spec.within(0.3):
                    await spec.enter_barrier("never")
            done.set()
        else:
            await done.wait()  # never enters "never", and stays until a has timed out

    await asyncio.wait_for(body(), 5.0)  # the barrier timeout is 30s


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
