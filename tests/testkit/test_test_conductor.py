"""TestConductor: throttle, blackhole, disconnect and stop nodes from the first role.

Ported from Pekko's ``TestConductorSpec``
(``remote-tests/src/multi-jvm/scala/org/apache/pekko/remote/testconductor/TestConductorSpec.scala``),
its two cases as one spec run, plus ohra's own cases for ``exit``, ``shutdown``, ``remove_node``,
``disconnect``, ``abort`` and the errors.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import pytest

from ohra import ActorRef, Behavior, Behaviors
from ohra.remote import Serialization
from ohra.testkit import (
    Direction,
    MultiNodeConfig,
    MultiNodeSpec,
    RoleName,
    TestProbe,
    multi_node_spec,
)


@dataclass(frozen=True)
class Echo:
    value: int | str
    reply_to: ActorRef[int | str]


Serialization.register(Echo, "TestConductorSpec.Echo")  # tests/test_remote_throttler.py has one


def echo(probe: ActorRef[int | str]) -> Behavior[Echo]:
    @Behaviors.receive_message
    def on_message(msg: Echo) -> Behavior[Echo]:
        probe.tell(msg.value)
        msg.reply_to.tell(msg.value)
        return Behaviors.same

    return on_message


def arrived(spec: MultiNodeSpec) -> tuple[RoleName, ...]:
    """The roles waiting at the current barrier: nothing outside the controller shows them."""
    return spec._run.controller._barrier.data.arrived  # pyright: ignore[reportPrivateUsage]


# --- TestConductorSpec ---------------------------------------------------------------------------

config = MultiNodeConfig()
leader, follower = config.role("leader"), config.role("follower")
config.test_transport(on=True)


@multi_node_spec(config)
async def test_a_test_conductor_enters_a_barrier_and_throttles(spec: MultiNodeSpec) -> None:
    probe: TestProbe[int | str] = spec.create_test_probe()

    # "enter a barrier"
    if spec.is_node(leader):
        spec.spawn(echo(probe.ref), "echo")
    await spec.enter_barrier("name")

    # "support throttling of network connections"
    target_path = spec.node(leader) + "/user/echo"
    target = spec.system.resolve(target_path)
    tc = spec.test_conductor

    # A fresh bucket passes one frame of T bytes free and then its 1000 tokens, so ten frames
    # pace 9T - 1000 bytes at 0.01 Mbit/s (1250 bytes/s). An Echo frame is about 300 bytes,
    # as the spec's 51-character name is in it twice: about 1.3s, inside 0.5s to 2s. A reply
    # frame is about 145 bytes, 0.24s, under the follower's 0.3s; Pekko's replies are as large
    # as its requests. So in the receive case the replies go to a probe named to make each
    # reply frame 191 bytes, 0.58s, whatever the spec's name.
    remoting = spec.system.remoting
    assert remoting is not None
    frame = remoting._frame  # pyright: ignore[reportPrivateUsage]
    unnamed = len(frame(spec.node(follower) + "/system/", 15))
    wide: TestProbe[int | str] = spec.create_test_probe("wide-" + "x" * (191 - unnamed - 5))
    if spec.is_node(follower):
        send = (9 * len(frame(target_path, Echo(5, probe.ref))) - 1000) / 1250
        recv = (9 * len(frame(wide.ref.path, 15)) - 1000) / 1250
        assert 0.8 <= send <= 1.6 and recv >= 0.45, (
            f"frame sizes moved (send {send:.2f}s, want 0.8s to 1.6s; recv {recv:.2f}s, want "
            "0.45s or more): the spec's name sizes the Echo frames, the wide probe's the replies"
        )

    if spec.is_node(follower):
        # start remote network connection so that it can be throttled
        target.tell(Echo("start", probe.ref))
    await probe.expect_message("start")

    if spec.is_node(leader):
        await tc.throttle(follower, leader, Direction.SEND, rate_mbit=0.01)
    await spec.enter_barrier("throttled_send")

    if spec.is_node(follower):
        for i in range(10):
            target.tell(Echo(i, probe.ref))
    async with probe.within(2.0, min=0.5):
        await probe.expect_message(0, 0.5)
        assert await probe.receive_messages(9) == list(range(1, 10))

    await spec.enter_barrier("throttled_send2")

    if spec.is_node(leader):
        await tc.throttle(follower, leader, Direction.SEND, -1)
        await tc.throttle(follower, leader, Direction.RECEIVE, rate_mbit=0.01)
    await spec.enter_barrier("throttled_recv")

    if spec.is_node(follower):
        for i in range(10, 20):
            target.tell(Echo(i, wide.ref))
    lo, hi, replies = (0.0, 0.5, probe) if spec.is_node(leader) else (0.3, 3.0, wide)
    async with replies.within(hi, min=lo):
        await replies.expect_message(10, 0.5)
        assert await replies.receive_messages(9) == list(range(11, 20))

    await spec.enter_barrier("throttled_recv2")

    if spec.is_node(leader):
        await tc.throttle(follower, leader, Direction.RECEIVE, -1)
    await spec.enter_barrier("after")


# --- exit and shutdown ---------------------------------------------------------------------------


async def test_exit_cancels_the_node_and_the_others_pass_the_barrier():
    three = MultiNodeConfig()
    first, second, third = three.role("first"), three.role("second"), three.role("third")
    seen: dict[RoleName | str, Any] = {}

    @multi_node_spec(three)
    async def body(spec: MultiNodeSpec) -> None:
        seen[spec.myself] = spec.system
        await spec.enter_barrier("started")
        if spec.is_node(third):
            # cancelled by exit; were the crash to wake it first, its barrier would raise
            await spec.enter_barrier("after-exit")
            raise AssertionError("unreachable")
        if spec.is_node(first):
            await spec.test_conductor.exit(third, 0)
            seen["nodes"] = await spec.test_conductor.get_nodes()
        await spec.enter_barrier("after-exit")

    await asyncio.wait_for(body(), 5.0)  # the barrier timeout is 30s
    assert seen[third].is_terminated
    assert seen["nodes"] == (first, second)


async def test_a_non_zero_exit_fails_the_spec_naming_the_role():
    two = MultiNodeConfig()
    first, second = two.role("first"), two.role("second")

    @multi_node_spec(two)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(first):
            await spec.test_conductor.exit(second, 3)
        else:
            await asyncio.Event().wait()

    with pytest.raises(AssertionError, match="role second exited with 3"):
        await asyncio.wait_for(body(), 5.0)


_TERMINATED = "TestConductor unavailable because system is terminated"


async def test_after_shutdown_the_body_runs_on_and_a_barrier_raises():
    two = MultiNodeConfig()
    first, second = two.role("first"), two.role("second")
    ran_on: list[RoleName] = []

    @multi_node_spec(two)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(first):
            await spec.test_conductor.shutdown(second)
        else:
            await spec.system.when_terminated()
            ran_on.append(spec.myself)
            with pytest.raises(RuntimeError, match=_TERMINATED):
                await spec.enter_barrier("after")

    await asyncio.wait_for(body(), 10.0)
    assert ran_on == [second]


@pytest.mark.parametrize("op", ["shutdown", "exit"])
async def test_a_stopped_node_leaves_get_nodes_at_once(op: str):
    # Pekko's Controller on Terminate: RemoveClient, then nodes -= node, in one step
    two = MultiNodeConfig()
    first, second = two.role("first"), two.role("second")
    seen: list[tuple[RoleName, ...]] = []

    @multi_node_spec(two)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(first):
            tc = spec.test_conductor
            await (tc.shutdown(second) if op == "shutdown" else tc.exit(second, 0))
            seen.append(await tc.get_nodes())
        else:
            await spec.system.when_terminated()

    await asyncio.wait_for(body(), 10.0)
    assert seen == [(first,)]


@pytest.mark.parametrize("abort", [False, True])
async def test_a_node_shut_down_while_waiting_at_a_barrier_raises_at_once(abort: bool):
    two = MultiNodeConfig()
    first, second = two.role("first"), two.role("second")
    took: list[float] = []

    @multi_node_spec(two)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(first):

            def waiting() -> None:
                assert arrived(spec) == (second,)

            await spec.await_assert(waiting)
            await spec.test_conductor.shutdown(second, abort=abort)
        else:
            start = time.monotonic()
            with pytest.raises(RuntimeError, match=_TERMINATED):
                await spec.enter_barrier("never")
            took.append(time.monotonic() - start)

    await asyncio.wait_for(body(), 10.0)
    assert took and took[0] < 2.0


# --- the errors ----------------------------------------------------------------------------------


async def test_a_conductor_op_off_the_first_role_raises():
    two = MultiNodeConfig()
    first, second = two.role("first"), two.role("second")
    two.test_transport(on=True)
    raised: list[str] = []

    @multi_node_spec(two)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(second):
            tc = spec.test_conductor
            for op in (
                tc.throttle(first, second, Direction.BOTH, 1.0),
                tc.blackhole(first, second, Direction.BOTH),
                tc.pass_through(first, second, Direction.BOTH),
                tc.disconnect(first, second),
                tc.abort(first, second),
                tc.exit(first, 0),
                tc.shutdown(first),
                tc.get_nodes(),
                tc.remove_node(first),
            ):
                with pytest.raises(RuntimeError) as info:
                    await op
                raised.append(str(info.value))

    await body()
    assert raised == ["TestConductorServer was not started"] * 9


async def test_network_ops_need_the_test_transport():
    two = MultiNodeConfig()
    first, second = two.role("first"), two.role("second")
    raised: list[str] = []

    @multi_node_spec(two)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(first):
            tc = spec.test_conductor
            for op in (
                tc.throttle(second, first, Direction.BOTH, 1.0),
                tc.blackhole(second, first, Direction.BOTH),
                tc.pass_through(second, first, Direction.BOTH),
                tc.disconnect(second, first),
                tc.abort(second, first),
            ):
                with pytest.raises(RuntimeError) as info:
                    await op
                raised.append(str(info.value))

    await body()
    assert (
        raised
        == [
            "To use this feature you must activate the failure injector adapters by specifying "
            "`test_transport(on=True)` in your MultiNodeConfig."
        ]
        * 5
    )


# --- the network ops -----------------------------------------------------------------------------


@multi_node_spec(config)
async def test_blackhole_then_pass_through_between_two_nodes(spec: MultiNodeSpec) -> None:
    probe: TestProbe[int | str] = spec.create_test_probe()
    if spec.is_node(leader):
        spec.spawn(echo(probe.ref), "echo")
    await spec.enter_barrier("deployed")
    target = spec.system.resolve(spec.node(leader) + "/user/echo")

    if spec.is_node(follower):
        target.tell(Echo("before", probe.ref))
    await probe.expect_message("before")
    await spec.enter_barrier("connected")

    if spec.is_node(leader):
        await spec.test_conductor.blackhole(follower, leader, Direction.BOTH)
    await spec.enter_barrier("blackholed")

    if spec.is_node(follower):
        target.tell(Echo("lost", probe.ref))
    await probe.expect_no_message(0.3)
    await spec.enter_barrier("checked")

    if spec.is_node(leader):
        await spec.test_conductor.pass_through(follower, leader, Direction.BOTH)
    await spec.enter_barrier("passed")

    if spec.is_node(follower):
        target.tell(Echo("after", probe.ref))
    await probe.expect_message("after")
    await spec.enter_barrier("done")


@multi_node_spec(config)
async def test_disconnect_and_abort_close_links_and_the_next_send_delivers(
    spec: MultiNodeSpec,
) -> None:
    probe: TestProbe[int | str] = spec.create_test_probe()
    if spec.is_node(leader):
        spec.spawn(echo(probe.ref), "echo")
    await spec.enter_barrier("deployed")
    target = spec.system.resolve(spec.node(leader) + "/user/echo")
    assert spec.system.remoting is not None
    leader_port = int(spec.node(leader).rpartition(":")[2])

    for i, op in enumerate(("disconnect", "abort")):
        if spec.is_node(follower):
            target.tell(Echo(f"{op}-before", probe.ref))
        await probe.expect_message(f"{op}-before")
        await spec.enter_barrier(f"{op}-connected")

        if spec.is_node(leader):
            await getattr(spec.test_conductor, op)(follower, leader)
        await spec.enter_barrier(f"{op}-closed")

        if spec.is_node(follower):
            associations = spec.system.remoting._associations  # pyright: ignore[reportPrivateUsage]
            assert ("127.0.0.1", leader_port) not in associations
            target.tell(Echo(i, probe.ref))
        await probe.expect_message(i)
        await spec.enter_barrier(f"{op}-done")


async def test_remove_node_lets_a_waiting_barrier_open():
    three = MultiNodeConfig()
    first, second, third = three.role("first"), three.role("second"), three.role("third")
    passed = asyncio.Event()

    @multi_node_spec(three)
    async def body(spec: MultiNodeSpec) -> None:
        if spec.is_node(third):
            await passed.wait()  # never enters "b"
            return
        if spec.is_node(second):
            await spec.enter_barrier("b")
            return
        waiting = asyncio.create_task(spec.enter_barrier("b"))

        def both_wait() -> None:
            assert set(arrived(spec)) == {first, second}

        await spec.await_assert(both_wait)
        await spec.test_conductor.remove_node(third)
        await waiting
        passed.set()

    await asyncio.wait_for(body(), 5.0)
