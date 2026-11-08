"""Controller with BarrierCoordinator, after Apache Pekko's BarrierSpec
(remote-tests/src/test/scala/org/apache/pekko/remote/testconductor/BarrierSpec.scala).

The spec's "A Controller with BarrierCoordinator" group is ported, 16 cases in order,
because ohra ships the coordinator inside the controller. Its replies become the
result of `enter_barrier`: `BarrierResult(x, true)` is a return, `BarrierResult(x, false)`
is a raised `BarrierError`. The assertions on the error data come from the
"A BarrierCoordinator" group, applied to the matching controller case.

Not ported:
- the "A BarrierCoordinator" group itself, which drives the coordinator as a bare actor
  whose failures are sent to a watching probe. Under the controller, `BarrierEmpty`
  resumes, so its "fail barrier after first failure" does not hold;
- "finally have no failure messages left" (both groups), which has no counterpart.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest

from ohra.testkit.conductor import (
    BarrierError,
    BarrierTimeout,
    ClientLost,
    Controller,
    Data,
    DuplicateNode,
    FailedBarrier,
    RoleName,
    WrongBarrier,
)

A = RoleName("a")
B = RoleName("b")
C = RoleName("c")


@asynccontextmanager
async def with_controller(participants: int) -> AsyncGenerator[Controller]:
    controller = Controller(participants, 5.0)
    try:
        yield controller
    finally:
        controller.close()
        await asyncio.sleep(0)


async def enter(
    controller: Controller, role: RoleName, barrier: str, timeout: float | None = None
) -> asyncio.Task[None]:
    task = asyncio.ensure_future(controller.enter_barrier(role, barrier, timeout))
    await asyncio.sleep(0)  # the task has sent its message
    return task


async def register(controller: Controller, *roles: RoleName) -> None:
    await asyncio.gather(*(controller.node_info(r) for r in roles))


async def no_msg(*tasks: asyncio.Task[None]) -> None:
    await asyncio.sleep(0.1)
    assert not any(t.done() for t in tasks)


async def settle(*tasks: asyncio.Task[None]) -> None:
    """Cancel tasks that are meant to stay pending, so nothing outlives the test."""
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def error_of(task: asyncio.Task[None]) -> BarrierError:
    with pytest.raises(BarrierError) as info:
        await asyncio.wait_for(task, 2)
    return info.value


async def test_register_clients_and_remove_them(caplog: pytest.LogCaptureFixture) -> None:
    async with with_controller(1) as b:
        await b.node_info(A)
        b.remove(B)
        b.remove(A)
        with caplog.at_level(logging.WARNING):
            b.remove(A)
        assert [
            r.getMessage() for r in caplog.records if r.getMessage().startswith("cannot remove")
        ]


async def test_register_clients_and_disconnect_them() -> None:
    async with with_controller(1) as b:
        await b.node_info(A)
        b.client_disconnected(B)
        b.client_disconnected(A)
        assert b.get_nodes() == frozenset()


async def test_fail_entering_barrier_when_nobody_registered() -> None:
    async with with_controller(0) as b:
        with pytest.raises(FailedBarrier):
            await b.enter_barrier(A, "b")


async def test_enter_barrier() -> None:
    async with with_controller(2) as barrier:
        await register(barrier, A, B)
        a = await enter(barrier, A, "bar11")
        await no_msg(a)
        b = await enter(barrier, B, "bar11")
        await asyncio.wait_for(asyncio.gather(a, b), 2)


async def test_enter_barrier_with_joining_node() -> None:
    async with with_controller(2) as barrier:
        await register(barrier, A, B)
        a = await enter(barrier, A, "bar12")
        await barrier.node_info(C)
        b = await enter(barrier, B, "bar12")
        await no_msg(a, b)
        c = await enter(barrier, C, "bar12")
        await asyncio.wait_for(asyncio.gather(a, b, c), 2)


async def test_enter_barrier_with_leaving_node() -> None:
    async with with_controller(3) as barrier:
        await register(barrier, A, B, C)
        a = await enter(barrier, A, "bar13")
        b = await enter(barrier, B, "bar13")
        barrier.remove(A)
        barrier.client_disconnected(A)
        await no_msg(a, b)
        barrier.remove(C)
        await asyncio.wait_for(b, 2)
        barrier.client_disconnected(C)
        await asyncio.sleep(0.1)
        await settle(a)


async def test_leave_barrier_when_last_arrived_is_removed() -> None:
    async with with_controller(2) as barrier:
        await register(barrier, A, B)
        a = await enter(barrier, A, "bar14")
        barrier.remove(A)
        await asyncio.wait_for(barrier.enter_barrier(B, "foo"), 2)
        await settle(a)


async def test_fail_barrier_with_disconnecting_node() -> None:
    async with with_controller(2) as barrier:
        await register(barrier, A, B)
        a = await enter(barrier, A, "bar15")
        await asyncio.sleep(0)
        barrier.client_disconnected(RoleName("unknown"))
        await no_msg(a)
        barrier.client_disconnected(B)
        err = await error_of(a)
        assert isinstance(err, ClientLost)
        assert err == ClientLost(Data(frozenset({A}), "bar15", (A,), err.data.deadline), B)


async def test_fail_barrier_with_disconnecting_node_who_already_arrived() -> None:
    async with with_controller(3) as barrier:
        await register(barrier, A, B, C)
        a = await enter(barrier, A, "bar16")
        b = await enter(barrier, B, "bar16")
        await asyncio.sleep(0)
        barrier.client_disconnected(B)
        err = await error_of(a)
        assert isinstance(err, ClientLost)
        assert err == ClientLost(Data(frozenset({A, C}), "bar16", (A,), err.data.deadline), B)
        await settle(b)


async def test_fail_when_entering_wrong_barrier() -> None:
    async with with_controller(2) as barrier:
        await register(barrier, A, B)
        a = await enter(barrier, A, "bar17")
        await asyncio.sleep(0)
        b = await enter(barrier, B, "foo")
        err_a = await error_of(a)
        err_b = await error_of(b)
        assert isinstance(err_a, WrongBarrier)
        assert err_a == WrongBarrier(
            "foo", B, Data(frozenset({A, B}), "bar17", (A,), err_a.data.deadline)
        )
        assert err_b == err_a


async def test_fail_after_barrier_timeout() -> None:
    async with with_controller(2) as barrier:
        await register(barrier, A, B)
        a = await enter(barrier, A, "bar18", 0.2)
        await asyncio.sleep(0.4)
        err = await error_of(a)
        assert isinstance(err, BarrierTimeout)
        assert err == BarrierTimeout(Data(frozenset({A, B}), "bar18", (A,), err.data.deadline))
        with pytest.raises(FailedBarrier):
            await barrier.enter_barrier(B, "bar18")


async def test_fail_if_a_node_registers_twice() -> None:
    async with with_controller(2) as controller:
        a = asyncio.ensure_future(controller.node_info(A))
        await asyncio.sleep(0)
        b = asyncio.ensure_future(controller.node_info(A))
        err_a = await error_of(a)
        err_b = await error_of(b)
        assert isinstance(err_a, DuplicateNode)
        assert err_a == DuplicateNode(Data(frozenset({A}), "", (), err_a.data.deadline), A)
        assert err_b == err_a


async def test_fail_subsequent_barriers_if_a_node_registers_twice() -> None:
    async with with_controller(1) as controller:
        await controller.node_info(A)
        with pytest.raises(DuplicateNode):
            await controller.node_info(A)
        with pytest.raises(FailedBarrier):
            await controller.enter_barrier(A, "bar19")


async def test_fail_subsequent_barriers_after_forced_failure() -> None:
    async with with_controller(2) as barrier:
        await register(barrier, A, B)
        a = await enter(barrier, A, "bar20", 2.0)
        await asyncio.sleep(0)
        barrier.fail_barrier(B, "bar20")
        assert isinstance(await error_of(a), FailedBarrier)
        a = await enter(barrier, A, "bar21")
        b = await enter(barrier, B, "bar21")
        assert isinstance(await error_of(a), FailedBarrier)
        assert isinstance(await error_of(b), FailedBarrier)


async def test_timeout_within_the_shortest_timeout_if_the_new_timeout_is_shorter() -> None:
    async with with_controller(3) as barrier:
        await register(barrier, A, B, C)
        a = await enter(barrier, A, "bar22", 10.0)
        b = await enter(barrier, B, "bar22", 0.2)
        await asyncio.sleep(0.4)
        assert isinstance(await error_of(a), BarrierTimeout)
        assert isinstance(await error_of(b), BarrierTimeout)
        with pytest.raises(FailedBarrier):
            await barrier.enter_barrier(C, "bar22")


async def test_timeout_within_the_shortest_timeout_if_the_new_timeout_is_longer() -> None:
    async with with_controller(3) as barrier:
        await register(barrier, A, B, C)
        a = await enter(barrier, A, "bar23", 0.2)
        b = await enter(barrier, B, "bar23", 10.0)
        await asyncio.sleep(0.4)
        assert isinstance(await error_of(a), BarrierTimeout)
        assert isinstance(await error_of(b), BarrierTimeout)
        with pytest.raises(FailedBarrier):
            await barrier.enter_barrier(C, "bar23")
