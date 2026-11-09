"""Pekko's test conductor: `TestConductor`, `Controller` and its `BarrierCoordinator`.

Ported from `Conductor.scala` (`Conductor`, `Controller.receive`, `BarrierCoordinator`) and
`Player.scala` (`ClientFSM`'s handling of the network and terminate ops). Pekko's actors
share one thread here, so they are plain classes: a handler that throws in Pekko raises,
and `_receive` plays the controller's supervisor strategy.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from ..remote.remoting import Remoting
from ..remote.throttler import Blackhole, Direction, ThrottleMode, TokenBucket, Unthrottled

if TYPE_CHECKING:
    from .multi_node_spec import _Run  # pyright: ignore[reportPrivateUsage]

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RoleName:
    name: str


@dataclass(frozen=True)
class Data:
    """BarrierCoordinator.Data. `arrived` is newest first, as Pekko prepends."""

    clients: frozenset[RoleName]
    barrier: str
    arrived: tuple[RoleName, ...]
    deadline: float | None


_EMPTY = Data(frozenset(), "", (), None)


class BarrierError(Exception):
    """Equal by arguments, as Pekko's case classes are."""

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self.args == other.args  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self), self.args))


class BarrierTimeout(BarrierError):  # noqa: N818
    def __init__(self, data: Data) -> None:
        super().__init__(data)
        self.data = data


class FailedBarrier(BarrierError):  # noqa: N818
    def __init__(self, data: Data) -> None:
        super().__init__(data)
        self.data = data


class DuplicateNode(BarrierError):  # noqa: N818
    def __init__(self, data: Data, node: RoleName) -> None:
        super().__init__(data, node)
        self.data = data
        self.node = node


class WrongBarrier(BarrierError):  # noqa: N818
    def __init__(self, barrier: str, client: RoleName, data: Data) -> None:
        super().__init__(barrier, client, data)
        self.barrier = barrier
        self.client = client
        self.data = data


class ClientLost(BarrierError):  # noqa: N818
    def __init__(self, data: Data, client: RoleName) -> None:
        super().__init__(data, client)
        self.data = data
        self.client = client


class BarrierEmpty(BarrierError):  # noqa: N818
    def __init__(self, data: Data, msg: str) -> None:
        super().__init__(data, msg)
        self.data = data
        self.msg = msg


type _Fut = asyncio.Future[None]


class BarrierCoordinator:
    """Idle or waiting on one barrier. Failures are reported to the waiters, then it
    restarts with `failed = True`, so every later barrier fails."""

    def __init__(self, barrier_timeout: float) -> None:
        self._barrier_timeout = barrier_timeout
        self._failed = False
        self._waiting = False
        self._data = _EMPTY
        self._waiters: list[tuple[RoleName, _Fut]] = []  # newest first, like data.arrived
        self._pending: set[_Fut] = set()  # every wait not yet settled, removed clients' too
        self._timer: asyncio.TimerHandle | None = None

    # -- supervision (Controller.supervisorStrategy) --

    def _receive(self, handler: Callable[..., None], *args: Any) -> BarrierError | None:
        try:
            handler(*args)
        except BarrierEmpty as e:  # Resume
            log.warning(e.msg)
            return None
        except BarrierError as e:  # failBarrier(data) and Restart
            self._cancel_timer()
            waiters, self._waiters = self._waiters, []
            for _, fut in waiters:
                if not fut.done():
                    fut.set_exception(e)
            self._failed = True
            self._waiting = False
            self._data = _EMPTY
            return e
        return None

    @property
    def data(self) -> Data:
        return self._data

    # -- events --

    def node_info(self, name: RoleName) -> BarrierError | None:
        return self._receive(self._on_node_info, name)

    def _on_node_info(self, name: RoleName) -> None:
        d = self._data
        if name in d.clients:
            raise DuplicateNode(d, name)
        self._data = replace(d, clients=d.clients | {name})

    def client_disconnected(self, name: RoleName) -> None:
        self._receive(self._on_client_disconnected, name)

    def _on_client_disconnected(self, name: RoleName) -> None:
        d = self._data
        if not d.arrived:
            self._data = replace(d, clients=d.clients - {name})
        elif name in d.clients:
            raise ClientLost(
                replace(
                    d,
                    clients=d.clients - {name},
                    arrived=tuple(r for r in d.arrived if r != name),
                ),
                name,
            )

    def remove_client(self, name: RoleName) -> None:
        self._receive(self._on_remove_client, name)

    def _on_remove_client(self, name: RoleName) -> None:
        d = self._data
        if not self._waiting:
            if not d.clients:
                raise BarrierEmpty(d, f"cannot remove {name}: no client to remove")
            self._data = replace(d, clients=d.clients - {name})
        elif name in d.clients:
            self._waiters = [(r, f) for r, f in self._waiters if r != name]
            self._handle_barrier(
                replace(
                    d,
                    clients=d.clients - {name},
                    arrived=tuple(r for r in d.arrived if r != name),
                )
            )

    def fail_barrier(self, name: RoleName, barrier: str) -> None:
        self._receive(self._on_fail_barrier, name, barrier)

    def _on_fail_barrier(self, name: RoleName, barrier: str) -> None:
        if not self._waiting:
            return
        d = self._data
        if barrier != d.barrier:
            raise WrongBarrier(barrier, name, d)
        raise FailedBarrier(d)

    async def enter_barrier(self, name: RoleName, barrier: str, timeout: float | None) -> None:
        fut: _Fut = asyncio.get_running_loop().create_future()
        self._pending.add(fut)
        fut.add_done_callback(self._pending.discard)
        err = self._receive(self._on_enter_barrier, name, barrier, timeout, fut)
        if isinstance(err, WrongBarrier) and not fut.done():
            fut.set_exception(err)  # the offender is told too, as Pekko's ToClient reply
        await fut

    def _on_enter_barrier(
        self, name: RoleName, barrier: str, timeout: float | None, fut: _Fut
    ) -> None:
        d = self._data
        if not self._waiting:  # Idle
            if self._failed:
                fut.set_exception(FailedBarrier(d))
            elif d.clients == frozenset({name}):
                fut.set_result(None)
            elif name not in d.clients:
                fut.set_exception(FailedBarrier(d))
            else:
                self._waiters = [(name, fut)]
                self._waiting = True
                self._data = replace(
                    d, barrier=barrier, arrived=(name,), deadline=self._get_deadline(timeout)
                )
                self._start_timer(self._data.deadline)
            return
        if barrier != d.barrier:
            raise WrongBarrier(barrier, name, d)
        together = d.arrived
        if name in d.clients:
            together = (name, *d.arrived)
            self._waiters.insert(0, (name, fut))
        else:  # Pekko never answers it, and its player times out
            fut.set_exception(FailedBarrier(d))
        enter_deadline = self._get_deadline(timeout)
        # we only allow the deadlines to get shorter
        if d.deadline is None or enter_deadline < d.deadline:
            self._start_timer(enter_deadline)
            self._handle_barrier(replace(d, arrived=together, deadline=enter_deadline))
        else:
            self._handle_barrier(replace(d, arrived=together))

    def _handle_barrier(self, data: Data) -> None:
        if not data.arrived:
            self._data = replace(data, barrier="")
            self._go_idle()
        elif not data.clients - set(data.arrived):
            waiters, self._waiters = self._waiters, []
            for _, fut in waiters:
                if not fut.done():
                    fut.set_result(None)
            self._data = replace(data, barrier="", arrived=())
            self._go_idle()
        else:
            self._data = data

    # -- timer (onTransition and StateTimeout) --

    def _go_idle(self) -> None:
        self._waiting = False
        self._cancel_timer()

    def _get_deadline(self, timeout: float | None) -> float:
        loop = asyncio.get_running_loop()
        return loop.time() + (self._barrier_timeout if timeout is None else timeout)

    def _start_timer(self, deadline: float | None) -> None:
        self._cancel_timer()
        loop = asyncio.get_running_loop()
        left = 0.0 if deadline is None else max(0.0, deadline - loop.time())
        self._timer = loop.call_later(left, self._on_timeout)

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _on_timeout(self) -> None:
        self._timer = None
        if self._waiting:
            self._receive(self._raise, BarrierTimeout(self._data))

    @staticmethod
    def _raise(e: BarrierError) -> None:
        raise e

    def close(self) -> None:
        self._cancel_timer()
        for fut in list(self._pending):
            if not fut.done():
                fut.set_exception(FailedBarrier(self._data))


class Controller:
    """Registers nodes and delegates barriers to its `BarrierCoordinator`."""

    def __init__(self, initial_participants: int, barrier_timeout: float) -> None:
        self._initial_participants = initial_participants
        self._barrier = BarrierCoordinator(barrier_timeout)
        self._nodes: set[RoleName] = set()
        self._startup: list[_Fut] = []  # node_info calls waiting for "initial startup"

    async def node_info(self, name: RoleName) -> None:
        failure = self._barrier.node_info(name)
        if failure is not None or name in self._nodes:
            err = failure or DuplicateNode(self._barrier.data, name)
            if self._initial_participants > 0:
                for fut in self._startup:
                    if not fut.done():
                        fut.set_exception(err)
                self._startup = []
                self._initial_participants = 0
            raise err
        self._nodes.add(name)
        if self._initial_participants <= 0:
            return
        if len(self._nodes) == self._initial_participants:
            for fut in self._startup:
                if not fut.done():
                    fut.set_result(None)
            self._startup = []
            self._initial_participants = 0
            return
        fut: _Fut = asyncio.get_running_loop().create_future()
        self._startup.append(fut)
        await fut

    def client_disconnected(self, name: RoleName) -> None:
        self._nodes.discard(name)
        self._barrier.client_disconnected(name)

    def remove(self, name: RoleName) -> None:
        self._barrier.remove_client(name)

    def terminate(self, name: RoleName) -> None:
        """Terminate's bookkeeping: out of the barriers and out of the nodes, at once."""
        self._barrier.remove_client(name)
        self._nodes.discard(name)

    def get_nodes(self) -> frozenset[RoleName]:
        return frozenset(self._nodes)

    async def enter_barrier(
        self, name: RoleName, barrier: str, timeout: float | None = None
    ) -> None:
        await self._barrier.enter_barrier(name, barrier, timeout)

    def fail_barrier(self, name: RoleName, barrier: str) -> None:
        self._barrier.fail_barrier(name, barrier)

    def close(self) -> None:
        """Cancels the barrier timer and fails the waits still pending."""
        self._barrier.close()
        for fut in self._startup:
            if not fut.done():
                fut.set_exception(FailedBarrier(_EMPTY))
        self._startup = []


_NO_TEST_TRANSPORT = (
    "To use this feature you must activate the failure injector adapters by specifying "
    "`test_transport(on=True)` in your MultiNodeConfig."
)


class TestConductor:
    """Pekko's `Conductor` ops, one per node as `spec.test_conductor`; only the first role's
    work. Each op completes at once in one process, so none waits on Pekko's query-timeout."""

    __test__ = False  # not a pytest test class

    def __init__(self, run: _Run, myself: RoleName) -> None:
        self._run = run
        self._myself = myself

    def _controller(self) -> Controller:
        if self._myself != self._run.roles[0]:
            raise RuntimeError("TestConductorServer was not started")
        return self._run.controller

    def _remoting(self, node: RoleName) -> Remoting:
        remoting = self._run.systems[node].remoting
        assert remoting is not None
        return remoting

    def _require_test_conductor_transport(self) -> None:
        if self._remoting(self._myself).throttler is None:
            raise RuntimeError(_NO_TEST_TRANSPORT)

    async def throttle(
        self, node: RoleName, target: RoleName, direction: Direction, rate_mbit: float
    ) -> None:
        """Throttle what `node` sends to or receives from `target` to `rate_mbit`: a negative
        rate is `Unthrottled`, 0 is `Blackhole`."""
        self._require_test_conductor_transport()
        self._controller()
        # the ClientFSM's ThrottleMsg
        mode: ThrottleMode
        if rate_mbit < 0.0:
            mode = Unthrottled()
        elif rate_mbit == 0.0:
            mode = Blackhole()
        else:
            # 125000 octets/s = 1 Mbit/s
            mode = TokenBucket(1000, rate_mbit * 125000.0, 0, 0)
        throttler = self._remoting(node).throttler
        assert throttler is not None
        throttler.set_throttle("127.0.0.1", self._run.ports[target], direction, mode)

    async def blackhole(self, node: RoleName, target: RoleName, direction: Direction) -> None:
        """Drop everything `node` sends to or receives from `target`."""
        await self.throttle(node, target, direction, 0.0)

    async def pass_through(self, node: RoleName, target: RoleName, direction: Direction) -> None:
        """Undo `throttle` and `blackhole` between `node` and `target`."""
        await self.throttle(node, target, direction, -1.0)

    async def disconnect(self, node: RoleName, target: RoleName) -> None:
        """Close the links between `node` and `target` on `node`'s side, gracefully."""
        await self._disconnect(node, target, False)

    async def abort(self, node: RoleName, target: RoleName) -> None:
        """Close the links between `node` and `target` on `node`'s side, by abort."""
        await self._disconnect(node, target, True)

    async def _disconnect(self, node: RoleName, target: RoleName, abort: bool) -> None:
        # Pekko's Player ignores DisconnectMsg; here it closes links, so it needs the transport
        self._require_test_conductor_transport()
        self._controller()
        await self._remoting(node).disconnect("127.0.0.1", self._run.ports[target], abort)

    async def exit(self, node: RoleName, exit_value: int) -> None:
        """Stop `node` as a process exit would: out of the barriers, its system crashed and its
        body cancelled. A non-zero `exit_value` fails the spec once the other bodies finish."""
        from .multi_node_spec import crash

        self._controller().terminate(node)
        self._run.exit_values[node] = exit_value
        # an exited process runs nothing more; cancelled first, so the crash cannot wake it
        self._run.tasks[node].cancel()
        await crash(self._run.systems[node])

    async def shutdown(self, node: RoleName, abort: bool = False) -> None:
        """Take `node` out of the barriers and terminate its system, or crash it with `abort`.
        Its body runs on, and its next barrier raises."""
        from .multi_node_spec import crash

        self._controller().terminate(node)
        system = self._run.systems[node]
        if abort:
            await crash(system)
        else:
            system.terminate()

    async def get_nodes(self) -> tuple[RoleName, ...]:
        """The roles still connected, in declaration order."""
        nodes = self._controller().get_nodes()
        return tuple(r for r in self._run.roles if r in nodes)

    async def remove_node(self, node: RoleName) -> None:
        """Take `node` out of the barriers, so the others pass them without it."""
        self._controller().remove(node)
