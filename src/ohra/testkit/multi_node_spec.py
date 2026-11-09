"""Pekko's multi-node testkit: `MultiNodeConfig`, `MultiNodeSpec` and the runner.

Ported from `MultiNodeSpec.scala`. Pekko runs one JVM per role; here every role is an
`ActorSystem` on one event loop, its body a task of one `TaskGroup`, and the first role's
`Controller` serves the barriers in process.
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import random
import re
import socket
import time
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
from typing import Any

from ..behavior import Behavior, Behaviors, MaybeAwaitable
from ..ref import ActorRef
from ..remote.throttler import Throttler
from ..system import ActorSystem
from .conductor import Controller, RoleName, TestConductor
from .probe import TestProbe
from .settings import TestKitSettings, seconds

# Below the ephemeral ranges (Linux from 32768, macOS from 49152), so the local end of an outbound
# connection cannot take a port between free_port() and the test's bind. Counting up from a random
# start never hands out the same port twice in a run.
# ponytail: about 1700 ports per run before 32768; wrap around if the suite outgrows that.
_ports = itertools.count(random.randrange(20000, 31000))


def free_port() -> int:
    """A port nothing listens on, from a range the OS does not use for outbound connections."""
    for port in _ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise AssertionError("unreachable")


async def crash(system: ActorSystem[Any]) -> None:
    """Close the system's remoting at once and stop it without the coordinated shutdown."""
    assert system.remoting is not None
    await system.remoting.close()
    system._final_terminate()  # pyright: ignore[reportPrivateUsage]  # a crash does not run the coordinated shutdown
    await system.when_terminated()


async def _shutdown(system: ActorSystem[Any], settings: TestKitSettings) -> None:
    """Pekko's shutdown(system) with verifySystemShutdown off: a system that does not stop
    within system_shutdown_default is left running with a warning, as ActorTestKit does."""
    system.terminate()
    d = settings.system_shutdown_default_dilated
    terminated = asyncio.ensure_future(system.when_terminated())
    done, _ = await asyncio.wait({terminated}, timeout=d)
    if not done:
        terminated.cancel()
        root = system.address.root if system.address else system.name
        system.log.warning(f"Failed to stop [{root}] within [{seconds(d)}]")


async def _final_shutdown(run: _Run, role: RoleName, system: ActorSystem[Any]) -> None:
    """The runner's last stop, for a node its after-all has not shut down already."""
    if role not in run.shut_down:
        await _shutdown(system, run.settings)


_RESERVED = ("address", "name")

# Pekko's testconductor barrier-timeout
_BARRIER_TIMEOUT = 30.0

_TERMINATED = "TestConductor unavailable because system is terminated"


def _check(system_kwargs: dict[str, Any]) -> dict[str, Any]:
    for key in _RESERVED:
        if key in system_kwargs:
            raise ValueError(f"{key} is set by the runner, not by the config")
    return system_kwargs


class MultiNodeConfig:
    """The roles of a spec and the `ActorSystem` keyword arguments of their nodes."""

    def __init__(self) -> None:
        self._roles: list[RoleName] = []
        self._common: dict[str, Any] = {}
        self._nodes: dict[RoleName, dict[str, Any]] = {}
        self._test_transport = False

    def common_config(self, **system_kwargs: Any) -> None:
        """Keyword arguments for every node's `ActorSystem`; replaces earlier ones."""
        self._common = _check(system_kwargs)

    def node_config(self, *roles: RoleName, **system_kwargs: Any) -> None:
        """Overrides for the given roles, merged over the common config."""
        _check(system_kwargs)
        for role in roles:
            self._nodes[role] = system_kwargs

    def role(self, name: str) -> RoleName:
        """Declare a role. The first one declared hosts the controller."""
        if any(r.name == name for r in self._roles):
            raise ValueError(f"non-unique role name {name}")
        r = RoleName(name)
        self._roles.append(r)
        return r

    def test_transport(self, on: bool) -> None:
        """Install a `Throttler` on every node, for blackhole, pass_through and throttle."""
        self._test_transport = on

    @property
    def roles(self) -> tuple[RoleName, ...]:
        return tuple(self._roles)


@dataclass
class _Run:
    """One run's shared state. `MultiNodeSpec` and `TestConductor` reach the other nodes
    only through it."""

    roles: tuple[RoleName, ...]
    settings: TestKitSettings
    controller: Controller
    systems: dict[RoleName, ActorSystem[Any]] = field(default_factory=lambda: {})
    ports: dict[RoleName, int] = field(default_factory=lambda: {})
    tasks: dict[RoleName, asyncio.Task[None]] = field(default_factory=lambda: {})
    exit_values: dict[RoleName, int] = field(default_factory=lambda: {})
    shut_down: set[RoleName] = field(default_factory=lambda: set())  # by their after-all


class MultiNodeSpec:
    """One per role, handed to the body of a `@multi_node_spec` test."""

    __test__ = False  # not a pytest test class

    def __init__(self, run: _Run, myself: RoleName) -> None:
        self._run = run
        self.myself = myself
        self.roles = run.roles
        self.initial_participants = len(run.roles)
        self.settings: TestKitSettings = run.settings
        self.system: ActorSystem[Any] = run.systems[myself]
        self._probe: TestProbe[Any] = TestProbe(self, "testActor")  # Pekko's testActor
        self.test_conductor = TestConductor(run, myself)

    def is_node(self, *nodes: RoleName) -> bool:
        """Whether this node is one of `nodes`."""
        return self.myself in nodes

    async def run_on(self, *nodes: RoleName, thunk: Callable[[], MaybeAwaitable[None]]) -> None:
        """Run `thunk` (sync or async) only on the given nodes."""
        if self.is_node(*nodes):
            r = thunk()
            if inspect.isawaitable(r):
                await r

    async def enter_barrier(self, *names: str, max: float | None = None) -> None:
        """Enter the named barriers in order, all within what is left of an enclosing `within`,
        else within `max` (dilated) or the barrier timeout. Once this node's system has
        terminated, or if it terminates while waiting, it raises `RuntimeError`."""
        timeout = self.settings.dilated(_BARRIER_TIMEOUT if max is None else max)
        stop = time.monotonic() + self._probe.remaining_or(timeout)
        controller = self._run.controller
        for name in names:  # Player.enter
            left = stop - time.monotonic()
            if left < 0:
                controller.fail_barrier(self.myself, name)
                raise TimeoutError(f"Server timed out while waiting for barrier {name}")
            await self._enter(controller, name, left)

    async def _enter(self, controller: Controller, name: str, timeout: float) -> None:
        # Pekko's Player.client: a terminated system has no conductor client
        if self.system.is_terminated:
            raise RuntimeError(_TERMINATED)
        entered = asyncio.ensure_future(controller.enter_barrier(self.myself, name, timeout))
        terminated = asyncio.ensure_future(self.system.when_terminated())
        try:
            await asyncio.wait({entered, terminated}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            entered.cancel()
            terminated.cancel()
            await asyncio.wait({entered, terminated})
        if entered.cancelled():
            raise RuntimeError(_TERMINATED)
        entered.result()

    def node(self, role: RoleName) -> str:
        """The role's root path: ``ohra://<name>@127.0.0.1:<port>``."""
        return f"ohra://{self.system.name}@127.0.0.1:{self._run.ports[role]}"

    def spawn[T](self, behavior: Behavior[T], name: str | None = None) -> ActorRef[T]:
        """Spawn an actor under ``/user`` on this node and return its ref at once."""
        return self.system._root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]

    def create_test_probe[M](self, name: str | None = None) -> TestProbe[M]:  # pyright: ignore[reportInvalidTypeVarUse]
        """A probe on this node: ``probe: TestProbe[int] = spec.create_test_probe()``."""
        return TestProbe[M](self, name)

    def within(self, max: float, min: float = 0.0) -> AbstractAsyncContextManager[None]:
        """`TestProbe.within`, on the spec's own probe."""
        return self._probe.within(max, min)

    async def await_assert[A](
        self, fn: Callable[[], MaybeAwaitable[A]], max: float | None = None, interval: float = 0.1
    ) -> A:
        """`TestProbe.await_assert`, on the spec's own probe."""
        return await self._probe.await_assert(fn, max, interval)

    async def _after_all(self) -> None:
        """Pekko's multiNodeSpecAfterAll."""
        controller = self._run.controller
        # wait for all nodes to remove themselves before we shut the conductor down
        if self.myself == self.roles[0]:
            controller.remove(self.myself)

            def removed() -> None:
                nodes = controller.get_nodes()
                assert nodes <= {self.myself}, f"Nodes not shutdown: {sorted(nodes, key=str)}"

            await self.await_assert(removed, _BARRIER_TIMEOUT)
        self._run.shut_down.add(self.myself)
        await _shutdown(self.system, self.settings)
        controller.client_disconnected(self.myself)  # the player's connection drops


async def _role(body: Callable[[MultiNodeSpec], Awaitable[None]], spec: MultiNodeSpec) -> None:
    try:
        await body(spec)
        await spec._after_all()  # pyright: ignore[reportPrivateUsage]
    except Exception as e:
        raise ExceptionGroup(f"role {spec.myself.name} failed", [e]) from None


async def _run_spec(
    config: MultiNodeConfig,
    settings: TestKitSettings,
    body: Callable[[MultiNodeSpec], Awaitable[None]],
) -> None:
    name = re.sub(r"[^A-Za-z0-9_-]", "", body.__name__) or "MultiNodeSpec"
    roles = config.roles
    controller = Controller(len(roles), settings.dilated(_BARRIER_TIMEOUT))
    run = _Run(roles, settings, controller)
    async with AsyncExitStack() as stack:
        try:
            for role in roles:
                port = free_port()
                kwargs = {**config._common, **config._nodes.get(role, {})}  # pyright: ignore[reportPrivateUsage]
                system = ActorSystem(Behaviors.empty, name, address=("127.0.0.1", port), **kwargs)
                if config._test_transport:  # pyright: ignore[reportPrivateUsage]
                    assert system.remoting is not None
                    system.remoting.throttler = Throttler()
                await system.__aenter__()
                stack.push_async_callback(_final_shutdown, run, role, system)
                # the player's connection drops when its system has terminated (ClientDisconnected)
                system._system_root.done.add_done_callback(  # pyright: ignore[reportPrivateUsage]
                    lambda _, role=role: controller.client_disconnected(role)
                )
                run.systems[role] = system
                run.ports[role] = port
            await asyncio.gather(*(controller.node_info(role) for role in roles))
            specs = [MultiNodeSpec(run, role) for role in roles]
            async with asyncio.TaskGroup() as tg:
                for spec in specs:
                    run.tasks[spec.myself] = tg.create_task(_role(body, spec))
            for role, value in run.exit_values.items():
                if value != 0:
                    raise AssertionError(f"role {role.name} exited with {value}")
        finally:
            controller.close()


def multi_node_spec(
    config: MultiNodeConfig, settings: TestKitSettings | None = None
) -> Callable[
    [Callable[[MultiNodeSpec], Awaitable[None]]], Callable[[], Coroutine[Any, Any, None]]
]:
    """Turn ``async def test_x(spec: MultiNodeSpec)`` into a pytest test with no parameters
    that runs the body once per role of `config`. `settings` default to `TestKitSettings()`."""

    def decorate(
        body: Callable[[MultiNodeSpec], Awaitable[None]],
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        async def test() -> None:
            await _run_spec(config, settings or TestKitSettings(), body)

        # not functools.wraps: its __wrapped__ would show pytest the spec parameter
        for attr in ("__name__", "__qualname__", "__doc__", "__module__"):
            setattr(test, attr, getattr(body, attr))
        return test

    return decorate
