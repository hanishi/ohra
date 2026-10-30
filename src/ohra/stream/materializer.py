"""Where streams run: Apache Pekko's ``Materializer``. Each system has one, which spawns Pekko's
``/system/Materializers/StreamSupervisor`` on first use and one child of it per run. The child
drives the sink with ``pipe_to_self`` and stops when the stream ends; when the system terminates,
stopping the child cancels the drive, and the sink fails its pending materialized value with
``AbruptStageTerminationError``."""

from __future__ import annotations

import weakref
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

from ..behavior import Behavior, Behaviors
from .dsl import _Ctx  # pyright: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from ..cell import ActorCell
    from ..context import ActorContext
    from ..system import ActorSystem
    from .dsl import RunnableGraph

_materializers: weakref.WeakKeyDictionary[ActorSystem[Any], Materializer] = (
    weakref.WeakKeyDictionary()
)


class Materializer:
    """``Materializer(system)`` is the system's one materializer, as Pekko's
    ``SystemMaterializer``. It holds its system weakly, so a terminated system can be collected."""

    _system: weakref.ref[ActorSystem[Any]]

    def __new__(cls, system: ActorSystem[Any]) -> Materializer:
        m = _materializers.get(system)
        if m is None:
            m = _materializers[system] = super().__new__(cls)
            m._system = weakref.ref(system)
        return m

    def materialize[M](self, graph: RunnableGraph[M]) -> M:
        """Build the graph and start it in a new child of the stream supervisor; return its
        materialized value. ``RuntimeError`` once the system is terminating."""
        system = self._system()
        if system is None or system._system_root.stopping:  # pyright: ignore[reportPrivateUsage]
            raise RuntimeError("stream materializer has been shut down")
        root = system._system_root  # pyright: ignore[reportPrivateUsage]
        drive, mat = graph._build(_Ctx(system))  # pyright: ignore[reportPrivateUsage]
        # While /system runs, a stop reaches the child only through its parents, a step each, so
        # the drive has started (and can fail its futures) before it can be cancelled.
        _child(_child(root, "Materializers"), "StreamSupervisor").spawn_child(
            _run(drive), None, None
        )
        return mat


class _Stop:
    """The stream ended: its actor stops."""


def _child(parent: ActorCell[Any], name: str) -> ActorCell[Any]:
    child = parent.children.get(name)
    return child if child is not None else parent.spawn_child(Behaviors.empty, name, None)


def _run(drive: Coroutine[Any, Any, None]) -> Behavior[_Stop]:
    def setup(ctx: ActorContext[_Stop]) -> Behavior[_Stop]:
        ctx.pipe_to_self(drive, lambda _: _Stop())
        return Behaviors.receive_message(lambda _: Behaviors.stopped())

    return Behaviors.setup(setup)
