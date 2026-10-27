"""Lifecycle signals, delivered to ``Receive.on_signal``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .ref import ActorRef


class Signal:
    __slots__ = ()


class _PreRestart(Signal):
    __slots__ = ()

    def __repr__(self) -> str:
        return "PreRestart"


class _PostStop(Signal):
    __slots__ = ()

    def __repr__(self) -> str:
        return "PostStop"


PreRestart: Signal = _PreRestart()
PostStop: Signal = _PostStop()


@dataclass(frozen=True, slots=True)
class Terminated(Signal):
    ref: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class ChildFailed(Terminated):
    cause: BaseException


class DeathPactError(Exception):
    """Raised when a watched actor terminates and the watcher leaves Terminated unhandled."""

    def __init__(self, ref: ActorRef[Any]) -> None:
        super().__init__(f"death pact with {ref.path}")
        self.ref = ref
