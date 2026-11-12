"""Pekko's BackoffOnStopSupervisor (pattern/internal/BackoffOnStopSupervisor.scala) with
HandleBackoff (pattern/HandleBackoff.scala), as ``BackoffOpts.onStop`` builds it with its
defaults: no retry limit, ``AutoReset(minBackoff)`` and ``ForwardDeathLetters``. Sharding runs the
coordinator under it (ClusterSharding.scala 816-824).

It starts the child and watches it. When the child stops, it starts it again after an exponential
backoff, unless the final stop message has gone to the child, in which case it stops too. A
message that arrives while there is no child is a dead letter, or stops the supervisor when it is
the final stop message.

Left out, as no caller in sharding uses them: BackoffSupervisor's queries ``GetRestartCount``,
``GetCurrentChild`` and ``Reset``, and HandleBackoff's forwarding of the child's messages to the
parent, since typed messages carry no sender and the coordinator never tells its parent.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ...behavior import Behavior, Behaviors
from ...context import ActorContext
from ...ref import ActorRef
from ...signals import Signal, Terminated
from ...supervision import Backoff


@dataclass(frozen=True)
class _StartChild:
    pass


@dataclass(frozen=True)
class _ResetRestartCount:
    count: int


def backoff_on_stop_supervisor(
    child: Behavior[Any],
    child_name: str,
    min_backoff: float,
    max_backoff: float,
    random_factor: float,
    is_final_stop_message: Callable[[Any], bool],
) -> Behavior[Any]:
    backoff = Backoff(min_backoff, max_backoff, random_factor)

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        current: ActorRef[Any] | None = None
        restart_count = 0
        final_stop_message_received = False

        def start_child() -> None:
            """HandleBackoff.startChild."""
            nonlocal current
            if current is None:
                current = ctx.spawn(child, child_name)
                ctx.watch(current)

        def on_terminated(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            """BackoffOnStopSupervisor.onTerminated, with no retry limit."""
            nonlocal current, restart_count
            if isinstance(sig, Terminated) and sig.ref == current:
                current = None
                if final_stop_message_received:
                    return Behaviors.stopped()
                delay = backoff.delay(restart_count, random.random())
                ctx.timers.start_single_timer("start-child", _StartChild(), delay)
                restart_count += 1
            return Behaviors.same

        def handle_message_to_child(msg: Any) -> Behavior[Any]:
            """BackoffOnStopSupervisor.handleMessageToChild, with ForwardDeathLetters."""
            nonlocal final_stop_message_received
            if current is not None:
                current.tell(msg)
                if not final_stop_message_received:
                    final_stop_message_received = is_final_stop_message(msg)
            elif is_final_stop_message(msg):
                return Behaviors.stopped()
            else:
                ctx.system.dead_letter(msg, ctx.self)
            return Behaviors.same

        @Behaviors.receive_message
        def handle_backoff(msg: Any) -> Behavior[Any]:
            """HandleBackoff.handleBackoff."""
            nonlocal restart_count
            match msg:
                case _StartChild():
                    start_child()
                    if restart_count > 0:  # AutoReset(minBackoff)
                        ctx.timers.start_single_timer(
                            "reset-restart-count", _ResetRestartCount(restart_count), min_backoff
                        )
                case _ResetRestartCount(count=seen):
                    if seen == restart_count and restart_count > 0:
                        restart_count = 0
                case _:
                    return handle_message_to_child(msg)
            return Behaviors.same

        start_child()  # preStart
        return handle_backoff.receive_signal(on_terminated)

    return setup
