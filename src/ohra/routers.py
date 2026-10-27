"""Pool routers (org.apache.pekko.actor.typed.scaladsl.Routers)."""

from __future__ import annotations

import itertools
import random
from collections.abc import Callable, Hashable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from .behavior import SAME, UNHANDLED, Behavior, Behaviors, Stopped
from .signals import Signal, Terminated

if TYPE_CHECKING:
    from .context import ActorContext

type _Logic[T] = Literal["round_robin", "random"] | Callable[[T], Hashable]


@dataclass(frozen=True, eq=False, slots=True)
class PoolRouter[T](Behavior[T]):
    """Spawns ``size`` routees as children and forwards each message to one of them.
    A routee that stops is removed; the router stops when none are left."""

    size: int
    routee: Behavior[T]
    logic: _Logic[T] = "round_robin"

    def with_round_robin_routing(self) -> PoolRouter[T]:
        return replace(self, logic="round_robin")

    def with_random_routing(self) -> PoolRouter[T]:
        return replace(self, logic="random")

    def with_consistent_hashing_routing(self, mapping: Callable[[T], Hashable]) -> PoolRouter[T]:
        """Messages with equal ``mapping(msg)`` go to the same routee (``hash(key) % size``;
        the pool has a fixed size, so there are no virtual nodes)."""
        return replace(self, logic=mapping)

    def start(self, ctx: ActorContext[T]) -> Behavior[T]:
        """Called by the runtime when the router starts."""
        routees = [ctx.spawn_anonymous(self.routee) for _ in range(self.size)]
        for r in routees:
            ctx.watch(r)
        turn = itertools.count()
        logic = self.logic

        def pick(msg: T) -> int:
            if logic == "round_robin":
                return next(turn) % len(routees)
            if logic == "random":
                return random.randrange(len(routees))
            return hash(logic(msg)) % len(routees)

        def on_message(msg: T) -> Behavior[T]:
            routees[pick(msg)].tell(msg)
            return SAME

        def on_signal(_ctx: ActorContext[T], sig: Signal) -> Behavior[T]:
            if isinstance(sig, Terminated):
                routees.remove(sig.ref)
                return SAME if routees else Stopped()
            return UNHANDLED

        return Behaviors.receive_message(on_message).receive_signal(on_signal)


class Routers:
    @staticmethod
    def pool[T](size: int, routee: Behavior[T]) -> PoolRouter[T]:
        if size < 1:
            raise ValueError("a pool needs at least one routee")
        return PoolRouter(size, routee)
