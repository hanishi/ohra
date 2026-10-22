"""Pool routers."""
from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass

from pekko_py import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, Routers

from .util import eventually


@dataclass(frozen=True)
class Work:
    key: int


@dataclass(frozen=True)
class Quit:
    pass


type Cmd = Work | Quit


def routee(seen: list[tuple[str, int]]) -> Behavior[Cmd]:
    def setup(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        def on_message(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Work(key):
                    seen.append((ctx.self.path, key))
                    return Behaviors.same
                case Quit():
                    return Behaviors.stopped()

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


async def test_round_robin_spreads_messages_evenly():
    seen: list[tuple[str, int]] = []
    async with ActorSystem(Routers.pool(4, routee(seen)), "t") as system:
        for i in range(12):
            system.tell(Work(i))
        await eventually(lambda: len(seen) == 12)
    assert Counter(path for path, _ in seen) == {f"pekko://t/user/${i}": 3 for i in range(4)}


async def test_consistent_hashing_sends_a_key_to_one_routee():
    seen: list[tuple[str, int]] = []
    router = Routers.pool(4, routee(seen)).with_consistent_hashing_routing(lambda m: m.key if isinstance(m, Work) else 0)
    async with ActorSystem(router, "t") as system:
        for i in range(40):
            system.tell(Work(i % 8))
        await eventually(lambda: len(seen) == 40)
    by_key: dict[int, set[str]] = {}
    for path, key in seen:
        by_key.setdefault(key, set()).add(path)
    assert all(len(paths) == 1 for paths in by_key.values())
    assert len({p for paths in by_key.values() for p in paths}) > 1


async def test_random_routing_uses_more_than_one_routee():
    seen: list[tuple[str, int]] = []
    async with ActorSystem(Routers.pool(4, routee(seen)).with_random_routing(), "t") as system:
        for i in range(40):
            system.tell(Work(i))
        await eventually(lambda: len(seen) == 40)
    assert len({path for path, _ in seen}) > 1


async def test_router_stops_when_all_routees_have_stopped():
    seen: list[tuple[str, int]] = []
    system = ActorSystem(Routers.pool(2, routee(seen)), "t")
    system.tell(Work(1))  # round robin: $0
    system.tell(Quit())  # $1
    system.tell(Quit())  # $0
    await asyncio.wait_for(system.when_terminated(), 1)
    assert [key for _, key in seen] == [1]


async def test_routees_are_children_of_the_router():
    seen: list[tuple[str, int]] = []
    names: list[ActorRef[Cmd]] = []

    def guardian(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        pool = ctx.spawn(Routers.pool(3, routee(seen)), "pool")
        names.append(pool)

        def forward(msg: Cmd) -> Behavior[Cmd]:
            pool.tell(msg)
            return Behaviors.same

        return Behaviors.receive_message(forward)

    async with ActorSystem(Behaviors.setup(guardian), "t") as system:
        system.tell(Work(7))
        await eventually(lambda: len(seen) == 1)
    assert seen[0][0].startswith("pekko://t/user/pool/$")
