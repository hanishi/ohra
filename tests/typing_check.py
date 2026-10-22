"""Checked by pyright, never run. Each `type: ignore` must be needed,
because reportUnnecessaryTypeIgnoreComment is an error: if typing regresses, pyright fails."""
from dataclasses import dataclass
from functools import partial

from pekko_py import ActorRef, ActorSystem, Behavior, Behaviors, MailboxSelector, Routers, SpawnProtocol, StashBuffer, TimerScheduler, batching


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


class Animal: ...


class Dog(Animal): ...


def check(ping_ref: ActorRef[Ping], str_ref: ActorRef[str], animals: ActorRef[Animal]) -> None:
    ping_ref.tell(Ping(str_ref))
    ping_ref.tell("not a ping")  # type: ignore[arg-type]
    str_ref.tell(42)  # type: ignore[arg-type]

    takes_dogs(animals)  # contravariant: a handler of Animal accepts Dogs


def takes_dogs(ref: ActorRef[Dog]) -> None:
    takes_animals(ref)  # type: ignore[arg-type]


def takes_animals(ref: ActorRef[Animal]) -> None:
    ref.tell(Dog())


def handler(msg: Ping) -> Behavior[Ping]:
    msg.reply_to.tell("pong")
    msg.reply_to.tell(1)  # type: ignore[arg-type]
    return Behaviors.same


_b: Behavior[Ping] = Behaviors.receive_message(handler)


def timers_and_stash(timers: TimerScheduler[Ping], buffer: StashBuffer[Ping], ping: Ping) -> None:
    timers.start_single_timer("k", ping, 1.0)
    timers.start_single_timer("k", "not a ping", 1.0)  # type: ignore[arg-type]
    buffer.stash(ping)
    buffer.stash(42)  # type: ignore[arg-type]


def handle_pings(batch: list[Ping]) -> None: ...


_batched: Behavior[Ping] = batching(10, 0.1, handle_pings)
_wrong: Behavior[str] = batching(10, 0.1, handle_pings)  # type: ignore[assignment]
_mailbox = MailboxSelector.bounded(10, overflow="drop_everything")  # type: ignore[arg-type]

_pool: Behavior[Ping] = Routers.pool(4, _b).with_consistent_hashing_routing(lambda p: p.reply_to.path)
_pool_wrong: Behavior[str] = Routers.pool(4, _b)  # type: ignore[assignment]


async def spawn_from_outside(system: ActorSystem[SpawnProtocol.Command]) -> None:
    ref: ActorRef[Ping] = await system.ask(partial(SpawnProtocol.Spawn[Ping], _b, "pinger"), timeout=1)
    wrong: ActorRef[str] = await system.ask(partial(SpawnProtocol.Spawn[Ping], _b, "pinger"), timeout=1)  # type: ignore[assignment]
    del ref, wrong
