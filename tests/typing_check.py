"""Checked by pyright, never run. Each `type: ignore` must be needed,
because reportUnnecessaryTypeIgnoreComment is an error: if typing regresses, pyright fails."""

from dataclasses import dataclass

from ohra import (
    ActorContext,
    ActorRef,
    RemoteRef,
    ShardingEnvelope,
    ActorSystem,
    Behavior,
    Behaviors,
    MailboxSelector,
    Routers,
    SpawnProtocol,
    StashBuffer,
    TimerScheduler,
    batching,
)


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


class Animal: ...


class Dog(Animal): ...


def check(ping_ref: ActorRef[Ping], str_ref: ActorRef[str], animals: ActorRef[Animal]) -> None:
    ping_ref.tell(Ping(str_ref))
    ping_ref.tell("not a ping")  # type: ignore[arg-type]
    str_ref.tell(42)  # type: ignore[arg-type]
    _ = str_ref.offer(42)  # type: ignore[arg-type]

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


def timers_and_stash(
    ctx: ActorContext[Ping], timers: TimerScheduler[Ping], buffer: StashBuffer[Ping], ping: Ping
) -> None:
    timers.start_single_timer("k", ping, 1.0)
    timers.start_single_timer("k", "not a ping", 1.0)  # type: ignore[arg-type]
    ctx.timers.start_single_timer("k", ping, 1.0)
    ctx.timers.start_single_timer("k", "not a ping", 1.0)  # type: ignore[arg-type]
    buffer.stash(ping)
    buffer.stash(42)  # type: ignore[arg-type]


def handle_pings(batch: list[Ping]) -> None: ...


_batched: Behavior[Ping] = batching(10, 0.1, handle_pings)
_wrong: Behavior[str] = batching(10, 0.1, handle_pings)  # type: ignore[assignment]
_mailbox = MailboxSelector.bounded(10, overflow="drop_everything")  # type: ignore[arg-type]

_pool: Behavior[Ping] = Routers.pool(4, _b).with_consistent_hashing_routing(
    lambda p: p.reply_to.path
)
_pool_wrong: Behavior[str] = Routers.pool(4, _b)  # type: ignore[assignment]


async def spawn_from_outside(system: ActorSystem[SpawnProtocol.Command]) -> None:
    ref: ActorRef[Ping] = await system.ask(
        lambda r: SpawnProtocol.Spawn(_b, "pinger", r), timeout=1
    )
    wrong: ActorRef[str] = await system.ask(
        lambda r: SpawnProtocol.Spawn(_b, "pinger", r), timeout=1
    )  # type: ignore[assignment]
    del ref, wrong


async def ask_infers_the_reply_type(pinger: ActorRef[Ping]) -> None:
    from ohra import ask

    pong: str = await ask(pinger, lambda r: Ping(r), timeout=1)  # a lambda, no partial needed
    also: str = await ask(pinger, Ping, timeout=1)  # or the class itself
    wrong: int = await ask(pinger, Ping, timeout=1)  # type: ignore[assignment]
    del pong, also, wrong


def fastapi_dependency_is_typed() -> None:
    from ohra.ext.fastapi import Actors

    actors = Actors(_b, "web")
    system: ActorSystem[Ping] = actors.depends()
    wrong: ActorSystem[str] = actors.depends()  # type: ignore[assignment]
    del system, wrong


def sharding_is_typed(region: ActorRef[ShardingEnvelope[Ping]]) -> None:
    from ohra import EntityRef, Sharding

    ref: EntityRef[Ping] = Sharding.entity_ref_for(region, "a")
    ref.tell("not a ping")  # type: ignore[arg-type]
    wrong: EntityRef[str] = Sharding.entity_ref_for(region, "a")  # type: ignore[assignment]
    del ref, wrong


def persistence_is_typed() -> None:
    from ohra.persistence import InMemoryStore

    store = InMemoryStore[int]()

    @Behaviors.persisted(store)
    def counter(total: int) -> Behavior[Ping]:
        del total
        return _b

    node: Behavior[Ping] = counter(1)
    counter("one")  # type: ignore[arg-type]
    counter.recover("id", empty="zero")  # type: ignore[arg-type]
    wrong: Behavior[str] = counter.recover("id", empty=0)  # type: ignore[assignment]
    del node, wrong


def remote_refs_are_typed(remote: RemoteRef[Ping], ping: Ping) -> None:
    remote.tell(ping)
    remote.tell("not a ping")  # type: ignore[arg-type]


@dataclass(frozen=True)
class Greeting:
    text: str


@dataclass(frozen=True)
class Bye:
    pass


async def probes_are_typed() -> None:
    from ohra.testkit import ActorTestKit, TestProbe

    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        x: int = await probe.receive_message()
        probe.ref.tell("not an int")  # type: ignore[arg-type]
        probe2: TestProbe[Greeting | Bye] = testkit.create_test_probe()
        s: Greeting = await probe2.expect_message_type(Greeting)
        wrong: Bye = await probe2.expect_message_type(Greeting)  # type: ignore[assignment]
        del x, s, wrong
