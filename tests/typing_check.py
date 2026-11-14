"""Checked by pyright, never run. Each `type: ignore` must be needed,
because reportUnnecessaryTypeIgnoreComment is an error: if typing regresses, pyright fails."""

import asyncio
from dataclasses import dataclass
from typing import Any

from ohra.cluster.sharding import ShardStopped
from ohra import (
    ActorContext,
    ActorRef,
    ClusterSharding,
    ClusterShardingStats,
    Entity,
    EntityTypeKey,
    GetClusterShardingStats,
    GetShardRegionStats,
    SetActiveEntityLimit,
    ShardRegionStats,
    RemoteRef,
    ShardingEnvelope,
    StartEntity,
    StopShards,
    ActorSystem,
    Behavior,
    Behaviors,
    GCounter,
    GCounterKey,
    GSet,
    MailboxSelector,
    ORMap,
    ORSet,
    ORSetKey,
    Replicator,
    Routers,
    SelfUniqueAddress,
    SpawnProtocol,
    StashBuffer,
    TimerScheduler,
    UniqueAddress,
    StatusReply,
    ask_with_status,
    batching,
)
from ohra.stream import (
    Flow,
    Keep,
    KillSwitches,
    NotUsed,
    RunnableGraph,
    Sink,
    Source,
    UniqueKillSwitch,
)
from ohra.testkit import Direction, MultiNodeConfig, MultiNodeSpec


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


def single_node_sharding_takes_no_start_entity(ctx: ActorContext[Any]) -> None:
    from ohra import Sharding

    region = ctx.spawn(Sharding.init(Entity(EntityTypeKey[Ping]("Ping"), lambda _e: _b)), "r")
    region.tell(StartEntity("a"))  # type: ignore[arg-type]


def cluster_sharding_region_takes_the_region_messages(
    system: ActorSystem[Any],
    stats: ActorRef[ShardRegionStats],
    cluster: ActorRef[ClusterShardingStats],
    stopped: ActorRef[ShardStopped],
) -> None:
    key = EntityTypeKey[Ping]("Ping")
    region = ClusterSharding(system).init(Entity(key, lambda _e: _b))
    envelopes: ActorRef[ShardingEnvelope[Ping]] = region
    region.tell(GetShardRegionStats(stats))
    region.tell(GetClusterShardingStats(3.0, cluster))
    region.tell(SetActiveEntityLimit(10))
    region.tell(StartEntity("a"))
    region.tell(ShardingEnvelope("a", "not a ping"))  # type: ignore[arg-type]
    region.tell(StopShards(frozenset({"1"}), stopped))
    region.tell(StopShards(frozenset({"1"}), stats))  # type: ignore[arg-type]
    del envelopes


def extracted_region_takes_its_messages(system: ActorSystem[Any], ping: Ping) -> None:
    from ohra import ExtractedEntity, ShardingMessageExtractor

    def ping_id(msg: Ping) -> str:
        return "a"

    def str_id(msg: str) -> str:
        return msg

    key = EntityTypeKey[Ping]("Ping")
    extractor = ShardingMessageExtractor.no_envelope(10, None, ping_id)
    entity = Entity(key, lambda _e: _b).with_message_extractor(extractor).with_role("r")
    kept: ExtractedEntity[Ping, Ping] = entity
    region = ClusterSharding(system).init(entity)
    region.tell(ping)
    region.tell(StartEntity("a"))
    region.tell("not a ping")  # type: ignore[arg-type]
    str_extractor = ShardingMessageExtractor.no_envelope(10, None, str_id)
    Entity(key, lambda _e: _b).with_message_extractor(str_extractor)  # type: ignore[arg-type]
    del kept


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


async def multi_node_specs_are_typed(spec: MultiNodeSpec) -> None:
    config = MultiNodeConfig()
    first, second = config.role("first"), config.role("second")
    spec.is_node(first)
    spec.is_node("first")  # type: ignore[arg-type]
    await spec.test_conductor.blackhole(first, second, Direction.BOTH)
    await spec.test_conductor.blackhole(first, second, "both")  # type: ignore[arg-type]


def streams_are_typed(system: ActorSystem[Any]) -> None:
    # Flow.of(str) alone is accepted here: type[T] is covariant, so pyright solves T = int | str,
    # and an identity flow of int | str does take ints. A flow that uses str is rejected.
    Source.from_iterable([1]).via(Flow.of(str).map(str.upper))  # type: ignore[arg-type]
    g: RunnableGraph[tuple[NotUsed, asyncio.Future[list[int]]]] = Source.from_iterable([1]).to_mat(
        Sink.seq(), Keep.both
    )
    empty: Source[int, NotUsed] = Source.empty()  # Source is covariant in its elements
    ints: Sink[int, NotUsed] = Sink.cancelled()  # and Sink contravariant
    flow: Flow[bool, object, object] = Flow.of(int)
    wrong: asyncio.Future[str] = Source.single(1).run_with(Sink.head(), system)  # type: ignore[assignment]
    zipped: Source[tuple[int, str], NotUsed] = Source.single(1).zip(Source.single("a"))
    # merge and concat widen to the union, as Pekko's merge[U >: Out]
    nums = Source.from_iterable([1, 2])
    bools = Source.single(True)
    strs = Source.single("a")
    from_empty: Source[int, NotUsed] = Source.empty().merge(nums)
    merged: Source[int, NotUsed] = nums.merge(bools)
    concat_empty: Source[int, NotUsed] = Source.empty().concat(nums)
    concatenated: Source[int, NotUsed] = nums.concat(bools)
    either: Source[int | str, NotUsed] = nums.merge(strs)
    flow_merged: Flow[int, int, NotUsed] = Flow.of(int).merge(bools)
    not_ints: Source[int, NotUsed] = nums.merge(strs)  # type: ignore[assignment]
    switched: Source[int, UniqueKillSwitch] = Source.single(1).via_mat(
        KillSwitches.single(), Keep.right
    )
    del g, empty, ints, flow, wrong, zipped, switched, from_empty, merged, concat_empty
    del concatenated, either, flow_merged, not_ints


GCounter().merge(GSet[int]())  # type: ignore[arg-type]
GSet[str]().add(1)  # type: ignore[arg-type]
node = SelfUniqueAddress(UniqueAddress("localhost", 7354, 1, "Sys"))
ORSet[str]().add(node, 1)  # type: ignore[arg-type]
GCounter().merge(ORSet[str]())  # type: ignore[arg-type]
ORMap[str, GCounter]().put(node, "k", 1)  # type: ignore[arg-type]


def check_replicator(rep: ActorRef[Replicator.Command], me: ActorRef[Any]) -> None:
    k = GCounterKey("a")
    rep.tell(Replicator.Get(k, Replicator.ReadLocal(), me))
    rep.tell(Replicator.Update(k, GCounter(), Replicator.WriteLocal(), me, lambda c: c))
    rep.tell(Replicator.Subscribe(k, me))
    rep.tell("not a command")  # type: ignore[arg-type]
    s = ORSetKey[str]("s")
    Replicator.Update(s, ORSet[str](), Replicator.WriteLocal(), me, lambda _: GCounter())  # type: ignore[arg-type]


def check_get_success(success: Replicator.GetSuccess[GCounter]) -> None:
    data: ORSet[str] = success.data  # type: ignore[assignment]
    del data


@dataclass(frozen=True)
class StatusRequest:
    reply_to: ActorRef[StatusReply[str]]


async def ask_with_status_is_typed(ref: ActorRef[StatusRequest]) -> None:
    ok: str = await ask_with_status(ref, StatusRequest, 1)
    wrong: int = await ask_with_status(ref, StatusRequest, 1)  # type: ignore[assignment]
    error_reply: StatusReply[str] = StatusReply.error("no")
    ref_reply: ActorRef[StatusReply[str]] = ref_of_status()
    ref_reply.tell(StatusReply.success(1))  # type: ignore[arg-type]
    print(ok, wrong, error_reply)


def ref_of_status() -> ActorRef[StatusReply[str]]: ...
