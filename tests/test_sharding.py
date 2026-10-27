"""Sharding: entity-per-id actors created on demand, passivated, and buffered across their
lifecycle."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass

import pytest

from pekko_py import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    CoordinatedShutdownSettings,
    CurrentShardRegionState,
    Entity,
    EntityContext,
    EntityTypeKey,
    GetShardRegionState,
    Passivate,
    ShardCommand,
    Sharding,
    ShardingEnvelope,
    Signal,
    ask,
)
from pekko_py.signals import PostStop

from .util import eventually


@dataclass(frozen=True)
class Add:
    n: int


@dataclass(frozen=True)
class Get:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Boom:
    pass


@dataclass(frozen=True)
class Stop:
    """Ask the shard to passivate this entity."""


@dataclass(frozen=True)
class Halt:
    """The stop message a region configured with with_stop_message delivers; the entity stops
    itself."""


@dataclass(frozen=True)
class Slow:
    seconds: float


@dataclass(frozen=True)
class Note:
    text: str


type CounterCmd = Add | Get | Boom | Stop | Halt | Slow | Note

Counter = EntityTypeKey[CounterCmd]("Counter")


def counter(
    log: list[str],
    gate: asyncio.Event | None = None,
    refs: list[tuple[ActorRef[ShardCommand], ActorRef[CounterCmd]]] | None = None,
    fail_ids: frozenset[str] = frozenset(),
) -> Callable[[EntityContext[CounterCmd]], Behavior[CounterCmd]]:
    """An entity that logs its lifecycle. Stop asks the shard to passivate it; PostStop waits for
    gate if given. refs collects (shard, self) on each Add; ids in fail_ids make the factory
    raise."""

    def create(entity: EntityContext[CounterCmd]) -> Behavior[CounterCmd]:
        if entity.entity_id in fail_ids:
            raise RuntimeError(f"cannot create {entity.entity_id}")
        log.append(f"start {entity.entity_id}")
        log.append(f"key {entity.entity_type_key.name}")

        def active(total: int) -> Behavior[CounterCmd]:
            async def on_message(
                ctx: ActorContext[CounterCmd], msg: CounterCmd
            ) -> Behavior[CounterCmd]:
                match msg:
                    case Add(n):
                        if refs is not None:
                            refs.append((entity.shard, ctx.self))
                        return active(total + n)
                    case Slow(seconds):
                        await asyncio.sleep(seconds)
                    case Note(text):
                        log.append(f"note {text} {entity.entity_id}")
                    case Get(reply_to):
                        reply_to.tell(total)
                    case Boom():
                        raise ValueError("boom")
                    case Stop():
                        log.append(f"got stop {entity.entity_id}")
                        entity.shard.tell(Passivate(ctx.self))
                    case Halt():
                        log.append(f"got halt {entity.entity_id}")
                        return Behaviors.stopped()
                return Behaviors.same

            async def on_signal(
                _ctx: ActorContext[CounterCmd], sig: Signal
            ) -> Behavior[CounterCmd]:
                if sig is PostStop:
                    if gate is not None:
                        await gate.wait()
                    log.append(f"stop {entity.entity_id}")
                return Behaviors.same

            return Behaviors.receive(on_message).receive_signal(on_signal)

        return active(0)

    return create


def region(
    log: list[str],
    gate: asyncio.Event | None = None,
    stop_message: CounterCmd | None = None,
    passivate_after: float | None = None,
    refs: list[tuple[ActorRef[ShardCommand], ActorRef[CounterCmd]]] | None = None,
    fail_ids: frozenset[str] = frozenset(),
) -> Behavior[ShardingEnvelope[CounterCmd] | GetShardRegionState]:
    entity = Entity(Counter, counter(log, gate, refs, fail_ids))
    if stop_message is not None:
        entity = entity.with_stop_message(stop_message)
    if passivate_after is not None:
        entity = entity.with_passivate_after(passivate_after)
    return Sharding.init(entity)


async def live(system: ActorRef[GetShardRegionState]) -> frozenset[str]:
    state: CurrentShardRegionState = await ask(system, GetShardRegionState, timeout=1)
    return state.entity_ids


# --- Task 1: create and route ------------------------------------------------------------


async def test_first_message_creates_the_entity_and_later_ones_reach_it():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Add(1))
        a.tell(Add(2))
        assert await ask(a, Get, timeout=1) == 3
    assert log.count("start a") == 1


async def test_entities_are_independent():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        Sharding.entity_ref_for(system, "a").tell(Add(1))
        Sharding.entity_ref_for(system, "b").tell(Add(2))
        assert await ask(Sharding.entity_ref_for(system, "a"), Get, timeout=1) == 1
        assert await ask(Sharding.entity_ref_for(system, "b"), Get, timeout=1) == 2


async def test_entity_ref_path_and_repr():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        assert Sharding.entity_ref_for(system, "a").path == f"{system.path}/a"
        assert Sharding.entity_ref_for(system, "a b/c").path == f"{system.path}/a%20b%2Fc"
        assert repr(Sharding.entity_ref_for(system, "a")).endswith("/a)")


async def test_entity_ids_may_contain_any_characters():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        odd = Sharding.entity_ref_for(system, "x/y$z")
        odd.tell(Add(7))
        assert await ask(odd, Get, timeout=1) == 7
    assert "start x/y$z" in log


async def test_get_shard_region_state_lists_live_entities():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        assert await live(system) == frozenset()
        Sharding.entity_ref_for(system, "a").tell(Add(1))
        Sharding.entity_ref_for(system, "b").tell(Add(1))
        assert await live(system) == frozenset({"a", "b"})


async def test_entity_context_carries_key_and_id():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        Sharding.entity_ref_for(system, "a").tell(Add(1))
        await eventually(lambda: "key Counter" in log)
    assert log[:2] == ["start a", "key Counter"]


# --- Task 2: passivation, stop message, buffering, failure ----------------------------------


async def test_entity_can_request_passivation_and_is_recreated_fresh():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Add(5))
        a.tell(Stop())
        await eventually(lambda: "stop a" in log)
        assert await ask(a, Get, timeout=1) == 0
    assert log.count("start a") == 2


async def test_stop_message_is_delivered_on_passivation():
    log: list[str] = []
    async with ActorSystem(region(log, stop_message=Halt()), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Add(1))
        # entity asks to passivate; region answers with Halt and the entity stops itself
        a.tell(Stop())
        await eventually(lambda: "stop a" in log)
        assert log.index("got halt a") < log.index("stop a")


async def test_messages_during_passivation_are_buffered_and_replayed_in_order():
    log: list[str] = []
    gate = asyncio.Event()
    async with ActorSystem(region(log, gate), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Stop())
        await eventually(lambda: "got stop a" in log)
        a.tell(Add(1))
        a.tell(Add(2))
        Sharding.entity_ref_for(system, "b").tell(Stop())  # left passivating at exit
        await asyncio.sleep(0.01)
        assert "stop a" not in log  # still stopping behind the gate
        gate.set()
        assert await ask(a, Get, timeout=1) == 3
    assert log.count("start a") == 2
    assert log.index("stop a") < log.index("start a", 1)


async def test_duplicate_passivate_is_ignored():
    log: list[str] = []
    gate = asyncio.Event()
    async with ActorSystem(region(log, gate), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Stop())
        a.tell(Stop())
        await eventually(lambda: "got stop a" in log)
        a.tell(Add(1))
        gate.set()
        assert await ask(a, Get, timeout=1) == 1
        await asyncio.sleep(0.02)
        # the second Passivate did not stop the new incarnation
        assert await ask(a, Get, timeout=1) == 1
    assert log.count("start a") == 2


async def test_failed_entity_is_recreated_on_next_message():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Add(5))
        a.tell(Boom())
        await eventually(lambda: "stop a" in log)
        a.tell(Add(1))
        assert await ask(a, Get, timeout=1) == 1
    assert log.count("start a") == 2


# --- Task 3: idle passivation ----------------------------------------------------------------


async def test_idle_entity_passivates_after_the_configured_time():
    log: list[str] = []
    async with ActorSystem(region(log, passivate_after=0.02), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Add(1))
        await eventually(lambda: "stop a" in log)
        assert await live(system) == frozenset()
        assert await ask(a, Get, timeout=1) == 0


async def test_idle_timer_restarts_on_each_message():
    log: list[str] = []
    async with ActorSystem(region(log, passivate_after=0.05), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        for _ in range(8):
            a.tell(Add(1))
            await asyncio.sleep(0.005)
        assert "stop a" not in log
        await eventually(lambda: "stop a" in log)


# --- review fixes --------------------------------------------------------------------------------


async def test_passivation_without_stop_message_lets_pending_messages_finish_first():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Stop())  # entity asks to passivate...
        a.tell(Slow(0.02))  # ...while it is busy, the region's stop arrives...
        a.tell(Note("after"))  # ...and this is already in its mailbox: it must still be processed
        await eventually(lambda: "stop a" in log)
        assert "note after a" in log
        assert log.index("note after a") < log.index("stop a")


async def test_stale_passivate_from_an_earlier_incarnation_is_ignored():
    log: list[str] = []
    refs: list[tuple[ActorRef[ShardCommand], ActorRef[CounterCmd]]] = []
    async with ActorSystem(region(log, refs=refs), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Add(1))
        await eventually(lambda: len(refs) == 1)
        shard, old_self = refs[0]
        a.tell(Stop())
        await eventually(lambda: "stop a" in log)
        a.tell(Add(2))
        assert await ask(a, Get, timeout=1) == 2  # second incarnation
        shard.tell(Passivate(old_self))  # from the first incarnation: must not stop the second
        await asyncio.sleep(0.02)
        assert await ask(a, Get, timeout=1) == 2
    assert log.count("start a") == 2


async def test_factory_failure_affects_only_that_entity():
    log: list[str] = []
    async with ActorSystem(region(log, fail_ids=frozenset({"bad"})), "s") as system:
        Sharding.entity_ref_for(system, "bad").tell(Add(1))
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Add(1))
        assert await ask(a, Get, timeout=1) == 1

        async def only_a() -> bool:
            return await live(system) == frozenset({"a"})

        for _ in range(50):
            if await only_a():
                break
            await asyncio.sleep(0.002)
        assert await only_a()
    assert "start bad" not in log


async def test_empty_entity_id_is_rejected():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        with pytest.raises(ValueError):
            Sharding.entity_ref_for(system, "")
        # a hand-built envelope: dropped, the region survives
        system.tell(ShardingEnvelope("", Add(1)))
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Add(1))
        assert await ask(a, Get, timeout=1) == 1


async def test_region_stop_dead_letters_messages_buffered_for_a_passivating_entity(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG, logger="pekko_py.dead_letters")
    log: list[str] = []
    gate = asyncio.Event()
    # the stop must reach the region before the gate opens
    direct = CoordinatedShutdownSettings(run_by_actor_system_terminate=False)
    async with ActorSystem(region(log, gate), "s", shutdown=direct) as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Stop())
        await eventually(lambda: "got stop a" in log)
        a.tell(Add(1))  # buffered: the entity is stopping behind the gate
        # the region has processed the envelope above
        assert await live(system) == frozenset({"a"})
        system.terminate()
        gate.set()
        await system.when_terminated()
    assert any(
        r.getMessage() == "dead letter to pekko://s/user/a: Add(n=1)" for r in caplog.records
    )


async def test_dot_ids_get_safe_names():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        dot = Sharding.entity_ref_for(system, ".")
        dots = Sharding.entity_ref_for(system, "..")
        assert dot.path.endswith("/%2E") and dots.path.endswith("/%2E%2E")
        dot.tell(Add(1))
        dots.tell(Add(2))
        assert await ask(dot, Get, timeout=1) == 1
        assert await ask(dots, Get, timeout=1) == 2


async def test_entity_refs_for_the_same_id_are_equal():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        a1 = Sharding.entity_ref_for(system, "a")
        a2 = Sharding.entity_ref_for(system, "a")
        b = Sharding.entity_ref_for(system, "b")
        assert a1 == a2 and hash(a1) == hash(a2)
        assert a1 != b
        assert len({a1, a2, b}) == 2


async def test_passivation_buffer_is_bounded(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger="pekko_py.dead_letters")
    log: list[str] = []
    gate = asyncio.Event()
    entity = Entity(Counter, counter(log, gate)).with_buffer_size(2)
    async with ActorSystem(Sharding.init(entity), "s") as system:
        a = Sharding.entity_ref_for(system, "a")
        a.tell(Stop())
        await eventually(lambda: "got stop a" in log)
        a.tell(Add(1))
        a.tell(Add(2))
        a.tell(Add(3))  # beyond the buffer: a dead letter
        # the shard has seen all three
        await eventually(lambda: any("Add(n=3)" in r.getMessage() for r in caplog.records))
        gate.set()
        # until then the full buffer rejects even a Get
        await eventually(lambda: log.count("start a") == 2)
        assert await ask(a, Get, timeout=1) == 3
    assert any(
        r.getMessage() == "dead letter to pekko://s/user/a: Add(n=3)" for r in caplog.records
    )


# --- the Shard actor between region and entities (cluster sharding plan, Task 1) -----------------

import zlib  # noqa: E402

from pekko_py.ref import FunctionRef  # noqa: E402
from pekko_py.cluster.sharding import ClusterShardingSettings, HandOff, ShardStopped, shard_id_of  # noqa: E402


def test_shard_id_is_stable_and_bounded():
    assert shard_id_of("cart-42", 10) == str(zlib.crc32(b"cart-42") % 10)
    assert shard_id_of("cart-42", 10) == shard_id_of("cart-42", 10)
    assert {int(shard_id_of(f"id{i}", 10)) for i in range(200)} <= set(range(10))


async def test_entities_live_under_shard_actors():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        Sharding.entity_ref_for(system, "a").tell(Add(1))
        await eventually(lambda: "start a" in log)
        shard = shard_id_of("a", ClusterShardingSettings().number_of_shards)
        assert system.resolve(f"{system.path}/{shard}/a").path == f"{system.path}/{shard}/a"
        with pytest.raises(LookupError):
            system.resolve(f"{system.path}/a")  # not a direct child of the region any more


def _id_in_another_shard_than(entity_id: str, n: int) -> str:
    return next(
        f"x{i}" for i in range(10_000) if shard_id_of(f"x{i}", n) != shard_id_of(entity_id, n)
    )


async def test_region_state_aggregates_over_shards():
    log: list[str] = []
    async with ActorSystem(region(log), "s") as system:
        other = _id_in_another_shard_than("a", ClusterShardingSettings().number_of_shards)
        Sharding.entity_ref_for(system, "a").tell(Add(1))
        Sharding.entity_ref_for(system, other).tell(Add(1))
        await eventually(lambda: f"start {other}" in log and "start a" in log)
        assert await live(system) == frozenset({"a", other})


async def test_hand_off_stops_a_shard_and_its_entities():
    log: list[str] = []
    async with ActorSystem(region(log, stop_message=Halt()), "s") as system:
        Sharding.entity_ref_for(system, "a").tell(Add(1))
        await eventually(lambda: "start a" in log)
        shard = shard_id_of("a", ClusterShardingSettings().number_of_shards)
        shard_ref = system.resolve(f"{system.path}/{shard}")
        replies: list[ShardStopped] = []
        shard_ref.tell(HandOff(shard, FunctionRef("probe", replies.append)))
        await eventually(lambda: replies == [ShardStopped(shard)])
        assert "got halt a" in log and "stop a" in log
        with pytest.raises(LookupError):
            system.resolve(f"{system.path}/{shard}")  # the shard stopped with its entities


async def test_hand_off_times_out_on_an_entity_that_ignores_its_stop_message():
    log: list[str] = []
    entity = (
        Entity(Counter, counter(log))
        .with_stop_message(Note("ignored"))
        .with_settings(ClusterShardingSettings(handoff_timeout=6.0))
    )
    async with ActorSystem(Sharding.init(entity), "s") as system:
        Sharding.entity_ref_for(system, "a").tell(Add(1))
        await eventually(lambda: "start a" in log)
        shard = shard_id_of("a", ClusterShardingSettings().number_of_shards)
        replies: list[ShardStopped] = []
        began = asyncio.get_running_loop().time()
        system.resolve(f"{system.path}/{shard}").tell(
            HandOff(shard, FunctionRef("probe", replies.append))
        )
        await eventually(lambda: replies == [ShardStopped(shard)], timeout=3)
        # max(handoff_timeout - 5, 1) seconds
        assert asyncio.get_running_loop().time() - began >= 0.9
        assert "note ignored a" in log and "stop a" in log


def test_cluster_sharding_settings_are_validated():
    for bad in (
        dict(number_of_shards=0),
        dict(buffer_size=0),
        dict(rebalance_relative_limit=0.0),
        dict(handoff_timeout=0.0),
    ):
        with pytest.raises(ValueError):
            ClusterShardingSettings(**bad)  # pyright: ignore[reportArgumentType]
    assert ClusterShardingSettings().number_of_shards == 1000


def test_fluent_setters_apply_on_top_of_settings():
    entity = (
        Entity(Counter, counter([]))
        .with_settings(ClusterShardingSettings(number_of_shards=5))
        .with_passivate_after(2.0)
        .with_buffer_size(7)
    )
    assert entity.effective_settings.number_of_shards == 5
    assert entity.effective_settings.passivate_after == 2.0
    assert entity.effective_settings.buffer_size == 7
