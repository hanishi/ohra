"""The Shard's remember-entities branches (Shard.scala 424-1200). Pekko has no unit spec for the
Shard's remember branches. The unit tests run one shard on ManualTime, with probes for the region,
the store and the starter manager:

- a remembering shard loads its entities from its store before it is initialized, and stashes
  what comes meanwhile;
- the remembered ids go to the starter manager, and the load timeout stops the shard;
- a store that stops restarts the shard, which releases its lease first; a store that throws is
  restarted on its own;
- a start, by a message or ``StartEntity``, is written before the entity starts, and what comes
  during a write is batched into the next one;
- a passivated entity is written as stopped, one that stops on its own restarts after the
  backoff;
- a write that times out, or a store that stops during it, restarts the shard, which keeps what it
  stashed.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ClusterShardingSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    Passivate,
    ShardingEnvelope,
    Signal,
)
from ohra.cluster.sharding.remember_entities import (
    DDataRememberEntitiesProvider,
    RememberEntitiesShardStore,
)
from ohra.cluster.sharding.shard import (
    CurrentShardState,
    EntitiesMovedToOtherShard,
    GetShardState,
    HandOff,
    ShardInitialized,
    ShardStopped,
    StartEntities,
    StartEntity,
    StartEntityAck,
    _StartEntity,  # pyright: ignore[reportPrivateUsage]
    shard_behavior,
)
from ohra.eventstream import Dropped
from ohra.signals import PostStop
from ohra.testkit import ActorTestKit, ManualTime, TestProbe
from ohra.testkit.lease import TestLease

from .test_shard_lease import LEASE, lease_of

Store = RememberEntitiesShardStore

REMEMBER = ClusterShardingSettings(
    remember_entities=True,
    verbose_debug_logging=True,
    fail_on_invalid_entity_state_transition=True,
)


A = frozenset({"a"})
"""The entity the tests message, remembered by the store so that the shard starts it without
writing to the store."""


@dataclass(frozen=True)
class StoreStarted:
    store: ActorRef[Any]


@dataclass(frozen=True)
class StopStore:
    """Stops the store a ``ProbeShardStoreProvider`` started."""


@dataclass(frozen=True)
class CrashStore:
    """Makes the store a ``ProbeShardStoreProvider`` started raise."""


@dataclass(frozen=True)
class ProbeShardStoreProvider(DDataRememberEntitiesProvider):
    """A provider whose shard store forwards every message to ``store``, and tells it
    ``StoreStarted`` each time it starts."""

    store: ActorRef[Any]

    def shard_store_behavior(self, shard_id: str) -> Behavior[Store.Command]:
        store = self.store

        @Behaviors.setup
        def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
            store.tell(StoreStarted(ctx.self))

            @Behaviors.receive_message
            def forward(msg: Any) -> Behavior[Any]:
                match msg:
                    case StopStore():
                        return Behaviors.stopped()
                    case CrashStore():
                        raise RuntimeError("store crash")
                    case _:
                        store.tell(msg)
                return Behaviors.same

            return forward

        return cast(Behavior[Store.Command], setup)


@dataclass(frozen=True)
class Started:
    entity_id: str


@dataclass(frozen=True)
class Stopped:
    entity_id: str


@dataclass(frozen=True)
class Pong:
    entity_id: str


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[Pong]


@dataclass(frozen=True)
class Stop:
    """The entity's stop message."""


@dataclass(frozen=True)
class GracefulStop:
    """Asks the entity to passivate."""


type Cmd = Ping | Stop | GracefulStop

Reporter = EntityTypeKey[Cmd]("Reporter")


def reporter(events: ActorRef[Any]) -> Entity[Cmd]:
    def create(entity: EntityContext[Cmd]) -> Behavior[Cmd]:
        events.tell(Started(entity.entity_id))

        def on_message(ctx: ActorContext[Cmd], msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Ping(reply_to):
                    reply_to.tell(Pong(entity.entity_id))
                case Stop():
                    return Behaviors.stopped()
                case GracefulStop():
                    entity.shard.tell(Passivate(ctx.self))
            return Behaviors.same

        def on_signal(_ctx: ActorContext[Cmd], sig: Signal) -> Behavior[Cmd]:
            if sig is PostStop:
                events.tell(Stopped(entity.entity_id))
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    return Entity(Reporter, create).with_stop_message(Stop())


@dataclass
class Harness:
    testkit: ActorTestKit
    manual: ManualTime
    shard: ActorRef[Any]
    region: TestProbe[Any]
    store: TestProbe[Any]
    starter: TestProbe[Any]
    events: TestProbe[Any]

    async def expect_get(self) -> Store.GetEntities:
        return await self.store.expect_message_type(Store.GetEntities, 3)

    async def initialized(self, remembered: frozenset[str] = frozenset()) -> None:
        """Answers the pending ``GetEntities`` and waits for ``ShardInitialized``."""
        (await self.expect_get()).reply_to.tell(Store.RememberedEntities(remembered))
        assert await self.region.expect_message_type(ShardInitialized, 3) == ShardInitialized("1")

    async def ping(self, entity_id: str) -> None:
        replies: TestProbe[Pong] = self.testkit.create_test_probe()
        self.shard.tell(ShardingEnvelope(entity_id, Ping(replies.ref)))
        await replies.expect_message(Pong(entity_id), 3)

    async def expect_update(
        self, started: frozenset[str] = frozenset(), stopped: frozenset[str] = frozenset()
    ) -> Store.Update:
        update = await self.store.expect_message_type(Store.Update, 3)
        assert (update.started, update.stopped) == (started, stopped)
        return update

    @staticmethod
    def done(update: Store.Update) -> None:
        update.reply_to.tell(Store.UpdateDone(update.started, update.stopped))

    async def active_ids_become(self, ids: frozenset[str]) -> None:
        state: TestProbe[CurrentShardState] = self.testkit.create_test_probe()

        async def check() -> None:
            self.shard.tell(GetShardState(state.ref))
            assert (await state.expect_message_type(CurrentShardState, 3)).entity_ids == ids

        await state.await_assert(check, 3)


@asynccontextmanager
async def remembering_shard(
    settings: ClusterShardingSettings = REMEMBER, lease: bool = False
) -> AsyncGenerator[Harness]:
    """A remembering shard "1", its store's ``StoreStarted`` seen; the store's ``GetEntities`` is
    left to the test."""
    async with ActorTestKit(manual_time=True) as testkit:
        if lease:
            TestLease.register(testkit.system)
            settings = settings.with_lease_settings(LEASE)
        region: TestProbe[Any] = testkit.create_test_probe()
        store: TestProbe[Any] = testkit.create_test_probe()
        starter: TestProbe[Any] = testkit.create_test_probe()
        events: TestProbe[Any] = testkit.create_test_probe()
        provider = ProbeShardStoreProvider("Reporter", settings, 5, region.ref, store.ref)
        entity = reporter(events.ref).with_settings(settings)
        shard = testkit.spawn(shard_behavior(entity, "1", region.ref, provider, starter.ref), "1")
        await store.expect_message_type(StoreStarted, 3)
        yield Harness(testkit, ManualTime(testkit.system), shard, region, store, starter, events)


async def test_a_remembering_shard_loads_its_entities_before_it_is_initialized():
    async with remembering_shard() as h:
        get = await h.expect_get()
        assert get.reply_to == h.shard
        await h.region.expect_no_message(0.1)  # no ShardInitialized yet
        get.reply_to.tell(Store.RememberedEntities(frozenset()))
        assert await h.region.expect_message_type(ShardInitialized) == ShardInitialized("1")


async def test_messages_wait_until_the_entities_are_loaded():
    async with remembering_shard() as h:
        get = await h.expect_get()
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        await replies.expect_no_message(0.1)
        get.reply_to.tell(Store.RememberedEntities(frozenset({"a"})))
        await replies.expect_message(Pong("a"), 3)


async def test_remembered_entities_are_handed_to_the_starter_manager():
    async with remembering_shard() as h:
        await h.initialized(frozenset({"a", "b"}))
        assert await h.starter.expect_message_type(StartEntities, 3) == StartEntities(
            h.shard, "1", frozenset({"a", "b"})
        )


async def test_no_starter_is_asked_when_nothing_is_remembered():
    async with remembering_shard() as h:
        await h.initialized()
        await h.starter.expect_no_message(0.1)


async def test_the_load_timeout_stops_the_shard(caplog: pytest.LogCaptureFixture):
    async with remembering_shard() as h:
        await h.expect_get()
        await h.manual.time_passes(REMEMBER.waiting_for_state_timeout)
        await h.region.expect_terminated(h.shard, 3)
        assert any(
            r.levelno == logging.ERROR
            and r.getMessage()
            == "Reporter: Failed to load initial entity ids from remember entities store within "
            "[2 s], stopping shard for backoff and restart"
            for r in caplog.records
        )


async def test_a_store_that_stops_restarts_the_shard():
    async with remembering_shard() as h:
        await h.initialized(A)
        await h.ping("a")
        await h.events.expect_message(Started("a"))
        h.testkit.system.resolve(f"{h.shard.path}/RememberEntitiesStore").tell(StopStore())
        await h.events.expect_message(Stopped("a"), 3)
        await h.store.expect_message_type(StoreStarted, 3)
        await h.initialized(A)
        await h.ping("a")  # the same shard ref, so the shard was restarted, not terminated
        await h.events.expect_message(Started("a"))


async def test_a_store_that_throws_is_restarted_and_the_shard_keeps_running():
    async with remembering_shard() as h:
        await h.initialized(A)
        await h.ping("a")
        await h.events.expect_message(Started("a"))
        h.testkit.system.resolve(f"{h.shard.path}/RememberEntitiesStore").tell(CrashStore())
        await h.store.expect_message_type(StoreStarted, 3)
        await h.store.expect_no_message(0.1)  # no new GetEntities
        await h.region.expect_no_message(0)  # no new ShardInitialized
        await h.events.expect_no_message(0)  # "a" was not stopped
        await h.ping("a")


async def test_a_restart_releases_the_lease_before_acquiring_it_again():
    async with remembering_shard(lease=True) as h:
        system = h.testkit.system
        lease = await lease_of(system, h.region, f"{system.name}-shard-Reporter-1")
        owner = lease.settings.owner_name
        await lease.probe.expect_message(TestLease.AcquireReq(owner), 3)
        lease.initial_promise.set_result(True)
        await h.initialized()
        system.resolve(f"{h.shard.path}/RememberEntitiesStore").tell(StopStore())
        await lease.probe.expect_message(TestLease.ReleaseReq(owner), 3)
        await lease.probe.expect_message(TestLease.AcquireReq(owner), 3)
        await h.store.expect_message_type(StoreStarted, 3)
        await h.initialized()


async def test_without_a_provider_the_shard_is_initialized_at_once():
    async with ActorTestKit(manual_time=True) as testkit:
        region: TestProbe[Any] = testkit.create_test_probe()
        events: TestProbe[Any] = testkit.create_test_probe()
        replies: TestProbe[Pong] = testkit.create_test_probe()
        shard = testkit.spawn(shard_behavior(reporter(events.ref), "1", region.ref), "1")
        await region.expect_message(ShardInitialized("1"), 3)
        shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        await replies.expect_message(Pong("a"), 3)


async def test_a_message_to_a_new_entity_is_remembered_before_the_entity_starts():
    async with remembering_shard() as h:
        await h.initialized()
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        update = await h.expect_update(started=A)
        await h.events.expect_no_message(0.1)
        h.done(update)
        await h.events.expect_message(Started("a"), 3)
        await replies.expect_message(Pong("a"), 3)


async def test_starts_during_a_write_are_batched_into_the_next_update():
    async with remembering_shard() as h:
        await h.initialized()
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        first = await h.expect_update(started=A)
        h.shard.tell(ShardingEnvelope("b", Ping(replies.ref)))
        h.shard.tell(ShardingEnvelope("c", Ping(replies.ref)))
        h.done(first)
        await replies.expect_message(Pong("a"), 3)
        second = await h.expect_update(started=frozenset({"b", "c"}))
        await replies.expect_no_message(0.1)
        h.done(second)
        assert {await replies.receive_message(), await replies.receive_message()} == {
            Pong("b"),
            Pong("c"),
        }


async def test_no_update_follows_a_write_with_nothing_pending():
    async with remembering_shard() as h:
        await h.initialized()
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        h.done(await h.expect_update(started=A))
        await replies.expect_message(Pong("a"), 3)
        await h.store.expect_no_message(0.2)


async def test_start_entity_is_acked_after_its_start_is_written():
    async with remembering_shard() as h:
        await h.initialized()
        ack: TestProbe[StartEntityAck] = h.testkit.create_test_probe()
        h.shard.tell(_StartEntity("a", ack.ref))
        update = await h.expect_update(started=A)
        await ack.expect_no_message(0.1)
        h.done(update)
        await ack.expect_message(StartEntityAck("a", "1"), 3)
        await h.events.expect_message(Started("a"), 3)


async def test_start_entity_for_a_running_entity_is_acked_at_once():
    async with remembering_shard() as h:
        await h.initialized(A)
        await h.ping("a")
        ack: TestProbe[StartEntityAck] = h.testkit.create_test_probe()
        h.shard.tell(_StartEntity("a", ack.ref))
        await ack.expect_message(StartEntityAck("a", "1"), 3)
        await h.store.expect_no_message(0.1)


async def test_a_remembered_entity_starts_on_start_entity_without_a_write():
    async with remembering_shard() as h:
        await h.initialized(A)
        ack: TestProbe[StartEntityAck] = h.testkit.create_test_probe()
        h.shard.tell(_StartEntity("a", ack.ref))
        await h.events.expect_message(Started("a"), 3)
        await ack.expect_message(StartEntityAck("a", "1"), 3)
        await h.store.expect_no_message(0.1)


async def test_start_entity_in_an_envelope_is_remembered_with_no_ack():
    async with remembering_shard() as h:
        await h.initialized()
        h.shard.tell(ShardingEnvelope("a", StartEntity("a")))
        update = await h.expect_update(started=A)
        await h.events.expect_no_message(0.1)
        h.done(update)
        await h.events.expect_message(Started("a"), 3)
        await h.active_ids_become(A)


async def test_an_entity_that_stops_on_its_own_restarts_after_the_backoff():
    async with remembering_shard() as h:
        await h.initialized(A)
        await h.ping("a")
        await h.events.expect_message(Started("a"))
        h.shard.tell(ShardingEnvelope("a", Stop()))
        await h.events.expect_message(Stopped("a"), 3)
        await h.active_ids_become(frozenset())
        await h.events.expect_no_message(0.1)
        await h.manual.time_passes(REMEMBER.entity_restart_backoff)
        await h.events.expect_message(Started("a"), 3)
        await h.active_ids_become(A)
        await h.store.expect_no_message(0)


async def test_a_message_to_an_entity_waiting_for_restart_starts_it_at_once():
    async with remembering_shard() as h:
        await h.initialized(A)
        await h.ping("a")
        await h.events.expect_message(Started("a"))
        h.shard.tell(ShardingEnvelope("a", Stop()))
        await h.events.expect_message(Stopped("a"), 3)
        await h.active_ids_become(frozenset())
        await h.ping("a")
        await h.events.expect_message(Started("a"))
        # the restart timer finds the entity running
        await h.manual.time_passes(REMEMBER.entity_restart_backoff)
        await h.ping("a")
        await h.events.expect_no_message(0.1)
        await h.store.expect_no_message(0)


async def test_a_passivated_entity_is_remembered_as_stopped():
    async with remembering_shard() as h:
        await h.initialized(A)
        await h.ping("a")
        await h.events.expect_message(Started("a"))
        h.shard.tell(ShardingEnvelope("a", GracefulStop()))
        await h.events.expect_message(Stopped("a"), 3)
        h.done(await h.expect_update(stopped=A))
        await h.active_ids_become(frozenset())
        await h.store.expect_no_message(0.1)


async def test_a_message_during_passivation_restarts_the_entity_once_the_stop_is_written():
    async with remembering_shard() as h:
        await h.initialized(A)
        await h.ping("a")
        await h.events.expect_message(Started("a"))
        h.shard.tell(ShardingEnvelope("a", GracefulStop()))
        await h.events.expect_message(Stopped("a"), 3)
        stop = await h.expect_update(stopped=A)
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        h.done(stop)
        start = await h.expect_update(started=A)
        await h.events.expect_no_message(0.1)
        h.done(start)
        await h.events.expect_message(Started("a"), 3)
        await replies.expect_message(Pong("a"), 3)


async def test_entities_moved_to_another_shard_are_written_as_stopped():
    async with remembering_shard() as h:
        await h.initialized(frozenset({"a", "b"}))
        h.shard.tell(EntitiesMovedToOtherShard(frozenset({"b"})))
        h.done(await h.expect_update(stopped=frozenset({"b"})))
        await h.store.expect_no_message(0.1)
        await h.ping("a")


async def test_the_write_timeout_restarts_the_shard_and_keeps_what_it_stashed(
    caplog: pytest.LogCaptureFixture,
):
    async with remembering_shard() as h:
        await h.initialized()
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        await h.expect_update(started=A)
        hand: TestProbe[ShardStopped] = h.testkit.create_test_probe()
        h.shard.tell(HandOff("1", hand.ref))
        await h.manual.time_passes(REMEMBER.updating_state_timeout)
        await h.store.expect_message_type(StoreStarted, 3)
        await h.initialized(A)  # the restarted shard loads again, then replays the HandOff
        await hand.expect_message(ShardStopped("1"), 3)
        assert any(
            r.levelno == logging.ERROR
            and r.getMessage()
            == "Reporter: Remember entity store did not respond, restarting shard"
            for r in caplog.records
        )


async def test_hand_off_waits_for_a_pending_write():
    async with remembering_shard() as h:
        await h.initialized()
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        update = await h.expect_update(started=A)
        hand: TestProbe[ShardStopped] = h.testkit.create_test_probe()
        h.shard.tell(HandOff("1", hand.ref))
        await hand.expect_no_message(0.1)
        h.done(update)
        await replies.expect_message(Pong("a"), 3)
        await hand.expect_message(ShardStopped("1"), 3)
        await h.events.expect_message(Started("a"))
        await h.events.expect_message(Stopped("a"), 3)


async def test_messages_beyond_buffer_size_are_dropped_while_a_start_is_written():
    async with remembering_shard(replace(REMEMBER, buffer_size=2)) as h:
        await h.initialized()
        dropped = h.testkit.create_dropped_message_probe()
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        for _ in range(3):
            h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        update = await h.expect_update(started=A)
        await dropped.expect_message(
            Dropped(ShardingEnvelope("a", Ping(replies.ref)), "Buffer for [a] is full", h.shard), 3
        )
        h.done(update)
        await replies.expect_message(Pong("a"), 3)
        await replies.expect_message(Pong("a"), 3)
        await replies.expect_no_message(0.1)


async def test_a_store_crash_while_writing_restarts_the_shard():
    async with remembering_shard() as h:
        await h.initialized()
        replies: TestProbe[Pong] = h.testkit.create_test_probe()
        h.shard.tell(ShardingEnvelope("a", Ping(replies.ref)))
        await h.expect_update(started=A)
        h.testkit.system.resolve(f"{h.shard.path}/RememberEntitiesStore").tell(StopStore())
        await h.store.expect_message_type(StoreStarted, 3)
        await h.initialized(A)
        await h.ping("a")
