"""The Shard's remember-entities branches (Shard.scala 424-1200). Pekko has no unit spec for the
Shard's remember branches. The unit tests run one shard on ManualTime, with probes for the region,
the store and the starter manager:

- a remembering shard loads its entities from its store before it is initialized, and stashes
  what comes meanwhile;
- the remembered ids go to the starter manager, and the load timeout stops the shard;
- a store that stops restarts the shard, which releases its lease first; a store that throws is
  restarted on its own.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
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
from ohra.cluster.sharding.shard import ShardInitialized, StartEntities, shard_behavior
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
