"""Ported from Apache Pekko's ShardWithLeaseSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/ShardWithLeaseSpec.scala): a shard with lease settings does not start its
entities until it holds its lease. The cases, in the Scala order:

- not initialize the shard until the lease is acquired (93)
- retry if lease acquire returns false (101)
- retry if the lease acquire fails (116)
- shutdown if lease is lost (130)

Each case runs on its own one-node cluster, the ``node`` of tests/cluster/test_cluster_sharding.py
with ``TestLease.register(system)``, and a new type name, as Pekko's ``Setup``.

Differences from the Scala:

- The entity id is ``"1"``, whose shard is ``shard_id_of("1", 5)``, which is ``"3"``, not
  Pekko's ``1 % 5``.
- The entity answers ``ack <text>`` to the message's ``reply_to``, a typed message having no
  sender.
- ``expectNoMessage(100.millis)`` is ``expect_no_message(0.1)``.
- ``EventFilter.error(start=..., occurrences=1)`` is caplog: the ERROR records that start with
  the text, counted.
- ``getCurrentCallback().apply(Some(BadLease(...)))`` is
  ``current_callback()(BadLease("bye bye lease"))``, after the lease's probe got the shard's
  ``AcquireReq``: ``TestLease`` sets the callback when the acquire coroutine first runs.
- ``Future.successful(b)`` is a done ``asyncio.Future``.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import dataclass
from typing import Any

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    ShardingEnvelope,
)
from ohra.cluster.sharding import shard_id_of
from ohra.coordination import LeaseUsageSettings
from ohra.testkit import TestProbe
from ohra.testkit.lease import TestLease, TestLeaseExt

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import node
from .test_cluster_sharding_leaving_spec import Host

SETTINGS = ClusterShardingSettings(
    number_of_shards=5,
    fail_on_invalid_entity_state_transition=True,
    verbose_debug_logging=True,
).with_lease_settings(LeaseUsageSettings("test-lease", 2.0))

SHARD = shard_id_of("1", 5)
assert SHARD == "3"


class BadLease(RuntimeError):  # noqa: N818
    pass


@dataclass(frozen=True)
class Msg:
    text: str
    reply_to: ActorRef[str]


def entity_actor(_entity: EntityContext[Msg]) -> Behavior[Msg]:
    @Behaviors.receive
    def on_message(ctx: ActorContext[Msg], msg: Msg) -> Behavior[Msg]:
        ctx.log.info("Msg %s", msg)
        msg.reply_to.tell(f"ack {msg.text}")
        return Behaviors.same

    return on_message


_type_idx = itertools.count(1)


class Setup:
    """Pekko's ``Setup``: a unique type name for each test, and its region."""

    def __init__(self, system: ActorSystem[Any]) -> None:
        self.system = system
        self.type_name = f"type{next(_type_idx)}"
        self.sharding = ClusterSharding(system).init(
            Entity(EntityTypeKey[Msg](self.type_name), entity_actor).with_settings(SETTINGS)
        )
        self.probe = TestProbe[Any](Host(system))

    async def lease_for(self, shard_id: str) -> TestLease:
        lease_name = f"{self.system.name}-shard-{self.type_name}-{shard_id}"
        return await self.probe.await_assert(
            lambda: TestLeaseExt(self.system).get_test_lease(lease_name)
        )

    def hello(self, probe: TestProbe[Any]) -> None:
        self.sharding.tell(ShardingEnvelope("1", Msg("hello", probe.ref)))


def done(value: bool) -> asyncio.Future[bool]:
    future = asyncio.get_running_loop().create_future()
    future.set_result(value)
    return future


def errors_starting(caplog: pytest.LogCaptureFixture, start: str) -> int:
    return sum(
        1
        for r in caplog.records
        if r.levelno == logging.ERROR and r.getMessage().startswith(start)
    )


def one_node() -> ActorSystem[Any]:
    port = free_port()
    system = node(port, [port])
    TestLease.register(system)
    return system


async def test_not_initialize_the_shard_until_the_lease_is_acquired():
    async with one_node() as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        setup = Setup(system)
        probe = TestProbe[str](Host(system))
        setup.hello(probe)
        await probe.expect_no_message(0.1)
        (await setup.lease_for(SHARD)).initial_promise.set_result(True)
        await probe.expect_message("ack hello")


async def test_retry_if_lease_acquire_returns_false(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.ERROR)
    async with one_node() as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        setup = Setup(system)
        probe = TestProbe[str](Host(system))
        setup.hello(probe)
        lease = await setup.lease_for(SHARD)
        lease.initial_promise.set_result(False)
        await probe.expect_no_message(0.1)
        start = f"{setup.type_name}: Failed to get lease for shard id [{SHARD}]"
        assert errors_starting(caplog, start) == 1

        lease.set_next_acquire_result(done(True))
        await probe.expect_message("ack hello")


async def test_retry_if_the_lease_acquire_fails(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.ERROR)
    async with one_node() as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        setup = Setup(system)
        probe = TestProbe[str](Host(system))
        setup.hello(probe)
        lease = await setup.lease_for(SHARD)
        lease.initial_promise.set_exception(BadLease("no lease for you"))
        await probe.expect_no_message(0.1)
        start = f"{setup.type_name}: Failed to get lease for shard id [{SHARD}]"
        assert errors_starting(caplog, start) == 1

        lease.set_next_acquire_result(done(True))
        await probe.expect_message("ack hello")


async def test_shutdown_if_lease_is_lost(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.ERROR)
    async with one_node() as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        setup = Setup(system)
        probe = TestProbe[str](Host(system))
        setup.hello(probe)
        lease = await setup.lease_for(SHARD)
        await lease.probe.expect_message(TestLease.AcquireReq(lease.settings.owner_name))
        lease.initial_promise.set_result(True)
        await probe.expect_message("ack hello")

        start = (
            f"{setup.type_name}: Shard id [{SHARD}] lease lost, stopping shard and killing [1] "
            f"entities. Reason for losing lease: {BadLease('bye bye lease')!r}"
        )
        lease.current_callback()(BadLease("bye bye lease"))
        await eventually(lambda: errors_starting(caplog, start) == 1)

        lease.set_next_acquire_result(done(False))
        setup.hello(probe)
        await probe.expect_no_message(0.1)
