"""Ported from Apache Pekko's ClusterShardingLeaseSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/ClusterShardingLeaseSpec.scala), without remembered entities. The cases, in
the Scala order:

- not start until lease is acquired (118)
- retry if initial acquire is false (125)
- retry if initial acquire fails (134)
- recover if lease lost (143)

Pekko's two subclasses with remembered entities, ``PersistenceClusterShardingLeaseSpec`` and
``DDataClusterShardingLeaseSpec`` (76-78), are left for sub-project 10, which adds them.

Differences from the Scala:

- The Scala runs every case on one system and one region. Here each case starts its own one-node
  cluster, the ``node`` of tests/cluster/test_cluster_sharding.py, so nothing outlives a test.
- The settings are ``FAST_SHARDING``'s with ``lease_retry_interval=0.2`` and
  ``number_of_shards=10``, verbose debug logging and failing on an invalid entity state
  transition.
- The entity echoes its ``int`` to the test's probe (Pekko's ``ImplicitSender``), and the
  entity id is ``str(n)``. The ids 1-4 land on shards ``"3"``, ``"7"``, ``"1"`` and ``"8"``
  (``shard_id_of(str(i), 10)``), all distinct, as Pekko's ``n % 10``.
- ``expectNoMessage(200.millis)`` is ``expect_no_message(0.2)``, and ``Future.successful(true)``
  is a done ``asyncio.Future``.
- ``getCurrentCallback()(Option(LeaseFailed("oh dear")))`` is
  ``current_callback()(LeaseFailed("oh dear"))``, after the lease's probe got the shard's
  ``AcquireReq``: ``TestLease`` sets the callback when the acquire coroutine first runs.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
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
from ..test_cluster_sharding import FAST_SHARDING, node
from .test_cluster_sharding_leaving_spec import Host

NUM_OF_SHARDS = 10
SETTINGS = replace(
    FAST_SHARDING,
    number_of_shards=NUM_OF_SHARDS,
    verbose_debug_logging=True,
    fail_on_invalid_entity_state_transition=True,
).with_lease_settings(LeaseUsageSettings("test-lease", 0.2))

SHARDS = {i: shard_id_of(str(i), NUM_OF_SHARDS) for i in range(1, 5)}
assert SHARDS == {1: "3", 2: "7", 3: "1", 4: "8"}

TYPE_NAME = "echo"


class LeaseFailed(RuntimeError):  # noqa: N818
    pass


def echo(reply_to: ActorRef[int]) -> Behavior[int]:
    @Behaviors.receive
    def on_message(_ctx: ActorContext[int], msg: int) -> Behavior[int]:
        reply_to.tell(msg)
        return Behaviors.same

    return on_message


@dataclass
class Spec:
    system: ActorSystem[Any]
    region: ActorRef[ShardingEnvelope[int]]
    probe: TestProbe[int]

    def tell(self, n: int) -> None:
        self.region.tell(ShardingEnvelope(str(n), n))

    async def lease_for_shard(self, n: int) -> TestLease:
        name = f"{self.system.name}-shard-{TYPE_NAME}-{SHARDS[n]}"
        return await self.probe.await_assert(
            lambda: TestLeaseExt(self.system).get_test_lease(name)
        )


def done(value: bool) -> asyncio.Future[bool]:
    future = asyncio.get_running_loop().create_future()
    future.set_result(value)
    return future


@asynccontextmanager
async def spec() -> AsyncGenerator[Spec]:
    """Pekko's ``atStartup``: a one-node cluster, Up, with the region started."""
    port = free_port()
    async with node(port, [port]) as system:
        TestLease.register(system)
        await eventually(lambda: all_up(system, 1), timeout=5)
        probe = TestProbe[int](Host(system))

        def create(_entity: EntityContext[int]) -> Behavior[int]:
            return echo(probe.ref)

        region = ClusterSharding(system).init(
            Entity(EntityTypeKey[int](TYPE_NAME), create).with_settings(SETTINGS)
        )
        yield Spec(system, region, probe)


async def test_not_start_until_lease_is_acquired():
    async with spec() as s:
        s.tell(1)
        await s.probe.expect_no_message(0.2)
        test_lease = await s.lease_for_shard(1)
        test_lease.initial_promise.set_result(True)
        await s.probe.expect_message(1)


async def test_retry_if_initial_acquire_is_false():
    async with spec() as s:
        s.tell(2)
        await s.probe.expect_no_message(0.2)
        test_lease = await s.lease_for_shard(2)
        test_lease.initial_promise.set_result(False)
        await s.probe.expect_no_message(0.2)
        test_lease.set_next_acquire_result(done(True))
        await s.probe.expect_message(2)


async def test_retry_if_initial_acquire_fails():
    async with spec() as s:
        s.tell(3)
        await s.probe.expect_no_message(0.2)
        test_lease = await s.lease_for_shard(3)
        test_lease.initial_promise.set_exception(LeaseFailed("oh no"))
        await s.probe.expect_no_message(0.2)
        test_lease.set_next_acquire_result(done(True))
        await s.probe.expect_message(3)


async def test_recover_if_lease_lost():
    async with spec() as s:
        s.tell(4)
        await s.probe.expect_no_message(0.2)
        test_lease = await s.lease_for_shard(4)
        await test_lease.probe.expect_message(TestLease.AcquireReq(test_lease.settings.owner_name))
        test_lease.initial_promise.set_result(True)
        await s.probe.expect_message(4)
        test_lease.current_callback()(LeaseFailed("oh dear"))

        async def answers() -> None:
            s.tell(4)
            await s.probe.expect_message(4)

        await s.probe.await_assert(answers, max=10.0)
