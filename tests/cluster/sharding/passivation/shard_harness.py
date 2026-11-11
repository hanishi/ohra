"""Pekko's ``AbstractEntityPassivationSpec`` (EntityPassivationSpec.scala 93-144) on real shards:
the entity of 56-80, its probes, and a stand-in for Pekko's ``ShardRegion`` that the shard-level
passivation specs drive.

The stand-in puts the entity id ``str(id)`` in the shard ``str(shard)`` the spec names. ohra's
regions route by ``shard_id_of``, which cannot place ids on chosen shards, and the composite
specs' admission decisions depend on the ids' hashes, so the ids stay Pekko's. It does what
Pekko's region does for these specs:

- a new shard is spawned with ``shard_behavior(entity, str(shard), region)``, and then every
  shard gets ``ShardsUpdated(n)`` (ShardRegion.scala ``getShard`` 1356-1360), unless
  passivation is disabled, before the envelope. The shard is initialized as it starts (it has
  no lease), so the region's buffering until ``ShardInitialized`` has nothing to hold;
- ``set_active_entity_limit`` tells every shard (``deliverToAllShards`` 1278);
- ``expect_state`` asks every shard ``GetShardState`` and retries, as Pekko's ``eventually``
  around ``GetShardRegionState``.

The test kit runs on ``ManualTime``: Pekko's ``Clock`` is the scheduler's seconds, and the
passivation interval ticks only when ``time_passes``. The real regions' part of this is tested
in ``tests/cluster/sharding/test_active_entity_limit.py``.
"""

from collections.abc import AsyncGenerator, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.cluster.sharding.passivation_settings import (
    NoPassivationStrategy,
    PassivationStrategySettings,
)
from ohra.cluster.sharding.shard import (
    ClusterShardingSettings,
    CurrentShardState,
    Entity,
    EntityContext,
    EntityTypeKey,
    GetShardState,
    Passivate,
    SetActiveEntityLimit,
    ShardingEnvelope,
    ShardsUpdated,
    shard_behavior,
)
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


@dataclass(frozen=True)
class Stop:
    """Scala 58: the entity's stop message."""


@dataclass(frozen=True)
class ManuallyPassivate:
    """Scala 59: the entity asks its shard to passivate it."""


@dataclass(frozen=True)
class Received:
    """Scala 61: what an entity received, and the scheduler's time then."""

    id: str
    message: object
    time: float


def _entity(
    probes: Mapping[str, ActorRef[Received]], ec: EntityContext[object]
) -> Behavior[object]:
    """Scala 66-80."""

    def setup(ctx: ActorContext[object]) -> Behavior[object]:
        def received(message: object) -> None:
            probes[ec.entity_id].tell(
                Received(ec.entity_id, message, ctx.system.scheduler.current_time)
            )

        def on_message(ctx: ActorContext[object], msg: object) -> Behavior[object]:
            received(msg)
            match msg:
                case Stop():
                    return Behaviors.stopped()
                case ManuallyPassivate():
                    ec.shard.tell(Passivate(ctx.self))
                case _:
                    pass
            return Behaviors.same

        return Behaviors.receive(on_message)

    return Behaviors.setup(setup)


class ShardRegionHarness:
    """The region of Pekko's spec, with its ``expectReceived``, ``expectNoMessage`` and
    ``expectState`` (Scala 111-130)."""

    def __init__(
        self,
        testkit: ActorTestKit,
        entity: Entity[object],
        probes: Mapping[int, TestProbe[Received]],
    ) -> None:
        self.settings = entity.effective_settings
        self._testkit = testkit
        self._entity = entity
        self._probes = probes
        self._manual_time = ManualTime(testkit.system)
        self._region: TestProbe[Any] = testkit.create_test_probe()  # gets ShardInitialized
        self._shards: dict[int, ActorRef[Any]] = {}  # in start order

    def tell(self, shard: int, id: int, message: object) -> None:
        """``region ! Envelope(shard, id, message)``: ``getShard`` (1331-1370), then the
        envelope to the shard."""
        if shard not in self._shards:
            self._shards[shard] = self._testkit.spawn(
                shard_behavior(self._entity, str(shard), self._region.ref), str(shard)
            )
            if self.settings.passivation_strategy != NoPassivationStrategy():
                update = ShardsUpdated(len(self._shards))
                for ref in self._shards.values():
                    ref.tell(update)
        self._shards[shard].tell(ShardingEnvelope(str(id), message))

    def set_active_entity_limit(self, limit: int) -> None:
        """``region ! ShardRegion.SetActiveEntityLimit(limit)``."""
        for ref in self._shards.values():
            ref.tell(SetActiveEntityLimit(limit))

    async def expect_received(self, id: int, message: object) -> Received:
        """Scala 111-115: the entity's next message is ``message``."""
        received = await self._probes[id].expect_message_type(Received)
        assert received.message == message, f"{id}: expected {message!r}, got {received!r}"
        return received

    async def expect_no_message(self, id: int) -> None:
        """Scala 117-118, once ``time_passes`` has moved the clock."""
        await self._probes[id].expect_no_message()

    async def expect_state(self, expected: Mapping[int, Iterable[int]]) -> None:
        """Scala 120-130: every shard's active entities, retried until they match."""
        want = {shard: {str(id) for id in ids} for shard, ids in expected.items()}

        async def check() -> None:
            replies: TestProbe[CurrentShardState] = self._testkit.create_test_probe()
            for ref in self._shards.values():
                ref.tell(GetShardState(replies.ref))
            states = [await replies.receive_message() for _ in self._shards]
            replies.stop()
            assert {int(s.shard): set(s.entity_ids) for s in states} == want

        await self._region.await_assert(check)

    async def time_passes(self, seconds: float) -> None:
        """``Thread.sleep``: the clock moves, and the passivation interval ticks on the way."""
        await self._manual_time.time_passes(seconds)

    def now(self) -> float:
        """Pekko's ``clock.currentTime()``."""
        return self._testkit.system.scheduler.current_time


@asynccontextmanager
async def shard_region(
    passivation: PassivationStrategySettings, expected_entities: int
) -> AsyncGenerator[ShardRegionHarness]:
    """Scala 93-144: the settings of ``EntityPassivationSpec.config`` with ``passivation``, and
    a probe for each of the ids 1 to ``expected_entities``."""
    async with ActorTestKit(manual_time=True) as testkit:
        settings = ClusterShardingSettings(
            verbose_debug_logging=True, fail_on_invalid_entity_state_transition=True
        ).with_passivation_strategy(passivation)
        probes: dict[int, TestProbe[Received]] = {
            id: testkit.create_test_probe() for id in range(1, expected_entities + 1)
        }
        refs = {str(id): probe.ref for id, probe in probes.items()}
        entity = (
            Entity(EntityTypeKey[object]("myType"), lambda ec: _entity(refs, ec))
            .with_settings(settings)
            .with_stop_message(Stop())
        )
        yield ShardRegionHarness(testkit, entity, probes)
