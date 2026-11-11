"""SupervisionSpec and DeprecatedSupervisionSpec, ported from Apache Pekko's cluster-sharding/
src/test/scala/org/apache/pekko/cluster/sharding/SupervisionSpec.scala.

Cases, in Scala order:

- DeprecatedSupervisionSpec, "allow passivation and early stop" (89): the entity is wrapped in
  ``BackoffOpts.onStop(...).props``, a backoff supervisor built from props.
- SupervisionSpec, "allow passivation and early stop" (136): the entity is wrapped in
  ``BackoffSupervisor.props(BackoffOpts.onStop(...))``.

No case waits for remembered entities.

Differences from the Scala spec:

- Classic builds the backoff-supervised entity in two ways, and ohra has one typed way: an
  entity wrapped in ``Behaviors.supervise(...).on_failure(SupervisorStrategy.restart_with_backoff(
  1.0, 30.0, 0.2))``. So both cases run one body, ``allow_passivation_and_early_stop``, and
  differ only in their names;
- one real cluster node that joins itself, with ``verbose_debug_logging`` and
  ``fail_on_invalid_entity_state_transition`` on, as the spec's config;
- ``"passivate"`` tells the shard ``Passivate(ctx.self)`` and returns ``Behaviors.stopped()``
  at once, so the entity stops before the region's stop message, as the Scala's
  ``Passivate(StopMessage)`` followed by ``context.stop(self)``; the stop message is never seen;
- ``Response(self, parentSupervisor)`` is ``Response(self_ref)``: typed has no
  ``parentSupervisor``, so the test watches the entity itself;
- ``EventFilter.debug("passy: Passivation started for [10]", occurrences = 1)`` is ``caplog``
  at DEBUG, but waits for the shard's ``"passy: Entity [10] terminated"`` instead. With no
  backoff supervisor parent that outlives the child, the shard sees the entity's Terminated
  (a system message, so first) before the ``Passivate``, as Pekko's shard would, and logs
  ``Unknown entity passivating`` rather than ``Passivation started``. The wait still shows the
  shard has seen the stop before the next ``Hello``;
- ids are 10 and the spec's ``id % 2`` shard resolver is ohra's ``number_of_shards=2``, so the
  shard is ``shard_id_of("10", 2)``, which the test checks the region hosts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Any

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ClusterSharding,
    Entity,
    EntityContext,
    EntityTypeKey,
    ShardingEnvelope,
    SupervisorStrategy,
    ask,
)
from ohra.cluster.sharding.shard import (
    CurrentShardRegionState,
    GetShardRegionState,
    Passivate,
    shard_id_of,
)
from ohra.testkit import TestProbe
from ohra.testkit.settings import TestKitSettings

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING


@dataclass(frozen=True)
class Response:
    self_ref: ActorRef[Any]


@dataclass(frozen=True)
class Hello:
    reply_to: ActorRef[Response]


@dataclass(frozen=True)
class PassivateNow:
    pass


type Command = Hello | PassivateNow

PASSY = EntityTypeKey[Command]("passy")


def passivating_actor(entity: EntityContext[Command]) -> Behavior[Command]:
    def on_message(ctx: ActorContext[Command], msg: Command) -> Behavior[Command]:
        match msg:
            case PassivateNow():
                entity.shard.tell(Passivate(ctx.self))
                # simulate another message causing a stop before the region sends the stop
                return Behaviors.stopped()
            case Hello(reply_to):
                reply_to.tell(Response(ctx.self))
                return Behaviors.same

    return Behaviors.supervise(Behaviors.receive(on_message)).on_failure(
        SupervisorStrategy.restart_with_backoff(1.0, 30.0, 0.2)
    )


@dataclass
class Host:
    system: Any
    settings: TestKitSettings


async def allow_passivation_and_early_stop(caplog: pytest.LogCaptureFixture) -> None:
    port = free_port()
    settings = replace(
        FAST_SHARDING,
        number_of_shards=2,
        verbose_debug_logging=True,
        fail_on_invalid_entity_state_transition=True,
    )
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        probe: TestProbe[Response] = TestProbe(Host(system, TestKitSettings()))
        region = ClusterSharding(system).init(
            Entity(PASSY, passivating_actor).with_settings(settings)
        )

        region.tell(ShardingEnvelope("10", Hello(probe.ref)))
        response = await probe.receive_message(5)

        with caplog.at_level(logging.DEBUG):
            region.tell(ShardingEnvelope("10", PassivateNow()))
            # waiting only for the entity to stop would race the shard seeing the passivation
            await probe.expect_terminated(response.self_ref, 5)
            await eventually(lambda: "passy: Entity [10] terminated" in caplog.text, timeout=3)

        state: CurrentShardRegionState = await ask(region, GetShardRegionState, timeout=3)
        assert state.shard_ids == frozenset({shard_id_of("10", 2)})

        # this would fail before as the sharded actor would be stuck passivating
        region.tell(ShardingEnvelope("10", Hello(probe.ref)))
        await probe.receive_message(20)


async def test_deprecated_supervision_allow_passivation_and_early_stop(
    caplog: pytest.LogCaptureFixture,
):
    # DeprecatedSupervisionSpec, "allow passivation and early stop" (89)
    await allow_passivation_and_early_stop(caplog)


async def test_supervision_allow_passivation_and_early_stop(caplog: pytest.LogCaptureFixture):
    # SupervisionSpec, "allow passivation and early stop" (136)
    await allow_passivation_and_early_stop(caplog)
