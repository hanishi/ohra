"""Pekko's ``RememberEntityStarterManager`` and ``RememberEntityStarter``
(cluster-sharding internal/RememberEntityStarter.scala 33-204): the per-region throttler for
starting remembered entities, and the actor that starts one shard's entities through the region.

Batches are taken in ``sorted(ids)`` order, as Pekko's ``Set.splitAt`` order is unspecified.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...behavior import Behavior, Behaviors
from ...context import ActorContext
from ...ref import ActorRef
from ...signals import Signal, Terminated
from .shard import (
    ClusterShardingSettings,
    EntitiesMovedToOtherShard,
    StartEntities,
    StartEntityAck,
    _StartEntity,  # pyright: ignore[reportPrivateUsage]
)


@dataclass(frozen=True)
class _ContinueAfterDelay:
    """RememberEntityStarter.scala 45."""


@dataclass(frozen=True)
class _StartBatch:
    """RememberEntityStarter.scala 111."""

    batch_size: int


@dataclass(frozen=True)
class _ResendUnAcked:
    """RememberEntityStarter.scala 112."""


def remember_entity_starter_manager(
    region: ActorRef[Any], settings: ClusterShardingSettings
) -> Behavior[StartEntities]:
    """RememberEntityStarter.scala 49-96."""
    delay = settings.entity_recovery_constant_rate_frequency

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        def start(s: StartEntities, constant: bool) -> None:
            ctx.watch(
                ctx.spawn_anonymous(
                    remember_entity_starter(region, s.shard, s.shard_id, s.ids, constant, settings)
                )
            )

        def all_strategy() -> Behavior[Any]:
            @Behaviors.receive_message
            def receive(msg: Any) -> Behavior[Any]:
                if isinstance(msg, StartEntities):
                    start(msg, constant=False)
                    return Behaviors.same
                return Behaviors.unhandled

            # a Terminated: the RememberEntityStarter was done
            return receive.receive_signal(lambda _c, _s: Behaviors.same)

        def constant_strategy_idle() -> Behavior[Any]:
            @Behaviors.receive_message
            def receive(msg: Any) -> Behavior[Any]:
                if isinstance(msg, StartEntities):
                    start(msg, constant=True)
                    return constant_strategy_waiting(())
                return Behaviors.unhandled

            return receive

        def constant_strategy_waiting(work_queue: tuple[StartEntities, ...]) -> Behavior[Any]:
            @Behaviors.receive_message
            def receive(msg: Any) -> Behavior[Any]:
                match msg:
                    case StartEntities():
                        return constant_strategy_waiting((*work_queue, msg))
                    case _ContinueAfterDelay():
                        if not work_queue:
                            return constant_strategy_idle()
                        start(work_queue[0], constant=True)
                        return constant_strategy_waiting(work_queue[1:])
                    case _:
                        return Behaviors.unhandled

            def on_signal(_c: ActorContext[Any], sig: Signal) -> Behavior[Any]:
                if isinstance(sig, Terminated):  # the RememberEntityStarter was done
                    ctx.timers.start_single_timer(
                        _ContinueAfterDelay, _ContinueAfterDelay(), delay
                    )
                    return Behaviors.same
                return Behaviors.unhandled

            return receive.receive_signal(on_signal)

        match settings.entity_recovery_strategy:
            case "all":
                return all_strategy()
            case "constant":
                return constant_strategy_idle()

    return setup


def remember_entity_starter(
    region: ActorRef[Any],
    shard: ActorRef[Any],
    shard_id: str,
    ids: frozenset[str],
    constant_strategy: bool,
    settings: ClusterShardingSettings,
) -> Behavior[Any]:
    """RememberEntityStarter.scala 101-204: starts the entities of one shard through the region,
    so that a changed shard id extractor starts them on the right shard."""
    if not ids:
        raise ValueError("requirement failed")  # require(ids.nonEmpty)

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        ids_left_to_start: frozenset[str] = frozenset()
        waiting_for_ack: frozenset[str] = frozenset()
        entities_moved: frozenset[str] = frozenset()

        def start_batch_ids(entity_ids: frozenset[str]) -> None:
            nonlocal waiting_for_ack
            # these go through the region rather than directly to the shard
            # so that shard id extractor changes make them start on the right shard
            waiting_for_ack = waiting_for_ack | entity_ids
            for entity_id in sorted(entity_ids):
                region.tell(_StartEntity(entity_id, ctx.self))

        def start_batch(batch_size: int) -> None:
            nonlocal ids_left_to_start
            ctx.log.debug("Starting batch of [%d] remembered entities", batch_size)
            ordered = sorted(ids_left_to_start)
            ids_left_to_start = frozenset(ordered[batch_size:])
            start_batch_ids(frozenset(ordered[:batch_size]))

        def on_ack(entity_id: str, ack_from_shard_id: str) -> Behavior[Any]:
            nonlocal ids_left_to_start, waiting_for_ack, entities_moved
            ids_left_to_start -= {entity_id}
            waiting_for_ack -= {entity_id}
            if shard_id != ack_from_shard_id:
                entities_moved |= {entity_id}
            if not waiting_for_ack and not ids_left_to_start:
                if entities_moved:
                    ctx.log.info("Found [%d] entities moved to new shard(s)", len(entities_moved))
                    shard.tell(EntitiesMovedToOtherShard(entities_moved))
                return Behaviors.stopped()
            return Behaviors.same

        def retry_unacked() -> None:
            if waiting_for_ack:
                ctx.log.debug(
                    "Found [%d] remembered entities waiting for StartEntityAck, retrying",
                    len(waiting_for_ack),
                )
                for entity_id in sorted(waiting_for_ack):
                    # for now we just retry all (as that was the existing behavior spread out
                    # over starter and shard) but in the future it could perhaps make sense to
                    # batch also the retries to avoid thundering herd
                    region.tell(_StartEntity(entity_id, ctx.self))

        ctx.log.debug(
            "Shard [%s] starting [%d] remembered entities using strategy [%s]",
            shard_id,
            len(ids),
            settings.entity_recovery_strategy,
        )
        if constant_strategy:
            ids_left_to_start = ids
            n = settings.entity_recovery_constant_rate_number_of_entities
            ctx.timers.start_timer_with_fixed_delay(
                "constant", _StartBatch(n), settings.entity_recovery_constant_rate_frequency
            )
            start_batch(n)
        else:
            ids_left_to_start = frozenset()
            start_batch_ids(ids)
        ctx.timers.start_timer_with_fixed_delay("retry", _ResendUnAcked(), settings.retry_interval)

        @Behaviors.receive_message
        def receive(msg: Any) -> Behavior[Any]:
            match msg:
                case _StartBatch(batch_size=n):
                    start_batch(n)
                case StartEntityAck(entity_id=entity_id, shard_id=ack_from):
                    return on_ack(entity_id, ack_from)
                case _ResendUnAcked():
                    retry_unacked()
                case _:
                    return Behaviors.unhandled
            return Behaviors.same

        return receive

    return setup
