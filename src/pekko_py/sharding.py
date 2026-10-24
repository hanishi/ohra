"""Sharding: one entity actor per id, created on its first message, passivated when idle or on request.

This is the region/shard layer of Apache Pekko's ClusterSharding inside one process: no coordinator,
no rebalancing, no remembered entities. Messages for a passivating entity are buffered and delivered
to its next incarnation; an entity that stops on its own, by returning ``Behaviors.stopped()``, loses
what is queued behind it, as in Pekko, so entities should passivate rather than stop themselves.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import quote

from .behavior import Behavior, Behaviors
from .context import ActorContext
from .ref import ActorRef
from .serialization import remotable
from .signals import PostStop, Signal, Terminated


def _child_name(entity_id: str) -> str:
    """Any id becomes a valid, unique child name. Pure-dot ids are escaped so paths never read as '.' or '..'."""
    name = quote(entity_id, safe="")
    return name.replace(".", "%2E") if set(name) == {"."} else name


@dataclass(frozen=True)
class EntityTypeKey[T]:
    """Names an entity type; the type parameter is the entity's message type."""

    name: str


class ShardCommand:
    """Messages an entity may send to its shard (``EntityContext.shard``)."""

    __slots__ = ()


@dataclass(frozen=True)
class Passivate(ShardCommand):
    """Sent by an entity to ask the shard to stop it. Messages meanwhile are buffered for the next incarnation."""

    entity: ActorRef[Any]


@dataclass(frozen=True)
class EntityContext[T]:
    entity_type_key: EntityTypeKey[T]
    entity_id: str
    shard: ActorRef[ShardCommand]


@remotable
@dataclass(frozen=True)
class ShardingEnvelope[T]:
    """What the region accepts: a message addressed to an entity id."""

    entity_id: str
    message: T


@dataclass(frozen=True)
class CurrentShardRegionState:
    entity_ids: frozenset[str]


@dataclass(frozen=True)
class GetShardRegionState:
    reply_to: ActorRef[CurrentShardRegionState]


@dataclass(frozen=True)
class Entity[T]:
    """How to create an entity of a type, plus its passivation settings."""

    type_key: EntityTypeKey[T]
    create_behavior: Callable[[EntityContext[T]], Behavior[T]]
    stop_message: T | None = None
    passivate_after: float | None = None
    buffer_size: int = 100_000  # messages kept for a passivating entity; beyond this they are dead letters

    def with_buffer_size(self, size: int) -> Entity[T]:
        """Cap on messages buffered while an entity passivates, as Pekko's sharding buffer-size setting."""
        return replace(self, buffer_size=size)

    def with_stop_message(self, msg: T) -> Entity[T]:
        """Passivation delivers msg and lets the entity stop itself, instead of stopping it outright."""
        return replace(self, stop_message=msg)

    def with_passivate_after(self, seconds: float) -> Entity[T]:
        """Stop an entity that has received no message for this long. Not in Pekko, which uses settings."""
        return replace(self, passivate_after=seconds)


class EntityRef[T](ActorRef[T]):
    """A ref to an entity by id. The entity need not exist yet: the region creates it on the first message."""

    __slots__ = ("_region", "entity_id")

    def __init__(self, region: ActorRef[ShardingEnvelope[T]], entity_id: str) -> None:
        self._region = region
        self.entity_id = entity_id

    @property
    def path(self) -> str:
        return f"{self._region.path}/{_child_name(self.entity_id)}"

    @property
    def region(self) -> ActorRef[ShardingEnvelope[T]]:
        return self._region

    def tell(self, msg: T) -> None:
        self._region.tell(ShardingEnvelope(self.entity_id, msg))

    async def offer(self, msg: T) -> None:
        await self._region.offer(ShardingEnvelope(self.entity_id, msg))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, EntityRef) and (self.path, self.entity_id) == (other.path, other.entity_id)

    def __hash__(self) -> int:
        return hash((self._region.path, self.entity_id))


@dataclass(frozen=True)
class _IdleTick:
    pass


type _RegionCmd[T] = ShardingEnvelope[T] | GetShardRegionState | ShardCommand | _IdleTick


class Sharding:
    @staticmethod
    def init[T](entity: Entity[T]) -> Behavior[ShardingEnvelope[T] | GetShardRegionState]:
        """The shard region behavior for one entity type. Spawn it; message it through entity refs."""

        def setup(ctx: ActorContext[_RegionCmd[T]]) -> Behavior[_RegionCmd[T]]:
            entities: dict[str, ActorRef[T]] = {}
            ids_by_ref: dict[ActorRef[T], str] = {}  # LocalRef hashes by identity: a stale incarnation misses
            passivating: dict[str, list[T]] = {}  # messages buffered until the entity's Terminated arrives
            last_seen: dict[str, float] = {}
            loop = asyncio.get_running_loop()
            if entity.passivate_after is not None:  # one tick for the region, as Pekko, instead of a timer per message
                ctx.timers.start_timer_with_fixed_delay("idle", _IdleTick(), entity.passivate_after / 2)

            def start(entity_id: str) -> ActorRef[T]:
                entity_ctx = EntityContext(entity.type_key, entity_id, ctx.self)
                # Created inside the child's own setup, so a failing factory fails that entity, not the region.
                behavior: Behavior[T] = Behaviors.setup(lambda _ctx: entity.create_behavior(entity_ctx))
                ref = ctx.spawn(behavior, _child_name(entity_id))
                ctx.watch(ref)
                entities[entity_id] = ref
                ids_by_ref[ref] = entity_id
                return ref

            def deliver(entity_id: str, message: T) -> None:
                (entities.get(entity_id) or start(entity_id)).tell(message)
                last_seen[entity_id] = loop.time()

            def passivate(entity_id: str) -> None:
                ref = entities.get(entity_id)
                if ref is None or entity_id in passivating:
                    return  # unknown, or already stopping: a duplicate Passivate must not touch the next incarnation
                passivating[entity_id] = []
                last_seen.pop(entity_id, None)
                if entity.stop_message is not None:
                    ref.tell(entity.stop_message)
                else:
                    ctx.stop(ref, after_pending=True)  # after what the region already forwarded, as Pekko's PoisonPill

            def on_message(_ctx: ActorContext[_RegionCmd[T]], msg: _RegionCmd[T]) -> Behavior[_RegionCmd[T]]:
                match msg:
                    case ShardingEnvelope("", message):
                        ctx.log.warning("dropping message for empty entity id: %r", message)
                        ctx.system.dead_letter(message, ctx.self)
                    case ShardingEnvelope(entity_id, message):
                        buffer = passivating.get(entity_id)
                        if buffer is None:
                            deliver(entity_id, message)
                        elif len(buffer) < entity.buffer_size:
                            buffer.append(message)
                        else:
                            ctx.system.dead_letter(message, EntityRef(ctx.self, entity_id))
                    case GetShardRegionState(reply_to):
                        reply_to.tell(CurrentShardRegionState(frozenset(entities)))
                    case Passivate(ref):
                        entity_id = ids_by_ref.get(ref)
                        if entity_id is not None:
                            passivate(entity_id)
                    case _IdleTick():
                        assert entity.passivate_after is not None
                        cutoff = loop.time() - entity.passivate_after
                        for entity_id in [i for i, t in last_seen.items() if t <= cutoff]:
                            passivate(entity_id)
                    case _:
                        return Behaviors.unhandled
                return Behaviors.same

            def on_signal(_ctx: ActorContext[_RegionCmd[T]], sig: Signal) -> Behavior[_RegionCmd[T]]:
                if sig is PostStop:  # the region is going away: what was buffered for passivating entities is lost
                    for entity_id, buffered in passivating.items():
                        for message in buffered:
                            ctx.system.dead_letter(message, EntityRef(ctx.self, entity_id))
                    return Behaviors.same
                if isinstance(sig, Terminated):
                    entity_id = ids_by_ref.pop(sig.ref, None)
                    if entity_id is not None:
                        entities.pop(entity_id, None)
                        last_seen.pop(entity_id, None)  # the entity may have stopped on its own
                        for message in passivating.pop(entity_id, []):
                            deliver(entity_id, message)
                    return Behaviors.same
                return Behaviors.unhandled

            return Behaviors.receive(on_message).receive_signal(on_signal)

        # ShardCommand and _IdleTick are only ever sent by entities and the region itself, so the public type
        # omits them; Behavior is contravariant, so the narrower public type is a plain assignment.
        return Behaviors.setup(setup)

    @staticmethod
    def entity_ref_for[T](region: ActorRef[ShardingEnvelope[T]], entity_id: str) -> EntityRef[T]:
        if not entity_id:
            raise ValueError("entity id must not be empty")
        return EntityRef(region, entity_id)
