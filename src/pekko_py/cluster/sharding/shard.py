"""Sharding: one entity actor per id, created on its first message, passivated when idle or on
request.

Apache Pekko's layering: a region routes each message by shard id to a Shard actor, which hosts the
entities of that shard and passivates them. ``Sharding.init`` is the region inside one process;
``ClusterSharding`` in ``pekko_py.cluster.sharding`` runs the same Shards under regions that span
the cluster. Messages for a passivating entity are buffered and delivered to its next incarnation;
an entity that stops on its own, by returning ``Behaviors.stopped()``, loses what is queued behind
it, as in Pekko, so entities should passivate rather than stop themselves.
"""

from __future__ import annotations

import asyncio
import zlib
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from ...behavior import Behavior, Behaviors
from ...context import ActorContext
from ...ref import ActorRef
from ...remote.serialization import remotable
from ...signals import PostStop, Signal, Terminated

if TYPE_CHECKING:
    from ..singleton import ClusterSingletonSettings


def _child_name(entity_id: str) -> str:
    """Any id becomes a valid, unique child name. Pure-dot ids are escaped so paths never read as
    '.' or '..'."""
    name = quote(entity_id, safe="")
    return name.replace(".", "%2E") if set(name) == {"."} else name


def shard_id_of(entity_id: str, number_of_shards: int) -> str:
    """Pekko's HashCodeMessageExtractor with a hash that is the same on every node."""
    return str(zlib.crc32(entity_id.encode()) % number_of_shards)


@dataclass(frozen=True)
class ClusterShardingSettings:
    """Pekko's cluster sharding settings with their defaults, in seconds. The single-node region
    uses ``number_of_shards``, ``buffer_size``, ``passivate_after``, ``handoff_timeout`` and
    ``shard_region_query_timeout`` too."""

    number_of_shards: int = 1000
    retry_interval: float = 2.0
    rebalance_interval: float = 10.0
    handoff_timeout: float = 60.0
    shard_start_timeout: float = 10.0
    coordinator_failure_backoff: float = 5.0
    buffer_size: int = 100_000
    rebalance_absolute_limit: int = 0
    """0 means no absolute cap, as Pekko."""
    rebalance_relative_limit: float = 0.1
    passivate_after: float | None = None
    shard_region_query_timeout: float = 3.0
    coordinator_singleton_settings: ClusterSingletonSettings | None = None
    """Settings of the coordinator's singleton manager (Pekko's coordinator-singleton); None means
    the defaults."""

    def __post_init__(self) -> None:
        for name in (
            "retry_interval", "rebalance_interval", "handoff_timeout", "shard_start_timeout",
            "coordinator_failure_backoff", "shard_region_query_timeout",
        ):  # fmt: skip
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if self.number_of_shards < 1 or self.buffer_size < 1:
            raise ValueError("number_of_shards and buffer_size must be >= 1")
        if self.rebalance_absolute_limit < 0:
            raise ValueError("rebalance_absolute_limit must be >= 0")
        if not 0.0 < self.rebalance_relative_limit <= 1.0:
            raise ValueError("rebalance_relative_limit must be in (0, 1]")
        if self.passivate_after is not None and self.passivate_after <= 0:
            raise ValueError("passivate_after must be > 0 or None")

    @property
    def entity_hand_off_timeout(self) -> float:
        """What a Shard gives its entities to stop on hand-off, as Pekko's entityHandOffTimeout."""
        return max(self.handoff_timeout - 5.0, 1.0)


@dataclass(frozen=True)
class EntityTypeKey[T]:
    """Names an entity type; the type parameter is the entity's message type."""

    name: str


class ShardCommand:
    """Messages an entity may send to its shard (``EntityContext.shard``)."""

    __slots__ = ()


@dataclass(frozen=True)
class Passivate(ShardCommand):
    """Sent by an entity to ask the shard to stop it. Messages meanwhile are buffered for the next
    incarnation."""

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
    """The live entity ids, and the shard ids this region hosts (a moved shard may hold no entity
    yet)."""

    entity_ids: frozenset[str]
    shard_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class GetShardRegionState:
    reply_to: ActorRef[CurrentShardRegionState]


@dataclass(frozen=True)
class CurrentShardState:
    shard: str
    entity_ids: frozenset[str]


@dataclass(frozen=True)
class GetShardState:
    reply_to: ActorRef[CurrentShardState]


@remotable
@dataclass(frozen=True)
class ShardStopped:
    shard: str


@remotable
@dataclass(frozen=True)
class HandOff:
    """Stop this shard and every entity in it, then answer ``ShardStopped``; a rebalance or a
    leaving node."""

    shard: str
    reply_to: ActorRef[ShardStopped]


@dataclass(frozen=True)
class Entity[T]:
    """How to create an entity of a type, plus its passivation settings."""

    type_key: EntityTypeKey[T]
    create_behavior: Callable[[EntityContext[T]], Behavior[T]]
    stop_message: T | None = None
    passivate_after: float | None = None
    # messages kept for a passivating entity; beyond this they are dead letters
    buffer_size: int = 100_000
    settings: ClusterShardingSettings | None = None

    def with_buffer_size(self, size: int) -> Entity[T]:
        """Cap on messages buffered while an entity passivates, as Pekko's sharding buffer-size
        setting."""
        if self.settings is not None:
            return replace(
                self, buffer_size=size, settings=replace(self.settings, buffer_size=size)
            )
        return replace(self, buffer_size=size)

    def with_stop_message(self, msg: T) -> Entity[T]:
        """Passivation delivers msg and lets the entity stop itself, instead of stopping it
        outright."""
        return replace(self, stop_message=msg)

    def with_passivate_after(self, seconds: float) -> Entity[T]:
        """Stop an entity that has received no message for this long. Not in Pekko, which uses
        settings."""
        if self.settings is not None:
            return replace(
                self,
                passivate_after=seconds,
                settings=replace(self.settings, passivate_after=seconds),
            )
        return replace(self, passivate_after=seconds)

    def with_settings(self, settings: ClusterShardingSettings) -> Entity[T]:
        return replace(self, settings=settings)

    @property
    def effective_settings(self) -> ClusterShardingSettings:
        if self.settings is not None:
            return self.settings
        return ClusterShardingSettings(
            passivate_after=self.passivate_after, buffer_size=self.buffer_size
        )


class EntityRef[T](ActorRef[T]):
    """A ref to an entity by id. The entity need not exist yet: the region creates it on the first
    message."""

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
        return isinstance(other, EntityRef) and (self.path, self.entity_id) == (
            other.path,
            other.entity_id,
        )

    def __hash__(self) -> int:
        return hash((self._region.path, self.entity_id))


@dataclass(frozen=True)
class _IdleTick:
    pass


@dataclass(frozen=True)
class _HandOffTimeout:
    pass


@dataclass(frozen=True)
class _QueryTimeout:
    pass


type _ShardCmd[T] = (
    ShardingEnvelope[T] | ShardCommand | HandOff | GetShardState | _IdleTick | _HandOffTimeout
)


def shard_behavior[T](
    entity: Entity[T], shard_id: str, region: ActorRef[ShardingEnvelope[T]]
) -> Behavior[_ShardCmd[T]]:
    """Pekko's Shard: hosts the entities of one shard id. ``region`` names the entities' logical
    refs."""
    settings = entity.effective_settings

    def setup(ctx: ActorContext[_ShardCmd[T]]) -> Behavior[_ShardCmd[T]]:
        entities: dict[str, ActorRef[T]] = {}
        # LocalRef hashes by identity: a stale incarnation misses
        ids_by_ref: dict[ActorRef[T], str] = {}
        # messages buffered until the entity's Terminated arrives
        passivating: dict[str, list[T]] = {}
        last_seen: dict[str, float] = {}
        hand_off_to: ActorRef[ShardStopped] | None = None
        loop = asyncio.get_running_loop()
        # one tick for the shard, as Pekko, instead of a timer per message
        if settings.passivate_after is not None:
            ctx.timers.start_timer_with_fixed_delay(
                "idle", _IdleTick(), settings.passivate_after / 2
            )

        def start(entity_id: str) -> ActorRef[T]:
            entity_ctx = EntityContext(entity.type_key, entity_id, ctx.self)
            # Created inside the child's own setup, so a failing factory fails that entity, not the
            # shard.
            behavior: Behavior[T] = Behaviors.setup(
                lambda _ctx: entity.create_behavior(entity_ctx)
            )
            ref = ctx.spawn(behavior, _child_name(entity_id))
            ctx.watch(ref)
            entities[entity_id] = ref
            ids_by_ref[ref] = entity_id
            return ref

        def deliver(entity_id: str, message: T) -> None:
            (entities.get(entity_id) or start(entity_id)).tell(message)
            last_seen[entity_id] = loop.time()

        def stop_entity(ref: ActorRef[T]) -> None:
            if entity.stop_message is not None:
                ref.tell(entity.stop_message)
            else:
                # after what the shard already forwarded, as Pekko's PoisonPill
                ctx.stop(ref, after_pending=True)

        def passivate(entity_id: str) -> None:
            ref = entities.get(entity_id)
            if ref is None or entity_id in passivating:
                # unknown, or already stopping: a duplicate Passivate must not touch the next
                # incarnation
                return
            passivating[entity_id] = []
            last_seen.pop(entity_id, None)
            stop_entity(ref)

        def hand_off_done() -> Behavior[_ShardCmd[T]]:
            assert hand_off_to is not None
            hand_off_to.tell(ShardStopped(shard_id))
            return Behaviors.stopped()

        def on_message(
            _ctx: ActorContext[_ShardCmd[T]], msg: _ShardCmd[T]
        ) -> Behavior[_ShardCmd[T]]:
            nonlocal hand_off_to
            if hand_off_to is not None:
                match msg:
                    case _HandOffTimeout():
                        # stragglers: stopped outright, as Pekko
                        for ref in list(entities.values()):
                            ctx.stop(ref)
                    case ShardingEnvelope(entity_id, message):
                        ctx.system.dead_letter(message, EntityRef(region, entity_id))
                    case GetShardState(reply_to):
                        reply_to.tell(CurrentShardState(shard_id, frozenset(entities)))
                    case _:
                        pass
                return Behaviors.same
            match msg:
                case ShardingEnvelope("", message):
                    ctx.log.warning("dropping message for empty entity id: %r", message)
                    ctx.system.dead_letter(message, ctx.self)
                case ShardingEnvelope(entity_id, message):
                    buffer = passivating.get(entity_id)
                    if buffer is None:
                        deliver(entity_id, message)
                    elif len(buffer) < settings.buffer_size:
                        buffer.append(message)
                    else:
                        ctx.system.dead_letter(message, EntityRef(region, entity_id))
                case GetShardState(reply_to):
                    reply_to.tell(CurrentShardState(shard_id, frozenset(entities)))
                case Passivate(ref):
                    entity_id = ids_by_ref.get(ref)
                    if entity_id is not None:
                        passivate(entity_id)
                case HandOff(_, reply_to):
                    hand_off_to = reply_to
                    # what waited for a next incarnation is lost
                    for entity_id, buffered in passivating.items():
                        for message in buffered:
                            ctx.system.dead_letter(message, EntityRef(region, entity_id))
                    passivating.clear()
                    if not entities:
                        return hand_off_done()
                    for ref in list(entities.values()):
                        stop_entity(ref)
                    ctx.timers.start_single_timer(
                        "hand-off", _HandOffTimeout(), settings.entity_hand_off_timeout
                    )
                case _IdleTick():
                    assert settings.passivate_after is not None
                    cutoff = loop.time() - settings.passivate_after
                    for entity_id in [i for i, t in last_seen.items() if t <= cutoff]:
                        passivate(entity_id)
                case _:
                    return Behaviors.unhandled
            return Behaviors.same

        def on_signal(_ctx: ActorContext[_ShardCmd[T]], sig: Signal) -> Behavior[_ShardCmd[T]]:
            # the shard is going away: what was buffered for passivating entities is lost
            if sig is PostStop:
                for entity_id, buffered in passivating.items():
                    for message in buffered:
                        ctx.system.dead_letter(message, EntityRef(region, entity_id))
                return Behaviors.same
            if isinstance(sig, Terminated):
                entity_id = ids_by_ref.pop(sig.ref, None)
                if entity_id is not None:
                    entities.pop(entity_id, None)
                    last_seen.pop(entity_id, None)  # the entity may have stopped on its own
                    if hand_off_to is not None:
                        if not entities:
                            return hand_off_done()
                    else:
                        for message in passivating.pop(entity_id, []):
                            deliver(entity_id, message)
                return Behaviors.same
            return Behaviors.unhandled

        return Behaviors.receive(on_message).receive_signal(on_signal)

    return Behaviors.setup(setup)


def _aggregate_shard_states(
    shards: list[ActorRef[Any]], reply_to: ActorRef[CurrentShardRegionState], timeout: float
) -> Behavior[CurrentShardState | _QueryTimeout]:
    """Pekko's region query: ask every Shard, reply the union when all have answered or at the
    timeout."""

    def setup(
        ctx: ActorContext[CurrentShardState | _QueryTimeout],
    ) -> Behavior[CurrentShardState | _QueryTimeout]:
        ids: set[str] = set()
        shard_ids: set[str] = set()
        remaining = len(shards)
        for shard in shards:
            shard.tell(GetShardState(ctx.self))
        ctx.timers.start_single_timer("timeout", _QueryTimeout(), timeout)

        def on_message(
            _ctx: ActorContext[Any], msg: CurrentShardState | _QueryTimeout
        ) -> Behavior[Any]:
            nonlocal remaining
            if isinstance(msg, CurrentShardState):
                ids.update(msg.entity_ids)
                shard_ids.add(msg.shard)
                remaining -= 1
                if remaining > 0:
                    return Behaviors.same
            reply_to.tell(CurrentShardRegionState(frozenset(ids), frozenset(shard_ids)))
            return Behaviors.stopped()

        return Behaviors.receive(on_message)

    return Behaviors.setup(setup)


type _RegionCmd[T] = ShardingEnvelope[T] | GetShardRegionState


class Sharding:
    @staticmethod
    def init[T](entity: Entity[T]) -> Behavior[ShardingEnvelope[T] | GetShardRegionState]:
        """The shard region behavior for one entity type, inside one process. Spawn it; message it
        through entity refs. Each shard id gets a child Shard actor on first use."""
        settings = entity.effective_settings

        def setup(ctx: ActorContext[_RegionCmd[T]]) -> Behavior[_RegionCmd[T]]:
            shards: dict[str, ActorRef[_ShardCmd[T]]] = {}
            ids_by_ref: dict[ActorRef[Any], str] = {}

            def shard_for(entity_id: str) -> ActorRef[_ShardCmd[T]]:
                shard_id = shard_id_of(entity_id, settings.number_of_shards)
                shard = shards.get(shard_id)
                if shard is None:
                    shard = ctx.spawn(shard_behavior(entity, shard_id, ctx.self), shard_id)
                    ctx.watch(shard)
                    shards[shard_id] = shard
                    ids_by_ref[shard] = shard_id
                return shard

            def on_message(
                _ctx: ActorContext[_RegionCmd[T]], msg: _RegionCmd[T]
            ) -> Behavior[_RegionCmd[T]]:
                match msg:
                    case ShardingEnvelope("", message):
                        ctx.log.warning("dropping message for empty entity id: %r", message)
                        ctx.system.dead_letter(message, ctx.self)
                    case ShardingEnvelope(entity_id, _):
                        shard_for(entity_id).tell(msg)
                    case GetShardRegionState(reply_to):
                        if not shards:
                            reply_to.tell(CurrentShardRegionState(frozenset()))
                        else:
                            ctx.spawn_anonymous(
                                _aggregate_shard_states(
                                    list(shards.values()),
                                    reply_to,
                                    settings.shard_region_query_timeout,
                                )
                            )
                return Behaviors.same

            def on_signal(
                _ctx: ActorContext[_RegionCmd[T]], sig: Signal
            ) -> Behavior[_RegionCmd[T]]:
                if isinstance(sig, Terminated):
                    shard_id = ids_by_ref.pop(sig.ref, None)
                    if shard_id is not None:
                        shards.pop(shard_id, None)
                    return Behaviors.same
                return Behaviors.unhandled

            return Behaviors.receive(on_message).receive_signal(on_signal)

        return Behaviors.setup(setup)

    @staticmethod
    def entity_ref_for[T](region: ActorRef[ShardingEnvelope[T]], entity_id: str) -> EntityRef[T]:
        if not entity_id:
            raise ValueError("entity id must not be empty")
        return EntityRef(region, entity_id)
