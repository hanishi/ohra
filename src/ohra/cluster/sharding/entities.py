"""Port of the entity state machine of Pekko's ``Shard`` (cluster-sharding/.../Shard.scala
133-412): ``EntityState`` and its states, and ``Entities``. Pekko's case objects are field-less
frozen dataclasses. Pekko's ``LoggingAdapter`` is a ``logging.Logger``; ``log.error(exception,
...)`` is ``log.error(..., exc_info=exception)``. ``IllegalArgumentException`` is ``ValueError``
and ``IllegalStateException`` is ``RuntimeError``.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, ClassVar

from ...ref import ActorRef


class EntityState(ABC):
    """Shard.scala 133-179 and 160-179: the state machine of one entity, in the diagram there."""

    @abstractmethod
    def transition(self, new_state: "EntityState", entities: "Entities") -> "EntityState": ...

    def invalid_transition(self, to: "EntityState", entities: "Entities") -> "EntityState":
        """Shard.scala 160-173: raises when ``fail_on_illegal_transition`` is set (the shard
        crashes), otherwise logs and takes the new state."""
        exception = ValueError(
            f"Transition from {self} to {to} not allowed, "
            f"remember entities: {entities.remembering_entities}"
        )
        if entities.fail_on_illegal_transition:
            raise exception
        entities.log.error("Ignoring illegal state transition in shard", exc_info=exception)
        return to


@dataclass(frozen=True)
class NoState(EntityState):
    """Shard.scala 181-193: never really kept, only used to verify transitions."""

    def transition(self, new_state: EntityState, entities: "Entities") -> EntityState:
        match new_state:
            case RememberedButNotCreated() if entities.remembering_entities:
                return new_state
            case RememberingStart():  # we go via this state even if not really remembering
                return new_state
            case Active() if not entities.remembering_entities:
                return new_state
            case _:
                return self.invalid_transition(new_state, entities)


@dataclass(frozen=True)
class RememberedButNotCreated(EntityState):
    """Shard.scala 195-205."""

    def transition(self, new_state: EntityState, entities: "Entities") -> EntityState:
        match new_state:
            case Active():  # started on this shard
                return new_state
            case RememberingStop():  # started on other shard
                return new_state
            case _:
                return self.invalid_transition(new_state, entities)


@dataclass(frozen=True)
class RememberingStart(EntityState):
    """Shard.scala 207-243 (``RememberingStart.empty`` is ``EMPTY``, ``apply(Option)`` is
    ``create``)."""

    ack_to: frozenset[ActorRef[Any]] = frozenset()

    EMPTY: ClassVar["RememberingStart"]

    @staticmethod
    def create(ack_to: ActorRef[Any] | None) -> "RememberingStart":
        """Shard.scala 212-220."""
        if ack_to is None:
            return RememberingStart.EMPTY
        return RememberingStart(frozenset((ack_to,)))

    def transition(self, new_state: EntityState, entities: "Entities") -> EntityState:
        match new_state:
            case Active():
                return new_state
            case RememberingStart() as r:
                if not self.ack_to:
                    return RememberingStart.EMPTY if not r.ack_to else new_state
                return self if not r.ack_to else RememberingStart(self.ack_to | r.ack_to)
            case _:
                return self.invalid_transition(new_state, entities)


RememberingStart.EMPTY = RememberingStart(frozenset())


@dataclass(frozen=True)
class RememberingStop(EntityState):
    """Shard.scala 245-257."""

    def transition(self, new_state: EntityState, entities: "Entities") -> EntityState:
        match new_state:
            case NoState():
                return new_state
            case _:
                return self.invalid_transition(new_state, entities)


class WithRef(EntityState):
    """Shard.scala 259-260: a state with an entity actor."""

    ref: ActorRef[Any]


@dataclass(frozen=True)
class Active(WithRef):
    """Shard.scala 261-267."""

    ref: ActorRef[Any]

    def transition(self, new_state: EntityState, entities: "Entities") -> EntityState:
        match new_state:
            case Passivating() | WaitingForRestart():
                return new_state
            case NoState() if not entities.remembering_entities:
                return new_state
            case _:
                return self.invalid_transition(new_state, entities)


@dataclass(frozen=True)
class Passivating(WithRef):
    """Shard.scala 268-273."""

    ref: ActorRef[Any]

    def transition(self, new_state: EntityState, entities: "Entities") -> EntityState:
        match new_state:
            case RememberingStop():
                return new_state
            case NoState() if not entities.remembering_entities:
                return new_state
            case _:
                return self.invalid_transition(new_state, entities)


@dataclass(frozen=True)
class WaitingForRestart(EntityState):
    """Shard.scala 275-279."""

    def transition(self, new_state: EntityState, entities: "Entities") -> EntityState:
        match new_state:
            case RememberingStart() | Active():
                return new_state
            case _:
                return self.invalid_transition(new_state, entities)


class Entities:
    """Shard.scala 281-412: the state of every entity of a shard, with a lookup by ref and the
    set of ids whose start or stop waits for the remember-entities store."""

    def __init__(
        self,
        log: logging.Logger,
        remembering_entities: bool,
        verbose_debug: bool,
        fail_on_illegal_transition: bool,
    ) -> None:
        self.log = log
        self.remembering_entities = remembering_entities
        self.fail_on_illegal_transition = fail_on_illegal_transition
        self._verbose_debug = verbose_debug
        self._entities: dict[str, EntityState] = {}
        # needed to look up entity by ref when a Passivating is received
        self._by_ref: dict[ActorRef[Any], str] = {}
        # optimization to not have to go through all entities to find batched writes
        self._remembering: set[str] = set()

    def already_remembered(self, ids: Iterable[str]) -> None:
        """Scala 292-297."""
        for entity_id in ids:
            self._entities[entity_id] = self.entity_state(entity_id).transition(
                RememberedButNotCreated(), self
            )

    def remembering_start(self, entity_id: str, ack_to: ActorRef[Any] | None) -> None:
        """Scala 298-304."""
        state = self.entity_state(entity_id).transition(RememberingStart.create(ack_to), self)
        self._entities[entity_id] = state
        if self.remembering_entities:
            self._remembering.add(entity_id)

    def remembering_stop(self, entity_id: str) -> None:
        """Scala 305-310."""
        state = self.entity_state(entity_id)
        self._remove_ref_if_there_is_one(state)
        self._entities[entity_id] = state.transition(RememberingStop(), self)
        if self.remembering_entities:
            self._remembering.add(entity_id)

    def waiting_for_restart(self, entity_id: str) -> None:
        """Scala 311-319."""
        state = self._entities.get(entity_id, NoState())
        if isinstance(state, WithRef):
            self._by_ref.pop(state.ref, None)
        self._entities[entity_id] = state.transition(WaitingForRestart(), self)

    def remove_entity(self, entity_id: str) -> None:
        """Scala 320-328."""
        state = self.entity_state(entity_id)
        state.transition(NoState(), self)  # just verify transition
        self._remove_ref_if_there_is_one(state)
        self._entities.pop(entity_id, None)
        if self.remembering_entities:
            self._remembering.discard(entity_id)

    def add_entity(self, entity_id: str, ref: ActorRef[Any]) -> None:
        """Scala 329-335."""
        self._entities[entity_id] = self.entity_state(entity_id).transition(Active(ref), self)
        self._by_ref[ref] = entity_id
        if self.remembering_entities:
            self._remembering.discard(entity_id)

    def entity(self, entity_id: str) -> ActorRef[Any] | None:
        """Scala 336-339."""
        state = self._entities.get(entity_id)
        return state.ref if isinstance(state, WithRef) else None

    def entity_state(self, entity_id: str) -> EntityState:
        """Scala 341-345."""
        return self._entities.get(entity_id, NoState())

    def entity_id(self, ref: ActorRef[Any]) -> str | None:
        """Scala 347."""
        return self._by_ref.get(ref)

    def is_passivating(self, entity_id: str) -> bool:
        """Scala 349-354."""
        return isinstance(self._entities.get(entity_id), Passivating)

    def entity_passivating(self, entity_id: str) -> None:
        """Scala 356-367: raises ``RuntimeError`` on a state without a ref."""
        if self._verbose_debug:
            self.log.debug("[%s] passivating", entity_id)
        state = self._entities.get(entity_id)
        if not isinstance(state, WithRef):
            raise RuntimeError(
                f"Tried to passivate entity without an actor ref {entity_id}. "
                f"Current state {state}"
            )
        self._entities[entity_id] = self.entity_state(entity_id).transition(
            Passivating(state.ref), self
        )

    def _remove_ref_if_there_is_one(self, state: EntityState) -> None:
        """Scala 368-373."""
        if isinstance(state, WithRef):
            self._by_ref.pop(state.ref, None)

    def active_entities(self) -> frozenset[ActorRef[Any]]:
        """Scala 378: only called once during handoff."""
        return frozenset(self._by_ref)

    def nr_active_entities(self) -> int:
        """Scala 380."""
        return len(self._by_ref)

    def active_entity_ids(self) -> frozenset[str]:
        """Scala 383: only called for getting shard stats."""
        return frozenset(self._by_ref.values())

    def pending_remember_entities(self) -> tuple[dict[str, RememberingStart], frozenset[str]]:
        """Scala 385-402: (remembering start, remembering stop)."""
        starts: dict[str, RememberingStart] = {}
        stops: set[str] = set()
        for entity_id in self._remembering:
            match self.entity_state(entity_id):
                case RememberingStart() as r:
                    starts[entity_id] = r
                case RememberingStop():
                    stops.add(entity_id)
                case wat:
                    raise RuntimeError(
                        f"{entity_id} was in the remembering set but has state {wat}"
                    )
        return starts, frozenset(stops)

    def pending_remembered_entities_exist(self) -> bool:
        """Scala 404."""
        return bool(self._remembering)

    def entity_id_exists(self, entity_id: str) -> bool:
        """Scala 406."""
        return entity_id in self._entities

    @property
    def size(self) -> int:
        """Scala 409."""
        return len(self._entities)

    def __repr__(self) -> str:
        return repr(self._entities)
