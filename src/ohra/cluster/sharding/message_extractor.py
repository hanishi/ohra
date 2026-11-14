"""Pekko Typed's ShardingMessageExtractor (cluster-sharding-typed ShardingMessageExtractor.scala):
how a region finds the entity id, the shard id and the entity's message in what it is sent."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

from .shard import ShardingEnvelope, shard_id_of


class ShardingMessageExtractor[E, M](ABC):
    """Takes the region's messages ``E`` apart: the entity id, the shard of an entity id, and
    the message ``M`` the entity gets. A ``ShardingEnvelope`` and ``StartEntity`` sent to the
    region give their entity id themselves; ``shard_id`` places every message. It must give the
    same shard for an entity id on every node."""

    @abstractmethod
    def entity_id(self, message: E) -> str: ...

    @abstractmethod
    def shard_id(self, entity_id: str) -> str: ...

    @abstractmethod
    def unwrap_message(self, message: E) -> M: ...

    @staticmethod
    def no_envelope[N](
        number_of_shards: int, stop_message: N | None, extract_entity_id: Callable[[N], str]
    ) -> HashCodeNoEnvelopeMessageExtractor[N]:
        """An extractor for messages that carry their own entity id, sent to the region without
        an envelope. ``stop_message`` is unused, as in Pekko: set it with
        ``Entity.with_stop_message``."""
        del stop_message
        return _NoEnvelope(number_of_shards, extract_entity_id)


class HashCodeMessageExtractor[M](ShardingMessageExtractor[ShardingEnvelope[M], M]):
    """The default: messages come in a ``ShardingEnvelope``, and the shard of an entity id is
    ``shard_id_of(entity_id, number_of_shards)``, a CRC-32 that is the same on every node."""

    def __init__(self, number_of_shards: int) -> None:
        self.number_of_shards = number_of_shards

    def entity_id(self, message: ShardingEnvelope[M]) -> str:
        return message.entity_id

    def shard_id(self, entity_id: str) -> str:
        return shard_id_of(entity_id, self.number_of_shards)

    def unwrap_message(self, message: ShardingEnvelope[M]) -> M:
        return message.message


class HashCodeNoEnvelopeMessageExtractor[M](ShardingMessageExtractor[M, M]):
    """For messages that carry their own entity id: define ``entity_id``. The shard of an entity
    id is ``shard_id_of(entity_id, number_of_shards)``."""

    def __init__(self, number_of_shards: int) -> None:
        self.number_of_shards = number_of_shards

    def shard_id(self, entity_id: str) -> str:
        return shard_id_of(entity_id, self.number_of_shards)

    def unwrap_message(self, message: M) -> M:
        return message

    def __repr__(self) -> str:
        return f"HashCodeNoEnvelopeMessageExtractor({self.number_of_shards})"


class _NoEnvelope[M](HashCodeNoEnvelopeMessageExtractor[M]):
    def __init__(self, number_of_shards: int, extract_entity_id: Callable[[M], str]) -> None:
        super().__init__(number_of_shards)
        self._extract_entity_id = extract_entity_id

    def entity_id(self, message: M) -> str:
        return self._extract_entity_id(message)
