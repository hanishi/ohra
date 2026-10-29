"""Streams, as far as ohra has them: Apache Pekko's stream-typed ``ActorSource``."""

from .actor_source import ActorSource, BufferOverflowError, OverflowStrategy

__all__ = ["ActorSource", "BufferOverflowError", "OverflowStrategy"]
