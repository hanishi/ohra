"""Pekko's ``PruningState`` (``@InternalApi``): what a replica holds for a removed node's data.

``PruningInitialized`` is owned by the node that started pruning and collects the nodes that have
seen it. ``PruningPerformed`` says the data was pruned, until ``obsolete_time`` (seconds since the
epoch). Pekko's ``estimatedSize`` is left out: it only feeds size logging.
"""

from __future__ import annotations

from dataclasses import dataclass

from ...remote import Address, UniqueAddress
from ...remote.serialization import remotable


class PruningState:
    def merge(self, that: PruningState) -> PruningState:
        match (self, that):
            case (PruningPerformed() as p1, PruningPerformed() as p2):
                return self if p1.obsolete_time >= p2.obsolete_time else that
            case (PruningPerformed(), _):
                return self
            case (_, PruningPerformed()):
                return that
            case (PruningInitialized() as p1, PruningInitialized() as p2):
                if p1.owner == p2.owner:
                    return PruningInitialized(p1.owner, p1.seen | p2.seen)
                return that if p2.owner.address < p1.owner.address else self
            case _:
                raise TypeError(f"cannot merge {self!r} with {that!r}")

    def add_seen(self, node: Address) -> PruningState:
        raise NotImplementedError


@remotable
@dataclass(frozen=True)
class PruningInitialized(PruningState):
    owner: UniqueAddress
    seen: frozenset[Address]

    def add_seen(self, node: Address) -> PruningState:
        if node in self.seen or self.owner.address == node:
            return self
        return PruningInitialized(self.owner, self.seen | {node})


@remotable
@dataclass(frozen=True)
class PruningPerformed(PruningState):
    obsolete_time: float

    def is_obsolete(self, now: float) -> bool:
        return self.obsolete_time <= now

    def add_seen(self, node: Address) -> PruningState:
        return self
