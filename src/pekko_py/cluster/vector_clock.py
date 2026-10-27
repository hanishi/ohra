"""VectorClock, as Pekko's: one counter per node, compared causally."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from ..remote.serialization import remotable

type Ordering = Literal["same", "before", "after", "concurrent"]


@remotable
@dataclass(frozen=True)
class VectorClock:
    versions: dict[str, int] = field(default_factory=lambda: {})

    def increment(self, node: str) -> VectorClock:
        return VectorClock({**self.versions, node: self.versions.get(node, 0) + 1})

    def compare(self, other: VectorClock) -> Ordering:
        """Where this clock stands relative to other: same, before, after, or concurrent."""
        if self.versions == other.versions:
            return "same"
        after = before = False
        for node in self.versions.keys() | other.versions.keys():
            mine, theirs = self.versions.get(node, 0), other.versions.get(node, 0)
            if mine > theirs:
                after = True
            elif mine < theirs:
                before = True
            if after and before:
                return "concurrent"
        return "after" if after else "before"

    def merge(self, other: VectorClock) -> VectorClock:
        merged = dict(other.versions)
        for node, time in self.versions.items():
            if time > merged.get(node, 0):
                merged[node] = time
        return VectorClock(merged)

    def prune(self, node: str) -> VectorClock:
        if node not in self.versions:
            return self
        return VectorClock({k: v for k, v in self.versions.items() if k != node})
