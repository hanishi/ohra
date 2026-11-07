"""Pekko's FastMerge.scala. INTERNAL API.

Optimization for add/remove followed by merge: merge should just fast forward to the new
instance. The "updating" methods and ``reset_delta`` set the new instance's ancestor, and merge
checks it. The ancestor is not a dataclass field, so it is never sent and never compared."""

from __future__ import annotations

from typing import Self


class FastMerge:
    _ancestor: FastMerge | None = None

    def _assign_ancestor[S: FastMerge](self, new_data: S) -> S:
        """Should be called from "updating" methods, and ``reset_delta``."""
        object.__setattr__(
            new_data, "_ancestor", self if self._ancestor is None else self._ancestor
        )
        object.__setattr__(self, "_ancestor", None)  # only one level, for GC
        return new_data

    def _is_ancestor_of(self, that: FastMerge) -> bool:
        """Should be used from merge."""
        return that._ancestor is self

    def _clear_ancestor(self) -> Self:
        """Should be called from merge."""
        object.__setattr__(self, "_ancestor", None)
        return self
