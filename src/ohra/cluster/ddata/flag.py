"""Pekko's Flag.scala: a boolean flag CRDT that is initialized to ``False`` and can be switched
to ``True``. ``True`` wins over ``False`` in merge.

Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, override

from ...remote.serialization import remotable
from .key import Key
from .replicated_data import ReplicatedData


@remotable
@dataclass(frozen=True)
class Flag(ReplicatedData):
    empty: ClassVar[Flag]
    Disabled: ClassVar[Flag]
    Enabled: ClassVar[Flag]

    enabled: bool = False

    def switch_on(self) -> Flag:
        return self if self.enabled else Flag.Enabled

    @override
    def merge(self, that: Flag) -> Flag:  # pyright: ignore[reportIncompatibleMethodOverride]
        return that if that.enabled else self


Flag.empty = Flag(False)
Flag.Disabled = Flag.empty
Flag.Enabled = Flag(True)


@remotable
@dataclass(frozen=True, eq=False)
class FlagKey(Key[Flag]):
    @override
    def with_id(self, new_id: str) -> FlagKey:
        return FlagKey(new_id)
