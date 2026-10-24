"""Reachability, as Pekko's: each node records what it observes about others, with a version per observer,
and the table merges by taking the newer row set per observer."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Literal

from ..remote import UniqueAddress
from ..remote.serialization import remotable

type ReachabilityStatus = Literal["reachable", "unreachable", "terminated"]


@remotable
@dataclass(frozen=True)
class Record:
    observer: UniqueAddress
    subject: UniqueAddress
    status: ReachabilityStatus
    version: int


@remotable
@dataclass(frozen=True)
class Reachability:
    records: tuple[Record, ...] = ()
    versions: tuple[tuple[UniqueAddress, int], ...] = ()

    @staticmethod
    def empty() -> Reachability:
        return Reachability()

    # --- derived views ----------------------------------------------------------------------------

    @cached_property
    def _observer_rows(self) -> dict[UniqueAddress, dict[UniqueAddress, Record]]:
        rows: dict[UniqueAddress, dict[UniqueAddress, Record]] = {}
        for r in self.records:
            rows.setdefault(r.observer, {})[r.subject] = r
        return rows

    @cached_property
    def _all_terminated(self) -> frozenset[UniqueAddress]:
        return frozenset(r.subject for r in self.records if r.status == "terminated")

    @cached_property
    def all_unreachable(self) -> frozenset[UniqueAddress]:
        """Observed Unreachable by at least one node, and not Terminated by any."""
        return frozenset(r.subject for r in self.records if r.status == "unreachable") - self._all_terminated

    @property
    def all_unreachable_or_terminated(self) -> frozenset[UniqueAddress]:
        return self.all_unreachable | self._all_terminated

    def version_of(self, observer: UniqueAddress) -> int:
        return dict(self.versions).get(observer, 0)

    # --- observations -----------------------------------------------------------------------------

    def unreachable(self, observer: UniqueAddress, subject: UniqueAddress) -> Reachability:
        return self._change(observer, subject, "unreachable")

    def reachable(self, observer: UniqueAddress, subject: UniqueAddress) -> Reachability:
        return self._change(observer, subject, "reachable")

    def terminated(self, observer: UniqueAddress, subject: UniqueAddress) -> Reachability:
        return self._change(observer, subject, "terminated")

    def _change(self, observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus) -> Reachability:
        version = self.version_of(observer) + 1
        new_versions = tuple((o, version if o == observer else v) for o, v in self.versions)
        if observer not in dict(self.versions):
            new_versions = (*new_versions, (observer, version))
        record = Record(observer, subject, status, version)
        rows = self._observer_rows.get(observer)
        if rows is None:
            if status == "reachable":
                return self  # nothing noted so far: a Reachable observation is the default
            return Reachability((*self.records, record), new_versions)
        old = rows.get(subject)
        if old is None:
            if status == "reachable" and all(r.status == "reachable" for r in rows.values()):
                return Reachability(tuple(r for r in self.records if r.observer != observer), new_versions)
            return Reachability((*self.records, record), new_versions)
        if old.status == "terminated" or old.status == status:
            return self
        if status == "reachable" and all(r.status == "reachable" or r.subject == subject for r in rows.values()):
            # all Reachable again: drop the observer's rows entirely, and bump the version
            return Reachability(tuple(r for r in self.records if r.observer != observer), new_versions)
        return Reachability(tuple(record if r is old else r for r in self.records), new_versions)

    # --- merging and pruning ----------------------------------------------------------------------

    def merge(self, allowed: frozenset[UniqueAddress], other: Reachability) -> Reachability:
        """Per observer, keep the row set of whichever side has the higher version; drop nodes not in allowed."""
        records: list[Record] = []
        versions = dict(self.versions)
        for observer in allowed:
            v1, v2 = self.version_of(observer), other.version_of(observer)
            rows1, rows2 = self._observer_rows.get(observer), other._observer_rows.get(observer)
            if rows1 is not None and rows2 is not None:
                rows = rows1 if v1 > v2 else rows2
                records.extend(r for r in rows.values() if r.subject in allowed)
            elif rows1 is not None and v1 > v2:
                records.extend(r for r in rows1.values() if r.subject in allowed)
            elif rows2 is not None and v2 > v1:
                records.extend(r for r in rows2.values() if r.subject in allowed)
            if v2 > v1:
                versions[observer] = v2
        return Reachability(tuple(records), tuple((o, v) for o, v in versions.items() if o in allowed))

    def remove(self, nodes: Iterable[UniqueAddress]) -> Reachability:
        gone = set(nodes)
        return Reachability(
            tuple(r for r in self.records if r.observer not in gone and r.subject not in gone),
            tuple((o, v) for o, v in self.versions if o not in gone),
        )

    def remove_observers(self, nodes: frozenset[UniqueAddress]) -> Reachability:
        if not nodes:
            return self
        return Reachability(
            tuple(r for r in self.records if r.observer not in nodes),
            tuple((o, v) for o, v in self.versions if o not in nodes),
        )

    # --- queries ----------------------------------------------------------------------------------

    def status(self, observer: UniqueAddress, subject: UniqueAddress | None = None) -> ReachabilityStatus:
        """status(observer, subject): one observation. status(node): the aggregate over all observers."""
        if subject is None:
            node = observer
            if node in self._all_terminated:
                return "terminated"
            return "unreachable" if node in self.all_unreachable else "reachable"
        record = self._observer_rows.get(observer, {}).get(subject)
        return "reachable" if record is None else record.status

    def is_reachable(self, observer: UniqueAddress, subject: UniqueAddress | None = None) -> bool:
        if subject is None:
            return self.is_all_reachable or observer not in self.all_unreachable_or_terminated
        return self.status(observer, subject) == "reachable"

    @property
    def is_all_reachable(self) -> bool:
        return not self.records

    def all_unreachable_from(self, observer: UniqueAddress) -> frozenset[UniqueAddress]:
        return frozenset(s for s, r in self._observer_rows.get(observer, {}).items() if r.status == "unreachable")

    @property
    def all_observers(self) -> frozenset[UniqueAddress]:
        return frozenset(r.observer for r in self.records)

    def records_from(self, observer: UniqueAddress) -> tuple[Record, ...]:
        return tuple(self._observer_rows.get(observer, {}).values())
