"""Apache Pekko's split-brain resolver: a downing provider that waits until membership and reachability have been
stable for ``stable_after`` and then downs one side of a partition by a strategy. Ported from
``cluster/.../sbr/{SplitBrainResolver,DowningStrategy,SplitBrainResolverProvider}.scala`` without roles, data
centers, the lease-majority strategy and quarantine."""
from __future__ import annotations

from dataclasses import dataclass

from ..remote import Address, UniqueAddress
from .gossip import Member
from .reachability import Reachability, Record

__all__ = [
    "DownAll",
    "DowningStrategy",
    "KeepMajority",
    "KeepOldest",
    "SplitBrainResolverSettings",
    "StaticQuorum",
    "_DowningStrategy",
]

# --- settings and strategy values ------------------------------------------------------------------------------


@dataclass(frozen=True)
class KeepMajority:
    """Down the unreachable side when this side is the majority, else this side; ties keep the lowest address."""


@dataclass(frozen=True)
class StaticQuorum:
    """Keep this side when it has at least ``quorum_size`` members, else down it."""

    quorum_size: int

    def __post_init__(self) -> None:
        if self.quorum_size < 1:
            raise ValueError("quorum_size must be >= 1")


@dataclass(frozen=True)
class KeepOldest:
    """Keep the side with the oldest member; with ``down_if_alone`` an oldest partitioned from everyone downs itself."""

    down_if_alone: bool = True


@dataclass(frozen=True)
class DownAll:
    """Down every node unconditionally."""


type DowningStrategy = KeepMajority | StaticQuorum | KeepOldest | DownAll


@dataclass(frozen=True)
class SplitBrainResolverSettings:
    """Pekko's ``split-brain-resolver`` settings, in seconds. ``down_all_when_unstable=None`` is Pekko's ``on``:
    three quarters of ``stable_after`` but at least four seconds; ``0`` is ``off``."""

    strategy: DowningStrategy = KeepMajority()
    stable_after: float = 20.0
    down_all_when_unstable: float | None = None

    def __post_init__(self) -> None:
        if self.stable_after <= 0:
            raise ValueError("stable_after must be > 0")
        if self.down_all_when_unstable is not None and self.down_all_when_unstable < 0:
            raise ValueError("down_all_when_unstable must be >= 0 or None")

    @property
    def effective_down_all_when_unstable(self) -> float:
        if self.down_all_when_unstable is None:
            return max(4.0, 0.75 * self.stable_after)
        return self.down_all_when_unstable

    @property
    def tick_interval(self) -> float:
        return min(1.0, self.stable_after / 4)


# --- decisions ------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DownUnreachable: ...


@dataclass(frozen=True)
class DownReachable: ...


@dataclass(frozen=True)
class DownAllDecision: ...


@dataclass(frozen=True)
class DownIndirectlyConnected: ...


type Decision = DownUnreachable | DownReachable | DownAllDecision | DownIndirectlyConnected


class _DoubleDecision(Exception):
    """Pekko's IllegalStateException: the decision without the indirectly connected is itself indirectly connected."""


_DOWN_OR_EXITING = frozenset({"down", "exiting"})


# --- the strategy state (DowningStrategy.scala) ---------------------------------------------------------------------


class _DowningStrategy:
    """Pekko's abstract DowningStrategy with its concrete ``decide`` chosen by ``config``: the members, the
    unreachable set, the reachability table and the seen set, as the resolver feeds them in."""

    def __init__(self, self_unique_address: UniqueAddress, config: DowningStrategy) -> None:
        self.self_unique_address = self_unique_address
        self.config = config
        self._unreachable: set[UniqueAddress] = set()
        self._reachability = Reachability.empty()
        self._seen_by: frozenset[Address] = frozenset()
        self._all_members: list[Member] = []  # sorted: by address, or by age for KeepOldest (Pekko's ageOrdering)

    # --- members --------------------------------------------------------------------------------------------------

    def _key(self, m: Member) -> tuple[int, Address] | Address:
        if isinstance(self.config, KeepOldest):
            return (m.up_number, m.address)
        return m.address

    @property
    def all_members(self) -> list[Member]:
        return list(self._all_members)

    def add(self, m: Member) -> None:
        self._remove_from_all_members(m)
        key = self._key(m)
        i = 0
        while i < len(self._all_members) and self._key(self._all_members[i]) < key:  # type: ignore[operator]
            i += 1
        self._all_members.insert(i, m)

    def remove(self, m: Member) -> None:
        self._remove_from_all_members(m)
        self._unreachable.discard(m.unique_address)

    def _remove_from_all_members(self, m: Member) -> None:
        self._all_members = [x for x in self._all_members if x.unique_address != m.unique_address]

    def add_unreachable(self, m: Member) -> None:
        self.add(m)
        self._unreachable.add(m.unique_address)

    def add_reachable(self, m: Member) -> None:
        self.add(m)
        self._unreachable.discard(m.unique_address)

    @property
    def unreachable(self) -> frozenset[UniqueAddress]:
        return frozenset(self._unreachable)

    def is_unreachable(self, m: Member) -> bool:
        return m.unique_address in self._unreachable

    @property
    def joining(self) -> list[Member]:
        return [m for m in self._all_members if m.status in ("joining", "weakly_up")]

    def members(self, including_possibly_up: bool = False, excluding_possibly_exiting: bool = False) -> list[Member]:
        """All members but Joining, WeaklyUp, Down and Exiting. ``including_possibly_up`` counts Joining and WeaklyUp,
        which may be Up on the other side; ``excluding_possibly_exiting`` drops Leaving, which may be Exiting there."""
        return [
            m
            for m in self._all_members
            if not (
                (not including_possibly_up and m.status in ("joining", "weakly_up"))
                or (excluding_possibly_exiting and m.status == "leaving")
                or m.status in _DOWN_OR_EXITING
            )
        ]

    def reachable_members(self, including_possibly_up: bool = False, excluding_possibly_exiting: bool = False) -> list[Member]:
        return [m for m in self.members(including_possibly_up, excluding_possibly_exiting) if not self.is_unreachable(m)]

    def unreachable_members(self, including_possibly_up: bool = False, excluding_possibly_exiting: bool = False) -> list[Member]:
        return [m for m in self.members(including_possibly_up, excluding_possibly_exiting) if self.is_unreachable(m)]

    # --- reachability and seen -------------------------------------------------------------------------------------

    @property
    def reachability(self) -> Reachability:
        return self._reachability

    def _is_member(self, node: UniqueAddress) -> bool:
        return any(m.unique_address == node for m in self._all_members)

    def set_reachability(self, r: Reachability) -> None:
        self._reachability = r.filter_records(
            lambda rec: rec.status in ("unreachable", "terminated") and self._is_member(rec.observer) and self._is_member(rec.subject)
        )

    @property
    def seen_by(self) -> frozenset[Address]:
        return self._seen_by

    def set_seen_by(self, s: frozenset[Address]) -> None:
        self._seen_by = s

    @property
    def indirectly_connected(self) -> frozenset[UniqueAddress]:
        """Unreachable nodes that still talk to someone: a cycle in the unreachability graph, or an unreachable node
        that has seen the current gossip. Neither happens for clean splits or crashed nodes."""
        return self._indirectly_connected_from_intersection() | self._indirectly_connected_from_seen()

    def _indirectly_connected_from_intersection(self) -> frozenset[UniqueAddress]:
        return self._reachability.all_observers & self._reachability.all_unreachable_or_terminated

    def _indirectly_connected_from_seen(self) -> frozenset[UniqueAddress]:
        out: set[UniqueAddress] = set()
        for r in self._reachability.records:
            if r.subject.address in self._seen_by:
                out.add(r.observer)
                out.add(r.subject)
        return frozenset(out)

    @property
    def has_indirectly_connected(self) -> bool:
        return bool(self.indirectly_connected)

    @property
    def unreachable_but_not_indirectly_connected(self) -> frozenset[UniqueAddress]:
        return self.unreachable - self.indirectly_connected

    @property
    def is_all_unreachable_down_or_exiting(self) -> bool:
        return not self._unreachable or all(m.status in _DOWN_OR_EXITING for m in self.unreachable_members())

    # --- decisions ------------------------------------------------------------------------------------------------

    def decide(self) -> Decision:
        match self.config:
            case StaticQuorum(quorum_size):
                return self._decide_static_quorum(quorum_size)
            case KeepMajority():
                return self._decide_keep_majority()
            case KeepOldest(down_if_alone):
                return self._decide_keep_oldest(down_if_alone)
            case DownAll():
                return DownAllDecision()

    # StaticQuorum: keep this side when it has at least quorum_size members. Never add more than
    # quorum_size * 2 - 1 members, or both sides may keep themselves; past that it downs all.

    @property
    def is_too_many_members(self) -> bool:
        return isinstance(self.config, StaticQuorum) and len(self.members()) > self.config.quorum_size * 2 - 1

    def _decide_static_quorum(self, quorum_size: int) -> Decision:
        if self.is_too_many_members:
            return DownAllDecision()
        if self.has_indirectly_connected:
            return DownIndirectlyConnected()
        if len(self.members()) - len(self.unreachable_members()) >= quorum_size:
            return DownUnreachable()
        return DownReachable()

    # KeepMajority: down the unreachable when this side is the majority of the last known membership, else this
    # side; equal sizes keep the side with the lowest address. Three or more parts with no majority all shut down.

    def _decide_keep_majority(self) -> Decision:
        if self.has_indirectly_connected:
            return DownIndirectlyConnected()
        ms = self.members()
        if not ms:
            return DownAllDecision()
        decision = self._majority_decision(len(self.reachable_members()), len(self.unreachable_members()), ms[0])
        if isinstance(decision, DownUnreachable):
            if isinstance(self._majority_decision_when_including_membership_changes_edge_case(), DownUnreachable):
                return DownUnreachable()
            return DownAllDecision()  # the edge case disagrees: safest to down all
        return decision

    def _majority_decision(self, this_side: int, other_side: int, lowest: Member) -> Decision:
        if this_side == other_side:
            return DownReachable() if self.is_unreachable(lowest) else DownUnreachable()
        return DownUnreachable() if this_side > other_side else DownReachable()

    def _majority_decision_when_including_membership_changes_edge_case(self) -> Decision:
        """A membership change at the same time as the partition: count as few as possible on this side (no
        Joining, no Leaving) and as many as possible on the other (Joining included, Leaving included)."""
        ms = self.members(including_possibly_up=False, excluding_possibly_exiting=True)
        if not ms:
            return DownAllDecision()
        this_side = len(self.reachable_members(including_possibly_up=False, excluding_possibly_exiting=True))
        other_side = len(self.unreachable_members(including_possibly_up=True, excluding_possibly_exiting=False))
        return self._majority_decision(this_side, other_side, ms[0])

    # KeepOldest: keep the side with the oldest member, unless down_if_alone and the oldest is alone against two or
    # more, in which case the oldest downs itself and the others continue.

    def _decide_keep_oldest(self, down_if_alone: bool) -> Decision:
        if self.has_indirectly_connected:
            return DownIndirectlyConnected()
        ms = self.members()
        if not ms:
            return DownAllDecision()
        oldest_is_reachable = not self.is_unreachable(ms[0])
        decision = self._oldest_decision(down_if_alone, oldest_is_reachable, len(self.reachable_members()), len(self.unreachable_members()))
        if isinstance(decision, DownUnreachable):
            if isinstance(self._oldest_decision_when_including_membership_changes_edge_case(down_if_alone), DownUnreachable):
                return DownUnreachable()
            return DownAllDecision()
        return decision

    def _oldest_decision(self, down_if_alone: bool, oldest_is_on_this_side: bool, this_side: int, other_side: int) -> Decision:
        # Two nodes: keep the oldest even though it is alone, the other side is no better. Three or more: the oldest
        # alone against two or more shuts itself down when down_if_alone.
        if oldest_is_on_this_side:
            if down_if_alone and this_side == 1 and other_side >= 2:
                return DownReachable()
            return DownUnreachable()
        if down_if_alone and other_side == 1 and this_side >= 2:
            return DownUnreachable()
        return DownReachable()

    def _oldest_decision_when_including_membership_changes_edge_case(self, down_if_alone: bool) -> Decision:
        """Exclude Leaving on this side (may be Exiting on the other); with down_if_alone count Joining and WeaklyUp
        too, since they may be Up on the other side and flip the alone test."""
        ms = self.members(including_possibly_up=False, excluding_possibly_exiting=True)
        if not ms:
            return DownAllDecision()
        oldest_is_reachable = not self.is_unreachable(ms[0])
        including_possibly_up = down_if_alone
        reachable = len(self.reachable_members(including_possibly_up, excluding_possibly_exiting=True))
        unreachable = len(self.unreachable_members(including_possibly_up, excluding_possibly_exiting=True))
        return self._oldest_decision(down_if_alone, oldest_is_reachable, reachable, unreachable)

    def nodes_to_down(self, decision: Decision | None = None) -> frozenset[UniqueAddress]:
        if decision is None:
            decision = self.decide()
        downable = frozenset(m.unique_address for m in self.members() if m.status not in _DOWN_OR_EXITING) | frozenset(
            m.unique_address for m in self.joining
        )
        match decision:
            case DownUnreachable():
                return downable & self.unreachable
            case DownReachable():
                return downable - self.unreachable
            case DownAllDecision():
                return downable
            case DownIndirectlyConnected():
                # Down the indirectly connected, and combine with the ordinary decision taken after excluding the
                # observations between them, so a clean partition alongside is resolved too.
                return downable & (self.indirectly_connected | self._additional_nodes_to_down_when_indirectly_connected(downable))

    def _additional_nodes_to_down_when_indirectly_connected(self, downable: frozenset[UniqueAddress]) -> frozenset[UniqueAddress]:
        if not self.unreachable_but_not_indirectly_connected:
            return frozenset()
        original_unreachable, original_reachability = self._unreachable, self._reachability
        try:
            intersection = self._indirectly_connected_from_intersection()
            have_seen = self._indirectly_connected_from_seen()

            def keep(r: Record) -> bool:
                return (
                    r.observer in downable
                    and r.subject in downable
                    and not (
                        (r.observer in intersection and r.subject in intersection)
                        or (r.observer in have_seen and r.subject in have_seen)
                    )
                )

            self._reachability = self._reachability.filter_records(keep)
            self._unreachable = set(self._reachability.all_unreachable_or_terminated)
            additional = self.decide()
            if isinstance(additional, DownIndirectlyConnected):
                raise _DoubleDecision(
                    f"SBR double {additional} decision, downing all instead. originalReachability: [{original_reachability}], "
                    f"filtered reachability [{self._reachability}], still indirectlyConnected: [{self.indirectly_connected}], "
                    f"seenBy: [{self._seen_by}]"
                )
            return self.nodes_to_down(additional)
        finally:
            self._unreachable, self._reachability = original_unreachable, original_reachability

