"""Apache Pekko's split-brain resolver: a downing provider that waits until membership and reachability have been
stable for ``stable_after`` and then downs one side of a partition by a strategy. Ported from
``cluster/.../sbr/{SplitBrainResolver,DowningStrategy,SplitBrainResolverProvider}.scala`` without roles, data
centers, the lease-majority strategy and quarantine."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..behavior import Behavior, Behaviors
from ..context import ActorContext
from ..ref import ActorRef
from ..remote import Address, UniqueAddress
from .core import (
    ClusterDomainEvent,
    Down,
    LeaderChanged,
    MemberDowned,
    MemberExited,
    MemberJoined,
    MemberLeft,
    MemberRemoved,
    MemberUp,
    MemberWeaklyUp,
    ReachabilityChanged,
    ReachableMember,
    SeenChanged,
    Subscribe,
    UnreachableMember,
)
from .gossip import Member
from .reachability import Reachability, Record

__all__ = [
    "DownAll",
    "DowningStrategy",
    "KeepMajority",
    "KeepOldest",
    "SplitBrainResolverSettings",
    "StaticQuorum",
    "split_brain_resolver",
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

    def _key(self, m: Member) -> tuple[int, Address, int]:
        """Pekko's Member.ordering (address, then uid), or ageOrdering (up number first) for KeepOldest."""
        return (m.up_number if isinstance(self.config, KeepOldest) else 0, m.address, m.unique_address.uid)

    @property
    def all_members(self) -> list[Member]:
        return list(self._all_members)

    def add(self, m: Member) -> None:
        self._remove_from_all_members(m)
        key = self._key(m)
        i = 0
        while i < len(self._all_members) and self._key(self._all_members[i]) < key:
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



# --- the resolver (SplitBrainResolver.scala) -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Tick: ...


class _Resolver:
    """SplitBrainResolverBase: fed by cluster events and ticks, active on the leader. Unreachable members are downed by
    the strategy once membership and reachability have been stable for ``stable_after``; an unstable cluster downs
    everything after ``stable_after + down_all_when_unstable``, on every node."""

    def __init__(
        self,
        settings: SplitBrainResolverSettings,
        self_unique_address: UniqueAddress,
        down: Callable[[Address], None],
        clock: Callable[[], float],
        log: logging.Logger,
    ) -> None:
        self.settings = settings
        self.self_unique_address = self_unique_address
        self._down = down
        self.clock = clock
        self.log = log
        self.strategy = _DowningStrategy(self_unique_address, settings.strategy)
        self.leader = False
        self.self_member_added = False
        self.stopped = False
        self.stable_deadline = 0.0
        self.reset_stable_deadline()
        self.reachability_changed_stats: tuple[float, float, int] = (clock(), clock(), 0)  # first change, latest, count
        self.log.info(
            "SBR started. Config: strategy [%s], stable-after [%s s], down-all-when-unstable [%s], selfUniqueAddress [%s].",
            type(settings.strategy).__name__,
            settings.stable_after,
            "off" if settings.effective_down_all_when_unstable == 0 else f"{settings.effective_down_all_when_unstable} s",
            self_unique_address,
        )

    # --- state helpers --------------------------------------------------------------------------------------------

    @property
    def is_responsible(self) -> bool:
        return self.leader and self.self_member_added

    def reset_stable_deadline(self) -> None:
        self.stable_deadline = self.clock() + self.settings.stable_after

    def reset_reachability_changed_stats(self) -> None:
        now = self.clock()
        self.reachability_changed_stats = (now, now, 0)

    def _stats_str(self) -> str:
        first, latest, count = self.reachability_changed_stats
        if count == 0:
            return "reachability unchanged"
        now = self.clock()
        return f"reachability changed {count} times since {now - first:.3f} s ago, latest change was {now - latest:.3f} s ago"

    def reset_reachability_changed_stats_if_all_unreachable_downed(self) -> None:
        if self.reachability_changed_stats[2] > 0 and self.strategy.is_all_unreachable_down_or_exiting:
            self.log.debug("SBR resetting reachability stats, after all unreachable healed, downed or removed")
            self.reset_reachability_changed_stats()

    def update_reachability_changed_stats(self) -> None:
        now = self.clock()
        first, _latest, count = self.reachability_changed_stats
        self.reachability_changed_stats = (now, now, 1) if count == 0 else (first, now, count + 1)

    def mutate_member_info(self, reset_stable: bool, f: Callable[[], None]) -> None:
        """Wrap an update of the strategy's members so the stable deadline is reset and the change is logged."""
        unreachable_before = len(self.strategy.unreachable)
        f()
        unreachable_after = len(self.strategy.unreachable)
        if reset_stable:
            if self.is_responsible:
                if unreachable_before == 0 and unreachable_after > 0:
                    self.log.info(
                        "SBR found unreachable members, waiting for stable-after = %s s before taking downing decision. Now %d unreachable members found.",
                        self.settings.stable_after,
                        unreachable_after,
                    )
                elif unreachable_before > 0 and unreachable_after == 0:
                    self.log.info("SBR found all unreachable members healed during stable-after period, no downing decision necessary for now.")
                elif unreachable_after > 0:
                    self.log.info(
                        "SBR found unreachable members changed during stable-after period. Resetting timer. Now %d unreachable members found.",
                        unreachable_after,
                    )
            self.reset_stable_deadline()

    def mutate_responsibility_info(self, f: Callable[[], None]) -> None:
        before = self.is_responsible
        f()
        after = self.is_responsible
        if not before and after:
            self.log.info("This node is now the leader responsible for taking SBR decisions among the reachable nodes (more leaders may exist).")
        elif before and not after:
            self.log.info("This node is not the leader any more and not responsible for taking SBR decisions.")

    # --- events ----------------------------------------------------------------------------------------------------

    def handle(self, event: ClusterDomainEvent) -> None:
        match event:
            case SeenChanged(seen_by=seen_by):
                self.strategy.set_seen_by(seen_by)
            case MemberJoined(member=m):
                self.strategy.add(m)
            case MemberWeaklyUp(member=m):
                if m.unique_address == self.self_unique_address:
                    self.mutate_responsibility_info(lambda: setattr(self, "self_member_added", True))
                self.strategy.add(m)
            case MemberUp(member=m):
                self.add_up(m)
            case MemberLeft(member=m):
                self.mutate_member_info(False, lambda: self.strategy.add(m))
            case MemberExited(member=m):
                self.mutate_member_info(True, lambda: self.strategy.add(m))
            case UnreachableMember(member=m) | MemberDowned(member=m):
                self.unreachable_member(m)
            case ReachableMember(member=m):
                self.reachable_member(m)
            case ReachabilityChanged(reachability=r):
                self.strategy.set_reachability(r)
            case MemberRemoved(member=m):
                self.remove(m)
            case LeaderChanged(leader=leader):
                self.mutate_responsibility_info(lambda: setattr(self, "leader", leader == self.self_unique_address.address))
            case _:
                pass

    def add_up(self, m: Member) -> None:
        def f() -> None:
            self.strategy.add(m)
            if m.unique_address == self.self_unique_address:
                self.mutate_responsibility_info(lambda: setattr(self, "self_member_added", True))

        self.mutate_member_info(True, f)
        if isinstance(self.settings.strategy, StaticQuorum) and self.strategy.is_too_many_members:
            q = self.settings.strategy.quorum_size
            self.log.warning(
                "The cluster size is [%d] and static-quorum.quorum-size is [%d]. You should not add more than [%d] (quorum-size * 2 - 1) "
                "members to the cluster. If the exceeded cluster size remains when a SBR decision is needed it will down all nodes.",
                len(self.strategy.members()),
                q,
                q * 2 - 1,
            )

    def unreachable_member(self, m: Member) -> None:
        if m.unique_address == self.self_unique_address:
            return

        def f() -> None:
            self.strategy.add_unreachable(m)
            self.update_reachability_changed_stats()
            self.reset_reachability_changed_stats_if_all_unreachable_downed()

        self.mutate_member_info(True, f)

    def reachable_member(self, m: Member) -> None:
        if m.unique_address == self.self_unique_address:
            return

        def f() -> None:
            self.strategy.add_reachable(m)
            self.update_reachability_changed_stats()
            self.reset_reachability_changed_stats_if_all_unreachable_downed()

        self.mutate_member_info(True, f)

    def remove(self, m: Member) -> None:
        if m.unique_address == self.self_unique_address:
            self.stopped = True
            return

        def f() -> None:
            self.strategy.remove(m)
            self.reset_reachability_changed_stats_if_all_unreachable_downed()

        self.mutate_member_info(False, f)

    # --- ticks and decisions ----------------------------------------------------------------------------------------

    def tick(self) -> None:
        first, latest, count = self.reachability_changed_stats
        if count > 0:
            now = self.clock()
            down_all_when_unstable = self.settings.effective_down_all_when_unstable
            # DownAll on instability runs on every node, to decide as quickly and aggressively as possible once time is out.
            if down_all_when_unstable > 0 and now - first > self.settings.stable_after + down_all_when_unstable:
                self.log.warning("SBR detected instability and will down all nodes: %s", self._stats_str())
                self.act_on_decision(DownAllDecision())
            elif down_all_when_unstable <= 0 and now - latest > self.settings.stable_after * 2:
                self.log.debug("SBR no reachability changes within %s s, resetting stats", self.settings.stable_after * 2)
                self.reset_reachability_changed_stats()
        if self.is_responsible and self.strategy.unreachable and self.clock() >= self.stable_deadline:
            self.act_on_decision(self.strategy.decide())

    def act_on_decision(self, decision: Decision) -> frozenset[UniqueAddress]:
        """Down the nodes the decision names, self last; returns them."""
        try:
            nodes_to_down = self.strategy.nodes_to_down(decision)
        except _DoubleDecision as e:
            self.log.warning("%s", e)
            nodes_to_down = self.strategy.nodes_to_down(DownAllDecision())
        down_myself = self.self_unique_address in nodes_to_down
        self.log.warning(
            "SBR took decision %s and is downing [%s]%s, [%d] unreachable of [%d] members%s, all members [%s], full reachability status: [%s]",
            type(decision).__name__,
            ", ".join(str(n.address) for n in sorted(nodes_to_down, key=lambda n: n.address)),
            " including myself" if down_myself else "",
            len(self.strategy.unreachable),
            len(self.strategy.members()),
            f", indirectly connected [{sorted(str(n) for n in self.strategy.indirectly_connected)}]" if isinstance(decision, DownIndirectlyConnected) else "",
            ", ".join(str(m) for m in self.strategy.all_members),
            self.strategy.reachability,
        )
        if nodes_to_down:
            # Downing is idempotent; self is downed last since it may shut down at once when alone.
            for node in sorted(nodes_to_down, key=lambda n: n.address):
                if node != self.self_unique_address:
                    self.down(node)
            if down_myself:
                self.down(self.self_unique_address)
            self.reset_reachability_changed_stats()
            self.reset_stable_deadline()
        return nodes_to_down

    def down(self, node: UniqueAddress) -> None:
        self.log.info("SBR is downing [%s]", node)
        self._down(node.address)


def split_brain_resolver(settings: SplitBrainResolverSettings, self_unique_address: UniqueAddress, core: ActorRef[Any]) -> Behavior[Any]:
    """The resolver actor: subscribes to every cluster event through the core daemon, ticks every
    ``settings.tick_interval``, downs through the daemon's ``Down``, and stops when this member is removed."""

    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        resolver = _Resolver(settings, self_unique_address, lambda a: core.tell(Down(a)), asyncio.get_running_loop().time, ctx.log)
        core.tell(Subscribe(ctx.self, ClusterDomainEvent))
        ctx.timers.start_timer_with_fixed_delay("tick", _Tick(), settings.tick_interval)

        def on_message(msg: Any) -> Behavior[Any]:
            match msg:
                case _Tick():
                    resolver.tick()
                case ClusterDomainEvent():
                    resolver.handle(msg)
                case _:
                    pass
            return Behaviors.stopped() if resolver.stopped else Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)
