"""Members, Gossip and MembershipState, as Gossip.scala, Member.scala and MembershipState.scala, for one data
center without roles."""
from __future__ import annotations

import hashlib
import random
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Literal

from ..remote import UniqueAddress
from ..serialization import remotable
from .reachability import Reachability
from .vector_clock import VectorClock

type MemberStatus = Literal["joining", "weakly_up", "up", "leaving", "exiting", "down", "removed"]

ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "joining": frozenset({"weakly_up", "up", "leaving", "down", "removed"}),
    "weakly_up": frozenset({"up", "leaving", "down", "removed"}),
    "up": frozenset({"leaving", "down", "removed"}),
    "leaving": frozenset({"exiting", "down", "removed"}),
    "down": frozenset({"removed"}),
    "exiting": frozenset({"removed", "down"}),
    "removed": frozenset(),
}

UP_NUMBER_UNSET = 2**31 - 1  # Pekko's Int.MaxValue: not yet Up

# Statuses the leader may hold, and the ones that must have seen the gossip for convergence.
LEADER_MEMBER_STATUS: frozenset[str] = frozenset({"up", "leaving"})
CONVERGENCE_MEMBER_STATUS: frozenset[str] = frozenset({"up", "leaving"})
# Unreachable members with these statuses neither block convergence nor stay: the leader removes them.
REMOVE_UNREACHABLE_WITH_MEMBER_STATUS: frozenset[str] = frozenset({"down", "exiting"})

_PRIORITY = {"removed": 0, "down": 1, "exiting": 2, "leaving": 3, "up": 4, "weakly_up": 5, "joining": 6}
_LEADER_RANK = {"up": 0, "leaving": 0, "weakly_up": 1, "joining": 2, "exiting": 3, "down": 4, "removed": 5}


def vclock_name(node: UniqueAddress) -> str:
    return f"{node.host}:{node.port}-{node.uid}"


@remotable
@dataclass(frozen=True, eq=False)
class Member:
    """A node in the cluster. Equal by unique address; ordered by address, as Pekko's Member.ordering."""

    unique_address: UniqueAddress
    up_number: int
    status: MemberStatus

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Member) and other.unique_address == self.unique_address

    def __hash__(self) -> int:
        return hash(self.unique_address)

    def __lt__(self, other: Member) -> bool:
        return self.unique_address.address < other.unique_address.address

    @property
    def address(self):  # noqa: ANN201  # Address, imported lazily by callers through remote
        return self.unique_address.address

    def is_older_than(self, other: Member) -> bool:
        if self.up_number == other.up_number:
            return self.unique_address.address < other.unique_address.address
        return self.up_number < other.up_number

    def with_status(self, status: MemberStatus) -> Member:
        if status == self.status:
            return self
        if status not in ALLOWED_TRANSITIONS[self.status]:
            raise ValueError(f"invalid member status transition [{self} -> {status}]")
        return replace(self, status=status)

    def copy_up(self, up_number: int) -> Member:
        return replace(self, up_number=up_number).with_status("up")

    def __repr__(self) -> str:
        return f"Member({self.unique_address.authority}, {self.status})"


def removed_placeholder(node: UniqueAddress) -> Member:
    return Member(node, UP_NUMBER_UNSET, "removed")


def highest_priority_of(m1: Member, m2: Member) -> Member:
    """Of two views of one member, the one further along in the lifecycle; the older when they agree."""
    if m1.status == m2.status:
        return m1 if m1.is_older_than(m2) else m2
    return m1 if _PRIORITY[m1.status] < _PRIORITY[m2.status] else m2


def pick_highest_priority(a: Iterable[Member], b: Iterable[Member], tombstones: dict[UniqueAddress, float]) -> set[Member]:
    grouped: dict[UniqueAddress, list[Member]] = {}
    for m in [*a, *b]:
        grouped.setdefault(m.unique_address, []).append(m)
    picked: set[Member] = set()
    for members in grouped.values():
        if len(members) == 2:
            picked.add(highest_priority_of(members[0], members[1]))
        else:
            (m,) = members
            if m.unique_address in tombstones or m.status in REMOVE_UNREACHABLE_WITH_MEMBER_STATUS:
                continue  # removed, or on one side only in a state the other side already dropped
            picked.add(m)
    return picked


def leader_status_key(m: Member) -> tuple[int, tuple[str, int]]:
    return (_LEADER_RANK[m.status], (m.unique_address.host, m.unique_address.port))


@remotable
@dataclass(frozen=True)
class Gossip:
    members: tuple[Member, ...] = ()  # sorted by address
    seen_by: tuple[UniqueAddress, ...] = ()  # nodes that have seen this exact version
    reachability: Reachability = Reachability()
    version: VectorClock = VectorClock()
    tombstones: tuple[tuple[UniqueAddress, float], ...] = ()

    @staticmethod
    def empty() -> Gossip:
        return Gossip()

    # --- members ------------------------------------------------------------------------------------

    def with_member(self, member: Member) -> Gossip:
        if member in self.members:
            return self
        return replace(self, members=tuple(sorted((*self.members, member))))

    def update(self, updated: Iterable[Member]) -> Gossip:
        """Replace the given members (matched by address), as Gossip.update."""
        new = {m.unique_address: m for m in updated}
        kept = [new.pop(m.unique_address, m) for m in self.members]
        return replace(self, members=tuple(sorted([*kept, *new.values()])))

    def member(self, node: UniqueAddress) -> Member:
        for m in self.members:
            if m.unique_address == node:
                return m
        return removed_placeholder(node)

    def has_member(self, node: UniqueAddress) -> bool:
        return any(m.unique_address == node for m in self.members)

    @property
    def is_singleton(self) -> bool:
        return len(self.members) == 1

    # --- seen ---------------------------------------------------------------------------------------

    def seen_by_node(self, node: UniqueAddress) -> bool:
        return node in self.seen_by

    def seen(self, node: UniqueAddress) -> Gossip:
        if node in self.seen_by:
            return self
        return replace(self, seen_by=(*self.seen_by, node))

    def only_seen(self, node: UniqueAddress) -> Gossip:
        return replace(self, seen_by=(node,))

    def clear_seen(self) -> Gossip:
        return replace(self, seen_by=())

    def merge_seen(self, other: Gossip) -> Gossip:
        return replace(self, seen_by=tuple(dict.fromkeys((*self.seen_by, *other.seen_by))))

    @property
    def seen_digest(self) -> str:
        text = ",".join(sorted(f"{n.host}:{n.port}" for n in self.seen_by))
        return hashlib.sha1(text.encode()).hexdigest()

    # --- versions and reachability ------------------------------------------------------------------

    def increment(self, node_name: str) -> Gossip:
        return replace(self, version=self.version.increment(node_name))

    def prune(self, node_name: str) -> Gossip:
        pruned = self.version.prune(node_name)
        return self if pruned is self.version else replace(self, version=pruned)

    def with_reachability(self, reachability: Reachability) -> Gossip:
        return replace(self, reachability=reachability)

    @property
    def reachability_excluding_downed_observers(self) -> Reachability:
        downed = frozenset(m.unique_address for m in self.members if m.status == "down")
        return self.reachability.remove_observers(downed)

    def is_reachable(self, from_node: UniqueAddress, to_node: UniqueAddress) -> bool:
        return self.has_member(to_node) and self.reachability.is_reachable(from_node, to_node)

    # --- lifecycle ----------------------------------------------------------------------------------

    def merge(self, other: Gossip) -> Gossip:
        tombstones = {**dict(self.tombstones), **dict(other.tombstones)}
        version = self.version.merge(other.version)
        for node in tombstones:
            version = version.prune(vclock_name(node))
        members = tuple(sorted(pick_highest_priority(self.members, other.members, tombstones)))
        allowed = frozenset(m.unique_address for m in members)
        reachability = self.reachability.merge(allowed, other.reachability)
        return Gossip(members, (), reachability, version, tuple(tombstones.items()))

    def remove(self, node: UniqueAddress, removal_timestamp: float) -> Gossip:
        return Gossip(
            members=tuple(m for m in self.members if m.unique_address != node),
            seen_by=tuple(n for n in self.seen_by if n != node),
            reachability=self.reachability.remove([node]),
            version=self.version.prune(vclock_name(node)),
            tombstones=(*((n, t) for n, t in self.tombstones if n != node), (node, removal_timestamp)),
        )

    def remove_all(self, nodes: Iterable[UniqueAddress], removal_timestamp: float) -> Gossip:
        gossip = self
        for node in nodes:
            gossip = gossip.remove(node, removal_timestamp)
        return gossip

    def mark_as_down(self, member: Member) -> Gossip:
        downed = self.update([member.with_status("down")])
        return replace(downed, seen_by=tuple(n for n in self.seen_by if n != member.unique_address))

    def prune_tombstones(self, remove_earlier_than: float) -> Gossip:
        kept = tuple((n, t) for n, t in self.tombstones if t > remove_earlier_than)
        return self if len(kept) == len(self.tombstones) else replace(self, tombstones=kept)


@dataclass(frozen=True)
class MembershipState:
    latest_gossip: Gossip
    self_unique_address: UniqueAddress

    @property
    def self_member(self) -> Member:
        return self.latest_gossip.member(self.self_unique_address)

    @property
    def members(self) -> tuple[Member, ...]:
        return self.latest_gossip.members

    def seen(self) -> MembershipState:
        return replace(self, latest_gossip=self.latest_gossip.seen(self.self_unique_address))

    def convergence(self, exiting_confirmed: frozenset[UniqueAddress]) -> bool:
        """Every Up or Leaving member has seen this gossip, and every unreachable member is Down or Exiting."""
        gossip = self.latest_gossip
        # Until some member is Up or Leaving, Joining and WeaklyUp members must have seen the gossip too, so the first
        # up_number is assigned only once (Pekko's firstMemberInDc).
        first_member = not any(m.status in CONVERGENCE_MEMBER_STATUS for m in gossip.members)
        member_status = CONVERGENCE_MEMBER_STATUS | {"joining", "weakly_up"} if first_member else CONVERGENCE_MEMBER_STATUS
        hindering = any(
            m.status in member_status
            and not (gossip.seen_by_node(m.unique_address) or m.unique_address in exiting_confirmed)
            for m in gossip.members
        )
        unreachable = [
            gossip.member(node)
            for node in self.reachability_excluding_downed_observers.all_unreachable_or_terminated
            if node != self.self_unique_address and node not in exiting_confirmed
        ]
        return not hindering and all(m.status in REMOVE_UNREACHABLE_WITH_MEMBER_STATUS for m in unreachable)

    @property
    def reachability_excluding_downed_observers(self) -> Reachability:
        return self.latest_gossip.reachability_excluding_downed_observers

    def is_reachable_excluding_downed_observers(self, node: UniqueAddress) -> bool:
        return self.latest_gossip.has_member(node) and self.reachability_excluding_downed_observers.is_reachable(node)

    @property
    def leader(self) -> UniqueAddress | None:
        return self.leader_of(self.members)

    def is_leader(self, node: UniqueAddress) -> bool:
        return self.leader == node

    def leader_of(self, members: Iterable[Member]) -> UniqueAddress | None:
        """The first member, by address, with a leader status among the reachable non-Down ones; else the
        least-bad reachable member, as leaderOf with leaderStatusOrdering."""
        reachability = self.latest_gossip.reachability
        reachable = [
            m
            for m in members
            if m.status != "down"
            and (
                reachability.is_all_reachable
                or reachability.is_reachable(m.unique_address)
                or m.unique_address == self.self_unique_address
            )
        ]
        if not reachable:
            return None
        for m in reachable:  # members iterate in address order
            if m.status in LEADER_MEMBER_STATUS:
                return m.unique_address
        return min(reachable, key=leader_status_key).unique_address

    def valid_node_for_gossip(self, node: UniqueAddress) -> bool:
        return node != self.self_unique_address and self.latest_gossip.reachability.is_reachable(
            self.self_unique_address, node
        )

    @property
    def youngest_member(self) -> Member:
        if not self.members:
            raise ValueError("no youngest when no members")
        return max(self.members, key=lambda m: 0 if m.up_number == UP_NUMBER_UNSET else m.up_number)

    def gossip_targets_for_exiting_members(self, exiting: Iterable[Member]) -> set[Member]:
        """The two oldest members, where singletons and shard coordinators live, as Pekko's optimization."""
        if not any(True for _ in exiting):
            return set()
        by_age = sorted(self.members, key=lambda m: (m.up_number, (m.unique_address.host, m.unique_address.port)))
        return set(by_age[:2])


class GossipTargetSelector:
    """Which node to gossip to this round: prefer one that has not seen the current gossip, with a
    probability that shrinks for very large clusters, as Pekko's GossipTargetSelector."""

    def __init__(self, gossip_different_view_probability: float = 0.8, reduce_at_cluster_size: int = 400) -> None:
        self._p = gossip_different_view_probability
        self._low = reduce_at_cluster_size

    def gossip_target(self, state: MembershipState) -> UniqueAddress | None:
        targets = self.gossip_targets(state)
        return random.choice(targets) if targets else None

    def gossip_targets(self, state: MembershipState) -> list[UniqueAddress]:
        gossip = state.latest_gossip
        valid = [m.unique_address for m in gossip.members if state.valid_node_for_gossip(m.unique_address)]
        if random.random() < self._adjusted_probability(len(gossip.members)):
            not_seen = [n for n in valid if not gossip.seen_by_node(n)]
            if not_seen:
                return not_seen
        return valid

    def random_nodes_for_full_gossip(self, state: MembershipState, n: int) -> list[UniqueAddress]:
        valid = [m.unique_address for m in state.members if state.valid_node_for_gossip(m.unique_address)]
        if len(valid) <= n:
            return valid
        return random.sample(valid, n)

    def _adjusted_probability(self, cluster_size: int) -> float:
        low, high = self._low, self._low * 3
        if cluster_size <= low:
            return self._p
        min_p = self._p / 10
        if cluster_size >= high:
            return min_p
        return self._p + (cluster_size - low) * (min_p - self._p) / (high - low)
