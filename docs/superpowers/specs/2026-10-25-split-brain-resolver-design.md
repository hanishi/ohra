# Split-brain resolver design (cluster layer 6)

Apache Pekko's SplitBrainResolver, read from `cluster/.../sbr/` in the local Pekko source: a downing provider
that runs as a child of the core daemon, waits until membership and reachability have been stable for
`stable_after`, and then applies a strategy to the unreachable members, downing one side of the partition so
the leader can remove it. Without it an unreachable member blocks the leader forever and a crashed oldest node
blocks singleton hand-over and shard reallocation until someone sends `Down`. Ported function for function from
`SplitBrainResolver.scala`, `DowningStrategy.scala` and `SplitBrainResolverProvider.scala`, narrowed by dropping
roles, data centers, the lease-majority strategy and quarantine. Designed with the user on 2026-10-25.

## API (`pekko_py/cluster/sbr.py`, exported from `pekko_py.cluster` and `pekko_py`)

- Strategies, frozen dataclasses: `KeepMajority()`, `StaticQuorum(quorum_size: int)`,
  `KeepOldest(down_if_alone: bool = True)`, `DownAll()`. `type DowningStrategy = KeepMajority | StaticQuorum |
  KeepOldest | DownAll`.
- `SplitBrainResolverSettings(strategy: DowningStrategy = KeepMajority(), stable_after: float = 20.0,
  down_all_when_unstable: float | None = None)`. `None` is Pekko's `on`: `max(4.0, 0.75 * stable_after)`; `0`
  is `off`; a positive value is the duration. `stable_after` must be positive, `quorum_size` at least 1.
- `ClusterSettings` gains `downing: SplitBrainResolverSettings | None = None` and `down_removal_margin: float =
  0.0`, validated like the other fields, plus the property `effective_down_removal_margin`: `down_removal_margin`
  if positive, else `downing.stable_after` if `downing` is set, else `0.0`, as `SplitBrainResolverProvider.
  downRemovalMargin` and `NoDowning`.
- `downing=None` keeps today's behaviour: no resolver, manual `Down`.

## The strategy (`DowningStrategy.scala`)

`_DowningStrategy(self_unique_address, config)` holds the state Pekko's abstract class holds: `_unreachable:
set[UniqueAddress]`, `_reachability: Reachability`, `_seen_by: frozenset[Address]`, `_all_members`, a list kept
in `Member` order (address order; `KeepOldest` uses age order, as Pekko's `ageOrdering`). Methods, each a port:
`add`, `remove`, `add_unreachable`, `add_reachable`, `set_reachability` (keeps only Unreachable and Terminated
records whose observer and subject are members), `set_seen_by`, `joining`, `members(including_possibly_up=False,
excluding_possibly_exiting=False)`, `reachable_members(...)`, `unreachable_members(...)`, `indirectly_connected`
(the union of the observer/subject intersection and the nodes that are subjects of a record but have seen the
current gossip), `unreachable_but_not_indirectly_connected`, `is_all_unreachable_down_or_exiting`,
`nodes_to_down(decision)` with `additional_nodes_to_down_when_indirectly_connected`, which filters the
reachability table, re-decides, and raises `_DoubleDecision` when the re-decision is itself an
indirectly-connected one; the resolver turns that into `DownAll`, as Pekko's `IllegalStateException` handling.
The `*_with_role` variants collapse to their plain forms.

Decisions: `DownUnreachable`, `DownReachable`, `DownAll`, `DownIndirectlyConnected` (a frozen dataclass each,
`is_indirectly_connected` only on the last). `decide()` per strategy:

- `KeepMajority`: indirectly connected -> `DownIndirectlyConnected`; no members -> `DownAll`; else
  `majority_decision(reachable_size, unreachable_size, lowest)`: equal sizes keep the side holding the lowest
  address, more keeps this side (`DownUnreachable`), fewer downs it (`DownReachable`). A `DownUnreachable` is
  confirmed by the edge case that counts this side without Joining/WeaklyUp and without Leaving, and the other
  side with Joining/WeaklyUp; a different answer becomes `DownAll`.
- `StaticQuorum`: too many members (`len(members) > 2 * quorum_size - 1`) -> `DownAll`; indirectly connected
  -> `DownIndirectlyConnected`; `len(members) - len(unreachable_members) >= quorum_size` -> `DownUnreachable`;
  else `DownReachable`.
- `KeepOldest`: indirectly connected -> `DownIndirectlyConnected`; no members -> `DownAll`; else
  `oldest_decision(oldest_is_reachable, reachable_count, unreachable_count)` with Pekko's alone rule when
  `down_if_alone` (this side is the oldest alone against two or more: `DownReachable`; the oldest alone on the
  other side against two or more here: `DownUnreachable`), confirmed by the edge case (Leaving excluded; Joining
  and WeaklyUp counted only when `down_if_alone`), a different answer becoming `DownAll`.
- `DownAll`: `DownAll`.

## The resolver (`SplitBrainResolver.scala`)

`_SplitBrainResolver`, an actor at `/system/cluster/downingProvider`, spawned by the core daemon when it becomes
initialized and `settings.downing` is set, as ClusterDaemon.scala does on `preStart` of the core. Its failure
fails the daemon, which shuts the node down ("the downing provider is vital to a working cluster"). It subscribes
to `ClusterDomainEvent` and holds `leader`, `self_member_added`, `stable_deadline`,
`reachability_changed_stats: (first_change, latest_change, count)` and the strategy.

- Events, as Pekko's `receive`: `SeenChanged` -> `set_seen_by`; `MemberJoined`, `MemberWeaklyUp` -> add
  (WeaklyUp of self sets `self_member_added`); `MemberUp` -> add, reset the deadline, self sets
  `self_member_added`; `MemberLeft` -> add, no reset; `MemberExited` -> add, reset; `UnreachableMember`,
  `MemberDowned` -> `add_unreachable`, bump the stats, reset; `ReachableMember` -> `add_reachable`, bump, reset;
  `ReachabilityChanged` -> `set_reachability`; `MemberRemoved` -> self: stop; other: remove, no reset;
  `LeaderChanged` -> `leader = leader == self.address`. Self is never added to the unreachable set.
  The stats reset whenever they are non-empty and `is_all_unreachable_down_or_exiting`.
- `is_responsible = leader and self_member_added`. Pekko's info logs on responsibility and stable-after changes
  kept.
- `_Tick` every `tick_interval = min(1.0, stable_after / 4)` (Pekko's fixed second, scaled so millisecond
  settings work). On tick: with `down_all_when_unstable` positive and `now - first_change > stable_after +
  down_all_when_unstable`, act on `DownAll` on every node, responsible or not; with it off and `now -
  latest_change > 2 * stable_after`, reset the stats. Then, if responsible, the unreachable set is non-empty and
  the deadline is overdue, act on `decide()`.
- `act_on_decision(decision)`: `nodes_to_down`, a `_DoubleDecision` becoming `nodes_to_down(DownAll)` with a
  warning; Pekko's warning line with the decision, the nodes, the counts and the full reachability; down every
  node but self, then self last; reset the stats and the deadline. Downing sends `Down(address)` to the core
  daemon, idempotent for a member already Down.
- The deadline is `loop.time() + stable_after`, checked against `loop.time()`.

## Core changes (`pekko_py/cluster/core.py`, `reachability.py`)

- Internal `ClusterDomainEvent` subclasses `SeenChanged(convergence: bool, seen_by: frozenset[Address])` and
  `ReachabilityChanged(reachability: Reachability)`, published by `publish_diff` through Pekko's `diffSeen`
  (seen set changed, or convergence changed) and `diffReachability` (reachability table changed), after
  `LeaderChanged`, last, as Pekko's `publishDiff` ordering. Existing
  subscribers match on specific events and ignore them.
- `Reachability` gains `records` (all records) and `filter_records(pred)`.
- The daemon spawns the resolver in `become_initialized` and watches it; a resolver that stops or fails stops
  the daemon.

## The removal margin

- Singleton: `ClusterSingletonSettings.removal_margin <= 0` means the cluster's `effective_down_removal_margin`
  (ClusterSingletonManager.scala line 520), resolved when the manager starts; `max_hand_over_retries` uses the
  resolved value. The manager already delays `MemberRemoved` by the margin.
- Sharding coordinator: `MemberRemoved` of a region's member is applied as `ShardRegionTerminated` after the
  margin through a `_DelayedShardRegionTerminated(path)` timer when the margin is positive and the region is
  still registered when it fires, as ShardCoordinator.scala line 1096. Zero margin applies it at once, as today.

## Invariants

- No node is downed before the cluster has been stable for `stable_after`, except by `down_all_when_unstable`.
- Only the leader of a side downs by strategy; every node may down all on instability.
- A surviving side starts no singleton or shard that the downed side may still host until the removal margin
  has passed after `MemberRemoved`.
- `downing=None` changes nothing observable.

## Deviations to document

No roles, data centers, lease-majority or quarantine; `tick_interval` is `min(1, stable_after / 4)`;
`SplitBrainResolverSettings` and the strategy dataclasses replace HOCON (`active-strategy`, `stable-after`,
`down-all-when-unstable`, `static-quorum.quorum-size`, `keep-oldest.down-if-alone`); `down_removal_margin` is a
`ClusterSettings` field; the resolver is turned on with `downing=` rather than `downing-provider-class`.

## Tests

Pure (`tests/cluster/test_sbr.py`), each with hand-built members and reachability, as Pekko's
`SplitBrainResolverSpec`: KeepMajority majority, minority, equal sides by lowest address, Joining on the other
side flips to `DownAll`, Leaving on this side flips to `DownAll`, no members; StaticQuorum met, not met, too many
members; KeepOldest oldest on this side, on the other side, alone with and without `down_if_alone`, two nodes;
DownAll; indirectly connected from a cycle and from seen-by, `nodes_to_down` for `DownIndirectlyConnected` with
and without a clean partition alongside, the double-decision guard; `nodes_to_down` skips Down and Exiting and
includes Joining; settings validation and `effective_down_removal_margin`. A driver-level test of the resolver's
tick logic with an injected clock: decision only after `stable_after`, reset on a change, `down_all_when_unstable`
fires from the first change, stats reset at `2 * stable_after` when off, self downed last.

Multi-node at millisecond settings (`tests/cluster/test_sbr_cluster.py`): a crashed node is removed without
`Down`; a singleton moves after its host crashes, not before the removal margin; a three-node partition made with
the frame filter on both sides keeps the majority and the isolated node downs itself; the coordinator reallocates
a crashed region's shards only after the margin; `downing=None` leaves a crashed node unreachable; every test
under the leak check.

## Example and docs

`examples/cluster/node.py` sets `downing=SplitBrainResolverSettings(stable_after=5.0)`, so a killed node is
removed on its own. README: a "Split-brain resolver" Tour subsection after Cluster, names rows for the settings
and strategies, the deviations above, and the Cluster deviations bullet's "downing is manual, as Pekko without an
SBR" replaced by "the resolver is off unless `downing` is set, as Pekko without a provider".
