# Cluster membership design (cluster layer 2)

Gossip-based membership as Apache Pekko Cluster does it, read from `cluster/` in the local Pekko source and
narrowed to one data center, no roles, no app-version or config checks, no full-cluster-shutdown states,
no split-brain resolver, no quarantine. Everything else follows the Scala code function for function.

## Data model (`pekko_py/cluster/` package: `vector_clock.py`, `reachability.py`, `gossip.py`)

All frozen dataclasses, all `@remotable` where they cross the wire. `UniqueAddress` (remote.py) gains
`address -> Address`; `Address(name, host, port)` is new, with `parse("pekko://name@host:port")` and `root`.
Address ordering is by (host, port), as `Member.addressOrdering`.

- `type MemberStatus = Literal["joining", "weakly_up", "up", "leaving", "exiting", "down", "removed"]`
  with `ALLOWED_TRANSITIONS` exactly Pekko's table minus the shutdown states.
- `Member(unique_address, up_number, status)`: equality and hash by unique address; `is_older_than`
  (up_number, then address); `with_status` (checks the transition); `copy_up(n)`; `highest_priority_of(a, b)`
  and `pick_highest_priority(a, b, tombstones)` with Pekko's priority order (Removed > Down > Exiting >
  Leaving > Up > WeaklyUp > Joining; equal status keeps the older); `leader_status_ordering` key.
- `VectorClock(versions: dict[str, int])`: `increment(node)`, `compare(other) -> "same"|"before"|"after"|
  "concurrent"` (Pekko's single-pass algorithm), `merge`, `prune(node)`. Node name `f"{host}:{port}-{uid}"`.
- `Reachability(records: tuple[Record, ...], versions: tuple[tuple[UniqueAddress, int], ...])` with
  `Record(observer, subject, status: Literal["reachable","unreachable","terminated"], version)`; methods
  `unreachable/reachable/terminated(observer, subject)`, `merge(allowed, other)` by observer version,
  `remove(nodes)`, `remove_observers(nodes)`, `status(observer, subject)`, `status(node)`, `is_reachable(node)`,
  `is_reachable(observer, subject)`, `is_all_reachable`, `all_unreachable`, `all_unreachable_or_terminated`,
  `all_unreachable_from(observer)`, `all_observers`. Semantics as Reachability.scala, including the
  "all reachable again: drop the observer's rows" pruning.
- `Gossip(members: tuple[Member, ...] sorted by address, seen: tuple[UniqueAddress, ...], reachability,
  version: VectorClock, tombstones: tuple[tuple[UniqueAddress, float], ...])`: `with_member`, `seen(node)`,
  `only_seen`, `clear_seen`, `merge_seen`, `merge(other)` (tombstones, clocks pruned by tombstones, members by
  priority, reachability by version, seen cleared), `member(node)` (a Removed placeholder when absent),
  `has_member`, `update(members)`, `remove(node, ts)`, `remove_all`, `mark_as_down(member)`, `prune(node)`,
  `prune_tombstones(before)`, `seen_digest` (sha1 hex of sorted seen addresses), `is_singleton`,
  `reachability_excluding_downed_observers`, `is_reachable(from, to)`.
- `MembershipState(latest_gossip, self_unique_address)`: `self_member`, `members`, `seen()`,
  `convergence(exiting_confirmed)`, `reachability_excluding_downed_observers`, `is_reachable_excluding_downed_observers`,
  `leader`, `is_leader(node)`, `leader_of(members)`, `valid_node_for_gossip`, `youngest_member`,
  `gossip_targets_for_exiting_members` (the two oldest).
- `GossipTargetSelector(gossip_different_view_probability=0.8, reduce_at_cluster_size=400)`: `gossip_target(state)`
  (prefer nodes that have not seen the gossip with the size-adjusted probability, else any valid node),
  `random_nodes_for_full_gossip(state, n)`.

## Failure detection (`pekko_py/cluster/failure_detector.py`)

- `HeartbeatHistory(max_sample_size)` with mean, variance, std deviation, `add(interval)` dropping the oldest.
- `PhiAccrualFailureDetector(threshold=8.0, max_sample_size=1000, min_std_deviation=0.1, acceptable_heartbeat_pause=3.0,
  first_heartbeat_estimate=1.0, clock=time.monotonic)`: bootstrap history of two samples at mean±mean/4,
  `heartbeat()` (first heartbeat seeds the history; an interval is recorded only if currently available),
  `phi`, `is_available` (phi < threshold), `is_monitoring`. Pekko's exact phi formula.
- `FailureDetectorRegistry(factory)`: `is_available(address)` (True when unknown), `is_monitoring`,
  `heartbeat(address)` (creates on first use), `remove`, `reset`.
- `HeartbeatNodeRing(self, nodes, unreachable, monitored_by_nr_of_members)`: ring ordered by hash then
  address; `my_receivers`: all others when the ring is small, else the next n reachable plus intermediate
  unreachable ones, as Pekko's `take`. `HeartbeatSenderState(ring, old_receivers_now_unreachable, registry)`
  with `init`, `add_member`, `remove_member`, `unreachable_member`, `reachable_member`, `heartbeat_rsp`,
  `active_receivers`, the same bookkeeping as ClusterHeartbeatSenderState.

## Runtime (`pekko_py/cluster/cluster.py`)

- `ClusterSettings` (frozen dataclass): `seed_nodes: tuple[Address, ...] = ()`, `seed_node_timeout=5.0`,
  `retry_unsuccessful_join_after=10.0`, `periodic_tasks_initial_delay=1.0`, `gossip_interval=1.0`,
  `leader_actions_interval=1.0`, `unreachable_nodes_reaper_interval=1.0`, `heartbeat_interval=1.0`,
  `heartbeat_expected_response_after=1.0`, `monitored_by_nr_of_members=9`, `failure_detector_threshold=8.0`,
  `failure_detector_max_sample_size=1000`, `failure_detector_min_std_deviation=0.1`,
  `acceptable_heartbeat_pause=3.0`, `allow_weakly_up_members: float | None = 7.0`, `min_nr_of_members=1`,
  `prune_gossip_tombstones_after=86400.0`, `gossip_different_view_probability=0.8`,
  `reduce_gossip_different_view_probability=400`, `max_gossips_before_shutting_down_myself=5`,
  `max_ticks_before_shutting_down_myself=4`. Pekko's defaults; tests pass millisecond values.
- `ActorSystem(..., address=(host, port), cluster=ClusterSettings(...))` starts the extension. `Cluster(system)`
  returns it (raises if the system has no cluster). `Cluster` has `self_member`, `state -> CurrentClusterState`,
  `manager: ActorRef[ClusterCommand]`, `subscriptions: ActorRef[ClusterStateSubscription]`,
  `self_unique_address`, `is_terminated`.
- Commands (`ClusterCommand`): `Join(address)`, `JoinSeedNodes(seed_nodes)`, `Leave(address)`, `Down(address)`.
  Subscriptions: `Subscribe(subscriber, event_class)` (initial state as events), `Unsubscribe(subscriber)`,
  `GetCurrentState(reply_to)`.
- Events (`ClusterDomainEvent`, with `MemberEvent` and `ReachabilityEvent` groups): `MemberJoined`, `MemberWeaklyUp`,
  `MemberUp`, `MemberLeft`, `MemberExited`, `MemberDowned`, `MemberRemoved(member, previous_status)`,
  `LeaderChanged(leader: Address | None)`, `UnreachableMember`, `ReachableMember`, `SelfUp(state)`,
  `SelfRemoved(previous_status)`. `CurrentClusterState(members, unreachable, seen_by, leader)`.
- The system root: when a cluster is configured the ActorSystem creates a second root cell at
  `pekko://name@host:port/system`, hosting `cluster` (the core daemon) with child `heartbeat` (the receiver).
  `resolve_local` accepts `/system` paths. Shutdown order: user root, then system root, then remoting.
- Wire messages, all `@remotable`, sent through remoting to the peer's `/system/cluster`: `InitJoin(reply_to)`,
  `InitJoinAck(address: UniqueAddress)`, `InitJoinNack(address)`, `Join(node: UniqueAddress, reply_to)`,
  `Welcome(from, gossip)`, `GossipEnvelope(from, to, gossip)`, `GossipStatus(from, version, seen_digest)`,
  `ExitingConfirmed(node)`; and to `/system/cluster/heartbeat`: `Heartbeat(from, seq, created_ns, reply_to)`,
  `HeartbeatRsp(from, seq, created_ns)`.
- Core daemon behaviour, state machine as ClusterDaemon: `uninitialized` (JoinTo, JoinSeedNodes, Welcome,
  InitJoin -> Nack), `trying_to_join(join_with, deadline)`, `initialized`. Functions ported one to one:
  `join_seed_nodes` (first seed process: InitJoin to the others every second until the seed timeout, then join
  self; join seed process: InitJoin to all other seeds, first Ack wins, retry after the timeout), `join`,
  `joining` (wrong system name ignored; self Down/Exiting ignored; existing member rejoining gets Welcome; new
  incarnation marks the old Terminated and downs it; else add both and Welcome), `welcome`, `leaving`,
  `exiting_completed` (confirm to the two potential leaders, then terminate the system), `downing`,
  `receive_gossip_status`, `receive_gossip` (Same/Before/After/Concurrent with pruning and talkback; self Exiting
  starts exiting; self Down starts `shutdown_self_when_down`), `gossip_tick` with speed-up, `gossip`,
  `gossip_random_n`, `leader_actions` (WeaklyUp after `allow_weakly_up_members` without convergence),
  `leader_actions_on_convergence` (Joining/WeaklyUp -> Up with up_number when min members met; Leaving ->
  Exiting; remove unreachable Down/Exiting and confirmed Exiting; prune tombstones; gossip exiting members to
  the oldest two), `shutdown_self_when_down`, `reap_unreachable_members`, `heartbeat` tick and `heartbeat_rsp`,
  `expected_first_heartbeat`, `update_latest_gossip` (bump own clock, only_seen self), `publish_membership_state`
  (diff old and new state into events for subscribers, as `diffMemberEvents`, `diffLeader`, `diffUnreachable`,
  `diffReachable`; `SelfUp` on own Up; `SelfRemoved` on own removal).
- Heartbeat receiver: replies `HeartbeatRsp` to `reply_to`.
- Self-termination: a node that is Down or Removed terminates its ActorSystem after gossiping, as Pekko's
  coordinated shutdown would.

## Invariants (Pekko's, kept)

- Only the leader changes member status other than Joining and Leaving and Down, and only on convergence.
- A member is never resurrected: tombstones outlive removal; a Removed member re-joining is a new incarnation.
- Reachability observations are per observer and versioned; a node only ever writes its own row.
- Gossip is only accepted from members this node considers reachable, and only when it contains self.
- Nothing leaks: all ticks are actor timers; the system root stops with the system.

## Deviations to document

Single data center, no roles, no app-version or config-compat check, no PreparingForShutdown states, no
split-brain resolver, no quarantine, heartbeat sender and event publisher live in the core daemon rather than
separate actors, `ClusterSettings` instead of HOCON config, `Address` has no protocol field, the cluster-wide
Receptionist is a separate plan.

## Tests

Pure: VectorClock ordering/merge/prune; Reachability change/merge/prune; member priority and transitions;
Gossip merge; convergence and leader selection on hand-built states; phi against known values; heartbeat ring
receivers. Multi-node, in one process at millisecond settings: first seed bootstraps alone; three nodes join
through seeds and all reach Up with distinct up_numbers; leader is the lowest address and changes when it
leaves; concurrent joins converge; Leave -> Exiting -> Removed and the leaving system terminates; a node torn
down (remoting closed) becomes Unreachable, `Down` removes it; a new incarnation on the same address rejoins
and the old one is removed; WeaklyUp when an unreachable member blocks convergence; subscribers receive
MemberUp/LeaderChanged/UnreachableMember/MemberRemoved; GetCurrentState snapshot. Every test under the
conftest leak check.

## Example and docs

`examples/cluster/node.py NAME PORT [SEED_PORT...]`: joins, subscribes to MemberEvent and prints them.
README: a "Cluster" Tour subsection, names-table rows, deviations, examples row.
