# Cluster Membership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pekko-style gossip membership: seeds, join/welcome, vector-clocked gossip with convergence, computed leader and leader actions, phi-accrual failure detection on a heartbeat ring, reachability, leaving, downing, and cluster events.

**Architecture:** A `pekko_py/cluster/` package: pure value modules (`vector_clock.py`, `reachability.py`, `gossip.py`, `failure_detector.py`) ported function for function from the Scala, and `cluster.py` with `ClusterSettings`, the `Cluster` extension, the core daemon behavior and the heartbeat receiver, running under a new `/system` root. Cluster messages are `@remotable` and travel over remoting.

**Tech Stack:** stdlib only.

**Spec:** `docs/superpowers/specs/2026-10-24-cluster-membership-design.md`

## Global Constraints

- Python `>=3.12`, core `dependencies = []`, `uv run pytest` and `uv run pyright` strict green after every task.
- `cell.py` is not modified. `remote.py` gains `Address` and `UniqueAddress.address`; `resolve_local` accepts `/system`.
- Commits: hanishi commits on request. Do not run `git commit`.
- Multi-node tests use `tests/util.free_port`, millisecond `ClusterSettings`, `eventually` with explicit timeouts up to 5s where convergence needs several gossip rounds; every test under the conftest leak check.
- Port the Scala names to snake_case and keep the control flow; where the Scala has multi-DC or role branches, drop the branch, do not generalize.

## Review Focus

- Convergence must be blocked by an Up member that has not seen the gossip and by an unreachable Up member, and unblocked once that member is Down. Task 1, `test_convergence_rules`.
- Concurrent gossips (two nodes each incrementing their own clock) must merge, not overwrite; the member set after merge is the union with the highest-priority status. Task 1, `test_gossip_merge_is_union_with_highest_priority`, and Task 3, `test_concurrent_joins_converge`.
- A new incarnation of a known address must get the old one Terminated and Downed, then be admitted. Task 4, `test_new_incarnation_replaces_the_old`.
- A node that learns it is Down must terminate itself and never rejoin on its own. Task 4, `test_downed_node_terminates_itself`.
- Nothing leaks when a cluster of three systems is torn down in any order. Every Task 3 and 4 test.

---

### Task 1: Pure membership model

**Files:**
- Create: `src/pekko_py/cluster/__init__.py`, `src/pekko_py/cluster/vector_clock.py`, `src/pekko_py/cluster/reachability.py`, `src/pekko_py/cluster/gossip.py`
- Modify: `src/pekko_py/remote.py` (`Address`, `UniqueAddress.address`, `resolve_local` accepts `/system` when the system has a system root)
- Test: `tests/cluster/__init__.py`, `tests/cluster/test_vector_clock.py`, `tests/cluster/test_reachability.py`, `tests/cluster/test_gossip.py`

**Interfaces:** as the spec's Data model section; `gossip.py` holds `MemberStatus`, `ALLOWED_TRANSITIONS`, `Member`, `Gossip`, `MembershipState`, `GossipTargetSelector`, and `vclock_name(node) -> str`.

- [ ] **Step 1: Write the failing tests** (one behavior each): clock increment/compare all four orderings/merge/prune; reachability unreachable-then-reachable prunes the observer row, merge picks the higher observer version, `status(node)` aggregates Terminated over Unreachable; member transitions allowed and rejected, `highest_priority_of` table; gossip merge union, tombstoned member dropped, seen cleared; `convergence` rules per Review Focus; `leader_of` skips Down/Exiting/Joining before Up and respects reachability; `youngest_member`; target selector returns only valid nodes and prefers not-seen ones when the probability is 1.0.
- [ ] **Step 2: Run to verify they fail** — `uv run pytest tests/cluster -q` → `ModuleNotFoundError`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright` → all pass, 0 errors.

### Task 2: Failure detection

**Files:**
- Create: `src/pekko_py/cluster/failure_detector.py`
- Test: `tests/cluster/test_failure_detector.py`

**Interfaces:** as the spec's Failure detection section.

- [ ] **Step 1: Write the failing tests**: history mean/std with a max sample size; phi is 0 before any heartbeat, small right after a heartbeat, above threshold after `mean + acceptable_pause + many std` with an injected clock; a long pause does not skew the history (interval not recorded when unavailable); registry creates on first heartbeat and `is_available` is True for unknown; ring with 5 nodes and `monitored_by_nr_of_members=2` gives each node 2 receivers and includes intermediate unreachable nodes; sender state `add_member/remove_member/heartbeat_rsp` bookkeeping including `old_receivers_now_unreachable`.
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright`.

### Task 3: Core daemon: joining, gossip, convergence, leader, leaving, events

**Files:**
- Create: `src/pekko_py/cluster/cluster.py`
- Modify: `src/pekko_py/system.py` (`cluster: ClusterSettings | None`; system root cell; `Cluster` wiring; shutdown order user root → system root → remoting), `src/pekko_py/__init__.py` (export `Cluster`, `ClusterSettings`, `Address`, the commands, subscriptions and events)
- Test: `tests/cluster/test_cluster.py`

**Interfaces:** as the spec's Runtime section, without heartbeats/reaper (Task 4): `reap_unreachable_members` and the heartbeat tick are no-ops until Task 4.

- [ ] **Step 1: Write the failing tests** with a helper `cluster_node(name, port, seeds, **settings)` returning an `ActorSystem` whose guardian ignores messages, and millisecond settings (`gossip_interval=0.02`, `leader_actions_interval=0.02`, `seed_node_timeout=0.1`, `periodic_tasks_initial_delay=0.0`, `heartbeat_interval=0.02`, `acceptable_heartbeat_pause=0.2`, `allow_weakly_up_members=None` unless the test needs it):

```python
async def test_first_seed_bootstraps_alone()               # self Up, leader is self
async def test_three_nodes_join_and_reach_up()             # all Up, up_numbers distinct 1..3, same member list on all nodes
async def test_leader_is_lowest_address()                  # LeaderChanged carries the lowest (host, port)
async def test_concurrent_joins_converge()                 # start three non-seed nodes at once against one seed
async def test_leave_moves_through_exiting_to_removed()    # Leave(b): others see MemberLeft, MemberExited, MemberRemoved; b terminates
async def test_subscribers_get_initial_state_as_events()   # late subscriber receives MemberUp for existing members + LeaderChanged
async def test_get_current_state_snapshot()
async def test_join_wrong_system_name_is_ignored()
```

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement**: the `/system` root in `ActorSystem` (created when `cluster` is given; `resolve` handles it), the `Cluster` extension object, the core daemon as a `Behaviors.setup` with closure state (`membership_state`, `seed_nodes`, `exiting_confirmed`, `subscribers`, `leader_action_counter`, `is_currently_leader`, `self_down_counter`, join bookkeeping) and `Behaviors.receive` over the union of internal ticks, commands, wire messages and subscriptions; `with_timers` for the periodic ticks. Remote sends go through `system.resolve(f"{address.root}/system/cluster").tell(...)`.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q` twice, `uv run pyright`.

### Task 4: Heartbeats, reachability, downing, incarnations

**Files:**
- Modify: `src/pekko_py/cluster/cluster.py`
- Test: `tests/cluster/test_cluster.py`

- [ ] **Step 1: Write the failing tests**:

```python
async def test_torn_down_node_becomes_unreachable()        # close b's remoting without leaving: a and c publish UnreachableMember(b); convergence blocked (no new joins reach Up)
async def test_down_removes_an_unreachable_member()        # Down(b) then MemberRemoved(b, previous_status="down") on a and c
async def test_downed_node_terminates_itself()             # Down(b) while b reachable: b's system terminates
async def test_new_incarnation_replaces_the_old()          # restart b on the same port: old member removed, new one Up
async def test_weakly_up_when_convergence_is_blocked()     # allow_weakly_up_members=0.05: joining node becomes WeaklyUp while another is unreachable
async def test_reachable_again_clears_unreachable()        # pause heartbeats by blocking the receiver briefly, then ReachableMember
```

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement**: heartbeat sender state and tick in the core, `heartbeat` child receiver, `reap_unreachable_members`, `shutdown_self_when_down`, the new-incarnation branch in `joining`, `move_joining_to_weakly_up`.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q` three times (timing), `uv run pyright`.

### Task 5: Example, README

**Files:**
- Create: `examples/cluster/node.py`
- Modify: `README.md`

- [ ] **Step 1:** `node.py NAME PORT [SEED_PORT ...]` joins and prints member events; verify with three in-process systems through the smoke pattern, and typecheck.
- [ ] **Step 2:** README: "Cluster" Tour subsection (snippet verified), names-table rows (`Cluster(system).manager ! Join(addr)` → `Cluster(system).manager.tell(Join(addr))`; `Cluster(system).subscriptions ! Subscribe(ref, classOf[MemberEvent])` → `Subscribe(ref, MemberEvent)`; `pekko.cluster.seed-nodes` → `ClusterSettings(seed_nodes=...)`; failure detector settings → `ClusterSettings` fields), deviations from the spec, examples row.
- [ ] **Step 3:** `uv run pytest -q`, `uv run pyright` → all pass, 0 errors. Report ready to commit.

## Self-review notes

- Spec coverage: model (1), failure detection (2), daemon and events (3), heartbeats/reachability/downing/incarnations (4), docs (5). Receptionist deliberately absent.
- Names: `Address`, `UniqueAddress.address`, `MemberStatus`, `Member`, `VectorClock`, `Reachability`, `Gossip`, `MembershipState`, `GossipTargetSelector`, `PhiAccrualFailureDetector`, `FailureDetectorRegistry`, `HeartbeatNodeRing`, `HeartbeatSenderState`, `ClusterSettings`, `Cluster`, commands/subscriptions/events as the spec.
- Review Focus lines each name a test and task.
