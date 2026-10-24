# Cluster Sharding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Entities addressable by id from any node, with a ShardCoordinator singleton allocating shards to regions, rebalancing on join, and handing shards off on a graceful leave, as Apache Pekko's ClusterSharding.

**Architecture:** `sharding.py` gains a Shard actor between region and entities (region routes by shard id, Shard hosts entities, passivation and buffering move into it), so one implementation serves the single-node region and the cluster region. `cluster/sharding.py` adds the pure allocation strategy and coordinator state, the coordinator run as a cluster singleton instance that rebuilds its map from the regions' registrations, the cluster region, hand-off workers, and the `ClusterSharding(system)` extension.

**Tech Stack:** Python 3.12, asyncio, `pekko_py` runtime and `cluster/` package (membership, singleton), pytest, pyright strict.

**Spec:** `docs/superpowers/specs/2026-10-24-cluster-sharding-design.md`

## Global Constraints

- Public names are Pekko's in snake_case; every intentional deviation goes in the README "Known deviations" list.
- Python 3.12 generics; frozen dataclasses for messages; `match` for dispatch; behaviors are values, state in closures or helper objects created in `setup`.
- `uv run pytest -q` green (every test fails if a task outlives it; `tests/test_sharding.py` must stay green throughout); `uv run pyright` 0 errors.
- Commits only when the user asks: no commit steps below.
- `ClusterShardingSettings` fields and defaults exactly as the spec: `number_of_shards=1000`, `retry_interval=2.0`, `rebalance_interval=10.0`, `handoff_timeout=60.0`, `shard_start_timeout=10.0`, `coordinator_failure_backoff=5.0`, `buffer_size=100_000`, `rebalance_absolute_limit=0`, `rebalance_relative_limit=0.1`, `passivate_after=None`, plus `shard_region_query_timeout=3.0` (Pekko's `shard-region-query-timeout`).
- Paths: region `/system/sharding/<type>`, Shard `<region>/<shardId>`, entity `<shard>/<escaped id>`; coordinator singleton name `<type>Coordinator`, so its instance is at `/system/singletonManager<type>Coordinator/singleton`.
- Shard id: `str(zlib.crc32(entity_id.encode()) % number_of_shards)`.

## Review Focus

- A message for a shard that is between `BeginHandOff` and the next `ShardHome` must be delivered after the move, never lost. Task 4, `test_messages_sent_during_a_rebalance_all_arrive`.
- A shard hosted by an unreachable but not yet downed node must not be allocated elsewhere; after `Down` and removal it must be. Task 3, `test_shards_of_an_unreachable_node_wait_for_its_removal`.
- When the coordinator moves, a region's existing entities must keep their state and their shard must not be re-hosted elsewhere. Task 3, `test_coordinator_leaving_keeps_entities_where_they_are`.
- A region whose buffer is full must dead-letter the new message, not fail. Task 3, `test_region_buffer_is_bounded`.
- Two entity types on the same cluster must have independent coordinators and shards. Task 3, `test_two_entity_types_side_by_side`.

---

### Task 1: The Shard actor under the single-node region

**Files:**
- Modify: `src/pekko_py/sharding.py`
- Test: `tests/test_sharding.py` (existing tests unchanged and green), new tests appended there

**Interfaces:**
- Produces, in `pekko_py.sharding`:
  - `ClusterShardingSettings` (frozen dataclass; fields per Global Constraints; `__post_init__` validates intervals and timeouts `> 0`, `number_of_shards >= 1`, `buffer_size >= 1`, `rebalance_absolute_limit >= 0`, `0 < rebalance_relative_limit <= 1`, `passivate_after` `> 0` or `None`).
  - `shard_id_of(entity_id: str, number_of_shards: int) -> str`.
  - `Entity.settings: ClusterShardingSettings | None = None` and `Entity.with_settings(settings) -> Entity[T]`; `Entity.effective_settings -> ClusterShardingSettings` (the given settings, else defaults with this entity's `passivate_after` and `buffer_size`).
  - `shard_behavior(entity: Entity[T], shard_id: str) -> Behavior[_ShardCmd[T]]` where `_ShardCmd[T] = ShardingEnvelope[T] | ShardCommand | HandOff | GetShardState | _IdleTick`: the Shard hosts entities of one shard id, with today's region logic moved in unchanged (create on first message, `Passivate`, idle tick, per-entity buffering bounded by `buffer_size`). `EntityContext.shard` is the Shard's ref.
  - `HandOff(shard: str, reply_to: ActorRef[ShardStopped])`, `ShardStopped(shard: str)` (both `@remotable`; defined here because the Shard handles them): the Shard stops every entity with the stop message or `ctx.stop(child, after_pending=True)`, waits up to `max(handoff_timeout - 5, 1)` seconds (Pekko's `entityHandOffTimeout`) then stops stragglers, replies `ShardStopped`, and stops. Messages arriving during hand-off are dead letters.
  - `GetShardState(reply_to: ActorRef[CurrentShardState])`, `CurrentShardState(shard: str, entity_ids: frozenset[str])`.
  - `Sharding.init(entity)`: the region now routes `ShardingEnvelope` to the Shard for `shard_id_of(entity_id, settings.number_of_shards)`, spawning it on first use as child `<shardId>`; `GetShardRegionState` is answered by an anonymous aggregator child that asks every Shard `GetShardState` and replies the union when all have answered or after `shard_region_query_timeout`.
- `EntityRef.path` stays `<region path>/<escaped id>` (a logical path used for equality); the actor's real path is `<region>/<shardId>/<escaped id>`.

- [ ] **Step 1: Write the failing tests** appended to `tests/test_sharding.py`:

```python
def test_shard_id_is_stable_and_bounded()              # shard_id_of("cart-42", 10) == str(zlib.crc32(b"cart-42") % 10); same twice; all ids land in range(10)
async def test_entities_live_under_shard_actors()        # after a message to "a": system.resolve(f"{region.path}/{shard_id_of('a', n)}/a") is the entity's ref; the region has one child per touched shard
async def test_region_state_aggregates_over_shards()     # entities in two different shards: GetShardRegionState lists both ids
async def test_hand_off_stops_a_shard_and_its_entities(caplog)  # HandOff(shard, probe) -> ShardStopped(shard); the entity got its stop message (with_stop_message) and stopped; the shard child is gone
async def test_hand_off_times_out_on_an_entity_that_ignores_its_stop_message()  # handoff_timeout=6 via settings -> entity timeout 1s; ShardStopped arrives after about 1s; entity stopped
def test_cluster_sharding_settings_are_validated()      # number_of_shards=0, buffer_size=0, rebalance_relative_limit=0, handoff_timeout=0 raise ValueError
```

- [ ] **Step 2: Run to verify they fail** — `uv run pytest -q tests/test_sharding.py -k "shard_id or shard_actors or aggregates or hand_off or settings_are_validated"` → `ImportError` for `shard_id_of` / `ClusterShardingSettings`.
- [ ] **Step 3: Implement.** Move the entity-hosting closure state of today's `Sharding.init` into `shard_behavior`; the region keeps `shards: dict[str, ActorRef]`, routes, and aggregates. Keep every existing test's observable behaviour (the stale-incarnation `Passivate` check, the passivation buffer bound and dead letters at region stop now happen in the Shard; the region's `PostStop` no longer needs them).
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright` → all pass, 0 errors.

### Task 2: Allocation strategy and coordinator state (pure)

**Files:**
- Create: `src/pekko_py/cluster/sharding.py`
- Test: `tests/cluster/test_shard_allocation.py`

**Interfaces:**
- Produces, in `pekko_py.cluster.sharding`:
  - `RegionEntry(region: str, member: Member, shard_ids: tuple[str, ...])` and `LeastShardAllocationStrategy(absolute_limit: int, relative_limit: float)` with `allocate_shard(shard: str, allocations: dict[str, tuple[str, ...]], members_by_region: dict[str, Member]) -> str | None` (fewest shards, ties by `Member.is_older_than`; `None` when no regions) and `rebalance(allocations, members_by_region, rebalance_in_progress: frozenset[str], cluster_state: CurrentClusterState) -> frozenset[str]` (Pekko's two phases with `limit(n) = max(1, min(int(relative_limit * n), absolute_limit if absolute_limit > 0 else n))`; empty when a rebalance is in progress or when `is_a_good_time_to_rebalance` is false: any member unreachable, or any member Joining or WeaklyUp). Regions are keyed by path string.
  - `CoordinatorState(shards: dict[str, str], regions: dict[str, tuple[str, ...]])` frozen, with `updated(event) -> CoordinatorState` for `ShardRegionRegistered(region)`, `ShardRegionTerminated(region)`, `ShardHomeAllocated(shard, region)`, `ShardHomeDeallocated(shard)`, raising `ValueError` on the conditions Pekko's `require`s check.

- [ ] **Step 1: Write the failing tests**:

```python
def test_allocate_picks_the_region_with_fewest_shards_oldest_on_ties()
def test_rebalance_phase_one_moves_shards_from_regions_above_optimal()   # 3 regions with 5,1,0 shards (6 shards, optimal 2): phase one selects 3 of region A's, capped by limit: relative 0.1 -> max(1, 0) = 1 shard
def test_rebalance_phase_two_fills_regions_below_optimal()               # 2,2,0 (optimal 2): phase one empty; phase two selects 1 (countBelowOptimal = 1)
def test_rebalance_limits()                                              # absolute_limit=2 with relative 1.0 -> 2; absolute 0 -> relative only
def test_rebalance_is_skipped_while_in_progress_or_not_a_good_time()     # in_progress non-empty; an unreachable member; a joining member
def test_coordinator_state_events()                                      # the four events; registering twice, allocating an unregistered region, deallocating an unknown shard raise ValueError; terminating a region frees its shards
```

- [ ] **Step 2: Run to verify they fail** — `ModuleNotFoundError: pekko_py.cluster.sharding`.
- [ ] **Step 3: Implement** the pure parts only.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright`.

### Task 3: Coordinator, cluster region and the extension (allocation on demand, state rebuild)

**Files:**
- Modify: `src/pekko_py/cluster/sharding.py`, `src/pekko_py/system.py` (`cluster_sharding: ClusterSharding | None = None`), `src/pekko_py/__init__.py` (export `ClusterSharding`, `ClusterShardingSettings`)
- Test: `tests/cluster/test_cluster_sharding.py`

**Interfaces:**
- Consumes: Task 1's `shard_behavior`, `HandOff`, `ShardStopped`, `shard_id_of`, `Entity.effective_settings`; Task 2's strategy and state; `ClusterSingleton(system).init(SingletonActor(...))`; `Cluster(system)` (`state`, `subscriptions`, `add_exiting_task`, `self_unique_address`); `system.resolve`.
- Produces: wire messages, all `@remotable`, with explicit refs where Pekko uses `sender()`: `Register(region: ActorRef, hosted_shards: tuple[str, ...])`, `RegisterAck(coordinator: ActorRef)`, `GetShardHome(shard: str, reply_to: ActorRef)`, `ShardHome(shard: str, region: ActorRef)`, `ShardHomes(homes: tuple[tuple[ActorRef, tuple[str, ...]], ...])`, `HostShard(shard: str, reply_to: ActorRef)`, `ShardStarted(shard: str)`, `BeginHandOff(shard: str, reply_to: ActorRef)`, `BeginHandOffAck(shard: str, from_: ActorRef)`, `GracefulShutdownReq(region: ActorRef)`; `ClusterSharding` with `__new__(cls, system)` (raises `RuntimeError` without a cluster), `init[M](self, entity: Entity[M]) -> ActorRef[ShardingEnvelope[M]]` (idempotent per type key; spawns `/system/sharding` once, the region under it, and `ClusterSingleton(system).init(SingletonActor(_coordinator(...), f"{type}Coordinator"))`), `entity_ref_for[M](self, key: EntityTypeKey[M], entity_id: str) -> EntityRef[M]` (raises `LookupError` if `init` was not called for the key on this node); `_coordinator(type_name, settings) -> Behavior`, `_region(entity, settings) -> Behavior`.

- [ ] **Step 1: Write the failing tests** with `cluster_node` from `tests/cluster/test_cluster.py` (`exiting_timeout=2.0`), `FAST_SHARDING = ClusterShardingSettings(number_of_shards=10, retry_interval=0.05, rebalance_interval=0.1, handoff_timeout=2.0, shard_start_timeout=0.2, coordinator_failure_backoff=0.1, shard_region_query_timeout=1.0)`, a `@remotable Add(n: int, reply_to: ActorRef[int])` message and a `counter` entity that replies its running total, and `start(system) -> ActorRef[ShardingEnvelope[Add]]` calling `ClusterSharding(system).init(Entity(COUNTER, counter).with_settings(FAST_SHARDING))`:

```python
async def test_entity_refs_on_three_nodes_reach_one_entity()        # Add(1) via each node's entity_ref_for("x") -> totals 1, 2, 3
async def test_entity_ref_for_before_registration_buffers_and_delivers()  # init on a joining node and ask at once: reply arrives
async def test_entities_are_spread_across_regions()                  # 30 ids over 3 nodes at number_of_shards=10: every node hosts at least one shard (GetShardRegionState per node)
async def test_two_entity_types_side_by_side()                       # a second Entity type key with its own coordinator; totals independent
async def test_region_buffer_is_bounded(caplog)                      # buffer_size=2 via settings; drop GetShardHome frames at the coordinator's node with the _handle filter so homes stay unknown; three tells from another node to ids in unknown shards: one dead letter logged, the region survives (restore the filter; a later ask is answered)
async def test_coordinator_leaving_keeps_entities_where_they_are()   # 3 nodes; totals on ids living on b and c; Leave(a) (the coordinator host); afterwards Add(0) returns the same totals (same entities) and a new id is allocated
async def test_coordinator_crash_is_recovered_after_removal()        # crash(a) + Down(a); existing entities on b/c keep totals; a new id is allocated after the new coordinator is up
async def test_shards_of_an_unreachable_node_wait_for_its_removal()  # crash(c) (a shard host); an Add to its id from a: no reply within 0.5 s (buffered); Down(c) -> the reply arrives from a fresh entity (total restarts)
```

- [ ] **Step 2: Run to verify they fail** — `ImportError: ClusterSharding`.
- [ ] **Step 3: Implement.**
  - `_Coordinator` helper in `setup` (as `_CoreDaemon`): `state: CoordinatorState`, `refs: dict[str, ActorRef]` (region path → ref), `alive_regions`, `graceful_shutdown_in_progress`, `region_termination_in_progress`, `un_acked_host_shards` (single timers keyed by shard), `rebalance_in_progress: dict[str, list[ActorRef]]` (deferred askers), `expected_registrations` while waiting. Subscribes to `ClusterDomainEvent`. States as behaviors: `waiting_for_state` (accept `Register`, apply `ShardRegionRegistered` + `ShardHomeAllocated` for each reported shard, reply `RegisterAck` and `ShardHomes`; defer `GetShardHome` for unknown shards; become `active` when every reachable Up/Leaving/Exiting member's region has registered and no unreachable member is unregistered, or when `retry_interval` has elapsed and only reachable members are missing; then replay deferred requests) and `active` (Pekko's `active`: `Register` as above, `GetShardHome` through `handle_get_shard_home` and `continue_get_shard_home` with `allocate_shard`, `HostShard` with resend every `shard_start_timeout` until `ShardStarted`, `GracefulShutdownReq`, `RebalanceTick`/`RebalanceDone` wired in Task 4). `MemberRemoved(m)` terminates regions at that address: `ShardRegionTerminated`, `GetShardHome` to self for each of its shards is NOT sent (no remembered entities), askers get a fresh allocation on their next request. `isMember`: the region's address is a current Up/Leaving/Exiting member or self.
  - `_Region` helper: `members_by_age`, `coordinator: ActorRef | None`, `shards: dict[str, ActorRef]` (local Shards from `shard_behavior`), `region_by_shard: dict[str, ActorRef]`, `shard_buffers: dict[str, deque[ShardingEnvelope]]` with the total bound and dead letters beyond it, registration with the doubling delay from `max(0.1, retry_interval / 8)` to `retry_interval` and a `Retry` tick every `retry_interval` that re-registers or re-requests homes for buffered shards; `HostShard`, `ShardHome`, `ShardHomes`, `RegisterAck`, `BeginHandOff`, `HandOff` (drop that shard's buffer, forward to the local Shard, or reply `ShardStopped` itself) as the spec; on `MemberRemoved(m)` forget homes at that address and, when the oldest changed, `coordinator = None` and re-register. `GetShardRegionState` aggregated as in Task 1. Stops on self `MemberRemoved`/`MemberDowned`.
  - `ClusterSharding.init`: spawn `/system/sharding` (`Behaviors.empty`) once, the region as its child `<type>`, the coordinator singleton; `entity_ref_for` returns `EntityRef(region_ref, id)`.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q` three times, `uv run pyright`.

### Task 4: Rebalance, hand-off and graceful leave

**Files:**
- Modify: `src/pekko_py/cluster/sharding.py`
- Test: `tests/cluster/test_cluster_sharding.py`

**Interfaces:**
- Consumes: Task 3's coordinator and region; Task 2's `rebalance`; Task 1's `HandOff`/`ShardStopped`; `Cluster.add_exiting_task`.
- Produces: `_RebalanceWorker(shard, from_region, regions, is_rebalance)` child behavior of the coordinator (`BeginHandOff` to all regions, `BeginHandOffAck` or a region's termination counts, then `HandOff` to the host, `ShardStopped` → `_RebalanceDone(shard, ok=True)`, timeout `handoff_timeout` → `ok=False`); coordinator `RebalanceTick` every `rebalance_interval` → `continue_rebalance`; `RebalanceDone` → `ShardHomeDeallocated`, replay deferred askers; `GracefulShutdownReq` → `shutdown_shards(region, its shards)`; the region's exiting task (`SelfExiting` → `graceful_shutdown_in_progress`, `GracefulShutdownReq` re-sent on each `Retry` tick, task completes when `shards` and `shard_buffers` are empty or at the exiting timeout).

- [ ] **Step 1: Write the failing tests**:

```python
async def test_a_joining_node_receives_shards_within_a_rebalance_interval()   # 2 nodes host 10 shards; a third joins; within 1 s it hosts >= 1 shard; per tick at most limit(10)=max(1, int(0.1*10))=1 shard moves (observe GetShardRegionState deltas)
async def test_rebalance_limits_are_respected()                               # rebalance_relative_limit=0.5: 5 shards move in one tick at most
async def test_messages_sent_during_a_rebalance_all_arrive()                  # a sender loop of Add(1) to 20 ids through node a while c joins and shards move; final totals sum to the number sent
async def test_graceful_leave_hands_shards_off_and_loses_nothing()           # Add(1) loop through a and c to ids on b while Leave(b); b exits only after its shards are stopped; totals sum to sent; b's shards are now on a or c
async def test_hand_off_stops_an_entity_that_ignores_its_stop_message_at_the_timeout()  # handoff_timeout=6 -> entity timeout 1 s; Leave(host) completes in about 1-2 s
async def test_region_exits_at_the_exiting_timeout_when_the_coordinator_never_answers()  # drop GracefulShutdownReq frames at the coordinator's node; exiting_timeout=0.5; the leaver terminates within 2 s
```

- [ ] **Step 2: Run to verify they fail** — the join test: the third node hosts no shard after 1 s; the leave test: totals short or b never exits.
- [ ] **Step 3: Implement** the worker, the tick, graceful shutdown and the exiting task, porting `continueRebalance`, `startShardRebalanceIfNeeded`, `shutdownShards`, `RebalanceDone`, `clearRebalanceInProgress` and the region's `GracefulShutdown`/`tryCompleteGracefulShutdownIfInProgress`.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q` three times, `uv run pyright`.

### Task 5: Example and README

**Files:**
- Modify: `examples/cluster/node.py`, `README.md`

- [ ] **Step 1: Example.** Each node calls `ClusterSharding(system).init(Entity(COUNTER, counter))` for a sharded counter type and, every second, sends `Add(1)` to three fixed ids through `entity_ref_for`, printing the reply with the hosting node's port (the entity includes it); the output shows ids spreading over nodes as they join and surviving a node's ctrl-c. Verify with three in-process systems through the smoke pattern at default settings (expect a shard to move within `rebalance_interval` after the third node joins), `uv run pyright examples/cluster/node.py`.
- [ ] **Step 2: README.** "Sharding across nodes" Tour subsection after the singleton (snippet verified by running); the existing Sharding section notes the Shard actor and that the single-node region is the same code; names rows (`ClusterSharding(system).init(Entity(key, create))` → same; `entityRefFor(key, id)` → `entity_ref_for(key, id)`; `ClusterShardingSettings` fields; `HashCodeMessageExtractor(n)` → `number_of_shards`); the deviations bullet from the spec; the Sharding deviations bullet rewritten; examples row.
- [ ] **Step 3:** `uv run pytest -q`, `uv run pyright` → all pass, 0 errors. Report ready to commit.

## Self-review notes

- Spec coverage: Shard actor and settings (1), strategy and state (2), coordinator/region/extension/state rebuild (3), rebalance/hand-off/graceful leave (4), example and docs (5).
- Names used across tasks match: `shard_behavior`, `HandOff`/`ShardStopped` from Task 1; `LeastShardAllocationStrategy`, `CoordinatorState` and its events from Task 2; the wire messages and `ClusterSharding` from Task 3; `_RebalanceWorker` in Task 4.
- Ruling carried into Task 3: Pekko's `stateInitialized` re-sends `HostShard` for every known shard; with the map rebuilt from the regions' own reports that is redundant and is not ported.
- Review Focus lines each name a test and its task.
