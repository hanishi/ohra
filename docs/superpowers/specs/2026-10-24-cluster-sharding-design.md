# Cluster sharding design (cluster layer 5)

Apache Pekko's ClusterSharding across nodes, read from `cluster-sharding/` and `cluster-sharding-typed/` in the local
Pekko source: a ShardCoordinator singleton that decides which region hosts each shard, a ShardRegion on every node
that routes by shard id and buffers while a home is unknown, a Shard actor per hosted shard holding its entities,
the LeastShardAllocationStrategy with Pekko's rebalance limits, hand-off of shards for rebalancing and for a leaving
node. Narrowed by dropping remembered entities, proxy-only regions, stats queries, custom allocation strategies,
lease, roles, data centers and the passivation strategies beyond an idle timeout. The one structural narrowing is
coordinator state: Pekko keeps it in distributed data; here a new coordinator incarnation rebuilds it from the
regions' registrations. Designed with the user on 2026-10-24; builds on membership, the Receptionist and the
singleton.

## API (`pekko_py/cluster/sharding.py`)

- `ClusterShardingSettings` (frozen dataclass, seconds): `number_of_shards: int = 1000`, `retry_interval: float =
  2.0`, `rebalance_interval: float = 10.0`, `handoff_timeout: float = 60.0`, `shard_start_timeout: float = 10.0`,
  `coordinator_failure_backoff: float = 5.0`, `buffer_size: int = 100_000`, `rebalance_absolute_limit: int = 0` (0
  means no absolute cap, as Pekko), `rebalance_relative_limit: float = 0.1`, `passivate_after: float | None = None`.
  Validated in `__post_init__` like `ClusterSettings`.
- `ClusterSharding(system)`: the extension, created on first use; raises `RuntimeError` without a cluster.
  `init(entity: Entity[M]) -> ActorRef[ShardingEnvelope[M]]` spawns the region at `/system/sharding/<type>` and
  starts the coordinator singleton named `<type>Coordinator` through `ClusterSingleton(system)`, idempotently per
  type key, and returns the region ref. `entity_ref_for(key: EntityTypeKey[M], entity_id: str) -> EntityRef[M]`
  returns a ref over that node's region. The existing `Entity`, `EntityTypeKey`, `EntityContext`, `EntityRef`,
  `ShardingEnvelope`, `Passivate` and `GetShardRegionState` from `pekko_py/sharding.py` are reused; `Entity` gains
  `with_settings(settings: ClusterShardingSettings)`; its `passivate_after` and `buffer_size` apply when no
  settings are given.
- `Sharding.init` (single node) keeps its API and behaviour; it shares the Shard actor with the cluster region.
- Shard id: `str(zlib.crc32(entity_id.encode()) % number_of_shards)`, Pekko's HashCodeMessageExtractor with a
  stable hash.

## Region (ShardRegion)

- State: `members_by_age` (Up, Leaving and Exiting members, as Pekko's `memberStatusOfInterest`), `coordinator:
  ActorRef | None`, `shards: dict[ShardId, ActorRef]` (local Shard actors), `region_by_shard: dict[ShardId,
  ActorRef]` (local region ref or a `RemoteRef` to another region), `shard_buffers: dict[ShardId, deque]` bounded by
  `buffer_size` in total, `graceful_shutdown_in_progress`.
- Registration: when the oldest member changes, `coordinator = None` and `Register(region, hosted_shards)` is sent
  to the coordinator path on the oldest member, retried with Pekko's doubling delay from
  `max(0.1, retry_interval / 8)` up to `retry_interval`, then on every `Retry` tick while unregistered. `RegisterAck`
  sets the coordinator and requests homes for every buffered shard. The coordinator path on a node is
  `/system/sharding/<type>Coordinator/singleton` (the singleton manager's child); only the manager on the oldest
  node holds it, as Pekko's `coordinatorSelection`.
- Delivery: a `ShardingEnvelope` whose shard has a local home goes to the Shard; a remote home goes to that region;
  otherwise it is buffered and `GetShardHome(shard)` is sent, resent on the `Retry` tick while buffered.
  `HostShard(shard)` starts a local Shard, records it, replies `ShardStarted`; `ShardHome(shard, region)` records
  the home, watches nothing (membership removes dead regions), and flushes that shard's buffer; `ShardHomes` the
  same for a batch on registration. `BeginHandOff(shard)` forgets the shard's home and replies `BeginHandOffAck`;
  `HandOff(shard)` drops that shard's buffered messages (Pekko: to avoid reordering), forwards to the local Shard,
  and replies `ShardStopped` itself when it hosts no such Shard. A `MemberRemoved` for a node whose region hosts
  shards forgets those homes.
- Graceful leave: the region registers an exiting task; on `SelfExiting` it sets `graceful_shutdown_in_progress`,
  sends `GracefulShutdownReq(region)` to the coordinator (re-sent on every `Retry` tick until done), and the task
  completes when it has no Shards and no buffered messages, or at the exiting timeout. A `HostShard` during graceful
  shutdown is answered with another `GracefulShutdownReq`, as Pekko.
- Stops itself on self `MemberRemoved` or `MemberDowned`.

## Shard (Shard.scala, narrowed)

- One actor per hosted shard at `/system/sharding/<type>/<shardId>`, holding that shard's entities as children.
  Today's region logic for entities moves into it unchanged: create on first message, `Passivate`, the idle tick
  with `passivate_after`, buffering for a passivating entity bounded by `buffer_size`, `GetShardRegionState` answered
  for its entities (the region aggregates).
- `HandOff(shard)` from the region: a HandOffStopper stops every entity (stop message, or `ctx.stop(child,
  after_pending=True)`), waits for all of them up to `handoff_timeout`, then stops the stragglers, replies
  `ShardStopped(shard)` to the requester and the Shard stops. Messages that arrive during hand-off are dead letters,
  as Pekko.
- The single-node `Sharding.init` region spawns Shards the same way, so one implementation serves both.

## Coordinator (ShardCoordinator, as a singleton instance)

- `State(shards: dict[ShardId, ActorRef], regions: dict[ActorRef, tuple[ShardId, ...]])`, updated by Pekko's events
  `ShardRegionRegistered`, `ShardRegionTerminated`, `ShardHomeAllocated`, `ShardHomeDeallocated`.
- Starts in `waiting_for_state`: accepts `Register(region, hosted_shards)` from regions on current members,
  applying `ShardRegionRegistered` and `ShardHomeAllocated` for each reported shard, replying `RegisterAck` and
  `ShardHomes` of what it knows. It becomes `active` when every reachable member running this type has registered
  (the Receptionist is not involved: "running this type" means the member's region answered, and Pekko's
  `waiting-for-state-timeout`, here `retry_interval`, bounds the wait for members that never register, which are
  then treated as not running it) and no unreachable member remains unregistered. `GetShardHome` for a shard with no
  known home is deferred while waiting; `GetShardHome` for a known shard is always answered.
- `active`, Pekko's `active` receive: `Register` (a re-register gets `RegisterAck` and the current homes),
  `GetShardHome` (known home: `ShardHome`; else allocate through the strategy among regions not shutting down or
  terminating, `ShardHomeAllocated`, `HostShard` to the chosen region with resend every `shard_start_timeout`
  until `ShardStarted`, and `ShardHome` to the asker), `ShardStarted`, `RebalanceTick`, `RebalanceDone`,
  `GracefulShutdownReq(region)` (hand off all its shards), region termination.
- Region termination: a region whose member is removed, or whose `GracefulShutdownReq` completed and whose node
  left, has `ShardRegionTerminated` applied: its shards become unallocated and the next `GetShardHome` reallocates
  them. Membership events replace Pekko's remote DeathWatch of regions; a region on a Down-but-not-removed member
  keeps its shards, as in Pekko, so nothing is duplicated while the node might still run them.
- Allocation: `LeastShardAllocationStrategy(absolute_limit, relative_limit)` ported with `allocate_shard`
  (fewest shards, ties by oldest member) and `rebalance` (phase one: shards above `optimal_per_region`; phase two:
  fill regions below; both capped by `limit = max(1, min(relative_limit * number_of_shards, absolute_limit or
  inf))`; empty while a rebalance is in progress or when `is_a_good_time_to_rebalance` is false: an unreachable
  member or a member still joining).
- Rebalance: every `rebalance_interval` the coordinator asks the strategy; for each shard it spawns a
  RebalanceWorker child: `BeginHandOff` to all registered regions, wait for every `BeginHandOffAck` (a region
  terminating counts as acked), then `HandOff` to the hosting region, wait for `ShardStopped`, bounded by
  `handoff_timeout`; `RebalanceDone(ok)` applies `ShardHomeDeallocated` and sends itself `GetShardHome` so the shard
  is reallocated. The coordinator moving to another node abandons its workers with it; the new incarnation's
  rebuilt state reflects whatever the regions actually host.
- The coordinator does not Leave or Down anything; it only observes membership.

## Invariants

- A shard is hosted by at most one region at a time: a home is allocated only to a registered region, reallocated
  only after `ShardStopped` or after the hosting region's member is Removed, and a new coordinator allocates an
  unknown shard only after every reachable region has registered and no unreachable member is unaccounted for.
- No message is lost on a graceful leave: the leaving region hands every shard off before its exiting task
  completes, and other regions buffer for a shard between `BeginHandOff` and the next `ShardHome`.
- Every actor here is under `/system/sharding/<type>` or the singleton; timers are the actors' own; everything stops
  with the system root.

## Deviations to document

Coordinator state rebuilt from `Register(region, hosted_shards)` instead of distributed data or persistence, with a
waiting state gated on reachable members' registrations. Membership events instead of remote DeathWatch for regions
and the coordinator. No remembered entities, proxy-only regions, stats queries, custom allocation strategies,
lease, roles, data centers, passivation strategies beyond the idle timeout, or `StartEntity`. `number_of_shards`
and `passivate_after` are settings fields; the shard id hash is crc32. The coordinator path is
`/system/sharding/<type>Coordinator/singleton` rather than Pekko's `.../singleton/coordinator`.

## Tests

Pure (`tests/cluster/test_shard_allocation.py`): `allocate_shard` picks the least loaded, ties by age;
`rebalance` phase one and two with the absolute and relative limits, empty while in progress, while a member is
unreachable or joining; coordinator `State` event application and its `require`s. Multi-node at millisecond
settings (`tests/cluster/test_cluster_sharding.py`): entity refs on three nodes reach one entity wherever it is;
a node that joins receives shards within a rebalance interval, respecting the limits; a node leaves gracefully and
a stream of counted messages sent through all nodes during the leave arrives in full (the final count equals the
number sent); a node crashes, is downed, and its shards are served again after removal; the coordinator's node
leaves, then in another test crashes and is downed, and a new shard is allocated afterwards; an entity that ignores
its stop message is stopped at `handoff_timeout` and the hand-off completes; `entity_ref_for` before any
registration buffers and delivers; two entity types side by side; the single-node `Sharding.init` tests still pass
with the Shard actor underneath; every test under the leak check.

## Example and docs

`examples/cluster/node.py` gains a sharded counter per id: each node calls `ClusterSharding(system).init` for the
same entity type and sends to a few fixed ids through `entity_ref_for`; the output shows which node hosts each
id, shards moving when a node joins, and ids surviving a node's ctrl-c. README: a "Sharding across nodes" Tour
subsection after the singleton, names rows (`ClusterSharding(system).init(Entity(key, create))` → same;
`entityRefFor(key, id)` → `entity_ref_for(key, id)`; `ClusterShardingSettings` fields), the deviations above, the
Sharding section and deviations bullet updated to say the single-node region is the same code, the examples row.
