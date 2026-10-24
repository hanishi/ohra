# Cluster Singleton design (cluster layer 4)

Apache Pekko's ClusterSingletonManager and ClusterSingletonProxy, read from
`cluster-tools/.../singleton/` and the Typed `ClusterSingleton` in `cluster-typed/`, ported state for state.
One instance of an actor runs on the oldest member; when that member leaves, the instance is handed over to the
next oldest; a proxy on every node forwards to wherever it runs. Narrowed by dropping the lease, roles, data
centers and the full-cluster-shutdown states, and by replacing `Identify` and remote DeathWatch, which pekko-py
does not have. Designed with the user on 2026-10-24; the next layer, sharding across nodes, builds on it.

## API (`pekko_py/cluster/singleton.py`)

- `ClusterSingletonSettings` (frozen dataclass): `removal_margin: float = 0.0`, `hand_over_retry_interval: float =
  1.0`, `min_number_of_hand_over_retries: int = 15`, `singleton_identification_interval: float = 1.0`,
  `buffer_size: int = 1000` (0 disables buffering; at most 10000). Pekko's names and defaults.
- `SingletonActor[M](behavior: Behavior[M], name: str)` with `with_stop_message(msg: M)` and
  `with_settings(settings)`; frozen, as Pekko's.
- `ClusterSingleton(system)`: the extension, created on first use like `Cluster(system)`; raises if the system has
  no cluster. `init(singleton: SingletonActor[M]) -> ActorRef[M]` spawns the manager at
  `/system/singletonManager<name>` (the instance is its child `singleton`) and the proxy at
  `/system/singletonProxy<name>`, both under the existing `/system` root, and returns the proxy ref. A second
  `init` with the same name returns the same proxy.
- Without a stop message the manager stops the instance with `ctx.stop(child, after_pending=True)`, which is
  Pekko's PoisonPill: pending messages are processed first.

## The manager (ClusterSingletonManager)

- States as Pekko's, each a returned behavior: `Start`, `Younger(previous_oldest)`,
  `BecomingOldest(previous_oldest)`, `Oldest(instance | None)`, `WasOldest(instance | None, new_oldest | None)`,
  `HandingOver(instance, hand_over_to | None)`, `Stopping(instance)`, `End`.
- Members by age: the manager subscribes to `ClusterDomainEvent` through `Cluster(system).subscriptions`. Once it
  has seen `SelfUp` it builds the age order (members with an up number, ordered by `Member.is_older_than`) from
  `Cluster(system).state`, computes Pekko's `InitialOldestState(oldest list up to and including self,
  safe_to_be_oldest)` where `safe_to_be_oldest` is false if any of those is Down, Exiting or Leaving, and from
  then on turns `MemberUp`, `MemberExited` (not self) and `MemberRemoved` into `OldestChanged(new oldest | None)`
  whenever the head of the order changes. Changes queue and are delivered one at a time: the manager takes the
  next only after it has handled the previous one (`get_next_oldest_changed`), as Pekko's OldestChangedBuffer.
- Wire messages between managers, `@remotable`, sent to the peer manager's path (`{address.root}/system/
  singletonManager<name>`), each carrying `from_: ActorRef[...]` since there is no implicit sender:
  `HandOverToMe(from_)`, `HandOverInProgress`, `HandOverDone(from_)`, `TakeOverFromMe(from_)`. Plus
  `IdentifySingleton(reply_to)` and `SingletonIdentity(instance: ActorRef | None)` for the proxy.
- Transitions, Pekko's exactly (ClusterSingletonManager.scala `when(...)` blocks): Start -> Oldest when self is
  the oldest and it is safe, BecomingOldest when self is oldest but not safe or a previous oldest is alive (sends
  `HandOverToMe`, retries every `hand_over_retry_interval` up to `max_hand_over_retries`), Younger otherwise;
  Younger -> BecomingOldest on `OldestChanged(self)`; BecomingOldest -> Oldest on `HandOverDone` from the
  expected previous oldest, or when every previous oldest has been removed (after `removal_margin`), or when the
  retries run out and all of them are removed; Oldest -> WasOldest on `OldestChanged(other)` (sends
  `TakeOverFromMe`, retries up to `max_take_over_retries`), -> HandingOver on `HandOverToMe`, -> Stopping on
  self Down; WasOldest -> HandingOver on `HandOverToMe` or when the new oldest is removed; HandingOver stops the
  instance, replies `HandOverInProgress` to repeated `HandOverToMe`, and on the instance's `Terminated` sends
  `HandOverDone` and goes to End (handed to someone) or Younger (nobody to hand to); Stopping and End stop the
  manager on the instance's termination or self removal. `max_hand_over_retries = max(min_number_of_hand_over_retries,
  removal_margin / hand_over_retry_interval + 3)`, `max_take_over_retries = max(1, max_hand_over_retries - 3)`.
  Retries exhausted with the previous oldest still a member raise `ClusterSingletonManagerIsStuck`, which fails
  the manager, as Pekko.
- `removed`: unique addresses seen in `MemberRemoved`, kept 15 minutes and pruned by a one-minute cleanup
  timer, as Pekko's.
- The instance dying under the manager is noted (`instance = None`) and not restarted, as Pekko; users wrap the
  behavior in `Behaviors.supervise` for restarts.

## The exiting phase (a change to `pekko_py/cluster/cluster.py`)

- `Cluster.add_exiting_task(fn: Callable[[], Awaitable[None]]) -> None`. When the core daemon learns that self is
  Exiting it runs every registered task concurrently with `ClusterSettings.exiting_timeout: float = 10.0`
  (Pekko's cluster-exiting phase timeout), through `ctx.pipe_to_self`, and only then sends itself
  `_ExitingCompleted`. A task that fails or times out is logged and the exit proceeds. The README's deviation
  "an Exiting node confirms and shuts down at once" is removed.
- The manager registers one task at start: it tells itself `SelfExiting` (which removes self from the age
  order and marks `self_exited`, as Pekko's `singleton-exiting-1/2`) and awaits `member_exiting_progress`, a
  future completed in `hand_over_done` or immediately when the manager never held the instance (Pekko's
  `wait-singleton-exiting`).

## The proxy (ClusterSingletonProxy)

- Keeps the Up members by age from `MemberUp`, `MemberExited`, `MemberRemoved`; when the head changes it forgets
  the instance and starts identifying: every `singleton_identification_interval` it sends
  `IdentifySingleton(ctx.self)` to the manager on the oldest member. `SingletonIdentity(instance)` with a ref
  sets the instance and flushes the buffer; `None` is ignored while searching.
- Messages while the instance is unknown are buffered up to `buffer_size`, dropping the oldest when full, or
  dropped to dead letters when `buffer_size` is 0, as Pekko.
- In place of remote DeathWatch: the proxy keeps identifying at the same interval after it has found the
  instance; an answer of `None` makes it forget the ref and buffer again. The manager answers with the instance
  whenever it holds a live one (Oldest, WasOldest, HandingOver, Stopping), which is what `Identify` to the
  instance's path yields in Pekko.
- Stops itself on self `MemberRemoved`, as Pekko.

## Invariants

- Never two instances: a new oldest starts the instance only after `HandOverDone` from the previous oldest or
  after every previous oldest is Removed (plus `removal_margin`).
- The manager is driven only by cluster events and the hand-over messages; the proxy only by cluster events and
  identities. Both stop with the system root; timers are the actors' own.
- One manager and one proxy per singleton name per system.

## Deviations to document

No lease, no roles, no data centers, no `PreparingForShutdown` / `ReadyForShutdown` handling. The oldest-changed
tracker is state in the manager rather than a child actor. `IdentifySingleton` to the manager plus periodic
re-identification replaces `Identify` to the instance and remote DeathWatch. `Cluster.add_exiting_task` and
`exiting_timeout` replace CoordinatedShutdown's cluster-exiting phase. Hand-over messages carry an explicit
`from_` ref.

## Tests

Pure: `max_hand_over_retries` / `max_take_over_retries` arithmetic; the age order and `OldestChanged` queueing
from a sequence of member events; `safe_to_be_oldest`. Multi-node, in one process at millisecond settings
(`tests/cluster/test_singleton.py`): the instance starts on the first seed and a proxy on the third node reaches
it by `ask`; messages sent through a proxy before identification arrive after it; the oldest leaves and the
instance moves to the next oldest with no overlap, checked by an instance behavior that appends `("start" |
"stop", node, time)` to a shared list; the oldest crashes, is downed, and the instance starts on the next oldest
only after the old member is removed; a younger node leaving does not move it; `buffer_size` drops the oldest
buffered message when full; a leaver whose successor never answers exits after `exiting_timeout`; `init` twice
returns the same proxy; every test under the conftest leak check.

## Example and docs

`examples/cluster/node.py` gains a singleton counter: every node calls `ClusterSingleton(system).init(...)` for
the same name and sends it through the proxy; the output shows which node hosts it and that it moves when that
node stops. README: a "Cluster singleton" Tour subsection, names-table rows (`ClusterSingleton(system).init(
SingletonActor(behavior, "name"))` -> same; `ClusterSingletonSettings` fields; `withStopMessage` ->
`with_stop_message`), the deviations above, the exiting-phase correction in the Cluster deviations bullet, the
examples row.
