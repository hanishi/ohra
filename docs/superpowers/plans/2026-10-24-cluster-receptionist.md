# Receptionist Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apache Pekko's Receptionist on every ActorSystem, replicated across cluster nodes by owner rows.

**Architecture:** One actor at `/system/receptionist`, created on first `Receptionist(system)`, holding the local
registry (Pekko's `LocalReceptionist` semantics) and, when the system has a cluster, a table of versioned rows
keyed by owner `UniqueAddress` that it exchanges with the other members' receptionists and prunes from membership
events. Every `ActorSystem` gets the `/system` root so the actor has a home on plain systems too.

**Tech Stack:** Python 3.12, asyncio, the existing `pekko_py` runtime (`cell.py`, `remote.py`, `cluster/`), pytest, pyright strict.

**Spec:** `docs/superpowers/specs/2026-10-24-cluster-receptionist-design.md`

## Global Constraints

- Public names are Pekko's in snake_case; every intentional deviation goes in the README "Known deviations" list.
- Python 3.12 generics syntax; frozen dataclasses for messages; `match` for dispatch; behaviors are values, state in closures.
- `uv run pytest -q` green (every test fails if a task outlives it); `uv run pyright` 0 errors; `tests/typing_check.py` ignores stay necessary.
- Commits only when the user asks: no commit steps below.
- Only `LocalRef`s may register (Pekko's rule); anything else is logged and ignored.
- `ClusterSettings.receptionist_resend_interval: float = 1.0`, validated `> 0` like the other intervals.

## Review Focus

- A service registered under two keys that stops must vanish from both listings in one notification round. Task 1, `test_a_stopped_service_leaves_every_key`.
- A subscriber that subscribes to two keys and stops must be dropped from both (no dead letters). Task 1, `test_a_stopped_subscriber_is_dropped_from_every_key`.
- A row that arrives from a node before its `MemberUp` must be hidden, then shown once the member appears, without a resend. Task 2, `test_a_row_from_a_not_yet_member_is_hidden_until_it_joins`.
- Two concurrent registrations on two nodes must both be visible on a third within one resend interval. Task 2, `test_registrations_on_two_nodes_both_reach_a_third`.
- A `Listing` must be usable: the refs it carries on node B for A's services must deliver to A. Task 2, `test_find_on_another_node_returns_a_working_remote_ref`.

---

### Task 1: Receptionist on every system (local semantics)

**Files:**
- Create: `src/pekko_py/receptionist.py`
- Modify: `src/pekko_py/system.py` (the `/system` root for every system; `self.receptionist: Receptionist | None = None`), `src/pekko_py/__init__.py` (exports listed under Interfaces)
- Test: `tests/test_receptionist.py`

**Interfaces:**
- Produces, in `pekko_py.receptionist`:
  - `ServiceKey[T]`: `@dataclass(frozen=True) class ServiceKey[T]: id: str`.
  - `Register[T](key: ServiceKey[T], service: ActorRef[T], reply_to: ActorRef[Registered[T]] | None = None)`,
    `Registered[T](key, service_instance: ActorRef[T])`, `Deregister[T](key, service, reply_to: ActorRef[Deregistered[T]] | None = None)`,
    `Deregistered[T](key, service_instance)`, `Find[T](key, reply_to: ActorRef[Listing[T]])`, `Subscribe[T](key, subscriber: ActorRef[Listing[T]])`,
    `Listing[T](key, service_instances: frozenset[ActorRef[T]], all_service_instances: frozenset[ActorRef[T]], services_were_added_or_removed: bool)`.
  - `type Command = Register[Any] | Deregister[Any] | Find[Any] | Subscribe[Any]`.
  - `class Receptionist` with `__new__(cls, system) -> Receptionist` (creates on first use, stored on `system.receptionist`) and `ref: ActorRef[Command]`.
  - Internal, consumed by Task 2: the behavior factory `_receptionist(system) -> Behavior[Any]`, its closure state `registry: dict[ServiceKey[Any], set[ActorRef[Any]]]`, and `listing(key: ServiceKey[Any]) -> Listing[Any]`.
- Top-level `pekko_py` exports: `Receptionist`, `ServiceKey`, `Listing`, `Register`, `Deregister`, `Find`. The receptionist's `Subscribe` clashes with the cluster's `Subscribe`; export it only from `pekko_py.receptionist` and document that in the README names row (Pekko has the same two `Subscribe`s in different packages).

- [ ] **Step 1: Write the failing tests** in `tests/test_receptionist.py`, each on a plain `ActorSystem(Behaviors.ignore, "r")` (no address), using `ask` and a `FunctionRef` probe list as in `tests/cluster/test_cluster.py`:

```python
async def test_register_then_find_lists_the_service()            # Find -> Listing(key, {svc}, {svc}, True); Registered reply when reply_to given
async def test_subscribe_gets_current_listing_then_each_change()  # first Listing immediately (empty), then one per Register/Deregister of that key only
async def test_deregister_removes_one_binding()                   # two keys, deregister one: other key unchanged; Deregistered reply
async def test_a_stopped_service_leaves_every_key()               # service under two keys stops: both subscribers get a Listing without it
async def test_a_stopped_subscriber_is_dropped_from_every_key()   # caplog on pekko_py.dead_letters: no dead letter after it stops and a change happens
async def test_only_local_refs_can_register(caplog)               # Register(key, FunctionRef) and Register(key, system.resolve("pekko://x@127.0.0.1:1/user")) ignored; Find empty
async def test_receptionist_is_one_per_system_and_lives_under_system()  # Receptionist(s) is Receptionist(s); ref.path == "pekko://r/system/receptionist"; system.resolve(path) is the ref
async def test_plain_system_still_stops_cleanly()                 # a system that never used the receptionist, and one that did, both terminate (leak check)
```

- [ ] **Step 2: Run to verify they fail** — `uv run pytest -q tests/test_receptionist.py` → `ModuleNotFoundError: pekko_py.receptionist`.

- [ ] **Step 3: Implement.** In `system.py`, create `_system_root` for every system (move it out of the `if cluster` block; the `root` path differs by address as today) and add `self.receptionist`. In `receptionist.py`: the messages; `Receptionist.__new__` spawning `_receptionist(system)` into `system._system_root` with `spawn_child(..., "receptionist", None)` as `Cluster._start` does; the behavior as `Behaviors.setup` with closure state `services_per_actor: dict[ActorRef, set[ServiceKey]]`, `registry: dict[ServiceKey, set[ActorRef]]`, `subscribers: dict[ServiceKey, set[ActorRef]]`, `subscriptions_per_actor`, and `Behaviors.receive` over `Command | _ServiceTerminated(ref) | _SubscriberTerminated(ref)` delivered through `ctx.watch_with`. Port `onCommand`/`onInternal` of `LocalReceptionist.scala` line for line: watch on first registration, reply if `reply_to`, `update_services(keys)` notifies each key's subscribers with the listing; `Deregister` of an unknown binding is a no-op; unwatch when the service's last key goes. `listing(key)` builds `Listing(key, instances, instances, True)` on a plain system; Task 2 changes the two sets.

- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright` → all pass, 0 errors.

### Task 2: Replication across cluster nodes

**Files:**
- Modify: `src/pekko_py/receptionist.py`, `src/pekko_py/cluster/cluster.py` (`ClusterSettings.receptionist_resend_interval: float = 1.0`, in the `positive` validation tuple)
- Test: `tests/cluster/test_receptionist_cluster.py`

**Interfaces:**
- Consumes: Task 1's registry and `listing(key)`; `Cluster(system)` with `subscriptions`, `state` (`CurrentClusterState.members`, `.unreachable`), `self_unique_address`; cluster events `MemberEvent`, `MemberRemoved`, `UnreachableMember`, `ReachableMember`; `system.resolve(f"{address.root}/system/receptionist")`; `Serialization` of `ActorRef` fields.
- Produces: `@remotable @dataclass(frozen=True) class ReceptionistRow: owner: UniqueAddress; version: int; bindings: tuple[tuple[str, ActorRef[Any]], ...]` (key id, ref); internal tick `_ResendTick`; the receptionist's message union grows by `ReceptionistRow | ClusterDomainEvent | _ResendTick`.

- [ ] **Step 1: Write the failing tests** with the `cluster_node(name, port, seeds, **settings)` helper copied from `tests/cluster/test_cluster.py` (import it; add `receptionist_resend_interval=0.05` to its FAST settings) and a `greeter` service behavior that replies to `Greet(name, reply_to)` (a `@remotable` message defined in the test):

```python
async def test_find_on_another_node_returns_a_working_remote_ref()        # register on A; Find on B -> one RemoteRef; ask it Greet -> reply
async def test_subscriber_on_another_node_sees_the_service_appear_and_leave()  # Subscribe on B before A registers: Listing empty, then {A's}, then empty after the service stops
async def test_registrations_on_two_nodes_both_reach_a_third()           # A and B register at once; C's listing has both within 2 s
async def test_a_row_from_a_not_yet_member_is_hidden_until_it_joins()    # Receptionist(B).ref.tell(ReceptionistRow(owner=unknown uid, ...)) -> Find empty; then that node joins -> visible (use a real third node started after the row is injected with its real UniqueAddress: start C's system, read its address, inject, then let it join)
async def test_a_left_node_takes_its_services_with_it()                  # Leave(A): B's subscriber gets a Listing without A's service, flag True
async def test_a_crashed_node_keeps_services_in_all_instances_until_downed()  # crash A: B gets Listing(service_instances=∅, all_service_instances={A's}, False); Down(A) -> both empty, True
async def test_a_restarted_node_replaces_its_old_row()                   # crash A, start A' on the same port registering again: B lists exactly one ref for the key, A's new one, after the old member is removed
```

- [ ] **Step 2: Run to verify they fail** — `uv run pytest -q tests/cluster/test_receptionist_cluster.py` → `ImportError: ReceptionistRow` and, for the behavioural ones once it imports, empty listings on the other node.

- [ ] **Step 3: Implement** in `receptionist.py`, enabled only when `system.cluster is not None`: closure state `rows: dict[UniqueAddress, tuple[int, frozenset[tuple[str, ActorRef]]]]`, `own_version: int`, `known: set[UniqueAddress]`; on setup subscribe `ctx.self` to `MemberEvent` and `ReachabilityEvent` via `Cluster(system).subscriptions.tell(Subscribe(ctx.self, ...))` (the cluster's `Subscribe`, imported as `ClusterSubscribe`) and start `ctx.timers.start_timer_with_fixed_delay("resend", _ResendTick(), settings.receptionist_resend_interval)`. `publish_own_row()`: bump `own_version`, build the row from the local registry, `send_row_to(all reachable members except self)`. Call it after every local change. `on ReceptionistRow`: store if `version > stored`, then notify subscribers of the keys whose computed listing changed. `on MemberEvent`: for a member not in `known`, add and send the own row to it; on `MemberRemoved` drop the row and notify its keys with flag `True`. `on UnreachableMember/ReachableMember`: notify the node's keys with flag `False`. `listing(key)`: `all_service_instances` = local instances ∪ bindings from rows whose owner is a current member; `service_instances` = those whose owner is self or not in `Cluster(system).state.unreachable`. `receptionist(address)` = `system.resolve(f"{address.root}/system/receptionist")`.

- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q` three times (timing), `uv run pyright` → all pass, 0 errors.

### Task 3: Example and README

**Files:**
- Modify: `examples/cluster/node.py`, `README.md`

- [ ] **Step 1: Example.** Each node spawns a greeter, registers it under `ServiceKey[Greet]("greeter")`, subscribes to the key and prints `listing.service_instances` paths as nodes come and go; `Greet` is `@remotable`. Verify with three in-process systems through the smoke pattern used for the cluster example, and `uv run pyright examples/cluster/node.py`.
- [ ] **Step 2: README.** A "Receptionist" Tour subsection after Cluster (snippet verified by running it); names-table rows: `Receptionist(system).ref ! Register(key, ref)` → `Receptionist(system).ref.tell(Register(key, ref))`; `ServiceKey[T]("id")` → `ServiceKey[T]("id")`; `Receptionist.Subscribe` → `pekko_py.receptionist.Subscribe` (the cluster's `Subscribe` is the top-level one); `Listing.serviceInstances(key)` / `allServiceInstances(key)` → `listing.service_instances` / `all_service_instances`; `Known deviations` bullet from the spec's "Deviations to document"; examples row updated.
- [ ] **Step 3:** `uv run pytest -q`, `uv run pyright` → all pass, 0 errors. Report ready to commit.

## Self-review notes

- Spec coverage: API and local semantics (Task 1), replication, membership and reachability (Task 2), invariants pinned by Task 1/2 tests, example and docs (Task 3). The "/system root on every system" change is in Task 1 because `Receptionist(system)` on a plain system needs it.
- Names used in later tasks match Task 1's Interfaces: `Listing`, `listing(key)`, `Receptionist(system).ref`, `ReceptionistRow`.
- Review Focus lines each name a test and its task.
