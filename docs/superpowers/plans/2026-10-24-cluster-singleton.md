# Cluster Singleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One instance of an actor on the oldest cluster member, handed over when that member leaves, reachable through a proxy on every node, as Apache Pekko's ClusterSingleton.

**Architecture:** `cluster/singleton.py` holds the settings, a pure oldest-changed tracker, the manager state machine (each Pekko state a returned behavior), the proxy, and the `ClusterSingleton(system)` extension that spawns both under `/system`. The cluster daemon gains an exiting phase (`Cluster.add_exiting_task`) so the manager can hand over before the node confirms its exit.

**Tech Stack:** Python 3.12, asyncio, `pekko_py` runtime and `cluster/` package, pytest, pyright strict.

**Spec:** `docs/superpowers/specs/2026-10-24-cluster-singleton-design.md`

## Global Constraints

- Public names are Pekko's in snake_case; every intentional deviation goes in the README "Known deviations" list.
- Python 3.12 generics; frozen dataclasses for messages; `match` for dispatch; behaviors are values, state in closures or helper objects created in `setup`.
- `uv run pytest -q` green (every test fails if a task outlives it); `uv run pyright` 0 errors.
- Commits only when the user asks: no commit steps below.
- Settings and defaults exactly as the spec: `removal_margin=0.0`, `hand_over_retry_interval=1.0`, `min_number_of_hand_over_retries=15`, `singleton_identification_interval=1.0`, `buffer_size=1000` (0..10000); `ClusterSettings.exiting_timeout=10.0`.
- Paths: `/system/singletonManager<name>` with child `singleton`; `/system/singletonProxy<name>`.

## Review Focus

- A manager whose instance fails must not restart it, and a proxy must notice within one identification interval and buffer. Task 3, `test_a_failed_instance_is_not_restarted_and_proxies_buffer`.
- Two oldest changes in quick succession (the oldest and the next oldest leave together) must still produce exactly one running instance. Task 3, `test_two_leaves_in_a_row_keep_one_instance`.
- `init` on a system without a cluster must raise, not create a proxy that buffers forever. Task 3, `test_init_without_a_cluster_raises`.
- An exiting task that raises must not block the exit. Task 1, `test_a_failing_exiting_task_does_not_block_the_exit`.
- A proxy on the oldest node itself must deliver locally, and after a hand-over the proxy on the new oldest must deliver locally too while the others follow. Task 3, `test_proxies_on_the_host_and_elsewhere_follow_the_hand_over`.

---

### Task 1: The exiting phase in the cluster daemon

**Files:**
- Modify: `src/pekko_py/cluster/cluster.py` (`ClusterSettings.exiting_timeout`, `Cluster.add_exiting_task`, `_CoreDaemon.start_exiting` replacing the two `self.ctx.self.tell(_ExitingCompleted())` sites), `README.md` (the Cluster deviations bullet: remove "an Exiting node confirms and shuts down at once", describe `add_exiting_task`)
- Test: `tests/cluster/test_cluster.py`

**Interfaces:**
- Produces: `ClusterSettings.exiting_timeout: float = 10.0` (validated `> 0`); `Cluster.add_exiting_task(fn: Callable[[], Awaitable[None]]) -> None`; `Cluster.exiting_tasks: list[Callable[[], Awaitable[None]]]`.

- [ ] **Step 1: Write the failing tests** in `tests/cluster/test_cluster.py` (helpers `cluster_node`, `all_up`, `eventually` already there):

```python
async def test_exiting_tasks_run_before_the_node_exits()          # two nodes; b registers a task that sleeps 0.1 then appends "done"; Leave(b); when b.when_terminated() returns, the list holds "done"; a's view of b went through MemberExited before MemberRemoved
async def test_a_slow_exiting_task_is_bounded_by_exiting_timeout()  # exiting_timeout=0.2; task sleeps 5; Leave(b); b terminates within 2 s
async def test_a_failing_exiting_task_does_not_block_the_exit()    # task raises RuntimeError; Leave(b); b terminates within 2 s
```

- [ ] **Step 2: Run to verify they fail** — `uv run pytest -q tests/cluster/test_cluster.py -k exiting_task` → `AttributeError: 'ClusterSettings' ... exiting_timeout` / `'Cluster' object has no attribute 'add_exiting_task'`.
- [ ] **Step 3: Implement.** `start_exiting()` in `_CoreDaemon`: when `exiting_tasks` is empty tell self `_ExitingCompleted()`; otherwise `ctx.pipe_to_self(asyncio.wait_for(asyncio.gather(*(fn() for fn in tasks), return_exceptions=True), timeout), lambda _result: _ExitingCompleted())`, logging a failure or timeout at warning. Both former `tell` sites (receive_gossip and leader_actions_on_convergence) call it.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright` → all pass, 0 errors.

### Task 2: Settings, retry arithmetic and the oldest-changed tracker (pure)

**Files:**
- Create: `src/pekko_py/cluster/singleton.py`
- Test: `tests/cluster/test_singleton_tracker.py`

**Interfaces:**
- Produces, in `pekko_py.cluster.singleton`:
  - `ClusterSingletonSettings` (frozen dataclass, fields and defaults per Global Constraints, `__post_init__` validation: intervals `> 0`, `removal_margin >= 0`, `min_number_of_hand_over_retries >= 1`, `0 <= buffer_size <= 10000`), with properties `max_hand_over_retries: int` and `max_take_over_retries: int` (`n = int(removal_margin / hand_over_retry_interval)`; `max(min_number_of_hand_over_retries, n + 3)` and `max(1, max_hand_over_retries - 3)`).
  - `SingletonActor[M](behavior: Behavior[M], name: str, stop_message: M | None = None, settings: ClusterSingletonSettings | None = None)` frozen, with `with_stop_message(msg: M) -> SingletonActor[M]` and `with_settings(settings) -> SingletonActor[M]`.
  - `InitialOldestState(oldest: tuple[UniqueAddress, ...], safe_to_be_oldest: bool)`, `OldestChanged(oldest: UniqueAddress | None)`, both frozen.
  - `OldestChangedTracker(self_unique_address: UniqueAddress)` with `handle_initial(members: Iterable[Member]) -> None` (queues an `InitialOldestState` built as the spec says), `add(member: Member) -> None`, `remove(member: Member) -> None`, `self_exiting() -> None` (removes self), `next_change() -> InitialOldestState | OldestChanged | None` (pops the oldest queued change), `members_by_age: list[Member]` (ordered by `Member.is_older_than`, only members whose `up_number != UP_NUMBER_UNSET`).
  - `ClusterSingletonManagerIsStuck(Exception)`.

- [ ] **Step 1: Write the failing tests**:

```python
def test_retry_arithmetic_follows_pekko()                 # defaults -> (15, 12); removal_margin=30, interval=1 -> (33, 30); min retries 1, margin 0 -> (3, 1)
def test_settings_are_validated()                        # buffer_size=10001, min_number_of_hand_over_retries=0, hand_over_retry_interval=0 raise ValueError
def test_initial_state_lists_the_oldest_up_to_self()      # A(up 1) B(up 2) C(up 3), self B -> oldest (A, B), safe; A leaving -> unsafe; a joining member (UNSET) is ignored
def test_changes_are_queued_and_delivered_one_at_a_time() # remove(A) then remove(B): next_change twice -> OldestChanged(B), OldestChanged(C); then None; add of a younger member queues nothing
def test_self_exiting_removes_self_from_the_order()
```

- [ ] **Step 2: Run to verify they fail** — `ModuleNotFoundError: pekko_py.cluster.singleton`.
- [ ] **Step 3: Implement** the pure parts only (no actors yet) in `singleton.py`.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright`.

### Task 3: Manager, proxy and the extension

**Files:**
- Modify: `src/pekko_py/cluster/singleton.py`, `src/pekko_py/__init__.py` (export `ClusterSingleton`, `ClusterSingletonSettings`, `SingletonActor`)
- Test: `tests/cluster/test_singleton.py`

**Interfaces:**
- Consumes: Task 1's `Cluster.add_exiting_task`; Task 2's tracker, settings and messages; `Cluster(system)` (`subscriptions`, `state`, `self_unique_address`, `self_member`); cluster events; `system._system_root.spawn_child(...)`; `ctx.stop(child, after_pending=True)`; `system.resolve(path)`.
- Produces: `ClusterSingleton` with `__new__(cls, system) -> ClusterSingleton` (raises `RuntimeError` without a cluster; stored on `system.cluster_singleton`) and `init[M](self, singleton: SingletonActor[M]) -> ActorRef[M]`; wire messages, all `@remotable`: `HandOverToMe(from_: ActorRef[Any])`, `HandOverInProgress()`, `HandOverDone(from_: ActorRef[Any])`, `TakeOverFromMe(from_: ActorRef[Any])`, `IdentifySingleton(reply_to: ActorRef[SingletonIdentity])`, `SingletonIdentity(instance: ActorRef[Any] | None)`; internal `_SelfExiting`, `_HandOverRetry(count)`, `_TakeOverRetry(count)`, `_DelayedMemberRemoved(member)`, `_Cleanup`, `_TryToIdentifySingleton`; `ActorSystem.cluster_singleton: ClusterSingleton | None = None` added in `system.py`.

- [ ] **Step 1: Write the failing tests** with `cluster_node` imported from `tests/cluster/test_cluster.py` (pass `exiting_timeout=1.0`), `FAST_SINGLETON = ClusterSingletonSettings(hand_over_retry_interval=0.05, min_number_of_hand_over_retries=5, singleton_identification_interval=0.02)`, a `@remotable Hit(reply_to: ActorRef[int])` message, and an instance behavior `counter(log: list[tuple[str, int, float]], port: int)` that appends `("start", port, loop.time())` in setup, `("stop", port, loop.time())` on `PostStop`, and answers `Hit` with a running count:

```python
async def test_singleton_runs_on_the_oldest_and_proxies_reach_it()      # 3 nodes, init on all; ask(proxy_on_c, Hit) == 1; log == [("start", p1, _)]
async def test_messages_before_identification_are_buffered()           # init on c then ask at once: reply arrives
async def test_hand_over_when_the_oldest_leaves()                      # Leave(a); a terminates; log has ("stop", p1) before ("start", p2), stop time <= start time; ask through c's proxy answers again
async def test_proxies_on_the_host_and_elsewhere_follow_the_hand_over()  # 3 nodes; the proxy on a answers through a local ref (not a RemoteRef); Leave(a); then the proxy on b answers locally and the proxy on c through a RemoteRef to b's instance
async def test_crashed_oldest_is_replaced_only_after_removal()         # crash(a); subscribe MemberRemoved on b with the time; Down(a) from b; eventually ("start", p2) with start time >= removal time
async def test_younger_node_leaving_does_not_move_it()                 # Leave(c); log still has one start
async def test_two_leaves_in_a_row_keep_one_instance()                 # 4 nodes; Leave(a) and Leave(b) at once; eventually exactly one running instance (starts - stops == 1) and it answers
async def test_a_failed_instance_is_not_restarted_and_proxies_buffer() # instance behavior raises on a Crash message; after it, log has no new start; a Hit through a proxy is buffered (no reply within 0.2 s), then Down nothing: still no instance
async def test_buffer_size_drops_the_oldest_message()                  # buffer_size=1; three tells before identification; the instance receives one
async def test_leaver_exits_after_exiting_timeout_when_successor_never_answers()  # drop HandOverToMe and TakeOverFromMe frames at b's remoting; exiting_timeout=0.3; Leave(a); a terminates within 2 s
async def test_init_twice_returns_the_same_proxy()
async def test_init_without_a_cluster_raises()                         # ActorSystem without cluster=: ClusterSingleton(system) raises RuntimeError
```

- [ ] **Step 2: Run to verify they fail** — `ImportError: ClusterSingleton`.
- [ ] **Step 3: Implement.** `_manager(singleton, settings, cluster) -> Behavior[_ManagerMsg]` as a `_Manager` helper object created in `setup` (like `_CoreDaemon`) whose methods return the state behaviors `start()`, `younger(previous_oldest)`, `becoming_oldest(previous_oldest)`, `oldest(instance)`, `was_oldest(instance, new_oldest)`, `handing_over(instance, hand_over_to)`, `stopping(instance)`, `end()`, porting each `when(...)` block of ClusterSingletonManager.scala; it subscribes `ctx.self` to `ClusterDomainEvent`, builds the tracker on `SelfUp`, requests changes with `get_next_oldest_changed()`, registers the exiting task, watches the instance with `ctx.watch_with(instance, _InstanceTerminated())`, answers `IdentifySingleton` with the instance in any state that holds one, and raises `ClusterSingletonManagerIsStuck` where Pekko does. `_proxy(name, settings, cluster) -> Behavior[Any]` ports ClusterSingletonProxy with the identify timer kept running after the instance is found (spec). `ClusterSingleton.init` spawns both via `system._system_root.spawn_child`, idempotently by name, and returns the proxy's ref.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q` three times (timing), `uv run pyright`.

### Task 4: Example and README

**Files:**
- Modify: `examples/cluster/node.py`, `README.md`

- [ ] **Step 1: Example.** Each node calls `ClusterSingleton(system).init(SingletonActor(counter, "counter"))` and sends `Hit` through the proxy every second, printing the reply with the host node's port; stop the hosting node and the output shows the count continuing from a fresh instance on the next oldest. Verify with three in-process systems through the smoke pattern, `uv run pyright examples/cluster/node.py`.
- [ ] **Step 2: README.** "Cluster singleton" Tour subsection after Receptionist (snippet verified by running); names rows (`ClusterSingleton(system).init(SingletonActor(behavior, "name"))` → same; `withStopMessage` → `with_stop_message`; `ClusterSingletonSettings` fields); deviations bullet from the spec; the Cluster deviations bullet already updated in Task 1; examples row.
- [ ] **Step 3:** `uv run pytest -q`, `uv run pyright` → all pass, 0 errors. Report ready to commit.

## Self-review notes

- Spec coverage: exiting phase (1), settings/arithmetic/tracker (2), manager, proxy, identify, extension, invariants via multi-node tests (3), example and docs (4).
- Names used across tasks match: `OldestChangedTracker.next_change`, `InitialOldestState`, `OldestChanged`, `ClusterSingletonSettings.max_hand_over_retries`, `Cluster.add_exiting_task`.
- Review Focus lines each name a test and its task.
