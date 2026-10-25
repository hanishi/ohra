# Coordinated Shutdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port Apache Pekko's CoordinatedShutdown so `ActorSystem.terminate()`, a `Leave`, a Down and an opted-in signal all stop a node through the same phases: user tasks, sharding hand-off, leave, exiting with singleton hand-over, exit confirmation, stop.

**Architecture:** A new `pekko_py/coordinated_shutdown.py` holds the fixed phase list, the settings, the reasons and the extension that runs tasks per phase with Pekko's timeout-and-continue rule; its last phase stops the system through `ActorSystem._final_terminate()`. The cluster registers its four tasks in `Cluster._start`; the singleton manager and the sharding region register theirs through the extension instead of `Cluster.add_exiting_task`, which goes away with `ClusterSettings.exiting_timeout`.

**Tech Stack:** Python 3.12, asyncio, pytest, pyright strict. Pekko sources: `/Users/hanishi/pekko/actor/src/main/scala/org/apache/pekko/actor/CoordinatedShutdown.scala`, `cluster/src/main/scala/org/apache/pekko/cluster/ClusterDaemon.scala` (lines 232-250, 400-416, 1290-1300, `CoordinatedShutdownLeave`), `cluster-tools/.../ClusterSingletonManager.scala` (`singleton-exiting-1/2`), `cluster-sharding/.../ShardRegion.scala` (`region-shutdown`).

**Spec:** `docs/superpowers/specs/2026-10-25-coordinated-shutdown-design.md`

## Global Constraints

- Frozen dataclasses for settings, phases, reasons and messages; `match` for dispatch; Python 3.12 generics.
- Public names follow Pekko in snake_case; every narrowing goes in the README deviations.
- `pytest` green under the conftest leak check; `pyright` strict 0 errors.
- A system without a cluster must behave as today but for log lines; the whole suite's wall time must not grow by phase timeouts (no phase may wait on a task that cannot complete in the plain-system path).
- Every stop path runs the phases exactly once.

## Review Focus

1. `terminate()` called twice, or after a `Leave`-started run, must join the running run, not start a second one or raise (Task 1 `test_terminate_twice_joins_the_same_run`; Task 2 `test_terminate_during_a_leave_started_run_is_one_run`).
2. `terminate()` on a cluster node that has not joined yet (no members) must stop promptly, not wait out `cluster-leave`, `cluster-exiting` and `cluster-exiting-done` timeouts (Task 2 `test_terminate_before_joining_stops_promptly`).
3. A user task that never completes must delay its phase by exactly its timeout and the run must still reach the stop (Task 1 `test_a_hanging_task_delays_only_its_phase`).
4. `async with` exit while the node is being downed: the daemon stop starts a `ClusterDowningReason` run, `__aexit__`'s `terminate()` joins it, `when_terminated` completes (Task 2 `test_downed_node_terminates_through_the_phases`).
5. A signal arriving twice, or after termination, must not raise from the handler (Task 1 `test_signal_handler_is_idempotent_and_removed`).

---

### Task 1: The extension and `ActorSystem.terminate()`

**Files:**
- Create: `src/pekko_py/coordinated_shutdown.py`
- Modify: `src/pekko_py/system.py:30-70` (constructor: `shutdown=` param, extension, signal handlers), `:92-110` (`terminate`, `_final_terminate`, `when_terminated`), `src/pekko_py/__init__.py`
- Test: `tests/test_coordinated_shutdown.py`

**Interfaces:**
- Produces:
  - `Phase(name: str, timeout: float)`; `PHASES: tuple[Phase, ...]` (the 12 names in spec order; 10.0 for `cluster-sharding-shutdown-region`, `cluster-exiting`, `actor-system-terminate`; 5.0 otherwise); `PHASE_NAMES: frozenset[str]`.
  - `CoordinatedShutdownSettings(default_phase_timeout: float = 5.0, phase_timeouts: Mapping[str, float] = MappingProxyType({}), run_by_actor_system_terminate: bool = True, run_on_signals: tuple[int, ...] = ())`; `ValueError` for a non-positive timeout or an unknown phase key.
  - `class Reason` (frozen dataclass base); `ActorSystemTerminateReason`, `JvmExitReason`, `ClusterDowningReason`, `ClusterLeavingReason` as frozen dataclass subclasses with no fields. Only the classes are exported; a reason value is `ActorSystemTerminateReason()`, and two values of one class compare equal.
  - `class CoordinatedShutdown` with `__new__(cls, system) -> CoordinatedShutdown` returning `system.coordinated_shutdown` (created by the system constructor via `CoordinatedShutdown._start(system, settings)`), `add_task(phase: str, name: str, task: Callable[[], Awaitable[None]]) -> None`, `add_actor_termination_task(phase: str, name: str, actor: ActorRef[Any], stop_msg: object | None = None) -> None`, `run(reason: Reason) -> asyncio.Future[None]`, `shutdown_reason: Reason | None`, `timeout(phase: str) -> float`, `total_timeout: float`.
  - `ActorSystem(..., shutdown: CoordinatedShutdownSettings | None = None)`; `system.coordinated_shutdown: CoordinatedShutdown`; `ActorSystem._final_terminate() -> None` (today's `terminate` body); `terminate()` runs the extension when `run_by_actor_system_terminate` else `_final_terminate()`.
  - Exports from `pekko_py`: `CoordinatedShutdown`, `CoordinatedShutdownSettings`, `Reason`, `ActorSystemTerminateReason`, `JvmExitReason`, `ClusterDowningReason`, `ClusterLeavingReason`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_coordinated_shutdown.py
import asyncio, logging, os, signal
import pytest
from pekko_py import (ActorSystem, ActorSystemTerminateReason, Behaviors, CoordinatedShutdown, CoordinatedShutdownSettings,
    JvmExitReason, PostStop, Reason)
from pekko_py.coordinated_shutdown import PHASES

FAST = CoordinatedShutdownSettings(default_phase_timeout=0.2, phase_timeouts={"actor-system-terminate": 0.5})

def test_phase_list_and_timeouts():
    assert [p.name for p in PHASES][:2] == ["before-service-unbind", "service-unbind"] and PHASES[-1].name == "actor-system-terminate"
    assert dict((p.name, p.timeout) for p in PHASES)["cluster-exiting"] == 10.0 and dict((p.name, p.timeout) for p in PHASES)["service-stop"] == 5.0
    with pytest.raises(ValueError): CoordinatedShutdownSettings(default_phase_timeout=0)
    with pytest.raises(ValueError): CoordinatedShutdownSettings(phase_timeouts={"no-such-phase": 1.0})

async def test_phases_run_in_order_and_tasks_in_a_phase_concurrently():
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=FAST)
    cs = CoordinatedShutdown(system)
    log: list[str] = []
    async def slow(name: str) -> None:
        log.append(f"{name}-start"); await asyncio.sleep(0.05); log.append(f"{name}-end")
    cs.add_task("service-unbind", "a", lambda: slow("unbind-a"))
    cs.add_task("service-unbind", "b", lambda: slow("unbind-b"))
    cs.add_task("service-stop", "c", lambda: slow("stop"))
    await cs.run(ActorSystemTerminateReason())
    await system.when_terminated()
    assert log[:2] == ["unbind-a-start", "unbind-b-start"] and log[4] == "stop-start"  # a and b overlapped, c after both
    assert cs.shutdown_reason == ActorSystemTerminateReason()

async def test_a_failing_task_and_a_hanging_task_do_not_stop_the_run(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.WARNING, logger="pekko_py.coordinated_shutdown")
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=FAST)
    cs = CoordinatedShutdown(system)
    async def failing() -> None: raise RuntimeError("boom")
    async def hanging() -> None: await asyncio.sleep(60)
    cs.add_task("service-unbind", "failing", failing)
    cs.add_task("service-stop", "hanging", hanging)
    began = asyncio.get_running_loop().time()
    await cs.run(ActorSystemTerminateReason())
    await system.when_terminated()
    assert 0.2 <= asyncio.get_running_loop().time() - began < 1.0  # the hanging task cost exactly its phase timeout
    assert any("failing" in r.message for r in caplog.records) and any("timed out" in r.message for r in caplog.records)

async def test_a_hanging_task_delays_only_its_phase():
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=CoordinatedShutdownSettings(default_phase_timeout=5.0, phase_timeouts={"service-stop": 0.1}))
    cs = CoordinatedShutdown(system)
    async def hanging() -> None: await asyncio.sleep(60)
    cs.add_task("service-stop", "hanging", hanging)
    began = asyncio.get_running_loop().time()
    system.terminate()
    await system.when_terminated()
    assert asyncio.get_running_loop().time() - began < 1.0

async def test_run_is_once_and_terminate_twice_joins_the_same_run():
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=FAST)
    cs = CoordinatedShutdown(system)
    first = cs.run(ActorSystemTerminateReason())
    assert cs.run(JvmExitReason()) is first and cs.shutdown_reason == ActorSystemTerminateReason()
    system.terminate(); system.terminate()  # no error, no second run
    await system.when_terminated()

def test_unknown_phase_and_empty_name_raise():
    async def main() -> None:
        async with ActorSystem(Behaviors.ignore, "cs", shutdown=FAST) as system:
            with pytest.raises(ValueError): CoordinatedShutdown(system).add_task("nope", "x", lambda: asyncio.sleep(0))
            with pytest.raises(ValueError): CoordinatedShutdown(system).add_task("service-stop", "", lambda: asyncio.sleep(0))
    asyncio.run(main())

async def test_actor_termination_task_waits_for_the_actor():
    stopped: list[str] = []
    refs: list[ActorRef[str]] = []
    def worker() -> Behavior[str]:
        def on_message(msg: str) -> Behavior[str]:
            return Behaviors.stopped() if msg == "stop" else Behaviors.same
        def on_signal(_ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
            if sig is PostStop: stopped.append("worker")
            return Behaviors.same
        return Behaviors.receive_message(on_message).receive_signal(on_signal)
    def guardian(ctx: ActorContext[str]) -> Behavior[str]:
        refs.append(ctx.spawn(worker(), "w"))
        return Behaviors.ignore
    system = ActorSystem(Behaviors.setup(guardian), "cs", shutdown=FAST)
    await asyncio.sleep(0)  # the guardian's setup has run
    cs = CoordinatedShutdown(system)
    cs.add_actor_termination_task("service-stop", "w", refs[0], "stop")
    seen_before_terminate: list[list[str]] = []
    async def check() -> None: seen_before_terminate.append(list(stopped))
    cs.add_task("before-actor-system-terminate", "check", check)
    system.terminate()
    await system.when_terminated()
    assert seen_before_terminate == [["worker"]]  # the worker had stopped before the later phase ran

async def test_terminate_runs_a_task_before_the_guardian_stops():
    order: list[str] = []
    def guardian() -> Behavior[str]:
        def on_signal(_ctx, sig) -> Behavior[str]:
            if sig is PostStop: order.append("guardian-stopped")
            return Behaviors.same
        return Behaviors.receive_message(lambda _m: Behaviors.same).receive_signal(on_signal)
    system = ActorSystem(guardian(), "cs", shutdown=FAST)
    async def task() -> None: order.append("task")
    CoordinatedShutdown(system).add_task("before-actor-system-terminate", "t", task)
    system.terminate()
    await system.when_terminated()
    assert order == ["task", "guardian-stopped"]

async def test_run_by_actor_system_terminate_off_stops_directly():
    ran: list[str] = []
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=CoordinatedShutdownSettings(run_by_actor_system_terminate=False))
    async def task() -> None: ran.append("x")
    CoordinatedShutdown(system).add_task("service-stop", "t", task)
    system.terminate()
    await system.when_terminated()
    assert ran == [] and CoordinatedShutdown(system).shutdown_reason is None

async def test_signal_runs_the_shutdown_and_handler_is_idempotent_and_removed():
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=CoordinatedShutdownSettings(default_phase_timeout=0.2, run_on_signals=(signal.SIGUSR1,)))
    os.kill(os.getpid(), signal.SIGUSR1)
    os.kill(os.getpid(), signal.SIGUSR1)  # twice: the second joins the run
    await asyncio.wait_for(system.when_terminated(), timeout=5)
    assert CoordinatedShutdown(system).shutdown_reason == JvmExitReason()
    assert asyncio.get_running_loop().remove_signal_handler(signal.SIGUSR1) is False  # already removed

def test_timeout_arithmetic():
    async def main() -> None:
        async with ActorSystem(Behaviors.ignore, "cs", shutdown=CoordinatedShutdownSettings(default_phase_timeout=1.0, phase_timeouts={"cluster-exiting": 2.0})) as system:
            cs = CoordinatedShutdown(system)
            assert cs.timeout("service-stop") == 1.0 and cs.timeout("cluster-exiting") == 2.0 and cs.timeout("actor-system-terminate") == 10.0
            assert cs.total_timeout == 9 * 1.0 + 2.0 + 10.0 + 10.0
    asyncio.run(main())

class MyReason(Reason): ...

async def test_user_reason():
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=FAST)
    await CoordinatedShutdown(system).run(MyReason())
    await system.when_terminated()
    assert CoordinatedShutdown(system).shutdown_reason == MyReason()
```

`timeout(phase)` returns `phase_timeouts[phase]` if set, else the phase's own default when it has one (the three 10 s phases), else `default_phase_timeout`; that is what Pekko's config does, where the three phases carry explicit timeouts and the rest fall back to `default-phase-timeout`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_coordinated_shutdown.py -q 2>&1 | tail -3`
Expected: FAIL at import, `cannot import name 'CoordinatedShutdown'`.

- [ ] **Step 3: Implement `coordinated_shutdown.py` and the `ActorSystem` changes**

Port `CoordinatedShutdown.scala` `run` (lines 716-775): one asyncio task created on the first `run`, looping over `PHASES`; per phase, `asyncio.gather(*tasks, return_exceptions=True)` wrapped in `asyncio.wait_for(..., timeout)`; a `TimeoutError` logs Pekko's "Coordinated shutdown phase [{}] timed out after {}" warning and continues; a task exception logs "Task [{}] in phase [{}] failed" and continues. Tasks are stored per phase in insertion order; `add_task` to a phase already run logs at debug and drops it. The built-in task `terminate-system` in `actor-system-terminate` calls `system._final_terminate()`. The logger is `pekko_py.coordinated_shutdown`. `ActorSystem.__init__` creates the extension before the cluster (`self.coordinated_shutdown = CoordinatedShutdown._start(self, shutdown or CoordinatedShutdownSettings())`), installs the signal handlers with `loop.add_signal_handler(sig, self._on_signal)` where `_on_signal` calls `run(JvmExitReason())`, and registers removal on `self._system_root.done`. `when_terminated()` awaits the run's future (shielded) when a run exists, after the roots. `_final_terminate` is today's `terminate` body.

- [ ] **Step 4: Run the tests and the whole suite**

Run: `pytest tests/test_coordinated_shutdown.py -q 2>&1 | tail -3 && pytest -q 2>&1 | tail -1 && pyright | tail -1`
Expected: all PASS; the suite's wall time within a few seconds of today's (about 45 s); 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/coordinated_shutdown.py src/pekko_py/system.py src/pekko_py/__init__.py tests/test_coordinated_shutdown.py
git commit -m "CoordinatedShutdown: Pekko's phases run by ActorSystem.terminate, with per-phase timeouts and an opt-in signal hook"
```

---

### Task 2: The cluster's four tasks

**Files:**
- Modify: `src/pekko_py/cluster/core.py` (`Cluster._start` :396-420, `add_exiting_task` :435-438, `shutdown` :440-445, the daemon's `start_exiting` :943-963 and `exiting_completed` :965-980, `_ExitingCompleted` :344, the exiting trigger :1072-1075 and :1195-1197, `_on_signal` PostStop, `ClusterSettings.exiting_timeout` :65-66), `README.md` only where a test name changes
- Test: `tests/cluster/test_cluster.py:430-480` (rewrite the three exiting-task tests), `tests/cluster/test_coordinated_shutdown_cluster.py` (new)

**Interfaces:**
- Consumes: Task 1's `CoordinatedShutdown(system).add_task`, `run`, reasons.
- Produces:
  - `Cluster.self_exiting: asyncio.Future[None]` (created in `_start`; completed by the daemon when gossip shows self Exiting, and in its PostStop).
  - `_ExitingCompleted(reply_to: ActorRef[None] | None = None)`: the daemon replies `None` to `reply_to` after `exiting_completed()`.
  - `Cluster.add_exiting_task(fn)` kept for this task only as `CoordinatedShutdown(self.system).add_task("cluster-exiting", f"exiting-task-{n}", fn)`; removed in Task 3. `ClusterSettings.exiting_timeout` removed now; its callers in tests switch to `shutdown=CoordinatedShutdownSettings(phase_timeouts={"cluster-exiting": x})`. Test helpers `cluster_node(..., shutdown=...)` gain a `shutdown` keyword passed to `ActorSystem`.
  - The four tasks registered in `_start`: `("cluster-leave", "leave")`, `("cluster-exiting", "wait-exiting")`, `("cluster-exiting-done", "exiting-completed")`, `("cluster-shutdown", "wait-shutdown")`, with the guards in the spec.
  - `core.done` callback: `self._terminated = True; CoordinatedShutdown(system).run(ClusterDowningReason())`.

- [ ] **Step 1: Rewrite the three exiting-task tests in `tests/cluster/test_cluster.py` and write the new multi-node tests**

In `test_cluster.py`: `Cluster(b).add_exiting_task(task)` becomes `CoordinatedShutdown(b).add_task("cluster-exiting", "t", task)`; `exiting_timeout=0.2` becomes `shutdown=CoordinatedShutdownSettings(phase_timeouts={"cluster-exiting": 0.2})`; add to the first test `assert CoordinatedShutdown(b).shutdown_reason == ClusterLeavingReason()`.

```python
# tests/cluster/test_coordinated_shutdown_cluster.py
async def test_terminate_leaves_gracefully():
    p1, p2 = sorted((free_port(), free_port()))
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), ClusterDomainEvent))
        b.terminate()
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        await eventually(lambda: all_up(a, 1), timeout=5)
        names = [type(e).__name__ for e in events if isinstance(e, MemberEvent) and e.member.unique_address.port == p2]
        assert names == ["MemberUp", "MemberLeft", "MemberExited", "MemberRemoved"]
        assert not any(isinstance(e, UnreachableMember) for e in events)
        assert CoordinatedShutdown(b).shutdown_reason == ActorSystemTerminateReason()

async def test_a_leave_from_another_node_runs_user_tasks():
    # two nodes; CoordinatedShutdown(b).add_task("service-stop", "t", task) appends "stopped"; Cluster(a).manager.tell(Leave(b's address))
    # b terminates; ran == ["stopped"]; shutdown_reason == ClusterLeavingReason()

async def test_terminate_during_a_leave_started_run_is_one_run():
    # as above, but b.terminate() right after the Leave; a counter task in "service-stop" ran once; b terminates within 5 s

async def test_terminate_before_joining_stops_promptly():
    p = free_port()
    began = asyncio.get_running_loop().time()
    async with cluster_node("c", p, [p + 1]):  # a seed that never answers: the node stays uninitialized
        pass
    assert asyncio.get_running_loop().time() - began < 2.0  # no cluster phase waited for its timeout

async def test_downed_node_terminates_through_the_phases():
    # three nodes; crash nothing: Cluster(a).manager.tell(Down(c's address)); c terminates by itself;
    # CoordinatedShutdown(c).shutdown_reason == ClusterDowningReason(); a user task in "service-stop" on c ran
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/cluster/test_cluster.py tests/cluster/test_coordinated_shutdown_cluster.py -q 2>&1 | tail -3`
Expected: FAIL: `TypeError` for `exiting_timeout`/`shutdown` kwargs and `ImportError`s, then `test_terminate_leaves_gracefully` sees `UnreachableMember` once the imports resolve.

- [ ] **Step 3: Implement**

`Cluster._start` registers the four tasks (closures over `self` and `system`); the leave task subscribes a `FunctionRef` adapter to `MemberEvent` and `CurrentClusterState` through `GetCurrentState`, completes a future on self Left/Exited/Downed/Removed or no members, then `Unsubscribe`s. The daemon's exiting trigger (both sites) becomes: set `exiting_tasks_in_progress`, `self.cluster.self_exiting.set_result(None)` if not done, `CoordinatedShutdown(system).run(ClusterLeavingReason())`. `_on_signal` PostStop also completes `self_exiting`. `exiting_completed` replies to `reply_to` and ends with `self.shutdown()` (stop the daemon: return `Behaviors.stopped()` from the handler, which the done-callback turns into the `ClusterDowningReason` run). Delete `start_exiting`, `Cluster.exiting_tasks`, `exiting_timeout`.

- [ ] **Step 4: Run the suite**

Run: `pytest -q 2>&1 | tail -1 && pyright | tail -1`
Expected: all PASS (the singleton and sharding tests still pass through the temporary `add_exiting_task`), 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/cluster/core.py tests/cluster/test_cluster.py tests/cluster/test_coordinated_shutdown_cluster.py tests/cluster/test_singleton.py tests/cluster/test_cluster_sharding.py
git commit -m "Cluster: leave, wait-exiting, exiting-completed and wait-shutdown as CoordinatedShutdown tasks, as Apache Pekko's"
```

---

### Task 3: Singleton and sharding register through the extension

**Files:**
- Modify: `src/pekko_py/cluster/singleton.py:243-258, 805-830`, `src/pekko_py/cluster/sharding/region.py:72-100, 290-320`, `src/pekko_py/cluster/sharding/__init__.py:90-105`, `src/pekko_py/cluster/core.py` (remove `add_exiting_task`)
- Test: `tests/cluster/test_coordinated_shutdown_cluster.py`, `tests/cluster/test_singleton.py`, `tests/cluster/test_cluster_sharding.py`

**Interfaces:**
- Consumes: Task 1's `add_task`; Task 2's removal of `exiting_timeout`.
- Produces: singleton `init` registers `("cluster-exiting", "singleton-exiting-1")` (tell `_SelfExiting(done)` with an `asyncio.Future` the manager completes on handling; done at once when terminated or Down) and `("cluster-exiting", "singleton-exiting-2")` (await `member_exiting_progress`); sharding `init` registers `("cluster-sharding-shutdown-region", "region-shutdown")` (tell the region `_GracefulShutdown()`, the renamed `_SelfExiting`, await the progress future; done at once when terminated or Down). `Cluster.add_exiting_task` deleted.

- [ ] **Step 1: Write the failing tests**

```python
async def test_singleton_hands_over_on_terminate():
    # three singleton nodes as tests/cluster/test_singleton.py; a.terminate(); starts(log) == [p1, p2], stops(log) == [p1], never_two_at_once(log)

async def test_shards_hand_off_before_the_node_leaves():
    # three sharding nodes; stream Count(1) to 12 ids through all nodes while nodes[2].terminate() runs;
    # answered + dead-lettered == sent (as test_cluster_sharding's graceful-leave test); and the region's
    # hand-off finished before MemberLeft: record the time of the last ShardStopped seen by the coordinator's
    # node via a MemberEvent probe on nodes[0] and the time nodes[2]'s region PostStop ran (hosted(region) empty)
```

Simplify the second assertion to what is observable without reaching into internals: subscribe a probe on `nodes[0]` to `MemberEvent`; in `nodes[2]`, register a user task in `cluster-leave` that records `await hosted(region)`; assert it recorded an empty set, which means the region had handed off before the leave phase began.

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/cluster/test_coordinated_shutdown_cluster.py -q -k "singleton or hand_off" 2>&1 | tail -3`
Expected: `test_shards_hand_off_before_the_node_leaves` FAILS (the region still hands off during exiting, after the leave); the singleton test may pass already through the temporary bridge, which is a finding about the bridge, not the test; keep it.

- [ ] **Step 3: Implement**

Replace both `add_exiting_task` calls with the registrations above; `_SelfExiting` in the singleton gains `done: asyncio.Future[None] | None = None` (frozen dataclass with `eq=False`), completed in each `_SelfExiting` arm after `self_member_exited()`; the region's message is renamed `_GracefulShutdown`. Delete `Cluster.add_exiting_task` and `Cluster.exiting_tasks`.

- [ ] **Step 4: Run the suite three times**

Run: `for i in 1 2 3; do pytest -q 2>&1 | tail -1; done && pyright | tail -1`
Expected: all PASS each time, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/cluster tests/cluster
git commit -m "Singleton and sharding register their shutdown tasks with CoordinatedShutdown; regions hand off before the leave, as Pekko's"
```

---

### Task 4: Example and README

**Files:**
- Modify: `examples/cluster/node.py:184-201`, `README.md` (Tour after "### Cluster"; names rows near 459; deviations bullets 513 and 517; FastAPI section), `CLAUDE.md` layout

- [ ] **Step 1: Example** — `ActorSystem(..., shutdown=CoordinatedShutdownSettings(run_on_signals=(signal.SIGINT, signal.SIGTERM)))`; delete the `add_signal_handler` line; docstring: ctrl-c or SIGTERM runs the coordinated shutdown. Run two nodes, send SIGTERM to one, confirm the other logs `MemberLeft`, `MemberExited`, `MemberRemoved` and no `UnreachableMember`.
- [ ] **Step 2: README** — a "Coordinated shutdown" subsection: three sentences, a snippet adding a `service-unbind` task and passing `run_on_signals`, the phase list on one line. Names rows per the spec. Deviations: the new bullet, the removal of "Terminating an ActorSystem does not `Leave` first; Pekko's `CoordinatedShutdown` does. Its cluster-exiting phase is `Cluster(system).add_exiting_task(fn)`..." from the cluster bullet, the singleton bullet's "The exiting phase is `Cluster.add_exiting_task` rather than CoordinatedShutdown" removed, the Cluster paragraph's "Shutting a system down does not `Leave` first, so stop a node gracefully by..." replaced by "`terminate()`, or `async with` exit, leaves gracefully through CoordinatedShutdown". FastAPI section: one sentence that `service-unbind` is where an app drains. CLAUDE.md: add `coordinated_shutdown.py` to the core list.
- [ ] **Step 3: Verify** — `pytest -q | tail -1`, `pyright | tail -1`, the README snippet pasted into a scratch file imports and runs.
- [ ] **Step 4: Commit**

```bash
git add examples/cluster/node.py README.md CLAUDE.md
git commit -m "CoordinatedShutdown: example and README"
```
