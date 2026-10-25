# Coordinated shutdown design (cluster layer 7)

Apache Pekko's CoordinatedShutdown, read from `actor/.../CoordinatedShutdown.scala`, the cluster's tasks in
`cluster/.../ClusterDaemon.scala`, `ClusterSingletonManager.scala` and `ShardRegion.scala`, and the phase list in
`actor/src/main/resources/reference.conf`. Today `ActorSystem.terminate()` stops the actors at once, which the other
members see as a crash; with the split-brain resolver that costs a `stable_after` wait on every rolling restart.
After this, `terminate()` runs Pekko's phases: user tasks, the sharding regions' hand-off, `Leave`, the exiting
phase where singletons hand over, exit confirmation, then the stop. The same phases run when another node sends
`Leave` for this one, when this node is downed, and, opted in, on a signal. Narrowed by dropping HOCON, custom
phases, `recover = off`, `exit-jvm`, reason overrides, and the on-by-default signal hook. Designed with the user on
2026-10-25.

## API (`pekko_py/coordinated_shutdown.py`, exported from `pekko_py`)

- `Phase(name: str, timeout: float)`, frozen. `PHASES: tuple[Phase, ...]`, Pekko's list in order with
  Pekko's defaults: `before-service-unbind`, `service-unbind`, `service-requests-done`, `service-stop`,
  `before-cluster-shutdown`, `cluster-sharding-shutdown-region` (10.0), `cluster-leave`, `cluster-exiting` (10.0),
  `cluster-exiting-done`, `cluster-shutdown`, `before-actor-system-terminate`, `actor-system-terminate` (10.0);
  every other timeout is `default_phase_timeout`, 5.0.
- `CoordinatedShutdownSettings(default_phase_timeout: float = 5.0, phase_timeouts: Mapping[str, float] = {},
  run_by_actor_system_terminate: bool = True, run_on_signals: tuple[int, ...] = ())`, frozen, validated:
  timeouts positive, every key in `phase_timeouts` a known phase. Passed as `ActorSystem(..., shutdown=...)`.
- `Reason` (a frozen dataclass base) with the instances `ActorSystemTerminateReason`, `JvmExitReason`,
  `ClusterDowningReason`, `ClusterLeavingReason`, each its own class as in Pekko; users subclass `Reason`.
- `CoordinatedShutdown(system)`: the extension, created with the system before the cluster so the cluster can
  register its tasks. Methods:
  - `add_task(phase: str, name: str, task: Callable[[], Awaitable[None]]) -> None`; `ValueError` for an unknown
    phase or an empty name, as Pekko's `require`s. Tasks may be added while a run is going; a task added to a phase
    that has already run is ignored with a debug log, as Pekko.
  - `add_actor_termination_task(phase: str, name: str, actor: ActorRef[Any], stop_msg: object | None = None) ->
    None`: tells `stop_msg` if given, then waits for the actor to stop (its cell's `done`; a remote ref raises
    `TypeError`).
  - `run(reason: Reason) -> Awaitable[None]`: starts the run once and returns a future that completes when the last
    phase has completed; later calls return the same future. `shutdown_reason -> Reason | None`.
  - `timeout(phase: str) -> float`, `total_timeout -> float` (the sum).
  - The run: for each phase in order, run its tasks concurrently and wait for all of them, bounded by the phase's
    timeout; a task that raises is logged as a warning and counts as done; a phase that times out is logged as a
    warning and the run continues (Pekko's `recover = true`). Pekko's info log lines "Running CoordinatedShutdown
    with reason [...]" and the per-phase debug lines are kept.
- Built-in task `terminate-system` in `actor-system-terminate`: `system._final_terminate()`, which is today's
  `terminate()`: stop the user root, then the system root, then remoting.

## `ActorSystem` (`pekko_py/system.py`)

- `terminate()` runs `CoordinatedShutdown(system).run(ActorSystemTerminateReason)` when
  `run_by_actor_system_terminate`, else `_final_terminate()`. It returns `None` as today.
- `when_terminated()` unchanged: it awaits the roots and remoting, however the stop was reached.
- The constructor installs `loop.add_signal_handler(sig, lambda: run(JvmExitReason))` for each signal in
  `run_on_signals` (a system is always created inside a running loop), and the system root's done callback removes
  them. `__aenter__` and `__aexit__` are unchanged (`terminate()` then `when_terminated()`).
- `system.coordinated_shutdown` holds the extension, as the other extensions.

## The cluster's tasks (`ClusterDaemon.scala` lines 232-250 and 400-416)

Registered by `Cluster._start`, with the guards Pekko has:

- `cluster-leave` / `leave`: done at once when `cluster.is_terminated` or self is Down; else tell the daemon
  `Leave(self_address)` and await self being Leaving, Exiting, Down or Removed, through a subscription to
  `MemberEvent` with an adapter ref that is unsubscribed afterwards; a `CurrentClusterState` with no members (not
  joined) completes it too. This is Pekko's `CoordinatedShutdownLeave` without the actor.
- `cluster-exiting` / `wait-exiting`: done at once when the node has no members; else await the daemon's
  `self_exiting` future, completed when gossip shows self Exiting and in the daemon's PostStop.
- `cluster-exiting-done` / `exiting-completed`: done at once when terminated or Down; else tell the daemon
  `ExitingCompleted(reply_to)` and await the reply; the daemon handles it with today's `exiting_completed`.
- `cluster-shutdown` / `wait-shutdown`: await the daemon's `done`.

The daemon: on self Exiting with no exiting in progress, set `exiting_tasks_in_progress`, complete `self_exiting`,
and call `run(ClusterLeavingReason)` (a no-op if a run is going), replacing today's `start_exiting`. On its own
stop, `Cluster._start`'s done callback sets `_terminated` and calls `run(ClusterDowningReason)` instead of
`system.terminate()`, as Pekko's `ClusterDaemon.postStop` with `run-coordinated-shutdown-when-down = on`.
`Cluster.shutdown()` keeps its meaning: stop the daemon, which ends the node through the phases.
`Cluster.add_exiting_task` and `ClusterSettings.exiting_timeout` are removed; `_ExitingCompleted` and the
internal exiting-task runner go with them.

## Singleton and sharding

- `ClusterSingletonManager.scala`: the manager registers `singleton-exiting-1` in `cluster-exiting` (done at once
  when terminated or self Down; else tell the manager `SelfExiting` and await its ack) and `singleton-exiting-2`
  in `cluster-exiting` (await `member_exiting_progress`). The existing `_ExitingProgress` holder provides both.
- `ShardRegion.scala`: the region registers `region-shutdown` in `cluster-sharding-shutdown-region` (done at once
  when terminated or self Down; else tell the region `GracefulShutdown` and await `graceful_shutdown_progress`).
  The region's handling is today's `_SelfExiting` path, renamed; it now runs before the node leaves, so shards
  are handed off while the member is Up, as Pekko. A region that is still buffering when the phase times out is
  left to the next phases, as Pekko.
- Both register through `CoordinatedShutdown(system)` when `init` is called, once per name.

## Invariants

- Every path to a stopped cluster node runs the phases exactly once: `terminate()`, a `Leave` from anywhere, Down,
  a signal, a dead daemon.
- A system without a cluster has only the built-in `terminate-system` task; its observable behaviour is unchanged
  but for log lines.
- A failing or slow task delays its phase by at most that phase's timeout and never blocks the stop.
- Nothing leaks: the run is one asyncio task that ends with its last phase; `when_terminated()` awaits it when
  one was started, so no test's leak check sees it outlive the system.

## Deviations to document

Phases are a fixed list and `CoordinatedShutdownSettings` replaces HOCON; `recover` is always on; no `exit-jvm`,
`exit-code` or reason overrides; `run_on_signals` is opt-in where Pekko's JVM hook is on by default, and keeps the
name `JvmExitReason`; `ClusterJoinUnsuccessfulReason` is absent since there is no
`shutdown-after-unsuccessful-join-seed-nodes`; the leave task subscribes with an adapter instead of a child actor.
The earlier deviation "terminating an ActorSystem does not Leave first" is removed, and `add_exiting_task` /
`exiting_timeout` are replaced by `add_task("cluster-exiting", ...)` and the phase timeout.

## Tests

Plain system (`tests/test_coordinated_shutdown.py`): phases in order with tasks in one phase concurrent (recorded
start times); a raising task and a sleeping task are logged and the run continues within the phase timeout; `run`
twice returns the same future and `shutdown_reason` is the first; unknown phase and empty name raise;
`add_actor_termination_task` waits for the actor; `terminate()` runs a `before-actor-system-terminate` task before
the guardian's PostStop; `run_by_actor_system_terminate=False` stops without running tasks; `run_on_signals=
(SIGUSR1,)` with `os.kill(os.getpid(), SIGUSR1)` terminates the system with `JvmExitReason`; `total_timeout` and
`timeout` arithmetic; every test under the leak check.

Multi-node at millisecond settings (`tests/cluster/test_coordinated_shutdown_cluster.py`): `terminate()` on a
node shows the others `MemberLeft`, `MemberExited`, `MemberRemoved` and never `UnreachableMember`; the singleton
moves on `terminate()` of its host with one start and one stop; a stream of counted messages sent through all
nodes during a `terminate()` arrives in full and the region handed off before `MemberLeft`; a `Leave` from
another node runs a user task registered in `service-stop`; a downed node terminates through the phases with the
three cluster tasks skipped and `shutdown_reason` `ClusterDowningReason`. Existing tests that pass
`exiting_timeout` switch to `shutdown=CoordinatedShutdownSettings(phase_timeouts={"cluster-exiting": ...})`, and
the two `add_exiting_task` tests in `tests/cluster/test_cluster.py` become `add_task("cluster-exiting", ...)`.

## Example and docs

`examples/cluster/node.py` passes `shutdown=CoordinatedShutdownSettings(run_on_signals=(SIGINT, SIGTERM))` and
drops its hand-made SIGINT handler; `await system.when_terminated()` stays. README: a "Coordinated shutdown" Tour
subsection after Cluster, names rows (`CoordinatedShutdown(system).addTask(phase, name)(task)` -> `add_task(phase,
name, task)`, `run(reason)`, the phase names, `run-by-jvm-shutdown-hook` -> `run_on_signals`, `coordinated-shutdown
.phases.<p>.timeout` -> `phase_timeouts`), the deviations above, the Cluster paragraph and the cluster deviations
bullet rewritten, the `add_exiting_task` row replaced, the FastAPI section noting `service-unbind` is where an
app would drain. CLAUDE.md layout line for `coordinated_shutdown.py`.
