"""Pekko's ShardedDaemonProcessCoordinator.scala: the coordinator reads the scaling state from
Distributed Data and keeps every worker alive by sending its sharding region a ``StartEntity`` per
id, throttled, each keep-alive interval. A rescale stops pinging, writes the new revision, stops
every shard of the old one through ``StopShards``, marks the revision completed and pings again.
A coordinator that finds a revision not completed when it starts runs the rescale again from the
stopping of the shards."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ...behavior import Behavior, Behaviors, StashBuffer
from ...context import ActorContext
from ...patterns import Done, StatusReply
from ...ref import ActorRef
from ...stream import Keep, KillSwitches, Sink, Source, UniqueKillSwitch
from ...supervision import SupervisorStrategy
from ..ddata import DistributedData, ReplicatorMessageAdapter
from ..ddata.replicator import Replicator
from .coordinator import StopShards
from .shard import ClusterShardingSettings, ShardStopped, StartEntity
from .sharded_daemon_process import (
    ChangeNumberOfProcesses,
    GetNumberOfProcesses,
    NumberOfProcesses,
    ShardedDaemonProcessSettings,
    ShardedDaemonProcessState,
    ShardedDaemonProcessStateKey,
    all_shards_for,
    sorted_identities_for,
)


@dataclass(frozen=True)
class _InternalGetResponse:
    rsp: Replicator.GetResponse[ShardedDaemonProcessState]


@dataclass(frozen=True)
class _InternalUpdateResponse:
    rsp: Replicator.UpdateResponse[ShardedDaemonProcessState]


@dataclass(frozen=True)
class _ShardStopped:
    shard: str


@dataclass(frozen=True)
class _Tick:
    pass


@dataclass(frozen=True)
class _SendKeepAliveDone:
    pass


@dataclass(frozen=True)
class _ShardStopTimeout:
    pass


_TICK_KEY = "tick"
_SHARD_STOP_TIMEOUT_KEY = "shard-stop-timeout"


def sharded_daemon_process_coordinator(
    settings: ShardedDaemonProcessSettings,
    sharding_settings: ClusterShardingSettings,
    initial_number_of_processes: int,
    daemon_process_name: str,
    sharding_ref: ActorRef[StartEntity | StopShards],
) -> Behavior[Any]:
    """``apply`` (87-116)."""

    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        ctx.log.debug("ShardedDaemonProcessCoordinator for [%s] starting", daemon_process_name)
        key = ShardedDaemonProcessStateKey(daemon_process_name)

        def coordinator(
            adapter: ReplicatorMessageAdapter[Any, ShardedDaemonProcessState],
        ) -> Behavior[Any]:
            # Stopping shards can take a lot of time, so don't hammer it with retries (even if the
            # stop shard command is idempotent)
            stop_shards_timeout = sharding_settings.handoff_timeout / 4

            def to_shard_stopped(m: ShardStopped) -> Any:
                return _ShardStopped(m.shard)

            shard_stopped_adapter = ctx.message_adapter(to_shard_stopped)

            def ask_update(new_state: ShardedDaemonProcessState) -> None:
                adapter.ask_update(
                    lambda reply_to: Replicator.Update(
                        key,
                        ShardedDaemonProcessState.initial_state(initial_number_of_processes),
                        sharding_settings.state_write_consistency(),
                        reply_to,
                        lambda _: new_state,
                    ),
                    _InternalUpdateResponse,
                )

            def start() -> Behavior[Any]:
                """168-208."""
                adapter.ask_get(
                    lambda reply_to: Replicator.Get(
                        key, sharding_settings.state_read_consistency(), reply_to
                    ),
                    _InternalGetResponse,
                )

                def with_stash(stash: StashBuffer[Any]) -> Behavior[Any]:
                    @Behaviors.receive_message
                    def receive(msg: Any) -> Behavior[Any]:
                        match msg:
                            case _InternalGetResponse(Replicator.GetSuccess(data=state)):
                                if state.completed:
                                    ctx.log.debug(
                                        "%s: Previous completed state found for Sharded Daemon "
                                        "Process rev [%s] from [%s]",
                                        daemon_process_name,
                                        state.revision,
                                        state.started,
                                    )
                                    stash.stash(_Tick())
                                    return stash.unstash_all(idle(state))
                                ctx.log.debug(
                                    "%s: Previous non completed state found for Sharded Daemon "
                                    "Process rev [%s] from [%s], retriggering scaling",
                                    daemon_process_name,
                                    state.revision,
                                    state.started,
                                )
                                return stop_all_shards(state, None, initial_number_of_processes)
                            case _InternalGetResponse(Replicator.NotFound()):
                                ctx.log.debug(
                                    "%s: No previous state stored for Sharded Daemon Process",
                                    daemon_process_name,
                                )
                                stash.stash(_Tick())
                                return stash.unstash_all(
                                    idle(
                                        ShardedDaemonProcessState.initial_state(
                                            initial_number_of_processes
                                        )
                                    )
                                )
                            case _InternalGetResponse(Replicator.GetFailure()):
                                ctx.log.info(
                                    "%s: Failed fetching initial state for Sharded Daemon "
                                    "Process, retrying",
                                    daemon_process_name,
                                )
                                return start()
                            case _:
                                stash.stash(msg)
                                return Behaviors.same

                    return receive

                return Behaviors.with_stash(100, with_stash)

            def idle(
                state: ShardedDaemonProcessState, kill_switch: UniqueKillSwitch | None = None
            ) -> Behavior[Any]:
                """210-264. A rescale to fewer than one process is refused, where Pekko has no
                check."""

                @Behaviors.receive_message
                def receive(msg: Any) -> Behavior[Any]:
                    match msg:
                        case ChangeNumberOfProcesses(new_number_of_processes=n, reply_to=reply_to):
                            if n < 1:
                                reply_to.tell(
                                    StatusReply.error(f"number_of_processes must be >= 1, got {n}")
                                )
                                return Behaviors.same
                            if n == state.number_of_processes:
                                ctx.log.info(
                                    "%s: Rescale command for Sharded Daemon Process ignored, "
                                    "already running [%s] processes",
                                    daemon_process_name,
                                    n,
                                )
                                reply_to.tell(StatusReply.ACK)
                                return Behaviors.same
                            if kill_switch is not None:
                                kill_switch.shutdown()
                            new_state = state.start_scaling_to(n)
                            ctx.log.info(
                                "%s: Starting rescaling of Sharded Daemon Process to [%s] "
                                "processes, rev [%s]",
                                daemon_process_name,
                                n,
                                new_state.revision,
                            )
                            ctx.timers.cancel(_TICK_KEY)
                            return prepare_rescale(new_state, msg, state.number_of_processes)
                        case _Tick():
                            sorted_identities = sorted_identities_for(
                                state.revision, state.number_of_processes
                            )
                            ctx.log.debug(
                                "Sending periodic keep alive for Sharded Daemon Process [%s] to "
                                "[%s] processes (revision [%s]).",
                                daemon_process_name,
                                len(sorted_identities),
                                state.revision,
                            )
                            sent = send_keep_alive_messages(sorted_identities)
                            if sent is None:
                                ctx.timers.start_single_timer(
                                    _TICK_KEY, _Tick(), settings.keep_alive_interval
                                )
                                return Behaviors.same
                            switch, done = sent
                            ctx.pipe_to_self(done, lambda _: _SendKeepAliveDone())
                            return idle(state, switch)
                        case _SendKeepAliveDone():
                            ctx.timers.start_single_timer(
                                _TICK_KEY, _Tick(), settings.keep_alive_interval
                            )
                            return Behaviors.same
                        case GetNumberOfProcesses(reply_to=reply_to):
                            reply_to.tell(number_of_processes(state))
                            return Behaviors.same
                        case _:
                            return Behaviors.unhandled

                return receive

            def number_of_processes(state: ShardedDaemonProcessState) -> NumberOfProcesses:
                return NumberOfProcesses(
                    state.number_of_processes, state.started, not state.completed, state.revision
                )

            def prepare_rescale(
                new_state: ShardedDaemonProcessState,
                request: ChangeNumberOfProcesses,
                previous_number_of_processes: int,
                update_retry: int = 0,
            ) -> Behavior[Any]:
                """268-297."""
                ask_update(new_state)

                # wait for update to complete before applying
                def receive(msg: Any) -> Behavior[Any] | None:
                    match msg:
                        case _InternalUpdateResponse(Replicator.UpdateSuccess()):
                            # we use our own state rather than the result of the update, as there
                            # is a single(ton) writer
                            return stop_all_shards(
                                new_state, request, previous_number_of_processes
                            )
                        case _InternalUpdateResponse(Replicator.UpdateTimeout()):
                            if update_retry > 5:
                                ctx.log.warning(
                                    "%s: Failed updating scale state for sharded daemon process, "
                                    "retrying, retry [%s]",
                                    daemon_process_name,
                                    update_retry,
                                )
                            else:
                                ctx.log.debug(
                                    "%s: Failed updating scale state for sharded daemon process, "
                                    "retrying",
                                    daemon_process_name,
                                )
                            return prepare_rescale(
                                new_state, request, previous_number_of_processes, update_retry + 1
                            )
                        case _:
                            return None

                return receive_while_rescaling("prepareRescale", new_state, receive)

            def stop_all_shards(
                state: ShardedDaemonProcessState,
                request: ChangeNumberOfProcesses | None,
                previous_number_of_processes: int,
            ) -> Behavior[Any]:
                """299-309."""
                all_shards = all_shards_for(state.revision - 1, previous_number_of_processes)
                sharding_ref.tell(StopShards(all_shards, shard_stopped_adapter))
                ctx.timers.start_single_timer(
                    _SHARD_STOP_TIMEOUT_KEY, _ShardStopTimeout(), stop_shards_timeout
                )
                return wait_for_shards_stopping(
                    state, request, previous_number_of_processes, all_shards
                )

            def wait_for_shards_stopping(
                state: ShardedDaemonProcessState,
                request: ChangeNumberOfProcesses | None,
                previous_number_of_processes: int,
                shards_still_running: frozenset[str],
            ) -> Behavior[Any]:
                """311-328."""

                def receive(msg: Any) -> Behavior[Any] | None:
                    match msg:
                        case _ShardStopped(shard):
                            new_shards_still_running = shards_still_running - {shard}
                            if not new_shards_still_running:
                                ctx.timers.cancel(_SHARD_STOP_TIMEOUT_KEY)
                                return rescaling_complete(state, request)
                            return wait_for_shards_stopping(
                                state,
                                request,
                                previous_number_of_processes,
                                new_shards_still_running,
                            )
                        case _ShardStopTimeout():
                            ctx.log.debug(
                                "%s: Stopping shards timed out after [%s] retrying",
                                daemon_process_name,
                                stop_shards_timeout,
                            )
                            return stop_all_shards(state, request, previous_number_of_processes)
                        case _:
                            return None

                return receive_while_rescaling("waitForShardsStopping", state, receive)

            def rescaling_complete(
                state: ShardedDaemonProcessState,
                request: ChangeNumberOfProcesses | None,
                ddata_write_retries: int = 0,
            ) -> Behavior[Any]:
                """330-359."""
                if request is not None:
                    request.reply_to.tell(StatusReply.ACK)
                new_state = state.complete_scaling()
                ask_update(new_state)

                def receive(msg: Any) -> Behavior[Any] | None:
                    match msg:
                        case _InternalUpdateResponse(Replicator.UpdateSuccess()):
                            ctx.self.tell(_Tick())
                            return idle(new_state)
                        case _InternalUpdateResponse(Replicator.UpdateTimeout()):
                            if ddata_write_retries < 5:
                                ctx.log.debug(
                                    "%s: Failed updating scale state for sharded daemon process "
                                    "after completing rescale, retrying",
                                    daemon_process_name,
                                )
                            else:
                                ctx.log.warning(
                                    "%s: Failed updating scale state for sharded daemon process "
                                    "after completing rescale, retry [%s]",
                                    daemon_process_name,
                                    ddata_write_retries,
                                )
                            return rescaling_complete(new_state, None, ddata_write_retries + 1)
                        case _:
                            return None

                return receive_while_rescaling("rescalingComplete", state, receive)

            def receive_while_rescaling(
                step_name: str,
                state: ShardedDaemonProcessState,
                pf: Callable[[Any], Behavior[Any] | None],
            ) -> Behavior[Any]:
                """361-379: ``pf`` returns None for a message it does not match."""

                @Behaviors.receive_message
                def receive(msg: Any) -> Behavior[Any]:
                    behavior = pf(msg)
                    if behavior is not None:
                        return behavior
                    match msg:
                        case ChangeNumberOfProcesses(reply_to=reply_to):
                            reply_to.tell(StatusReply.error("Rescale in progress, retry later"))
                        case GetNumberOfProcesses(reply_to=reply_to):
                            reply_to.tell(number_of_processes(state))
                        case _:
                            ctx.log.debug(
                                "%s: Unexpected message in step %s: %s",
                                daemon_process_name,
                                step_name,
                                type(msg).__name__,
                            )
                    return Behaviors.same

                return receive

            def send_keep_alive_messages(
                sorted_identities: list[str],
            ) -> tuple[UniqueKillSwitch, asyncio.Future[Done]] | None:
                """381-396."""
                if settings.keep_alive_throttle_interval == 0:
                    for entity_id in sorted_identities:
                        sharding_ref.tell(StartEntity(entity_id))
                    return None
                return (
                    Source.from_iterable(sorted_identities)
                    .via_mat(KillSwitches.single(), Keep.right)
                    .throttle(1, settings.keep_alive_throttle_interval)
                    .to_mat(
                        Sink.foreach(lambda entity_id: sharding_ref.tell(StartEntity(entity_id))),
                        Keep.both,
                    )
                    .run(ctx.system)
                )

            return start()

        return DistributedData.with_replicator_message_adapter(coordinator)

    return Behaviors.supervise(Behaviors.setup(setup)).on_failure(SupervisorStrategy.restart())
