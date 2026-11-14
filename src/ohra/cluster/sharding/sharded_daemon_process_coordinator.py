"""Pekko's ShardedDaemonProcessCoordinator.scala, for a fixed set of processes: the coordinator
reads the scaling state from Distributed Data and keeps every worker alive by sending its
sharding region a ``StartEntity`` per id, throttled, each keep-alive interval."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from ...behavior import Behavior, Behaviors, StashBuffer
from ...context import ActorContext
from ...patterns import Done
from ...ref import ActorRef
from ...stream import Keep, KillSwitches, Sink, Source, UniqueKillSwitch
from ...supervision import SupervisorStrategy
from ..ddata import DistributedData, ReplicatorMessageAdapter
from ..ddata.replicator import Replicator
from .shard import ClusterShardingSettings, StartEntity
from .sharded_daemon_process import (
    ShardedDaemonProcessSettings,
    ShardedDaemonProcessState,
    ShardedDaemonProcessStateKey,
    sorted_identities_for,
)


@dataclass(frozen=True)
class _InternalGetResponse:
    rsp: Replicator.GetResponse[ShardedDaemonProcessState]


@dataclass(frozen=True)
class _Tick:
    pass


@dataclass(frozen=True)
class _SendKeepAliveDone:
    pass


_TICK_KEY = "tick"


def sharded_daemon_process_coordinator(
    settings: ShardedDaemonProcessSettings,
    sharding_settings: ClusterShardingSettings,
    initial_number_of_processes: int,
    daemon_process_name: str,
    sharding_ref: ActorRef[StartEntity],
) -> Behavior[Any]:
    """``apply`` (87-116)."""

    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        ctx.log.debug("ShardedDaemonProcessCoordinator for [%s] starting", daemon_process_name)
        key = ShardedDaemonProcessStateKey(daemon_process_name)

        def coordinator(
            adapter: ReplicatorMessageAdapter[Any, ShardedDaemonProcessState],
        ) -> Behavior[Any]:
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
                                # PR 3: stopAllShards for a state that is not completed
                                ctx.log.debug(
                                    "%s: Previous completed state found for Sharded Daemon "
                                    "Process rev [%s] from [%s]",
                                    daemon_process_name,
                                    state.revision,
                                    state.started,
                                )
                                stash.stash(_Tick())
                                return stash.unstash_all(idle(state))
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
                """210-270, without ChangeNumberOfProcesses and GetNumberOfProcesses."""

                @Behaviors.receive_message
                def receive(msg: Any) -> Behavior[Any]:
                    match msg:
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
                        case _:
                            return Behaviors.unhandled

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
