"""The ShardRegion: routes by shard id, registers with the coordinator, hosts Shard actors, hands
off on leave."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

from ...behavior import Behavior, Behaviors
from ...context import ActorContext
from ...ref import ActorRef
from ...signals import PostStop, Signal
from ...supervision import SupervisorStrategy
from .shard import (
    ClusterShardingSettings,
    ClusterShardingStats,
    Entity,
    GetClusterShardingStats,
    GetShardRegionState,
    GetShardRegionStats,
    HandOff,
    SetActiveEntityLimit,
    ShardingEnvelope,
    ShardInitialized,
    ShardsUpdated,
    ShardStopped,
    _log_passivation_strategy,  # pyright: ignore[reportPrivateUsage]
    region_state_query,
    region_stats_query,
    shard_behavior,
    shard_id_of,
)
from .passivation_settings import NoPassivationStrategy
from ..core import (
    Cluster,
    MemberDowned,
    MemberEvent,
    MemberExited,
    MemberLeft,
    MemberPreparingForShutdown,
    MemberReadyForShutdown,
    MemberRemoved,
    MemberUp,
)
from ..core import Subscribe as ClusterSubscribe
from ..gossip import Member
from .coordinator import (
    BeginHandOff,
    BeginHandOffAck,
    CoordinatorStopped,
    GetShardHome,
    GracefulShutdownReq,
    HostShard,
    Register,
    RegionStopped,
    RegisterAck,
    ShardHome,
    ShardHomes,
    ShardStarted,
    by_age,
    _is_at,  # pyright: ignore[reportPrivateUsage]
)
from ..singleton import _manager_name  # pyright: ignore[reportPrivateUsage]


@dataclass(frozen=True)
class _Retry: ...


@dataclass(frozen=True)
class _RegisterRetry: ...


@dataclass(frozen=True)
class _ShardTerminated:
    shard: str


@dataclass(frozen=True)
class _GracefulShutdown:
    """CoordinatedShutdown's region-shutdown task (Pekko's GracefulShutdown): hand every shard
    off."""


class _ExitingProgress:
    """The region's cluster-sharding-shutdown-region task: ask it to shut down gracefully, wait
    until it holds nothing."""

    def __init__(self) -> None:
        self.future: asyncio.Future[None] | None = None

    def task(self, cluster: Cluster, region: ActorRef[Any]) -> Callable[[], Awaitable[None]]:
        async def run() -> None:
            if cluster.is_terminated or cluster.self_member.status == "down":
                return
            region.tell(_GracefulShutdown())
            if self.future is not None:
                await asyncio.shield(self.future)

        return run


def region_behavior(
    entity: Entity[Any], settings: ClusterShardingSettings, progress: _ExitingProgress
) -> Behavior[Any]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        progress.future = asyncio.get_running_loop().create_future()
        type_name = entity.type_key.name
        log = ctx.log
        cluster = Cluster(ctx.system)
        members_by_age: list[Member] = []
        coordinator: ActorRef[Any] | None = None
        shards: dict[str, ActorRef[Any]] = {}
        # shards spawned that have not sent ShardInitialized yet (ShardRegion.scala 652)
        starting_shards: set[str] = set()
        region_by_shard: dict[str, ActorRef[Any]] = {}
        shard_buffers: dict[str, deque[ShardingEnvelope[Any]]] = {}
        buffered = 0
        logged_full_buffer_warning = False
        next_registration_delay = max(0.1, settings.retry_interval / 8)
        graceful_shutdown_in_progress = False
        preparing_for_shutdown = False
        passivation_strategy = settings.passivation_strategy
        cluster.subscriptions.tell(ClusterSubscribe(ctx.self, MemberEvent))
        ctx.timers.start_timer_with_fixed_delay("retry", _Retry(), settings.retry_interval)
        _log_passivation_strategy(log, type_name, passivation_strategy)

        # --- coordinator location ----------------------------------------------------------------

        def coordinator_path(member: Member) -> str:
            manager = _manager_name(f"{type_name}Coordinator")
            return f"{member.address.root}/system/{manager}/singleton/coordinator"

        def change_members(new_members: list[Member]) -> None:
            nonlocal members_by_age, coordinator
            before = members_by_age[0].unique_address if members_by_age else None
            members_by_age = new_members
            after = new_members[0].unique_address if new_members else None
            if before != after:
                log.info("%s: Coordinator moved from [%s] to [%s]", type_name, before, after)
                coordinator = None
                start_registration()

        def start_registration() -> None:
            nonlocal next_registration_delay
            next_registration_delay = max(0.1, settings.retry_interval / 8)
            register()
            schedule_next_registration()

        def schedule_next_registration() -> None:
            nonlocal next_registration_delay
            if next_registration_delay < settings.retry_interval:
                ctx.timers.start_single_timer(
                    "register-retry", _RegisterRetry(), next_registration_delay
                )
                next_registration_delay *= 2

        def coordinator_selection() -> list[Member]:
            """Pekko's coordinatorSelection: the members by age up to and including the first Up
            one, since the coordinator may still be on an older member that is leaving."""
            selected: list[Member] = []
            for m in members_by_age:
                selected.append(m)
                if m.status == "up":
                    break
            return selected

        def register() -> None:
            for m in coordinator_selection():
                try:
                    found = ctx.system.resolve(coordinator_path(m))
                # the local singleton instance is not up yet: the next retry asks again
                except LookupError:
                    continue
                found.tell(Register(ctx.self))

        # --- delivery ----------------------------------------------------------------------------

        def get_shard(shard: str) -> ActorRef[Any] | None:
            """ShardRegion.scala 1331-1370: the shard once it is initialized. On first use it is
            spawned and starting, and None is returned until its ``ShardInitialized``. A region
            always hosts shards here, never a proxy."""
            if shard in starting_shards:
                return None
            ref = shards.get(shard)
            if ref is not None:
                return ref
            log.debug("%s: Starting shard [%s] in region", type_name, shard)
            ref = ctx.spawn(shard_behavior(entity, shard, ctx.self), shard)
            ctx.watch_with(ref, _ShardTerminated(shard))
            shards[shard] = ref
            starting_shards.add(shard)
            if passivation_strategy != NoPassivationStrategy():
                deliver_to_all_shards(ShardsUpdated(len(shards)))
            return None

        def deliver_to_all_shards(msg: SetActiveEntityLimit | ShardsUpdated) -> None:
            """ShardRegion.scala 1278."""
            for ref in shards.values():
                ref.tell(msg)

        def initialize_shard(shard: str) -> None:
            """ShardRegion.scala 1210-1214. Pekko delivers to the sender; here it is the shard
            the region holds under that id."""
            log.debug("%s: Shard was initialized [%s]", type_name, shard)
            starting_shards.discard(shard)
            ref = shards.get(shard)
            if ref is not None:
                deliver_buffered(shard, ref)

        def deliver(envelope: ShardingEnvelope[Any]) -> None:
            """ShardRegion.scala 1281-1329, for an envelope."""
            shard = shard_id_of(envelope.entity_id, settings.number_of_shards)
            home = region_by_shard.get(shard)
            if home is None:
                # as Pekko's deliverMessage: ask on a shard's first message, even one the full
                # buffer then drops
                if coordinator is not None and shard not in shard_buffers:
                    coordinator.tell(GetShardHome(shard, ctx.self))
                buffer(shard, envelope)
            elif home.path == ctx.self.path:
                ref = get_shard(shard)
                if ref is None:
                    buffer(shard, envelope)
                elif shard in shard_buffers:
                    # Since now messages to a shard is buffered then those messages must be in
                    # right order
                    buffer(shard, envelope)
                    deliver_buffered(shard, ref)
                else:
                    ref.tell(envelope)
            else:
                home.tell(envelope)

        def buffer(shard: str, envelope: ShardingEnvelope[Any]) -> None:
            """ShardRegion.scala 1216-1240, without the fill-level logs: the full-buffer warning
            is logged once until the next delivery of a buffer."""
            nonlocal buffered, logged_full_buffer_warning
            if buffered >= settings.buffer_size:
                if logged_full_buffer_warning:
                    log.debug(
                        "%s: buffer is full, dropping message for shard [%s]", type_name, shard
                    )
                else:
                    log.warning(
                        "%s: buffer is full, dropping message for shard [%s]", type_name, shard
                    )
                    logged_full_buffer_warning = True
                ctx.system.dead_letter(envelope, ctx.self)
                return
            shard_buffers.setdefault(shard, deque()).append(envelope)
            buffered += 1

        def deliver_buffered(shard: str, receiver: ActorRef[Any]) -> None:
            """ShardRegion.scala 1242-1260."""
            nonlocal buffered, logged_full_buffer_warning
            for envelope in shard_buffers.pop(shard, ()):
                buffered -= 1
                receiver.tell(envelope)
            logged_full_buffer_warning = False

        def receive_shard_home(shard: str, region: ActorRef[Any]) -> None:
            nonlocal buffered
            current = region_by_shard.get(shard)
            if (
                current is not None
                and current.path == ctx.self.path
                and region.path != ctx.self.path
            ):
                # Pekko throws here, since its coordinator never moves a shard off a region that
                # hosts it; ohra hands the local copy off instead
                log.error(
                    "%s: shard [%s] moved from self to [%s]; stopping the local copy",
                    type_name,
                    shard,
                    region.path,
                )
                local = shards.get(shard)
                if local is not None:
                    local.tell(HandOff(shard, ctx.system.ignore_ref))
                for envelope in shard_buffers.pop(shard, ()):
                    buffered -= 1
                    ctx.system.dead_letter(envelope, ctx.self)
            region_by_shard[shard] = region
            if region.path == ctx.self.path:
                ref = get_shard(shard)
                if ref is not None:
                    deliver_buffered(shard, ref)
            else:
                deliver_buffered(shard, region)

        def request_shard_buffer_homes() -> None:
            if coordinator is not None:
                for shard in shard_buffers:
                    coordinator.tell(GetShardHome(shard, ctx.self))

        def send_graceful_shutdown_to_coordinator() -> None:
            if graceful_shutdown_in_progress and coordinator is not None:
                coordinator.tell(GracefulShutdownReq(ctx.self))

        def try_complete_graceful_shutdown() -> bool:
            """Pekko's tryCompleteGracefulShutdownIfInProgress: every shard handed off and nothing
            buffered."""
            if graceful_shutdown_in_progress and not shards and not shard_buffers:
                exiting_done()
                return True
            return False

        def exiting_done() -> None:
            if progress.future is not None and not progress.future.done():
                progress.future.set_result(None)

        # --- the behavior ------------------------------------------------------------------------

        @Behaviors.receive
        def active(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            nonlocal coordinator, buffered, logged_full_buffer_warning
            nonlocal graceful_shutdown_in_progress, preparing_for_shutdown
            if isinstance(msg, ShardingEnvelope):
                envelope = cast(ShardingEnvelope[Any], msg)
                if not envelope.entity_id:
                    ctx.system.dead_letter(envelope.message, ctx.self)
                else:
                    deliver(envelope)
                return Behaviors.stopped() if try_complete_graceful_shutdown() else Behaviors.same
            match msg:
                # receiveQuery (ShardRegion.scala 966-985)
                case GetClusterShardingStats(_, reply_to):
                    if coordinator is None:
                        reply_to.tell(ClusterShardingStats({}))
                    else:
                        coordinator.tell(msg)
                case GetShardRegionState(reply_to):
                    ctx.spawn_anonymous(
                        region_state_query(
                            type_name, shards, reply_to, settings.shard_region_query_timeout
                        )
                    )
                case GetShardRegionStats(reply_to):
                    ctx.spawn_anonymous(
                        region_stats_query(
                            type_name, shards, reply_to, settings.shard_region_query_timeout
                        )
                    )
                case MemberUp(member=m) | MemberLeft(member=m) | MemberExited(member=m):
                    change_members(
                        by_age(
                            [x for x in members_by_age if x.unique_address != m.unique_address]
                            + [m]
                        )
                    )
                case MemberRemoved(member=m) if m.unique_address == cluster.self_unique_address:
                    return Behaviors.stopped()
                case MemberDowned(member=m) if m.unique_address == cluster.self_unique_address:
                    return Behaviors.stopped()
                case MemberRemoved(member=m):
                    for shard in [
                        s
                        for s, r in region_by_shard.items()
                        if r.path != ctx.self.path and _is_at(r, m.address)
                    ]:
                        del region_by_shard[shard]
                    change_members(
                        [x for x in members_by_age if x.unique_address != m.unique_address]
                    )
                case MemberReadyForShutdown() | MemberPreparingForShutdown():
                    # ShardRegion.scala 801-805
                    if not preparing_for_shutdown:
                        log.info("%s. preparing for shutdown", type_name)
                    preparing_for_shutdown = True
                case MemberEvent():
                    pass
                case RegisterAck(acked):
                    coordinator = acked
                    ctx.timers.cancel("register-retry")
                    request_shard_buffer_homes()
                case CoordinatorStopped(stopped):
                    if coordinator is not None and coordinator.path == stopped.path:
                        coordinator = None
                        start_registration()
                case HostShard(shard, reply_to):
                    if graceful_shutdown_in_progress:
                        send_graceful_shutdown_to_coordinator()
                    else:
                        region_by_shard[shard] = ctx.self
                        # Start the shard, if already started this does nothing; its buffer
                        # goes out on ShardInitialized (ShardRegion.scala 819-836)
                        get_shard(shard)
                        reply_to.tell(ShardStarted(shard))
                case ShardHome(shard, region):
                    receive_shard_home(shard, region)
                case ShardHomes(homes):
                    for region, shard_ids in homes:
                        for shard in shard_ids:
                            receive_shard_home(shard, region)
                case BeginHandOff(shard, reply_to):
                    # ShardRegion.scala 855-868
                    log.debug("%s: BeginHandOff shard [%s]", type_name, shard)
                    if not preparing_for_shutdown:
                        region_by_shard.pop(shard, None)
                        reply_to.tell(BeginHandOffAck(shard, ctx.self))
                    else:
                        log.debug(
                            "%s: Ignoring begin handoff of shard [%s] as preparing to shutdown",
                            type_name,
                            shard,
                        )
                case HandOff(shard, reply_to):
                    dropped = shard_buffers.pop(shard, None)
                    if dropped:  # Pekko: avoid reordering buffered messages across a hand-off
                        buffered -= len(dropped)
                        for envelope in dropped:
                            ctx.system.dead_letter(envelope, ctx.self)
                        logged_full_buffer_warning = False
                    local = shards.get(shard)
                    if local is not None:
                        local.tell(msg)
                    else:
                        reply_to.tell(ShardStopped(shard))
                case _ShardTerminated(shard):
                    # ShardRegion.scala 1007-1031
                    shards.pop(shard, None)
                    starting_shards.discard(shard)
                    if (
                        passivation_strategy != NoPassivationStrategy()
                        and not graceful_shutdown_in_progress
                    ):
                        deliver_to_all_shards(ShardsUpdated(len(shards)))
                    if (
                        region_by_shard.get(shard) is not None
                        and region_by_shard[shard].path == ctx.self.path
                    ):
                        del region_by_shard[shard]
                case _Retry():
                    if coordinator is None:
                        register()
                    else:
                        request_shard_buffer_homes()
                    if graceful_shutdown_in_progress:
                        send_graceful_shutdown_to_coordinator()
                case _RegisterRetry():
                    if coordinator is None:
                        register()
                        schedule_next_registration()
                case _GracefulShutdown():
                    # ShardRegion.scala 926-946
                    if preparing_for_shutdown:
                        log.debug(
                            "%s: Skipping graceful shutdown of region and all its shards as "
                            "cluster is preparing for shutdown",
                            type_name,
                        )
                        exiting_done()
                        return Behaviors.stopped()
                    graceful_shutdown_in_progress = True
                    send_graceful_shutdown_to_coordinator()
                case ShardInitialized(shard):
                    initialize_shard(shard)
                case SetActiveEntityLimit():
                    deliver_to_all_shards(msg)
                case _:
                    pass
            return Behaviors.stopped() if try_complete_graceful_shutdown() else Behaviors.same

        def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            if sig is PostStop:
                if coordinator is not None:
                    # ShardRegion.postStop
                    coordinator.tell(RegionStopped(ctx.self.path))
                exiting_done()
            return Behaviors.same

        return active.receive_signal(on_signal)

    # as under Pekko's system guardian
    return Behaviors.supervise(setup).on_failure(SupervisorStrategy.restart())
