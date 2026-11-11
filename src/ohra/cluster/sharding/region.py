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
    CurrentShardRegionState,
    Entity,
    GetShardRegionState,
    HandOff,
    SetActiveEntityLimit,
    ShardingEnvelope,
    ShardInitialized,
    ShardsUpdated,
    ShardStopped,
    _aggregate_shard_states,  # pyright: ignore[reportPrivateUsage]
    _log_passivation_strategy,  # pyright: ignore[reportPrivateUsage]
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


class _Region:
    def __init__(
        self,
        ctx: ActorContext[Any],
        entity: Entity[Any],
        settings: ClusterShardingSettings,
        progress: _ExitingProgress,
    ) -> None:
        self.ctx = ctx
        self.entity = entity
        self.settings = settings
        self.progress = progress
        progress.future = asyncio.get_running_loop().create_future()
        self.type_name = entity.type_key.name
        self.log = ctx.log
        self.cluster = Cluster(ctx.system)
        self.members_by_age: list[Member] = []
        self.coordinator: ActorRef[Any] | None = None
        self.shards: dict[str, ActorRef[Any]] = {}
        # shards spawned that have not sent ShardInitialized yet (ShardRegion.scala 652)
        self.starting_shards: set[str] = set()
        self.region_by_shard: dict[str, ActorRef[Any]] = {}
        self.shard_buffers: dict[str, deque[ShardingEnvelope[Any]]] = {}
        self.buffered = 0
        self.logged_full_buffer_warning = False
        self.next_registration_delay = max(0.1, settings.retry_interval / 8)
        self.graceful_shutdown_in_progress = False
        self.preparing_for_shutdown = False
        self.passivation_strategy = settings.passivation_strategy
        self.cluster.subscriptions.tell(ClusterSubscribe(ctx.self, MemberEvent))
        ctx.timers.start_timer_with_fixed_delay("retry", _Retry(), settings.retry_interval)
        _log_passivation_strategy(self.log, self.type_name, self.passivation_strategy)

    # --- coordinator location --------------------------------------------------------------------

    def coordinator_path(self, member: Member) -> str:
        manager = _manager_name(f"{self.type_name}Coordinator")
        return f"{member.address.root}/system/{manager}/singleton"

    def change_members(self, new_members: list[Member]) -> None:
        before = self.members_by_age[0].unique_address if self.members_by_age else None
        self.members_by_age = new_members
        after = new_members[0].unique_address if new_members else None
        if before != after:
            self.log.info("%s: Coordinator moved from [%s] to [%s]", self.type_name, before, after)
            self.coordinator = None
            self.start_registration()

    def start_registration(self) -> None:
        self.next_registration_delay = max(0.1, self.settings.retry_interval / 8)
        self.register()
        self.schedule_next_registration()

    def schedule_next_registration(self) -> None:
        if self.next_registration_delay < self.settings.retry_interval:
            self.ctx.timers.start_single_timer(
                "register-retry", _RegisterRetry(), self.next_registration_delay
            )
            self.next_registration_delay *= 2

    def coordinator_selection(self) -> list[Member]:
        """Pekko's coordinatorSelection: the members by age up to and including the first Up one,
        since the coordinator may still be on an older member that is leaving."""
        selected: list[Member] = []
        for m in self.members_by_age:
            selected.append(m)
            if m.status == "up":
                break
        return selected

    def register(self) -> None:
        for m in self.coordinator_selection():
            try:
                coordinator = self.ctx.system.resolve(self.coordinator_path(m))
            # the local singleton instance is not up yet: the next retry asks again
            except LookupError:
                continue
            coordinator.tell(Register(self.ctx.self))

    # --- delivery --------------------------------------------------------------------------------

    def get_shard(self, shard: str) -> ActorRef[Any] | None:
        """ShardRegion.scala 1331-1370: the shard once it is initialized. On first use it is
        spawned and starting, and None is returned until its ``ShardInitialized``. A region
        always hosts shards here, never a proxy."""
        if shard in self.starting_shards:
            return None
        ref = self.shards.get(shard)
        if ref is not None:
            return ref
        self.log.debug("%s: Starting shard [%s] in region", self.type_name, shard)
        ref = self.ctx.spawn(shard_behavior(self.entity, shard, self.ctx.self), shard)
        self.ctx.watch_with(ref, _ShardTerminated(shard))
        self.shards[shard] = ref
        self.starting_shards.add(shard)
        if self.passivation_strategy != NoPassivationStrategy():
            self.deliver_to_all_shards(ShardsUpdated(len(self.shards)))
        return None

    def deliver_to_all_shards(self, msg: SetActiveEntityLimit | ShardsUpdated) -> None:
        """ShardRegion.scala 1278."""
        for ref in self.shards.values():
            ref.tell(msg)

    def initialize_shard(self, shard: str) -> None:
        """ShardRegion.scala 1210-1214. Pekko delivers to the sender; here it is the shard
        the region holds under that id."""
        self.log.debug("%s: Shard was initialized [%s]", self.type_name, shard)
        self.starting_shards.discard(shard)
        ref = self.shards.get(shard)
        if ref is not None:
            self.deliver_buffered(shard, ref)

    def deliver(self, envelope: ShardingEnvelope[Any]) -> None:
        """ShardRegion.scala 1281-1329, for an envelope."""
        shard = shard_id_of(envelope.entity_id, self.settings.number_of_shards)
        home = self.region_by_shard.get(shard)
        if home is None:
            # as Pekko's deliverMessage: ask on a shard's first message, even one the full
            # buffer then drops
            if self.coordinator is not None and shard not in self.shard_buffers:
                self.coordinator.tell(GetShardHome(shard, self.ctx.self))
            self.buffer(shard, envelope)
        elif home.path == self.ctx.self.path:
            ref = self.get_shard(shard)
            if ref is None:
                self.buffer(shard, envelope)
            elif shard in self.shard_buffers:
                # Since now messages to a shard is buffered then those messages must be in
                # right order
                self.buffer(shard, envelope)
                self.deliver_buffered(shard, ref)
            else:
                ref.tell(envelope)
        else:
            home.tell(envelope)

    def buffer(self, shard: str, envelope: ShardingEnvelope[Any]) -> None:
        """ShardRegion.scala 1216-1240, without the fill-level logs: the full-buffer warning
        is logged once until the next delivery of a buffer."""
        if self.buffered >= self.settings.buffer_size:
            if self.logged_full_buffer_warning:
                self.log.debug(
                    "%s: buffer is full, dropping message for shard [%s]", self.type_name, shard
                )
            else:
                self.log.warning(
                    "%s: buffer is full, dropping message for shard [%s]", self.type_name, shard
                )
                self.logged_full_buffer_warning = True
            self.ctx.system.dead_letter(envelope, self.ctx.self)
            return
        self.shard_buffers.setdefault(shard, deque()).append(envelope)
        self.buffered += 1

    def deliver_buffered(self, shard: str, receiver: ActorRef[Any]) -> None:
        """ShardRegion.scala 1242-1260."""
        for envelope in self.shard_buffers.pop(shard, ()):
            self.buffered -= 1
            receiver.tell(envelope)
        self.logged_full_buffer_warning = False

    def receive_shard_home(self, shard: str, region: ActorRef[Any]) -> None:
        current = self.region_by_shard.get(shard)
        if (
            current is not None
            and current.path == self.ctx.self.path
            and region.path != self.ctx.self.path
        ):
            # Pekko throws here, since its coordinator never moves a shard off a region that
            # hosts it; ohra hands the local copy off instead
            self.log.error(
                "%s: shard [%s] moved from self to [%s]; stopping the local copy",
                self.type_name,
                shard,
                region.path,
            )
            local = self.shards.get(shard)
            if local is not None:
                local.tell(HandOff(shard, self.ctx.system.ignore_ref))
            for envelope in self.shard_buffers.pop(shard, ()):
                self.buffered -= 1
                self.ctx.system.dead_letter(envelope, self.ctx.self)
        self.region_by_shard[shard] = region
        if region.path == self.ctx.self.path:
            ref = self.get_shard(shard)
            if ref is not None:
                self.deliver_buffered(shard, ref)
        else:
            self.deliver_buffered(shard, region)

    def request_shard_buffer_homes(self) -> None:
        if self.coordinator is not None:
            for shard in self.shard_buffers:
                self.coordinator.tell(GetShardHome(shard, self.ctx.self))

    # --- the behavior ----------------------------------------------------------------------------

    def behavior(self) -> Behavior[Any]:
        @Behaviors.receive
        def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            if isinstance(msg, ShardingEnvelope):
                envelope = cast(ShardingEnvelope[Any], msg)
                if not envelope.entity_id:
                    self.ctx.system.dead_letter(envelope.message, self.ctx.self)
                else:
                    self.deliver(envelope)
                return (
                    Behaviors.stopped()
                    if self.try_complete_graceful_shutdown()
                    else Behaviors.same
                )
            match msg:
                case GetShardRegionState(reply_to):
                    if not self.shards:
                        reply_to.tell(CurrentShardRegionState(frozenset()))
                    else:
                        self.ctx.spawn_anonymous(
                            _aggregate_shard_states(
                                list(self.shards.values()),
                                reply_to,
                                self.settings.shard_region_query_timeout,
                            )
                        )
                case MemberUp(member=m) | MemberLeft(member=m) | MemberExited(member=m):
                    self.change_members(
                        by_age(
                            [
                                x
                                for x in self.members_by_age
                                if x.unique_address != m.unique_address
                            ]
                            + [m]
                        )
                    )
                case MemberRemoved(member=m) if (
                    m.unique_address == self.cluster.self_unique_address
                ):
                    return Behaviors.stopped()
                case MemberDowned(member=m) if (
                    m.unique_address == self.cluster.self_unique_address
                ):
                    return Behaviors.stopped()
                case MemberRemoved(member=m):
                    for shard in [
                        s
                        for s, r in self.region_by_shard.items()
                        if r.path != self.ctx.self.path and _is_at(r, m.address)
                    ]:
                        del self.region_by_shard[shard]
                    self.change_members(
                        [x for x in self.members_by_age if x.unique_address != m.unique_address]
                    )
                case MemberReadyForShutdown() | MemberPreparingForShutdown():
                    # ShardRegion.scala 801-805
                    if not self.preparing_for_shutdown:
                        self.log.info("%s. preparing for shutdown", self.type_name)
                    self.preparing_for_shutdown = True
                case MemberEvent():
                    pass
                case RegisterAck(coordinator):
                    self.coordinator = coordinator
                    self.ctx.timers.cancel("register-retry")
                    self.request_shard_buffer_homes()
                case CoordinatorStopped(coordinator):
                    if self.coordinator is not None and self.coordinator.path == coordinator.path:
                        self.coordinator = None
                        self.start_registration()
                case HostShard(shard, reply_to):
                    if self.graceful_shutdown_in_progress:
                        self.send_graceful_shutdown_to_coordinator()
                    else:
                        self.region_by_shard[shard] = self.ctx.self
                        # Start the shard, if already started this does nothing; its buffer
                        # goes out on ShardInitialized (ShardRegion.scala 819-836)
                        self.get_shard(shard)
                        reply_to.tell(ShardStarted(shard))
                case ShardHome(shard, region):
                    self.receive_shard_home(shard, region)
                case ShardHomes(homes):
                    for region, shards in homes:
                        for shard in shards:
                            self.receive_shard_home(shard, region)
                case BeginHandOff(shard, reply_to):
                    # ShardRegion.scala 855-868
                    self.log.debug("%s: BeginHandOff shard [%s]", self.type_name, shard)
                    if not self.preparing_for_shutdown:
                        self.region_by_shard.pop(shard, None)
                        reply_to.tell(BeginHandOffAck(shard, self.ctx.self))
                    else:
                        self.log.debug(
                            "%s: Ignoring begin handoff of shard [%s] as preparing to shutdown",
                            self.type_name,
                            shard,
                        )
                case HandOff(shard, reply_to):
                    dropped = self.shard_buffers.pop(shard, None)
                    if dropped:  # Pekko: avoid reordering buffered messages across a hand-off
                        self.buffered -= len(dropped)
                        for envelope in dropped:
                            self.ctx.system.dead_letter(envelope, self.ctx.self)
                        self.logged_full_buffer_warning = False
                    local = self.shards.get(shard)
                    if local is not None:
                        local.tell(msg)
                    else:
                        reply_to.tell(ShardStopped(shard))
                case _ShardTerminated(shard):
                    # ShardRegion.scala 1007-1031
                    self.shards.pop(shard, None)
                    self.starting_shards.discard(shard)
                    if (
                        self.passivation_strategy != NoPassivationStrategy()
                        and not self.graceful_shutdown_in_progress
                    ):
                        self.deliver_to_all_shards(ShardsUpdated(len(self.shards)))
                    if (
                        self.region_by_shard.get(shard) is not None
                        and self.region_by_shard[shard].path == self.ctx.self.path
                    ):
                        del self.region_by_shard[shard]
                case _Retry():
                    if self.coordinator is None:
                        self.register()
                    else:
                        self.request_shard_buffer_homes()
                    if self.graceful_shutdown_in_progress:
                        self.send_graceful_shutdown_to_coordinator()
                case _RegisterRetry():
                    if self.coordinator is None:
                        self.register()
                        self.schedule_next_registration()
                case _GracefulShutdown():
                    # ShardRegion.scala 926-946
                    if self.preparing_for_shutdown:
                        self.log.debug(
                            "%s: Skipping graceful shutdown of region and all its shards as "
                            "cluster is preparing for shutdown",
                            self.type_name,
                        )
                        self.exiting_done()
                        return Behaviors.stopped()
                    self.graceful_shutdown_in_progress = True
                    self.send_graceful_shutdown_to_coordinator()
                case ShardInitialized(shard):
                    self.initialize_shard(shard)
                case SetActiveEntityLimit():
                    self.deliver_to_all_shards(msg)
                case _:
                    pass
            return Behaviors.stopped() if self.try_complete_graceful_shutdown() else Behaviors.same

        def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            if sig is PostStop:
                if self.coordinator is not None:
                    # ShardRegion.postStop
                    self.coordinator.tell(RegionStopped(self.ctx.self.path))
                self.exiting_done()
            return Behaviors.same

        return on_message.receive_signal(on_signal)

    def send_graceful_shutdown_to_coordinator(self) -> None:
        if self.graceful_shutdown_in_progress and self.coordinator is not None:
            self.coordinator.tell(GracefulShutdownReq(self.ctx.self))

    def try_complete_graceful_shutdown(self) -> bool:
        """Pekko's tryCompleteGracefulShutdownIfInProgress: every shard handed off and nothing
        buffered."""
        if self.graceful_shutdown_in_progress and not self.shards and not self.shard_buffers:
            self.exiting_done()
            return True
        return False

    def exiting_done(self) -> None:
        if self.progress.future is not None and not self.progress.future.done():
            self.progress.future.set_result(None)


def region_behavior(
    entity: Entity[Any], settings: ClusterShardingSettings, progress: _ExitingProgress
) -> Behavior[Any]:
    behavior: Behavior[Any] = Behaviors.setup(
        lambda ctx: _Region(ctx, entity, settings, progress).behavior()
    )
    # as under Pekko's system guardian
    return Behaviors.supervise(behavior).on_failure(SupervisorStrategy.restart())
