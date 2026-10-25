"""The ShardRegion: routes by shard id, registers with the coordinator, hosts Shard actors, hands off on leave."""
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
    ShardingEnvelope,
    ShardStopped,
    _aggregate_shard_states,  # pyright: ignore[reportPrivateUsage]
    shard_behavior,
    shard_id_of,
)
from ..core import (
    Cluster,
    MemberDowned,
    MemberEvent,
    MemberExited,
    MemberLeft,
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
    """CoordinatedShutdown's region-shutdown task (Pekko's GracefulShutdown): hand every shard off."""


class _ExitingProgress:
    """The region's cluster-sharding-shutdown-region task: ask it to shut down gracefully, wait until it holds nothing."""

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
    def __init__(self, ctx: ActorContext[Any], entity: Entity[Any], settings: ClusterShardingSettings, progress: _ExitingProgress) -> None:
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
        self.region_by_shard: dict[str, ActorRef[Any]] = {}
        self.shard_buffers: dict[str, deque[ShardingEnvelope[Any]]] = {}
        self.buffered = 0
        self.next_registration_delay = max(0.1, settings.retry_interval / 8)
        self.graceful_shutdown_in_progress = False
        self.cluster.subscriptions.tell(ClusterSubscribe(ctx.self, MemberEvent))
        ctx.timers.start_timer_with_fixed_delay("retry", _Retry(), settings.retry_interval)

    # --- coordinator location -----------------------------------------------------------------------------------------

    def coordinator_path(self, member: Member) -> str:
        return f"{member.address.root}/system/{_manager_name(f'{self.type_name}Coordinator')}/singleton"

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
            self.ctx.timers.start_single_timer("register-retry", _RegisterRetry(), self.next_registration_delay)
            self.next_registration_delay *= 2

    def coordinator_selection(self) -> list[Member]:
        """Pekko's coordinatorSelection: the members by age up to and including the first Up one, since the
        coordinator may still be on an older member that is leaving."""
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
            except LookupError:  # the local singleton instance is not up yet: the next retry asks again
                continue
            coordinator.tell(Register(self.ctx.self, tuple(self.shards)))

    # --- delivery -----------------------------------------------------------------------------------------------------

    def shard_for(self, shard: str) -> ActorRef[Any]:
        ref = self.shards.get(shard)
        if ref is None:
            ref = self.ctx.spawn(shard_behavior(self.entity, shard, self.ctx.self), shard)
            self.ctx.watch_with(ref, _ShardTerminated(shard))
            self.shards[shard] = ref
        return ref

    def deliver(self, envelope: ShardingEnvelope[Any]) -> None:
        shard = shard_id_of(envelope.entity_id, self.settings.number_of_shards)
        home = self.region_by_shard.get(shard)
        if home is None:
            self.buffer(shard, envelope)
            if self.coordinator is not None and len(self.shard_buffers[shard]) == 1:
                self.coordinator.tell(GetShardHome(shard, self.ctx.self))
        elif home.path == self.ctx.self.path:
            self.shard_for(shard).tell(envelope)
        else:
            home.tell(envelope)

    def buffer(self, shard: str, envelope: ShardingEnvelope[Any]) -> None:
        if self.buffered >= self.settings.buffer_size:
            self.log.warning("%s: buffer is full, dropping message for shard [%s]", self.type_name, shard)
            self.ctx.system.dead_letter(envelope, self.ctx.self)
            return
        self.shard_buffers.setdefault(shard, deque()).append(envelope)
        self.buffered += 1

    def deliver_buffered(self, shard: str, receiver: ActorRef[Any]) -> None:
        for envelope in self.shard_buffers.pop(shard, ()):
            self.buffered -= 1
            receiver.tell(envelope)

    def receive_shard_home(self, shard: str, region: ActorRef[Any]) -> None:
        current = self.region_by_shard.get(shard)
        if current is not None and current.path == self.ctx.self.path and region.path != self.ctx.self.path:
            # Pekko throws here because distributed data makes it impossible; without it a registration that
            # arrives after the coordinator's gate opened can lose a shard it hosts: hand the local copy off
            self.log.error("%s: shard [%s] moved from self to [%s]; stopping the local copy", self.type_name, shard, region.path)
            local = self.shards.get(shard)
            if local is not None:
                local.tell(HandOff(shard, self.ctx.system.ignore_ref))
            for envelope in self.shard_buffers.pop(shard, ()):
                self.buffered -= 1
                self.ctx.system.dead_letter(envelope, self.ctx.self)
        self.region_by_shard[shard] = region
        if region.path == self.ctx.self.path:
            self.deliver_buffered(shard, self.shard_for(shard))
        else:
            self.deliver_buffered(shard, region)

    def request_shard_buffer_homes(self) -> None:
        if self.coordinator is not None:
            for shard in self.shard_buffers:
                self.coordinator.tell(GetShardHome(shard, self.ctx.self))

    # --- the behavior ---------------------------------------------------------------------------------------------

    def behavior(self) -> Behavior[Any]:
        def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            if isinstance(msg, ShardingEnvelope):
                envelope = cast(ShardingEnvelope[Any], msg)
                if not envelope.entity_id:
                    self.ctx.system.dead_letter(envelope.message, self.ctx.self)
                else:
                    self.deliver(envelope)
                return Behaviors.stopped() if self.try_complete_graceful_shutdown() else Behaviors.same
            match msg:
                case GetShardRegionState(reply_to):
                    if not self.shards:
                        reply_to.tell(CurrentShardRegionState(frozenset()))
                    else:
                        self.ctx.spawn_anonymous(_aggregate_shard_states(list(self.shards.values()), reply_to, self.settings.shard_region_query_timeout))
                case MemberUp(member=m) | MemberLeft(member=m) | MemberExited(member=m):
                    self.change_members(by_age([x for x in self.members_by_age if x.unique_address != m.unique_address] + [m]))
                case MemberRemoved(member=m) if m.unique_address == self.cluster.self_unique_address:
                    return Behaviors.stopped()
                case MemberDowned(member=m) if m.unique_address == self.cluster.self_unique_address:
                    return Behaviors.stopped()
                case MemberRemoved(member=m):
                    for shard in [s for s, r in self.region_by_shard.items() if r.path != self.ctx.self.path and _is_at(r, m.address)]:
                        del self.region_by_shard[shard]
                    self.change_members([x for x in self.members_by_age if x.unique_address != m.unique_address])
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
                        self.shard_for(shard)
                        reply_to.tell(ShardStarted(shard))
                        self.deliver_buffered(shard, self.shards[shard])
                case ShardHome(shard, region):
                    self.receive_shard_home(shard, region)
                case ShardHomes(homes):
                    for region, shards in homes:
                        for shard in shards:
                            self.receive_shard_home(shard, region)
                case BeginHandOff(shard, reply_to):
                    self.region_by_shard.pop(shard, None)
                    reply_to.tell(BeginHandOffAck(shard, self.ctx.self))
                case HandOff(shard, reply_to):
                    dropped = self.shard_buffers.pop(shard, None)
                    if dropped:  # Pekko: avoid reordering buffered messages across a hand-off
                        self.buffered -= len(dropped)
                        for envelope in dropped:
                            self.ctx.system.dead_letter(envelope, self.ctx.self)
                    local = self.shards.get(shard)
                    if local is not None:
                        local.tell(msg)
                    else:
                        reply_to.tell(ShardStopped(shard))
                case _ShardTerminated(shard):
                    self.shards.pop(shard, None)
                    if self.region_by_shard.get(shard) is not None and self.region_by_shard[shard].path == self.ctx.self.path:
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
                    self.graceful_shutdown_in_progress = True
                    self.send_graceful_shutdown_to_coordinator()
                case _:
                    pass
            return Behaviors.stopped() if self.try_complete_graceful_shutdown() else Behaviors.same

        def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            if sig is PostStop:
                if self.coordinator is not None:
                    self.coordinator.tell(RegionStopped(self.ctx.self.path))  # ShardRegion.postStop
                self.exiting_done()
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    def send_graceful_shutdown_to_coordinator(self) -> None:
        if self.graceful_shutdown_in_progress and self.coordinator is not None:
            self.coordinator.tell(GracefulShutdownReq(self.ctx.self))

    def try_complete_graceful_shutdown(self) -> bool:
        """Pekko's tryCompleteGracefulShutdownIfInProgress: every shard handed off and nothing buffered."""
        if self.graceful_shutdown_in_progress and not self.shards and not self.shard_buffers:
            self.exiting_done()
            return True
        return False

    def exiting_done(self) -> None:
        if self.progress.future is not None and not self.progress.future.done():
            self.progress.future.set_result(None)


def region_behavior(entity: Entity[Any], settings: ClusterShardingSettings, progress: _ExitingProgress) -> Behavior[Any]:
    behavior: Behavior[Any] = Behaviors.setup(lambda ctx: _Region(ctx, entity, settings, progress).behavior())
    return Behaviors.supervise(behavior).on_failure(SupervisorStrategy.restart())  # as under Pekko's system guardian


