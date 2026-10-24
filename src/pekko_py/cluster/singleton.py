"""Apache Pekko's Cluster Singleton: one instance of an actor on the oldest member, handed over when that member
leaves, reached through a proxy on every node. ClusterSingletonManager.scala and ClusterSingletonProxy.scala,
ported state for state, without lease, roles or data centers."""
from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from ..behavior import Behavior, Behaviors, Stopped
from ..context import ActorContext
from ..ref import ActorRef
from ..remote import Address, UniqueAddress
from ..remote.serialization import remotable
from ..signals import PostStop, Signal
from ..supervision import SupervisorStrategy
from .core import (
    Cluster,
    ClusterDomainEvent,
    MemberDowned,
    MemberEvent,
    MemberExited,
    MemberRemoved,
    MemberUp,
    SelfUp,
)
from .core import Subscribe as ClusterSubscribe
from .gossip import UP_NUMBER_UNSET, Member

if TYPE_CHECKING:
    from ..system import ActorSystem


@dataclass(frozen=True)
class ClusterSingletonSettings:
    """Pekko's settings and defaults, in seconds."""

    removal_margin: float = 0.0
    hand_over_retry_interval: float = 1.0
    min_number_of_hand_over_retries: int = 15
    singleton_identification_interval: float = 1.0
    buffer_size: int = 1000

    def __post_init__(self) -> None:
        if self.removal_margin < 0:
            raise ValueError("removal_margin must be >= 0")
        if self.hand_over_retry_interval <= 0 or self.singleton_identification_interval <= 0:
            raise ValueError("hand_over_retry_interval and singleton_identification_interval must be > 0")
        if self.min_number_of_hand_over_retries < 1:
            raise ValueError("min_number_of_hand_over_retries must be >= 1")
        if not 0 <= self.buffer_size <= 10000:
            raise ValueError("buffer_size must be >= 0 and <= 10000")

    def max_hand_over_retries_for(self, removal_margin: float) -> int:
        n = round(removal_margin * 1000) // round(self.hand_over_retry_interval * 1000)  # whole ms, as Pekko
        return max(self.min_number_of_hand_over_retries, n + 3)

    def max_take_over_retries_for(self, removal_margin: float) -> int:
        return max(1, self.max_hand_over_retries_for(removal_margin) - 3)

    @property
    def max_hand_over_retries(self) -> int:
        return self.max_hand_over_retries_for(self.removal_margin)

    @property
    def max_take_over_retries(self) -> int:
        return self.max_take_over_retries_for(self.removal_margin)


@dataclass(frozen=True)
class SingletonActor[M]:
    """What to run as a singleton: the behavior, its name, and how to stop it (Pekko's SingletonActor)."""

    behavior: Behavior[M]
    name: str
    stop_message: M | None = None
    settings: ClusterSingletonSettings | None = None

    def with_stop_message(self, msg: M) -> SingletonActor[M]:
        return replace(self, stop_message=msg)

    def with_settings(self, settings: ClusterSingletonSettings) -> SingletonActor[M]:
        return replace(self, settings=settings)


class ClusterSingletonManagerIsStuck(Exception):
    """Thrown when a hand-over or take-over never completes while the other node is still a member."""


# --- oldest-changed tracking (OldestChangedBuffer) ---------------------------------------------------------


@dataclass(frozen=True)
class InitialOldestState:
    oldest: tuple[UniqueAddress, ...]
    safe_to_be_oldest: bool


@dataclass(frozen=True)
class OldestChanged:
    oldest: UniqueAddress | None


class OldestChangedTracker:
    """Members by age, and a queue of changes of the oldest, consumed one at a time by the manager."""

    def __init__(self, self_unique_address: UniqueAddress) -> None:
        self.self_unique_address = self_unique_address
        self.members_by_age: list[Member] = []
        self._changes: list[InitialOldestState | OldestChanged] = []

    def handle_initial(self, members: Iterable[Member]) -> None:
        aged = [m for m in members if m.up_number != UP_NUMBER_UNSET]
        self.members_by_age = _by_age(aged)
        self_up_number = next((m.up_number for m in aged if m.unique_address == self.self_unique_address), UP_NUMBER_UNSET)
        oldest = [m for m in self.members_by_age if m.up_number <= self_up_number]
        safe = not any(m.status in ("down", "exiting", "leaving") for m in oldest)
        self._changes.append(InitialOldestState(tuple(m.unique_address for m in oldest), safe))

    def add(self, member: Member) -> None:
        self._track_change(lambda: _by_age([m for m in self.members_by_age if m.unique_address != member.unique_address] + [member]))

    def remove(self, member: Member) -> None:
        self._track_change(lambda: [m for m in self.members_by_age if m.unique_address != member.unique_address])

    def self_exiting(self) -> None:
        self._track_change(lambda: [m for m in self.members_by_age if m.unique_address != self.self_unique_address])

    def next_change(self) -> InitialOldestState | OldestChanged | None:
        return self._changes.pop(0) if self._changes else None

    def _track_change(self, update: Any) -> None:
        before = self.members_by_age[0].unique_address if self.members_by_age else None
        self.members_by_age = update()
        after = self.members_by_age[0].unique_address if self.members_by_age else None
        if before != after:
            self._changes.append(OldestChanged(after))


def _by_age(members: Iterable[Member]) -> list[Member]:
    ordered: list[Member] = []
    for m in members:
        i = 0
        while i < len(ordered) and ordered[i].is_older_than(m):
            i += 1
        ordered.insert(i, m)
    return ordered


# --- wire messages ----------------------------------------------------------------------------------------------


@remotable
@dataclass(frozen=True)
class HandOverToMe:
    from_: ActorRef[Any]


@remotable
@dataclass(frozen=True)
class HandOverInProgress: ...


@remotable
@dataclass(frozen=True)
class HandOverDone:
    from_: ActorRef[Any]


@remotable
@dataclass(frozen=True)
class TakeOverFromMe:
    from_: ActorRef[Any]


@remotable
@dataclass(frozen=True)
class SingletonIdentity:
    """The manager's answer to IdentifySingleton: the running instance, or None. ``identify_id`` echoes the
    request's, so a proxy can ignore answers from an earlier round, as Pekko's ActorIdentity correlation."""

    identify_id: int
    instance: ActorRef[Any] | None


@remotable
@dataclass(frozen=True)
class IdentifySingleton:
    reply_to: ActorRef[SingletonIdentity]
    identify_id: int


# --- the manager (ClusterSingletonManager) --------------------------------------------------------------------


@dataclass(frozen=True)
class _SelfExiting: ...


@dataclass(frozen=True)
class _HandOverRetry:
    count: int


@dataclass(frozen=True)
class _TakeOverRetry:
    count: int


@dataclass(frozen=True)
class _DelayedMemberRemoved:
    member: Member


@dataclass(frozen=True)
class _Cleanup: ...


@dataclass(frozen=True)
class _InstanceTerminated:
    ref: ActorRef[Any]


type _ManagerMsg = (
    ClusterDomainEvent
    | InitialOldestState
    | OldestChanged
    | HandOverToMe
    | HandOverInProgress
    | HandOverDone
    | TakeOverFromMe
    | IdentifySingleton
    | _SelfExiting
    | _HandOverRetry
    | _TakeOverRetry
    | _DelayedMemberRemoved
    | _Cleanup
    | _InstanceTerminated
)

class _ExitingProgress:
    """Pekko's memberExitingProgress and the SelfExiting ask, held outside the actor so a restarted manager
    (the system guardian restarts a stuck one) keeps the one registered exiting task."""

    def __init__(self) -> None:
        self.future: asyncio.Future[None] | None = None

    def task(self, cluster: Cluster, manager: ActorRef[Any]) -> Callable[[], Awaitable[None]]:
        async def run() -> None:
            if cluster.is_terminated or cluster.self_member.status == "down":
                return
            manager.tell(_SelfExiting())
            if self.future is not None:
                await asyncio.shield(self.future)

        return run


_HAND_OVER_RETRY = "hand-over-retry"
_TAKE_OVER_RETRY = "take-over-retry"


def _manager_name(name: str) -> str:
    return f"singletonManager{name}"


def _proxy_name(name: str) -> str:
    return f"singletonProxy{name}"


def _is_at(ref: ActorRef[Any], address: Address) -> bool:
    return ref.path.startswith(address.root + "/")


class _Manager:
    """ClusterSingletonManager's vars and `when` blocks; each state is a behavior returned by a method."""

    def __init__(
        self, ctx: ActorContext[_ManagerMsg], singleton: SingletonActor[Any], settings: ClusterSingletonSettings, progress: _ExitingProgress
    ) -> None:
        self.ctx = ctx
        self.singleton = singleton
        self.settings = settings
        self.cluster = Cluster(ctx.system)
        # removal_margin <= 0 falls back to the cluster's down-removal margin (ClusterSingletonManager.scala 520)
        self.removal_margin = settings.removal_margin if settings.removal_margin > 0 else self.cluster.settings.effective_down_removal_margin
        self.max_hand_over_retries = settings.max_hand_over_retries_for(self.removal_margin)
        self.max_take_over_retries = settings.max_take_over_retries_for(self.removal_margin)
        self.log = ctx.log
        self.loop = asyncio.get_running_loop()
        self.self_unique_address = self.cluster.self_unique_address
        self.tracker: OldestChangedTracker | None = None
        self.oldest_changed_received = True
        self.waiting_for_change = False
        self.self_exited = False
        self.removed: dict[UniqueAddress, float] = {}
        self.member_exiting_progress: asyncio.Future[None] = self.loop.create_future()
        progress.future = self.member_exiting_progress
        self.cluster.subscriptions.tell(ClusterSubscribe(ctx.self, ClusterDomainEvent))
        ctx.timers.start_timer_with_fixed_delay("cleanup", _Cleanup(), 60.0)

    # --- plumbing ------------------------------------------------------------------------------------------------

    def exiting_done(self) -> None:
        if not self.member_exiting_progress.done():
            self.member_exiting_progress.set_result(None)

    def peer(self, address: Address) -> ActorRef[Any]:
        return self.ctx.system.resolve(f"{address.root}/system/{_manager_name(self.singleton.name)}")

    def start_instance(self) -> ActorRef[Any]:
        self.log.info("Singleton manager starting singleton actor [%s/singleton]", self.ctx.self.path)
        ref = self.ctx.spawn(self.singleton.behavior, "singleton")
        self.ctx.watch_with(ref, _InstanceTerminated(ref))
        return ref

    def stop_instance(self, ref: ActorRef[Any]) -> None:
        if self.singleton.stop_message is not None:
            ref.tell(self.singleton.stop_message)
        else:
            self.ctx.stop(ref, after_pending=True)  # Pekko's PoisonPill

    def add_removed(self, node: UniqueAddress) -> None:
        self.removed[node] = self.loop.time() + 15 * 60

    def cleanup_overdue_not_member_any_more(self) -> None:
        now = self.loop.time()
        self.removed = {n: d for n, d in self.removed.items() if d > now}

    def get_next_oldest_changed(self) -> None:
        if self.oldest_changed_received:
            self.oldest_changed_received = False
            self.waiting_for_change = True
            self.deliver_change()

    def deliver_change(self) -> None:
        """OldestChangedBuffer's GetNext: one change at a time, only when the manager has asked."""
        if self.waiting_for_change and self.tracker is not None:
            change = self.tracker.next_change()
            if change is not None:
                self.waiting_for_change = False
                self.ctx.self.tell(change)

    def track(self, event: ClusterDomainEvent) -> None:
        """What OldestChangedBuffer does with the cluster events, before the manager's own handling."""
        match event:
            case SelfUp(current_cluster_state=state) if self.tracker is None:
                self.tracker = OldestChangedTracker(self.self_unique_address)
                self.tracker.handle_initial(state.members)
                self.get_next_oldest_changed()
            case MemberUp(member=m) if self.tracker is not None:
                self.tracker.add(m)
            case MemberExited(member=m) if self.tracker is not None and m.unique_address != self.self_unique_address:
                self.tracker.remove(m)
            case MemberRemoved(member=m) if self.tracker is not None:
                self.tracker.remove(m)
            case _:
                pass
        self.deliver_change()

    def schedule_delayed_member_removed(self, m: Member) -> None:
        if self.removal_margin > 0:
            self.ctx.timers.start_single_timer(("delayed-removed", m.unique_address), _DelayedMemberRemoved(m), self.removal_margin)
        else:
            self.ctx.self.tell(_DelayedMemberRemoved(m))

    def member_address_of(self, ref: ActorRef[Any]) -> UniqueAddress | None:
        return next((m.unique_address for m in self.cluster.state.members if _is_at(ref, m.address)), None)

    def self_member_exited(self) -> None:
        self.self_exited = True
        self.log.info("Exited [%s]", self.cluster.self_address)

    def stop(self) -> Behavior[_ManagerMsg]:
        return Behaviors.stopped()

    def behavior(self, handler: Callable[[_ManagerMsg], Behavior[_ManagerMsg] | None]) -> Behavior[_ManagerMsg]:
        """A state: the state's own cases first, then Pekko's whenUnhandled."""

        def on_message(_ctx: ActorContext[_ManagerMsg], msg: _ManagerMsg) -> Behavior[_ManagerMsg]:
            if isinstance(msg, ClusterDomainEvent):
                self.track(msg)
            elif isinstance(msg, _SelfExiting) and self.tracker is not None:
                self.tracker.self_exiting()  # OldestChangedBuffer's SelfExiting: a leaving node is no longer a candidate
                self.deliver_change()
            result = handler(msg)
            return result if result is not None else self.unhandled(msg)

        def on_signal(_ctx: ActorContext[_ManagerMsg], sig: Signal) -> Behavior[_ManagerMsg]:
            if sig is PostStop:
                self.exiting_done()
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    def unhandled(self, msg: _ManagerMsg) -> Behavior[_ManagerMsg]:
        match msg:
            case _SelfExiting():
                self.self_member_exited()
                self.exiting_done()
            case MemberRemoved(member=m) if m.unique_address == self.self_unique_address and not self.self_exited:
                return self.stop()
            case MemberRemoved(member=m):
                self.add_removed(m.unique_address)
            case _DelayedMemberRemoved(member=m):
                self.add_removed(m.unique_address)
            case _Cleanup():
                self.cleanup_overdue_not_member_any_more()
            case IdentifySingleton(reply_to, identify_id):
                reply_to.tell(SingletonIdentity(identify_id, None))
            case _:
                pass
        return Behaviors.same

    def handle_member_event(self, event: ClusterDomainEvent) -> Behavior[_ManagerMsg] | None:
        match event:
            case MemberRemoved(member=m) | MemberDowned(member=m) if m.unique_address == self.self_unique_address:
                return self.stop()
            case _:
                return Behaviors.same

    # --- states ------------------------------------------------------------------------------------------------

    def start(self) -> Behavior[_ManagerMsg]:
        def handler(msg: _ManagerMsg) -> Behavior[_ManagerMsg] | None:
            match msg:
                case InitialOldestState(oldest, safe_to_be_oldest):
                    self.oldest_changed_received = True
                    head = oldest[0] if oldest else None
                    previous = [o for o in oldest if o != self.self_unique_address]
                    if head == self.self_unique_address and safe_to_be_oldest:
                        return self.goto_oldest()
                    if head == self.self_unique_address:
                        return self.goto_becoming_oldest(previous)
                    return self.goto_younger(previous)
                case HandOverToMe():
                    return Behaviors.same
                case ClusterDomainEvent():
                    return self.handle_member_event(msg)
                case _:
                    return None

        return self.behavior(handler)

    def goto_younger(self, previous_oldest: list[UniqueAddress]) -> Behavior[_ManagerMsg]:
        if self.self_unique_address in self.removed:
            return self.stop()
        self.get_next_oldest_changed()
        return self.younger(previous_oldest)

    def younger(self, previous_oldest: list[UniqueAddress]) -> Behavior[_ManagerMsg]:
        def handler(msg: _ManagerMsg) -> Behavior[_ManagerMsg] | None:
            match msg:
                case OldestChanged(oldest):
                    self.oldest_changed_received = True
                    if oldest == self.self_unique_address:
                        if all(p in self.removed for p in previous_oldest):
                            return self.goto_oldest()
                        self.peer(previous_oldest[0].address).tell(HandOverToMe(self.ctx.self))
                        return self.goto_becoming_oldest(previous_oldest)
                    self.log.info("Younger observed OldestChanged: [%s -> %s]", previous_oldest[:1], oldest)
                    self.get_next_oldest_changed()
                    if oldest is not None and oldest not in previous_oldest:
                        return self.younger([oldest, *previous_oldest])
                    return Behaviors.same
                case MemberRemoved(member=m) if m.unique_address != self.self_unique_address:
                    self.schedule_delayed_member_removed(m)
                    return Behaviors.same
                case ClusterDomainEvent():
                    return self.handle_member_event(msg)
                case _DelayedMemberRemoved(member=m):
                    self.add_removed(m.unique_address)
                    return self.younger([p for p in previous_oldest if p != m.unique_address])
                case HandOverToMe(from_):
                    if self.cluster.self_member.status not in ("leaving", "exiting"):
                        from_.tell(HandOverDone(self.ctx.self))
                    return Behaviors.same
                case _:
                    return None

        return self.behavior(handler)

    def goto_becoming_oldest(self, previous_oldest: list[UniqueAddress]) -> Behavior[_ManagerMsg]:
        self.ctx.timers.start_single_timer(_HAND_OVER_RETRY, _HandOverRetry(1), self.settings.hand_over_retry_interval)
        return self.becoming_oldest(previous_oldest)

    def becoming_oldest(self, previous_oldest: list[UniqueAddress]) -> Behavior[_ManagerMsg]:
        def leave() -> None:
            self.ctx.timers.cancel(_HAND_OVER_RETRY)

        def handler(msg: _ManagerMsg) -> Behavior[_ManagerMsg] | None:
            match msg:
                case HandOverInProgress():
                    self.ctx.timers.cancel(_HAND_OVER_RETRY)
                    return Behaviors.same
                case HandOverDone(from_):
                    if previous_oldest and _is_at(from_, previous_oldest[0].address):
                        leave()
                        return self.goto_oldest()
                    self.log.info("Ignoring HandOverDone in BecomingOldest from [%s]. Expected previous oldest [%s]", from_.path, previous_oldest[:1])
                    return Behaviors.same
                case MemberRemoved(member=m) if m.unique_address != self.self_unique_address:
                    self.schedule_delayed_member_removed(m)
                    return Behaviors.same
                case ClusterDomainEvent() if isinstance(msg, MemberEvent) and msg.member.unique_address == self.self_unique_address:
                    result = self.handle_member_event(msg)
                    if isinstance(result, Stopped):
                        leave()
                    return result
                case _DelayedMemberRemoved(member=m):
                    self.add_removed(m.unique_address)
                    if self.cluster.is_terminated:
                        return Behaviors.same
                    if m.unique_address in previous_oldest and all(p in self.removed for p in previous_oldest):
                        leave()
                        return self.goto_oldest()
                    return self.becoming_oldest([p for p in previous_oldest if p != m.unique_address])
                case TakeOverFromMe(from_):
                    sender = self.member_address_of(from_)
                    if sender is None:
                        return Behaviors.same
                    if previous_oldest:
                        if previous_oldest[0] == sender:
                            from_.tell(HandOverToMe(self.ctx.self))
                        else:
                            self.log.info("Ignoring TakeOver request in BecomingOldest from [%s]. Expected previous oldest [%s]", from_.path, previous_oldest[0])
                        return Behaviors.same
                    from_.tell(HandOverToMe(self.ctx.self))
                    return self.becoming_oldest([sender, *previous_oldest])
                case _HandOverRetry(count):
                    if count <= self.max_hand_over_retries:
                        if previous_oldest:
                            self.peer(previous_oldest[0].address).tell(HandOverToMe(self.ctx.self))
                        self.ctx.timers.start_single_timer(_HAND_OVER_RETRY, _HandOverRetry(count + 1), self.settings.hand_over_retry_interval)
                        return Behaviors.same
                    if all(p in self.removed for p in previous_oldest):
                        return self.goto_oldest()
                    if self.cluster.is_terminated:
                        return self.stop()
                    raise ClusterSingletonManagerIsStuck(
                        f"Becoming singleton oldest was stuck because previous oldest [{previous_oldest[:1]}] is unresponsive"
                    )
                case _:
                    return None

        return self.behavior(handler)

    def goto_oldest(self) -> Behavior[_ManagerMsg]:
        instance = self.start_instance()
        self.get_next_oldest_changed()
        return self.oldest(instance)

    def oldest(self, instance: ActorRef[Any] | None) -> Behavior[_ManagerMsg]:
        def handler(msg: _ManagerMsg) -> Behavior[_ManagerMsg] | None:
            match msg:
                case OldestChanged(new_oldest):
                    return self.handle_oldest_changed(instance, new_oldest)
                case HandOverToMe(from_):
                    return self.goto_handing_over(instance, from_)
                case TakeOverFromMe(from_):
                    from_.tell(HandOverToMe(self.ctx.self))
                    return Behaviors.same
                case _InstanceTerminated(ref) if ref == instance:
                    return self.oldest(None)
                case _SelfExiting():
                    self.self_member_exited()
                    return Behaviors.same
                case MemberDowned(member=m) if m.unique_address == self.self_unique_address:
                    return self.goto_stopping(instance) if instance is not None else self.stop()
                case ClusterDomainEvent():
                    return self.handle_member_event(msg)
                case HandOverDone():
                    return Behaviors.same
                case IdentifySingleton(reply_to, identify_id):
                    reply_to.tell(SingletonIdentity(identify_id, instance))
                    return Behaviors.same
                case _:
                    return None

        return self.behavior(handler)

    def handle_oldest_changed(self, instance: ActorRef[Any] | None, new_oldest: UniqueAddress | None) -> Behavior[_ManagerMsg]:
        self.oldest_changed_received = True
        if new_oldest == self.self_unique_address:
            return Behaviors.same
        if new_oldest is not None and not self.self_exited and new_oldest in self.removed:
            return self.goto_handing_over(instance, None)
        if new_oldest is not None:
            self.peer(new_oldest.address).tell(TakeOverFromMe(self.ctx.self))
        self.ctx.timers.start_single_timer(_TAKE_OVER_RETRY, _TakeOverRetry(1), self.settings.hand_over_retry_interval)
        return self.was_oldest(instance, new_oldest)

    def was_oldest(self, instance: ActorRef[Any] | None, new_oldest: UniqueAddress | None) -> Behavior[_ManagerMsg]:
        def leave() -> None:
            self.ctx.timers.cancel(_TAKE_OVER_RETRY)

        def handler(msg: _ManagerMsg) -> Behavior[_ManagerMsg] | None:
            match msg:
                case _TakeOverRetry(count):
                    if (self.cluster.is_terminated or self.self_exited) and (new_oldest is None or count > self.max_take_over_retries):
                        leave()
                        return self.goto_stopping(instance) if instance is not None else self.stop()
                    if count <= self.max_take_over_retries:
                        if new_oldest is not None:
                            self.peer(new_oldest.address).tell(TakeOverFromMe(self.ctx.self))
                        self.ctx.timers.start_single_timer(_TAKE_OVER_RETRY, _TakeOverRetry(count + 1), self.settings.hand_over_retry_interval)
                        return Behaviors.same
                    raise ClusterSingletonManagerIsStuck(f"Expected hand-over to [{new_oldest}] never occurred")
                case HandOverToMe(from_):
                    leave()
                    return self.goto_handing_over(instance, from_)
                case MemberRemoved(member=m) if m.unique_address == self.self_unique_address and not self.self_exited:
                    leave()
                    return self.stop()
                case MemberRemoved(member=m) if not self.self_exited and new_oldest is not None and m.unique_address == new_oldest:
                    self.add_removed(m.unique_address)
                    leave()
                    return self.goto_handing_over(instance, None)
                case _InstanceTerminated(ref) if instance is not None and ref == instance:
                    return self.was_oldest(None, new_oldest)
                case _SelfExiting():
                    self.self_member_exited()
                    return Behaviors.same
                case MemberDowned(member=m) if m.unique_address == self.self_unique_address:
                    leave()
                    return self.goto_stopping(instance) if instance is not None else self.stop()
                case ClusterDomainEvent():
                    return self.handle_member_event(msg)
                case IdentifySingleton(reply_to, identify_id):
                    reply_to.tell(SingletonIdentity(identify_id, instance))
                    return Behaviors.same
                case _:
                    return None

        return self.behavior(handler)

    def goto_handing_over(self, instance: ActorRef[Any] | None, hand_over_to: ActorRef[Any] | None) -> Behavior[_ManagerMsg]:
        if instance is None:
            return self.hand_over_done(hand_over_to)
        if hand_over_to is not None:
            hand_over_to.tell(HandOverInProgress())
        self.stop_instance(instance)
        return self.handing_over(instance, hand_over_to)

    def handing_over(self, instance: ActorRef[Any], hand_over_to: ActorRef[Any] | None) -> Behavior[_ManagerMsg]:
        def handler(msg: _ManagerMsg) -> Behavior[_ManagerMsg] | None:
            match msg:
                case _InstanceTerminated(ref) if ref == instance:
                    return self.hand_over_done(hand_over_to)
                case HandOverToMe(from_) if hand_over_to is not None and from_.path == hand_over_to.path:
                    from_.tell(HandOverInProgress())
                    return Behaviors.same
                case _SelfExiting():
                    self.self_member_exited()
                    return Behaviors.same
                case IdentifySingleton(reply_to, identify_id):
                    reply_to.tell(SingletonIdentity(identify_id, instance))
                    return Behaviors.same
                case _:
                    return None

        return self.behavior(handler)

    def hand_over_done(self, hand_over_to: ActorRef[Any] | None) -> Behavior[_ManagerMsg]:
        self.log.info("Singleton terminated, hand-over done [%s -> %s]", self.cluster.self_address, hand_over_to.path if hand_over_to else None)
        if hand_over_to is not None:
            hand_over_to.tell(HandOverDone(self.ctx.self))
        self.exiting_done()
        if self.self_unique_address in self.removed:
            return self.stop()
        if hand_over_to is None:
            return self.goto_younger([])
        return self.end()

    def goto_stopping(self, instance: ActorRef[Any]) -> Behavior[_ManagerMsg]:
        self.stop_instance(instance)
        return self.stopping(instance)

    def stopping(self, instance: ActorRef[Any]) -> Behavior[_ManagerMsg]:
        def handler(msg: _ManagerMsg) -> Behavior[_ManagerMsg] | None:
            match msg:
                case _InstanceTerminated(ref) if ref == instance:
                    return self.stop()
                case IdentifySingleton(reply_to, identify_id):
                    reply_to.tell(SingletonIdentity(identify_id, instance))
                    return Behaviors.same
                case _:
                    return None

        return self.behavior(handler)

    def end(self) -> Behavior[_ManagerMsg]:
        if self.self_unique_address in self.removed:
            return self.stop()

        def handler(msg: _ManagerMsg) -> Behavior[_ManagerMsg] | None:
            match msg:
                case MemberRemoved(member=m) if m.unique_address == self.self_unique_address:
                    return self.stop()
                case OldestChanged() | HandOverToMe():
                    return Behaviors.same
                case _:
                    return None

        return self.behavior(handler)


def _manager(singleton: SingletonActor[Any], settings: ClusterSingletonSettings, progress: _ExitingProgress) -> Behavior[_ManagerMsg]:
    behavior: Behavior[_ManagerMsg] = Behaviors.setup(lambda ctx: _Manager(ctx, singleton, settings, progress).start())
    # Pekko's manager lives under the classic system guardian, which restarts it; ClusterSingletonManagerIsStuck is
    # meant to be handled by a restart
    return Behaviors.supervise(behavior).on_failure(SupervisorStrategy.restart())


# --- the proxy (ClusterSingletonProxy) ------------------------------------------------------------------------


@dataclass(frozen=True)
class _TryToIdentifySingleton: ...


def _proxy(name: str, settings: ClusterSingletonSettings) -> Behavior[Any]:
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        cluster = Cluster(ctx.system)
        members_by_age: list[Member] = []
        instance: ActorRef[Any] | None = None
        buffer: deque[Any] = deque()
        identify_id = 0
        cluster.subscriptions.tell(ClusterSubscribe(ctx.self, MemberEvent))

        def identify_singleton() -> None:
            nonlocal instance, identify_id
            instance = None
            identify_id += 1
            ctx.timers.start_timer_with_fixed_delay("identify", _TryToIdentifySingleton(), settings.singleton_identification_interval, 0.0)

        def track_change(update: Callable[[], list[Member]]) -> None:
            nonlocal members_by_age
            before = members_by_age[0].unique_address if members_by_age else None
            members_by_age = update()
            after = members_by_age[0].unique_address if members_by_age else None
            ctx.log.debug("proxy oldest %s -> %s", before, after)
            if before != after:
                identify_singleton()

        def send_buffered(target: ActorRef[Any]) -> None:
            while buffer:
                target.tell(buffer.popleft())

        def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            nonlocal instance
            match msg:
                case MemberUp(member=m):
                    track_change(lambda: _by_age([x for x in members_by_age if x.unique_address != m.unique_address] + [m]))
                case MemberRemoved(member=m) if m.unique_address == cluster.self_unique_address:
                    return Behaviors.stopped()
                case MemberExited(member=m) | MemberRemoved(member=m):
                    track_change(lambda: [x for x in members_by_age if x.unique_address != m.unique_address])
                case ClusterDomainEvent():
                    pass
                case SingletonIdentity(answered_id, _) if answered_id != identify_id:
                    pass  # an answer from an earlier round, possibly from a host that is gone
                case SingletonIdentity(_, found):
                    if found is not None:
                        instance = found
                        send_buffered(found)
                    elif instance is not None:
                        instance = None  # the instance is gone: buffer until the next identity
                case _TryToIdentifySingleton():
                    if members_by_age:
                        oldest = members_by_age[0]
                        try:
                            manager = ctx.system.resolve(f"{oldest.address.root}/system/{_manager_name(name)}")
                        except LookupError:  # a local manager that is between restarts: ask again next tick
                            manager = None
                        if manager is not None:
                            manager.tell(IdentifySingleton(ctx.self, identify_id))
                case _:
                    if instance is not None:
                        instance.tell(msg)
                    elif settings.buffer_size == 0:
                        ctx.system.dead_letter(msg, ctx.self)
                    else:
                        if len(buffer) == settings.buffer_size:
                            ctx.system.dead_letter(buffer.popleft(), ctx.self)
                        buffer.append(msg)
            return Behaviors.same

        return Behaviors.receive(on_message)

    return Behaviors.setup(setup)


# --- the extension -----------------------------------------------------------------------------------------------


class ClusterSingleton:
    """``ClusterSingleton(system).init(SingletonActor(behavior, name))`` returns the proxy ref, as Pekko Typed."""

    system: ActorSystem[Any]
    _proxies: dict[str, ActorRef[Any]]

    def __new__(cls, system: ActorSystem[Any]) -> ClusterSingleton:
        if system.cluster is None:
            raise RuntimeError(f"system {system.name!r} has no cluster: pass cluster=ClusterSettings(...) to ActorSystem")
        if system.cluster_singleton is None:
            self = object.__new__(cls)
            self.system = system
            self._proxies = {}
            system.cluster_singleton = self
        return system.cluster_singleton

    def init[M](self, singleton: SingletonActor[M]) -> ActorRef[M]:
        existing = self._proxies.get(singleton.name)
        if existing is not None:
            return existing
        settings = singleton.settings if singleton.settings is not None else ClusterSingletonSettings()
        root = self.system._system_root  # pyright: ignore[reportPrivateUsage]
        progress = _ExitingProgress()
        manager = root.spawn_child(_manager(singleton, settings, progress), _manager_name(singleton.name), None)
        Cluster(self.system).add_exiting_task(progress.task(Cluster(self.system), manager.ref))
        proxy = root.spawn_child(_proxy(singleton.name, settings), _proxy_name(singleton.name), None)
        self._proxies[singleton.name] = proxy.ref
        return proxy.ref
