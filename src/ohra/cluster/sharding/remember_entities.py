"""Pekko's remember-entities stores: the store protocols (RememberEntitiesStore.scala 54-94) and
their Distributed Data implementation, DDataRememberEntitiesShardStore.scala. Internal, as in
Pekko."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from ...behavior import Behavior, Behaviors
from ...context import ActorContext
from ...ref import ActorRef
from ...util.frequency_sketch import java_hash_code
from ..core import Cluster
from ..ddata import SelfUniqueAddress
from ..ddata.distributed_data import ReplicatorMessageAdapter
from ..ddata.gset import GSet, GSetKey
from ..ddata.orset import ORSet, ORSetKey
from ..ddata.replicator import Replicator

if TYPE_CHECKING:
    from .shard import ClusterShardingSettings

# --- the protocols (RememberEntitiesStore.scala 54-94) -------------------------------------------


class RememberEntitiesShardStore:
    """The protocol of a shard's store (RememberEntitiesStore.scala 54-71). The store is not
    expected to receive a new ``Update`` before it has answered the previous one."""

    @dataclass(frozen=True)
    class Update:
        started: frozenset[str]
        stopped: frozenset[str]
        reply_to: ActorRef[RememberEntitiesShardStore.UpdateDone]

    @dataclass(frozen=True)
    class UpdateDone:
        started: frozenset[str]
        stopped: frozenset[str]

    @dataclass(frozen=True)
    class GetEntities:
        reply_to: ActorRef[RememberEntitiesShardStore.RememberedEntities]

    @dataclass(frozen=True)
    class RememberedEntities:
        entities: frozenset[str]

    type Command = RememberEntitiesShardStore.Update | RememberEntitiesShardStore.GetEntities


class RememberEntitiesCoordinatorStore:
    """The protocol of the coordinator's store (RememberEntitiesStore.scala 73-94)."""

    @dataclass(frozen=True)
    class AddShard:
        """Sent once for every started shard (but could be retried), answered with
        ``UpdateDone`` or ``UpdateFailed``."""

        shard_id: str
        reply_to: ActorRef[
            RememberEntitiesCoordinatorStore.UpdateDone
            | RememberEntitiesCoordinatorStore.UpdateFailed
        ]

    @dataclass(frozen=True)
    class UpdateDone:
        shard_id: str

    @dataclass(frozen=True)
    class UpdateFailed:
        shard_id: str

    @dataclass(frozen=True)
    class GetShards:
        """Sent once when the coordinator starts (but could be retried), answered with
        ``RememberedShards``."""

        reply_to: ActorRef[RememberEntitiesCoordinatorStore.RememberedShards]

    @dataclass(frozen=True)
    class RememberedShards:
        shard_ids: frozenset[str]

    type Command = (
        RememberEntitiesCoordinatorStore.AddShard | RememberEntitiesCoordinatorStore.GetShards
    )


# --- the consistencies, shared by the DData stores -----------------------------------------------


def _read_consistency(
    settings: ClusterShardingSettings, timeout: float, majority_min_cap: int
) -> Replicator.ReadAll | Replicator.ReadMajorityPlus:
    n = settings.coordinator_state_read_majority_plus
    if n == "all":
        return Replicator.ReadAll(timeout)
    return Replicator.ReadMajorityPlus(timeout, n, majority_min_cap)


def _write_consistency(
    settings: ClusterShardingSettings, timeout: float, majority_min_cap: int
) -> Replicator.WriteAll | Replicator.WriteMajorityPlus:
    n = settings.coordinator_state_write_majority_plus
    if n == "all":
        return Replicator.WriteAll(timeout)
    return Replicator.WriteMajorityPlus(timeout, n, majority_min_cap)


# --- DDataRememberEntitiesShardStore -------------------------------------------------------------

# The default maximum-frame-size is 256 KiB with Artery.
# When using entity identifiers with 36 character strings (e.g. UUID.randomUUID).
# By splitting the elements over 5 keys we can support 10000 entities per shard.
# The Gossip message size of 5 ORSet with 2000 ids is around 200 KiB.
# This is by intention not configurable because it's important to have the same
# configuration on each node.
NUMBER_OF_KEYS = 5


@dataclass(frozen=True)
class _Started:
    """Pekko's private ``Evt`` (74-78)."""

    id: str


@dataclass(frozen=True)
class _Stopped:
    id: str


type _Evt = _Started | _Stopped


@dataclass(frozen=True)
class _GetResult:
    """A Get's response with its key index: Pekko's ``Some(i)`` request."""

    i: int
    response: Replicator.GetResponse[ORSet[str]]


@dataclass(frozen=True)
class _UpdateResult:
    """An Update's response with the events it wrote: Pekko's ``Some(evts)`` request."""

    evts: frozenset[_Evt]
    response: Replicator.UpdateResponse[ORSet[str]]


type _Msg = RememberEntitiesShardStore.Command | _GetResult | _UpdateResult


def ddata_shard_store(
    type_name: str,
    shard_id: str,
    replicator: ActorRef[Replicator.Command],
    settings: ClusterShardingSettings,
    majority_min_cap: int,
) -> Behavior[RememberEntitiesShardStore.Command]:
    """DDataRememberEntitiesShardStore (DDataRememberEntitiesShardStore.scala 86-295): a shard's
    entity ids in ``NUMBER_OF_KEYS`` ORSets, read by majority when the store starts and written
    by majority, one Update per key, for each batch of started and stopped ids. It stops when a
    read or write fails for good, which restarts the shard."""

    @Behaviors.setup
    def setup(ctx: ActorContext[_Msg]) -> Behavior[_Msg]:
        log = ctx.log
        self_unique_address = SelfUniqueAddress(Cluster(ctx.system).self_unique_address)
        adapter = ReplicatorMessageAdapter[_Msg, ORSet[str]](
            ctx,
            replicator,
            # past both the Get's and the Update's own timeout, by which the Replicator answers
            max(settings.waiting_for_state_timeout, settings.updating_state_timeout) + 1,
        )
        read_consistency = _read_consistency(
            settings, settings.waiting_for_state_timeout, majority_min_cap
        )
        # Note that the timeout is actually updatingStateTimeout / 4 so that we fit 3 retries and
        # a response in the timeout before the shard sees it as a failure
        write_consistency = _write_consistency(
            settings, settings.updating_state_timeout / 4, majority_min_cap
        )
        max_update_attempts = 3
        # Note: total for all 5 keys
        max_read_attempts_left = 15
        keys = [ORSetKey[str](f"shard-{type_name}-{shard_id}-{i}") for i in range(NUMBER_OF_KEYS)]

        log.debug(
            "Starting up DDataRememberEntitiesStore, read timeout: [%g s], write timeout: [%g s], "
            "majority min cap: [%s]",
            settings.waiting_for_state_timeout,
            settings.updating_state_timeout,
            majority_min_cap,
        )

        def key(entity_id: str) -> ORSetKey[str]:
            """124-127: Scala's ``math.abs(id.hashCode % 5)``."""
            return keys[abs(java_hash_code(entity_id)) % NUMBER_OF_KEYS]

        def get(i: int) -> None:
            adapter.ask_get(
                lambda reply_to: Replicator.Get(keys[i], read_consistency, reply_to),
                lambda rsp: _GetResult(i, rsp),
            )

        def update(evts: frozenset[_Evt]) -> None:
            def modify(existing: ORSet[str]) -> ORSet[str]:
                for evt in evts:
                    match evt:
                        case _Started(entity_id):
                            existing = existing.add(self_unique_address, entity_id)
                        case _Stopped(entity_id):
                            existing = existing.remove(self_unique_address, entity_id)
                return existing

            evts_key = key(next(iter(evts)).id)  # all on one key
            adapter.ask_update(
                lambda reply_to: Replicator.Update(
                    evts_key, ORSet.empty, write_consistency, reply_to, modify
                ),
                lambda rsp: _UpdateResult(evts, rsp),
            )

        @Behaviors.receive_message
        def idle(msg: _Msg) -> Behavior[_Msg]:
            """133-139."""
            match msg:
                case RememberEntitiesShardStore.GetEntities():
                    # not supported, but we may get several if the shard timed out and retried
                    log.debug(
                        "Remember entities shard store got another get entities request after "
                        "responding to one, not expected/supported, ignoring"
                    )
                    return Behaviors.same
                case RememberEntitiesShardStore.Update():
                    return on_update(msg)
                case _:
                    return Behaviors.unhandled

        def waiting_for_all_entity_ids(
            got_keys: frozenset[int],
            ids: frozenset[str],
            shard_waiting: ActorRef[RememberEntitiesShardStore.RememberedEntities] | None,
        ) -> Behavior[_Msg]:
            """141-209, without the stash: every message that reaches this state has a case of
            its own."""

            def receive_one(i: int, ids_for_key: frozenset[str]) -> Behavior[_Msg]:
                """142-160."""
                new_got_keys = got_keys | {i}
                new_ids = ids | ids_for_key
                if len(new_got_keys) == NUMBER_OF_KEYS:
                    if shard_waiting is not None:
                        log.debug(
                            "Shard waiting for remembered entities, sending remembered and going "
                            "idle"
                        )
                        shard_waiting.tell(RememberEntitiesShardStore.RememberedEntities(new_ids))
                        return idle
                    # we haven't seen request yet
                    log.debug("Got remembered entities, waiting for shard to request them")
                    return waiting_for_all_entity_ids(new_got_keys, new_ids, None)
                return waiting_for_all_entity_ids(new_got_keys, new_ids, shard_waiting)

            @Behaviors.receive_message
            def receive(msg: _Msg) -> Behavior[_Msg]:
                nonlocal max_read_attempts_left
                match msg:
                    case _GetResult(i, Replicator.GetSuccess(data=data)):
                        return receive_one(i, data.elements)
                    case _GetResult(i, Replicator.NotFound()):
                        return receive_one(i, frozenset())
                    case _GetResult(i, Replicator.GetFailure(failed_key)):
                        max_read_attempts_left -= 1
                        if max_read_attempts_left > 0:
                            log.warning(
                                "Remember entities shard store unable to get an initial state "
                                "within 'waiting-for-state-timeout' for key [%s], retrying",
                                failed_key,
                            )
                            get(i)
                            return Behaviors.same
                        log.error(
                            "Remember entities shard store unable to get an initial state within "
                            "'waiting-for-state-timeout' giving up after retrying: [%g s] using "
                            "[%s] (key [%s])",
                            read_consistency.timeout,
                            read_consistency,
                            failed_key,
                        )
                        return Behaviors.stopped()
                    case _GetResult(_, Replicator.GetDataDeleted()):
                        log.error(
                            "Remember entities shard store unable to get an initial state because "
                            "it was deleted"
                        )
                        return Behaviors.stopped()
                    case RememberEntitiesShardStore.Update():
                        log.warning(
                            "Remember entities shard store got an update before load of initial "
                            "entities completed, dropping update: [%s]",
                            msg,
                        )
                        return Behaviors.same
                    case RememberEntitiesShardStore.GetEntities(reply_to):
                        if len(got_keys) == NUMBER_OF_KEYS:
                            # we already got all and was waiting for a request
                            log.debug(
                                "Remember entities shard store got request from shard, sending "
                                "remembered entities"
                            )
                            reply_to.tell(RememberEntitiesShardStore.RememberedEntities(ids))
                            return idle
                        # we haven't seen all ids yet
                        log.debug(
                            "Remember entities shard store got request from shard, waiting for "
                            "all remembered entities to arrive"
                        )
                        return waiting_for_all_entity_ids(got_keys, ids, reply_to)
                    case _:
                        return Behaviors.unhandled

            return receive

        def on_update(msg: RememberEntitiesShardStore.Update) -> Behavior[_Msg]:
            """211-232: one Update per key, each for the events of the ids on that key."""
            all_evts: list[_Evt] = [_Started(i) for i in msg.started]
            all_evts += [_Stopped(i) for i in msg.stopped]
            by_key: dict[str, set[_Evt]] = {}
            for evt in all_evts:
                by_key.setdefault(key(evt.id).id, set()).add(evt)
            # map from set of evts (for same ddata key) to the retries left for its update
            ddata_updates = {frozenset(evts): max_update_attempts for evts in by_key.values()}
            for evts in ddata_updates:
                update(evts)
            return waiting_for_updates(msg.reply_to, msg, ddata_updates)

        def waiting_for_updates(
            requester: ActorRef[RememberEntitiesShardStore.UpdateDone],
            pending: RememberEntitiesShardStore.Update,
            all_updates: dict[frozenset[_Evt], int],
        ) -> Behavior[_Msg]:
            """234-286: Pekko's map holds each key's Update to resend; here the events are the
            Update, re-issued through the adapter for a fresh ``reply_to``."""

            # updates_left used both to keep track of what work remains and for retrying on
            # timeout up to a limit
            def next_(updates_left: dict[frozenset[_Evt], int]) -> Behavior[_Msg]:
                @Behaviors.receive_message
                def receive(msg: _Msg) -> Behavior[_Msg]:
                    match msg:
                        case _UpdateResult(evts, Replicator.UpdateSuccess()):
                            log.debug(
                                "Remember entities shard store state was successfully updated "
                                "for [%s]",
                                evts,
                            )
                            remaining_after_this = {
                                e: n for e, n in updates_left.items() if e != evts
                            }
                            if not remaining_after_this:
                                requester.tell(
                                    RememberEntitiesShardStore.UpdateDone(
                                        pending.started, pending.stopped
                                    )
                                )
                                return idle
                            return next_(remaining_after_this)
                        case _UpdateResult(evts, Replicator.UpdateTimeout()):
                            retries_left = updates_left[evts]
                            if retries_left > 0:
                                log.debug(
                                    "Remember entities shard store retrying update because of "
                                    "write timeout, tries left [%s]",
                                    retries_left,
                                )
                                update(evts)
                                return next_({**updates_left, evts: retries_left - 1})
                            log.error(
                                "Remember entities shard store unable to update state, within "
                                "'updating-state-timeout'= [%g s], gave up after [%s] retries",
                                write_consistency.timeout,
                                max_update_attempts,
                            )
                            # will trigger shard restart
                            return Behaviors.stopped()
                        case _UpdateResult(_, Replicator.StoreFailure()):
                            log.error(
                                "Remember entities shard store unable to update state, due to "
                                "store failure"
                            )
                            # will trigger shard restart
                            return Behaviors.stopped()
                        case _UpdateResult(_, Replicator.ModifyFailure(_, error, cause)):
                            log.error(
                                "Remember entities shard store unable to update state, due to "
                                "modify failure: %s",
                                error,
                                exc_info=cause,
                            )
                            # will trigger shard restart
                            return Behaviors.stopped()
                        case _UpdateResult(_, Replicator.UpdateDataDeleted()):
                            log.error(
                                "Remember entities shard store unable to update state, due to "
                                "delete"
                            )
                            # will trigger shard restart
                            return Behaviors.stopped()
                        case RememberEntitiesShardStore.Update():
                            log.warning(
                                "Remember entities shard store got a new update before write of "
                                "previous completed, dropping update: [%s]",
                                msg,
                            )
                            return Behaviors.same
                        case _:
                            return Behaviors.unhandled

                return receive

            return next_(all_updates)

        def load_all_entities() -> None:
            """288-293."""
            for i in range(NUMBER_OF_KEYS):
                get(i)

        load_all_entities()
        return waiting_for_all_entity_ids(frozenset(), frozenset(), None)

    return cast(Behavior[RememberEntitiesShardStore.Command], setup)


# --- DDataRememberEntitiesCoordinatorStore -------------------------------------------------------


@dataclass(frozen=True)
class _AllShardsResult:
    """The Get's response."""

    response: Replicator.GetResponse[GSet[str]]


@dataclass(frozen=True)
class _AddShardResult:
    """An Update's response with its request context, Pekko's ``Some((sender(), shardId))``."""

    reply_to: ActorRef[
        RememberEntitiesCoordinatorStore.UpdateDone | RememberEntitiesCoordinatorStore.UpdateFailed
    ]
    shard_id: str
    response: Replicator.UpdateResponse[GSet[str]]


type _CoordMsg = RememberEntitiesCoordinatorStore.Command | _AllShardsResult | _AddShardResult


def ddata_coordinator_store(
    type_name: str,
    replicator: ActorRef[Replicator.Command],
    settings: ClusterShardingSettings,
    majority_min_cap: int,
) -> Behavior[RememberEntitiesCoordinatorStore.Command]:
    """DDataRememberEntitiesCoordinatorStore (DDataRememberEntitiesCoordinatorStore.scala 45-145):
    the ids of all started shards in one GSet, loaded when the store starts and handed once to
    the latest coordinator that asked. ``receive`` (77-129) and ``on_got_all_shards`` (131-143)."""

    @Behaviors.setup
    def setup(ctx: ActorContext[_CoordMsg]) -> Behavior[_CoordMsg]:
        log = ctx.log
        adapter = ReplicatorMessageAdapter[_CoordMsg, GSet[str]](
            ctx,
            replicator,
            max(settings.waiting_for_state_timeout, settings.updating_state_timeout) + 1,
        )
        read_consistency = _read_consistency(
            settings, settings.waiting_for_state_timeout, majority_min_cap
        )
        write_consistency = _write_consistency(
            settings, settings.updating_state_timeout, majority_min_cap
        )
        all_shards_key = GSetKey[str](f"shard-{type_name}-all")
        retry_get_counter = 0
        all_shards: frozenset[str] | None = None
        coordinator_waiting_for_shards: (
            ActorRef[RememberEntitiesCoordinatorStore.RememberedShards] | None
        ) = None

        def get_all_shards() -> None:
            """72-75."""
            adapter.ask_get(
                lambda reply_to: Replicator.Get(all_shards_key, read_consistency, reply_to),
                _AllShardsResult,
            )

        def on_got_all_shards(shard_ids: frozenset[str]) -> None:
            nonlocal retry_get_counter, all_shards, coordinator_waiting_for_shards
            retry_get_counter = 0
            if coordinator_waiting_for_shards is not None:
                coordinator_waiting_for_shards.tell(
                    RememberEntitiesCoordinatorStore.RememberedShards(shard_ids)
                )
                coordinator_waiting_for_shards = None
                # clear the shards out now that we have sent them to coordinator, to save some
                # memory
                all_shards = None
            else:
                # wait for coordinator to ask
                all_shards = shard_ids

        def add_shard(
            reply_to: ActorRef[
                RememberEntitiesCoordinatorStore.UpdateDone
                | RememberEntitiesCoordinatorStore.UpdateFailed
            ],
            shard_id: str,
        ) -> None:
            adapter.ask_update(
                lambda r: Replicator.Update(
                    all_shards_key,
                    cast(GSet[str], GSet.empty),
                    write_consistency,
                    r,
                    lambda existing: existing.add(shard_id),
                ),
                lambda rsp: _AddShardResult(reply_to, shard_id, rsp),
            )

        @Behaviors.receive_message
        def receive(msg: _CoordMsg) -> Behavior[_CoordMsg]:
            nonlocal retry_get_counter, all_shards, coordinator_waiting_for_shards
            match msg:
                case RememberEntitiesCoordinatorStore.GetShards(reply_to):
                    # reply when we get them, since there is only ever one coordinator
                    # communicating with us and it may retry we can just keep the latest sender
                    coordinator_waiting_for_shards = reply_to
                    if all_shards is not None:
                        on_got_all_shards(all_shards)
                    return Behaviors.same
                case _AllShardsResult(Replicator.GetSuccess(data=data)):
                    on_got_all_shards(data.elements)
                    return Behaviors.same
                case _AllShardsResult(Replicator.NotFound()):
                    on_got_all_shards(frozenset())
                    return Behaviors.same
                case _AllShardsResult(Replicator.GetFailure()):
                    retry_get_counter += 1
                    template = (
                        "Remember entities coordinator store unable to get initial shards within "
                        "'waiting-for-state-timeout': %d millis (retrying)"
                    )
                    millis = int(read_consistency.timeout * 1000)
                    if retry_get_counter < 5:
                        log.warning(template, millis)
                    else:
                        log.error(template, millis)
                    # repeat until GetSuccess
                    get_all_shards()
                    return Behaviors.same
                case RememberEntitiesCoordinatorStore.AddShard(shard_id, reply_to):
                    add_shard(reply_to, shard_id)
                    return Behaviors.same
                case _AddShardResult(reply_to, shard_id, Replicator.UpdateSuccess()):
                    log.debug(
                        "Remember entities coordinator store shards successfully updated with %s",
                        shard_id,
                    )
                    reply_to.tell(RememberEntitiesCoordinatorStore.UpdateDone(shard_id))
                    return Behaviors.same
                case _AddShardResult(reply_to, shard_id, Replicator.UpdateTimeout()):
                    log.error(
                        "Remember entities coordinator store unable to update shards state "
                        "within 'updating-state-timeout': %d millis (retrying), adding shard=%s",
                        int(write_consistency.timeout * 1000),
                        shard_id,
                    )
                    reply_to.tell(RememberEntitiesCoordinatorStore.UpdateFailed(shard_id))
                    return Behaviors.same
                case _AddShardResult(
                    reply_to, shard_id, Replicator.ModifyFailure(failed_key, error, cause)
                ):
                    log.error(
                        "Remember entities coordinator store was unable to add shard [%s] "
                        "(key [%s], failed with error: %s)",
                        shard_id,
                        failed_key,
                        error,
                        exc_info=cause,
                    )
                    reply_to.tell(RememberEntitiesCoordinatorStore.UpdateFailed(shard_id))
                    return Behaviors.same
                case _:
                    # GetDataDeleted, StoreFailure and UpdateDataDeleted have no case in Pekko
                    return Behaviors.unhandled

        # eager load of remembered shard ids
        get_all_shards()
        return receive

    return cast(Behavior[RememberEntitiesCoordinatorStore.Command], setup)


# --- DDataRememberEntitiesProvider ---------------------------------------------------------------


@dataclass(frozen=True)
class DDataRememberEntitiesProvider:
    """DDataRememberEntitiesProvider (DDataRememberEntitiesProvider.scala 26-39): the stores of one
    entity type on one Replicator, without Pekko's ``RememberEntitiesProvider`` trait, as the
    event-sourced and custom providers are not ported."""

    type_name: str
    settings: ClusterShardingSettings
    majority_min_cap: int
    replicator: ActorRef[Replicator.Command]

    def coordinator_store_behavior(self) -> Behavior[RememberEntitiesCoordinatorStore.Command]:
        return ddata_coordinator_store(
            self.type_name, self.replicator, self.settings, self.majority_min_cap
        )

    def shard_store_behavior(self, shard_id: str) -> Behavior[RememberEntitiesShardStore.Command]:
        return ddata_shard_store(
            self.type_name, shard_id, self.replicator, self.settings, self.majority_min_cap
        )
