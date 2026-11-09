"""Pekko's ReplicatorDocSpec counter: an actor that keeps a GCounter in the Replicator."""

from dataclasses import dataclass

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    Replicator,
    ReplicatorMessageAdapter,
)


@dataclass(frozen=True)
class Increment:
    pass


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class GetCachedValue:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Unsubscribe:
    pass


@dataclass(frozen=True)
class InternalUpdateResponse:
    rsp: Replicator.UpdateResponse[GCounter]


@dataclass(frozen=True)
class InternalGetResponse:
    rsp: Replicator.GetResponse[GCounter]
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class InternalSubscribeResponse:
    chg: Replicator.SubscribeResponse[GCounter]


type Command = (
    Increment
    | GetValue
    | GetCachedValue
    | Unsubscribe
    | InternalUpdateResponse
    | InternalGetResponse
    | InternalSubscribeResponse
)


def counter(key: GCounterKey) -> Behavior[Command]:
    def setup(ctx: ActorContext[Command]) -> Behavior[Command]:
        node = DistributedData(ctx.system).self_unique_address

        def with_adapter(
            adapter: ReplicatorMessageAdapter[Command, GCounter],
        ) -> Behavior[Command]:
            adapter.subscribe(key, InternalSubscribeResponse)

            def updated(cached_value: int) -> Behavior[Command]:
                def receive(msg: Command) -> Behavior[Command]:
                    match msg:
                        case Increment():
                            adapter.ask_update(
                                lambda reply_to: Replicator.Update(
                                    key,
                                    GCounter(),
                                    Replicator.WriteLocal(),
                                    reply_to,
                                    lambda c: c.increment(node, 1),
                                ),
                                InternalUpdateResponse,
                            )
                            return Behaviors.same
                        case GetValue(reply_to):
                            adapter.ask_get(
                                lambda ask_reply_to: Replicator.Get(
                                    key, Replicator.ReadLocal(), ask_reply_to
                                ),
                                lambda rsp: InternalGetResponse(rsp, reply_to),
                            )
                            return Behaviors.same
                        case GetCachedValue(reply_to):
                            reply_to.tell(cached_value)
                            return Behaviors.same
                        case Unsubscribe():
                            adapter.unsubscribe(key)
                            return Behaviors.same
                        case InternalUpdateResponse():
                            return Behaviors.same  # ok
                        case InternalGetResponse(Replicator.GetSuccess() as rsp, reply_to):
                            reply_to.tell(rsp.get(key).value)
                            return Behaviors.same
                        case InternalGetResponse():
                            return Behaviors.unhandled  # not dealing with failures
                        case InternalSubscribeResponse(Replicator.Changed() as chg):
                            return updated(chg.get(key).value)
                        case InternalSubscribeResponse():
                            return Behaviors.unhandled  # no deletes

                return Behaviors.receive_message(receive)

            return updated(cached_value=0)

        return DistributedData.with_replicator_message_adapter(with_adapter)

    return Behaviors.setup(setup)
