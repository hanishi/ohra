"""Pekko's VotingService (pekko-sample-distributed-data-scala), the replicated voting demo.

Differences from the Scala:

- ``Votes.result`` holds ``int`` counts, where Scala has ``BigInt``.
- The Scala's ``Behaviors.receiveMessagePartial(open.orElse(getVotes(open = true)))`` is one
  handler that tries the open cases first and then the get-votes cases.
- ``Open`` and ``Close`` are frozen dataclasses, not case objects: send ``Open()``.
- The three keys are module constants, so the tests of other samples can read them.
"""

from dataclasses import dataclass
from typing import Any

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.cluster.ddata import (
    DistributedData,
    Flag,
    FlagKey,
    PNCounterMap,
    PNCounterMapKey,
    Replicator,
    ReplicatorMessageAdapter,
)

OPENED_KEY = FlagKey("contestOpened")
CLOSED_KEY = FlagKey("contestClosed")
COUNTERS_KEY = PNCounterMapKey[str]("contestCounters")


@dataclass(frozen=True)
class Open:
    pass


@dataclass(frozen=True)
class Close:
    pass


@dataclass(frozen=True)
class Vote:
    participant: str


@dataclass(frozen=True)
class Votes:
    result: dict[str, int]
    open: bool


@dataclass(frozen=True)
class GetVotes:
    reply_to: ActorRef[Votes]


@dataclass(frozen=True)
class _InternalSubscribeResponse:
    chg: Replicator.SubscribeResponse[Flag]


@dataclass(frozen=True)
class _InternalUpdateResponse:
    rsp: Replicator.UpdateResponse[Any]


@dataclass(frozen=True)
class _InternalGetResponse:
    reply_to: ActorRef[Votes]
    rsp: Replicator.GetResponse[PNCounterMap[str]]


type Command = (
    Open
    | Close
    | Vote
    | GetVotes
    | _InternalSubscribeResponse
    | _InternalUpdateResponse
    | _InternalGetResponse
)


def voting_service() -> Behavior[Command]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Command]) -> Behavior[Command]:
        def with_flag(flag: ReplicatorMessageAdapter[Command, Flag]) -> Behavior[Command]:
            def with_counters(
                counters: ReplicatorMessageAdapter[Command, PNCounterMap[str]],
            ) -> Behavior[Command]:
                node = DistributedData(ctx.system).self_unique_address
                flag.subscribe(OPENED_KEY, _InternalSubscribeResponse)

                def switch_on(key: FlagKey) -> None:
                    flag.ask_update(
                        lambda reply_to: Replicator.Update(
                            key,
                            Flag(),
                            Replicator.WriteAll(timeout=5.0),
                            reply_to,
                            lambda f: f.switch_on(),
                        ),
                        _InternalUpdateResponse,
                    )

                @Behaviors.receive_message
                def start(msg: Command) -> Behavior[Command]:
                    match msg:
                        case Open():
                            switch_on(OPENED_KEY)
                            return become_open()
                        case _InternalSubscribeResponse(Replicator.Changed() as c) if (
                            c.key == OPENED_KEY and c.get(OPENED_KEY).enabled
                        ):
                            return become_open()
                        case GetVotes(reply_to):
                            reply_to.tell(Votes({}, open=False))
                            return Behaviors.same
                        case _:
                            return Behaviors.unhandled

                def become_open() -> Behavior[Command]:
                    flag.unsubscribe(OPENED_KEY)
                    flag.subscribe(CLOSED_KEY, _InternalSubscribeResponse)
                    return open_state

                def get_votes(msg: Command, open: bool) -> Behavior[Command]:
                    match msg:
                        case GetVotes(reply_to):
                            counters.ask_get(
                                lambda ask_reply_to: Replicator.Get(
                                    COUNTERS_KEY, Replicator.ReadAll(timeout=3.0), ask_reply_to
                                ),
                                lambda rsp: _InternalGetResponse(reply_to, rsp),
                            )
                            return Behaviors.same
                        case _InternalGetResponse(reply_to, Replicator.GetSuccess() as g):
                            reply_to.tell(Votes(g.get(COUNTERS_KEY).entries, open))
                            return Behaviors.same
                        case _InternalGetResponse(reply_to, Replicator.NotFound()):
                            reply_to.tell(Votes({}, open))
                            return Behaviors.same
                        case _InternalGetResponse(_, Replicator.GetFailure()):
                            return Behaviors.same
                        case _InternalUpdateResponse(Replicator.UpdateSuccess()):
                            return Behaviors.same
                        case _:
                            return Behaviors.unhandled

                @Behaviors.receive_message
                def open_state(msg: Command) -> Behavior[Command]:
                    match msg:
                        case Vote(participant):
                            counters.ask_update(
                                lambda reply_to: Replicator.Update(
                                    COUNTERS_KEY,
                                    PNCounterMap[str](),
                                    Replicator.WriteLocal(),
                                    reply_to,
                                    lambda m: m.increment(node, participant, 1),
                                ),
                                _InternalUpdateResponse,
                            )
                            return Behaviors.same
                        case Close():
                            switch_on(CLOSED_KEY)
                            return closed_state
                        case _InternalSubscribeResponse(Replicator.Changed() as c) if (
                            c.key == CLOSED_KEY and c.get(CLOSED_KEY).enabled
                        ):
                            return closed_state
                        case _InternalSubscribeResponse(Replicator.Changed() as c) if (
                            c.key == OPENED_KEY
                        ):
                            return Behaviors.same
                        case _:
                            return get_votes(msg, open=True)

                @Behaviors.receive_message
                def closed_state(msg: Command) -> Behavior[Command]:
                    return get_votes(msg, open=False)

                return start

            return DistributedData.with_replicator_message_adapter(with_counters)

        return DistributedData.with_replicator_message_adapter(with_flag)

    return setup
