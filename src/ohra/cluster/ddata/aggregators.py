"""Pekko's ``ReadWriteAggregator``, ``WriteAggregator`` and ``ReadAggregator``, from
``Replicator.scala``: the children a Replicator starts for a Get or an Update with a consistency
level other than local.

Delta writes and the durable store's replies are left out, until sub-projects 6 and 5.
Typed actors have no ``sender()``. Each request goes out with a message adapter of its own as
``reply_to``, and the adapter names the node it was sent to, where Pekko reads the sender's
address. Pekko's ``replica(address)`` is the ``replica`` parameter.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence, Set
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Never, cast

from ...behavior import Behavior, Behaviors
from .envelope import (
    DataEnvelope,
    Read,
    ReadRepair,
    ReadRepairAck,
    ReadResult,
    Write,
    WriteAck,
    WriteNack,
    send_to_replica,
)
from .replicator import Replicator

if TYPE_CHECKING:
    from ...context import ActorContext
    from ...ref import ActorRef
    from ...remote import UniqueAddress
    from .key import Key

MAX_SECONDARY_NODES = 10


def calculate_majority(min_cap: int, num_nodes: int, additional: int) -> int:
    majority = num_nodes // 2 + 1
    return min(num_nodes, max(majority + additional, min_cap))


@dataclass(frozen=True)
class _SendToSecondary:
    pass


@dataclass(frozen=True)
class _ReceiveTimeout:
    pass


@dataclass(frozen=True)
class _WriteReply:
    """A ``WriteAck`` or ``WriteNack`` from ``node``, through the adapter sent to it."""

    node: UniqueAddress
    msg: WriteAck | WriteNack


@dataclass(frozen=True)
class _ReadReply:
    """A ``ReadResult`` from ``node``, through the adapter sent to it."""

    node: UniqueAddress
    msg: ReadResult


type _WriteMsg = _WriteReply | _SendToSecondary | _ReceiveTimeout
type _ReadMsg = _ReadReply | ReadRepairAck | _SendToSecondary | _ReceiveTimeout


def _start_timers(ctx: ActorContext[Any], timeout: float) -> None:
    """ReadWriteAggregator's sendToSecondarySchedule and timeoutSchedule. The actor's timers are
    cancelled when it stops, as Pekko's postStop cancels them."""
    ctx.timers.start_single_timer("send-to-secondary", _SendToSecondary(), timeout / 5)
    ctx.timers.start_single_timer("timeout", _ReceiveTimeout(), timeout)


def _primary_and_secondary_nodes(
    nodes: Sequence[UniqueAddress],
    unreachable: Set[UniqueAddress],
    done_when_remaining_size: int,
    shuffle: bool,
) -> tuple[list[UniqueAddress], list[UniqueAddress]]:
    primary_size = len(nodes) - done_when_remaining_size
    if primary_size >= len(nodes):
        return list(nodes), []
    # Prefer reachable nodes over unreachable ones. Without shuffle (prefer_oldest) the order of
    # `nodes` is kept. Pekko's unreachable Set has no order; here it is sorted.
    reachable_nodes = [n for n in nodes if n not in unreachable]
    unreachable_nodes = sorted(unreachable)
    if shuffle:
        random.shuffle(reachable_nodes)
        random.shuffle(unreachable_nodes)
    ordered = reachable_nodes + unreachable_nodes
    split = max(primary_size, 0)  # Scala's splitAt takes a negative index as 0
    return ordered[:split], ordered[split:][:MAX_SECONDARY_NODES]


def write_aggregator(
    key: Key[Any],
    envelope: DataEnvelope,
    consistency: Replicator.WriteConsistency,
    reply_to: ActorRef[Any],
    nodes: Sequence[UniqueAddress],
    unreachable: Set[UniqueAddress],
    replica: Callable[[UniqueAddress], ActorRef[Any]],
    self_unique_address: UniqueAddress,
    *,
    shuffle: bool,
) -> Behavior[Never]:
    """Pekko's WriteAggregator. It replies once to ``reply_to``, which is an Update's or a
    Delete's, and stops. It takes no messages from outside."""

    @Behaviors.setup
    def setup(ctx: ActorContext[_WriteMsg]) -> Behavior[_WriteMsg]:
        match consistency:
            case Replicator.WriteTo(n, timeout):
                done_when_remaining_size = len(nodes) - (n - 1)
            case Replicator.WriteAll(timeout):
                done_when_remaining_size = 0
            case Replicator.WriteMajority(timeout, min_cap):
                # +1 because the local node is not included in `nodes`
                num_nodes = len(nodes) + 1
                w = calculate_majority(min_cap, num_nodes, 0)
                ctx.log.debug("WriteMajority [%s] [%s] of [%s].", key, w, num_nodes)
                done_when_remaining_size = num_nodes - w
            case Replicator.WriteMajorityPlus(timeout, additional, min_cap):
                num_nodes = len(nodes) + 1
                w = calculate_majority(min_cap, num_nodes, additional)
                ctx.log.debug("WriteMajorityPlus [%s] [%s] of [%s].", key, w, num_nodes)
                done_when_remaining_size = num_nodes - w
            case Replicator.WriteLocal():
                raise ValueError("WriteLocal not supported by WriteAggregator")
        _start_timers(ctx, timeout)

        remaining = set(nodes)
        got_write_nack_from: set[UniqueAddress] = set()
        primary_nodes, secondary_nodes = _primary_and_secondary_nodes(
            nodes, unreachable, done_when_remaining_size, shuffle
        )

        def write_msg(node: UniqueAddress) -> Write:
            def adapt(msg: WriteAck | WriteNack) -> _WriteMsg:
                return _WriteReply(node, msg)

            return Write(key.id, envelope, self_unique_address, ctx.message_adapter(adapt))

        def is_done() -> bool:
            return (
                len(remaining) <= done_when_remaining_size
                or not (remaining - got_write_nack_from)
                or not_enough_nodes()
            )

        def not_enough_nodes() -> bool:
            return done_when_remaining_size < 0 or len(nodes) < done_when_remaining_size

        def reply() -> Behavior[_WriteMsg]:
            is_success = len(remaining) <= done_when_remaining_size and not not_enough_nodes()
            # Without durable storage Pekko replies UpdateTimeout, or ReplicationDeleteFailure to
            # a Delete on a timeout or too few nodes. A Delete that fails by WriteNacks gets
            # UpdateTimeout there, which is not a DeleteResponse: here it gets DeleteFailure.
            # Pekko's isTimeout only tells those apart, and picks StoreFailure, so it is left out.
            reply_msg: Any
            if is_success and envelope.is_deleted:
                reply_msg = Replicator.DeleteSuccess(key)
            elif is_success:
                reply_msg = Replicator.UpdateSuccess(key)
            elif envelope.is_deleted:
                reply_msg = Replicator.DeleteFailure(key)
            else:
                reply_msg = Replicator.UpdateTimeout(key)
            reply_to.tell(reply_msg)
            return Behaviors.stopped()

        # Pekko's preStart
        for node in primary_nodes:
            send_to_replica(ctx.log, replica(node), write_msg(node), [key.id])
        if is_done():
            return reply()

        @Behaviors.receive_message
        def receive(msg: _WriteMsg) -> Behavior[_WriteMsg]:
            match msg:
                case _WriteReply(node, WriteAck()):
                    remaining.discard(node)
                    if is_done():
                        return reply()
                case _WriteReply(node, _):  # WriteNack
                    got_write_nack_from.add(node)
                    if is_done():
                        return reply()
                case _SendToSecondary():
                    for node in secondary_nodes:
                        send_to_replica(ctx.log, replica(node), write_msg(node), [key.id])
                case _ReceiveTimeout():
                    return reply()
            return Behaviors.same

        return receive

    # Behavior has no narrow: the replies and the ticks are private
    return cast(Behavior[Never], setup)


def read_aggregator(
    key: Key[Any],
    consistency: Replicator.ReadConsistency,
    reply_to: ActorRef[Replicator.GetResponse[Any]],
    nodes: Sequence[UniqueAddress],
    unreachable: Set[UniqueAddress],
    local_value: DataEnvelope | None,
    replica: Callable[[UniqueAddress], ActorRef[Any]],
    replicator: ActorRef[ReadRepair],
    self_unique_address: UniqueAddress,
    *,
    shuffle: bool,
    now: Callable[[], float],
) -> Behavior[Never]:
    """Pekko's ReadAggregator. It replies once to ``reply_to`` and stops. Before a GetSuccess it
    sends the merged value to ``replicator`` as a ReadRepair, and waits for the ack. It takes no
    messages from outside."""

    @Behaviors.setup
    def setup(ctx: ActorContext[_ReadMsg]) -> Behavior[_ReadMsg]:
        match consistency:
            case Replicator.ReadFrom(n, timeout):
                done_when_remaining_size = len(nodes) - (n - 1)
            case Replicator.ReadAll(timeout):
                done_when_remaining_size = 0
            case Replicator.ReadMajority(timeout, min_cap):
                # +1 because the local node is not included in `nodes`
                num_nodes = len(nodes) + 1
                r = calculate_majority(min_cap, num_nodes, 0)
                ctx.log.debug("ReadMajority [%s] [%s] of [%s].", key, r, num_nodes)
                done_when_remaining_size = num_nodes - r
            case Replicator.ReadMajorityPlus(timeout, additional, min_cap):
                num_nodes = len(nodes) + 1
                r = calculate_majority(min_cap, num_nodes, additional)
                ctx.log.debug("ReadMajorityPlus [%s] [%s] of [%s].", key, r, num_nodes)
                done_when_remaining_size = num_nodes - r
            case Replicator.ReadLocal():
                raise ValueError("ReadLocal not supported by ReadAggregator")
        _start_timers(ctx, timeout)

        result = local_value
        remaining = set(nodes)
        primary_nodes, secondary_nodes = _primary_and_secondary_nodes(
            nodes, unreachable, done_when_remaining_size, shuffle
        )

        def read_msg(node: UniqueAddress) -> Read:
            def adapt(msg: ReadResult) -> _ReadMsg:
                return _ReadReply(node, msg)

            return Read(key.id, self_unique_address, ctx.message_adapter(adapt))

        def reply(ok: bool) -> Behavior[_ReadMsg]:
            if ok and result is not None:
                replicator.tell(ReadRepair(key.id, result, ctx.self))
                # read-repair happens before GetSuccess
                return wait_read_repair_ack(result)
            reply_to.tell(Replicator.NotFound(key) if ok else Replicator.GetFailure(key))
            return Behaviors.stopped()

        def wait_read_repair_ack(envelope: DataEnvelope) -> Behavior[_ReadMsg]:
            @Behaviors.receive_message
            def receive(msg: _ReadMsg) -> Behavior[_ReadMsg]:
                match msg:
                    case ReadRepairAck():
                        reply_to.tell(
                            Replicator.GetDataDeleted(key)
                            if envelope.is_deleted
                            else Replicator.GetSuccess(key, envelope.data)
                        )
                        return Behaviors.stopped()
                    case _ReadReply(node):
                        remaining.discard(node)  # collect late replies
                    case _SendToSecondary() | _ReceiveTimeout():
                        pass
                return Behaviors.same

            return receive

        # Pekko's preStart
        for node in primary_nodes:
            send_to_replica(ctx.log, replica(node), read_msg(node), [key.id])
        if len(remaining) == done_when_remaining_size:
            return reply(ok=True)
        elif done_when_remaining_size < 0 or len(remaining) < done_when_remaining_size:
            return reply(ok=False)

        @Behaviors.receive_message
        def receive(msg: _ReadMsg) -> Behavior[_ReadMsg]:
            nonlocal result
            match msg:
                case _ReadReply(node, ReadResult(envelope)):
                    if result is not None and envelope is not None:
                        try:
                            result = result.merge(envelope, now())
                        except TypeError as e:
                            # a replica holds another type: no reply could merge, so fail now
                            # rather than restart and leave the caller to its timeout
                            ctx.log.warning("Couldn't merge [%s], due to: %s", key.id, e)
                            return reply(ok=False)
                    elif envelope is not None:
                        result = envelope
                    remaining.discard(node)
                    if len(remaining) == done_when_remaining_size:
                        return reply(ok=True)
                case _SendToSecondary():
                    for node in secondary_nodes:
                        send_to_replica(ctx.log, replica(node), read_msg(node), [key.id])
                case _ReceiveTimeout():
                    return reply(ok=False)
                case ReadRepairAck():
                    pass  # only sent after a ReadRepair
            return Behaviors.same

        return receive

    # Behavior has no narrow: the replies and the ticks are private
    return cast(Behavior[Never], setup)
