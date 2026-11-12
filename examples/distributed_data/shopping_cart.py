"""Pekko's ShoppingCart (pekko-sample-distributed-data-scala), a cart replicated per user.

Differences from the Scala:

- ``Cart.items`` is a ``frozenset``, where Scala has an immutable ``Set``.
- The Scala's ``receiveGetCart.orElse(receiveAddItem).orElse(receiveRemoveItem)
  .orElse(receiveOther)`` is one ``match`` with the four groups' cases in that order, marked by
  comments. A message none of them matches is unhandled.
- An unexpected update failure raises ``RuntimeError``, where the Scala throws
  ``IllegalStateException``.
- The Scala matches ``GetSuccess(DataKey, _)`` and so on; there is one key per actor here, so
  the patterns match the response class alone.
- ``LineItem`` is ``@remotable``, which is the Scala spec's jackson-cbor serialization binding
  for ``LineItem``: the replicas carry it between nodes.
- ``cart_key`` is public, so the tests of other samples can read the cart.
"""

from dataclasses import dataclass, replace
from collections.abc import Callable
from typing import Any

from ohra import ActorContext, ActorRef, Behavior, Behaviors, remotable
from ohra.cluster.ddata import (
    DistributedData,
    LWWMap,
    LWWMapKey,
    Replicator,
    ReplicatorMessageAdapter,
)


@remotable
@dataclass(frozen=True)
class LineItem:
    product_id: str
    title: str
    quantity: int


@dataclass(frozen=True)
class Cart:
    items: frozenset[LineItem]


@dataclass(frozen=True)
class GetCart:
    reply_to: ActorRef[Cart]


@dataclass(frozen=True)
class AddItem:
    item: LineItem


@dataclass(frozen=True)
class RemoveItem:
    product_id: str


@dataclass(frozen=True)
class _InternalGetResponse:
    reply_to: ActorRef[Cart]
    rsp: Replicator.GetResponse[LWWMap[str, LineItem]]


@dataclass(frozen=True)
class _InternalUpdateResponse:
    rsp: Replicator.UpdateResponse[Any]


@dataclass(frozen=True)
class _InternalRemoveItem:
    product_id: str
    get_response: Replicator.GetResponse[LWWMap[str, LineItem]]


type Command = (
    GetCart
    | AddItem
    | RemoveItem
    | _InternalGetResponse
    | _InternalUpdateResponse
    | _InternalRemoveItem
)

_READ_MAJORITY = Replicator.ReadMajority(timeout=3.0)
_WRITE_MAJORITY = Replicator.WriteMajority(timeout=3.0)


def cart_key(user_id: str) -> LWWMapKey[str, LineItem]:
    return LWWMapKey[str, LineItem](f"cart-{user_id}")


def shopping_cart(user_id: str) -> Behavior[Command]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Command]) -> Behavior[Command]:
        def with_replicator(
            replicator: ReplicatorMessageAdapter[Command, LWWMap[str, LineItem]],
        ) -> Behavior[Command]:
            node = DistributedData(ctx.system).self_unique_address
            key = cart_key(user_id)

            def get(
                consistency: Replicator.ReadConsistency,
                wrap: Callable[[Replicator.GetResponse[LWWMap[str, LineItem]]], Command],
            ) -> None:
                replicator.ask_get(
                    lambda reply_to: Replicator.Get(key, consistency, reply_to), wrap
                )

            def update(modify: Callable[[LWWMap[str, LineItem]], LWWMap[str, LineItem]]) -> None:
                replicator.ask_update(
                    lambda reply_to: Replicator.Update(
                        key, LWWMap[str, LineItem](), _WRITE_MAJORITY, reply_to, modify
                    ),
                    _InternalUpdateResponse,
                )

            def update_cart(data: LWWMap[str, LineItem], item: LineItem) -> LWWMap[str, LineItem]:
                existing = data.get(item.product_id)
                if existing is not None:
                    item = replace(item, quantity=existing.quantity + item.quantity)
                return data.put(node, item.product_id, item)

            def remove_item(product_id: str) -> None:
                update(lambda data: data.remove(node, product_id))

            @Behaviors.receive_message
            def behavior(msg: Command) -> Behavior[Command]:
                match msg:
                    # receiveGetCart
                    case GetCart(reply_to):
                        get(_READ_MAJORITY, lambda rsp: _InternalGetResponse(reply_to, rsp))
                        return Behaviors.same
                    case _InternalGetResponse(reply_to, Replicator.GetSuccess() as g):
                        reply_to.tell(Cart(frozenset(g.get(key).entries.values())))
                        return Behaviors.same
                    case _InternalGetResponse(reply_to, Replicator.NotFound()):
                        reply_to.tell(Cart(frozenset()))
                        return Behaviors.same
                    case _InternalGetResponse(reply_to, Replicator.GetFailure()):
                        # ReadMajority failure, try again with local read
                        get(
                            Replicator.ReadLocal(),
                            lambda rsp: _InternalGetResponse(reply_to, rsp),
                        )
                        return Behaviors.same
                    # receiveAddItem
                    case AddItem(item):
                        update(lambda data: update_cart(data, item))
                        return Behaviors.same
                    # receiveRemoveItem
                    case RemoveItem(product_id):
                        # Fetch the latest from a majority first, since an ORMap remove
                        # must have seen the item to be able to remove it.
                        get(_READ_MAJORITY, lambda rsp: _InternalRemoveItem(product_id, rsp))
                        return Behaviors.same
                    case _InternalRemoveItem(product_id, Replicator.GetSuccess()):
                        remove_item(product_id)
                        return Behaviors.same
                    case _InternalRemoveItem(product_id, Replicator.GetFailure()):
                        # ReadMajority failed, fall back to the best effort local value
                        remove_item(product_id)
                        return Behaviors.same
                    case _InternalRemoveItem(_, Replicator.NotFound()):
                        # nothing to remove
                        return Behaviors.same
                    # receiveOther
                    case _InternalUpdateResponse(Replicator.UpdateSuccess()):
                        return Behaviors.same
                    case _InternalUpdateResponse(Replicator.UpdateTimeout()):
                        # will eventually be replicated
                        return Behaviors.same
                    case _InternalUpdateResponse(rsp):
                        raise RuntimeError(f"Unexpected failure: {rsp}")
                    case _:
                        return Behaviors.unhandled

            return behavior

        return DistributedData.with_replicator_message_adapter(with_replicator)

    return setup
