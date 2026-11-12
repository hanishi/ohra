"""Pekko's ReplicatedCache (pekko-sample-distributed-data-scala), entries over 100 LWWMaps.

Differences from the Scala:

- ``data_key`` picks the map with ``zlib.crc32`` of the entry key, where the Scala uses
  ``math.abs(entryKey.hashCode % 100)``. Python's ``str`` hash is randomized per process, so
  nodes would disagree on the key; ohra's shard ids use ``crc32`` for the same reason.
- ``Cached.value`` is ``str | None``, where Scala has ``Option[String]``.
- The Scala's two catch-all cases for ``InternalGetResponse`` and ``InternalUpdateResponse``
  are one ``case`` with an or-pattern here.
- ``data_key`` is public, so the soak test can read the cache.
"""

import zlib
from dataclasses import dataclass
from typing import Any

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.cluster.ddata import (
    DistributedData,
    LWWMap,
    LWWMapKey,
    Replicator,
    ReplicatorMessageAdapter,
)


@dataclass(frozen=True)
class PutInCache:
    key: str
    value: str


@dataclass(frozen=True)
class Cached:
    key: str
    value: str | None


@dataclass(frozen=True)
class GetFromCache:
    key: str
    reply_to: ActorRef[Cached]


@dataclass(frozen=True)
class Evict:
    key: str


@dataclass(frozen=True)
class _InternalGetResponse:
    key: str
    reply_to: ActorRef[Cached]
    rsp: Replicator.GetResponse[LWWMap[str, str]]


@dataclass(frozen=True)
class _InternalUpdateResponse:
    rsp: Replicator.UpdateResponse[Any]


type Command = PutInCache | GetFromCache | Evict | _InternalGetResponse | _InternalUpdateResponse


def data_key(entry_key: str) -> LWWMapKey[str, str]:
    return LWWMapKey[str, str](f"cache-{zlib.crc32(entry_key.encode()) % 100}")


def replicated_cache() -> Behavior[Command]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Command]) -> Behavior[Command]:
        def with_replicator(
            replicator: ReplicatorMessageAdapter[Command, LWWMap[str, str]],
        ) -> Behavior[Command]:
            node = DistributedData(ctx.system).self_unique_address

            @Behaviors.receive_message
            def behavior(msg: Command) -> Behavior[Command]:
                match msg:
                    case PutInCache(key, value):
                        replicator.ask_update(
                            lambda reply_to: Replicator.Update(
                                data_key(key),
                                LWWMap[str, str](),
                                Replicator.WriteLocal(),
                                reply_to,
                                lambda data: data.put(node, key, value),
                            ),
                            _InternalUpdateResponse,
                        )
                        return Behaviors.same
                    case Evict(key):
                        replicator.ask_update(
                            lambda reply_to: Replicator.Update(
                                data_key(key),
                                LWWMap[str, str](),
                                Replicator.WriteLocal(),
                                reply_to,
                                lambda data: data.remove(node, key),
                            ),
                            _InternalUpdateResponse,
                        )
                        return Behaviors.same
                    case GetFromCache(key, reply_to):
                        replicator.ask_get(
                            lambda ask_reply_to: Replicator.Get(
                                data_key(key), Replicator.ReadLocal(), ask_reply_to
                            ),
                            lambda rsp: _InternalGetResponse(key, reply_to, rsp),
                        )
                        return Behaviors.same
                    case _InternalGetResponse(key, reply_to, Replicator.GetSuccess() as g):
                        reply_to.tell(Cached(key, g.get(data_key(key)).get(key)))
                        return Behaviors.same
                    case _InternalGetResponse(key, reply_to, Replicator.NotFound()):
                        reply_to.tell(Cached(key, None))
                        return Behaviors.same
                    case _InternalGetResponse() | _InternalUpdateResponse():
                        return Behaviors.same  # ok
                    case _:
                        return Behaviors.unhandled

            return behavior

        return DistributedData.with_replicator_message_adapter(with_replicator)

    return setup
