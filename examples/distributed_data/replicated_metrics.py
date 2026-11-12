"""Pekko's ReplicatedMetrics (pekko-sample-distributed-data-scala), memory use of every node.

Differences from the Scala:

- ``memory_usage`` reports this process's peak resident memory (``ru_maxrss``) against the
  machine's physical memory, where Pekko reports the JVM heap in use against its maximum.
  Python has no heap limit to compare with. ``USED_HEAP_KEY`` and ``MAX_HEAP_KEY`` keep
  Pekko's names and ``"usedHeap"`` and ``"maxHeap"`` as ids.
- ``measure`` is a parameter, ``memory_usage`` by default, so a test can pin the numbers.
- The Scala's ``var maxHeap`` and ``var nodesInCluster`` are the parameters of ``running``,
  which returns the behavior for the next state.
- The values are ``int``, where Scala has ``Long``.
- Pekko's two catch-all cases for subscribe and update responses are one ``case`` here.
- The members are tracked from ``MemberUp`` and ``MemberRemoved`` through ``message_adapter``,
  as in the Scala; ohra's ``Subscribe`` replays the current members as ``MemberUp`` first.
"""

import logging
import os
import resource
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ohra import (
    ActorContext,
    Address,
    Behavior,
    Behaviors,
    Cluster,
    EventStream,
    MemberRemoved,
    MemberUp,
    TimerScheduler,
)
from ohra.cluster import Subscribe
from ohra.cluster.ddata import (
    DistributedData,
    LWWMap,
    LWWMapKey,
    Replicator,
    ReplicatorMessageAdapter,
)

log = logging.getLogger(__name__)

USED_HEAP_KEY = LWWMapKey[str, int]("usedHeap")
MAX_HEAP_KEY = LWWMapKey[str, int]("maxHeap")


@dataclass(frozen=True)
class _Tick:
    pass


@dataclass(frozen=True)
class _Cleanup:
    pass


@dataclass(frozen=True)
class _InternalSubscribeResponse:
    rsp: Replicator.SubscribeResponse[LWWMap[str, int]]


@dataclass(frozen=True)
class _InternalClusterMemberUp:
    msg: MemberUp


@dataclass(frozen=True)
class _InternalClusterMemberRemoved:
    msg: MemberRemoved


@dataclass(frozen=True)
class _InternalUpdateResponse:
    rsp: Replicator.UpdateResponse[Any]


type Command = (
    _Tick
    | _Cleanup
    | _InternalSubscribeResponse
    | _InternalClusterMemberUp
    | _InternalClusterMemberRemoved
    | _InternalUpdateResponse
)


@dataclass(frozen=True)
class UsedHeap:
    percent_per_node: dict[str, float]

    def __str__(self) -> str:
        return "\n".join(f"{k} --> {v} %" for k, v in sorted(self.percent_per_node.items()))


def node_key(address: Address) -> str:
    return f"{address.host}:{address.port}"


def memory_usage() -> tuple[int, int]:
    """``(used, max)`` in bytes: peak resident memory of this process, physical memory."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    used = peak if sys.platform == "darwin" else peak * 1024  # bytes on macOS, KiB elsewhere
    return used, os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")


def replicated_metrics(
    measure_interval: float = 1.0,
    cleanup_interval: float = 60.0,
    measure: Callable[[], tuple[int, int]] = memory_usage,
) -> Behavior[Command]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Command]) -> Behavior[Command]:
        def with_replicator(
            replicator: ReplicatorMessageAdapter[Command, LWWMap[str, int]],
        ) -> Behavior[Command]:
            @Behaviors.with_timers
            def with_timers(timers: TimerScheduler[Command]) -> Behavior[Command]:
                self_node = DistributedData(ctx.system).self_unique_address
                cluster = Cluster(ctx.system)
                node = node_key(cluster.self_address)

                timers.start_timer_with_fixed_delay(_Tick(), _Tick(), measure_interval)
                timers.start_timer_with_fixed_delay(_Cleanup(), _Cleanup(), cleanup_interval)

                replicator.subscribe(USED_HEAP_KEY, _InternalSubscribeResponse)
                replicator.subscribe(MAX_HEAP_KEY, _InternalSubscribeResponse)

                member_up_ref = ctx.message_adapter(_InternalClusterMemberUp)
                member_removed_ref = ctx.message_adapter(_InternalClusterMemberRemoved)
                cluster.subscriptions.tell(Subscribe(member_up_ref, MemberUp))
                cluster.subscriptions.tell(Subscribe(member_removed_ref, MemberRemoved))

                def update(
                    key: LWWMapKey[str, int],
                    modify: Callable[[LWWMap[str, int]], LWWMap[str, int]],
                ) -> None:
                    replicator.ask_update(
                        lambda reply_to: Replicator.Update(
                            key, LWWMap[str, int](), Replicator.WriteLocal(), reply_to, modify
                        ),
                        _InternalUpdateResponse,
                    )

                def cleanup_removed(
                    nodes_in_cluster: frozenset[str],
                ) -> Callable[[LWWMap[str, int]], LWWMap[str, int]]:
                    def cleanup(data: LWWMap[str, int]) -> LWWMap[str, int]:
                        for key in data.entries.keys() - nodes_in_cluster:
                            data = data.remove(self_node, key)
                        return data

                    return cleanup

                def running(
                    max_heap: dict[str, int], nodes_in_cluster: frozenset[str]
                ) -> Behavior[Command]:
                    @Behaviors.receive_message
                    def behavior(msg: Command) -> Behavior[Command]:
                        match msg:
                            case _Tick():
                                used, max_ = measure()
                                update(USED_HEAP_KEY, lambda data: data.put(self_node, node, used))
                                update(
                                    MAX_HEAP_KEY,
                                    lambda data: (
                                        data
                                        if data.get(node) == max_
                                        else data.put(self_node, node, max_)
                                    ),
                                )
                                return Behaviors.same
                            case _InternalSubscribeResponse(Replicator.Changed() as c) if (
                                c.key == MAX_HEAP_KEY
                            ):
                                return running(c.get(MAX_HEAP_KEY).entries, nodes_in_cluster)
                            case _InternalSubscribeResponse(Replicator.Changed() as c) if (
                                c.key == USED_HEAP_KEY
                            ):
                                used_heap = UsedHeap(
                                    {
                                        key: value / max_heap[key] * 100.0
                                        for key, value in c.get(USED_HEAP_KEY).entries.items()
                                        if key in max_heap
                                    }
                                )
                                log.debug("Node %s observed:\n%s", node, used_heap)
                                ctx.system.event_stream.tell(EventStream.Publish(used_heap))
                                return Behaviors.same
                            case _InternalSubscribeResponse() | _InternalUpdateResponse():
                                return Behaviors.same  # ok
                            case _InternalClusterMemberUp(MemberUp(m)):
                                return running(max_heap, nodes_in_cluster | {node_key(m.address)})
                            case _InternalClusterMemberRemoved(MemberRemoved(m, _)):
                                if m.address == cluster.self_address:
                                    return Behaviors.stopped()
                                return running(max_heap, nodes_in_cluster - {node_key(m.address)})
                            case _Cleanup():
                                cleanup = cleanup_removed(nodes_in_cluster)
                                update(USED_HEAP_KEY, cleanup)
                                update(MAX_HEAP_KEY, cleanup)
                                return Behaviors.same

                    return behavior

                return running({}, frozenset())

            return with_timers

        return DistributedData.with_replicator_message_adapter(with_replicator)

    return setup
