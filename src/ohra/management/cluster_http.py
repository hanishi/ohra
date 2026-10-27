"""Pekko's cluster HTTP management, read-only: the members, one member, and a sharding type's
shards, with Pekko's JSON field names. Mutating routes (join, leave, down) are not served, as with
route-providers-read-only."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any

from ..cluster.core import Cluster
from ..cluster.gossip import Member
from ..cluster.sharding import (
    ClusterSharding,
    CurrentShardRegionState,
    GetShardRegionState,
    shard_id_of,
)
from ..patterns import AskTimeoutError, ask
from .http import Response, json_response

if TYPE_CHECKING:
    from ..system import ActorSystem
    from . import Management

_STATUS_NAMES = {
    "joining": "Joining",
    "weakly_up": "WeaklyUp",
    "up": "Up",
    "leaving": "Leaving",
    "exiting": "Exiting",
    "down": "Down",
    "removed": "Removed",
}


def member_json(m: Member) -> dict[str, object]:
    return {
        "node": m.address.root,
        "nodeUid": m.unique_address.uid,
        "status": _STATUS_NAMES[m.status],
        "roles": [],
    }


def members_json(cluster: Cluster) -> dict[str, object]:
    """ClusterHttpManagementRoutes.routeGetMembers: members, unreachable with their observers,
    leader and oldest."""
    state = cluster.state
    reachability = cluster._membership_state.latest_gossip.reachability  # pyright: ignore[reportPrivateUsage]
    observers: dict[str, list[str]] = {}
    for r in reachability.records:
        if r.status == "unreachable":
            observers.setdefault(r.subject.address.root, []).append(r.observer.address.root)
    up = [m for m in state.members if m.status == "up"]
    oldest = min(up, key=lambda m: (m.up_number, m.address)) if up else None
    return {
        "selfNode": cluster.self_address.root,
        "members": [member_json(m) for m in state.members],
        "unreachable": [
            {"node": node, "observedBy": sorted(obs)} for node, obs in sorted(observers.items())
        ],
        "leader": state.leader.root if state.leader is not None else None,
        "oldest": oldest.address.root if oldest is not None else None,
        "oldestPerRole": {},
    }


def register_cluster_routes(management: Management, system: ActorSystem[Any]) -> None:
    cluster = Cluster(system)

    async def members(_path: str) -> Response:
        return json_response(200, members_json(cluster))

    async def member(address: str) -> Response:
        for m in cluster.state.members:
            if m.address.root == address:
                return json_response(200, member_json(m))
        return json_response(404, {"message": f"Member [{address}] not found"})

    async def shards(type_name: str) -> Response:
        sharding = ClusterSharding(system)
        region = sharding._regions.get(type_name)  # pyright: ignore[reportPrivateUsage]
        entity = sharding._entities.get(type_name)  # pyright: ignore[reportPrivateUsage]
        if region is None or entity is None:
            return json_response(
                404, {"message": f"Shard region [{type_name}] not started on this node"}
            )
        try:
            state: CurrentShardRegionState = await ask(region, GetShardRegionState, timeout=5)
        except AskTimeoutError:
            return json_response(
                500, {"message": f"Shard region [{type_name}] did not answer within 5 s"}
            )
        counts = Counter(
            shard_id_of(entity_id, entity.effective_settings.number_of_shards)
            for entity_id in state.entity_ids
        )
        return json_response(
            200,
            {
                "regions": [
                    {"shardId": shard, "numEntities": n} for shard, n in sorted(counts.items())
                ]
            },
        )

    management.add_route("/cluster/members", members)
    management.add_route("/cluster/members/", member)
    management.add_route("/cluster/shards/", shards)
