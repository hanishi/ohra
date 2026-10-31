"""Read-only cluster HTTP management: Pekko's JSON for members and shards; mutating methods are
405."""

from __future__ import annotations

import asyncio
import json
from contextlib import AsyncExitStack

from ohra import Management
from ohra.cluster.core import Cluster
from ohra.cluster.sharding import shard_id_of
from ohra.management.http import get
from ohra.patterns import ask

from ..cluster.test_cluster import all_up, cluster_node, crash, unreachable_ports
from ..cluster.test_cluster_sharding import Count, ref
from ..cluster.test_cluster_sharding import node as sharding_node
from ..cluster.test_cluster_sharding import start as start_sharding
from ..util import eventually, free_port


async def test_cluster_members_json():
    p1, p2 = sorted((free_port(), free_port()))
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        uri = await Management(a).start()
        status, body = await get(uri + "/cluster/members", timeout=2)
        doc = json.loads(body)
        assert (
            status == 200
            and doc["selfNode"] == f"ohra://c@127.0.0.1:{p1}"
            and doc["leader"] == f"ohra://c@127.0.0.1:{p1}"
        )
        assert sorted(m["node"] for m in doc["members"]) == [
            f"ohra://c@127.0.0.1:{p1}",
            f"ohra://c@127.0.0.1:{p2}",
        ]
        assert all(m["status"] == "Up" and m["roles"] == [] for m in doc["members"])
        assert (
            doc["unreachable"] == []
            and doc["oldestPerRole"] == {}
            and doc["oldest"] == f"ohra://c@127.0.0.1:{p1}"
        )
        one = json.loads(
            (await get(uri + f"/cluster/members/ohra://c@127.0.0.1:{p2}", timeout=2))[1]
        )
        assert one["node"] == f"ohra://c@127.0.0.1:{p2}" and isinstance(one["nodeUid"], int)
        assert (await get(uri + "/cluster/members/ohra://c@127.0.0.1:1", timeout=2))[0] == 404


async def test_a_member_address_may_be_percent_encoded():
    p = free_port()
    async with cluster_node("c", p, [p]) as a:
        await eventually(lambda: all_up(a, 1), timeout=5)
        uri = await Management(a).start()
        status, body = await get(
            uri + f"/cluster/members/ohra%3A%2F%2Fc%40127.0.0.1%3A{p}", timeout=2
        )
        assert status == 200 and json.loads(body)["node"] == f"ohra://c@127.0.0.1:{p}"


async def test_unreachable_members_list_their_observers():
    p1, p2 = sorted((free_port(), free_port()))
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        uri = await Management(a).start()
        await crash(b)
        await eventually(lambda: unreachable_ports(a) == {p2}, timeout=5)
        doc = json.loads((await get(uri + "/cluster/members", timeout=2))[1])
        assert doc["unreachable"] == [
            {"node": f"ohra://c@127.0.0.1:{p2}", "observedBy": [f"ohra://c@127.0.0.1:{p1}"]}
        ]


async def test_cluster_shards_route():
    p = free_port()
    async with sharding_node(p, [p]) as a:
        await eventually(lambda: all_up(a, 1), timeout=5)
        start_sharding(a)
        ids = ["a", "b", "c", "d", "e", "f"]
        for i in ids:
            assert await ask(ref(a, i), lambda r: Count(1, r), timeout=5) == 1
        uri = await Management(a).start()
        status, body = await get(uri + "/cluster/shards/Counter", timeout=3)
        doc = json.loads(body)
        assert status == 200 and sum(r["numEntities"] for r in doc["regions"]) == 6
        assert {r["shardId"] for r in doc["regions"]} == {shard_id_of(i, 10) for i in ids}
        assert (await get(uri + "/cluster/shards/Nope", timeout=2))[0] == 404


async def test_mutating_routes_are_405():
    p = free_port()
    async with cluster_node("c", p, [p]) as a:
        uri = await Management(a).start()
        host, port = uri.removeprefix("http://").split("/")[0].split(":")
        for request in (
            b"PUT /cluster/members/x HTTP/1.1\r\nHost: x\r\n\r\n",
            b"POST /cluster/members HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n",
        ):
            reader, writer = await asyncio.open_connection(host, int(port))
            writer.write(request)
            await writer.drain()
            assert (await reader.readline()).startswith(b"HTTP/1.1 405")
            writer.close()
            await writer.wait_closed()
