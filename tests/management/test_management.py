"""PekkoManagement: the endpoint binds once, serves Pekko's health checks, and a cluster node is ready when Up."""
from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from pekko_py import ActorSystem, Address, Behaviors, HealthCheckSettings, Join, PekkoManagement, PekkoManagementSettings
from pekko_py.cluster.core import Cluster
from pekko_py.management.http import get

from ..cluster.test_cluster import all_up, cluster_node
from ..util import eventually, free_port


def mgmt(**kw: Any) -> PekkoManagementSettings:
    return PekkoManagementSettings(hostname="127.0.0.1", port=0, **kw)


async def test_start_binds_and_serves_health_routes():
    async with ActorSystem(Behaviors.ignore, "m", management=mgmt()) as system:
        uri = await PekkoManagement(system).start()
        assert uri.startswith("http://127.0.0.1:") and await PekkoManagement(system).start() == uri
        for path in ("/ready", "/alive", "/startup"):
            assert (await get(uri + path, timeout=2))[0] == 200
    with pytest.raises(OSError):
        await get(uri + "/ready", timeout=1)  # closed with the system


async def test_failing_check_names_itself_and_times_out():
    async with ActorSystem(Behaviors.ignore, "m", management=mgmt(health_checks=HealthCheckSettings(check_timeout=0.1))) as system:
        m = PekkoManagement(system)

        async def ok() -> bool:
            return True

        async def no() -> bool:
            return False

        async def boom() -> bool:
            raise RuntimeError("db down")

        async def slow() -> bool:
            await asyncio.sleep(5)
            return True

        m.health.add_readiness_check("ok", ok)
        m.health.add_readiness_check("db", no)
        m.health.add_liveness_check("boom", boom)
        m.health.add_startup_check("slow", slow)
        uri = await m.start()
        assert await get(uri + "/ready", timeout=2) == (500, b"Not Healthy: Check [db] not ok")
        assert (await get(uri + "/alive", timeout=2))[1] == b"Not Healthy: Check [boom] failed: db down"
        assert (await get(uri + "/startup", timeout=2))[1] == b"Not Healthy: Check [slow] timed out after 0.1 s"


async def test_base_path_and_custom_check_paths():
    settings = mgmt(base_path="/management", health_checks=HealthCheckSettings(readiness_path="health/ready"))
    async with ActorSystem(Behaviors.ignore, "m", management=settings) as system:
        uri = await PekkoManagement(system).start()
        assert uri.endswith("/management") and (await get(uri + "/health/ready", timeout=2))[0] == 200
        assert (await get(uri.removesuffix("/management") + "/health/ready", timeout=2))[0] == 404


async def test_start_on_a_taken_port_raises():
    blocker = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = blocker.sockets[0].getsockname()[1]
    try:
        async with ActorSystem(Behaviors.ignore, "m", management=PekkoManagementSettings(hostname="127.0.0.1", port=port)) as system:
            with pytest.raises(OSError):
                await PekkoManagement(system).start()
            assert PekkoManagement(system).uri is None
    finally:
        blocker.close()
        await blocker.wait_closed()


def test_settings_validation():
    with pytest.raises(ValueError):
        PekkoManagementSettings(bootstrap=cast(Any, object()))  # bootstrap without discovery
    with pytest.raises(ValueError):
        PekkoManagementSettings(base_path="management")
    with pytest.raises(ValueError):
        PekkoManagementSettings(port=70000)


async def test_cluster_membership_readiness():
    p = free_port()
    async with cluster_node("c", p, []) as node:  # no seed nodes: never joins by itself
        uri = await PekkoManagement(node).start()
        assert (await get(uri + "/ready", timeout=2))[0] == 500
        Cluster(node).manager.tell(Join(Address("c", "127.0.0.1", p)))
        await eventually(lambda: all_up(node, 1), timeout=5)
        assert (await get(uri + "/ready", timeout=2))[0] == 200
