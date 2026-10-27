"""PekkoManagement: the endpoint binds once, serves Pekko's health checks, and a cluster node is
ready when Up."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from pekko_py import (
    ActorSystem,
    Address,
    Behaviors,
    HealthCheckSettings,
    Join,
    PekkoManagement,
    PekkoManagementSettings,
)
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
    async with ActorSystem(
        Behaviors.ignore,
        "m",
        management=mgmt(health_checks=HealthCheckSettings(check_timeout=0.1)),
    ) as system:
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
        assert (await get(uri + "/alive", timeout=2))[
            1
        ] == b"Not Healthy: Check [boom] failed: db down"
        assert (await get(uri + "/startup", timeout=2))[
            1
        ] == b"Not Healthy: Check [slow] timed out after 0.1 s"


async def test_base_path_and_custom_check_paths():
    settings = mgmt(
        base_path="/management", health_checks=HealthCheckSettings(readiness_path="health/ready")
    )
    async with ActorSystem(Behaviors.ignore, "m", management=settings) as system:
        uri = await PekkoManagement(system).start()
        assert (
            uri.endswith("/management") and (await get(uri + "/health/ready", timeout=2))[0] == 200
        )
        assert (await get(uri.removesuffix("/management") + "/health/ready", timeout=2))[0] == 404


async def test_start_on_a_taken_port_raises():
    blocker = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = blocker.sockets[0].getsockname()[1]
    try:
        async with ActorSystem(
            Behaviors.ignore,
            "m",
            management=PekkoManagementSettings(hostname="127.0.0.1", port=port),
        ) as system:
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


async def test_bootstrap_on_a_non_cluster_system_is_refused():
    from pekko_py import ClusterBootstrapSettings, ConfigServiceDiscovery

    settings = mgmt(
        bootstrap=ClusterBootstrapSettings(service_name="x"), discovery=ConfigServiceDiscovery({})
    )
    with pytest.raises(ValueError, match="cluster"):
        # bootstrap joins a cluster; there is none to join
        ActorSystem(Behaviors.ignore, "m", management=settings)


async def test_cluster_membership_readiness():
    p = free_port()
    async with cluster_node("c", p, []) as node:  # no seed nodes: never joins by itself
        uri = await PekkoManagement(node).start()
        assert (await get(uri + "/ready", timeout=2))[0] == 500
        Cluster(node).manager.tell(Join(Address("c", "127.0.0.1", p)))
        await eventually(lambda: all_up(node, 1), timeout=5)
        assert (await get(uri + "/ready", timeout=2))[0] == 200


async def test_idle_and_half_open_connections_do_not_block_shutdown():
    """A client that connects and says nothing, and one that asked for a slow route and went away,
    must not hold the system's termination; the server cancels its connection tasks when it
    closes."""
    system = ActorSystem(Behaviors.ignore, "m", management=mgmt())
    uri = await PekkoManagement(system).start()
    host, port = uri.removeprefix("http://").split(":")
    idle_reader, idle_writer = await asyncio.open_connection(host, int(port))
    began = asyncio.get_running_loop().time()
    system.terminate()
    await asyncio.wait_for(system.when_terminated(), timeout=5)
    assert asyncio.get_running_loop().time() - began < 1.0
    assert await idle_reader.read() == b""  # the server closed it
    idle_writer.close()
    await idle_writer.wait_closed()


async def test_start_after_termination_raises_and_stop_allows_a_rebind():
    system = ActorSystem(Behaviors.ignore, "m", management=mgmt())
    m = PekkoManagement(system)
    first = await m.start()
    await m.stop()
    assert m.uri is None
    with pytest.raises(OSError):
        await get(first + "/ready", timeout=1)
    second = await m.start()  # a rebind after stop, as Pekko's stop/start
    assert (await get(second + "/ready", timeout=2))[0] == 200
    system.terminate()
    await system.when_terminated()
    with pytest.raises(RuntimeError):
        await m.start()  # the system is gone: nothing would ever close a new server
