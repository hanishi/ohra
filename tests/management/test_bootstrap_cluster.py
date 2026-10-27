"""Cluster bootstrap across nodes over ConfigServiceDiscovery: forming a cluster with no seed
nodes, late joiners, and the guards."""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any

import pytest

from ohra import (
    ActorSystem,
    Address,
    ClusterBootstrapSettings,
    ConfigServiceDiscovery,
    Lookup,
    Management,
    ManagementSettings,
    Resolved,
    ResolvedTarget,
    ServiceDiscovery,
)
from ohra.cluster.core import Cluster

from ..cluster.test_cluster import all_up, cluster_node
from ..util import eventually, free_port

BOOT: dict[str, Any] = dict(
    stable_margin=0.3,
    interval=0.05,
    probe_interval=0.05,
    probing_failure_timeout=0.5,
    resolve_timeout=0.5,
    required_contact_point_nr=2,
    # every node is on 127.0.0.1: Pekko's per-host fallback-port filter would keep one
    filter_on_fallback_port=False,
)


class LiveDiscovery(ServiceDiscovery):
    """Like the Kubernetes API: only pods that exist are listed, so the list grows as nodes
    start."""

    def __init__(self, mgmt_ports: list[int]) -> None:
        self.mgmt_ports = mgmt_ports

    async def lookup(self, lookup: Lookup, resolve_timeout: float) -> Resolved:
        return Resolved(
            lookup.service_name,
            tuple(ResolvedTarget("127.0.0.1", p, "127.0.0.1") for p in self.mgmt_ports),
        )


def bootstrap_node(
    port: int,
    mgmt_ports: list[int],
    my_mgmt: int,
    seeds: list[int] | None = None,
    discovery: ServiceDiscovery | None = None,
    **boot: Any,
) -> ActorSystem[Any]:
    discovery = discovery or ConfigServiceDiscovery(
        {"svc": [ResolvedTarget("127.0.0.1", p, "127.0.0.1") for p in mgmt_ports]}
    )
    management = ManagementSettings(
        hostname="127.0.0.1",
        port=my_mgmt,
        discovery=discovery,
        bootstrap=ClusterBootstrapSettings(service_name="svc", **{**BOOT, **boot}),
    )
    return cluster_node("c", port, seeds or [], management=management)


def three() -> tuple[list[int], list[int]]:
    return sorted(free_port() for _ in range(3)), sorted(free_port() for _ in range(3))


async def test_three_nodes_form_one_cluster_and_the_lowest_joins_itself():
    ports, mgmt = three()
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(bootstrap_node(p, mgmt, m))
            for p, m in zip(ports, mgmt, strict=True)
        ]
        for n in nodes:
            await Management(n).start()
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=10)
        assert len({Cluster(n).state.leader for n in nodes}) == 1
        oldest = min(Cluster(nodes[0]).state.members, key=lambda m: m.up_number)
        assert oldest.address.port == ports[0]  # the lowest management address self-joined first


async def test_a_late_node_joins_through_probing():
    ports, mgmt = three()
    p4, m4 = free_port(), free_port()
    live = LiveDiscovery(list(mgmt))
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(bootstrap_node(p, mgmt, m, discovery=live))
            for p, m in zip(ports, mgmt, strict=True)
        ]
        for n in nodes:
            await Management(n).start()
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=10)
        live.mgmt_ports.append(m4)  # the fourth pod is now listed
        late = await stack.enter_async_context(bootstrap_node(p4, mgmt, m4, discovery=live))
        await Management(late).start()
        await eventually(lambda: all(all_up(n, 4) for n in [*nodes, late]), timeout=10)


async def test_a_node_missing_itself_in_discovery_joins_once_seed_nodes_appear():
    ports, mgmt = three()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(bootstrap_node(ports[0], mgmt[:2], mgmt[0]))
        b = await stack.enter_async_context(bootstrap_node(ports[1], mgmt[:2], mgmt[1]))
        # c does not see itself
        c = await stack.enter_async_context(bootstrap_node(ports[2], mgmt[:2], mgmt[2]))
        for n in (a, b, c):
            await Management(n).start()
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=10)


async def test_new_cluster_disabled_forms_nothing():
    ports, mgmt = three()
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(
                bootstrap_node(p, mgmt[:2], m, new_cluster_enabled=False)
            )
            for p, m in zip(ports[:2], mgmt[:2], strict=True)
        ]
        for n in nodes:
            await Management(n).start()
        await asyncio.sleep(1.5 * BOOT["stable_margin"] + 0.5)
        assert all(not Cluster(n).state.members for n in nodes)


async def test_probe_refuses_own_remoting_address(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.WARNING, logger="ohra.management.bootstrap")
    p, m = free_port(), free_port()
    # the remoting port, not management
    discovery = ConfigServiceDiscovery({"svc": [ResolvedTarget("127.0.0.1", p, "127.0.0.1")]})
    management = ManagementSettings(
        hostname="127.0.0.1",
        port=m,
        discovery=discovery,
        bootstrap=ClusterBootstrapSettings(
            service_name="svc", **{**BOOT, "required_contact_point_nr": 1}
        ),
    )
    async with cluster_node("c", p, [], management=management) as node:
        await Management(node).start()
        await eventually(
            lambda: any("Misconfiguration detected" in r.message for r in caplog.records),
            timeout=5,
        )
        await asyncio.sleep(1.5 * BOOT["stable_margin"])
        assert not Cluster(node).state.members


async def test_bootstrap_with_seed_nodes_configured_bails_out(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.WARNING, logger="ohra.management.bootstrap")
    p, m = free_port(), free_port()
    async with bootstrap_node(p, [m], m, seeds=[p]) as node:
        await Management(node).start()
        assert any("bailing out of the bootstrap process" in r.message for r in caplog.records)
        await eventually(lambda: all_up(node, 1), timeout=5)


async def test_ensure_probing_reuses_a_child_that_is_still_stopping():
    """After a probe is dropped (ProbingFailed or a vanished contact point) its child may still
    exist while the next discovery lists the same point; spawning a duplicate by name would fail
    the coordinator. Pekko uses context.child."""
    from ohra import ActorContext, Behavior, Behaviors
    from ohra.management.bootstrap import _Coordinator  # pyright: ignore[reportPrivateUsage]

    p, m = free_port(), free_port()
    outcome: asyncio.Future[str] = asyncio.get_running_loop().create_future()
    cp = ResolvedTarget("127.0.0.1", free_port(), "127.0.0.1")
    settings = ClusterBootstrapSettings(service_name="svc", **BOOT)

    def probe_twice(ctx: ActorContext[None]) -> Behavior[None]:
        coordinator = _Coordinator(
            ctx, settings, ConfigServiceDiscovery({"svc": [cp]}), ("127.0.0.1", m), ""
        )
        coordinator.ensure_probing(cp)
        coordinator.probes.clear()  # as _ProbingFailed does; the child still exists
        try:
            coordinator.ensure_probing(cp)
            outcome.set_result("reused" if len(ctx.children) == 1 else "duplicated")
        except Exception as e:
            outcome.set_result(f"raised {type(e).__name__}")
        return Behaviors.ignore

    async with cluster_node(
        "c", p, [], management=ManagementSettings(hostname="127.0.0.1", port=m)
    ) as node:
        node._system_root.spawn_child(Behaviors.setup(probe_twice), "probeTwice", None)  # pyright: ignore[reportPrivateUsage]
        assert await asyncio.wait_for(outcome, 5) == "reused"


def test_decider_resolves_its_own_hostname_once(monkeypatch: pytest.MonkeyPatch):
    """matches_self must not call DNS on the event loop per tick: the self IP is resolved once at
    construction."""
    import socket

    from ohra.management.bootstrap import (
        JoinSelf,
        LowestAddressJoinDecider,
        SeedNodesInformation,
        SeedNodesObservation,
    )

    calls: list[str] = []

    def fake(host: str) -> str:
        calls.append(host)
        return "10.0.0.1"

    monkeypatch.setattr(socket, "gethostbyname", fake)
    decider = LowestAddressJoinDecider(
        ClusterBootstrapSettings(service_name="svc", required_contact_point_nr=1),
        ("mgmt.local", 7626),
        logging.getLogger("t"),
    )
    a = ResolvedTarget("10-0-0-1.ns.pod.cluster.local", 7626, "10.0.0.1")
    info = SeedNodesInformation(
        200.0,
        100.0,
        frozenset({a}),
        frozenset({SeedNodesObservation(100.0, a, Address("c", "10.0.0.1", 2552), frozenset())}),
    )
    for _ in range(20):
        assert decider.decide(info) == JoinSelf()
    assert calls == ["mgmt.local"]
