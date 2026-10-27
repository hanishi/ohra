"""The bootstrap's pure parts: host selection, the self match, the backoff and the lowest-address
join decider."""

from __future__ import annotations

import logging
from dataclasses import replace

from ohra import Address, ClusterBootstrapSettings, Lookup, ResolvedTarget
from ohra.management.bootstrap import (
    JoinOtherSeedNodes,
    JoinSelf,
    KeepProbing,
    LowestAddressJoinDecider,
    SeedNodesInformation,
    SeedNodesObservation,
    backed_off_interval,
    matches_self,
    select_hosts,
)

S = ClusterBootstrapSettings(service_name="svc", stable_margin=5.0, required_contact_point_nr=2)
A, B, C = (
    ResolvedTarget("10.0.0.1", 7626, "10.0.0.1"),
    ResolvedTarget("10.0.0.2", 7626, "10.0.0.2"),
    ResolvedTarget("10.0.0.3", 7626, "10.0.0.3"),
)


def node(i: int) -> Address:
    return Address("c", f"10.0.0.{i}", 2552)


def obs(cp: ResolvedTarget, *seeds: Address, at: float = 100.0) -> SeedNodesObservation:
    return SeedNodesObservation(at, cp, node(int(cp.host[-1])), frozenset(seeds))


def info(
    points: list[ResolvedTarget],
    observations: list[SeedNodesObservation],
    now: float = 200.0,
    changed_at: float = 100.0,
) -> SeedNodesInformation:
    return SeedNodesInformation(now, changed_at, frozenset(points), frozenset(observations))


def decider(
    self_cp: tuple[str, int] = ("10.0.0.1", 7626), settings: ClusterBootstrapSettings = S
) -> LowestAddressJoinDecider:
    return LowestAddressJoinDecider(settings, self_cp, logging.getLogger("t"))


def test_seed_nodes_seen_join_them():
    assert decider().decide(
        info([A, B], [obs(A), obs(B, node(2), node(3))])
    ) == JoinOtherSeedNodes(frozenset({node(2), node(3)}))


def test_too_few_contact_points_keep_probing():
    assert decider().decide(info([A, B], [obs(A)])) == KeepProbing()


def test_inside_stable_margin_keep_probing():
    assert decider().decide(info([A, B], [obs(A), obs(B)], now=104.0)) == KeepProbing()


def test_missing_observation_keep_probing():
    assert decider().decide(info([A, B, C], [obs(A), obs(B)])) == KeepProbing()


def test_lowest_is_self_joins_self():
    assert decider().decide(info([A, B], [obs(A), obs(B)])) == JoinSelf()


def test_lowest_is_other_keeps_probing():
    assert (
        decider(self_cp=("10.0.0.2", 7626)).decide(info([A, B], [obs(A), obs(B)])) == KeepProbing()
    )


def test_new_cluster_disabled_never_joins_self():
    assert (
        decider(settings=replace(S, new_cluster_enabled=False)).decide(
            info([A, B], [obs(A), obs(B)])
        )
        == KeepProbing()
    )


def test_matches_self_by_ip():
    assert matches_self(
        ResolvedTarget("10-0-0-1.ns.pod.cluster.local", 7626, "10.0.0.1"), ("10.0.0.1", 7626)
    )
    assert not matches_self(A, ("10.0.0.1", 7627))
    assert matches_self(ResolvedTarget("10.0.0.1", None), ("10.0.0.1", 7627))


def test_select_hosts_keeps_fallback_port_per_host():
    pts = [ResolvedTarget("h", 7626), ResolvedTarget("h", 2552), ResolvedTarget("g", 2552)]
    assert select_hosts(Lookup("svc"), 7626, True, pts) == [
        ResolvedTarget("h", 7626),
        ResolvedTarget("g", 2552),
    ]
    assert select_hosts(Lookup("svc", port_name="m"), 7626, True, pts) == pts
    assert select_hosts(Lookup("svc"), 7626, False, pts) == pts


def test_backed_off_interval_doubles_and_caps():
    assert backed_off_interval(0, 1.0, 15.0, 0.0) == 1.0
    assert backed_off_interval(3, 1.0, 15.0, 0.0) == 8.0
    assert backed_off_interval(10, 1.0, 15.0, 0.0) == 15.0
    assert 2.0 <= backed_off_interval(1, 1.0, 15.0, 0.5) <= 3.0
    # Pekko: the random factor applies after the cap
    assert all(15.0 <= backed_off_interval(10, 1.0, 15.0, 0.5) <= 22.5 for _ in range(50))
