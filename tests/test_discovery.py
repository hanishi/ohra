"""Pekko's ServiceDiscovery API and the config-based implementation."""

from __future__ import annotations

from pekko_py import ConfigServiceDiscovery, Lookup, Resolved, ResolvedTarget


async def test_config_discovery_resolves_known_and_unknown_names():
    d = ConfigServiceDiscovery(
        {
            "svc": [
                ResolvedTarget("10.0.0.2", 7626, "10.0.0.2"),
                ResolvedTarget("10.0.0.1", 7626, "10.0.0.1"),
            ]
        }
    )
    assert await d.lookup(Lookup("svc"), 1.0) == Resolved(
        "svc",
        (
            ResolvedTarget("10.0.0.2", 7626, "10.0.0.2"),
            ResolvedTarget("10.0.0.1", 7626, "10.0.0.1"),
        ),
    )
    assert await d.lookup(Lookup("other"), 1.0) == Resolved("other", ())


def test_resolved_targets_order_by_host_then_port():
    assert sorted([ResolvedTarget("b", 1), ResolvedTarget("a", 2), ResolvedTarget("a", None)]) == [
        ResolvedTarget("a", None),
        ResolvedTarget("a", 2),
        ResolvedTarget("b", 1),
    ]
