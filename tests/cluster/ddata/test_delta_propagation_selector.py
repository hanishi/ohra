"""DeltaPropagationSelector, ported from Pekko's ``DeltaPropagationSelectorSpec``. Its
``TestSelector`` is an instance built with the constructor callables."""

import pytest

from ohra.cluster.ddata import SelfUniqueAddress
from ohra.cluster.ddata.delta_propagation_selector import DeltaPropagationSelector
from ohra.cluster.ddata.envelope import (
    NO_DELTA_PLACEHOLDER,
    DataEnvelope,
    Delta,
    DeltaPropagation,
)
from ohra.cluster.ddata.gset import GSet
from ohra.cluster.ddata.pncounter_map import PNCounterMap
from ohra.cluster.ddata.replicated_data import ReplicatedData
from ohra.remote import UniqueAddress

self_unique_address = UniqueAddress("localhost", 4999, 17, "Sys")
nodes = [UniqueAddress("localhost", n, 17, "Sys") for n in range(2500, 2600)]


def make_selector(all_nodes: list[UniqueAddress]) -> DeltaPropagationSelector:
    def create(deltas: dict[str, tuple[ReplicatedData, int, int]]) -> DeltaPropagation:
        return DeltaPropagation(
            self_unique_address,
            False,
            {k: Delta(DataEnvelope(d), f, t) for k, (d, f, t) in deltas.items()},
            None,
        )

    return DeltaPropagationSelector(
        gossip_interval_divisor=5,
        all_nodes=lambda: all_nodes,
        max_delta_size=10,
        create_delta_propagation=create,
    )


def dp(**deltas: tuple[ReplicatedData, int, int]) -> DeltaPropagation:
    return DeltaPropagation(
        self_unique_address,
        False,
        {k: Delta(DataEnvelope(d), f, t) for k, (d, f, t) in deltas.items()},
        None,
    )


def gset(*xs: str) -> GSet[str]:
    return GSet(frozenset(xs))


delta_a = GSet[str]().add("a")
delta_b = GSet[str]().add("b")
delta_c = GSet[str]().add("c")


def test_collect_none_when_no_nodes() -> None:
    selector = make_selector([])
    selector.update("A", delta_a)
    assert selector.collect_propagations() == {}
    selector.cleanup_delta_entries()
    assert not selector.has_delta_entries("A")


def test_collect_1_when_one_node() -> None:
    selector = make_selector(nodes[:1])
    selector.update("A", delta_a)
    selector.update("B", delta_b)
    selector.cleanup_delta_entries()
    assert selector.has_delta_entries("A")
    assert selector.has_delta_entries("B")
    expected = dp(A=(delta_a, 1, 1), B=(delta_b, 1, 1))
    assert selector.collect_propagations() == {nodes[0]: expected}
    assert selector.collect_propagations() == {}
    selector.cleanup_delta_entries()
    assert not selector.has_delta_entries("A")
    assert not selector.has_delta_entries("B")


def test_collect_2_plus_1_when_three_nodes() -> None:
    selector = make_selector(nodes[:3])
    selector.update("A", delta_a)
    selector.update("B", delta_b)
    expected = dp(A=(delta_a, 1, 1), B=(delta_b, 1, 1))
    assert selector.collect_propagations() == {nodes[0]: expected, nodes[1]: expected}
    selector.cleanup_delta_entries()
    assert selector.has_delta_entries("A")
    assert selector.has_delta_entries("B")
    assert selector.collect_propagations() == {nodes[2]: expected}
    assert selector.collect_propagations() == {}
    selector.cleanup_delta_entries()
    assert not selector.has_delta_entries("A")
    assert not selector.has_delta_entries("B")


def test_keep_track_of_deltas_per_node() -> None:
    selector = make_selector(nodes[:3])
    selector.update("A", delta_a)
    selector.update("B", delta_b)
    expected1 = dp(A=(delta_a, 1, 1), B=(delta_b, 1, 1))
    assert selector.collect_propagations() == {nodes[0]: expected1, nodes[1]: expected1}
    # new update before previous was propagated to all nodes
    selector.update("C", delta_c)
    expected2 = dp(A=(delta_a, 1, 1), B=(delta_b, 1, 1), C=(delta_c, 1, 1))
    expected3 = dp(C=(delta_c, 1, 1))
    assert selector.collect_propagations() == {nodes[2]: expected2, nodes[0]: expected3}
    selector.cleanup_delta_entries()
    assert not selector.has_delta_entries("A")
    assert not selector.has_delta_entries("B")
    assert selector.has_delta_entries("C")
    assert selector.collect_propagations() == {nodes[1]: expected3}
    assert selector.collect_propagations() == {}
    selector.cleanup_delta_entries()
    assert not selector.has_delta_entries("C")


def test_bump_version_for_each_update() -> None:
    delta1 = GSet[str]().add("a1")
    delta2 = GSet[str]().add("a2")
    delta3 = GSet[str]().add("a3")
    selector = make_selector(nodes[:1])
    selector.update("A", delta1)
    assert selector.current_version("A") == 1
    selector.update("A", delta2)
    assert selector.current_version("A") == 2
    assert selector.collect_propagations() == {nodes[0]: dp(A=(delta1.merge(delta2), 1, 2))}
    selector.update("A", delta3)
    assert selector.current_version("A") == 3
    assert selector.collect_propagations() == {nodes[0]: dp(A=(delta3, 3, 3))}
    assert selector.collect_propagations() == {}


def test_merge_deltas(monkeypatch: pytest.MonkeyPatch) -> None:
    delta1 = GSet[str]().add("a1")
    delta2 = GSet[str]().add("a2")
    delta3 = GSet[str]().add("a3")
    selector = make_selector(nodes[:3])
    monkeypatch.setattr(selector, "nodes_slice_size", lambda n: 1)
    selector.update("A", delta1)
    assert selector.collect_propagations() == {nodes[0]: dp(A=(delta1, 1, 1))}

    selector.update("A", delta2)
    assert selector.collect_propagations() == {nodes[1]: dp(A=(delta1.merge(delta2), 1, 2))}

    selector.update("A", delta3)
    expected3 = dp(A=(delta1.merge(delta2).merge(delta3), 1, 3))
    assert selector.collect_propagations() == {nodes[2]: expected3}

    assert selector.collect_propagations() == {nodes[0]: dp(A=(delta2.merge(delta3), 2, 3))}
    assert selector.collect_propagations() == {nodes[1]: dp(A=(delta3, 3, 3))}
    assert selector.collect_propagations() == {}


def test_discard_too_large_deltas(monkeypatch: pytest.MonkeyPatch) -> None:
    selector = make_selector(nodes[:3])
    monkeypatch.setattr(selector, "nodes_slice_size", lambda n: 1)
    data = PNCounterMap[str].empty
    me = SelfUniqueAddress(self_unique_address)
    for n in range(1, 1001):
        d = data.reset_delta().increment(me, str(n % 2), 1)
        assert d.delta is not None
        selector.update("A", d.delta)
        data = d
    expected = dp(A=(NO_DELTA_PLACEHOLDER, 1, 1000))
    assert selector.collect_propagations() == {nodes[0]: expected}


def test_calculate_right_slice_size() -> None:
    selector = make_selector(nodes)
    assert selector.nodes_slice_size(0) == 0
    assert selector.nodes_slice_size(1) == 1
    for n in range(2, 10):
        assert selector.nodes_slice_size(n) == 2, n
    for lo, size in [(10, 3), (15, 4), (20, 5), (25, 6), (30, 7), (35, 8), (40, 9)]:
        for n in range(lo, lo + 5):
            assert selector.nodes_slice_size(n) == size, n
    for n in range(45, 201):
        assert selector.nodes_slice_size(n) == 10, n
