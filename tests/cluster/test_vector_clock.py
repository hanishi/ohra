"""VectorClock: Pekko's causal ordering of gossip versions."""
from __future__ import annotations

from pekko_py.cluster.vector_clock import VectorClock


def test_increment_and_compare_orderings():
    empty = VectorClock()
    a1 = empty.increment("a")
    assert a1.versions == {"a": 1}
    assert empty.compare(empty) == "same"
    assert a1.compare(a1.increment("b").prune("b")) == "same"
    assert empty.compare(a1) == "before"
    assert a1.compare(empty) == "after"
    a1b1 = a1.increment("b")
    assert a1.compare(a1b1) == "before" and a1b1.compare(a1) == "after"
    a2 = a1.increment("a")
    assert a2.compare(a1b1) == "concurrent" and a1b1.compare(a2) == "concurrent"


def test_merge_takes_the_maximum_per_node_and_prune_drops_a_node():
    x = VectorClock().increment("a").increment("a").increment("b")
    y = VectorClock().increment("a").increment("c")
    merged = x.merge(y)
    assert merged.versions == {"a": 2, "b": 1, "c": 1}
    assert merged.compare(x) == "after" and merged.compare(y) == "after"
    assert merged.prune("c").versions == {"a": 2, "b": 1}
    assert merged.prune("zzz") is merged
