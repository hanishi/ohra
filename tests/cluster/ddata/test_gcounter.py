"""Ported from Apache Pekko's GCounterSpec."""

import json

import pytest

from ohra.cluster.ddata import SelfUniqueAddress
from ohra.cluster.ddata.gcounter import GCounter, GCounterKey
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")
node3 = UniqueAddress("localhost", 7356, 3, "Sys")


def _never(*_a: object) -> object:
    raise AssertionError("resolver called")


def rt[T](v: T) -> T:
    wire = json.loads(json.dumps(Serialization.encode(v, lambda r: r.path)))
    return Serialization.decode(wire, _never)  # type: ignore[return-value]


def test_be_able_to_increment_each_nodes_record_by_one():
    c1 = GCounter()

    c2 = c1._increment(node1)
    c3 = c2._increment(node1)

    c4 = c3._increment(node2)
    c5 = c4._increment(node2)
    c6 = c5.reset_delta()._increment(node2)

    assert c6.state[node1] == 2
    assert c6.state[node2] == 3

    assert c2.delta.state[node1] == 1
    assert c1.merge_delta(c2.delta) == c2
    assert c3.delta.state[node1] == 2
    assert c2.merge_delta(c3.delta) == c3
    assert c6.delta.state[node2] == 3
    assert c5.merge_delta(c6.delta) == c6


def test_be_able_to_increment_each_nodes_record_by_arbitrary_delta():
    c1 = GCounter()

    c2 = c1._increment(node1, 3)
    c3 = c2._increment(node1, 4)

    c4 = c3._increment(node2, 2)
    c5 = c4._increment(node2, 7)
    c6 = c5._increment(node2)

    assert c6.state[node1] == 7
    assert c6.state[node2] == 10

    # Pekko's require(n >= 0)
    with pytest.raises(ValueError):
        c6._increment(node1, -1)
    with pytest.raises(ValueError):
        c6.increment(SelfUniqueAddress(node1), -1)


def test_be_able_to_summarize_the_history_to_the_correct_aggregated_value():
    c1 = GCounter()

    c2 = c1._increment(node1, 3)
    c3 = c2._increment(node1, 4)

    c4 = c3._increment(node2, 2)
    c5 = c4._increment(node2, 7)
    c6 = c5._increment(node2)

    assert c6.state[node1] == 7
    assert c6.state[node2] == 10

    assert c6.value == 17


def test_be_able_to_have_its_history_correctly_merged_with_another_gcounter_1():
    # counter 1
    c11 = GCounter()
    c12 = c11._increment(node1, 3)
    c13 = c12._increment(node1, 4)
    c14 = c13._increment(node2, 2)
    c15 = c14._increment(node2, 7)
    c16 = c15._increment(node2)

    assert c16.state[node1] == 7
    assert c16.state[node2] == 10
    assert c16.value == 17

    # counter 2
    c21 = GCounter()
    c22 = c21._increment(node1, 2)
    c23 = c22._increment(node1, 2)
    c24 = c23._increment(node2, 3)
    c25 = c24._increment(node2, 2)
    c26 = c25._increment(node2)

    assert c26.state[node1] == 4
    assert c26.state[node2] == 6
    assert c26.value == 10

    # merge both ways
    merged1 = c16.merge(c26)
    assert merged1.state[node1] == 7
    assert merged1.state[node2] == 10
    assert merged1.value == 17
    assert merged1.delta is None

    merged2 = c26.merge(c16)
    assert merged2.state[node1] == 7
    assert merged2.state[node2] == 10
    assert merged2.value == 17
    assert merged2.delta is None


def test_be_able_to_have_its_history_correctly_merged_with_another_gcounter_2():
    # counter 1
    c11 = GCounter()
    c12 = c11._increment(node1, 2)
    c13 = c12._increment(node1, 2)
    c14 = c13._increment(node2, 2)
    c15 = c14._increment(node2, 7)
    c16 = c15._increment(node2)

    assert c16.state[node1] == 4
    assert c16.state[node2] == 10
    assert c16.value == 14

    # counter 1
    c21 = GCounter()
    c22 = c21._increment(node1, 3)
    c23 = c22._increment(node1, 4)
    c24 = c23._increment(node2, 3)
    c25 = c24._increment(node2, 2)
    c26 = c25._increment(node2)

    assert c26.state[node1] == 7
    assert c26.state[node2] == 6
    assert c26.value == 13

    # merge both ways
    merged1 = c16.merge(c26)
    assert merged1.state[node1] == 7
    assert merged1.state[node2] == 10
    assert merged1.value == 17

    merged2 = c26.merge(c16)
    assert merged2.state[node1] == 7
    assert merged2.state[node2] == 10
    assert merged2.value == 17


def test_be_able_to_have_its_history_correctly_merged_with_another_gcounter_3():
    c1 = GCounter()

    c2 = c1._increment(node1, 3)
    c3 = c2._increment(node1, 4)

    # new empty counter for node2
    c4 = GCounter()
    c5 = c4._increment(node2, 5)
    c6 = c5._increment(node2)

    c7 = c6.merge(c3)

    assert c7.state[node1] == 7
    assert c7.state[node2] == 6

    assert c7.value == 13


def test_have_support_for_pruning():
    c1 = GCounter()
    c2 = c1._increment(node1)
    c3 = c2._increment(node2)
    assert c2.modified_by_nodes == {node1}
    assert c2.need_pruning_from(node1)
    assert not c2.need_pruning_from(node2)
    assert c3.modified_by_nodes == {node1, node2}
    assert c3.need_pruning_from(node1)
    assert c3.need_pruning_from(node2)
    assert c3.value == 2

    c4 = c3.prune(node1, node2)
    assert c4.modified_by_nodes == {node2}
    assert c4.need_pruning_from(node2)
    assert not c4.need_pruning_from(node1)
    assert c4.value == 2

    c5 = c4._increment(node1).pruning_cleanup(node1)
    assert c5.modified_by_nodes == {node2}
    assert not c5.need_pruning_from(node1)
    assert c4.value == 2


def test_have_unapply_extractor():
    # Pekko matches on GCounter(value) and Changed(GCounterKey("key")); ohra has the value and
    # the key's identity.
    c1 = GCounter.empty._increment(node1)._increment(node2)
    assert c1.value == 2
    assert GCounterKey("key") == GCounterKey("key")


def test_merge_with_its_own_descendant_equals_a_full_merge():
    def pair() -> tuple[GCounter, GCounter]:
        a = rt(GCounter()._increment(node1, 2))  # decoded: no ancestor
        return a, a._increment(node2, 5)

    a, b = pair()
    full = rt(a).merge(rt(b))  # decoded copies carry no ancestor
    assert b._ancestor is a
    assert a.merge(b) is b and b == full and full.value == 7
    a, b = pair()
    assert b._ancestor is a
    assert b.merge(a) is b and b == full
