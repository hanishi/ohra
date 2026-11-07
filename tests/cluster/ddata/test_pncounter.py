"""Ported from Apache Pekko's PNCounterSpec."""

from ohra.cluster.ddata.pncounter import PNCounter, PNCounterKey
from ohra.remote import UniqueAddress

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")


def test_be_able_to_increment_each_nodes_record_by_one():
    c1 = PNCounter()

    c2 = c1._increment(node1)
    c3 = c2._increment(node1)

    c4 = c3._increment(node2)
    c5 = c4._increment(node2)
    c6 = c5.reset_delta()._increment(node2)

    assert c6.increments.state[node1] == 2
    assert c6.increments.state[node2] == 3

    assert c2.delta.value == 1
    assert c2.delta.increments.state[node1] == 1
    assert c3.delta.value == 2
    assert c3.delta.increments.state[node1] == 2

    assert c6.delta.value == 3
    assert c6.delta.increments.state[node2] == 3


def test_be_able_to_decrement_each_nodes_record_by_one():
    c1 = PNCounter()

    c2 = c1._decrement(node1)
    c3 = c2._decrement(node1)

    c4 = c3._decrement(node2)
    c5 = c4._decrement(node2)
    c6 = c5.reset_delta()._decrement(node2)

    assert c6.decrements.state[node1] == 2
    assert c6.decrements.state[node2] == 3

    assert c3.delta.value == -2
    assert c3.delta.decrements.state[node1] == 2

    assert c6.delta.value == -3
    assert c6.delta.decrements.state[node2] == 3


def test_be_able_to_increment_each_nodes_record_by_arbitrary_delta():
    c1 = PNCounter()

    c2 = c1._increment(node1, 3)
    c3 = c2._increment(node1, 4)

    c4 = c3._increment(node2, 2)
    c5 = c4._increment(node2, 7)
    c6 = c5._increment(node2)

    assert c6.increments.state[node1] == 7
    assert c6.increments.state[node2] == 10


def test_be_able_to_increment_each_nodes_record_by_arbitrary_bigint_delta():
    c1 = PNCounter()

    c2 = c1._increment(node1, 3)
    c3 = c2._increment(node1, 4)

    c4 = c3._increment(node2, 2)
    c5 = c4._increment(node2, 7)
    c6 = c5._increment(node2)

    assert c6.increments.state[node1] == 7
    assert c6.increments.state[node2] == 10


def test_be_able_to_decrement_each_nodes_record_by_arbitrary_delta():
    c1 = PNCounter()

    c2 = c1._decrement(node1, 3)
    c3 = c2._decrement(node1, 4)

    c4 = c3._decrement(node2, 2)
    c5 = c4._decrement(node2, 7)
    c6 = c5._decrement(node2)

    assert c6.decrements.state[node1] == 7
    assert c6.decrements.state[node2] == 10


def test_be_able_to_increment_and_decrement_each_nodes_record_by_arbitrary_delta():
    c1 = PNCounter()

    c2 = c1._increment(node1, 3)
    c3 = c2._decrement(node1, 2)

    c4 = c3._increment(node2, 5)
    c5 = c4._decrement(node2, 2)
    c6 = c5._increment(node2)

    assert c6.increments.value == 9
    assert c6.decrements.value == 4


def test_be_able_to_summarize_the_history_to_the_correct_aggregated_value():
    c1 = PNCounter()

    c2 = c1._increment(node1, 3)
    c3 = c2._decrement(node1, 2)

    c4 = c3._increment(node2, 5)
    c5 = c4._decrement(node2, 2)
    c6 = c5._increment(node2)

    assert c6.increments.value == 9
    assert c6.decrements.value == 4

    assert c6.value == 5


def test_be_able_to_have_its_history_correctly_merged_with_another_gcounter():
    # counter 1
    c11 = PNCounter()
    c12 = c11._increment(node1, 3)
    c13 = c12._decrement(node1, 2)
    c14 = c13._increment(node2, 5)
    c15 = c14._decrement(node2, 2)
    c16 = c15._increment(node2)

    assert c16.increments.value == 9
    assert c16.decrements.value == 4
    assert c16.value == 5

    # counter 1
    c21 = PNCounter()
    c22 = c21._increment(node1, 2)
    c23 = c22._decrement(node1, 3)
    c24 = c23._increment(node2, 3)
    c25 = c24._decrement(node2, 2)
    c26 = c25._increment(node2)

    assert c26.increments.value == 6
    assert c26.decrements.value == 5
    assert c26.value == 1

    # merge both ways
    merged1 = c16.merge(c26)
    assert merged1.increments.value == 9
    assert merged1.decrements.value == 5
    assert merged1.value == 4

    merged2 = c26.merge(c16)
    assert merged2.increments.value == 9
    assert merged2.decrements.value == 5
    assert merged2.value == 4


def test_have_support_for_pruning():
    c1 = PNCounter()
    c2 = c1._increment(node1)
    c3 = c2._decrement(node2)
    assert c2.modified_by_nodes == {node1}
    assert c2.need_pruning_from(node1)
    assert not c2.need_pruning_from(node2)
    assert c3.modified_by_nodes == {node1, node2}
    assert c3.need_pruning_from(node1)
    assert c3.need_pruning_from(node2)

    c4 = c3.prune(node1, node2)
    assert c4.modified_by_nodes == {node2}
    assert c4.need_pruning_from(node2)
    assert not c4.need_pruning_from(node1)

    c5 = c4._increment(node1).pruning_cleanup(node1)
    assert c5.modified_by_nodes == {node2}
    assert not c5.need_pruning_from(node1)


def test_have_unapply_extractor():
    # Pekko matches on Changed(PNCounterKey("key")); ohra has the value and the key's identity.
    c1 = PNCounter.empty._increment(node1)._increment(node1)._decrement(node2)
    assert c1.value == 1
    assert PNCounterKey("key") == PNCounterKey("key")
