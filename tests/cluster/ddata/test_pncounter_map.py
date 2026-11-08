"""Ported from Apache Pekko's PNCounterMapSpec.

The "unapply extractor" case checks ``case PNCounterMap(entries)`` and key equality only: there
is no ``Replicator.Changed`` yet. ``incrementBy`` and ``decrementBy`` are ``increment`` and
``decrement`` with a ``SelfUniqueAddress``."""

from ohra.cluster.ddata import SelfUniqueAddress
from ohra.cluster.ddata.pncounter_map import PNCounterMap, PNCounterMapKey
from ohra.remote import UniqueAddress

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")


def test_be_able_to_increment_and_decrement_entries_with_implicit_selfuniqueaddress():
    node = SelfUniqueAddress(node1)
    m = (
        PNCounterMap()
        .increment(node, "a", 2)
        .increment(node, "b", 1)
        .increment(node, "b", 2)
        .decrement(node, "a", 1)
    )
    assert m.entries == {"a": 1, "b": 3}


def test_be_able_to_increment_and_decrement_entries():
    m = (
        PNCounterMap()
        ._increment(node1, "a", 2)
        ._increment(node1, "b", 3)
        ._decrement(node2, "a", 1)
    )
    assert m.entries == {"a": 1, "b": 3}
    assert m.get("a") == 1
    assert m.get("z") is None


def test_be_able_to_have_its_entries_correctly_merged_with_another_ormap_with_other_entries():
    m1 = (
        PNCounterMap()
        ._increment(node1, "a", 1)
        ._increment(node1, "b", 3)
        ._increment(node1, "c", 2)
    )
    m2 = PNCounterMap()._increment(node2, "c", 5)

    # merge both ways
    expected = {"a": 1, "b": 3, "c": 7}
    assert m1.merge(m2).entries == expected
    assert m2.merge(m1).entries == expected


def test_be_able_to_remove_entry():
    m1 = (
        PNCounterMap()
        ._increment(node1, "a", 1)
        ._increment(node1, "b", 3)
        ._increment(node1, "c", 2)
    )
    m2 = PNCounterMap()._increment(node2, "c", 5)

    merged1 = m1.merge(m2)

    m3 = merged1._remove(node1, "b")
    assert merged1.merge(m3).entries == {"a": 1, "c": 7}

    # but if there is a conflicting update the entry is not removed
    m4 = merged1._increment(node2, "b", 10)
    assert m3.merge(m4).entries == {"a": 1, "b": 13, "c": 7}


def test_be_able_to_work_with_deltas():
    m1 = (
        PNCounterMap()
        ._increment(node1, "a", 1)
        ._increment(node1, "b", 3)
        ._increment(node1, "c", 2)
    )
    m2 = PNCounterMap()._increment(node2, "c", 5)

    expected = {"a": 1, "b": 3, "c": 7}
    assert PNCounterMap().merge_delta(m1.delta).merge_delta(m2.delta).entries == expected
    assert PNCounterMap().merge_delta(m2.delta).merge_delta(m1.delta).entries == expected

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    assert merged1.merge_delta(m3.delta).entries == {"a": 1, "c": 7}

    # but if there is a conflicting update the entry is not removed
    m4 = merged1.reset_delta()._increment(node2, "b", 10)
    assert m3.merge_delta(m4.delta).entries == {"a": 1, "b": 13, "c": 7}


def test_have_unapply_extractor():
    m1 = PNCounterMap.empty._increment(node1, "a", 1)._increment(node2, "b", 2)
    match m1:
        case PNCounterMap(entries1):
            assert entries1 == {"a": 1, "b": 2}
        case _:
            raise AssertionError
    assert PNCounterMapKey("key") == PNCounterMapKey("key")
    assert PNCounterMapKey("key").with_id("k2") == PNCounterMapKey("k2")
