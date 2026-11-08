"""Ported from Apache Pekko's LWWMapSpec.

The "unapply extractor" case checks ``case LWWMap(entries)`` and key equality only: there is no
``Replicator.Changed`` yet."""

from ohra.cluster.ddata.lww_map import LWWMap, LWWMapKey
from ohra.cluster.ddata.lww_register import default_clock
from ohra.remote import UniqueAddress

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")


def test_be_able_to_set_entries():
    m = LWWMap.empty._put(node1, "a", 1, default_clock)._put(node2, "b", 2, default_clock)
    assert m.entries == {"a": 1, "b": 2}


def test_be_able_to_have_its_entries_correctly_merged_with_another_lwwmap_with_other_entries():
    m1 = LWWMap.empty._put(node1, "a", 1, default_clock)._put(node1, "b", 2, default_clock)
    m2 = LWWMap.empty._put(node2, "c", 3, default_clock)

    # merge both ways
    expected = {"a": 1, "b": 2, "c": 3}
    assert m1.merge(m2).entries == expected
    assert m2.merge(m1).entries == expected


def test_be_able_to_remove_entry():
    m1 = LWWMap.empty._put(node1, "a", 1, default_clock)._put(node1, "b", 2, default_clock)
    m2 = LWWMap.empty._put(node2, "c", 3, default_clock)

    merged1 = m1.merge(m2)

    m3 = merged1._remove(node1, "b")
    assert merged1.merge(m3).entries == {"a": 1, "c": 3}

    # but if there is a conflicting update the entry is not removed
    m4 = merged1._put(node2, "b", 22, default_clock)
    assert m3.merge(m4).entries == {"a": 1, "b": 22, "c": 3}


def test_be_able_to_work_with_deltas():
    m1 = LWWMap.empty._put(node1, "a", 1, default_clock)._put(node1, "b", 2, default_clock)
    m2 = LWWMap.empty._put(node2, "c", 3, default_clock)

    expected = {"a": 1, "b": 2, "c": 3}
    assert m1.merge(m2).entries == expected
    assert m2.merge(m1).entries == expected

    assert LWWMap.empty.merge_delta(m1.delta).merge_delta(m2.delta).entries == expected
    assert LWWMap.empty.merge_delta(m2.delta).merge_delta(m1.delta).entries == expected

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    assert merged1.merge_delta(m3.delta).entries == {"a": 1, "c": 3}

    # but if there is a conflicting update the entry is not removed
    m4 = merged1.reset_delta()._put(node2, "b", 22, default_clock)
    assert m3.merge_delta(m4.delta).entries == {"a": 1, "b": 22, "c": 3}


def test_have_unapply_extractor():
    m1 = LWWMap.empty._put(node1, "a", 1, default_clock)
    match m1:
        case LWWMap(entries1):
            assert entries1 == {"a": 1}
        case _:
            raise AssertionError
    assert LWWMapKey("key") == LWWMapKey("key")
    assert LWWMapKey("key").with_id("k2") == LWWMapKey("k2")
