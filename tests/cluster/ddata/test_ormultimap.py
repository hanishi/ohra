"""Ported from Apache Pekko's ORMultiMapSpec.

The "unapply extractor" case checks ``case ORMultiMap(entries)`` and key equality only: there is
no ``Replicator.Changed`` yet. Some merges are repeated with the other side's state or delta
after a round trip through the wire (``rt``), which must not change the outcome."""

import json

from ohra.cluster.ddata import orset
from ohra.cluster.ddata.ormap import DeltaGroup, PutDeltaOp, RemoveKeyDeltaOp, UpdateDeltaOp
from ohra.cluster.ddata.ormultimap import ORMultiMap, ORMultiMapKey
from ohra.cluster.ddata.orset import ORSet
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")


def _never(*_a: object) -> object:
    raise AssertionError("resolver called")


def rt[T](v: T) -> T:
    wire = json.loads(json.dumps(Serialization.encode(v, lambda r: r.path)))
    return Serialization.decode(wire, _never)  # type: ignore[return-value]


def delta(m: ORMultiMap) -> object:
    # Pekko's m.delta.get
    assert m.delta is not None
    return m.delta


def s(*elements: object) -> frozenset[object]:
    # Pekko's Set(...)
    return frozenset(elements)


# A ORMultiMap


def test_be_able_to_add_entries():
    m = ORMultiMap()._add_binding(node1, "a", "A")._add_binding(node1, "b", "B")
    assert m.entries == {"a": s("A"), "b": s("B")}

    m2 = m._add_binding(node1, "a", "C")
    assert m2.entries == {"a": s("A", "C"), "b": s("B")}


def test_be_able_to_remove_entry():
    m = (
        ORMultiMap()
        ._add_binding(node1, "a", "A")
        ._add_binding(node1, "b", "B")
        ._remove_binding(node1, "a", "A")
    )
    assert m.entries == {"b": s("B")}


def test_be_able_to_replace_an_entry():
    m = ORMultiMap()._add_binding(node1, "a", "A")._replace_binding(node1, "a", "A", "B")
    assert m.entries == {"a": s("B")}


def test_not_handle_concurrent_updates_to_the_same_set():
    m = ORMultiMap()._add_binding(node1, "a", "A")

    m1 = m._remove_binding(node1, "a", "A")

    m2 = m._add_binding(node2, "a", "B")

    merged1 = m1.merge(m2)
    merged2 = m2.merge(m1)

    # more to document that the concurrent removal from the set may be lost
    # than asserting anything
    assert merged1.entries == {"a": s("A", "B")}
    assert merged2.entries == {"a": s("A", "B")}


def test_be_able_to_have_its_entries_correctly_merged_with_another_ormultimap_with_other_entries():  # noqa: E501
    m1 = ORMultiMap()._add_binding(node1, "a", "A")._add_binding(node1, "b", "B")
    m2 = ORMultiMap()._add_binding(node2, "c", "C")

    # merge both ways

    expected_merge = {"a": s("A"), "b": s("B"), "c": s("C")}

    merged1 = m1.merge(m2)
    assert merged1.entries == expected_merge

    merged2 = m2.merge(m1)
    assert merged2.entries == expected_merge


def test_be_able_to_have_its_entries_correctly_merged_with_another_ormultimap_with_overlapping_entries():  # noqa: E501
    m1 = (
        ORMultiMap()
        ._add_binding(node1, "a", "A1")
        ._add_binding(node1, "b", "B1")
        ._remove_binding(node1, "a", "A1")
        ._add_binding(node1, "d", "D1")
    )
    m2 = (
        ORMultiMap()
        ._add_binding(node2, "c", "C2")
        ._add_binding(node2, "a", "A2")
        ._add_binding(node2, "b", "B2")
        ._remove_binding(node2, "b", "B2")
        ._add_binding(node2, "d", "D2")
    )

    # merge both ways

    expected_merged = {"a": s("A2"), "b": s("B1"), "c": s("C2"), "d": s("D1", "D2")}

    merged1 = m1.merge(m2)
    assert merged1.entries == expected_merged

    merged2 = m2.merge(m1)
    assert merged2.entries == expected_merged

    merged3 = m1.merge_delta(delta(m2))
    assert merged3.entries == expected_merged

    merged4 = m2.merge_delta(delta(m1))
    assert merged4.entries == expected_merged

    assert m1.merge(rt(m2)) == merged1
    assert m1.merge_delta(rt(delta(m2))) == merged3
    assert m2.merge_delta(rt(delta(m1))) == merged4


def test_be_able_to_have_its_entries_correctly_merged_with_another_ormultimap_with_overlapping_entries_2():  # noqa: E501
    m1 = ORMultiMap()._add_binding(node1, "b", "B1")
    m2 = ORMultiMap()._add_binding(node2, "b", "B2")._remove(node2, "b")

    # merge both ways

    expected_merged = {"b": s("B1")}

    merged1 = m1.merge(m2)
    assert merged1.entries == expected_merged

    merged2 = m2.merge(m1)
    assert merged2.entries == expected_merged

    merged3 = m1.merge_delta(delta(m2))
    assert merged3.entries == expected_merged

    merged4 = m2.merge_delta(delta(m1))
    assert merged4.entries == expected_merged

    assert m1.merge_delta(rt(delta(m2))) == merged3
    assert m2.merge_delta(rt(delta(m1))) == merged4


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas():
    m2a = ORMultiMap.empty._add_binding(node1, "q", "Q")._remove_binding(node1, "q", "Q")
    m1 = (
        ORMultiMap.empty._add_binding(node1, "z", "Z")
        ._add_binding(node2, "x", "X")
        ._remove_binding(node1, "z", "Z")
    )

    m2 = m2a.reset_delta()._remove_binding(node2, "a", "A")

    merged1 = m1.merge(m2)

    assert merged1.contains("a") is False

    merged2 = m1.merge_delta(delta(m2))

    assert merged2.contains("a") is False
    assert m1.merge_delta(rt(delta(m2))).contains("a") is False


def test_be_able_to_get_all_bindings_for_an_entry_and_then_reduce_them_upon_putting_them_back():
    m = (
        ORMultiMap()
        ._add_binding(node1, "a", "A1")
        ._add_binding(node1, "a", "A2")
        ._add_binding(node1, "b", "B1")
    )
    a = m.get("a")
    assert a is not None

    assert a == s("A1", "A2")

    m2 = m._put(node1, "a", a - {"A1"})

    expected_merged = {"a": s("A2"), "b": s("B1")}

    assert m2.entries == expected_merged


def test_return_the_value_for_an_existing_key_and_the_default_for_a_non_existing_one_when_using_get_or_else():  # noqa: E501
    m = ORMultiMap()._add_binding(node1, "a", "A")
    assert m.get_or_else("a", s("B")) == s("A")
    assert m.get_or_else("b", s("B")) == s("B")


def test_remove_all_bindings_for_a_given_key():
    m = (
        ORMultiMap()
        ._add_binding(node1, "a", "A1")
        ._add_binding(node1, "a", "A2")
        ._add_binding(node1, "b", "B1")
    )
    m2 = m._remove(node1, "a")
    assert m2.entries == {"b": s("B1")}


def test_not_have_usual_anomalies_for_remove_add_binding_scenario_and_delta_deltas():
    m1 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m2 = ORMultiMap.empty_with_value_deltas._put(node2, "c", s("C"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    m4 = m3.reset_delta()._add_binding(node1, "b", "B2")

    merged2 = m3.merge(m4)

    assert merged2.entries["a"] == s("A")
    assert merged2.entries["b"] == s("B2")
    assert merged2.entries["c"] == s("C")

    merged3 = m3.merge_delta(delta(m4))

    assert merged3.entries["a"] == s("A")
    assert merged3.entries["b"] == s("B2")
    assert merged3.entries["c"] == s("C")

    merged4 = merged1.merge(m3).merge(m4)

    assert merged4.entries["a"] == s("A")
    assert merged4.entries["b"] == s("B2")
    assert merged4.entries["c"] == s("C")

    merged5 = merged1.merge_delta(delta(m3)).merge_delta(delta(m4))

    assert merged5.entries["a"] == s("A")
    assert merged5.entries["b"] == s("B2")
    assert merged5.entries["c"] == s("C")

    merged6 = merged1.merge_delta(delta(m3).merge(delta(m4)))

    assert merged6.entries["a"] == s("A")
    assert merged6.entries["b"] == s("B2")
    assert merged6.entries["c"] == s("C")

    assert m3.merge_delta(rt(delta(m4))) == merged3
    assert merged1.merge(rt(m3)).merge(rt(m4)) == merged4
    assert merged1.merge_delta(rt(delta(m3))).merge_delta(rt(delta(m4))) == merged5
    assert merged1.merge_delta(rt(delta(m3).merge(delta(m4)))) == merged6


def test_not_have_usual_anomalies_for_remove_add_binding_scenario_and_delta_deltas_2():
    # the new delta-delta ORMultiMap is free from this anomaly
    m1 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m2 = ORMultiMap.empty_with_value_deltas._put(node2, "c", s("C"))

    # m1 - node1 gets the update from m2
    merged1 = m1.merge(m2)
    # m2 - node2 gets the update from m1
    merged2 = m2.merge(m1)

    # no race condition
    m3 = merged1.reset_delta()._remove(node1, "b")
    # let's imagine that m3 (node1) update gets propagated here (full state or delta - doesn't
    # matter) and is in flight, but in the meantime, an element is being added somewhere else
    # (m4 - node2) and the update is propagated before the update from node1 is merged
    m4 = merged2.reset_delta()._add_binding(node2, "b", "B2")
    # and later merged on node1
    merged3 = m3.merge(m4)
    # and the other way round...
    merged4 = m4.merge(m3)

    # result -  the element "B" is kept on both sides...
    assert merged3.entries["a"] == s("A")
    assert merged3.entries["b"] == s("B2")
    assert merged3.entries["c"] == s("C")

    assert merged4.entries["a"] == s("A")
    assert merged4.entries["b"] == s("B2")
    assert merged4.entries["c"] == s("C")

    # full state or delta - doesn't matter, nor does the wire
    assert m3.merge_delta(delta(m4)).entries == merged3.entries
    assert m4.merge_delta(delta(m3)).entries == merged4.entries
    assert m3.merge(rt(m4)) == merged3
    assert m4.merge(rt(m3)) == merged4
    assert m3.merge_delta(rt(delta(m4))).entries == merged3.entries
    assert m4.merge_delta(rt(delta(m3))).entries == merged4.entries

    # but if the timing was slightly different, so that the update from node1
    # would get merged just before update on node2:
    merged5 = m2.merge(m3).reset_delta()._add_binding(node2, "b", "B2")
    # the update propagated ... and merged on node1:
    merged6 = m3.merge(merged5)

    # then the outcome would be the same...
    assert merged5.entries["a"] == s("A")
    assert merged5.entries["b"] == s("B2")
    assert merged5.entries["c"] == s("C")

    assert merged6.entries["a"] == s("A")
    assert merged6.entries["b"] == s("B2")
    assert merged6.entries["c"] == s("C")

    assert m3.merge_delta(delta(merged5)).entries == merged6.entries
    assert m3.merge(rt(merged5)) == merged6


def test_work_with_delta_coalescing_scenario_1():
    m1 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m2 = m1.reset_delta()._put(node2, "b", s("B2"))._add_binding(node2, "b", "B3")

    merged1 = m1.merge(m2)

    assert merged1.entries["a"] == s("A")
    assert merged1.entries["b"] == s("B2", "B3")

    merged2 = m1.merge_delta(delta(m2))

    assert merged2.entries["a"] == s("A")
    assert merged2.entries["b"] == s("B2", "B3")
    assert m1.merge_delta(rt(delta(m2))) == merged2

    m3 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m4 = m3.reset_delta()._put(node2, "b", s("B2"))._put(node2, "b", s("B3"))

    merged3 = m3.merge(m4)

    assert merged3.entries["a"] == s("A")
    assert merged3.entries["b"] == s("B3")

    merged4 = m3.merge_delta(delta(m4))

    assert merged4.entries["a"] == s("A")
    assert merged4.entries["b"] == s("B3")
    assert m3.merge_delta(rt(delta(m4))) == merged4

    m5 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m6 = (
        m5.reset_delta()
        ._put(node2, "b", s("B2"))
        ._add_binding(node2, "b", "B3")
        ._add_binding(node2, "b", "B4")
    )

    merged5 = m5.merge(m6)

    assert merged5.entries["a"] == s("A")
    assert merged5.entries["b"] == s("B2", "B3", "B4")

    merged6 = m5.merge_delta(delta(m6))

    assert merged6.entries["a"] == s("A")
    assert merged6.entries["b"] == s("B2", "B3", "B4")
    assert m5.merge_delta(rt(delta(m6))) == merged6

    m7 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m8 = (
        m7.reset_delta()
        ._put(node2, "d", s("D"))
        ._add_binding(node2, "b", "B3")
        ._put(node2, "b", s("B4"))
    )

    merged7 = m7.merge(m8)

    assert merged7.entries["a"] == s("A")
    assert merged7.entries["b"] == s("B4")
    assert merged7.entries["d"] == s("D")

    merged8 = m7.merge_delta(delta(m8))

    assert merged8.entries["a"] == s("A")
    assert merged8.entries["b"] == s("B4")
    assert merged8.entries["d"] == s("D")
    assert m7.merge_delta(rt(delta(m8))) == merged8

    m9 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m10 = m9.reset_delta()._add_binding(node2, "b", "B3")._add_binding(node2, "b", "B4")

    merged9 = m9.merge(m10)

    assert merged9.entries["a"] == s("A")
    assert merged9.entries["b"] == s("B", "B3", "B4")

    merged10 = m9.merge_delta(delta(m10))

    assert merged10.entries["a"] == s("A")
    assert merged10.entries["b"] == s("B", "B3", "B4")
    assert m9.merge_delta(rt(delta(m10))) == merged10

    m11 = (
        ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))
        ._put(node1, "b", s("B", "B1"))
        ._remove(node1, "b")
    )
    m12 = m11.reset_delta()._add_binding(node2, "b", "B2")._add_binding(node2, "b", "B3")

    merged11 = m11.merge(m12)

    assert merged11.entries["a"] == s("A")
    assert merged11.entries["b"] == s("B2", "B3")

    merged12 = m11.merge_delta(delta(m12))

    assert merged12.entries["a"] == s("A")
    assert merged12.entries["b"] == s("B2", "B3")
    assert m11.merge_delta(rt(delta(m12))) == merged12

    m13 = (
        ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))
        ._put(node1, "b", s("B", "B1"))
        ._remove(node1, "b")
    )
    m14 = m13.reset_delta()._add_binding(node2, "b", "B2")._put(node2, "b", s("B3"))

    merged13 = m13.merge(m14)

    assert merged13.entries["a"] == s("A")
    assert merged13.entries["b"] == s("B3")

    merged14 = m13.merge_delta(delta(m14))

    assert merged14.entries["a"] == s("A")
    assert merged14.entries["b"] == s("B3")
    assert m13.merge_delta(rt(delta(m14))) == merged14

    m15 = (
        ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))
        ._put(node1, "b", s("B", "B1"))
        ._put(node1, "c", s("C"))
    )
    m16 = m15.reset_delta()._add_binding(node2, "b", "B2")._add_binding(node2, "c", "C1")

    merged15 = m15.merge(m16)

    assert merged15.entries["a"] == s("A")
    assert merged15.entries["b"] == s("B", "B1", "B2")
    assert merged15.entries["c"] == s("C", "C1")

    merged16 = m15.merge_delta(delta(m16))

    assert merged16.entries["a"] == s("A")
    assert merged16.entries["b"] == s("B", "B1", "B2")
    assert merged16.entries["c"] == s("C", "C1")
    assert m15.merge_delta(rt(delta(m16))) == merged16

    # somewhat artificial setup
    m17 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))._put(
        node1, "b", s("B", "B1")
    )
    m18 = m17.reset_delta()._add_binding(node2, "b", "B2")
    m19 = ORMultiMap.empty_with_value_deltas.reset_delta()._put(node2, "b", s("B3"))

    merged17 = m17.merge(m18).merge(m19)

    assert merged17.entries["a"] == s("A")
    assert merged17.entries["b"] == s("B", "B1", "B3")

    merged18 = m17.merge_delta(delta(m18).merge(delta(m19)))

    assert merged18.entries["a"] == s("A")
    assert merged18.entries["b"] == s("B", "B1", "B3")
    assert m17.merge_delta(rt(delta(m18).merge(delta(m19)))) == merged18


def test_work_with_delta_coalescing_scenario_2():
    m1 = ORMultiMap.empty._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m2 = m1.reset_delta()._put(node2, "b", s("B2"))._add_binding(node2, "b", "B3")

    merged1 = m1.merge(m2)

    assert merged1.entries["a"] == s("A")
    assert merged1.entries["b"] == s("B2", "B3")

    merged2 = m1.merge_delta(delta(m2))

    assert merged2.entries["a"] == s("A")
    assert merged2.entries["b"] == s("B2", "B3")
    assert m1.merge_delta(rt(delta(m2))) == merged2

    m3 = ORMultiMap.empty._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m4 = m3.reset_delta()._put(node2, "b", s("B2"))._put(node2, "b", s("B3"))

    merged3 = m3.merge(m4)

    assert merged3.entries["a"] == s("A")
    assert merged3.entries["b"] == s("B3")

    merged4 = m3.merge_delta(delta(m4))

    assert merged4.entries["a"] == s("A")
    assert merged4.entries["b"] == s("B3")
    assert m3.merge_delta(rt(delta(m4))) == merged4

    m5 = ORMultiMap.empty._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m6 = (
        m5.reset_delta()
        ._put(node2, "b", s("B2"))
        ._add_binding(node2, "b", "B3")
        ._add_binding(node2, "b", "B4")
    )

    merged5 = m5.merge(m6)

    assert merged5.entries["a"] == s("A")
    assert merged5.entries["b"] == s("B2", "B3", "B4")

    merged6 = m5.merge_delta(delta(m6))

    assert merged6.entries["a"] == s("A")
    assert merged6.entries["b"] == s("B2", "B3", "B4")
    assert m5.merge_delta(rt(delta(m6))) == merged6

    m7 = ORMultiMap.empty._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m8 = (
        m7.reset_delta()
        ._put(node2, "d", s("D"))
        ._add_binding(node2, "b", "B3")
        ._put(node2, "b", s("B4"))
    )

    merged7 = m7.merge(m8)

    assert merged7.entries["a"] == s("A")
    assert merged7.entries["b"] == s("B4")
    assert merged7.entries["d"] == s("D")

    merged8 = m7.merge_delta(delta(m8))

    assert merged8.entries["a"] == s("A")
    assert merged8.entries["b"] == s("B4")
    assert merged8.entries["d"] == s("D")
    assert m7.merge_delta(rt(delta(m8))) == merged8

    m9 = ORMultiMap.empty._put(node1, "a", s("A"))._put(node1, "b", s("B"))
    m10 = m9.reset_delta()._add_binding(node2, "b", "B3")._add_binding(node2, "b", "B4")

    merged9 = m9.merge(m10)

    assert merged9.entries["a"] == s("A")
    assert merged9.entries["b"] == s("B", "B3", "B4")

    merged10 = m9.merge_delta(delta(m10))

    assert merged10.entries["a"] == s("A")
    assert merged10.entries["b"] == s("B", "B3", "B4")
    assert m9.merge_delta(rt(delta(m10))) == merged10

    m11 = (
        ORMultiMap.empty._put(node1, "a", s("A"))
        ._put(node1, "b", s("B", "B1"))
        ._remove(node1, "b")
    )
    m12 = ORMultiMap.empty._add_binding(node2, "b", "B2")._add_binding(node2, "b", "B3")

    merged11 = m11.merge(m12)

    assert merged11.entries["a"] == s("A")
    assert merged11.entries["b"] == s("B2", "B3")

    merged12 = m11.merge_delta(delta(m12))

    assert merged12.entries["a"] == s("A")
    assert merged12.entries["b"] == s("B2", "B3")
    assert m11.merge_delta(rt(delta(m12))) == merged12


def test_work_with_tombstones_for_ormultimap_with_value_deltas_and_its_delta_delta_operations():
    # ORMultiMap.withValueDeltas has the following (public) interface:
    # put - place (or replace) a value in a destructive way - no tombstone is created
    #       this can be seen in the relevant delta:
    #       PutDeltaOp(AddDeltaOp(ORSet(a)),(a,ORSet()),ORMultiMapWithValueDeltasTag)
    # remove - to avoid anomalies that ORMultiMap has, value for the key being removed is being
    #          cleared before key removal, this can be seen in the following deltas created by
    #          the remove op (depending on situation):
    #          DeltaGroup(Vector(PutDeltaOp(AddDeltaOp(ORSet(a)),(a,ORSet()),
    #            ORMultiMapWithValueDeltasTag), RemoveKeyDeltaOp(RemoveDeltaOp(ORSet(a)),a,
    #            ORMultiMapWithValueDeltasTag)))
    #          DeltaGroup(Vector(UpdateDeltaOp(AddDeltaOp(ORSet(c)),Map(c ->
    #            FullStateDeltaOp(ORSet())),ORMultiMapWithValueDeltasTag),
    #            RemoveKeyDeltaOp(RemoveDeltaOp(ORSet(c)),c,ORMultiMapWithValueDeltasTag)))
    #          after applying the remove operation the tombstone for the given map looks as
    #          follows: Map(a -> ORSet()) (or Map(c -> ORSet()) )

    m1 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))
    m2 = m1.reset_delta()._remove(node1, "a")
    # ohra's own: the deltas of remove that the comment above shows
    match delta(m2):
        case DeltaGroup(
            ops=(
                UpdateDeltaOp(values={"a": orset.FullStateDeltaOp()}),
                RemoveKeyDeltaOp(removed_key="a"),
            )
        ):
            pass
        case d:
            raise AssertionError(d)
    match delta(ORMultiMap.empty_with_value_deltas._remove(node1, "a")):
        case DeltaGroup(ops=(PutDeltaOp(value=("a", ORSet() as v)), RemoveKeyDeltaOp())) if (
            v.is_empty
        ):
            pass
        case d:
            raise AssertionError(d)

    m3 = m1.merge_delta(delta(m2))
    m4 = m1.merge(m2)

    # tombstone for 'a' has been optimized away at the end of the mergeDelta
    assert "a" not in m3.underlying.values
    # tombstone for 'a' has been optimized away at the end of the merge
    assert "a" not in m4.underlying.values
    assert "a" not in m1.merge_delta(rt(delta(m2))).underlying.values
    assert "a" not in m1.merge(rt(m2)).underlying.values

    m5 = ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A1"))
    assert m3.merge_delta(delta(m5)).entries["a"] == s("A1")
    assert m4.merge_delta(delta(m5)).entries["a"] == s("A1")
    assert m4.merge(m5).entries["a"] == s("A1")

    # addBinding - add a binding for a certain value - no tombstone is created
    #              this operation works through "updated" call of the underlying ORMap, that is
    #              not exposed in the ORMultiMap interface
    #              the side-effect of addBinding is that it can lead to anomalies with the
    #              standard "ORMultiMap"

    # removeBinding - remove binding for a certain value, and if there are no more remaining
    #                 elements, remove the now superfluous key, please note that for
    #                 .withValueDeltas variant tombstone will be created

    um1 = ORMultiMap.empty_with_value_deltas._add_binding(node1, "a", "A")
    um2 = um1.reset_delta()._remove_binding(node1, "a", "A")
    assert um2.underlying.values["a"].is_empty  # ohra's own: the tombstone the comment mentions

    um3 = um1.merge_delta(delta(um2))
    um4 = um1.merge(um2)

    # tombstone for 'a' has been optimized away at the end of the mergeDelta
    assert "a" not in um3.underlying.values
    # tombstone for 'a' has been optimized away at the end of the merge
    assert "a" not in um4.underlying.values
    assert "a" not in um1.merge_delta(rt(delta(um2))).underlying.values

    um5 = ORMultiMap.empty_with_value_deltas._add_binding(node1, "a", "A1")
    assert um3.merge_delta(delta(um5)).entries["a"] == s("A1")
    assert um4.merge_delta(delta(um5)).entries["a"] == s("A1")
    assert um4.merge(um5).entries["a"] == s("A1")

    # replaceBinding - that would first addBinding for new binding and then removeBinding for old
    #                  binding so no tombstone would be created

    # so the only option to create a tombstone with non-zero (!= Set() ) contents would be to
    # call removeKey (not remove!) for the underlying ORMap (or have a removeKeyOp delta that
    # does exactly that) but this is not possible in applications, as both remove and removeKey
    # operations are API of internal ORMap and are not externally exposed in the ORMultiMap, and
    # deltas are causal, so removeKeyOp delta cannot arise without previous delta containing
    # 'clear' or 'put' operation setting the tombstone at Set()
    # the example shown below cannot happen in practice

    tm1 = ORMultiMap(
        ORMultiMap.empty_with_value_deltas._add_binding(node1, "a", "A").underlying._remove_key(
            node1, "a"
        ),
        True,
    )
    assert tm1.underlying.values["a"].elements == s("A")  # tombstone
    assert tm1._add_binding(node1, "a", "A1").entries["a"] == s("A", "A1")
    tm2 = (
        ORMultiMap.empty_with_value_deltas._put(node1, "a", s("A"))
        .reset_delta()
        ._add_binding(node1, "a", "A1")
    )
    assert tm1.merge_delta(delta(tm2)).entries["a"] == s("A", "A1")
    assert tm1.merge(tm2).entries["a"] == s("A", "A1")
    assert tm1.merge_delta(rt(delta(tm2))).entries["a"] == s("A", "A1")
    # ohra's own: a tombstone survives a merge of either kind with a side that still holds it,
    # the reason for ORMap's merges retaining deleted values
    assert tm1.merge(tm1).underlying.values["a"].elements == s("A")
    tb = ORMultiMap.empty_with_value_deltas._add_binding(node2, "b", "B")
    assert tm1.merge_delta(delta(tb)).underlying.values["a"].elements == s("A")
    tm3 = ORMultiMap(
        ORMultiMap.empty_with_value_deltas._add_binding(node1, "a", "A").underlying._remove(
            node1, "a"
        ),
        True,
    )
    assert tm3.underlying.contains("a") is False  # no tombstone, because remove not removeKey
    # no tombstone - update delta could not be applied
    assert tm3.merge_delta(delta(tm2)).entries == {}
    assert tm3.merge(tm2).entries == {}

    # The only valid value for tombstone created by means of either API call or application of
    # delta propagation is Set() which is then garbage collected at every `merge` and
    # `mergeDelta` operation. Hence in the case of valid API usage and normal operation of delta
    # propagation no tombstones will be permanently created.


def test_have_unapply_extractor():
    m1 = ORMultiMap.empty._put(node1, "a", s(1, 2))._put(node2, "b", s(3))
    match m1:
        case ORMultiMap(entries1):
            assert entries1 == {"a": s(1, 2), "b": s(3)}
        case _:
            raise AssertionError
    assert ORMultiMapKey("key") == ORMultiMapKey("key")
    assert ORMultiMapKey("key").with_id("k2") == ORMultiMapKey("k2")
