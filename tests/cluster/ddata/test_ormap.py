"""Ported from Apache Pekko's ORMapSpec.

The ``test_prune_...`` and ``test_pruning_cleanup_...`` tests are ohra's own: ORMapSpec has no
case for the pruning methods."""

import json

import pytest

from ohra.cluster.ddata import orset
from ohra.cluster.ddata.flag import Flag
from ohra.cluster.ddata.gcounter import GCounter
from ohra.cluster.ddata.gset import GSet
from ohra.cluster.ddata.ormap import DeltaGroup, ORMap, ORMapKey, PutDeltaOp
from ohra.cluster.ddata.orset import ORSet
from ohra.cluster.ddata.pncounter import PNCounter
from ohra.cluster.ddata.replicated_data import ReplicatedData
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")


def _never(*_a: object) -> object:
    raise AssertionError("resolver called")


def rt[T](v: T) -> T:
    wire = json.loads(json.dumps(Serialization.encode(v, lambda r: r.path)))
    return Serialization.decode(wire, _never)  # type: ignore[return-value]


def delta(m: ORMap) -> object:
    # Pekko's m.delta.get
    assert m.delta is not None
    return m.delta


# A ORMap


def test_be_able_to_add_entries():
    m = ORMap()._put(node1, "a", GSet().add("A"))._put(node1, "b", GSet().add("B"))
    match m.entries["a"]:
        case GSet(a):
            assert a == {"A"}
        case _:
            raise AssertionError
    match m.entries["b"]:
        case GSet(b):
            assert b == {"B"}
        case _:
            raise AssertionError

    m2 = m._put(node1, "a", GSet().add("C"))
    assert m2.entries["a"].elements == {"C"}


def test_be_able_to_add_entries_with_deltas():
    m = ORMap()._put(node1, "a", GSet().add("A"))._put(node1, "b", GSet().add("B"))
    md = delta(m)

    m1 = ORMap().merge_delta(md)

    assert m1.entries["a"].elements == {"A"}
    assert m1.entries["b"].elements == {"B"}

    m2 = m1._put(node1, "a", GSet().add("C"))
    assert m2.entries["a"].elements == {"C"}


def test_be_able_to_remove_entry():
    m = ORMap()._put(node1, "a", GSet().add("A"))._put(node1, "b", GSet().add("B"))
    m = m._remove(node1, "a")
    assert "a" not in m.entries
    assert "b" in m.entries


def test_be_able_to_remove_entry_using_a_delta():
    m = ORMap()._put(node1, "a", GSet().add("A"))._put(node1, "b", GSet().add("B"))
    add_delta = delta(m)

    remove_delta = delta(m.reset_delta()._remove(node1, "a"))

    m1 = ORMap().merge_delta(add_delta)
    assert "a" in m1.entries

    m2 = m1.merge_delta(remove_delta)
    assert "a" not in m2.entries
    assert "b" in m2.entries


def test_be_able_to_add_removed():
    m = ORMap()._put(node1, "a", GSet().add("A"))._put(node1, "b", GSet().add("B"))
    m = m._remove(node1, "a")
    assert "a" not in m.entries
    assert "b" in m.entries
    m2 = m._put(node1, "a", GSet().add("C"))
    assert "a" in m2.entries
    assert "b" in m2.entries


def test_be_able_to_have_its_entries_correctly_merged_with_another_ormap_with_other_entries():
    m1 = ORMap()._put(node1, "a", GSet().add("A"))._put(node1, "b", GSet().add("B"))
    m2 = ORMap()._put(node2, "c", GSet().add("C"))

    # merge both ways
    merged1 = m1.merge(m2)
    assert {"a", "b", "c"} <= merged1.entries.keys()

    merged2 = m2.merge(m1)
    assert {"a", "b", "c"} <= merged2.entries.keys()


def test_be_able_to_have_its_entries_correctly_merged_with_another_ormap_with_overlapping_entries():  # noqa: E501
    m1 = (
        ORMap()
        ._put(node1, "a", GSet().add("A1"))
        ._put(node1, "b", GSet().add("B1"))
        ._remove(node1, "a")
        ._put(node1, "d", GSet().add("D1"))
    )
    m2 = (
        ORMap()
        ._put(node2, "c", GSet().add("C2"))
        ._put(node2, "a", GSet().add("A2"))
        ._put(node2, "b", GSet().add("B2"))
        ._remove(node2, "b")
        ._put(node2, "d", GSet().add("D2"))
    )

    # merge both ways
    merged1 = m1.merge(m2)
    assert merged1.entries["a"].elements == {"A2"}
    assert merged1.entries["b"].elements == {"B1"}
    assert "c" in merged1.entries
    assert merged1.entries["d"].elements == {"D1", "D2"}

    merged2 = m2.merge(m1)
    assert merged2.entries["a"].elements == {"A2"}
    assert merged2.entries["b"].elements == {"B1"}
    assert "c" in merged2.entries
    assert merged2.entries["d"].elements == {"D1", "D2"}


def test_illustrate_the_danger_of_using_remove_put_to_replace_an_entry():
    m1 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))._put(node1, "b", GSet.empty.add("B"))
    m2 = ORMap.empty._put(node2, "c", GSet.empty.add("C"))

    merged1 = m1.merge(m2)

    m3 = merged1._remove(node1, "b")._put(node1, "b", GSet.empty.add("B2"))
    # same thing if only put is used
    #      m3 = merged1._put(node1, "b", GSet.empty.add("B2"))
    merged2 = merged1.merge(m3)

    assert merged2.entries["a"].elements == {"A"}
    # note that B is included, because GSet("B") is merged with GSet("B2")
    assert merged2.entries["b"].elements == {"B", "B2"}
    assert merged2.entries["c"].elements == {"C"}


def test_do_not_have_divergence_in_dot_versions_between_the_underlying_map_and_ormap_delta():
    m1 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))

    match m1.delta:
        case PutDeltaOp(orset.AddDeltaOp(u), _, _):
            delta_version = (
                u.elements_map["a"].version_at(node1) if "a" in u.elements_map else None
            )
        case _:
            delta_version = None

    full_version = (
        m1.keys.elements_map["a"].version_at(node1) if "a" in m1.keys.elements_map else None
    )
    assert delta_version == full_version


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas():
    m1 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))._put(node1, "b", GSet.empty.add("B"))
    m2 = ORMap.empty._put(node2, "c", GSet.empty.add("C"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    m4 = merged1.reset_delta()._update(node1, "b", GSet.empty, lambda s: s.add("B2"))

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    # note that B is included, because GSet("B") is merged with GSet("B2")
    assert merged2.entries["b"].elements == {"B", "B2"}
    assert merged2.entries["c"].elements == {"C"}

    merged3 = m3.merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    # note that B is included, because GSet("B") is merged with GSet("B2")
    assert merged3.entries["b"].elements == {"B", "B2"}
    assert merged3.entries["c"].elements == {"C"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_2():
    m1 = ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))._put(
        node1, "b", ORSet.empty._add(node1, "B")
    )
    m2 = ORMap.empty._put(node2, "c", ORSet.empty._add(node2, "C"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    m4 = (
        merged1.reset_delta()
        ._remove(node1, "b")
        ._update(node1, "b", ORSet.empty, lambda s: s._add(node1, "B2"))
    )

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    # note that B is not included, because it was removed in both timelines
    assert merged2.entries["b"].elements == {"B2"}
    assert merged2.entries["c"].elements == {"C"}

    merged3 = m3.merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    # note that B is not included, because it was removed in both timelines
    assert merged3.entries["b"].elements == {"B2"}
    assert merged3.entries["c"].elements == {"C"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_3():
    m1 = ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))._put(
        node1, "b", ORSet.empty._add(node1, "B")
    )
    m2 = ORMap.empty._put(node2, "c", ORSet.empty._add(node2, "C"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    m4 = (
        merged1.reset_delta()
        ._remove(node2, "b")
        ._update(node2, "b", ORSet.empty, lambda s: s._add(node2, "B2"))
    )

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    # note that B is not included, because it was removed in both timelines
    assert merged2.entries["b"].elements == {"B2"}
    assert merged2.entries["c"].elements == {"C"}

    merged3 = m3.merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    # note that B is not included, because it was removed in both timelines
    assert merged3.entries["b"].elements == {"B2"}
    assert merged3.entries["c"].elements == {"C"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_4():
    m1 = ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))._put(
        node1, "b", ORSet.empty._add(node1, "B")
    )
    m2 = ORMap.empty._put(node2, "c", ORSet.empty._add(node2, "C"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    m4 = merged1.reset_delta()._update(node1, "b", ORSet.empty, lambda s: s._add(node1, "B2"))

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    # note that B is included, because ORSet("B") is merged with ORSet("B2")
    assert merged2.entries["b"].elements == {"B", "B2"}
    assert merged2.entries["c"].elements == {"C"}

    merged3 = m3.merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    # note that B is included, because ORSet("B") is merged with ORSet("B2")
    assert merged3.entries["b"].elements == {"B", "B2"}
    assert merged3.entries["c"].elements == {"C"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_5():
    m1 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))._put(node1, "b", GSet.empty.add("B"))
    m2 = ORMap.empty._put(node2, "c", GSet.empty.add("C"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    m4 = merged1.reset_delta()._put(node2, "b", GSet.empty.add("B2"))

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    # note that B is not included, because it was removed in both timelines
    assert merged2.entries["b"].elements == {"B2"}
    assert merged2.entries["c"].elements == {"C"}

    merged3 = m3.merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    # note that B is not included, because it was removed in both timelines
    assert merged3.entries["b"].elements == {"B2"}
    assert merged3.entries["c"].elements == {"C"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_6():
    m1 = ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))._put(
        node1, "b", ORSet.empty._add(node1, "B")
    )
    m2 = ORMap.empty._put(node2, "b", ORSet.empty._add(node2, "B3"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")
    m4 = (
        merged1.reset_delta()
        ._remove(node2, "b")
        ._update(node2, "b", ORSet.empty, lambda s: s._add(node2, "B1"))
        ._update(node2, "b", ORSet.empty, lambda s: s._add(node2, "B2"))
    )

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    # note that B is not included, because it was removed in both timelines
    assert merged2.entries["b"].elements == {"B1", "B2"}

    merged3 = m3.merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    # note that B is not included, because it was removed in both timelines
    assert merged3.entries["b"].elements == {"B1", "B2"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_7():
    m1 = (
        ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))
        ._put(node1, "b", ORSet.empty._add(node1, "B1"))
        ._remove(node1, "b")
    )
    m2 = ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))._put(
        node1, "b", ORSet.empty._add(node1, "B2")
    )
    m2d = m2.reset_delta()._remove(node1, "b")
    m2u = (
        m2.reset_delta()
        ._update(node1, "b", ORSet.empty, lambda s: s._add(node1, "B3"))
        ._update(node2, "b", ORSet.empty, lambda s: s._add(node2, "B4"))
    )

    merged1 = m1.merge(m2d).merge_delta(delta(m2u))

    assert merged1.entries["a"].elements == {"A"}
    # note that B1 is lost as it was added and removed earlier in timeline than B2
    assert merged1.entries["b"].elements == {"B2", "B3", "B4"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_8():
    m1 = (
        ORMap.empty._put(node1, "a", GSet.empty.add("A"))
        ._put(node1, "b", GSet.empty.add("B"))
        ._put(node2, "b", GSet.empty.add("B"))
    )
    m2 = ORMap.empty._put(node2, "c", GSet.empty.add("C"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")._remove(node2, "b")
    m4 = (
        merged1.reset_delta()
        ._put(node2, "b", GSet.empty.add("B2"))
        ._put(node2, "b", GSet.empty.add("B3"))
    )

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    assert merged2.entries["b"].elements == {"B3"}
    assert merged2.entries["c"].elements == {"C"}

    merged3 = merged1.merge_delta(delta(m3)).merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    assert merged3.entries["b"].elements == {"B3"}
    assert merged3.entries["c"].elements == {"C"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_9():
    m1 = (
        ORMap.empty._put(node1, "a", GSet.empty.add("A"))
        ._put(node1, "b", GSet.empty.add("B"))
        ._put(node2, "b", GSet.empty.add("B"))
    )
    m2 = ORMap.empty._put(node2, "c", GSet.empty.add("C"))

    merged1 = m1.merge(m2)

    m3 = merged1.reset_delta()._remove(node1, "b")._remove(node2, "b")
    m4 = (
        merged1.reset_delta()
        ._update(node2, "b", GSet.empty, lambda s: s.add("B2"))
        ._update(node2, "b", GSet.empty, lambda s: s.add("B3"))
    )

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    assert merged2.entries["b"].elements == {"B2", "B3"}
    assert merged2.entries["c"].elements == {"C"}

    merged3 = merged1.merge_delta(delta(m3)).merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    assert merged3.entries["b"].elements == {"B2", "B3"}
    assert merged3.entries["c"].elements == {"C"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_10():
    m1 = ORMap.empty._put(node2, "a", GSet.empty.add("A"))._put(node2, "b", GSet.empty.add("B"))

    m3 = m1.reset_delta()._remove(node2, "b")
    m4 = (
        m3.reset_delta()
        ._put(node2, "b", GSet.empty.add("B2"))
        ._update(node2, "b", GSet.empty, lambda s: s.add("B3"))
    )

    merged2 = m3.merge(m4)

    assert merged2.entries["a"].elements == {"A"}
    assert merged2.entries["b"].elements == {"B2", "B3"}

    merged3 = m3.merge_delta(delta(m4))

    assert merged3.entries["a"].elements == {"A"}
    assert merged3.entries["b"].elements == {"B2", "B3"}


def test_not_have_anomalies_for_remove_updated_scenario_and_deltas_11():
    m1 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))

    m2 = ORMap.empty._put(node2, "a", GSet.empty)._remove(node2, "a")

    merged1 = m1.merge(m2)

    assert merged1.entries["a"].elements == {"A"}

    merged2 = m1.merge_delta(delta(m2))

    assert merged2.entries["a"].elements == {"A"}


def test_have_the_usual_anomalies_for_remove_updated_scenario():
    # please note that the current ORMultiMap has the same anomaly
    # because the condition of keeping global vvector is violated
    # by removal of the whole entry for the removed key "b" which results in removal of it's
    # value's vvector
    m1 = ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))._put(
        node1, "b", ORSet.empty._add(node1, "B")
    )
    m2 = ORMap.empty._put(node2, "c", ORSet.empty._add(node2, "C"))

    # m1 - node1 gets the update from m2
    merged1 = m1.merge(m2)
    # m2 - node2 gets the update from m1
    merged2 = m2.merge(m1)

    # RACE CONDITION ahead!
    m3 = merged1.reset_delta()._remove(node1, "b")
    # let's imagine that m3 (node1) update gets propagated here (full state or delta - doesn't
    # matter) and is in flight, but in the meantime, an element is being added somewhere else
    # (m4 - node2) and the update is propagated before the update from node1 is merged
    m4 = merged2.reset_delta()._update(node2, "b", ORSet.empty, lambda s: s._add(node2, "B2"))
    # and later merged on node1
    merged3 = m3.merge(m4)
    # and the other way round...
    merged4 = m4.merge(m3)

    # result -  the element "B" is kept on both sides...
    assert merged3.entries["a"].elements == {"A"}
    assert merged3.entries["b"].elements == {"B", "B2"}
    assert merged3.entries["c"].elements == {"C"}

    assert merged4.entries["a"].elements == {"A"}
    assert merged4.entries["b"].elements == {"B", "B2"}
    assert merged4.entries["c"].elements == {"C"}

    # but if the timing was slightly different, so that the update from node1
    # would get merged just before update on node2:
    merged5 = (
        m2.merge(m3).reset_delta()._update(node2, "b", ORSet.empty, lambda s: s._add(node2, "B2"))
    )
    # the update propagated ... and merged on node1:
    merged6 = m3.merge(merged5)

    # then the outcome is different... because the vvector of value("b") was lost...
    assert merged5.entries["a"].elements == {"A"}
    # this time it's different...
    assert merged5.entries["b"].elements == {"B2"}
    assert merged5.entries["c"].elements == {"C"}

    assert merged6.entries["a"].elements == {"A"}
    # this time it's different...
    assert merged6.entries["b"].elements == {"B2"}
    assert merged6.entries["c"].elements == {"C"}


def test_work_with_delta_coalescing_scenario_1():
    m1 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))._put(node2, "b", GSet.empty.add("B"))
    m2 = (
        m1.reset_delta()
        ._put(node2, "b", GSet.empty.add("B2"))
        ._update(node2, "b", GSet.empty, lambda s: s.add("B3"))
    )

    merged1 = m1.merge(m2)

    assert merged1.entries["a"].elements == {"A"}
    assert merged1.entries["b"].elements == {"B", "B2", "B3"}

    merged2 = m1.merge_delta(delta(m2))

    assert merged2.entries["a"].elements == {"A"}
    assert merged2.entries["b"].elements == {"B", "B2", "B3"}

    m3 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))._put(node2, "b", GSet.empty.add("B"))
    m4 = (
        m3.reset_delta()
        ._put(node2, "b", GSet.empty.add("B2"))
        ._put(node2, "b", GSet.empty.add("B3"))
    )

    merged3 = m3.merge(m4)

    assert merged3.entries["a"].elements == {"A"}
    assert merged3.entries["b"].elements == {"B", "B3"}

    merged4 = m3.merge_delta(delta(m4))

    assert merged4.entries["a"].elements == {"A"}
    assert merged4.entries["b"].elements == {"B", "B3"}

    m5 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))._put(node2, "b", GSet.empty.add("B"))
    m6 = (
        m5.reset_delta()
        ._put(node2, "b", GSet.empty.add("B2"))
        ._update(node2, "b", GSet.empty, lambda s: s.add("B3"))
        ._update(node2, "b", GSet.empty, lambda s: s.add("B4"))
    )

    merged5 = m5.merge(m6)

    assert merged5.entries["a"].elements == {"A"}
    assert merged5.entries["b"].elements == {"B", "B2", "B3", "B4"}

    merged6 = m5.merge_delta(delta(m6))

    assert merged6.entries["a"].elements == {"A"}
    assert merged6.entries["b"].elements == {"B", "B2", "B3", "B4"}

    m7 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))._put(node2, "b", GSet.empty.add("B"))
    m8 = (
        m7.reset_delta()
        ._put(node2, "b", GSet.empty.add("B2"))
        ._put(node2, "d", GSet.empty.add("D"))
        ._put(node2, "b", GSet.empty.add("B3"))
    )

    merged7 = m7.merge(m8)

    assert merged7.entries["a"].elements == {"A"}
    assert merged7.entries["b"].elements == {"B", "B3"}
    assert merged7.entries["d"].elements == {"D"}

    merged8 = m7.merge_delta(delta(m8))

    assert merged8.entries["a"].elements == {"A"}
    assert merged8.entries["b"].elements == {"B", "B3"}
    assert merged8.entries["d"].elements == {"D"}

    m9 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))._put(node2, "b", GSet.empty.add("B"))
    m10 = (
        m9.reset_delta()
        ._put(node2, "b", GSet.empty.add("B2"))
        ._put(node2, "d", GSet.empty.add("D"))
        ._remove(node2, "d")
        ._put(node2, "b", GSet.empty.add("B3"))
    )

    merged9 = m9.merge(m10)

    assert merged9.entries["a"].elements == {"A"}
    assert merged9.entries["b"].elements == {"B", "B3"}

    merged10 = m9.merge_delta(delta(m10))

    assert merged10.entries["a"].elements == {"A"}
    assert merged10.entries["b"].elements == {"B", "B3"}


def test_work_with_deltas_and_updated_for_gset_elements_type():
    m1 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))
    m2 = m1.reset_delta()._update(node1, "a", GSet.empty, lambda s: s.add("B"))
    m3 = ORMap().merge_delta(delta(m1)).merge_delta(delta(m2))
    match m3.entries["a"]:
        case GSet(d3):
            assert d3 == {"A", "B"}
        case _:
            raise AssertionError


def test_work_with_deltas_and_updated_for_orset_elements_type():
    m1 = ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))
    m2 = m1.reset_delta()._update(node1, "a", ORSet.empty, lambda s: s._add(node1, "B"))
    m3 = ORMap().merge_delta(delta(m1)).merge_delta(delta(m2))

    match m3.entries["a"]:
        case ORSet(d3):
            assert d3 == {"A", "B"}
        case _:
            raise AssertionError


def test_work_with_aggregated_deltas_and_updated_for_gset_elements_type():
    m1 = ORMap.empty._put(node1, "a", GSet.empty.add("A"))
    m2 = (
        m1.reset_delta()
        ._update(node1, "a", GSet.empty, lambda s: s.add("B"))
        ._update(node1, "a", GSet.empty, lambda s: s.add("C"))
    )
    m3 = ORMap().merge_delta(delta(m1)).merge_delta(delta(m2))
    assert m3.entries["a"].elements == {"A", "B", "C"}


def test_work_with_deltas_and_updated_for_gcounter_elements_type():
    m1 = ORMap.empty._put(node1, "a", GCounter.empty)
    m2 = m1.reset_delta()._update(node1, "a", GCounter.empty, lambda c: c._increment(node1, 10))
    m3 = m2.reset_delta()._update(node2, "a", GCounter.empty, lambda c: c._increment(node2, 10))
    m4 = ORMap().merge_delta(delta(m1)).merge_delta(delta(m2)).merge_delta(delta(m3))
    assert m4.entries["a"].value == 20


def test_work_with_deltas_and_updated_for_pncounter_elements_type():
    m1 = ORMap.empty._put(node1, "a", PNCounter.empty)
    m2 = m1.reset_delta()._update(node1, "a", PNCounter.empty, lambda c: c._increment(node1, 10))
    m3 = m2.reset_delta()._update(node2, "a", PNCounter.empty, lambda c: c._decrement(node2, 10))
    m4 = ORMap().merge_delta(delta(m1)).merge_delta(delta(m2)).merge_delta(delta(m3))
    assert m4.entries["a"].value == 0


def test_work_with_deltas_and_updated_for_flag_elements_type():
    m1 = ORMap.empty._put(node1, "a", Flag(False))
    m2 = m1.reset_delta()._update(node1, "a", Flag.Disabled, lambda f: f.switch_on())
    m3 = ORMap().merge_delta(delta(m1)).merge_delta(delta(m2))
    match m3.entries["a"]:
        case Flag(d3):
            assert d3 is True
        case _:
            raise AssertionError


def test_not_allow_put_for_orset_elements_type():
    m = ORMap()._put(node1, "a", ORSet()._add(node1, "A"))

    with pytest.raises(ValueError):
        m._put(node1, "a", ORSet()._add(node1, "B"))


def test_be_able_to_update_entry():
    m1 = (
        ORMap[str, ORSet[str]]()
        ._put(node1, "a", ORSet.empty._add(node1, "A"))
        ._put(node1, "b", ORSet.empty._add(node1, "B01")._add(node1, "B02")._add(node1, "B03"))
    )
    m2 = ORMap[str, ORSet[str]]()._put(node2, "c", ORSet.empty._add(node2, "C"))

    merged1 = m1.merge(m2)

    m3 = merged1._update(node1, "b", ORSet.empty, lambda s: s._clear()._add(node1, "B2"))

    merged2 = merged1.merge(m3)
    assert merged2.entries["a"].elements == {"A"}
    assert merged2.entries["b"].elements == {"B2"}
    assert merged2.entries["c"].elements == {"C"}

    m4 = merged1._update(node2, "b", ORSet.empty, lambda s: s._add(node2, "B3"))
    merged3 = m3.merge(m4)
    assert merged3.entries["a"].elements == {"A"}
    assert merged3.entries["b"].elements == {"B2", "B3"}
    assert merged3.entries["c"].elements == {"C"}


def test_be_able_to_update_orset_entry_with_remove_put():
    m1 = (
        ORMap[str, ORSet[str]]()
        ._put(node1, "a", ORSet.empty._add(node1, "A01"))
        ._update(node1, "a", ORSet.empty, lambda s: s._add(node1, "A02"))
        ._update(node1, "a", ORSet.empty, lambda s: s._add(node1, "A03"))
        ._put(node1, "b", ORSet.empty._add(node1, "B01")._add(node1, "B02")._add(node1, "B03"))
    )
    m2 = ORMap[str, ORSet[str]]()._put(node2, "c", ORSet.empty._add(node2, "C"))

    merged1 = m1.merge(m2)

    # note that remove + put work because the new VersionVector version is incremented
    # from a global counter
    m3 = merged1._remove(node1, "b")._put(node1, "b", ORSet.empty._add(node1, "B2"))

    merged2 = merged1.merge(m3)
    assert merged2.entries["a"].elements == {"A01", "A02", "A03"}
    assert merged2.entries["b"].elements == {"B2"}
    assert merged2.entries["c"].elements == {"C"}

    m4 = merged1._update(node2, "b", ORSet.empty, lambda s: s._add(node2, "B3"))
    merged3 = m3.merge(m4)
    assert merged3.entries["a"].elements == {"A01", "A02", "A03"}
    assert merged3.entries["b"].elements == {"B2", "B3"}
    assert merged3.entries["c"].elements == {"C"}


def test_be_able_to_update_orset_entry_with_remove_merge_put():
    m1 = ORMap.empty._put(node1, "a", ORSet.empty._add(node1, "A"))._put(
        node1, "b", ORSet.empty._add(node1, "B01")._add(node1, "B02")._add(node1, "B03")
    )
    m2 = ORMap.empty._put(node2, "c", ORSet.empty._add(node2, "C"))

    merged1 = m1.merge(m2)

    m3 = merged1._remove(node1, "b")

    merged2 = merged1.merge(m3)
    assert merged2.entries["a"].elements == {"A"}
    assert merged2.contains("b") is False
    assert merged2.entries["c"].elements == {"C"}

    m4 = merged2._put(node1, "b", ORSet.empty._add(node1, "B2"))
    m5 = merged2._update(node2, "c", ORSet.empty, lambda s: s._add(node2, "C2"))._put(
        node2, "b", ORSet.empty._add(node2, "B3")
    )

    merged3 = m5.merge(m4)
    assert merged3.entries["a"].elements == {"A"}
    assert merged3.entries["b"].elements == {"B2", "B3"}
    assert merged3.entries["c"].elements == {"C", "C2"}


def test_have_unapply_extractor():
    # Pekko matches on ORMap(entries) and Changed(ORMapKey("key")); ohra has the extractor and
    # the key's identity.
    m1 = ORMap.empty._put(node1, "a", Flag(True))._put(node2, "b", Flag(False))
    match m1:
        case ORMap(entries1):
            pass
        case _:
            raise AssertionError
    assert entries1 == {"a": Flag(True), "b": Flag(False)}

    data: ReplicatedData = m1
    match data:
        case ORMap(entries3):
            pass
        case _:
            raise AssertionError
    assert entries3 == {"a": Flag(True), "b": Flag(False)}

    assert ORMapKey("key") == ORMapKey("key")
    r = rt(m1)  # the wire carries the keys, the values and the zero tag, not the delta
    assert r == m1 and r.delta is None and r.zero_tag == m1.zero_tag
    group = rt(m1.delta)
    assert isinstance(group, DeltaGroup) and group == m1.delta
    assert all(isinstance(op, PutDeltaOp) and type(op.value) is tuple for op in group.ops)


def test_update_deltas_of_a_counter_value_do_not_double_count():
    m1 = ORMap[str, PNCounter]()._update(
        node1, "a", PNCounter(), lambda c: c._increment(node1, 1), value_deltas=True
    )
    replica = ORMap[str, PNCounter]().merge_delta(delta(m1))
    m2 = m1.reset_delta()._update(
        node1, "a", PNCounter(), lambda c: c._increment(node1, 1), value_deltas=True
    )
    replica = replica.merge_delta(delta(m2))
    assert replica.entries["a"].value == 2 and m2.entries["a"].value == 2


# Pruning: not in ORMapSpec


def test_prune_collapses_the_removed_node_in_keys_and_values():
    m = ORMap()._put(node1, "a", GCounter()._increment(node2, 3))._put(node2, "b", GSet().add("B"))
    assert ORMap()._put(node1, "a", GCounter()._increment(node2, 3)).need_pruning_from(node2)

    pruned = m.prune(node2, node1)

    assert pruned.entries == {"a": GCounter({node1: 3}), "b": GSet(frozenset({"B"}))}
    assert not pruned.need_pruning_from(node2)
    assert pruned.modified_by_nodes == {node1}
    assert pruned.keys.elements_map["b"].modified_by_nodes == {node1}


def test_pruning_cleanup_drops_the_removed_node_from_keys_and_values():
    m = ORMap()._put(node1, "a", GCounter()._increment(node2, 3))._put(node2, "b", GSet().add("B"))

    cleaned = m.pruning_cleanup(node2)

    # as Pekko: the key "b" keeps an empty dot, and the counter loses node2's count
    assert cleaned.entries == {"a": GCounter(), "b": GSet(frozenset({"B"}))}
    assert cleaned.keys.elements_map["b"].is_empty
    assert not cleaned.need_pruning_from(node2)
    assert cleaned.modified_by_nodes == {node1}
