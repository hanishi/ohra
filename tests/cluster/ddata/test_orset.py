"""Ported from Apache Pekko's ORSetSpec.

The ``test_prune_...`` and ``test_pruning_cleanup_...`` tests are ohra's own: ORSetSpec has no
case for the pruning methods."""

import json

from ohra.cluster.ddata import orset
from ohra.cluster.ddata.orset import ORSet, ORSetKey
from ohra.cluster.ddata.replicated_data import ReplicatedData
from ohra.cluster.ddata.version_vector import OneVersionVector, VersionVector
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")
node3 = UniqueAddress("localhost", 7356, 3, "Sys")

node_a = UniqueAddress("a", 7355, 1, "Sys")
node_b = UniqueAddress("b", 7355, 2, "Sys")
node_c = UniqueAddress("c", 7355, 3, "Sys")
node_d = UniqueAddress("d", 7355, 4, "Sys")
node_e = UniqueAddress("e", 7355, 5, "Sys")
node_f = UniqueAddress("f", 7355, 6, "Sys")
node_g = UniqueAddress("g", 7355, 7, "Sys")
node_h = UniqueAddress("h", 7355, 8, "Sys")

user1 = '{"username":"john","password":"coltrane"}'
user2 = '{"username":"sonny","password":"rollins"}'
user3 = '{"username":"charlie","password":"parker"}'
user4 = '{"username":"charles","password":"mingus"}'


def _never(*_a: object) -> object:
    raise AssertionError("resolver called")


def rt[T](v: T) -> T:
    wire = json.loads(json.dumps(Serialization.encode(v, lambda r: r.path)))
    return Serialization.decode(wire, _never)  # type: ignore[return-value]


def add_delta_op(s: ORSet[str]) -> orset.AddDeltaOp[str]:
    assert s.delta is not None
    return as_add_delta_op(s.delta)


def as_add_delta_op(delta: object) -> orset.AddDeltaOp[str]:
    if isinstance(delta, orset.AddDeltaOp):
        return delta  # type: ignore[return-value]
    raise ValueError("Expected AddDeltaOp")


# A ORSet


def test_be_able_to_add_user():
    c1 = ORSet()

    c2 = c1._add(node1, user1)
    c3 = c2._add(node1, user2)

    c4 = c3._add(node1, user4)
    c5 = c4._add(node1, user3)

    assert user1 in c5.elements
    assert user2 in c5.elements
    assert user3 in c5.elements
    assert user4 in c5.elements


def test_be_able_to_remove_added_user():
    c1 = ORSet()

    c2 = c1._add(node1, user1)
    c3 = c2._add(node1, user2)

    c4 = c3._remove(node1, user2)
    c5 = c4._remove(node1, user1)

    assert user1 not in c5.elements
    assert user2 not in c5.elements

    c6 = c3.merge(c5)
    assert user1 not in c6.elements
    assert user2 not in c6.elements

    c7 = c5.merge(c3)
    assert user1 not in c7.elements
    assert user2 not in c7.elements


def test_be_able_to_add_removed():
    c1 = ORSet()
    c2 = c1._remove(node1, user1)
    c3 = c2._add(node1, user1)
    assert user1 in c3.elements
    c4 = c3._remove(node1, user1)
    assert user1 not in c4.elements
    c5 = c4._add(node1, user1)
    assert user1 in c5.elements


def test_be_able_to_remove_and_add_several_times():
    c1 = ORSet()

    c2 = c1._add(node1, user1)
    c3 = c2._add(node1, user2)
    c4 = c3._remove(node1, user1)
    assert user1 not in c4.elements
    assert user2 in c4.elements

    c5 = c4._add(node1, user1)
    c6 = c5._add(node1, user2)
    assert user1 in c6.elements
    assert user2 in c6.elements

    c7 = c6._remove(node1, user1)
    c8 = c7._add(node1, user2)
    c9 = c8._remove(node1, user1)
    assert user1 not in c9.elements
    assert user2 in c9.elements


def test_be_able_to_have_its_user_set_correctly_merged_with_another_orset_with_unique_user_sets():  # noqa: E501
    # set 1
    c1 = ORSet()._add(node1, user1)._add(node1, user2)
    assert user1 in c1.elements
    assert user2 in c1.elements

    # set 2
    c2 = ORSet()._add(node2, user3)._add(node2, user4)._remove(node2, user3)

    assert user3 not in c2.elements
    assert user4 in c2.elements

    # merge both ways
    merged1 = c1.merge(c2)
    assert user1 in merged1.elements
    assert user2 in merged1.elements
    assert user3 not in merged1.elements
    assert user4 in merged1.elements

    merged2 = c2.merge(c1)
    assert user1 in merged2.elements
    assert user2 in merged2.elements
    assert user3 not in merged2.elements
    assert user4 in merged2.elements


def test_be_able_to_have_its_user_set_correctly_merged_with_another_orset_with_overlapping_user_sets():  # noqa: E501
    # set 1
    c1 = (
        ORSet()
        ._add(node1, user1)
        ._add(node1, user2)
        ._add(node1, user3)
        ._remove(node1, user1)
        ._remove(node1, user3)
    )

    assert user1 not in c1.elements
    assert user2 in c1.elements
    assert user3 not in c1.elements

    # set 2
    c2 = (
        ORSet()
        ._add(node2, user1)
        ._add(node2, user2)
        ._add(node2, user3)
        ._add(node2, user4)
        ._remove(node2, user3)
    )

    assert user1 in c2.elements
    assert user2 in c2.elements
    assert user3 not in c2.elements
    assert user4 in c2.elements

    # merge both ways
    merged1 = c1.merge(c2)
    assert user1 in merged1.elements
    assert user2 in merged1.elements
    assert user3 not in merged1.elements
    assert user4 in merged1.elements

    merged2 = c2.merge(c1)
    assert user1 in merged2.elements
    assert user2 in merged2.elements
    assert user3 not in merged2.elements
    assert user4 in merged2.elements


def test_be_able_to_have_its_user_set_correctly_merged_for_concurrent_updates():
    c1 = ORSet()._add(node1, user1)._add(node1, user2)._add(node1, user3)

    assert user1 in c1.elements
    assert user2 in c1.elements
    assert user3 in c1.elements

    c2 = c1._add(node2, user1)._remove(node2, user2)._remove(node2, user3)

    assert user1 in c2.elements
    assert user2 not in c2.elements
    assert user3 not in c2.elements

    # merge both ways
    merged1 = c1.merge(c2)
    assert user1 in merged1.elements
    assert user2 not in merged1.elements
    assert user3 not in merged1.elements

    merged2 = c2.merge(c1)
    assert user1 in merged2.elements
    assert user2 not in merged2.elements
    assert user3 not in merged2.elements

    c3 = c1._add(node1, user4)._remove(node1, user3)._add(node1, user2)

    # merge both ways
    merged3 = c2.merge(c3)
    assert user1 in merged3.elements
    assert user2 in merged3.elements
    assert user3 not in merged3.elements
    assert user4 in merged3.elements

    merged4 = c3.merge(c2)
    assert user1 in merged4.elements
    assert user2 in merged4.elements
    assert user3 not in merged4.elements
    assert user4 in merged4.elements


def test_be_able_to_have_its_user_set_correctly_merged_after_remove():
    c1 = ORSet()._add(node1, user1)._add(node1, user2)
    c2 = c1._remove(node2, user2)

    # merge both ways
    merged1 = c1.merge(c2)
    assert user1 in merged1.elements
    assert user2 not in merged1.elements

    merged2 = c2.merge(c1)
    assert user1 in merged2.elements
    assert user2 not in merged2.elements

    c3 = c1._add(node1, user3)

    # merge both ways
    merged3 = c3.merge(c2)
    assert user1 in merged3.elements
    assert user2 not in merged3.elements
    assert user3 in merged3.elements

    merged4 = c2.merge(c3)
    assert user1 in merged4.elements
    assert user2 not in merged4.elements
    assert user3 in merged4.elements


# ORSet deltas


def test_work_for_additions():
    s1 = ORSet.empty
    s2 = s1._add(node1, "a")
    assert add_delta_op(s2).underlying.elements == {"a"}
    assert s1.merge_delta(s2.delta) == s2

    s3 = s2.reset_delta()._add(node1, "b")._add(node1, "c")
    assert add_delta_op(s3).underlying.elements == {"b", "c"}
    assert s2.merge_delta(s3.delta) == s3

    # another node adds "d"
    s4 = s3.reset_delta()._add(node2, "d")
    assert add_delta_op(s4).underlying.elements == {"d"}
    assert s3.merge_delta(s4.delta) == s4

    # concurrent update
    s5 = s3.reset_delta()._add(node1, "e")
    s6 = s5.merge(s4)
    assert s5.merge_delta(s4.delta) == s6

    # concurrent add of same element
    s7 = s3.reset_delta()._add(node1, "d")
    s8 = s7.merge(s4)
    # the dot contains both nodes
    assert s8.elements_map["d"].contains(node1)
    assert s8.elements_map["d"].contains(node2)
    # and same result when merging the deltas
    assert s7.merge_delta(s4.delta) == s8
    assert s4.merge_delta(s7.delta) == s8


def test_handle_another_concurrent_add_scenario():
    s1 = ORSet.empty
    s2 = s1._add(node1, "a")
    s3 = s2._add(node1, "b")
    s4 = s2._add(node2, "c")

    # full state merge for reference
    s5 = s4.merge(s3)
    assert s5.elements == {"a", "b", "c"}

    s6 = s4.merge_delta(s3.delta)
    assert s6.elements == {"a", "b", "c"}


def test_merge_deltas_into_delta_groups():
    s1 = ORSet.empty
    s2 = s1._add(node1, "a")
    d2 = s2.delta
    s3 = s2.reset_delta()._add(node1, "b")
    d3 = s3.delta
    d4 = d2.merge(d3)
    assert as_add_delta_op(d4).underlying.elements == {"a", "b"}
    assert s1.merge_delta(d4) == s3
    assert s2.merge_delta(d4) == s3

    s5 = s3.reset_delta()._remove(node1, "b")
    d5 = s5.delta
    d6 = d4.merge(d5)
    assert isinstance(d6, orset.DeltaGroup)
    assert type(d6.ops[-1]) is orset.RemoveDeltaOp
    assert len(d6.ops) == 2
    assert s3.merge_delta(d6) == s5

    s7 = s5.reset_delta()._add(node1, "c")
    s8 = s7.reset_delta()._add(node1, "d")
    d9 = d6.merge(s7.delta).merge(s8.delta)
    assert isinstance(d9, orset.DeltaGroup)
    # the add "c" and add "d" are merged into one AddDeltaOp
    assert as_add_delta_op(d9.ops[-1]).underlying.elements == {"c", "d"}
    assert len(d9.ops) == 3
    assert s5.merge_delta(d9) == s8
    assert s5.merge_delta(s7.delta).merge_delta(s8.delta) == s8


def test_work_for_removals():
    s1 = ORSet.empty
    s2 = s1._add(node1, "a")._add(node1, "b").reset_delta()
    s3 = s2._remove(node1, "b")
    assert s2.merge(s3) == s3
    assert s2.merge_delta(s3.delta) == s3
    assert s2.merge_delta(s3.delta).elements == {"a"}

    # concurrent update
    s4 = s2._add(node2, "c").reset_delta()
    s5 = s4.merge(s3)
    assert s5.elements == {"a", "c"}
    assert s4.merge_delta(s3.delta) == s5

    # add "b" again
    s6 = s5._add(node2, "b")
    # merging the old delta should not remove it
    assert s6.merge_delta(s3.delta) == s6
    assert s6.merge_delta(s3.delta).elements == {"a", "b", "c"}


def test_work_for_clear():
    s1 = ORSet.empty
    s2 = s1._add(node1, "a")._add(node1, "b")
    s3 = s2.reset_delta()._clear()
    s4 = s3.reset_delta()._add(node1, "c")
    assert s2.merge(s3) == s3
    assert s2.merge_delta(s3.delta) == s3
    s5 = s2.merge_delta(s3.delta).merge_delta(s4.delta)
    assert s5.elements == {"c"}
    assert s5 == s4

    # concurrent update
    s6 = s2.reset_delta()._add(node2, "d")
    s7 = s6.merge(s3)
    assert s7.elements == {"d"}
    assert s6.merge_delta(s3.delta) == s7

    # add "b" again
    s8 = s7._add(node2, "b")
    # merging the old delta should not remove it
    assert s8.merge_delta(s3.delta) == s8
    assert s8.merge_delta(s3.delta).elements == {"b", "d"}


def test_handle_a_mixed_add_remove_scenario():
    s1 = ORSet.empty
    s2 = s1.reset_delta()._remove(node1, "e")
    s3 = s2.reset_delta()._add(node1, "b")
    s4 = s3.reset_delta()._add(node1, "a")
    s5 = s4.reset_delta()._remove(node1, "b")

    delta_group1 = s3.delta.merge(s4.delta).merge(s5.delta)

    s7 = s2.merge_delta(delta_group1)
    assert s7.elements == {"a"}
    # The above scenario was constructed from failing ReplicatorDeltaSpec,
    # some more checks...

    s8 = s2.reset_delta()._add(node2, "z")  # concurrent update from node2
    s9 = s8.merge_delta(delta_group1)
    assert s9.elements == {"a", "z"}


def test_handle_a_mixed_add_remove_scenario_2():
    s1 = ORSet.empty
    s2 = s1.reset_delta()._add(node1, "a")
    s3 = s2.reset_delta()._add(node1, "b")
    s4 = s3.reset_delta()._add(node2, "a")
    s5 = s4.reset_delta()._remove(node1, "a")

    assert s5.elements == {"b"}

    delta1 = s2.delta.merge(s3.delta)
    delta2 = s4.delta

    t1 = ORSet.empty
    t2 = t1.merge_delta(delta1).merge_delta(delta2)
    assert t2.elements == {"a", "b"}
    t3 = t2.reset_delta()._add(node3, "z")

    t4 = t3.merge_delta(s5.delta)

    assert t4.elements == {"b", "z"}


def test_handle_a_mixed_add_remove_scenario_3():
    s1 = ORSet.empty
    s2 = s1.reset_delta()._add(node1, "a")
    s3 = s2.reset_delta()._add(node1, "b")
    s4 = s3.reset_delta()._add(node2, "a")
    s5 = s4.reset_delta()._remove(node1, "a")

    assert s5.elements == {"b"}

    delta1 = s2.delta.merge(s3.delta)

    t1 = ORSet.empty

    t2 = t1.merge_delta(delta1)
    assert t2.elements == {"a", "b"}
    t3 = t2.reset_delta()._add(node3, "a")

    t4 = t3.merge_delta(s5.delta)

    assert t4.elements == {"b", "a"}


def test_not_have_anomalies_for_orset_in_complex_but_realistic_scenario():
    node1_1 = ORSet.empty._add(node1, "q")._remove(node1, "q")
    delta1_1 = node1_1.delta
    node1_2 = node1_1.reset_delta().reset_delta()._add(node1, "z")._remove(node1, "z")
    delta1_2 = node1_2.delta
    # we finished doing stuff on node1 - there are two separate deltas that will be propagated
    # node2 is created, then gets first delta from node1 and then adds an element "x"
    node2_1 = ORSet.empty.merge_delta(delta1_1).reset_delta()._add(node2, "x")
    delta2_1 = node2_1.delta
    # node2 continues its existence adding and later removing the element
    # it still didn't get the second update from node1 (that is fully legit :) )
    node2_2 = node2_1.reset_delta()._add(node2, "a")._remove(node2, "a")
    delta2_2 = node2_2.delta

    # in the meantime there is some node3
    # there is not much activity on it, it just gets the first delta from node1 then it gets
    # first delta from node2
    # then it gets the second delta from node1 (that node2 still didn't get, but, hey!, this is
    # fine)
    node3_1 = ORSet.empty.merge_delta(delta1_1).merge_delta(delta2_1).merge_delta(delta1_2)

    # and node3_1 receives full update from node2 via gossip
    merged1 = node3_1.merge(node2_2)

    assert not merged1.contains("a")

    # and node3_1 receives delta update from node2 (it just needs to get the second delta,
    # as it already got the first delta just a second ago)

    merged2 = node3_1.merge_delta(delta2_2)

    match merged2:
        case ORSet(mg2):
            pass
        case _:
            raise AssertionError
    assert mg2 == {"x"}  # !!!


def test_require_causal_delivery_of_deltas():
    # This test illustrates why we need causal delivery of deltas.
    # Otherwise the following could happen.

    # s0 is the stable state that is initially replicated to all nodes
    s0 = ORSet.empty._add(node1, "a")

    # add element "b" and "c" at node1
    s11 = s0.reset_delta()._add(node1, "b")
    s12 = s11.reset_delta()._add(node1, "c")

    # at the same time, add element "d" at node2
    s21 = s0.reset_delta()._add(node2, "d")

    # node3 receives delta for "d" and "c", but the delta for "b" is lost
    s31 = s0.merge_delta(s21.delta).merge_delta(s12.delta)
    assert s31.elements == {"a", "c", "d"}

    # node4 receives all deltas
    s41 = s0.merge_delta(s11.delta).merge_delta(s12.delta).merge_delta(s21.delta)
    assert s41.elements == {"a", "b", "c", "d"}

    # node3 and node4 sync with full state gossip
    s32 = s31.merge(s41)
    # one would expect elements "a", "b", "c", "d", but "b" is removed
    # because we applied s12.delta without applying s11.delta
    assert s32.elements == {"a", "c", "d"}


# ORSet unit test


def test_verify_subtract_dots():
    dot = VersionVector.create({node_a: 3, node_b: 2, node_d: 14, node_g: 22})
    vvector = VersionVector.create(
        {node_a: 4, node_b: 1, node_c: 1, node_d: 14, node_e: 5, node_f: 2}
    )
    expected = VersionVector.create({node_b: 2, node_g: 22})
    assert orset._subtract_dots(dot, vvector) == expected


def test_verify_merge_common_keys():
    common_keys = {"K1", "K2"}
    this_dot1 = VersionVector.create({node_a: 3, node_d: 7})
    this_dot2 = VersionVector.create({node_b: 5, node_c: 2})
    this_vvector = VersionVector.create({node_a: 3, node_b: 5, node_c: 2, node_d: 7})
    this_set = ORSet(elements_map={"K1": this_dot1, "K2": this_dot2}, vvector=this_vvector)
    that_dot1 = OneVersionVector(node_a, 3)
    that_dot2 = OneVersionVector(node_b, 6)
    that_vvector = VersionVector.create({node_a: 3, node_b: 6, node_c: 1, node_d: 8})
    that_set = ORSet(elements_map={"K1": that_dot1, "K2": that_dot2}, vvector=that_vvector)

    expected_dots = {
        "K1": OneVersionVector(node_a, 3),
        "K2": VersionVector.create({node_b: 6, node_c: 2}),
    }

    assert orset._merge_common_keys(common_keys, this_set, that_set) == expected_dots


def test_verify_merge_disjoint_keys():
    keys = {"K3", "K4", "K5"}
    elements = {
        "K3": OneVersionVector(node_a, 4),
        "K4": VersionVector.create({node_a: 3, node_d: 8}),
        "K5": OneVersionVector(node_a, 2),
    }
    vvector = VersionVector.create({node_a: 3, node_d: 7})
    acc = {"K1": OneVersionVector(node_a, 3)}
    # "a" -> 3 removed, optimized to include only those unseen
    expected_dots = {**acc, "K3": OneVersionVector(node_a, 4), "K4": OneVersionVector(node_d, 8)}

    assert orset._merge_disjoint_keys(keys, elements, vvector, acc) == expected_dots


def test_verify_disjoint_merge():
    a1 = ORSet()._add(node1, "bar")
    b1 = ORSet()._add(node2, "baz")
    c = a1.merge(b1)
    a2 = a1._remove(node1, "bar")
    d = a2.merge(c)
    assert d.elements == {"baz"}


def test_verify_removed_after_merge():
    # Add Z at node1 replica
    a = ORSet()._add(node1, "Z")
    # Replicate it to some node3, i.e. it has dot 'Z'->{node1 -> 1}
    c = a
    # Remove Z at node1 replica
    a2 = a._remove(node1, "Z")
    # Add Z at node2, a new replica
    b = ORSet()._add(node2, "Z")
    # Replicate b to node1, so now node1 has a Z, the one with a Dot of
    # {node2 -> 1} and version vector of [{node1 -> 1}, {node2 -> 1}]
    a3 = b.merge(a2)
    assert a3.elements == {"Z"}
    # Remove the 'Z' at node2 replica
    b2 = b._remove(node2, "Z")
    # Both node3 (c) and node1 (a3) have a 'Z', but when they merge, there should be
    # no 'Z' as node3 (c)'s has been removed by node1 and node1 (a3)'s has been removed by
    # node2
    assert c.elements == {"Z"}
    assert a3.elements == {"Z"}
    assert b2.elements == set()

    assert a3.merge(c).merge(b2).elements == set()
    assert a3.merge(b2).merge(c).elements == set()
    assert c.merge(b2).merge(a3).elements == set()
    assert c.merge(a3).merge(b2).elements == set()
    assert b2.merge(c).merge(a3).elements == set()
    assert b2.merge(a3).merge(c).elements == set()


def test_verify_removed_after_merge_2():
    a = ORSet()._add(node1, "Z")
    b = ORSet()._add(node2, "Z")
    # replicate node3
    c = a
    a2 = a._remove(node1, "Z")
    # replicate b to node1, now node1 has node2's 'Z'
    a3 = a2.merge(b)
    assert a3.elements == {"Z"}
    # Remove node2's 'Z'
    b2 = b._remove(node2, "Z")
    # Replicate c to node2, now node2 has node1's old 'Z'
    b3 = b2.merge(c)
    assert b3.elements == {"Z"}
    # Merge everytyhing
    assert a3.merge(c).merge(b3).elements == set()
    assert a3.merge(b3).merge(c).elements == set()
    assert c.merge(b3).merge(a3).elements == set()
    assert c.merge(a3).merge(b3).elements == set()
    assert b3.merge(c).merge(a3).elements == set()
    assert b3.merge(a3).merge(c).elements == set()


def test_not_pollute_the_vvector_of_result_during_merge_remove_delta():
    a = ORSet()._add(node_a, "a")
    # a1 now contains dot from nodeB
    a1 = a._add(node_b, "b")._remove(node_b, "b").reset_delta()._remove(node_a, "a")

    assert a.vvector.contains(node_a)
    assert not a.vvector.contains(node_b)
    assert a1.vvector.contains(node_a)
    assert a1.vvector.contains(node_b)

    a2 = ORSet().merge_delta(a.delta).merge_delta(a1.delta)
    assert a2.elements == set()
    # a2 should not be polluted by the nodeB dot, as all operations on it pertained only to
    # elements from nodeA
    assert not a2.vvector.contains(node_b)


def test_have_unapply_extractor():
    # Pekko matches on ORSet(elements) and Changed(ORSetKey("key")); ohra has the extractor and
    # the key's identity.
    s1 = ORSet.empty._add(node1, "a")._add(node2, "b")
    match s1:
        case ORSet(elements1):
            pass
        case _:
            raise AssertionError
    assert elements1 == {"a", "b"}

    data: ReplicatedData = s1
    match data:
        case ORSet(elements3):
            pass
        case _:
            raise AssertionError
    assert elements3 == {"a", "b"}

    assert ORSetKey("key") == ORSetKey("key")
    r = rt(s1)  # the wire carries the elements and the version vector, not the delta
    assert r == s1 and r.delta is None and r.size == 2 and r.contains("a") and not r.is_empty


def test_merge_with_its_own_descendant_equals_a_full_merge():
    def pair() -> tuple[ORSet[str], ORSet[str]]:
        a = rt(ORSet[str]()._add(node1, "x"))  # decoded: no ancestor
        return a, a._add(node2, "y")._remove(node1, "x")

    a, b = pair()
    full = rt(a).merge(rt(b))  # decoded copies carry no ancestor
    assert b._ancestor is a
    assert a.merge(b) is b and b == full and full.elements == {"y"}
    a, b = pair()
    full = rt(a).merge(rt(b))  # a new pair has new timestamps
    assert b._ancestor is a
    assert b.merge(a) is b and b == full


# Pruning: not in ORSetSpec


def concurrent_adds() -> ORSet[str]:
    # "a" has a dot from each node, "b" only node2's
    return ORSet[str]()._add(node1, "a").merge(ORSet[str]()._add(node2, "a")._add(node2, "b"))


def test_prune_collapses_the_removed_nodes_dots_into_the_target():
    s = concurrent_adds()
    assert s.need_pruning_from(node2)

    pruned = s.prune(node2, node1)

    assert pruned.elements == {"a", "b"}
    assert not pruned.need_pruning_from(node2)
    assert pruned.modified_by_nodes == {node1}
    for dot in pruned.elements_map.values():
        # each element is re-added on node1, so its dot is node1's, seen by the vvector
        assert dot.modified_by_nodes == {node1}
        assert dot.version_at(node1) <= pruned.vvector.version_at(node1)


def test_prune_of_a_node_with_no_dots_only_prunes_the_vvector():
    s = ORSet[str]()._add(node2, "x")._remove(node2, "x")._add(node1, "a")
    assert s.elements_map["a"].modified_by_nodes == {node1}

    pruned = s.prune(node2, node1)

    assert pruned.elements_map == s.elements_map
    assert pruned.modified_by_nodes == {node1}
    assert not pruned.need_pruning_from(node2)


def test_pruning_cleanup_drops_the_removed_node_from_dots_and_vvector():
    s = concurrent_adds()

    cleaned = s.pruning_cleanup(node2)

    # as Pekko: "b" stays, with an empty dot
    assert cleaned.elements == {"a", "b"}
    assert cleaned.elements_map["a"] == OneVersionVector(
        node1, s.elements_map["a"].version_at(node1)
    )
    assert cleaned.elements_map["b"] == VersionVector.empty
    assert not cleaned.need_pruning_from(node2)
    assert cleaned.modified_by_nodes == {node1}
