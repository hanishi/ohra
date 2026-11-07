"""Ported from Apache Pekko's VersionVectorSpec."""

from ohra.cluster.ddata.version_vector import ManyVersionVector, OneVersionVector, VersionVector
from ohra.remote import UniqueAddress

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")
node3 = UniqueAddress("localhost", 7356, 3, "Sys")
node4 = UniqueAddress("localhost", 7357, 4, "Sys")
empty = VersionVector.empty


def test_have_zero_versions_when_created():
    vv = empty
    assert vv.size == 0


def test_not_happen_before_itself():
    vv1 = empty
    vv2 = empty

    assert not vv1.is_concurrent(vv2)


def test_increment_correctly():
    vv1 = empty
    vv2 = vv1._increment(node1)
    assert vv2.version_at(node1) > vv1.version_at(node1)
    vv3 = vv2._increment(node1)
    assert vv3.version_at(node1) > vv2.version_at(node1)

    vv4 = vv3._increment(node2)
    assert vv4.version_at(node1) == vv3.version_at(node1)
    assert vv4.version_at(node2) > vv3.version_at(node2)


def test_pass_misc_comparison_test_1():
    vv1_1 = empty
    vv2_1 = vv1_1._increment(node1)
    vv3_1 = vv2_1._increment(node2)
    vv4_1 = vv3_1._increment(node1)

    vv1_2 = empty
    vv2_2 = vv1_2._increment(node1)
    vv3_2 = vv2_2._increment(node2)
    vv4_2 = vv3_2._increment(node1)

    assert not vv4_1.is_concurrent(vv4_2)


def test_pass_misc_comparison_test_2():
    vv1_1 = empty
    vv2_1 = vv1_1._increment(node1)
    vv3_1 = vv2_1._increment(node2)
    vv4_1 = vv3_1._increment(node1)

    vv1_2 = empty
    vv2_2 = vv1_2._increment(node1)
    vv3_2 = vv2_2._increment(node2)
    vv4_2 = vv3_2._increment(node1)
    vv5_2 = vv4_2._increment(node3)

    assert vv4_1 < vv5_2


def test_pass_misc_comparison_test_3():
    vv1_1 = empty
    vv2_1 = vv1_1._increment(node1)

    vv1_2 = empty
    vv2_2 = vv1_2._increment(node2)

    assert vv2_1.is_concurrent(vv2_2)


def test_pass_misc_comparison_test_4():
    vv1_3 = empty
    vv2_3 = vv1_3._increment(node1)
    vv3_3 = vv2_3._increment(node2)
    vv4_3 = vv3_3._increment(node1)

    vv1_4 = empty
    vv2_4 = vv1_4._increment(node1)
    vv3_4 = vv2_4._increment(node1)
    vv4_4 = vv3_4._increment(node3)

    assert vv4_3.is_concurrent(vv4_4)


def test_pass_misc_comparison_test_5():
    vv1_1 = empty
    vv2_1 = vv1_1._increment(node2)
    vv3_1 = vv2_1._increment(node2)

    vv1_2 = empty
    vv2_2 = vv1_2._increment(node1)
    vv3_2 = vv2_2._increment(node2)
    vv4_2 = vv3_2._increment(node2)
    vv5_2 = vv4_2._increment(node3)

    assert vv3_1 < vv5_2
    assert vv5_2 > vv3_1


def test_pass_misc_comparison_test_6():
    vv1_1 = empty
    vv2_1 = vv1_1._increment(node1)
    vv3_1 = vv2_1._increment(node2)

    vv1_2 = empty
    vv2_2 = vv1_2._increment(node1)
    vv3_2 = vv2_2._increment(node1)

    assert vv3_1.is_concurrent(vv3_2)
    assert vv3_2.is_concurrent(vv3_1)


def test_pass_misc_comparison_test_7():
    vv1_1 = empty
    vv2_1 = vv1_1._increment(node1)
    vv3_1 = vv2_1._increment(node2)
    vv4_1 = vv3_1._increment(node2)
    vv5_1 = vv4_1._increment(node3)

    vv1_2 = vv4_1
    vv2_2 = vv1_2._increment(node2)
    vv3_2 = vv2_2._increment(node2)

    assert vv5_1.is_concurrent(vv3_2)
    assert vv3_2.is_concurrent(vv5_1)


def test_pass_misc_comparison_test_8():
    vv1_1 = empty
    vv2_1 = vv1_1._increment(node1)
    vv3_1 = vv2_1._increment(node3)

    vv1_2 = vv3_1._increment(node2)

    vv4_1 = vv3_1._increment(node3)

    assert vv4_1.is_concurrent(vv1_2)
    assert vv1_2.is_concurrent(vv4_1)


def test_correctly_merge_two_version_vectors():
    vv1_1 = empty
    vv2_1 = vv1_1._increment(node1)
    vv3_1 = vv2_1._increment(node2)
    vv4_1 = vv3_1._increment(node2)
    vv5_1 = vv4_1._increment(node3)

    vv1_2 = vv4_1
    vv2_2 = vv1_2._increment(node2)
    vv3_2 = vv2_2._increment(node2)

    merged1 = vv3_2.merge(vv5_1)
    assert merged1.size == 3
    assert merged1.contains(node1) is True
    assert merged1.contains(node2) is True
    assert merged1.contains(node3) is True

    merged2 = vv5_1.merge(vv3_2)
    assert merged2.size == 3
    assert merged2.contains(node1) is True
    assert merged2.contains(node2) is True
    assert merged2.contains(node3) is True

    assert vv3_2 < merged1
    assert vv5_1 < merged1

    assert vv3_2 < merged2
    assert vv5_1 < merged2

    assert merged1 == merged2


def test_correctly_merge_two_disjoint_version_vectors():

    vv1_1 = empty
    vv2_1 = vv1_1._increment(node1)
    vv3_1 = vv2_1._increment(node2)
    vv4_1 = vv3_1._increment(node2)
    vv5_1 = vv4_1._increment(node3)

    vv1_2 = empty
    vv2_2 = vv1_2._increment(node4)
    vv3_2 = vv2_2._increment(node4)

    merged1 = vv3_2.merge(vv5_1)
    assert merged1.size == 4
    assert merged1.contains(node1) is True
    assert merged1.contains(node2) is True
    assert merged1.contains(node3) is True
    assert merged1.contains(node4) is True

    merged2 = vv5_1.merge(vv3_2)
    assert merged2.size == 4
    assert merged2.contains(node1) is True
    assert merged2.contains(node2) is True
    assert merged2.contains(node3) is True
    assert merged2.contains(node4) is True

    assert vv3_2 < merged1
    assert vv5_1 < merged1

    assert vv3_2 < merged2
    assert vv5_1 < merged2

    assert merged1 == merged2


def test_pass_blank_version_vector_incrementing():
    v1 = empty
    v2 = empty

    vv1 = v1._increment(node1)
    vv2 = v2._increment(node2)

    assert vv1 > v1
    assert vv2 > v2

    assert vv1 > v2
    assert vv2 > v1

    assert not vv2 > vv1
    assert not vv1 > vv2


def test_pass_merging_behavior():
    a = empty
    b = empty

    a1 = a._increment(node1)
    b1 = b._increment(node2)

    a2 = a1._increment(node1)
    c = a2.merge(b1)
    c1 = c._increment(node3)

    assert c1 > a2
    assert c1 > b1


def test_compare_vectors_of_different_lengths_both_ways():
    short = VersionVector.create({node2: 1})
    long = VersionVector.create({node1: 1, node2: 1, node3: 1})
    assert short < long and long > short
    assert short.compare_to(long) is VersionVector.Ordering.before
    assert long.compare_to(short) is VersionVector.Ordering.after
    tail = VersionVector.create({node1: 1, node4: 1})  # sorts after every key of long but node1
    assert long.is_concurrent(tail) and tail.is_concurrent(long)
    assert OneVersionVector(node1, 1) == ManyVersionVector({node1: 1})
