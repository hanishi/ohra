"""Ported from Apache Pekko's GSetSpec."""

import json

from ohra.cluster.ddata.gset import GSet, GSetKey
from ohra.remote.serialization import Serialization

user1 = '{"username":"john","password":"coltrane"}'
user2 = '{"username":"sonny","password":"rollins"}'
user3 = '{"username":"charlie","password":"parker"}'
user4 = '{"username":"charles","password":"mingus"}'


def _never(*_a: object) -> object:
    raise AssertionError("resolver called")


def rt[T](v: T) -> T:
    wire = json.loads(json.dumps(Serialization.encode(v, lambda r: r.path)))
    return Serialization.decode(wire, _never)  # type: ignore[return-value]


def test_be_able_to_add_user():
    c1 = GSet[str]()

    c2 = c1.add(user1)
    c3 = c2.add(user2)

    c4 = c3.add(user4)
    c5 = c4.add(user3)

    assert user1 in c5.elements
    assert user2 in c5.elements
    assert user3 in c5.elements
    assert user4 in c5.elements


def test_be_able_to_have_its_user_set_correctly_merged_with_another_gset_with_unique_user_sets():
    # set 1
    c11 = GSet[str]()

    c12 = c11.add(user1)
    c13 = c12.add(user2)

    assert c13.elements == {user1, user2}

    # set 2
    c21 = GSet[str]()

    c22 = c21.add(user3)
    c23 = c22.add(user4)

    assert c23.elements == {user3, user4}

    # merge both ways
    merged1 = c13.merge(c23)
    assert merged1.elements == {user1, user2, user3, user4}

    merged2 = c23.merge(c13)
    assert merged2.elements == {user1, user2, user3, user4}


def test_be_able_to_work_with_deltas():
    # set 1
    c11 = GSet[str]()

    c12 = c11.add(user1)
    c13 = c12.add(user2)

    assert c12.delta.elements == {user1}
    assert c13.delta.elements == {user1, user2}

    # deltas build state
    assert c12.merge_delta(c13.delta) == c13

    # own deltas are idempotent
    assert c13.merge_delta(c13.delta) == c13

    # set 2
    c21 = GSet[str]()

    c22 = c21.add(user3)
    c23 = c22.reset_delta().add(user4)

    assert c22.delta.elements == {user3}
    assert c23.delta.elements == {user4}

    assert c23.elements == {user3, user4}

    c33 = c13.merge(c23)

    # merge both ways
    merged1 = (
        GSet[str]()
        .merge_delta(c12.delta)
        .merge_delta(c13.delta)
        .merge_delta(c22.delta)
        .merge_delta(c23.delta)
    )
    assert merged1.elements == {user1, user2, user3, user4}

    merged2 = GSet[str]().merge_delta(c23.delta).merge_delta(c13.delta).merge_delta(c22.delta)
    assert merged2.elements == {user1, user2, user3, user4}

    assert merged1 == c33
    assert merged2 == c33


def test_be_able_to_have_its_user_set_correctly_merged_with_another_gset_with_overlapping_sets():
    # set 1
    c10 = GSet[str]()

    c11 = c10.add(user1)
    c12 = c11.add(user2)
    c13 = c12.add(user3)

    assert {user1, user2, user3} <= c13.elements

    # set 2
    c20 = GSet[str]()

    c21 = c20.add(user2)
    c22 = c21.add(user3)
    c23 = c22.add(user4)

    assert {user2, user3, user4} <= c23.elements

    # merge both ways
    merged1 = c13.merge(c23)
    assert {user1, user2, user3, user4} <= merged1.elements

    merged2 = c23.merge(c13)
    assert {user1, user2, user3, user4} <= merged2.elements


def test_have_unapply_extractor():
    # Pekko matches on GSet(elements) and Changed(GSetKey("key")); ohra has the value and
    # the key's identity.
    s1 = GSet[str]().add("a").add("b")
    assert s1.elements == {"a", "b"}
    assert GSetKey("key") == GSetKey("key")
    r = rt(s1)  # the wire carries the elements, not the delta
    assert r == s1 and r.delta is None and r.size == 2 and r.contains("a") and not r.is_empty
