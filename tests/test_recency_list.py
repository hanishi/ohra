"""Port of Pekko's ``RecencyListSpec`` (actor-tests/.../util/RecencyListSpec.scala).

Cases, in Scala order: ``test_track_recency_of_elements`` (34).

Differences: ``3.seconds`` is ``3.0`` on ``TestClock``; ``removeLeastRecent(n, skip = k)`` is
``remove_least_recent(n, skip=k)``.
"""

from ohra.util.recency_list import RecencyList

from .util import TestClock


def check(recency_list: RecencyList[str], expected_least_to_most_recent: list[str]) -> None:
    assert all(recency_list.contains(v) for v in expected_least_to_most_recent)
    assert recency_list.size == len(expected_least_to_most_recent)
    assert list(recency_list.least_to_most_recent()) == expected_least_to_most_recent
    assert list(recency_list.most_to_least_recent()) == expected_least_to_most_recent[::-1]


def test_track_recency_of_elements() -> None:
    clock = TestClock()
    recency = RecencyList[str](clock)

    check(recency, [])

    clock.tick()  # time = 1
    recency.update("a")
    check(recency, ["a"])

    clock.tick()  # time = 2
    recency.update("b").update("c")
    check(recency, ["a", "b", "c"])

    clock.tick()  # time = 3
    recency.update("a").update("c")
    check(recency, ["b", "a", "c"])

    clock.tick()  # time = 4
    recency.update("d").update("e").update("f").update("g")
    check(recency, ["b", "a", "c", "d", "e", "f", "g"])

    clock.tick()  # time = 5
    recency.update("c").update("f")
    check(recency, ["b", "a", "d", "e", "g", "c", "f"])

    clock.tick()  # time = 6
    recency.remove("d").remove("g").remove("b").remove("f")
    check(recency, ["a", "e", "c"])

    clock.tick()  # time = 7
    recency.update("e").update("h").update("i").update("j")
    check(recency, ["a", "c", "e", "h", "i", "j"])

    clock.tick()  # time = 8
    assert recency.remove_least_recent(3) == ["a", "c", "e"]
    check(recency, ["h", "i", "j"])

    clock.tick()  # time = 9
    recency.update("k").update("l").update("m").update("i")
    check(recency, ["h", "j", "k", "l", "m", "i"])

    clock.tick()  # time = 10
    assert recency.remove_most_recent(3) == ["i", "m", "l"]
    check(recency, ["h", "j", "k"])

    clock.tick()  # time = 11
    recency.update("n").update("o")
    check(recency, ["h", "j", "k", "n", "o"])

    clock.tick()  # time = 12
    assert recency.remove_least_recent_outside(3.0) == ["h", "j"]
    check(recency, ["k", "n", "o"])

    clock.tick()  # time = 13
    recency.update("p").update("q").update("k").update("r")
    check(recency, ["n", "o", "p", "q", "k", "r"])

    clock.tick()  # time = 14
    assert recency.remove_most_recent_within(3.0) == ["r", "k", "q", "p"]
    check(recency, ["n", "o"])

    clock.tick()  # time = 15
    recency.update("s").update("t").update("u").update("v").update("w").update("x").update("y")
    recency.update("z")
    check(recency, ["n", "o", "s", "t", "u", "v", "w", "x", "y", "z"])

    clock.tick()  # time = 16
    assert recency.remove_least_recent(3, skip=3) == ["t", "u", "v"]
    check(recency, ["n", "o", "s", "w", "x", "y", "z"])

    clock.tick()  # time = 17
    assert recency.remove_most_recent(3, skip=2) == ["x", "w", "s"]
    check(recency, ["n", "o", "y", "z"])

    clock.tick()  # time = 18
    assert recency.remove_least_recent(10) == ["n", "o", "y", "z"]
    check(recency, [])

    clock.tick()  # time = 19
    recency.update("a").update("b").update("c")
    check(recency, ["a", "b", "c"])

    clock.tick()  # time = 20
    assert recency.remove_most_recent(10) == ["c", "b", "a"]
    check(recency, [])
