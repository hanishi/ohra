"""Port of Pekko's ``SegmentedRecencyListSpec``
(actor-tests/.../util/SegmentedRecencyListSpec.scala).

Cases, in Scala order: ``test_track_recency_of_elements_across_2_segments`` (37),
``test_track_recency_of_elements_across_4_segments`` (67),
``test_allow_protected_limits_to_be_updated`` (100), ``test_remove_overall_least_recent_elements``
(122).

Differences: ``SegmentedRecencyList.empty(limits)`` is ``SegmentedRecencyList(limits)``;
``new SegmentedRecencyList(limits, OptionVal.Some(clock))`` is
``SegmentedRecencyList(limits, clock)``; ``2.seconds`` is ``2.0`` on ``TestClock``.
"""

from ohra.util.segmented_recency_list import SegmentedRecencyList

from .util import TestClock


def check(recency: SegmentedRecencyList[str], expected_segments: list[list[str]]) -> None:
    assert recency.size == sum(len(s) for s in expected_segments)
    for level, expected_segment in enumerate(expected_segments):
        assert all(recency.contains(v) for v in expected_segment)
        assert list(recency.least_to_most_recent_of(level)) == expected_segment
        assert recency.size_of(level) == len(expected_segment)


def test_track_recency_of_elements_across_2_segments() -> None:
    recency = SegmentedRecencyList[str]([2, 3])

    check(recency, [[], []])

    recency.update("a").update("b").update("c").update("d").update("e").update("f")
    check(recency, [["a", "b", "c", "d", "e", "f"], []])

    assert recency.remove_least_recent_over_limit() == ["a"]
    check(recency, [["b", "c", "d", "e", "f"], []])

    recency.update("a").update("b").update("c").update("d")
    check(recency, [["e", "f", "a"], ["b", "c", "d"]])

    assert recency.remove_least_recent_over_limit() == ["e"]
    check(recency, [["f", "a"], ["b", "c", "d"]])

    recency.update("a")
    check(recency, [["f", "b"], ["c", "d", "a"]])

    assert recency.remove_least_recent_over_limit() == []
    check(recency, [["f", "b"], ["c", "d", "a"]])

    recency.update("a").update("b").update("c").update("d").update("e").update("f").update("g")
    check(recency, [["a", "e", "b", "g"], ["c", "d", "f"]])

    assert recency.remove_least_recent_over_limit() == ["a", "e"]
    check(recency, [["b", "g"], ["c", "d", "f"]])


def test_track_recency_of_elements_across_4_segments() -> None:
    recency = SegmentedRecencyList[str]([4, 3, 2, 1])

    check(recency, [[], [], [], []])

    recency.update("a").update("b").update("c").update("d").update("e").update("f")
    check(recency, [["a", "b", "c", "d", "e", "f"], [], [], []])

    assert recency.remove_least_recent_over_limit() == []
    check(recency, [["a", "b", "c", "d", "e", "f"], [], [], []])

    recency.update("g").update("h").update("i").update("j").update("k").update("l")
    check(recency, [["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"], [], [], []])

    assert recency.remove_least_recent_over_limit() == ["a", "b"]
    check(recency, [["c", "d", "e", "f", "g", "h", "i", "j", "k", "l"], [], [], []])

    recency.update("b").update("d").update("f").update("h").update("j").update("l")
    check(recency, [["c", "e", "g", "i", "k", "b", "d", "f"], ["h", "j", "l"], [], []])

    recency.update("h").update("i").update("j").update("k").update("l").update("m")
    check(recency, [["c", "e", "g", "b", "d", "f", "m"], ["i", "k", "h"], ["j", "l"], []])

    assert recency.remove_least_recent_over_limit() == ["c", "e"]
    check(recency, [["g", "b", "d", "f", "m"], ["i", "k", "h"], ["j", "l"], []])

    recency.update("j").update("k").update("l").update("m").update("n").update("o").update("p")
    check(recency, [["g", "b", "d", "f", "n", "o", "p"], ["i", "h", "m"], ["k", "j"], ["l"]])

    assert recency.remove_least_recent_over_limit() == ["g", "b", "d"]
    check(recency, [["f", "n", "o", "p"], ["i", "h", "m"], ["k", "j"], ["l"]])


def test_allow_protected_limits_to_be_updated() -> None:
    recency = SegmentedRecencyList[str]([2, 8])

    check(recency, [[], []])

    recency.update("a").update("b").update("c").update("d").update("e").update("f")
    recency.update("g").update("h").update("i").update("j").update("k").update("l")
    check(recency, [["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"], []])

    recency.update("a").update("b").update("c").update("d").update("e").update("f")
    recency.update("g").update("h").update("i").update("j").update("k").update("l")
    check(recency, [["a", "b", "c", "d"], ["e", "f", "g", "h", "i", "j", "k", "l"]])

    assert recency.remove_least_recent_over_limit() == ["a", "b"]
    check(recency, [["c", "d"], ["e", "f", "g", "h", "i", "j", "k", "l"]])

    recency.update_limits([1, 4])

    assert recency.remove_least_recent_over_limit() == ["c", "d", "e", "f", "g"]
    check(recency, [["h"], ["i", "j", "k", "l"]])


def test_remove_overall_least_recent_elements() -> None:
    clock = TestClock()
    recency = SegmentedRecencyList[str]([5, 5], clock)

    check(recency, [[], []])

    clock.tick()  # time = 1
    recency.update("a").update("b").update("c").update("d").update("e").update("f").update("g")
    check(recency, [["a", "b", "c", "d", "e", "f", "g"], []])

    clock.tick()  # time = 2
    recency.update("a").update("b").update("c").update("d").update("e").update("f").update("g")
    check(recency, [["a", "b"], ["c", "d", "e", "f", "g"]])

    clock.tick()  # time = 3
    recency.update("h").update("i").update("j").update("k").update("j").update("k")
    check(recency, [["a", "b", "h", "i", "c", "d"], ["e", "f", "g", "j", "k"]])

    clock.tick()  # time = 4
    recency.update("l").update("m").update("n").update("n").update("o")
    check(recency, [["a", "b", "h", "i", "c", "d", "l", "m", "e", "o"], ["f", "g", "j", "k", "n"]])

    clock.tick()  # time = 5
    assert recency.remove_overall_least_recent_outside(2.0) == ["a", "b", "c", "d", "e", "f", "g"]
    check(recency, [["h", "i", "l", "m", "o"], ["j", "k", "n"]])
