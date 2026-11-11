"""Port of Pekko's ``FrequencyListSpec`` (actor-tests/.../util/FrequencyListSpec.scala).

Cases, in Scala order: ``test_track_frequency_of_elements`` (41),
``test_track_overall_recency_of_elements_when_enabled`` (95),
``test_support_dynamic_aging_with_least_frequently_used_eviction`` (172).

Differences: ``FrequencyList.empty[String]()`` is ``FrequencyList[str]()``;
``new FrequencyList(dynamicAging = false, OptionVal.Some(clock))`` is
``FrequencyList(dynamic_aging=False, clock=clock)``; ``OptionVal.Some("j")`` is ``skip="j"``;
``3.seconds`` is ``3.0`` on ``TestClock``. The ``/* 1: */`` frequency markers in the Scala
``check`` calls are comments here.
"""

import pytest

from ohra.util.frequency_list import FrequencyList

from .util import TestClock


def check(frequency_list: FrequencyList[str], expected_least_to_most_frequent: list[str]) -> None:
    assert all(frequency_list.contains(v) for v in expected_least_to_most_frequent)
    assert frequency_list.size == len(expected_least_to_most_frequent)
    assert list(frequency_list.least_to_most_frequent()) == expected_least_to_most_frequent
    assert list(frequency_list.most_to_least_frequent()) == expected_least_to_most_frequent[::-1]


def check_recency(frequency_list: FrequencyList[str], expected_least_to_most_recent: list[str]):
    assert all(frequency_list.contains(v) for v in expected_least_to_most_recent)
    assert frequency_list.size == len(expected_least_to_most_recent)
    assert list(frequency_list.overall_least_to_most_recent()) == expected_least_to_most_recent
    assert (
        list(frequency_list.overall_most_to_least_recent()) == expected_least_to_most_recent[::-1]
    )


def test_track_frequency_of_elements() -> None:
    frequency = FrequencyList[str]()

    check(frequency, [])

    frequency.update("a")
    check(frequency, ["a"])  # 1: a

    frequency.update("b").update("c")
    check(frequency, ["a", "b", "c"])  # 1: a b c

    frequency.update("a").update("c")
    check(frequency, ["b", "a", "c"])  # 1: b, 2: a c

    frequency.update("d").update("e").update("f").update("g")
    check(frequency, ["b", "d", "e", "f", "g", "a", "c"])  # 1: b d e f g, 2: a c

    frequency.update("c").update("f")
    check(frequency, ["b", "d", "e", "g", "a", "f", "c"])  # 1: b d e g, 2: a f, 3: c

    frequency.remove("d").remove("g").remove("b").remove("f")
    check(frequency, ["e", "a", "c"])  # 1: e, 2: a, 3: c

    frequency.update("e").update("h").update("i")
    check(frequency, ["h", "i", "a", "e", "c"])  # 1: h i, 2: a e, 3: c

    assert frequency.remove_least_frequent(3) == ["h", "i", "a"]
    check(frequency, ["e", "c"])  # 2: e, 3: c

    frequency.update("j").update("k").update("l").update("m")
    check(frequency, ["j", "k", "l", "m", "e", "c"])  # 1: j k l m, 2: e, 3: c

    assert frequency.remove_least_frequent(skip="j") == ["k"]
    check(frequency, ["j", "l", "m", "e", "c"])  # 1: j l m, 2: e, 3: c

    assert frequency.remove_least_frequent(2, skip="l") == ["j", "m"]
    check(frequency, ["l", "e", "c"])  # 1: l, 2: e, 3: c

    frequency.update("n").update("o").update("p").update("e").update("o").update("l")
    check(frequency, ["n", "p", "o", "l", "c", "e"])  # 1: n p, 2: o l, 3: c e

    assert frequency.remove_most_frequent(3) == ["e", "c", "l"]
    check(frequency, ["n", "p", "o"])  # 1: n p, 2: o

    frequency.update("q").update("r").update("p").update("o").update("n")
    check(frequency, ["q", "r", "p", "n", "o"])  # 1: q r, 2: p n, 3: o

    assert frequency.remove_most_frequent(skip="o") == ["n"]
    check(frequency, ["q", "r", "p", "o"])  # 1: q r, 2: p, 3: o

    assert frequency.remove_most_frequent(2, skip="p") == ["o", "r"]
    check(frequency, ["q", "p"])  # 1: q, 2: p


def test_track_overall_recency_of_elements_when_enabled() -> None:
    clock = TestClock()
    frequency = FrequencyList[str](dynamic_aging=False, clock=clock)

    check(frequency, [])

    clock.tick()  # time = 1
    frequency.update("a")
    check(frequency, ["a"])  # 1: a
    check_recency(frequency, ["a"])

    clock.tick()  # time = 2
    frequency.update("b").update("c")
    check(frequency, ["a", "b", "c"])  # 1: a b c
    check_recency(frequency, ["a", "b", "c"])

    clock.tick()  # time = 3
    frequency.update("a").update("c")
    check(frequency, ["b", "a", "c"])  # 1: b, 2: a c
    check_recency(frequency, ["b", "a", "c"])

    clock.tick()  # time = 4
    frequency.update("d").update("e").update("f")
    check(frequency, ["b", "d", "e", "f", "a", "c"])  # 1: b d e f, 2: a c
    check_recency(frequency, ["b", "a", "c", "d", "e", "f"])

    clock.tick()  # time = 5
    frequency.update("c").update("f")
    check(frequency, ["b", "d", "e", "a", "f", "c"])  # 1: b d e, 2: a f, 3: c
    check_recency(frequency, ["b", "a", "d", "e", "c", "f"])

    clock.tick()  # time = 6
    frequency.remove("d").remove("b").remove("f")
    check(frequency, ["e", "a", "c"])  # 1: e, 2: a, 3: c
    check_recency(frequency, ["a", "e", "c"])

    clock.tick()  # time = 7
    frequency.update("e").update("h").update("i")
    check(frequency, ["h", "i", "a", "e", "c"])  # 1: h i, 2: a e, 3: c
    check_recency(frequency, ["a", "c", "e", "h", "i"])

    clock.tick()  # time = 8
    assert frequency.remove_overall_least_recent() == ["a"]
    check(frequency, ["h", "i", "e", "c"])  # 1: h i, 2: e, 3: c
    check_recency(frequency, ["c", "e", "h", "i"])

    clock.tick()  # time = 9
    frequency.update("i").update("j").update("k")
    check(frequency, ["h", "j", "k", "e", "i", "c"])  # 1: h j k, 2: e i, 3: c
    check_recency(frequency, ["c", "e", "h", "i", "j", "k"])

    clock.tick()  # time = 10
    assert frequency.remove_overall_most_recent() == ["k"]
    check(frequency, ["h", "j", "e", "i", "c"])  # 1: h j, 2: e i, 3: c
    check_recency(frequency, ["c", "e", "h", "i", "j"])

    clock.tick()  # time = 11
    assert frequency.remove_overall_least_recent_outside(3.0) == ["c", "e", "h"]
    check(frequency, ["j", "i"])  # 1: j, 2: i
    check_recency(frequency, ["i", "j"])

    clock.tick()  # time = 12
    frequency.update("l").update("m")
    check(frequency, ["j", "l", "m", "i"])  # 1: j l m, 2: i
    check_recency(frequency, ["i", "j", "l", "m"])

    clock.tick()  # time = 13
    assert frequency.remove_overall_most_recent_within(3.0) == ["m", "l"]
    check(frequency, ["j", "i"])  # 1: j, 2: i
    check_recency(frequency, ["i", "j"])

    clock.tick()  # time = 14
    frequency.update("n").update("o").update("n")
    check(frequency, ["j", "o", "i", "n"])  # 1: j o, 2: i n
    check_recency(frequency, ["i", "j", "o", "n"])


def test_support_dynamic_aging_with_least_frequently_used_eviction() -> None:
    regular = FrequencyList[str]()
    aging = FrequencyList[str](dynamic_aging=True)

    check(regular, [])
    check(aging, [])

    for _ in range(10):
        regular.update("a").update("b").update("c")
    check(regular, ["a", "b", "c"])  # 10

    for _ in range(10):
        aging.update("a").update("b").update("c")
    check(aging, ["a", "b", "c"])  # 10+0

    # age = 0

    regular.update("x").update("y").update("z")
    check(regular, ["x", "y", "z", "a", "b", "c"])  # 1, 10

    aging.update("x").update("y").update("z")
    check(aging, ["x", "y", "z", "a", "b", "c"])  # 1+0, 10+0

    assert regular.remove_least_frequent() == ["x"]
    check(regular, ["y", "z", "a", "b", "c"])  # 1, 10

    assert aging.remove_least_frequent() == ["x"]
    check(aging, ["y", "z", "a", "b", "c"])  # 1+0, 10+0

    # age = 1 (from last removal of "x")

    regular.update("x").update("y").update("z").update("z")
    check(regular, ["x", "y", "z", "a", "b", "c"])  # 1, 2, 3, 10

    aging.update("x").update("y").update("z").update("z")
    check(aging, ["x", "y", "z", "a", "b", "c"])  # 1+1, 2+1, 3+1, 10+0

    assert regular.remove_least_frequent(2) == ["x", "y"]
    check(regular, ["z", "a", "b", "c"])  # 3, 10

    assert aging.remove_least_frequent(2) == ["x", "y"]
    check(aging, ["z", "a", "b", "c"])  # 3+1, 10+0

    # age = 3 (from last removal of "y")

    regular.update("x").update("y").update("z")
    check(regular, ["x", "y", "z", "a", "b", "c"])  # 1, 4, 10

    aging.update("x").update("y").update("z")
    check(aging, ["x", "y", "z", "a", "b", "c"])  # 1+3, 4+3, 10+0

    assert regular.remove_least_frequent(3) == ["x", "y", "z"]
    check(regular, ["a", "b", "c"])  # 10

    assert aging.remove_least_frequent(3) == ["x", "y", "z"]
    check(aging, ["a", "b", "c"])  # 10+0

    # age = 7 (from last removal of "z")

    regular.update("x").update("y").update("y").update("z").update("z").update("z")
    check(regular, ["x", "y", "z", "a", "b", "c"])  # 1, 2, 3, 10

    aging.update("x").update("y").update("y").update("z").update("z").update("z")
    check(aging, ["x", "y", "a", "b", "c", "z"])  # 1+7, 2+7, 10+0, 3+7

    assert regular.remove_least_frequent(2) == ["x", "y"]
    check(regular, ["z", "a", "b", "c"])  # 3, 10

    assert aging.remove_least_frequent(2) == ["x", "y"]
    check(aging, ["a", "b", "c", "z"])  # 10+0, 3+7

    # age = 9 (from last removal of "y")

    regular.update("x").update("y").update("z")
    check(regular, ["x", "y", "z", "a", "b", "c"])  # 1, 4, 10

    aging.update("x").update("y").update("z")
    check(aging, ["a", "b", "c", "x", "y", "z"])  # 10+0, 1+9, 4+9

    assert regular.remove_least_frequent(3) == ["x", "y", "z"]
    check(regular, ["a", "b", "c"])  # 10

    assert aging.remove_least_frequent(3) == ["a", "b", "c"]
    check(aging, ["x", "y", "z"])  # 1+9, 4+9

    # age = 10 (from last removal of "c")


def test_a_negative_count_removes_nothing_as_scalas_take() -> None:
    """ohra's own: Scala's ``take`` returns nothing for a negative count."""
    frequency = FrequencyList[str](clock=TestClock()).update("a").update("b")
    assert frequency.remove_least_frequent(-1) == []
    assert frequency.remove_most_frequent(-1) == []
    assert frequency.remove_overall_least_recent(-1) == []
    assert frequency.remove_overall_most_recent(-1) == []
    assert frequency.size == 2


def test_overall_recency_without_a_clock_raises_pekkos_message() -> None:
    """ohra's own: Pekko's UnsupportedOperationException, FrequencyList.scala 171."""
    with pytest.raises(
        RuntimeError, match="^Overall recency is not enabled for this FrequencyList$"
    ):
        FrequencyList[str]().remove_overall_least_recent()
