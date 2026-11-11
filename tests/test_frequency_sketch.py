"""Port of Pekko's ``FrequencySketchSpec`` (actor-tests/.../util/FrequencySketchSpec.scala).

Cases, in Scala order. ``FrequencySketch``: ``test_frequency_sketch_increment_counters`` (25),
``..._increment_counters_to_max_value`` (39), ``..._reset_counters_when_reset_size_is_reached``
(47), ``..._compare_frequencies_for_more_popular_items_with_reasonable_accuracy`` (92),
``..._compare_frequencies_for_random_zipfian_distribution_with_reasonable_accuracy`` (100),
``..._allow_counter_size_to_be_configured_as_2_bits`` (129), ``..._8_bits`` (156),
``..._16_bits`` (183), ``..._32_bits`` (210), ``..._64_bits`` (234),
``..._allow_depth_to_be_configured`` (258),
``..._reject_capacity_that_would_cause_width_to_overflow_int`` (268),
``..._detect_width_overflow_that_would_have_been_missed_by_int_arithmetic`` (273),
``..._reject_capacity_that_would_cause_width_to_overflow_int_with_custom_width_multiplier``
(285). ``FastFrequencySketch``: the same names with ``test_fast_frequency_sketch_`` at 293, 307,
314, 359, 367, 396, 401 and 411.

Differences: ``an[IllegalArgumentException]`` is ``pytest.raises(ValueError)``. The width
overflow cases (273, 401) keep only the ``Long`` half, since Python ints do not overflow:
``4 * ceiling_power_of_two(536_870_913)`` exceeds ``Int.MaxValue``. ``ZipfianGenerator``
(actor-tests/.../util/ZipfianGenerator.scala) is ported below with ``random.Random(502539523)`` in
place of ``scala.util.Random(502539523)``, so its draws differ from the JVM's; and ties in the
actual frequencies sort in insertion order rather than ``mutable.Map``'s, which only matters to
the accuracy measure.

Added: ``test_string_hashes_are_javas_and_scalas`` pins ``String.hashCode`` and
``MurmurHash3.stringHash`` to values from the JVM, non-ASCII and overflowing ids included;
``test_the_fast_sketch_indexes_as_pekkos`` pins the fast sketch's rehash and row indexes; and
``test_a_zero_capacity_sketch_still_counts`` covers a shard limit of 0; and
``test_colliding_sketches_count_as_pekkos`` pins counts and a reset to values from the JVM.
"""

import math
import random

import pytest

from ohra.util.frequency_sketch import (
    _SEEDS,  # pyright: ignore[reportPrivateUsage]
    FastFrequencySketch,
    FrequencySketch,
    _fast_index,  # pyright: ignore[reportPrivateUsage]
    _fast_rehash,  # pyright: ignore[reportPrivateUsage]
    ceiling_power_of_two,
    java_hash_code,
    string_hash,
)

INT_MAX = 2**31 - 1


class ZipfianGenerator:
    """ZipfianGenerator.scala 20-57, from "Quickly Generating Billion-Record Synthetic
    Databases", Jim Gray et al. ``ZipfianGenerator(n)`` is Scala's ``apply(n: Int)``."""

    def __init__(self, n: int, theta: float = 0.99, seed: int = 502539523) -> None:
        self._min = 0
        self._n = n
        self._theta = theta
        self._alpha = 1.0 / (1.0 - theta)
        zeta2 = self._zeta(2, theta)
        self._zeta_n = self._zeta(n, theta)
        self._eta = (1 - math.pow(2.0 / n, 1 - theta)) / (1 - zeta2 / self._zeta_n)
        self._random = random.Random(seed)

    @staticmethod
    def _zeta(n: int, theta: float) -> float:
        return sum(1 / math.pow(i, theta) for i in range(1, n + 1))

    def next(self) -> int:
        u = self._random.random()
        uz = u * self._zeta_n
        if uz < 1.0:
            return self._min
        if uz < 1.0 + math.pow(0.5, self._theta):
            return self._min + 1
        return self._min + int(self._n * math.pow(self._eta * u - self._eta + 1, self._alpha))


type Sketch = FrequencySketch | FastFrequencySketch


def increment_counters(sketch: Sketch) -> None:
    sketch.increment("foo")
    sketch.increment("bar")
    sketch.increment("bar")
    sketch.increment("baz")
    sketch.increment("baz")
    sketch.increment("baz")
    assert sketch.frequency("foo") == 1
    assert sketch.frequency("bar") == 2
    assert sketch.frequency("baz") == 3
    assert sketch.size == 6  # total number of increments


def increment_counters_to_max_value(sketch: Sketch) -> None:
    for _ in range(20):
        sketch.increment("foo")
    assert sketch.frequency("foo") == 15
    assert sketch.size == 15  # total number of increments


def halved(sketch: Sketch, before: list[int], last: int) -> int:
    after = [sketch.frequency(str(i)) for i in range(1, last + 1)]
    return sum(1 for f1, f2 in zip(before, after, strict=True) if f2 == f1 // 2)


def reset_counters_when_reset_size_is_reached(sketch: Sketch) -> None:
    # increment counters until the reset size
    for i in range(1, 501):
        sketch.increment(str(i))
    for i in range(1, 500):
        sketch.increment(str(i))
    assert sketch.size == 999

    frequencies1 = [sketch.frequency(str(i)) for i in range(1, 500)]

    # the 1000th increment will trigger a reset operation (halving all counters)
    sketch.increment("500")
    assert sketch.size == 500  # all counters (including hash collisions) will be even

    # frequencies should be halved now (ignore value 500, the reset trigger); the trigger can
    # collide with up to four other counters
    assert halved(sketch, frequencies1, 499) >= 499 - 4

    # increment more values, creating odd counts and more hash collisions
    for i in range(501, 1000):
        sketch.increment(str(i))
    assert sketch.size == 999

    frequencies3 = [sketch.frequency(str(i)) for i in range(1, 1000)]

    # the 1000th increment will trigger a reset operation (halving counters)
    sketch.increment("1000")
    assert 300 < sketch.size < 500  # some counters will be odd numbers, rounded down when halved

    assert halved(sketch, frequencies3, 999) >= 999 - 4


def more_popular_items(sketch: Sketch) -> None:
    for i in range(1000, 10001):  # add some noise to the sketch
        sketch.increment(str(i))
    for i in range(1, 6):  # 1-5 are most popular, in order
        for _ in range((6 - i) * 2):
            sketch.increment(str(i))
    for i in range(1, 6):
        assert sketch.frequency(str(i)) >= sketch.frequency(str(i + 1))
    for i in range(6, 11):
        assert sketch.frequency("5") >= sketch.frequency(str(i))


def random_zipfian_distribution(sketch: Sketch) -> None:
    number_of_ids = 1000
    most_popular = 100
    zipfian = ZipfianGenerator(number_of_ids)  # lower numbers are more popular
    actual_frequencies: dict[int, int] = {}
    for _ in range(100 * number_of_ids):
        id_ = zipfian.next()
        sketch.increment(str(id_))
        actual_frequencies[id_] = actual_frequencies.get(id_, 0) + 1
    # compare the most popular item frequencies with every other frequency, using the order of
    # the actual frequency counts
    sorted_actual = sorted(actual_frequencies.items(), key=lambda kv: -kv[1])
    comparisons = 0
    correct = 0
    for i in range(most_popular):
        frequency = sketch.frequency(str(sorted_actual[i][0]))
        for j in range(i + 1, len(sorted_actual)):
            other_frequency = sketch.frequency(str(sorted_actual[j][0]))
            if frequency >= other_frequency:
                correct += 1
            comparisons += 1
    accuracy = correct / comparisons
    assert accuracy > 0.95  # depends on the hash collisions, and random distribution


def configured_counter_size(
    sketch: FrequencySketch, max_value: int, max_increments: int, reset_size: int
) -> None:
    increment_counters(sketch)
    if max_increments:
        # check max value
        for _ in range(max_increments):
            sketch.increment("foo")
        assert sketch.frequency("foo") == max_value
        assert sketch.size == 6 + max_value - 1  # total increments
    # check reset
    for i in range(1, reset_size - sketch.size):  # up to reset size
        sketch.increment(str(i))
    assert sketch.size == reset_size - 1
    sketch.increment("qux")  # trigger reset
    assert sketch.size <= reset_size // 2


# FrequencySketch


def test_frequency_sketch_increment_counters() -> None:
    increment_counters(FrequencySketch.create(capacity=100))


def test_frequency_sketch_increment_counters_to_max_value() -> None:
    # default of 4 bit counters, max value = 15
    increment_counters_to_max_value(FrequencySketch.create(capacity=100))


def test_frequency_sketch_reset_counters_when_reset_size_is_reached() -> None:
    reset_counters_when_reset_size_is_reached(
        FrequencySketch.create(capacity=100, reset_multiplier=10)
    )


def test_frequency_sketch_compare_frequencies_for_more_popular_items_with_reasonable_accuracy():
    more_popular_items(FrequencySketch.create(capacity=100, reset_multiplier=10))


def test_frequency_sketch_compare_frequencies_for_random_zipfian_distribution_with_reasonable_accuracy():  # noqa: E501
    random_zipfian_distribution(FrequencySketch.create(capacity=100))


def test_frequency_sketch_allow_counter_size_to_be_configured_as_2_bits() -> None:
    sketch = FrequencySketch.create(capacity=100, counter_bits=2, reset_multiplier=1)
    configured_counter_size(sketch, max_value=3, max_increments=10, reset_size=100)


def test_frequency_sketch_allow_counter_size_to_be_configured_as_8_bits() -> None:
    sketch = FrequencySketch.create(capacity=100, counter_bits=8)
    configured_counter_size(sketch, max_value=255, max_increments=1000, reset_size=1000)


def test_frequency_sketch_allow_counter_size_to_be_configured_as_16_bits() -> None:
    sketch = FrequencySketch.create(capacity=100, counter_bits=16, reset_multiplier=1000)
    configured_counter_size(sketch, max_value=65535, max_increments=100000, reset_size=100000)


def test_frequency_sketch_allow_counter_size_to_be_configured_as_32_bits() -> None:
    # max value is getting big to check, assume it's working; reset at 1000
    sketch = FrequencySketch.create(capacity=100, counter_bits=32)
    configured_counter_size(sketch, max_value=0, max_increments=0, reset_size=1000)


def test_frequency_sketch_allow_counter_size_to_be_configured_as_64_bits() -> None:
    # max value is getting big to check, assume it's working; reset at 1000
    sketch = FrequencySketch.create(capacity=100, counter_bits=64)
    configured_counter_size(sketch, max_value=0, max_increments=0, reset_size=1000)


def test_frequency_sketch_allow_depth_to_be_configured() -> None:
    # a frequency sketch with just a single counter and hash function, which will always collide
    sketch = FrequencySketch.create(capacity=1, width_multiplier=1, counter_bits=64, depth=1)
    sketch.increment("foo")
    sketch.increment("bar")
    assert sketch.frequency("foo") == 2
    assert sketch.frequency("bar") == 2


def test_frequency_sketch_reject_capacity_that_would_cause_width_to_overflow_int() -> None:
    # ceilingPowerOfTwo gives 2^30 = 1073741824, times 4 overflows
    with pytest.raises(ValueError, match="exceeds Int.MaxValue"):
        FrequencySketch.create(capacity=536_870_913)


def test_frequency_sketch_detect_width_overflow_that_would_have_been_missed_by_int_arithmetic():
    assert 4 * ceiling_power_of_two(536_870_913) > INT_MAX


def test_frequency_sketch_reject_capacity_that_would_cause_width_to_overflow_int_with_custom_width_multiplier():  # noqa: E501
    with pytest.raises(ValueError, match="exceeds Int.MaxValue"):
        FrequencySketch.create(capacity=100, width_multiplier=INT_MAX)


# FastFrequencySketch


def test_fast_frequency_sketch_increment_counters() -> None:
    increment_counters(FastFrequencySketch.create(capacity=100))


def test_fast_frequency_sketch_increment_counters_to_max_value() -> None:
    increment_counters_to_max_value(FastFrequencySketch.create(capacity=100))


def test_fast_frequency_sketch_reset_counters_when_reset_size_is_reached() -> None:
    reset_counters_when_reset_size_is_reached(
        FastFrequencySketch.create(capacity=100, reset_multiplier=10)
    )


def test_fast_frequency_sketch_compare_frequencies_for_more_popular_items_with_reasonable_accuracy():  # noqa: E501
    more_popular_items(FastFrequencySketch.create(capacity=100, reset_multiplier=10))


def test_fast_frequency_sketch_compare_frequencies_for_random_zipfian_distribution_with_reasonable_accuracy():  # noqa: E501
    random_zipfian_distribution(FastFrequencySketch.create(capacity=100))


def test_fast_frequency_sketch_reject_capacity_that_would_cause_width_to_overflow_int() -> None:
    with pytest.raises(ValueError, match="exceeds Int.MaxValue"):
        FastFrequencySketch.create(capacity=536_870_913)


def test_fast_frequency_sketch_detect_width_overflow_that_would_have_been_missed_by_int_arithmetic():  # noqa: E501
    assert 4 * ceiling_power_of_two(536_870_913) > INT_MAX


def test_fast_frequency_sketch_reject_capacity_that_would_cause_width_to_overflow_int_with_custom_width_multiplier():  # noqa: E501
    with pytest.raises(ValueError, match="exceeds Int.MaxValue"):
        FastFrequencySketch.create(capacity=100, width_multiplier=INT_MAX)


# Added


@pytest.mark.parametrize(
    ("value", "java", "murmur"),
    [
        ("", 0, -1699863197),
        ("a", 97, 329321386),
        ("foo", 101574, 537187922),
        ("polygenelubricants", -2147483648, 154744167),
        ("é", 233, -1993592674),
        ("\U0001d11e", 1772394, 935167160),
        ("17", 1574, -1161360053),
    ],
)
def test_string_hashes_are_javas_and_scalas(value: str, java: int, murmur: int):
    assert java_hash_code(value) == java
    assert string_hash(value) == murmur


def test_the_fast_sketch_indexes_as_pekkos() -> None:
    assert _fast_rehash(101574) == 68644682

    def rows(value: str) -> list[int]:
        return [_fast_index(_fast_rehash(java_hash_code(value)), s, 63) for s in _SEEDS]

    assert rows("foo") == [29, 29, 56, 53]
    assert rows("17") == [63, 21, 36, 32]


def test_a_zero_capacity_sketch_still_counts() -> None:
    assert ceiling_power_of_two(0) == 1
    sketch = FastFrequencySketch.create(0)
    sketch.increment("a")
    sketch.increment("a")
    assert sketch.frequency("a") == 2
    assert sketch.size == 2


def test_colliding_sketches_count_as_pekkos() -> None:
    # JVM values (pekko-actor 1.4.0): width 16, so every id collides; 45 increments pass the
    # reset at 40, and the 2-bit counters saturate first
    generic = FrequencySketch.create(capacity=4)
    fast = FastFrequencySketch.create(capacity=4)
    two_bits = FrequencySketch.create(capacity=4, counter_bits=2, depth=3)
    for i in range(1, 46):
        id_ = f"{i * 7 % 13}é"
        generic.increment(id_)
        fast.increment(id_)
        two_bits.increment(id_)
    ids = [f"{i}é" for i in range(13)]
    assert generic.size == 22
    assert [generic.frequency(i) for i in ids] == [1, 4, 2, 2, 1, 1, 1, 2, 2, 2, 1, 1, 1]
    assert fast.size == 22
    assert [fast.frequency(i) for i in ids] == [1, 2, 2, 2, 1, 1, 1, 2, 2, 2, 1, 1, 1]
    assert two_bits.size == 39
    assert [two_bits.frequency(i) for i in ids] == [3] * 13
