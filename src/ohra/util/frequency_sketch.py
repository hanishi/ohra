"""Port of Pekko's ``FrequencySketch`` and ``FastFrequencySketch``
(actor/.../util/FrequencySketch.scala 1-383), the count-min sketches behind TinyLFU admission.

The arithmetic is the JVM's, bit for bit: a Scala ``Int`` wraps at signed 32 bits, a ``Long`` at
64 bits. Counter words are unsigned 64-bit ints masked with ``_M64`` after each ``+``, ``*`` and
``<<``, so ``>>>`` is ``>>`` on the masked value; a Java ``<< 64`` is ``<< 0``, which only adds
bits ``oddMask`` and ``resetMask`` already hold, so the mask (which drops them) gives the same
masks. Ids hash over their UTF-16 code units, as Java strings do. Only ``String`` values are
hashed: Pekko's ``Hasher[A]`` has only ``StringHasher``. ``toDebugString`` is not ported.
``size`` and the row sizes do not wrap: Pekko's would only after 2**31 updates with no reset,
which takes a reset size of 0 or less.
"""

from __future__ import annotations

import math

_M32 = 0xFFFF_FFFF
_M64 = 0xFFFF_FFFF_FFFF_FFFF
_INT_MAX = 2**31 - 1


def _i32(x: int) -> int:
    """A Java ``Int``: the low 32 bits, signed."""
    x &= _M32
    return x - (1 << 32) if x & 0x8000_0000 else x


def _utf16(value: str) -> list[int]:
    data = value.encode("utf-16-le", "surrogatepass")
    return [data[i] | data[i + 1] << 8 for i in range(0, len(data), 2)]


def java_hash_code(value: str) -> int:
    """Java's ``String.hashCode`` (FastFrequencySketch's ``value.hashCode``, Scala 313)."""
    h = 0
    for c in _utf16(value):
        h = (31 * h + c) & _M32
    return _i32(h)


def _rotl(x: int, r: int) -> int:
    return ((x << r) | (x >> (32 - r))) & _M32


def _mix_last(h: int, k: int) -> int:
    """Scala 2.13's ``MurmurHash3.mixLast``, on unsigned 32-bit values."""
    k = (k * 0xCC9E2D51) & _M32
    k = _rotl(k, 15)
    k = (k * 0x1B873593) & _M32
    return h ^ k


def _mix(h: int, k: int) -> int:
    h = _rotl(_mix_last(h, k), 13)
    return (h * 5 + 0xE6546B64) & _M32


def _finalize_hash(h: int, length: int) -> int:
    """``MurmurHash3.finalizeHash``: ``avalanche(hash ^ length)``, signed."""
    h ^= length & _M32
    h ^= h >> 16
    h = (h * 0x85EBCA6B) & _M32
    h ^= h >> 13
    h = (h * 0xC2B2AE35) & _M32
    h ^= h >> 16
    return _i32(h)


def string_hash(value: str, seed: int = 135283237) -> int:
    """Scala 2.13's ``MurmurHash3.stringHash(str, seed)``: Pekko's ``Hasher.StringHasher`` with
    ``DefaultSeed`` (FrequencySketch.scala 71-84)."""
    chars = _utf16(value)
    h = seed & _M32
    i = 0
    while i + 1 < len(chars):
        h = _mix(h, (chars[i] << 16) + chars[i + 1])
        i += 2
    if i < len(chars):
        h = _mix_last(h, chars[i])
    return _finalize_hash(h, len(chars))


def _number_of_leading_zeros(i: int) -> int:
    """Java's ``Integer.numberOfLeadingZeros``: of a negative ``Int``, 0."""
    return 32 - (i & _M32).bit_length()


def is_power_of_two(i: int) -> bool:
    """``Bits.isPowerOfTwo`` (Scala 87)."""
    return (i & _i32(i - 1)) == 0


def power_of_two_exponent(i: int) -> int:
    """``Bits.powerOfTwoExponent`` (Scala 89)."""
    return 32 - _number_of_leading_zeros(_i32(i - 1))


def ceiling_power_of_two(i: int) -> int:
    """``Bits.ceilingPowerOfTwo`` (Scala 91): Java masks the shift distance with ``& 31``."""
    return _i32(1 << (-_number_of_leading_zeros(_i32(i - 1)) & 31))


def _double_to_long(x: float) -> int:
    """Java's ``Double.toLong``: truncates, saturates, and NaN is 0."""
    if math.isnan(x):
        return 0
    if math.isinf(x):
        return _M64 >> 1 if x > 0 else -(1 << 63)
    return max(-(1 << 63), min(_M64 >> 1, int(x)))


def _width_and_reset_size(
    capacity: int, width_multiplier: int, reset_multiplier: float
) -> tuple[int, int]:
    """The shared body of both ``apply`` factories (Scala 63-67, 262-266)."""
    width_long = width_multiplier * ceiling_power_of_two(capacity)
    if width_long > _INT_MAX:
        raise ValueError(
            f"computed width ({width_long}) exceeds Int.MaxValue; "
            "reduce capacity or widthMultiplier"
        )
    reset_size = _i32(min(_INT_MAX, _double_to_long(reset_multiplier * capacity)))
    return _i32(width_long), reset_size


def _reset(matrix: list[list[int]], row_sizes: list[int], odd_mask: int, reset_mask: int) -> int:
    """The TinyLFU reset (Scala 228-242, 365-379): halves every counter, and corrects each
    row's size for the odd counters truncated. Returns the new ``updatedSize``."""
    for row, words in enumerate(matrix):
        odd = 0
        for column, word in enumerate(words):
            odd += (word & odd_mask).bit_count()
            words[column] = (word >> 1) & reset_mask
        row_sizes[row] = ((row_sizes[row] - odd) & _M32) >> 1
    return max(row_sizes)


class FrequencySketch:
    """FrequencySketch.scala 129-245: a count-min sketch with a configurable depth and counter
    size, whose rows combine two hashes as gi(x) = h1(x) + i * h2(x)."""

    @staticmethod
    def create(
        capacity: int,
        width_multiplier: int = 4,
        reset_multiplier: float = 10.0,
        depth: int = 4,
        counter_bits: int = 4,
    ) -> FrequencySketch:
        """Scala 57-69."""
        width, reset_size = _width_and_reset_size(capacity, width_multiplier, reset_multiplier)
        return FrequencySketch(depth, width, counter_bits, reset_size)

    def __init__(self, depth: int, width: int, counter_bits: int, reset_size: int) -> None:
        if not is_power_of_two(width):
            raise ValueError("width must be a power of two")
        if counter_bits not in (2, 4, 8, 16, 32, 64):
            raise ValueError("counterBits must be 2, 4, 8, 16, 32, or 64 bits")
        self._depth = depth
        self._reset_size = reset_size
        counter_width = counter_bits
        slots = 64 // counter_width
        self._row_width = max(1, int(width / slots))
        self._column_mask = _i32(width - 1)
        self._slot_shift = power_of_two_exponent(slots)
        self._slot_mask = slots - 1
        self._counter_shift = power_of_two_exponent(counter_width)
        self._counter_mask = _M64 >> 1 if counter_bits == 64 else (1 << counter_width) - 1
        self._odd_mask = 1
        for count in range(1, slots + 1):
            self._odd_mask |= (1 << (count * counter_width)) & _M64
        counter_reset_mask = self._counter_mask >> 1
        self._reset_mask = counter_reset_mask
        for count in range(1, slots + 1):
            self._reset_mask |= (counter_reset_mask << (count * counter_width)) & _M64
        self._matrix = [[0] * self._row_width for _ in range(depth)]
        self._row_sizes = [0] * depth
        self._updated_size = 0

    @property
    def size(self) -> int:
        """Scala 164: the number of incremented counters."""
        return self._updated_size

    def frequency(self, value: str) -> int:
        """Scala 170-181."""
        hash1 = string_hash(value)
        hash2 = self._rehash(hash1)
        min_count = _INT_MAX
        for row in range(self._depth):
            min_count = min(min_count, self._get_counter(row, _i32(hash1 + row * hash2)))
        return min_count

    def increment(self, value: str) -> None:
        """Scala 187-201."""
        hash1 = string_hash(value)
        hash2 = self._rehash(hash1)
        updated = False
        for row in range(self._depth):
            updated |= self._increment_counter(row, _i32(hash1 + row * hash2))
        if updated:
            self._updated_size += 1
            if self._updated_size == self._reset_size:
                self._updated_size = _reset(
                    self._matrix, self._row_sizes, self._odd_mask, self._reset_mask
                )

    @staticmethod
    def _rehash(hash: int) -> int:
        """Scala 203-204."""
        h = hash & _M32
        return _finalize_hash(_mix_last(h, h), 2)

    def _get_counter(self, row: int, hash: int) -> int:
        """Scala 206-210."""
        column = ((hash & self._column_mask) & _M32) >> self._slot_shift
        slot = (hash & self._slot_mask) << self._counter_shift
        return _i32((self._matrix[row][column] >> slot) & self._counter_mask)

    def _increment_counter(self, row: int, hash: int) -> bool:
        """Scala 212-221."""
        column = ((hash & self._column_mask) & _M32) >> self._slot_shift
        slot = (hash & self._slot_mask) << self._counter_shift
        mask = (self._counter_mask << slot) & _M64
        words = self._matrix[row]
        if words[column] & mask != mask:
            words[column] = (words[column] + (1 << slot)) & _M64
            self._row_sizes[row] += 1
            return True
        return False


# FastFrequencySketch's seeds (Scala 297-301): large primes between 2^63 and 2^64.
_SEEDS = (0xC3A5C85C97CB3127, 0xB492B66FBE98F273, 0x9AE16A3B2F90404F, 0xCBF29CE484222325)


def _fast_rehash(hash: int) -> int:
    """Scala 333-341: a low-bias hash found by Hash Function Prospector."""
    x = hash & _M32
    x = (((x >> 15) ^ x) * 0xD168AAAD) & _M32
    x = (((x >> 15) ^ x) * 0xAF723597) & _M32
    return _i32((x >> 15) ^ x)


def _fast_index(hash: int, seed: int, index_mask: int) -> int:
    """Scala 343-346: the ``Int`` hash is sign-extended onto the ``Long`` seed."""
    x = ((hash + seed) * seed) & _M64
    return _i32(x + (x >> 32)) & index_mask


class FastFrequencySketch:
    """FrequencySketch.scala 286-383: depth 4 and 4-bit counters as constants, and its own
    rehash, after Caffeine's ``FrequencySketch``."""

    @staticmethod
    def create(
        capacity: int, width_multiplier: int = 4, reset_multiplier: float = 10.0
    ) -> FastFrequencySketch:
        """Scala 261-268."""
        width, reset_size = _width_and_reset_size(capacity, width_multiplier, reset_multiplier)
        return FastFrequencySketch(width, reset_size)

    def __init__(self, width: int, reset_size: int) -> None:
        if not is_power_of_two(width):
            raise ValueError("width must be a power of two")
        self._reset_size = reset_size
        self._row_width = max(1, (width & _M32) >> 4)
        self._index_mask = _i32(width - 1)
        self._matrix = [[0] * self._row_width for _ in range(4)]
        self._row_sizes = [0] * 4
        self._updated_size = 0

    @property
    def size(self) -> int:
        """Scala 310."""
        return self._updated_size

    def frequency(self, value: str) -> int:
        """Scala 312-319."""
        hash = _fast_rehash(java_hash_code(value))
        return min(
            self._get_counter(row, _fast_index(hash, seed, self._index_mask))
            for row, seed in enumerate(_SEEDS)
        )

    def increment(self, value: str) -> None:
        """Scala 321-331."""
        hash = _fast_rehash(java_hash_code(value))
        updated = False
        for row, seed in enumerate(_SEEDS):
            updated |= self._increment_counter(row, _fast_index(hash, seed, self._index_mask))
        if updated:
            self._updated_size += 1
            if self._updated_size == self._reset_size:
                self._updated_size = _reset(
                    self._matrix, self._row_sizes, 0x1111111111111111, 0x7777777777777777
                )

    def _get_counter(self, row: int, index: int) -> int:
        """Scala 348-352."""
        column = (index & _M32) >> 4
        slot = (index & 0xF) << 2
        return (self._matrix[row][column] >> slot) & 0xF

    def _increment_counter(self, row: int, index: int) -> bool:
        """Scala 354-363."""
        column = (index & _M32) >> 4
        slot = (index & 0xF) << 2
        mask = 0xF << slot
        words = self._matrix[row]
        if words[column] & mask != mask:
            words[column] = (words[column] + (1 << slot)) & _M64
            self._row_sizes[row] += 1
            return True
        return False
