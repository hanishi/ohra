"""Ported from Apache Pekko's LWWRegisterSpec."""

import itertools

from ohra.cluster.ddata import SelfUniqueAddress
from ohra.cluster.ddata.lww_register import (
    LWWRegister,
    LWWRegisterKey,
    default_clock,
    reverse_clock,
)
from ohra.remote import UniqueAddress

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")


def test_use_latest_of_successive_assignments():
    r = LWWRegister._create(node1, 0, default_clock)
    for n in range(1, 101):
        assert r.value == n - 1
        r = r._with_value(node1, n, default_clock)
    assert r.value == 100


def test_merge_by_picking_max_timestamp():
    i = itertools.count(100)

    def clock(_current: int, _value: str) -> int:
        return next(i)

    r1 = LWWRegister._create(node1, "A", clock)
    assert r1.timestamp == 100
    r2 = r1._with_value(node2, "B", clock)
    assert r2.timestamp == 101
    m1 = r1.merge(r2)
    assert m1.value == "B"
    assert m1.timestamp == 101
    m2 = r2.merge(r1)
    assert m2.value == "B"
    assert m2.timestamp == 101


def test_merge_by_picking_least_address_when_same_timestamp():
    def clock(_current: int, _value: str) -> int:
        return 100

    r1 = LWWRegister._create(node1, "A", clock)
    r2 = LWWRegister._create(node2, "B", clock)
    m1 = r1.merge(r2)
    assert m1.value == "A"
    m2 = r2.merge(r1)
    assert m2.value == "A"


def test_use_monotonically_increasing_default_clock():
    node = SelfUniqueAddress(node1)
    r = LWWRegister.create(node, 0)
    for n in range(1, 101):
        assert r.value == n - 1
        r2 = r.with_value(node, n)
        assert r2.timestamp > r.timestamp
        r = r2


def test_have_unapply_extractor():
    # Pekko matches on LWWRegister(value) and Changed(LWWRegisterKey("key")); ohra has the
    # value and the key's identity.
    r1 = LWWRegister._create(node1, "a", default_clock)
    assert r1.value == "a"
    assert LWWRegisterKey("key") == LWWRegisterKey("key")
    assert LWWRegister._create(node2, "x", default_clock).updated_by == node2


def test_can_be_used_as_first_write_wins_register():
    r = LWWRegister._create(node1, 0, reverse_clock)
    for n in range(1, 101):
        assert r.value == 0
        new_register = r.merge(r._with_value(node1, n, reverse_clock))
        assert new_register == r
        r = new_register
    assert r.value == 0


def test_equal_timestamps_go_to_the_lower_node():
    def same(_t: int, _v: str) -> int:
        return 7

    low = UniqueAddress("localhost", 7354, 1, "Sys")
    by_port = UniqueAddress("localhost", 7355, 0, "Sys")  # port outranks uid
    by_uid = UniqueAddress("localhost", 7354, 2, "Sys")
    for high in (by_port, by_uid):
        a = LWWRegister._create(low, "low", same)
        b = LWWRegister._create(high, "high", same)
        assert a.merge(b).value == "low" and b.merge(a).value == "low"
    # host outranks port: the lower host wins over a lower port
    low_host = UniqueAddress("a.example", 7999, 9, "Sys")
    a = LWWRegister._create(low_host, "low", same)
    b = LWWRegister._create(low, "high", same)
    assert a.merge(b).value == "low" and b.merge(a).value == "low"
