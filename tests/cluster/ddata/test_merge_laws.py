"""Merge laws every ReplicatedData must keep: merge is commutative, associative and idempotent.

Not a Pekko spec.
"""

import json
import random
from collections.abc import Callable
from typing import Any, NamedTuple

import pytest

from ohra.cluster.ddata.flag import Flag
from ohra.cluster.ddata.gcounter import GCounter
from ohra.cluster.ddata.gset import GSet
from ohra.cluster.ddata.lww_map import LWWMap
from ohra.cluster.ddata.lww_register import LWWRegister
from ohra.cluster.ddata.ormap import ORMap
from ohra.cluster.ddata.ormultimap import ORMultiMap
from ohra.cluster.ddata.orset import ORSet
from ohra.cluster.ddata.pncounter import PNCounter
from ohra.cluster.ddata.pncounter_map import PNCounterMap
from ohra.cluster.ddata.replicated_data import ReplicatedData
from ohra.cluster.ddata.version_vector import VersionVector
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")
node3 = UniqueAddress("localhost", 7356, 3, "Sys")


class Case(NamedTuple):
    name: str
    empty: ReplicatedData
    op: Callable[[random.Random, UniqueAddress, Any], Any]


def clock(current: int, _value: object) -> int:
    # Deterministic, and it ties: two nodes writing from one state get the same timestamp.
    return current + 1


def _pn(rng: random.Random, node: UniqueAddress, s: PNCounter) -> PNCounter:
    n = rng.randint(1, 5)
    return s._increment(node, n) if rng.random() < 0.5 else s._decrement(node, n)


def _orset(rng: random.Random, node: UniqueAddress, s: ORSet[int]) -> ORSet[int]:
    e = rng.randint(0, 5)
    return s._add(node, e) if rng.random() < 0.5 else s._remove(node, e)


def _ormap(rng: random.Random, node: UniqueAddress, s: ORMap[int, GCounter]) -> Any:
    k = rng.randint(0, 3)
    if rng.random() < 0.7:
        return s._update(node, k, GCounter(), lambda c: c._increment(node, rng.randint(1, 5)))
    return s._remove(node, k)


def _lww_map(rng: random.Random, node: UniqueAddress, s: LWWMap[int, int]) -> Any:
    k = rng.randint(0, 3)
    return s._put(node, k, rng.randint(0, 9), clock) if rng.random() < 0.7 else s._remove(node, k)


def _pn_map(rng: random.Random, node: UniqueAddress, s: PNCounterMap[int]) -> Any:
    k, n = rng.randint(0, 3), rng.randint(1, 5)
    r = rng.random()
    if r < 0.4:
        return s._increment(node, k, n)
    return s._decrement(node, k, n) if r < 0.7 else s._remove(node, k)


def _multimap(rng: random.Random, node: UniqueAddress, s: ORMultiMap[int, int]) -> Any:
    k, e = rng.randint(0, 3), rng.randint(0, 3)
    return s._add_binding(node, k, e) if rng.random() < 0.6 else s._remove_binding(node, k, e)


CASES: list[Case] = [
    Case("GCounter", GCounter(), lambda rng, node, s: s._increment(node, rng.randint(1, 5))),
    Case("PNCounter", PNCounter(), _pn),
    Case("GSet", GSet(), lambda rng, node, s: s.add(rng.randint(0, 9))),
    Case("Flag", Flag(), lambda rng, node, s: s.switch_on() if rng.random() < 0.3 else s),
    Case(
        "LWWRegister",
        LWWRegister._create(node1, 0, clock),
        lambda rng, node, s: s._with_value(node, rng.randint(0, 9), clock),
    ),
    Case("ORSet", ORSet(), _orset),
    Case("VersionVector", VersionVector.empty, lambda rng, node, s: s._increment(node)),
    Case("ORMap", ORMap(), _ormap),
    Case("LWWMap", LWWMap(), _lww_map),
    Case("PNCounterMap", PNCounterMap(), _pn_map),
    Case("ORMultiMap", ORMultiMap.empty, _multimap),
    # not ORMultiMap.empty_with_value_deltas: its merge is not associative, as in Pekko; see
    # test_ormultimap.py's test_a_removal_with_value_deltas_depends_on_the_merge_order
]


def _never(*_a: object) -> Any:
    raise AssertionError("resolver called")


def rt(v: Any) -> Any:
    # a decoded copy: equal, but not the same object, so merge takes no shortcut
    return Serialization.decode(json.loads(json.dumps(Serialization.encode(v, _never))), _never)


def norm(v: Any) -> Any:
    return v.reset_delta() if hasattr(v, "reset_delta") else v


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_merge_is_commutative_associative_and_idempotent(case: Case, seed: int):
    rng = random.Random(seed)
    common = case.empty
    for _ in range(3):
        common = case.op(rng, node1, common)
    states: list[Any] = []
    for node in (node1, node2, node3):
        s = common
        for _ in range(rng.randint(0, 6)):
            s = case.op(rng, node, s)
        states.append(s)
    a, b, c = states
    assert norm(a.merge(b)) == norm(b.merge(a))
    assert norm(a.merge(b).merge(c)) == norm(a.merge(b.merge(c)))
    assert norm(a.merge(rt(a))) == norm(a)
