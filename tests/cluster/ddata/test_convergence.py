"""Replicas gossiping over a lossy, partitioned, duplicating network converge to one value.

Not a Pekko spec. The wire is the JSON of ``Serialization``, so a merge or an encoding bug shows
here. The network is a deterministic simulation: a failure's id names its seed.
"""

import json
import random
from collections.abc import Callable
from typing import Any, NamedTuple

import pytest

from ohra.cluster.ddata.flag import Flag
from ohra.cluster.ddata.gcounter import GCounter
from ohra.cluster.ddata.gset import GSet
from ohra.cluster.ddata.lww_register import LWWRegister, default_clock
from ohra.cluster.ddata.pncounter import PNCounter
from ohra.cluster.ddata.replicated_data import DeltaReplicatedData, ReplicatedData
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

TICKS = 200
SPLIT_AT = 50
HEAL_AT = 150


def _no_ref(*_a: Any) -> Any:
    raise AssertionError("no refs on the wire")


class SimNetwork:
    """Holds JSON payloads for a few ticks. Drops 20%, duplicates 10%, and drops everything
    between two partition groups."""

    def __init__(self, rng: random.Random, n: int) -> None:
        self.rng = rng
        self.n = n
        self.now = 0
        self._queue: list[tuple[int, int, int, str]] = []
        self._group: dict[int, int] = {}

    def partition(self, groups: list[set[int]]) -> None:
        self._group = {i: g for g, members in enumerate(groups) for i in members}

    def heal(self) -> None:
        self._group = {}

    def _cut(self, src: int, dst: int) -> bool:
        return self._group.get(src) != self._group.get(dst)

    def send(self, src: int, dst: int, payload: str) -> None:
        if self.rng.random() < 0.2:
            return
        copies = 2 if self.rng.random() < 0.1 else 1
        for _ in range(copies):
            self._queue.append((self.now + self.rng.randint(0, 5), src, dst, payload))

    def deliver(self) -> list[tuple[int, int, str]]:
        """The messages due now, shuffled. Advances the clock by one tick."""
        due = [m for m in self._queue if m[0] <= self.now]
        self._queue = [m for m in self._queue if m[0] > self.now]
        self.now += 1
        out = [(s, d, p) for _, s, d, p in due if not self._cut(s, d)]
        self.rng.shuffle(out)
        return out


class ConvergenceCase(NamedTuple):
    name: str
    empty: ReplicatedData
    op: Callable[[random.Random, UniqueAddress, Any], Any]
    causal: bool
    check: Callable[[Any, list[tuple[Any, Any]]], None]


def _check_counter(value: Any, log: list[tuple[Any, Any]]) -> None:
    assert value.value == sum(after.value - before.value for before, after in log)


def _check_gset(value: Any, log: list[tuple[Any, Any]]) -> None:
    assert value.elements == frozenset().union(*(a.elements for _, a in log))


def _check_flag(value: Any, log: list[tuple[Any, Any]]) -> None:
    assert value.enabled == any(a.enabled for _, a in log)


def _check_lww(value: Any, log: list[tuple[Any, Any]]) -> None:
    # Pekko's rule: the larger timestamp wins, the lower node on a tie.
    best = min(((-a.timestamp, a.updated_by, a.value) for _, a in log), key=lambda t: t[:2])
    assert (value.timestamp, value.updated_by, value.value) == (-best[0], best[1], best[2])


node1 = UniqueAddress("localhost", 7354, 1, "Sys")

CONVERGENCE: list[ConvergenceCase] = [
    ConvergenceCase(
        "GCounter",
        GCounter(),
        lambda rng, node, s: s._increment(node, rng.randint(1, 5)),
        False,
        _check_counter,
    ),
    ConvergenceCase(
        "PNCounter",
        PNCounter(),
        lambda rng, node, s: (
            s._increment(node, rng.randint(1, 5))
            if rng.random() < 0.5
            else s._decrement(node, rng.randint(1, 5))
        ),
        False,
        _check_counter,
    ),
    ConvergenceCase(
        "GSet", GSet(), lambda rng, node, s: s.add(rng.randint(0, 9)), False, _check_gset
    ),
    ConvergenceCase(
        "Flag",
        Flag(),
        lambda rng, node, s: s.switch_on() if rng.random() < 0.5 else s,
        False,
        _check_flag,
    ),
    ConvergenceCase(
        "LWWRegister",
        LWWRegister._create(node1, 0, default_clock),
        lambda rng, node, s: s._with_value(node, rng.randint(0, 9), default_clock),
        False,
        _check_lww,
    ),
]


def run(case: ConvergenceCase, seed: int) -> tuple[list[Any], list[tuple[Any, Any]]]:
    rng = random.Random(seed)
    n = rng.randint(3, 5)
    nodes = [UniqueAddress("localhost", 7354 + i, i + 1, "Sys") for i in range(n)]
    states: list[Any] = [case.empty] * n
    net = SimNetwork(rng, n)
    log: list[tuple[Any, Any]] = []
    has_deltas = isinstance(case.empty, DeltaReplicatedData)
    seqs = [0] * n  # causal: the last delta number each replica sent
    last_applied = [[0] * n for _ in range(n)]  # causal: [receiver][sender]

    def pack(src: int, seq: int, is_delta: bool, v: Any) -> str:
        return json.dumps(
            {"src": src, "seq": seq, "delta": is_delta, "v": Serialization.encode(v, _no_ref)}
        )

    def receive(dst: int, payload: str) -> None:
        m = json.loads(payload)
        v = Serialization.decode(m["v"], _no_ref)
        if not m["delta"]:
            states[dst] = states[dst].merge(v)
        elif not case.causal or m["seq"] == last_applied[dst][m["src"]] + 1:
            # ponytail: stands in for the Replicator's causal delivery, not a copy of it
            last_applied[dst][m["src"]] = m["seq"]
            states[dst] = states[dst].merge_delta(v)

    for tick in range(TICKS):
        if tick == SPLIT_AT:
            net.partition([set(range(n // 2)), set(range(n // 2, n))])
        if tick == HEAL_AT:
            net.heal()
        for src, dst, payload in net.deliver():
            receive(dst, payload)
        for i in range(n):
            if rng.random() < 0.5:
                after = case.op(rng, nodes[i], states[i])
                log.append((states[i], after))
                states[i] = after
            dst = rng.choice([j for j in range(n) if j != i])
            if not has_deltas or rng.random() < 0.3:
                net.send(i, dst, pack(i, 0, False, states[i]))
            elif states[i].delta is not None:
                seqs[i] += 1
                net.send(i, dst, pack(i, seqs[i], True, states[i].delta))
                states[i] = states[i].reset_delta()

    # The final exchange: everyone's full state reaches everyone, with no faults.
    payloads = [pack(i, 0, False, states[i]) for i in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                receive(j, payloads[i])
    return states, log


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("case", CONVERGENCE, ids=lambda c: c.name)
def test_replicas_converge(case: ConvergenceCase, seed: int):
    states, log = run(case, seed)
    norm = [s.reset_delta() if hasattr(s, "reset_delta") else s for s in states]
    assert all(s == norm[0] for s in norm), f"{case.name} seed {seed}: {norm}"
    case.check(norm[0], log)
