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
from ohra.cluster.ddata.lww_register import LWWRegister
from ohra.cluster.ddata.orset import ORSet
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
    check: Callable[[Any, list[Any]], None]


# A log entry is (the op applied to case.empty, the op's result on the replica). The first holds
# only what the op itself wrote, so the checks never read merged-in data. LWWRegister reads the
# second: a write is the whole register, its timestamp comes from the replica's clock.


def _check_counter(value: Any, log: list[Any]) -> None:
    assert value.value == sum(alone.value for alone, _ in log)


def _check_gset(value: Any, log: list[Any]) -> None:
    assert value.elements == frozenset().union(*(alone.elements for alone, _ in log))


def _check_flag(value: Any, log: list[Any]) -> None:
    assert value.enabled == any(alone.enabled for alone, _ in log)


def _check_lww(value: Any, log: list[Any]) -> None:
    # Pekko's rule: the larger timestamp wins, the lower node on a tie.
    best = min(((-a.timestamp, a.updated_by, a.value) for _, a in log), key=lambda t: t[:2])
    assert (value.timestamp, value.updated_by, value.value) == (-best[0], best[1], best[2])


def _check_orset(value: Any, log: list[Any]) -> None:
    # Which element survives depends on what each remove saw; every replica being equal is the
    # check. A removed element is never resurrected out of nothing:
    assert value.elements <= frozenset().union(*(alone.elements for alone, _ in log))


def _orset(rng: random.Random, node: UniqueAddress, s: ORSet[int]) -> ORSet[int]:
    e = rng.randint(0, 5)
    return s._add(node, e) if rng.random() < 0.5 else s._remove(node, e)


node1 = UniqueAddress("localhost", 7354, 1, "Sys")


def clock(current: int, _value: object) -> int:
    # Deterministic, and it ties: two nodes writing from one state get the same timestamp.
    return current + 1


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
        "GSet", GSet(), lambda rng, node, s: s.add(rng.randint(0, 999)), False, _check_gset
    ),
    ConvergenceCase(
        "Flag",
        Flag(),
        lambda rng, node, s: s.switch_on() if rng.random() < 0.003 else s,
        False,
        _check_flag,
    ),
    ConvergenceCase(
        "LWWRegister",
        LWWRegister._create(node1, 0, clock),
        lambda rng, node, s: s._with_value(node, rng.randint(0, 9), clock),
        False,
        _check_lww,
    ),
    ConvergenceCase("ORSet", ORSet(), _orset, True, _check_orset),
]


class Replicas:
    """The replicas of one case and their wire format. ``receive`` applies a full state, or a
    delta: in sequence per sender when the case is causal."""

    def __init__(self, case: ConvergenceCase, n: int, stats: dict[str, int] | None = None) -> None:
        self.case = case
        self.nodes = [UniqueAddress("localhost", 7354 + i, i + 1, "Sys") for i in range(n)]
        self.states: list[Any] = [case.empty] * n
        self.stats = {} if stats is None else stats
        self.stats.setdefault("sent", 0)
        self.stats.setdefault("applied", 0)
        self.has_deltas = isinstance(case.empty, DeltaReplicatedData)
        self.seqs = [0] * n  # causal: the last delta number each replica sent
        self.last_applied = [[0] * n for _ in range(n)]  # causal: [receiver][sender]

    def pack(self, src: int, is_delta: bool, v: Any) -> str:
        return json.dumps(
            {
                "src": src,
                "seq": self.seqs[src],
                "delta": is_delta,
                "v": Serialization.encode(v, _no_ref),
            }
        )

    def full_state(self, src: int) -> str:
        return self.pack(src, False, self.states[src])

    def update(self, i: int, after: Any) -> str | None:
        """Replica ``i`` takes ``after``. Returns the payload of its delta, if any.

        Like the Replicator, which takes the delta out at update time: it goes to every peer,
        then the sender resets. A delta left on the state would be dropped by the next incoming
        merge, while the sequence numbers ran on."""
        self.states[i] = after
        if not self.has_deltas or after.delta is None:
            return None
        self.seqs[i] += 1
        self.states[i] = after.reset_delta()
        return self.pack(i, True, after.delta)

    def receive(self, dst: int, payload: str) -> None:
        m = json.loads(payload)
        v = Serialization.decode(m["v"], _no_ref)
        last = self.last_applied[dst]
        if not m["delta"]:
            self.states[dst] = self.states[dst].merge(v)
            # A full state holds every delta its sender has sent so far.
            last[m["src"]] = max(last[m["src"]], m["seq"])
        elif not self.case.causal or m["seq"] == last[m["src"]] + 1:
            # ponytail: stands in for the Replicator's causal delivery, not a copy of it
            last[m["src"]] = m["seq"]
            self.stats["applied"] += 1
            self.states[dst] = self.states[dst].merge_delta(v)

    def exchange(self) -> None:
        """Everyone's full state reaches everyone, with no faults."""
        payloads = [self.full_state(i) for i in range(len(self.states))]
        for i, payload in enumerate(payloads):
            for j in range(len(self.states)):
                if i != j:
                    self.receive(j, payload)


def run(
    case: ConvergenceCase, seed: int, stats: dict[str, int] | None = None
) -> tuple[list[Any], list[Any]]:
    rng = random.Random(seed)
    n = rng.randint(3, 5)
    r = Replicas(case, n, stats)
    net = SimNetwork(rng, n)
    log: list[Any] = []

    for tick in range(TICKS):
        if tick == SPLIT_AT:
            net.partition([set(range(n // 2)), set(range(n // 2, n))])
        if tick == HEAL_AT:
            net.heal()
        for src, dst, payload in net.deliver():
            r.receive(dst, payload)
        for i in range(n):
            if rng.random() < 0.5:
                op_seed = rng.getrandbits(32)
                after = case.op(random.Random(op_seed), r.nodes[i], r.states[i])
                log.append((case.op(random.Random(op_seed), r.nodes[i], case.empty), after))
                payload = r.update(i, after)
                if payload is not None:
                    for dst in range(n):
                        if dst != i:
                            r.stats["sent"] += 1
                            net.send(i, dst, payload)
            if not r.has_deltas or rng.random() < 0.3:
                dst = rng.choice([j for j in range(n) if j != i])
                net.send(i, dst, r.full_state(i))

    r.exchange()
    return r.states, log


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("case", CONVERGENCE, ids=lambda c: c.name)
def test_replicas_converge(case: ConvergenceCase, seed: int):
    states, log = run(case, seed)
    norm = [s.reset_delta() if hasattr(s, "reset_delta") else s for s in states]
    assert all(s == norm[0] for s in norm), f"{case.name} seed {seed}: {norm}"
    case.check(norm[0], log)


def test_causal_delivery_applies_most_deltas():
    case = CONVERGENCE[0]._replace(causal=True)
    stats: dict[str, int] = {}
    for seed in range(20):
        states, log = run(case, seed, stats)
        assert all(s.reset_delta() == states[0].reset_delta() for s in states)
        case.check(states[0], log)
    share = stats["applied"] / stats["sent"]
    print(f"causal deltas applied: {stats['applied']} of {stats['sent']} = {share:.3f}")
    # Reordering, 20% loss and the partition each stall a sender's chain until a full state
    # arrives, so most deltas are ignored: 0.105 measured. This pins that the path does work.
    assert share >= 0.10


DELTA_CASES = [c for c in CONVERGENCE if isinstance(c.empty, DeltaReplicatedData)]


@pytest.mark.parametrize("case", DELTA_CASES, ids=lambda c: c.name)
def test_every_ops_delta_is_sent_to_every_peer(case: ConvergenceCase):
    # A delta left on a replica is lost by the next incoming merge, which returns delta=None,
    # while the sequence numbers run on unbroken: receivers would miss ops.
    for seed in range(5):
        stats: dict[str, int] = {}
        states, log = run(case, seed, stats)
        with_delta = sum(after.delta is not None for _, after in log)
        assert with_delta == stats["sent"] / (len(states) - 1), f"seed {seed}"


ORSET = next(c for c in CONVERGENCE if c.name == "ORSet")


def _add_and_remove_x_concurrently() -> tuple[Replicas, str, str]:
    """Replicas 0 and 1 hold {"x"}; then, split, 0 removes "x" while 1 adds it again. Returns
    the replicas and the two queued deltas: 0's remove and 1's add."""
    r = Replicas(ORSET, 2)
    added = r.update(0, r.states[0]._add(r.nodes[0], "x"))
    assert added is not None
    r.receive(1, added)
    assert r.states[0].elements == r.states[1].elements == {"x"}
    removed = r.update(0, r.states[0]._remove(r.nodes[0], "x"))
    re_added = r.update(1, r.states[1]._add(r.nodes[1], "x"))
    assert removed is not None and re_added is not None
    assert r.states[0].elements == set() and r.states[1].elements == {"x"}
    return r, removed, re_added


def test_orset_add_wins_over_a_concurrent_remove():
    r, _, _ = _add_and_remove_x_concurrently()
    r.exchange()
    assert r.states[0].elements == r.states[1].elements == {"x"}
    assert r.states[0] == r.states[1]


def test_orset_add_wins_over_a_concurrent_remove_through_deltas():
    r, removed, re_added = _add_and_remove_x_concurrently()
    r.receive(1, removed)
    r.receive(0, re_added)
    assert r.stats["applied"] == 3  # every delta passed the causal check
    assert r.states[0].elements == r.states[1].elements == {"x"}
    assert r.states[0] == r.states[1]
