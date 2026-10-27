"""Phi-accrual failure detection and the heartbeat ring, as PhiAccrualFailureDetector.scala and
ClusterHeartbeat.scala."""

from __future__ import annotations

import math

from pekko_py.cluster.failure_detector import (
    FailureDetectorRegistry,
    HeartbeatHistory,
    HeartbeatNodeRing,
    HeartbeatSenderState,
    PhiAccrualFailureDetector,
    phi,
)
from pekko_py.remote import Address, UniqueAddress

AA = UniqueAddress("aa", 7355, 1, "sys")
BB = UniqueAddress("bb", 7355, 2, "sys")
CC = UniqueAddress("cc", 7355, 3, "sys")
DD = UniqueAddress("dd", 7355, 4, "sys")
EE = UniqueAddress("ee", 7355, 5, "sys")
FF = UniqueAddress("ff", 7355, 6, "sys")
NODES = frozenset({AA, BB, CC, DD, EE, FF})


class Clock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def detector(clock: Clock, acceptable_heartbeat_pause: float = 0.0) -> PhiAccrualFailureDetector:
    return PhiAccrualFailureDetector(
        threshold=8.0,
        max_sample_size=1000,
        min_std_deviation=0.1,
        acceptable_heartbeat_pause=acceptable_heartbeat_pause,
        first_heartbeat_estimate=1.0,
        clock=clock,
    )


# --- HeartbeatHistory ----------------------------------------------------------------------------


def test_history_mean_variance_and_std_deviation():
    h = HeartbeatHistory(max_sample_size=10).add(1.0).add(2.0).add(3.0)
    assert h.mean == 2.0
    assert math.isclose(h.variance, 2.0 / 3.0)
    assert math.isclose(h.std_deviation, math.sqrt(2.0 / 3.0))


def test_history_drops_the_oldest_beyond_max_sample_size():
    h = HeartbeatHistory(max_sample_size=2).add(1.0).add(2.0).add(3.0)
    assert h.intervals == (2.0, 3.0)
    assert h.mean == 2.5


# --- PhiAccrualFailureDetector -------------------------------------------------------------------


def test_phi_is_zero_before_any_heartbeat_and_available():
    fd = detector(Clock())
    assert fd.phi == 0.0
    assert fd.is_available
    assert not fd.is_monitoring


def test_phi_is_small_right_after_a_heartbeat_and_grows_with_silence():
    clock = Clock(0.0)
    fd = detector(clock)
    for _ in range(5):
        clock.now += 1.0
        fd.heartbeat()
    assert fd.is_monitoring
    assert fd.phi < 0.5 and fd.is_available
    clock.now += 2.0  # far past mean + acceptable pause + many std deviations (std is min 0.1)
    assert fd.phi > 8.0
    assert not fd.is_available


def test_phi_is_zero_right_after_a_heartbeat_at_default_settings():
    # mean 1s + 3s acceptable pause with the 0.1s std floor: y is about -40 and exp overflows in
    # Python where the JVM gives Infinity; the answer is 0 either way
    assert phi(0.0, 4.0, 0.1) == 0.0
    assert phi(0.05, 4.0, 0.1) == 0.0
    clock = Clock(0.0)
    fd = PhiAccrualFailureDetector(clock=clock)  # Pekko's defaults
    for _ in range(3):
        clock.now += 1.0
        fd.heartbeat()
    clock.now += 0.05
    assert fd.is_available and fd.phi == 0.0


def test_acceptable_pause_delays_the_verdict():
    clock = Clock(0.0)
    fd = detector(clock, acceptable_heartbeat_pause=3.0)
    for _ in range(5):
        clock.now += 1.0
        fd.heartbeat()
    clock.now += 2.0
    assert fd.is_available
    clock.now += 3.0
    assert not fd.is_available


def test_a_long_pause_is_not_recorded_in_the_history():
    clock = Clock(0.0)
    fd = detector(clock)
    for _ in range(5):
        clock.now += 1.0
        fd.heartbeat()
    clock.now += 100.0
    assert not fd.is_available
    fd.heartbeat()  # the 100s interval is dropped: the detector was unavailable when it arrived
    assert fd.is_available
    clock.now += 1.0
    fd.heartbeat()
    assert fd.phi < 0.5  # the history still believes in ~1s intervals


# --- FailureDetectorRegistry ---------------------------------------------------------------------


def test_registry_creates_a_detector_on_first_heartbeat_and_defaults_to_available():
    clock = Clock(0.0)
    registry = FailureDetectorRegistry(lambda: detector(clock))
    a = Address("sys", "aa", 7355)
    assert registry.is_available(a) and not registry.is_monitoring(a)
    registry.heartbeat(a)
    assert registry.is_monitoring(a)
    for _ in range(5):
        clock.now += 1.0
        registry.heartbeat(a)
    clock.now += 50.0
    assert not registry.is_available(a)
    registry.remove(a)
    assert registry.is_available(a) and not registry.is_monitoring(a)
    registry.heartbeat(a)
    registry.reset()
    assert not registry.is_monitoring(a)


# --- HeartbeatNodeRing ---------------------------------------------------------------------------


def test_ring_picks_the_specified_number_of_receivers_for_every_node():
    ring = HeartbeatNodeRing(CC, NODES, frozenset(), 3)
    assert ring.my_receivers == ring.receivers(CC)
    for n in NODES:
        receivers = ring.receivers(n)
        assert len(receivers) == 3 and n not in receivers


def test_ring_includes_intermediate_unreachable_nodes_up_to_the_limit():
    order = HeartbeatNodeRing(CC, NODES, frozenset(), 2).node_ring
    assert set(order) == NODES and len(order) == 6
    me = order[0]
    nxt = order[1:] + order[:1]
    # the next two are unreachable: both are included and the two reachable after them are still
    # picked
    ring = HeartbeatNodeRing(me, NODES, frozenset(nxt[:2]), 2)
    assert ring.my_receivers == frozenset(nxt[:4])
    # three unreachable in a row: only the first two unreachable are kept, the third is skipped
    ring = HeartbeatNodeRing(me, NODES, frozenset(nxt[:3]), 2)
    assert ring.my_receivers == frozenset(nxt[:2]) | frozenset(nxt[3:5])


def test_ring_uses_all_others_when_small_and_none_when_alone():
    expected = NODES - {CC}
    for n in (5, 6, 7):
        assert HeartbeatNodeRing(CC, NODES, frozenset(), n).my_receivers == expected
    assert HeartbeatNodeRing(CC, frozenset({CC}), frozenset(), 3).my_receivers == frozenset()


def test_ring_order_is_the_same_on_every_node():
    a = HeartbeatNodeRing(CC, NODES, frozenset(), 2).node_ring
    b = HeartbeatNodeRing(FF, NODES, frozenset(), 2).node_ring
    assert a == b


# --- HeartbeatSenderState ------------------------------------------------------------------------


def state(clock: Clock, n: int = 3) -> HeartbeatSenderState:
    registry = FailureDetectorRegistry(lambda: detector(clock))
    return HeartbeatSenderState(
        HeartbeatNodeRing(AA, frozenset({AA}), frozenset(), n), frozenset(), registry
    )


def test_sender_state_init_and_add_member():
    s = state(Clock()).init(frozenset({BB, CC}), frozenset())
    assert s.active_receivers == frozenset({BB, CC})
    assert s.contains(BB) and not s.contains(DD)
    s = s.add_member(DD)
    assert s.active_receivers == frozenset({BB, CC, DD})
    assert s.add_member(DD) == s


def test_sender_state_keeps_heartbeating_an_unavailable_node_that_dropped_out_of_its_receivers():
    clock = Clock(0.0)
    s = state(clock, n=1).init(frozenset({BB, CC, DD, EE}), frozenset())
    (first,) = s.active_receivers
    for _ in range(5):
        clock.now += 1.0
        s = s.heartbeat_rsp(first)
    assert s.failure_detector.is_monitoring(first.address)
    clock.now += 100.0
    assert not s.failure_detector.is_available(first.address)
    # a new member joins between me and `first` on the ring, so `first` is no longer my receiver
    joiner = next(
        n
        for n in (UniqueAddress("zz", 7355, uid, "sys") for uid in range(100, 10_000))
        if HeartbeatNodeRing(AA, s.ring.nodes | {n}, frozenset(), 1).my_receivers == frozenset({n})
    )
    s = s.add_member(joiner)
    assert s.ring.my_receivers == frozenset({joiner})
    assert first in s.old_receivers_now_unreachable  # still unavailable: keep heartbeating it
    assert s.active_receivers == frozenset({joiner, first})
    # it answers again: back from unreachable and no longer a receiver, so stop monitoring it
    clock.now += 1.0
    s = s.heartbeat_rsp(first)
    assert first not in s.old_receivers_now_unreachable
    assert not s.failure_detector.is_monitoring(first.address)
    assert s.failure_detector.is_available(first.address)


def test_sender_state_remove_member_forgets_the_detector():
    clock = Clock(0.0)
    s = state(clock).init(frozenset({BB, CC}), frozenset())
    s = s.heartbeat_rsp(BB)
    assert s.failure_detector.is_monitoring(BB.address)
    s = s.remove_member(BB)
    assert not s.contains(BB)
    assert not s.failure_detector.is_monitoring(BB.address)
    assert s.active_receivers == frozenset({CC})


def test_sender_state_ignores_responses_from_non_receivers():
    clock = Clock(0.0)
    s = state(clock).init(frozenset({BB}), frozenset())
    assert s.heartbeat_rsp(DD) == s
    assert not s.failure_detector.is_monitoring(DD.address)


def test_sender_state_reachable_member_removes_detector_of_dropped_receiver():
    clock = Clock(0.0)
    s = state(clock, n=1).init(frozenset({BB, CC, DD, EE}), frozenset())
    (first,) = s.active_receivers
    s = s.heartbeat_rsp(first)
    other = next(iter(s.ring.nodes - {AA, first}))
    s = s.unreachable_member(other)  # ring shape unchanged for me unless `other` was in my path
    s = s.reachable_member(other)
    assert s.active_receivers == frozenset({first})
