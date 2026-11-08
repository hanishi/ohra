"""Throttle modes and the Throttler, ported from Pekko's ThrottleModeSpec
(remote/src/test/scala/org/apache/pekko/remote/classic/transport/ThrottleModeSpec.scala)."""

import asyncio
import time
from types import SimpleNamespace

import pytest

import ohra.remote.throttler as throttler_module

from ohra.remote.throttler import Blackhole, Direction, Throttler, TokenBucket, Unthrottled

half = 500_000_000


def test_unthrottled_allows_any_amount_of_tokens():
    u = Unthrottled()
    assert u.try_consume_tokens(0, 100) == (Unthrottled(), True)
    assert u.try_consume_tokens(100000, 1000) == (Unthrottled(), True)
    assert u.try_consume_tokens(1000000, 10000) == (Unthrottled(), True)


def test_a_token_bucket_allows_consuming_up_to_capacity():
    b0 = TokenBucket(100, 100, 0, 100)
    b1, ok1 = b0.try_consume_tokens(0, 10)
    assert (b1, ok1) == (TokenBucket(100, 100, 0, 90), True)
    b2, ok2 = b1.try_consume_tokens(0, 40)
    assert (b2, ok2) == (TokenBucket(100, 100, 0, 50), True)
    b3, ok3 = b2.try_consume_tokens(0, 50)
    assert (b3, ok3) == (TokenBucket(100, 100, 0, 0), True)
    assert b3.try_consume_tokens(0, 1) == (TokenBucket(100, 100, 0, 0), False)


def test_a_token_bucket_replenishes_accurately():
    b0 = TokenBucket(100, 100, 0, 0)
    b1, ok1 = b0.try_consume_tokens(0, 0)
    assert (b1, ok1) == (TokenBucket(100, 100, 0, 0), True)
    b2, ok2 = b1.try_consume_tokens(half, 0)
    assert (b2, ok2) == (TokenBucket(100, 100, half, 50), True)
    b3, ok3 = b2.try_consume_tokens(2 * half, 0)
    assert (b3, ok3) == (TokenBucket(100, 100, 2 * half, 100), True)
    b4, ok4 = b3.try_consume_tokens(3 * half, 0)
    assert (b4, ok4) == (TokenBucket(100, 100, 3 * half, 100), True)


def test_replenish_and_consume_interleave_accurately():
    b0 = TokenBucket(100, 100, 0, 20)
    b1, ok1 = b0.try_consume_tokens(0, 10)
    assert (b1, ok1) == (TokenBucket(100, 100, 0, 10), True)
    b2, ok2 = b1.try_consume_tokens(half, 60)
    assert (b2, ok2) == (TokenBucket(100, 100, half, 0), True)
    b3, ok3 = b2.try_consume_tokens(2 * half, 40)
    assert (b3, ok3) == (TokenBucket(100, 100, 2 * half, 10), True)
    b4, ok4 = b3.try_consume_tokens(3 * half, 70)
    assert (b4, ok4) == (TokenBucket(100, 100, 2 * half, 10), False)


def test_oversized_packets_pass_by_loaning():
    b0 = TokenBucket(100, 100, 0, 20)
    b1, ok1 = b0.try_consume_tokens(0, 30)
    assert (b1, ok1) == (TokenBucket(100, 100, 0, 20), False)
    b2, ok2 = b1.try_consume_tokens(half, 110)
    assert (b2, ok2) == (TokenBucket(100, 100, half, -40), True)
    b3, ok3 = b2.try_consume_tokens(2 * half, 20)
    assert (b3, ok3) == (TokenBucket(100, 100, half, -40), False)
    b4, ok4 = b3.try_consume_tokens(3 * half, 20)
    assert (b4, ok4) == (TokenBucket(100, 100, 3 * half, 40), True)


def test_both_includes_send_and_receive_and_the_others_only_themselves():
    for d in Direction:
        assert Direction.BOTH.includes(d)
    assert Direction.SEND.includes(Direction.SEND)
    assert not Direction.SEND.includes(Direction.RECEIVE)
    assert not Direction.SEND.includes(Direction.BOTH)
    assert Direction.RECEIVE.includes(Direction.RECEIVE)
    assert not Direction.RECEIVE.includes(Direction.SEND)


async def test_set_throttle_with_both_sets_send_and_receive():
    t = Throttler()
    t.set_throttle("h", 1, Direction.BOTH, Blackhole())
    assert await t.admit("h", 1, Direction.SEND, 10) is False
    assert await t.admit("h", 1, Direction.RECEIVE, 10) is False


async def test_an_unknown_peer_is_unthrottled():
    assert await Throttler().admit("h", 1, Direction.SEND, 10**6) is True


async def test_a_slow_bucket_delays_admit():
    t = Throttler()
    t.set_throttle("h", 1, Direction.SEND, TokenBucket(100, 1000, time.monotonic_ns(), 100))
    assert await t.admit("h", 1, Direction.SEND, 100) is True
    start = time.monotonic()
    assert await t.admit("h", 1, Direction.SEND, 100) is True
    assert time.monotonic() - start >= 0.05


async def _drained(t: Throttler) -> asyncio.Task[bool]:
    t.set_throttle("h", 1, Direction.SEND, TokenBucket(100, 1.0, time.monotonic_ns(), 100))
    assert await t.admit("h", 1, Direction.SEND, 100) is True
    task = asyncio.create_task(t.admit("h", 1, Direction.SEND, 100))
    await asyncio.sleep(0.01)
    assert not task.done()
    return task


async def test_a_waiting_admit_passes_at_once_when_set_back_to_unthrottled():
    t = Throttler()
    task = await _drained(t)
    t.set_throttle("h", 1, Direction.SEND, Unthrottled())
    assert await asyncio.wait_for(task, 0.1) is True


async def test_a_waiting_admit_returns_false_when_set_to_blackhole():
    t = Throttler()
    task = await _drained(t)
    t.set_throttle("h", 1, Direction.SEND, Blackhole())
    assert await asyncio.wait_for(task, 0.1) is False


async def test_oversized_frames_on_a_fresh_player_bucket_all_pass():
    # Pekko Player's bucket: a frame over capacity once available_tokens <= 0 must not stall
    t = Throttler()
    t.set_throttle("h", 1, Direction.SEND, TokenBucket(1000, 125000.0, 0, 0))
    async with asyncio.timeout(0.5):
        assert await t.admit("h", 1, Direction.SEND, 10) is True
        assert await t.admit("h", 1, Direction.SEND, 1500) is True
        assert await t.admit("h", 1, Direction.SEND, 1500) is True


async def test_an_oversized_frame_on_receive_passes_too():
    t = Throttler()
    t.set_throttle("h", 1, Direction.RECEIVE, TokenBucket(1000, 125000.0, 0, 0))
    async with asyncio.timeout(0.5):
        for size in (10, 1500, 1500):
            assert await t.admit("h", 1, Direction.RECEIVE, size) is True


def test_time_to_available_is_the_missing_tokens_over_the_rate():
    assert TokenBucket(100, 100, 0, 0).time_to_available(500_000_000, 60) == pytest.approx(0.1)


def test_time_to_available_for_a_frame_over_capacity_needs_one_token():
    assert TokenBucket(100, 100, 0, 0).time_to_available(0, 110) == pytest.approx(0.01)


async def test_a_mode_replaced_while_the_loop_was_blocked_is_honoured():
    # set_throttle(Blackhole) and the wait deadline both expire while the loop is blocked
    t = Throttler()
    t.set_throttle("h", 1, Direction.SEND, TokenBucket(100, 100.0, time.monotonic_ns(), 0))
    loop = asyncio.get_running_loop()
    task = loop.create_task(t.admit("h", 1, Direction.SEND, 2))  # needs about 20ms
    await asyncio.sleep(0)
    loop.call_later(0.012, t.set_throttle, "h", 1, Direction.SEND, Blackhole())
    time.sleep(0.06)  # both handles are due by the next loop iteration
    assert await task is False


async def test_waiters_timing_out_together_do_not_both_spend_the_same_tokens(
    monkeypatch: pytest.MonkeyPatch,
):
    clock = [time.monotonic_ns()]  # the bucket's clock stands still unless the test moves it
    monkeypatch.setattr(throttler_module, "time", SimpleNamespace(monotonic_ns=lambda: clock[0]))
    t = Throttler()
    t.set_throttle("h", 1, Direction.SEND, TokenBucket(2, 40.0, clock[0], 0))
    tasks = [asyncio.create_task(t.admit("h", 1, Direction.SEND, 2)) for _ in range(2)]
    await asyncio.sleep(0)  # both wait for 2 tokens: 50ms
    clock[0] += 60_000_000  # 2 tokens accrued for one waiter, none left for the other
    time.sleep(0.055)  # both deadlines are due by the next loop iteration
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    assert len(done) == 1
    # the other one keeps waiting, then is delivered through the timeout path
    assert await asyncio.wait_for(next(iter(pending)), 1) is True
