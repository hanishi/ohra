"""Throttle modes and the Throttler, ported from Pekko's ThrottleModeSpec
(remote/src/test/scala/org/apache/pekko/remote/classic/transport/ThrottleModeSpec.scala)."""

import asyncio
import time

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
