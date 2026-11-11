"""Tests for ``TestLease`` and ``TestLeaseExt`` (``coordination/src/test/.../TestLease.scala``
36-113). Pekko has no spec of ``TestLease``, so these are ohra's own, one per behavior.

Differences: ``getTestLease`` raises ``RuntimeError``, not ``IllegalStateException``, so that
``await_assert`` retries it; ``acquire`` awaits the promise through ``asyncio.shield``, so a
cancelled acquire (a stopped actor's ``pipe_to_self``) leaves the promise pending.
"""

import asyncio
from dataclasses import dataclass

import pytest

from ohra import ActorContext, ActorRef, Behavior, Behaviors, Success, Try
from ohra.coordination.lease import LeaseError, LeaseProvider
from ohra.testkit import ActorTestKit, TestLease, TestLeaseExt


def make_lease(testkit: ActorTestKit) -> TestLease:
    TestLease.register(testkit.system)
    lease = LeaseProvider(testkit.system).get_lease("l1", "test-lease", "owner1")
    assert isinstance(lease, TestLease)
    return lease


async def test_get_test_lease_raises_until_the_lease_is_created():
    async with ActorTestKit() as testkit:
        TestLease.register(testkit.system)
        ext = TestLeaseExt(testkit.system)
        assert TestLeaseExt(testkit.system) is ext
        with pytest.raises(RuntimeError, match="Test lease l1 has not been set yet") as info:
            ext.get_test_lease("l1")
        assert not isinstance(info.value, AssertionError)
        lease = LeaseProvider(testkit.system).get_lease("l1", "test-lease", "owner1")
        assert ext.get_test_lease("l1") is lease


async def test_acquire_reports_to_the_probe_and_waits_for_the_initial_promise():
    async with ActorTestKit() as testkit:
        lease = make_lease(testkit)
        task = asyncio.ensure_future(lease.acquire())
        try:
            await lease.probe.expect_message(TestLease.AcquireReq("owner1"))
            await asyncio.sleep(0)
            assert not task.done()
            lease.initial_promise.set_result(True)
            assert await task is True
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_a_failed_initial_promise_fails_the_acquire():
    async with ActorTestKit() as testkit:
        lease = make_lease(testkit)
        lease.initial_promise.set_exception(LeaseError("no"))
        with pytest.raises(LeaseError, match="no"):
            await lease.acquire()


async def test_set_next_acquire_result_answers_later_acquires():
    async with ActorTestKit() as testkit:
        lease = make_lease(testkit)
        nxt: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        nxt.set_result(False)
        lease.set_next_acquire_result(nxt)
        assert await lease.acquire() is False
        assert await lease.acquire() is False


async def test_acquire_with_a_callback_becomes_the_current_callback():
    async with ActorTestKit() as testkit:
        lease = make_lease(testkit)
        lease.current_callback()(None)
        lease.initial_promise.set_result(True)

        def cb(_: BaseException | None) -> None:
            pass

        await lease.acquire(cb)
        assert lease.current_callback() is cb
        await lease.acquire()
        assert lease.current_callback() is cb


async def test_release_reports_to_the_probe_and_returns_true():
    async with ActorTestKit() as testkit:
        lease = make_lease(testkit)
        assert await lease.release() is True
        await lease.probe.expect_message(TestLease.ReleaseReq("owner1"))


async def test_check_lease_returns_the_next_check_lease_result():
    async with ActorTestKit() as testkit:
        lease = make_lease(testkit)
        assert lease.check_lease() is False
        lease.set_next_check_lease_result(True)
        assert lease.check_lease() is True


async def test_a_cancelled_acquire_leaves_the_initial_promise_pending():
    async with ActorTestKit() as testkit:
        lease = make_lease(testkit)
        task = asyncio.ensure_future(lease.acquire())
        await lease.probe.expect_message(TestLease.AcquireReq("owner1"))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not lease.initial_promise.cancelled()
        lease.initial_promise.set_result(True)
        assert await lease.acquire() is True


@dataclass(frozen=True)
class Go:
    reply_to: ActorRef[bool]


@dataclass(frozen=True)
class Acquired:
    result: Try[bool]


async def test_an_acquire_from_an_actor_is_piped_back():
    async with ActorTestKit() as testkit:
        lease = make_lease(testkit)
        replies = testkit.create_test_probe()

        def setup(ctx: ActorContext[Go | Acquired]) -> Behavior[Go | Acquired]:
            def on(msg: Go | Acquired) -> Behavior[Go | Acquired]:
                match msg:
                    case Go():
                        ctx.pipe_to_self(lease.acquire(lambda _: None), Acquired)
                    case Acquired(Success(value)):
                        replies.ref.tell(value)
                    case Acquired():
                        replies.ref.tell(False)
                return Behaviors.same

            return Behaviors.receive_message(on)

        actor = testkit.spawn(Behaviors.setup(setup))
        actor.tell(Go(replies.ref))
        await lease.probe.expect_message(TestLease.AcquireReq("owner1"))
        lease.initial_promise.set_result(True)
        await replies.expect_message(True)
