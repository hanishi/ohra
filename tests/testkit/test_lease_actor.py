"""Tests for ``TestLeaseActor``, ``TestLeaseActorClient`` and ``TestLeaseActorClientExt``
(``coordination/src/test/.../TestLeaseActor.scala`` 38-131). Pekko has no spec of its own for
them: ``ClusterSingletonManagerLeaseSpec`` uses them, and it is out of scope. These are ohra's
own, one per behavior.

Differences: ``sender()`` becomes the ``reply_to`` of ``Request``; the exception results are
``BaseException`` values, not ``Status.Failure``; ``GetRequests`` is a class, not a case object.
"""

import asyncio

import pytest

from ohra import ActorRef, ask
from ohra.coordination.lease import LeaseError, LeaseProvider
from ohra.system import ActorSystem
from ohra.testkit import (
    ActorTestKit,
    TestLeaseActor,
    TestLeaseActorClient,
    TestLeaseActorClientExt,
)

Acquire = TestLeaseActor.Acquire
Release = TestLeaseActor.Release


def make_client(
    testkit: ActorTestKit,
) -> tuple[TestLeaseActorClient, ActorRef[TestLeaseActor.Command]]:
    system = testkit.system
    actor = testkit.spawn(TestLeaseActor.behavior())
    TestLeaseActorClientExt(system).set_actor_lease(actor)
    LeaseProvider(system).register("test-lease-actor", lambda s: TestLeaseActorClient(s, system))
    lease = LeaseProvider(system).get_lease("l1", "test-lease-actor", "o1")
    assert isinstance(lease, TestLeaseActorClient)
    return lease, actor


async def requests(
    actor: ActorRef[TestLeaseActor.Command],
) -> tuple[TestLeaseActor.LeaseRequest, ...]:
    reply = await ask(actor, TestLeaseActor.GetRequests, 3.0)
    return reply.requests


async def test_an_acquire_waits_until_it_is_actioned():
    async with ActorTestKit() as testkit:
        lease, actor = make_client(testkit)
        probe = testkit.create_test_probe()
        task = asyncio.ensure_future(lease.acquire())
        try:

            async def pending() -> None:
                assert await requests(actor) == (Acquire("o1"),)

            await probe.await_assert(pending)
            actor.tell(TestLeaseActor.ActionRequest(Acquire("o1"), True))
            assert await task is True
            assert await requests(actor) == ()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_requests_are_listed_newest_first():
    async with ActorTestKit() as testkit:
        lease, actor = make_client(testkit)
        probe = testkit.create_test_probe()
        acquire = asyncio.ensure_future(lease.acquire())
        try:

            async def one() -> None:
                assert await requests(actor) == (Acquire("o1"),)

            await probe.await_assert(one)
            release = asyncio.ensure_future(lease.release())
            try:

                async def two() -> None:
                    assert await requests(actor) == (Release("o1"), Acquire("o1"))

                await probe.await_assert(two)
                actor.tell(TestLeaseActor.ActionRequest(Acquire("o1"), True))
                actor.tell(TestLeaseActor.ActionRequest(Release("o1"), True))
                assert await acquire is True
                assert await release is True
            finally:
                release.cancel()
                await asyncio.gather(release, return_exceptions=True)
        finally:
            acquire.cancel()
            await asyncio.gather(acquire, return_exceptions=True)


async def test_an_exception_result_fails_the_acquire():
    async with ActorTestKit() as testkit:
        lease, actor = make_client(testkit)
        probe = testkit.create_test_probe()
        task = asyncio.ensure_future(lease.acquire())
        try:

            async def pending() -> None:
                assert await requests(actor) == (Acquire("o1"),)

            await probe.await_assert(pending)
            actor.tell(TestLeaseActor.ActionRequest(Acquire("o1"), LeaseError("boom")))
            with pytest.raises(LeaseError, match="boom"):
                await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_an_unknown_action_request_restarts_the_actor_with_no_requests():
    async with ActorTestKit() as testkit:
        lease, actor = make_client(testkit)
        probe = testkit.create_test_probe()
        task = asyncio.ensure_future(lease.acquire())
        try:

            async def pending() -> None:
                assert await requests(actor) == (Acquire("o1"),)

            await probe.await_assert(pending)
            actor.tell(TestLeaseActor.ActionRequest(Release("nobody"), True))

            async def restarted() -> None:
                assert await requests(actor) == ()

            await probe.await_assert(restarted)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_the_client_ext_needs_a_lease_actor_first():
    async with ActorTestKit() as testkit:
        system: ActorSystem[None] = testkit.system
        with pytest.raises(RuntimeError, match="LeaseActorRef must be set first"):
            TestLeaseActorClientExt(system).get_lease_actor()


async def test_check_lease_is_false():
    async with ActorTestKit() as testkit:
        lease, _ = make_client(testkit)
        assert lease.check_lease() is False
