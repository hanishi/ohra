"""Micro-batching and bounded mailboxes."""
from __future__ import annotations

import asyncio

import pytest

from pekko_py import (
    ActorContext,
    ActorSystem,
    Behavior,
    Behaviors,
    MailboxOverflowError,
    MailboxSelector,
    batching,
)

from .util import eventually

# --- batching -------------------------------------------------------------------------


async def test_batch_flushes_at_max_n_and_the_rest_after_max_wait():
    batches: list[list[int]] = []
    b = batching(4, 0.02, lambda batch: batches.append(list(batch)))
    async with ActorSystem(b, "t") as system:
        for i in range(10):
            system.tell(i)
        await eventually(lambda: len(batches) == 2)
        assert batches == [[0, 1, 2, 3], [4, 5, 6, 7]]
        await eventually(lambda: len(batches) == 3)
        assert batches[2] == [8, 9]


async def test_max_wait_counts_from_the_first_message_of_a_batch():
    batches: list[list[int]] = []
    loop = asyncio.get_running_loop()
    b = batching(100, 0.03, lambda batch: batches.append(list(batch)))
    async with ActorSystem(b, "t") as system:
        t0 = loop.time()
        system.tell(1)
        await asyncio.sleep(0.01)
        system.tell(2)  # must not push the deadline back
        await eventually(lambda: batches == [[1, 2]])
        assert 0.025 <= loop.time() - t0 < 0.06


async def test_messages_during_an_async_flush_form_the_next_batch():
    batches: list[list[int]] = []

    async def handle(batch: list[int]) -> None:
        await asyncio.sleep(0.01)
        batches.append(list(batch))

    async with ActorSystem(batching(2, 1.0, handle), "t") as system:
        system.tell(1)
        system.tell(2)
        await asyncio.sleep(0.002)  # the flush of [1, 2] is in progress
        system.tell(3)
        system.tell(4)
        await eventually(lambda: len(batches) == 2)
        assert batches == [[1, 2], [3, 4]]


async def test_pending_batch_is_flushed_on_stop():
    batches: list[list[int]] = []
    async with ActorSystem(batching(10, 10.0, lambda batch: batches.append(list(batch))), "t") as system:
        system.tell(1)
        system.tell(2)
        await asyncio.sleep(0.01)  # stop is a system message; let the two be collected first
    assert batches == [[1, 2]]


# --- bounded mailboxes ------------------------------------------------------------------


def blocker(gate: asyncio.Event, seen: list[int]) -> Behavior[int]:
    async def on_message(msg: int) -> Behavior[int]:
        if msg == 0:
            await gate.wait()
        seen.append(msg)
        return Behaviors.same

    return Behaviors.receive_message(on_message)


def guardian(gate: asyncio.Event, seen: list[int], mailbox: MailboxSelector) -> Behavior[int]:
    def setup(ctx: ActorContext[int]) -> Behavior[int]:
        child = ctx.spawn(blocker(gate, seen), "child", mailbox)
        return Behaviors.receive_message(lambda m: (child.tell(m), Behaviors.same)[1])

    return Behaviors.setup(setup)


async def test_bounded_mailbox_drops_new_messages_by_default():
    gate, seen = asyncio.Event(), []
    async with ActorSystem(guardian(gate, seen, MailboxSelector.bounded(2)), "t") as system:
        system.tell(0)  # blocks the child while it is being processed
        await asyncio.sleep(0.01)
        for i in range(1, 6):
            system.tell(i)
        await asyncio.sleep(0.01)
        gate.set()
        await eventually(lambda: len(seen) == 3)
        assert seen == [0, 1, 2]


async def test_bounded_mailbox_can_drop_old_messages():
    gate, seen = asyncio.Event(), []
    mailbox = MailboxSelector.bounded(2, overflow="drop_old")
    async with ActorSystem(guardian(gate, seen, mailbox), "t") as system:
        system.tell(0)
        await asyncio.sleep(0.01)
        for i in range(1, 6):
            system.tell(i)
        await asyncio.sleep(0.01)
        gate.set()
        await eventually(lambda: len(seen) == 3)
        assert seen == [0, 4, 5]


async def test_bounded_mailbox_can_fail_the_sender():
    gate, seen = asyncio.Event(), []
    mailbox = MailboxSelector.bounded(1, overflow="fail")
    async with ActorSystem(blocker(gate, seen), "t", mailbox) as system:
        system.tell(0)
        await asyncio.sleep(0.01)
        system.tell(1)
        with pytest.raises(MailboxOverflowError):
            system.tell(2)
        gate.set()
        await eventually(lambda: seen == [0, 1])
