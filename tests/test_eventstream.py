"""The event stream and dead letters, after Apache Pekko's EventStream and DeadLetter."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from ohra import (
    ActorRef,
    ActorContext,
    AllDeadLetters,
    Behavior,
    Behaviors,
    DeadLetter,
    Dropped,
    EventStream,
    MailboxSelector,
    SupervisorStrategy,
    UnhandledMessage,
)
from ohra.testkit import ActorTestKit, TestProbe


@dataclass(frozen=True)
class Hello:
    text: str


async def test_a_message_to_a_stopped_actor_is_a_dead_letter():
    async with ActorTestKit() as testkit:
        dead: TestProbe[DeadLetter] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        ref = testkit.spawn(Behaviors.ignore)
        await testkit.stop(ref)
        ref.tell("too late")
        assert await dead.expect_message(DeadLetter("too late", ref)) == DeadLetter(
            "too late", ref
        )


async def test_an_unhandled_message_is_published():
    async with ActorTestKit() as testkit:
        unhandled: TestProbe[UnhandledMessage] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(UnhandledMessage, unhandled.ref))
        ref = testkit.spawn(Behaviors.empty)
        ref.tell("nobody wants this")
        await unhandled.expect_message(UnhandledMessage("nobody wants this", ref))


async def test_a_full_mailbox_drops_into_dead_letters():
    async with ActorTestKit() as testkit:
        dead: TestProbe[DeadLetter] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        await dead.expect_no_message(0.01)  # the subscription is in place

        async def slow(_msg: str) -> Behavior[str]:
            await asyncio.sleep(0.05)
            return Behaviors.same

        cell = testkit.system._root.spawn_child(  # pyright: ignore[reportPrivateUsage]
            Behaviors.receive_message(slow), "slow", MailboxSelector.bounded(1)
        )
        for m in ("a", "b", "c"):
            cell.ref.tell(m)  # the mailbox holds one: "a"
        assert await dead.receive_messages(2) == [
            DeadLetter("b", cell.ref),
            DeadLetter("c", cell.ref),
        ]


async def test_a_full_backoff_stash_drops():
    def failing(_ctx: object) -> Behavior[str]:
        @Behaviors.receive_message
        def on_message(msg: str) -> Behavior[str]:
            raise RuntimeError(msg)

        return on_message

    async with ActorTestKit(manual_time=True) as testkit:
        dropped: TestProbe[Dropped] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(Dropped, dropped.ref))
        strategy = SupervisorStrategy.restart_with_backoff(10, 10).with_stash_capacity(1)
        ref = testkit.spawn(Behaviors.supervise(Behaviors.setup(failing)).on_failure(strategy))
        for m in ("boom", "kept", "dropped"):
            ref.tell(m)
        d = await dropped.receive_message()
        assert (d.message, d.recipient) == ("dropped", ref)
        assert d.reason == "Stash is full in [RestartSupervisor]"


async def test_subscribing_to_a_base_class_gets_every_kind():
    async with ActorTestKit() as testkit:
        every: TestProbe[AllDeadLetters] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(AllDeadLetters, every.ref))
        ref = testkit.spawn(Behaviors.empty)
        ref.tell("unhandled")
        await every.expect_message(UnhandledMessage("unhandled", ref))
        await testkit.stop(ref)  # a stop overtakes queued messages, so wait for the first
        ref.tell("dead")
        await every.expect_message(DeadLetter("dead", ref))


async def test_publish_any_event():
    async with ActorTestKit() as testkit:
        hellos: TestProbe[Hello] = testkit.create_test_probe()
        others: TestProbe[DeadLetter] = testkit.create_test_probe()
        stream = testkit.system.event_stream
        stream.tell(EventStream.Subscribe(Hello, hellos.ref))
        stream.tell(EventStream.Subscribe(DeadLetter, others.ref))
        stream.tell(EventStream.Publish(Hello("hi")))
        await hellos.expect_message(Hello("hi"))
        await others.expect_no_message()


async def test_a_subscriber_gets_an_event_once():
    async with ActorTestKit() as testkit:
        probe: TestProbe[AllDeadLetters] = testkit.create_test_probe()
        stream = testkit.system.event_stream
        stream.tell(EventStream.Subscribe(DeadLetter, probe.ref))
        stream.tell(EventStream.Subscribe(AllDeadLetters, probe.ref))
        stream.tell(EventStream.Publish(DeadLetter("x", probe.ref)))
        await probe.expect_message(DeadLetter("x", probe.ref))
        await probe.expect_no_message()


async def test_unsubscribe():
    async with ActorTestKit() as testkit:
        hellos: TestProbe[Hello] = testkit.create_test_probe()
        stream = testkit.system.event_stream
        stream.tell(EventStream.Subscribe(Hello, hellos.ref))
        stream.tell(EventStream.Unsubscribe(hellos.ref))
        stream.tell(EventStream.Publish(Hello("hi")))
        await hellos.expect_no_message()


async def test_a_stopped_subscriber_is_removed_without_a_loop():
    async with ActorTestKit() as testkit:
        gone: TestProbe[DeadLetter] = testkit.create_test_probe()
        watcher: TestProbe[DeadLetter] = testkit.create_test_probe()
        stream = testkit.system.event_stream
        stream.tell(EventStream.Subscribe(DeadLetter, gone.ref))
        stream.tell(EventStream.Subscribe(DeadLetter, watcher.ref))
        gone.stop()
        await watcher.expect_terminated(gone.ref)
        ref = testkit.spawn(Behaviors.ignore)
        await testkit.stop(ref)
        ref.tell("late")
        await watcher.expect_message(DeadLetter("late", ref))
        await watcher.expect_no_message()  # nothing about the stopped subscriber, no loop


async def test_a_dead_letter_about_a_dead_letter_is_not_republished():
    async with ActorTestKit() as testkit:
        dead: TestProbe[DeadLetter] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        ref = testkit.spawn(Behaviors.ignore)
        await testkit.stop(ref)
        ref.tell(DeadLetter("inner", ref))  # an event that itself becomes a dead letter
        await dead.expect_no_message()


async def test_the_test_kit_probes():
    async with ActorTestKit() as testkit:
        dead = testkit.create_dead_letter_probe()
        unhandled = testkit.create_unhandled_message_probe()
        dropped = testkit.create_dropped_message_probe()
        ref = testkit.spawn(Behaviors.empty)
        ref.tell("unhandled")
        await unhandled.expect_message(UnhandledMessage("unhandled", ref))
        await testkit.stop(ref)
        ref.tell("dead")
        await dead.expect_message(DeadLetter("dead", ref))
        testkit.system.event_stream.tell(EventStream.Publish(Dropped("x", "why", ref)))
        await dropped.expect_message(Dropped("x", "why", ref))


async def test_shutdown_with_subscribers_does_not_loop():
    async with ActorTestKit() as testkit:
        dead: TestProbe[DeadLetter] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        for _ in range(3):
            testkit.spawn(Behaviors.ignore).tell("pending")


async def test_an_adapter_subscriber_is_removed_when_its_actor_stops():
    def saw(d: DeadLetter) -> str:
        return f"saw {d.message}"

    def listener(ctx: ActorContext[str]) -> Behavior[str]:
        ctx.system.event_stream.tell(EventStream.Subscribe(DeadLetter, ctx.message_adapter(saw)))
        return Behaviors.ignore

    async with ActorTestKit() as testkit:
        others: TestProbe[DeadLetter] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(DeadLetter, others.ref))
        await testkit.stop(testkit.spawn(Behaviors.setup(listener)))
        ref = testkit.spawn(Behaviors.ignore)
        await testkit.stop(ref)
        ref.tell("late")
        await others.expect_message(DeadLetter("late", ref))
        await others.expect_no_message()  # no dead letters for the stopped adapter, no loop


async def test_a_subscriber_that_cannot_take_an_event_does_not_stop_the_stream():
    async with ActorTestKit() as testkit:
        hellos: TestProbe[Hello] = testkit.create_test_probe()
        stream = testkit.system.event_stream
        full = testkit.system._root.spawn_child(  # pyright: ignore[reportPrivateUsage]
            Behaviors.receive_message(lambda _m: Behaviors.same),
            "full",
            MailboxSelector.bounded(1, overflow="fail"),
        )
        stream.tell(EventStream.Subscribe(Hello, full.ref))
        stream.tell(EventStream.Subscribe(Hello, hellos.ref))
        for n in range(3):  # the second and third overflow the full mailbox
            stream.tell(EventStream.Publish(Hello(str(n))))
        assert await hellos.receive_messages(3) == [Hello("0"), Hello("1"), Hello("2")]


async def test_a_topic_must_be_a_class():
    async with ActorTestKit() as testkit:
        probe: TestProbe[list[int]] = testkit.create_test_probe()
        with pytest.raises(TypeError, match="topic must be a class"):
            EventStream.Subscribe(list[int], probe.ref)


async def test_a_dead_letter_to_an_adapter_carries_the_message_sent():
    async with ActorTestKit() as testkit:
        dead: TestProbe[DeadLetter] = testkit.create_test_probe()
        adapters: TestProbe[ActorRef[str]] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))

        def owner(ctx: ActorContext[str]) -> Behavior[str]:
            adapters.ref.tell(ctx.message_adapter(lambda s: s.upper()))
            return Behaviors.ignore

        ref = testkit.spawn(Behaviors.setup(owner))
        adapter = await adapters.receive_message()
        await testkit.stop(ref)
        adapter.tell("late")
        await dead.expect_message(DeadLetter("late", ref))


async def test_an_adapter_subscriber_that_cannot_take_a_dead_letter_does_not_loop():
    gate = asyncio.Event()
    busy = asyncio.Event()

    async def wait(_m: str) -> Behavior[str]:
        busy.set()
        await gate.wait()
        return Behaviors.same

    def listener(ctx: ActorContext[str]) -> Behavior[str]:
        adapter = ctx.message_adapter(lambda d: f"saw {d.message}")
        ctx.system.event_stream.tell(EventStream.Subscribe(DeadLetter, adapter))
        return Behaviors.receive_message(wait)

    async with ActorTestKit() as testkit:
        try:
            others: TestProbe[DeadLetter] = testkit.create_test_probe()
            testkit.system.event_stream.tell(EventStream.Subscribe(DeadLetter, others.ref))
            full = testkit.system._root.spawn_child(  # pyright: ignore[reportPrivateUsage]
                Behaviors.setup(listener), "full", MailboxSelector.bounded(1)
            )
            full.ref.tell("busy")
            await busy.wait()
            full.ref.tell("fills the one slot")
            ref = testkit.spawn(Behaviors.ignore)
            await testkit.stop(ref)
            ref.tell("late")  # its DeadLetter overflows the listener's mailbox
            await others.expect_message(DeadLetter("late", ref))
            await others.expect_no_message()  # the overflowed one is not published again
        finally:
            gate.set()
