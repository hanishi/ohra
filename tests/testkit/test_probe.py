"""TestProbe, after Apache Pekko's TestProbeSpec."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from ohra import ActorRef, Behavior, Behaviors, ask
from ohra.testkit import (
    ActorTestKit,
    FishingOutcome,
    FishingOutcomes,
    TestKitSettings,
    TestProbe,
)


@dataclass(frozen=True)
class Reply:
    n: int
    reply_to: ActorRef[int]


def replier() -> Behavior[Reply]:
    @Behaviors.receive_message
    def on_message(msg: Reply) -> Behavior[Reply]:
        msg.reply_to.tell(msg.n)
        return Behaviors.same

    return on_message


async def test_expect_message():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        testkit.spawn(replier()).tell(Reply(5, probe.ref))
        assert await probe.expect_message(5) == 5


async def test_expect_message_wrong():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        probe.ref.tell(6)
        with pytest.raises(AssertionError, match="^expected 5, found 6$"):
            await probe.expect_message(5)


async def test_expect_message_timeout():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        with pytest.raises(
            AssertionError, match=r"^timeout \(0.05s\) during expect_message while waiting for 5$"
        ):
            await probe.expect_message(5, max=0.05)


async def test_expect_message_hint():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        probe.ref.tell(6)
        with pytest.raises(AssertionError, match="^expected 5, found 6: the total$"):
            await probe.expect_message(5, hint="the total")


@dataclass(frozen=True)
class Greeting:
    text: str


@dataclass(frozen=True)
class Bye:
    pass


async def test_expect_message_type():
    async with ActorTestKit() as testkit:
        probe: TestProbe[Greeting | Bye] = testkit.create_test_probe()
        probe.ref.tell(Greeting("hi"))
        g = await probe.expect_message_type(Greeting)
        assert g.text == "hi"
        probe.ref.tell(Bye())
        with pytest.raises(AssertionError, match="^Expected <class"):
            await probe.expect_message_type(Greeting)


async def test_expect_no_message():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        await probe.expect_no_message(0.02)
        probe.ref.tell(1)
        with pytest.raises(AssertionError, match="^Received unexpected message 1$"):
            await probe.expect_no_message(0.02)


async def test_receive_messages():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        for n in (1, 2, 3):
            probe.ref.tell(n)
        assert await probe.receive_messages(3) == [1, 2, 3]
        probe.ref.tell(4)
        probe.ref.tell(5)
        with pytest.raises(AssertionError, match=r"while expecting 3 messages \(got 2\)$"):
            await probe.receive_messages(3, max=0.05)


async def test_receive_message_timeout():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        with pytest.raises(AssertionError, match=r"^Timeout \(0.05s\) during receive_message"):
            await probe.receive_message(0.05)


async def test_fish_for_message():
    def fisher(n: int) -> FishingOutcome:
        match n:
            case 1:
                return FishingOutcomes.continue_and_ignore()
            case 2:
                return FishingOutcomes.continue_()
            case _:
                return FishingOutcomes.complete()

    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        for n in (1, 2, 3):
            probe.ref.tell(n)
        assert await probe.fish_for_message(1, fisher) == [2, 3]


async def test_fish_for_message_fail():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        probe.ref.tell(1)
        with pytest.raises(AssertionError, match="^bad, hint: counting$"):
            await probe.fish_for_message(1, lambda _: FishingOutcomes.fail("bad"), "counting")


async def test_fish_for_message_timeout():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        probe.ref.tell(1)
        with pytest.raises(
            AssertionError,
            match=r"^timeout \(0.05s\) during fish_for_message, seen messages \[1\], hint: $",
        ):
            await probe.fish_for_message(0.05, lambda _: FishingOutcomes.continue_())


async def test_expect_terminated():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(replier())
        await testkit.stop(ref)
        await probe.expect_terminated(ref)  # watching a stopped actor answers at once
        live = testkit.spawn(replier())
        with pytest.raises(AssertionError, match=r"waiting for actor \[.*\] to stop$"):
            await probe.expect_terminated(live, max=0.05)


async def test_await_assert_retries():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        flag: list[bool] = []
        asyncio.get_running_loop().call_later(0.05, flag.append, True)

        def check() -> int:
            assert flag
            return 7

        async def check_async() -> int:
            await asyncio.sleep(0)
            assert flag
            return 8

        assert await probe.await_assert(check, max=1, interval=0.01) == 7
        assert await probe.await_assert(check_async, max=1, interval=0.01) == 8

        def never() -> None:
            raise AssertionError("still no")

        with pytest.raises(AssertionError, match="^still no$"):
            await probe.await_assert(never, max=0.05, interval=0.01)


async def test_await_assert_retries_any_exception():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        calls: list[int] = []

        def flaky() -> int:
            calls.append(1)
            if len(calls) < 3:
                raise KeyError("not yet")
            return len(calls)

        assert await probe.await_assert(flaky, max=1, interval=0.01) == 3


async def test_within_max():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        with pytest.raises(AssertionError, match=r"^block took .*, exceeding "):
            async with probe.within(0.05):
                await asyncio.sleep(0.1)


async def test_within_min():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        with pytest.raises(AssertionError, match=r"should at least have been 0.05s$"):
            async with probe.within(1, min=0.05):
                pass


async def test_within_bounds_the_waits_inside():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        with pytest.raises(AssertionError, match=r"during expect_message"):
            async with probe.within(0.05):
                assert 0 < probe.remaining <= 0.05
                await probe.expect_message(1)  # waits only what is left, not 3s


async def test_within_ends_with_expect_no_message():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        async with probe.within(0.05):
            await probe.expect_no_message(0.1)


async def test_remaining_outside_within_fails():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        with pytest.raises(AssertionError, match="may not be called outside of `within`"):
            probe.remaining
        assert probe.remaining_or_default == 3.0
        assert probe.remaining_or(1.5) == 1.5


async def test_default_waits_use_the_timefactor():
    settings = TestKitSettings(single_expect_default=0.05, timefactor=2)
    async with ActorTestKit(settings=settings) as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        with pytest.raises(AssertionError, match=r"^timeout \(0.1s\)"):
            await probe.expect_message(5)
        with pytest.raises(AssertionError, match=r"^timeout \(0.1s\)"):
            await probe.expect_message(5, max=0.05)


async def test_probe_stop():
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        other: TestProbe[int] = testkit.create_test_probe()
        probe.stop()
        await other.expect_terminated(probe.ref)


async def test_probes_live_under_system():
    async with ActorTestKit() as testkit:
        a: TestProbe[int] = testkit.create_test_probe()
        b = TestProbe[int](testkit, "named")
        assert "/system/testProbe-" in a.ref.path
        assert "/system/named-" in b.ref.path


async def test_spawn_does_not_race_the_guardian():
    async with ActorTestKit() as testkit:
        refs = [testkit.spawn(replier()) for _ in range(100)]
        assert len({r.path for r in refs}) == 100
        replies = await asyncio.gather(*(ask(r, lambda t: Reply(1, t), timeout=1) for r in refs))
        assert replies == [1] * 100
