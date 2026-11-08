"""Remoting consults a Throttler on send and receive. Ported from Pekko's
ThrottlerTransportAdapterSpec (remote/src/test/scala/org/apache/pekko/remote/classic/transport/
ThrottlerTransportAdapterSpec.scala)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from ohra import (
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    EventStream,
    remotable,
)
from ohra.ref import FunctionRef
from ohra.remote import Direction
from ohra.remote.throttler import Blackhole, Throttler, TokenBucket, Unthrottled

from .util import eventually, free_port


@remotable
@dataclass(frozen=True)
class Echo:
    value: str
    reply_to: ActorRef[str]


def echo(seen: list[str]) -> Behavior[Echo]:
    @Behaviors.receive_message
    def on_message(msg: Echo) -> Behavior[Echo]:
        seen.append(msg.value)
        msg.reply_to.tell("pong" if msg.value == "ping" else msg.value)
        return Behaviors.same

    return on_message


@dataclass
class Pair:
    a: ActorSystem[None]
    b: ActorSystem[None]
    throttler: Throttler
    seen_on_b: list[str]
    replies: list[str]
    reply_ref: ActorRef[str]

    @property
    def echo_path(self) -> str:
        return f"{self.b.path}/echo"

    @property
    def b_port(self) -> int:
        assert self.b.address is not None
        return self.b.address.port

    def send(self, value: str) -> None:
        self.a.resolve(self.echo_path).tell(Echo(value, self.reply_ref))

    def frame_size(self, value: str) -> int:
        assert self.a.remoting is not None
        return len(self.a.remoting._frame(self.echo_path, Echo(value, self.reply_ref)))  # pyright: ignore[reportPrivateUsage]


@asynccontextmanager
async def pair() -> AsyncGenerator[Pair, None]:
    async with (
        ActorSystem(Behaviors.empty, "a", address=("127.0.0.1", free_port())) as a,
        ActorSystem(Behaviors.empty, "b", address=("127.0.0.1", free_port())) as b,
    ):
        assert a.remoting is not None
        a.remoting.throttler = throttler = Throttler()
        seen: list[str] = []
        b._root.spawn_child(echo(seen), "echo", None)  # pyright: ignore[reportPrivateUsage]
        replies: list[str] = []
        yield Pair(a, b, throttler, seen, replies, FunctionRef("replies", replies.append))


async def test_remoting_has_no_throttler_by_default():
    async with ActorSystem(Behaviors.empty, "a", address=("127.0.0.1", free_port())) as a:
        assert a.remoting is not None and a.remoting.throttler is None


async def test_a_throttled_send_keeps_the_average_rate():
    # Pekko's "maintain average message rate", scaled to about 1s
    async with pair() as p:
        ping_size = p.frame_size("ping")
        count, total = 30, 1.0
        p.throttler.set_throttle(
            "127.0.0.1",
            p.b_port,
            Direction.SEND,
            TokenBucket(2 * ping_size, 30 * ping_size / total, 0, 0),
        )
        start = time.monotonic()
        for _ in range(count):
            p.send("ping")
        await eventually(lambda: len(p.replies) == count, timeout=total + 3)
        assert time.monotonic() - start > total * 0.6
        p.throttler.set_throttle("127.0.0.1", p.b_port, Direction.SEND, Unthrottled())


async def test_a_blackholed_send_drops_frames_without_dead_letters():
    async with pair() as p:
        dead: list[DeadLetter] = []
        p.a.event_stream.tell(EventStream.Subscribe(DeadLetter, FunctionRef("dl", dead.append)))
        p.throttler.set_throttle("127.0.0.1", p.b_port, Direction.SEND, Blackhole())
        p.send("lost")
        await asyncio.sleep(0.3)
        assert p.seen_on_b == [] and p.replies == [] and dead == []


async def test_a_blackholed_receive_drops_only_inbound_frames():
    async with pair() as p:
        p.throttler.set_throttle("127.0.0.1", p.b_port, Direction.RECEIVE, Blackhole())
        p.send("hello")
        await eventually(lambda: p.seen_on_b == ["hello"])
        await asyncio.sleep(0.3)
        assert p.replies == []


async def test_a_blackhole_set_before_any_connection_still_connects():
    async with pair() as p:
        assert p.a.remoting is not None
        p.throttler.set_throttle("127.0.0.1", p.b_port, Direction.BOTH, Blackhole())
        p.send("first")
        await asyncio.sleep(0.3)
        assert p.seen_on_b == [] and p.replies == []
        p.throttler.set_throttle("127.0.0.1", p.b_port, Direction.BOTH, Unthrottled())
        p.send("second")
        await eventually(lambda: p.replies == ["second"])
        assert len(p.a.remoting._associations) == 1  # pyright: ignore[reportPrivateUsage]


async def test_frames_waiting_under_a_slow_bucket_are_released_in_order():
    async with pair() as p:
        size = p.frame_size("1")
        p.throttler.set_throttle(
            "127.0.0.1",
            p.b_port,
            Direction.SEND,
            TokenBucket(size, 1.0, time.monotonic_ns(), 0),
        )
        for i in "12345":
            p.send(i)
        await asyncio.sleep(0.1)
        assert len(p.replies) < 5
        p.throttler.set_throttle("127.0.0.1", p.b_port, Direction.SEND, Unthrottled())
        await eventually(lambda: p.replies == list("12345"))


async def test_survives_blackholing():
    # Pekko's "survive blackholing", waits scaled to 0.3s
    async with pair() as p:
        assert p.a.remoting is not None
        p.send("Blackhole 1")
        await eventually(lambda: "Blackhole 1" in p.replies)
        p.throttler.set_throttle("127.0.0.1", p.b_port, Direction.BOTH, Blackhole())
        p.send("Blackhole 2")
        await asyncio.sleep(0.3)
        assert "Blackhole 2" not in p.replies
        await p.a.remoting.disconnect("127.0.0.1", p.b_port)
        await asyncio.sleep(0.3)
        assert "Blackhole 2" not in p.replies
        p.throttler.set_throttle("127.0.0.1", p.b_port, Direction.BOTH, Unthrottled())

        def resend() -> bool:
            p.send("Blackhole 3")
            return "Blackhole 3" in p.replies

        await eventually(resend, timeout=5)
        p.replies.clear()
        p.send("Cleanup")
        await eventually(lambda: "Cleanup" in p.replies)


async def _round_trip_then_disconnect(abort: bool) -> None:
    async with pair() as p:
        assert p.a.remoting is not None and p.b.remoting is not None
        p.send("one")
        await eventually(lambda: p.replies == ["one"])
        # B answers over its own association to A, which A accepts
        await eventually(lambda: ("127.0.0.1", p.b_port) in p.a.remoting._inbound)  # pyright: ignore[reportPrivateUsage]
        await p.a.remoting.disconnect("127.0.0.1", p.b_port, abort)
        assert ("127.0.0.1", p.b_port) not in p.a.remoting._associations  # pyright: ignore[reportPrivateUsage]
        assert ("127.0.0.1", p.b_port) not in p.a.remoting._inbound  # pyright: ignore[reportPrivateUsage]
        p.send("two")
        await eventually(lambda: "two" in p.replies)


async def test_disconnect_closes_both_links_and_the_next_send_reconnects():
    await _round_trip_then_disconnect(False)


async def test_abort_closes_both_links_and_the_next_send_reconnects():
    await _round_trip_then_disconnect(True)


async def test_disconnect_from_an_unknown_peer_returns():
    async with pair() as p:
        assert p.a.remoting is not None
        await p.a.remoting.disconnect("127.0.0.1", 1)
