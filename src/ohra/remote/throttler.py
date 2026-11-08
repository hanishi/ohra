"""Throttle modes and a per-peer ``Throttler``, ported from Pekko's
``ThrottlerTransportAdapter.scala`` (``Direction``, ``ThrottleMode``, ``TokenBucket``,
``Unthrottled``, ``Blackhole``). Pure: no I/O. ``Remoting`` asks ``admit`` per frame."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum


class Direction(Enum):
    SEND = "send"
    RECEIVE = "receive"
    BOTH = "both"

    def includes(self, other: Direction) -> bool:
        return self is Direction.BOTH or self is other


class ThrottleMode:
    def try_consume_tokens(self, nano_time_of_send: int, tokens: int) -> tuple[ThrottleMode, bool]:
        raise NotImplementedError

    def time_to_available(self, current_nano_time: int, tokens: int) -> float:
        raise NotImplementedError


@dataclass(frozen=True)
class TokenBucket(ThrottleMode):
    capacity: int
    tokens_per_second: float
    nano_time_of_last_send: int
    available_tokens: int

    def _tokens_generated(self, now: int) -> int:
        return int(
            (now - self.nano_time_of_last_send) // 1_000_000 * self.tokens_per_second / 1000.0
        )

    def _is_available(self, now: int, tokens: int) -> bool:
        if tokens > self.capacity and self.available_tokens > 0:
            return True  # larger than capacity: let it through, recorded as negative tokens
        return min(self.available_tokens + self._tokens_generated(now), self.capacity) >= tokens

    def try_consume_tokens(self, nano_time_of_send: int, tokens: int) -> tuple[ThrottleMode, bool]:
        if self._is_available(nano_time_of_send, tokens):
            generated = self._tokens_generated(nano_time_of_send)
            return (
                TokenBucket(
                    self.capacity,
                    self.tokens_per_second,
                    nano_time_of_send,
                    min(self.available_tokens - tokens + generated, self.capacity),
                ),
                True,
            )
        return self, False

    def time_to_available(self, current_nano_time: int, tokens: int) -> float:
        needed = (1 if tokens > self.capacity else tokens) - self._tokens_generated(
            current_nano_time
        )
        return needed / self.tokens_per_second


@dataclass(frozen=True)
class Unthrottled(ThrottleMode):
    def try_consume_tokens(self, nano_time_of_send: int, tokens: int) -> tuple[ThrottleMode, bool]:
        return self, True

    def time_to_available(self, current_nano_time: int, tokens: int) -> float:
        return 0.0


@dataclass(frozen=True)
class Blackhole(ThrottleMode):
    def try_consume_tokens(self, nano_time_of_send: int, tokens: int) -> tuple[ThrottleMode, bool]:
        return self, False

    def time_to_available(self, current_nano_time: int, tokens: int) -> float:
        return 0.0


type _Key = tuple[str, int, Direction]


class Throttler:
    """The mode per (host, port, SEND | RECEIVE); an unknown key is ``Unthrottled``."""

    def __init__(self) -> None:
        self._modes: dict[_Key, ThrottleMode] = {}
        self._changed: dict[_Key, asyncio.Event] = {}

    def set_throttle(self, host: str, port: int, direction: Direction, mode: ThrottleMode) -> None:
        for d in (Direction.SEND, Direction.RECEIVE):
            if direction.includes(d):
                key = (host, port, d)
                self._modes[key] = mode
                if (event := self._changed.pop(key, None)) is not None:
                    event.set()

    async def admit(self, host: str, port: int, direction: Direction, size: int) -> bool:
        key = (host, port, direction)
        while True:
            mode = self._modes.get(key, Unthrottled())
            now = time.monotonic_ns()
            new_mode, ok = mode.try_consume_tokens(now, size)
            if ok:
                if new_mode is not mode:
                    self._modes[key] = new_mode
                return True
            if isinstance(mode, Blackhole):
                return False
            event = self._changed.setdefault(key, asyncio.Event())
            # ponytail: 1ms floor, Pekko's timeToAvailable can be <= 0 after a loan
            delay = max(mode.time_to_available(now, size), 0.001)
            try:
                await asyncio.wait_for(event.wait(), delay)
            except TimeoutError:
                pass
