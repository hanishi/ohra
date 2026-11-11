import asyncio
import logging
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import contextmanager
from logging.handlers import BufferingHandler

from ohra.testkit.multi_node_spec import free_port as free_port


async def eventually(cond: Callable[[], bool], timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.002)


def suspending(
    log: list[str], started: asyncio.Event, *first: int
) -> Callable[[], AsyncGenerator[int, None]]:
    """An upstream for ``Source.from_async_iterable``: yields ``first``, sets ``started`` and
    suspends for good inside its pull; closing it appends "closed" to ``log``."""

    async def gen() -> AsyncGenerator[int, None]:
        try:
            for x in first:
                yield x
            started.set()
            await asyncio.Event().wait()  # never set
            yield -1
        finally:
            log.append("closed")

    return gen


@contextmanager
def errors_logged(logger: str = "ohra.actor") -> Generator[list[logging.LogRecord]]:
    """The records at ERROR and above that ``logger`` and its children log inside the block."""
    handler = BufferingHandler(2**31)  # never flushes, so the buffer keeps every record
    handler.setLevel(logging.ERROR)
    logging.getLogger(logger).addHandler(handler)
    try:
        yield handler.buffer
    finally:
        logging.getLogger(logger).removeHandler(handler)


class TestClock:
    """Pekko's ``TestClock.scala``: a controlled clock for recency windows, in seconds."""

    __test__ = False

    def __init__(self) -> None:
        self.now: float = 0.0

    def tick(self) -> None:
        self.now += 1.0

    def __call__(self) -> float:
        return self.now
