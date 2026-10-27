import asyncio
import itertools
import random
import socket
from collections.abc import Callable


async def eventually(cond: Callable[[], bool], timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.002)


# Below the ephemeral ranges (Linux from 32768, macOS from 49152), so the local end of an outbound
# connection cannot take a port between free_port() and the test's bind. Counting up from a random
# start never hands out the same port twice in a run.
# ponytail: about 1700 ports per run before 32768; wrap around if the suite outgrows that.
_ports = itertools.count(random.randrange(20000, 31000))


def free_port() -> int:
    """A port nothing listens on, from a range the OS does not use for outbound connections."""
    for port in _ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise AssertionError("unreachable")
