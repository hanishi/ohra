import asyncio

import pytest


@pytest.fixture(autouse=True)
async def no_leaked_tasks():
    """Every test must leave nothing running: the Pekko-style 'stop means stopped'."""
    yield
    await asyncio.sleep(0)
    leaked = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    assert not leaked, f"leaked tasks: {leaked}"
