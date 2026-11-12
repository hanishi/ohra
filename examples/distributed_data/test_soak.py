"""The soak, ohra's own: each sample under random load on three nodes, then a broken one.

Not a port of a Pekko spec. The first test runs ``soak.run`` as the CLI does, briefly; the second
pins that an actor answering differently from the converged views is a violation, and that a
failed check does not hang; the third, that a cancellation of ``run`` propagates. The last three
pin the lag store: its run statistics stay bounded, and only a view a node had to wait for is a
lag.
"""

import asyncio
import random
from dataclasses import replace
from typing import Any

import pytest

import soak
from ohra import ActorRef
from ohra.cluster.ddata import Flag, FlagKey


@pytest.mark.parametrize("sample", ["voting", "cart", "cache", "metrics"])
async def test_each_sample_converges_under_load(sample: str):
    lines: list[str] = []
    result = await soak.run(
        sample,
        nodes=3,
        rate=50,
        duration=5,
        check_every=4,
        report_every=1,
        seed=1,
        out=lines.append,
    )
    assert result.checks_passed >= 2  # the timed check and the final one
    assert result.checks_failed == 0
    assert result.commands > 0
    assert max(result.lags) > 0
    assert any(line.startswith("check #1:") and line.endswith("actors agree") for line in lines)


async def test_a_disagreeing_actor_is_a_violation(monkeypatch: pytest.MonkeyPatch):
    cache = soak.WORKLOADS["cache"]

    async def query(ref: ActorRef[Any], state: soak.SoakState) -> object:
        return (await cache.query(ref, state), state.refs.index(ref))

    monkeypatch.setitem(soak.WORKLOADS, "broken", replace(cache, query=query))
    lines: list[str] = []
    loop = asyncio.get_running_loop()
    start = loop.time()
    async with asyncio.timeout(10):
        result = await soak.run(
            "broken",
            nodes=2,
            rate=20,
            duration=3,
            check_every=2,
            check_timeout=2,
            seed=1,
            out=lines.append,
        )
    assert loop.time() - start < 10
    assert result.checks_failed >= 1
    assert any(line.startswith("VIOLATION check #") for line in lines)


async def test_a_cancellation_propagates():
    reported = asyncio.Event()

    def out(line: str) -> None:
        if line.startswith("t="):
            reported.set()

    task = asyncio.create_task(soak.run("cache", nodes=2, rate=20, report_every=0.5, out=out))
    async with asyncio.timeout(30):
        await reported.wait()
    task.cancel()
    async with asyncio.timeout(10):
        with pytest.raises(asyncio.CancelledError):
            await task


def test_the_run_statistics_stay_bounded():
    lags = soak.Lags(2, random.Random(1), capacity=10)
    for i in range(1000):
        lags.record(i / 1000)
    assert len(lags.sample) == 10
    assert lags.count == 1000
    assert lags.max == 0.999


def test_a_view_the_other_node_already_holds_is_no_lag():
    lags = soak.Lags(2, random.Random(1))
    key = FlagKey("open")
    lags.changed(1, key, Flag.Enabled, 0.0)
    lags.changed(0, key, Flag.Enabled, 0.5)  # relayed from node 1, which holds it already
    assert lags.sample == [0.5]  # node 0's wait for node 1's change, and nothing for the relay


def test_a_view_the_other_node_lacks_is_a_lag_once_it_catches_up():
    lags = soak.Lags(2, random.Random(1))
    key = FlagKey("open")
    lags.changed(0, key, Flag.Enabled, 0.0)
    assert lags.count == 0
    lags.changed(1, key, Flag.Enabled, 0.3)
    assert lags.sample == [0.3]
