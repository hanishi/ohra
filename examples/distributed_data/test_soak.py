"""The soak, ohra's own: each sample under random load on three nodes, then a broken one.

Not a port of a Pekko spec. The first test runs ``soak.run`` as the CLI does, briefly; the second
pins that an actor answering differently from the converged views is a violation, and that a
failed check does not hang.
"""

import asyncio
from dataclasses import replace
from typing import Any

import pytest

import soak
from ohra import ActorRef


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
    assert result.lags
    assert any(line.startswith("check #1:") and line.endswith("actors agree") for line in lines)


async def test_a_disagreeing_actor_is_a_violation(monkeypatch: pytest.MonkeyPatch):
    cache = soak.WORKLOADS["cache"]

    async def query(ref: ActorRef[Any], state: soak.SoakState) -> object:
        return (await cache.query(ref, state), state.refs.index(ref))

    monkeypatch.setitem(soak.WORKLOADS, "broken", replace(cache, query=query))
    lines: list[str] = []
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
    assert result.checks_failed >= 1
    assert any(line.startswith("VIOLATION check #") for line in lines)
