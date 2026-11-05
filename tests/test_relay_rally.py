"""examples/fastapi_relay_rally.py: the game actor, with probes for the phones and the wall and
the test kit's manual time for the clock."""

from __future__ import annotations

import importlib.util
import math
import random
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")

from ohra import ActorRef  # noqa: E402
from ohra.testkit import ActorTestKit, ManualTime, TestProbe  # noqa: E402


def _load() -> Any:
    """The example, loaded from its file as a reader runs it."""
    path = Path(__file__).resolve().parents[1] / "examples" / "fastapi_relay_rally.py"
    spec = importlib.util.spec_from_file_location("fastapi_relay_rally", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rr = _load()

type Frame = dict[str, Any]


async def drain(probe: TestProbe[Frame]) -> list[Frame]:
    """Every frame the probe has now."""
    frames: list[Frame] = []
    while True:
        try:
            frames.append(await probe.receive_message(0.02))
        except AssertionError:
            return frames


def of(frames: list[Frame], kind: str) -> list[Frame]:
    return [f for f in frames if f["type"] == kind]


@dataclass
class Game:
    kit: ActorTestKit
    manual: ManualTime
    rally: ActorRef[Any]
    wall: TestProbe[Frame]
    phones: list[TestProbe[Frame]]

    def now(self) -> float:
        return self.manual.scheduler.current_time

    async def settle(self, seconds: float = 0.0) -> None:
        await self.manual.time_passes(seconds)

    def join(self, id: str, name: str) -> TestProbe[Frame]:
        probe: TestProbe[Frame] = self.kit.create_test_probe()
        self.rally.tell(rr.Join(id, name, probe.ref))
        return probe

    async def frames(self) -> list[list[Frame]]:
        """Everything each phone has now, in phone order."""
        await self.settle()
        return [await drain(p) for p in self.phones]

    async def state(self) -> Frame:
        """The last state the wall was told."""
        await self.settle()
        return of(await drain(self.wall), "state")[-1]

    async def start(self) -> tuple[int, Frame]:
        """Start, and the phone that got the first serve, with its incoming."""
        self.rally.tell(rr.Start())
        return await self.puck()

    async def puck(self) -> tuple[int, Frame]:
        """The one phone with an incoming now, and that incoming."""
        got = [(i, of(fs, "incoming")) for i, fs in enumerate(await self.frames())]
        got = [(i, inc[-1]) for i, inc in got if inc]
        assert len(got) == 1, got
        return got[0]


def player_id(i: int) -> str:
    return "abc"[i]


@asynccontextmanager
async def game(players: int = 2, seed: int = 1) -> AsyncGenerator[Game]:
    async with ActorTestKit(manual_time=True) as kit:
        manual = ManualTime(kit.system)
        rally = kit.spawn(
            rr.rally(clock=lambda: manual.scheduler.current_time, rng=random.Random(seed))
        )
        wall: TestProbe[Frame] = kit.create_test_probe()
        rally.tell(rr.WatchWall(wall.ref))
        g = Game(kit, manual, rally, wall, [])
        for i in range(players):
            g.phones.append(g.join(player_id(i), "Player" + player_id(i).upper()))
        await g.settle()
        yield g


def speed(incoming: Frame) -> float:
    return math.hypot(incoming["vx"], incoming["vy"])


# --- Task 3: lobby, serve, pass, miss -----------------------------------------------------------


async def test_a_join_gets_you_and_everyone_gets_the_state():
    async with game(players=1) as g:
        (frames,) = await g.frames()
        you = of(frames, "you")[0]
        assert you["id"] == "a" and you["name"] == "PlayerA" and you["color"]
        state = of(frames, "state")[-1]
        assert state["phase"] == "lobby"
        assert [(p["id"], p["in_game"], p["connected"]) for p in state["players"]] == [
            ("a", False, True)
        ]
        assert (await g.state())["players"][0]["name"] == "PlayerA"


async def test_a_long_name_is_cut_and_a_blank_one_is_numbered():
    async with game(players=0) as g:
        g.join("a", "x" * 20)
        g.join("b", "   ")
        names = [p["name"] for p in (await g.state())["players"]]
        assert names == ["x" * 12, "P2"]


async def test_start_needs_two_players_and_is_ignored_while_playing():
    async with game(players=1) as g:
        g.rally.tell(rr.Start())
        assert (await g.state())["phase"] == "lobby"
        g.phones.append(g.join("b", "Ben"))
        await g.start()
        state = await g.state()
        assert state["phase"] == "playing"
        assert state["ends_at"] == pytest.approx(g.now() + 180.0)
        assert [(p["lives"], p["in_game"]) for p in state["players"]] == [(5, True), (5, True)]
        g.rally.tell(rr.Start())
        assert all(not of(fs, "incoming") for fs in await g.frames())  # no second serve


async def test_the_first_serve_goes_to_a_player_one_second_ahead():
    async with game() as g:
        _, incoming = await g.start()
        assert incoming["t"] == pytest.approx(g.now() + 1.0)
        assert speed(incoming) == pytest.approx(10.0)
        assert abs(math.atan2(incoming["vx"], incoming["vy"])) <= math.radians(20) + 1e-9
        assert 0.0 <= incoming["x"] <= 1.0


async def test_out_passes_to_another_player_faster():
    async with game() as g:
        i, incoming = await g.start()
        g.rally.tell(rr.Out(player_id(i), incoming["serial"], 5.0, 0.3, 0.2))
        j, passed = await g.puck()
        assert j != i
        assert passed["t"] == pytest.approx(5.6) and passed["x"] == pytest.approx(0.3)
        assert passed["serial"] > incoming["serial"]
        s = 10.0 * 1.06
        assert passed["vx"] == pytest.approx(s * math.sin(0.2))
        assert passed["vy"] == pytest.approx(s * math.cos(0.2))
        await g.settle()
        (pass_,) = of(await drain(g.wall), "pass")
        assert pass_ == {
            "type": "pass",
            "from": player_id(i),
            "to": player_id(j),
            "t0": 5.0,
            "t1": pytest.approx(5.6),
        }


async def test_a_miss_costs_a_life_and_serves_again_at_base_speed():
    async with game() as g:
        i, incoming = await g.start()
        g.rally.tell(rr.Out(player_id(i), incoming["serial"], 1.0, 0.5, 0.0))
        j, passed = await g.puck()
        g.rally.tell(rr.Miss(player_id(j), passed["serial"]))
        lives = {p["id"]: p["lives"] for p in (await g.state())["players"]}
        assert lives == {player_id(i): 5, player_id(j): 4}
        await g.settle(1.0)
        _, served = await g.puck()
        assert speed(served) == pytest.approx(10.0)
