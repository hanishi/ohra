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
    last_state: Frame | None = None

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
        for frame in of(await drain(self.wall), "state"):
            self.last_state = frame
        assert self.last_state is not None
        return self.last_state

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
    return "abcdefgh"[i]


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


async def test_the_wall_sees_every_drop_with_the_rally_so_far():
    async with game() as g:
        i, incoming = await g.start()
        (first,) = of(await drain(g.wall), "drop")
        assert first == {
            "type": "drop",
            "to": player_id(i),
            "t": incoming["t"],
            "vy": pytest.approx(incoming["vy"]),
            "speed": pytest.approx(10.0),
            "rally": 0,
        }
        g.rally.tell(rr.Out(player_id(i), incoming["serial"], g.now(), 0.5, 0.0))
        j, passed = await g.puck()
        (second,) = of(await drain(g.wall), "drop")
        assert (second["to"], second["t"], second["rally"]) == (player_id(j), passed["t"], 1)
        assert second["speed"] == pytest.approx(speed(passed))
        g.rally.tell(rr.Miss(player_id(j), passed["serial"]))
        await g.settle(1.0)
        await g.puck()
        (served,) = of(await drain(g.wall), "drop")
        assert served["rally"] == 0  # a miss ends the rally


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


# --- Task 4: the end, the watchdog, leaving and coming back -------------------------------------


async def puck_to(g: Game, target: int, i: int, incoming: Frame) -> Frame:
    """Pass until the puck heads to phone ``target``; its incoming."""
    while i != target:
        g.rally.tell(rr.Out(player_id(i), incoming["serial"], g.now(), 0.5, 0.0))
        i, incoming = await g.puck()
    return incoming


async def test_the_game_ends_when_time_is_up_and_most_lives_win():
    async with game() as g:
        i, incoming = await g.start()
        g.rally.tell(rr.Miss(player_id(i), incoming["serial"]))
        await g.settle(180.0)
        state = await g.state()
        assert state["phase"] == "over"
        assert state["winners"] == [player_id(1 - i)]


async def test_the_state_counts_returns_fastest_saves_and_the_longest_rally():
    async with game() as g:
        i, incoming = await g.start()
        g.rally.tell(rr.Out(player_id(i), incoming["serial"], g.now(), 0.5, 0.0))
        j, passed = await g.puck()
        g.rally.tell(rr.Out(player_id(j), passed["serial"], g.now(), 0.5, 0.0))
        k, again = await g.puck()
        g.rally.tell(rr.Miss(player_id(k), again["serial"]))
        state = await g.state()
        stats = {p["id"]: (p["returns"], p["best"]) for p in state["players"]}
        assert stats == {
            player_id(i): (1, pytest.approx(10.0)),  # the serve
            player_id(j): (1, pytest.approx(10.0 * 1.06)),  # the pass, faster
        }
        assert state["longest"] == 2


async def test_a_new_game_counts_from_zero():
    async with game() as g:
        i, incoming = await g.start()
        g.rally.tell(rr.Out(player_id(i), incoming["serial"], g.now(), 0.5, 0.0))
        await g.settle(180.0)
        assert (await g.state())["longest"] == 1
        await g.settle(rr.LOBBY_AFTER_S)
        g.rally.tell(rr.Start())
        state = await g.state()
        assert state["longest"] == 0
        assert {(p["returns"], p["best"]) for p in state["players"]} == {(0, 0.0)}


async def test_a_tie_is_shared():
    async with game() as g:
        await g.start()
        await g.settle(180.0)
        state = await g.state()
        assert state["phase"] == "over"
        assert sorted(state["winners"]) == ["a", "b"]


async def test_the_game_ends_when_one_player_has_lives():
    async with game() as g:
        i, incoming = await g.start()
        for _ in range(5):
            incoming = await puck_to(g, 1, i, incoming)
            g.rally.tell(rr.Miss("b", incoming["serial"]))
            await g.settle(1.0)
            if (await g.state())["phase"] == "over":
                break
            i, incoming = await g.puck()
        state = await g.state()
        assert state["phase"] == "over"
        assert state["winners"] == ["a"]


async def test_back_to_the_lobby_after_fifteen_seconds():
    async with game() as g:
        await g.start()
        await g.settle(180.0)
        assert (await g.state())["phase"] == "over"
        await g.settle(15.0)
        state = await g.state()
        assert state["phase"] == "lobby"
        assert all(not p["in_game"] for p in state["players"])


async def test_the_watchdog_serves_again_without_a_life():
    async with game() as g:
        i, incoming = await g.start()
        await g.settle(15.0)
        _, again = await g.puck()
        assert again["serial"] > incoming["serial"]
        assert all(p["lives"] == 5 for p in (await g.state())["players"])


async def test_a_stale_report_is_ignored():
    async with game() as g:
        i, incoming = await g.start()
        await g.settle(15.0)
        await g.puck()
        g.rally.tell(rr.Out(player_id(i), incoming["serial"], g.now(), 0.5, 0.0))
        g.rally.tell(rr.Miss(player_id(i), incoming["serial"]))
        assert all(not of(fs, "incoming") for fs in await g.frames())
        wall = await drain(g.wall)
        assert not of(wall, "pass")
        assert all(p["lives"] == 5 for f in of(wall, "state") for p in f["players"])


async def test_a_disconnect_serves_again_and_keeps_the_lives():
    async with game(players=3) as g:
        i, _ = await g.start()
        g.phones[i].stop()
        await g.settle()
        state = await g.state()
        gone = {p["id"]: p for p in state["players"]}[player_id(i)]
        assert gone["connected"] is False and gone["lives"] == 5
        assert state["phase"] == "playing"
        others = [p for k, p in enumerate(g.phones) if k != i]
        await g.settle()
        got = [of(await drain(p), "incoming") for p in others]
        assert sum(len(x) for x in got) == 1


async def test_a_reconnect_with_the_same_id_keeps_the_lives():
    async with game(players=3) as g:
        i, incoming = await g.start()
        g.rally.tell(rr.Miss(player_id(i), incoming["serial"]))
        await g.settle()
        g.phones[i].stop()
        await g.settle()
        again = g.join(player_id(i), "ignored")
        await g.settle()
        frames = await drain(again)
        assert of(frames, "you")[0]["name"] == "Player" + player_id(i).upper()
        back = {p["id"]: p for p in (await g.state())["players"]}[player_id(i)]
        assert back["connected"] is True and back["lives"] == 4


async def test_the_older_tab_closing_does_not_disconnect_the_newer():
    async with game() as g:
        g.join("a", "PlayerA")
        await g.settle()
        g.phones[0].stop()
        await g.settle()
        a = {p["id"]: p for p in (await g.state())["players"]}["a"]
        assert a["connected"] is True


async def test_everyone_but_one_leaving_ends_the_game():
    async with game() as g:
        await g.start()
        g.phones[1].stop()
        await g.settle()
        state = await g.state()
        assert state["phase"] == "over"
        assert state["winners"] == ["a"]


async def test_a_player_who_joins_mid_game_waits():
    async with game() as g:
        i, incoming = await g.start()
        late = g.join("c", "Chi")
        await g.settle()
        c = {p["id"]: p for p in (await g.state())["players"]}["c"]
        assert c["in_game"] is False
        for _ in range(6):
            g.rally.tell(rr.Out(player_id(i), incoming["serial"], g.now(), 0.5, 0.0))
            i, incoming = await g.puck()
        assert not of(await drain(late), "incoming")


# --- Task 5: the app, over its sockets ----------------------------------------------------------


class First(random.Random):
    """Always picks the first: the serve goes to the first player, a pass to the next."""

    def choice(self, seq: Any) -> Any:
        return seq[0]


def until(ws: Any, kind: str) -> Frame:
    while True:
        frame = ws.receive_json()
        if frame["type"] == kind:
            return frame


def test_two_phones_and_a_wall_play_over_the_sockets():
    from fastapi.testclient import TestClient

    with TestClient(rr.make_app(rng=First())) as client:
        with (
            client.websocket_connect("/ws/wall") as wall,
            client.websocket_connect("/ws/player?id=a&name=Aki") as aki,
            client.websocket_connect("/ws/player?id=b&name=Ben") as ben,
        ):
            assert until(aki, "you")["name"] == "Aki"  # sent on connect, before any pong
            for ws in (wall, aki, ben):
                ws.send_json({"type": "ping", "c": 0})
                assert "s" in until(ws, "pong")
            wall.send_json({"type": "start"})
            assert until(aki, "state")["phase"] == "playing"
            incoming = until(aki, "incoming")
            aki.send_json(
                {
                    "type": "out",
                    "serial": incoming["serial"],
                    "t": incoming["t"] + 1,
                    "x": 0.5,
                    "angle": 0,
                }
            )
            assert until(ben, "incoming")["x"] == 0.5
            assert until(wall, "pass")["to"] == "b"


@pytest.mark.parametrize(
    "frame",
    [
        {"type": "hit"},
        {"type": "out", "serial": 1, "t": "soon", "x": 0.5, "angle": 0},
        {"type": "out", "serial": 1, "t": 1.0, "x": float("nan"), "angle": 0},
        {"type": "miss"},
    ],
)
def test_a_bad_frame_from_a_phone_closes_it(frame: Frame):
    from fastapi.testclient import TestClient
    from starlette.websockets import WebSocketDisconnect

    with TestClient(rr.make_app()) as client:
        with client.websocket_connect("/ws/player?id=a&name=Aki") as aki:
            aki.send_text(__import__("json").dumps(frame))
            with pytest.raises(WebSocketDisconnect) as closed:
                while True:
                    aki.receive_json()
            assert closed.value.code == 1003


built = pytest.mark.skipif(not (rr.PAGES / "phone.html").exists(), reason="run npm run build")


@built
def test_the_pages_and_clock_js_are_served():
    import re

    from fastapi.testclient import TestClient

    with TestClient(rr.make_app(), base_url="http://localhost") as client:
        assert "<!doctype html>" in client.get("/wall").text.lower()
        play = client.get("/play").text
        assert "<!doctype html>" in play.lower()
        script = client.get(re.findall(r'src="(/assets/[^"]+\.js)"', play)[0])
        assert script.status_code == 200
        assert script.headers["content-type"].startswith("text/javascript")
        assert "export function connect" in client.get("/ohra/clock.js").text
        url = client.get("/join").json()["url"]
        assert url.startswith("http://") and url.endswith(":80/play")


@built
def test_the_pages_are_revalidated_so_a_redeploy_never_leaves_stale_asset_names():
    from fastapi.testclient import TestClient

    with TestClient(rr.make_app(), base_url="http://localhost") as client:
        for path in ("/wall", "/play"):
            assert client.get(path).headers["cache-control"] == "no-cache"


@built
def test_the_build_imports_clock_js_from_the_server():
    scripts = [f.read_text() for f in (rr.PAGES / "assets").glob("*.js")]
    assert any('"/ohra/clock.js"' in s for s in scripts)
    assert not any("SAMPLE_WINDOW" in s for s in scripts)  # nothing of clock.js is bundled


@built
def test_the_built_wall_loads_the_qr_library_async():
    page = (rr.PAGES / "wall.html").read_text()
    (tag,) = [line for line in page.splitlines() if "qrcode.min.js" in line]
    assert " async" in tag


def test_the_pages_say_to_build_when_dist_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(rr, "PAGES", tmp_path)
    with TestClient(rr.make_app(), base_url="http://localhost") as client:
        play = client.get("/play")
        assert play.status_code == 503
        assert play.text == rr.BUILD_FIRST
        assert client.get("/assets/phone.js").status_code == 503  # not a 500
        assert client.get("/join").status_code == 200


def test_the_join_url_is_the_address_the_wall_was_opened_at():
    from fastapi.testclient import TestClient

    with TestClient(rr.make_app(), base_url="https://rally.example.com") as client:
        assert client.get("/join").json()["url"] == "https://rally.example.com/play"


def test_the_join_url_falls_back_to_localhost(monkeypatch: pytest.MonkeyPatch):
    import socket

    def broken(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("no network")

    monkeypatch.setattr(socket, "socket", broken)
    assert rr.lan_url(8000) == "http://127.0.0.1:8000/play"


# --- review fixes -------------------------------------------------------------------------------


async def test_a_late_out_lands_in_the_future():
    async with game() as g:
        i, incoming = await g.start()
        await g.settle(5.0)
        g.rally.tell(rr.Out(player_id(i), incoming["serial"], g.now() - 4.0, 0.5, 0.0))
        _, passed = await g.puck()
        assert passed["t"] == pytest.approx(g.now() + 0.6)
        (pass_,) = of(await drain(g.wall), "pass")
        assert pass_["t0"] == pytest.approx(g.now())


async def test_time_up_ends_the_game_on_a_clock_that_runs_a_little_slow():
    async with ActorTestKit(manual_time=True) as kit:
        manual = ManualTime(kit.system)
        rally = kit.spawn(
            rr.rally(clock=lambda: manual.scheduler.current_time * 0.999, rng=random.Random(1))
        )
        wall: TestProbe[Frame] = kit.create_test_probe()
        rally.tell(rr.WatchWall(wall.ref))
        g = Game(kit, manual, rally, wall, [])
        g.phones = [g.join("a", "A"), g.join("b", "B")]
        await g.start()
        await g.settle(180.0)
        assert (await g.state())["phase"] == "over"


async def test_a_new_player_never_shares_a_color_or_a_name():
    async with game(players=0) as g:
        g.phones = [g.join("a", "A"), g.join("b", "B"), g.join("c", " ")]
        await g.settle()
        g.phones[1].stop()
        await g.settle()
        await g.start()  # drops b, who is gone
        g.join("d", " ")
        players = (await g.state())["players"]
        assert len({p["color"] for p in players}) == len(players)
        assert len({p["name"] for p in players}) == len(players)


def test_the_wall_does_not_wait_for_the_qr_library():
    page = (Path(rr.__file__).parent / "relay_rally" / "wall.html").read_text()
    (tag,) = [line for line in page.splitlines() if "qrcode.min.js" in line]
    assert " async" in tag  # defer would still hold the module script back behind it


def test_the_phone_knows_the_servers_base_speed():
    page = (Path(rr.__file__).parent / "relay_rally" / "src" / "phone.ts").read_text()
    (line,) = [x for x in page.splitlines() if "const BASE_SPEED = " in x]
    assert float(line.split("=")[1].split(";")[0]) == rr.BASE_SPEED  # the hit's pitch follows it


def test_the_wall_and_the_phone_agree_on_the_paddle_line():
    pages = Path(rr.__file__).parent / "relay_rally" / "src"
    (wall,) = [x for x in (pages / "wall.ts").read_text().splitlines() if "PADDLE_Y = " in x]
    (phone,) = [x for x in (pages / "phone.ts").read_text().splitlines() if "PADDLE_Y = " in x]
    assert "H = 16, PADDLE_Y = H - 1.4," in phone
    assert float(wall.split("=")[1].split(";")[0]) == pytest.approx(16 - 1.4)  # the closing ring


def test_the_phone_uses_no_api_that_needs_https():
    page = (Path(rr.__file__).parent / "relay_rally" / "src" / "phone.ts").read_text()
    assert "randomUUID" not in page  # the join URL is plain http on the LAN


def test_the_books_clock_js_route_sets_its_media_type():
    book = Path(__file__).resolve().parents[1] / "docs" / "book" / "09-fastapi.md"
    (line,) = [x for x in book.read_text().splitlines() if "FileResponse(CLOCK_JS" in x]
    assert 'media_type="text/javascript"' in line


# --- at most 8 players --------------------------------------------------------------------------


async def test_a_ninth_player_is_told_the_game_is_full():
    async with game(players=8) as g:
        ninth = g.join("i", "Ivy")
        await g.settle()
        assert of(await drain(ninth), "full")
        assert [p["id"] for p in (await g.state())["players"]] == list("abcdefgh")


async def test_a_player_who_leaves_the_lobby_frees_the_spot():
    async with game(players=8) as g:
        g.phones[0].stop()
        await g.settle()
        ninth = g.join("i", "Ivy")
        await g.settle()
        frames = await drain(ninth)
        assert of(frames, "you") and not of(frames, "full")
        assert "a" not in [p["id"] for p in (await g.state())["players"]]


async def test_a_player_away_mid_game_keeps_the_spot():
    async with game(players=8) as g:
        await g.start()
        g.phones[0].stop()
        await g.settle()
        ninth = g.join("i", "Ivy")
        await g.settle()
        assert of(await drain(ninth), "full")


def test_a_full_phone_stays_connected_long_enough_to_sync_and_see_it():
    from fastapi.testclient import TestClient
    from starlette.websockets import WebSocketDisconnect

    with TestClient(rr.make_app()) as client:
        phones = [client.websocket_connect(f"/ws/player?id={i}&name=P") for i in range(8)]
        for p in phones:
            p.__enter__()
            until(p, "you")
        with client.websocket_connect("/ws/player?id=late&name=Late") as late:
            assert late.receive_json() == {"type": "full"}
            late.send_json({"type": "ping", "c": 0})  # clock.js shows nothing before its pong
            assert until(late, "pong")["c"] == 0
            with pytest.raises(WebSocketDisconnect):  # then it closes: the page retries
                while True:
                    late.receive_json()
        for p in phones:
            p.__exit__(None, None, None)


# --- computer players ---------------------------------------------------------------------------


def seats(state: Frame) -> list[tuple[str, bool]]:
    return [(p["name"], p["cpu"]) for p in state["players"]]


async def test_add_cpu_seats_a_computer_player():
    async with game(players=1) as g:
        g.rally.tell(rr.AddCpu())
        state = await g.state()
        assert seats(state) == [("PlayerA", False), ("CPU 1", True)]
        assert state["players"][1]["connected"]


async def test_add_cpu_is_refused_when_every_seat_is_taken():
    async with game(players=7) as g:
        g.rally.tell(rr.AddCpu())
        g.rally.tell(rr.AddCpu())
        assert len((await g.state())["players"]) == rr.MAX_PLAYERS


async def test_remove_cpu_frees_the_newest_computer_seat():
    async with game(players=1) as g:
        g.rally.tell(rr.AddCpu())
        g.rally.tell(rr.AddCpu())
        g.rally.tell(rr.RemoveCpu())
        g.rally.tell(rr.RemoveCpu())
        g.rally.tell(rr.RemoveCpu())  # none left: nothing happens
        assert seats(await g.state()) == [("PlayerA", False)]


async def test_one_player_and_a_cpu_can_start():
    async with game(players=1) as g:
        g.rally.tell(rr.AddCpu())
        g.rally.tell(rr.Start())
        assert (await g.state())["phase"] == "playing"


async def test_cpus_are_only_added_and_removed_in_the_lobby():
    async with game(players=1) as g:
        g.rally.tell(rr.AddCpu())
        await g.start()
        g.rally.tell(rr.AddCpu())
        g.rally.tell(rr.RemoveCpu())
        assert seats(await g.state()) == [("PlayerA", False), ("CPU 1", True)]


class Always(random.Random):
    """random() is always ``r``; uniform(a, b) is the middle."""

    def __init__(self, r: float) -> None:
        super().__init__()
        self.r = r

    def random(self) -> float:
        return self.r

    def uniform(self, a: float, b: float) -> float:
        return (a + b) / 2


async def cpu_answer(r: float) -> tuple[float, Any]:
    """A CPU given a puck that enters at t = 1, straight down at 10 units a second: when it
    answered and what."""
    async with ActorTestKit(manual_time=True) as kit:
        manual = ManualTime(kit.system)
        rally: TestProbe[Any] = kit.create_test_probe()
        now = lambda: manual.scheduler.current_time  # noqa: E731
        cpu = kit.spawn(rr.cpu("cpu-1", rally.ref, now, Always(r)))
        cpu.tell({"type": "incoming", "serial": 7, "t": 1.0, "x": 0.5, "vx": 0.0, "vy": 10.0})
        for _ in range(50):
            await manual.time_passes(0.1)
            try:
                return now(), await rally.receive_message(0.01)
            except AssertionError:
                pass
        raise AssertionError("the CPU never answered")


async def test_a_cpu_that_hits_reports_the_puck_out_when_it_leaves_the_top():
    at, msg = await cpu_answer(0.0)
    # down 16 units at 10 a second, then back up straight: out at 1 + 1.6 + 1.6
    assert msg == rr.Out("cpu-1", 7, pytest.approx(4.2), 0.5, 0.0)
    assert at == pytest.approx(4.2, abs=0.11)


async def test_a_cpu_that_misses_reports_it_when_the_puck_reaches_its_paddle():
    at, msg = await cpu_answer(0.999)
    assert msg == rr.Miss("cpu-1", 7)
    assert at == pytest.approx(2.6, abs=0.11)


async def test_cpus_play_a_whole_game_by_themselves():
    async with game(players=0, seed=3) as g:
        for _ in range(3):
            g.rally.tell(rr.AddCpu())
        g.rally.tell(rr.Start())
        await g.settle(rr.GAME_S + 1)
        frames = await drain(g.wall)
        assert len(of(frames, "pass")) > 20
        over = of(frames, "state")[-1]
        assert over["phase"] == "over" and over["winners"]
        assert any(p["lives"] < rr.LIVES for p in over["players"])


def test_the_wall_adds_and_removes_cpus_over_its_socket():
    from fastapi.testclient import TestClient

    with TestClient(rr.make_app()) as client:
        with client.websocket_connect("/ws/wall") as wall:
            until(wall, "state")
            wall.send_json({"type": "add_cpu"})
            assert seats(until(wall, "state")) == [("CPU 1", True)]
            wall.send_json({"type": "remove_cpu"})
            assert seats(until(wall, "state")) == []
