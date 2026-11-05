"""Relay Rally: phones are the courts and the paddles, a big screen is the scoreboard, and the
puck is passed from phone to phone at random. Every page is on the server's clock with
``synced`` and ``clock.js``: the server says when the puck drops onto a phone, and the phone
draws it from ``conn.now()``, so nothing is streamed.

    uv run --extra web uvicorn examples.fastapi_relay_rally:app --host 0.0.0.0 --port 8000

then open http://localhost:8000/wall on a big screen and scan its code with phones on the same
Wi-Fi, up to 8 of them. Press START with two or more players: each has 5 lives, a miss costs
one, and after 3:00 (or when one player is left) the most lives win. +CPU on the wall adds a
computer player, so one person can play, or nobody: CPUs play each other.

``rally`` is the game, the one place its state lives; ``player`` and ``wall`` are one actor per
connection, relaying between it and the page. ``cpu`` is a player with no connection: it gets the
frames a phone gets and answers as a phone does. The phones are trusted for their hits and misses:
it is a party game.
"""

import itertools
import json
import math
import random
import socket
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fastapi import FastAPI, Query, Request, WebSocket  # noqa: E402
from fastapi.responses import FileResponse, RedirectResponse  # noqa: E402

from ohra import ActorContext, ActorRef, Behavior, Behaviors  # noqa: E402
from ohra.ext.fastapi import CLOCK_JS, Actors, synced  # noqa: E402

PAGES = Path(__file__).resolve().parent / "relay_rally"

LIVES = 5
GAME_S = 180.0
SERVE_LEAD_S = 1.0  # a serve drops in this long after it is sent: time to get ready
FLIGHT_S = 0.6  # the puck in the air between two phones
BASE_SPEED = 10.0  # court units per second; the court is 9 wide and 16 tall
COURT_H = 16.0
CPU_HIT = (
    0.92  # a CPU's chance to return a puck at BASE_SPEED; a faster puck, CPU_HIT ** (v / BASE)
)
SPEEDUP = 1.06  # per pass, back to BASE_SPEED after a miss
SERVE_MAX_ANGLE = math.radians(20)
MAX_ANGLE = math.radians(60)  # off vertical, at the paddle's edge
RESERVE_AFTER_MISS_S = 1.0
WATCHDOG_S = 15.0  # no out or miss from the phone with the puck: serve again
LOBBY_AFTER_S = 15.0
NAME_MAX = 12
MIN_PLAYERS = 2
MAX_PLAYERS = 8  # one tint each
FULL_HOLD_S = 2.0  # a full phone stays connected this long: clock.js shows nothing before it syncs
COLORS = ["#39ff6a", "#ffb000", "#4fd8ff", "#ff5c8a", "#c38bff", "#f4ff52", "#ff7a3d", "#7dffd8"]

type Frame = dict[str, Any]
type Phase = Literal["lobby", "playing", "over"]


# --- what the connections tell rally ------------------------------------------------------------


@dataclass(frozen=True)
class Join:
    """A phone's connection: ``player`` is its connection actor, which relays rally's frames."""

    id: str
    name: str
    player: ActorRef[Frame]


@dataclass(frozen=True)
class WatchWall:
    wall: ActorRef[Frame]


@dataclass(frozen=True)
class Start:
    pass


@dataclass(frozen=True)
class AddCpu:
    pass


@dataclass(frozen=True)
class RemoveCpu:
    pass


@dataclass(frozen=True)
class Out:
    """The puck left ``id``'s court through the top at server time ``t``, at ``x`` (0 to 1 of the
    width), ``angle`` radians off vertical, + to the right."""

    id: str
    serial: int
    t: float
    x: float
    angle: float


@dataclass(frozen=True)
class Miss:
    id: str
    serial: int


@dataclass(frozen=True)
class _Serve:
    pass


@dataclass(frozen=True)
class _Watchdog:
    serial: int


@dataclass(frozen=True)
class _TimeUp:
    pass


@dataclass(frozen=True)
class _ToLobby:
    pass


@dataclass(frozen=True)
class _Gone:
    """Player ``id``'s connection ``ref`` closed; a newer one may have taken over since."""

    id: str
    ref: ActorRef[Frame]


@dataclass(frozen=True)
class _WallGone:
    ref: ActorRef[Frame]


type RallyMsg = (
    Join
    | WatchWall
    | Start
    | AddCpu
    | RemoveCpu
    | Out
    | Miss
    | _Serve
    | _Watchdog
    | _TimeUp
    | _ToLobby
    | _Gone
    | _WallGone
)


# --- the game -----------------------------------------------------------------------------------


@dataclass
class _Player:
    id: str
    name: str
    color: str
    ref: ActorRef[Frame] | None  # None while disconnected
    lives: int = LIVES
    in_game: bool = False
    cpu: bool = False


@dataclass
class _Puck:
    holder: str  # the player it is heading to, or on the court of
    serial: int
    speed: float


@dataclass
class _Game:
    phase: Phase = "lobby"
    ends_at: float | None = None
    winners: list[str] = field(default_factory=list[str])
    puck: _Puck | None = None


def rally(
    clock: Callable[[], float] = time.monotonic, rng: random.Random | None = None
) -> Behavior[RallyMsg]:
    """``clock`` stamps every ``t``: it must be the clock the connections answer pings with."""
    rand = rng if rng is not None else random.Random()

    def setup(ctx: ActorContext[RallyMsg]) -> Behavior[RallyMsg]:
        players: dict[str, _Player] = {}
        walls: list[ActorRef[Frame]] = []
        game = _Game()
        serials = itertools.count(1)
        joins = itertools.count()  # colours and default names, never reused in a session
        cpus = itertools.count(1)
        timers = ctx.timers

        def state() -> Frame:
            return {
                "type": "state",
                "phase": game.phase,
                "players": [
                    {
                        "id": p.id,
                        "name": p.name,
                        "color": p.color,
                        "lives": p.lives,
                        "connected": p.ref is not None,
                        "in_game": p.in_game,
                        "cpu": p.cpu,
                    }
                    for p in players.values()
                ],
                "ends_at": game.ends_at,
                "winners": game.winners,
            }

        def tell_everyone(frame: Frame) -> None:
            for p in players.values():
                if p.ref is not None:
                    p.ref.tell(frame)
            for w in walls:
                w.tell(frame)

        def playing() -> list[_Player]:
            """In this game, connected, with lives."""
            return [p for p in players.values() if p.in_game and p.lives > 0 and p.ref]

        def send_puck(p: _Player, t: float, x: float, angle: float, speed: float) -> None:
            assert p.ref is not None
            serial = next(serials)
            game.puck = _Puck(p.id, serial, speed)
            vx, vy = speed * math.sin(angle), speed * math.cos(angle)
            p.ref.tell({"type": "incoming", "serial": serial, "t": t, "x": x, "vx": vx, "vy": vy})
            timers.start_single_timer("watchdog", _Watchdog(serial), WATCHDOG_S)

        def serve() -> None:
            ready = playing()
            if game.phase != "playing" or not ready:
                return
            angle = rand.uniform(-SERVE_MAX_ANGLE, SERVE_MAX_ANGLE)
            send_puck(
                rand.choice(ready),
                clock() + SERVE_LEAD_S,
                rand.uniform(0.2, 0.8),
                angle,
                BASE_SPEED,
            )

        def current(id: str, serial: int) -> _Puck | None:
            puck = game.puck
            if puck is not None and puck.holder == id and puck.serial == serial:
                return puck
            return None

        def ended(time_up: bool = False) -> bool:
            """Ends the game when its time is up or one player is left; True if it did."""
            if game.phase != "playing":
                return False
            assert game.ends_at is not None
            if not time_up and clock() < game.ends_at and len(playing()) > 1:
                return False
            # ponytail: a disconnected player cannot win, so a walkout leaves the one who stayed
            here = [p for p in players.values() if p.in_game and p.ref is not None]
            best = max((p.lives for p in here), default=0)
            game.phase, game.puck = "over", None
            game.winners = [p.id for p in here if p.lives == best]
            for key in ("serve", "watchdog", "time-up"):
                timers.cancel(key)
            timers.start_single_timer("lobby", _ToLobby(), LOBBY_AFTER_S)
            tell_everyone(state())
            return True

        def on_message(msg: RallyMsg) -> Behavior[RallyMsg]:
            match msg:
                case Join(id, name, ref):
                    p = players.get(id)
                    if p is None and len(players) >= MAX_PLAYERS:
                        ref.tell(
                            {"type": "full"}
                        )  # its phone retries, and gets in when one leaves
                        return Behaviors.same
                    if p is None:
                        n = next(joins)
                        name = name.strip()[:NAME_MAX] or f"P{n + 1}"
                        p = _Player(id, name, COLORS[n % len(COLORS)], ref)
                        players[id] = p
                    p.ref = ref  # a reconnect, or a second tab, takes over
                    ctx.watch_with(ref, _Gone(id, ref))
                    ref.tell({"type": "you", "id": p.id, "name": p.name, "color": p.color})
                    tell_everyone(state())
                case WatchWall(wall):
                    walls.append(wall)
                    ctx.watch_with(wall, _WallGone(wall))
                    wall.tell(state())
                case Start():
                    here = [p for p in players.values() if p.ref is not None]
                    if game.phase != "lobby" or len(here) < MIN_PLAYERS:
                        return Behaviors.same
                    for id in [id for id, p in players.items() if p.ref is None]:
                        del players[id]
                    for p in here:
                        p.lives, p.in_game = LIVES, True
                    game.phase, game.ends_at, game.winners = "playing", clock() + GAME_S, []
                    timers.start_single_timer("time-up", _TimeUp(), GAME_S)
                    tell_everyone(state())
                    serve()
                case AddCpu():
                    if game.phase != "lobby" or len(players) >= MAX_PLAYERS:
                        return Behaviors.same
                    k, n = next(cpus), next(joins)
                    id = f"cpu-{k}"
                    ref = ctx.spawn(cpu(id, ctx.self, clock, rand), id)
                    players[id] = _Player(id, f"CPU {k}", COLORS[n % len(COLORS)], ref, cpu=True)
                    ctx.watch_with(ref, _Gone(id, ref))
                    tell_everyone(state())
                case RemoveCpu():
                    ids = [id for id, p in players.items() if p.cpu]
                    if game.phase != "lobby" or not ids:
                        return Behaviors.same
                    gone = players.pop(ids[-1])
                    assert gone.ref is not None
                    ctx.stop(gone.ref)  # its _Gone finds no player and is ignored
                    tell_everyone(state())
                case Out(id, serial, t, x, angle):
                    now = current(id, serial)
                    if now is None:
                        return Behaviors.same
                    others = [p for p in playing() if p.id != id] or playing()
                    if not others:
                        return Behaviors.same
                    to = rand.choice(others)
                    x = min(max(x, 0.0), 1.0)
                    angle = min(max(angle, -MAX_ANGLE), MAX_ANGLE)
                    t = max(t, clock())  # a late report (a phone that stalled): land in the future
                    send_puck(to, t + FLIGHT_S, x, angle, now.speed * SPEEDUP)
                    for w in walls:
                        w.tell(
                            {"type": "pass", "from": id, "to": to.id, "t0": t, "t1": t + FLIGHT_S}
                        )
                case Miss(id, serial):
                    if current(id, serial) is None:
                        return Behaviors.same
                    game.puck = None
                    timers.cancel("watchdog")
                    players[id].lives -= 1
                    if not ended():
                        tell_everyone(state())
                        timers.start_single_timer("serve", _Serve(), RESERVE_AFTER_MISS_S)
                case _Serve():
                    serve()
                case _Watchdog(serial):
                    if game.puck is not None and game.puck.serial == serial:
                        serve()  # the phone went quiet: no life lost
                case _TimeUp():
                    ended(time_up=True)  # the timer, not the clock: they may disagree a little
                case _ToLobby():
                    for id in [id for id, p in players.items() if p.ref is None]:
                        del players[id]
                    for p in players.values():
                        p.in_game = False
                    game.phase, game.ends_at, game.winners = "lobby", None, []
                    tell_everyone(state())
                case _Gone(id, ref):
                    p = players.get(id)
                    if p is None or p.ref != ref:
                        return Behaviors.same  # an older tab of a player who is still here
                    p.ref = None
                    if game.phase == "lobby":
                        del players[id]  # nothing to come back to yet: free the spot
                    if ended():
                        return Behaviors.same
                    tell_everyone(state())
                    if game.puck is not None and game.puck.holder == id:
                        serve()  # it was heading to a phone that is gone: no life lost
                case _WallGone(ref):
                    walls.remove(ref)
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


# --- a computer player -------------------------------------------------------------------------


def cpu(
    id: str, rally: ActorRef[RallyMsg], clock: Callable[[], float], rng: random.Random
) -> Behavior[Frame]:
    """A player with no phone: rally tells it what it tells a phone, and it answers as a phone
    would, when the phone would. The puck falls COURT_H to the paddle, and a return goes back up
    as far; the side walls only fold x, so they do not change the times."""

    def setup(ctx: ActorContext[Frame]) -> Behavior[Frame]:
        def at(t: float, frame: Frame) -> None:
            ctx.timers.start_single_timer("puck", frame, max(0.0, t - clock()))

        def on_message(frame: Frame) -> Behavior[Frame]:
            match frame["type"]:
                case "incoming":
                    speed = math.hypot(frame["vx"], frame["vy"])
                    paddle = frame["t"] + COURT_H / frame["vy"]
                    if rng.random() < CPU_HIT ** (speed / BASE_SPEED):
                        angle = rng.uniform(-MAX_ANGLE / 2, MAX_ANGLE / 2)
                        t = paddle + COURT_H / (speed * math.cos(angle))
                        x = rng.uniform(0.1, 0.9)
                        at(
                            t,
                            {
                                "type": "_out",
                                "serial": frame["serial"],
                                "t": t,
                                "x": x,
                                "angle": angle,
                            },
                        )
                    else:
                        at(paddle, {"type": "_miss", "serial": frame["serial"]})
                case "_out":
                    rally.tell(Out(id, frame["serial"], frame["t"], frame["x"], frame["angle"]))
                case "_miss":
                    rally.tell(Miss(id, frame["serial"]))
                case _:
                    pass  # you, state: a CPU needs nothing from them
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


# --- one actor per connection -------------------------------------------------------------------


def _number(frame: dict[str, object], key: str) -> float:
    v = frame.get(key)
    if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v):
        raise ValueError(f"{key} must be a finite number, not {v!r}")
    return float(v)


def _serial(frame: dict[str, object]) -> int:
    v = frame.get("serial")
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f"serial must be an integer, not {v!r}")
    return v


def _object(text: str) -> dict[str, object]:
    doc: object = json.loads(text)
    if not isinstance(doc, dict):
        raise ValueError("expected an object")
    return cast(dict[str, object], doc)


def decode_player(text: str) -> Frame:
    """``{"type": "out", "serial", "t", "x", "angle"}`` or ``{"type": "miss", "serial"}``.
    Anything else raises ValueError, which closes the socket with 1003."""
    frame = _object(text)
    match frame.get("type"):
        case "out":
            return {
                "type": "out",
                "serial": _serial(frame),
                **{k: _number(frame, k) for k in ("t", "x", "angle")},
            }
        case "miss":
            return {"type": "miss", "serial": _serial(frame)}
        case other:
            raise ValueError(f"unknown frame type: {other!r}")


def decode_wall(text: str) -> Frame:
    """``{"type": "start" | "add_cpu" | "remove_cpu"}``; anything else raises ValueError."""
    kind = _object(text).get("type")
    if kind not in ("start", "add_cpu", "remove_cpu"):
        raise ValueError(f"unknown frame type: {kind!r}")
    return {"type": kind}


def player(id: str, name: str, rally: ActorRef[RallyMsg], out: ActorRef[Frame]) -> Behavior[Frame]:
    """A phone's frames go to rally; rally's go to the phone."""

    def setup(ctx: ActorContext[Frame]) -> Behavior[Frame]:
        rally.tell(Join(id, name, ctx.self))

        def on_message(frame: Frame) -> Behavior[Frame]:
            match frame["type"]:
                case "out":
                    rally.tell(Out(id, frame["serial"], frame["t"], frame["x"], frame["angle"]))
                case "miss":
                    rally.tell(Miss(id, frame["serial"]))
                case "full":
                    out.tell(frame)
                    ctx.timers.start_single_timer("full", {"type": "_bye"}, FULL_HOLD_S)
                case "_bye":  # never from the phone: decode_player rejects it
                    return Behaviors.stopped()  # closes the socket; the page retries
                case _:
                    out.tell(frame)
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


def wall(rally: ActorRef[RallyMsg], out: ActorRef[Frame]) -> Behavior[Frame]:
    def setup(ctx: ActorContext[Frame]) -> Behavior[Frame]:
        rally.tell(WatchWall(ctx.self))

        def on_message(frame: Frame) -> Behavior[Frame]:
            match frame["type"]:
                case "start":
                    rally.tell(Start())
                case "add_cpu":
                    rally.tell(AddCpu())
                case "remove_cpu":
                    rally.tell(RemoveCpu())
                case _:
                    out.tell(frame)
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


# --- the app ------------------------------------------------------------------------------------


def lan_url(port: int) -> str:
    """Where phones join: this machine's address on the LAN, even when the wall page was opened
    at localhost. Connecting a UDP socket sends nothing; it only picks the outgoing interface."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("10.255.255.255", 1))
            host = s.getsockname()[0]
    except OSError:
        host = "127.0.0.1"
    return f"http://{host}:{port}/play"


def make_app(
    clock: Callable[[], float] = time.monotonic, rng: random.Random | None = None
) -> FastAPI:
    actors = Actors(rally(clock, rng), "rally")
    app = FastAPI(lifespan=actors.lifespan, exception_handlers=actors.exception_handlers)

    @app.get("/")
    async def index() -> RedirectResponse:
        return RedirectResponse("/wall")

    @app.get("/wall")
    async def wall_page() -> FileResponse:
        return FileResponse(PAGES / "wall.html")

    @app.get("/play")
    async def phone_page() -> FileResponse:
        return FileResponse(PAGES / "phone.html")

    @app.get("/arcade.js")
    async def arcade_js() -> FileResponse:
        return FileResponse(PAGES / "arcade.js", media_type="text/javascript")

    @app.get("/ohra/clock.js")
    async def clock_js() -> FileResponse:
        return FileResponse(CLOCK_JS, media_type="text/javascript")

    @app.get("/join")
    async def join(request: Request) -> dict[str, str]:
        return {"url": lan_url(request.url.port or 80)}

    @app.websocket("/ws/player")
    async def player_socket(
        ws: WebSocket,
        id: str = Query(min_length=1, max_length=64),
        name: str = Query(default="", max_length=64),
    ) -> None:
        def props(out: ActorRef[Frame]) -> Behavior[Frame]:
            return player(id, name, actors.system, out)

        await actors.websocket(ws, *synced(props, decode_player, clock))

    @app.websocket("/ws/wall")
    async def wall_socket(ws: WebSocket) -> None:
        def props(out: ActorRef[Frame]) -> Behavior[Frame]:
            return wall(actors.system, out)

        await actors.websocket(ws, *synced(props, decode_wall, clock))

    return app


app = make_app()
