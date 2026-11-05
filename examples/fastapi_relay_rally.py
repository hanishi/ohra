"""Relay Rally: phones are the courts and the paddles, a big screen is the scoreboard, and the
puck is passed from phone to phone at random, every phone on the server's clock.

``rally`` is the game, the one place its state lives.
"""

import math
import random
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ohra import ActorContext, ActorRef, Behavior, Behaviors  # noqa: E402

LIVES = 5
GAME_S = 180.0
SERVE_LEAD_S = 1.0  # a serve drops in this long after it is sent: time to get ready
FLIGHT_S = 0.6  # the puck in the air between two phones
BASE_SPEED = 10.0  # court units per second; the court is 9 wide and 16 tall
SPEEDUP = 1.06  # per pass, back to BASE_SPEED after a miss
SERVE_MAX_ANGLE = math.radians(20)
MAX_ANGLE = math.radians(60)  # off vertical, at the paddle's edge
RESERVE_AFTER_MISS_S = 1.0
NAME_MAX = 12
MIN_PLAYERS = 2
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


type RallyMsg = Join | WatchWall | Start | Out | Miss | _Serve


# --- the game -----------------------------------------------------------------------------------


@dataclass
class _Player:
    id: str
    name: str
    color: str
    ref: ActorRef[Frame] | None
    lives: int = LIVES
    in_game: bool = False


@dataclass
class _Puck:
    holder: str  # the player it is heading to, or on the court of
    serial: int
    speed: float


def rally(
    clock: Callable[[], float] = time.monotonic, rng: random.Random | None = None
) -> Behavior[RallyMsg]:
    """``clock`` stamps every ``t``: it must be the clock the connections answer pings with."""
    rand = rng if rng is not None else random.Random()

    def setup(ctx: ActorContext[RallyMsg]) -> Behavior[RallyMsg]:
        players: dict[str, _Player] = {}
        walls: list[ActorRef[Frame]] = []
        game: dict[str, Any] = {"phase": "lobby", "ends_at": None, "winners": []}
        puck: list[_Puck] = []  # the puck in play, if any
        serials = iter(range(1, 1 << 62))

        def phase() -> Phase:
            return game["phase"]

        def state() -> Frame:
            return {
                "type": "state",
                "phase": game["phase"],
                "players": [
                    {
                        "id": p.id,
                        "name": p.name,
                        "color": p.color,
                        "lives": p.lives,
                        "connected": p.ref is not None,
                        "in_game": p.in_game,
                    }
                    for p in players.values()
                ],
                "ends_at": game["ends_at"],
                "winners": game["winners"],
            }

        def tell_everyone(frame: Frame) -> None:
            for p in players.values():
                if p.ref is not None:
                    p.ref.tell(frame)
            for w in walls:
                w.tell(frame)

        def playing() -> list[_Player]:
            return [p for p in players.values() if p.in_game and p.lives > 0 and p.ref]

        def send_puck(p: _Player, t: float, x: float, angle: float, speed: float) -> None:
            assert p.ref is not None
            serial = next(serials)
            puck[:] = [_Puck(p.id, serial, speed)]
            vx, vy = speed * math.sin(angle), speed * math.cos(angle)
            p.ref.tell({"type": "incoming", "serial": serial, "t": t, "x": x, "vx": vx, "vy": vy})

        def serve() -> None:
            ready = playing()
            if phase() != "playing" or not ready:
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
            if puck and puck[0].holder == id and puck[0].serial == serial:
                return puck[0]
            return None

        def on_message(msg: RallyMsg) -> Behavior[RallyMsg]:
            match msg:
                case Join(id, name, ref):
                    p = players.get(id)
                    if p is None:
                        name = name.strip()[:NAME_MAX] or f"P{len(players) + 1}"
                        p = _Player(id, name, COLORS[len(players) % len(COLORS)], ref)
                        players[id] = p
                    p.ref = ref
                    ref.tell({"type": "you", "id": p.id, "name": p.name, "color": p.color})
                    tell_everyone(state())
                case WatchWall(wall):
                    walls.append(wall)
                    wall.tell(state())
                case Start():
                    here = [p for p in players.values() if p.ref is not None]
                    if phase() != "lobby" or len(here) < MIN_PLAYERS:
                        return Behaviors.same
                    for id in [id for id, p in players.items() if p.ref is None]:
                        del players[id]
                    for p in here:
                        p.lives, p.in_game = LIVES, True
                    game.update(phase="playing", ends_at=clock() + GAME_S, winners=[])
                    tell_everyone(state())
                    serve()
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
                    send_puck(to, t + FLIGHT_S, x, angle, now.speed * SPEEDUP)
                    for w in walls:
                        w.tell(
                            {"type": "pass", "from": id, "to": to.id, "t0": t, "t1": t + FLIGHT_S}
                        )
                case Miss(id, serial):
                    if current(id, serial) is None:
                        return Behaviors.same
                    puck.clear()
                    players[id].lives -= 1
                    tell_everyone(state())
                    ctx.timers.start_single_timer("serve", _Serve(), RESERVE_AFTER_MISS_S)
                case _Serve():
                    serve()
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)
