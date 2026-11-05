"""Relay Rally: phones are the courts and the paddles, a big screen is the scoreboard, and the
puck is passed from phone to phone at random, every phone on the server's clock.

``rally`` is the game, the one place its state lives.
"""

import math
import random
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
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
WATCHDOG_S = 15.0  # no out or miss from the phone with the puck: serve again
LOBBY_AFTER_S = 15.0
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
        serials = iter(range(1, 1 << 62))
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

        def ended() -> bool:
            """Ends the game when its time is up or one player is left; True if it did."""
            if game.phase != "playing":
                return False
            assert game.ends_at is not None
            if clock() < game.ends_at and len(playing()) > 1:
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
                    if p is None:
                        name = name.strip()[:NAME_MAX] or f"P{len(players) + 1}"
                        p = _Player(id, name, COLORS[len(players) % len(COLORS)], ref)
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
                    ended()
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
