"""Port of the ChatRoom example from the Pekko Typed docs: sessions as child actors,
narrowed refs through contravariance, message adapters, and DeathWatch.

    uv run python examples/chatroom.py
"""

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, Signal, Terminated

# --- protocol ----------------------------------------------------------------------------


@dataclass(frozen=True)
class GetSession:
    screen_name: str
    reply_to: ActorRef["SessionEvent"]


@dataclass(frozen=True)
class SessionGranted:
    handle: ActorRef["PostMessage"]


@dataclass(frozen=True)
class SessionDenied:
    reason: str


@dataclass(frozen=True)
class MessagePosted:
    screen_name: str
    message: str


type SessionEvent = SessionGranted | SessionDenied | MessagePosted


@dataclass(frozen=True)
class PostMessage:
    message: str


@dataclass(frozen=True)
class PublishSessionMessage:  # room-internal, sent by sessions
    screen_name: str
    message: str


@dataclass(frozen=True)
class NotifyClient:  # session-internal, sent by the room
    event: MessagePosted


type RoomCommand = GetSession | PublishSessionMessage
type SessionCommand = PostMessage | NotifyClient


# --- chat room: one child per session ---------------------------------------------------


def chat_room(sessions: tuple[ActorRef[SessionCommand], ...] = ()) -> Behavior[RoomCommand]:
    @Behaviors.receive
    def on_message(ctx: ActorContext[RoomCommand], msg: RoomCommand) -> Behavior[RoomCommand]:
        match msg:
            case GetSession(screen_name, client):
                if any(c.path.endswith("/" + screen_name) for c in ctx.children):
                    client.tell(SessionDenied(f"{screen_name!r} is taken"))
                    return Behaviors.same
                ses = ctx.spawn(session(ctx.self, screen_name, client), screen_name)
                # ActorRef[SessionCommand] narrows to ActorRef[PostMessage]
                client.tell(SessionGranted(ses))
                return chat_room((*sessions, ses))
            case PublishSessionMessage(screen_name, message):
                for s in sessions:
                    s.tell(NotifyClient(MessagePosted(screen_name, message)))
                return Behaviors.same

    return on_message


def session(
    room: ActorRef[PublishSessionMessage], screen_name: str, client: ActorRef[SessionEvent]
) -> Behavior[SessionCommand]:
    @Behaviors.receive_message
    def on_message(msg: SessionCommand) -> Behavior[SessionCommand]:
        match msg:
            case PostMessage(message):
                room.tell(PublishSessionMessage(screen_name, message))
            case NotifyClient(event):
                client.tell(event)
        return Behaviors.same

    return on_message


# --- a client that says one thing and leaves ---------------------------------------------


def gabbler() -> Behavior[SessionEvent]:
    @Behaviors.receive
    def on_message(ctx: ActorContext[SessionEvent], msg: SessionEvent) -> Behavior[SessionEvent]:
        match msg:
            case SessionDenied(reason):
                ctx.log.warning("denied: %s", reason)
                return Behaviors.stopped()
            case SessionGranted(handle):
                handle.tell(PostMessage("Hello World!"))
                return Behaviors.same
            case MessagePosted(screen_name, message):
                print(f"[{ctx.self.path.rsplit('/', 1)[-1]}] {screen_name}: {message}")
                return Behaviors.stopped()

    return on_message


@Behaviors.setup
def main_actor(ctx: ActorContext[None]) -> Behavior[None]:
    room = ctx.spawn(chat_room(), "chatroom")
    for name in ("alice", "bob", "alice"):  # the second alice is denied
        g = ctx.spawn_anonymous(gabbler())
        ctx.watch(g)
        room.tell(GetSession(name, g))

    @Behaviors.receive_signal
    def on_signal(ctx: ActorContext[None], sig: Signal) -> Behavior[None]:
        if isinstance(sig, Terminated):
            # only the room left
            return Behaviors.same if len(ctx.children) > 1 else Behaviors.stopped()
        return Behaviors.unhandled

    return on_signal


async def main() -> None:
    system = ActorSystem(main_actor, "chat")
    await system.when_terminated()


asyncio.run(main())
