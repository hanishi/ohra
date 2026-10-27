"""Per session child actor: a short-lived child collects replies from several actors for one
request and stops when done (Pekko interaction patterns)."""

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from typing import Any

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class Keys:
    pass


@dataclass(frozen=True)
class Wallet:
    pass


@dataclass(frozen=True)
class GetKeys:
    whose: str
    reply_to: ActorRef[Keys]


@dataclass(frozen=True)
class GetWallet:
    whose: str
    reply_to: ActorRef[Wallet]


def key_cabinet() -> Behavior[GetKeys]:
    @Behaviors.receive_message
    def on_message(msg: GetKeys) -> Behavior[GetKeys]:
        msg.reply_to.tell(Keys())
        return Behaviors.same

    return on_message


def drawer() -> Behavior[GetWallet]:
    @Behaviors.receive_message
    def on_message(msg: GetWallet) -> Behavior[GetWallet]:
        msg.reply_to.tell(Wallet())
        return Behaviors.same

    return on_message


@dataclass(frozen=True)
class LeaveHome:
    who: str
    reply_to: ActorRef["ReadyToLeaveHome"]


@dataclass(frozen=True)
class ReadyToLeaveHome:
    who: str
    keys: Keys
    wallet: Wallet


def home() -> Behavior[LeaveHome]:
    @Behaviors.setup
    def setup(ctx: ActorContext[LeaveHome]) -> Behavior[LeaveHome]:
        cabinet = ctx.spawn(key_cabinet(), "key-cabinet")
        drw = ctx.spawn(drawer(), "drawer")

        @Behaviors.receive_message
        def on_message(msg: LeaveHome) -> Behavior[LeaveHome]:
            ctx.spawn(
                prepare_to_leave_home(msg.who, msg.reply_to, cabinet, drw), f"leaving-{msg.who}"
            )
            return Behaviors.same

        return on_message

    return setup


def prepare_to_leave_home(
    who: str,
    reply_to: ActorRef[ReadyToLeaveHome],
    cabinet: ActorRef[GetKeys],
    drw: ActorRef[GetWallet],
) -> Behavior[Any]:
    """The session actor accepts anything, since only the replies it asked for will arrive.
    ActorRef is contravariant, so ActorRef[Any] narrows to ActorRef[Keys] and ActorRef[Wallet]."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        cabinet.tell(GetKeys(who, ctx.self))
        drw.tell(GetWallet(who, ctx.self))

        def collecting(keys: Keys | None, wallet: Wallet | None) -> Behavior[Any]:
            if keys is not None and wallet is not None:
                reply_to.tell(ReadyToLeaveHome(who, keys, wallet))
                return Behaviors.stopped()

            @Behaviors.receive_message
            def on_message(msg: Any) -> Behavior[Any]:
                match msg:
                    case Keys():
                        return collecting(msg, wallet)
                    case Wallet():
                        return collecting(keys, msg)
                    case _:
                        return Behaviors.unhandled

            return on_message

        return collecting(None, None)

    return setup


async def main() -> None:
    async with ActorSystem(home(), "home") as system:
        print(await system.ask(lambda r: LeaveHome("Bobby", r), timeout=1))


asyncio.run(main())
