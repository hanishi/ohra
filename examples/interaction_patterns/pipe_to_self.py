"""Send future result to self: pipe_to_self turns an awaitable's outcome into a message, so
the actor's state is only ever touched from its own handler (Pekko interaction patterns)."""

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from collections.abc import Awaitable, Callable

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Failure,
    Success,
    Try,
)


@dataclass(frozen=True)
class Customer:
    id: str
    version: int
    name: str
    address: str


@dataclass(frozen=True)
class Update:
    value: Customer
    reply_to: ActorRef["UpdateResult"]


@dataclass(frozen=True)
class UpdateSuccess:
    id: str


@dataclass(frozen=True)
class UpdateFailure:
    id: str
    reason: str


type UpdateResult = UpdateSuccess | UpdateFailure


@dataclass(frozen=True)
class _WrappedUpdateResult:
    result: UpdateResult
    reply_to: ActorRef[UpdateResult]


type Command = Update | _WrappedUpdateResult

MAX_OPERATIONS_IN_PROGRESS = 10


def customer_repository(data_access: Callable[[Customer], Awaitable[None]]) -> Behavior[Command]:
    def next_behavior(in_progress: int) -> Behavior[Command]:
        @Behaviors.receive
        def on_message(ctx: ActorContext[Command], msg: Command) -> Behavior[Command]:
            match msg:
                case Update(value, reply_to):
                    if in_progress == MAX_OPERATIONS_IN_PROGRESS:
                        reply_to.tell(
                            UpdateFailure(
                                value.id,
                                f"Max {MAX_OPERATIONS_IN_PROGRESS} concurrent operations "
                                "supported",
                            )
                        )
                        return Behaviors.same

                    def adapt(r: Try[None]) -> _WrappedUpdateResult:
                        match r:
                            case Success(_):
                                return _WrappedUpdateResult(UpdateSuccess(value.id), reply_to)
                            case Failure(e):
                                return _WrappedUpdateResult(
                                    UpdateFailure(value.id, str(e)), reply_to
                                )

                    ctx.pipe_to_self(data_access(value), adapt)
                    return next_behavior(in_progress + 1)
                case _WrappedUpdateResult(result, reply_to):
                    reply_to.tell(result)
                    return next_behavior(in_progress - 1)

        return on_message

    return next_behavior(0)


async def main() -> None:
    async def save(c: Customer) -> None:
        await asyncio.sleep(0.01)  # a database round trip
        if c.version < 0:
            raise ValueError("stale version")

    async with ActorSystem(customer_repository(save), "repo") as system:
        print(
            await system.ask(
                lambda r: Update(Customer("123", 1, "Alice", "Fairy tail road 7"), r), timeout=1
            )
        )
        print(await system.ask(lambda r: Update(Customer("124", -1, "Bob", ""), r), timeout=1))


asyncio.run(main())
