"""Adapted response: a message adapter wraps another actor's replies into our own protocol
(Pekko interaction patterns)."""

import asyncio
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors

# --- backend protocol -------------------------------------------------------------------


@dataclass(frozen=True)
class StartTranslationJob:
    task_id: int
    site: str
    reply_to: ActorRef["BackendResponse"]


@dataclass(frozen=True)
class JobStarted:
    task_id: int


@dataclass(frozen=True)
class JobProgress:
    task_id: int
    progress: float


@dataclass(frozen=True)
class JobCompleted:
    task_id: int
    result: str


type BackendResponse = JobStarted | JobProgress | JobCompleted


def backend() -> Behavior[StartTranslationJob]:
    @Behaviors.receive_message
    def on_message(msg: StartTranslationJob) -> Behavior[StartTranslationJob]:
        msg.reply_to.tell(JobStarted(msg.task_id))
        for p in (0.25, 0.5, 0.75):
            msg.reply_to.tell(JobProgress(msg.task_id, p))
        msg.reply_to.tell(JobCompleted(msg.task_id, msg.site.replace("/docs/", "/docs/sv/")))
        return Behaviors.same

    return on_message


# --- frontend: its own protocol, plus a private wrapper for backend responses -------------


@dataclass(frozen=True)
class Translate:
    site: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class _WrappedBackendResponse:
    response: BackendResponse


type Command = Translate | _WrappedBackendResponse


def frontend(backend_ref: ActorRef[StartTranslationJob]) -> Behavior[Command]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Command]) -> Behavior[Command]:
        response_mapper: ActorRef[BackendResponse] = ctx.message_adapter(_WrappedBackendResponse)

        def active(in_progress: dict[int, ActorRef[str]], count: int) -> Behavior[Command]:
            @Behaviors.receive_message
            def on_message(msg: Command) -> Behavior[Command]:
                match msg:
                    case Translate(site, reply_to):
                        task_id = count + 1
                        backend_ref.tell(StartTranslationJob(task_id, site, response_mapper))
                        return active({**in_progress, task_id: reply_to}, task_id)
                    case _WrappedBackendResponse(response):
                        match response:
                            case JobStarted(task_id):
                                ctx.log.info("Started %d", task_id)
                            case JobProgress(task_id, progress):
                                ctx.log.info("Progress %d: %.2f", task_id, progress)
                            case JobCompleted(task_id, result):
                                ctx.log.info("Completed %d: %s", task_id, result)
                                in_progress[task_id].tell(result)
                                return active(
                                    {k: v for k, v in in_progress.items() if k != task_id}, count
                                )
                return Behaviors.same

            return on_message

        return active({}, 0)

    return setup


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")

    @Behaviors.setup
    def app(ctx: ActorContext[Translate]) -> Behavior[Translate]:
        fe = ctx.spawn(frontend(ctx.spawn(backend(), "backend")), "frontend")

        @Behaviors.receive_message
        def forward(msg: Translate) -> Behavior[Translate]:
            fe.tell(msg)
            return Behaviors.same

        return forward

    async with ActorSystem(app, "adapted") as system:
        print(
            "translated:",
            await system.ask(lambda r: Translate("https://pekko.apache.org/docs/", r), timeout=1),
        )


asyncio.run(main())
