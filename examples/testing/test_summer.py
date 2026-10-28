import asyncio
from dataclasses import dataclass

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.testkit import ActorTestKit, FishingOutcome, FishingOutcomes, TestProbe


@dataclass(frozen=True)
class Progress:
    percent: int


@dataclass(frozen=True)
class Done:
    total: int


type Report = Progress | Done


def summer(numbers: list[int], report_to: ActorRef[Report]) -> Behavior[str]:
    """Adds the numbers up, reporting progress, then stops."""

    async def setup(_ctx: ActorContext[str]) -> Behavior[str]:
        total = 0
        for i, n in enumerate(numbers, 1):
            await asyncio.sleep(0.01)  # some real work
            total += n
            report_to.tell(Progress(100 * i // len(numbers)))
        report_to.tell(Done(total))
        return Behaviors.stopped()

    return Behaviors.setup(setup)


def until_done(report: Report) -> FishingOutcome:
    match report:
        case Progress():
            return FishingOutcomes.continue_and_ignore()
        case Done():
            return FishingOutcomes.complete()


async def test_summer_reports_the_total_and_stops():
    async with ActorTestKit() as testkit:
        probe: TestProbe[Report] = testkit.create_test_probe()
        worker = testkit.spawn(summer([1, 2, 3, 4], probe.ref))
        assert await probe.fish_for_message(1, until_done) == [Done(10)]
        await probe.expect_terminated(worker)
