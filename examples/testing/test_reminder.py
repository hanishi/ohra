from dataclasses import dataclass

import pytest

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


@dataclass(frozen=True)
class Remind:
    after: float
    text: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Due:
    text: str
    reply_to: ActorRef[str]


type ReminderCmd = Remind | Due


def reminders() -> Behavior[ReminderCmd]:
    @Behaviors.receive
    def on_message(ctx: ActorContext[ReminderCmd], msg: ReminderCmd) -> Behavior[ReminderCmd]:
        match msg:
            case Remind(after, text, reply_to):
                ctx.timers.start_single_timer(text, Due(text, reply_to), after)
            case Due(text, reply_to):
                reply_to.tell(text)
        return Behaviors.same

    return on_message


@pytest.mark.manual_time
async def test_a_reminder_waits_for_its_time(testkit: ActorTestKit, manual_time: ManualTime):
    probe: TestProbe[str] = testkit.create_test_probe()
    testkit.spawn(reminders()).tell(Remind(3600, "stand up", probe.ref))
    await manual_time.expect_no_message_for(3599, probe)
    await manual_time.time_passes(1)
    await probe.expect_message("stand up")
