"""The pytest plugin, run in a separate pytest through pytester."""

from __future__ import annotations

import time

import pytest

pytest_plugins = ["pytester"]

_INI = "[pytest]\nasyncio_mode = auto\n"

_HEADER = """
from dataclasses import dataclass

import pytest

from ohra import ActorContext, ActorRef, Behavior, Behaviors
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


@dataclass(frozen=True)
class Echo:
    text: str
    reply_to: ActorRef[str]


def echo() -> Behavior[Echo]:
    @Behaviors.receive_message
    def on_message(msg: Echo) -> Behavior[Echo]:
        msg.reply_to.tell(msg.text)
        return Behaviors.same

    return on_message


def reminder(delay: float, probe: ActorRef[str]) -> Behavior[str]:
    def setup(ctx: ActorContext[str]) -> Behavior[str]:
        ctx.timers.start_single_timer("k", "remind", delay)

        @Behaviors.receive_message
        def on_message(msg: str) -> Behavior[str]:
            probe.tell(msg)
            return Behaviors.same

        return on_message

    return Behaviors.setup(setup)
"""


def _run(pytester: pytest.Pytester, body: str, *args: str) -> pytest.RunResult:
    pytester.makeini(_INI)
    pytester.makepyfile(_HEADER + body)
    return pytester.runpytest_subprocess("-p", "ohra.testkit.pytest_plugin", *args)


def test_testkit_fixture(pytester: pytest.Pytester):
    result = _run(
        pytester,
        """
async def test_echo(testkit: ActorTestKit):
    probe: TestProbe[str] = testkit.create_test_probe()
    testkit.spawn(echo()).tell(Echo("hi", probe.ref))
    await probe.expect_message("hi")
    assert testkit.system.name == "test_echo"
""",
    )
    result.assert_outcomes(passed=1)


def test_manual_time_marker(pytester: pytest.Pytester):
    t0 = time.monotonic()
    result = _run(
        pytester,
        """
@pytest.mark.manual_time
async def test_reminder(testkit: ActorTestKit, manual_time: ManualTime):
    probe: TestProbe[str] = testkit.create_test_probe()
    testkit.spawn(reminder(10, probe.ref))
    await manual_time.expect_no_message_for(9, probe)
    await manual_time.time_passes(1)
    await probe.expect_message("remind")
""",
    )
    result.assert_outcomes(passed=1)
    assert time.monotonic() - t0 < 10


def test_manual_time_without_marker_fails(pytester: pytest.Pytester):
    result = _run(
        pytester,
        """
async def test_unmarked(manual_time: ManualTime):
    pass
""",
    )
    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*the manual_time fixture needs @pytest.mark.manual_time*"])


def test_marker_is_registered(pytester: pytest.Pytester):
    result = _run(
        pytester,
        """
@pytest.mark.manual_time
async def test_marked(testkit: ActorTestKit):
    pass
""",
        "--strict-markers",
    )
    result.assert_outcomes(passed=1)
