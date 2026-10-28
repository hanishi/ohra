"""The explicitly triggered scheduler, after Apache Pekko's ExplicitlyTriggeredScheduler."""

from __future__ import annotations

from ohra.testkit import ExplicitlyTriggeredScheduler


def test_runs_due_jobs_in_time_order():
    s = ExplicitlyTriggeredScheduler()
    ran: list[int] = []
    for t in (3, 1, 2):
        s.schedule_once(t, lambda t=t: ran.append(t))
    s.time_passes(3)
    assert ran == [1, 2, 3]


def test_does_not_run_jobs_beyond_the_target():
    s = ExplicitlyTriggeredScheduler()
    ran: list[int] = []
    s.schedule_once(2, lambda: ran.append(2))
    s.time_passes(1)
    assert ran == []
    s.time_passes(1)
    assert ran == [2]


def test_zero_delay_runs_at_once():
    s = ExplicitlyTriggeredScheduler()
    ran: list[int] = []
    s.schedule_once(0, lambda: ran.append(0))
    assert ran == [0]


def test_current_time_is_the_jobs_time_while_it_runs():
    s = ExplicitlyTriggeredScheduler()
    seen: list[float] = []
    s.schedule_once(1.5, lambda: seen.append(s.current_time))
    s.time_passes(2)
    assert seen == [1.5]
    assert s.current_time == 2.0


def test_repeating_job_runs_every_interval():
    s = ExplicitlyTriggeredScheduler()
    ran: list[float] = []
    s.schedule_with_fixed_delay(0.5, 0.5, lambda: ran.append(s.current_time))
    s.time_passes(2)
    assert ran == [0.5, 1.0, 1.5, 2.0]


def test_float_steps_add_up():
    s = ExplicitlyTriggeredScheduler()
    ran: list[int] = []
    s.schedule_at_fixed_rate(0.1, 0.1, lambda: ran.append(1))
    for _ in range(10):
        s.time_passes(0.1)
    assert len(ran) == 10


def test_repeating_job_that_raises_is_not_rescheduled():
    s = ExplicitlyTriggeredScheduler()
    runs: list[int] = []

    def fail() -> None:
        runs.append(1)
        raise RuntimeError("boom")

    s.schedule_at_fixed_rate(1, 1, fail)
    s.time_passes(5)
    assert runs == [1]


def test_job_scheduled_by_a_job_runs_in_the_same_window():
    s = ExplicitlyTriggeredScheduler()
    ran: list[float] = []

    def first() -> None:
        ran.append(s.current_time)
        s.schedule_once(0.5, lambda: ran.append(s.current_time))

    s.schedule_once(1, first)
    s.time_passes(2)
    assert ran == [1, 1.5]


def test_cancel_removes_a_job():
    s = ExplicitlyTriggeredScheduler()
    ran: list[int] = []
    c = s.schedule_once(1, lambda: ran.append(1))
    assert c.cancel() is True
    assert c.cancel() is False
    assert c.is_cancelled
    s.time_passes(2)
    assert ran == []


def test_a_repeating_job_cancelled_while_it_runs_stops():
    s = ExplicitlyTriggeredScheduler()
    ran: list[int] = []

    def once_then_cancel() -> None:
        ran.append(1)
        c.cancel()

    c = s.schedule_with_fixed_delay(1, 1, once_then_cancel)
    s.time_passes(5)
    assert ran == [1]
