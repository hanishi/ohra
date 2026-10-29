"""Persisted state: the latest state is written before a reply and recovered in one read."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from ohra import ActorRef
from ohra.persistence import InMemoryStore, RevisionConflictError, Store

# --- Task 1: stores ----------------------------------------------------------------------


async def test_in_memory_store_round_trip():
    store = InMemoryStore[int]()
    assert await store.load("a") is None
    await store.save("a", 5, 1)
    assert await store.load("a") == (5, 1)
    await store.save("a", 6, 2)
    assert await store.load("a") == (6, 2)
    await store.delete("a")
    assert await store.load("a") is None


async def test_in_memory_store_rejects_stale_revisions():
    store = InMemoryStore[int]()
    with pytest.raises(RevisionConflictError):
        await store.save("a", 5, 2)  # nothing stored: the first revision is 1
    await store.save("a", 5, 1)
    with pytest.raises(RevisionConflictError):
        await store.save("a", 6, 1)  # stale
    with pytest.raises(RevisionConflictError):
        await store.save("a", 6, 3)  # skipped one
    assert await store.load("a") == (5, 1)


# --- Task 2: Behaviors.persisted ---------------------------------------------------------------

from ohra import (  # noqa: E402
    ActorContext,
    ActorSystem,
    Behavior,
    Behaviors,
    ChildFailed,
    Entity,
    EntityTypeKey,
    Sharding,
    Signal,
    SupervisorStrategy,
    Terminated,
    ask,
)
from ohra.patterns import AskTimeoutError  # noqa: E402
from ohra.persistence import PersistenceError, Persisted  # noqa: E402

from .util import eventually  # noqa: E402


@dataclass(frozen=True)
class Add:
    n: int
    reply_to: ActorRef[int] | None = None


@dataclass(frozen=True)
class Get:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Boom:
    pass


type Cmd = Add | Get | Boom


def counter_on(store: Store[int], log: list[str]) -> Persisted[int, Cmd]:
    @Behaviors.persisted(store)
    def counter(total: int) -> Behavior[Cmd]:
        log.append(f"become {total}")

        @Behaviors.receive_message
        def on_message(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Add(n, None):
                    return counter(total + n)
                case Add(n, reply_to):
                    return counter(total + n).then_reply(reply_to, total + n)
                case Get(reply_to):
                    reply_to.tell(total)
                    return Behaviors.same
                case Boom():
                    raise ValueError("boom")

        return on_message

    return counter


counter = counter_on


class GatedStore(InMemoryStore[int]):
    """save waits for the gate and records what happened, so tests can see ordering."""

    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()
        self.events: list[str] = []

    async def save(self, persistence_id: str, state: int, revision: int) -> None:
        self.events.append("saving")
        await self.gate.wait()
        await super().save(persistence_id, state, revision)
        self.events.append("saved")


class FailingStore(InMemoryStore[int]):
    async def save(self, persistence_id: str, state: int, revision: int) -> None:
        raise OSError("disk on fire")


async def test_recover_starts_from_empty_when_nothing_is_stored():
    store, log = InMemoryStore[int](), []
    async with ActorSystem(counter(store, log).recover("c", empty=0), "s") as system:
        assert await system.ask(Get, timeout=1) == 0
    assert store.states == {}
    assert log == ["become 0"]


async def test_persist_writes_before_becoming_the_new_behavior():
    store, log = InMemoryStore[int](), []
    async with ActorSystem(counter(store, log).recover("c", empty=0), "s") as system:
        system.tell(Add(2))
        assert await system.ask(Get, timeout=1) == 2
    assert store.states["c"] == (2, 1)


async def test_recover_loads_the_stored_state():
    store, log = InMemoryStore[int](), []
    async with ActorSystem(counter(store, log).recover("c", empty=0), "s") as system:
        system.tell(Add(2))
        assert await system.ask(Get, timeout=1) == 2
    async with ActorSystem(counter(store, log).recover("c", empty=0), "s") as system:
        assert await system.ask(Get, timeout=1) == 2
        system.tell(Add(3))
        assert await system.ask(Get, timeout=1) == 5
    assert store.states["c"] == (5, 2)  # the revision continues across incarnations


async def test_then_reply_runs_after_the_write_completes():
    store, log = GatedStore(), []
    async with ActorSystem(counter(store, log).recover("c", empty=0), "s") as system:
        pending = asyncio.ensure_future(system.ask(lambda r: Add(1, r), timeout=1))
        await eventually(lambda: store.events == ["saving"])
        await asyncio.sleep(0.01)
        assert not pending.done()  # the reply waits for the store
        store.gate.set()
        assert await pending == 1
    assert store.events == ["saving", "saved"]


async def test_then_run_runs_after_the_write_in_order():
    store, log = GatedStore(), []
    seen: list[str] = []

    @Behaviors.persisted(store)
    def runner(total: int) -> Behavior[Cmd]:
        def on_message(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Add(n, _):
                    return (
                        runner(total + n)
                        .then_run(lambda: seen.append("first"))
                        .then_run(lambda: seen.append("second"))
                    )
                case Get(reply_to):
                    reply_to.tell(total)
                case Boom():
                    pass
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    store.gate.set()
    async with ActorSystem(runner.recover("r", empty=0), "s") as system:
        system.tell(Add(1))
        assert await system.ask(Get, timeout=1) == 1
    assert seen == ["first", "second"]
    assert store.events == ["saving", "saved"]


async def test_same_and_reads_do_not_write():
    store, log = GatedStore(), []
    async with ActorSystem(counter(store, log).recover("c", empty=0), "s") as system:
        for _ in range(3):
            assert await system.ask(Get, timeout=1) == 0
    assert store.events == [] and store.states == {}


def watcher(
    child_behavior: Behavior[Cmd], log: list[str], holder: list[ActorRef[Cmd]]
) -> Behavior[str]:
    """Spawns the child, watches it, and records how it ended."""

    def setup(ctx: ActorContext[str]) -> Behavior[str]:
        ref = ctx.spawn(child_behavior, "child")
        ctx.watch(ref)
        holder.append(ref)

        def on_signal(_ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
            if isinstance(sig, ChildFailed):
                log.append(f"failed {type(sig.cause).__name__}")
            elif isinstance(sig, Terminated):
                log.append("terminated")
            return Behaviors.same

        return Behaviors.receive_signal(on_signal)

    return Behaviors.setup(setup)


async def test_failed_write_fails_the_actor_before_any_reply():
    store, log = FailingStore(), []
    holder: list[ActorRef[Cmd]] = []
    async with ActorSystem(watcher(counter(store, log).recover("c", empty=0), log, holder), "s"):
        await eventually(lambda: bool(holder))
        with pytest.raises(AskTimeoutError):
            await ask(holder[0], lambda r: Add(1, r), timeout=0.05)
        await eventually(lambda: "failed OSError" in log)
    assert store.states == {}
    assert log.count("become 1") == 0  # the new behavior was never installed


async def test_restart_recovers_the_committed_state():
    store, log = InMemoryStore[int](), []
    supervised = Behaviors.supervise(counter(store, log).recover("c", empty=0)).on_failure(
        SupervisorStrategy.restart()
    )
    async with ActorSystem(supervised, "s") as system:
        system.tell(Add(1))
        assert await system.ask(Get, timeout=1) == 1
        system.tell(Boom())
        assert await system.ask(Get, timeout=1) == 1  # reloaded from the store, not from memory
    assert log == ["become 0", "become 1", "become 1"]


async def test_revision_conflict_fails_the_second_writer():
    store, log = InMemoryStore[int](), []
    holder_a: list[ActorRef[Cmd]] = []
    holder_b: list[ActorRef[Cmd]] = []
    async with (
        ActorSystem(watcher(counter(store, log).recover("c", empty=0), log, holder_a), "a"),
        ActorSystem(watcher(counter(store, log).recover("c", empty=0), log, holder_b), "b"),
    ):
        await eventually(lambda: bool(holder_a) and bool(holder_b))
        assert await ask(holder_a[0], lambda r: Add(1, r), timeout=1) == 1
        with pytest.raises(AskTimeoutError):
            # b still thinks revision 0: conflict
            await ask(holder_b[0], lambda r: Add(10, r), timeout=0.05)
        await eventually(lambda: "failed RevisionConflictError" in log)
    assert store.states["c"] == (1, 1)


async def test_persist_outside_recover_fails_the_actor():
    store, log = InMemoryStore[int](), []
    holder: list[ActorRef[Cmd]] = []
    async with ActorSystem(watcher(counter(store, log)(0), log, holder), "s"):
        await eventually(lambda: "failed PersistenceError" in log)
    assert store.states == {}


async def test_persisted_entity_survives_passivation():
    store, log = InMemoryStore[int](), []
    counter_key = EntityTypeKey[Cmd]("Counter")
    persisted = counter(store, log)
    region = Sharding.init(
        Entity(
            counter_key, lambda e: persisted.recover(e.entity_id, empty=0)
        ).with_passivate_after(0.02)
    )
    async with ActorSystem(region, "s") as system:
        c = Sharding.entity_ref_for(system, "c")
        c.tell(Add(3))
        assert await ask(c, Get, timeout=1) == 3
        await eventually(lambda: log.count("become 3") == 1 and store.states["c"] == (3, 1))
        await asyncio.sleep(0.05)  # idle: passivated
        assert await ask(c, Get, timeout=1) == 3  # recovered from the store
    assert log == ["become 0", "become 3", "become 3"]


# --- recovering and persisting states, as Apache Pekko's DurableStateBehavior ----------------

from ohra import Dropped, PostStop, RecoveryTimedOutError, StashBuffer  # noqa: E402
from ohra.testkit import ActorTestKit, ManualTime, TestProbe  # noqa: E402


class GatedLoadStore(GatedStore):
    """A GatedStore whose load also waits, on its own gate."""

    def __init__(self) -> None:
        super().__init__()
        self.load_gate = asyncio.Event()

    async def load(self, persistence_id: str) -> tuple[int, int] | None:
        await self.load_gate.wait()
        return await super().load(persistence_id)


def failures(child_behavior: Behavior[Cmd], seen: list[BaseException]) -> Behavior[Cmd]:
    """Spawns the child, forwards to it, and records the cause it failed with."""

    def setup(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        child = ctx.spawn(child_behavior, "child")
        ctx.watch(child)

        def on_message(msg: Cmd) -> Behavior[Cmd]:
            child.tell(msg)
            return Behaviors.same

        def on_signal(_ctx: ActorContext[Cmd], sig: Signal) -> Behavior[Cmd]:
            if isinstance(sig, ChildFailed):
                seen.append(sig.cause)
            return Behaviors.same

        return Behaviors.receive_message(on_message).receive_signal(on_signal)

    return Behaviors.setup(setup)


async def test_commands_during_recovery_run_after_it_in_order():
    store, log = GatedLoadStore(), []
    store.states["c"] = (5, 1)
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(counter(store, log).recover("c", empty=0))
        ref.tell(Add(1, probe.ref))
        ref.tell(Add(2, probe.ref))
        store.gate.set()
        store.load_gate.set()
        assert await probe.receive_messages(2) == [6, 8]


async def test_a_stalled_load_fails_the_actor_with_recovery_timed_out():
    store, log = GatedLoadStore(), []
    seen: list[BaseException] = []
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        testkit.spawn(
            failures(counter(store, log).recover("c", empty=0, recovery_timeout=5), seen)
        )
        await manual.time_passes(5)
        await eventually(lambda: bool(seen))
    assert isinstance(seen[0], RecoveryTimedOutError)
    assert str(seen[0]) == "Recovery timed out, didn't get state within 5s"


async def test_a_full_recovery_stash_drops_with_a_reason():
    store, log = GatedLoadStore(), []

    @Behaviors.persisted(store, stash_capacity=2)
    def small(total: int) -> Behavior[Cmd]:
        return Behaviors.receive_message(lambda _msg: Behaviors.same)

    async with ActorTestKit() as testkit:
        dropped = testkit.create_dropped_message_probe()
        ref = testkit.spawn(small.recover("c", empty=0))
        third = Add(3)
        for msg in (Add(1), Add(2), third):
            ref.tell(msg)
        assert await dropped.receive_message() == Dropped(
            third, "Stash is full in [DurableState]", ref
        )
        store.load_gate.set()


async def test_a_stop_during_recovery_stops_at_once():
    store, log = GatedLoadStore(), []
    async with ActorTestKit() as testkit:
        ref = testkit.spawn(counter(store, log).recover("c", empty=0))
        await asyncio.sleep(0)
        await testkit.stop(ref, max=1)  # raises if the actor is still waiting on its load
    assert log == []


# --- Task 3: SqliteStore -------------------------------------------------------------------------

import contextlib  # noqa: E402
import json  # noqa: E402
import sqlite3  # noqa: E402
from pathlib import Path  # noqa: E402

from ohra.persistence import SqliteStore  # noqa: E402


async def test_sqlite_store_round_trip_and_conflicts(tmp_path: Path):
    store = SqliteStore[int](tmp_path / "s.db")
    assert await store.load("a") is None
    with pytest.raises(RevisionConflictError):
        await store.save("a", 5, 2)
    await store.save("a", 5, 1)
    assert await store.load("a") == (5, 1)
    with pytest.raises(RevisionConflictError):
        await store.save("a", 6, 1)
    await store.save("a", 6, 2)
    assert await store.load("a") == (6, 2)
    await store.delete("a")
    assert await store.load("a") is None
    await store.save("a", 7, 1)  # after a delete the id starts over
    assert await store.load("a") == (7, 1)


def test_sqlite_store_bad_directory_raises_at_construction(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        SqliteStore[int](tmp_path / "missing" / "s.db")


async def test_sqlite_store_custom_codec(tmp_path: Path):
    path = tmp_path / "s.db"
    store = SqliteStore[dict[str, int]](
        path, codec=(lambda s: json.dumps(s).encode(), lambda b: json.loads(b))
    )
    await store.save("a", {"x": 1}, 1)
    assert await store.load("a") == ({"x": 1}, 1)
    with contextlib.closing(sqlite3.connect(path)) as conn:
        (raw,) = conn.execute("SELECT data FROM state WHERE persistence_id = 'a'").fetchone()
    assert raw == b'{"x": 1}'


async def test_state_survives_a_process_restart(tmp_path: Path):
    path = tmp_path / "s.db"
    log: list[str] = []

    def system_on(store: SqliteStore[int]) -> ActorSystem[Cmd]:
        persisted = counter_on(store, log)
        return ActorSystem(persisted.recover("c", empty=0), "s")

    async with system_on(SqliteStore[int](path)) as system:
        system.tell(Add(4))
        assert await system.ask(Get, timeout=1) == 4
    # a new store object, as after a process restart
    async with system_on(SqliteStore[int](path)) as system:
        assert await system.ask(Get, timeout=1) == 4


# --- review fixes --------------------------------------------------------------------------------

import logging  # noqa: E402

from ohra import ChildFailed as _ChildFailed  # noqa: E402,F401


async def test_persist_from_another_factory_fails_the_actor():
    store_a, store_b, log = InMemoryStore[int](), InMemoryStore[str](), []
    holder: list[ActorRef[Cmd]] = []

    @Behaviors.persisted(store_b)
    def other(_word: str) -> Behavior[Cmd]:
        return Behaviors.ignore

    @Behaviors.persisted(store_a)
    def confused(total: int) -> Behavior[Cmd]:
        def on_message(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Add():
                    # a Persist from a different factory: must not write into store_a
                    return other("hello")
                case Get(reply_to):
                    reply_to.tell(total)
                case Boom():
                    pass
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    async with ActorSystem(watcher(confused.recover("c", empty=0), log, holder), "s"):
        await eventually(lambda: bool(holder))
        holder[0].tell(Add(1))
        await eventually(lambda: "failed PersistenceError" in log)
    assert store_a.states == {} and store_b.states == {}


async def test_child_of_a_persisted_actor_cannot_use_its_incarnation():
    store, log = InMemoryStore[int](), []

    @Behaviors.persisted(store)
    def parent(total: int) -> Behavior[Cmd]:
        def on_message(ctx: ActorContext[Cmd], msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Add():
                    # a Persist spawned directly, in a child task
                    ctx.watch(ctx.spawn(parent(99), "child"))
                case Get(reply_to):
                    reply_to.tell(total)
                case Boom():
                    pass
            return Behaviors.same

        def on_signal(_ctx: ActorContext[Cmd], sig: Signal) -> Behavior[Cmd]:
            if isinstance(sig, ChildFailed):
                log.append(f"child failed {type(sig.cause).__name__}")
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    async with ActorSystem(parent.recover("p", empty=0), "s") as system:
        system.tell(Add(1))
        await eventually(lambda: "child failed PersistenceError" in log)
        assert await system.ask(Get, timeout=1) == 0
    assert store.states == {}  # the child did not write 99 under the parent's id


async def test_effect_failure_after_the_write_does_not_roll_back_memory(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.ERROR, logger="ohra.actor")
    store, log = InMemoryStore[int](), []

    def explode() -> None:
        raise RuntimeError("effect failed")

    @Behaviors.persisted(store)
    def fragile(total: int) -> Behavior[Cmd]:
        def on_message(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Add(n, _):
                    return fragile(total + n).then_run(explode)
                case Get(reply_to):
                    reply_to.tell(total)
                case Boom():
                    pass
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    async with ActorSystem(fragile.recover("f", empty=0), "s") as system:
        system.tell(Add(1))
        assert await system.ask(Get, timeout=1) == 1  # memory matches the committed store
    assert store.states["f"] == (1, 1)
    assert any(
        "effect failed" in r.getMessage() or "effect" in r.getMessage() for r in caplog.records
    )


async def test_factory_failure_after_the_write_sends_no_reply():
    store, log = InMemoryStore[int](), []
    holder: list[ActorRef[Cmd]] = []

    @Behaviors.persisted(store)
    def picky(total: int) -> Behavior[Cmd]:
        if total == 2:
            raise ValueError("cannot be two")

        def on_message(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Add(n, reply_to) if reply_to is not None:
                    return picky(total + n).then_reply(reply_to, total + n)
                case Get(reply_to):
                    reply_to.tell(total)
                case _:
                    pass
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    async with ActorSystem(watcher(picky.recover("p", empty=0), log, holder), "s"):
        await eventually(lambda: bool(holder))
        with pytest.raises(AskTimeoutError):
            # written, but the behavior could not be built
            await ask(holder[0], lambda r: Add(2, r), timeout=0.05)
        await eventually(lambda: "failed ValueError" in log)
    assert store.states["p"] == (2, 1)


def test_sqlite_store_rejects_in_memory_databases():
    with pytest.raises(ValueError):
        # each call opens a fresh connection, so the table would vanish between calls
        SqliteStore[int](":memory:")


def _open_store(path: str, barrier: object, out: object) -> None:
    from ohra import SqliteStore

    barrier.wait()  # type: ignore[attr-defined]
    try:
        SqliteStore[int](path)
        out.put("ok")  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        out.put(repr(e))  # type: ignore[attr-defined]


def test_sqlite_store_opens_a_fresh_database_from_racing_processes(tmp_path):
    # Pods starting together on one shared file: switching a fresh database to WAL fails at once
    # with "database is locked" for the losers (SQLite's deadlock avoidance skips the busy
    # timeout), so it is retried.
    import multiprocessing as mp

    ctx = mp.get_context("spawn")
    results: list[str] = []
    for i in range(10):
        barrier, out = ctx.Barrier(4), ctx.Queue()
        procs = [
            ctx.Process(target=_open_store, args=(str(tmp_path / f"r{i}.db"), barrier, out))
            for _ in range(4)
        ]
        for p in procs:
            p.start()
        results += [out.get(timeout=30) for _ in procs]
        for p in procs:
            p.join()
    assert [r for r in results if r != "ok"] == []


async def test_commands_during_a_write_run_after_it_and_see_the_new_state():
    store, log = GatedStore(), []
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(counter(store, log).recover("c", empty=0))
        ref.tell(Add(1, probe.ref))
        ref.tell(Get(probe.ref))
        await eventually(lambda: store.events == ["saving"])
        store.gate.set()
        assert await probe.receive_messages(2) == [1, 1]


async def test_a_stop_during_a_stalled_write_stops_at_once_without_a_reply():
    store, log = GatedStore(), []
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(counter(store, log).recover("c", empty=0))
        ref.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        await testkit.stop(ref, max=1)  # raises if the actor is still waiting on its write
        await probe.expect_no_message(0.05)
    assert store.states == {}


async def test_a_message_replayed_after_a_write_can_write_again_in_order():
    store, log = GatedStore(), []
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(counter(store, log).recover("c", empty=0))
        for n in (1, 2, 3):
            ref.tell(Add(n, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        store.gate.set()
        assert await probe.receive_messages(3) == [1, 3, 6]
    assert store.states["c"] == (6, 3)


async def test_a_late_load_from_before_a_restart_is_ignored():
    store, log = GatedLoadStore(), []
    store.states["c"] = (5, 1)
    supervised = Behaviors.supervise(
        counter(store, log).recover("c", empty=0, recovery_timeout=1)
    ).on_failure(SupervisorStrategy.restart_with_backoff(0.5, 0.5, 0))
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(supervised)
        await manual.time_passes(1)  # the first recovery times out
        await manual.time_passes(0.5)  # the backoff restart loads again
        store.gate.set()
        store.load_gate.set()  # both loads answer; the first incarnation's must be ignored
        ref.tell(Add(1, probe.ref))
        assert await probe.receive_message() == 6
    assert log == ["become 5", "become 6"]
    assert store.states["c"] == (6, 2)


@dataclass(frozen=True)
class Hold:
    pass


@dataclass(frozen=True)
class Release:
    reply_to: ActorRef[int]


type HoldCmd = Add | Hold | Release


async def test_messages_a_user_stash_replays_into_a_write_run_after_it():
    store = GatedStore()
    store.gate.set()

    def with_stash(stash: StashBuffer[HoldCmd]) -> Behavior[HoldCmd]:
        def idle(total: int) -> Behavior[HoldCmd]:
            def on_message(msg: HoldCmd) -> Behavior[HoldCmd]:
                match msg:
                    case Add(n, reply_to) if reply_to is not None:
                        return saved(total + n).then_reply(reply_to, total + n)
                    case Hold():
                        return busy(total)
                    case _:
                        return Behaviors.same

            return Behaviors.receive_message(on_message)

        def busy(total: int) -> Behavior[HoldCmd]:
            def on_message(msg: HoldCmd) -> Behavior[HoldCmd]:
                match msg:
                    case Release(reply_to):
                        return stash.unstash_all(saved(total + 100).then_reply(reply_to, 100))
                    case _:
                        stash.stash(msg)
                        return Behaviors.same

            return Behaviors.receive_message(on_message)

        saved = Behaviors.persisted(store)(idle)
        return saved.recover("h", empty=0)

    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(Behaviors.with_stash(10, with_stash))
        for msg in (Hold(), Add(1, probe.ref), Add(2, probe.ref), Release(probe.ref)):
            ref.tell(msg)
        assert await probe.receive_messages(3) == [100, 101, 103]
    assert store.states["h"] == (103, 3)


@dataclass(frozen=True)
class WatchIt:
    ref: ActorRef[object]


type WatchCmd = Add | WatchIt


async def test_terminated_during_a_write_is_delivered_after_it():
    store = GatedStore()
    probe_box: list[TestProbe[int]] = []

    @Behaviors.persisted(store)
    def watching(total: int) -> Behavior[WatchCmd]:
        def on_message(ctx: ActorContext[WatchCmd], msg: WatchCmd) -> Behavior[WatchCmd]:
            match msg:
                case WatchIt(ref):
                    ctx.watch(ref)
                    return Behaviors.same
                case Add(n, reply_to) if reply_to is not None:
                    return watching(total + n).then_reply(reply_to, total + n)
                case Add():
                    return Behaviors.same

        def on_signal(_ctx: ActorContext[WatchCmd], sig: Signal) -> Behavior[WatchCmd]:
            if isinstance(sig, Terminated):
                probe_box[0].ref.tell(-1)
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        probe_box.append(probe)
        other = testkit.spawn(Behaviors.receive_message(lambda _m: Behaviors.same))
        ref = testkit.spawn(watching.recover("w", empty=0))
        ref.tell(WatchIt(other))
        ref.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        await testkit.stop(other)
        await asyncio.sleep(0.01)
        store.gate.set()
        assert await probe.receive_messages(2) == [1, -1]


async def test_post_stop_during_a_write_reaches_the_pre_write_behavior():
    store = GatedStore()
    probe_box: list[TestProbe[int]] = []

    @Behaviors.persisted(store)
    def stopping(total: int) -> Behavior[Cmd]:
        def on_message(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Add(n, reply_to) if reply_to is not None:
                    return stopping(total + n).then_reply(reply_to, total + n)
                case _:
                    return Behaviors.same

        def on_signal(_ctx: ActorContext[Cmd], sig: Signal) -> Behavior[Cmd]:
            if sig is PostStop:
                probe_box[0].ref.tell(-total - 100)
            return Behaviors.same

        return Behaviors.receive_message(on_message).receive_signal(on_signal)

    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        probe_box.append(probe)
        ref = testkit.spawn(stopping.recover("p", empty=0))
        ref.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        await testkit.stop(ref, max=1)
        assert await probe.receive_message() == -100  # the pre-write behavior, total 0


@dataclass(frozen=True)
class StopChild:
    pass


def stopper(child_behavior: Behavior[Cmd], events: list[str]) -> Behavior[Cmd | StopChild]:
    """Forwards to its child, stops it after pending on StopChild, and records its end."""

    def setup(ctx: ActorContext[Cmd | StopChild]) -> Behavior[Cmd | StopChild]:
        child = ctx.spawn(child_behavior, "child")
        ctx.watch(child)

        def on_message(msg: Cmd | StopChild) -> Behavior[Cmd | StopChild]:
            match msg:
                case StopChild():
                    ctx.stop(child, after_pending=True)
                case _:
                    child.tell(msg)
            return Behaviors.same

        def on_signal(
            _ctx: ActorContext[Cmd | StopChild], sig: Signal
        ) -> Behavior[Cmd | StopChild]:
            if isinstance(sig, Terminated):
                events.append("terminated")
            return Behaviors.same

        return Behaviors.receive_message(on_message).receive_signal(on_signal)

    return Behaviors.setup(setup)


async def test_an_after_pending_stop_during_a_write_finishes_it_first():
    store, log, events = GatedStore(), [], []
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        unhandled = testkit.create_unhandled_message_probe()
        parent = testkit.spawn(stopper(counter(store, log).recover("c", empty=0), events))
        parent.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        late = Add(3, probe.ref)
        for msg in (Add(2, probe.ref), StopChild(), late):
            parent.tell(msg)
        assert (await unhandled.receive_message()).message == late
        assert events == []
        store.gate.set()
        assert await probe.receive_messages(2) == [1, 3]
        await eventually(lambda: events == ["terminated"])
    assert store.states["c"] == (3, 2)


async def test_passivating_an_entity_mid_write_loses_no_forwarded_message():
    store, log = GatedStore(), []
    counter_key = EntityTypeKey[Cmd]("Counter")
    persisted = counter(store, log)
    region = Sharding.init(
        Entity(
            counter_key, lambda e: persisted.recover(e.entity_id, empty=0)
        ).with_passivate_after(0.02)
    )
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        c = Sharding.entity_ref_for(testkit.spawn(region), "c")
        c.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        c.tell(Add(2, probe.ref))
        await asyncio.sleep(0.1)  # idle past passivate_after while the write is gated
        store.gate.set()
        assert await probe.receive_messages(2) == [1, 3]
        assert await ask(c, Get, timeout=1) == 3
    assert store.states["c"] == (3, 2)


# --- failures and supervision while recovering or persisting ------------------------------------


class DownStore(InMemoryStore[int]):
    """Every load fails, and counts the attempts."""

    def __init__(self) -> None:
        super().__init__()
        self.loads = 0

    async def load(self, persistence_id: str) -> tuple[int, int] | None:
        self.loads += 1
        raise OSError("down")


class TwoLoadStore(GatedStore):
    """The first load waits on gates[0], every later one on gates[1]."""

    def __init__(self) -> None:
        super().__init__()
        self.gates = [asyncio.Event(), asyncio.Event()]
        self.loads = 0

    async def load(self, persistence_id: str) -> tuple[int, int] | None:
        i, self.loads = self.loads, self.loads + 1
        await self.gates[min(i, 1)].wait()
        return await super().load(persistence_id)


class SwitchableStore(InMemoryStore[int]):
    def __init__(self) -> None:
        super().__init__()
        self.fail = True

    async def save(self, persistence_id: str, state: int, revision: int) -> None:
        if self.fail:
            raise OSError("db down")
        await super().save(persistence_id, state, revision)


async def test_a_failing_load_under_restart_fails_the_actor_instead_of_spinning():
    store, log = DownStore(), []
    seen: list[BaseException] = []
    supervised = Behaviors.supervise(counter(store, log).recover("c", empty=0)).on_failure(
        SupervisorStrategy.restart()
    )
    async with ActorTestKit() as testkit:
        testkit.spawn(failures(supervised, seen))
        await eventually(lambda: bool(seen))
        await asyncio.sleep(0.02)
    assert isinstance(seen[0], OSError)
    assert (
        store.loads == 1
    )  # a plain restart on a failing recovery escalates, as on a failing setup


async def test_a_late_load_from_before_a_restart_never_reaches_the_user_behavior():
    store = TwoLoadStore()
    store.gate.set()
    store.states["c"] = (5, 1)
    seen: list[object] = []

    @Behaviors.persisted(store)
    def spy(total: int) -> Behavior[Cmd]:
        def on_message(msg: Cmd) -> Behavior[Cmd]:
            seen.append(msg)
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    supervised = Behaviors.supervise(spy.recover("c", empty=0, recovery_timeout=1)).on_failure(
        SupervisorStrategy.restart_with_backoff(0.5, 0.5, 0)
    )
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        ref = testkit.spawn(supervised)
        await manual.time_passes(1)  # the first recovery times out
        await manual.time_passes(0.5)  # the backoff restart loads again
        store.gates[1].set()  # the second load answers first: the actor is running
        await eventually(lambda: store.loads == 2)
        await asyncio.sleep(0.01)
        store.gates[0].set()  # the first incarnation's load answers late
        await asyncio.sleep(0.01)
        ref.tell(Add(1))
        await eventually(lambda: bool(seen))
    assert seen == [Add(1)]


async def test_resume_after_a_failed_write_carries_on_with_the_pre_write_behavior():
    store, log = SwitchableStore(), []
    supervised = Behaviors.supervise(counter(store, log).recover("c", empty=0)).on_failure(
        SupervisorStrategy.resume()
    )
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(supervised)
        ref.tell(Add(1, probe.ref))  # the save fails: resumed
        await asyncio.sleep(0.02)
        store.fail = False
        ref.tell(Add(2, probe.ref))
        assert await probe.receive_message() == 2  # the pre-write total was 0
    assert store.states["c"] == (2, 1)


async def test_a_restart_during_a_write_keeps_the_stashed_commands_and_held_signals():
    store = GatedStore()
    events: list[str] = []

    @Behaviors.persisted(store)
    def w(total: int) -> Behavior[WatchCmd]:
        def on_message(ctx: ActorContext[WatchCmd], msg: WatchCmd) -> Behavior[WatchCmd]:
            match msg:
                case WatchIt(ref):
                    ctx.watch(ref)
                    return Behaviors.same
                case Add(n, reply_to) if reply_to is not None:
                    return w(total + n).then_reply(reply_to, total + n)
                case Add():
                    return Behaviors.same

        def on_signal(_ctx: ActorContext[WatchCmd], sig: Signal) -> Behavior[WatchCmd]:
            if isinstance(sig, Terminated):
                events.append("terminated")
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    supervised = Behaviors.supervise(w.recover("c", empty=0)).on_failure(
        SupervisorStrategy.restart()
    )
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        other = testkit.spawn(Behaviors.receive_message(lambda _m: Behaviors.same))
        ref = testkit.spawn(supervised)
        ref.tell(WatchIt(other))
        ref.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        ref.tell(Add(2, probe.ref))  # into the internal stash
        await testkit.stop(other)  # its Terminated is held
        await asyncio.sleep(0.01)
        store.states["c"] = (99, 7)  # the save becomes a revision conflict: restart
        store.gate.set()
        assert await probe.receive_message() == 101  # recovered 99, then the stashed Add(2)
        await eventually(lambda: events == ["terminated"])


async def test_nested_supervision_lets_an_after_pending_stop_finish_the_write():
    store, log, events = GatedStore(), [], []
    inner = Behaviors.supervise(counter(store, log).recover("c", empty=0)).on_failure(
        SupervisorStrategy.restart(), ValueError
    )
    outer = Behaviors.supervise(inner).on_failure(SupervisorStrategy.stop(), OSError)
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        parent = testkit.spawn(stopper(outer, events))
        parent.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        parent.tell(Add(2, probe.ref))
        parent.tell(StopChild())
        await asyncio.sleep(0.01)
        store.gate.set()
        assert await probe.receive_messages(2) == [1, 3]
        await eventually(lambda: events == ["terminated"])


async def test_a_stop_during_a_write_dead_letters_the_stashed_commands():
    store, log = GatedStore(), []
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        dead = testkit.create_dead_letter_probe()
        ref = testkit.spawn(counter(store, log).recover("c", empty=0))
        ref.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        stashed = Add(2, probe.ref)
        ref.tell(stashed)
        await asyncio.sleep(0.01)  # let it reach the internal stash
        await testkit.stop(ref, max=1)
        assert (await dead.receive_message()).message == stashed


async def test_resume_after_a_replayed_command_fails_keeps_the_committed_state():
    store, log = GatedStore(), []
    supervised = Behaviors.supervise(counter(store, log).recover("c", empty=0)).on_failure(
        SupervisorStrategy.resume()
    )
    async with ActorTestKit() as testkit:
        probe: TestProbe[int] = testkit.create_test_probe()
        ref = testkit.spawn(supervised)
        ref.tell(Add(1, probe.ref))
        await eventually(lambda: store.events == ["saving"])
        ref.tell(Boom())  # stashed; raises when replayed after the write
        ref.tell(Add(2, probe.ref))
        await asyncio.sleep(0.01)
        store.gate.set()
        assert await probe.receive_messages(2) == [1, 3]
    assert store.states["c"] == (3, 2)
