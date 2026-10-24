# Event-Sourced Behaviors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Actors whose state survives a restart and a process restart, built as Pekko's `EventSourcedBehavior`: commands produce events, events are journaled, state is a fold over events, recovery replays the journal before the first command.

**Architecture:** A new optional module `pekko_py/persistence.py` with the behavior factory, the `Effect` values and a `Journal` protocol; two journals, in-memory (tests, examples) and SQLite over the stdlib `sqlite3` (durable, zero dependencies). The runtime in `cell.py` is untouched: recovery is an `await` inside the behavior's `setup`, which already blocks the mailbox, and journal writes are `await`s inside the command handler, which already serializes commands. Snapshots are out of scope until replay time is measured to matter.

**Tech Stack:** stdlib only (`sqlite3`, `pickle`, `asyncio.to_thread`).

**Spec:** Pekko's `persistence-typed/src/main/scala/org/apache/pekko/persistence/typed/scaladsl/EventSourcedBehavior.scala` and `Effect.scala` in `/Users/hanishi/pekko`, narrowed to: `persist`, `none`, `stop`, `unhandled`, `thenReply`, `thenRun`; no snapshots, no retention, no replication, no tagging.

## Global Constraints

- Same as the production-readiness plan: Python `>=3.12`, core `dependencies = []`, pytest and pyright strict green, Pekko names in snake_case, deviations documented.
- `cell.py` is not modified by this plan.
- Decisions a reviewer might contest, stated so they are not re-litigated per task: events are journaled before the state changes and before any reply, as in Pekko; a journal write that raises fails the actor (supervision decides what happens next), as in Pekko; `pickle` is the SQLite codec by default, documented as trusted-local-storage only, with a `codec: tuple[Callable[[object], bytes], Callable[[bytes], object]]` override.

## Review Focus

- A command handler that returns `Effect.persist(e)` where the event handler raises must leave the journal with `e` written and the actor failed, not half-applied; Task 3's `test_event_handler_failure_after_persist_fails_the_actor` asserts the journal length and that the actor stopped.
- Replaying an empty journal must start from `empty_state` without calling the event handler; Task 3's `test_recovery_with_empty_journal` asserts the handler was not called.
- Two actors with the same `persistence_id` on the same journal must see each other's events on recovery (that is the feature) and the plan does not try to lock; Task 4's `test_restart_recovers_from_sqlite` restarts the whole system and checks the state.
- `then_reply` after a persist of several events must reply once, after all of them; Task 3's `test_persist_many_then_reply_once`.
- A `SqliteJournal` opened on a path whose directory does not exist must raise at construction, not at first `append`; Task 4's `test_sqlite_journal_bad_path_raises_early`.

---

### Task 1: Effect values and the Journal protocol

**Files:**
- Create: `src/pekko_py/persistence.py`
- Test: `tests/test_persistence.py`

**Interfaces:**
- Produces:
  - `class Journal[E](Protocol)`: `async def append(self, persistence_id: str, events: Sequence[E]) -> None`; `async def replay(self, persistence_id: str) -> list[E]`.
  - `class InMemoryJournal[E]`: implements `Journal[E]`, backed by `dict[str, list[E]]`; `.events(persistence_id) -> list[E]` for tests.
  - `@dataclass(frozen=True) class Effect[E, S]`: fields `events: tuple[E, ...]`, `stop: bool = False`, `unhandled: bool = False`, `reply: tuple[ActorRef[Any], Callable[[S], Any]] | None = None`, `run: Callable[[S], None] | None = None`. Builders as static methods: `Effect.persist(*events: E) -> Effect[E, S]`, `Effect.none() -> Effect[E, S]`, `Effect.stop() -> Effect[E, S]`, `Effect.unhandled() -> Effect[E, S]`; chaining methods returning a new Effect: `then_reply(reply_to: ActorRef[R], message: Callable[[S], R])`, `then_run(callback: Callable[[S], None])`.

- [ ] **Step 1: Write the failing tests**

```python
def test_effect_builders_are_values():
    e = Effect.persist("a", "b").then_run(print)
    assert e.events == ("a", "b") and e.run is print and not e.stop
    assert Effect.none().events == () and Effect.stop().stop and Effect.unhandled().unhandled

async def test_in_memory_journal_round_trip():
    j: InMemoryJournal[str] = InMemoryJournal()
    await j.append("p1", ["a"]); await j.append("p1", ["b", "c"]); await j.append("p2", ["z"])
    assert await j.replay("p1") == ["a", "b", "c"] and await j.replay("p2") == ["z"] and await j.replay("nobody") == []
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_persistence.py -q`
Expected: `ModuleNotFoundError: pekko_py.persistence`.

- [ ] **Step 3: Implement `persistence.py` with the Effect dataclass, builders, Journal protocol and InMemoryJournal**

- [ ] **Step 4: Run tests and pyright**

Run: `uv run pytest tests/test_persistence.py -q && uv run pyright`
Expected: `2 passed`, `0 errors`.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/persistence.py tests/test_persistence.py
git commit -m "persistence: Effect values, Journal protocol, InMemoryJournal"
```

### Task 2: `event_sourced` behavior

**Files:**
- Modify: `src/pekko_py/persistence.py`
- Modify: `src/pekko_py/__init__.py` (export `Effect`, `Journal`, `InMemoryJournal`, `event_sourced`)
- Test: `tests/test_persistence.py`

**Interfaces:**
- Produces: `def event_sourced[C, E, S](persistence_id: str, empty_state: S, command_handler: Callable[[ActorContext[C], S, C], MaybeAwaitable[Effect[E, S]]], event_handler: Callable[[S, E], S], journal: Journal[E]) -> Behavior[C]`.
- Semantics per command: call `command_handler`; if `unhandled`, mark unhandled and keep state; else `await journal.append(persistence_id, effect.events)` (skipped when empty), fold each event through `event_handler` into the new state, run `effect.run(new_state)` if set, send `effect.reply[1](new_state)` to `effect.reply[0]` if set, then return the behavior with the new state, or `Behaviors.stopped()` if `effect.stop`.
- Recovery: `Behaviors.setup` whose factory is `async`, awaits `journal.replay`, folds, and returns the live behavior. Commands arriving during recovery wait in the mailbox (the setup has not returned).

- [ ] **Step 1: Write the failing tests**

The test fixture is a counter: commands `Add(n)`, `Get(reply_to)`, `Reset`; events `Added(n)`, `WasReset`; state `int`.

```python
async def test_commands_persist_events_and_reply_with_new_state():
    j = InMemoryJournal[Added | WasReset]()
    async with ActorSystem(counter("c1", j), "t") as system:
        system.tell(Add(2)); system.tell(Add(3))
        assert await system.ask(Get, timeout=1) == 5
    assert j.events("c1") == [Added(2), Added(3)]

async def test_recovery_replays_the_journal_before_the_first_command():
    j = InMemoryJournal[Added | WasReset](); await j.append("c1", [Added(40), Added(2)])
    async with ActorSystem(counter("c1", j), "t") as system:
        assert await system.ask(Get, timeout=1) == 42

async def test_restart_recovers_state_from_the_journal():
    # counter raises on Add(-1); supervised with restart; after the restart Get returns the pre-failure total
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_persistence.py -q`
Expected: `ImportError: cannot import name 'event_sourced'`.

- [ ] **Step 3: Implement `event_sourced` in `persistence.py`**

Live behavior is `Behaviors.receive(on_command)` closed over `state`, returning a new closure per command (state in behavior parameters, not a cell). `Get` in the fixture is `Effect.none().then_reply(msg.reply_to, lambda s: s)`.

- [ ] **Step 4: Run tests and pyright**

Run: `uv run pytest -q && uv run pyright`
Expected: all pass, `0 errors`.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/persistence.py src/pekko_py/__init__.py tests/test_persistence.py
git commit -m "event_sourced behavior: persist, fold, reply, recover"
```

### Task 3: Failure semantics

**Files:**
- Test: `tests/test_persistence.py`
- Modify: `src/pekko_py/persistence.py` only if a test fails.

- [ ] **Step 1: Write the tests from the Review Focus**

`test_journal_append_failure_fails_the_actor` (a journal whose `append` raises `OSError`; unsupervised actor terminates; `Get` afterwards times out), `test_event_handler_failure_after_persist_fails_the_actor`, `test_recovery_with_empty_journal`, `test_persist_many_then_reply_once`, `test_unhandled_effect_keeps_state_and_marks_unhandled` (asserts `system.stats.unhandled == 1` if the production-readiness plan's Task 3 has landed; otherwise asserts via a caplog on `pekko_py.dead_letters`).

- [ ] **Step 2: Run, fix anything red in `persistence.py`, run again**

Run: `uv run pytest tests/test_persistence.py -q`
Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_persistence.py src/pekko_py/persistence.py
git commit -m "persistence: failure semantics pinned"
```

### Task 4: SQLite journal

**Files:**
- Modify: `src/pekko_py/persistence.py` (add `SqliteJournal`)
- Test: `tests/test_persistence.py`

**Interfaces:**
- Produces: `class SqliteJournal[E]`: `__init__(self, path: str | Path, codec: tuple[Callable[[object], bytes], Callable[[bytes], object]] = (pickle.dumps, pickle.loads))`; implements `Journal[E]`; table `events(persistence_id TEXT, seq INTEGER, payload BLOB, PRIMARY KEY (persistence_id, seq))`; `append` runs its `executemany` plus commit via `asyncio.to_thread`; `replay` selects ordered by `seq`; `close()`. The connection is opened with `check_same_thread=False` and guarded by a `threading.Lock` around each transaction.

- [ ] **Step 1: Write the tests**

`test_sqlite_journal_round_trip(tmp_path)`, `test_restart_recovers_from_sqlite(tmp_path)` (run a system, add, stop the system, start a new system on the same file, `Get` returns the total), `test_sqlite_journal_bad_path_raises_early(tmp_path)` (`SqliteJournal(tmp_path / "missing" / "j.db")` raises `sqlite3.OperationalError`), `test_sqlite_journal_custom_codec(tmp_path)` with a JSON codec.

- [ ] **Step 2: Run to verify failure, implement, run again with pyright**

Run: `uv run pytest tests/test_persistence.py -q && uv run pyright`
Expected: all pass, `0 errors`.

- [ ] **Step 3: Commit**

```bash
git add src/pekko_py/persistence.py tests/test_persistence.py
git commit -m "SqliteJournal over the stdlib, pickle by default"
```

### Task 5: Example and documentation

**Files:**
- Create: `examples/event_sourced_cart.py`
- Modify: `README.md` (tour section "Persistence", a row in "Pekko names", deviations lines, examples row)

- [ ] **Step 1: Write the example**

The cart from `examples/fastapi_carts.py` as an event-sourced actor on `SqliteJournal("carts.db")`: commands `AddItem`, `Checkout`, `GetCart`; events `ItemAdded`, `CheckedOut`. `main()` adds two items, exits the system, starts a new one on the same file and prints the recovered cart, then deletes the file. Run it: `uv run python examples/event_sourced_cart.py` prints the same items twice, the second time after a fresh start.

- [ ] **Step 2: Document**

README tour: a 15-line snippet with the counter fixture. "Pekko names" row: `EventSourcedBehavior(persistenceId, emptyState, commandHandler, eventHandler)`, `Effect.persist / none / stop / unhandled / thenReply / thenRun` → `event_sourced(persistence_id, empty_state, command_handler, event_handler, journal)`, same effects in snake_case. Deviations: the journal is a constructor argument, not a system-wide plugin; no snapshots, retention, replication or tags; `pickle` codec by default; recovery blocks the mailbox by awaiting in `setup` rather than stashing.

- [ ] **Step 3: Verify and commit**

Run: `uv run python examples/event_sourced_cart.py && uv run pyright examples && uv run pytest -q`
Expected: recovered cart printed; `0 errors`; all tests pass.

```bash
git add examples/event_sourced_cart.py README.md
git commit -m "Event-sourced cart example and persistence docs"
```

---

## Self-review notes

- Spec coverage: `persist`, `none`, `stop`, `unhandled`, `thenReply`, `thenRun` (Tasks 1, 2); recovery (Task 2); journal failure (Task 3); durable journal (Task 4); docs (Task 5).
- Names used across tasks: `Effect`, `Journal`, `InMemoryJournal`, `SqliteJournal`, `event_sourced`, `InMemoryJournal.events` are consistent.
- The decision not to touch `cell.py` is what keeps this a 300-line module: recovery and journaling ride on the two properties the runtime already has, awaiting setup blocks the mailbox and handlers are serialized.
