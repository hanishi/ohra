# Persisted State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Actors whose state survives passivation, restarts and process restarts: a persisting command writes the whole state before replying, recovery is one read.

**Architecture:** One new module `pekko_py/persistence.py`: a `Store[S]` protocol with `InMemoryStore` and `SqliteStore`, a `Persisted[S, T]` wrapper produced by `Behaviors.persisted(store)` around a state-to-behavior factory, and a `Persist[T]` node (a `Setup` subclass) that writes, runs deferred effects, then becomes `factory(state)`. The incarnation (id, store, revision) lives in a `ContextVar` set by `recover`'s setup, visible only inside that actor's task. `cell.py` is untouched.

**Tech Stack:** stdlib only (`contextvars`, `sqlite3`, `pickle`, `asyncio.to_thread`).

**Spec:** `docs/superpowers/specs/2026-10-24-persisted-state-design.md`

## Global Constraints

- Python `>=3.12`, core `dependencies = []`, `uv run pytest` and `uv run pyright` (strict) green after every task.
- `cell.py` is not modified. `behavior.py` gains only the `Behaviors.persisted` staticmethod, which imports `persistence` lazily to avoid a cycle.
- Commits: hanishi commits on request. Do not run `git commit`.
- Tests follow `tests/test_sharding.py` style: frozen dataclass messages, one behavior per test, `eventually`, no sleeps over 50ms, every test under the conftest leak check.

## Review Focus

- A write that raises must leave the actor on its old behavior with no deferred reply sent, and a supervised restart must reload the committed state. Task 2, `test_failed_write_fails_the_actor_before_any_reply` and `test_restart_recovers_the_committed_state`.
- The deferred reply must leave only after the store has acknowledged the write, not merely after the call started. Task 2, `test_then_reply_runs_after_the_write_completes` with a store whose `save` awaits a gate.
- Two incarnations of one id must not silently overwrite each other. Task 2, `test_revision_conflict_fails_the_second_writer`.
- A `Persist` node in an actor not started through `recover` must fail that actor with `PersistenceError`, not corrupt another actor's incarnation. Task 2, `test_persist_outside_recover_fails_the_actor`.
- `SqliteStore` on a directory that does not exist must raise at construction. Task 3, `test_sqlite_store_bad_directory_raises_at_construction`.

---

### Task 1: Store protocol, in-memory store, exceptions

**Files:**
- Create: `src/pekko_py/persistence.py`
- Test: `tests/test_persistence.py`

**Interfaces:**
- Produces:
  - `class RevisionConflictError(Exception)`, `class PersistenceError(Exception)`.
  - `class Store[S](Protocol)`: `async def load(self, persistence_id: str) -> tuple[S, int] | None`; `async def save(self, persistence_id: str, state: S, revision: int) -> None`; `async def delete(self, persistence_id: str) -> None`.
  - `class InMemoryStore[S]`: implements `Store[S]` over `self.states: dict[str, tuple[S, int]]`. `save` raises `RevisionConflictError` unless `revision == 1 and id absent` or `revision == stored + 1`.

- [ ] **Step 1: Write the failing tests**

```python
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
        await store.save("a", 5, 2)       # nothing stored, revision must be 1
    await store.save("a", 5, 1)
    with pytest.raises(RevisionConflictError):
        await store.save("a", 6, 1)       # stale
    with pytest.raises(RevisionConflictError):
        await store.save("a", 6, 3)       # skipped
```

- [ ] **Step 2: Run to verify they fail** — `uv run pytest tests/test_persistence.py -q` → `ModuleNotFoundError`.
- [ ] **Step 3: Implement** the three names above in `persistence.py`.
- [ ] **Step 4: Run to verify they pass** — same command, then `uv run pyright` → 0 errors.

### Task 2: `Behaviors.persisted`, `Persisted`, `Persist`, `recover`

**Files:**
- Modify: `src/pekko_py/persistence.py`, `src/pekko_py/behavior.py` (add `Behaviors.persisted`), `src/pekko_py/__init__.py` (export `InMemoryStore`, `Persist`, `Persisted`, `PersistenceError`, `RevisionConflictError`, `Store`)
- Test: `tests/test_persistence.py`, `tests/typing_check.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class _Incarnation[S]: persistence_id: str; store: Store[S]; revision: int` and `_current: ContextVar[_Incarnation[Any] | None] = ContextVar("pekko_py.persistence", default=None)`.
  - `@dataclass(frozen=True, eq=False, slots=True) class Persist[T](Setup[T])`: extra fields `state: Any`, `effects: tuple[Callable[[], None], ...]`, `owner: Persisted[Any, T]`; methods `then_reply[R](self, reply_to: ActorRef[R], message: R) -> Persist[T]` and `then_run(self, callback: Callable[[], None]) -> Persist[T]`. Its `factory` is `owner._write_then_become(self)`: an async function taking the ctx that reads `_current`, raises `PersistenceError` if `None`, awaits `store.save(id, state, revision + 1)`, sets `_current` to the advanced incarnation, runs effects in order, returns `owner.factory(state)`.
  - `class Persisted[S, T]`: `__init__(self, store: Store[S], factory: Callable[[S], Behavior[T]])`; `__call__(self, state: S) -> Persist[T]`; `recover(self, persistence_id: str, empty: S) -> Behavior[T]` as a `Setup` whose async factory loads, sets `_current`, returns `self.factory(state)`.
  - `def persisted[S](store: Store[S]) -> Callable[[Callable[[S], Behavior[T]]], Persisted[S, T]]` (module function; typed so pyright infers S and T from the decorated function), and `Behaviors.persisted = staticmethod(...)` delegating to it via a lazy import.

- [ ] **Step 1: Write the failing tests**

Fixture: `Add(n)`, `Get(reply_to: ActorRef[int])`, `Boom`; `type Cmd = Add | Get | Boom`; `counter(store) -> Persisted[int, Cmd]` built with `Behaviors.persisted(store)` on `def counter(total: int) -> Behavior[Cmd]` whose handler returns `counter(total + n).then_reply(...)` for `Add` when a reply_to is given (make `Add(n, reply_to: ActorRef[int] | None = None)`), `reply_to.tell(total)` for `Get`, raises on `Boom`. A `GatedStore` wrapping `InMemoryStore` whose `save` awaits an `asyncio.Event` and records the order of events in a list; a `FailingStore` whose `save` raises `OSError`.

```python
async def test_recover_starts_from_empty_when_nothing_is_stored()      # Get == 0, store.states == {}
async def test_persist_writes_before_becoming_the_new_behavior()       # Add(2); Get == 2; store.states["c"] == (2, 1)
async def test_recover_loads_the_stored_state()                        # second system on same store: Get == 2; revision continues at 2
async def test_then_reply_runs_after_the_write_completes()             # GatedStore: reply not received while gate closed; received after
async def test_then_run_runs_after_the_write_in_order()                # two then_run callbacks append in order after "saved"
async def test_same_and_reads_do_not_write()                           # Get x3: store untouched
async def test_failed_write_fails_the_actor_before_any_reply()         # FailingStore: ask → AskTimeoutError, actor terminated (watch), states unchanged
async def test_restart_recovers_the_committed_state()                  # supervise(recover).on_failure(restart): Add(1); Boom; Get == 1 (from store), start count 2
async def test_revision_conflict_fails_the_second_writer()             # two systems on one store, same id, both Add: second's actor fails; ask times out; store has one write
async def test_persist_outside_recover_fails_the_actor()               # spawn counter(0) directly: ChildFailed with PersistenceError
async def test_persisted_entity_survives_passivation()                 # Sharding + with_passivate_after(0.02): Add(3); wait passivation; Get == 3; two starts
```

`typing_check.py`:

```python
def persistence_is_typed(store: InMemoryStore[int]) -> None:
    @Behaviors.persisted(store)
    def counter(total: int) -> Behavior[Ping]:
        return _b
    node: Behavior[Ping] = counter(1)
    counter("one")  # type: ignore[arg-type]
    counter.recover("id", empty="zero")  # type: ignore[arg-type]
    wrong: Behavior[str] = counter.recover("id", empty=0)  # type: ignore[assignment]
    del node, wrong
```

- [ ] **Step 2: Run to verify they fail** — `uv run pytest tests/test_persistence.py -q` → `ImportError`/`AttributeError` for `Behaviors.persisted`.
- [ ] **Step 3: Implement** per the Interfaces block. Ruling to apply if `Setup`'s slots dataclass resists subclassing with new fields: make `Persist` its own frozen dataclass and have `recover` and `Persist` both build a plain `Setup` internally, exposing `Persist` as `Behavior[T]`; record the ruling.
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright` → all pass, 0 errors; every type-ignore in `typing_check.py` necessary.

### Task 3: `SqliteStore`

**Files:**
- Modify: `src/pekko_py/persistence.py`, `src/pekko_py/__init__.py` (export `SqliteStore`)
- Test: `tests/test_persistence.py`

**Interfaces:**
- Produces: `class SqliteStore[S]`: `__init__(self, path: str | os.PathLike[str], codec: tuple[Callable[[S], bytes], Callable[[bytes], S]] = (pickle.dumps, pickle.loads))`; raises `FileNotFoundError` if the parent directory does not exist; creates table `state(persistence_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, data BLOB NOT NULL)` on construction. `load`/`save`/`delete` run their sqlite work in `asyncio.to_thread`, each opening a short-lived connection (`sqlite3.connect(path)`), `save` uses `INSERT ... WHERE NOT EXISTS` for revision 1 and `UPDATE ... WHERE persistence_id = ? AND revision = ?` otherwise, raising `RevisionConflictError` when `rowcount == 0`.

- [ ] **Step 1: Write the failing tests** (use `tmp_path`):

```python
async def test_sqlite_store_round_trip_and_conflicts(tmp_path)         # same assertions as the in-memory tests
async def test_sqlite_store_bad_directory_raises_at_construction(tmp_path)
async def test_sqlite_store_custom_codec(tmp_path)                     # json codec; raw row data is JSON text
async def test_state_survives_a_process_restart(tmp_path)              # system 1 writes, system 2 (new SqliteStore on same path) recovers
```

- [ ] **Step 2: Run to verify they fail** — `ImportError: SqliteStore`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run to verify they pass** — `uv run pytest -q`, `uv run pyright` → all pass, 0 errors.

### Task 4: Carts example, README, gitignore

**Files:**
- Modify: `examples/fastapi_carts.py`, `README.md`, `.gitignore` (add `examples/*.db`)

- [ ] **Step 1: Carts on persisted state.** `cart` becomes `@Behaviors.persisted(store)` over `def cart(items)`; `AddItem` returns `cart(new).then_reply(reply_to, new)`; `Checkout` returns `cart({}).then_reply(reply_to, items).then_run(lambda: entity.shard.tell(Passivate(self_ref)))` where `self_ref` is captured from `ctx` (use `Behaviors.receive` to get it); the closed behavior stays. `store = SqliteStore(os.environ.get("CARTS_DB", Path(__file__).with_name("carts.db")))`. The entity factory is `lambda e: cart.recover(e.entity_id, empty={})`. The `existing()` helper and routes are unchanged.
- [ ] **Step 2: Verify** — `uv run pyright examples/fastapi_carts.py` → 0 errors. In-process smoke (lifespan + httpx) against a tmp `CARTS_DB`: add, get, restart the app (second lifespan on the same db), get again returns the items; checkout; get → 404.
- [ ] **Step 3: README** — "Persistence" Tour subsection with the spec snippet; names table rows: `DurableStateBehavior(persistenceId, emptyState, commandHandler)` → `@Behaviors.persisted(store)` on a state-to-behavior factory plus `recover(id, empty)`; `Effect.persist(state).thenReply(...)` → `factory(state).then_reply(...)`; `DurableStateBehavior` stores → `InMemoryStore`, `SqliteStore`; deviations from the spec; carts row mentions SQLite persistence; install note unchanged (stdlib). Verify the snippet with pyright and a run, as for the other Tour blocks.
- [ ] **Step 4: Final verification** — `uv run pytest -q`, `uv run pyright` → all pass, 0 errors. Report the tree as ready to commit.

## Self-review notes

- Spec coverage: API and `Store` (Tasks 1, 2), mechanism and invariants (2), SQLite (3), example and docs (4). Out-of-scope items appear nowhere.
- Names consistent: `Behaviors.persisted`, `Persisted`, `Persist`, `recover`, `then_reply`, `then_run`, `Store`, `InMemoryStore`, `SqliteStore`, `RevisionConflictError`, `PersistenceError`, `_Incarnation`, `_current`.
- Review Focus lines each name a test and task.
- Proportion: signatures, state and tests; bodies left to the implementer except the sqlite statements and the ContextVar flow, which the tests do not determine.
