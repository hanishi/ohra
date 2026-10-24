# Production Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn pekko-py from a repository into a dependency: CI on every push, a tagged release on PyPI, counters for what goes wrong at runtime, and a documented, tested shutdown story.

**Architecture:** Nothing here changes message semantics. CI and release are configuration under `.github/`. Observability is a small `SystemStats` counter object owned by `ActorSystem` and incremented from the three places that already log. Shutdown is a test and documentation of the ordering the runtime already has, plus one uvicorn flag in the web examples.

**Tech Stack:** GitHub Actions, `uv`, PyPI trusted publishing (no API token), `pytest`, `pyright`.

**Spec:** The six-item list in the conversation of 2026-10-23 ("What I would add before depending on it in production"). Persistence is the separate plan `2026-10-23-persistence.md`. "Someone besides me reading `cell.py`" is Task 6, a process step.

## Global Constraints

- Python `>=3.12`; the core package keeps `dependencies = []`.
- The core stays readable in one sitting: `src/pekko_py/` is 1,535 lines today and this plan may add at most 60. New capability goes in new, optional modules.
- `pytest` must pass and every test fails if an asyncio task outlives it (`tests/conftest.py`).
- `pyright` strict on `src/` and `tests/typing_check.py` reports 0 errors; `pyright examples/` also reports 0 errors.
- Public names follow Pekko in snake_case; intentional deviations are documented in the README's "Known deviations" section.
- No `functools.partial`, no tuple-index lambdas, no phase numbers anywhere in docs or code.
- Commit messages have no co-author trailer.

## Review Focus

- A dead letter raised while the system is terminating (e.g. a late ask reply) must still be counted and must not raise; Task 3 adds `test_stats_count_dead_letters_during_shutdown`.
- `SystemStats.actors` must return to 0 after the system terminates even when children failed; Task 3 adds `test_stats_actors_returns_to_zero_after_failure`.
- A restart budget exhausted by a backoff supervisor must count every attempt, not only the successful restarts; Task 3's `test_stats_count_restarts` sends enough failures to exhaust `with_max_restarts(2)` and asserts `restarts == 2`.
- Terminating a system while an `ask` is in flight must fail that ask with `AskTimeoutError` (dead letter), not hang; Task 5 adds `test_ask_in_flight_at_terminate_fails_fast`.
- CI must fail when a `# type: ignore` in `tests/typing_check.py` becomes unnecessary; Task 1's workflow runs `pyright` with the project's `reportUnnecessaryTypeIgnoreComment = "error"` setting, verified by Step 4.

---

### Task 1: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: a workflow named `ci` with one job `check` that later tasks (release) depend on by name.

- [ ] **Step 1: Write the workflow**

`.github/workflows/ci.yml`, triggered on `push` to `main` and on `pull_request`. One job `check` on `ubuntu-latest` with a matrix of `python-version: ["3.12", "3.13", "3.14"]`. Steps: `actions/checkout@v4`, `astral-sh/setup-uv@v5` with `python-version: ${{ matrix.python-version }}`, `uv sync --extra dev --extra numpy`, `uv run pytest -q`, `uv run pyright`, `uv run pyright examples`. The `web` extra is not installed in CI; `pyright examples` must therefore skip the three `fastapi_*.py` files: pass them explicitly as `uv run pyright examples --ignoreexternal` is not a thing, so instead run `uv run pyright $(ls examples/*.py examples/interaction_patterns/*.py | grep -v fastapi_)`.

- [ ] **Step 2: Verify the workflow locally with the same commands**

Run: `uv sync --extra dev --extra numpy && uv run pytest -q && uv run pyright && uv run pyright $(ls examples/*.py examples/interaction_patterns/*.py | grep -v fastapi_)`
Expected: `48 passed`, then `0 errors` twice.

- [ ] **Step 3: Commit and push, then watch the run**

```bash
git add .github/workflows/ci.yml
git commit -m "CI: pytest and pyright on 3.12, 3.13, 3.14"
git push
gh run watch
```
Expected: three green matrix legs.

- [ ] **Step 4: Prove CI catches a typing regression**

On a branch, delete the `# type: ignore[arg-type]` on the `str_ref.tell(42)` line of `tests/typing_check.py`, push, and confirm the run fails on `pyright` with "Unnecessary ... type: ignore" is NOT the failure (the ignore is needed, so removing it makes `tell(42)` itself an error). Either way the run must be red. Delete the branch without merging.

### Task 2: Release to PyPI

**Files:**
- Modify: `pyproject.toml` (add `[project.urls]`, `readme = "README.md"`, classifiers)
- Create: `.github/workflows/release.yml`
- Create: `CHANGELOG.md`

**Interfaces:**
- Consumes: the `ci` workflow from Task 1.
- Produces: tags of the form `v0.1.0` publish `pekko-py` to PyPI.

- [ ] **Step 1: Add package metadata**

In `pyproject.toml` under `[project]`: `readme = "README.md"`, `classifiers = ["Programming Language :: Python :: 3", "License :: OSI Approved :: Apache Software License", "Framework :: AsyncIO", "Typing :: Typed"]`, and `[project.urls]` with `Homepage = "https://github.com/hanishi/pekko-py"`. Add an empty `src/pekko_py/py.typed` so the `Typing :: Typed` classifier is honest.

- [ ] **Step 2: Verify the package builds and installs clean**

Run: `uv build && cd /tmp && uv venv -q .pkgtest && .pkgtest/bin/python -m pip install -q /Users/hanishi/pekko-py/dist/pekko_py-0.1.0-py3-none-any.whl && .pkgtest/bin/python -c "import pekko_py, pathlib; print(pekko_py.__file__); print((pathlib.Path(pekko_py.__file__).parent / 'py.typed').exists())"`
Expected: a site-packages path and `True`. Confirm `tar tzf dist/pekko_py-0.1.0.tar.gz | grep -c LICENSE` prints at least `1` and the same for `NOTICE`.

- [ ] **Step 3: Write the release workflow**

`.github/workflows/release.yml`, triggered on `push` of tags matching `v*`. Job `publish` on `ubuntu-latest` with `permissions: id-token: write`, `environment: pypi`. Steps: checkout, `astral-sh/setup-uv@v5`, `uv build`, `pypa/gh-action-pypi-publish@release/v1`. No token: the PyPI project must have a trusted publisher configured for `hanishi/pekko-py`, workflow `release.yml`, environment `pypi`; that is a one-time setting on pypi.org the human owner does.

- [ ] **Step 4: Write `CHANGELOG.md`**

Heading `# Changelog`, then `## 0.1.0 — 2026-10-23` with one bullet per README "Pekko names" table row group: behaviors and refs, ask and adapters, lifecycle and watch, supervision, timers and stash, mailboxes and batching, routers and SpawnProtocol, shared memory, examples. Keep it under 25 lines.

- [ ] **Step 5: Commit, tag, push, verify**

```bash
git add pyproject.toml src/pekko_py/py.typed .github/workflows/release.yml CHANGELOG.md
git commit -m "Release metadata, changelog, PyPI publish workflow"
git tag v0.1.0
git push && git push --tags
gh run watch
```
Expected: `publish` job green and `pip install pekko-py==0.1.0` works from a clean venv. If the trusted publisher is not configured yet, the job fails at upload; configure it and re-run the job rather than re-tagging.

### Task 3: Runtime counters

**Files:**
- Create: `src/pekko_py/stats.py`
- Modify: `src/pekko_py/system.py:23-60` (own a `SystemStats`, increment in `dead_letter` and `unhandled`)
- Modify: `src/pekko_py/cell.py:462-505` (increment `restarts` beside the three `self.log.warning(...)` calls) and `cell.py:248-262` / `cell.py:546-580` (increment and decrement `actors`)
- Modify: `src/pekko_py/__init__.py` (export `SystemStats`)
- Test: `tests/test_stats.py`
- Modify: `README.md` (one row in "Pekko names": `(none)` → `system.stats`; one line in "Runtime model")

**Interfaces:**
- Produces: `@dataclass class SystemStats: dead_letters: int = 0; unhandled: int = 0; restarts: int = 0; actors: int = 0` (mutable, not frozen) and `ActorSystem.stats: SystemStats`.

- [ ] **Step 1: Write the failing tests**

```python
async def test_stats_count_dead_letters_and_unhandled():
    async with ActorSystem(Behaviors.empty, "t") as system:  # empty: every message is unhandled
        system.tell("a")
        await eventually(lambda: system.stats.unhandled == 1)
        system.terminate()
        await system.when_terminated()
        system.tell("late")
    assert system.stats.dead_letters == 1

async def test_stats_count_restarts():
    b = Behaviors.supervise(failing_on_negative()).on_failure(SupervisorStrategy.restart_with_backoff(0.001, 0.01).with_max_restarts(2))
    system = ActorSystem(b, "t")
    for _ in range(3): system.tell(-1)
    await asyncio.wait_for(system.when_terminated(), 1)
    assert system.stats.restarts == 2

async def test_stats_actors_returns_to_zero_after_failure():
    # guardian spawns 3 children, one of which raises on its first message
    ...
    assert system.stats.actors == 4  # guardian + 3, before the failure
    ...
    assert system.stats.actors == 0  # after terminate

async def test_stats_count_dead_letters_during_shutdown():
    # an ask whose reply arrives after terminate: the reply is a dead letter, counted, no exception
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_stats.py -q`
Expected: `AttributeError: 'ActorSystem' object has no attribute 'stats'`.

- [ ] **Step 3: Implement**

`stats.py` holds `SystemStats` with a docstring saying the counters are process-local and monotonic except `actors`. `ActorSystem.__init__` creates `self.stats = SystemStats()`; `dead_letter` and `unhandled` do `self.stats.dead_letters += 1` / `unhandled += 1` before logging. In `cell.py`, `ActorCell.__init__` does `system.stats.actors += 1`; `_finish` decrements it on the line before `self.terminated = True`; `_on_failure` increments `restarts` on each of the two restart branches (plain and backoff), not on resume. Count attempts, so the increment sits before `await self._pre_restart(sup)` in both branches.

- [ ] **Step 4: Run the tests to verify they pass, then the whole suite and pyright**

Run: `uv run pytest -q && uv run pyright`
Expected: `52 passed`, `0 errors`.

- [ ] **Step 5: Document and commit**

README "Runtime model": "`system.stats` counts dead letters, unhandled messages, restarts and live actors; expose it from a `/metrics` route or a timer." Add the `(none) | system.stats` row.

```bash
git add src/pekko_py/stats.py src/pekko_py/system.py src/pekko_py/cell.py src/pekko_py/__init__.py tests/test_stats.py README.md
git commit -m "system.stats: dead letter, unhandled, restart and live-actor counters"
```

### Task 4: Expose the counters in the web examples

**Files:**
- Modify: `examples/fastapi_carts.py`, `examples/fastapi_online_learning.py`, `examples/fastapi_chatroom.py` (one `GET /stats` route each)

**Interfaces:**
- Consumes: `ActorSystem.stats: SystemStats` from Task 3.

- [ ] **Step 1: Add the route**

In each file, after the existing routes: `@app.get("/stats") async def stats() -> dict[str, int]: return dataclasses.asdict(system().stats)` (the carts and chat files have `system()`; the online-learning file uses `app.state.system`).

- [ ] **Step 2: Verify**

Extend the in-process smoke script used for the carts example (`/private/tmp/.../smoke_carts.py` pattern: run uvicorn in a task, call with `urllib` via `asyncio.to_thread`) to `GET /stats` after the checkout and assert `actors == 1` (only the manager remains) and `dead_letters == 0`. Run `uv run pyright examples`.
Expected: the assertion holds; `0 errors`.

- [ ] **Step 3: Commit**

```bash
git add examples/fastapi_carts.py examples/fastapi_online_learning.py examples/fastapi_chatroom.py
git commit -m "GET /stats in the web examples"
```

### Task 5: Shutdown story

**Files:**
- Test: `tests/test_lifecycle.py` (two tests appended)
- Modify: `README.md` (a "Shutdown" paragraph in "Runtime model")
- Modify: the three `examples/fastapi_*.py` docstrings (the uvicorn command gains `--timeout-graceful-shutdown 10`)

**Interfaces:**
- Consumes: nothing new. This task pins behavior the runtime already has.

- [ ] **Step 1: Write the failing-or-passing tests**

```python
async def test_ask_in_flight_at_terminate_fails_fast():
    """terminate() while an ask is pending: the reply is a dead letter and the ask times out; nothing hangs."""
    async with ActorSystem(slow_replier(delay=0.2), "t") as system:   # replies after 200ms
        pending = asyncio.create_task(system.ask(Get, timeout=0.5))
        await asyncio.sleep(0.01)
        system.terminate()
        with pytest.raises(AskTimeoutError):
            await pending

async def test_pending_user_messages_become_dead_letters_on_terminate():
    async with ActorSystem(recorder("a", events), "t") as system:
        for i in range(100): system.tell(str(i))
        system.terminate()
        await system.when_terminated()
    assert system.stats.dead_letters > 0 and system.stats.dead_letters + len(events) == 100
```

- [ ] **Step 2: Run them**

Run: `uv run pytest tests/test_lifecycle.py -q`
Expected: both pass without runtime changes. If the first hangs, the bug is in `PromiseRef`/`ask` and must be fixed in `src/pekko_py/patterns.py` before continuing; do not loosen the test.

- [ ] **Step 3: Document**

README "Runtime model", new bullet "Shutdown": `terminate()` is a system message, so it runs before queued user messages, which become dead letters; in-flight asks fail with `AskTimeoutError` when their reply is dead-lettered. Behind a web server, let the server drain requests first: `uvicorn --timeout-graceful-shutdown 10` stops accepting, waits for in-flight handlers (and their asks), then runs lifespan shutdown, which terminates the system. Work that must finish on stop belongs in `PostStop`, as `batching` does with its last batch.

- [ ] **Step 4: Commit**

```bash
git add tests/test_lifecycle.py README.md examples/fastapi_carts.py examples/fastapi_online_learning.py examples/fastapi_chatroom.py
git commit -m "Shutdown: pin ask-in-flight and dead-letter behavior, document the web drain"
```

### Task 6: Outside review of the message loop

**Files:**
- None created. A review of `src/pekko_py/cell.py` and `src/pekko_py/patterns.py`.

- [ ] **Step 1: Run the review**

The human runs `/code-review ultra` in Claude Code on `main` (user-triggered; it cannot be launched by the agent), or opens a PR and asks a colleague. The brief: verify the four invariants in `CLAUDE.md` against the code, with attention to `_finish` (no awaits between "mark terminated" and the notifications), `_supervise` during backoff, and `_replay`.

- [ ] **Step 2: Triage**

Each finding becomes either a test plus a fix, a documented deviation, or a rejected finding with a one-line reason recorded in the PR or commit message. Use `superpowers:receiving-code-review` for the triage.

---

## Self-review notes

- Spec coverage: release (Task 2), CI (Task 1), visibility (Tasks 3, 4), persistence (separate plan), shutdown (Task 5), outside review (Task 6).
- The 60-line core budget: Task 3 adds about 15 lines across `stats.py`, `system.py`, `cell.py`. Tasks 1, 2, 4, 5 add nothing to `src/` except `py.typed`.
- Order: Task 1 first (everything after is checked by it), Task 2 last among code tasks so the release includes `system.stats`. Suggested execution order: 1, 3, 4, 5, 2, 6.
