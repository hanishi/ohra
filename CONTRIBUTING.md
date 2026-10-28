# Contributing to ohra

ohra brings Apache Pekko Typed semantics to asyncio. Start with [the book](docs/book/README.md), especially [the runtime model](docs/book/01-actors.md) in chapter 1 and [the known deviations](docs/book/appendix-deviations.md) in appendix B.

Bug reports, fixes, docs and new ports of Apache Pekko features are all welcome. Before you start anything large, open an issue so we can agree on the scope.

## Setup

```
uv sync --all-extras        # or: pip install -e '.[dev,numpy,web,postgres,torch,numba,testkit]'
git config blame.ignoreRevsFile .git-blame-ignore-revs   # git blame skips the PEP 8 reformat
```

## Checks

Every PR has to pass these checks, and CI runs them on Python 3.12 and 3.13:

```
uv run ruff format   # PEP 8 layout, lines up to 99 characters
uv run ruff check    # PEP 8 style and naming
uv run pytest        # every test fails if any asyncio task outlives it (tests/conftest.py)
uv run pytest examples/testing   # the book's chapter 12 tests, run as a reader runs them
uv run pyright       # strict mode, 0 errors
```

- `ruff format` leaves comments, docstrings and strings alone, so wrap those yourself. A pragma comment (`# pyright: ignore[...]`, `# type: ignore`) has to stay on the line it is about.
- `# noqa` is for names that come from Apache Pekko (such as `ClusterSingletonManagerIsStuck`) and for the minified pages the examples serve.

- `tests/typing_check.py` has `# type: ignore` lines that assert wrong message types are rejected. They must stay necessary, and pyright reports an unnecessary one as an error.
- `tests/test_postgres_store.py` is skipped unless `OHRA_POSTGRES_DSN` is set:
  ```
  docker run -d --rm -p 55432:5432 -e POSTGRES_PASSWORD=pg postgres:17
  OHRA_POSTGRES_DSN=postgresql://postgres:pg@localhost:55432/postgres uv run pytest tests/test_postgres_store.py
  ```

## Writing tests

- **Layout.** `tests/` follows `src/ohra/`: `tests/cluster/`, `tests/management/`, `tests/testkit/`, and roughly one file per core module at the top. Run one area with `uv run pytest tests/cluster -q`, or one test with `-k`.
- **Port the Pekko spec.** When you port a feature, port its tests too, from the matching `*Spec.scala` in the Apache Pekko repository (`TestProbeSpec`, `ActorTestKitSpec`, ...). Name the spec in the module docstring, as `tests/testkit/test_probe.py` does. Each test asserts one behavior, on what can be seen from outside: replies, `Terminated`, what a store holds. Reach into private fields only when nothing else shows the behavior.
- **Nothing may outlive a test.** The autouse fixture in `tests/conftest.py` fails every test that leaves an asyncio task running. Start systems with `async with ActorSystem(...)` or `async with ActorTestKit()`, so they stop when the block ends. If a test starts a task of its own, it awaits or cancels it before it returns. A test that fails only through the leak check usually means a stop that does not stop: look there before you add a sleep.
- **Wait on a condition, not a duration.** Use the test kit: a `TestProbe` as `reply_to` and `await probe.expect_message(...)`, `expect_terminated`, `await_assert`, and `ManualTime` for timers, backoff delays and ask timeouts. [Chapter 12](docs/book/12-testing.md) covers them. For a condition no probe can see, `tests/util.py` has `eventually(cond, timeout=1.0)`. A bare `asyncio.sleep` is for letting a few loop iterations pass, not for waiting on an outcome.
- **Many nodes in one process.** Cluster and remoting tests run several `ActorSystem`s on one event loop, each on its own port from `tests/util.py`'s `free_port()`, which hands out ports below the ephemeral range so they never collide with outbound connections. They shorten Pekko's intervals so a test takes milliseconds rather than seconds: copy `FAST` and `FAST_SHUTDOWN` from `tests/cluster/test_cluster.py`.
- **Typing is tested too.** `tests/typing_check.py` is checked by pyright and never run. To assert that pyright rejects something, such as a wrong message type to a ref, add the line with `# type: ignore[...]`; pyright reports an ignore it does not need, so the line fails the check if typing regresses.
- **Book snippets are tests.** Every Python block in `docs/book/` must run and pass pyright strict as a reader would assemble it. Put a chapter's tests in `examples/` when they can run as files, as `examples/testing/` does for chapter 12, so CI keeps them honest.

## Invariants (do not break)

- An actor processes one message at a time, including across awaits in its handler.
- System messages are processed before user messages.
- Stop order: children (await their `done`) → PostStop → mark terminated → notify watchers and parent. There are no awaits between "mark terminated" and the notifications.
- Nothing leaks: timers in `ActorCell._timers` and background tasks in `ActorCell._tasks` are cancelled in `_finish`.
- Behaviors are values. Handlers return the next behavior. State lives in closures or parameters, not in mutable fields.

## Following Apache Pekko

- Public API names follow Pekko's Scala DSL in snake_case.
- When you port a feature, follow the control flow of the Apache Pekko source function for function. Don't invent a simpler protocol. If you have to narrow something (for example by dropping multi-DC or roles), document the narrowing.
- Document every intentional deviation in [appendix B](docs/book/appendix-deviations.md), and new features in the book chapter for their layer.
- The core has no dependencies. Integrations that need a third-party package go in `ohra.ext` behind an optional extra.
- Distributed Data is out of scope. Use a single-writer actor (a singleton or a sharded entity) instead.

## Layout

`src/ohra/` is the core, Pekko's actor-typed module; subpackages follow Pekko's module split.
- `behavior.py`: behavior values and the `Behaviors` factory.
- `cell.py`: the runtime, including the message loop, interpretation, supervision, and lifecycle. Most work happens here.
- `context.py`: `ActorContext`, a thin facade over the cell.
- `ref.py`, `patterns.py`, `routers.py`, `system.py`, `signals.py`, `supervision.py`, `mailbox.py`, `result.py`, `receptionist.py`, `persistence.py`, `coordinated_shutdown.py` (Pekko's phases, run by `terminate()`), `discovery.py` (Pekko's ServiceDiscovery API), `scheduler.py` (`system.scheduler`: timers, backoff delays and ask timeouts run on it), `eventstream.py` (`system.event_stream`: dead letters, unhandled and dropped messages are published on it).
- `remote/`: `remoting.py` (transport, addresses, `RemoteRef`) and `serialization.py` (`@remotable`).
- `cluster/`: `core.py` (the membership daemon and the `Cluster` extension), its internals `gossip.py`, `reachability.py`, `vector_clock.py`, `failure_detector.py`, the split-brain resolver `sbr.py`, then `singleton.py` and `sharding/` (`shard.py`: the Shard actor and the single-node `Sharding`; `coordinator.py`; `region.py`; the `ClusterSharding` extension in `__init__.py`).
- `management/`: Pekko Management for Kubernetes: `http.py` (stdlib HTTP/1.1 server and client), `health.py`, the `Management` extension in `__init__.py`, `cluster_http.py` (read-only routes), `bootstrap.py` (cluster bootstrap), `kubernetes.py` (API discovery).
- `ext/`: `fastapi.py`, `postgres.py` (`PostgresStore`, durable state for clusters) and `shm.py`, integrations outside the core.
- `testkit/`: Pekko's actor-testkit-typed: `actor_testkit.py`, `probe.py` (`TestProbe`, `FishingOutcomes`), `manual_time.py` (`ManualTime`, `ExplicitlyTriggeredScheduler`), `settings.py`, and `pytest_plugin.py`, the only module that imports pytest.
- `docs/book/`: the documentation, one chapter per layer with Mermaid diagrams; README.md is only the front page.
- Public names are importable from `ohra`, and also from the subpackage they belong to (`ohra.cluster`, `ohra.cluster.sharding`, `ohra.remote`). The exception is the test kit, importable only from `ohra.testkit`, as Pekko ships it as a module of its own.

## Style

- Python 3.12 generics syntax (`class X[T]`, `type A = ...`).
- Frozen dataclasses for messages, and `match` for dispatch.
- Tests read like the Pekko docs: one behavior per test, asserting on observable events.
- Pyright strict quirks: there's no `type` statement inside functions, `Try` needs a nested `match` to be exhaustive, and `contextmanager`/`asynccontextmanager` functions are annotated `-> AsyncGenerator[...]`.

## Pull requests

- Keep each PR to one change, with tests that fail without it.
- Explain the why in the description. Link the Apache Pekko source or docs when you port behavior.
- By contributing, you agree that your contribution is licensed under Apache-2.0, the license of this project.
