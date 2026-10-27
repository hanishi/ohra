# Contributing to ohra

ohra brings Apache Pekko Typed semantics to asyncio. Start with [the book](docs/book/README.md), especially [the runtime model](docs/book/01-actors.md) in chapter 1 and [the known deviations](docs/book/appendix-deviations.md) in appendix B.

Bug reports, fixes, docs and new ports of Apache Pekko features are all welcome. Before you start anything large, open an issue so we can agree on the scope.

## Setup

```
uv sync --all-extras        # or: pip install -e '.[dev,numpy,web,postgres]'
git config blame.ignoreRevsFile .git-blame-ignore-revs   # git blame skips the PEP 8 reformat
```

## Checks

Every PR has to pass these checks, and CI runs them on Python 3.12 and 3.13:

```
uv run ruff format   # PEP 8 layout, lines up to 99 characters
uv run ruff check    # PEP 8 style and naming
uv run pytest        # every test fails if any asyncio task outlives it (tests/conftest.py)
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
- `ref.py`, `patterns.py`, `routers.py`, `system.py`, `signals.py`, `supervision.py`, `mailbox.py`, `result.py`, `receptionist.py`, `persistence.py`, `coordinated_shutdown.py` (Pekko's phases, run by `terminate()`), `discovery.py` (Pekko's ServiceDiscovery API).
- `remote/`: `remoting.py` (transport, addresses, `RemoteRef`) and `serialization.py` (`@remotable`).
- `cluster/`: `core.py` (the membership daemon and the `Cluster` extension), its internals `gossip.py`, `reachability.py`, `vector_clock.py`, `failure_detector.py`, the split-brain resolver `sbr.py`, then `singleton.py` and `sharding/` (`shard.py`: the Shard actor and the single-node `Sharding`; `coordinator.py`; `region.py`; the `ClusterSharding` extension in `__init__.py`).
- `management/`: Pekko Management for Kubernetes: `http.py` (stdlib HTTP/1.1 server and client), `health.py`, the `Management` extension in `__init__.py`, `cluster_http.py` (read-only routes), `bootstrap.py` (cluster bootstrap), `kubernetes.py` (API discovery).
- `ext/`: `fastapi.py`, `postgres.py` (`PostgresStore`, durable state for clusters) and `shm.py`, integrations outside the core.
- `docs/book/`: the documentation, one chapter per layer with Mermaid diagrams; README.md is only the front page.
- Public names are importable from `ohra`, and also from the subpackage they belong to (`ohra.cluster`, `ohra.cluster.sharding`, `ohra.remote`).

## Style

- Python 3.12 generics syntax (`class X[T]`, `type A = ...`).
- Frozen dataclasses for messages, and `match` for dispatch.
- Tests read like the Pekko docs: one behavior per test, asserting on observable events.
- Pyright strict quirks: there's no `type` statement inside functions, `Try` needs a nested `match` to be exhaustive, and `contextmanager`/`asynccontextmanager` functions are annotated `-> AsyncGenerator[...]`.

## Pull requests

- Keep each PR to one change, with tests that fail without it.
- Explain the why in the description. Link the Apache Pekko source or docs when you port behavior.
- By contributing, you agree that your contribution is licensed under Apache-2.0, the license of this project.
