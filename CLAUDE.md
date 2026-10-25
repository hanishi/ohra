# pekko-py: notes for Claude Code

Pekko Typed semantics on asyncio. Read the book in docs/book/ first, especially the runtime model (chapter 1) and the known deviations (appendix B).

## Commands
- `pytest`: must pass. Every test fails if any asyncio task outlives it (tests/conftest.py).
- `pyright`: strict mode must report 0 errors. tests/typing_check.py contains `# type: ignore` lines that must stay necessary; they assert that wrong message types are rejected.

## Invariants (do not break)
- An actor processes one message at a time, including across awaits in its handler.
- System messages are processed before user messages.
- Stop order: children (await their `done`) → PostStop → mark terminated → notify watchers and parent. There are no awaits between "mark terminated" and the notifications.
- Nothing leaks: timers in `ActorCell._timers` and background tasks in `ActorCell._tasks` are cancelled in `_finish`.
- Behaviors are values. Handlers return the next behavior. State lives in closures or parameters, not in mutable fields.
- Public API names follow Pekko's Scala DSL in snake_case. Document every intentional deviation in docs/book/appendix-deviations.md, and new features in the chapter for their layer.

## Layout
`src/pekko_py/` is the core, Pekko's actor-typed module; subpackages follow Pekko's module split.
- `behavior.py`: behavior values and the `Behaviors` factory.
- `cell.py`: the runtime, including the message loop, interpretation, supervision, and lifecycle. Most work happens here.
- `context.py`: `ActorContext`, a thin facade over the cell.
- `ref.py`, `patterns.py`, `routers.py`, `system.py`, `signals.py`, `supervision.py`, `mailbox.py`, `result.py`, `receptionist.py`, `persistence.py`, `coordinated_shutdown.py` (Pekko's phases, run by `terminate()`), `discovery.py` (Pekko's ServiceDiscovery API).
- `remote/`: `remoting.py` (transport, addresses, `RemoteRef`) and `serialization.py` (`@remotable`).
- `cluster/`: `core.py` (the membership daemon and the `Cluster` extension), its internals `gossip.py`, `reachability.py`, `vector_clock.py`, `failure_detector.py`, the split-brain resolver `sbr.py`, then `singleton.py` and `sharding/` (`shard.py`: the Shard actor and the single-node `Sharding`; `coordinator.py`; `region.py`; the `ClusterSharding` extension in `__init__.py`).
- `management/`: Pekko Management for Kubernetes: `http.py` (stdlib HTTP/1.1 server and client), `health.py`, the `PekkoManagement` extension in `__init__.py`, `cluster_http.py` (read-only routes), `bootstrap.py` (cluster bootstrap), `kubernetes.py` (API discovery).
- `ext/`: `fastapi.py` and `shm.py`, integrations that are not part of Pekko.
- `docs/book/`: the documentation, one chapter per layer with Mermaid diagrams; README.md is only the front page.
- Public names are importable from `pekko_py`, and also from the subpackage they belong to (`pekko_py.cluster`, `pekko_py.cluster.sharding`, `pekko_py.remote`).

## Style
- Python 3.12 generics syntax (`class X[T]`, `type A = ...`).
- Frozen dataclasses for messages, and `match` for dispatch.
- Tests read like the Pekko docs: one behavior per test, asserting on observable events.
