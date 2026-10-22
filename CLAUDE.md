# pekko-py: notes for Claude Code

Pekko Typed semantics on asyncio. Read README.md first, especially the runtime model and the known deviations.

## Commands
- `pytest`: must pass. Every test fails if any asyncio task outlives it (tests/conftest.py).
- `pyright`: strict mode must report 0 errors. tests/typing_check.py contains `# type: ignore` lines that must stay necessary; they assert that wrong message types are rejected.

## Invariants (do not break)
- An actor processes one message at a time, including across awaits in its handler.
- System messages are processed before user messages.
- Stop order: children (await their `done`) → PostStop → mark terminated → notify watchers and parent. There are no awaits between "mark terminated" and the notifications.
- Nothing leaks: timers in `ActorCell._timers` and background tasks in `ActorCell._tasks` are cancelled in `_finish`.
- Behaviors are values. Handlers return the next behavior. State lives in closures or parameters, not in mutable fields.
- Public API names follow Pekko's Scala DSL in snake_case. Document every intentional deviation in the README.

## Layout
- `behavior.py`: behavior values and the `Behaviors` factory.
- `cell.py`: the runtime, including the message loop, interpretation, supervision, and lifecycle. Most work happens here.
- `context.py`: `ActorContext`, a thin facade over the cell.
- `ref.py`, `patterns.py`, `system.py`, `signals.py`, `supervision.py`, `result.py`.

## Style
- Python 3.12 generics syntax (`class X[T]`, `type A = ...`).
- Frozen dataclasses for messages, and `match` for dispatch.
- Tests read like the Pekko docs: one behavior per test, asserting on observable events.
