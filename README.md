# pekko-py

Pekko Typed actors on asyncio, for Python 3.12+. No dependencies.

An actor is a function from a message to the next behavior. Typed refs make wrong messages a type error under pyright strict. Everything runs on one event loop, so a handler can `await` without losing the guarantee that an actor handles one message at a time. numpy work goes to threads and comes back as a message.

```python
import asyncio
from dataclasses import dataclass
from pekko_py import ActorRef, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class Increment:
    by: int


@dataclass(frozen=True)
class GetTotal:
    reply_to: ActorRef[int]


type Command = Increment | GetTotal


def counter(total: int = 0) -> Behavior[Command]:
    def on_message(msg: Command) -> Behavior[Command]:
        match msg:
            case Increment(by):
                return counter(total + by)        # state is the next behavior
            case GetTotal(reply_to):
                reply_to.tell(total)
                return Behaviors.same

    return Behaviors.receive_message(on_message)


async def main() -> None:
    async with ActorSystem(counter(), "demo") as system:
        system.tell(Increment(5))
        print(await system.ask(GetTotal, timeout=1))  # 5


asyncio.run(main())
```

If you know Pekko or Akka Typed, the names are the same in snake_case, and the semantics are checked against the Pekko source. If you don't, the tour below is enough.

## Install

```
pip install -e .            # the library
pip install -e '.[numpy]'   # plus numpy, for the examples that use it
pip install -e '.[web]'     # plus FastAPI and uvicorn, for examples/fastapi_carts.py
pip install -e '.[dev]'     # plus pytest, pytest-asyncio, pyright
```

## Tour

### Messages and behaviors

Messages are frozen dataclasses, a command protocol is a union, and a handler dispatches with `match`. A handler returns the next behavior: a new one carrying new state, `Behaviors.same`, `Behaviors.stopped()`, or `Behaviors.unhandled`. Handlers may be `async def`; an awaiting handler keeps the actor busy, and the next message waits.

`Behaviors.setup(lambda ctx: ...)` runs once per actor and is where per-actor state and children are created. `Behaviors.receive(lambda ctx, msg: ...)` is `receive_message` with the context.

### Ask and reply

A request carries its own `reply_to`. From outside the system, `ask` returns the reply or raises `AskTimeoutError`. From inside an actor, `ctx.ask` delivers the reply to the asking actor as a message, wrapped in `Success` or `Failure`.

```python
from pekko_py import Failure, Success, Try


@dataclass(frozen=True)
class Fetch:
    url: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Fetched:
    result: Try[str]


def client(fetcher: ActorRef[Fetch]) -> Behavior[Fetched | str]:
    def on_message(ctx: ActorContext[Fetched | str], msg: Fetched | str) -> Behavior[Fetched | str]:
        match msg:
            case str(url):
                ctx.ask(fetcher, lambda r: Fetch(url, r), Fetched, timeout=2)
            case Fetched(result):
                match result:
                    case Success(body):
                        ctx.log.info("got %d bytes", len(body))
                    case Failure(e):
                        ctx.log.warning("fetch failed: %r", e)
        return Behaviors.same

    return Behaviors.receive(on_message)
```

The `make` argument is anything that turns a reply ref into the request: a lambda, or the message class itself when `reply_to` is its only field. Pyright infers the reply type from the request's `reply_to` field.

`ctx.pipe_to_self(awaitable, adapt)` does the same for any awaitable, and `ctx.message_adapter(f)` gives you a ref of another type that translates into your own protocol.

### Children, stopping, and DeathWatch

`ctx.spawn(behavior, name)` and `ctx.spawn_anonymous(behavior)` create children. `ctx.stop(child)` stops one. Stopping a parent stops its subtree first, then delivers `PostStop`. `ctx.watch(ref)` delivers a `Terminated` signal when that actor stops, or `ChildFailed` if it was a child that failed. Leaving `Terminated` unhandled fails the watcher with `DeathPactError`, as in Pekko.

```python
from pekko_py import Signal, Terminated


def parent(ctx: ActorContext[str]) -> Behavior[str]:
    worker = ctx.spawn(counter(), "worker")
    ctx.watch(worker)

    def on_signal(ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
        if isinstance(sig, Terminated):
            ctx.log.info("%s stopped", sig.ref.path)
            return Behaviors.stopped()
        return Behaviors.unhandled

    return Behaviors.receive_message(lambda _: Behaviors.same).receive_signal(on_signal)
```

To create actors from plain async code, run `SpawnProtocol.apply()` as the guardian and ask it: `await system.ask(lambda r: SpawnProtocol.Spawn(behavior, "name", r), timeout=1)`.

### Supervision

A failing handler stops the actor unless a supervisor says otherwise. `resume` keeps the state and drops the message. `restart` reruns the supervised behavior from its `setup`, stopping children first. `restart_with_backoff` waits with exponential delay and stashes messages meanwhile, which is the right choice for a `setup` that can fail.

```python
from pekko_py import SupervisorStrategy

resilient = Behaviors.supervise(counter()).on_failure(
    SupervisorStrategy.restart().with_limit(3, within=10.0), exc_type=ValueError
)
```

`PreRestart` is signalled before a restart. Both restart strategies cancel the actor's timers first, so timers started in the supervised `setup` come back and timers started from a handler do not.

### Timers and stash

`Behaviors.with_timers` gives a `TimerScheduler`. Starting a timer with a key already in use cancels the old one, and a message from a cancelled timer that already reached the mailbox is dropped. `Behaviors.with_stash` gives a `StashBuffer`; `buffer.unstash_all(behavior)` replays the stashed messages before any new one.

```python
from pekko_py import StashBuffer, TimerScheduler


@dataclass(frozen=True)
class Tick:
    pass


@dataclass(frozen=True)
class Ready:
    pass


type Cmd = Tick | Ready | str


def gated() -> Behavior[Cmd]:
    def with_timers(timers: TimerScheduler[Cmd]) -> Behavior[Cmd]:
        def with_stash(buffer: StashBuffer[Cmd]) -> Behavior[Cmd]:
            timers.start_timer_with_fixed_delay("tick", Tick(), 1.0)

            def waiting(msg: Cmd) -> Behavior[Cmd]:
                match msg:
                    case Ready():
                        return buffer.unstash_all(active)
                    case Tick():
                        return Behaviors.same
                    case _:
                        buffer.stash(msg)
                        return Behaviors.same

            active = Behaviors.receive_message(lambda m: (print(m), Behaviors.same)[1])
            return Behaviors.receive_message(waiting)

        return Behaviors.with_stash(1000, with_stash)

    return Behaviors.with_timers(with_timers)
```

### Routers, batching, and bounded mailboxes

`Routers.pool(n, behavior)` spawns `n` routees and forwards each message by round robin, at random, or by `with_consistent_hashing_routing(key)` so that equal keys reach the same routee. `batching(max_n, max_wait, handle)` is a behavior that collects messages and hands them to `handle` in batches, which is where `np.add.at` earns its keep. `MailboxSelector.bounded(capacity, overflow=...)` passed to `spawn` bounds a mailbox and drops the new message, drops the oldest, or raises in the sender.

### numpy and parallelism

A handler that blocks the event loop stalls every actor. Run numpy work with `ctx.pipe_to_self(asyncio.to_thread(fn, ...), adapt)` and stash until the result comes back. numpy releases the GIL, so eight such actors use eight cores while a timer keeps ticking on time; pure-Python work in threads gains nothing. For another process, `pekko_py.shm.SharedArray` hands over a large array as a three-field handle instead of a copy.

Messages are shared by reference and never copied. Keep them immutable: frozen dataclasses, and for arrays either copy before sending or set `arr.flags.writeable = False`.

## Pekko names

| Pekko | pekko-py |
|---|---|
| `Behaviors.receive / receiveMessage / receiveSignal / setup / same / stopped / unhandled / empty / ignore` | same names, snake_case; handlers may be `async def` |
| `ActorRef[-T]` | `ActorRef[T]`, contravariant, enforced by pyright strict |
| `sealed trait` + case classes | `type Cmd = A \| B` + frozen dataclasses + `match` |
| `ask`, `ctx.ask` | `await ask(ref, make, timeout)`, `ctx.ask(ref, make, adapt, timeout)` |
| `ctx.pipeToSelf(future)(Try => T)` | `ctx.pipe_to_self(awaitable, adapt)` with `Success` / `Failure` |
| `ctx.messageAdapter` | `ctx.message_adapter` |
| `spawn`, `spawnAnonymous`, `ctx.stop(child)` | same |
| `watch`, `watchWith`, `Terminated`, `ChildFailed`, `DeathPactException` | same; `DeathPactError` |
| `PreRestart`, `PostStop` | same |
| `SupervisorStrategy.resume / restart / stop / restartWithBackoff`, `withLimit`, `withResetBackoffAfter`, `withStashCapacity` | same, snake_case |
| `Behaviors.withTimers`, `TimerScheduler.startSingleTimer / startTimerWithFixedDelay / cancel / cancelAll / isTimerActive` | same, snake_case |
| `Behaviors.withStash`, `StashBuffer.stash / unstashAll / size / isEmpty / isFull`, `StashOverflowException` | same, snake_case; `StashOverflowError` |
| `MailboxSelector.bounded(capacity)` passed to `spawn` | same, plus `overflow="drop_new" \| "drop_old" \| "fail"` |
| `Routers.pool(n)(behavior)`, `withRoundRobinRouting / withRandomRouting / withConsistentHashingRouting` | `Routers.pool(n, behavior)`, same methods in snake_case |
| `SpawnProtocol()`, `SpawnProtocol.Spawn(behavior, name, props, replyTo)` | `SpawnProtocol.apply()`, `SpawnProtocol.Spawn(behavior, name, reply_to, mailbox=None)` |
| (implicit) | `Replyable[Res]`, the protocol a request satisfies by having a `reply_to: ActorRef[Res]` field |
| `system.ignoreRef` | `system.ignore_ref` |
| (none) | `batching(max_n, max_wait, handle)` |
| (none) | `pekko_py.shm.SharedArray`, a picklable handle to an array in shared memory |

## Runtime model

- One asyncio task per actor. System messages (stop, watch, death notifications, backoff timers) are processed before user messages.
- A behavior is a value, and one value can start many actors (`Routers.pool`, or spawning it twice). Anything captured in its closure is then shared by all of them. Create per-actor state inside `Behaviors.setup`, `with_timers` or `with_stash`, which run once per actor.
- A handler that awaits keeps the actor busy. Messages are serialized across awaits, which is the main reason to use actors on a single event loop. `test_async_handler_keeps_messages_serialized` pins this down.
- Stopping an actor stops its children first (depth-first), then delivers `PostStop` to the last running behavior, then runs the `Behaviors.stopped(callback)` callback, then notifies watchers and the parent. Pending user messages become dead letters (logger `pekko_py.dead_letters`).
- There is one `TimerScheduler` per actor, and a `restart` or `restart_with_backoff` cancels all of its timers before `PreRestart`, as Pekko's `RestartSupervisor` does. `resume` leaves them running.
- Background work started with `pipe_to_self` is cancelled when the actor stops. Every test asserts that no asyncio task outlives the system.
- Timer messages are validated when dequeued, so a message from a timer that was cancelled or restarted while it sat in the mailbox is dropped. `unstash_all` replays the stashed messages inside the current message's turn, before any new message.
- An idle actor costs about 5KB and 50µs to spawn. 100k actors fit, but `examples/population.py` shows why 16 shard actors holding struct-of-arrays beat 100k single-arm actors by hundreds of times on memory and query time.

## Known deviations from Pekko

- A plain `restart` whose setup fails stops the actor. Pekko does the same when restarts are unlimited, but retries while a `with_limit` budget remains. Use `restart_with_backoff` for setups that can fail.
- An exception in a message adapter or `pipe_to_self` adapt function is logged and the message is dropped. Pekko delivers a `MessageAdaptionFailure` signal, which fails the actor unless handled.
- If a handler fails partway through `unstash_all`, the unprocessed messages stay in the buffer as in Pekko, but `resume` and `PreRestart` see the behavior from before the unstash began. Pekko tracks the intermediate behavior reached by the last successful message.
- Exceptions in `PreRestart` and `PostStop` handlers are logged, not supervised.
- `ask` and `ctx.ask` check the reply type but not that the target accepts the request, since the reply type is read from the request's `reply_to` field and pyright cannot tie both to one type parameter. `tell` is fully checked. Pekko checks both.
- `start_timer_at_fixed_rate`, `StashBuffer.unstash(n)` and `Routers.group` are not implemented.
- Pekko's bounded mailbox only drops the new message. `overflow="drop_old"` and `overflow="fail"` (raise `MailboxOverflowError` in the sender) are additions. Adapted messages, timer messages and `pipe_to_self` results count against the capacity.
- `with_consistent_hashing_routing(mapping)` takes no virtual-nodes factor and routes with `hash(mapping(msg)) % size`, since a pool has a fixed size. The mapping may return any hashable.
- `batching` is not in Pekko. It flushes a pending batch in `PostStop` and keeps the batch in a closure-local list.
- Only local refs can be watched. A bridge to a JVM Pekko system over TCP is possible but deferred until there is a caller for it.
- `SharedArray` ownership is a convention: whoever holds the handle owns the segment, sending it transfers ownership, and the owner calls `unlink()` once. A process that crashes while owning a segment leaks it. On Python 3.13+ the resource tracker is bypassed; on 3.12 expect its warnings.

## Examples

| File | Shows |
|---|---|
| `examples/counter.py` | the functional style, `ask`, supervision with `resume` |
| `examples/chatroom.py` | the ChatRoom from the Pekko docs: sessions as children, narrowed refs, DeathWatch |
| `examples/database_client.py` | backoff restarts of a failing setup, a stash that outlives restarts, timers, `ctx.ask` |
| `examples/interaction_patterns/` | one runnable file per pattern in Pekko's [Interaction Patterns](https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html) page: fire and forget, request-response, adapted response, ask between actors, ask from outside, generic response wrapper, ignoring replies, pipe to self, per-session child, aggregator, tail chopping, scheduling to self |
| `examples/fastapi_carts.py` | a FastAPI service over actors: one cart actor per id, spawned on demand and stopped when idle, handlers that `ask` the manager; run with `uv run --extra web uvicorn examples.fastapi_carts:app` |
| `examples/fastapi_chatroom.py` | the chat room over WebSockets: one connection actor per socket with an async handler that writes to it, rooms created on demand, and DeathWatch turning a closed socket into a "left" notice; serves a browser page at `/` |
| `examples/fastapi_online_learning.py` | a feedback loop behind FastAPI: predictions micro-batched into one numpy call, outcomes posted back, a learner actor taking gradient steps in a thread and hot-swapping the model; serves a playground page at `/`, and `python examples/fastapi_online_learning.py` runs the server with a demo client |
| `examples/bandit.py` | Thompson sampling over sharded arms: Beta posteriors as arrays, one vectorized draw per shard, `ctx.ask` fan-out, a progress timer |
| `examples/compute.py` | numpy in threads via `pipe_to_self(asyncio.to_thread(...))`, with a latency probe |
| `examples/batching.py` | `batching` throughput and p99 latency curves |
| `examples/population.py` | 100k single-arm actors against 16 shards behind `Routers.pool` |
| `examples/shared_memory.py` | `SharedArray` against pickling for a 400MB array |

The last four need numpy: `uv run --extra numpy python examples/bandit.py`.

## Development

```
pytest      # every test fails if any asyncio task outlives it
pyright     # strict; tests/typing_check.py asserts type errors that must occur
```

## License

Apache-2.0, the same as Pekko. The chat room and interaction-pattern examples are derived from the Pekko documentation; see [NOTICE](NOTICE).

pekko-py is not affiliated with or endorsed by the Apache Software Foundation or the Apache Pekko project.
