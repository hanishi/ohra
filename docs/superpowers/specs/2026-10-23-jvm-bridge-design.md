# JVM bridge design

Talk to Apache Pekko Typed actors on a JVM from pekko-py, and let them talk back, over a plain TCP connection carrying JSON frames. The protocol and the Scala side come from the `pekko-python-bridge` prototype in `~/Downloads`; this spec replaces its Python side with a pekko-py actor and keeps the rest.

## Goals

- `tell` and `ask` from Python to named JVM actors, and from the JVM to named Python actors.
- Core stays dependency-free: the Python side is stdlib `asyncio` streams and `json`.
- The bridge is an ordinary pekko-py actor. It is spawned, supervised, watched and stopped like any other, and leaks nothing.
- A runnable JVM peer in the repo, started with one `scala-cli run` command.

## Non-goals

- DeathWatch across the boundary, cluster membership, reconnection inside the bridge (a parent restarts it), typed payload codecs (payloads are JSON values), TLS, and a Python-side listener (Python connects, the JVM listens).

## Wire protocol

Each frame is a 4-byte big-endian length followed by that many bytes of UTF-8 JSON. Frames above 16 MiB close the connection. The protocol is symmetric.

| kind    | fields                   | meaning                                             |
|---------|--------------------------|-----------------------------------------------------|
| `tell`  | `to`, `payload`          | fire and forget                                     |
| `ask`   | `to`, `id`, `payload`    | request; the peer answers with `reply` or `error`   |
| `reply` | `id`, `payload`          | answer to an `ask`                                  |
| `error` | `id`, `error`            | failed `ask`: unknown actor, exception, or timeout  |

`to` is a name registered on the receiving side, not an actor path. `id` is any string unique to the sender; Python uses `uuid4`.

## Python API: `pekko_py.bridge`

```python
type Json = None | bool | int | float | str | list[Json] | dict[str, Json]


class JvmPeer:
    """One connection to a JVM. Spawn it; message it like the Scala PythonPeer."""

    type Command = Tell | Ask

    @dataclass(frozen=True)
    class Tell:
        to: str
        payload: Json

    @dataclass(frozen=True)
    class Ask:
        to: str
        payload: Json
        reply_to: ActorRef[Try[Json]]

    @staticmethod
    def connect(
        host: str, port: int, exposed: dict[str, ActorRef[FromJvm]], ask_timeout: float = 5.0
    ) -> Behavior[JvmPeer.Command]: ...


@dataclass(frozen=True)
class FromJvm:
    """What an exposed Python actor receives. reply_to is system.ignore_ref for a tell."""

    payload: Json
    reply_to: ActorRef[Json]
```

Usage from a parent actor:

```python
peer = ctx.spawn(JvmPeer.connect("127.0.0.1", 26520, {"counter": counter}), "jvm")
peer.tell(JvmPeer.Tell("greeter", {"name": "fire-and-forget"}))
ctx.ask(peer, lambda r: JvmPeer.Ask("greeter", {"name": "pekko"}, r), Answered, timeout=3)
```

`Ask.reply_to` carries `Try[Json]` so a JVM-side `error` frame arrives as `Failure(JvmError(message))` and a `reply` as `Success(payload)`. A missing answer surfaces as the caller's own `AskTimeoutError`, the same as for any local ask.

`JvmError(Exception)` is the only new exception type.

## Behavior of the peer actor

Setup (async) opens the connection with `asyncio.open_connection`. A failed connect raises, so a parent that wants retries wraps the peer in `restart_with_backoff`.

Setup then starts the read loop with `ctx.pipe_to_self(read_loop(), adapt)`. The loop reads frames and `tell`s `ctx.self` an internal `_Inbound(frame)` per frame; it returns on EOF or a protocol error, and `adapt` turns the result into `_Disconnected`. Using `pipe_to_self` means the loop is a tracked background task that `_finish` cancels, so nothing leaks.

Message handling, with pending asks kept as a `dict[str, ActorRef[Try[Json]]]` in the behavior's closure:

- `Tell(to, payload)`: write a `tell` frame.
- `Ask(to, payload, reply_to)`: pick an id, store `reply_to`, write an `ask` frame, and start a single timer keyed by the id for `ask_timeout`. On `_Expire(id)` drop the entry; the caller's own timeout reports the failure.
- `_Inbound` with kind `tell`: look up `to` in `exposed`; deliver `FromJvm(payload, system.ignore_ref)` or log a warning for an unknown name.
- `_Inbound` with kind `ask`: deliver `FromJvm(payload, reply_ref)` where `reply_ref` is a `FunctionRef` that tells the peer `_Reply(id, value)`. Unknown name: write an `error` frame. The JVM side enforces its own timeout, so a Python actor that never replies is not an error here.
- `_Reply(id, value)`: write a `reply` frame. A second reply for the same id is a dead letter, since the id is only ever written once and the `FunctionRef` records that it has fired.
- `_Inbound` with kind `reply` or `error`: pop the pending ref and `tell` it `Success(payload)` or `Failure(JvmError(message))`. Unknown id: debug log, since the caller probably timed out.
- `_Disconnected`: return `Behaviors.stopped()`. Pending callers time out on their own. The parent sees `Terminated` or `ChildFailed` and decides whether to respawn.

Writes happen inside the handler as `writer.write(frame)` followed by `await writer.drain()`. Because the actor handles one message at a time, a slow socket slows the peer actor, and its mailbox is the buffer. A caller that wants backpressure uses `offer` on the peer with a bounded mailbox, as anywhere else.

`PostStop` closes the writer.

## JVM side: `examples/jvm_bridge/PekkoSide.scala`

The prototype's `PythonBridge.scala` and `Main.scala` combined into one scala-cli file with `//> using dep` lines for `pekko-actor-typed`, `pekko-stream`, `pekko-stream-typed`, `upickle` and `logback-classic`. Behavior is unchanged: `PythonBridge.bind` accepts connections, one `PythonPeer` actor per connection registered in the Receptionist, a `Greeter` exposed under `greeter`, and a watcher that tells and asks Python's `counter` whenever a Python peer connects.

`examples/jvm_bridge/python_side.py` is the pekko-py mirror of the prototype's `example.py`: a `counter` actor exposed to the JVM, one tell and one ask to `greeter`, one ask to an unknown name to show the `Failure`, then shutdown.

`examples/jvm_bridge/README.md` says how to run both and shows the expected output.

## Testing

`tests/test_bridge.py` runs a fake JVM peer in-process with `asyncio.start_server`, speaking the protocol, so the suite needs no JDK. Tests, each a single observable behavior:

- a `Tell` arrives at the fake peer as a `tell` frame with the given name and payload;
- an `Ask` round trip: the fake peer answers `reply` and the asker receives `Success(payload)`;
- an `error` frame produces `Failure(JvmError(...))` with the message;
- an inbound `tell` reaches the exposed actor with `reply_to` equal to `system.ignore_ref`;
- an inbound `ask` reaches the exposed actor, and its `reply_to.tell(value)` produces a `reply` frame with the same id;
- an inbound `ask` for an unknown name produces an `error` frame;
- the fake peer closing the socket stops the peer actor, observed as `Terminated` by a watching parent;
- a refused connection fails setup, observed as `ChildFailed`.

The conftest leak check covers the read loop and the socket.

`tests/typing_check.py` gains a line asserting that `JvmPeer.Ask` with a `reply_to: ActorRef[Json]` (not `Try[Json]`) is rejected.

## Documentation

README: a "JVM bridge" subsection in the Tour, a names-table row mapping the Scala `PythonPeer.Tell / Ask` to `JvmPeer.Tell / Ask`, the deviations entry about the deferred bridge replaced by what the bridge does and does not do, and an Examples-table row.

NOTICE: if the prototype's Scala code was written by someone other than you, it needs an attribution line. Please confirm its provenance during spec review.
