# Remoting design (cluster layer 1)

Two or more pekko-py processes send messages to each other's actors by path, and `ask` works unchanged.
This is the first of the cluster layers: no membership, no cluster-wide receptionist, no remote watch,
no JVM (the JVM bridge spec stays separate: that protocol addresses by name because the JVM cannot
resolve pekko-py paths). Modelled on Apache Pekko's Artery, read from the local source, and narrowed.

## Addressing

- `ActorSystem(guardian, name, address=("10.0.0.1", 2552))` gives the system a `UniqueAddress(host, port,
  uid)`, uid a random 63-bit int per incarnation, as Artery. Paths become `pekko://a@10.0.0.1:2552/user/...`.
  Without `address` paths stay `pekko://a/user/...` and the system is unreachable remotely.
- `system.resolve(path: str) -> ActorRef[Any]`: a path on this system resolves to the local ref by walking
  the actor tree (and the temp table, below); any other `pekko://name@host:port/...` path returns a
  `RemoteRef`. Unresolvable local paths raise `LookupError`. Pekko's name for this is `ActorRefResolver`.
- `RemoteRef[T](ActorRef[T])`: `path`; `tell` serializes and sends; `offer` awaits the connection's
  write backpressure; equality and hash by path.

## Wire protocol

Length-prefixed frames: 4-byte big-endian length, then UTF-8 JSON. Frames over 16 MiB close the
connection. Each node opens its own outbound connection to a peer on first send and only reads from
connections it accepted, so a request and its reply travel on two connections, as in Artery. A node must
therefore listen (``async with`` does) and advertise a host its peers can reach: ``address=`` is both the
bind and the advertised address, and ``127.0.0.1`` and ``localhost`` are different authorities.

| kind    | fields                                 | meaning                                         |
|---------|----------------------------------------|-------------------------------------------------|
| `hello` | `address` (`{host, port, uid, name}`)  | first frame in each direction                   |
| `msg`   | `to`, `from` (path or null), `message` | deliver `message` to the actor at path `to`     |

- Receiving side: `to` is resolved locally; failure is a dead letter (`system.dead_letter`), no error frame.
- A peer whose `hello` carries a new uid for a known address is logged as restarted and served. Artery
  quarantines; quarantine protects remote watch and system-message ordering, neither of which exists here.
- Sending to a node with no live connection queues frames (bounded, 1000) while connecting; if the connect
  fails the queue is dead-lettered and the association is dropped so the next send retries.

## Serialization (`pekko_py.serialization`)

- `@remotable` on a frozen dataclass, or `Serialization.register(cls, name=None)`, makes the type sendable.
  The manifest is the class name unless `name` is given; a second registration under the same manifest
  raises `SerializationError` unless it is the same class.
- Encoding is self-describing JSON: a registered dataclass is `{"$type": manifest, <field>: ...}`, an
  `ActorRef` is `{"$ref": path}`, `None`/bool/int/float/str pass through, list and tuple become lists,
  dict with str keys becomes an object. Anything else, including an unregistered dataclass, raises
  `SerializationError` in the sender at `tell`.
- Decoding needs no type hints: `$type` builds the dataclass, `$ref` resolves through `system.resolve`,
  lists stay lists (a tuple field receives a list; dataclass equality still holds for list-typed fields,
  and tuple-typed fields are rebuilt as tuples from the dataclass field annotation when it names `tuple`).
- Refs by kind: `LocalRef`, `RemoteRef`, the system itself and `ignore_ref` encode as their own path. An
  `EntityRef` encodes as `{"$entity": id, "$region": path}` and is rebuilt on the other node. Any other ref
  (ask's `PromiseRef`, `FunctionRef` adapters) is registered in the sending system's temp table under
  `pekko://name@host:port/temp/$N`, weakly held, and encoded as that path. `ask` holds its reply ref until
  it completes, and an actor keeps its `message_adapter` refs for its lifetime, so both stay resolvable
  exactly as long as they can be replied to. That is how `ask` crosses nodes with no ask/reply frame kinds.
- Out of scope: numpy arrays, bytes, enums, datetimes, custom codecs.

## Runtime (`pekko_py.remote`)

- `system.remoting` exists when `address` is given. `await system.remoting.listen()` binds the listener;
  `ActorSystem.__aenter__` calls it. `terminate` closes the listener and every connection; the reader
  tasks are tracked so nothing outlives the system (conftest).
- Inbound: read frames, decode, `system.resolve(to).tell(message)`. Decode or resolve failure: dead letter
  with the raw frame logged at debug.
- Outbound: `Association` per peer address with a writer, a bounded queue while connecting, and `offer`
  awaiting `drain()`.

## Invariants

- A `tell` to a `RemoteRef` never blocks the caller and never raises for network reasons; it raises only
  `SerializationError` for an unsendable message. `offer` awaits backpressure.
- Messages from one sender to one recipient over one connection arrive in send order. No ordering across
  connections and no delivery guarantee: TCP loss on disconnect is a dead letter on the sender if the queue
  was not yet flushed, or silent if the peer died, as in Pekko (at-most-once).
- Nothing leaks: listener, connections, reader tasks and temp refs all go with the system.
- Security: the listener accepts any peer. Private network or a tunnel; TLS is out of scope and the README
  says so.

## Deviations to document

`address=` and `system.resolve` instead of config and `ActorRefResolver`; JSON with `$type`/`$ref` instead
of Pekko's serializer ids and manifests; unsendable message raises at `tell` instead of log-and-drop; a
restarted peer is served instead of quarantined; no remote watch, no system-message delivery guarantees.

## Example and docs

`examples/remoting/greeter_node.py` (listens on 2552, `greeter` under `/user`) and `client_node.py`
(resolves `pekko://greeter@127.0.0.1:2552/user/greeter`, tells, asks, prints). README: a "Remoting"
Tour subsection, names-table rows, deviations, examples-table rows, and the security sentence.
