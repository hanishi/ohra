# 4. Remoting

Give a system `address=(host, port)` and its actor paths become `ohra://name@host:port/user/...`. `system.resolve(path)` returns a `LocalRef` when the path is on this node and a `RemoteRef` otherwise. Code that tells a ref cannot see the difference, which is the whole point.

- **Identity.** A node is a `UniqueAddress`: host, port and a random uid minted per incarnation. A restarted node on the same port has a new uid, which is how the cluster tells a new process from the old one.
- **Messages.** A type crosses the wire once it carries `@remotable`, on a frozen dataclass. It is encoded as JSON with a `$type` manifest. A ref inside a message is encoded as its path and resolved on arrival. A function ref, such as an `ask` reply address, is registered under `/temp/` so the other side can answer it.
- **Connections.** The first send to a peer opens one outbound TCP connection, the association, and queues up to 1000 frames until it is up. Each frame is a length-prefixed JSON object. The receiving side resolves the `to` path and tells the local actor. A path nobody owns becomes a dead letter on the receiver.
- **Failure.** `tell` never blocks and never raises for network reasons. A connection that fails turns its queued messages into dead letters, and the next send opens a fresh one. A message that cannot be serialized raises at the sender, before anything touches the network.

Remoting alone has no membership, no remote death watch and no quarantine. Those belong to the cluster, which builds them out of heartbeats over these same connections.

## Using it

Give a system an address and its actors are reachable from other ohra processes by path. `system.resolve(path)` returns a `RemoteRef`, an `ActorRef` whose `tell` serializes the message and sends it over a TCP connection opened on first use. `ask` works unchanged, because the reply ref crosses the wire as a path too. A message type is sendable once it is marked `@remotable`, which derives a JSON codec from the frozen dataclass; an `ActorRef` field becomes a path, and nested or union-typed fields work because the encoding is self-describing.

```python
from ohra import remotable


@remotable
@dataclass(frozen=True)
class Greeted:
    text: str


@remotable
@dataclass(frozen=True)
class Greet:
    name: str
    reply_to: ActorRef[Greeted]


async def client() -> None:
    async with ActorSystem(Behaviors.ignore, "client", address=("127.0.0.1", 2553)) as system:
        greeter: ActorRef[Greet] = system.resolve("ohra://greeter@127.0.0.1:2552/user")
        print(await ask(greeter, lambda r: Greet("pekko", r), timeout=3))
```

A `tell` to a node that is down never blocks or raises; once the connection attempt fails the message is a dead letter, as in Pekko. Sending an unregistered type, or a message over 16 MiB, raises `SerializationError` at `tell`. A message for a path nobody owns, or one that fails to decode, is a dead letter on the receiving node and the connection stays up. Replies travel on the replying node's own connection back, so the asking side must be listening too (`async with` does that) and `address=` must be a host its peers can reach: it is both the bind and the advertised address, and `127.0.0.1` and `localhost` are different authorities. Message adapters are kept for the actor's lifetime, so create them in `setup` rather than per message. Remoting alone has no membership and no remote `watch`, and a peer that restarts is served rather than quarantined; membership is the next section. The listener accepts any peer, so run it on a private network or behind a tunnel; TLS is out of scope, as it is for Apache Pekko's Artery without TLS configured.

---

[← Persistence](03-persistence.md) · [Contents](README.md) · [Cluster membership →](05-cluster.md)
