# Receptionist design (cluster layer 3)

Apache Pekko's Receptionist, read from `actor-typed/.../receptionist/` and
`cluster-typed/.../receptionist/ClusterReceptionist.scala` in the local Pekko source: a service registry where
actors register under a `ServiceKey` and anyone finds or subscribes to the set of refs under that key. The API and
the local semantics follow the Scala code. The one narrowing is replication: Pekko replicates the registry with
distributed data (an `ORMultiMap` through the `Replicator`), which pekko-py does not have; here each node owns its
own row of the registry and pushes it to the other members, as designed with the user on 2026-10-24.

## API (`pekko_py/receptionist.py`)

- `ServiceKey[T](id: str)`: frozen dataclass, equal and hashed by `id`; `T` is the service's message type and is
  static only (it never crosses the wire).
- Commands (`Command` union): `Register(key, service, reply_to=None)`, `Deregister(key, service, reply_to=None)`,
  `Find(key, reply_to)`, `Subscribe(key, subscriber)`.
- Replies: `Registered(key, service_instance)`, `Deregistered(key, service_instance)`,
  `Listing[T](key, service_instances: frozenset[ActorRef[T]], all_service_instances: frozenset[ActorRef[T]],
  services_were_added_or_removed: bool)`.
- `Receptionist(system)` is the extension, created on first use like `Cluster(system)`, with `ref:
  ActorRef[Command]`. The actor lives at `pekko://name[@host:port]/system/receptionist`. Every `ActorSystem` now
  has the `/system` root (today only cluster systems do); the shutdown order user root -> system root -> remoting
  is unchanged.
- One actor serves both cases: a plain system gets the local semantics below; a system with `cluster=` adds the
  replication section. Pekko has two classes (`LocalReceptionist`, `ClusterReceptionist`) chosen at start; the
  behaviour is the same.

## Local semantics (Pekko's `LocalReceptionist`)

- `Register`: only a local actor ref (`LocalRef`) may register; anything else is logged and ignored, as
  `ClusterReceptionist` does for non-local refs. The receptionist watches the service, adds the binding, replies
  `Registered` when `reply_to` is given, and notifies the key's subscribers. Registering the same ref under the
  same key twice is a no-op.
- `Deregister`: removes that one binding, replies `Deregistered` when asked, notifies subscribers of that key. The
  service is unwatched once it has no bindings left.
- A registered service that stops is removed from every key it was under; the affected keys' subscribers are
  notified.
- `Find`: replies one `Listing` for the key. `Subscribe`: the subscriber is watched, receives the current
  `Listing` immediately, then one `Listing` per change of that key; a subscriber that stops is dropped. A
  subscriber may subscribe to several keys.
- `services_were_added_or_removed` is `True` for every local change. Without a cluster, `service_instances` and
  `all_service_instances` are the same set.

## Replication (the narrowing of `ClusterReceptionist`)

- State per node: `rows: dict[UniqueAddress, Row]` where `Row(version: int, bindings: frozenset[tuple[str,
  ActorRef]])` holds (key id, ref). The node's own row is the only one it writes; every local change bumps the
  own version by one.
- Wire message, `@remotable`: `ReceptionistRow(owner: UniqueAddress, version: int, bindings: tuple[tuple[str,
  ActorRef], ...])`, sent to a peer's `/system/receptionist`. A receiver stores it only if `version` is higher
  than the stored row's for that owner; equal or older rows are ignored. Refs decode to `RemoteRef`s on the
  receiving node, so a listing hands out refs that `tell` across the wire.
- The own row is pushed to every reachable member other than self (a) after each local change, (b) when a
  `MemberEvent` names a node not yet in the known set, so two nodes exchange rows as soon as each learns of the
  other, and (c) on a timer every `ClusterSettings.receptionist_resend_interval` (default 1.0 s), as anti-entropy
  for rows lost while a connection was opening. Nothing else is sent.
- Membership: the receptionist subscribes to `MemberEvent` and `ReachabilityEvent` through
  `Cluster(system).subscriptions`. The known set is the current members' unique addresses.
  - `MemberRemoved`: drop that owner's row and notify subscribers of the keys it had, with
    `services_were_added_or_removed=True`. A restarted node has a new uid, hence a new row; the old row leaves
    with the old member.
  - `UnreachableMember` / `ReachableMember`: no row changes; recompute listings and notify subscribers of the
    keys that node has bindings under, with `services_were_added_or_removed=False`.
  - A row received from a node that is not a member yet is stored and hidden until the member appears.
- Listings on a cluster node: `all_service_instances` is every binding under the key from members (including
  unreachable and Down ones); `service_instances` is the subset whose owner is self or is reachable in
  `Cluster(system).state`, as Pekko's `activeActorRefsFor`.
- A node without a cluster, or whose cluster has no other members, behaves exactly as the local section.

## Invariants

- One writer per row: a node's bindings are changed only by that node; others only replace whole rows by version.
- A binding never outlives its service on the owning node (watch), nor its node in the cluster (`MemberRemoved`).
- Subscribers of a key are notified once per change that touches that key, local or replicated, as Pekko's
  `notifySubscribers`; keys a change does not touch are not notified.
- Nothing leaks: the resend timer and the watches are the actor's; the actor stops with the system root.

## Deviations to document

Owner-row replication in place of distributed data: no CRDT, no tombstones, no `distributed-key-count` sharding
of keys over several ddata entries, no write-consistency or pruning settings; one `receptionist_resend_interval`
on `ClusterSettings` instead. One actor for both the local and the cluster case. `Listing[T]` is a generic
dataclass with fields rather than Pekko's untyped `Listing` with `serviceInstances(key)` accessors.

## Tests

Local, on a plain system: register then find; subscribe receives the current listing then each change;
deregister; a stopped service disappears; a stopped subscriber is dropped (no dead letters); a remote or
function ref is rejected; two keys for one service. Cluster, in one process at the millisecond settings used by
`tests/cluster/test_cluster.py`: register on A and find on B returns a `RemoteRef`; a subscriber on B sees A's
service appear, then disappear when the service stops, when A leaves, and when A crashes and is downed; an
unreachable A moves its service from `service_instances` to `all_service_instances` with the flag `False`, and
back when reachable; a restarted A replaces its old row; a node that joins late gets the existing rows within one
exchange; every test under the conftest leak check.

## Example and docs

`examples/cluster/node.py` grows a service: each node registers a greeter under `ServiceKey[Greet]("greeter")`,
subscribes, and prints the listing as nodes come and go; `uv run python examples/cluster/node.py one 2551 2551`
then more nodes. README: a "Receptionist" Tour subsection after Cluster, names-table rows
(`Receptionist(system).ref ! Register(key, ref)` -> `Receptionist(system).ref.tell(Register(key, ref))`,
`ServiceKey[T]("id")` -> `ServiceKey[T]("id")`, `Listing.serviceInstances(key)` -> `listing.service_instances`),
the deviations above, examples row.
