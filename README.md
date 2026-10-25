# pekko-py

**pekko-py is an independent project. It is not affiliated with or endorsed by the Apache Software Foundation or the Apache Pekko project.**

Apache Pekko Typed actors on asyncio, for Python 3.12+. No dependencies.

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
    @Behaviors.receive_message
    def on_message(msg: Command) -> Behavior[Command]:
        match msg:
            case Increment(by):
                return counter(total + by)        # state is the next behavior
            case GetTotal(reply_to):
                reply_to.tell(total)
                return Behaviors.same

    return on_message


async def main() -> None:
    async with ActorSystem(counter(), "demo") as system:
        system.tell(Increment(5))
        print(await system.ask(GetTotal, timeout=1))  # 5


asyncio.run(main())
```

If you know Apache Pekko or Akka Typed, the names are the same in snake_case, and the semantics are checked against the Apache Pekko source. If you don't, the tour below is enough.

## Install

```
pip install -e .            # the library
pip install -e '.[numpy]'   # plus numpy, for the examples that use it
pip install -e '.[web]'     # plus FastAPI and uvicorn, for pekko_py.ext.fastapi and the fastapi_* examples
pip install -e '.[dev]'     # plus pytest, pytest-asyncio, pyright, httpx
```

## Tour

### Messages and behaviors

Messages are frozen dataclasses, a command protocol is a union, and a handler dispatches with `match`. A handler returns the next behavior: a new one carrying new state, `Behaviors.same`, `Behaviors.stopped()`, or `Behaviors.unhandled`. Handlers may be `async def`; an awaiting handler keeps the actor busy, and the next message waits.

`Behaviors.setup`, `receive`, `receive_message` and `receive_signal` each take one function and return a behavior, so they work as decorators. The decorator form and the call form are the same thing and can be mixed freely: `@Behaviors.receive_message` above `def on_message` is `Behaviors.receive_message(on_message)`. The decorator form is new in pekko-py, since Apache Pekko has no equivalent; the snippets below and the examples use it. `setup` runs once per actor and is where per-actor state and children are created. `receive` is `receive_message` with the context.

### Ask and reply

A request carries its own `reply_to`. From outside the system, `ask` returns the reply or raises `AskTimeoutError`. From inside an actor, `ctx.ask` delivers the reply to the asking actor as a message, wrapped in `Success` or `Failure`.

```python
from pekko_py import ActorContext, Failure, Success, Try


@dataclass(frozen=True)
class Fetch:
    url: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Fetched:
    result: Try[str]


def client(fetcher: ActorRef[Fetch]) -> Behavior[Fetched | str]:
    @Behaviors.receive
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

    return on_message
```

The `make` argument is anything that turns a reply ref into the request: a lambda, or the message class itself when `reply_to` is its only field. Pyright infers the reply type from the request's `reply_to` field.

`ctx.pipe_to_self(awaitable, adapt)` does the same for any awaitable, and `ctx.message_adapter(f)` gives you a ref of another type that translates into your own protocol.

### Children, stopping, and DeathWatch

`ctx.spawn(behavior, name)` and `ctx.spawn_anonymous(behavior)` create children. `ctx.stop(child)` stops one. Stopping a parent stops its subtree first, then delivers `PostStop`. `ctx.watch(ref)` delivers a `Terminated` signal when that actor stops, or `ChildFailed` if it was a child that failed. Leaving `Terminated` unhandled fails the watcher with `DeathPactError`, as in Pekko.

```python
from pekko_py import Signal, Terminated


@Behaviors.setup
def parent(ctx: ActorContext[str]) -> Behavior[str]:
    worker = ctx.spawn(counter(), "worker")
    ctx.watch(worker)

    @Behaviors.receive_signal
    def on_signal(ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
        if isinstance(sig, Terminated):
            ctx.log.info("%s stopped", sig.ref.path)
            return Behaviors.stopped()
        return Behaviors.unhandled

    return on_signal
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

`ctx.timers` is the actor's `TimerScheduler`. Starting a timer with a key already in use cancels the old one, and a message from a cancelled timer that already reached the mailbox is dropped. A `StashBuffer` holds messages for later; `buffer.unstash_all(behavior)` replays them before any new one. One `setup` is enough to wire timers, a stash and state:

```python
from pekko_py import ActorContext, StashBuffer


@dataclass(frozen=True)
class Tick:
    pass


@dataclass(frozen=True)
class Ready:
    pass


type Cmd = Tick | Ready | str


@Behaviors.setup
def gated(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
    ctx.timers.start_timer_with_fixed_delay("tick", Tick(), 1.0)
    buffer = StashBuffer[Cmd](1000)

    @Behaviors.receive_message
    def active(msg: Cmd) -> Behavior[Cmd]:
        print(msg)
        return Behaviors.same

    @Behaviors.receive_message
    def waiting(msg: Cmd) -> Behavior[Cmd]:
        match msg:
            case Ready():
                return buffer.unstash_all(active)
            case Tick():
                return Behaviors.same
            case _:
                buffer.stash(msg)
                return Behaviors.same

    return waiting
```

The Apache Pekko spelling, `Behaviors.with_timers(factory)` and `Behaviors.with_stash(capacity, factory)`, works too and is what the ported examples use.

### Routers, batching, and bounded mailboxes

`Routers.pool(n, behavior)` spawns `n` routees and forwards each message by round robin, at random, or by `with_consistent_hashing_routing(key)` so that equal keys reach the same routee. `batching(max_n, max_wait, handle)` is a behavior that collects messages and hands them to `handle` in batches, which is where `np.add.at` earns its keep. `MailboxSelector.bounded(capacity, overflow=...)` passed to `spawn` bounds a mailbox and drops the new message, drops the oldest, or raises in the sender. A producer that can await should use `await ref.offer(msg)` instead of `tell`: on a full mailbox it waits for room, so a slow consumer slows the producer and nothing is lost. `tell` never waits, so it stays usable from callbacks and timers.

### Sharding

`Sharding.init(Entity(key, create))` is a shard region: one entity actor per id, created by its first message, passivated when idle or when it asks, with messages that arrive while it is passivating buffered for the next incarnation. This is the region layer of Apache Pekko's `ClusterSharding` inside one process, which is what most single-node apps use it for: address by id, create on demand, and keep only hot entities in memory.

```python
from pekko_py import Entity, EntityContext, EntityTypeKey, Sharding

Cart = EntityTypeKey[Command]("Cart")


def cart(entity: EntityContext[Command]) -> Behavior[Command]:
    return counter()  # entity.entity_id names this cart; entity.shard.tell(Passivate(ctx.self)) stops it early


region = Sharding.init(Entity(Cart, cart).with_passivate_after(30.0))
```

Spawn `region` like any behavior, then `Sharding.entity_ref_for(region_ref, "cart-42")` gives an `EntityRef`, an `ActorRef` that `tell`s and `ask`s the entity whether or not it exists yet. `GetShardRegionState` asks the region which entities are live. Under the region, as in Pekko, a Shard actor per shard id hosts its entities; the same Shards serve the cluster-wide regions of the "Sharding across nodes" section. `GetShardRegionState` is answered through a child that asks every Shard, so the reply arrives once all have answered or after `shard_region_query_timeout`.

### Persistence

`Behaviors.persisted(store)` makes a state-to-behavior factory durable: calling it with a new state returns a behavior that writes the state before it is installed and before any deferred reply leaves. `factory.recover(id, empty)` starts the actor by loading the state for that id in one read. There are no events and no replay. The latest state is the truth, which is what carts, sessions and rooms want.

```python
from pekko_py import SqliteStore

store = SqliteStore[int]("counters.db")


@Behaviors.persisted(store)
def durable_counter(total: int) -> Behavior[Command]:
    @Behaviors.receive_message
    def on_message(msg: Command) -> Behavior[Command]:
        match msg:
            case Increment(by):
                return durable_counter(total + by)  # written, then installed
            case GetTotal(reply_to):
                reply_to.tell(total)  # a read: nothing written
                return Behaviors.same

    return on_message


recovered = durable_counter.recover("counter-1", empty=0)
```

`then_reply(ref, msg)` and `then_run(callback)` on the returned behavior defer effects until the write has completed; anything done inside the handler before returning happens whether or not the write succeeds. Every write carries a revision, and a store rejects a stale one, so two incarnations of one id cannot overwrite each other silently. A failed write fails the actor with the old behavior still current, and a `restart` reloads the committed state when `supervise` wraps the `recover(...)` behavior. Recovery always calls the one factory it was given, so an actor with several phases keeps the phase inside the persisted state rather than switching to another persisted factory. `InMemoryStore` is for tests; `SqliteStore` is stdlib SQLite with writes in a thread, pickle by default (trusted local storage only) and a codec argument for anything else. With sharding, the entity factory is `lambda e: cart.recover(e.entity_id, empty={})`, and a passivated entity recovers on its next message.

#### How this differs from Apache Pekko persistence

Apache Pekko offers two persistence styles. `EventSourcedBehavior` stores the events a command produced and rebuilds state by replaying them. `DurableStateBehavior` stores the latest state. Both share one API: a command handler that receives the state and the command and returns an `Effect` such as `Effect.persist(...)` or `Effect.none`, chained with `thenReply`. That shared shape exists so the two styles are interchangeable. pekko-py has no event sourcing, so it does not carry the shape either, and the design differs in five ways.

- **State stays where every other behavior keeps it.** In pekko-py, state is the parameter of a behavior factory, `counter(total)`, and a handler returns the next behavior. Persistence keeps that: `Behaviors.persisted(store)` wraps the factory, and a handler that returns `counter(total + by)` has, by construction, named the next state. There is no command handler, no event handler and no `Effect` type. A read returns `Behaviors.same` and writes nothing.
- **Recovery is one read, always.** `factory.recover(id, empty)` loads the stored state and calls the factory once. There is no replay, so recovery time does not grow with the number of commands handled, and there are no snapshots to tune or event schemas to keep decodable as the code evolves. What is given up is history: there are no events to project into other read models or to audit. If an entity's history is the product, as in a ledger, event sourcing is the right tool and this is not it.
- **Write, then install, then effects.** The returned behavior is a `setup` whose async factory writes the state, builds the next behavior, runs the deferred `then_reply` and `then_run` effects, and installs. A failed write raises before anything is installed or replied, so the actor fails with its old behavior and supervision decides; a `restart` around `recover` reloads the committed state. A failing effect after the commit is logged and does not roll memory back behind the store. Pekko reaches the same ordering through its stash-while-persisting machinery; here the runtime's existing rule, that a returned `setup` runs before the next message, is enough, and `cell.py` has no persistence code.
- **Every write carries a revision.** The store rejects a write that is not exactly one past what it holds, so two incarnations of one id, for example in two processes, cannot overwrite each other silently; the second writer's actor fails. Pekko's durable state has the same optimistic check and surfaces it the same way, as a failure of the actor.
- **The incarnation is implicit.** Pekko threads the persistence id through the behavior. Here `recover` records the id, store and revision for the actor's asyncio task, and every state the actor later returns finds it there. The record is bound to the task that recovered it, so a child actor cannot inherit its parent's incarnation, and bound to the factory, so a state from a different persisted factory fails the actor instead of writing into the wrong store.

The stores are small by design: a `Store` protocol with `load`, `save` and `delete`, an in-memory store for tests, and a SQLite store over the stdlib with writes in a thread and a pluggable codec. Pekko's durable state plugs into JDBC or R2DBC through its serialization extension.

### Remoting

Give a system an address and its actors are reachable from other pekko-py processes by path. `system.resolve(path)` returns a `RemoteRef`, an `ActorRef` whose `tell` serializes the message and sends it over a TCP connection opened on first use. `ask` works unchanged, because the reply ref crosses the wire as a path too. A message type is sendable once it is marked `@remotable`, which derives a JSON codec from the frozen dataclass; an `ActorRef` field becomes a path, and nested or union-typed fields work because the encoding is self-describing.

```python
from pekko_py import remotable


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
        greeter: ActorRef[Greet] = system.resolve("pekko://greeter@127.0.0.1:2552/user")
        print(await ask(greeter, lambda r: Greet("pekko", r), timeout=3))
```

A `tell` to a node that is down never blocks or raises; once the connection attempt fails the message is a dead letter, as in Pekko. Sending an unregistered type, or a message over 16 MiB, raises `SerializationError` at `tell`. A message for a path nobody owns, or one that fails to decode, is a dead letter on the receiving node and the connection stays up. Replies travel on the replying node's own connection back, so the asking side must be listening too (`async with` does that) and `address=` must be a host its peers can reach: it is both the bind and the advertised address, and `127.0.0.1` and `localhost` are different authorities. Message adapters are kept for the actor's lifetime, so create them in `setup` rather than per message. Remoting alone has no membership and no remote `watch`, and a peer that restarts is served rather than quarantined; membership is the next section. The listener accepts any peer, so run it on a private network or behind a tunnel; TLS is out of scope, as it is for Apache Pekko's Artery without TLS configured.

### Cluster

Give a system `cluster=ClusterSettings(seed_nodes=...)` and it runs Apache Pekko's membership protocol under `/system/cluster`: it joins through the seed nodes, gossips the member list with a vector clock, elects the lowest address among reachable Up members as leader, and lets only that leader, on convergence, move members `joining → up` and `leaving → exiting → removed`. Phi-accrual failure detection over heartbeats marks a silent node `unreachable`; `Down` it and the leader removes it. A node that is downed, or that leaves, terminates its own system. `Cluster(system)` is the extension: `manager` takes the commands, `subscriptions` the subscriptions, and `state` / `self_member` are the current view.

```python
from pekko_py import Address, Cluster, ClusterSettings, Leave, MemberEvent, Subscribe


def cluster_node(port: int, seed_port: int) -> ActorSystem[MemberEvent]:
    seeds = (Address("app", "127.0.0.1", seed_port),)
    system = ActorSystem(listener, "app", address=("127.0.0.1", port), cluster=ClusterSettings(seed_nodes=seeds))
    Cluster(system).subscriptions.tell(Subscribe(system, MemberEvent))  # current state first, then every change
    return system


async def main() -> None:
    async with cluster_node(2551, 2551) as first, cluster_node(2552, 2551) as second:
        await until(lambda: all(m.status == "up" for m in Cluster(first).state.members) and len(Cluster(first).state.members) == 2)
        print(Cluster(first).state.leader)                        # Address(name='app', host='127.0.0.1', port=2551)
        Cluster(first).manager.tell(Leave(Cluster(second).self_address))
        await second.when_terminated()                            # exiting nodes shut themselves down
```

`listener` is any `Behavior[MemberEvent]`; it receives `MemberJoined`, `MemberUp`, `MemberLeft`, `MemberExited`, `MemberRemoved(member, previous_status)` and, under `ReachabilityEvent`, `UnreachableMember` / `ReachableMember`; `LeaderChanged`, `SelfUp` and `SelfRemoved` arrive when subscribing to `ClusterDomainEvent`, as do the internal `SeenChanged` and `ReachabilityChanged`, which fire on most gossip rounds, so a listener for every event should match what it wants and ignore the rest. The first seed node forms the cluster alone once the other seeds have not answered within `seed_node_timeout`; any other node asks every seed and joins the first to answer, and a node with no seed nodes waits for a `Join(address)` or `JoinSeedNodes(...)` command. All settings are `ClusterSettings` fields with Pekko's defaults in seconds (`gossip_interval`, `heartbeat_interval`, `acceptable_heartbeat_pause`, `failure_detector_threshold`, `monitored_by_nr_of_members`, `allow_weakly_up_members`, ...). A member that rejoins from a restarted process is a new incarnation: the old one is marked terminated and removed before the new one is let in. `terminate()`, or leaving the `async with`, stops a node gracefully through the coordinated shutdown below: it leaves, hands over, and only then stops. Actors on different nodes find each other through the Receptionist, next.

### Coordinated shutdown

`ActorSystem.terminate()` runs Apache Pekko's CoordinatedShutdown: a fixed sequence of phases, each bounded by a timeout, to which you add tasks. On a cluster node the cluster adds its own: `cluster-sharding-shutdown-region` hands the node's shards off, `cluster-leave` sends `Leave`, `cluster-exiting` waits for the leader to move the node to Exiting and is where singletons hand over, `cluster-exiting-done` confirms the exit, `cluster-shutdown` waits for the cluster daemon, and `actor-system-terminate` stops the actors. The same run happens when another node sends `Leave` for this one, when this node is downed, and, if you opt in, on a signal.

```python
import signal

from pekko_py import CoordinatedShutdown, CoordinatedShutdownSettings

system = ActorSystem(guardian, "app", shutdown=CoordinatedShutdownSettings(run_on_signals=(signal.SIGINT, signal.SIGTERM)))


async def drain() -> None:
    await server.close()  # stop taking requests before the node leaves the cluster


CoordinatedShutdown(system).add_task("service-unbind", "stop-http", drain)
await system.when_terminated()  # ctrl-c or SIGTERM runs the phases: user tasks, hand-offs, leave, stop
```

The phases, in order: `before-service-unbind`, `service-unbind`, `service-requests-done`, `service-stop`, `before-cluster-shutdown`, `cluster-sharding-shutdown-region`, `cluster-leave`, `cluster-exiting`, `cluster-exiting-done`, `cluster-shutdown`, `before-actor-system-terminate`, `actor-system-terminate`. Tasks in one phase run concurrently; a task that fails is logged and the run continues, as Pekko's `recover = on`. A phase that outlives its timeout (`default_phase_timeout` 5 s; 10 s for the region, exiting and terminate phases; `phase_timeouts` overrides) is logged and its still-running tasks are cancelled, so a task must tolerate `CancelledError` at any await; Pekko lets such a task run on. `terminate()` returns at once, as Pekko's does: messages told after it may still be processed, so await `when_terminated()`. A guardian that stops on its own also runs the phases, after its actors are gone. `run(reason)` starts the run by hand; `shutdown_reason` says who did. A node that terminates while another member is unreachable and not downed waits out `cluster-exiting`, since the leader cannot move it to Exiting; the split-brain resolver below removes that wait.

### Management and Kubernetes bootstrap

Apache Pekko Management, narrowed to what a Kubernetes deployment uses: an HTTP endpoint on port 7626 with the health checks Kubernetes probes (`/startup`, `/ready`, `/alive`; a cluster node is ready when it is Up or WeaklyUp), read-only cluster routes (`/cluster/members`, `/cluster/members/{address}`, `/cluster/shards/{type}`), and cluster bootstrap: with no seed nodes configured, each node lists its peers through service discovery, probes their endpoints for seed nodes, joins any cluster it sees, and when nobody has one and the peer set has been stable for `stable_margin`, the lowest address joins itself. `KubernetesApiServiceDiscovery` lists the pods carrying the app label through the API server with the pod's service account; `ConfigServiceDiscovery` is a fixed list for local runs, and any `ServiceDiscovery` subclass with one `lookup` method works.

```python
from pekko_py import ClusterBootstrapSettings, KubernetesApiServiceDiscovery, PekkoManagement, PekkoManagementSettings

management = PekkoManagementSettings(
    discovery=KubernetesApiServiceDiscovery(),
    bootstrap=ClusterBootstrapSettings(service_name="myapp", port_name="management"),
)
system = ActorSystem(guardian, "myapp", address=(pod_ip, 2552), cluster=ClusterSettings(), management=management)
await PekkoManagement(system).start()  # binds 7626, then starts the bootstrap
PekkoManagement(system).health.add_readiness_check("db", db.ping)  # your own checks join Pekko's
```

On Kubernetes: give the pods the label `app=myapp`; bind a Role with `get` and `list` on `pods` to their service account; declare container ports for remoting and a port named `management` on 7626; point the readiness probe at `/ready` and the liveness probe at `/alive` on 7626; the namespace comes from `KUBERNETES_NAMESPACE` or the mounted service-account file; if the cluster's DNS does not resolve pod names (`10-1-0-5.ns.pod.cluster.local`), set `KubernetesApiSettings(use_raw_ip=True)` so the contact points are the pod IPs. Once a cluster exists, `new_cluster_enabled=False` forbids a partitioned minority from ever starting a second one. Locally, `examples/cluster/node.py one 2551 --bootstrap 2551 2552 2553` in three terminals forms a cluster with no seeds; its management runs on port + 1000.

#### Running on Docker Desktop Kubernetes

`examples/k8s/` is a complete deployment: `node.py` spreads a simulated calculation over the pods with sharding (each pod submits one task a second, and whichever pod owns the task's shard computes it and names itself in the result), `Dockerfile` builds the image from this checkout, and `cluster.yaml` holds the service account that may list pods, its Role and RoleBinding, and a three-replica Deployment with the pod IP and name from the downward API and the health checks as probes. Enable Kubernetes in Docker Desktop's settings (`kubectl config current-context` prints `docker-desktop`), then from the repository root, with your Docker Hub user for `YOU`:

```sh
docker build -t YOU/pekko-py-cluster:dev -f examples/k8s/Dockerfile .
docker login && docker push YOU/pekko-py-cluster:dev        # Docker Desktop's Kubernetes has its own image store, so the push is needed
kubectl create secret docker-registry regcred --docker-username=YOU --docker-password=ACCESS_TOKEN   # only if the repository is private
sed "s|IMAGE|YOU/pekko-py-cluster:dev|" examples/k8s/cluster.yaml | kubectl apply -f -
kubectl get pods -l app=cluster -w                            # three pods, Ready once they are Up
kubectl logs -l app=cluster -f --prefix                       # members joining, then the tasks and the pod that computed each
kubectl port-forward deploy/cluster 7626 &
curl -s localhost:7626/cluster/members | python -m json.tool  # also /cluster/shards/calculator, /ready, /alive, /bootstrap/seed-nodes
kubectl delete pod $(kubectl get pods -l app=cluster -o name | head -1)   # SIGTERM: the pod leaves gracefully, its shards move, a new pod bootstraps in
kubectl scale deploy/cluster --replicas=5                     # the new pods find the cluster and take shards
kubectl delete -f examples/k8s/cluster.yaml
```

`ErrImagePull` with `pull access denied` means the repository is private: make it public in its settings on hub.docker.com, or create the `regcred` secret above with a Docker Hub access token; the manifest references it and the kubelet ignores it when it is missing. A new image needs a push and `kubectl rollout restart deploy/cluster` (or a new tag), since `IfNotPresent` keeps the one it has; the restart is itself a demonstration, since the old pods leave gracefully while the new ones bootstrap in. Any other cluster works the same way once the image is in a registry it can pull from.

### Split-brain resolver

Without a downing provider an unreachable member stays unreachable until someone sends `Down`, and until then the leader cannot move any member, so a crashed oldest node blocks singleton hand-over and shard reallocation. Apache Pekko's split-brain resolver is the provider: a child of the core daemon, active on the leader, that waits until membership and reachability have been stable for `stable_after` and then downs one side by a strategy, so the leader can remove it. Turn it on with `downing=`:

```python
from pekko_py import ClusterSettings, KeepMajority, SplitBrainResolverSettings

settings = ClusterSettings(
    seed_nodes=seeds,
    downing=SplitBrainResolverSettings(strategy=KeepMajority(), stable_after=20.0, down_all_when_unstable=None),
    down_removal_margin=0.0,  # 0: use stable_after
)
```

- `KeepMajority()`, the default: down the unreachable side when this side holds the majority of the last known membership, else down this side; equal sides keep the one with the lowest address. Three or more parts with no majority all shut down.
- `StaticQuorum(quorum_size)`: keep this side when it has at least `quorum_size` members, else down it. Never grow the cluster past `2 * quorum_size - 1`; past that it downs all.
- `KeepOldest(down_if_alone=True)`: keep the side with the oldest member, except that an oldest partitioned from two or more others downs itself.
- `DownAll()`: down every node, the conservative choice.

All four detect indirectly connected nodes, which are marked unreachable over some links but still talk over others, and down those first, as Pekko. `down_all_when_unstable` (`None` is Pekko's `on`: three quarters of `stable_after`, at least four seconds; `0` is off) downs everything on every node when reachability keeps flapping for `stable_after + down_all_when_unstable`. `down_removal_margin` is how long the surviving side waits after a removal before starting that node's singletons and shards, so the downed side has time to stop them; it defaults to `stable_after`.

### Receptionist

Actors find each other through the Receptionist, Apache Pekko's service registry: a service registers under a `ServiceKey`, and anyone asks `Find` for the current refs under that key or `Subscribe`s to be sent a `Listing` now and after every change. Every system starts one; on a cluster node the registry is replicated, so a service registered on one node is found on the others as a `RemoteRef`. Entries follow their node: they vanish when the service stops or the member is removed, and while a node is unreachable its services drop from `service_instances` but stay in `all_service_instances`.

```python
from pekko_py import Find, Listing, Receptionist, Register, ServiceKey
from pekko_py.receptionist import Subscribe  # the cluster's Subscribe is the top-level one

GREETER = ServiceKey[Greet]("greeter")


def greeter_node(ctx: ActorContext[Listing[Greet]]) -> Behavior[Listing[Greet]]:
    Receptionist(ctx.system).ref.tell(Register(GREETER, ctx.spawn(greeter, "greeter")))
    Receptionist(ctx.system).ref.tell(Subscribe(GREETER, ctx.self))  # a Listing now, then one per change
    return Behaviors.receive_message(lambda listing: print(sorted(r.path for r in listing.service_instances)) or Behaviors.same)


def greeter_system(port: int) -> ActorSystem[Listing[Greet]]:
    seeds = (Address("app", "127.0.0.1", 2551),)
    return ActorSystem(Behaviors.setup(greeter_node), "app", address=("127.0.0.1", port), cluster=ClusterSettings(seed_nodes=seeds))


async def main() -> None:
    async with greeter_system(2551) as first, greeter_system(2552) as second:
        await until(lambda: len(Cluster(second).state.members) == 2)
        listing = await ask(Receptionist(second).ref, lambda r: Find(GREETER, r), timeout=1)
        for ref in listing.service_instances:  # one local ref, one RemoteRef to the greeter on the first node
            print(await ask(ref, lambda r: Greet("pekko", r), timeout=1))
```

Only local actors can register or subscribe, as in Pekko; the receptionist watches them, message adapters included. A `Find` may come from another node: `Listing` and the replies are `@remotable`. Replication is one versioned row per node, pushed to the other members on every change and re-sent every `receptionist_resend_interval`, instead of Pekko's distributed data; see the deviations.

### Cluster singleton

`ClusterSingleton(system).init(SingletonActor(behavior, "name"))` runs exactly one instance of `behavior` in the cluster, on the oldest member, and returns a proxy ref that works on every node: messages sent through it reach the instance wherever it is, buffered while its location is unknown. When the host leaves, the instance is handed over to the next oldest member during the exiting phase, so it is stopped on the old node before it starts on the new one; when the host crashes, the next oldest starts it once the crashed member has been downed and removed. Apache Pekko's ClusterSingletonManager and ClusterSingletonProxy, state for state.

```python
from pekko_py import ClusterSingleton, SingletonActor

proxy = ClusterSingleton(system).init(SingletonActor(counter, "counter"))  # call it on every node
print(await ask(proxy, Hit, timeout=3))                                    # answered by the one instance
```

Give the instance a stop message with `with_stop_message` if it needs to finish work before it goes; otherwise it is stopped after its pending messages, as Pekko's PoisonPill. `ClusterSingletonSettings` carries Pekko's `removal_margin`, `hand_over_retry_interval`, `min_number_of_hand_over_retries`, `singleton_identification_interval` and `buffer_size`. An instance that fails is not restarted by the manager; wrap it in `Behaviors.supervise` as in Pekko.

### Sharding across nodes

`ClusterSharding(system).init(Entity(key, create))` is `Sharding.init` for the whole cluster: the same entity type on every node, each entity living on exactly one of them. A ShardCoordinator, run as a cluster singleton, decides which node hosts each shard with Pekko's least-shard allocation; every node's region routes a message to the shard's home, local or remote, and buffers while the home is unknown. `entity_ref_for(key, id)` works on any node. When a node joins, the coordinator moves shards to it over the following rebalance intervals; when a node leaves, its region hands every shard off during the exiting phase before the node confirms its exit; when a node crashes, its shards are served again once it has been downed and removed, and not before, so no entity ever runs twice.

```python
from pekko_py import ClusterSharding, Entity, EntityTypeKey

COUNTER = EntityTypeKey[CountUp]("Counter")
region = ClusterSharding(system).init(Entity(COUNTER, counter))          # call it on every node
print(await ask(ClusterSharding(system).entity_ref_for(COUNTER, "alice"), CountUp, timeout=3))
```

Entities are in memory: a moved or handed-off entity starts fresh on its new node, and `Behaviors.persisted` is how state outlives that. A rebalance can drop messages that reach the old host in its hand-off window, as Pekko's does; they are dead letters, never silent. `ClusterShardingSettings` carries Pekko's `number_of_shards`, `retry_interval`, `rebalance_interval`, `handoff_timeout`, `shard_start_timeout`, `buffer_size`, `rebalance_absolute_limit`, `rebalance_relative_limit` and `coordinator_singleton_settings` (the coordinator's `ClusterSingletonSettings`), plus `passivate_after` and `shard_region_query_timeout`.

### numpy and parallelism

A handler that blocks the event loop stalls every actor. Run numpy work with `ctx.pipe_to_self(asyncio.to_thread(fn, ...), adapt)` and stash until the result comes back. numpy releases the GIL, so eight such actors use eight cores while a timer keeps ticking on time; pure-Python work in threads gains nothing. For another process, `pekko_py.ext.shm.SharedArray` hands over a large array as a three-field handle instead of a copy.

Messages are shared by reference and never copied. Keep them immutable: frozen dataclasses, and for arrays either copy before sending or set `arr.flags.writeable = False`.

### FastAPI

`pekko_py.ext.fastapi.Actors` owns an `ActorSystem` for the life of a FastAPI app and hands it to routes as a typed dependency, so `ask` replies are type-checked all the way to the response. An `AskTimeoutError` that escapes a route becomes a 504. Needs the `web` extra. To stop taking requests before a cluster node leaves, add a task to the `service-unbind` phase of the coordinated shutdown, and let uvicorn keep its signals: its shutdown should call `system.terminate()` rather than `run_on_signals` taking SIGTERM.

```python
from fastapi import FastAPI
from pekko_py.ext.fastapi import Actors

actors = Actors(counter(), "web")
app = FastAPI(lifespan=actors.lifespan)


@app.get("/total")
async def total(system: ActorSystem[Command] = actors.depends()) -> dict[str, int]:
    return {"total": await system.ask(GetTotal, timeout=1)}
```

Inside a WebSocket route the same dependency works, and the connection actor pattern in `examples/fastapi_chatroom.py` keeps one actor per socket.

## Apache Pekko names

| Apache Pekko | pekko-py |
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
| `Behaviors.setup(ctx => ...)` | `@Behaviors.setup` on a `def`, or the call form; the same for `receive`, `receive_message` and `receive_signal` |
| (none) | `ctx.timers`, the same scheduler `with_timers` hands out, so one `setup` needs no timer factory layer |
| `Behaviors.withTimers`, `TimerScheduler.startSingleTimer / startTimerWithFixedDelay / cancel / cancelAll / isTimerActive` | same, snake_case |
| `Behaviors.withStash`, `StashBuffer.stash / unstashAll / size / isEmpty / isFull`, `StashOverflowException` | same, snake_case; `StashOverflowError` |
| `MailboxSelector.bounded(capacity)` passed to `spawn` | same, plus `overflow="drop_new" \| "drop_old" \| "fail"` |
| (none) | `await ref.offer(msg)`: `tell` that waits for room on a full bounded mailbox |
| `ClusterSharding.init(Entity(key)(create))` | `ctx.spawn(Sharding.init(Entity(key, create)), name)`; single node |
| `sharding.entityRefFor(key, id)` | `Sharding.entity_ref_for(region_ref, id)` |
| `EntityTypeKey`, `EntityContext`, `EntityRef`, `ShardingEnvelope`, `ClusterSharding.Passivate`, `GetShardRegionState` | same; `Passivate` |
| `DurableStateBehavior(persistenceId, emptyState, commandHandler)` | `@Behaviors.persisted(store)` on a state-to-behavior factory, started with `factory.recover(id, empty)` |
| `Effect.persist(state).thenReply(replyTo)(f)`, `Effect.none` | `factory(state).then_reply(ref, msg)` / `.then_run(callback)`; any plain behavior, including `same`, writes nothing |
| durable state stores (JDBC, R2DBC) | `InMemoryStore`, `SqliteStore(path, codec=...)` |
| `pekko.remote.artery.canonical.hostname / port` | `ActorSystem(..., address=(host, port))`; paths read `pekko://name@host:port/user/...` |
| `ActorRefResolver.resolveActorRef(path)` | `system.resolve(path)`: a local ref, or a `RemoteRef` for another node |
| serialization bindings and manifests | `@remotable` on a frozen dataclass; self-describing JSON with `$type` and `$ref` |
| `pekko.cluster.seed-nodes` and the other `pekko.cluster.*` settings | `ActorSystem(..., cluster=ClusterSettings(seed_nodes=(Address(name, host, port), ...), ...))`; the failure detector settings are `ClusterSettings` fields in seconds |
| `Cluster(system).manager ! Join(addr)` / `JoinSeedNodes` / `Leave` / `Down` | `Cluster(system).manager.tell(Join(addr))`, same names |
| `Cluster(system).subscriptions ! Subscribe(ref, classOf[MemberEvent])`, `Unsubscribe`, `GetCurrentState` | `Cluster(system).subscriptions.tell(Subscribe(ref, MemberEvent))`; initial state always arrives as events |
| `Cluster(system).selfMember / state / selfUniqueAddress / isTerminated` | `self_member`, `state`, `self_unique_address`, `is_terminated` |
| `MemberEvent`, `ReachabilityEvent`, `LeaderChanged`, `SelfUp`, `SelfRemoved`, `CurrentClusterState` | same; `MemberRemoved(member, previous_status)` |
| `MemberStatus.Joining / WeaklyUp / Up / Leaving / Exiting / Down / Removed` | `member.status` is the string `"joining"`, `"weakly_up"`, ... |
| `Receptionist(system).ref ! Register(key, ref)`, `Deregister`, `Find(key, replyTo)` | `Receptionist(system).ref.tell(Register(key, ref))`, same names; `Registered` / `Deregistered` replies when `reply_to` is given |
| `ServiceKey[T]("id")` | `ServiceKey[T]("id")` |
| `Receptionist.Subscribe(key, subscriber)` | `pekko_py.receptionist.Subscribe(key, subscriber)`; the top-level `Subscribe` is the cluster's |
| `Listing.serviceInstances(key)` / `allServiceInstances(key)` / `servicesWereAddedOrRemoved` | `listing.service_instances` / `all_service_instances` / `services_were_added_or_removed`, fields of `Listing[T]` |
| `ClusterSingleton(system).init(SingletonActor(behavior, "name"))`, `withStopMessage`, `withSettings` | same, `with_stop_message`, `with_settings`; returns the proxy ref |
| `ClusterSingletonSettings` / `ClusterSingletonManagerSettings` (`removal-margin`, `hand-over-retry-interval`, `min-number-of-hand-over-retries`, `singleton-identification-interval`, `buffer-size`) | `ClusterSingletonSettings` with the same fields in snake_case, seconds |
| `CoordinatedShutdown(system).addTask(phase, name)(task)` | `CoordinatedShutdown(system).add_task(phase, name, task)`; `add_actor_termination_task`, `run(reason)`, `shutdown_reason`, `timeout(phase)`, `total_timeout` |
| `pekko.coordinated-shutdown.phases.<p>.timeout`, `default-phase-timeout` | `ActorSystem(..., shutdown=CoordinatedShutdownSettings(phase_timeouts={...}, default_phase_timeout=5.0))` |
| `run-by-actor-system-terminate`, `run-by-jvm-shutdown-hook` | `CoordinatedShutdownSettings(run_by_actor_system_terminate=True, run_on_signals=(signal.SIGTERM, ...))` |
| `ActorSystemTerminateReason`, `JvmExitReason`, `ClusterLeavingReason`, `ClusterDowningReason` | the same, as frozen dataclasses; subclass `Reason` for your own |
| `PekkoManagement(system).start()` / `routes` | `await PekkoManagement(system).start() -> uri`; routes are built in: health checks, `/cluster/*`, `/bootstrap/seed-nodes` |
| `pekko.management.http.hostname / port / bind-hostname / bind-port / base-path` | `ActorSystem(..., management=PekkoManagementSettings(hostname, port, bind_hostname, bind_port, base_path))` |
| `pekko.management.health-checks.{startup,readiness,liveness}-checks` | `PekkoManagement(system).health.add_{startup,readiness,liveness}_check(name, check)`; `HealthCheckSettings` for the paths, timeout and `ready_states` |
| `ClusterBootstrap(system).start()` and `pekko.management.cluster.bootstrap.*` | started by `PekkoManagement.start()` when `PekkoManagementSettings.bootstrap` is set; `ClusterBootstrapSettings` fields in snake_case, seconds |
| `pekko.discovery.kubernetes-api.*` | `KubernetesApiServiceDiscovery(KubernetesApiSettings(...))` |
| `ServiceDiscovery.lookup(Lookup(name), timeout)` | `await discovery.lookup(Lookup(name, port_name), resolve_timeout) -> Resolved(service_name, addresses)` |
| `pekko.cluster.downing-provider-class = "...SplitBrainResolverProvider"` | `ClusterSettings(downing=SplitBrainResolverSettings(...))` |
| `split-brain-resolver.active-strategy`, `stable-after`, `down-all-when-unstable` | `SplitBrainResolverSettings(strategy, stable_after, down_all_when_unstable)` |
| `static-quorum.quorum-size`, `keep-oldest.down-if-alone` | `StaticQuorum(quorum_size)`, `KeepOldest(down_if_alone)`; `KeepMajority()`, `DownAll()` |
| `pekko.cluster.down-removal-margin` | `ClusterSettings.down_removal_margin` |
| `ClusterSharding(system).init(Entity(key, create))`, `entityRefFor(key, id)` | same, `entity_ref_for(key, id)`; the region ref is returned, as Pekko Typed |
| `HashCodeMessageExtractor(numberOfShards)` | `ClusterShardingSettings(number_of_shards=...)`; the shard id is `crc32(entity_id) % number_of_shards` |
| `ClusterShardingSettings` tuning parameters (`retry-interval`, `rebalance-interval`, `handoff-timeout`, `shard-start-timeout`, `buffer-size`, `rebalance-absolute-limit`, `rebalance-relative-limit`, `coordinator-singleton`) | `ClusterShardingSettings` fields in snake_case, seconds; `coordinator_singleton_settings` is a `ClusterSingletonSettings` |
| `Routers.pool(n)(behavior)`, `withRoundRobinRouting / withRandomRouting / withConsistentHashingRouting` | `Routers.pool(n, behavior)`, same methods in snake_case |
| `SpawnProtocol()`, `SpawnProtocol.Spawn(behavior, name, props, replyTo)` | `SpawnProtocol.apply()`, `SpawnProtocol.Spawn(behavior, name, reply_to, mailbox=None)` |
| (implicit) | `Replyable[Res]`, the protocol a request satisfies by having a `reply_to: ActorRef[Res]` field |
| `system.ignoreRef` | `system.ignore_ref` |
| (none) | `batching(max_n, max_wait, handle)` |
| (none) | `pekko_py.ext.shm.SharedArray`, a picklable handle to an array in shared memory |

## Runtime model

- One asyncio task per actor. System messages (stop, watch, death notifications, backoff timers) are processed before user messages.
- A behavior is a value, and one value can start many actors (`Routers.pool`, or spawning it twice). Anything captured in its closure is then shared by all of them. Create per-actor state inside `Behaviors.setup`, `with_timers` or `with_stash`, which run once per actor.
- A handler that awaits keeps the actor busy. Messages are serialized across awaits, which is the main reason to use actors on a single event loop. `test_async_handler_keeps_messages_serialized` pins this down.
- Stopping an actor stops its children first (depth-first), then delivers `PostStop` to the last running behavior, then runs the `Behaviors.stopped(callback)` callback, then notifies watchers and the parent. Pending user messages become dead letters (logger `pekko_py.dead_letters`).
- There is one `TimerScheduler` per actor, and a `restart` or `restart_with_backoff` cancels all of its timers before `PreRestart`, as Pekko's `RestartSupervisor` does. `resume` leaves them running.
- Background work started with `pipe_to_self` is cancelled when the actor stops. Every test asserts that no asyncio task outlives the system.
- Timer messages are validated when dequeued, so a message from a timer that was cancelled or restarted while it sat in the mailbox is dropped. `unstash_all` replays the stashed messages inside the current message's turn, before any new message.
- An idle actor costs about 3.5KB and 40µs to spawn. A busy actor handles over a million trivial messages a second; waking an idle one costs an asyncio task switch, about 16µs, which is what `batching` amortizes. 100k actors fit, but `examples/population.py` shows why 16 shard actors holding struct-of-arrays beat 100k single-arm actors by hundreds of times on memory and query time.

## Performance

From `examples/benchmark.py` on an Apple Silicon laptop with Python 3.14 and the default event loop. Run it yourself; the numbers move with the machine.

| path | rate | cost |
|---|---|---|
| `tell` into a busy actor, handler returns `same` | 1.3M msg/s | 0.8µs per message |
| `tell`, handler returns a new behavior each time | 630k msg/s | 1.6µs per message |
| `tell` through a pool of 8 routees | 630k msg/s | 1.6µs per message |
| ping-pong between two actors | 31k round trips/s | 32µs per round trip |
| `ask` from outside, one at a time | 30k/s | 33µs per ask |
| spawn and stop an actor | 77k/s | 13µs per actor |

The two regimes matter more than the figures. A busy actor drains its mailbox without yielding, so a message costs a deque pop, a `match` and a call. A message to an idle actor costs an asyncio task switch, about 16µs, which is what every asyncio design pays and what `batching` amortizes. Returning a new behavior per message allocates a closure and a `Receive`, which is the price of the functional style; keep `Behaviors.same` with state in a per-actor cell on hot paths. The other benchmarks under `examples/` cover threads, batching, population shards and shared memory.

## Known deviations from Apache Pekko

- A plain `restart` whose setup fails stops the actor. Pekko does the same when restarts are unlimited, but retries while a `with_limit` budget remains. Use `restart_with_backoff` for setups that can fail.
- An exception in a message adapter or `pipe_to_self` adapt function is logged and the message is dropped. Pekko delivers a `MessageAdaptionFailure` signal, which fails the actor unless handled.
- If a handler fails partway through `unstash_all`, the unprocessed messages stay in the buffer as in Pekko, but `resume` and `PreRestart` see the behavior from before the unstash began. Pekko tracks the intermediate behavior reached by the last successful message.
- Exceptions in `PreRestart` and `PostStop` handlers are logged, not supervised.
- `ask` and `ctx.ask` check the reply type but not that the target accepts the request, since the reply type is read from the request's `reply_to` field and pyright cannot tie both to one type parameter. `tell` is fully checked. Pekko checks both.
- The `Behaviors` factories double as decorators. This is only a Python spelling of the same call, so there is nothing to port, but Apache Pekko code has no counterpart to it.
- `ctx.timers` exposes the actor's `TimerScheduler` directly; Pekko hands it out only through `Behaviors.withTimers`. Both reach the same scheduler, and a restart cancels its timers either way.
- `start_timer_at_fixed_rate`, `StashBuffer.unstash(n)` and `Routers.group` are not implemented.
- Pekko's bounded mailbox only drops the new message. `overflow="drop_old"`, `overflow="fail"` (raise `MailboxOverflowError` in the sender) and the awaitable `offer` are additions. Adapted messages, timer messages and `pipe_to_self` results count against the capacity; they always use `tell`.
- `with_consistent_hashing_routing(mapping)` takes no virtual-nodes factor and routes with `hash(mapping(msg)) % size`, since a pool has a fixed size. The mapping may return any hashable.
- Remoting is Artery narrowed to one layer: TCP with length-prefixed JSON frames, `@remotable` dataclasses instead of serializer ids and manifests, an unsendable message raises at `tell` instead of being logged and dropped, a restarted peer is served instead of quarantined, and there is no remote `watch`, no membership and no system-message delivery guarantee. `ask` across nodes works because the reply ref is given a temporary path on the asking node for exactly as long as the ask is pending.
- The cluster is Pekko Cluster narrowed to one data center and no roles: the gossip, vector clock, reachability table, convergence rule, leader election, member lifecycle, seed-node join processes, phi-accrual failure detector and heartbeat ring are ports of the Scala code, function for function. Left out: the app-version and config-compatibility checks on join, `PreparingForShutdown` and quarantine; the split-brain resolver is off unless `downing` is set, as Pekko without a provider. The heartbeat sender, the seed-node processes and the event publisher are state inside the one core daemon instead of separate actors, and the heartbeat receiver is its child. `ClusterSettings` replaces HOCON, with `allow_weakly_up_members` as a duration or `None` instead of a flag plus `weakly-up-after`. `Address` has no protocol field. The seed-node processes retry on a fixed-delay timer (the first seed every `min(1s, seed_node_timeout)`, the others every `seed_node_timeout`) rather than on Pekko's one-second task and `ReceiveTimeout`. The gossip target selector prefers nodes that have not seen the gossip with probability `gossip_different_view_probability` (0.8) up to `reduce_gossip_different_view_probability` members, which is what Pekko's settings document; Pekko's code passes the member count into the probability slot, so it always prefers such nodes in clusters of that size or smaller.
- The Receptionist has Pekko's API and local semantics, but replicates by owner rows instead of distributed data: each node owns one versioned row of its bindings, pushes it to the other members on every change and re-sends it every `receptionist_resend_interval`, and membership events drop the rows of removed nodes. There is no CRDT, no tombstone, no `distributed-key-count` sharding of keys and no write-consistency setting. One actor serves both the local and the cluster case where Pekko picks `LocalReceptionist` or `ClusterReceptionist` at start, and `Listing[T]` is a generic dataclass with fields rather than Pekko's untyped `Listing` with `serviceInstances(key)` accessors. The actor is always `/system/receptionist`; Pekko's cluster one is `/system/clusterReceptionist`. Every system has a `/system` root, as Pekko's classic system does.
- Cluster sharding's coordinator keeps its state in memory and rebuilds it from the regions' registrations: `Register` carries the shards a region hosts, and a new coordinator incarnation allocates an unknown shard only once every reachable member's region has registered and no unreachable member is unaccounted for. Pekko keeps that state in distributed data. Membership events replace remote DeathWatch of regions. Left out: remembered entities, proxy-only regions, stats queries, custom allocation strategies, lease, roles, data centers, passivation strategies beyond the idle timeout, and `StartEntity`. `rebalance_absolute_limit` 0 means no absolute cap (in Pekko, 0 selects its older allocation algorithm). The coordinator lives at `/system/singletonManager<type>Coordinator/singleton`; Pekko's is one level deeper. A region may register while its member is Up, Leaving or Exiting, where Pekko's coordinator accepts Up only, so a region can re-register after a coordinator move during its own leave. A coordinator that stops tells its regions so, in place of their remote DeathWatch of it, and the regions send their registration to every member up to the first Up one, as Pekko's `coordinatorSelection`. A registration that arrives after a new coordinator's rebuild gate has opened is resolved by handing the late copy of any conflicting shard off; Pekko cannot meet that case.
- CoordinatedShutdown has Pekko's phases as a fixed list and `CoordinatedShutdownSettings` in place of HOCON: no custom phases or dependency graph, `recover` always on, no `exit-jvm`, `exit-code` or reason overrides. The signal hook is opt-in through `run_on_signals` where Pekko's JVM hook is on by default; its reason keeps the name `JvmExitReason`, and the handler owns the signal for the whole event loop (Python handlers do not compose as JVM hooks do): use it only when the system owns the process's signals, and under a server such as uvicorn call `system.terminate()` from the server's shutdown hook instead. A phase that times out cancels its still-running tasks, where Pekko lets them run on; a task that swallows the cancellation holds the run. A guardian that stops on its own runs the phases after its actors are gone, where Pekko Typed's guardian wrapper runs them before. There is no `ClusterJoinUnsuccessfulReason`, since there is no `shutdown-after-unsuccessful-join-seed-nodes`. The cluster-leave task subscribes with an adapter rather than Pekko's `CoordinatedShutdownLeave` child actor.
- Pekko Management is narrowed to the Kubernetes path and the standard library: the endpoint is a small HTTP/1.1 server (GET only, one request per connection, no HTTPS) and probing and the API call use a matching client, so Pekko's TLS client for probing is dropped; settings objects replace HOCON and class-name loading (`routes`, `*-checks`, `join-decider.class`, `discovery-method`); only `KubernetesApiServiceDiscovery` and `ConfigServiceDiscovery` exist; `roles` are always `[]` and `oldestPerRole` is `{}`; the mutating cluster routes answer 405 with no switch, and there is no `/cluster/domain-events` stream; the bootstrap starts from `PekkoManagement.start()` rather than as an auto-started route provider; `ResolvedTarget.address` is a string. Pekko's per-host fallback-port filter is kept, so several nodes on one host need `filter_on_fallback_port=False`, as the example does.
- The split-brain resolver is Pekko's SplitBrainResolver and DowningStrategy without roles, data centers, the lease-majority strategy or quarantine's `DownSelfQuarantinedByRemote`. It ticks every `min(1 s, stable_after / 4)` rather than a fixed second, so millisecond settings work. `SplitBrainResolverSettings` and the strategy dataclasses replace HOCON (`active-strategy`, `stable-after`, `down-all-when-unstable`, `static-quorum.quorum-size`, `keep-oldest.down-if-alone`); `down_removal_margin` is a `ClusterSettings` field, and the resolver is turned on with `downing=` rather than `downing-provider-class`. It is a child of the core daemon at `/system/cluster/downingProvider`, and its failure shuts the node down, as in Pekko.
- The cluster singleton is ClusterSingletonManager and ClusterSingletonProxy without the lease, roles, data centers or the full-cluster-shutdown states; the oldest-changed tracker is state in the manager rather than a child actor. The proxy finds the instance with an `IdentifySingleton` message to the manager on the oldest member, answered with the instance ref, in place of `Identify` to the instance's path; it keeps asking every `singleton_identification_interval` after it has found it, in place of remote DeathWatch, so a dead instance is noticed within one interval. Hand-over messages carry an explicit `from_` ref. The actors are `/system/singletonManager<name>` and `/system/singletonProxy<name>`, without Pekko Typed's `-no-dc` suffix on the proxy.
- `ctx.stop(child, after_pending=True)` queues the stop behind the child's pending messages, like classic Pekko's `PoisonPill`; Pekko Typed has no public equivalent. `Sharding` uses it so passivation never drops a message the region already forwarded.
- Persistence is pekko-py's own shape, not `DurableStateBehavior`, and there is no event sourcing; see "How this differs from Apache Pekko persistence" in the Tour. The default codec is pickle; Pekko uses its serialization extension.
- `Sharding.init` is `ClusterSharding`'s region and Shard on one node, with no coordinator; `ClusterSharding(system).init` adds the coordinator, remote homes and rebalancing. The single-node region ref is passed to `entity_ref_for` explicitly since there is no extension registry; `with_passivate_after` and `with_buffer_size` replace Pekko's passivation and buffer-size settings, with idle checked on a tick of half the timeout as in Pekko, so an entity passivates between one and one and a half timeouts after its last message; and the region answers `GetShardRegionState` itself rather than through a separate query ref.
- `pekko_py.ext.fastapi` is not in Pekko; it is the lifespan, dependency and 504 mapping that every FastAPI app over actors otherwise writes by hand.
- `batching` is not in Pekko. It flushes a pending batch in `PostStop` and keeps the batch in a closure-local list.
- Only local refs can be watched. A bridge to a JVM Apache Pekko system over TCP is possible but deferred until there is a caller for it.
- `SharedArray` ownership is a convention: whoever holds the handle owns the segment, sending it transfers ownership, and the owner calls `unlink()` once. A process that crashes while owning a segment leaks it. On Python 3.13+ the resource tracker is bypassed; on 3.12 expect its warnings.

## Examples

| File | Shows |
|---|---|
| `examples/counter.py` | the functional style, `ask`, supervision with `resume` |
| `examples/chatroom.py` | the ChatRoom from the Apache Pekko docs: sessions as children, narrowed refs, DeathWatch |
| `examples/remoting/` | two processes: `greeter_node.py` listens on 2552, `client_node.py` resolves it by path, tells and asks across the wire |
| `examples/cluster/` | `node.py NAME PORT [SEED_PORT ...]`, or `node.py NAME PORT --bootstrap PORT...` to form the cluster with no seeds through Pekko's bootstrap: start three in three terminals, watch them join, go Up and elect a leader, each node's greeter appear in every node's Receptionist listing, one counter singleton answer all of them, and the sharded counters for three ids spread over the nodes; ctrl-c a node, which leaves gracefully, and the singleton and the shards it hosted move to the others; management on port + 1000 (`curl localhost:3551/cluster/members`) |
| `examples/k8s/` | `node.py`, `Dockerfile` and `cluster.yaml`: a three-pod cluster on Docker Desktop Kubernetes that finds itself through the API server and spreads a simulated calculation over the pods with sharding; delete a pod and its tasks move, scale up and the new pods take shards; see "Running on Docker Desktop Kubernetes" |
| `examples/persisted_counter.py` | `Behaviors.persisted` with `SqliteStore`: the total survives a supervised restart and a new actor system; run it twice and it keeps counting |
| `examples/database_client.py` | backoff restarts of a failing setup, a stash that outlives restarts, timers, `ctx.ask` |
| `examples/interaction_patterns/` | one runnable file per pattern in Apache Pekko's [Interaction Patterns](https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html) page: fire and forget, request-response, adapted response, ask between actors, ask from outside, generic response wrapper, ignoring replies, pipe to self, per-session child, aggregator, tail chopping, scheduling to self |
| `examples/fastapi_carts.py` | a FastAPI service over actors: one entity per cart id through `Sharding`, persisted in SQLite with `Behaviors.persisted` so carts survive passivation and restarts, typed `ask` from routes to entities |
| `examples/fastapi_chatroom.py` | the chat room over WebSockets: one connection actor per socket with an async handler that writes to it, rooms created on demand, and DeathWatch turning a closed socket into a "left" notice; serves a browser page at `/` |
| `examples/fastapi_online_learning.py` | a feedback loop behind FastAPI: predictions micro-batched into one numpy call, outcomes posted back, a learner actor taking gradient steps in a thread and hot-swapping the model; serves a playground page at `/`, and `python examples/fastapi_online_learning.py` runs the server with a demo client |
| `examples/bandit.py` | Thompson sampling over sharded arms: Beta posteriors as arrays, one vectorized draw per shard, `ctx.ask` fan-out, a progress timer |
| `examples/benchmark.py` | message throughput, round-trip latency and spawn cost of the runtime; the source of the Performance table |
| `examples/compute.py` | numpy in threads via `pipe_to_self(asyncio.to_thread(...))`, with a latency probe |
| `examples/batching.py` | `batching` throughput and p99 latency curves |
| `examples/population.py` | 100k single-arm actors against 16 shards behind `Routers.pool` |
| `examples/shared_memory.py` | `SharedArray` against pickling for a 400MB array |

The last four need numpy: `uv run --extra numpy python examples/bandit.py`. The `fastapi_*` examples need the `web` extra and run under uvicorn: `uv run --extra web uvicorn examples.fastapi_carts:app --port 8000`.

## Development

Layout follows Apache Pekko's modules: `pekko_py` is the actor core, `pekko_py.remote` the transport and serialization, `pekko_py.cluster` membership and the singleton, `pekko_py.cluster.sharding` the Shard, region and coordinator, `pekko_py.ext` the FastAPI and shared-memory integrations. Every public name is importable from `pekko_py` and from its own subpackage.

```
pytest      # every test fails if any asyncio task outlives it
pyright     # strict; tests/typing_check.py asserts type errors that must occur
```

## License

Apache-2.0, the same as Apache Pekko. The chat room and interaction-pattern examples are derived from the Apache Pekko documentation; see [NOTICE](NOTICE).

pekko-py is an independent project, not affiliated with or endorsed by the Apache Software Foundation or the Apache Pekko project. Apache Pekko is a trademark of the Apache Software Foundation.
