"""A cluster node: joins through the seed nodes, registers a greeter service, runs a cluster-wide singleton
counter, hosts sharded per-id counters, and prints every member event, every change of the greeter listing, and
what the singleton and the sharded counters answer each second. The listing on each node holds refs to the
greeters on all nodes; the singleton runs on the oldest node only and moves when that node stops; the sharded
counters for "alice", "bob" and "carol" live wherever the coordinator put their shards, spread over the nodes as
they join.

    uv run python examples/cluster/node.py NAME PORT [SEED_PORT ...]

    uv run python examples/cluster/node.py one 2551 2551          # terminal 1: the first seed forms the cluster
    uv run python examples/cluster/node.py two 2552 2551          # terminal 2: joins through 2551
    uv run python examples/cluster/node.py three 2553 2551 2552   # terminal 3: either seed will do

Or with no seed nodes at all, as on Kubernetes: every node lists the deployment's ports and the cluster forms by itself
through Pekko's bootstrap (discovery, contact-point probing, the lowest address self-joins):

    uv run python examples/cluster/node.py one 2551 --bootstrap 2551 2552 2553     # in three terminals, with two, three
    curl localhost:3551/cluster/members                                            # management is on port + 1000

Every node must run under the same system name, so NAME is only used for the log lines; the system is "cluster".
Ctrl-c or SIGTERM runs the coordinated shutdown, so the node leaves gracefully: its shards are handed off, its
greeter leaves every listing and, if it hosted the counter, the counter is handed over before the node exits and
starts again at 1 on the next oldest node. Kill a node
instead (kill -9) and the others report it unreachable, and the split-brain resolver downs and removes it after
five seconds of stability (keep-majority): its singleton and shards start elsewhere without anyone sending `Down`.
"""
import asyncio
import logging
import signal
import sys
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import (  # noqa: E402
    ActorContext,
    ActorRef,
    ActorSystem,
    Address,
    Behavior,
    Behaviors,
    Cluster,
    ClusterDomainEvent,
    ClusterSettings,
    ClusterSharding,
    ClusterBootstrapSettings,
    ClusterSingleton,
    ConfigServiceDiscovery,
    CoordinatedShutdownSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    LeaderChanged,
    Listing,
    MemberEvent,
    PekkoManagement,
    PekkoManagementSettings,
    ReachabilityEvent,
    Receptionist,
    Register,
    ResolvedTarget,
    ServiceKey,
    SingletonActor,
    SplitBrainResolverSettings,
    Subscribe,
    remotable,
)
from pekko_py.receptionist import Subscribe as SubscribeToKey  # noqa: E402  # the cluster's Subscribe is the top-level one

HOST = "127.0.0.1"


@remotable
@dataclass(frozen=True)
class Greet:
    name: str
    reply_to: ActorRef[str]


GREETER = ServiceKey[Greet]("greeter")


@remotable
@dataclass(frozen=True)
class Hit:
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Tick: ...


@dataclass(frozen=True)
class Counted:
    text: str


@remotable
@dataclass(frozen=True)
class CountUp:
    reply_to: ActorRef[str]


PER_ID = EntityTypeKey[CountUp]("PerIdCounter")

type Event = ClusterDomainEvent | Listing[Greet] | Tick | Counted


def per_id_counter(node_name: str) -> Callable[[EntityContext[CountUp]], Behavior[CountUp]]:
    """A sharded entity: one per id, created on its first message, hosted by whichever node owns its shard."""

    def create(entity: EntityContext[CountUp]) -> Behavior[CountUp]:
        def active(n: int) -> Behavior[CountUp]:
            @Behaviors.receive_message
            def on_message(msg: CountUp) -> Behavior[CountUp]:
                msg.reply_to.tell(f"{entity.entity_id} = {n + 1} (on {node_name})")
                return active(n + 1)

            return on_message

        return active(0)

    return create


def counter(node_name: str) -> Behavior[Hit]:
    """The singleton: one of these runs in the whole cluster, on the oldest node."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Hit]) -> Behavior[Hit]:
        count = 0
        print(f"[{node_name}] counter singleton started here")

        @Behaviors.receive_message
        def on_hit(msg: Hit) -> Behavior[Hit]:
            nonlocal count
            count += 1
            msg.reply_to.tell(f"{count} (counted on {node_name})")
            return Behaviors.same

        return on_hit

    return setup


def greeter(node_name: str) -> Behavior[Greet]:
    @Behaviors.receive_message
    def on_greet(msg: Greet) -> Behavior[Greet]:
        msg.reply_to.tell(f"hello {msg.name} from {node_name}")
        return Behaviors.same

    return on_greet


def listener(name: str) -> Behavior[Event]:
    """Registers this node's greeter, then prints what the cluster and the receptionist tell it, as the
    ClusterListener in the Apache Pekko docs."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Event]) -> Behavior[Event]:
        receptionist = Receptionist(ctx.system).ref
        receptionist.tell(Register(GREETER, ctx.spawn(greeter(name), "greeter")))
        receptionist.tell(SubscribeToKey(GREETER, ctx.self))
        proxy = ClusterSingleton(ctx.system).init(SingletonActor(counter(name), "counter"))  # the same on every node
        ClusterSharding(ctx.system).init(Entity(PER_ID, per_id_counter(name)))  # the same on every node
        counted = ctx.message_adapter(Counted)
        ctx.timers.start_timer_with_fixed_delay("tick", Tick(), 1.0)

        @Behaviors.receive_message
        def on_event(event: Event) -> Behavior[Event]:
            match event:
                case Tick():
                    proxy.tell(Hit(counted))  # buffered by the proxy until it has found the singleton
                    for entity_id in ("alice", "bob", "carol"):
                        ClusterSharding(ctx.system).entity_ref_for(PER_ID, entity_id).tell(CountUp(counted))
                case Counted(text):
                    print(f"[{name}] count {text}")
                case Listing(service_instances=refs):
                    print(f"[{name}] greeters: {sorted(r.path for r in refs)}")
                case MemberEvent(member=m):
                    print(f"[{name}] {type(event).__name__}: {m.unique_address.authority} is {m.status}")
                case ReachabilityEvent(member=m):
                    print(f"[{name}] {type(event).__name__}: {m.unique_address.authority}")
                case LeaderChanged(leader=leader):
                    print(f"[{name}] leader is now {leader.root if leader else None}")
                case _:
                    pass
            return Behaviors.same

        return on_event

    return setup


def node(name: str, port: int, seed_ports: list[int], bootstrap_ports: list[int]) -> ActorSystem[Event]:
    settings = ClusterSettings(
        seed_nodes=tuple(Address("cluster", HOST, p) for p in seed_ports),
        downing=SplitBrainResolverSettings(stable_after=5.0),  # keep-majority: a dead node is removed on its own
    )
    shutdown = CoordinatedShutdownSettings(run_on_signals=(signal.SIGINT, signal.SIGTERM))  # Pekko's JVM shutdown hook
    management = PekkoManagementSettings(hostname=HOST, port=port + 1000)  # health checks and /cluster/members on port + 1000
    if bootstrap_ports:  # --bootstrap: no seed nodes; the nodes find each other through discovery and probing, as on Kubernetes
        management = replace(
            management,
            discovery=ConfigServiceDiscovery({"cluster": [ResolvedTarget(HOST, p + 1000, HOST) for p in bootstrap_ports]}),
            bootstrap=ClusterBootstrapSettings(service_name="cluster", stable_margin=3.0, required_contact_point_nr=2, filter_on_fallback_port=False),
        )
    system = ActorSystem(listener(name), "cluster", address=(HOST, port), cluster=settings, shutdown=shutdown, management=management)
    Cluster(system).subscriptions.tell(Subscribe(system, ClusterDomainEvent))
    return system


async def main(name: str, port: int, seed_ports: list[int], bootstrap_ports: list[int]) -> None:
    async with node(name, port, seed_ports, bootstrap_ports) as system:
        cluster = Cluster(system)
        uri = await PekkoManagement(system).start()
        how = f"bootstrapping with {bootstrap_ports}" if bootstrap_ports else f"joining through {seed_ports}"
        print(f"[{name}] {cluster.self_address.root} {how}; management at {uri}; ctrl-c to leave")
        await system.when_terminated()  # ctrl-c runs the coordinated shutdown: shards hand off, the node leaves, singletons hand over


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    rest = sys.argv[3:]
    if rest[:1] == ["--bootstrap"]:
        asyncio.run(main(sys.argv[1], int(sys.argv[2]), [], [int(p) for p in rest[1:]]))
    else:
        asyncio.run(main(sys.argv[1], int(sys.argv[2]), [int(p) for p in rest], []))
