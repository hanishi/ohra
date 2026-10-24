"""A cluster node: joins through the seed nodes, registers a greeter service, runs a cluster-wide singleton
counter, and prints every member event, every change of the greeter listing, and the count it gets back from the
singleton each second. The listing on each node holds refs to the greeters on all nodes; the counter runs on the
oldest node only and moves when that node stops.

    uv run python examples/cluster/node.py NAME PORT [SEED_PORT ...]

    uv run python examples/cluster/node.py one 2551 2551          # terminal 1: the first seed forms the cluster
    uv run python examples/cluster/node.py two 2552 2551          # terminal 2: joins through 2551
    uv run python examples/cluster/node.py three 2553 2551 2552   # terminal 3: either seed will do

Every node must run under the same system name, so NAME is only used for the log lines; the system is "cluster".
Ctrl-c makes a node leave the cluster gracefully: its greeter leaves every listing and, if it hosted the counter,
the counter is handed over before the node exits and starts again at 1 on the next oldest node. Kill a node
instead (kill -9) and the others report it unreachable until you `Down` it.
"""
import asyncio
import logging
import signal
import sys
from dataclasses import dataclass
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
    ClusterSingleton,
    LeaderChanged,
    Leave,
    Listing,
    MemberEvent,
    ReachabilityEvent,
    Receptionist,
    Register,
    ServiceKey,
    SingletonActor,
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


type Event = ClusterDomainEvent | Listing[Greet] | Tick | Counted


def counter(node_name: str) -> Behavior[Hit]:
    """The singleton: one of these runs in the whole cluster, on the oldest node."""

    def setup(ctx: ActorContext[Hit]) -> Behavior[Hit]:
        count = 0
        print(f"[{node_name}] counter singleton started here")

        def on_hit(msg: Hit) -> Behavior[Hit]:
            nonlocal count
            count += 1
            msg.reply_to.tell(f"{count} (counted on {node_name})")
            return Behaviors.same

        return Behaviors.receive_message(on_hit)

    return Behaviors.setup(setup)


def greeter(node_name: str) -> Behavior[Greet]:
    def on_greet(msg: Greet) -> Behavior[Greet]:
        msg.reply_to.tell(f"hello {msg.name} from {node_name}")
        return Behaviors.same

    return Behaviors.receive_message(on_greet)


def listener(name: str) -> Behavior[Event]:
    """Registers this node's greeter, then prints what the cluster and the receptionist tell it, as the
    ClusterListener in the Apache Pekko docs."""

    def setup(ctx: ActorContext[Event]) -> Behavior[Event]:
        receptionist = Receptionist(ctx.system).ref
        receptionist.tell(Register(GREETER, ctx.spawn(greeter(name), "greeter")))
        receptionist.tell(SubscribeToKey(GREETER, ctx.self))
        proxy = ClusterSingleton(ctx.system).init(SingletonActor(counter(name), "counter"))  # the same on every node
        counted = ctx.message_adapter(Counted)
        ctx.timers.start_timer_with_fixed_delay("tick", Tick(), 1.0)

        def on_event(event: Event) -> Behavior[Event]:
            match event:
                case Tick():
                    proxy.tell(Hit(counted))  # buffered by the proxy until it has found the singleton
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

        return Behaviors.receive_message(on_event)

    return Behaviors.setup(setup)


def node(name: str, port: int, seed_ports: list[int]) -> ActorSystem[Event]:
    settings = ClusterSettings(seed_nodes=tuple(Address("cluster", HOST, p) for p in seed_ports))
    system = ActorSystem(listener(name), "cluster", address=(HOST, port), cluster=settings)
    Cluster(system).subscriptions.tell(Subscribe(system, ClusterDomainEvent))
    return system


async def main(name: str, port: int, seed_ports: list[int]) -> None:
    async with node(name, port, seed_ports) as system:
        cluster = Cluster(system)
        print(f"[{name}] {cluster.self_address.root} joining through {seed_ports}; ctrl-c to leave")
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: cluster.manager.tell(Leave(cluster.self_address)))  # as CoordinatedShutdown's JVM hook
        await system.when_terminated()  # the exiting phase hands the singleton over, then the node terminates itself


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    asyncio.run(main(sys.argv[1], int(sys.argv[2]), [int(p) for p in sys.argv[3:]]))
