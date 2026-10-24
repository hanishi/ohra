"""A cluster node: joins through the seed nodes, registers a greeter service, and prints every member event and
every change of the greeter listing it sees. The listing on each node holds refs to the greeters on all nodes.

    uv run python examples/cluster/node.py NAME PORT [SEED_PORT ...]

    uv run python examples/cluster/node.py one 2551 2551          # terminal 1: the first seed forms the cluster
    uv run python examples/cluster/node.py two 2552 2551          # terminal 2: joins through 2551
    uv run python examples/cluster/node.py three 2553 2551 2552   # terminal 3: either seed will do

Every node must run under the same system name, so NAME is only used for the log lines; the system is "cluster".
Stop a node with ctrl-c and the others report it unreachable and drop its greeter from `service_instances`; a
node that is `Down`ed or that leaves is removed, and its greeter leaves the listing for good.
"""
import asyncio
import logging
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
    LeaderChanged,
    Listing,
    MemberEvent,
    ReachabilityEvent,
    Receptionist,
    Register,
    ServiceKey,
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

type Event = ClusterDomainEvent | Listing[Greet]


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

        def on_event(event: Event) -> Behavior[Event]:
            match event:
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
        print(f"[{name}] {Cluster(system).self_address.root} joining through {seed_ports}; ctrl-c to stop")
        await system.when_terminated()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    asyncio.run(main(sys.argv[1], int(sys.argv[2]), [int(p) for p in sys.argv[3:]]))
