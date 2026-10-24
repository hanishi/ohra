"""A cluster node: joins through the seed nodes, then prints every member event it sees.

    uv run python examples/cluster/node.py NAME PORT [SEED_PORT ...]

    uv run python examples/cluster/node.py one 2551 2551          # terminal 1: the first seed forms the cluster
    uv run python examples/cluster/node.py two 2552 2551          # terminal 2: joins through 2551
    uv run python examples/cluster/node.py three 2553 2551 2552   # terminal 3: either seed will do

Every node must run under the same system name, so NAME is only used for the log lines; the system is "cluster".
Stop a node with ctrl-c and the others report it unreachable; a node that is `Down`ed or that leaves is removed.
"""
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import (  # noqa: E402
    ActorSystem,
    Address,
    Behavior,
    Behaviors,
    Cluster,
    ClusterDomainEvent,
    ClusterSettings,
    LeaderChanged,
    MemberEvent,
    ReachabilityEvent,
    Subscribe,
)

HOST = "127.0.0.1"


def listener(name: str) -> Behavior[ClusterDomainEvent]:
    """Prints what the cluster tells it, as the ClusterListener in the Apache Pekko docs."""

    def on_event(event: ClusterDomainEvent) -> Behavior[ClusterDomainEvent]:
        match event:
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


def node(name: str, port: int, seed_ports: list[int]) -> ActorSystem[ClusterDomainEvent]:
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
