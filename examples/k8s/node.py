"""A cluster node for Kubernetes: the pods find each other through the API server and Pekko's bootstrap, and spread a
simulated calculation over the cluster with sharding.

Each pod hosts a sharded "calculator" entity type. Every second each pod submits one task, with a rotating id, to
`ClusterSharding.entity_ref_for`; whichever pod owns the task's shard computes it (the sum of the primes below n) and
replies with the result and its own pod name, which the submitter prints:

    task 17 (n=60000) = 157_016_522 on cluster-7d9f6c9b8-xk2p

Member events are printed too, so the logs show the pods joining, going Up and leaving. Deleting a pod runs the
coordinated shutdown (SIGTERM): its shards hand off and its tasks continue on the others. The pod IP and name come
from the downward API (POD_IP, POD_NAME); discovery needs the pods labelled app=cluster and a service account that
may list them. See docs/book/08-management-and-kubernetes.md.
"""
import asyncio
import os
import signal
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import (  # noqa: E402
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Cluster,
    ClusterBootstrapSettings,
    ClusterDomainEvent,
    ClusterSettings,
    ClusterSharding,
    CoordinatedShutdownSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    KubernetesApiServiceDiscovery,
    KubernetesApiSettings,
    LeaderChanged,
    MemberEvent,
    PekkoManagement,
    PekkoManagementSettings,
    ReachabilityEvent,
    SplitBrainResolverSettings,
    Subscribe,
    remotable,
)

REMOTING_PORT = 2551
MANAGEMENT_PORT = 7626
TASKS = 20  # task ids rotate over this many entities, so they spread over the shards


@remotable
@dataclass(frozen=True)
class Result:
    task_id: int
    n: int
    value: int
    pod: str


@remotable
@dataclass(frozen=True)
class Compute:
    n: int
    reply_to: ActorRef[Result]


@dataclass(frozen=True)
class Tick:
    pass


CALCULATOR = EntityTypeKey[Compute]("calculator")

type Event = ClusterDomainEvent | Result | Tick


def sum_of_primes_below(n: int) -> int:
    """The simulated work: a sieve, a few tens of milliseconds for n around 100 000."""
    sieve = bytearray([1]) * n
    sieve[:2] = b"\x00\x00"
    for i in range(2, int(n**0.5) + 1):
        if sieve[i]:
            sieve[i * i :: i] = bytes(len(range(i * i, n, i)))
    return sum(i for i in range(n) if sieve[i])


def calculator(pod: str) -> Callable[[EntityContext[Compute]], Behavior[Compute]]:
    """A sharded entity: created on its first task, on whichever pod owns its shard."""

    def create(entity: EntityContext[Compute]) -> Behavior[Compute]:
        @Behaviors.receive_message
        def on_compute(msg: Compute) -> Behavior[Compute]:
            msg.reply_to.tell(Result(int(entity.entity_id), msg.n, sum_of_primes_below(msg.n), pod))
            return Behaviors.same

        return on_compute

    return create


def submitter(pod: str) -> Behavior[Event]:
    """Submits one task per second and prints the results and the member events."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Event]) -> Behavior[Event]:
        ClusterSharding(ctx.system).init(Entity(CALCULATOR, calculator(pod)))  # the same on every pod
        ctx.timers.start_timer_with_fixed_delay("tick", Tick(), 1.0)
        next_task = 0

        @Behaviors.receive_message
        def on_event(event: Event) -> Behavior[Event]:
            nonlocal next_task
            match event:
                case Tick():
                    task_id, next_task = next_task, (next_task + 1) % TASKS
                    n = 50_000 + task_id * 5_000
                    ClusterSharding(ctx.system).entity_ref_for(CALCULATOR, str(task_id)).tell(Compute(n, ctx.self))
                case Result(task_id, n, value, on):
                    print(f"task {task_id} (n={n}) = {value:_} on {on}")
                case MemberEvent(member=m):
                    print(f"{type(event).__name__}: {m.unique_address.authority} is {m.status}")
                case ReachabilityEvent(member=m):
                    print(f"{type(event).__name__}: {m.unique_address.authority}")
                case LeaderChanged(leader=leader):
                    print(f"leader is now {leader.root if leader else None}")
                case _:
                    pass
            return Behaviors.same

        return on_event

    return setup


async def main(pod: str, ip: str) -> None:
    settings = ClusterSettings(downing=SplitBrainResolverSettings(stable_after=5.0))  # no seed nodes: the bootstrap joins
    shutdown = CoordinatedShutdownSettings(run_on_signals=(signal.SIGINT, signal.SIGTERM))  # a deleted pod leaves gracefully
    management = PekkoManagementSettings(
        hostname=ip,
        port=MANAGEMENT_PORT,
        bind_hostname="0.0.0.0",  # the probes and the peers come in on the pod IP, kubectl port-forward on localhost
        discovery=KubernetesApiServiceDiscovery(KubernetesApiSettings(use_raw_ip=True)),
        bootstrap=ClusterBootstrapSettings(service_name="cluster", port_name="management", required_contact_point_nr=2),
    )
    async with ActorSystem(submitter(pod), "cluster", address=(ip, REMOTING_PORT), cluster=settings, shutdown=shutdown, management=management) as system:
        Cluster(system).subscriptions.tell(Subscribe(system, ClusterDomainEvent))
        uri = await PekkoManagement(system).start()
        print(f"{pod}: {Cluster(system).self_address.root} bootstrapping; management at {uri}", flush=True)
        await system.when_terminated()


if __name__ == "__main__":
    pod, ip = os.environ.get("POD_NAME"), os.environ.get("POD_IP")
    if not pod or not ip:
        sys.exit("POD_NAME and POD_IP must be set (the downward API does so in examples/k8s/cluster.yaml)")
    asyncio.run(main(pod, ip))
