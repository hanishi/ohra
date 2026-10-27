# 2. The actor system

`ActorSystem(guardian, name)` creates two root cells. `/user` runs your guardian behavior, and the system itself is a ref to it. `/system` hosts the infrastructure: the receptionist, the cluster daemon, the split-brain resolver, singleton managers and sharding regions. Your actors and the machinery never share a parent, so stopping one tree does not tear the other mid-step.

**Extensions.** `Cluster(system)`, `ClusterSharding(system)`, `ClusterSingleton(system)`, `Receptionist(system)` and `PekkoManagement(system)` each return the one instance for that system, as Pekko's extensions do. The constructor's arguments decide which exist: `address=` turns on remoting, `cluster=` the cluster, `management=` the management endpoint.

## Receptionist

Actors find each other through the Receptionist, Apache Pekko's service registry: a service registers under a `ServiceKey`, and anyone asks `Find` for the current refs under that key or `Subscribe`s to be sent a `Listing` now and after every change. Every system starts one; on a cluster node the registry is replicated, so a service registered on one node is found on the others as a `RemoteRef`. Entries follow their node: they vanish when the service stops or the member is removed, and while a node is unreachable its services drop from `service_instances` but stay in `all_service_instances`.

```python
from pekko_py import Find, Listing, Receptionist, Register, ServiceKey
from pekko_py.receptionist import Subscribe  # the cluster's Subscribe is the top-level one

GREETER = ServiceKey[Greet]("greeter")


def greeter_node(ctx: ActorContext[Listing[Greet]]) -> Behavior[Listing[Greet]]:
    Receptionist(ctx.system).ref.tell(Register(GREETER, ctx.spawn(greeter, "greeter")))
    # a Listing now, then one per change
    Receptionist(ctx.system).ref.tell(Subscribe(GREETER, ctx.self))
    return Behaviors.receive_message(
        lambda listing: print(sorted(r.path for r in listing.service_instances)) or Behaviors.same
    )


def greeter_system(port: int) -> ActorSystem[Listing[Greet]]:
    seeds = (Address("app", "127.0.0.1", 2551),)
    return ActorSystem(
        Behaviors.setup(greeter_node),
        "app",
        address=("127.0.0.1", port),
        cluster=ClusterSettings(seed_nodes=seeds),
    )


async def main() -> None:
    async with greeter_system(2551) as first, greeter_system(2552) as second:
        await until(lambda: len(Cluster(second).state.members) == 2)
        listing = await ask(Receptionist(second).ref, lambda r: Find(GREETER, r), timeout=1)
        # one local ref, one RemoteRef to the greeter on the first node
        for ref in listing.service_instances:
            print(await ask(ref, lambda r: Greet("pekko", r), timeout=1))
```

Only local actors can register or subscribe, as in Pekko; the receptionist watches them, message adapters included. A `Find` may come from another node: `Listing` and the replies are `@remotable`. Replication is one versioned row per node, pushed to the other members on every change and re-sent every `receptionist_resend_interval`, instead of Pekko's distributed data; see the deviations.

## Coordinated shutdown

`terminate()` does not just stop the guardian. It runs Pekko's twelve phases in order, each with a timeout, and each layer adds its tasks to the phase where it belongs:

| Phase | Who adds work there |
| --- | --- |
| `before-service-unbind` to `service-stop` | your application, for example stop taking HTTP requests |
| `cluster-sharding-shutdown-region` | every sharding region hands off its shards |
| `cluster-leave` | the cluster sends Leave for this node |
| `cluster-exiting` | the cluster waits for Exiting; singletons hand over |
| `cluster-exiting-done`, `cluster-shutdown` | the cluster waits for removal and stops its daemon |
| `actor-system-terminate` | stops `/user`, then `/system`, then closes remoting |

The same sequence runs when a node is downed, when the guardian stops on its own, and, if you opt in with `run_on_signals`, on SIGTERM. A task that outlives its phase is cancelled and the run moves on, so a stuck hand-off cannot hold the process forever.

### Using it

`ActorSystem.terminate()` runs Apache Pekko's CoordinatedShutdown: a fixed sequence of phases, each bounded by a timeout, to which you add tasks. On a cluster node the cluster adds its own: `cluster-sharding-shutdown-region` hands the node's shards off, `cluster-leave` sends `Leave`, `cluster-exiting` waits for the leader to move the node to Exiting and is where singletons hand over, `cluster-exiting-done` confirms the exit, `cluster-shutdown` waits for the cluster daemon, and `actor-system-terminate` stops the actors. The same run happens when another node sends `Leave` for this one, when this node is downed, and, if you opt in, on a signal.

```python
import signal

from pekko_py import CoordinatedShutdown, CoordinatedShutdownSettings

system = ActorSystem(
    guardian,
    "app",
    shutdown=CoordinatedShutdownSettings(run_on_signals=(signal.SIGINT, signal.SIGTERM)),
)


async def drain() -> None:
    await server.close()  # stop taking requests before the node leaves the cluster


CoordinatedShutdown(system).add_task("service-unbind", "stop-http", drain)
# ctrl-c or SIGTERM runs the phases: user tasks, hand-offs, leave, stop
await system.when_terminated()
```

The phases, in order: `before-service-unbind`, `service-unbind`, `service-requests-done`, `service-stop`, `before-cluster-shutdown`, `cluster-sharding-shutdown-region`, `cluster-leave`, `cluster-exiting`, `cluster-exiting-done`, `cluster-shutdown`, `before-actor-system-terminate`, `actor-system-terminate`. Tasks in one phase run concurrently; a task that fails is logged and the run continues, as Pekko's `recover = on`. A phase that outlives its timeout (`default_phase_timeout` 5 s; 10 s for the region, exiting and terminate phases; `phase_timeouts` overrides) is logged and its still-running tasks are cancelled, so a task must tolerate `CancelledError` at any await; Pekko lets such a task run on. `terminate()` returns at once, as Pekko's does: messages told after it may still be processed, so await `when_terminated()`. A guardian that stops on its own also runs the phases, after its actors are gone. `run(reason)` starts the run by hand; `shutdown_reason` says who did. A node that terminates while another member is unreachable and not downed waits out `cluster-exiting`, since the leader cannot move it to Exiting; the split-brain resolver below removes that wait.

---

[← Actors](01-actors.md) · [Contents](README.md) · [Persistence →](03-persistence.md)
