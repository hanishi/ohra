<p align="center"><img src="https://raw.githubusercontent.com/hanishi/ohra/main/docs/logo.png" width="160" alt="ohra logo: an ear of barley"></p>

# ohra

[![PyPI](https://img.shields.io/pypi/v/ohra)](https://pypi.org/project/ohra/)
[![CI](https://github.com/hanishi/ohra/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/hanishi/ohra/actions/workflows/ci.yml)
[![Python 3.12 | 3.13](https://img.shields.io/badge/python-3.12%20%7C%203.13-blue)](https://github.com/hanishi/ohra/blob/main/pyproject.toml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/hanishi/ohra/blob/main/LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with pyright](https://img.shields.io/badge/pyright-strict-informational)](https://microsoft.github.io/pyright/)
[![Dependencies: none](https://img.shields.io/badge/dependencies-none-brightgreen)](https://github.com/hanishi/ohra/blob/main/pyproject.toml)

Apache Pekko Typed actors on asyncio, for Python 3.12+. No dependencies. *Ohra* is Finnish for barley, the crop of Pekko, the Finnish god of barley and brewing.

ohra is an independent project, built on the design of Apache Pekko and, before it, Akka, with thanks to both communities. It follows their semantics and names closely, but it is not part of the Apache Software Foundation.

An actor is a function from a message to the next behavior. Typed refs make wrong messages a type error under pyright strict. Everything runs on one event loop, so a handler can `await` without losing the guarantee that an actor handles one message at a time. numpy work goes to threads and comes back as a message.

```python
import asyncio
from dataclasses import dataclass
from ohra import ActorRef, ActorSystem, Behavior, Behaviors


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
                return counter(total + by)  # state is the next behavior
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

If you know Apache Pekko or Akka Typed, the names are the same in snake_case, and the semantics are checked against the Apache Pekko source. If you don't, [the book](https://github.com/hanishi/ohra/blob/main/docs/book/README.md) explains everything from the first actor to a sharded web app on Kubernetes.

## Install

```
pip install ohra               # the library
pip install 'ohra[numpy]'      # plus numpy, for ohra.ext.shm and the examples that use it
pip install 'ohra[web]'        # plus FastAPI and uvicorn, for ohra.ext.fastapi and the fastapi_* examples
pip install 'ohra[postgres]'   # plus psycopg, for ohra.ext.postgres, the durable state store for clusters
pip install 'ohra[torch]'      # plus PyTorch, for the purchase predictor example
```

The examples and tests live in the repository, not the package. To run them, clone it and install it in editable mode, with the `dev` extra for pytest, pytest-asyncio, pyright, httpx and ruff:

```
git clone https://github.com/hanishi/ohra.git && cd ohra
pip install -e '.[dev,numpy,web,postgres,torch]'   # or: uv sync --all-extras
```

## The book

[How ohra works](https://github.com/hanishi/ohra/blob/main/docs/book/README.md) is the guide and the reference, one chapter per layer, with diagrams:

1. [Actors](https://github.com/hanishi/ohra/blob/main/docs/book/01-actors.md): behaviors, ask, children and DeathWatch, supervision, timers and stash, routers, the runtime model
2. [The actor system](https://github.com/hanishi/ohra/blob/main/docs/book/02-actor-system.md): guardians, extensions, the receptionist, coordinated shutdown
3. [Persistence](https://github.com/hanishi/ohra/blob/main/docs/book/03-persistence.md): write, then install, then reply; recovery in one read
4. [Remoting](https://github.com/hanishi/ohra/blob/main/docs/book/04-remoting.md): addresses, `@remotable` messages, connections
5. [Cluster membership](https://github.com/hanishi/ohra/blob/main/docs/book/05-cluster.md): gossip, the member lifecycle, failure detection, the split-brain resolver
6. [Cluster singleton](https://github.com/hanishi/ohra/blob/main/docs/book/06-singleton.md): one actor on the oldest member, and the hand-over
7. [Sharding](https://github.com/hanishi/ohra/blob/main/docs/book/07-sharding.md): regions, shards and the coordinator, in one process and across nodes
8. [Management and Kubernetes](https://github.com/hanishi/ohra/blob/main/docs/book/08-management-and-kubernetes.md): health checks, bootstrap, discovery, running on Docker Desktop
9. [FastAPI](https://github.com/hanishi/ohra/blob/main/docs/book/09-fastapi.md): the actor system owned by the app, routes that ask actors
10. [numpy and shared memory](https://github.com/hanishi/ohra/blob/main/docs/book/10-numpy-and-shared-memory.md): blocking work in a thread, large arrays to another process without a copy
11. [Putting it together](https://github.com/hanishi/ohra/blob/main/docs/book/11-putting-it-together.md): one cart request through every layer

Appendices: [Apache Pekko names](https://github.com/hanishi/ohra/blob/main/docs/book/appendix-pekko-names.md), [known deviations from Apache Pekko](https://github.com/hanishi/ohra/blob/main/docs/book/appendix-deviations.md), [performance](https://github.com/hanishi/ohra/blob/main/docs/book/appendix-performance.md), and a [glossary](https://github.com/hanishi/ohra/blob/main/docs/book/glossary.md).

## Examples

| File | Shows |
|---|---|
| `examples/counter.py` | the functional style, `ask`, supervision with `resume` |
| `examples/chatroom.py` | the ChatRoom from the Apache Pekko docs: sessions as children, narrowed refs, DeathWatch |
| `examples/remoting/` | two processes: `greeter_node.py` listens on 2552, `client_node.py` resolves it by path, tells and asks across the wire |
| `examples/cluster/` | `node.py NAME PORT [SEED_PORT ...]`, or `node.py NAME PORT --bootstrap PORT...` to form the cluster with no seeds through Pekko's bootstrap: start three in three terminals, watch them join, go Up and elect a leader, each node's greeter appear in every node's Receptionist listing, one counter singleton answer all of them, and the sharded counters for three ids spread over the nodes; ctrl-c a node, which leaves gracefully, and the singleton and the shards it hosted move to the others; management on port + 1000 (`curl localhost:3551/cluster/members`) |
| `examples/k8s/` | `node.py`, `Dockerfile` and `cluster.yaml`: a three-pod cluster on Docker Desktop Kubernetes that finds itself through the API server and spreads a simulated calculation over the pods with sharding; delete a pod and its tasks move, scale up and the new pods take shards; see [Running on Docker Desktop Kubernetes](https://github.com/hanishi/ohra/blob/main/docs/book/08-management-and-kubernetes.md#running-on-docker-desktop-kubernetes) |
| `examples/k8s/carts.py` | the FastAPI carts on a three-pod cluster: any pod serves any cart, each cart lives on one pod, persisted in Postgres, and moves with its items when that pod goes; `carts.yaml`, see [Carts](https://github.com/hanishi/ohra/blob/main/docs/book/08-management-and-kubernetes.md#carts-cluster-sharding-behind-a-web-app) |
| `examples/persisted_counter.py` | `Behaviors.persisted` with `SqliteStore`: the total survives a supervised restart and a new actor system; run it twice and it keeps counting |
| `examples/database_client.py` | backoff restarts of a failing setup, a stash that outlives restarts, timers, `ctx.ask` |
| `examples/interaction_patterns/` | one runnable file per pattern in Apache Pekko's [Interaction Patterns](https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html) page: fire and forget, request-response, adapted response, ask between actors, ask from outside, generic response wrapper, ignoring replies, pipe to self, per-session child, aggregator, tail chopping, scheduling to self |
| `examples/fastapi_carts.py` | a FastAPI service over actors: one entity per cart id through `Sharding`, persisted in SQLite with `Behaviors.persisted` so carts survive passivation and restarts, typed `ask` from routes to entities |
| `examples/fastapi_chatroom.py` | the chat room over WebSockets: one connection actor per socket with an async handler that writes to it, rooms created on demand, and DeathWatch turning a closed socket into a "left" notice; serves a browser page at `/` |
| `examples/fastapi_online_learning.py` | a feedback loop behind FastAPI: predictions micro-batched into one numpy call, outcomes posted back, a learner actor taking gradient steps in a thread and hot-swapping the model; serves a playground page at `/`, and `python examples/fastapi_online_learning.py` runs the server with a demo client |
| `examples/fastapi_purchase_predictor.py` | the same loop with PyTorch: a model of which shopper buys which product, learned from simulated shoppers with hidden tastes; the learner trains its own copy in a thread and sends the predictor copies of the weights; the page at `/` shows the true tastes beside the model's predictions, accuracy over time, and a button that shifts the tastes so you can watch the model catch up. Needs the `web` and `torch` extras |
| `examples/bandit.py` | Thompson sampling over sharded arms: Beta posteriors as arrays, one vectorized draw per shard, `ctx.ask` fan-out, a progress timer |
| `examples/benchmark.py` | message throughput, round-trip latency and spawn cost of the runtime; the source of the [Performance](https://github.com/hanishi/ohra/blob/main/docs/book/appendix-performance.md) table |
| `examples/compute.py` | numpy in threads via `pipe_to_self(asyncio.to_thread(...))`, with a latency probe |
| `examples/batching.py` | `batching` throughput and p99 latency curves |
| `examples/population.py` | 100k single-arm actors against 16 shards behind `Routers.pool` |
| `examples/shared_memory.py` | `SharedArray` against pickling for a 400MB array |

The last four need numpy: `uv run --extra numpy python examples/bandit.py`. The `fastapi_*` examples need the `web` extra and run under uvicorn: `uv run --extra web uvicorn examples.fastapi_carts:app --port 8000`.

## Development

Layout follows Apache Pekko's modules: `ohra` is the actor core, `ohra.remote` the transport and serialization, `ohra.cluster` membership and the singleton, `ohra.cluster.sharding` the Shard, region and coordinator, `ohra.ext` the FastAPI, Postgres and shared-memory integrations. Every public name is importable from `ohra` and from its own subpackage.

```
pytest      # every test fails if any asyncio task outlives it
pyright     # strict; tests/typing_check.py asserts type errors that must occur
```

See [CONTRIBUTING.md](https://github.com/hanishi/ohra/blob/main/CONTRIBUTING.md) for setup, the runtime invariants and how to port from Apache Pekko. Report vulnerabilities as described in [SECURITY.md](https://github.com/hanishi/ohra/blob/main/SECURITY.md).

## License

Apache-2.0, the same as Apache Pekko. The chat room and interaction-pattern examples are derived from the Apache Pekko documentation; see [NOTICE](https://github.com/hanishi/ohra/blob/main/NOTICE).

ohra is an independent project, not affiliated with or endorsed by the Apache Software Foundation or the Apache Pekko project. Apache Pekko is a trademark of the Apache Software Foundation.
