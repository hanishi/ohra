"""ohra's soak for the Distributed Data samples: N nodes in one process, random load, forever.

``soak.py SAMPLE [--nodes 5] [--rate 100] [--report-every 5] [--check-every 30]
[--duration SECONDS] [--seed N] [--check-timeout 30]``, where ``SAMPLE`` is ``voting``,
``cart``, ``cache`` or ``metrics``. Not a port: Pekko's sample project has no soak.

It runs N ``ActorSystem``s on one event loop, joined into one cluster over TCP, with the sample's
actor on each, and sends random commands at ``--rate`` per second to random nodes. Each node's
guardian subscribes to the sample's keys, so the soak holds every node's view of every key.

- **Lag.** When node A's view changes at time t, ``(t, A's view)`` goes on a queue for each other
  node B that has not caught up with it yet. B has caught up with it when, for every key of it,
  merging A's data into B's changes nothing a reader of B sees:
  ``observed(merge(B's data, A's data)) == observed(B's data)``. A's views only grow, so each
  queue is popped from its head while B has caught up, and the lag of an entry is the time B
  first caught up minus t. A view B already holds, such as one A got from B, is no lag: counted,
  it would add a 0 for every relay and pull the percentiles down. Only the lags under the load
  count: the soak waits for the views to converge after startup and clears the queues before the
  load starts. The run's percentiles come from a uniform sample of ``LAG_SAMPLE_SIZE`` lags; its
  count and max are exact.
- **Check.** Every ``--check-every`` seconds the load stops (the metrics' measure freezes at its
  last values), the soak waits until every node's view is equal, asks every node's actor through
  the sample's own query, and compares the answers with each other and with what the converged
  views imply. It passes only if they agree and the views held still while it asked; otherwise it
  tries again, every 10 ms, and reports a VIOLATION from its last try at ``--check-timeout``. A
  command still in flight when the load stopped, such as a cart's remove (a read, then a write),
  can land after the views converged, so an answer may be ahead of the views for a moment.

Ctrl-C (SIGINT or SIGTERM), or the end of ``--duration``, runs one last check, prints a summary
and terminates every system: ``main`` turns the signal into ``run``'s ``stop`` event. During
startup it skips the check; after a Ctrl-C the last check waits at most ``STOP_CHECK_TIMEOUT``.
A second Ctrl-C cancels ``run``. A cancellation of ``run`` is not a stop: the systems terminate
and it propagates, so a caller's timeout bounds the soak. Every random choice comes from one
``random.Random`` seeded with ``--seed``.

**Containment and convergence compare what a reader sees, not whole-data equality.** The spec
asks for ``merge(b, a) == b``; the soak tests ``observed(merge(b, a)) == observed(b)``, and its
checks wait for ``observed`` to be equal on every node. ``observed(data)`` is the entries of an
``LWWMap`` or a ``PNCounterMap``, else the data. The reason is the ORSet under those maps' keys:
merge subtracts from each element's dot the versions the other side's version vector has seen,
so a state built from deltas and the same state built from full merges hold different dots for
the same elements, and are not ``==`` until full-state gossip has merged them alike. Pekko's
ORSet does the same (``ORSet.scala``, ``mergeCommonKeys``, lines 198-258, and the add delta at
365). With whole-data equality the lags would measure the 2 s gossip interval, not the
replication. The metrics sample also rewrites its entries with fresh timestamps on every tick,
frozen or not, so its whole data never settles at all.
"""

import argparse
import asyncio
import math
import random
import reprlib
import signal
import statistics
import sys
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass, field
from typing import Any, cast

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Cluster,
    ClusterSettings,
    EventStream,
    Join,
    ask,
)
from ohra.cluster.ddata import (
    DistributedData,
    FlagKey,
    Key,
    LWWMap,
    LWWMapKey,
    PNCounterMap,
    ReplicatedData,
    Replicator,
    ReplicatorSettings,
)
from ohra.testkit.multi_node_spec import free_port
from replicated_cache import Cached, Evict, GetFromCache, PutInCache, data_key, replicated_cache
from replicated_metrics import (
    MAX_HEAP_KEY,
    USED_HEAP_KEY,
    UsedHeap,
    replicated_metrics,
)
from shopping_cart import AddItem, Cart, GetCart, LineItem, RemoveItem, cart_key, shopping_cart
from shopping_cart import Command as CartCommand
from voting_service import (
    CLOSED_KEY,
    COUNTERS_KEY,
    OPENED_KEY,
    GetVotes,
    Open,
    Vote,
    Votes,
    voting_service,
)

NOTIFY_SUBSCRIBERS_INTERVAL = 0.01
ASK_TIMEOUT = 5.0  # above the samples' own read timeouts of 3 s
STARTUP_TIMEOUT = 30.0
STOP_CHECK_TIMEOUT = 5.0  # the last check's, after a Ctrl-C
LAG_SAMPLE_SIZE = 100_000  # the run's percentiles come from a uniform sample of this many lags

type View = dict[Key[Any], ReplicatedData]


@dataclass
class SoakState:
    """What the workloads remember across commands."""

    rng: random.Random
    measure_interval: float
    """The metrics' ``measure_interval``: ``nodes / rate``."""
    refs: list[ActorRef[Any]] = field(default_factory=list[ActorRef[Any]])
    """Each node's sample actor, by node index."""
    touched: set[str] = field(default_factory=set[str])
    """The cache keys touched so far."""
    frozen: bool = True
    """No load: the soak sets it while it starts and while it checks."""
    used_heap: dict[ActorRef[Any], UsedHeap] = field(default_factory=dict[ActorRef[Any], UsedHeap])
    """The last ``UsedHeap`` each node's metrics actor published, by its ref."""
    commands: int = 0


def observed(data: ReplicatedData) -> object:
    """What a reader of ``data`` sees, which is what the lags and the checks compare."""
    if isinstance(data, LWWMap | PNCounterMap):
        return data.entries  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    return data


@dataclass(frozen=True)
class Workload:
    spawn: Callable[[ActorContext[Any], SoakState], ActorRef[Any]]
    keys: Callable[[SoakState], Sequence[Key[Any]]]
    command: Callable[[random.Random, SoakState], Any] | None
    """A random command for a random node; ``None`` when the sample loads itself (metrics)."""
    query: Callable[[ActorRef[Any], SoakState], Awaitable[object]]
    """Asks the actor; returns a comparable answer."""
    expected: Callable[[View, SoakState], object]
    """The answer every actor must give, from a converged view."""
    start: Callable[[SoakState], Awaitable[None]] | None = None
    """Runs once the cluster is up, before the load."""


# voting


async def _votes(ref: ActorRef[Any]) -> Votes:
    return await ask(ref, GetVotes, ASK_TIMEOUT)


async def _open_voting(state: SoakState) -> None:
    state.refs[0].tell(Open())
    async with asyncio.timeout(STARTUP_TIMEOUT):
        while not all(v.open for v in await asyncio.gather(*map(_votes, state.refs))):
            await asyncio.sleep(0.1)


async def _query_votes(ref: ActorRef[Any], state: SoakState) -> object:
    votes = await _votes(ref)
    return votes.result, votes.open


def _get[A: ReplicatedData](view: View, key: Key[A]) -> A | None:
    return cast(A | None, view.get(key))


def _expected_votes(view: View, state: SoakState) -> object:
    def enabled(key: FlagKey) -> bool:
        flag = _get(view, key)
        return flag is not None and flag.enabled

    counters = _get(view, COUNTERS_KEY)
    result = {} if counters is None else counters.entries
    return result, enabled(OPENED_KEY) and not enabled(CLOSED_KEY)


VOTING = Workload(
    spawn=lambda ctx, state: ctx.spawn(voting_service(), "voting"),
    keys=lambda state: [OPENED_KEY, CLOSED_KEY, COUNTERS_KEY],
    command=lambda rng, state: Vote(f"#{rng.randint(1, 20)}"),
    query=_query_votes,
    expected=_expected_votes,
    start=_open_voting,
)

# cart

USERS = ("alice", "bob", "carol")


@dataclass(frozen=True)
class ToCart:
    user_id: str
    command: CartCommand


@dataclass(frozen=True)
class GetCartOf:
    user_id: str
    reply_to: ActorRef[Cart]


def carts() -> Behavior[ToCart | GetCartOf]:
    """One shopping cart per user, behind one ref."""

    @Behaviors.setup
    def setup(ctx: ActorContext[ToCart | GetCartOf]) -> Behavior[ToCart | GetCartOf]:
        by_user = {user: ctx.spawn(shopping_cart(user), user) for user in USERS}

        @Behaviors.receive_message
        def forward(msg: ToCart | GetCartOf) -> Behavior[ToCart | GetCartOf]:
            match msg:
                case ToCart(user, command):
                    by_user[user].tell(command)
                case GetCartOf(user, reply_to):
                    by_user[user].tell(GetCart(reply_to))
            return Behaviors.same

        return forward

    return setup


def _cart_command(rng: random.Random, state: SoakState) -> ToCart:
    user = rng.choice(USERS)
    product = rng.randint(1, 10)
    if rng.random() < 0.5:
        item = LineItem(f"product-{product}", f"Product {product}", rng.randint(1, 3))
        return ToCart(user, AddItem(item))
    return ToCart(user, RemoveItem(f"product-{product}"))


def _sorted_items(items: Sequence[LineItem] | frozenset[LineItem]) -> list[LineItem]:
    return sorted(items, key=lambda i: (i.product_id, i.title, i.quantity))


async def _query_carts(ref: ActorRef[Any], state: SoakState) -> object:
    async def cart(user: str) -> list[LineItem]:
        answer = await ask(ref, lambda r: GetCartOf(user, r), ASK_TIMEOUT)
        return _sorted_items(answer.items)

    return await asyncio.gather(*map(cart, USERS))


def _expected_carts(view: View, state: SoakState) -> object:
    def items(user: str) -> list[LineItem]:
        data = _get(view, cart_key(user))
        return [] if data is None else _sorted_items(list(data.entries.values()))

    return [items(user) for user in USERS]


CART = Workload(
    spawn=lambda ctx, state: ctx.spawn(carts(), "carts"),
    keys=lambda state: [cart_key(user) for user in USERS],
    command=_cart_command,
    query=_query_carts,
    expected=_expected_carts,
)

# cache

CACHE_KEYS = [f"key-{i}" for i in range(1000)]


def _cache_command(rng: random.Random, state: SoakState) -> PutInCache | Evict:
    key = rng.choice(CACHE_KEYS)
    state.touched.add(key)
    if rng.random() < 0.75:
        return PutInCache(key, f"value-{state.commands}")
    return Evict(key)


async def _query_cache(ref: ActorRef[Any], state: SoakState) -> object:
    async def get(key: str) -> Cached:
        return await ask(ref, lambda r: GetFromCache(key, r), ASK_TIMEOUT)

    answers = await asyncio.gather(*map(get, sorted(state.touched)))
    return [(c.key, c.value) for c in answers]


def _expected_cache(view: View, state: SoakState) -> object:
    def value(key: str) -> object:
        data = _get(view, data_key(key))
        return None if data is None else data.get(key)

    return [(key, value(key)) for key in sorted(state.touched)]


CACHE = Workload(
    spawn=lambda ctx, state: ctx.spawn(replicated_cache(), "cache"),
    # Subscribed at startup, so every map the load can touch, not only those touched so far
    keys=lambda state: sorted({data_key(key) for key in CACHE_KEYS}, key=str),
    command=_cache_command,
    query=_query_cache,
    expected=_expected_cache,
)

# metrics


def _spawn_metrics(ctx: ActorContext[Any], state: SoakState) -> ActorRef[Any]:
    last: tuple[int, int] | None = None

    def measure() -> tuple[int, int]:
        nonlocal last
        if last is None or not state.frozen:
            last = (state.rng.randint(1, 10**9), 2 * 10**9)
            if not state.frozen:
                state.commands += 1
        return last

    ref = ctx.spawn(replicated_metrics(state.measure_interval, measure=measure), "metrics")

    @Behaviors.receive_message
    def record(used_heap: UsedHeap) -> Behavior[UsedHeap]:
        state.used_heap[ref] = used_heap
        return Behaviors.same

    ctx.system.event_stream.tell(EventStream.Subscribe(UsedHeap, ctx.spawn(record, "used-heap")))
    return ref


async def _query_metrics(ref: ActorRef[Any], state: SoakState) -> object:
    # the last UsedHeap the node published; until it has published from the converged maps,
    # a tick or two, the check asks again
    used_heap = state.used_heap.get(ref)
    return None if used_heap is None else used_heap.percent_per_node


def _expected_metrics(view: View, state: SoakState) -> object:
    def entries(key: LWWMapKey[str, int]) -> dict[str, int]:
        data = _get(view, key)
        return {} if data is None else data.entries

    max_heap = entries(MAX_HEAP_KEY)
    return {
        key: value / max_heap[key] * 100.0
        for key, value in entries(USED_HEAP_KEY).items()
        if key in max_heap
    }


METRICS = Workload(
    spawn=_spawn_metrics,
    keys=lambda state: [USED_HEAP_KEY, MAX_HEAP_KEY],
    command=None,
    query=_query_metrics,
    expected=_expected_metrics,
)

WORKLOADS: dict[str, Workload] = {
    "voting": VOTING,
    "cart": CART,
    "cache": CACHE,
    "metrics": METRICS,
}


@dataclass(frozen=True)
class SoakResult:
    commands: int
    checks_passed: int
    checks_failed: int
    lags: list[float]
    """A uniform sample of the lags measured, in seconds: all of them up to
    ``LAG_SAMPLE_SIZE``."""


# the nodes


@dataclass(frozen=True)
class _Changed:
    rsp: Replicator.SubscribeResponse[Any]


def _guardian(
    index: int,
    workload: Workload,
    state: SoakState,
    ready: asyncio.Future[ActorRef[Any]],
    on_change: Callable[[int, Key[Any], ReplicatedData], None],
) -> Behavior[_Changed]:
    @Behaviors.setup
    def setup(ctx: ActorContext[_Changed]) -> Behavior[_Changed]:
        ready.set_result(workload.spawn(ctx, state))
        adapter = ctx.message_adapter(_Changed)
        replicator = DistributedData(ctx.system).replicator
        for key in workload.keys(state):
            replicator.tell(Replicator.Subscribe(key, adapter))

        @Behaviors.receive_message
        def receive(msg: _Changed) -> Behavior[_Changed]:
            match msg.rsp:
                case Replicator.Changed(key, data):
                    on_change(index, key, data)
                case _:
                    pass
            return Behaviors.same

        return receive

    return setup


# the measuring


class Lags:
    """Every node's view, and for each pair (A, B) the queue of A's views B has not caught up
    with. B has caught up with a view of A when merging it into B's view changes nothing a
    reader of B sees.

    The lags since the last report are ``period``. The run's are ``count`` and ``max``, exact,
    and ``sample``, a uniform sample of at most ``capacity`` (Algorithm R), so a run that goes
    on for days holds the same memory as one of minutes."""

    def __init__(self, nodes: int, rng: random.Random, capacity: int = LAG_SAMPLE_SIZE) -> None:
        self.views: list[View] = [{} for _ in range(nodes)]
        pairs = [(a, b) for a in range(nodes) for b in range(nodes) if a != b]
        self.queues: dict[tuple[int, int], deque[tuple[float, View]]] = {p: deque() for p in pairs}
        # per pair and key, the last (A's data, B's data, contained) compared: the data are
        # immutable, so a key that changed on neither node needs no new merge
        self._seen: dict[tuple[int, int], dict[Key[Any], tuple[object, object, bool]]] = {
            p: {} for p in pairs
        }
        self._rng = rng
        self._capacity = capacity
        self.period: list[float] = []
        self.count = 0
        self.max = 0.0
        self.sample: list[float] = []

    def reset(self) -> None:
        """Forget every queue and lag, the views kept."""
        for queue in self.queues.values():
            queue.clear()
        self.period = []
        self.count, self.max, self.sample = 0, 0.0, []

    def record(self, lag: float) -> None:
        self.period.append(lag)
        self.count += 1
        self.max = max(self.max, lag)
        if len(self.sample) < self._capacity:
            self.sample.append(lag)
        else:
            j = self._rng.randrange(self.count)
            if j < self._capacity:
                self.sample[j] = lag

    def changed(self, node: int, key: Key[Any], data: ReplicatedData, now: float) -> None:
        view = {**self.views[node], key: data}
        self.views[node] = view
        for (a, b), queue in self.queues.items():
            if a == node:
                # a longer queue's head is not caught up, so b cannot hold this later view; an
                # empty one may: a view a got from b, or from a node b heard from first, is no
                # wait of b's, and no lag
                if queue or not self._contains(a, b, view):
                    queue.append((now, view))
            elif b == node:
                self._catch_up(a, b, now)

    def _catch_up(self, a: int, b: int, now: float) -> None:
        queue = self.queues[(a, b)]
        while queue and self._contains(a, b, queue[0][1]):
            t, _ = queue.popleft()
            self.record(now - t)

    def _contains(self, a: int, b: int, view_a: View) -> bool:
        view_b = self.views[b]
        seen = self._seen[(a, b)]
        for key, data_a in view_a.items():
            data_b = view_b.get(key)
            if data_b is None:
                return False
            last = seen.get(key)
            if last is None or last[0] is not data_a or last[1] is not data_b:
                last = (data_a, data_b, observed(data_b.merge(data_a)) == observed(data_b))
                seen[key] = last
            if not last[2]:
                return False
        return True

    @property
    def behind(self) -> int:
        return sum(1 for queue in self.queues.values() if queue)


def _ms(seconds: float) -> str:
    return f"{seconds * 1000:.0f}ms"


def _percentiles(lags: list[float], *ps: int) -> list[str]:
    if not lags:
        return ["-" for _ in ps]
    if len(lags) == 1:
        return [_ms(lags[0]) for _ in ps]
    q = statistics.quantiles(lags, n=100, method="inclusive")
    return [_ms(q[p - 1]) for p in ps]


def _nodes(indexes: Sequence[int]) -> str:
    return ", ".join(str(i) for i in indexes)


async def run(
    sample: str,
    nodes: int = 5,
    rate: float = 100.0,
    report_every: float = 5.0,
    check_every: float = 30.0,
    duration: float | None = None,
    seed: int | None = None,
    check_timeout: float = 30.0,
    out: Callable[[str], None] = print,
    *,
    stop: asyncio.Event | None = None,
) -> SoakResult:
    """Run the soak for ``duration`` seconds, or until ``stop`` is set (``main`` sets it on
    Ctrl-C); either way the last check and the summary run. A cancellation is let through: the
    systems terminate and it propagates, with no last check."""
    stop = asyncio.Event() if stop is None else stop
    workload = WORKLOADS[sample]
    seed = time.time_ns() if seed is None else seed
    rng = random.Random(seed)
    state = SoakState(rng, measure_interval=nodes / rate)
    loop = asyncio.get_running_loop()
    lags = Lags(nodes, rng)
    passed = failed = 0
    running = asyncio.Event()  # the load runs while it is set

    def on_change(node: int, key: Key[Any], data: ReplicatedData) -> None:
        lags.changed(node, key, data, loop.time())

    def converged() -> bool:
        first = lags.views[0]
        return all(
            view.keys() == first.keys()
            and all(observed(data) == observed(first[k]) for k, data in view.items())
            for view in lags.views[1:]
        )

    def snapshot() -> list[dict[Key[Any], object]]:
        return [{k: observed(d) for k, d in view.items()} for view in lags.views]

    async def query(ref: ActorRef[Any]) -> object:
        try:
            return await workload.query(ref, state)
        except Exception as e:  # an answer that cannot be right, not a crash of the soak
            return e

    def divergence() -> str:
        views = snapshot()
        differ = [i for i, v in enumerate(views) if v != views[0]]
        keys = sorted(
            {
                str(key)
                for v in views[1:]
                for key in v.keys() | views[0].keys()
                if v.get(key) != views[0].get(key)
            }
        )
        return (
            f"nodes {_nodes(differ)} differ from node 0 on keys {', '.join(keys)}"
            if differ
            else "they changed while the actors were asked"
        )

    async def check(k: int, timeout: float) -> bool:
        """Pass once the views have converged and every actor's answer is what they imply,
        with the views held still while it was asked; until the deadline, try again."""
        stopped = loop.time()
        deadline = stopped + timeout
        running.clear()  # the load task stops before its next command
        state.frozen = True
        disagreement: str | None = None  # the last attempt's, if it got as far as the answers
        while True:
            if converged():
                took = loop.time() - stopped
                views, view = snapshot(), lags.views[0]  # views are replaced, never mutated
                expected = workload.expected(view, state)
                answers = await asyncio.gather(*map(query, state.refs))
                differ = [i for i, answer in enumerate(answers) if answer != expected]
                still = snapshot() == views  # else a command in flight landed while asking
                if not differ and still:
                    out(
                        f"check #{k}: {nodes} nodes converged {took:.2f} s after the load "
                        "stopped; actors agree"
                    )
                    return True
                disagreement = (
                    f"actors disagree; nodes {_nodes(differ)} differ from the converged "
                    f"views; node {differ[0]} answered {reprlib.repr(answers[differ[0]])}, "
                    f"the views imply {reprlib.repr(expected)}"
                    if differ
                    else "the views changed while the actors were asked"
                )
            else:
                disagreement = None
            if loop.time() >= deadline:
                reason = disagreement or (
                    f"views did not converge within {timeout:g} s; {divergence()}"
                )
                out(f"VIOLATION check #{k}: {reason}")
                return False
            await asyncio.sleep(0.01)

    def resume() -> None:
        state.frozen = False
        running.set()

    async def load(command: Callable[[random.Random, SoakState], Any]) -> None:
        next_at = loop.time()
        while True:
            if not running.is_set():
                await running.wait()
                next_at = loop.time()
            state.refs[rng.randrange(nodes)].tell(command(rng, state))
            state.commands += 1
            next_at += 1 / rate
            await asyncio.sleep(max(0.0, next_at - loop.time()))

    async def cancel(task: asyncio.Task[None]) -> None:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def unless_stopped(work: Awaitable[None]) -> bool:
        """Run ``work``; False if ``stop`` was set first, which cancels it."""
        task = asyncio.ensure_future(work)
        stopped = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait((task, stopped), return_when=asyncio.FIRST_COMPLETED)
        finally:
            task.cancel()
            stopped.cancel()
            await asyncio.gather(task, stopped, return_exceptions=True)
        if task.cancelled():
            return False
        task.result()  # its error, if it failed
        return True

    out(
        f"soak {sample}: {nodes} nodes, rate {rate:g}/s, seed {seed}, "
        f"notify_subscribers_interval={NOTIFY_SUBSCRIBERS_INTERVAL:g} s"
    )
    settings = ClusterSettings(
        distributed_data=ReplicatorSettings(
            notify_subscribers_interval=NOTIFY_SUBSCRIBERS_INTERVAL
        )
    )
    systems: list[ActorSystem[Any]] = []

    async def terminate() -> None:  # all at once: one after the other, each waits on its leave
        for system in systems:
            system.terminate()
        await asyncio.gather(*(system.when_terminated() for system in systems))

    async def replica_count(system: ActorSystem[Any]) -> int:
        replicator = DistributedData(system).replicator
        return (await ask(replicator, Replicator.GetReplicaCount, ASK_TIMEOUT)).n

    async def startup() -> None:
        for i in range(nodes):
            ready: asyncio.Future[ActorRef[Any]] = loop.create_future()
            system = ActorSystem(
                _guardian(i, workload, state, ready, on_change),
                "soak",
                address=("127.0.0.1", free_port()),
                cluster=settings,
            )
            systems.append(system)
            await system.__aenter__()
            state.refs.append(await asyncio.wait_for(ready, STARTUP_TIMEOUT))
        seed_node = Cluster(systems[0]).self_address
        for system in systems:
            Cluster(system).manager.tell(Join(seed_node))
        async with asyncio.timeout(STARTUP_TIMEOUT):
            while any(n != nodes for n in await asyncio.gather(*map(replica_count, systems))):
                await asyncio.sleep(0.1)
        if workload.start is not None:
            await workload.start(state)
        async with asyncio.timeout(STARTUP_TIMEOUT):
            # data written before every node had joined reaches the late ones by gossip only
            while not converged():
                await asyncio.sleep(0.01)

    def summary() -> SoakResult:
        p50, p99 = _percentiles(lags.sample, 50, 99)
        peak = _ms(lags.max) if lags.count else "-"
        out(
            f"summary: {state.commands} commands, {passed} checks passed, {failed} failed, "
            f"lag p50={p50} p99={p99} max={peak}"
        )
        return SoakResult(state.commands, passed, failed, list(lags.sample))

    async with AsyncExitStack() as stack:
        stack.push_async_callback(terminate)
        if not await unless_stopped(startup()):  # a Ctrl-C during startup: nothing to check
            return summary()
        lags.reset()  # the lags are measured under the load only
        resume()
        if workload.command is not None:
            stack.push_async_callback(cancel, asyncio.create_task(load(workload.command)))

        start = loop.time()
        end = math.inf if duration is None else start + duration
        next_report, next_check = start + report_every, start + check_every
        last_report, last_commands, k = start, 0, 0
        while not stop.is_set():
            with suppress(TimeoutError):
                async with asyncio.timeout(min(next_report, next_check, end) - loop.time()):
                    await stop.wait()
            now = loop.time()
            if stop.is_set() or now >= end:
                break
            if now >= next_check:
                k += 1
                if await check(k, check_timeout):
                    passed += 1
                else:
                    failed += 1
                resume()
                while next_check <= loop.time():
                    next_check += check_every
            now = loop.time()
            if now >= next_report:
                p50, p95, p99 = _percentiles(lags.period, 50, 95, 99)
                peak = _ms(max(lags.period)) if lags.period else "-"
                per_second = (state.commands - last_commands) / (now - last_report)
                out(
                    f"t={now - start:.0f}s cmds={state.commands} ({per_second:.0f}/s) "
                    f"lag p50={p50} p95={p95} p99={p99} max={peak} "
                    f"behind={lags.behind}/{len(lags.queues)} pairs"
                )
                lags.period = []
                last_report, last_commands = now, state.commands
                while next_report <= now:
                    next_report += report_every
        k += 1
        # after a Ctrl-C, a short last check: whoever pressed it wants out
        timeout = min(check_timeout, STOP_CHECK_TIMEOUT) if stop.is_set() else check_timeout
        if await check(k, timeout):
            passed += 1
        else:
            failed += 1
        return summary()


def main() -> None:
    parser = argparse.ArgumentParser(description="Soak a Distributed Data sample.")
    parser.add_argument("sample", choices=sorted(WORKLOADS))
    parser.add_argument("--nodes", type=int, default=5)
    parser.add_argument("--rate", type=float, default=100.0, help="commands per second")
    parser.add_argument("--report-every", type=float, default=5.0, metavar="SECONDS")
    parser.add_argument("--check-every", type=float, default=30.0, metavar="SECONDS")
    parser.add_argument("--duration", type=float, default=None, metavar="SECONDS")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--check-timeout", type=float, default=30.0, metavar="SECONDS")
    args = parser.parse_args()

    async def soak() -> SoakResult:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        assert task is not None

        def interrupt() -> None:  # the first Ctrl-C: the last check and the summary
            if stop.is_set():
                task.cancel()  # the second: no check, the systems still terminate
            stop.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, interrupt)
        return await run(
            args.sample,
            nodes=args.nodes,
            rate=args.rate,
            report_every=args.report_every,
            check_every=args.check_every,
            duration=args.duration,
            seed=args.seed,
            check_timeout=args.check_timeout,
            stop=stop,
        )

    try:
        result = asyncio.run(soak())
    except asyncio.CancelledError:
        print("soak interrupted: no last check", file=sys.stderr)
        sys.exit(130)
    if args.duration is not None and result.checks_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
