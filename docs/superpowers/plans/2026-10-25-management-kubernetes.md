# Management and Kubernetes Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port Pekko Management's Kubernetes path: a management HTTP endpoint with health checks and read-only cluster routes, cluster bootstrap with the lowest-address join decider, and Kubernetes API discovery, all on the standard library.

**Architecture:** `pekko_py/discovery.py` holds Pekko's ServiceDiscovery API and the config implementation. `pekko_py/management/` holds a minimal HTTP/1.1 server and client (`http.py`), the health checks (`health.py`), the extension and settings (`__init__.py`), the read-only cluster routes (`cluster_http.py`), the bootstrap coordinator, probes and decider (`bootstrap.py`), and the Kubernetes discovery (`kubernetes.py`). `ActorSystem(..., management=PekkoManagementSettings(...))` creates the extension; nothing binds until `await PekkoManagement(system).start()`.

**Tech Stack:** Python 3.12 stdlib (`asyncio` streams, `ssl`, `json`, `socket`), pytest, pyright strict. Pekko sources: `/Users/hanishi/pekko-management/{management,management-cluster-bootstrap,management-cluster-http,discovery-kubernetes-api}/src/main/scala`, `/Users/hanishi/pekko/discovery/src/main/scala/org/apache/pekko/discovery/ServiceDiscovery.scala`.

**Spec:** `docs/superpowers/specs/2026-10-25-management-kubernetes-design.md`

## Global Constraints

- No new dependency: `pyproject.toml` `dependencies = []` stays.
- Frozen dataclasses for settings, messages and JSON models; `match` for dispatch; Python 3.12 generics; Pekko names in snake_case; every narrowing in the README deviations.
- `pytest` green under the conftest leak check (the HTTP server and every probe must stop with the system); `pyright` strict 0 errors.
- Nothing binds a port or probes a peer until `PekkoManagement(system).start()`.
- The JSON shapes of `/bootstrap/seed-nodes`, `/cluster/members` and `/cluster/shards/{type}` are Pekko's field names, camelCase.

## Review Focus

1. A malformed or oversized HTTP request (no request line, 100 KB of headers, a body on a GET) must not hang or crash the server, only close that connection (Task 1 `test_server_rejects_garbage_and_closes`).
2. Two nodes whose discovery lists the same contact points but whose management hostnames differ in form (one an IP, the other a name resolving to it) must still agree on the lowest address and form one cluster, not two: `matches_self` compares by IP as well as host (Task 4 `test_matches_self_by_ip`).
3. A probe target that is this node's own remoting port (misconfigured fallback port) must be refused with Pekko's warning, not probed (Task 4 `test_probe_refuses_own_remoting_address`).
4. `PekkoManagement.start()` on a port already taken must raise and leave the system usable, not log and continue with no endpoint (Task 2 `test_start_on_a_taken_port_raises`).
5. A Kubernetes API response that is valid JSON but missing optional fields (`status` without `podIP`, `spec` without `ports`, no `metadata`) must yield no target for that pod, not an exception (Task 5 `test_targets_tolerate_missing_fields`).

---

### Task 1: Discovery API and the HTTP server and client

**Files:**
- Create: `src/pekko_py/discovery.py`, `src/pekko_py/management/__init__.py` (docstring only for now), `src/pekko_py/management/http.py`
- Modify: `src/pekko_py/__init__.py` (exports)
- Test: `tests/test_discovery.py`, `tests/management/__init__.py`, `tests/management/test_http.py`

**Interfaces:**
- Produces:
  - `Lookup(service_name: str, port_name: str | None = None, protocol: str | None = None)`; `ResolvedTarget(host: str, port: int | None, address: str | None = None)` ordered by `(host, port or 0)`; `Resolved(service_name: str, addresses: tuple[ResolvedTarget, ...])`; `class DiscoveryTimeoutError(Exception)`; `class ServiceDiscovery` with `async def lookup(self, lookup: Lookup, resolve_timeout: float) -> Resolved` (raises `NotImplementedError`); `ConfigServiceDiscovery(services: Mapping[str, Sequence[ResolvedTarget]])`.
  - `management/http.py`: `Response(status: int, body: bytes = b"", content_type: str = "application/json")`; `type Handler = Callable[[str, str], Awaitable[Response]]` (method, path without the query); `async def serve(host: str, port: int, handler: Handler) -> asyncio.Server`; `async def get(url: str, *, headers: Mapping[str, str] = {}, timeout: float, ssl_context: ssl.SSLContext | None = None) -> tuple[int, bytes]`; `class HttpError(Exception)` for a malformed response; `json_response(status: int, payload: object) -> Response`; `REASONS: dict[int, str]` for 200, 400, 404, 405, 500.
  - Exports from `pekko_py`: `Lookup`, `ResolvedTarget`, `Resolved`, `ServiceDiscovery`, `ConfigServiceDiscovery`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_discovery.py
async def test_config_discovery_resolves_known_and_unknown_names():
    d = ConfigServiceDiscovery({"svc": [ResolvedTarget("10.0.0.2", 7626, "10.0.0.2"), ResolvedTarget("10.0.0.1", 7626, "10.0.0.1")]})
    assert await d.lookup(Lookup("svc"), 1.0) == Resolved("svc", (ResolvedTarget("10.0.0.2", 7626, "10.0.0.2"), ResolvedTarget("10.0.0.1", 7626, "10.0.0.1")))
    assert await d.lookup(Lookup("other"), 1.0) == Resolved("other", ())

def test_resolved_targets_order_by_host_then_port():
    assert sorted([ResolvedTarget("b", 1), ResolvedTarget("a", 2), ResolvedTarget("a", None)]) == [ResolvedTarget("a", None), ResolvedTarget("a", 2), ResolvedTarget("b", 1)]

# tests/management/test_http.py
async def handler(method: str, path: str) -> Response:
    if path == "/boom": raise RuntimeError("boom")
    if path == "/slow": await asyncio.sleep(5); return Response(200)
    if path == "/hello": return json_response(200, {"hello": "world"})
    return Response(404, b"not found", "text/plain")

async def test_get_json_404_405_and_500():
    server = await serve("127.0.0.1", 0, handler)
    port = server.sockets[0].getsockname()[1]
    try:
        assert await get(f"http://127.0.0.1:{port}/hello?x=1", timeout=2) == (200, b'{"hello": "world"}')
        assert (await get(f"http://127.0.0.1:{port}/nope", timeout=2))[0] == 404
        assert (await get(f"http://127.0.0.1:{port}/boom", timeout=2))[0] == 500
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"POST /hello HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n"); await writer.drain()
        assert (await reader.readline()).startswith(b"HTTP/1.1 405"); writer.close(); await writer.wait_closed()
    finally:
        server.close(); await server.wait_closed()

async def test_client_times_out():
    # /slow with timeout=0.2 raises TimeoutError

async def test_server_rejects_garbage_and_closes():
    # for payload in (b"\r\n\r\n", b"GET\r\n\r\n", b"GET /hello HTTP/1.1\r\nX: " + b"a" * 100_000 + b"\r\n\r\n"):
    #   open a raw connection, send it, the first line read back is b"" (closed) or starts with b"HTTP/1.1 400"
    # then a normal get(/hello) still answers 200: the server survived
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_discovery.py tests/management/test_http.py -q 2>&1 | tail -3`
Expected: FAIL at import (`pekko_py.discovery`, `pekko_py.management.http` missing).

- [ ] **Step 3: Implement**

`discovery.py` as the Interfaces block. `http.py` server: per connection read the request line with `readline()` under an 8 KB limit, then headers up to 8 KB total or 100 lines; on any violation or parse error (`LimitOverrunError`, `IncompleteReadError`, a request line without method, path and `HTTP/1.`) answer 400 and close; ignore any body; GET goes to the handler, anything else is 405 with `Allow: GET`; write `HTTP/1.1 {status} {reason}`, `Content-Type`, `Content-Length`, `Connection: close`, the body, then close; a handler exception is 500 with its text. `serve` wraps `asyncio.start_server` and returns the server. `get`: `urllib.parse.urlsplit`, `asyncio.open_connection` (TLS when the scheme is `https`, with `ssl_context` or `ssl.create_default_context()`), send `GET {path}?{query} HTTP/1.1`, `Host`, `Connection: close` and the headers, read the status line and headers, then the body by `Content-Length`, chunked decoding, or to EOF; all under `asyncio.wait_for(timeout)`.

- [ ] **Step 4: Run the tests and pyright**

Run: `pytest tests/test_discovery.py tests/management/test_http.py -q 2>&1 | tail -1 && pyright | tail -1`
Expected: all PASS, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/discovery.py src/pekko_py/management tests/test_discovery.py tests/management
git commit -m "Discovery API with ConfigServiceDiscovery, and a stdlib HTTP/1.1 server and client for management"
```

---

### Task 2: The management extension and health checks

**Files:**
- Create: `src/pekko_py/management/health.py`
- Modify: `src/pekko_py/management/__init__.py`, `src/pekko_py/system.py` (`management=` parameter, `self.management`, close on the system root's stop), `src/pekko_py/__init__.py`, `tests/cluster/test_cluster.py` (`cluster_node` gains `management=`)
- Test: `tests/management/test_management.py`

**Interfaces:**
- Consumes: Task 1's `serve`, `get`, `Response`, `json_response`.
- Produces:
  - `HealthCheckSettings(startup_path: str = "startup", readiness_path: str = "ready", liveness_path: str = "alive", check_timeout: float = 1.0, ready_states: frozenset[MemberStatus] = frozenset({"up", "weakly_up"}))`.
  - `PekkoManagementSettings(hostname: str | None = None, port: int = 7626, bind_hostname: str | None = None, bind_port: int | None = None, base_path: str = "", health_checks: HealthCheckSettings = HealthCheckSettings(), bootstrap: ClusterBootstrapSettings | None = None, discovery: ServiceDiscovery | None = None)`: `ValueError` when `bootstrap` is set without `discovery`, when `port` is not in `0..65535` (0 binds an ephemeral port), when `base_path` is non-empty and does not start with `/`. Properties `effective_hostname` (`socket.gethostbyname(socket.gethostname())` for `None`), `effective_bind_hostname`, `effective_bind_port`. Until Task 4 the `bootstrap` field is typed `object | None`; Task 4 tightens it.
  - `type HealthCheck = Callable[[], Awaitable[bool]]`; `class HealthChecks(settings: HealthCheckSettings)`: `add_startup_check(name, check)`, `add_readiness_check(name, check)`, `add_liveness_check(name, check)`, `async startup() -> str | None`, `async ready() -> str | None`, `async alive() -> str | None`.
  - `class PekkoManagement`: `__new__(cls, system)` returns `system.management`; `_start(system, settings)`; `settings`; `health: HealthChecks`; `uri: str | None`; `async start() -> str`; `async stop() -> None`; `add_route(path: str, handler: Callable[[str], Awaitable[Response]])` for GET routes under `base_path` (an exact path, or a prefix when `path` ends with `/`, the handler then receiving the remainder).
  - `ActorSystem(..., management: PekkoManagementSettings | None = None)`; `system.management: PekkoManagement`. On a cluster node the readiness check `cluster-membership` (self status in `ready_states`) is registered at creation.
  - Exports: `PekkoManagement`, `PekkoManagementSettings`, `HealthCheckSettings`.

- [ ] **Step 1: Write the failing tests**

```python
def mgmt(**kw): return PekkoManagementSettings(hostname="127.0.0.1", port=0, **kw)

async def test_start_binds_and_serves_health_routes():
    async with ActorSystem(Behaviors.ignore, "m", management=mgmt()) as system:
        uri = await PekkoManagement(system).start()
        assert uri.startswith("http://127.0.0.1:") and await PekkoManagement(system).start() == uri
        for path in ("/ready", "/alive", "/startup"):
            assert (await get(uri + path, timeout=2))[0] == 200
    with pytest.raises(OSError):
        await get(uri + "/ready", timeout=1)  # closed with the system

async def test_failing_check_names_itself_and_times_out():
    async with ActorSystem(Behaviors.ignore, "m", management=mgmt(health_checks=HealthCheckSettings(check_timeout=0.1))) as system:
        m = PekkoManagement(system)
        async def ok() -> bool: return True
        async def no() -> bool: return False
        async def boom() -> bool: raise RuntimeError("db down")
        async def slow() -> bool: await asyncio.sleep(5); return True
        m.health.add_readiness_check("ok", ok); m.health.add_readiness_check("db", no)
        m.health.add_liveness_check("boom", boom); m.health.add_startup_check("slow", slow)
        uri = await m.start()
        assert await get(uri + "/ready", timeout=2) == (500, b"Not Healthy: Check [db] not ok")
        assert (await get(uri + "/alive", timeout=2))[1] == b"Not Healthy: Check [boom] failed: db down"
        assert (await get(uri + "/startup", timeout=2))[1] == b"Not Healthy: Check [slow] timed out after 0.1 s"

async def test_base_path_and_custom_check_paths():
    settings = mgmt(base_path="/management", health_checks=HealthCheckSettings(readiness_path="health/ready"))
    async with ActorSystem(Behaviors.ignore, "m", management=settings) as system:
        uri = await PekkoManagement(system).start()
        assert uri.endswith("/management") and (await get(uri + "/health/ready", timeout=2))[0] == 200
        assert (await get(uri.removesuffix("/management") + "/health/ready", timeout=2))[0] == 404

async def test_start_on_a_taken_port_raises():
    blocker = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0); port = blocker.sockets[0].getsockname()[1]
    try:
        async with ActorSystem(Behaviors.ignore, "m", management=PekkoManagementSettings(hostname="127.0.0.1", port=port)) as system:
            with pytest.raises(OSError):
                await PekkoManagement(system).start()
            assert PekkoManagement(system).uri is None
    finally:
        blocker.close(); await blocker.wait_closed()

def test_settings_validation():
    with pytest.raises(ValueError): PekkoManagementSettings(bootstrap=object())  # bootstrap without discovery
    with pytest.raises(ValueError): PekkoManagementSettings(base_path="management")
    with pytest.raises(ValueError): PekkoManagementSettings(port=70000)

async def test_cluster_membership_readiness():
    p = free_port()
    async with cluster_node("c", p, []) as node:  # no seed nodes: never joins by itself
        uri = await PekkoManagement(node).start()
        assert (await get(uri + "/ready", timeout=2))[0] == 500
        Cluster(node).manager.tell(Join(Address("c", "127.0.0.1", p)))
        await eventually(lambda: all_up(node, 1), timeout=5)
        assert (await get(uri + "/ready", timeout=2))[0] == 200
```

`cluster_node` gains `management: PekkoManagementSettings | None = None`, defaulting to `PekkoManagementSettings(hostname="127.0.0.1", port=0)`.

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/management/test_management.py -q 2>&1 | tail -3`
Expected: FAIL at import (`PekkoManagement` missing).

- [ ] **Step 3: Implement**

`health.py` ports `HealthChecksImpl.check`: every check under `asyncio.wait_for(check_timeout)`; `True` passes, `False` is `Check [name] not ok`, an exception `Check [name] failed: {e}`, a timeout `Check [name] timed out after {t} s`; the first failure in registration order is the reason. `__init__.py`: a route table of exact paths and prefix paths, all relative to `base_path`; `start()` calls `serve(effective_bind_hostname, effective_bind_port, dispatch)`, sets `uri = f"http://{effective_hostname}:{bound_port}{base_path}"`, logs "Bound Pekko Management (HTTP) endpoint to: {}:{}"; an `OSError` from `serve` propagates and `uri` stays `None`; `stop()` closes and awaits the server. `system.py`: `self.management = PekkoManagement._start(self, management or PekkoManagementSettings())` after the cluster; the system root's done callback schedules `stop()` as a task that `when_terminated()` awaits, like `_close_remoting`. The readiness check is added in `_start` when `system.cluster` is set.

- [ ] **Step 4: Run the tests and the suite**

Run: `pytest tests/management -q 2>&1 | tail -1 && pytest -q 2>&1 | tail -1 && pyright | tail -1`
Expected: all PASS, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/management src/pekko_py/system.py src/pekko_py/__init__.py tests/management tests/cluster/test_cluster.py
git commit -m "PekkoManagement: the management HTTP endpoint with Pekko's health checks and the cluster-membership readiness check"
```

---

### Task 3: Read-only cluster HTTP management

**Files:**
- Create: `src/pekko_py/management/cluster_http.py`
- Modify: `src/pekko_py/management/__init__.py` (register the routes on a cluster node), `src/pekko_py/cluster/sharding/__init__.py` (keep each type's `Entity` in `_entities: dict[str, Entity[Any]]`)
- Test: `tests/management/test_cluster_http.py`

**Interfaces:**
- Consumes: Task 2's `add_route`; `Cluster(system).state`; `Cluster._membership_state.latest_gossip.reachability.records` (private, for `observedBy`); `ClusterSharding(system)._regions` and `_entities`; `shard_id_of`.
- Produces: `register_cluster_routes(management: PekkoManagement, system: ActorSystem[Any]) -> None` adding `GET /cluster/members`, `GET /cluster/members/` (prefix: the address), `GET /cluster/shards/` (prefix: the type name); `member_json(m: Member) -> dict[str, object]` as `{"node": "pekko://name@host:port", "nodeUid": uid, "status": "Up" | "WeaklyUp" | "Joining" | "Leaving" | "Exiting" | "Down" | "Removed", "roles": []}`.

- [ ] **Step 1: Write the failing tests**

```python
async def test_cluster_members_json():
    p1, p2 = sorted((free_port(), free_port()))
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        uri = await PekkoManagement(a).start()
        status, body = await get(uri + "/cluster/members", timeout=2)
        doc = json.loads(body)
        assert status == 200 and doc["selfNode"] == f"pekko://c@127.0.0.1:{p1}" and doc["leader"] == f"pekko://c@127.0.0.1:{p1}"
        assert sorted(m["node"] for m in doc["members"]) == [f"pekko://c@127.0.0.1:{p1}", f"pekko://c@127.0.0.1:{p2}"]
        assert all(m["status"] == "Up" and m["roles"] == [] for m in doc["members"]) and doc["unreachable"] == [] and doc["oldestPerRole"] == {}
        assert doc["oldest"] == f"pekko://c@127.0.0.1:{p1}"
        one = json.loads((await get(uri + f"/cluster/members/pekko://c@127.0.0.1:{p2}", timeout=2))[1])
        assert one["node"] == f"pekko://c@127.0.0.1:{p2}"
        assert (await get(uri + "/cluster/members/pekko://c@127.0.0.1:1", timeout=2))[0] == 404

async def test_unreachable_members_list_their_observers():
    # two nodes; crash b (tests/cluster/test_cluster.py's crash); eventually /cluster/members on a has
    # unreachable == [{"node": "<b>", "observedBy": ["<a>"]}]

async def test_cluster_shards_route():
    # one sharding node (tests/cluster/test_cluster_sharding.py's node/start/ref; FAST_SHARDING has number_of_shards=10);
    # touch ids "a".."f"; GET /cluster/shards/Counter -> {"regions": [{"shardId": ..., "numEntities": ...}]} whose
    # numEntities sum to 6 and whose shardIds equal {shard_id_of(i, 10) for i in ids}; GET /cluster/shards/Nope -> 404

async def test_mutating_routes_are_405():
    # raw connection: "PUT /cluster/members/x HTTP/1.1" -> 405; "POST /cluster/members HTTP/1.1" -> 405
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/management/test_cluster_http.py -q 2>&1 | tail -3`
Expected: FAIL: `/cluster/members` answers 404.

- [ ] **Step 3: Implement**

As `ClusterHttpManagementRoutes.routeGetMembers`: members from `Cluster(system).state.members`; `unreachable` by grouping reachability records with status `unreachable` by subject, observers sorted; `leader` from the state; `oldest` the Up member with the lowest `(up_number, address)`. The shards route asks the local region `GetShardRegionState` (five-second timeout) and groups `entity_ids` by `shard_id_of(id, entity.effective_settings.number_of_shards)`; an `AskTimeoutError` is 500 with Pekko's message. Registered by `PekkoManagement._start` when `system.cluster` is set.

- [ ] **Step 4: Run the tests and pyright**

Run: `pytest tests/management -q 2>&1 | tail -1 && pyright | tail -1`
Expected: all PASS, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/management src/pekko_py/cluster/sharding/__init__.py tests/management/test_cluster_http.py
git commit -m "Management: read-only cluster HTTP routes (/cluster/members, /cluster/shards), as Pekko's with route-providers-read-only"
```

---

### Task 4: Cluster bootstrap

**Files:**
- Create: `src/pekko_py/management/bootstrap.py`
- Modify: `src/pekko_py/management/__init__.py` (the seed-nodes route; `start()` starts the bootstrap; the `bootstrap` field's type), `src/pekko_py/__init__.py`
- Test: `tests/management/test_bootstrap_decider.py`, `tests/management/test_bootstrap_cluster.py`

**Interfaces:**
- Consumes: Task 1's `get`, `ResolvedTarget`, `Lookup`, `ServiceDiscovery`; Task 2's `PekkoManagement`, `add_route`; the cluster's `Join`, `JoinSeedNodes`, `Cluster(system).state`, `self_address`.
- Produces:
  - `ClusterBootstrapSettings(...)` as the spec, validated in `__post_init__` (positive durations, `required_contact_point_nr >= 1`, `0 <= probe_interval_jitter < 1`).
  - `SeedNodesObservation(observed_at: float, contact_point: ResolvedTarget, self_node: Address, seed_nodes: frozenset[Address])`; `SeedNodesInformation(current_time: float, contact_points_changed_at: float, contact_points: frozenset[ResolvedTarget], seed_nodes_observations: frozenset[SeedNodesObservation])` with properties `has_seed_nodes` and `all_seed_nodes`; decisions `KeepProbing()`, `JoinSelf()`, `JoinOtherSeedNodes(seed_nodes: frozenset[Address])`; `type JoinDecision`.
  - `select_hosts(lookup: Lookup, fallback_port: int, filter_on_fallback_port: bool, contact_points: Sequence[ResolvedTarget]) -> list[ResolvedTarget]`; `matches_self(target: ResolvedTarget, self_contact_point: tuple[str, int]) -> bool` (host equal, or `target.address` equal to the host; the port too when the target has one); `backed_off_interval(restart_count: int, min_backoff: float, max_backoff: float, random_factor: float) -> float` (`min(max_backoff, min_backoff * 2 ** restart_count) * (1 + random() * random_factor)`, capped at `max_backoff`).
  - `class LowestAddressJoinDecider(settings: ClusterBootstrapSettings, self_contact_point: tuple[str, int], log: logging.Logger)` with `decide(info: SeedNodesInformation) -> JoinDecision`.
  - `class ClusterBootstrap` with `__new__(cls, system)` returning `system.management.bootstrap` (`RuntimeError` when the settings have none), `start(self_contact_point: tuple[str, int]) -> None`, and `seed_nodes_json(system, settings) -> dict[str, object]`.
  - Exports: `ClusterBootstrap`, `ClusterBootstrapSettings`.

- [ ] **Step 1: Write the failing decider tests**

```python
S = ClusterBootstrapSettings(service_name="svc", stable_margin=5.0, required_contact_point_nr=2)
A, B, C = ResolvedTarget("10.0.0.1", 7626, "10.0.0.1"), ResolvedTarget("10.0.0.2", 7626, "10.0.0.2"), ResolvedTarget("10.0.0.3", 7626, "10.0.0.3")
def node(i: int) -> Address: return Address("c", f"10.0.0.{i}", 2552)
def obs(cp: ResolvedTarget, *seeds: Address, at: float = 100.0) -> SeedNodesObservation:
    return SeedNodesObservation(at, cp, node(int(cp.host[-1])), frozenset(seeds))
def info(points, observations, now=200.0, changed_at=100.0):
    return SeedNodesInformation(now, changed_at, frozenset(points), frozenset(observations))
def decider(self_cp=("10.0.0.1", 7626), settings=S): return LowestAddressJoinDecider(settings, self_cp, logging.getLogger("t"))

def test_seed_nodes_seen_join_them():
    assert decider().decide(info([A, B], [obs(A), obs(B, node(2), node(3))])) == JoinOtherSeedNodes(frozenset({node(2), node(3)}))
def test_too_few_contact_points_keep_probing(): assert decider().decide(info([A, B], [obs(A)])) == KeepProbing()
def test_inside_stable_margin_keep_probing(): assert decider().decide(info([A, B], [obs(A), obs(B)], now=104.0)) == KeepProbing()
def test_missing_observation_keep_probing(): assert decider().decide(info([A, B, C], [obs(A), obs(B)])) == KeepProbing()
def test_lowest_is_self_joins_self(): assert decider().decide(info([A, B], [obs(A), obs(B)])) == JoinSelf()
def test_lowest_is_other_keeps_probing(): assert decider(self_cp=("10.0.0.2", 7626)).decide(info([A, B], [obs(A), obs(B)])) == KeepProbing()
def test_new_cluster_disabled_never_joins_self():
    assert decider(settings=replace(S, new_cluster_enabled=False)).decide(info([A, B], [obs(A), obs(B)])) == KeepProbing()
def test_matches_self_by_ip():
    assert matches_self(ResolvedTarget("10-0-0-1.ns.pod.cluster.local", 7626, "10.0.0.1"), ("10.0.0.1", 7626))
    assert not matches_self(A, ("10.0.0.1", 7627)) and matches_self(ResolvedTarget("10.0.0.1", None), ("10.0.0.1", 7627))
def test_select_hosts_keeps_fallback_port_per_host():
    pts = [ResolvedTarget("h", 7626), ResolvedTarget("h", 2552), ResolvedTarget("g", 2552)]
    assert select_hosts(Lookup("svc"), 7626, True, pts) == [ResolvedTarget("h", 7626), ResolvedTarget("g", 2552)]
    assert select_hosts(Lookup("svc", port_name="m"), 7626, True, pts) == pts and select_hosts(Lookup("svc"), 7626, False, pts) == pts
def test_backed_off_interval_doubles_and_caps():
    assert backed_off_interval(0, 1.0, 15.0, 0.0) == 1.0 and backed_off_interval(3, 1.0, 15.0, 0.0) == 8.0 and backed_off_interval(10, 1.0, 15.0, 0.0) == 15.0
    assert 2.0 <= backed_off_interval(1, 1.0, 15.0, 0.5) <= 3.0
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/management/test_bootstrap_decider.py -q 2>&1 | tail -3`
Expected: FAIL at import.

- [ ] **Step 3: Implement the pure parts** (`select_hosts`, `matches_self`, `backed_off_interval`, the decider with Pekko's info and warning lines) and run Step 1's tests to green.

- [ ] **Step 4: Write the failing multi-node tests**

```python
BOOT = dict(stable_margin=0.3, interval=0.05, probe_interval=0.05, probing_failure_timeout=0.5, resolve_timeout=0.5, required_contact_point_nr=2)

def bootstrap_node(port: int, mgmt_ports: list[int], my_mgmt: int, **boot) -> ActorSystem[Any]:
    discovery = ConfigServiceDiscovery({"svc": [ResolvedTarget("127.0.0.1", p, "127.0.0.1") for p in mgmt_ports]})
    management = PekkoManagementSettings(hostname="127.0.0.1", port=my_mgmt, discovery=discovery, bootstrap=ClusterBootstrapSettings(service_name="svc", **{**BOOT, **boot}))
    return cluster_node("c", port, [], management=management)  # no seed nodes

async def test_three_nodes_form_one_cluster_and_the_lowest_joins_itself():
    ports, mgmt = sorted(free_port() for _ in range(3)), sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(bootstrap_node(p, mgmt, m)) for p, m in zip(ports, mgmt, strict=True)]
        for n in nodes: await PekkoManagement(n).start()
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=10)
        assert len({Cluster(n).state.leader for n in nodes}) == 1
        oldest = min(Cluster(nodes[0]).state.members, key=lambda m: m.up_number)
        assert oldest.address.port == ports[0]  # the lowest management address self-joined first

async def test_a_late_node_joins_through_probing():
    # three nodes as above, up; a fourth whose discovery lists all four management ports starts later: all_up(n, 4) everywhere

async def test_a_node_missing_itself_in_discovery_joins_once_seed_nodes_appear():
    # c's discovery lists only a and b; c never self-joins; once a has formed the cluster, c joins through a's seed nodes

async def test_new_cluster_disabled_forms_nothing():
    # two nodes, new_cluster_enabled=False; after 1.5 * stable_margin no node has members

async def test_probe_refuses_own_remoting_address(caplog):
    # one node whose discovery lists ResolvedTarget("127.0.0.1", <its remoting port>, "127.0.0.1"); start; eventually caplog has
    # "Misconfiguration detected"; the node has no members after 1.5 * stable_margin

async def test_bootstrap_with_seed_nodes_configured_bails_out(caplog):
    # cluster_node("c", p, [p], management=<bootstrap settings>): start() logs "bailing out of the bootstrap process"; the node joins its seed
```

- [ ] **Step 5: Run them to verify they fail**

Run: `pytest tests/management/test_bootstrap_cluster.py -q 2>&1 | tail -3`
Expected: FAIL: `all_up` never becomes true.

- [ ] **Step 6: Implement the coordinator, probes, route and start**

`bootstrap.py`: a `_Coordinator` state class in a `Behaviors.setup` closure at `/system/bootstrapCoordinator`; messages `_DiscoverTick`, `_DecideTick`, `_Resolved(resolved)`, `_ResolveFailed(error)`, `_Observed(observation)`, `_ProbingFailed(contact_point)`; discovery through `ctx.pipe_to_self`; one single-shot discovery timer rescheduled with `backed_off_interval(failures, interval, exponential_backoff_max, random_factor)`; a fixed-delay decide timer every `probe_interval` (Pekko's `startPeriodicDecisionTimer`). Probes are children named `contactPointProbe-{host}-{port}` with dots and colons replaced by dashes; each runs `get(f"http://{host}:{port}{base_path}/bootstrap/seed-nodes", timeout=probing_failure_timeout)` on a jittered single timer, pipes the parsed observation or the failure to itself, reports to the parent, and stops after `probing_failure_timeout` of continuous failure; the parent stops probes whose contact point left the discovered set and refuses a target equal to this node's remoting host and port with Pekko's "Misconfiguration detected!" warning. Decisions: `JoinOtherSeedNodes` → `Cluster(system).manager.tell(JoinSeedNodes(tuple(sorted(seeds - {self}))))` and `Behaviors.stopped()`; `JoinSelf` → `Join(self_address)` and stopped. `seed_nodes_json` as the spec. `ClusterBootstrap.start`: with `ClusterSettings.seed_nodes` set, log Pekko's "bailing out" warning and return; else spawn the coordinator once under `system._system_root`. `PekkoManagement._start` registers `GET /bootstrap/seed-nodes` on a cluster node and creates `ClusterBootstrap` when `settings.bootstrap` is set; `start()` calls `ClusterBootstrap(system).start((effective_hostname, bound_port))` after binding.

- [ ] **Step 7: Run the tests three times, then the suite**

Run: `for i in 1 2 3; do pytest tests/management -q 2>&1 | tail -1; done && pytest -q 2>&1 | tail -1 && pyright | tail -1`
Expected: all PASS each time, 0 errors.

- [ ] **Step 8: Commit**

```bash
git add src/pekko_py/management src/pekko_py/__init__.py tests/management
git commit -m "Cluster bootstrap: discovery, contact-point probing and the lowest-address join decider, as Pekko Management's"
```

---

### Task 5: Kubernetes API discovery

**Files:**
- Create: `src/pekko_py/management/kubernetes.py`; `tests/management/fixtures/cert.pem` and `key.pem` (generated once with `openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 36500 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1"`); `tests/management/fixtures/pods.json` (a `PodList` with: a Running pod, IP `10.1.0.5`, container `app` with port `management` 7626 and container status running; a Running pod with `metadata.deletionTimestamp`; a Pending pod; a Running pod whose `app` container status is `waiting`; a Running pod with `status` but no `podIP`; a pod with no `metadata`)
- Modify: `src/pekko_py/__init__.py`
- Test: `tests/management/test_kubernetes.py`

**Interfaces:**
- Consumes: Task 1's `get`, `ServiceDiscovery`, `Resolved`, `ResolvedTarget`, `Lookup`.
- Produces: `KubernetesApiSettings(...)` as the spec; `class KubernetesApiError(Exception)`; `targets(pod_list: Mapping[str, Any], port_name: str | None, namespace: str, pod_domain: str, raw_ip: bool, container_name: str | None) -> list[ResolvedTarget]`; `KubernetesApiServiceDiscovery(settings: KubernetesApiSettings = KubernetesApiSettings())`. Exports: `KubernetesApiServiceDiscovery`, `KubernetesApiSettings`.

- [ ] **Step 1: Write the failing tests**

```python
PODS = json.loads((Path(__file__).parent / "fixtures" / "pods.json").read_text())

def test_targets_filter_pods_and_pick_the_named_port():
    assert targets(PODS, "management", "ns", "cluster.local", False, None) == [ResolvedTarget("10-1-0-5.ns.pod.cluster.local", 7626, "10.1.0.5")]
    assert targets(PODS, "management", "ns", "cluster.local", True, None) == [ResolvedTarget("10.1.0.5", 7626, "10.1.0.5")]
    assert targets(PODS, None, "ns", "cluster.local", True, None) == [ResolvedTarget("10.1.0.5", None, "10.1.0.5")]
    assert targets(PODS, "management", "ns", "cluster.local", True, "app") == [ResolvedTarget("10.1.0.5", 7626, "10.1.0.5")]  # the waiting one is excluded

def test_targets_tolerate_missing_fields():
    assert targets({"items": [{"status": {"phase": "Running"}}, {"spec": {"containers": []}, "status": {"phase": "Running", "podIP": "1.2.3.4"}}, {}]}, "management", "ns", "d", True, None) == []

# fake_api(status, body, seen_headers) -> (server, port): an asyncio TLS server using the fixture cert and key that reads one
# request, records its Authorization header and the request target, and answers `status` with `body` as application/json

async def test_lookup_resolves_pods_and_rereads_the_token(tmp_path, monkeypatch):
    # token file "t1", namespace file "ns", the fixture cert copied as ca.crt; KUBERNETES_SERVICE_HOST=127.0.0.1, _PORT=<fake port>
    # settings = KubernetesApiSettings(api_ca_path=..., api_token_path=..., pod_namespace_path=..., use_raw_ip=True)
    # resolved = await KubernetesApiServiceDiscovery(settings).lookup(Lookup("svc", port_name="management"), 2.0)
    # resolved.addresses == (ResolvedTarget("10.1.0.5", 7626, "10.1.0.5"),); the request target was
    # "/api/v1/namespaces/ns/pods?labelSelector=app%3Dsvc" and the header "Bearer t1"; after rewriting the token to "t2" a second
    # lookup sends "Bearer t2"

async def test_lookup_fails_on_403_and_on_missing_env(tmp_path, monkeypatch, caplog):
    # 403 -> KubernetesApiError and "check RBAC" in caplog; KUBERNETES_SERVICE_HOST unset -> KubernetesApiError("... check Kubernetes environment ...")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/management/test_kubernetes.py -q 2>&1 | tail -3`
Expected: FAIL at import.

- [ ] **Step 3: Implement** `kubernetes.py` as the spec: per lookup read the token and namespace files and the two environment variables, build `ssl.create_default_context(cafile=api_ca_path)`, call `get(f"https://{host}:{port}/api/v1/namespaces/{ns}/pods?labelSelector={quote(pod_label_selector % service_name)}", headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, timeout=resolve_timeout, ssl_context=ctx)`; 200 parses and maps through `targets`, 403 logs and raises, other statuses raise; `targets` ports `KubernetesApiServiceDiscovery.targets` with `.get` on every optional field.

- [ ] **Step 4: Run the tests and pyright**

Run: `pytest tests/management -q 2>&1 | tail -1 && pyright | tail -1`
Expected: all PASS, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/management/kubernetes.py src/pekko_py/__init__.py tests/management
git commit -m "Kubernetes API discovery: pods by label through the service account, as pekko-discovery-kubernetes-api"
```

---

### Task 6: Example and README

**Files:**
- Modify: `examples/cluster/node.py` (a `--bootstrap` mode), `README.md` (Tour subsection after "### Coordinated shutdown"; names rows; deviations; examples row), `CLAUDE.md`

- [ ] **Step 1: Example** — `node.py NAME PORT --bootstrap PORT...`: no seed nodes; `management=PekkoManagementSettings(hostname=HOST, port=port + 1000, discovery=ConfigServiceDiscovery({"cluster": [ResolvedTarget(HOST, p + 1000, HOST) for p in all_ports]}), bootstrap=ClusterBootstrapSettings(service_name="cluster", stable_margin=3.0, required_contact_point_nr=2))`; `await PekkoManagement(system).start()` before waiting; the docstring shows three terminals forming a cluster without seeds and `curl localhost:3551/cluster/members`. Run three nodes and confirm one cluster forms and the members route answers.
- [ ] **Step 2: README** — a "Management and Kubernetes bootstrap" subsection: what the endpoint serves and on which port; the Kubernetes settings snippet (`KubernetesApiServiceDiscovery()`, `ClusterBootstrapSettings(service_name="myapp", port_name="management")`, `await PekkoManagement(system).start()`); the Kubernetes essentials as a list (pod label `app=myapp`; a Role with `get` and `list` on `pods` bound to the pod's service account; container ports for remoting and `management` 7626; `readinessProbe` `/ready` and `livenessProbe` `/alive` on 7626; `KUBERNETES_NAMESPACE` or the mounted namespace file; set `new_cluster_enabled=False` once the cluster exists if you want to forbid accidental new clusters); the local `--bootstrap` example. Names rows for `PekkoManagement(system).start()`, `pekko.management.http.*`, `pekko.management.health-checks.*`, `ClusterBootstrap`, `pekko.management.cluster.bootstrap.*`, `pekko.discovery.kubernetes-api.*`, `ServiceDiscovery.lookup`; the deviations bullet from the spec; the examples row. CLAUDE.md: `discovery.py` in the core list and a `management/` line.
- [ ] **Step 3: Verify** — `pytest -q | tail -1`, `pyright | tail -1`, the README snippet pasted into a scratch file imports and type-checks.
- [ ] **Step 4: Commit**

```bash
git add examples/cluster/node.py README.md CLAUDE.md
git commit -m "Management: example bootstrap mode and README"
```
