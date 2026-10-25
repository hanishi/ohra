# Management and Kubernetes bootstrap design (cluster layer 8)

Apache Pekko Management narrowed to what a Kubernetes deployment uses on day one, read from the local
`pekko-management` checkout: the management HTTP endpoint with the health checks (`management/`), cluster bootstrap
with the lowest-address join decider (`management-cluster-bootstrap/`), Kubernetes API discovery
(`discovery-kubernetes-api/`), read-only cluster HTTP management (`management-cluster-http/`), and the Pekko
Discovery API it all rests on (`pekko/discovery/`). Dropped: the Kubernetes lease, rolling-update support, the
other discovery methods, log-level routes, PKI, mutating cluster routes, HTTPS on the endpoint, and loading checks
or routes by class name. Everything is standard library: a minimal HTTP/1.1 server and client on asyncio streams,
`ssl` for the API server. Designed with the user on 2026-10-25.

## Discovery (`pekko_py/discovery.py`, exported from `pekko_py`)

- `Lookup(service_name: str, port_name: str | None = None, protocol: str | None = None)`, `ResolvedTarget(host:
  str, port: int | None, address: str | None)` (the IP when known), `Resolved(service_name: str, addresses:
  tuple[ResolvedTarget, ...])`, frozen dataclasses. `ResolvedTarget` orders by `(host, port or 0)`, Pekko's ordering
  the decider sorts by.
- `class ServiceDiscovery` with `async def lookup(self, lookup: Lookup, resolve_timeout: float) -> Resolved`
  raising `DiscoveryTimeoutError` on timeout; any other exception means the lookup failed.
- `ConfigServiceDiscovery(services: Mapping[str, Sequence[ResolvedTarget]])`: Pekko's config discovery; an
  unknown name resolves to no addresses.

## Kubernetes API discovery (`pekko_py/management/kubernetes.py`)

- `KubernetesApiSettings(api_ca_path="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt", api_token_path=
  ".../token", api_service_host_env_name="KUBERNETES_SERVICE_HOST", api_service_port_env_name=
  "KUBERNETES_SERVICE_PORT", pod_namespace_path=".../namespace", pod_namespace: str | None = None (else the
  `KUBERNETES_NAMESPACE` variable, else the file), pod_domain="cluster.local", pod_label_selector="app=%s",
  use_raw_ip=False, container_name: str | None = None)`, Pekko's names and defaults.
- `KubernetesApiServiceDiscovery(settings=KubernetesApiSettings())`: `lookup` reads the token and namespace on
  every call (bound tokens rotate), issues `GET /api/v1/namespaces/{ns}/pods?labelSelector={pod_label_selector %
  service_name}` with `Authorization: Bearer` over TLS verified against `api_ca_path`, bounded by `resolve_timeout`.
  Targets, as `KubernetesApiServiceDiscovery.targets`: pods without `metadata.deletionTimestamp`, with
  `status.phase == "Running"`, with `container_name`'s status not `waiting` when set, taking `status.podIP`; the
  port is the container port named `lookup.port_name` (none when no port name); the host is the pod IP when
  `use_raw_ip`, else `{ip with dots as dashes}.{namespace}.pod.{pod_domain}`. A 403 logs "Forbidden ... check RBAC
  settings" and raises `KubernetesApiError`; any other non-200 raises it with the status; a missing service host
  variable raises it with Pekko's "check Kubernetes environment" message.

## The management endpoint (`pekko_py/management/__init__.py`, `http.py`, `health.py`)

- `PekkoManagementSettings(hostname: str | None = None, port: int = 7626, bind_hostname: str | None = None,
  bind_port: int | None = None, base_path: str = "", health_checks: HealthCheckSettings = HealthCheckSettings(),
  bootstrap: ClusterBootstrapSettings | None = None, discovery: ServiceDiscovery | None = None)`. `hostname=None`
  is this host's address (`socket.gethostbyname(socket.gethostname())`, Pekko's `InetAddress.getLocalHost`);
  `bind_*` default to `hostname` and `port`; `base_path` is a prefix like `/management`. `bootstrap` requires
  `discovery`.
- `HealthCheckSettings(startup_path="startup", readiness_path="ready", liveness_path="alive", check_timeout=1.0,
  ready_states=frozenset({"up", "weakly_up"}))`.
- `PekkoManagement(system)`, created with the system, reached like the other extensions; `settings` from
  `ActorSystem(..., management=PekkoManagementSettings(...))`, default settings when not given.
  - `async start() -> str`: binds the server once and returns the base URI `http://{hostname}:{port}{base_path}`;
    a second call returns the same URI. Routes: the health checks always; on a cluster node `/bootstrap/seed-nodes`
    and `/cluster/...` too. When `settings.bootstrap` is set it then starts `ClusterBootstrap`. Logs Pekko's
    "Bound Pekko Management (HTTP) endpoint to: {}:{}".
  - `async stop()`: closes the server. The system root's done callback also closes it, so `terminate()` needs no
    task; the handle is a tracked task that `when_terminated()` awaits.
  - `health: HealthChecks`.
- `HealthChecks`: `add_startup_check(name, check)`, `add_readiness_check(name, check)`, `add_liveness_check(name,
  check)` with `check: Callable[[], Awaitable[bool]]`; `async startup() / ready() / alive() -> str | None`, the
  first failing check's reason (`"Check [name] not ok"`, `"Check [name] failed: ..."`, `"Check [name] timed out
  after ..."`) or `None`, each check under `asyncio.wait_for(check_timeout)`, as `HealthChecksImpl.check`. On a
  cluster node the readiness check `cluster-membership` is registered at creation: self status in `ready_states`.
  Routes `GET {startup_path}`, `{readiness_path}`, `{liveness_path}`: `200 OK` or `500 Not Healthy: {reason}`.
- The server (`http.py`): `serve(host, port, handler) -> asyncio.Server` reading one HTTP/1.1 request per
  connection (request line, headers, no body), dispatching `handler(method, path) -> Response(status, body,
  content_type)`, answering `Connection: close`; unknown paths 404, methods other than GET 405, a handler exception
  500. The client: `async get(url, *, headers, timeout, ssl_context=None) -> tuple[int, bytes]` over
  `asyncio.open_connection`, reading a `Content-Length` or chunked body, `Connection: close`.

## Cluster bootstrap (`pekko_py/management/bootstrap.py`)

- `ClusterBootstrapSettings(service_name: str, port_name: str | None = None, protocol: str = "tcp",
  new_cluster_enabled: bool = True, stable_margin: float = 5.0, interval: float = 1.0,
  exponential_backoff_random_factor: float = 0.2, exponential_backoff_max: float = 15.0,
  required_contact_point_nr: int = 2, resolve_timeout: float = 3.0, contact_with_all_contact_points: bool = True,
  fallback_port: int | None = None, filter_on_fallback_port: bool = True, probing_failure_timeout: float = 3.0,
  probe_interval: float = 1.0, probe_interval_jitter: float = 0.2, http_max_seed_nodes_to_expose: int = 5)`;
  `fallback_port=None` means the management port. Validated like the other settings.
- `ClusterBootstrap(system)`: `start()` logs Pekko's warning and does nothing when `ClusterSettings.seed_nodes`
  is set; otherwise, once, spawns the coordinator at `/system/bootstrapCoordinator` with this node's management
  host and port as the self contact point.
- The coordinator (`BootstrapCoordinator.scala`):
  - Discovery every `interval`: `lookup(Lookup(service_name, port_name, protocol), resolve_timeout)`; on failure
    `last_contacts_observation = None`, interval backed off as `backedOffInterval` (doubling with the random factor,
    capped at `exponential_backoff_max`); on success the interval resets, the targets pass `select_hosts` (with a
    port name or `filter_on_fallback_port=False` unchanged; else per host, when several targets resolve and any has a
    port, keep those on the fallback port), and the observation is `ServiceContactsObservation(observed_at,
    contact_points)` keeping the previous `observed_at` when the set is unchanged (`sameOrChanged`).
  - A probe per contact point (`HttpContactPointBootstrap`), child
    `contactPointProbe-{host}-{port}`, refusing a target equal to this node's remoting address. Every
    `probe_interval` with `probe_interval_jitter`, `GET http://{host}:{port}{base_path}/bootstrap/seed-nodes`
    bounded by `probing_failure_timeout`; a success reports `SeedNodesObservation(observed_at, contact_point,
    self_node, seed_nodes)` and resets the failure deadline; a failure after `probing_failure_timeout` without a
    success reports `ProbingFailed` and the probe stops, the coordinator drops its observation and probes again on
    the next discovery that still lists it. Probes for contact points no longer discovered are stopped.
  - `decide()` on every `probe_interval` (Pekko's `DecideTick` timer uses the probe interval) and after any observation with seed nodes, once at a time:
    observations older than `probing_failure_timeout` are dropped; `SeedNodesInformation(now, contacts_changed_at,
    contact_points, observations)` goes to the decider; `JoinOtherSeedNodes(seeds)` tells the cluster manager
    `JoinSeedNodes(seeds - self)` and stops the coordinator; `JoinSelf` tells `Join(self_address)` and stops;
    `KeepProbing` continues.
  - `LowestAddressJoinDecider.decide`, as the Scala: seed nodes seen → join up to five of them; fewer than
    `required_contact_point_nr` observations → keep probing (info log); contact points changed less than
    `stable_margin` ago → keep probing; with `contact_with_all_contact_points`, a contact point without an
    observation → keep probing (info log); else the lowest observed contact point (sorted `ResolvedTarget`) matching
    this node's management host and port (`matchesSelf`: host or IP, and the port when the target has one) and
    `new_cluster_enabled` → `JoinSelf`; else keep probing with Pekko's "Expecting node [...] to perform the
    self-join" info, or the `new_cluster_enabled=False` warning.
- Route `GET /bootstrap/seed-nodes` → `{"selfNode": "<self address>", "seedNodes": [{"node": "...", "nodeUid":
  n, "status": "up", "roles": []}, ...]}`: reachable members that are Up, WeaklyUp or Joining, at most
  `http_max_seed_nodes_to_expose`, logged as Pekko's "Bootstrap request from {}: Contact Point returning {}
  seed-nodes".

## Cluster HTTP management (`pekko_py/management/cluster_http.py`), read-only

- `GET /cluster/members` → `{"selfNode", "members": [{"node", "nodeUid", "status", "roles": []}], "unreachable":
  [{"node", "observedBy": [...]}], "leader", "oldest", "oldestPerRole": {}}`; `oldest` is the oldest Up member.
- `GET /cluster/members/{address}` → one member, else 404 `{"message": "Member [...] not found"}`.
- `GET /cluster/shards/{type}` → `{"regions": [{"shardId", "numEntities"}]}` from the local region's
  `GetShardRegionState` (five-second ask), 404 when `ClusterSharding.init` was not called for the type here.
- `POST`, `PUT`, `DELETE` under `/cluster` → 405 (Pekko's `route-providers-read-only` default).

## Invariants

- One management server per system; nothing binds or probes until `PekkoManagement(system).start()`.
- Bootstrap never joins with explicit seed nodes configured, never forms a cluster when `new_cluster_enabled` is
  off, and joins self only when every discovered contact point has answered and the set has been stable.
- Everything stops with the system: the server on the system root's stop, the coordinator and probes as actors.
- No new dependency.

## Deviations to document

Standard-library HTTP/1.1 (GET-only server, one request per connection; no HTTPS on the endpoint, Pekko's TLS
client for probing dropped); settings objects replace HOCON and class-name loading (`routes`, `*-checks`,
`join-decider.class`, `discovery-method`); only `KubernetesApiServiceDiscovery` and `ConfigServiceDiscovery`;
`roles` are always empty and `oldestPerRole` is `{}`; mutating cluster routes answer 405 with no switch; no
`/cluster/domain-events` stream; bootstrap starts from `PekkoManagement.start()` rather than being auto-started as a
route provider; `ResolvedTarget.address` is a string.

## Tests

Unit (`tests/test_management.py`, `tests/test_discovery.py`, `tests/management/test_bootstrap_decider.py`,
`tests/management/test_kubernetes.py`): `select_hosts` with and without a port name and fallback port;
the decider on hand-built `SeedNodesInformation`: seed nodes present, too few points, inside the margin, a missing
observation, lowest is self, lowest is other, `new_cluster_enabled=False`, and `matchesSelf` by IP; the Kubernetes
`targets` filter on a recorded `PodList` (deleting, pending, waiting container, named port, raw IP vs pod DNS
name); the client against the server: a JSON GET, 404, 405, a handler error → 500, a client timeout; health checks:
all pass, one `False` names itself, one raising, one timing out, the routes' 200/500; `PekkoManagement.start()`
twice returns one URI; `/cluster/members` JSON shape on a single node.

Multi-node at millisecond settings (`tests/management/test_bootstrap_cluster.py`) with `ConfigServiceDiscovery`
over the nodes' management addresses: three nodes with no seed nodes form one cluster and the lowest management
address is the one that joined itself; a fourth node started later joins through probing; a node whose discovery
omits one peer joins once it sees seed nodes; `new_cluster_enabled=False` everywhere forms nothing within a
stable margin and a half; `/ready` is 500 before Up and 200 after; `/cluster/shards/{type}` after sharding starts;
the Kubernetes discovery against a fake API server (an asyncio TLS server with a self-signed certificate in a temp
dir, a token file and a namespace file) resolves the expected targets, re-reads a rotated token, and fails on 403;
every test under the leak check.

## Example and docs

`examples/cluster/node.py` gains `--bootstrap PORT...`: management on `port + 1000`, `ConfigServiceDiscovery`
over the given ports' management addresses, no seed nodes; three terminals form a cluster by themselves. README: a
"Management and Kubernetes bootstrap" Tour subsection (settings snippet, the Kubernetes essentials: pod label,
RBAC `get`/`list` on pods, container ports for remoting and 7626, readiness and liveness probes on `/ready` and
`/alive`, `KUBERNETES_NAMESPACE`), names rows, the deviations above, the examples row. CLAUDE.md layout line.
