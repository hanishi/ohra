# Appendix A: Apache Pekko names

| Apache Pekko | ohra |
|---|---|
| `Behaviors.receive / receiveMessage / receiveSignal / setup / same / stopped / unhandled / empty / ignore` | same names, snake_case; handlers may be `async def` |
| `ActorRef[-T]` | `ActorRef[T]`, contravariant, enforced by pyright strict |
| `sealed trait` + case classes | `type Cmd = A \| B` + frozen dataclasses + `match` |
| `ask`, `ctx.ask` | `await ask(ref, make, timeout)`, `ctx.ask(ref, make, adapt, timeout)`, or `@ctx.ask(ref, make, timeout=...)` over `adapt` |
| `ctx.pipeToSelf(future)(Try => T)` | `ctx.pipe_to_self(awaitable, adapt)` with `Success` / `Failure`, or `@ctx.pipe_to_self(awaitable)` over `adapt` |
| `ctx.messageAdapter` | `ctx.message_adapter` |
| `spawn`, `spawnAnonymous`, `ctx.stop(child)` | same |
| `watch`, `watchWith`, `Terminated`, `ChildFailed`, `DeathPactException` | same; `DeathPactError` |
| `PreRestart`, `PostStop` | same |
| `SupervisorStrategy.resume / restart / stop / restartWithBackoff`, `withLimit`, `withResetBackoffAfter`, `withStashCapacity` | same, snake_case |
| `Behaviors.setup(ctx => ...)` | `@Behaviors.setup` on a `def`, or the call form; the same for `receive`, `receive_message` and `receive_signal` |
| (none) | `ctx.timers`, the same scheduler `with_timers` hands out, so one `setup` needs no timer factory layer |
| `Behaviors.withTimers`, `TimerScheduler.startSingleTimer / startTimerWithFixedDelay / cancel / cancelAll / isTimerActive` | same, snake_case |
| `Behaviors.withStash`, `StashBuffer.stash / unstashAll / size / isEmpty / isFull`, `StashOverflowException` | same, snake_case; `StashOverflowError` |
| `MailboxSelector.bounded(capacity)` passed to `spawn` | same, plus `overflow="drop_new" \| "drop_old" \| "fail"` |
| (none) | `await ref.offer(msg)`: `tell` that waits for room on a full bounded mailbox |
| `ClusterSharding.init(Entity(key)(create))` | `ctx.spawn(Sharding.init(Entity(key, create)), name)`; single node |
| `sharding.entityRefFor(key, id)` | `Sharding.entity_ref_for(region_ref, id)` |
| `EntityTypeKey`, `EntityContext`, `EntityRef`, `ShardingEnvelope`, `ClusterSharding.Passivate`, `GetShardRegionState` | same; `Passivate` |
| `DurableStateBehavior(persistenceId, emptyState, commandHandler)` | `@Behaviors.persisted(store)` on a state-to-behavior factory, started with `factory.recover(id, empty)` |
| `Effect.persist(state).thenReply(replyTo)(f)`, `Effect.none` | `factory(state).then_reply(ref, msg)` / `.then_run(callback)`; any plain behavior, including `same`, writes nothing |
| durable state stores (JDBC, R2DBC) | `InMemoryStore`, `SqliteStore(path, codec=...)`, `ohra.ext.postgres.PostgresStore(dsn, table=..., codec=...)` |
| `pekko.remote.artery.canonical.hostname / port` | `ActorSystem(..., address=(host, port))`; paths read `ohra://name@host:port/user/...` |
| `ActorRefResolver.resolveActorRef(path)` | `system.resolve(path)`: a local ref, or a `RemoteRef` for another node |
| serialization bindings and manifests | `@remotable` on a frozen dataclass; self-describing JSON with `$type` and `$ref` |
| `pekko.cluster.seed-nodes` and the other `pekko.cluster.*` settings | `ActorSystem(..., cluster=ClusterSettings(seed_nodes=(Address(name, host, port), ...), ...))`; the failure detector settings are `ClusterSettings` fields in seconds |
| `Cluster(system).manager ! Join(addr)` / `JoinSeedNodes` / `Leave` / `Down` | `Cluster(system).manager.tell(Join(addr))`, same names |
| `Cluster(system).subscriptions ! Subscribe(ref, classOf[MemberEvent])`, `Unsubscribe`, `GetCurrentState` | `Cluster(system).subscriptions.tell(Subscribe(ref, MemberEvent))`; initial state always arrives as events |
| `Cluster(system).selfMember / state / selfUniqueAddress / isTerminated` | `self_member`, `state`, `self_unique_address`, `is_terminated` |
| `MemberEvent`, `ReachabilityEvent`, `LeaderChanged`, `SelfUp`, `SelfRemoved`, `CurrentClusterState` | same; `MemberRemoved(member, previous_status)` |
| `MemberStatus.Joining / WeaklyUp / Up / Leaving / Exiting / Down / Removed` | `member.status` is the string `"joining"`, `"weakly_up"`, ... |
| `Receptionist(system).ref ! Register(key, ref)`, `Deregister`, `Find(key, replyTo)` | `Receptionist(system).ref.tell(Register(key, ref))`, same names; `Registered` / `Deregistered` replies when `reply_to` is given |
| `ServiceKey[T]("id")` | `ServiceKey[T]("id")` |
| `Receptionist.Subscribe(key, subscriber)` | `ohra.receptionist.Subscribe(key, subscriber)`; the top-level `Subscribe` is the cluster's |
| `Listing.serviceInstances(key)` / `allServiceInstances(key)` / `servicesWereAddedOrRemoved` | `listing.service_instances` / `all_service_instances` / `services_were_added_or_removed`, fields of `Listing[T]` |
| `ClusterSingleton(system).init(SingletonActor(behavior, "name"))`, `withStopMessage`, `withSettings` | same, `with_stop_message`, `with_settings`; returns the proxy ref |
| `ClusterSingletonSettings` / `ClusterSingletonManagerSettings` (`removal-margin`, `hand-over-retry-interval`, `min-number-of-hand-over-retries`, `singleton-identification-interval`, `buffer-size`) | `ClusterSingletonSettings` with the same fields in snake_case, seconds |
| `CoordinatedShutdown(system).addTask(phase, name)(task)` | `CoordinatedShutdown(system).add_task(phase, name, task)`; `add_actor_termination_task`, `run(reason)`, `shutdown_reason`, `timeout(phase)`, `total_timeout` |
| `pekko.coordinated-shutdown.phases.<p>.timeout`, `default-phase-timeout` | `ActorSystem(..., shutdown=CoordinatedShutdownSettings(phase_timeouts={...}, default_phase_timeout=5.0))` |
| `run-by-actor-system-terminate`, `run-by-jvm-shutdown-hook` | `CoordinatedShutdownSettings(run_by_actor_system_terminate=True, run_on_signals=(signal.SIGTERM, ...))` |
| `ActorSystemTerminateReason`, `JvmExitReason`, `ClusterLeavingReason`, `ClusterDowningReason` | the same, as frozen dataclasses; subclass `Reason` for your own |
| `PekkoManagement(system).start()` / `routes` | `await Management(system).start() -> uri`; routes are built in: health checks, `/cluster/*`, `/bootstrap/seed-nodes` |
| `pekko.management.http.hostname / port / bind-hostname / bind-port / base-path` | `ActorSystem(..., management=ManagementSettings(hostname, port, bind_hostname, bind_port, base_path))` |
| `pekko.management.health-checks.{startup,readiness,liveness}-checks` | `Management(system).health.add_{startup,readiness,liveness}_check(name, check)`; `HealthCheckSettings` for the paths, timeout and `ready_states` |
| `ClusterBootstrap(system).start()` and `pekko.management.cluster.bootstrap.*` | started by `Management.start()` when `ManagementSettings.bootstrap` is set; `ClusterBootstrapSettings` fields in snake_case, seconds |
| `pekko.discovery.kubernetes-api.*` | `KubernetesApiServiceDiscovery(KubernetesApiSettings(...))` |
| `ServiceDiscovery.lookup(Lookup(name), timeout)` | `await discovery.lookup(Lookup(name, port_name), resolve_timeout) -> Resolved(service_name, addresses)` |
| `pekko.cluster.downing-provider-class = "...SplitBrainResolverProvider"` | `ClusterSettings(downing=SplitBrainResolverSettings(...))` |
| `split-brain-resolver.active-strategy`, `stable-after`, `down-all-when-unstable` | `SplitBrainResolverSettings(strategy, stable_after, down_all_when_unstable)` |
| `static-quorum.quorum-size`, `keep-oldest.down-if-alone` | `StaticQuorum(quorum_size)`, `KeepOldest(down_if_alone)`; `KeepMajority()`, `DownAll()` |
| `pekko.cluster.down-removal-margin` | `ClusterSettings.down_removal_margin` |
| `ClusterSharding(system).init(Entity(key, create))`, `entityRefFor(key, id)` | same, `entity_ref_for(key, id)`; the region ref is returned, as Pekko Typed |
| `HashCodeMessageExtractor(numberOfShards)` | `ClusterShardingSettings(number_of_shards=...)`; the shard id is `crc32(entity_id) % number_of_shards` |
| `ClusterShardingSettings` tuning parameters (`retry-interval`, `rebalance-interval`, `handoff-timeout`, `shard-start-timeout`, `buffer-size`, `rebalance-absolute-limit`, `rebalance-relative-limit`, `coordinator-singleton`) | `ClusterShardingSettings` fields in snake_case, seconds; `coordinator_singleton_settings` is a `ClusterSingletonSettings` |
| `Routers.pool(n)(behavior)`, `withRoundRobinRouting / withRandomRouting / withConsistentHashingRouting` | `Routers.pool(n, behavior)`, same methods in snake_case |
| `SpawnProtocol()`, `SpawnProtocol.Spawn(behavior, name, props, replyTo)` | `SpawnProtocol.apply()`, `SpawnProtocol.Spawn(behavior, name, reply_to, mailbox=None)` |
| (implicit) | `Replyable[Res]`, the protocol a request satisfies by having a `reply_to: ActorRef[Res]` field |
| `system.ignoreRef` | `system.ignore_ref` |
| (none) | `batching(max_n, max_wait, handle)` |
| (none) | `ohra.ext.shm.SharedArray`, a picklable handle to an array in shared memory |

---

[← Putting it together: one cart request](11-putting-it-together.md) · [Contents](README.md) · [Appendix B: Known deviations from Apache Pekko →](appendix-deviations.md)
