# 6. Cluster singleton

`ClusterSingleton(system).init(SingletonActor(behavior, name))` runs on every node and returns a proxy ref. Behind it are two actors per node, ported state for state from Pekko.

- **The manager** at `/system/singletonManager<name>` tracks members ordered by age. Only the manager on the oldest Up member starts the real actor. The others wait as the next in line.
- **The proxy** at `/system/singletonProxy<name>` finds the current instance by asking each node's manager in age order, buffers messages until it does, and forwards to it. When the instance moves, the proxy buffers again and finds the new one.
- **Hand-over.** When the oldest member leaves, its manager stops the instance and confirms with `HandOverDone` to the next oldest, which then starts its own. The two coordinated-shutdown tasks in `cluster-exiting` hold the leaving node until the hand-over is through. If the oldest crashes instead, the next oldest takes over once the crashed member is removed.

There is never more than one instance as long as membership is agreed. That is why removal of a crashed member waits for the split-brain resolver rather than happening on a timeout.

## Using it

`ClusterSingleton(system).init(SingletonActor(behavior, "name"))` runs exactly one instance of `behavior` in the cluster, on the oldest member, and returns a proxy ref that works on every node: messages sent through it reach the instance wherever it is, buffered while its location is unknown. When the host leaves, the instance is handed over to the next oldest member during the exiting phase, so it is stopped on the old node before it starts on the new one; when the host crashes, the next oldest starts it once the crashed member has been downed and removed. Apache Pekko's ClusterSingletonManager and ClusterSingletonProxy, state for state.

```python
from pekko_py import ClusterSingleton, SingletonActor

proxy = ClusterSingleton(system).init(SingletonActor(counter, "counter"))  # call it on every node
# answered by the one instance
print(await ask(proxy, Hit, timeout=3))
```

Give the instance a stop message with `with_stop_message` if it needs to finish work before it goes; otherwise it is stopped after its pending messages, as Pekko's PoisonPill. `ClusterSingletonSettings` carries Pekko's `removal_margin`, `hand_over_retry_interval`, `min_number_of_hand_over_retries`, `singleton_identification_interval` and `buffer_size`. An instance that fails is not restarted by the manager; wrap it in `Behaviors.supervise` as in Pekko.

---

[← Cluster membership](05-cluster.md) · [Contents](README.md) · [Sharding →](07-sharding.md)
