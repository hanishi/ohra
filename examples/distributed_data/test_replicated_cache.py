"""Pekko's ReplicatedCacheSpec (pekko-sample-distributed-data-scala), on three cluster nodes.

``test_join_cluster``, ``test_replicate_cached_entry``, ``test_replicate_many_cached_entries``,
``test_replicate_evicted_entry`` and ``test_replicate_updated_cached_entry`` are Pekko's five
cases. ``test_the_data_key_is_the_same_in_every_process`` is ohra's own: ``data_key`` must not
depend on the per-process ``hash``.
"""

import os
import subprocess
import sys
import zlib
from pathlib import Path

from replicated_cache import Cached, Command, Evict, GetFromCache, PutInCache, data_key
from replicated_cache import replicated_cache
from ohra import Address, ActorRef, Cluster, ClusterSettings, Join
from ohra.cluster.ddata import DistributedData, Replicator
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

config = MultiNodeConfig()
node1 = config.role("node-1")
node2 = config.role("node-2")
node3 = config.role("node-3")
config.common_config(cluster=ClusterSettings())


async def join_cluster(spec: MultiNodeSpec) -> None:
    for src, dst in ((node1, node1), (node2, node1), (node3, node1)):
        if spec.is_node(src):
            Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(dst))))
        await spec.enter_barrier(f"{src.name}-joined")

    replicator = DistributedData(spec.system).replicator
    probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

    async def all_are_replicas() -> None:
        replicator.tell(Replicator.GetReplicaCount(probe.ref))
        assert await probe.receive_message() == Replicator.ReplicaCount(3)

    await spec.await_assert(all_are_replicas, max=20.0)
    await spec.enter_barrier("after-1")


async def expect_cached(
    spec: MultiNodeSpec,
    cache: ActorRef[Command],
    entries: dict[str, str | None],
    max: float,
) -> None:
    async def has_entries() -> None:
        probe: TestProbe[Cached] = spec.create_test_probe()
        for key, value in entries.items():
            cache.tell(GetFromCache(key, probe.ref))
            assert await probe.receive_message(3.0) == Cached(key, value)

    await spec.await_assert(has_entries, max=max)


@multi_node_spec(config)
async def test_join_cluster(spec: MultiNodeSpec):
    await join_cluster(spec)


@multi_node_spec(config)
async def test_replicate_cached_entry(spec: MultiNodeSpec):
    cache = spec.spawn(replicated_cache())
    await join_cluster(spec)
    if spec.is_node(node1):
        cache.tell(PutInCache("key1", "A"))
    await expect_cached(spec, cache, {"key1": "A"}, 10.0)
    await spec.enter_barrier("after-2")


@multi_node_spec(config)
async def test_replicate_many_cached_entries(spec: MultiNodeSpec):
    cache = spec.spawn(replicated_cache())
    await join_cluster(spec)
    if spec.is_node(node1):
        for i in range(100, 201):
            cache.tell(PutInCache(f"key{i}", f"entry-{i}"))
    await expect_cached(spec, cache, {f"key{i}": f"entry-{i}" for i in range(100, 201)}, 10.0)
    await spec.enter_barrier("after-3")


@multi_node_spec(config)
async def test_replicate_evicted_entry(spec: MultiNodeSpec):
    cache = spec.spawn(replicated_cache())
    await join_cluster(spec)
    if spec.is_node(node1):
        cache.tell(PutInCache("key2", "B"))
    await expect_cached(spec, cache, {"key2": "B"}, 15.0)
    await spec.enter_barrier("key2-replicated")
    if spec.is_node(node3):
        cache.tell(Evict("key2"))
    await expect_cached(spec, cache, {"key2": None}, 15.0)
    await spec.enter_barrier("after-4")


@multi_node_spec(config)
async def test_replicate_updated_cached_entry(spec: MultiNodeSpec):
    cache = spec.spawn(replicated_cache())
    await join_cluster(spec)
    if spec.is_node(node2):
        cache.tell(PutInCache("key1", "A2"))
        cache.tell(PutInCache("key1", "A3"))
    await expect_cached(spec, cache, {"key1": "A3"}, 10.0)
    await spec.enter_barrier("after-5")


def test_the_data_key_is_the_same_in_every_process():
    assert data_key("key1").id == f"cache-{zlib.crc32(b'key1') % 100}"
    here = Path(__file__).parent
    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "from replicated_cache import data_key; print(data_key('key1').id)",
        ],
        cwd=here,
        env={**os.environ, "PYTHONHASHSEED": "1"},
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    assert out.stdout.strip() == data_key("key1").id
