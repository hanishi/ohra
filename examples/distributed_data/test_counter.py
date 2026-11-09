"""Pekko's ReplicatorDocSpec, on one cluster node: the multi-node test kit with a single role."""

from counter import GetCachedValue, GetValue, Increment, Unsubscribe, counter
from ohra import ClusterSettings
from ohra.cluster.ddata import DistributedData, GCounter, GCounterKey, Replicator
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

config = MultiNodeConfig()
config.role("node")
config.common_config(cluster=ClusterSettings())


@multi_node_spec(config)
async def test_have_api_for_update_and_get(spec: MultiNodeSpec):
    c = spec.spawn(counter(GCounterKey("counter1")))

    probe: TestProbe[int] = spec.create_test_probe()
    c.tell(Increment())
    c.tell(GetValue(probe.ref))
    await probe.expect_message(1)


@multi_node_spec(config)
async def test_have_api_for_subscribe_and_unsubscribe(spec: MultiNodeSpec):
    c = spec.spawn(counter(GCounterKey("counter2")))
    probe: TestProbe[int] = spec.create_test_probe()

    def cached_value_is(n: int):
        async def check() -> None:
            c.tell(GetCachedValue(probe.ref))
            assert await probe.receive_message() == n

        return check

    c.tell(Increment())
    c.tell(Increment())
    await spec.await_assert(cached_value_is(2))
    c.tell(Increment())
    await spec.await_assert(cached_value_is(3))

    c.tell(Unsubscribe())
    c.tell(Increment())
    c.tell(GetValue(probe.ref))
    await probe.expect_message(4)  # the Replicator has the increment
    # a Changed would go out now if we hadn't unsubscribed
    DistributedData(spec.system).replicator.tell(Replicator.FlushChanges())
    # wait so it would update the cached value if we didn't unsubscribe
    await probe.expect_no_message(0.5)
    c.tell(GetCachedValue(probe.ref))
    await probe.expect_message(3)  # old value, not 4


@multi_node_spec(config)
async def test_have_an_extension(spec: MultiNodeSpec):
    key = GCounterKey("counter3")
    c = spec.spawn(counter(key))

    probe: TestProbe[int] = spec.create_test_probe()
    c.tell(Increment())
    c.tell(GetValue(probe.ref))
    await probe.expect_message(1)

    get_reply_probe: TestProbe[Replicator.GetResponse[GCounter]] = spec.create_test_probe()
    replicator = DistributedData(spec.system).replicator
    replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), get_reply_probe.ref))
    rsp = await get_reply_probe.receive_message()
    assert isinstance(rsp, Replicator.GetSuccess)
    assert rsp.get(key).value == 1
