"""Pekko's multi-node ``WildcardSubscribeSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/WildcardSubscribeSpec.scala``),
as one ``@multi_node_spec`` test whose cases run in Pekko's order and share its cluster, with the
spec's own barrier names.

Each case is a section, at its line in the Scala file:

- ``notify_changed_entry``: "notify changed entry", :71
- ``notify_expired_entry``: "notify expired entry", :146
- ``notify_when_changed_from_another_node``: "notify when changed from another node", :174

Mapping:

- Pekko starts its own Replicator under ``/user/replicator``. Here the ``DistributedData``
  extension's Replicator runs with Pekko's ``notify-subscribers-interval`` of 0.1 s and
  ``expire-keys-after-inactivity`` of two seconds for ``expiry-*``, and a gossip interval of 0.1 s
  instead of 0.5 s.
- Pekko's classic ``replicator ! Update(...)`` with ``ImplicitSender`` is the typed protocol here,
  with a ``TestProbe`` as ``reply_to``. Pekko's ``NotFound(KeyExpiryA, None)`` is
  ``Replicator.NotFound(KeyExpiryA)``.
- "notify expired entry" waits five seconds with ``expectNoMessage(5.seconds)``, then reads the key
  and expects ``Expired``. Here the spec waits up to five seconds for ``Expired`` itself, and then
  reads the key: a ``Get`` refreshes the key's used timestamp, so the key cannot be polled until it
  expires.
- Pekko's ``enterBarrier("done-3")`` sits in the ``must`` block outside any case, so ScalaTest runs
  it when it registers the cases; here it closes the last section.

No assertion is skipped: none of them needs pruning, durable storage, deltas or roles.
"""

from __future__ import annotations

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    Key,
    Replicator,
    ReplicatorSettings,
)
from ohra.remote import Address
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, RoleName, TestProbe, multi_node_spec

from ...test_cluster import FAST, FAST_SHUTDOWN

config = MultiNodeConfig()
first, second = config.role("first"), config.role("second")
config.common_config(
    cluster=ClusterSettings(
        **FAST,
        distributed_data=ReplicatorSettings(
            gossip_interval=0.1,
            notify_subscribers_interval=0.1,
            expire_keys_after_inactivity={"expiry-*": 2.0},
        ),
    ),
    shutdown=FAST_SHUTDOWN,
)

READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()

KeyA = GCounterKey("counter-A")
KeyB = GCounterKey("counter-B")
KeyOtherA = GCounterKey("other-A")
KeyExpiryA = GCounterKey("expiry-A")

type Subscriber = TestProbe[Replicator.SubscribeResponse[GCounter]]


async def changed(probe: Subscriber, max: float | None = None) -> Replicator.Changed[GCounter]:
    """Pekko's ``expectMsgType[Changed[GCounter]]``."""
    c = await probe.receive_message(max)
    assert isinstance(c, Replicator.Changed), c
    return c


@multi_node_spec(config)
async def test_wildcard_subscribe_spec(spec: MultiNodeSpec):
    cluster = Cluster(spec.system)
    replicator = DistributedData(spec.system).replicator
    self_address = DistributedData(spec.system).self_unique_address

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    async def plus(key: GCounterKey, n: int) -> None:
        """Pekko's ``replicator ! Update(key, GCounter.empty, WriteLocal)(_ :+ n)`` and
        ``expectMsgType[UpdateSuccess[_]]``."""
        probe: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
        replicator.tell(
            Replicator.Update(
                key, GCounter(), WRITE_LOCAL, probe.ref, lambda c: c.increment(self_address, n)
            )
        )
        await probe.expect_message_type(Replicator.UpdateSuccess)

    async def get(key: GCounterKey) -> Replicator.GetResponse[GCounter]:
        probe: TestProbe[Replicator.GetResponse[GCounter]] = spec.create_test_probe()
        replicator.tell(Replicator.Get(key, READ_LOCAL, probe.ref))
        return await probe.receive_message()

    def subscribe(key: Key[GCounter]) -> Subscriber:
        probe: Subscriber = spec.create_test_probe()
        replicator.tell(Replicator.Subscribe(key, probe.ref))
        return probe

    async def notify_changed_entry() -> None:
        await join(first, first)

        if spec.is_node(first):
            subscriber_probe = subscribe(GCounterKey("counter-*"))

            await plus(KeyA, 1)
            chg1 = await changed(subscriber_probe)
            assert chg1.key == KeyA
            assert type(chg1.key) is type(KeyA)
            assert chg1.data.value == 1

            await plus(KeyA, 1)
            chg2 = await changed(subscriber_probe)
            assert chg2.key == KeyA
            assert chg2.data.value == 2

            await plus(KeyB, 1)
            chg3 = await changed(subscriber_probe)
            assert chg3.key == KeyB
            assert chg3.data.value == 1

            await plus(KeyOtherA, 17)
            await subscriber_probe.expect_no_message(0.2)

            # a few more subscribers
            subscriber_probe2 = subscribe(GCounterKey("counter-*"))
            subscriber_probe_a = subscribe(KeyA)
            subscriber_probe_other = subscribe(GCounterKey("other-*"))
            await subscriber_probe.expect_no_message(0.2)
            for chg in await subscriber_probe2.receive_messages(2):
                assert isinstance(chg, Replicator.Changed), f"Unexpected {chg}"
                if chg.key == KeyA:
                    assert chg.data.value == 2
                elif chg.key == KeyB:
                    assert chg.data.value == 1
                else:
                    raise AssertionError(f"unexpected change {chg.key}")
            await subscriber_probe2.expect_no_message()
            assert (await changed(subscriber_probe_a)).data.value == 2
            assert (await changed(subscriber_probe_other)).data.value == 17

            await plus(KeyB, 10)
            chg4 = await changed(subscriber_probe)
            assert chg4.key == KeyB
            assert chg4.data.value == 11
            chg5 = await changed(subscriber_probe2)
            assert chg5.key == KeyB
            assert chg5.data.value == 11

            # unsubscribe
            replicator.tell(Replicator.Unsubscribe(GCounterKey("counter-*"), subscriber_probe.ref))
            await plus(KeyB, 5)
            await subscriber_probe.expect_no_message(0.2)
            chg6 = await changed(subscriber_probe2)
            assert chg6.key == KeyB
            assert chg6.data.value == 16

        await spec.enter_barrier("done-1")

    async def notify_expired_entry() -> None:
        if spec.is_node(first):
            subscriber_probe = subscribe(GCounterKey("expiry-*"))

            await plus(KeyExpiryA, 1)
            await changed(subscriber_probe)

            got = await get(KeyExpiryA)
            assert isinstance(got, Replicator.GetSuccess) and got.data.value == 1, got

            # Pekko's expectNoMessage(5.seconds) before the Get: a Get would keep the key alive
            await subscriber_probe.expect_message(Replicator.Expired(KeyExpiryA), 5.0)
            assert await get(KeyExpiryA) == Replicator.NotFound(KeyExpiryA)

            # same key can be used again
            await plus(KeyExpiryA, 2)
            await changed(subscriber_probe)
            got = await get(KeyExpiryA)
            assert isinstance(got, Replicator.GetSuccess) and got.data.value == 2, got

        await spec.enter_barrier("done-2")

    async def notify_when_changed_from_another_node() -> None:
        if spec.is_node(first):
            await plus(KeyOtherA, 1)
            await spec.enter_barrier("updated-1")

            await spec.enter_barrier("second-joined")

            await spec.enter_barrier("received-1")

            await plus(KeyOtherA, 1)
            await spec.enter_barrier("updated-2")

            await spec.enter_barrier("received-2")

        if spec.is_node(second):
            await spec.enter_barrier("updated-1")

            # it's possible to subscribe before join
            subscriber_probe = subscribe(GCounterKey("other-*"))

            await join(second, first)

            chg1 = await changed(subscriber_probe, 10.0)
            assert chg1.key == KeyOtherA
            assert chg1.data.value == 18  # it was also incremented to 17 in earlier test
            await spec.enter_barrier("received-1")

            await spec.enter_barrier("updated-2")

            chg2 = await changed(subscriber_probe)
            assert chg2.key == KeyOtherA
            assert chg2.data.value == 19
            await spec.enter_barrier("received-2")

        await spec.enter_barrier("done-3")

    await notify_changed_entry()
    await notify_expired_entry()
    await notify_when_changed_from_another_node()
