"""Replicator subscriptions, wildcards and key expiry on one node, ported from Pekko's
ReplicatorWildcardSubscriptionSpec, plus the subscriber and expiry behaviors of Replicator.scala.

The node runs on ManualTime, so the notify, gossip and clock ticks fire only when time passes.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from ohra.behavior import Behaviors
from ohra.cluster import Cluster, ClusterSettings
from ohra.cluster.ddata import GCounter, GCounterKey, SelfUniqueAddress
from ohra.cluster.ddata.replicator import Replicator
from ohra.cluster.ddata.settings import ReplicatorSettings
from ohra.eventstream import DeadLetter, EventStream
from ohra.management import ManagementSettings
from ohra.remote import Address
from ohra.system import ActorSystem
from ohra.testkit import ManualTime, TestProbe
from ohra.testkit.manual_time import ExplicitlyTriggeredScheduler

from ...util import free_port
from ..test_cluster import FAST, FAST_SHUTDOWN, all_up
from .test_replicator_local import Node, update_counter

# the notify interval of Pekko's ReplicatorWildcardSubscriptionSpec
NOTIFY = 0.1

type Sub = TestProbe[Replicator.SubscribeResponse[GCounter]]


@asynccontextmanager
async def manual_node(
    settings: ReplicatorSettings = ReplicatorSettings(notify_subscribers_interval=NOTIFY),
) -> AsyncGenerator[tuple[Node, ManualTime]]:
    port = free_port()
    system = ActorSystem(
        Behaviors.ignore,
        "ddata",
        address=("127.0.0.1", port),
        cluster=ClusterSettings(seed_nodes=(Address("ddata", "127.0.0.1", port),), **FAST),
        shutdown=FAST_SHUTDOWN,
        management=ManagementSettings(hostname="127.0.0.1", port=0),
        scheduler=ExplicitlyTriggeredScheduler(),
    )
    async with system:
        manual = ManualTime(system)
        while not all_up(system, 1):
            await manual.time_passes(0.02)
        cell = system._system_root.spawn_child(  # pyright: ignore[reportPrivateUsage]
            Replicator.behavior(settings), "ddataReplicator", None
        )
        yield (
            Node(system, cell.ref, SelfUniqueAddress(Cluster(system).self_unique_address)),
            manual,
        )


def counter_of(msg: Replicator.SubscribeResponse[GCounter]) -> tuple[str, int]:
    assert isinstance(msg, Replicator.Changed), msg
    return msg.key.id, msg.get(msg.key).value


async def get(node: Node, key: GCounterKey) -> Replicator.GetResponse[GCounter]:
    probe: TestProbe[Replicator.GetResponse[GCounter]] = node.probe()
    node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), probe.ref))
    return await probe.receive_message()


# ReplicatorWildcardSubscriptionSpec


async def test_notify_subscriber_for_keys_matching_the_wildcard_prefix():
    async with manual_node() as (node, manual):
        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(GCounterKey("notif-counter-*"), probe.ref))

        await update_counter(node, GCounterKey("notif-counter-a1"), 1)
        await manual.time_passes(NOTIFY)
        assert counter_of(await probe.receive_message()) == ("notif-counter-a1", 1)

        await update_counter(node, GCounterKey("notif-counter-a2"), 2)
        await manual.time_passes(NOTIFY)
        assert counter_of(await probe.receive_message()) == ("notif-counter-a2", 2)

        await update_counter(node, GCounterKey("notif-other-counter"), 1)
        await manual.expect_no_message_for(0.5, probe)


async def test_send_current_value_to_new_wildcard_subscriber_for_existing_matching_keys():
    async with manual_node() as (node, manual):
        await update_counter(node, GCounterKey("current-counter-a"), 10)

        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(GCounterKey("current-counter-*"), probe.ref))
        await manual.time_passes(NOTIFY)
        assert counter_of(await probe.receive_message()) == ("current-counter-a", 10)


async def test_unsubscribe_wildcard_subscriber():
    async with manual_node() as (node, manual):
        key_a, wildcard = GCounterKey("unsub-counter-a"), GCounterKey("unsub-counter-*")
        await update_counter(node, key_a, 1)

        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(wildcard, probe.ref))
        await manual.time_passes(NOTIFY)
        assert counter_of(await probe.receive_message()) == ("unsub-counter-a", 1)

        node.replicator.tell(Replicator.Unsubscribe(wildcard, probe.ref))
        await update_counter(node, key_a, 100)
        await manual.expect_no_message_for(0.5, probe)


# Replicator.scala's subscribers


async def test_a_new_subscriber_gets_the_current_value():
    async with manual_node() as (node, manual):
        key = GCounterKey("A")
        await update_counter(node, key, 3)

        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(key, probe.ref))
        await manual.time_passes(NOTIFY)
        assert counter_of(await probe.receive_message()) == ("A", 3)


async def test_changes_are_batched_per_notify_interval():
    async with manual_node() as (node, manual):
        key = GCounterKey("A")
        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(key, probe.ref))
        await manual.expect_no_message_for(NOTIFY, probe)  # nothing for a missing key

        for _ in range(3):
            await update_counter(node, key, 1)
        await manual.time_passes(NOTIFY)
        assert counter_of(await probe.receive_message()) == ("A", 3)
        await manual.expect_no_message_for(NOTIFY, probe)


async def test_flush_changes_notifies_at_once():
    async with manual_node(ReplicatorSettings(notify_subscribers_interval=60.0)) as (node, _):
        key = GCounterKey("A")
        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(key, probe.ref))
        node.replicator.tell(Replicator.FlushChanges())

        await update_counter(node, key, 5)
        node.replicator.tell(Replicator.FlushChanges())
        assert counter_of(await probe.receive_message()) == ("A", 5)


async def test_subscribing_twice_notifies_once_and_a_stray_unsubscribe_is_ignored():
    async with manual_node() as (node, manual):
        key = GCounterKey("A")
        await update_counter(node, key, 1)
        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(key, probe.ref))
        node.replicator.tell(Replicator.Subscribe(key, probe.ref))
        await manual.time_passes(NOTIFY)
        assert counter_of(await probe.receive_message()) == ("A", 1)
        await probe.expect_no_message(0)

        stranger: Sub = node.probe()
        node.replicator.tell(Replicator.Unsubscribe(key, stranger.ref))
        node.replicator.tell(Replicator.Unsubscribe(GCounterKey("B"), probe.ref))
        node.replicator.tell(Replicator.Unsubscribe(GCounterKey("B*"), probe.ref))

        await update_counter(node, key, 1)
        await manual.time_passes(NOTIFY)
        assert counter_of(await probe.receive_message()) == ("A", 2)
        await manual.expect_no_message_for(NOTIFY, probe, stranger)


async def test_a_terminated_subscriber_is_removed():
    async with manual_node() as (node, manual):
        key = GCounterKey("A")
        await update_counter(node, key, 1)
        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(key, probe.ref))
        await manual.time_passes(NOTIFY)
        counter_of(await probe.receive_message())

        dead: TestProbe[DeadLetter] = node.probe()
        node.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        watcher: TestProbe[Any] = node.probe()
        probe.stop()
        # the Replicator is notified of the stop with the watcher, before the Update below
        await watcher.expect_terminated(probe.ref)

        await update_counter(node, key, 1)
        node.replicator.tell(Replicator.FlushChanges())
        await manual.expect_no_message_for(NOTIFY, dead)


async def test_deleted_notifies_subscribers():
    async with manual_node() as (node, manual):
        key = GCounterKey("A")
        await update_counter(node, key, 1)
        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(key, probe.ref))
        await manual.time_passes(NOTIFY)
        counter_of(await probe.receive_message())

        deleted: TestProbe[Replicator.DeleteResponse[GCounter]] = node.probe()
        node.replicator.tell(Replicator.Delete(key, Replicator.WriteLocal(), deleted.ref))
        await deleted.expect_message(Replicator.DeleteSuccess(key))
        await manual.time_passes(NOTIFY)
        await probe.expect_message(Replicator.Deleted(key))


# Replicator.scala's expiry

EXPIRY = ReplicatorSettings(
    notify_subscribers_interval=NOTIFY,
    expire_keys_after_inactivity={"cache-*": 0.2},
    gossip_interval=0.05,
)


async def test_an_inactive_key_expires_and_notifies_expired():
    async with manual_node(EXPIRY) as (node, manual):
        key = GCounterKey("cache-a")
        await update_counter(node, key, 1)
        probe: Sub = node.probe()
        node.replicator.tell(Replicator.Subscribe(key, probe.ref))
        await manual.time_passes(NOTIFY)
        counter_of(await probe.receive_message())

        await manual.time_passes(0.3)
        await probe.expect_message(Replicator.Expired(key))
        assert await get(node, key) == Replicator.NotFound(key)


async def test_a_touched_key_does_not_expire():
    async with manual_node(EXPIRY) as (node, manual):
        key, untouched = GCounterKey("cache-b"), GCounterKey("cache-c")
        await update_counter(node, key, 1)
        await update_counter(node, untouched, 1)
        for _ in range(10):  # 1 s, five times the expiry, with a Get every 0.1 s
            await manual.time_passes(0.1)
            assert isinstance(await get(node, key), Replicator.GetSuccess)
        assert await get(node, untouched) == Replicator.NotFound(untouched)
