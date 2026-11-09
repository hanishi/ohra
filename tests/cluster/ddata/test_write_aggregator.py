"""Ported from Apache Pekko's WriteAggregatorSpec, its first group.

The replicas are probes, through the aggregator's ``replica`` parameter as Pekko's
``TestWriteAggregator`` overrides ``replica``. A probe answers a ``Write`` through its
``reply_to``, which stands for Pekko's sender. The groups "with delta" and "with durable" wait for
sub-projects 6 and 5.
"""

from typing import Any

import pytest

from ohra.cluster.ddata import GSet, GSetKey, Replicator
from ohra.cluster.ddata.aggregators import calculate_majority, write_aggregator
from ohra.cluster.ddata.envelope import DELETED_ENVELOPE, DataEnvelope, Write, WriteAck, WriteNack
from ohra.remote import UniqueAddress
from ohra.testkit import ActorTestKit, ManualTime, TestProbe

KeyA = GSetKey[str]("A")

node_a = UniqueAddress("a", 7355, 17, "Sys")
node_b = UniqueAddress("b", 7355, 17, "Sys")
node_c = UniqueAddress("c", 7355, 17, "Sys")
node_d = UniqueAddress("d", 7355, 17, "Sys")
# 4 replicas + the local => 5
nodes = (node_a, node_b, node_c, node_d)
self_unique_address = UniqueAddress("self", 7355, 17, "Sys")

data = GSet[str]().add("A").add("B")
timeout = 3.0
write_majority = Replicator.WriteMajority(timeout)

type Reply = Replicator.UpdateResponse[Any] | Replicator.DeleteResponse[Any]


async def test_send_to_at_least_n_2_plus_1_replicas_when_write_majority():
    async with ActorTestKit(manual_time=True) as kit:
        probe: TestProbe[Write] = kit.create_test_probe()
        reply: TestProbe[Reply] = kit.create_test_probe()
        aggr = kit.spawn(
            write_aggregator(
                KeyA,
                DataEnvelope(data),
                write_majority,
                reply.ref,
                nodes,
                frozenset(),
                lambda _: probe.ref,
                self_unique_address,
                shuffle=False,
            )
        )

        (await probe.expect_message_type(Write)).reply_to.tell(WriteAck())
        (await probe.expect_message_type(Write)).reply_to.tell(WriteAck())
        await reply.expect_message(Replicator.UpdateSuccess(KeyA))
        await reply.expect_terminated(aggr)


async def test_send_to_more_when_no_immediate_reply():
    async with ActorTestKit(manual_time=True) as kit:
        manual_time = ManualTime(kit.system)
        probes: dict[UniqueAddress, TestProbe[Write]] = {n: kit.create_test_probe() for n in nodes}
        reply: TestProbe[Reply] = kit.create_test_probe()
        aggr = kit.spawn(
            write_aggregator(
                KeyA,
                DataEnvelope(data),
                write_majority,
                reply.ref,
                nodes,
                frozenset({node_c, node_d}),
                lambda n: probes[n].ref,
                self_unique_address,
                shuffle=False,
            )
        )

        await probes[node_a].expect_message_type(Write)
        # no reply
        (await probes[node_b].expect_message_type(Write)).reply_to.tell(WriteAck())
        # unreachable nodes get no message until 1/5 of the time the reachable nodes did not
        # answer
        await manual_time.expect_no_message_for(timeout / 5 - 0.05, probes[node_c], probes[node_d])
        await manual_time.time_passes(0.05)
        (await probes[node_c].expect_message_type(Write)).reply_to.tell(WriteAck())
        (await probes[node_d].expect_message_type(Write)).reply_to.tell(WriteAck())

        await reply.expect_message(Replicator.UpdateSuccess(KeyA))
        await reply.expect_terminated(aggr)


async def test_timeout_when_less_than_required_acks():
    async with ActorTestKit(manual_time=True) as kit:
        manual_time = ManualTime(kit.system)
        probe: TestProbe[Write] = kit.create_test_probe()
        reply: TestProbe[Reply] = kit.create_test_probe()
        aggr = kit.spawn(
            write_aggregator(
                KeyA,
                DataEnvelope(data),
                write_majority,
                reply.ref,
                nodes,
                frozenset(),
                lambda _: probe.ref,
                self_unique_address,
                shuffle=False,
            )
        )

        await probe.expect_message_type(Write)
        # no reply
        (await probe.expect_message_type(Write)).reply_to.tell(WriteAck())
        await manual_time.time_passes(timeout / 5)
        await probe.expect_message_type(Write)
        # no reply
        await probe.expect_message_type(Write)
        # no reply
        await manual_time.expect_no_message_for(timeout - timeout / 5 - 0.01, reply)
        await manual_time.time_passes(0.01)
        await reply.expect_message(Replicator.UpdateTimeout(KeyA))
        await reply.expect_terminated(aggr)


def test_calculate_majority_with_min_cap():
    min_cap = 5

    assert calculate_majority(min_cap, 3, 0) == 3
    assert calculate_majority(min_cap, 4, 0) == 4
    assert calculate_majority(min_cap, 5, 0) == 5
    assert calculate_majority(min_cap, 6, 0) == 5
    assert calculate_majority(min_cap, 7, 0) == 5
    assert calculate_majority(min_cap, 8, 0) == 5
    assert calculate_majority(min_cap, 9, 0) == 5
    assert calculate_majority(min_cap, 10, 0) == 6
    assert calculate_majority(min_cap, 11, 0) == 6
    assert calculate_majority(min_cap, 12, 0) == 7


def test_calculate_majority_with_additional():
    assert calculate_majority(0, 3, 1) == 3
    assert calculate_majority(0, 3, 2) == 3
    assert calculate_majority(0, 4, 1) == 4
    assert calculate_majority(0, 5, 1) == 4
    assert calculate_majority(0, 5, 2) == 5
    assert calculate_majority(0, 6, 1) == 5
    assert calculate_majority(0, 7, 1) == 5
    assert calculate_majority(0, 8, 1) == 6
    assert calculate_majority(0, 8, 2) == 7
    assert calculate_majority(0, 9, 1) == 6
    assert calculate_majority(0, 10, 1) == 7
    assert calculate_majority(0, 11, 1) == 7
    assert calculate_majority(0, 11, 3) == 9


def test_calculate_majority_with_additional_and_min_cap():
    assert calculate_majority(5, 9, 1) == 6
    assert calculate_majority(7, 9, 1) == 7
    assert calculate_majority(10, 9, 1) == 9


# ohra's own


def test_write_to_zero_is_rejected():
    # Pekko's WriteTo requires n >= 2; ohra runs n == 1 locally, so n < 1 is the error
    with pytest.raises(ValueError):
        Replicator.WriteTo(0, timeout)


async def test_write_to_more_nodes_than_there_are_times_out_at_once():
    async with ActorTestKit(manual_time=True) as kit:
        probe: TestProbe[Write] = kit.create_test_probe()
        reply: TestProbe[Reply] = kit.create_test_probe()
        aggr = kit.spawn(
            write_aggregator(
                KeyA,
                DataEnvelope(data),
                Replicator.WriteTo(6, timeout),  # 4 replicas + the local => 5
                reply.ref,
                nodes,
                frozenset(),
                lambda _: probe.ref,
                self_unique_address,
                shuffle=False,
            )
        )

        await reply.expect_message(Replicator.UpdateTimeout(KeyA))
        await reply.expect_terminated(aggr)


async def test_a_delete_that_gets_nacks_replies_delete_failure():
    async with ActorTestKit(manual_time=True) as kit:
        probe: TestProbe[Write] = kit.create_test_probe()
        reply: TestProbe[Reply] = kit.create_test_probe()
        aggr = kit.spawn(
            write_aggregator(
                KeyA,
                DELETED_ENVELOPE,
                Replicator.WriteAll(timeout),
                reply.ref,
                nodes[:2],
                frozenset(),
                lambda _: probe.ref,
                self_unique_address,
                shuffle=False,
            )
        )

        (await probe.expect_message_type(Write)).reply_to.tell(WriteAck())
        (await probe.expect_message_type(Write)).reply_to.tell(WriteNack())
        await reply.expect_message(Replicator.DeleteFailure(KeyA))
        await reply.expect_terminated(aggr)
