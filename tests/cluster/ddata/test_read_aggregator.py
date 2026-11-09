"""The ReadAggregator, Pekko's ``ReadAggregator`` in ``Replicator.scala``. No Pekko spec covers it
alone, so these are ohra's own, in the style of WriteAggregatorSpec: the replicas and the
Replicator are probes, and a probe answers through the request's ``reply_to``."""

from typing import Any, Never

import pytest

from ohra import Behavior
from ohra.cluster.ddata import GCounter, GCounterKey, Replicator, SelfUniqueAddress
from ohra.cluster.ddata.aggregators import read_aggregator
from ohra.cluster.ddata.envelope import DataEnvelope, Read, ReadRepair, ReadRepairAck, ReadResult
from ohra.remote import UniqueAddress
from ohra.testkit import ActorTestKit, ManualTime, TestProbe

KeyA = GCounterKey("A")

node_a = UniqueAddress("a", 7355, 17, "Sys")
node_b = UniqueAddress("b", 7355, 17, "Sys")
node_c = UniqueAddress("c", 7355, 17, "Sys")
node_d = UniqueAddress("d", 7355, 17, "Sys")
# 4 replicas + the local => 5
nodes = (node_a, node_b, node_c, node_d)
self_unique_address = UniqueAddress("self", 7355, 17, "Sys")

timeout = 3.0
read_majority = Replicator.ReadMajority(timeout)

local = GCounter().increment(SelfUniqueAddress(self_unique_address), 1)
from_a = GCounter().increment(SelfUniqueAddress(node_a), 2)


class Setup:
    def __init__(self, kit: ActorTestKit) -> None:
        self.replicas: dict[UniqueAddress, TestProbe[Read]] = {
            n: kit.create_test_probe() for n in nodes
        }
        self.replicator: TestProbe[ReadRepair] = kit.create_test_probe()
        self.reply: TestProbe[Replicator.GetResponse[Any]] = kit.create_test_probe()

    def aggregator(
        self,
        consistency: Replicator.ReadConsistency,
        local_value: DataEnvelope | None,
        unreachable: frozenset[UniqueAddress] = frozenset(),
    ) -> Behavior[Never]:
        return read_aggregator(
            KeyA,
            consistency,
            self.reply.ref,
            nodes,
            unreachable,
            local_value,
            lambda n: self.replicas[n].ref,
            self.replicator.ref,
            self_unique_address,
            shuffle=False,
        )


async def test_read_majority_merges_replies():
    async with ActorTestKit(manual_time=True) as kit:
        s = Setup(kit)
        aggr = kit.spawn(s.aggregator(read_majority, DataEnvelope(local)))

        read = await s.replicas[node_a].expect_message_type(Read)
        assert read == Read(KeyA.id, self_unique_address, read.reply_to)
        read.reply_to.tell(ReadResult(DataEnvelope(from_a)))
        (await s.replicas[node_b].expect_message_type(Read)).reply_to.tell(ReadResult(None))

        repair = await s.replicator.expect_message_type(ReadRepair)
        assert repair.envelope == DataEnvelope(local.merge(from_a))
        repair.reply_to.tell(ReadRepairAck())
        await s.reply.expect_message(Replicator.GetSuccess(KeyA, local.merge(from_a)))
        await s.reply.expect_terminated(aggr)


async def test_read_repair_happens_before_get_success():
    async with ActorTestKit(manual_time=True) as kit:
        manual_time = ManualTime(kit.system)
        s = Setup(kit)
        aggr = kit.spawn(s.aggregator(read_majority, None))

        for n in (node_a, node_b):
            (await s.replicas[n].expect_message_type(Read)).reply_to.tell(
                ReadResult(DataEnvelope(from_a))
            )
        repair = await s.replicator.expect_message_type(ReadRepair)
        assert (repair.key_id, repair.envelope) == (KeyA.id, DataEnvelope(from_a))
        # no GetSuccess until the Replicator acks, not even after the timeout
        await manual_time.expect_no_message_for(timeout, s.reply)
        repair.reply_to.tell(ReadRepairAck())
        await s.reply.expect_message(Replicator.GetSuccess(KeyA, from_a))
        await s.reply.expect_terminated(aggr)


async def test_read_all_times_out_with_get_failure():
    async with ActorTestKit(manual_time=True) as kit:
        manual_time = ManualTime(kit.system)
        s = Setup(kit)
        aggr = kit.spawn(s.aggregator(Replicator.ReadAll(timeout), DataEnvelope(local)))

        for n in (node_a, node_b, node_c):
            (await s.replicas[n].expect_message_type(Read)).reply_to.tell(ReadResult(None))
        await s.replicas[node_d].expect_message_type(Read)
        # no reply
        await manual_time.expect_no_message_for(timeout - 0.01, s.reply)
        await manual_time.time_passes(0.01)
        await s.reply.expect_message(Replicator.GetFailure(KeyA))
        await s.reply.expect_terminated(aggr)
        await s.replicator.expect_no_message(0)


async def test_not_found_when_no_replica_has_the_key():
    async with ActorTestKit(manual_time=True) as kit:
        s = Setup(kit)
        aggr = kit.spawn(s.aggregator(read_majority, None))

        for n in (node_a, node_b):
            (await s.replicas[n].expect_message_type(Read)).reply_to.tell(ReadResult(None))
        await s.reply.expect_message(Replicator.NotFound(KeyA))
        await s.reply.expect_terminated(aggr)
        await s.replicator.expect_no_message(0)


async def test_reachable_nodes_are_the_primaries():
    async with ActorTestKit(manual_time=True) as kit:
        manual_time = ManualTime(kit.system)
        s = Setup(kit)
        kit.spawn(s.aggregator(read_majority, None, frozenset({node_a, node_b})))

        await s.replicas[node_c].expect_message_type(Read)
        await s.replicas[node_d].expect_message_type(Read)
        # the unreachable ones are the secondaries
        unreachable = (s.replicas[node_a], s.replicas[node_b])
        await manual_time.expect_no_message_for(timeout / 5 - 0.05, *unreachable)
        await manual_time.time_passes(0.05)
        await s.replicas[node_a].expect_message_type(Read)
        await s.replicas[node_b].expect_message_type(Read)


def test_read_from_zero_is_rejected():
    # Pekko's ReadFrom requires n >= 2; ohra runs n == 1 locally, so n < 1 is the error
    with pytest.raises(ValueError):
        Replicator.ReadFrom(0, timeout)
