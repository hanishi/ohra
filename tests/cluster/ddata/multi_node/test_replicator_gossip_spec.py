"""Pekko's multi-node ``ReplicatorGossipSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/ReplicatorGossipSpec.scala``),
as one ``@multi_node_spec`` test with the spec's own barrier names.

Its one case is a section, at its line in the Scala file:

- ``catch_up_when_adding_node``: "catch up when adding node", :84

Mapping:

- Pekko's ``distributed-data`` config is kept, but for the gossip interval: ``max-delta-elements =
  400`` and ``log-data-size-exceeding = 2000``. Its gossip interval of one second is 0.1 s here,
  with a notify interval of 0.05 s. ``delta-crdt.enabled = off`` is ``delta_crdt_enabled=False``:
  this spec's "only gossip" (a DeltaPropagation of 515 entries would be over the frame limit).
- Pekko lowers Artery's ``maximum-frame-size`` to 50000 bytes, so that the 515 entries (the 15
  large ones alone are over 60000 bytes) have to be split over many Gossip messages. ohra's frame
  limit is the module constant ``MAX_FRAME``, which no setting lowers, so the test patches it to
  50000 in both modules that read it: in ``ohra.remote.remoting``, whose sender refuses a larger
  frame, and in ``ohra.cluster.ddata.replicator``, which hands it to the Replicator's
  ``_PayloadSizeAggregator`` when the Replicator sets up. Every node runs in this process, so the
  patch wraps the whole run, from before the first system starts until the last one stops.
- ``rnd.nextString(n)`` is ``n`` random characters below the surrogates, so that each one is
  valid UTF-8 and most take more than one byte, as Pekko's note says.
- Pekko's Artery publishes ``Dropped`` for a message over its frame size, and the spec expects
  none. ohra publishes ``Dropped`` only for a full stash; a replica message that is too large to
  send is logged as an error by ``send_to_replica``. So ``droppedProbe.expectNoMessage()`` is
  ``errors_logged()`` with no ``Dropped ...`` record. That check is also how the spec sees the
  splitting from outside: an unsplit Gossip would be too large to send, and logged, and the
  second node would not catch up.
"""

from __future__ import annotations

import random
from unittest.mock import patch

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    ORSet,
    ORSetKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.cluster.ddata import replicator as replicator_module
from ohra.remote import Address
from ohra.remote import remoting as remoting_module
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, RoleName, TestProbe, multi_node_spec

from ....util import errors_logged
from ...test_cluster import FAST, FAST_SHUTDOWN

config = MultiNodeConfig()
first, second = config.role("first"), config.role("second")
config.common_config(
    cluster=ClusterSettings(
        **FAST,
        distributed_data=ReplicatorSettings(
            gossip_interval=0.1,
            notify_subscribers_interval=0.05,
            max_delta_elements=400,
            log_data_size_exceeding=2000,
            delta_crdt_enabled=False,
        ),
    ),
    shutdown=FAST_SHUTDOWN,
)

MAXIMUM_FRAME_SIZE = 50000  # Pekko's pekko.remote.artery.advanced.maximum-frame-size

READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()


def three_digits(n: int) -> str:
    return f"00{n}"[-3:]


small_or_set_keys: list[ORSetKey[str]] = [
    ORSetKey(f"small-{three_digits(n)}") for n in range(1, 1000)
]
large_or_set_keys: list[ORSetKey[str]] = [
    ORSetKey(f"large-{three_digits(n)}") for n in range(1, 100)
]

SMALL_PAYLOAD_SIZE = 100
LARGE_PAYLOAD_SIZE = 4000


def random_string(n: int) -> str:
    """Scala's ``Random.nextString``, without surrogates."""
    return "".join(chr(random.randrange(0x20, 0xD800)) for _ in range(n))


def small_payload() -> str:
    return random_string(SMALL_PAYLOAD_SIZE)


def large_payload() -> str:
    return random_string(LARGE_PAYLOAD_SIZE)


@multi_node_spec(config)
async def replicator_gossip_spec(spec: MultiNodeSpec):
    cluster = Cluster(spec.system)
    replicator = DistributedData(spec.system).replicator
    self_address = DistributedData(spec.system).self_unique_address

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    async def catch_up_when_adding_node() -> None:
        await join(first, first)

        number_of_small = 500
        number_of_large = 15

        if spec.is_node(first):
            probe: TestProbe[Replicator.UpdateResponse[ORSet[str]]] = spec.create_test_probe()
            empty: ORSet[str] = ORSet()
            for i in range(number_of_small):
                payload = small_payload()
                replicator.tell(
                    Replicator.Update(
                        small_or_set_keys[i],
                        empty,
                        WRITE_LOCAL,
                        probe.ref,
                        lambda s, p=payload: s.add(self_address, p),
                    )
                )
                await probe.expect_message_type(Replicator.UpdateSuccess)
            for i in range(number_of_large):
                payload = large_payload()
                replicator.tell(
                    Replicator.Update(
                        large_or_set_keys[i],
                        empty,
                        WRITE_LOCAL,
                        probe.ref,
                        lambda s, p=payload: s.add(self_address, p),
                    )
                )
                await probe.expect_message_type(Replicator.UpdateSuccess)
        await spec.enter_barrier("updated-first")

        await join(second, first)

        async def replica_count() -> None:
            probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()
            replicator.tell(Replicator.GetReplicaCount(probe.ref))
            await probe.expect_message(Replicator.ReplicaCount(2))

        async with spec.within(10.0):
            await spec.await_assert(replica_count, interval=0.05)
        await spec.enter_barrier("second-added")

        if spec.is_node(second):

            async def caught_up() -> None:
                probe: TestProbe[Replicator.GetResponse[ORSet[str]]] = spec.create_test_probe()
                for keys, n, size in (
                    (small_or_set_keys, number_of_small, SMALL_PAYLOAD_SIZE),
                    (large_or_set_keys, number_of_large, LARGE_PAYLOAD_SIZE),
                ):
                    for i in range(n):
                        replicator.tell(Replicator.Get(keys[i], READ_LOCAL, probe.ref))
                        g = await probe.expect_message_type(Replicator.GetSuccess)
                        assert len(next(iter(g.data.elements))) == size

            async with spec.within(10.0):
                await spec.await_assert(caught_up, interval=0.05)
        await spec.enter_barrier("second-caught-up")

        # Pekko's droppedProbe.expectNoMessage(): no replica message was too large to send
        assert not [r for r in errors if r.getMessage().startswith("Dropped ")], errors

        await spec.enter_barrier("done-1")

    with errors_logged() as errors:
        await catch_up_when_adding_node()


async def test_replicator_gossip_spec():
    with (
        patch.object(remoting_module, "MAX_FRAME", MAXIMUM_FRAME_SIZE),
        patch.object(replicator_module, "MAX_FRAME", MAXIMUM_FRAME_SIZE),
    ):
        await replicator_gossip_spec()
