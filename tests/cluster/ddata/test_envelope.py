"""DataEnvelope and DeletedData, from Pekko's Replicator.Internal, and the typed protocol.
The pruning tests are ported from Pekko's ``DataEnvelopeSpec``, with an explicit ``now``."""

import dataclasses
import json
import time
from typing import Any

import pytest

from ohra.cluster.ddata import GCounterKey, SelfUniqueAddress
from ohra.cluster.ddata.envelope import (
    DELETED_DATA,
    DELETED_ENVELOPE,
    DataEnvelope,
    Read,
    ReadRepair,
    ReadRepairAck,
    ReadResult,
    Write,
    WriteAck,
    WriteNack,
)
from ohra.cluster.ddata.flag import Flag
from ohra.cluster.ddata.gcounter import GCounter
from ohra.cluster.ddata.pruning_state import PruningInitialized, PruningPerformed
from ohra.cluster.ddata.replicator import Replicator
from ohra.ref import ActorRef, FunctionRef
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

node1 = SelfUniqueAddress(UniqueAddress("localhost", 7354, 1, "Sys"))
node2 = SelfUniqueAddress(UniqueAddress("localhost", 7355, 2, "Sys"))
node3 = SelfUniqueAddress(UniqueAddress("localhost", 7356, 3, "Sys"))
u1, u2, u3 = node1.unique_address, node2.unique_address, node3.unique_address
NOW = time.time()
IN_FUTURE = NOW + 3600
OLD = NOW - 3600
ref: ActorRef[Any] = FunctionRef("ohra://Sys@localhost:7354/temp/$1", lambda _m: None)


def test_merge_merges_the_data():
    a = GCounter().increment(node1, 2)
    b = GCounter().increment(node2, 3)
    assert DataEnvelope(a).merge(DataEnvelope(b), NOW) == DataEnvelope(a.merge(b))
    merged = DataEnvelope(a).merge(DataEnvelope(b), NOW).data
    assert isinstance(merged, GCounter) and merged.value == 5


def test_deleted_wins_on_either_side():
    e = DataEnvelope(GCounter().increment(node1))
    assert e.merge(DELETED_ENVELOPE, NOW) is DELETED_ENVELOPE
    assert DELETED_ENVELOPE.merge(e, NOW) is DELETED_ENVELOPE
    assert DELETED_ENVELOPE.is_deleted
    assert not e.is_deleted
    assert DELETED_DATA.merge(DELETED_DATA) is DELETED_DATA


def test_merging_different_types_raises_type_error():
    with pytest.raises(TypeError):
        DataEnvelope(GCounter()).merge(DataEnvelope(Flag()), NOW)


def test_handle_pruning_transitions():
    g1 = GCounter().increment(node1, 1)
    d1 = DataEnvelope(g1)

    d2 = d1.init_removed_node_pruning(u1, u2)
    marker = d2.pruning[u1]
    assert isinstance(marker, PruningInitialized)
    assert marker.owner == u2

    d3 = d2.add_seen(u3.address)
    marker = d3.pruning[u1]
    assert isinstance(marker, PruningInitialized)
    assert marker.seen == frozenset({u3.address})

    d4 = d3.prune(u1, PruningPerformed(IN_FUTURE))
    assert isinstance(d4.data, GCounter)
    assert d4.data.modified_by_nodes == {u2}


def test_merge_correctly():
    g1 = GCounter().increment(node1, 1)
    d1 = DataEnvelope(g1)
    g2 = GCounter().increment(node2, 2)
    d2 = DataEnvelope(g2)

    d3 = d1.merge(d2, NOW)
    assert isinstance(d3.data, GCounter)
    assert d3.data.value == 3
    assert d3.data.modified_by_nodes == {u1, u2}
    d4 = d3.init_removed_node_pruning(u1, u2)
    d5 = d4.prune(u1, PruningPerformed(IN_FUTURE))
    assert isinstance(d5.data, GCounter)
    assert d5.data.modified_by_nodes == {u2}

    # late update from node1
    g11 = g1.increment(node1, 10)
    d6 = d5.merge(DataEnvelope(g11), NOW)
    assert isinstance(d6.data, GCounter)
    assert d6.data.value == 3
    assert d6.data.modified_by_nodes == {u2}
    d6r = DataEnvelope(g11).merge(d5, NOW)
    assert isinstance(d6r.data, GCounter)
    assert d6r.data.value == 3
    assert d6r.data.modified_by_nodes == {u2}

    # remove obsolete
    d7 = dataclasses.replace(d5, pruning={**d5.pruning, u1: PruningPerformed(OLD)})
    d8 = dataclasses.replace(d5, pruning={})
    assert d8.merge(d7, NOW).pruning == {}
    assert d7.merge(d8, NOW).pruning == {}

    assert d5.merge(d7, NOW).pruning[u1] == PruningPerformed(IN_FUTURE)
    assert d7.merge(d5, NOW).pruning[u1] == PruningPerformed(IN_FUTURE)


def test_prune_without_a_marker_raises_value_error():
    with pytest.raises(ValueError):
        DataEnvelope(GCounter().increment(node1)).prune(u1, PruningPerformed(IN_FUTURE))


def test_merge_drops_a_performed_marker_once_now_passes_it():
    d = DataEnvelope(GCounter().increment(node2, 2), {u1: PruningPerformed(IN_FUTURE)})
    other = DataEnvelope(GCounter().increment(node2, 2))
    assert d.merge(other, OLD - 1).pruning == {u1: PruningPerformed(IN_FUTURE)}
    assert d.merge(other, IN_FUTURE + 1).pruning == {}


def test_an_envelope_round_trips_over_serialization():
    for env in (DataEnvelope(GCounter().increment(node1, 4)), DELETED_ENVELOPE):
        wire = json.dumps(Serialization.encode(env, lambda r: r.path))
        back = Serialization.decode(json.loads(wire), lambda p: pytest.fail(p))
        assert back == env
        assert isinstance(back, DataEnvelope)
    assert Serialization.decode(
        Serialization.encode(DELETED_ENVELOPE, lambda r: r.path), lambda p: pytest.fail(p)
    ) == DataEnvelope(DELETED_DATA)


def test_an_envelope_with_pruning_round_trips_over_serialization():
    env = DataEnvelope(
        GCounter().increment(node2, 4),
        {
            u1: PruningInitialized(u2, frozenset({u2.address, u3.address})),
            u3: PruningPerformed(IN_FUTURE),
        },
    )
    wire = json.dumps(Serialization.encode(env, lambda r: r.path))
    back = Serialization.decode(json.loads(wire), lambda p: pytest.fail(p))
    assert back == env


def test_success_get_returns_the_data():
    k = GCounterKey("a")
    c = GCounter()
    assert Replicator.GetSuccess(k, c).get(k) is c
    assert Replicator.Changed(k, c).get(k) is c


envelope = DataEnvelope(GCounter().increment(node1, 4))


@pytest.mark.parametrize(
    "msg",
    [
        Read("a", node1.unique_address, ref),
        Read("a", None, ref),
        ReadResult(envelope),
        ReadResult(None),
        Write("a", envelope, node1.unique_address, ref),
        Write("a", DELETED_ENVELOPE, None, ref),
        WriteAck(),
        WriteNack(),
        ReadRepair("a", envelope, ref),
        ReadRepairAck(),
    ],
)
def test_a_wire_message_round_trips_over_serialization(msg: object):
    wire = json.dumps(Serialization.encode(msg, lambda r: r.path))
    back = Serialization.decode(
        json.loads(wire), lambda p: ref if p == ref.path else pytest.fail(p)
    )
    assert back == msg
    assert type(back) is type(msg)
