"""DataEnvelope and DeletedData, from Pekko's Replicator.Internal, and the typed protocol."""

import json
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
from ohra.cluster.ddata.replicator import Replicator
from ohra.ref import ActorRef, FunctionRef
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

node1 = SelfUniqueAddress(UniqueAddress("localhost", 7354, 1, "Sys"))
node2 = SelfUniqueAddress(UniqueAddress("localhost", 7355, 2, "Sys"))
ref: ActorRef[Any] = FunctionRef("ohra://Sys@localhost:7354/temp/$1", lambda _m: None)


def test_merge_merges_the_data():
    a = GCounter().increment(node1, 2)
    b = GCounter().increment(node2, 3)
    assert DataEnvelope(a).merge(DataEnvelope(b)) == DataEnvelope(a.merge(b))
    merged = DataEnvelope(a).merge(DataEnvelope(b)).data
    assert isinstance(merged, GCounter) and merged.value == 5


def test_deleted_wins_on_either_side():
    e = DataEnvelope(GCounter().increment(node1))
    assert e.merge(DELETED_ENVELOPE) is DELETED_ENVELOPE
    assert DELETED_ENVELOPE.merge(e) is DELETED_ENVELOPE
    assert DELETED_ENVELOPE.is_deleted
    assert not e.is_deleted
    assert DELETED_DATA.merge(DELETED_DATA) is DELETED_DATA


def test_merging_different_types_raises_type_error():
    with pytest.raises(TypeError):
        DataEnvelope(GCounter()).merge(DataEnvelope(Flag()))


def test_an_envelope_round_trips_over_serialization():
    for env in (DataEnvelope(GCounter().increment(node1, 4)), DELETED_ENVELOPE):
        wire = json.dumps(Serialization.encode(env, lambda r: r.path))
        back = Serialization.decode(json.loads(wire), lambda p: pytest.fail(p))
        assert back == env
        assert isinstance(back, DataEnvelope)
    assert Serialization.decode(
        Serialization.encode(DELETED_ENVELOPE, lambda r: r.path), lambda p: pytest.fail(p)
    ) == DataEnvelope(DELETED_DATA)


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
