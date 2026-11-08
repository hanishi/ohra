"""Ported from Apache Pekko's protobuf/ReplicatedDataSerializerSpec, for the types of this release.

The wire format is the JSON of ``@remotable``, not protobuf. The size bounds of "serialize large
GSet" and "serialize large ORSet" are dropped: Pekko measures gzip, which comes with the
Replicator.
"""

import json
from dataclasses import dataclass
from typing import Any

from ohra import ActorSystem, Behavior, Behaviors, ask
from ohra.cluster.ddata import (
    FlagKey,
    GCounterKey,
    GSetKey,
    LWWRegisterKey,
    ORSetKey,
    PNCounterKey,
    SelfUniqueAddress,
    UnspecificKey,
)
from ohra.cluster.ddata.flag import Flag
from ohra.cluster.ddata.gcounter import GCounter
from ohra.cluster.ddata.gset import GSet
from ohra.cluster.ddata.lww_register import LWWRegister, default_clock
from ohra.cluster.ddata.orset import ORSet
from ohra.cluster.ddata.pncounter import PNCounter
from ohra.cluster.ddata.version_vector import VersionVector
from ohra.ref import ActorRef, FunctionRef
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization, remotable

from ...test_serialization import A
from ...util import eventually, free_port

address1 = UniqueAddress("localhost", 7354, 1, "Sys")
address2 = UniqueAddress("localhost", 7355, 2, "Sys")
address3 = UniqueAddress("localhost", 7356, 3, "Sys")

ref1: ActorRef[Any] = FunctionRef("ohra://Sys/user/a", lambda _m: None)
ref2: ActorRef[Any] = FunctionRef("ohra://Sys/user/b", lambda _m: None)
ref3: ActorRef[Any] = FunctionRef("ohra://Sys/user/c", lambda _m: None)
refs = {r.path: r for r in (ref1, ref2, ref3)}


@remotable
@dataclass(frozen=True)
class Item:
    name: str


def wire(v: object) -> str:
    return json.dumps(Serialization.encode(v, lambda r: r.path))


def check_serialization[T](v: T) -> T:
    back = Serialization.decode(json.loads(wire(v)), lambda p: refs[p])
    assert back == v
    assert type(back) is type(v)  # a Key's equality is by id alone
    return back  # type: ignore[return-value]


def check_same_content(a: object, b: object) -> None:
    # Pekko compares the bytes. ohra's encoding keeps insertion order, so this compares the
    # decoded values: both must decode to one value.
    assert a == b
    assert check_serialization(a) == check_serialization(b)


def test_serialize_gset():
    check_serialization(GSet())
    check_serialization(GSet().add("a"))
    check_serialization(GSet().add("a").add("b"))
    check_serialization(GSet().add(1).add(2).add(3))
    check_serialization(GSet().add(ref1).add(ref2))
    check_serialization(GSet().add(1).add("2").add(3).add(ref1))
    check_serialization(GSet().add((1, "x")).add(Item("n")).add(ref1).add("s").add(7))
    back = check_serialization(GSet().add((1, "x")).add(Item("n")))
    assert (1, "x") in back.elements and Item("n") in back.elements
    check_same_content(GSet().add("a").add("b"), GSet().add("a").add("b"))
    check_same_content(GSet().add("a").add("b"), GSet().add("b").add("a"))
    check_same_content(GSet().add(ref1).add(ref2).add(ref3), GSet().add(ref2).add(ref1).add(ref3))
    check_same_content(GSet().add(ref1).add(ref2).add(ref3), GSet().add(ref3).add(ref2).add(ref1))
    check_serialization(GSetKey("id"))


def test_serialize_gset_of_frozensets():
    back = check_serialization(GSet().add(frozenset({1, 2})).add(frozenset({"a", (3, "b")})))
    assert all(isinstance(e, frozenset) for e in back.elements)


def test_serialize_large_gset():
    large = GSet[str]()
    for n in range(10000, 20000):
        large = large.reset_delta().add(str(n))
    check_serialization(large)


def test_serialize_orset():
    check_serialization(ORSet())
    check_serialization(ORSet()._add(address1, "a"))
    check_serialization(ORSet()._add(address1, "a")._add(address2, "a"))
    check_serialization(ORSet()._add(address1, "a")._remove(address2, "a"))
    check_serialization(ORSet()._add(address1, "a")._add(address2, "b")._remove(address1, "a"))
    check_serialization(ORSet()._add(address1, 1)._add(address2, 2))
    check_serialization(
        ORSet()._add(address1, "a")._add(address2, 2)._add(address3, 3)._add(address3, ref3)
    )

    s1 = ORSet()._add(address1, "a")._add(address2, "b")
    s2 = ORSet()._add(address2, "b")._add(address1, "a")
    check_same_content(s1.merge(s2), s2.merge(s1))

    s3 = ORSet()._add(address1, "a")._add(address2, 17)._remove(address3, 17)
    s4 = ORSet()._add(address2, 17)._remove(address3, 17)._add(address1, "a")
    check_same_content(s3.merge(s4), s4.merge(s3))

    # ORSet with ActorRef
    check_serialization(ORSet()._add(address1, ref1))
    check_serialization(ORSet()._add(address1, ref1)._add(address1, ref2))
    check_serialization(
        ORSet()._add(address1, ref1)._add(address1, "a")._add(address2, ref2)._add(address2, "b")
    )

    s5 = ORSet()._add(address1, "a")._add(address2, ref1)
    s6 = ORSet()._add(address2, ref1)._add(address1, "a")
    check_same_content(s5.merge(s6), s6.merge(s5))
    check_serialization(ORSetKey("id"))


@remotable
@dataclass(frozen=True)
class EchoRequest:
    reply_to: ActorRef[str]


@Behaviors.receive_message
def echo(msg: EchoRequest) -> Behavior[EchoRequest]:
    msg.reply_to.tell("pong")
    return Behaviors.same


async def test_serialize_orset_with_actorref_message_sent_between_two_systems():
    received: list[ORSet[ActorRef[Any]]] = []

    @Behaviors.receive_message
    def collect(msg: ORSet[ActorRef[Any]]) -> Behavior[ORSet[ActorRef[Any]]]:
        received.append(msg)
        return Behaviors.same

    async with (
        ActorSystem(echo, "a", address=("127.0.0.1", free_port())) as a,
        ActorSystem(collect, "b", address=("127.0.0.1", free_port())) as b,
    ):
        assert a.address is not None
        node = SelfUniqueAddress(a.address)
        to_b = a.resolve(b.path)
        to_b.tell(ORSet[ActorRef[Any]]().add(node, a).add(node, to_b))
        await eventually(lambda: bool(received))
        assert received[0].elements == {b.resolve(a.path), b.resolve(b.path)}
        (echo1,) = (r for r in received[0].elements if r.path == a.path)
        assert await ask(echo1, EchoRequest, timeout=1) == "pong"


def test_serialize_orset_delta():
    check_serialization(ORSet()._add(address1, "a").delta)
    check_serialization(ORSet()._add(address1, "a").reset_delta()._remove(address2, "a").delta)
    check_serialization(ORSet()._add(address1, "a")._remove(address2, "a").delta)
    check_serialization(ORSet()._add(address1, "a").reset_delta()._clear().delta)
    check_serialization(ORSet()._add(address1, "a")._clear().delta)


def test_serialize_large_orset():
    large = ORSet[str]()
    for n in range(10000, 20000):
        large = large.reset_delta()._add((address1, address2, address3)[n % 3], str(n))
    check_serialization(large)


def test_orset_with_tuple_and_dataclass_elements_merges_after_a_round_trip():
    s = ORSet()._add(address1, ("cart", 42))._add(address2, A(3))
    merged = check_serialization(s).merge(s)
    assert merged.elements == {("cart", 42), A(3)}
    assert merged.size == 2


def test_orset_with_a_reserved_string_element_round_trips():
    s = ORSet()._add(address1, "$type")._add(address1, "$ref")._add(address2, "tag")
    assert check_serialization(s).elements == {"$type", "$ref", "tag"}


def test_serialize_flag():
    check_serialization(Flag())
    check_serialization(Flag().switch_on())
    check_serialization(FlagKey("id"))


def test_serialize_lww_register():
    check_serialization(LWWRegister(address1, "value1", default_clock(0, "value1")))
    check_serialization(
        LWWRegister._create(address1, "value2", default_clock)._with_value(
            address2, "value3", default_clock
        )
    )
    check_serialization(LWWRegisterKey("id"))


def test_serialize_lww_register_with_a_tuple_or_frozenset_value():
    for value in ((1, "x", (2, 3)), frozenset({1, (2, "y"), frozenset({"z"})})):
        r = LWWRegister(address1, value, 1)
        back = check_serialization(r)
        assert type(back.value) is type(value) and hash(back) == hash(r)


def test_serialize_gcounter():
    check_serialization(GCounter())
    check_serialization(GCounter()._increment(address1, 3))
    check_serialization(GCounter()._increment(address1, 2)._increment(address2, 5))
    check_same_content(
        GCounter()._increment(address1, 2)._increment(address2, 5),
        GCounter()._increment(address2, 5)._increment(address1, 1)._increment(address1, 1),
    )
    check_same_content(
        GCounter()._increment(address1, 2)._increment(address3, 5),
        GCounter()._increment(address3, 5)._increment(address1, 2),
    )
    check_serialization(GCounterKey("id"))


def test_serialize_pncounter():
    check_serialization(PNCounter())
    check_serialization(PNCounter()._increment(address1, 3))
    check_serialization(PNCounter()._increment(address1, 3)._decrement(address1, 1))
    check_serialization(PNCounter()._increment(address1, 2)._increment(address2, 5))
    check_serialization(
        PNCounter()._increment(address1, 2)._increment(address2, 5)._decrement(address1, 1)
    )
    check_same_content(
        PNCounter()._increment(address1, 2)._increment(address2, 5),
        PNCounter()._increment(address2, 5)._increment(address1, 1)._increment(address1, 1),
    )
    check_same_content(
        PNCounter()._increment(address1, 2)._increment(address3, 5),
        PNCounter()._increment(address3, 5)._increment(address1, 2),
    )
    check_same_content(
        PNCounter()._increment(address1, 2)._decrement(address1, 1)._increment(address3, 5),
        PNCounter()._increment(address3, 5)._increment(address1, 2)._decrement(address1, 1),
    )
    check_serialization(PNCounterKey("id"))


def test_serialize_version_vector():
    check_serialization(VersionVector.empty)
    check_serialization(VersionVector.empty._increment(address1))
    check_serialization(VersionVector.empty._increment(address1)._increment(address2))
    v1 = VersionVector.empty._increment(address1)._increment(address1)
    v2 = VersionVector.empty._increment(address2)
    check_same_content(v1.merge(v2), v2.merge(v1))


def test_serialize_unspecific_key():
    check_serialization(UnspecificKey("id"))
