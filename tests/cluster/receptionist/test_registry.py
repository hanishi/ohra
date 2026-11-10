"""Tests for the receptionist's registry, after Pekko's Registry.scala (no Pekko spec covers it
on its own; ClusterReceptionistSpec exercises it)."""

import json
from typing import Any

from ohra.cluster.ddata import ORMultiMap, SelfUniqueAddress
from ohra.cluster.receptionist.registry import (
    EMPTY_OR_MULTI_MAP,
    Entry,
    ServiceRegistry,
    ShardedServiceRegistry,
    collect_changed_keys,
    java_string_hash,
)
from ohra.receptionist import ServiceKey
from ohra.ref import ActorRef, FunctionRef
from ohra.remote import RemoteRef, UniqueAddress
from ohra.remote.serialization import Serialization

me = UniqueAddress("localhost", 7354, 1, "Sys")
other = UniqueAddress("localhost", 7355, 2, "Sys")
node = SelfUniqueAddress(me)

local: ActorRef[Any] = FunctionRef("ohra://Sys/user/a", lambda _m: None)
remote: ActorRef[Any] = RemoteRef("ohra://Sys@localhost:7355/user/b", None)  # type: ignore[arg-type]
refs = {r.path: r for r in (local, remote)}


def test_java_string_hash():
    assert java_string_hash("") == 0
    assert java_string_hash("a") == 97
    assert java_string_hash("hello") == 99162322
    assert java_string_hash("polygenelubricants") == -2147483648
    assert java_string_hash("é") == 233
    assert java_string_hash("😀") == 1772899


def test_index_uses_javas_truncating_remainder():
    reg = ShardedServiceRegistry.create(5)
    assert reg.ddata_key_for(ServiceKey("polygenelubricants")).id == "ReceptionistKey_3"
    assert reg.ddata_key_for(ServiceKey("hello")).id == "ReceptionistKey_2"


def test_entry_equality_ignores_timestamp():
    assert Entry(local, 1, 10) == Entry(local, 1, 20)
    assert hash(Entry(local, 1, 10)) == hash(Entry(local, 1, 20))
    assert Entry(local, 1) != Entry(local, 2)


def test_entry_unique_address():
    assert Entry(local, 1).unique_address(me.address) == UniqueAddress("localhost", 7354, 1, "Sys")
    assert Entry(remote, 2).unique_address(me.address) == other


def test_service_registry_add_remove_and_changed_keys():
    k: ServiceKey[Any] = ServiceKey("k")
    e = Entry(local, 1)
    empty = ServiceRegistry(EMPTY_OR_MULTI_MAP)
    one = empty.add_binding(k, e, node)
    assert one.entries_for(k) == {e}
    assert one.actor_refs_for(k) == {local}
    assert collect_changed_keys(empty, one) == {k}
    assert collect_changed_keys(one, one) == set()
    gone = one.remove_binding(k, e, node)
    assert gone.entries_for(k) == frozenset()
    assert collect_changed_keys(one, gone) == {k}


def test_remove_all_and_keys_for():
    k1: ServiceKey[Any] = ServiceKey("k1")
    k2: ServiceKey[Any] = ServiceKey("k2")
    e1, e2 = Entry(local, 1), Entry(remote, 2)
    reg = ServiceRegistry(EMPTY_OR_MULTI_MAP).add_binding(k1, e1, node).add_binding(k2, e2, node)
    assert reg.keys_for(me, me.address) == {k1}
    assert reg.keys_for(other, me.address) == {k2}
    assert reg.remove_all({k1: {e1}, k2: {e2}}, node).to_or_multi_map().entries == {}


def test_sharded_registry():
    k1: ServiceKey[Any] = ServiceKey("k1")
    k2: ServiceKey[Any] = ServiceKey("k2")
    e1, e2 = Entry(local, 1), Entry(remote, 2)
    sharded = ShardedServiceRegistry.create(3)
    assert [k.id for k in sharded.all_ddata_keys] == [f"ReceptionistKey_{n}" for n in range(3)]
    for k, e in ((k1, e1), (k2, e2)):
        dk = sharded.ddata_key_for(k)
        sharded = sharded.with_service_registry(
            dk, sharded.registry_for(dk).add_binding(k, e, node)
        )
    assert {k for k, _ in sharded.all_services()} == {k1, k2}
    assert set(sharded.all_entries()) == {e1, e2}
    assert sharded.actor_refs_for(k2) == {remote}
    assert sharded.keys_for(other, node) == {k2}
    assert sharded.all_unique_addresses_in_state(me) == {me, other}
    grouped = sharded.entries_per_ddata_key({k1: {e1}, k2: {e2}})
    assert grouped[sharded.ddata_key_for(k1)][k1] == {e1}
    changed = sharded.collect_changed_keys(
        sharded.ddata_key_for(k1), ServiceRegistry(EMPTY_OR_MULTI_MAP)
    )
    assert changed == {k1} | (
        {k2} if sharded.ddata_key_for(k1) == sharded.ddata_key_for(k2) else set()
    )


def test_sharded_registry_nodes_and_unreachable():
    s = ShardedServiceRegistry.create(2).add_node(me).add_node(other).add_unreachable(other)
    assert s.nodes == {me, other} and s.unreachable == {other}
    s = s.remove_unreachable(other)
    assert s.unreachable == frozenset()
    assert s.add_unreachable(other).remove_node(other).unreachable == frozenset()
    assert s.remove_node(other).nodes == {me}


def test_wire_round_trip():
    k: ServiceKey[Any] = ServiceKey("k")
    e1, e2 = Entry(local, 1, 5), Entry(remote, 2, 6)
    m: ORMultiMap[ServiceKey[Any], Entry] = (
        ServiceRegistry(EMPTY_OR_MULTI_MAP).add_binding(k, e1, node).add_binding(k, e2, node)
    ).to_or_multi_map()
    for v in (e2, m):
        text = json.dumps(Serialization.encode(v, lambda r: r.path))
        back = Serialization.decode(json.loads(text), lambda p: refs[p])
        assert back == v and type(back) is type(v)
    assert back.entries == {k: frozenset({e1, e2})}
