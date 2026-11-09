"""PruningState, from Pekko's PruningState.scala, ported with PruningStateSpec."""

import json
import time

import pytest

from ohra.cluster.ddata.pruning_state import PruningInitialized, PruningPerformed
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

node1 = UniqueAddress("localhost", 7354, 1, "Sys")
node2 = UniqueAddress("localhost", 7355, 2, "Sys")
node3 = UniqueAddress("localhost", 7356, 3, "Sys")
node4 = UniqueAddress("localhost", 7357, 4, "Sys")


def test_merge_state_correctly():
    p1 = PruningInitialized(node1, frozenset())
    p2 = PruningPerformed(time.time() + 3600)
    assert p1.merge(p2) == p2
    assert p2.merge(p1) == p2
    p3 = PruningPerformed(p2.obsolete_time - 1)
    assert p2.merge(p3) == p2
    assert p3.merge(p2) == p2


def test_merge_owner_correctly():
    p1 = PruningInitialized(node1, frozenset())
    p2 = PruningInitialized(node2, frozenset())
    expected = PruningInitialized(node1, frozenset())
    assert p1.merge(p2) == expected
    assert p2.merge(p1) == expected


def test_merge_seen_correctly():
    p1 = PruningInitialized(node1, frozenset({node2.address}))
    p2 = PruningInitialized(node1, frozenset({node4.address}))
    expected = PruningInitialized(node1, frozenset({node2.address, node4.address}))
    assert p1.merge(p2) == expected
    assert p2.merge(p1) == expected


def test_add_seen_skips_the_owner_and_known_nodes():
    p = PruningInitialized(node1, frozenset())
    assert p.add_seen(node1.address) == p
    once = p.add_seen(node2.address)
    assert once == PruningInitialized(node1, frozenset({node2.address}))
    assert once.add_seen(node2.address) == once
    assert PruningPerformed(5.0).add_seen(node2.address) == PruningPerformed(5.0)


def test_is_obsolete_at_and_after_the_time():
    p = PruningPerformed(10.0)
    assert p.is_obsolete(10.0)
    assert p.is_obsolete(11.0)
    assert not p.is_obsolete(9.9)


def test_pruning_states_round_trip_over_serialization():
    for state in (
        PruningInitialized(node1, frozenset({node2.address, node3.address})),
        PruningInitialized(node1, frozenset()),
        PruningPerformed(12.5),
    ):
        wire = json.dumps(Serialization.encode(state, lambda r: r.path))
        back = Serialization.decode(json.loads(wire), lambda p: pytest.fail(p))
        assert back == state
        assert type(back) is type(state)
