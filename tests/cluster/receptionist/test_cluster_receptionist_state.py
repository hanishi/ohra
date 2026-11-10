"""The cluster receptionist's State, ported from Pekko's ClusterReceptionistStateSpec."""

from typing import Any

from ohra.cluster.receptionist.cluster_receptionist import State
from ohra.cluster.receptionist.registry import ShardedServiceRegistry
from ohra.receptionist import ServiceKey
from ohra.ref import FunctionRef
from ohra.remote import UniqueAddress

SOME_SERVICE = ServiceKey[int]("boy-oh-boy!")
SOME_OTHER_SERVICE = ServiceKey[int]("disappointing!")
SELF = UniqueAddress("127.0.0.1", 0, 555, "MySystem")


def empty_state(distributed_key_count: int = 1) -> State:
    return State(registry=ShardedServiceRegistry.create(distributed_key_count).add_node(SELF))


def some_ref() -> FunctionRef[Any]:
    return FunctionRef("ohra://MySystem/user/some", lambda _: None)


def test_keep_track_of_local_keys_per_service():
    ref = some_ref()
    state = empty_state().add_local_service(ref, SOME_SERVICE)
    state = state.add_local_service(ref, SOME_OTHER_SERVICE)
    assert state.services_per_actor[ref] == {SOME_SERVICE, SOME_OTHER_SERVICE}
    state = state.remove_local_service(ref, SOME_SERVICE, 0.0)
    state = state.remove_local_service(ref, SOME_OTHER_SERVICE, 0.0)
    assert ref not in state.services_per_actor


def test_keep_a_tombstone_for_removed_services():
    ref = some_ref()
    state = empty_state().add_local_service(ref, SOME_SERVICE)
    state = state.remove_local_service(ref, SOME_SERVICE, 0.0)
    assert state.has_tombstone(SOME_SERVICE, ref)


def test_prune_tombstones():
    ref = some_ref()
    state = empty_state().add_local_service(ref, SOME_SERVICE)
    state = state.remove_local_service(
        ref, SOME_SERVICE, 100.0 - 10.0
    )  # Deadline.now - 10.seconds
    state = state.prune_tombstones(100.0)
    assert not state.tombstones


def test_a_tombstone_with_time_left_is_kept():
    ref = some_ref()
    state = empty_state().add_local_service(ref, SOME_SERVICE)
    state = state.remove_local_service(ref, SOME_SERVICE, 110.0)
    assert state.prune_tombstones(100.0) is state
