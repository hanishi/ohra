"""Ported from Apache Pekko's EntitiesSpec (cluster-sharding/src/test/scala/org/apache/pekko/
cluster/sharding/EntitiesSpec.scala), in the Scala order:

- start empty (40)
- set already remembered entities to state RememberedButNotStarted (46)
- set state to remembering start (56)
- set state to remembering stop (70)
- fully remove an entity (88)
- add an entity as active (100)
- look up actor ref by id (106)
- set state to passivating (112)

Differences: ``ActorRef.noSender`` is a ``FunctionRef`` that drops what it is sent, and
``RememberingStart(None)`` is ``RememberingStart.create(None)``. ohra adds two cases of its own,
for ``invalid_transition`` with ``fail_on_illegal_transition`` set and not set.
"""

import logging

import pytest

from ohra.cluster.sharding.entities import (
    Active,
    Entities,
    NoState,
    Passivating,
    RememberedButNotCreated,
    RememberingStart,
    RememberingStop,
)
from ohra.ref import FunctionRef

log = logging.getLogger("test.entities")


def no_sender() -> FunctionRef[object]:
    return FunctionRef("ohra://t/a", lambda _: None)


def new_entities(remembering_entities: bool, fail: bool = True) -> Entities:
    return Entities(
        log,
        remembering_entities=remembering_entities,
        verbose_debug=False,
        fail_on_illegal_transition=fail,
    )


def test_start_empty() -> None:
    entities = new_entities(False)
    assert entities.active_entity_ids() == frozenset()
    assert entities.size == 0
    assert entities.active_entities() == frozenset()


def test_set_already_remembered_entities_to_state_remembered_but_not_created() -> None:
    entities = new_entities(True)
    ids = {"a", "b", "c"}
    entities.already_remembered(ids)
    assert entities.active_entities() == frozenset()
    assert entities.size == 3
    for i in ids:
        assert entities.entity_state(i) == RememberedButNotCreated()


def test_set_state_to_remembering_start() -> None:
    entities = new_entities(True)
    entities.remembering_start("a", None)
    assert entities.entity_state("a") == RememberingStart.create(None)
    assert entities.entity_state("a") == RememberingStart()
    assert entities.pending_remembered_entities_exist()
    starts, stops = entities.pending_remember_entities()
    assert "a" in starts
    assert not stops

    # also verify removal from pending once it starts
    entities.add_entity("a", no_sender())
    assert not entities.pending_remembered_entities_exist()
    assert not entities.pending_remember_entities()[0]


def test_set_state_to_remembering_stop() -> None:
    entities = new_entities(True)
    entities.remembering_start("a", None)  # need to go through remembering start to be active
    entities.add_entity("a", no_sender())  # need to go through active to passivate
    entities.entity_passivating("a")  # need to go through passivate to remember stop
    entities.remembering_stop("a")
    assert entities.entity_state("a") == RememberingStop()
    assert entities.pending_remembered_entities_exist()
    starts, stops = entities.pending_remember_entities()
    assert "a" in stops
    assert not starts

    # also verify removal from pending once it stops
    entities.remove_entity("a")
    assert not entities.pending_remembered_entities_exist()
    assert not entities.pending_remember_entities()[1]


def test_fully_remove_an_entity() -> None:
    entities = new_entities(True)
    entities.remembering_start("a", None)
    entities.add_entity("a", no_sender())
    entities.entity_passivating("a")
    entities.remembering_stop("a")
    entities.remove_entity("a")
    assert entities.entity_state("a") == NoState()
    assert not entities.active_entities()
    assert not entities.active_entity_ids()


def test_add_an_entity_as_active() -> None:
    entities = new_entities(False)
    ref = no_sender()
    entities.add_entity("a", ref)
    assert entities.entity_state("a") == Active(ref)


def test_look_up_actor_ref_by_id() -> None:
    entities = new_entities(False)
    ref = no_sender()
    entities.add_entity("a", ref)
    assert entities.entity_id(ref) == "a"


def test_set_state_to_passivating() -> None:
    entities = new_entities(False)
    ref = no_sender()
    entities.add_entity("a", ref)
    entities.entity_passivating("a")
    assert entities.entity_state("a") == Passivating(ref)


def test_an_illegal_transition_raises_when_fail_on_illegal_transition_is_set() -> None:
    entities = new_entities(True)
    ref = no_sender()
    entities.remembering_start("a", None)
    entities.add_entity("a", ref)
    with pytest.raises(ValueError, match=r"^Transition from Active\("):
        entities.add_entity("a", ref)


def test_an_illegal_transition_is_logged_and_taken_otherwise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    entities = new_entities(True, fail=False)
    ref = no_sender()
    entities.remembering_start("a", None)
    entities.add_entity("a", ref)
    with caplog.at_level(logging.ERROR, logger="test.entities"):
        entities.add_entity("a", ref)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert [r.getMessage() for r in errors] == ["Ignoring illegal state transition in shard"]
    assert entities.entity_state("a") == Active(ref)
