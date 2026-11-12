"""The remember-entities stores on Distributed Data, with a TestProbe standing in for the
Replicator: DDataRememberEntitiesShardStore's keys, consistencies, batching, retries and failures,
and the messages it drops (DDataRememberEntitiesShardStore.scala). Pekko has no spec of its own
for these steps."""

from __future__ import annotations

import logging
from typing import Any, cast

import pytest

from ohra import ActorRef
from ohra.cluster.ddata import DistributedData, ORSet, Replicator
from ohra.cluster.sharding import ClusterShardingSettings
from ohra.cluster.sharding.remember_entities import RememberEntitiesShardStore, ddata_shard_store
from ohra.testkit import TestProbe

from ...util import eventually
from .test_remember_entities_shard_store_spec import Node, one_node

Store = RememberEntitiesShardStore
type Set_ = ORSet[str]


def store(node: Node, replicator: TestProbe[Any]) -> ActorRef[Store.Command]:
    behavior = ddata_shard_store("T", "S", replicator.ref, ClusterShardingSettings(), 1)
    return node.spawn(behavior, "store")


def orset(node: Node, ids: frozenset[str]) -> Set_:
    me = DistributedData(node.system).self_unique_address
    s: Set_ = ORSet.empty
    for i in ids:
        s = s.add(me, i)
    return s


async def expect_gets(replicator: TestProbe[Any]) -> dict[int, Replicator.Get[Set_]]:
    """The five initial Gets, by key index."""
    gets: dict[int, Replicator.Get[Set_]] = {}
    for _ in range(5):
        get = cast(Replicator.Get[Set_], await replicator.expect_message_type(Replicator.Get, 3))
        gets[int(get.key.id.rsplit("-", 1)[1])] = get
    return gets


async def expect_update(replicator: TestProbe[Any]) -> Replicator.Update[Set_]:
    return cast(
        Replicator.Update[Set_], await replicator.expect_message_type(Replicator.Update, 3)
    )


async def load(
    node: Node, replicator: TestProbe[Any], ids_by_key: dict[int, frozenset[str]] | None = None
) -> None:
    """Answer the five initial Gets: ``GetSuccess`` for a key in ``ids_by_key``, else
    ``NotFound``."""
    ids_by_key = ids_by_key or {}
    for i, get in (await expect_gets(replicator)).items():
        if i in ids_by_key:
            get.reply_to.tell(Replicator.GetSuccess(get.key, orset(node, ids_by_key[i])))
        else:
            get.reply_to.tell(Replicator.NotFound(get.key))


async def idle(node: Node, replicator: TestProbe[Any], shard: TestProbe[Any]) -> ActorRef[Any]:
    """A store past its load, with the remembered entities sent to ``shard``."""
    s = store(node, replicator)
    await load(node, replicator)
    s.tell(Store.GetEntities(shard.ref))
    await shard.expect_message(Store.RememberedEntities(frozenset()), 3)
    return s


async def failed_update(response: Any) -> None:
    """A store whose write gets ``response(key)`` stops."""
    async with one_node() as node:
        replicator, shard = node.probe(), node.probe()
        s = await idle(node, replicator, shard)
        s.tell(Store.Update(frozenset({"a"}), frozenset(), shard.ref))
        update = await expect_update(replicator)
        update.reply_to.tell(response(update.key))
        await shard.expect_terminated(s, 3)


def logged(caplog: pytest.LogCaptureFixture, level: int, text: str) -> int:
    return sum(1 for r in caplog.records if r.levelno == level and text in r.getMessage())


async def test_the_store_reads_five_keys_with_read_majority_plus():
    async with one_node() as node:
        replicator = node.probe()
        store(node, replicator)
        gets = await expect_gets(replicator)
        assert {g.key.id for g in gets.values()} == {f"shard-T-S-{i}" for i in range(5)}
        assert {g.consistency for g in gets.values()} == {Replicator.ReadMajorityPlus(2.0, 5, 1)}


async def test_an_id_is_written_to_the_key_of_its_java_hash():
    async with one_node() as node:
        replicator, shard = node.probe(), node.probe()
        s = await idle(node, replicator, shard)

        s.tell(Store.Update(frozenset({"a"}), frozenset(), shard.ref))  # "a".hashCode = 97
        update = await expect_update(replicator)
        assert update.key.id == "shard-T-S-2"
        assert update.write_consistency == Replicator.WriteMajorityPlus(1.25, 3, 1)
        assert update.modify(ORSet.empty).elements == frozenset({"a"})
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        await shard.expect_message(Store.UpdateDone(frozenset({"a"}), frozenset()), 3)

        # hashCode = Integer.MIN_VALUE, whose abs is itself in Java
        s.tell(Store.Update(frozenset({"polygenelubricants"}), frozenset(), shard.ref))
        update = await expect_update(replicator)
        assert update.key.id == "shard-T-S-3"


async def test_a_batch_across_keys_is_one_update_per_key_and_one_update_done():
    async with one_node() as node:
        replicator, shard = node.probe(), node.probe()
        s = await idle(node, replicator, shard)
        started, stopped = frozenset({"a", "polygenelubricants"}), frozenset({"b"})
        s.tell(Store.Update(started, stopped, shard.ref))  # "b".hashCode = 98 shares key 3
        updates = {u.key.id: u for u in [await expect_update(replicator) for _ in range(2)]}
        await replicator.expect_no_message(0.1)
        assert set(updates) == {"shard-T-S-2", "shard-T-S-3"}
        key3 = updates["shard-T-S-3"]
        assert key3.modify(orset(node, frozenset({"b"}))).elements == frozenset(
            {"polygenelubricants"}
        )

        key3.reply_to.tell(Replicator.UpdateSuccess(key3.key))
        await shard.expect_no_message(0.1)
        key2 = updates["shard-T-S-2"]
        key2.reply_to.tell(Replicator.UpdateSuccess(key2.key))
        await shard.expect_message(Store.UpdateDone(started, stopped), 3)
        await shard.expect_no_message(0.1)


async def test_an_update_timeout_is_retried_three_times_then_the_store_stops(
    caplog: pytest.LogCaptureFixture,
):
    async with one_node() as node:
        replicator, shard = node.probe(), node.probe()
        s = await idle(node, replicator, shard)
        s.tell(Store.Update(frozenset({"a"}), frozenset(), shard.ref))
        update = await expect_update(replicator)
        for _ in range(3):
            update.reply_to.tell(Replicator.UpdateTimeout(update.key))
            update = await expect_update(replicator)
            assert update.key.id == "shard-T-S-2"
        update.reply_to.tell(Replicator.UpdateTimeout(update.key))
        await shard.expect_terminated(s, 3)
        await replicator.expect_no_message(0.1)
        assert logged(caplog, logging.ERROR, "gave up after [3] retries") == 1


async def test_a_get_failure_is_retried_until_fifteen_attempts_then_the_store_stops(
    caplog: pytest.LogCaptureFixture,
):
    async with one_node() as node:
        replicator, shard = node.probe(), node.probe()
        s = store(node, replicator)
        get = (await expect_gets(replicator))[0]
        for _ in range(14):
            get.reply_to.tell(Replicator.GetFailure(get.key))
            get = cast(
                Replicator.Get[Set_], await replicator.expect_message_type(Replicator.Get, 3)
            )
            assert get.key.id == "shard-T-S-0"
        get.reply_to.tell(Replicator.GetFailure(get.key))
        await shard.expect_terminated(s, 3)
        assert logged(caplog, logging.WARNING, "unable to get an initial state") == 14
        assert logged(caplog, logging.ERROR, "giving up after retrying") == 1


async def test_get_data_deleted_stops_the_store():
    async with one_node() as node:
        replicator, shard = node.probe(), node.probe()
        s = store(node, replicator)
        get = (await expect_gets(replicator))[0]
        get.reply_to.tell(Replicator.GetDataDeleted(get.key))
        await shard.expect_terminated(s, 3)


async def test_a_store_failure_stops_the_store():
    await failed_update(Replicator.StoreFailure)


async def test_a_modify_failure_stops_the_store():
    await failed_update(lambda key: Replicator.ModifyFailure(key, "boom", ValueError("boom")))


async def test_update_data_deleted_stops_the_store():
    await failed_update(Replicator.UpdateDataDeleted)


async def test_an_update_before_the_load_is_dropped_with_a_warning(
    caplog: pytest.LogCaptureFixture,
):
    async with one_node() as node:
        replicator, shard, requester = node.probe(), node.probe(), node.probe()
        s = store(node, replicator)
        gets = await expect_gets(replicator)
        s.tell(Store.Update(frozenset({"a"}), frozenset(), requester.ref))
        await eventually(
            lambda: (
                "got an update before load of initial entities completed, dropping update"
                in caplog.text
            ),
            timeout=3,
        )
        for get in gets.values():
            get.reply_to.tell(Replicator.NotFound(get.key))
        s.tell(Store.GetEntities(shard.ref))
        await shard.expect_message(Store.RememberedEntities(frozenset()), 3)
        await replicator.expect_no_message(0.1)
        await requester.expect_no_message(0.1)


async def test_entities_requested_before_the_load_are_sent_when_it_completes():
    async with one_node() as node:
        replicator, shard = node.probe(), node.probe()
        s = store(node, replicator)
        s.tell(Store.GetEntities(shard.ref))
        await load(node, replicator, {0: frozenset({"x"}), 3: frozenset({"y", "z"})})
        await shard.expect_message(Store.RememberedEntities(frozenset({"x", "y", "z"})), 3)


async def test_a_second_get_entities_is_ignored(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    async with one_node() as node:
        replicator, shard = node.probe(), node.probe()
        s = await idle(node, replicator, shard)
        s.tell(Store.GetEntities(shard.ref))
        await eventually(
            lambda: "got another get entities request after responding to one" in caplog.text,
            timeout=3,
        )
        await shard.expect_no_message(0.1)


async def test_an_update_during_a_write_is_dropped_with_a_warning(
    caplog: pytest.LogCaptureFixture,
):
    async with one_node() as node:
        replicator, shard, second = node.probe(), node.probe(), node.probe()
        s = await idle(node, replicator, shard)
        s.tell(Store.Update(frozenset({"a"}), frozenset(), shard.ref))
        update = await expect_update(replicator)
        s.tell(Store.Update(frozenset({"b"}), frozenset(), second.ref))
        await eventually(
            lambda: (
                "got a new update before write of previous completed, dropping update"
                in caplog.text
            ),
            timeout=3,
        )
        await replicator.expect_no_message(0.1)
        update.reply_to.tell(Replicator.UpdateSuccess(update.key))
        await shard.expect_message(Store.UpdateDone(frozenset({"a"}), frozenset()), 3)
        await second.expect_no_message(0.1)
        await replicator.expect_no_message(0.1)
