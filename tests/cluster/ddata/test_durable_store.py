"""The store side of Pekko's ``DurableStore.scala`` (``LmdbDurableStore``). The envelope cases
follow ``ReplicatorMessageSerializer.durableDataEnvelopeToProto``."""

from pathlib import Path
from typing import Any

from ohra import ActorRef
from ohra.cluster.ddata import DurableDataEnvelope, DurableStore, sqlite_durable_store
from ohra.cluster.ddata.durable_store import decode_durable, durable_dir, encode_durable
from ohra.cluster.ddata.envelope import DataEnvelope
from ohra.cluster.ddata.gcounter import GCounter
from ohra.cluster.ddata.pruning_state import PruningInitialized, PruningPerformed
from ohra.cluster.ddata import SelfUniqueAddress
from ohra.remote import UniqueAddress
from ohra.testkit import ActorTestKit, TestProbe

removed = UniqueAddress("localhost", 7354, 1, "Sys")
owner = UniqueAddress("localhost", 7355, 2, "Sys")
node = SelfUniqueAddress(owner)


def counter(n: int) -> DurableDataEnvelope:
    return DurableDataEnvelope(DataEnvelope(GCounter().increment(node, n)))


def envelope(n: int) -> DataEnvelope:
    return DataEnvelope(GCounter().increment(node, n))


def test_the_envelope_keeps_only_pruning_performed_markers():
    g = GCounter().increment(node, 1)
    env = DurableDataEnvelope(
        DataEnvelope(
            g,
            {
                removed: PruningPerformed(100.0),
                owner: PruningInitialized(owner, frozenset()),
            },
        )
    )
    back = decode_durable(encode_durable(env))
    assert back.data_envelope.pruning == {removed: PruningPerformed(100.0)}
    assert back.data == g


def test_envelopes_are_equal_on_their_data():
    g = GCounter().increment(node, 1)
    a = DurableDataEnvelope(DataEnvelope(g, {removed: PruningPerformed(1.0)}))
    b = DurableDataEnvelope(DataEnvelope(g))
    assert a == b and hash(a) == hash(b)


def test_the_dir_rule():
    assert durable_dir("target/x-ddata", "Sys", "ddataReplicator", 2552) == Path(
        "target/x-ddata-Sys-ddataReplicator-2552"
    )
    assert durable_dir("/var/lib/store", "Sys", "r", 1) == Path("/var/lib/store")


def load_all(kit: ActorTestKit, path: Path, **kw: Any) -> tuple[ActorRef[Any], TestProbe[Any]]:
    probe: TestProbe[Any] = kit.create_test_probe()
    store = kit.spawn(sqlite_durable_store(path, **kw))
    store.tell(DurableStore.LoadAll(probe.ref))
    return store, probe


async def loaded(kit: ActorTestKit, path: Path) -> dict[str, DurableDataEnvelope]:
    store, probe = load_all(kit, path)
    first = await probe.receive_message()
    if isinstance(first, DurableStore.LoadAllCompleted):
        data: dict[str, DurableDataEnvelope] = {}
    else:
        data = dict(first.data)
        await probe.expect_message(DurableStore.LoadAllCompleted())
    await kit.stop(store)
    return data


async def test_load_all_without_files_completes_empty(tmp_path: Path):
    async with ActorTestKit() as kit:
        _, probe = load_all(kit, tmp_path / "none")
        await probe.expect_message(DurableStore.LoadAllCompleted())
        await probe.expect_no_message(0.05)


async def test_load_all_of_an_empty_existing_dir_completes_empty(tmp_path: Path):
    (tmp_path / "store").mkdir()
    async with ActorTestKit() as kit:
        _, probe = load_all(kit, tmp_path / "store")
        await probe.expect_message(DurableStore.LoadAllCompleted())
        await probe.expect_no_message(0.05)


async def test_stored_entries_load_in_a_new_store(tmp_path: Path):
    async with ActorTestKit() as kit:
        a, b = counter(1), counter(2)
        store, probe = load_all(kit, tmp_path)
        await probe.expect_message(DurableStore.LoadAllCompleted())
        reply = DurableStore.StoreReply("ok", "failed", probe.ref)
        store.tell(DurableStore.Store("a", a, reply))
        store.tell(DurableStore.Store("b", b, reply))
        await probe.expect_message("ok")
        await probe.expect_message("ok")
        await kit.stop(store)
        _, probe2 = load_all(kit, tmp_path)
        await probe2.expect_message(DurableStore.LoadData({"a": a, "b": b}))
        await probe2.expect_message(DurableStore.LoadAllCompleted())


async def test_write_behind_keeps_the_last_value_and_flushes_on_stop(tmp_path: Path):
    async with ActorTestKit() as kit:
        store, probe = load_all(kit, tmp_path, write_behind_interval=60.0)
        await probe.expect_message(DurableStore.LoadAllCompleted())
        reply = DurableStore.StoreReply("ok", "failed", probe.ref)
        for n in (1, 2, 3):
            store.tell(DurableStore.Store("a", counter(n), reply))
            await probe.expect_message("ok")
        assert await loaded(kit, tmp_path) == {}
        await kit.stop(store)
        assert await loaded(kit, tmp_path) == {"a": counter(3)}


async def test_expire_deletes_the_keys(tmp_path: Path):
    async with ActorTestKit() as kit:
        store, probe = load_all(kit, tmp_path)
        await probe.expect_message(DurableStore.LoadAllCompleted())
        reply = DurableStore.StoreReply("ok", "failed", probe.ref)
        store.tell(DurableStore.Store("a", counter(1), reply))
        store.tell(DurableStore.Store("b", counter(2), reply))
        await probe.expect_message("ok")
        await probe.expect_message("ok")
        store.tell(DurableStore.Expire(frozenset({"a"})))
        store.tell(DurableStore.Store("c", counter(3), reply))  # a reply means Expire is done
        await probe.expect_message("ok")
        await kit.stop(store)
        assert await loaded(kit, tmp_path) == {"b": counter(2), "c": counter(3)}


async def test_expire_drops_a_pending_write_behind_value(tmp_path: Path):
    async with ActorTestKit() as kit:
        store, probe = load_all(kit, tmp_path, write_behind_interval=60.0)
        await probe.expect_message(DurableStore.LoadAllCompleted())
        store.tell(DurableStore.Store("a", counter(1), None))
        store.tell(DurableStore.Expire(frozenset({"a"})))
        store.tell(
            DurableStore.Store("b", counter(2), DurableStore.StoreReply("ok", "x", probe.ref))
        )
        await probe.expect_message("ok")
        await kit.stop(store)
        assert await loaded(kit, tmp_path) == {"b": counter(2)}


async def test_a_corrupt_file_fails_the_load_and_stops_the_store(tmp_path: Path):
    (tmp_path / "ddata.db").write_bytes(b"not a database")
    async with ActorTestKit() as kit:
        store, probe = load_all(kit, tmp_path)
        await probe.expect_terminated(store)
        await probe.expect_no_message(0.05)


async def test_a_failing_store_replies_the_failure_message(tmp_path: Path):
    path = tmp_path / "later"
    async with ActorTestKit() as kit:
        store, probe = load_all(kit, path)
        await probe.expect_message(DurableStore.LoadAllCompleted())
        (path / "ddata.db").mkdir(parents=True)
        reply = DurableStore.StoreReply("ok", "failed", probe.ref)
        store.tell(DurableStore.Store("a", counter(1), reply))
        await probe.expect_message("failed")
        store.tell(DurableStore.Store("a", counter(1), reply))
        await probe.expect_message("failed")


async def test_a_negative_write_behind_interval_is_off(tmp_path: Path):
    async with ActorTestKit() as kit:
        store, probe = load_all(kit, tmp_path, write_behind_interval=-1.0)
        await probe.expect_message(DurableStore.LoadAllCompleted())
        store.tell(
            DurableStore.Store("a", counter(1), DurableStore.StoreReply("ok", "x", probe.ref))
        )
        await probe.expect_message("ok")
        assert await loaded(kit, tmp_path) == {"a": counter(1)}
