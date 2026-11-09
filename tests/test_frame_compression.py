"""Frame compression: remoting gzips a frame over 32 KiB, as Pekko's ddata serializer gzips its
large payloads (SerializationSupport.compress), and refuses a frame that inflates past the frame
limit without inflating it into memory."""

from __future__ import annotations

import asyncio
import gzip
import json
import struct
import tracemalloc
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from ohra import ActorSystem, Behaviors, SerializationError
from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import DistributedData, ORSet, ORSetKey, Replicator
from ohra.remote import Address, remotable
from ohra.remote import remoting as remoting_module
from ohra.remote.remoting import COMPRESS_ABOVE, MAX_FRAME, pack, read_frame
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

from .cluster.test_cluster import FAST, FAST_SHUTDOWN
from .util import free_port


@remotable
@dataclass(frozen=True)
class Bulky:
    text: str


def reader_of(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


async def test_a_large_frame_is_compressed_on_the_wire_and_decodes_equal():
    obj = {"kind": "msg", "items": [f"element-{i}" for i in range(10_000)]}
    body = json.dumps(obj, separators=(",", ":")).encode()
    assert len(body) > COMPRESS_ABOVE

    frame = pack(obj)

    (n,) = struct.unpack(">I", frame[:4])
    assert n == len(frame) - 4 < len(body)
    assert frame[4:6] == b"\x1f\x8b"
    assert frame == pack(obj)  # mtime=0: the same frame is the same bytes
    assert await read_frame(reader_of(frame)) == obj


async def test_a_small_frame_goes_out_unchanged():
    obj = {"kind": "msg", "text": "hello"}
    body = json.dumps(obj, separators=(",", ":")).encode()

    frame = pack(obj)

    assert frame == struct.pack(">I", len(body)) + body
    assert await read_frame(reader_of(frame)) == obj


async def test_an_inflating_frame_past_max_frame_is_refused():
    bomb = gzip.compress(bytes(MAX_FRAME + 1), mtime=0)
    with pytest.raises(ValueError, match="exceeds"):
        await read_frame(reader_of(struct.pack(">I", len(bomb)) + bomb))


async def test_an_inflating_frame_is_never_inflated_past_max_frame_into_memory():
    limit = 2**16
    bomb = gzip.compress(bytes(64 * limit), mtime=0)  # 4 MiB of zeros in a few KiB
    frame = struct.pack(">I", len(bomb)) + bomb
    with patch.object(remoting_module, "MAX_FRAME", limit):
        tracing = tracemalloc.is_tracing()
        tracemalloc.start()
        tracemalloc.reset_peak()
        try:
            with pytest.raises(ValueError, match="exceeds"):
                await read_frame(reader_of(frame))
            _, peak = tracemalloc.get_traced_memory()
        finally:
            if not tracing:
                tracemalloc.stop()
    assert peak < 16 * limit, peak


def large_gzipped() -> bytes:
    """A large frame's body, as pack gzips it."""
    return pack({"items": [f"element-{i}" for i in range(10_000)]})[4:]


async def test_trailing_data_after_a_compressed_frame_is_refused():
    body = large_gzipped() + b"garbage"
    with pytest.raises(ValueError, match="trailing"):
        await read_frame(reader_of(struct.pack(">I", len(body)) + body))


async def test_a_second_gzip_member_in_a_frame_is_refused():
    body = large_gzipped() + gzip.compress(b'{"kind":"msg"}', mtime=0)
    with pytest.raises(ValueError, match="trailing"):
        await read_frame(reader_of(struct.pack(">I", len(body)) + body))


async def test_a_body_over_max_frame_is_refused_by_the_sender_before_compression():
    """MAX_FRAME bounds the uncompressed body on both sides, so a frame that is sent is always
    one the receiver takes."""
    with patch.object(remoting_module, "MAX_FRAME", 2**16):
        async with ActorSystem(Behaviors.empty, "a", address=("127.0.0.1", free_port())) as a:
            remote = a.resolve("ohra://b@127.0.0.1:1/user/x")
            with pytest.raises(SerializationError, match="frame limit"):
                remote.tell(Bulky("x" * 2**17))  # gzips to a few hundred bytes


config = MultiNodeConfig()
first, second = config.role("first"), config.role("second")
config.common_config(cluster=ClusterSettings(**FAST), shutdown=FAST_SHUTDOWN)

LargeKey: ORSetKey[str] = ORSetKey("large")


@multi_node_spec(config)
async def test_two_nodes_exchange_a_large_orset(spec: MultiNodeSpec):
    Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(first))))
    dd = DistributedData(spec.system)
    replicator = dd.replicator
    count: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

    async def joined() -> None:
        replicator.tell(Replicator.GetReplicaCount(count.ref))
        assert await count.receive_message() == Replicator.ReplicaCount(2)

    await spec.await_assert(joined, max=10.0, interval=0.05)
    await spec.enter_barrier("after-join")

    elements = frozenset(f"element-{i}" for i in range(20_000))
    if spec.is_node(first):
        probe: TestProbe[Replicator.UpdateResponse[ORSet[str]]] = spec.create_test_probe()

        def add_all(s: ORSet[str]) -> ORSet[str]:
            for e in elements:
                s = s.add(dd.self_unique_address, e)
            return s

        empty: ORSet[str] = ORSet()
        replicator.tell(
            Replicator.Update(LargeKey, empty, Replicator.WriteAll(10.0), probe.ref, add_all)
        )
        await probe.expect_message(Replicator.UpdateSuccess(LargeKey), 15.0)
    await spec.enter_barrier("after-update")

    got: TestProbe[Replicator.GetResponse[ORSet[str]]] = spec.create_test_probe()
    replicator.tell(Replicator.Get(LargeKey, Replicator.ReadLocal(), got.ref))
    reply = await got.receive_message()
    assert isinstance(reply, Replicator.GetSuccess), reply
    assert reply.data.elements == elements
    await spec.enter_barrier("after-get")
