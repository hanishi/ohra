"""TLS for remoting, after Apache Pekko's TlsTcpSpec, PeerSubjectVerifierSpec and
RotatingKeysSSLEngineProviderSpec. The certificates under tests/tls/ come from
tests/tls/make.sh."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import shutil
import socket
import ssl
import threading
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from ohra import (
    ActorRef,
    ActorSystem,
    Address,
    Behavior,
    Behaviors,
    Cluster,
    ClusterSettings,
    CoordinatedShutdownSettings,
    ask,
    remotable,
)
from ohra.remote import remoting
from ohra.remote.remoting import pack
from ohra.remote.tls import RotatingKeysSSLEngineProvider, _subject_names  # pyright: ignore[reportPrivateUsage]

from .util import eventually, free_port

TLS = Path(__file__).parent / "tls"


def provider(
    name: str, key_file: str | None = None, ca_cert_file: str | None = None
) -> RotatingKeysSSLEngineProvider:
    return RotatingKeysSSLEngineProvider(
        str(TLS / name), key_file=key_file, ca_cert_file=ca_cert_file
    )


# --- the provider --------------------------------------------------------------------------------


def test_subject_names_are_the_common_name_and_dns_alt_names():
    cert = {
        "subject": ((("commonName", "a"),),),
        "subjectAltName": (("DNS", "b"), ("IP Address", "10.0.0.1")),
    }
    assert _subject_names(cert) == {"a", "b"}


def test_subject_names_from_cn_only_or_san_only():
    assert _subject_names({"subject": ((("commonName", "a"),),)}) == {"a"}
    assert _subject_names({"subject": (), "subjectAltName": (("DNS", "b"),)}) == {"b"}


def test_accept_a_peer_when_both_peers_have_the_same_subject_name():
    peer = {"subject": ((("commonName", "x"),),), "subjectAltName": (("DNS", "ohra.local"),)}
    assert provider("node-a").verify_session(peer) is None


def test_reject_a_peer_when_peers_dont_have_the_same_subject_name():
    reason = provider("node-a").verify_session({"subject": ((("commonName", "someone-else"),),)})
    assert reason is not None and "someone-else" in reason


def test_reject_a_peer_without_a_certificate():
    assert provider("node-a").verify_session(None) is not None
    assert provider("node-a").verify_session({}) is not None


@pytest.mark.parametrize("name", ["cn-only", "san-only"])
def test_a_certificate_with_one_kind_of_name_still_has_it(name: str):
    peer = {"subject": ((("commonName", "ohra-node"),),)}
    assert provider(name).verify_session(peer) is None


def test_contexts_are_tls13_with_mutual_auth():
    p = provider("node-a")
    for ctx in (p.server_context(), p.client_context()):
        assert ctx.minimum_version == ctx.maximum_version == ssl.TLSVersion.TLSv1_3
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert not ctx.check_hostname


def test_files_default_under_the_mount_point():
    p = RotatingKeysSSLEngineProvider("/m")
    assert (p.key_file, p.cert_file, p.ca_cert_file) == ("/m/tls.key", "/m/tls.crt", "/m/ca.crt")
    default = RotatingKeysSSLEngineProvider()
    assert default.cert_file == "/var/run/secrets/ohra-tls/rotating-keys-engine/tls.crt"
    assert default.ssl_context_cache_ttl == 300.0


def test_negative_ttl_is_rejected():
    with pytest.raises(ValueError):
        RotatingKeysSSLEngineProvider(ssl_context_cache_ttl=-1)


def test_a_key_that_does_not_match_its_certificate_fails_construction():
    key = str(TLS / "other-name/tls.key")
    p = provider("node-a", key_file=key)
    with pytest.raises(ssl.SSLError, match=re.escape(key)):
        p.server_context()


def test_a_missing_file_is_named_in_the_error(tmp_path: Path):
    p = RotatingKeysSSLEngineProvider(str(tmp_path))
    with pytest.raises(ssl.SSLError, match=re.escape(str(tmp_path / "tls.crt"))):
        p.server_context()


def test_verification_is_the_same_on_every_python():
    # Python 3.13's default context adds OpenSSL's strict X.509 checks; JSSE, under Pekko, has
    # neither, and a certificate must not pass on one Python and fail on the next
    p = provider("node-a")
    for ctx in (p.server_context(), p.client_context()):
        assert not ctx.verify_flags & ssl.VERIFY_X509_STRICT
        assert not ctx.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN


def test_a_session_checked_while_the_files_are_bad_is_rejected(tmp_path: Path):
    shutil.copytree(TLS / "node-a", tmp_path, dirs_exist_ok=True)
    p = RotatingKeysSSLEngineProvider(str(tmp_path), ssl_context_cache_ttl=0)
    p.client_context()
    shutil.copy(TLS / "other-name/tls.key", tmp_path / "tls.key")  # mid-rotation
    reason = p.verify_session({"subject": ((("commonName", "ohra-node"),),)})
    assert reason is not None and str(tmp_path / "tls.key") in reason


def test_a_certificate_from_another_ca_fails_construction():
    # node-a's certificate does not chain to the foreign CA, so the self-handshake fails
    p = provider("node-a", ca_cert_file=str(TLS / "foreign/ca.crt"))
    with pytest.raises(ssl.SSLError):
        p.server_context()


def test_rebuild_the_ssl_context(tmp_path: Path):
    shutil.copytree(TLS / "node-a", tmp_path, dirs_exist_ok=True)
    rotating = RotatingKeysSSLEngineProvider(str(tmp_path), ssl_context_cache_ttl=0)
    first = rotating.server_context()
    assert rotating.server_context() is not first
    cached = RotatingKeysSSLEngineProvider(str(tmp_path))
    assert cached.server_context() is cached.server_context()


# --- remoting over TLS ---------------------------------------------------------------------------


@remotable
@dataclass(frozen=True)
class Hail:
    text: str
    reply_to: ActorRef[Hail] | None = None


def recorder(log: list[str]) -> Behavior[Hail]:
    def on_message(msg: Hail) -> Behavior[Hail]:
        log.append(msg.text)
        if msg.reply_to is not None:
            msg.reply_to.tell(Hail(f"ack {msg.text}"))
        return Behaviors.same

    return Behaviors.receive_message(on_message)


def tls_node(
    behavior: Behavior[Hail], name: str, port: int, cert_dir: Path, ttl: float = 300.0
) -> ActorSystem[Hail]:
    return ActorSystem(
        behavior,
        name,
        address=("127.0.0.1", port),
        ssl=RotatingKeysSSLEngineProvider(str(cert_dir), ssl_context_cache_ttl=ttl),
    )


def hail_frame(path: str, text: str) -> bytes:
    return pack({"kind": "msg", "to": path, "message": {"$type": "Hail", "text": text}})


def raw_context(cert_dir: Path, server_side: bool) -> ssl.SSLContext:
    """A raw peer that presents cert_dir's certificate and checks nothing, so only the node's
    own checks decide."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if server_side else ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.load_cert_chain(cert_dir / "tls.crt", cert_dir / "tls.key")
    return context


async def read_all(reader: asyncio.StreamReader) -> bytes:
    data = b""
    with contextlib.suppress(ConnectionError, ssl.SSLError):
        while chunk := await reader.read(65536):
            data += chunk
    return data


def logged(caplog: pytest.LogCaptureFixture, text: str) -> bool:
    return any(text in r.getMessage() for r in caplog.records)


async def test_deliver_messages():
    log: list[str] = []
    async with (
        tls_node(recorder(log), "a", free_port(), TLS / "node-a") as a,
        tls_node(Behaviors.ignore, "b", free_port(), TLS / "node-b") as b,
    ):
        remote: ActorRef[Hail] = b.resolve(a.path)
        remote.tell(Hail("hi"))
        await eventually(lambda: log == ["hi"])
        assert await ask(remote, lambda r: Hail("asked", r), timeout=1) == Hail("ack asked")


async def test_reject_an_inbound_peer_with_another_subject_name(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.WARNING, logger="ohra.remote")
    log: list[str] = []
    port = free_port()
    async with tls_node(recorder(log), "a", port, TLS / "node-a") as a:
        context = raw_context(TLS / "other-name", server_side=False)
        reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=context)
        writer.write(hail_frame(a.path, "stranger"))
        assert b'"hello"' not in await read_all(reader)
        writer.close()
        with contextlib.suppress(ConnectionError, ssl.SSLError):
            await writer.wait_closed()
        assert logged(caplog, "someone-else")
        assert log == []


async def test_reject_an_outbound_peer_with_another_subject_name(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.WARNING, logger="ohra.remote")
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")  # last: it sets the handler too
    port = free_port()

    async def serve(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()

    server = await asyncio.start_server(
        serve, "127.0.0.1", port, ssl=raw_context(TLS / "other-name", server_side=True)
    )
    async with server, tls_node(Behaviors.ignore, "a", free_port(), TLS / "node-a") as a:
        a.resolve(f"ohra://x@127.0.0.1:{port}/user").tell(Hail("to a stranger"))
        await eventually(lambda: logged(caplog, "to a stranger"), timeout=3)
        assert logged(caplog, "someone-else")


async def test_a_node_with_a_foreign_ca_cannot_deliver(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    log: list[str] = []
    async with (
        tls_node(recorder(log), "a", free_port(), TLS / "node-a") as a,
        tls_node(Behaviors.ignore, "f", free_port(), TLS / "foreign") as f,
        tls_node(Behaviors.ignore, "b", free_port(), TLS / "node-b") as b,
    ):
        f.resolve(a.path).tell(Hail("from foreign"))
        await eventually(lambda: logged(caplog, "from foreign"), timeout=3)
        b.resolve(a.path).tell(Hail("from b"))
        await eventually(lambda: log == ["from b"])


async def test_a_plain_tcp_peer_gets_no_hello():
    log: list[str] = []
    port = free_port()
    async with (
        tls_node(recorder(log), "a", port, TLS / "node-a") as a,
        tls_node(Behaviors.ignore, "b", free_port(), TLS / "node-b") as b,
    ):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(hail_frame(a.path, "plain"))
        assert b'"hello"' not in await read_all(reader)
        writer.close()
        await writer.wait_closed()
        b.resolve(a.path).tell(Hail("from b"))
        await eventually(lambda: log == ["from b"])


async def test_a_tls_node_sending_to_a_plain_node_dead_letters(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.WARNING, logger="ohra.remote")
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")  # last: it sets the handler too
    log: list[str] = []
    async with (
        ActorSystem(recorder(log), "p", address=("127.0.0.1", free_port())) as p,
        ActorSystem(Behaviors.ignore, "q", address=("127.0.0.1", free_port())) as q,
        tls_node(Behaviors.ignore, "a", free_port(), TLS / "node-a") as a,
    ):
        a.resolve(p.path).tell(Hail("over tls"))
        await eventually(lambda: logged(caplog, "over tls"), timeout=3)
        q.resolve(p.path).tell(Hail("plain"))
        await eventually(lambda: log == ["plain"])
        assert logged(caplog, f"TLS connection to 127.0.0.1:{p.address.port} failed")


async def test_rebuild_the_ssl_context_using_new_keys(tmp_path: Path):
    shutil.copytree(TLS / "node-a", tmp_path, dirs_exist_ok=True)
    port = free_port()

    async def serial() -> str:
        context = raw_context(TLS / "node-b", server_side=False)
        reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=context)
        # CERT_NONE leaves getpeercert() empty: read the binary form instead
        der = writer.get_extra_info("ssl_object").getpeercert(binary_form=True)
        writer.close()
        with contextlib.suppress(ConnectionError, ssl.SSLError):
            await writer.wait_closed()
        del reader
        return der.hex()

    async with tls_node(Behaviors.ignore, "a", port, tmp_path, ttl=0):
        before = await serial()
        shutil.copytree(TLS / "node-b", tmp_path, dirs_exist_ok=True)
        after = await serial()
    node_b = ssl.PEM_cert_to_DER_cert((TLS / "node-b/tls.crt").read_text()).hex()
    assert before != after == node_b


async def test_keep_existing_connections_alive(tmp_path: Path):
    shutil.copytree(TLS / "node-b", tmp_path, dirs_exist_ok=True)
    log: list[str] = []
    async with (
        tls_node(recorder(log), "a", free_port(), TLS / "node-a") as a,
        tls_node(Behaviors.ignore, "b", free_port(), tmp_path, ttl=0) as b,
    ):
        remote: ActorRef[Hail] = b.resolve(a.path)
        remote.tell(Hail("first"))
        await eventually(lambda: log == ["first"])
        shutil.copy(TLS / "other-name/tls.key", tmp_path / "tls.key")  # b cannot build a context
        remote.tell(Hail("second"))
        await eventually(lambda: log == ["first", "second"])


async def test_half_rotated_files_dead_letter_then_recover(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    shutil.copytree(TLS / "node-b", tmp_path, dirs_exist_ok=True)
    log: list[str] = []
    async with (
        tls_node(recorder(log), "a", free_port(), TLS / "node-a") as a,
        tls_node(Behaviors.ignore, "b", free_port(), tmp_path, ttl=0) as b,
    ):
        shutil.copy(TLS / "other-name/tls.key", tmp_path / "tls.key")
        b.resolve(a.path).tell(Hail("mid-rotation"))
        await eventually(lambda: logged(caplog, "mid-rotation"), timeout=3)
        shutil.copy(TLS / "node-b/tls.key", tmp_path / "tls.key")
        b.resolve(a.path).tell(Hail("rotated"))
        await eventually(lambda: log == ["rotated"])


async def test_a_silent_peer_does_not_hold_shutdown():
    port = free_port()
    async with tls_node(Behaviors.ignore, "a", port, TLS / "node-a"):
        _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.01)  # let the node start its side of the handshake
    async with asyncio.timeout(1):
        writer.close()
        with contextlib.suppress(ConnectionError):
            await writer.wait_closed()


async def test_a_peer_that_never_handshakes_is_closed(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    monkeypatch.setattr(remoting, "TLS_HANDSHAKE_TIMEOUT", 0.1)
    caplog.set_level(logging.WARNING, logger="ohra.remote")
    port = free_port()
    async with tls_node(Behaviors.ignore, "a", port, TLS / "node-a"):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        async with asyncio.timeout(1):
            assert await read_all(reader) == b""  # closed by the node, not by the test
        writer.close()
        await writer.wait_closed()
        assert logged(caplog, "rejecting TLS connection")


async def test_a_frozen_peer_does_not_hold_shutdown():
    port = free_port()
    frozen, release = threading.Event(), threading.Event()

    def peer() -> None:
        context = raw_context(TLS / "node-b", server_side=False)
        with socket.create_connection(("127.0.0.1", port)) as raw:
            with context.wrap_socket(raw) as tls:
                tls.recv(1)  # handshake done and the hello arriving
                frozen.set()
                release.wait(10)  # then never read again, nor answer close_notify

    thread = threading.Thread(target=peer)
    loop = asyncio.get_running_loop()
    async with tls_node(Behaviors.ignore, "a", port, TLS / "node-a"):
        thread.start()
        await eventually(frozen.is_set, timeout=2)
        started = loop.time()
    elapsed = loop.time() - started
    release.set()
    await asyncio.to_thread(thread.join)
    assert elapsed < 3


async def test_a_bad_key_fails_startup_and_leaks_nothing(tmp_path: Path):
    shutil.copytree(TLS / "node-a", tmp_path, dirs_exist_ok=True)
    shutil.copy(TLS / "other-name/tls.key", tmp_path / "tls.key")
    with pytest.raises(ssl.SSLError):
        async with tls_node(Behaviors.ignore, "a", free_port(), tmp_path):
            pass


async def test_ssl_without_address_is_rejected():
    with pytest.raises(ValueError):
        ActorSystem(Behaviors.ignore, "x", ssl=RotatingKeysSSLEngineProvider(str(TLS / "node-a")))


FAST: dict[str, Any] = dict(
    gossip_interval=0.02,
    leader_actions_interval=0.02,
    unreachable_nodes_reaper_interval=0.02,
    seed_node_timeout=0.1,
    periodic_tasks_initial_delay=0.0,
    heartbeat_interval=0.02,
    acceptable_heartbeat_pause=0.2,
    allow_weakly_up_members=None,
)
FAST_SHUTDOWN = CoordinatedShutdownSettings(
    default_phase_timeout=1.0,
    phase_timeouts={
        "cluster-sharding-shutdown-region": 1.0,
        "cluster-exiting": 0.3,
        "actor-system-terminate": 1.0,
    },
)


async def test_three_nodes_form_a_cluster_over_tls():
    ports = [free_port(), free_port(), free_port()]
    seeds = (Address("c", "127.0.0.1", ports[0]),)

    def cluster_node(port: int, cert_dir: str) -> ActorSystem[Any]:
        return ActorSystem(
            Behaviors.ignore,
            "c",
            address=("127.0.0.1", port),
            cluster=ClusterSettings(seed_nodes=seeds, **FAST),
            shutdown=FAST_SHUTDOWN,
            ssl=RotatingKeysSSLEngineProvider(str(TLS / cert_dir)),
        )

    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(cluster_node(p, d))
            for p, d in zip(ports, ["node-a", "node-b", "node-a"], strict=True)
        ]

        def all_up(system: ActorSystem[Any]) -> bool:
            members = Cluster(system).state.members
            return len(members) == 3 and all(m.status == "up" for m in members)

        await eventually(lambda: all(all_up(n) for n in nodes), timeout=5)
