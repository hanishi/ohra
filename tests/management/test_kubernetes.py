"""Kubernetes API discovery against a fake API server with a self-signed certificate and a
service-account token."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from pathlib import Path

import subprocess

import pytest

from ohra import Lookup, ResolvedTarget
from ohra.management.kubernetes import (
    KubernetesApiError,
    KubernetesApiServiceDiscovery,
    KubernetesApiSettings,
    targets,
)

FIXTURES = Path(__file__).parent / "fixtures"
PODS = json.loads((FIXTURES / "pods.json").read_text())


def test_targets_filter_pods_and_pick_the_named_port():
    # Running pods not being deleted: 10.1.0.5 and 10.1.0.8 (whose container is still waiting); the
    # deleting, the Pending, the IP-less and the status-less pods are out. Container state counts
    # only with a container_name, as Pekko's.
    assert targets(PODS, "management", "ns", "cluster.local", False, None) == [
        ResolvedTarget("10-1-0-5.ns.pod.cluster.local", 7626, "10.1.0.5"),
        ResolvedTarget("10-1-0-8.ns.pod.cluster.local", 7626, "10.1.0.8"),
    ]
    assert targets(PODS, "management", "ns", "cluster.local", True, None) == [
        ResolvedTarget("10.1.0.5", 7626, "10.1.0.5"),
        ResolvedTarget("10.1.0.8", 7626, "10.1.0.8"),
    ]
    assert targets(PODS, None, "ns", "cluster.local", True, None) == [
        ResolvedTarget("10.1.0.5", None, "10.1.0.5"),
        ResolvedTarget("10.1.0.8", None, "10.1.0.8"),
    ]
    # the waiting one is excluded
    assert targets(PODS, "management", "ns", "cluster.local", True, "app") == [
        ResolvedTarget("10.1.0.5", 7626, "10.1.0.5")
    ]
    assert targets(PODS, "nope", "ns", "cluster.local", True, None) == []  # no such port name


def test_targets_tolerate_missing_fields():
    broken = {
        "items": [
            {"status": {"phase": "Running"}},
            {"spec": {"containers": []}, "status": {"phase": "Running", "podIP": "1.2.3.4"}},
            {},
            {"spec": {}, "status": {"phase": "Running", "podIP": "1.2.3.5"}},
        ]
    }
    assert targets(broken, "management", "ns", "d", True, None) == []


@pytest.fixture
def tls(tmp_path: Path) -> tuple[Path, Path]:
    """A self-signed certificate for 127.0.0.1 and its key, generated into the test's temp dir
    (nothing committed)."""
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "2",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


async def fake_api(
    tls: tuple[Path, Path], status: int, body: bytes, seen: list[tuple[str, str]]
) -> asyncio.Server:
    """One TLS request per connection: records (target, authorization) and answers `status` with
    `body`."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tls[0], tls[1])

    async def on_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request_line = (await reader.readline()).decode()
        headers: dict[str, str] = {}
        while (line := (await reader.readline()).decode()) not in ("\r\n", "\n", ""):
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()
        seen.append((request_line.split(" ")[1], headers.get("authorization", "")))
        head = (
            f"HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
        )
        writer.write(head.encode() + body)
        await writer.drain()
        writer.close()

    return await asyncio.start_server(on_connection, "127.0.0.1", 0, ssl=context)


def settings_in(tmp_path: Path, tls: tuple[Path, Path]) -> KubernetesApiSettings:
    (tmp_path / "token").write_text("t1")
    (tmp_path / "namespace").write_text("ns")
    return KubernetesApiSettings(
        api_ca_path=str(tls[0]),
        api_token_path=str(tmp_path / "token"),
        pod_namespace_path=str(tmp_path / "namespace"),
        use_raw_ip=True,
    )


async def test_lookup_resolves_pods_and_rereads_the_token(
    tmp_path: Path, tls: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
):
    seen: list[tuple[str, str]] = []
    server = await fake_api(tls, 200, json.dumps(PODS).encode(), seen)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", str(port))
    monkeypatch.delenv("KUBERNETES_NAMESPACE", raising=False)
    try:
        discovery = KubernetesApiServiceDiscovery(settings_in(tmp_path, tls))
        resolved = await discovery.lookup(Lookup("svc", port_name="management"), 2.0)
        assert resolved.addresses == (
            ResolvedTarget("10.1.0.5", 7626, "10.1.0.5"),
            ResolvedTarget("10.1.0.8", 7626, "10.1.0.8"),
        )
        assert seen[-1] == ("/api/v1/namespaces/ns/pods?labelSelector=app%3Dsvc", "Bearer t1")
        (tmp_path / "token").write_text("t2")  # the bound token rotated
        await discovery.lookup(Lookup("svc", port_name="management"), 2.0)
        assert seen[-1][1] == "Bearer t2"
    finally:
        server.close()
        await server.wait_closed()


async def test_lookup_fails_on_403_and_on_missing_env(
    tmp_path: Path,
    tls: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.WARNING, logger="ohra.management.kubernetes")
    seen: list[tuple[str, str]] = []
    server = await fake_api(tls, 403, b'{"message": "forbidden"}', seen)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", str(port))
    try:
        with pytest.raises(KubernetesApiError):
            await KubernetesApiServiceDiscovery(settings_in(tmp_path, tls)).lookup(
                Lookup("svc"), 2.0
            )
        assert any("check RBAC" in r.message for r in caplog.records)
        monkeypatch.delenv("KUBERNETES_SERVICE_HOST")
        with pytest.raises(KubernetesApiError, match="check Kubernetes environment"):
            await KubernetesApiServiceDiscovery(settings_in(tmp_path, tls)).lookup(
                Lookup("svc"), 2.0
            )
    finally:
        server.close()
        await server.wait_closed()
