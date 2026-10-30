"""TLS for remoting: a port of Apache Pekko's ``RotatingKeysSSLEngineProvider``
(``remote/artery/tcp/ssl/RotatingKeysSSLEngineProvider.scala``), with its ``PeerSubjectVerifier``
(``SessionVerifier.scala``) and ``X509Readers.getAllSubjectNames``.

Every node holds a key, a certificate and the CA certificate as PEM files, as a Kubernetes TLS
secret mounts them. Both sides of a connection present their certificate, both must chain to the
CA, and the peer's certificate must share a subject name with this node's own. The files are read
again once ``ssl_context_cache_ttl`` has passed, so rotated certificates reach new connections
without a restart.
"""

from __future__ import annotations

import ssl
import time
from dataclasses import dataclass
from typing import Any, cast

_HANDSHAKE_ROUNDS = 10


@dataclass(frozen=True)
class _ConfiguredContext:
    server: ssl.SSLContext
    client: ssl.SSLContext
    names: frozenset[str]
    """This node's subject names, for the peer-subject check."""


class RotatingKeysSSLEngineProvider:
    """Pass as ``ActorSystem(..., ssl=RotatingKeysSSLEngineProvider())`` to run remoting over TLS
    1.3 with mutual authentication."""

    def __init__(
        self,
        secret_mount_point: str = "/var/run/secrets/ohra-tls/rotating-keys-engine",
        key_file: str | None = None,
        cert_file: str | None = None,
        ca_cert_file: str | None = None,
        ssl_context_cache_ttl: float = 300.0,
    ) -> None:
        if ssl_context_cache_ttl < 0:
            raise ValueError(f"ssl_context_cache_ttl must be >= 0, not {ssl_context_cache_ttl}")
        self.key_file = key_file or f"{secret_mount_point}/tls.key"
        self.cert_file = cert_file or f"{secret_mount_point}/tls.crt"
        self.ca_cert_file = ca_cert_file or f"{secret_mount_point}/ca.crt"
        self.ssl_context_cache_ttl = ssl_context_cache_ttl
        self._cached: tuple[_ConfiguredContext, float] | None = None

    def server_context(self) -> ssl.SSLContext:
        return self._get_context().server

    def client_context(self) -> ssl.SSLContext:
        return self._get_context().client

    def verify_session(self, peercert: dict[str, Any] | None) -> str | None:
        """None when the peer may stay, else why not: Pekko's ``PeerSubjectVerifier``."""
        if not peercert:
            return "no peer certificate"
        try:
            mine = self._get_context().names
        except OSError as e:  # the cache expired and the files are mid-rotation
            return str(e)
        peer = _subject_names(peercert)
        if mine & peer:
            return None
        return (
            f"none of the peer subject names {sorted(peer)} are in local subject names "
            f"{sorted(mine)}"
        )

    def _get_context(self) -> _ConfiguredContext:
        now = time.monotonic()
        if self._cached is not None and now < self._cached[1]:
            return self._cached[0]
        # a failed rebuild raises and caches nothing, so the next connection tries again
        context = self._construct_context()
        self._cached = (context, now + self.ssl_context_cache_ttl)
        return context

    def _construct_context(self) -> _ConfiguredContext:
        try:
            server = self._context(ssl.Purpose.CLIENT_AUTH)
            client = self._context(ssl.Purpose.SERVER_AUTH)
            names = _subject_names(_own_certificate(server, client))
        except OSError as e:
            # Pekko's SslTransportException; naming the files makes a wrong mount plain
            raise ssl.SSLError(
                None,
                f"cannot build a TLS context from key {self.key_file}, certificate "
                f"{self.cert_file} and CA {self.ca_cert_file}: {e}",
            ) from e
        return _ConfiguredContext(server, client, names)

    def _context(self, purpose: ssl.Purpose) -> ssl.SSLContext:
        context = ssl.create_default_context(purpose, cafile=self.ca_cert_file)
        # Python 3.13 turned these on by default; JSSE, under Pekko, checks neither, and a
        # certificate must not pass on one Python and fail on the next
        context.verify_flags &= ~(ssl.VERIFY_X509_STRICT | ssl.VERIFY_X509_PARTIAL_CHAIN)
        context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.check_hostname = False  # Pekko's provider checks subject names, not hostnames
        context.verify_mode = ssl.CERT_REQUIRED  # the server one is setNeedClientAuth(true)
        context.load_cert_chain(self.cert_file, self.key_file)
        return context


def _subject_names(cert: dict[str, Any]) -> frozenset[str]:
    """The first common name and every DNS subject alternative name, as
    ``X509Readers.getAllSubjectNames``."""
    names: set[str] = set()
    for rdn in cert.get("subject", ()):
        for key, value in rdn:
            if key == "commonName" and not names:
                names.add(value)
    for kind, value in cert.get("subjectAltName", ()):
        if kind == "DNS":
            names.add(value)
    return frozenset(names)


def _own_certificate(server: ssl.SSLContext, client: ssl.SSLContext) -> dict[str, Any]:
    """This node's certificate, decoded: the stdlib decodes a peer's certificate but not a file,
    so hand the two contexts to each other in memory. Fails on a key that does not match the
    certificate or a certificate that does not chain to the CA."""
    s_in, s_out, c_in, c_out = ssl.MemoryBIO(), ssl.MemoryBIO(), ssl.MemoryBIO(), ssl.MemoryBIO()
    c = client.wrap_bio(c_in, c_out)
    s = server.wrap_bio(s_in, s_out, server_side=True)
    pending = [(c, c_out, s_in), (s, s_out, c_in)]
    for _ in range(_HANDSHAKE_ROUNDS):
        for side in list(pending):
            obj, out, peer_in = side
            try:
                obj.do_handshake()
                pending.remove(side)
            except ssl.SSLWantReadError:
                pass
            peer_in.write(out.read())
        if not pending:
            return cast(dict[str, Any], c.getpeercert())
    raise ssl.SSLError("the TLS handshake with this node's own certificate did not complete")
