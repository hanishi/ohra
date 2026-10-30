"""Remoting: TCP transport, optionally over TLS, with JSON frames, addresses, remote refs,
and the serialization of ``@remotable`` messages."""

from .remoting import Address, RemoteRef, Remoting, UniqueAddress, resolve_local, unique_address
from .serialization import Json, Serialization, SerializationError, remotable
from .tls import RotatingKeysSSLEngineProvider

__all__ = [
    "Address",
    "Json",
    "RemoteRef",
    "Remoting",
    "RotatingKeysSSLEngineProvider",
    "Serialization",
    "SerializationError",
    "UniqueAddress",
    "remotable",
    "resolve_local",
    "unique_address",
]
