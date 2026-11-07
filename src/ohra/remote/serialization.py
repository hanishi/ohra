"""Serialization for messages that cross a node boundary.

A message type is sendable once registered, with ``@remotable`` on a frozen dataclass or
``Serialization.register(cls)``, as Pekko's serialization bindings. Encoding is self-describing
JSON: a dataclass is ``{"$type": manifest, ...fields}``, an ``ActorRef`` is ``{"$ref": path}``,
scalars pass through, lists and tuples become lists, string-keyed dicts become objects. A dict
with any other key becomes ``{"$map": [[key, value], ...]}``, and its decoded keys, like the
elements of a ``frozenset[...]``, have every list turned into a tuple so they hash. A dataclass
field declared ``compare=False`` is transient: it is not encoded and decodes to its default.
Decoding needs no type hints, so union-typed fields and nested messages work; a field annotated
``tuple[...]`` or ``frozenset[...]`` is rebuilt as one.
"""

from __future__ import annotations

import dataclasses
import types
import typing
from collections.abc import Callable, Iterable
from typing import Any, cast

from ..ref import ActorRef

type Json = None | bool | int | float | str | list["Json"] | dict[str, "Json"]


class SerializationError(Exception):
    """A value cannot cross the wire: unregistered type, unsupported value, or unknown manifest."""


_registry: dict[str, type] = {}
_manifests: dict[type, str] = {}
_RESERVED = frozenset(("$ref", "$type", "$entity", "$region", "$map"))
_hints: dict[type, dict[str, Any]] = {}


def remotable[C](cls: C) -> C:
    """Register a frozen dataclass under its class name."""
    Serialization.register(cast(type[Any], cls))
    return cls


class Serialization:
    @staticmethod
    def register(klass: type[Any], name: str | None = None) -> None:
        if not dataclasses.is_dataclass(klass):
            raise SerializationError(f"{klass.__qualname__} is not a dataclass")
        manifest = name or klass.__name__
        existing = _registry.get(manifest)
        if existing is not None and existing is not klass:
            raise SerializationError(
                f"manifest {manifest!r} is already taken by {existing.__qualname__}"
            )
        _registry[manifest] = klass
        _manifests[klass] = manifest

    @staticmethod
    def encode(value: object, ref_path: Callable[[ActorRef[Any]], str]) -> Json:
        """``ref_path`` turns a ref into the path another node can send to."""
        if value is None or isinstance(value, bool | int | float | str):
            return value
        if isinstance(value, ActorRef):
            # an EntityRef is a region plus an id; the other node rebuilds it
            from ..cluster.sharding.shard import EntityRef

            if isinstance(value, EntityRef):
                entity = cast(EntityRef[Any], value)
                return {"$entity": entity.entity_id, "$region": ref_path(entity.region)}
            return {"$ref": ref_path(cast(ActorRef[Any], value))}
        if isinstance(value, list | tuple | frozenset | set):
            return [Serialization.encode(v, ref_path) for v in cast(Iterable[object], value)]
        if isinstance(value, dict):
            items = list(cast(dict[object, object], value).items())
            if all(isinstance(k, str) for k, _ in items):
                out: dict[str, Json] = {}
                for k, v in items:
                    if k in _RESERVED:  # decode would read the dict as a ref or a dataclass
                        raise SerializationError(
                            f"dict key {k!r} is reserved for refs and manifests"
                        )
                    out[cast(str, k)] = Serialization.encode(v, ref_path)
                return out
            return {
                "$map": [
                    [Serialization.encode(k, ref_path), Serialization.encode(v, ref_path)]
                    for k, v in items
                ]
            }
        manifest = _manifests.get(type(value))
        if manifest is None or not dataclasses.is_dataclass(value):
            raise SerializationError(
                f"{type(value).__qualname__} is not registered; decorate it with @remotable"
            )
        encoded: dict[str, Json] = {"$type": manifest}
        for f in dataclasses.fields(value):
            if not f.compare:  # transient, as Pekko's delta: never on the wire
                continue
            encoded[f.name] = Serialization.encode(getattr(value, f.name), ref_path)
        return encoded

    @staticmethod
    def decode(data: Json, resolve: Callable[[str], ActorRef[Any]]) -> object:
        """``resolve`` turns a path back into a ref: a local one, or a RemoteRef."""
        if isinstance(data, list):
            return [Serialization.decode(v, resolve) for v in data]
        if not isinstance(data, dict):
            return data
        pairs = data.get("$map")
        if len(data) == 1 and isinstance(pairs, list):
            return {
                _hashable(Serialization.decode(k, resolve)): Serialization.decode(v, resolve)
                for k, v in cast(list[list[Json]], pairs)
            }
        ref = data.get("$ref")
        if isinstance(ref, str):
            return resolve(ref)
        entity_id, region = data.get("$entity"), data.get("$region")
        if isinstance(entity_id, str) and isinstance(region, str):
            from ..cluster.sharding.shard import EntityRef

            return cast(object, EntityRef[Any](resolve(region), entity_id))
        manifest = data.get("$type")
        if manifest is None:
            return {k: Serialization.decode(v, resolve) for k, v in data.items()}
        if not isinstance(manifest, str) or manifest not in _registry:
            raise SerializationError(f"unknown manifest {manifest!r}")
        klass = _registry[manifest]
        # resolved on first decode: a self-referential annotation (``delta: GSet[A]``) names the
        # class, which does not exist yet when the decorator runs
        if klass not in _hints:
            _hints[klass] = _hints_of(klass)
        hints = _hints[klass]
        kwargs: dict[str, object] = {}
        for k, v in data.items():
            if k == "$type":
                continue
            kwargs[k] = _rebuild(Serialization.decode(v, resolve), hints.get(k))
        return klass(**kwargs)


def _hints_of(klass: type[Any]) -> dict[str, Any]:
    try:
        try:
            return typing.get_type_hints(klass)
        except NameError:
            # Python 3.12.2 does not resolve a PEP 695 class's own type parameters (``class X[A]``)
            # in string annotations; later 3.12 and 3.13 do. Passing any localns would stop
            # get_type_hints from looking in the class's own namespace (nested classes), so
            # this is only the retry.
            params = {
                p.__name__: p for c in klass.__mro__ for p in getattr(c, "__type_params__", ())
            }
            return typing.get_type_hints(klass, localns=params)
    # a locally defined class whose annotations cannot be evaluated: no tuple rebuilding
    except Exception:
        return {}


def _hashable(x: object) -> object:
    """JSON has no tuples: a decoded key or set element is hashable only with lists as tuples."""
    if isinstance(x, list):
        return tuple(_hashable(v) for v in cast(list[object], x))
    return x


def _is_tuple(hint: Any) -> bool:
    return hint is tuple or typing.get_origin(hint) is tuple


def _is_frozenset(hint: Any) -> bool:
    return hint is frozenset or typing.get_origin(hint) is frozenset


def _rebuild(value: object, hint: Any) -> object:
    """JSON has no tuples: rebuild them where the field annotation says tuple, including inside
    unions, lists and dict values, so a decoded message equals and hashes like the original."""
    if hint is None:
        return value
    origin, args = typing.get_origin(hint), typing.get_args(hint)
    if origin in (types.UnionType, typing.Union):
        candidates: list[Any] = [a for a in args if a is not type(None)]
        if isinstance(value, list):
            for a in candidates:
                if _is_tuple(a) or _is_frozenset(a) or a is list or typing.get_origin(a) is list:
                    return _rebuild(cast(list[object], value), a)
        return cast(object, value)
    if isinstance(value, list):
        items: list[object] = cast(list[object], value)
        if _is_frozenset(hint):
            return frozenset(_hashable(_rebuild(v, args[0]) if args else v) for v in items)
        if _is_tuple(hint):
            if args and args[-1] is Ellipsis:
                return tuple(_rebuild(v, args[0]) for v in items)
            if args and len(args) == len(items):
                return tuple(_rebuild(v, a) for v, a in zip(items, args, strict=True))
            return tuple(items)
        if origin is list and args:
            rebuilt: list[object] = [_rebuild(v, args[0]) for v in items]
            return rebuilt
        return items
    if isinstance(value, dict) and origin is dict and len(args) == 2:
        mapping: dict[object, object] = {
            k: _rebuild(v, args[1]) for k, v in cast(dict[object, object], value).items()
        }
        return mapping
    return cast(object, value)
