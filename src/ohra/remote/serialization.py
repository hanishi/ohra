"""Serialization for messages that cross a node boundary.

A message type is sendable once registered, with ``@remotable`` on a frozen dataclass or
``Serialization.register(cls)``, as Pekko's serialization bindings. Encoding is self-describing
JSON: a dataclass is ``{"$type": manifest, ...fields}``, an ``ActorRef`` is ``{"$ref": path}``,
scalars pass through, lists become lists, string-keyed dicts become objects. A dict with any
other key becomes ``{"$map": [[key, value], ...]}``.

A tuple, frozenset or set becomes a plain list where its static hint says what it is: a field
annotated ``tuple[...]`` or ``frozenset[...]``, or an element, key or value of a container
hinted so. Decoding rebuilds it from the hint, with every list in a decoded key or set element
turned into a tuple so it hashes. Where no hint says so (a TypeVar, ``Any``, ``object``, the
top-level value, the elements of an unparametrized container), the value says it itself:
``{"$tuple": [...]}`` or ``{"$frozenset": [...]}``. So ``LWWRegister[A]`` with a tuple value
round-trips equal and hashable. A field declared with ``transient()`` is not encoded and
decodes to its default, as Pekko's ``delta``. Decoding needs no other type hints, so
union-typed fields and nested messages work.
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
_RESERVED = frozenset(("$ref", "$type", "$entity", "$region", "$map", "$tuple", "$frozenset"))
_hints: dict[type, dict[str, Any]] = {}


def transient(default: Any = None) -> Any:
    """A dataclass field that never goes on the wire and decodes to ``default``, as Pekko's
    ``delta``. It is left out of equality and repr too."""
    return dataclasses.field(
        default=default, compare=False, repr=False, metadata={"ohra.transient": True}
    )


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
        return _encode(value, ref_path, None)

    @staticmethod
    def decode(data: Json, resolve: Callable[[str], ActorRef[Any]]) -> object:
        """``resolve`` turns a path back into a ref: a local one, or a RemoteRef."""
        if isinstance(data, list):
            return [Serialization.decode(v, resolve) for v in data]
        if not isinstance(data, dict):
            return data
        if len(data) == 1:
            pairs, items = data.get("$map"), data.get("$tuple")
            if isinstance(pairs, list):
                return {
                    _hashable(Serialization.decode(k, resolve)): Serialization.decode(v, resolve)
                    for k, v in cast(list[list[Json]], pairs)
                }
            if isinstance(items, list):
                return tuple(Serialization.decode(v, resolve) for v in items)
            items = data.get("$frozenset")
            if isinstance(items, list):
                return frozenset(_hashable(Serialization.decode(v, resolve)) for v in items)
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
        hints = _class_hints(klass)
        kwargs: dict[str, object] = {}
        for k, v in data.items():
            if k == "$type":
                continue
            kwargs[k] = _rebuild(Serialization.decode(v, resolve), hints.get(k))
        return klass(**kwargs)


# A field whose annotation cannot be evaluated: it encodes as it did before hints were read.
_UNKNOWN: Any = object()


def _encode(value: object, ref_path: Callable[[ActorRef[Any]], str], hint: Any) -> Json:
    """``hint`` is the static type of the position ``value`` sits in: a field's annotation, or
    the element, key or value type of a hinted container. ``None`` is a position with no hint."""
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
        elements = list(cast(Iterable[object], value))
        encoded_list: list[Json] = [
            _encode(v, ref_path, h)
            for v, h in zip(elements, _element_hints(hint, len(elements)), strict=True)
        ]
        if isinstance(value, list) or not _loose(hint):
            return encoded_list
        # JSON has no tuples or sets: where no hint says so, the value says it
        return {"$tuple" if isinstance(value, tuple) else "$frozenset": encoded_list}
    if isinstance(value, dict):
        items = list(cast(dict[object, object], value).items())
        kh, vh = _item_hints(hint)
        if all(isinstance(k, str) for k, _ in items):
            out: dict[str, Json] = {}
            for k, v in items:
                if k in _RESERVED:  # decode would read the dict as a ref, a dataclass or a tag
                    raise SerializationError(
                        f"dict key {k!r} is reserved for refs, manifests and tags"
                    )
                out[cast(str, k)] = _encode(v, ref_path, vh)
            return out
        return {"$map": [[_encode(k, ref_path, kh), _encode(v, ref_path, vh)] for k, v in items]}
    manifest = _manifests.get(type(value))
    if manifest is None or not dataclasses.is_dataclass(value):
        raise SerializationError(
            f"{type(value).__qualname__} is not registered; decorate it with @remotable"
        )
    hints = _class_hints(type(value))
    encoded: dict[str, Json] = {"$type": manifest}
    for f in dataclasses.fields(value):
        if f.metadata.get("ohra.transient"):  # as Pekko's delta: never on the wire
            continue
        encoded[f.name] = _encode(getattr(value, f.name), ref_path, hints.get(f.name, _UNKNOWN))
    return encoded


def _is_union(hint: Any) -> bool:
    return typing.get_origin(hint) in (types.UnionType, typing.Union)


def _is_collection(hint: Any) -> bool:
    return (hint if typing.get_origin(hint) is None else typing.get_origin(hint)) in (
        list,
        tuple,
        frozenset,
        set,
    )


def _loose(hint: Any) -> bool:
    """A position whose static type does not say tuple or frozenset: a TypeVar, ``Any``,
    ``object``, no hint at all, or a union of those with no collection in it."""
    if hint is None or hint is Any or hint is object or isinstance(hint, typing.TypeVar):
        return True
    if _is_union(hint):
        args = typing.get_args(hint)
        return any(_loose(a) for a in args) and not any(_is_collection(a) for a in args)
    return False


def _element_hints(hint: Any, n: int) -> list[Any]:
    """The hints of a collection's elements: ``None`` (no hint) only under a loose hint or a
    bare container. A hint this cannot see through (an alias, a NewType, an abc such as
    ``Sequence``) gives ``_UNKNOWN``, so the elements encode as before."""
    if _loose(hint):
        return [None] * n
    if _is_union(hint):
        hint = next((a for a in typing.get_args(hint) if _is_collection(a)), _UNKNOWN)
    if hint is _UNKNOWN or not _is_collection(hint):
        return [_UNKNOWN] * n
    args = typing.get_args(hint)
    if not args:
        return [None] * n
    if typing.get_origin(hint) is tuple and not (len(args) == 2 and args[1] is Ellipsis):
        return list(args) if len(args) == n else [_UNKNOWN] * n
    return [args[0]] * n


def _item_hints(hint: Any) -> tuple[Any, Any]:
    """The hints of a dict's keys and values, by the rules of ``_element_hints``."""
    if _loose(hint):
        return None, None
    if _is_union(hint):
        hint = next((a for a in typing.get_args(hint) if _is_dict(a)), _UNKNOWN)
    if hint is _UNKNOWN or not _is_dict(hint):
        return _UNKNOWN, _UNKNOWN
    args = typing.get_args(hint)
    return (args[0], args[1]) if len(args) == 2 else (None, None)


def _is_dict(hint: Any) -> bool:
    return hint is dict or typing.get_origin(hint) is dict


def _class_hints(klass: type[Any]) -> dict[str, Any]:
    # resolved on first use, not by the decorator: a self-referential annotation
    # (``delta: GSet[A]``) names the class, which does not exist yet when the decorator runs
    if klass not in _hints:
        _hints[klass] = _hints_of(klass)
    return _hints[klass]


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
