"""Serialization: registered dataclasses to self-describing JSON, refs as paths."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from ohra import ActorRef
from ohra.ref import FunctionRef
from ohra.remote.serialization import Serialization, SerializationError, remotable


def path(r: Any) -> str:
    return r.path


def _never(*_a: Any) -> Any:
    raise AssertionError("resolver called")


def rt(v: object) -> object:
    return Serialization.decode(json.loads(json.dumps(Serialization.encode(v, path))), _never)


@remotable
@dataclass(frozen=True)
class Cached:
    n: int
    memo: int | None = field(default=None, compare=False)


@remotable
@dataclass(frozen=True)
class A:
    n: int


@remotable
@dataclass(frozen=True)
class B:
    s: str


@remotable
@dataclass(frozen=True)
class Outer:
    inner: A | B
    items: tuple[int, ...]
    tags: dict[str, str]
    maybe: A | None = None
    many: list[A | B] = ()  # type: ignore[assignment]  # a list default is fine in a frozen dataclass test fixture


@remotable
@dataclass(frozen=True)
class WithRef:
    reply_to: ActorRef[str]
    label: str


@dataclass(frozen=True)
class Unregistered:
    x: int


def test_round_trip_nested_dataclasses_with_union_fields():
    value = Outer(inner=B("x"), items=(1, 2), tags={"k": "v"}, maybe=A(3), many=[A(1), B("y")])
    data = Serialization.encode(value, lambda _r: "unused")
    assert data == {
        "$type": "Outer",
        "inner": {"$type": "B", "s": "x"},
        "items": [1, 2],
        "tags": {"k": "v"},
        "maybe": {"$type": "A", "n": 3},
        "many": [{"$type": "A", "n": 1}, {"$type": "B", "s": "y"}],
    }
    # tuple field comes back as a tuple
    assert Serialization.decode(data, lambda _p: _never()) == value


def _never() -> ActorRef[Any]:
    raise AssertionError("no refs in this message")


def test_refs_encode_as_paths_and_decode_through_resolve():
    ref: ActorRef[str] = FunctionRef("ohra://a/user/x", lambda _m: None)
    data = Serialization.encode(WithRef(ref, "hi"), lambda r: r.path)
    assert data == {"$type": "WithRef", "reply_to": {"$ref": "ohra://a/user/x"}, "label": "hi"}
    resolved: dict[str, ActorRef[Any]] = {"ohra://a/user/x": ref}
    back = Serialization.decode(data, lambda p: resolved[p])
    assert back == WithRef(ref, "hi")
    assert isinstance(back, WithRef) and back.reply_to is ref


def test_unregistered_dataclass_raises():
    with pytest.raises(SerializationError):
        Serialization.encode(Unregistered(1), lambda r: r.path)
    with pytest.raises(SerializationError):
        Serialization.decode({"$type": "Unregistered", "x": 1}, lambda _p: _never())


def test_a_dict_with_a_reserved_key_raises_at_the_sender():
    # decode reads them as a ref, a dataclass, or (as a pair) an entity ref: refuse them all
    for key in ("$ref", "$type", "$entity", "$region"):
        with pytest.raises(SerializationError, match="reserved"):
            Serialization.encode({key: "x"}, lambda r: r.path)


def test_manifest_collision_raises_unless_same_class():
    Serialization.register(A)  # same class again: fine

    @dataclass(frozen=True)
    class Other:
        pass

    with pytest.raises(SerializationError):
        Serialization.register(Other, name="A")
    Serialization.register(Other, name="OtherName")  # an explicit name avoids the collision


def test_unsupported_value_raises():
    with pytest.raises(SerializationError):
        Serialization.encode(object(), lambda r: r.path)


@remotable
@dataclass(frozen=True)
class WithFrozenset:
    names: frozenset[str]
    pairs: frozenset[tuple[str, int]]


def test_frozensets_round_trip():
    original = WithFrozenset(frozenset({"a", "b"}), frozenset({("x", 1), ("y", 2)}))
    encoded = Serialization.encode(original, lambda r: r.path)
    assert Serialization.decode(encoded, lambda _p: None) == original  # pyright: ignore[reportArgumentType]


@remotable
@dataclass(frozen=True)
class MaybeTuple:
    xs: tuple[int, ...] | None
    nested: list[tuple[str, int]]


def test_tuples_inside_unions_and_lists_are_rebuilt():
    value = MaybeTuple(xs=(1, 2), nested=[("a", 1)])
    back = Serialization.decode(Serialization.encode(value, lambda r: r.path), lambda _p: _never())
    assert back == value
    assert (
        isinstance(back, MaybeTuple)
        and isinstance(back.xs, tuple)
        and isinstance(back.nested[0], tuple)
    )


def test_a_dict_with_non_string_keys_encodes_as_map_and_round_trips():
    value = {1: "a", ("cart", 42): "b", A(3): "c"}
    assert Serialization.encode(value, path) == {
        "$map": [[1, "a"], [["cart", 42], "b"], [{"$type": "A", "n": 3}, "c"]]
    }
    assert rt(value) == value


def test_a_dict_with_only_string_keys_encodes_as_before():
    assert Serialization.encode({"x": 1}, path) == {"x": 1}


def test_tuple_keys_and_frozenset_elements_decode_hashable():
    assert rt({("a", (1, 2)): {("b", 3): 4}}) == {("a", (1, 2)): {("b", 3): 4}}
    back = rt(WithFrozenset(frozenset(), frozenset({("x", 1)})))
    assert isinstance(back, WithFrozenset) and back.pairs == frozenset({("x", 1)})


def test_a_user_dict_key_map_is_refused():
    with pytest.raises(SerializationError):
        Serialization.encode({"$map": []}, path)


def test_a_field_with_compare_false_is_not_encoded_and_decodes_to_its_default():
    assert "memo" not in Serialization.encode(Cached(1, 99), path)  # pyright: ignore[reportOperatorIssue]
    back = rt(Cached(1, 99))
    assert back == Cached(1) and isinstance(back, Cached) and back.memo is None
