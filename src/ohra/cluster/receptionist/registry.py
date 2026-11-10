"""The cluster receptionist's registry on ``ORMultiMap``, ported from Pekko's Registry.scala and
``Entry`` of ClusterReceptionist.scala (60-68)."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, cast

from ...receptionist import ServiceKey
from ...ref import ActorRef
from ...remote import Address, UniqueAddress
from ...remote.serialization import Serialization
from ..ddata import ORMultiMap, ORMultiMapKey, SelfUniqueAddress


def java_string_hash(s: str) -> int:
    """Java's ``String.hashCode``: 31-based over UTF-16 code units, as a signed 32-bit int."""
    h = 0
    data = s.encode("utf-16-be")
    for i in range(0, len(data), 2):
        h = (h * 31 + int.from_bytes(data[i : i + 2])) & 0xFFFFFFFF
    return h - (1 << 32) if h >= 1 << 31 else h


def _is_local(ref: ActorRef[Any]) -> bool:
    """Pekko's ``hasLocalScope``: ohra's local paths carry no ``name@host:port``."""
    return "@" not in ref.path.split("/", 3)[2]


@dataclass(frozen=True)
class Entry:
    """ClusterReceptionist.scala 60-68. ``system_uid`` tells actors at one path in different
    incarnations of a node apart; ``created_timestamp`` is not part of equality (Pekko's second
    parameter list)."""

    ref: ActorRef[Any]
    system_uid: int
    created_timestamp: int = field(default=0, compare=False)

    def unique_address(self, self_address: Address) -> UniqueAddress:
        if _is_local(self.ref):
            return UniqueAddress(
                self_address.host, self_address.port, self.system_uid, self_address.name
            )
        a = Address.parse("/".join(self.ref.path.split("/", 3)[:3]))
        return UniqueAddress(a.host, a.port, self.system_uid, a.name)


# not @remotable: "Entry" is too likely to collide with an application's own message
Serialization.register(Entry, "ClusterReceptionist.Entry")

type DDataKey = ORMultiMapKey[ServiceKey[Any], Entry]

EMPTY_OR_MULTI_MAP: ORMultiMap[ServiceKey[Any], Entry] = ORMultiMap.empty


@dataclass(frozen=True)
class ServiceRegistry:
    """Registry.scala 100-140."""

    entries: ORMultiMap[ServiceKey[Any], Entry]

    def actor_refs_for(self, key: ServiceKey[Any]) -> frozenset[ActorRef[Any]]:
        return frozenset(e.ref for e in self.entries_for(key))

    def entries_for(self, key: ServiceKey[Any]) -> frozenset[Entry]:
        return self.entries.get_or_else(key, frozenset())

    def keys_for(
        self, address: UniqueAddress, self_address: Address
    ) -> frozenset[ServiceKey[Any]]:
        return frozenset(
            k
            for k, es in self.entries.entries.items()
            if any(e.unique_address(self_address) == address for e in es)
        )

    def add_binding(
        self, key: ServiceKey[Any], value: Entry, node: SelfUniqueAddress
    ) -> ServiceRegistry:
        return ServiceRegistry(self.entries.add_binding(node, key, value))

    def remove_binding(
        self, key: ServiceKey[Any], value: Entry, node: SelfUniqueAddress
    ) -> ServiceRegistry:
        return ServiceRegistry(self.entries.remove_binding(node, key, value))

    def remove_all(
        self, entries: Mapping[ServiceKey[Any], Iterable[Entry]], node: SelfUniqueAddress
    ) -> ServiceRegistry:
        acc = self
        for key, es in entries.items():
            for entry in es:
                acc = acc.remove_binding(key, entry, node)
        return acc

    def to_or_multi_map(self) -> ORMultiMap[ServiceKey[Any], Entry]:
        return self.entries


def collect_changed_keys(previous: ServiceRegistry, new: ServiceRegistry) -> set[ServiceKey[Any]]:
    """ServiceRegistry.collectChangedKeys, Registry.scala 154-166."""
    all_keys = previous.to_or_multi_map().entries.keys() | new.to_or_multi_map().entries.keys()
    return {k for k in all_keys if previous.entries_for(k) != new.entries_for(k)}


@dataclass(frozen=True)
class ShardedServiceRegistry:
    """Registry.scala 25-113: the service registry sharded over several ddata keys."""

    service_registries: Mapping[DDataKey, ServiceRegistry]
    nodes: frozenset[UniqueAddress] = frozenset()
    unreachable: frozenset[UniqueAddress] = frozenset()
    _keys: tuple[DDataKey, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_keys", tuple(self.service_registries))

    @staticmethod
    def create(number_of_keys: int) -> ShardedServiceRegistry:
        empty = ServiceRegistry(EMPTY_OR_MULTI_MAP)
        return ShardedServiceRegistry(
            {
                cast(DDataKey, ORMultiMapKey(f"ReceptionistKey_{n}")): empty
                for n in range(number_of_keys)
            }
        )

    def registry_for(self, ddata_key: DDataKey) -> ServiceRegistry:
        return self.service_registries[ddata_key]

    @property
    def all_ddata_keys(self) -> tuple[DDataKey, ...]:
        return self._keys

    def ddata_key_for(self, service_key: ServiceKey[Any]) -> DDataKey:
        # Java's % truncates toward zero; Python's floors
        h, n = java_string_hash(service_key.id), len(self.service_registries)
        return self._keys[abs(h - n * int(h / n))]

    def all_services(self) -> Iterator[tuple[ServiceKey[Any], frozenset[Entry]]]:
        for r in self.service_registries.values():
            yield from r.entries.entries.items()

    def all_entries(self) -> Iterator[Entry]:
        for _, es in self.all_services():
            yield from es

    def actor_refs_for(self, key: ServiceKey[Any]) -> frozenset[ActorRef[Any]]:
        return self.service_registries[self.ddata_key_for(key)].actor_refs_for(key)

    def keys_for(
        self, address: UniqueAddress, node: SelfUniqueAddress
    ) -> frozenset[ServiceKey[Any]]:
        """Keys that have a registered service instance on ``address``."""
        return frozenset(
            k
            for r in self.service_registries.values()
            for k in r.keys_for(address, node.unique_address.address)
        )

    def with_service_registry(
        self, ddata_key: DDataKey, registry: ServiceRegistry
    ) -> ShardedServiceRegistry:
        return replace(self, service_registries={**self.service_registries, ddata_key: registry})

    def all_unique_addresses_in_state(
        self, self_unique_address: UniqueAddress
    ) -> frozenset[UniqueAddress]:
        # Pekko keeps entries whose ref path has global scope; on a clustered Pekko system a local
        # ref's path carries the node's address, so it counts as self. ohra's local paths carry
        # none, and Entry.unique_address maps them to self, so no filter is needed.
        return frozenset(e.unique_address(self_unique_address.address) for e in self.all_entries())

    def collect_changed_keys(
        self, ddata_key: DDataKey, new_registry: ServiceRegistry
    ) -> set[ServiceKey[Any]]:
        return collect_changed_keys(self.registry_for(ddata_key), new_registry)

    def entries_per_ddata_key(
        self, entries: Mapping[ServiceKey[Any], frozenset[Entry] | set[Entry]]
    ) -> dict[DDataKey, dict[ServiceKey[Any], frozenset[Entry] | set[Entry]]]:
        acc: dict[DDataKey, dict[ServiceKey[Any], frozenset[Entry] | set[Entry]]] = {}
        for key, es in entries.items():
            acc.setdefault(self.ddata_key_for(key), {})[key] = es
        return acc

    def add_node(self, node: UniqueAddress) -> ShardedServiceRegistry:
        return replace(self, nodes=self.nodes | {node})

    def remove_node(self, node: UniqueAddress) -> ShardedServiceRegistry:
        return replace(self, nodes=self.nodes - {node}, unreachable=self.unreachable - {node})

    def add_unreachable(self, unique_address: UniqueAddress) -> ShardedServiceRegistry:
        return replace(self, unreachable=self.unreachable | {unique_address})

    def remove_unreachable(self, unique_address: UniqueAddress) -> ShardedServiceRegistry:
        return replace(self, unreachable=self.unreachable - {unique_address})
