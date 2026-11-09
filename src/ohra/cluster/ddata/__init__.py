"""Pekko's Distributed Data: conflict-free replicated data types.

The data types and their building blocks: the ``ReplicatedData`` traits, ``Key``,
``VersionVector``, the counters ``GCounter`` and ``PNCounter``, ``GSet``, ``ORSet``, ``ORMap``,
``ORMultiMap``, ``LWWMap``, ``PNCounterMap``, ``Flag`` and ``LWWRegister`` with its clocks. The
``Replicator`` holds them on each node, reached through the ``DistributedData`` extension.
Reads and writes reach other nodes at every consistency level; gossip between them is not here
yet.
"""

from dataclasses import dataclass

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .distributed_data import DistributedData, ReplicatorMessageAdapter
from .flag import Flag, FlagKey
from .gcounter import GCounter, GCounterKey
from .gset import GSet, GSetKey
from .key import Key, UnspecificKey
from .lww_map import LWWMap, LWWMapKey
from .lww_register import Clock, LWWRegister, LWWRegisterKey, default_clock, reverse_clock
from .ormap import ORMap, ORMapKey
from .ormultimap import ORMultiMap, ORMultiMapKey
from .orset import ORSet, ORSetKey
from .pncounter import PNCounter, PNCounterKey
from .pncounter_map import PNCounterMap, PNCounterMapKey
from .replicated_data import (
    DeltaReplicatedData,
    RemovedNodePruning,
    ReplicatedData,
    ReplicatedDelta,
    ReplicatedDeltaSize,
    RequiresCausalDeliveryOfDeltas,
)
from .replicator import Replicator
from .settings import ReplicatorSettings
from .version_vector import ManyVersionVector, OneVersionVector, VersionVector


@remotable
@dataclass(frozen=True)
class SelfUniqueAddress:
    """The address of the local node, so that only the local node's address can be passed
    where a value is updated on behalf of this node."""

    unique_address: UniqueAddress


__all__ = [
    "Clock",
    "Flag",
    "FlagKey",
    "GCounter",
    "GCounterKey",
    "GSet",
    "GSetKey",
    "DeltaReplicatedData",
    "DistributedData",
    "Key",
    "LWWMap",
    "LWWMapKey",
    "LWWRegister",
    "LWWRegisterKey",
    "ManyVersionVector",
    "OneVersionVector",
    "ORMap",
    "ORMapKey",
    "ORMultiMap",
    "ORMultiMapKey",
    "ORSet",
    "ORSetKey",
    "PNCounter",
    "PNCounterKey",
    "PNCounterMap",
    "PNCounterMapKey",
    "RemovedNodePruning",
    "ReplicatedData",
    "ReplicatedDelta",
    "ReplicatedDeltaSize",
    "Replicator",
    "ReplicatorMessageAdapter",
    "ReplicatorSettings",
    "RequiresCausalDeliveryOfDeltas",
    "SelfUniqueAddress",
    "UnspecificKey",
    "VersionVector",
    "default_clock",
    "reverse_clock",
]
