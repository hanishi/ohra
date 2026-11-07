"""Pekko's Distributed Data: conflict-free replicated data types.

This release has the data types' building blocks: the ``ReplicatedData`` traits, ``Key``,
``VersionVector`` and the counters. The ``Replicator`` that gossips them between nodes comes in
a later release.
"""

from dataclasses import dataclass

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .fast_merge import FastMerge
from .flag import Flag, FlagKey
from .gcounter import GCounter, GCounterKey
from .gset import GSet, GSetKey
from .key import Key, UnspecificKey
from .lww_register import LWWRegister, LWWRegisterKey
from .pncounter import PNCounter, PNCounterKey
from .replicated_data import (
    DeltaReplicatedData,
    RemovedNodePruning,
    ReplicatedData,
    ReplicatedDelta,
    ReplicatedDeltaSize,
    RequiresCausalDeliveryOfDeltas,
)
from .version_vector import ManyVersionVector, OneVersionVector, VersionVector


@remotable
@dataclass(frozen=True)
class SelfUniqueAddress:
    """The address of the local node, so that only the local node's address can be passed
    where a value is updated on behalf of this node."""

    unique_address: UniqueAddress


__all__ = [
    "FastMerge",
    "Flag",
    "FlagKey",
    "GCounter",
    "GCounterKey",
    "GSet",
    "GSetKey",
    "DeltaReplicatedData",
    "Key",
    "LWWRegister",
    "LWWRegisterKey",
    "ManyVersionVector",
    "OneVersionVector",
    "PNCounter",
    "PNCounterKey",
    "RemovedNodePruning",
    "ReplicatedData",
    "ReplicatedDelta",
    "ReplicatedDeltaSize",
    "RequiresCausalDeliveryOfDeltas",
    "SelfUniqueAddress",
    "UnspecificKey",
    "VersionVector",
]
