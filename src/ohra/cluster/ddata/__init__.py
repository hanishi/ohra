"""Pekko's Distributed Data: conflict-free replicated data types.

This release has the data types' building blocks: the ``ReplicatedData`` traits, ``Key`` and
``VersionVector``. The ``Replicator`` that gossips them between nodes comes in a later release.
"""

from dataclasses import dataclass

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .key import Key, UnspecificKey
from .replicated_data import (
    DeltaReplicatedData,
    RemovedNodePruning,
    ReplicatedData,
    ReplicatedDelta,
    ReplicatedDeltaSize,
    RequiresCausalDeliveryOfDeltas,
)


@remotable
@dataclass(frozen=True)
class SelfUniqueAddress:
    """The address of the local node, so that only the local node's address can be passed
    where a value is updated on behalf of this node."""

    unique_address: UniqueAddress


from .version_vector import ManyVersionVector, OneVersionVector, VersionVector  # noqa: E402

__all__ = [
    "DeltaReplicatedData",
    "Key",
    "ManyVersionVector",
    "OneVersionVector",
    "RemovedNodePruning",
    "ReplicatedData",
    "ReplicatedDelta",
    "ReplicatedDeltaSize",
    "RequiresCausalDeliveryOfDeltas",
    "SelfUniqueAddress",
    "UnspecificKey",
    "VersionVector",
]
