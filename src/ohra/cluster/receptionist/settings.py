"""ClusterReceptionistSettings, after Pekko's ClusterReceptionistSettings.scala and the
``pekko.cluster.typed.receptionist`` block of cluster-typed's reference.conf, in seconds."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from ..ddata.replicator import Replicator
from ..ddata.settings import ReplicatorSettings

if TYPE_CHECKING:
    from ..core import ClusterSettings


@dataclass(frozen=True)
class ClusterReceptionistSettings:
    write_consistency: Replicator.WriteConsistency = field(default_factory=Replicator.WriteLocal)
    """The consistency of the Replicator updates the receptionist makes."""
    pruning_interval: float = 3.0
    """How often the receptionist looks for entries of removed nodes to prune."""
    prune_removed_older_than: float = 60.0
    """How long a removed node's entries are kept before they are pruned, so a late reader
    still sees them."""
    distributed_key_count: int = 5
    """The number of ddata keys the services are spread over; all nodes must agree."""
    replicator_settings: ReplicatorSettings | None = None
    """The receptionist's Replicator; None takes the cluster's distributed_data."""

    def __post_init__(self) -> None:
        if self.pruning_interval <= 0:
            raise ValueError("pruning_interval must be > 0")
        if self.prune_removed_older_than < 0:
            raise ValueError("prune_removed_older_than must be >= 0")
        if self.distributed_key_count < 1:
            raise ValueError("distributed_key_count must be >= 1")

    def effective_replicator_settings(
        self, cluster_settings: ClusterSettings
    ) -> ReplicatorSettings:
        """The receptionist's data is never durable and has no role (Pekko's
        ``withDurableKeys(Set.empty).withRole(None)``: cluster-typed reference.conf 30-34)."""
        base = self.replicator_settings or cluster_settings.distributed_data
        return replace(base, durable_keys=frozenset(), roles=frozenset())
