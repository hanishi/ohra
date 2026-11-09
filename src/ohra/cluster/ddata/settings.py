"""Pekko's ReplicatorSettings with the defaults of ``pekko.cluster.distributed-data``, in seconds,
minus roles, durable keys and pruning (which come with the features that use them)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ...system import ActorSystem


@dataclass(frozen=True)
class ReplicatorSettings:
    """How the Replicator runs. Replication between nodes is not here yet: every consistency
    level, ``WriteMajority`` included, runs on the local node."""

    name: str = "ddataReplicator"
    """The name of the Replicator actor, under /system."""
    gossip_interval: float = 2.0
    notify_subscribers_interval: float = 0.5
    max_delta_elements: int = 500
    log_data_size_exceeding: int | None = 10 * 1024
    """Log a message for data larger than this many bytes; None disables it."""
    prefer_oldest: bool = False
    """Gossip to the oldest nodes first."""
    expire_keys_after_inactivity: Mapping[str, float] = field(default_factory=dict[str, float])
    """Key (or ``prefix*`` pattern) to the seconds of inactivity after which it expires; 0 never
    expires."""

    def __post_init__(self) -> None:
        for name in ("gossip_interval", "notify_subscribers_interval", "max_delta_elements"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if self.log_data_size_exceeding is not None and self.log_data_size_exceeding <= 0:
            raise ValueError("log_data_size_exceeding must be > 0, or None to disable it")
        for key, seconds in self.expire_keys_after_inactivity.items():
            if seconds < 0:
                raise ValueError(f"expire_keys_after_inactivity[{key!r}] must be >= 0")

    @staticmethod
    def name_for(system: ActorSystem[Any], prefix: str | None) -> str:
        """Pekko's ReplicatorSettings.name: the name in the system's cluster settings, or
        ``"ddataReplicator"`` without a cluster. With a prefix, the name follows it with its first
        letter capitalized."""
        cluster = system.cluster
        name = "ddataReplicator" if cluster is None else cluster.settings.distributed_data.name
        return name if prefix is None else prefix + name[:1].upper() + name[1:]
