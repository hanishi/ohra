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
    name: str = "ddataReplicator"
    """The name of the Replicator actor, under /system."""
    gossip_interval: float = 2.0
    notify_subscribers_interval: float = 0.5
    max_delta_elements: int = 500
    log_data_size_exceeding: int | None = 10 * 1024
    """Log a message for data larger than this many bytes; None disables it."""
    prefer_oldest: bool = False
    """Gossip to the oldest nodes first."""
    expire_keys_after_inactivity: Mapping[str, float] = field(default_factory=lambda: {})
    """Key (or ``prefix*`` pattern) to the seconds of inactivity after which it expires."""

    def __post_init__(self) -> None:
        for name in ("gossip_interval", "notify_subscribers_interval", "max_delta_elements"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")

    @staticmethod
    def name_for(system: ActorSystem[Any], prefix: str | None) -> str:
        """Pekko's ReplicatorSettings.name: the default name, or one with the prefix."""
        return "ddataReplicator" if prefix is None else prefix + "DdataReplicator"
