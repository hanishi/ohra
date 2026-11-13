"""Pekko's ReplicatorSettings with the defaults of ``pekko.cluster.distributed-data``, in
seconds."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from ...behavior import Behavior
from .durable_store import DurableStore, sqlite_durable_store

if TYPE_CHECKING:
    from ...system import ActorSystem


def is_wildcard(key_id: str) -> bool:
    return key_id.endswith("*")


def drop_wildcard(key_id: str) -> str:
    return key_id[:-1]


@dataclass(frozen=True)
class ReplicatorSettings:
    """How the Replicator runs."""

    name: str = "ddataReplicator"
    """The name of the Replicator actor, under /system."""
    roles: frozenset[str] = frozenset()
    """Only members with every one of these roles are replicas; none: every member is."""
    gossip_interval: float = 2.0
    notify_subscribers_interval: float = 0.5
    max_delta_elements: int = 500
    delta_crdt_enabled: bool = True
    """Propagate the deltas of delta-CRDTs instead of their full state."""
    max_delta_size: int = 50
    """The number of operations a merged ORSet or ORMap delta may hold before it is replaced by
    the placeholder and the full state is sent instead; adds in a row merge into one operation.
    Pekko's "number of elements or similar size hint, not size in bytes"."""
    log_data_size_exceeding: int | None = 10 * 1024
    """Log a message for data larger than this many bytes; None disables it."""
    prefer_oldest: bool = False
    """Read from and write to the oldest nodes first; gossip still picks its node at random."""
    expire_keys_after_inactivity: Mapping[str, float] = field(default_factory=dict[str, float])
    """Key (or ``prefix*`` pattern) to the seconds of inactivity after which it expires; 0 never
    expires."""
    pruning_interval: float = 120.0
    """How often the Replicator checks for data of removed nodes to prune; 0 or less turns
    pruning off, as Pekko's ``off``."""
    max_pruning_dissemination: float = 300.0
    """How long a removed node's updates are given to reach every node before its data is
    pruned. The clock runs only while every node is reachable."""
    pruning_marker_time_to_live: float = 6 * 3600.0
    """How long a performed pruning marker is kept, so that a stale write cannot bring the
    removed node's data back."""
    durable_keys: frozenset[str] = frozenset()
    """Keys (or ``prefix*`` patterns, ``"*"`` for all) whose data is kept in the durable store
    and loaded back when the Replicator starts."""
    durable_pruning_marker_time_to_live: float = 10 * 86400.0
    """How long a performed pruning marker of a durable key is kept, as
    ``pruning_marker_time_to_live`` for the others."""
    durable_store: Callable[[], Behavior[DurableStore.Command]] = sqlite_durable_store
    """The durable store's behavior, spawned as the Replicator's child when there are durable
    keys; Pekko's ``durable.store-actor-class``."""

    def __post_init__(self) -> None:
        if isinstance(self.roles, str):
            raise TypeError("roles must be a frozenset of str, not a str")
        object.__setattr__(self, "roles", frozenset(self.roles))
        for name in (
            "gossip_interval",
            "notify_subscribers_interval",
            "max_delta_elements",
            "max_delta_size",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if self.log_data_size_exceeding is not None and self.log_data_size_exceeding <= 0:
            raise ValueError("log_data_size_exceeding must be > 0, or None to disable it")
        for key, seconds in self.expire_keys_after_inactivity.items():
            if seconds < 0:
                raise ValueError(f"expire_keys_after_inactivity[{key!r}] must be >= 0")
        for name in (
            "max_pruning_dissemination",
            "pruning_marker_time_to_live",
            "durable_pruning_marker_time_to_live",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be >= 0")

    def with_role(self, role: str | None) -> ReplicatorSettings:
        """Pekko's withRole: None or ``""`` means no role."""
        return replace(self, roles=frozenset({role}) if role else frozenset())

    def with_roles(self, *roles: str) -> ReplicatorSettings:
        return replace(self, roles=frozenset(roles))

    @property
    def role(self) -> str | None:
        """Pekko's ``roles.headOption``: any one of the roles."""
        return next(iter(self.roles), None)

    def is_durable(self, key: str) -> bool:
        """Pekko's isDurable: an exact key, or a ``prefix*`` entry the key starts with."""
        return key in self.durable_keys or any(
            key.startswith(drop_wildcard(k)) for k in self.durable_keys if is_wildcard(k)
        )

    @staticmethod
    def name_for(system: ActorSystem[Any], prefix: str | None) -> str:
        """Pekko's ReplicatorSettings.name: the name in the system's cluster settings, or
        ``"ddataReplicator"`` without a cluster. With a prefix, the name follows it with its first
        letter capitalized."""
        cluster = system.cluster
        name = "ddataReplicator" if cluster is None else cluster.settings.distributed_data.name
        return name if prefix is None else prefix + name[:1].upper() + name[1:]
