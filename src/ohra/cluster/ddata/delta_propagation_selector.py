"""Pekko's DeltaPropagationSelector.scala. INTERNAL API: used by the Replicator actor. Extracted
to a class of its own to make it easy to test. Pekko's abstract members are constructor
parameters here."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, cast

from ...remote import UniqueAddress
from .envelope import NO_DELTA_PLACEHOLDER, DeltaPropagation
from .replicated_data import ReplicatedData, ReplicatedDeltaSize


class DeltaPropagationSelector:
    def __init__(
        self,
        *,
        gossip_interval_divisor: int,
        all_nodes: Callable[[], Sequence[UniqueAddress]],
        max_delta_size: int,
        create_delta_propagation: Callable[
            [dict[str, tuple[ReplicatedData, int, int]]], DeltaPropagation
        ],
    ) -> None:
        self._gossip_interval_divisor = gossip_interval_divisor
        self._all_nodes = all_nodes
        self._max_delta_size = max_delta_size
        self._create_delta_propagation = create_delta_propagation
        self._propagation_count = 0
        self._delta_counter: dict[str, int] = {}
        # per key, version to delta, in version order (versions only increase)
        self._delta_entries: dict[str, dict[int, ReplicatedData]] = {}
        self._delta_sent_to_node: dict[str, dict[UniqueAddress, int]] = {}
        self._delta_node_round_robin_counter = 0

    @property
    def propagation_count(self) -> int:
        return self._propagation_count

    def current_version(self, key: str) -> int:
        return self._delta_counter.get(key, 0)

    def update(self, key: str, delta: ReplicatedData) -> None:
        # bump the counter for each update
        version = self._delta_counter.get(key, 0) + 1
        self._delta_counter[key] = version
        self._delta_entries.setdefault(key, {})[version] = delta

    def delete(self, key: str) -> None:
        self._delta_entries.pop(key, None)
        self._delta_counter.pop(key, None)
        self._delta_sent_to_node.pop(key, None)

    def nodes_slice_size(self, all_nodes_size: int) -> int:
        # 2 - 10 nodes
        return min(
            max(all_nodes_size // self._gossip_interval_divisor + 1, 2), min(all_nodes_size, 10)
        )

    def collect_propagations(self) -> dict[UniqueAddress, DeltaPropagation]:
        self._propagation_count += 1
        all_nodes = list(self._all_nodes())
        if not all_nodes:
            return {}
        # For each tick we pick a few nodes in round-robin fashion, 2 - 10 nodes for each tick.
        # Normally the delta is propagated to all nodes within the gossip tick, so that
        # full state gossip is not needed.
        slice_size = self.nodes_slice_size(len(all_nodes))
        if len(all_nodes) <= slice_size:
            node_slice = all_nodes
        else:
            i = self._delta_node_round_robin_counter % len(all_nodes)
            first = all_nodes[i : i + slice_size]
            node_slice = (
                first if len(first) == slice_size else first + all_nodes[: slice_size - len(first)]
            )
        self._delta_node_round_robin_counter += slice_size

        result: dict[UniqueAddress, DeltaPropagation] = {}
        cache: dict[tuple[str, int, int], ReplicatedData] = {}
        for node in node_slice:
            # collect the deltas that have not already been sent to the node and merge
            # them into a delta group
            deltas: dict[str, tuple[ReplicatedData, int, int]] = {}
            for key, entries in self._delta_entries.items():
                sent_for_key = self._delta_sent_to_node.get(key, {})
                j = sent_for_key.get(node, 0)
                after_j = self._delta_entries_after(entries, j)
                if after_j:
                    versions = list(after_j)
                    from_seq_nr = versions[0]
                    to_seq_nr = versions[-1]
                    # in most cases the delta group merging will be the same for each node,
                    # so we cache the merged results
                    cache_key = (key, from_seq_nr, to_seq_nr)
                    group = cache.get(cache_key)
                    if group is None:
                        group = self._merge_group(list(after_j.values()))
                        cache[cache_key] = group
                    deltas[key] = (group, from_seq_nr, to_seq_nr)
                    self._delta_sent_to_node[key] = {**sent_for_key, node: to_seq_nr}

            if deltas:
                # Important to include the pruning state in the deltas. For example if the delta
                # is based on an entry that has been pruned but that has not yet been performed
                # on the target node.
                result[node] = self._create_delta_propagation(deltas)
        return result

    def _merge_group(self, group: list[ReplicatedData]) -> ReplicatedData:
        acc: ReplicatedData = group[0]
        for d2 in group[1:]:
            merged: ReplicatedData
            if d2 is NO_DELTA_PLACEHOLDER:
                merged = NO_DELTA_PLACEHOLDER
            else:
                # this is fine also if acc is a NoDeltaPlaceholder
                merged = cast(ReplicatedData, cast(Any, acc).merge(d2))
            if (
                isinstance(merged, ReplicatedDeltaSize)
                and merged.delta_size >= self._max_delta_size
            ):
                # discard too large deltas
                merged = NO_DELTA_PLACEHOLDER
            acc = merged
        return acc

    @staticmethod
    def _delta_entries_after(
        entries: dict[int, ReplicatedData], version: int
    ) -> dict[int, ReplicatedData]:
        # exclude version j, that was already sent
        return {v: d for v, d in entries.items() if v > version}

    def has_delta_entries(self, key: str) -> bool:
        return bool(self._delta_entries.get(key))

    def _find_smallest_version_propagated_to_all_nodes(
        self, key: str, all_nodes: Sequence[UniqueAddress]
    ) -> int:
        sent_for_key = self._delta_sent_to_node.get(key)
        if not sent_for_key:
            return 0
        if any(node not in sent_for_key for node in all_nodes):
            return 0
        return min(sent_for_key.values())

    def cleanup_delta_entries(self) -> None:
        all_nodes = self._all_nodes()
        if not all_nodes:
            self._delta_entries = {}
        else:
            # TODO perhaps also remove oldest when deltaCounter is too far ahead (e.g. 10 cycles)
            self._delta_entries = {
                key: self._delta_entries_after(
                    entries, self._find_smallest_version_propagated_to_all_nodes(key, all_nodes)
                )
                for key, entries in self._delta_entries.items()
            }

    def cleanup_removed_node(self, address: UniqueAddress) -> None:
        self._delta_sent_to_node = {
            key: {n: v for n, v in sent.items() if n != address}
            for key, sent in self._delta_sent_to_node.items()
        }
