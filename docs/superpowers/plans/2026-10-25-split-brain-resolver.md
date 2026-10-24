# Split-Brain Resolver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port Apache Pekko's SplitBrainResolver so unreachable members are downed by a strategy after the cluster has been stable for `stable_after`, with the removal margin wired into the singleton and the sharding coordinator.

**Architecture:** A new `pekko_py/cluster/sbr.py` holds the settings and strategy dataclasses, the stateful `_DowningStrategy` port, and the resolver as a state class (`_Resolver`) driven by cluster events and ticks, wrapped in a behavior the core daemon spawns as its child. The core gains two internal events (`SeenChanged`, `ReachabilityChanged`); `Reachability` gains `filter_records`; the singleton and coordinator read `ClusterSettings.effective_down_removal_margin`.

**Tech Stack:** Python 3.12, asyncio, pytest, pyright strict. Pekko sources: `/Users/hanishi/pekko/cluster/src/main/scala/org/apache/pekko/cluster/sbr/{SplitBrainResolver,DowningStrategy,SplitBrainResolverProvider}.scala`, test scenarios in `cluster/src/test/scala/org/apache/pekko/cluster/sbr/SplitBrainResolverSpec.scala`.

**Spec:** `docs/superpowers/specs/2026-10-25-split-brain-resolver-design.md`

## Global Constraints

- Frozen dataclasses for settings, strategies, decisions and messages; `match` for dispatch; Python 3.12 generics.
- Public names follow Pekko in snake_case; every narrowing goes in the README deviations.
- `pytest` green under the conftest leak check; `pyright` strict 0 errors; multi-node tests use `FAST` settings and `free_port()`.
- No roles, data centers, lease or quarantine anywhere in the port.
- `downing=None` must leave every existing test and example unchanged.

## Review Focus

1. A node that is the only member and loses nothing must never down itself: `tick()` with an empty unreachable set is a no-op even when responsible and overdue (Task 4 test `test_tick_with_no_unreachable_does_nothing`).
2. A partition where the minority holds the leader: the minority's resolver downs its own side and the node shuts itself down; the majority's new leader downs the minority. Both sides must act, neither twice (Task 4 multi-node partition test checks the isolated node terminates and the majority removes it).
3. A member that becomes Unreachable and then Down by someone else before `stable_after`: `MemberDowned` keeps it in the unreachable set, `is_all_unreachable_down_or_exiting` resets the stats, and the decision still runs but `nodes_to_down` excludes Down members (Task 1 test `test_nodes_to_down_skips_down_and_exiting`, Task 4 test `test_member_downed_elsewhere_is_not_downed_again`).
4. `down_all_when_unstable` must fire on every node, not only the leader, and only once per instability episode: after acting the stats reset (Task 4 test `test_down_all_when_unstable_fires_on_non_leader_once`).
5. The resolver stops when self is removed and its stop must not take the daemon down with it in the normal exit path, while a crash must (Task 4 tests `test_resolver_stops_on_self_removed` and `test_resolver_failure_fails_the_daemon`).

---

### Task 1: Settings, strategy values, and the `_DowningStrategy` base

**Files:**
- Create: `src/pekko_py/cluster/sbr.py`
- Modify: `src/pekko_py/cluster/core.py:36-100` (`ClusterSettings`), `src/pekko_py/cluster/reachability.py:150-158`
- Test: `tests/cluster/test_sbr.py`

**Interfaces:**
- Consumes: `Member`, `UP_NUMBER_UNSET`, `MemberStatus` from `cluster/gossip.py`; `Reachability`, `Record` from `cluster/reachability.py`; `UniqueAddress`, `Address` from `remote`.
- Produces:
  - `KeepMajority()`, `StaticQuorum(quorum_size: int)`, `KeepOldest(down_if_alone: bool = True)`, `DownAll()`; `type DowningStrategy = KeepMajority | StaticQuorum | KeepOldest | DownAll`.
  - `SplitBrainResolverSettings(strategy: DowningStrategy = KeepMajority(), stable_after: float = 20.0, down_all_when_unstable: float | None = None)` with property `effective_down_all_when_unstable -> float` (`None` -> `max(4.0, 0.75 * stable_after)`; else the value) and `tick_interval -> float` (`min(1.0, stable_after / 4)`).
  - `ClusterSettings.downing: SplitBrainResolverSettings | None = None`, `ClusterSettings.down_removal_margin: float = 0.0`, property `effective_down_removal_margin -> float`.
  - Decisions: `DownUnreachable`, `DownReachable`, `DownAllDecision`, `DownIndirectlyConnected` (frozen dataclasses with no fields; `type Decision = ...`). The strategy value `DownAll` and the decision `DownAllDecision` are distinct classes.
  - `class _DoubleDecision(Exception)`.
  - `class _DowningStrategy(self_unique_address: UniqueAddress, config: DowningStrategy)` with: `unreachable: frozenset[UniqueAddress]`, `is_unreachable(m: Member) -> bool`, `add(m)`, `remove(m)`, `add_unreachable(m)`, `add_reachable(m)`, `set_reachability(r: Reachability)`, `reachability: Reachability`, `set_seen_by(s: frozenset[Address])`, `seen_by`, `all_members: list[Member]` (sorted; age order when `config` is `KeepOldest`, else address order), `joining -> list[Member]`, `members(including_possibly_up=False, excluding_possibly_exiting=False) -> list[Member]`, `reachable_members(...)`, `unreachable_members(...)`, `indirectly_connected -> frozenset[UniqueAddress]`, `has_indirectly_connected -> bool`, `unreachable_but_not_indirectly_connected`, `is_all_unreachable_down_or_exiting -> bool`, `decide() -> Decision` (Task 2), `nodes_to_down(decision: Decision | None = None) -> frozenset[UniqueAddress]`.
  - `Reachability.filter_records(pred: Callable[[Record], bool]) -> Reachability` (keeps versions).

- [ ] **Step 1: Write the failing settings and member-set tests**

```python
# tests/cluster/test_sbr.py
from pekko_py.cluster.core import ClusterSettings
from pekko_py.cluster.gossip import UP_NUMBER_UNSET, Member
from pekko_py.cluster.reachability import Reachability, Record
from pekko_py.cluster.sbr import (DownAll, DownAllDecision, DownIndirectlyConnected, DownReachable, DownUnreachable,
    KeepMajority, KeepOldest, SplitBrainResolverSettings, StaticQuorum, _DoubleDecision, _DowningStrategy)
from pekko_py.remote import UniqueAddress

def member(host: str, up_number: int) -> Member:
    return Member(UniqueAddress(host, 7355, 0, "sys"), up_number, "up")

A, B, C, D, E, F, G = (member(h, n) for h, n in zip("abcdefg", (5, 4, 3, 2, 1, 5, 6)))  # TestAddresses.scala: E is the oldest
def joining(m: Member) -> Member: return Member(m.unique_address, UP_NUMBER_UNSET, "joining")
def leaving(m: Member) -> Member: return m.with_status("leaving")
def exiting(m: Member) -> Member: return m.with_status("exiting")
def downed(m: Member) -> Member: return m.with_status("down")
def nodes(*ms: Member) -> frozenset[UniqueAddress]: return frozenset(m.unique_address for m in ms)

def reachability(pairs: list[tuple[Member, Member]]) -> Reachability:
    return Reachability(tuple(Record(f.unique_address, t.unique_address, "unreachable", 1) for f, t in pairs),
                        tuple((f.unique_address, 1) for f, _ in pairs))

def strategy(config, side: set[Member], *others: set[Member], indirectly: list[tuple[Member, Member]] = []) -> _DowningStrategy:
    """SplitBrainResolverSpec.StrategySetup.strategy: this side sees every other side as unreachable."""
    s = _DowningStrategy(A.unique_address, config)
    for m in side.union(*others): s.add(m)
    other = set().union(*others)
    head = min(side)
    pairs = list(dict.fromkeys(indirectly + [(head, o) for o in other]))
    s.set_reachability(reachability(pairs))
    for _, to in pairs: s.add_unreachable(to)
    s.set_seen_by(frozenset(m.address for m in side))
    return s

def test_settings_defaults_and_validation():
    s = SplitBrainResolverSettings()
    assert s.strategy == KeepMajority() and s.stable_after == 20.0 and s.down_all_when_unstable is None
    assert s.effective_down_all_when_unstable == 15.0 and s.tick_interval == 1.0
    assert SplitBrainResolverSettings(stable_after=2.0).effective_down_all_when_unstable == 4.0
    assert SplitBrainResolverSettings(stable_after=0.1).tick_interval == 0.025
    assert SplitBrainResolverSettings(down_all_when_unstable=0).effective_down_all_when_unstable == 0
    with pytest.raises(ValueError): SplitBrainResolverSettings(stable_after=0)
    with pytest.raises(ValueError): SplitBrainResolverSettings(down_all_when_unstable=-1)
    with pytest.raises(ValueError): StaticQuorum(0)

def test_effective_down_removal_margin():
    assert ClusterSettings().effective_down_removal_margin == 0.0
    assert ClusterSettings(downing=SplitBrainResolverSettings(stable_after=7.0)).effective_down_removal_margin == 7.0
    assert ClusterSettings(downing=SplitBrainResolverSettings(stable_after=7.0), down_removal_margin=3.0).effective_down_removal_margin == 3.0
    with pytest.raises(ValueError): ClusterSettings(down_removal_margin=-1)

def test_add_and_remove_members_with_address_order():  # "add and remove members with default Member ordering"
    s = _DowningStrategy(A.unique_address, KeepMajority())
    for m in (C, A, B): s.add(m)
    assert s.all_members == [A, B, C]
    s.add(joining(D)); assert s.all_members == [A, B, C, joining(D)]
    s.add(D); assert s.all_members == [A, B, C, D] and s.all_members[3].status == "up"
    s.remove(B); assert s.all_members == [A, C, D]

def test_add_and_remove_members_with_age_order():  # "... with oldest Member ordering"
    s = _DowningStrategy(A.unique_address, KeepOldest())
    for m in (A, C, E, joining(B)): s.add(m)
    assert s.all_members == [E, C, A, joining(B)]
    s.add(B); assert s.all_members == [E, C, B, A]  # up numbers E=1, C=3, B=4, A=5

def test_collect_and_filter_members():  # "collect and filter members with default Member ordering"
    s = _DowningStrategy(A.unique_address, KeepMajority())
    for m in (A, joining(B), C, leaving(D), exiting(E), downed(F)): s.add(m)
    s.add_unreachable(C)
    assert s.members() == [A, leaving(D)]
    assert s.members(including_possibly_up=True) == [A, joining(B), leaving(D)]
    assert s.members(excluding_possibly_exiting=True) == [A]
    assert s.joining == [joining(B)]
    assert s.reachable_members() == [A, leaving(D)] and s.unreachable_members() == []
    s.add_unreachable(A)
    assert s.reachable_members() == [leaving(D)] and s.unreachable_members() == [A]
    assert s.unreachable == nodes(A, C) and s.is_unreachable(A)
    s.add_reachable(A); assert s.unreachable == nodes(C)
    s.remove(C); assert s.unreachable == frozenset()

def test_set_reachability_keeps_only_unreachable_records_between_members():
    s = _DowningStrategy(A.unique_address, KeepMajority())
    for m in (A, B): s.add(m)
    r = Reachability((Record(A.unique_address, B.unique_address, "unreachable", 1), Record(A.unique_address, C.unique_address, "unreachable", 2),
                      Record(B.unique_address, A.unique_address, "reachable", 1)), ((A.unique_address, 2), (B.unique_address, 1)))
    s.set_reachability(r)
    assert [(x.observer, x.subject) for x in s.reachability.records] == [(A.unique_address, B.unique_address)]

def test_is_all_unreachable_down_or_exiting():
    s = _DowningStrategy(A.unique_address, KeepMajority())
    for m in (A, B, downed(C)): s.add(m)
    assert s.is_all_unreachable_down_or_exiting
    s.add_unreachable(downed(C)); assert s.is_all_unreachable_down_or_exiting
    s.add_unreachable(B); assert not s.is_all_unreachable_down_or_exiting

def test_indirectly_connected_from_cycle_and_from_seen_by():
    s = strategy(KeepMajority(), {A, B, C}, indirectly=[(A, B), (B, A)])
    assert s.indirectly_connected == nodes(A, B) and s.has_indirectly_connected
    s = strategy(KeepMajority(), {A, B, C}, {D})           # D unreachable, has not seen the gossip
    assert s.indirectly_connected == frozenset()
    s.set_seen_by(frozenset(m.address for m in (A, B, C, D)))  # D has seen the current gossip: indirectly connected
    assert s.indirectly_connected == nodes(A, D) and s.unreachable_but_not_indirectly_connected == frozenset()

def test_nodes_to_down_skips_down_and_exiting_and_includes_joining():
    s = strategy(KeepMajority(), {A, joining(B), exiting(C), downed(D)}, {E})
    assert s.nodes_to_down(DownAllDecision()) == nodes(A, B, E)
    assert s.nodes_to_down(DownUnreachable()) == nodes(E)
    assert s.nodes_to_down(DownReachable()) == nodes(A, B)

def test_filter_records_keeps_versions():
    r = reachability([(A, B), (B, C)])
    kept = r.filter_records(lambda x: x.observer == A.unique_address)
    assert len(kept.records) == 1 and kept.versions == r.versions
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/cluster/test_sbr.py -q 2>&1 | tail -3`
Expected: FAIL at import, `No module named 'pekko_py.cluster.sbr'`.

- [ ] **Step 3: Implement the settings, the values and the base in `src/pekko_py/cluster/sbr.py`; add the two fields and the property to `ClusterSettings`; add `Reachability.filter_records`**

Port `DowningStrategy.scala` lines 70-330 minus roles and DC. `all_members` is a plain list kept sorted on every `add`/`remove` (replace a member with the same unique address, then insert by `Member.__lt__` for address order or `Member.is_older_than` for `KeepOldest`). `set_reachability` keeps records with status `unreachable` or `terminated` whose observer and subject are in `all_members`. `indirectly_connected` is `all_observers & all_unreachable_or_terminated` union the observer and subject of every record whose subject's address is in `seen_by`. `nodes_to_down` follows the Scala `match` exactly, with `additional_nodes_to_down_when_indirectly_connected` swapping `_reachability` and `_unreachable` in a `try/finally` and raising `_DoubleDecision` when the re-decision `is_indirectly_connected`. `decide()` raises `NotImplementedError` until Task 2. `ClusterSettings.__post_init__` adds `down_removal_margin` to `non_negative`. The `cluster/sbr.py` module imports from `.core` only `ClusterSettings` under `TYPE_CHECKING` if at all; `core.py` imports `SplitBrainResolverSettings` from `.sbr` for the field type, so `sbr.py` must not import `core.py` at module level (the resolver in Task 4 gets what it needs through constructor arguments).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/cluster/test_sbr.py -q 2>&1 | tail -3 && pyright | tail -1`
Expected: all PASS, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/cluster/sbr.py src/pekko_py/cluster/core.py src/pekko_py/cluster/reachability.py tests/cluster/test_sbr.py
git commit -m "SBR: settings, strategy values and the DowningStrategy base, as Apache Pekko's"
```

---

### Task 2: The four strategies' `decide()`

**Files:**
- Modify: `src/pekko_py/cluster/sbr.py`
- Test: `tests/cluster/test_sbr.py`

**Interfaces:**
- Consumes: Task 1's `strategy(...)` test fixture and `_DowningStrategy`.
- Produces: `_DowningStrategy.decide() -> Decision` dispatching on `self.config`; `_DowningStrategy.is_too_many_members -> bool` (StaticQuorum only).

- [ ] **Step 1: Write the failing scenario tests, Pekko's `SplitBrainResolverSpec` names in comments**

```python
def downing(config, members, *sides, indirectly=[]):
    """assertDowning: every non-empty side reaches the same nodes_to_down."""
    for side in sides:
        if side:
            assert strategy(config, side, *(o for o in sides if o is not side), indirectly=indirectly).nodes_to_down() == nodes(*members)

# StaticQuorum
def test_static_quorum_downs_unreachable_when_enough_reachable():
    downing(StaticQuorum(3), {B, D}, {A, C, E}, {B, D})
def test_static_quorum_downs_reachable_when_not_enough():
    assert strategy(StaticQuorum(3), {A, B}, {C, D}).decide() == DownReachable()
    assert strategy(StaticQuorum(3), {C, D}, {A, B}).decide() == DownReachable()
def test_static_quorum_downs_all_when_too_many_members():
    downing(StaticQuorum(3), {A, B, C, D, E, F}, {A, B, C}, {D, E, F})
def test_static_quorum_does_not_count_joining():  # "handle joining"
    assert strategy(StaticQuorum(3), {A, B, joining(C)}, {D, E}).decide() == DownReachable()
    assert strategy(StaticQuorum(3), {A, B, C}, {joining(D), E}).decide() == DownUnreachable()
def test_static_quorum_does_not_count_leaving_and_exiting():  # "handle leaving/exiting"
    assert strategy(StaticQuorum(3), {A, B, leaving(C)}, {D, E}).decide() == DownUnreachable()
    assert strategy(StaticQuorum(3), {A, B, exiting(C)}, {D, E}).decide() == DownReachable()

# KeepMajority
def test_keep_majority_downs_minority():          # {A, B, C} | {D, E} => {A, B, C}
    downing(KeepMajority(), {D, E}, {A, B, C}, {D, E})
def test_keep_majority_keeps_lowest_address_when_equal():  # {A, B} | {C, D} => {A, B}
    downing(KeepMajority(), {C, D}, {A, B}, {C, D})
def test_keep_majority_two_node_cluster():        # {A} | {B} => {A}
    downing(KeepMajority(), {B}, {A}, {B})
def test_keep_majority_does_not_count_joining_but_downs_it():  # {B, D} | {Aj, C} => {B, D}
    downing(KeepMajority(), {joining(A), C}, {B, D}, {joining(A), C})
def test_keep_majority_downs_each_part_when_split_in_three():
    s1, s2, s3 = {A, B}, {C, D}, {E}
    assert strategy(KeepMajority(), s1, s2, s3).nodes_to_down() == nodes(A, B)
    assert strategy(KeepMajority(), s2, s1, s3).nodes_to_down() == nodes(C, D)
    assert strategy(KeepMajority(), s3, s1, s2).nodes_to_down() == nodes(E)
def test_keep_majority_membership_change_edge_case():  # {A, B, F', G'} | {C, D, E} => {A, B, F, G}
    s1 = strategy(KeepMajority(), {A, B, F, G}, {C, D, E})
    assert s1.decide() == DownUnreachable() and s1.nodes_to_down() == nodes(C, D, E)
    s2 = strategy(KeepMajority(), {C, D, E}, {A, B, joining(F), joining(G)})
    assert s2.decide() == DownAllDecision() and s2.nodes_to_down() == nodes(A, B, C, D, E, F, G)
def test_keep_majority_membership_change_edge_case_equal_size():  # {A, B, F'} | {C, D, E}
    assert strategy(KeepMajority(), {A, B, F}, {C, D, E}).decide() == DownUnreachable()
    assert strategy(KeepMajority(), {C, D, E}, {A, B, joining(F)}).decide() == DownAllDecision()
def test_keep_majority_safe_edge_case():  # {A, B} | {C, D, E, F'}
    assert strategy(KeepMajority(), {A, B}, {C, D, E, joining(F)}).decide() == DownReachable()
    assert strategy(KeepMajority(), {C, D, E, F}, {A, B}).decide() == DownUnreachable()
def test_keep_majority_leaving_edge_case():  # {A', B} | {C, D} => {C, D}
    s1 = strategy(KeepMajority(), {leaving(A), B, joining(E)}, {C, D})
    assert s1.decide() == DownAllDecision()
    s2 = strategy(KeepMajority(), {C, D}, {exiting(A), B})
    assert s2.decide() == DownUnreachable() and s2.nodes_to_down() == nodes(B)
def test_keep_majority_downs_indirectly_connected():  # {(A, B), C} => {C}
    downing(KeepMajority(), {A, B}, {A, B, C}, indirectly=[(A, B), (B, A)])
def test_keep_majority_indirectly_connected_with_clean_partition():  # {A, (B, C)} | {D, E} => {A}
    s1 = strategy(KeepMajority(), {A, B, C}, {D, E}, indirectly=[(B, C), (C, B)])
    assert s1.decide() == DownIndirectlyConnected() and s1.nodes_to_down() == nodes(B, C, D, E)
    s2 = strategy(KeepMajority(), {D, E}, {A, B, C})
    assert s2.nodes_to_down() == nodes(D, E)
def test_keep_majority_indirectly_connected_detected_before_partition():  # {A, B, C} | {(D, E), F, G}
    assert strategy(KeepMajority(), {A, B, C}, {D, E, F, G}).nodes_to_down() == nodes(A, B, C)
    assert strategy(KeepMajority(), {A, B, C}, {D, E, F, G}, indirectly=[(D, E), (E, D)]).nodes_to_down() == nodes(A, B, C, D, E)
    assert strategy(KeepMajority(), {D, E, F, G}, {A, B, C}, indirectly=[(D, E), (E, D)]).nodes_to_down() == nodes(A, B, C, D, E)
def test_keep_majority_double_indirectly_connected_raises():  # "double DownIndirectlyConnected ..."
    s1 = strategy(KeepMajority(), {A, B, C}, {D, E, F, G}, indirectly=[(D, E), (G, F)])
    with pytest.raises(_DoubleDecision):
        s1.nodes_to_down()
    assert strategy(KeepMajority(), {D, E, F, G}, {A, B, C}, indirectly=[(D, E), (G, F)]).nodes_to_down() == nodes(A, B, C, D, E, F, G)

# KeepOldest (E is the oldest)
def test_keep_oldest_keeps_partition_with_oldest():
    downing(KeepOldest(), {B, C, D}, {A, E}, {B, C, D})
def test_keep_oldest_unless_alone():
    downing(KeepOldest(), {E}, {E}, {A, B, C, D})
def test_keep_oldest_in_two_node_cluster():
    downing(KeepOldest(), {A}, {B}, {A})
def test_keep_oldest_even_when_alone_when_down_if_alone_false():
    downing(KeepOldest(down_if_alone=False), {A, B, C, D}, {E}, {A, B, C, D})
def test_keep_oldest_leaving_edge_cases():
    assert strategy(KeepOldest(), {leaving(D), E}, {A, B, C}).decide() == DownAllDecision()
    assert strategy(KeepOldest(), {A, B, C}, {leaving(D), E}).decide() == DownReachable()
    assert strategy(KeepOldest(), {E}, {A, B, leaving(C)}).decide() == DownReachable()
    assert strategy(KeepOldest(), {A, B, leaving(C)}, {E}).decide() == DownUnreachable()
    assert strategy(KeepOldest(), {E}, {A, leaving(B), leaving(C), leaving(D)}).decide() == DownReachable()
    assert strategy(KeepOldest(), {A, leaving(B), leaving(C), leaving(D)}, {E}).decide() == DownAllDecision()
def test_keep_oldest_exiting_alone_edge_case():
    assert strategy(KeepOldest(), {D, exiting(E)}, {A, B, C}).decide() == DownReachable()
    assert strategy(KeepOldest(), {A, B, C}, {D, leaving(E)}).decide() == DownReachable()
    assert strategy(KeepOldest(), {A}, {exiting(B)}).decide() == DownUnreachable()
    assert strategy(KeepOldest(), {leaving(B)}, {A}).decide() == DownAllDecision()
def test_keep_oldest_joining_edge_case():
    assert strategy(KeepOldest(), {joining(A), E}, {B, C, D}).decide() == DownReachable()
    assert strategy(KeepOldest(), {B, C, D}, {joining(A), E}).decide() == DownAllDecision()
    assert strategy(KeepOldest(down_if_alone=False), {joining(A), E}, {B, C, D}).decide() == DownUnreachable()
    assert strategy(KeepOldest(down_if_alone=False), {B, C, D}, {joining(A), E}).decide() == DownReachable()
def test_keep_oldest_downs_indirectly_connected():
    downing(KeepOldest(), {A, B}, {A, B, C}, indirectly=[(A, B), (B, A)])

# DownAll
def test_down_all_downs_everything():
    downing(DownAll(), {A, B, C, D}, {A, B}, {C, D})
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/cluster/test_sbr.py -q 2>&1 | tail -3`
Expected: the new tests FAIL with `NotImplementedError` from `decide()`.

- [ ] **Step 3: Implement `decide()` for the four strategies in `_DowningStrategy`**

Port `StaticQuorum.decide`, `KeepMajority.decide` with `majority_decision` and `majority_decision_when_including_membership_changes_edge_case`, `KeepOldest.decide` with `oldest_decision` and its edge case, `DownAllNodes.decide`, from `DowningStrategy.scala` lines 340-560. `KeepOldest` reads the oldest as `members(...)[0]` since `all_members` is in age order for it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/cluster/test_sbr.py -q 2>&1 | tail -3 && pyright | tail -1`
Expected: all PASS, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/cluster/sbr.py tests/cluster/test_sbr.py
git commit -m "SBR: KeepMajority, StaticQuorum, KeepOldest and DownAll decisions, with Pekko's spec scenarios"
```

---

### Task 3: `SeenChanged` and `ReachabilityChanged` events

**Files:**
- Modify: `src/pekko_py/cluster/core.py:160-175` (events), `:493-507` (`publish_diff`), `diff_*` functions near `:464-492`
- Test: `tests/cluster/test_cluster.py`

**Interfaces:**
- Consumes: `MembershipState`, `Gossip` from `gossip.py`.
- Produces: `SeenChanged(convergence: bool, seen_by: frozenset[Address])` and `ReachabilityChanged(reachability: Reachability)`, both `ClusterDomainEvent` subclasses, not exported from `pekko_py`; `diff_seen(old, new) -> list[SeenChanged]`; `diff_reachability(old, new) -> list[ReachabilityChanged]`; `publish_diff` publishes them last.

- [ ] **Step 1: Write the failing tests**

Find the existing `publish_diff` / `diff_*` tests in `tests/cluster/test_cluster.py` (grep `diff_member_events`) and add beside them:

```python
def test_publish_diff_emits_seen_and_reachability_changes_last():
    a, b = UniqueAddress("127.0.0.1", 1, 1, "c"), UniqueAddress("127.0.0.1", 2, 2, "c")
    g = Gossip.empty().with_member(Member(a, 1, "up")).with_member(Member(b, 2, "up"))
    old = MembershipState(g.seen(a), a)
    new = MembershipState(g.seen(a).seen(b).with_reachability(Reachability.empty().unreachable(a, b)), a)
    events: list[ClusterDomainEvent] = []
    publish_diff(old, new, events.append)
    assert isinstance(events[-2], SeenChanged) and events[-2].seen_by == frozenset({a.address, b.address})
    assert isinstance(events[-1], ReachabilityChanged) and events[-1].reachability.all_unreachable == frozenset({b})
    assert diff_seen(new, new) == [] and diff_reachability(new, new) == []
    assert diff_seen(old, MembershipState(g.seen(a), a)) == []  # same seen set, same convergence: nothing
```

`SeenChanged.convergence` is `new.convergence(frozenset())`; a change of convergence alone also emits `SeenChanged`, as Pekko's `diffSeen`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/cluster/test_cluster.py -q -k seen_and_reachability 2>&1 | tail -3`
Expected: FAIL, `ImportError: cannot import name 'SeenChanged'`.

- [ ] **Step 3: Implement the events and diffs, and append them to `publish_diff` after `diff_leader`**

- [ ] **Step 4: Run the whole suite**

Run: `pytest -q 2>&1 | tail -2 && pyright | tail -1`
Expected: all PASS (existing subscribers ignore the new events), 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/cluster/core.py tests/cluster/test_cluster.py
git commit -m "Cluster: SeenChanged and ReachabilityChanged events, as Pekko's diffSeen and diffReachability"
```

---

### Task 4: The resolver

**Files:**
- Modify: `src/pekko_py/cluster/sbr.py`, `src/pekko_py/cluster/core.py:700-715` (`become_initialized`), `:695-698` (`_on_signal`), `src/pekko_py/cluster/__init__.py`, `src/pekko_py/__init__.py`
- Test: `tests/cluster/test_sbr.py` (driver), `tests/cluster/test_sbr_cluster.py` (multi-node)

**Interfaces:**
- Consumes: Task 1's settings and `_DowningStrategy`; Task 3's events; `Down`, `Subscribe`, `ClusterDomainEvent` and the member/reachability events from `core.py`.
- Produces:
  - `class _Resolver(settings: SplitBrainResolverSettings, self_unique_address: UniqueAddress, down: Callable[[Address], None], clock: Callable[[], float], log: logging.Logger)` with `handle(event: ClusterDomainEvent) -> None`, `tick() -> None`, `is_responsible -> bool`, `leader: bool`, `self_member_added: bool`, `stable_deadline: float`, `reachability_changed_stats: tuple[float, float, int]` (first, latest, count), `strategy: _DowningStrategy`, `act_on_decision(decision) -> frozenset[UniqueAddress]`, `stopped: bool` (set on self `MemberRemoved`).
  - `split_brain_resolver(settings, self_unique_address, core: ActorRef[Any], subscriptions: ActorRef[Any]) -> Behavior[Any]`: `Behaviors.setup` that subscribes to `ClusterDomainEvent`, starts the `_Tick` timer at `settings.tick_interval`, feeds events and ticks to a `_Resolver` whose `down` tells `core` `Down(address)`, and returns `Behaviors.stopped()` when `stopped`.
  - Exports: `SplitBrainResolverSettings`, `KeepMajority`, `StaticQuorum`, `KeepOldest`, `DownAll` from `pekko_py.cluster` and `pekko_py`.

- [ ] **Step 1: Write the failing driver tests in `tests/cluster/test_sbr.py`**

```python
from pekko_py.cluster.core import (LeaderChanged, MemberDowned, MemberRemoved, MemberUp, ReachabilityChanged, ReachableMember, SeenChanged, UnreachableMember)
from pekko_py.cluster.sbr import _Resolver

class Driver:
    """SplitBrainResolverSpec.DowningTestActor: a resolver with a manual clock, recording what it downs."""
    def __init__(self, strategy=KeepMajority(), stable_after=10.0, down_all_when_unstable=0.0, self_member=A):
        self.now = 1000.0
        self.downed: list[Address] = []
        self.r = _Resolver(SplitBrainResolverSettings(strategy, stable_after, down_all_when_unstable), self_member.unique_address,
                           self.downed.append, lambda: self.now, logging.getLogger("test.sbr"))
    def up(self, *ms): [self.r.handle(MemberUp(m)) for m in ms]
    def lead(self, m=A): self.r.handle(LeaderChanged(m.address))
    def unreachable(self, *ms): [self.r.handle(UnreachableMember(m)) for m in ms]
    def advance(self, seconds): self.now += seconds; self.r.tick()

def test_downs_unreachable_only_after_stable_after():
    d = Driver(); d.up(A, B, C); d.lead(); d.unreachable(C)
    d.advance(9.9); assert d.downed == []
    d.advance(0.2); assert d.downed == [C.address]

def test_a_change_resets_the_stable_deadline():
    d = Driver(); d.up(A, B, C, D); d.lead(); d.unreachable(C)
    d.advance(8); d.unreachable(D); d.advance(8); assert d.downed == []
    d.advance(2.1); assert sorted(d.downed) == sorted([C.address, D.address])

def test_only_the_leader_downs():
    d = Driver(); d.up(A, B, C); d.unreachable(C); d.advance(11); assert d.downed == []
    d.lead(B); d.advance(11); assert d.downed == []
    d.lead(A); d.advance(0.1); assert d.downed == [C.address]

def test_leader_before_self_up_is_not_responsible():
    d = Driver(); d.lead(); d.up(B, C); d.unreachable(C); d.advance(11); assert d.downed == []
    d.up(A); d.advance(11); assert d.downed == [C.address]

def test_minority_downs_itself_last():
    d = Driver(); d.up(A, B, C); d.lead(); d.unreachable(B, C); d.advance(11)
    assert d.downed == [A.address]  # DownReachable: self is the only reachable node

def test_tick_with_no_unreachable_does_nothing():
    d = Driver(); d.up(A); d.lead(); d.advance(100); assert d.downed == []

def test_healed_member_is_not_downed():
    d = Driver(); d.up(A, B, C); d.lead(); d.unreachable(C); d.advance(5)
    d.r.handle(ReachableMember(C)); d.advance(20); assert d.downed == []

def test_member_downed_elsewhere_is_not_downed_again():
    d = Driver(); d.up(A, B, C); d.lead(); d.unreachable(C); d.r.handle(MemberDowned(downed(C)))
    d.advance(11); assert d.downed == []

def test_removed_member_leaves_the_unreachable_set():
    d = Driver(); d.up(A, B, C); d.lead(); d.unreachable(C); d.r.handle(MemberRemoved(C.with_status("removed"), "down"))
    d.advance(11); assert d.downed == [] and d.r.strategy.unreachable == frozenset()

def test_resolver_stops_on_self_removed():
    d = Driver(); d.up(A); d.r.handle(MemberRemoved(A.with_status("removed"), "down")); assert d.r.stopped

def test_down_all_when_unstable_fires_on_non_leader_once():
    d = Driver(stable_after=10.0, down_all_when_unstable=5.0); d.up(A, B, C, D)  # not the leader
    d.unreachable(C); d.advance(7); d.unreachable(D); d.advance(7); d.r.handle(ReachableMember(D))  # C stays unreachable
    d.advance(1.1)  # 15.1 s since the first change: stable_after + down_all_when_unstable exceeded
    assert sorted(d.downed) == sorted(m.address for m in (A, B, C, D)) and d.downed[-1] == A.address
    d.advance(1); assert len(d.downed) == 4  # stats reset after acting, and a non-leader takes no decision

def test_stats_reset_after_two_stable_after_when_down_all_is_off():
    d = Driver(stable_after=10.0); d.up(A, B, C); d.unreachable(C); d.advance(21)
    assert d.r.reachability_changed_stats[2] == 0

def test_double_decision_becomes_down_all():
    d = Driver(); d.up(A, B, C, D, E, F, G); d.lead()
    d.r.handle(ReachabilityChanged(reachability([(D, E), (G, F), (A, D), (A, E), (A, F), (A, G)])))
    d.unreachable(D, E, F, G); d.r.handle(SeenChanged(False, frozenset(m.address for m in (A, B, C))))
    d.advance(11)
    assert set(d.downed) == {m.address for m in (A, B, C, D, E, F, G)} and d.downed[-1] == A.address
```

- [ ] **Step 2: Run the driver tests to verify they fail**

Run: `pytest tests/cluster/test_sbr.py -q -k "stable_after or leader or downs or tick or healed or removed or unstable or stats or double" 2>&1 | tail -3`
Expected: FAIL, `ImportError: cannot import name '_Resolver'`.

- [ ] **Step 3: Implement `_Resolver` and `split_brain_resolver` in `sbr.py`**

Port `SplitBrainResolverBase` lines 160-520 without lease, DC and quarantine. `handle` is a `match` over the events of the spec's list; `mutate_member_info(reset_stable)` and `mutate_responsibility_info` are kept as small helpers with Pekko's log lines. `act_on_decision` catches `_DoubleDecision`, logs a warning and uses `nodes_to_down(DownAllDecision())`. Downing order: every node but self, then self. The behavior wraps it as `Behaviors.setup` -> `Behaviors.receive_message` with `_Tick` from `ctx.timers.start_timer_with_fixed_delay("tick", _Tick(), settings.tick_interval)`; it subscribes via `subscriptions.tell(Subscribe(ctx.self, ClusterDomainEvent))`.

- [ ] **Step 4: Run the driver tests to verify they pass**

Run: `pytest tests/cluster/test_sbr.py -q 2>&1 | tail -3 && pyright | tail -1`
Expected: all PASS, 0 errors.

- [ ] **Step 5: Write the failing multi-node tests in `tests/cluster/test_sbr_cluster.py`**

Reuse `FAST`, `cluster_node`, `all_up`, `crash` from `tests/cluster/test_cluster.py` and `test_singleton.py` (import or copy the three-line helpers as those files do), with `SBR = dict(downing=SplitBrainResolverSettings(stable_after=0.3))`. Add the partition helper:

```python
def partition(system: ActorSystem[Any], blocked_ports: set[int]) -> None:
    """Drop every inbound frame from the given peers at this node; each connection announces its peer in its hello."""
    remoting = system.remoting; assert remoting is not None
    async def read_loop(reader: asyncio.StreamReader) -> None:
        peer: int | None = None
        with contextlib.suppress(ValueError, OSError):
            while (frame := await read_frame(reader)) is not None:
                if isinstance(frame, dict) and frame.get("kind") == "hello":
                    peer = cast(dict[str, Any], frame["address"]).get("port")
                if peer not in blocked_ports:
                    remoting._handle(frame)  # pyright: ignore[reportPrivateUsage]
    remoting.read_loop = read_loop  # pyright: ignore[reportAttributeAccessIssue]
```

Tests:

```python
async def test_crashed_node_is_removed_without_manual_down():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1], **SBR)) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        await crash(nodes[2])
        await eventually(lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[:2]), timeout=5)

async def test_without_downing_a_crashed_node_stays_unreachable():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all(all_up(n, 2) for n in nodes), timeout=5)
        await crash(nodes[1])
        await eventually(lambda: len(Cluster(nodes[0]).state.unreachable) == 1, timeout=5)
        await asyncio.sleep(1.0)
        assert len(Cluster(nodes[0]).state.members) == 2

async def test_partition_keeps_the_majority_and_the_minority_downs_itself():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1], **SBR)) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        partition(nodes[2], {p1, p2}); partition(nodes[0], {p3}); partition(nodes[1], {p3})
        await eventually(lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[:2]), timeout=5)
        await eventually(lambda: Cluster(nodes[2]).is_terminated, timeout=5)  # downed itself and shut down

async def test_partition_isolating_the_leader():
    p1, p2, p3 = sorted((free_port(), free_port(), free_port()))
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1], **SBR)) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        partition(nodes[0], {p2, p3}); partition(nodes[1], {p1}); partition(nodes[2], {p1})
        await eventually(lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[1:]), timeout=5)
        await eventually(lambda: Cluster(nodes[0]).is_terminated, timeout=5)  # the isolated leader downed itself

async def test_resolver_failure_fails_the_daemon(monkeypatch: pytest.MonkeyPatch):
    def boom(self: Any) -> None: raise RuntimeError("resolver crashed")
    monkeypatch.setattr(_Resolver, "tick", boom)
    async with cluster_node("c", (p := free_port()), [p], **SBR) as node:
        await eventually(lambda: Cluster(node).is_terminated, timeout=5)
```

For the ports: `cluster_node` names are "c"; the lowest address is the lowest port, so sort `(p1, p2, p3)` before use so that `nodes[0]` is the lowest.

- [ ] **Step 6: Run the multi-node tests to verify they fail**

Run: `pytest tests/cluster/test_sbr_cluster.py -q 2>&1 | tail -3`
Expected: FAIL: `test_crashed_node_is_removed_without_manual_down` times out (nothing downs the node), the partition tests time out.

- [ ] **Step 7: Spawn the resolver from the core daemon and watch it**

In `become_initialized`, when `self.settings.downing is not None`: `ref = self.ctx.spawn(split_brain_resolver(self.settings.downing, self.self_unique_address, self.ctx.self, self.ctx.self), "downingProvider")` and `self.ctx.watch(ref)`. In `_on_signal`: `ChildFailed` for that child, or `Terminated` for it while self is not Removed or Down, re-raises / returns `Behaviors.stopped()` so the cluster shuts down (Pekko: the provider is vital). A `Terminated` after self `MemberRemoved` is the normal exit and is ignored. Export the public names.

- [ ] **Step 8: Run the multi-node tests and the whole suite**

Run: `pytest tests/cluster/test_sbr_cluster.py -q 2>&1 | tail -3 && pytest -q 2>&1 | tail -2 && pyright | tail -1`
Expected: all PASS, 0 errors. Run the suite three times to check for flakes.

- [ ] **Step 9: Commit**

```bash
git add src/pekko_py/cluster/sbr.py src/pekko_py/cluster/core.py src/pekko_py/cluster/__init__.py src/pekko_py/__init__.py tests/cluster/test_sbr.py tests/cluster/test_sbr_cluster.py
git commit -m "SBR: the resolver actor, a child of the core daemon, downing by strategy after stable-after, as Apache Pekko's"
```

---

### Task 5: The removal margin in the singleton and the sharding coordinator

**Files:**
- Modify: `src/pekko_py/cluster/singleton.py:36-66, 271-290, 352-358`, `src/pekko_py/cluster/sharding/coordinator.py:506-535, 546-548`
- Test: `tests/cluster/test_singleton_tracker.py` or `test_singleton.py` (pure part), `tests/cluster/test_sbr_cluster.py` (multi-node)

**Interfaces:**
- Consumes: `ClusterSettings.effective_down_removal_margin`.
- Produces: `_Manager.removal_margin: float` (settings value if positive, else the cluster's effective margin) used by `schedule_delayed_member_removed` and the retry counts; `ClusterSingletonSettings.max_hand_over_retries_for(removal_margin: float) -> int` and `max_take_over_retries_for(removal_margin)` (the existing properties stay, delegating with `self.removal_margin`); `_DelayedShardRegionTerminated(path: str)` in `coordinator.py`.

- [ ] **Step 1: Write the failing tests**

Pure, in `tests/cluster/test_singleton_tracker.py`:

```python
def test_retry_counts_use_the_resolved_removal_margin():
    s = ClusterSingletonSettings(hand_over_retry_interval=1.0)
    assert s.max_hand_over_retries_for(20.0) == 23 and s.max_take_over_retries_for(20.0) == 20
    assert s.max_hand_over_retries_for(0.0) == 15
```

Multi-node, in `tests/cluster/test_sbr_cluster.py`, reusing `test_singleton.py`'s `start` and `Log` helpers for a logging singleton and `test_cluster_sharding.py`'s counter entity:

```python
async def test_singleton_moves_after_crash_only_after_the_removal_margin():
    # three nodes, SBR stable_after=0.3, down_removal_margin=1.0; crash the oldest at t0
    # assert: no "start" entry on the new oldest before the old member is Removed + 1.0 s (within 0.2 s tolerance),
    # and exactly one afterwards
async def test_shards_of_a_crashed_region_move_only_after_the_removal_margin():
    # three nodes, same settings; find an entity hosted on node 3 via GetShardRegionState; crash node 3
    # assert: an ask through node 1 for that id is answered only after Removed + margin, and the
    # entity is then on node 1 or 2
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/cluster/test_singleton_tracker.py tests/cluster/test_sbr_cluster.py -q -k margin 2>&1 | tail -3`
Expected: FAIL: `AttributeError: max_hand_over_retries_for`; the multi-node tests see the singleton start too early.

- [ ] **Step 3: Implement**

`_Manager.__init__` computes `self.removal_margin = settings.removal_margin if settings.removal_margin > 0 else cluster.settings.effective_down_removal_margin` and uses it in `schedule_delayed_member_removed` and for the retry counts. In `_Coordinator.on_member_removed`: when `self.cluster.settings.effective_down_removal_margin > 0` and the path is in `self.alive_regions`, start `ctx.timers.start_single_timer(("region-removed", path), _DelayedShardRegionTerminated(path), margin)` instead of calling `region_terminated`; the message handler calls `region_terminated(path)` if the path is still known.

- [ ] **Step 4: Run the suite**

Run: `pytest -q 2>&1 | tail -2 && pyright | tail -1`
Expected: all PASS, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/pekko_py/cluster/singleton.py src/pekko_py/cluster/sharding/coordinator.py tests/cluster/test_singleton_tracker.py tests/cluster/test_sbr_cluster.py
git commit -m "SBR: the down-removal margin delays singleton take-over and shard reallocation after a removal, as Pekko's"
```

---

### Task 6: Example and README

**Files:**
- Modify: `examples/cluster/node.py:184`, `README.md` (Tour after "### Cluster" at line 288; names table near line 426; deviations bullet at line 488; CLAUDE.md layout line for `cluster/`)

- [ ] **Step 1: Example** — `ClusterSettings(seed_nodes=..., downing=SplitBrainResolverSettings(stable_after=5.0))`; docstring: a killed node is removed after five seconds without `Down`. Run two nodes, kill one with ctrl-c's sibling `kill -9`, and confirm the survivor logs `MemberRemoved` within ~7 s.

- [ ] **Step 2: README** — a "Split-brain resolver" subsection: three sentences on what it does, the settings snippet from the spec's API section, and the four strategies in one line each. Names rows: `pekko.cluster.downing-provider-class = SplitBrainResolverProvider` -> `ClusterSettings(downing=SplitBrainResolverSettings(...))`; `split-brain-resolver.active-strategy / stable-after / down-all-when-unstable` -> the fields; `static-quorum.quorum-size`, `keep-oldest.down-if-alone` -> `StaticQuorum(quorum_size)`, `KeepOldest(down_if_alone)`; `down-removal-margin` -> `ClusterSettings.down_removal_margin`. Deviations: replace "the split-brain resolver (downing is manual, as Pekko without an SBR)" with "the resolver is off unless `downing` is set, as Pekko without a provider", and add a bullet with the spec's "Deviations to document" paragraph. CLAUDE.md: add `sbr.py` to the `cluster/` line.

- [ ] **Step 3: Verify** — `pytest -q | tail -1`, `pyright | tail -1`, and the README snippet pasted into a scratch file imports and type-checks.

- [ ] **Step 4: Commit**

```bash
git add examples/cluster/node.py README.md CLAUDE.md
git commit -m "SBR: example and README"
```
