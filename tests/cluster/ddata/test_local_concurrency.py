"""Local updates from two actors, ported from Pekko's LocalConcurrencySpec ("be possible to do
from two actors").
"""

from ohra.behavior import Behavior, Behaviors
from ohra.cluster.ddata import ORSet, ORSetKey
from ohra.cluster.ddata.replicator import Replicator
from ohra.testkit import TestProbe

from .test_replicator_local import Node, single_node

KEY: ORSetKey[str] = ORSetKey("key")
UPDATES = 100


def updater(
    node: Node, prefix: str, reply_to: TestProbe[Replicator.UpdateResponse[ORSet[str]]]
) -> Behavior[None]:
    def start() -> Behavior[None]:
        for i in range(UPDATES):
            node.replicator.tell(
                Replicator.Update(
                    KEY,
                    ORSet[str](),
                    Replicator.WriteLocal(),
                    reply_to.ref,
                    lambda s, e=f"{prefix}{i}": s.add(node.self_address, e),
                )
            )
        return Behaviors.ignore

    return Behaviors.setup(lambda _: start())


async def test_updates_from_two_actors_are_all_kept():
    async with single_node() as node:
        probes: list[TestProbe[Replicator.UpdateResponse[ORSet[str]]]] = [
            node.probe(),
            node.probe(),
        ]
        for name, probe in zip("ab", probes):
            node.system._system_root.spawn_child(  # pyright: ignore[reportPrivateUsage]
                updater(node, name, probe), f"updater-{name}", None
            )
        for probe in probes:
            for _ in range(UPDATES):
                assert isinstance(await probe.receive_message(), Replicator.UpdateSuccess)

        got: TestProbe[Replicator.GetResponse[ORSet[str]]] = node.probe()
        node.replicator.tell(Replicator.Get(KEY, Replicator.ReadLocal(), got.ref))
        reply = await got.receive_message()
        assert isinstance(reply, Replicator.GetSuccess)
        expected = {f"{p}{i}" for p in "ab" for i in range(UPDATES)}
        assert reply.get(KEY).elements == expected
