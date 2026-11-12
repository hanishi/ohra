"""Pekko's VotingServiceSpec (pekko-sample-distributed-data-scala), on three cluster nodes."""

from voting_service import GetVotes, Open, Vote, Votes, Close, voting_service
from ohra import Address, Cluster, ClusterSettings, Join
from ohra.cluster.ddata import DistributedData, Replicator
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

config = MultiNodeConfig()
node1 = config.role("node-1")
node2 = config.role("node-2")
node3 = config.role("node-3")
config.common_config(cluster=ClusterSettings())

one_node = MultiNodeConfig()
one_node.role("node")
one_node.common_config(cluster=ClusterSettings())


async def join_cluster(spec: MultiNodeSpec) -> None:
    for src, dst in ((node1, node1), (node2, node1), (node3, node1)):
        if spec.is_node(src):
            Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(dst))))
        await spec.enter_barrier(f"{src.name}-joined")

    replicator = DistributedData(spec.system).replicator
    probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

    async def all_are_replicas() -> None:
        replicator.tell(Replicator.GetReplicaCount(probe.ref))
        assert await probe.receive_message() == Replicator.ReplicaCount(3)

    await spec.await_assert(all_are_replicas, max=20.0)
    await spec.enter_barrier("after-1")


@multi_node_spec(config)
async def test_join_cluster(spec: MultiNodeSpec):
    await join_cluster(spec)


@multi_node_spec(config)
async def test_count_votes_correctly(spec: MultiNodeSpec):
    await join_cluster(spec)
    service = spec.spawn(voting_service(), "votingService")
    n_votes = 1000
    if spec.is_node(node1):
        service.tell(Open())
        for n in range(1, n_votes + 1):
            service.tell(Vote(f"#{n % 20 + 1}"))
    if spec.is_node(node2) or spec.is_node(node3):
        # wait for it to open
        p: TestProbe[Votes] = spec.create_test_probe()

        async def is_open() -> None:
            service.tell(GetVotes(p.ref))
            assert (await p.receive_message(3.0)).open

        await spec.await_assert(is_open, max=15.0)
        for n in range(1, n_votes + 1):
            service.tell(Vote(f"#{n % 20 + 1}"))
    await spec.enter_barrier("voting-done")
    if spec.is_node(node3):
        service.tell(Close())

    expected = {f"#{n}": 3 * n_votes // 20 for n in range(1, 21)}

    async def all_counted_and_closed() -> None:
        probe: TestProbe[Votes] = spec.create_test_probe()
        service.tell(GetVotes(probe.ref))
        await probe.expect_message(Votes(expected, False), 3.0)

    await spec.await_assert(all_counted_and_closed, max=15.0)
    await spec.enter_barrier("after-2")


@multi_node_spec(one_node)
async def test_votes_before_the_contest_opens_are_not_counted(spec: MultiNodeSpec):
    service = spec.spawn(voting_service())
    probe: TestProbe[Votes] = spec.create_test_probe()
    service.tell(Vote("#1"))
    service.tell(GetVotes(probe.ref))
    await probe.expect_message(Votes({}, False))

    service.tell(Open())
    service.tell(Vote("#2"))

    async def counted() -> None:
        service.tell(GetVotes(probe.ref))
        await probe.expect_message(Votes({"#2": 1}, True), 3.0)

    await spec.await_assert(counted, max=10.0)
