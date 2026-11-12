"""Pekko's ShoppingCartSpec (pekko-sample-distributed-data-scala), on three cluster nodes.

``test_join_cluster``, ``test_handle_updates_directly_after_start`` and
``test_handle_updates_from_different_nodes`` are Pekko's three cases.
``test_removing_an_item_from_an_empty_cart_does_nothing`` is ohra's own, for the Review Focus:
a ``RemoveItem`` on a cart that was never written.
"""

from shopping_cart import AddItem, Cart, Command, GetCart, LineItem, RemoveItem, shopping_cart
from ohra import Address, ActorRef, Cluster, ClusterSettings, Join
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


async def expect_cart(spec: MultiNodeSpec, cart: ActorRef[Command], items: set[LineItem]) -> None:
    async def has_items() -> None:
        probe: TestProbe[Cart] = spec.create_test_probe()
        cart.tell(GetCart(probe.ref))
        assert (await probe.receive_message(3.0)).items == items

    await spec.await_assert(has_items, max=15.0)


async def updates_directly_after_start(spec: MultiNodeSpec, cart: ActorRef[Command]) -> None:
    if spec.is_node(node2):
        cart.tell(AddItem(LineItem("1", "Apples", 2)))
        cart.tell(AddItem(LineItem("2", "Oranges", 3)))
    await spec.enter_barrier("updates-done")
    await expect_cart(spec, cart, {LineItem("1", "Apples", 2), LineItem("2", "Oranges", 3)})
    await spec.enter_barrier("after-2")


@multi_node_spec(config)
async def test_join_cluster(spec: MultiNodeSpec):
    await join_cluster(spec)


@multi_node_spec(config)
async def test_handle_updates_directly_after_start(spec: MultiNodeSpec):
    cart = spec.spawn(shopping_cart("user-1"))
    await join_cluster(spec)
    await updates_directly_after_start(spec, cart)


@multi_node_spec(config)
async def test_handle_updates_from_different_nodes(spec: MultiNodeSpec):
    cart = spec.spawn(shopping_cart("user-1"))
    await join_cluster(spec)
    await updates_directly_after_start(spec, cart)
    if spec.is_node(node2):
        cart.tell(AddItem(LineItem("1", "Apples", 5)))
        cart.tell(RemoveItem("2"))
    if spec.is_node(node3):
        cart.tell(AddItem(LineItem("3", "Bananas", 4)))
    await spec.enter_barrier("updates-done-2")
    await expect_cart(spec, cart, {LineItem("1", "Apples", 7), LineItem("3", "Bananas", 4)})
    await spec.enter_barrier("after-3")


@multi_node_spec(one_node)
async def test_removing_an_item_from_an_empty_cart_does_nothing(spec: MultiNodeSpec):
    cart = spec.spawn(shopping_cart("user-1"))
    cart.tell(RemoveItem("9"))
    cart.tell(AddItem(LineItem("1", "Apples", 1)))
    await expect_cart(spec, cart, {LineItem("1", "Apples", 1)})
