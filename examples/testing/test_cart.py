from cart import AddItem, GetCart, cart
from ohra.testkit import ActorTestKit, TestProbe


async def test_adding_an_item_replies_with_the_cart():
    async with ActorTestKit() as testkit:
        shop = testkit.spawn(cart({}), "cart")
        probe: TestProbe[dict[str, int]] = testkit.create_test_probe()
        shop.tell(AddItem("book", 1, probe.ref))
        await probe.expect_message({"book": 1})
        shop.tell(AddItem("pen", 2, probe.ref))
        shop.tell(GetCart(probe.ref))
        assert await probe.receive_messages(2) == [{"book": 1, "pen": 2}] * 2
