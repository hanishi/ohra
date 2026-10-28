from cart import AddItem, cart
from ohra.testkit import ActorTestKit, TestProbe


async def test_adding_an_item_replies_with_the_cart(testkit: ActorTestKit):
    shop = testkit.spawn(cart({}), "cart")
    probe: TestProbe[dict[str, int]] = testkit.create_test_probe()
    shop.tell(AddItem("book", 1, probe.ref))
    await probe.expect_message({"book": 1})
