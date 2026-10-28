import asyncio
from dataclasses import dataclass

import pytest

from cart import AddItem, CartCmd, GetCart, cart
from ohra import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    Entity,
    EntityTypeKey,
    InMemoryStore,
    Sharding,
    SupervisorStrategy,
    ask,
)
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


async def test_ask_the_cart_directly(testkit: ActorTestKit):
    shop = testkit.spawn(cart({}))
    reply = await ask(shop, lambda r: AddItem("book", 1, r), timeout=testkit.timeout)
    assert reply == {"book": 1}


@dataclass(frozen=True)
class Gone:
    path: str


def watcher(target: ActorRef[str], report_to: ActorRef[Gone]) -> Behavior[Gone]:
    """Tells report_to when target stops, then stops too."""

    def setup(ctx: ActorContext[Gone]) -> Behavior[Gone]:
        ctx.watch_with(target, Gone(target.path))

        @Behaviors.receive_message
        def on_message(msg: Gone) -> Behavior[Gone]:
            report_to.tell(msg)
            return Behaviors.stopped()

        return on_message

    return Behaviors.setup(setup)


async def test_the_watcher_reports_a_stop(testkit: ActorTestKit):
    target: TestProbe[str] = testkit.create_test_probe()
    reports: TestProbe[Gone] = testkit.create_test_probe()
    w = testkit.spawn(watcher(target.ref, reports.ref))
    target.stop()
    await reports.expect_message(Gone(target.ref.path))
    await reports.expect_terminated(w)


@dataclass(frozen=True)
class Add:
    n: int


@dataclass(frozen=True)
class Total:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Crash:
    pass


type CounterCmd = Add | Total | Crash


def counter(total: int = 0) -> Behavior[CounterCmd]:
    @Behaviors.receive_message
    def on_message(msg: CounterCmd) -> Behavior[CounterCmd]:
        match msg:
            case Add(n):
                return counter(total + n)
            case Total(reply_to):
                reply_to.tell(total)
                return Behaviors.same
            case Crash():
                raise RuntimeError("crash")

    return on_message


async def test_a_restart_starts_from_the_initial_state(testkit: ActorTestKit):
    probe: TestProbe[int] = testkit.create_test_probe()
    supervised = Behaviors.supervise(counter()).on_failure(SupervisorStrategy.restart())
    ref = testkit.spawn(supervised)
    ref.tell(Add(5))
    ref.tell(Total(probe.ref))
    await probe.expect_message(5)
    ref.tell(Crash())
    ref.tell(Total(probe.ref))
    await probe.expect_message(0)


def flaky(probe: ActorRef[str]) -> Behavior[str]:
    """Reports each start; fails on every message; restarts after a backoff of 1 s."""

    def setup(_ctx: ActorContext[str]) -> Behavior[str]:
        probe.tell("started")

        @Behaviors.receive_message
        def on_message(msg: str) -> Behavior[str]:
            raise RuntimeError(msg)

        return on_message

    backoff = SupervisorStrategy.restart_with_backoff(1, 60, random_factor=0)
    return Behaviors.supervise(Behaviors.setup(setup)).on_failure(backoff)


@pytest.mark.manual_time
async def test_a_backoff_restart_waits_for_its_delay(
    testkit: ActorTestKit, manual_time: ManualTime
):
    probe: TestProbe[str] = testkit.create_test_probe()
    ref = testkit.spawn(flaky(probe.ref))
    await probe.expect_message("started")
    ref.tell("boom")
    await manual_time.expect_no_message_for(0.9, probe)
    await manual_time.time_passes(0.1)
    await probe.expect_message("started")


store = InMemoryStore[dict[str, int]]()


@Behaviors.persisted(store)
def durable_cart(items: dict[str, int]) -> Behavior[CartCmd]:
    @Behaviors.receive_message
    def on_message(msg: CartCmd) -> Behavior[CartCmd]:
        match msg:
            case AddItem(sku, qty, reply_to):
                updated = {**items, sku: items.get(sku, 0) + qty}
                return durable_cart(updated).then_reply(reply_to, updated)
            case GetCart(reply_to):
                reply_to.tell(items)
                return Behaviors.same

    return on_message


async def test_a_persisted_cart_survives_a_stop(testkit: ActorTestKit):
    probe: TestProbe[dict[str, int]] = testkit.create_test_probe()
    first = testkit.spawn(durable_cart.recover("alice", empty={}))
    first.tell(AddItem("book", 1, probe.ref))
    await probe.expect_message({"book": 1})
    await testkit.stop(first)
    second = testkit.spawn(durable_cart.recover("alice", empty={}))
    second.tell(GetCart(probe.ref))
    await probe.expect_message({"book": 1})


CART = EntityTypeKey[CartCmd]("Cart")


async def test_each_id_gets_its_own_cart(testkit: ActorTestKit):
    probe: TestProbe[dict[str, int]] = testkit.create_test_probe()
    region = testkit.spawn(Sharding.init(Entity(CART, lambda _entity: cart({}))))
    alice = Sharding.entity_ref_for(region, "alice")
    bob = Sharding.entity_ref_for(region, "bob")
    alice.tell(AddItem("book", 1, probe.ref))
    await probe.expect_message({"book": 1})
    bob.tell(GetCart(probe.ref))
    await probe.expect_message({})


def cache_writer(cache: dict[str, int]) -> Behavior[tuple[str, int]]:
    @Behaviors.receive_message
    async def on_message(msg: tuple[str, int]) -> Behavior[tuple[str, int]]:
        await asyncio.sleep(0.01)  # a slow write
        key, value = msg
        cache[key] = value
        return Behaviors.same

    return on_message


async def test_the_write_lands(testkit: ActorTestKit):
    cache: dict[str, int] = {}
    testkit.spawn(cache_writer(cache)).tell(("a", 1))
    probe: TestProbe[None] = testkit.create_test_probe()

    def landed() -> None:
        assert cache == {"a": 1}

    await probe.await_assert(landed)


async def test_a_message_to_a_stopped_cart_is_a_dead_letter(testkit: ActorTestKit):
    dead_letters = testkit.create_dead_letter_probe()
    shop = testkit.spawn(cart({}))
    await testkit.stop(shop)
    probe: TestProbe[dict[str, int]] = testkit.create_test_probe()
    shop.tell(GetCart(probe.ref))
    letter = await dead_letters.receive_message()
    assert (letter.message, letter.recipient) == (GetCart(probe.ref), shop)
    await probe.expect_no_message()
