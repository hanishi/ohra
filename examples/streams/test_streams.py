import pytest

from actors import Broken, Checkout, GetPrice, LedgerCmd, Scan, shop, till
from ohra import AskTimeoutError
from ohra.stream import (
    DONE,
    AbruptStageTerminationError,
    Flow,
    Keep,
    KillSwitches,
    OverflowStrategy,
    QueueOfferResult,
    Sink,
    Source,
    SourceQueue,
)
from ohra.testkit import ActorTestKit, ManualTime, TestProbe
from pipeline import invoices


async def test_the_pipeline_prices_known_skus_two_to_an_invoice(testkit: ActorTestKit):
    out = invoices(["book", "pen", "lamp", "mug"]).run_with(Sink.seq(), testkit.system)
    assert await out == [[1200, 150], [800]]


async def test_a_blueprint_runs_again_from_the_start(testkit: ActorTestKit):
    blueprint = invoices(["pen", "mug"]).map(sum)
    assert await blueprint.run_with(Sink.head(), testkit.system) == 950
    assert await blueprint.run_with(Sink.head(), testkit.system) == 950


async def test_a_flow_is_a_reusable_piece(testkit: ActorTestKit):
    double = Flow.of(int).map(lambda n: n * 2)
    out = Source.from_iterable([1, 2, 3]).via(double).run_with(Sink.seq(), testkit.system)
    assert await out == [2, 4, 6]


async def test_keep_both_hands_back_both_values(testkit: ActorTestKit):
    graph = (
        Source.from_iterable([1, 2, 3])
        .watch_termination(Keep.right)  # the source's NotUsed is dropped
        .to_mat(Sink.fold(0, lambda total, n: total + n), Keep.both)
    )
    finished, total = graph.run(testkit.system)
    assert await total == 6
    assert await finished == DONE


async def test_a_stream_between_two_queues(testkit: ActorTestKit):
    orders: Source[str, SourceQueue[str]] = Source.queue(8, OverflowStrategy.backpressure)
    inbox, outbox = orders.map(str.upper).to_mat(Sink.queue(), Keep.both).run(testkit.system)
    assert await inbox.offer("book") == QueueOfferResult.Enqueued
    assert await outbox.pull() == "BOOK"
    inbox.complete()
    assert await outbox.pull() is None  # the stream has completed


@pytest.mark.manual_time
async def test_throttle_lets_one_element_through_per_second(
    testkit: ActorTestKit, manual_time: ManualTime
):
    probe: TestProbe[int] = testkit.create_test_probe()
    done = (
        Source.from_iterable([1, 2, 3])
        .throttle(1, 1.0, maximum_burst=1)
        .run_foreach(probe.ref.tell, testkit.system)
    )
    await probe.expect_message(1)
    await manual_time.expect_no_message_for(0.9, probe)
    await manual_time.time_passes(0.1)
    await probe.expect_message(2)
    await manual_time.time_passes(1.0)
    await probe.expect_message(3)
    await done


@pytest.mark.manual_time
async def test_tick_emits_once_a_second(testkit: ActorTestKit, manual_time: ManualTime):
    probe: TestProbe[str] = testkit.create_test_probe()
    done = Source.tick(1.0, 1.0, "tick").take(2).run_foreach(probe.ref.tell, testkit.system)
    await manual_time.expect_no_message_for(0.9, probe)
    await manual_time.time_passes(0.1)
    await probe.expect_message("tick")
    await manual_time.time_passes(1.0)
    await probe.expect_message("tick")
    await done


async def test_zip_pairs_two_sources(testkit: ActorTestKit):
    letters = Source.from_iterable("ab").concat(Source.single("c"))
    pairs = Source.from_iterable([1, 2, 3]).zip(letters).run_with(Sink.seq(), testkit.system)
    assert await pairs == [(1, "a"), (2, "b"), (3, "c")]


async def test_a_kill_switch_ends_an_endless_stream(testkit: ActorTestKit):
    switch, done = (
        Source.repeat("beat")
        .via_mat(KillSwitches.single(), Keep.right)
        .to_mat(Sink.ignore(), Keep.both)
        .run(testkit.system)
    )
    switch.shutdown()
    assert await done == DONE


async def test_a_checkout_streams_through_the_pricing_actor_into_the_ledger(
    testkit: ActorTestKit,
):
    probe: TestProbe[int] = testkit.create_test_probe()
    testkit.spawn(shop({"book": 1200, "pen": 150})).tell(
        Checkout(("book", "pen", "pen"), probe.ref)
    )
    await probe.expect_message(1500)


@pytest.mark.manual_time
async def test_a_price_that_never_comes_fails_the_till_into_the_ledger(
    testkit: ActorTestKit, manual_time: ManualTime
):
    prices: TestProbe[GetPrice] = testkit.create_test_probe()  # never replies
    book: TestProbe[LedgerCmd] = testkit.create_test_probe()
    scanner = till(prices.ref, book.ref).run(testkit.system)
    scanner.tell(Scan("book"))
    await prices.expect_message_type(GetPrice)
    await manual_time.time_passes(1.0)  # the ask's timeout
    broken = await book.expect_message_type(Broken)
    assert isinstance(broken.cause, AskTimeoutError)


async def test_terminate_fails_a_stream_that_is_still_running():
    async with ActorTestKit() as testkit:
        done = Source.repeat("tick").run_with(Sink.ignore(), testkit.system)
    with pytest.raises(AbruptStageTerminationError):
        await done
