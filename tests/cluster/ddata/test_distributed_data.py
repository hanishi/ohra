"""The DistributedData extension and ReplicatorMessageAdapter, from cluster-typed's ReplicatorSpec
and ReplicatorDocSpec, whose Counter is the actor here."""

from collections.abc import Callable
from dataclasses import dataclass

import pytest

from ohra import ActorContext, Behavior, Behaviors
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    Replicator,
    ReplicatorMessageAdapter,
    ReplicatorSettings,
)
from ohra.ref import ActorRef
from ohra.testkit import ActorTestKit, TestProbe

from .test_replicator_local import Node, single_node


# ReplicatorDocSpec's Counter


@dataclass(frozen=True)
class Increment:
    pass


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class GetCachedValue:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Unsubscribe:
    pass


@dataclass(frozen=True)
class InternalUpdateResponse:
    rsp: Replicator.UpdateResponse[GCounter]


@dataclass(frozen=True)
class InternalGetResponse:
    rsp: Replicator.GetResponse[GCounter]
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class InternalSubscribeResponse:
    chg: Replicator.SubscribeResponse[GCounter]


type Command = (
    Increment
    | GetValue
    | GetCachedValue
    | Unsubscribe
    | InternalUpdateResponse
    | InternalGetResponse
    | InternalSubscribeResponse
)


def counter(key: GCounterKey) -> Behavior[Command]:
    def setup(ctx: ActorContext[Command]) -> Behavior[Command]:
        node = DistributedData(ctx.system).self_unique_address

        def with_adapter(
            adapter: ReplicatorMessageAdapter[Command, GCounter],
        ) -> Behavior[Command]:
            adapter.subscribe(key, InternalSubscribeResponse)

            def updated(cached_value: int) -> Behavior[Command]:
                def receive(msg: Command) -> Behavior[Command]:
                    match msg:
                        case Increment():
                            adapter.ask_update(
                                lambda r: Replicator.Update(
                                    key,
                                    GCounter(),
                                    Replicator.WriteLocal(),
                                    r,
                                    lambda c: c.increment(node, 1),
                                ),
                                InternalUpdateResponse,
                            )
                            return Behaviors.same
                        case GetValue(reply_to):
                            adapter.ask_get(
                                lambda r: Replicator.Get(key, Replicator.ReadLocal(), r),
                                lambda rsp: InternalGetResponse(rsp, reply_to),
                            )
                            return Behaviors.same
                        case GetCachedValue(reply_to):
                            reply_to.tell(cached_value)
                            return Behaviors.same
                        case Unsubscribe():
                            adapter.unsubscribe(key)
                            return Behaviors.same
                        case InternalUpdateResponse():
                            return Behaviors.same
                        case InternalGetResponse(Replicator.GetSuccess() as rsp, reply_to):
                            reply_to.tell(rsp.get(key).value)
                            return Behaviors.same
                        case InternalGetResponse():
                            return Behaviors.unhandled  # not dealing with failures
                        case InternalSubscribeResponse(Replicator.Changed() as chg):
                            return updated(chg.get(key).value)
                        case InternalSubscribeResponse():
                            return Behaviors.unhandled

                return Behaviors.receive_message(receive)

            return updated(0)

        return DistributedData.with_replicator_message_adapter(with_adapter)

    return Behaviors.setup(setup)


def spawn_counter(node: Node, key: GCounterKey) -> ActorRef[Command]:
    return node.system._system_root.spawn_child(counter(key), "counter", None).ref  # pyright: ignore[reportPrivateUsage]


async def test_have_the_prefixed_replicator_name():
    async with ActorTestKit() as kit:
        assert ReplicatorSettings.name_for(kit.system, "typed") == "typedDdataReplicator"


async def test_distributed_data_is_one_per_system():
    async with single_node() as node:
        dd = DistributedData(node.system)
        assert DistributedData(node.system) is dd
        assert DistributedData(node.system).replicator is node.replicator


async def test_distributed_data_without_a_cluster_raises():
    async with ActorTestKit() as kit:
        with pytest.raises(RuntimeError):
            DistributedData(kit.system)


async def test_the_replicator_lives_under_system():
    async with single_node() as node:
        assert str(node.replicator.path).endswith("/system/ddataReplicator")


async def test_the_replicator_has_the_configured_name():
    async with single_node(ReplicatorSettings(name="fooReplicator")) as node:
        assert str(node.replicator.path).endswith("/system/fooReplicator")


async def test_adapter_ask_update_and_ask_get():
    async with single_node() as node:
        key = GCounterKey("counter1")
        c = spawn_counter(node, key)
        probe: TestProbe[int] = node.probe()
        c.tell(Increment())
        c.tell(GetValue(probe.ref))
        await probe.expect_message(1)

        got: TestProbe[Replicator.GetResponse[GCounter]] = node.probe()
        node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), got.ref))
        rsp = await got.receive_message()
        assert isinstance(rsp, Replicator.GetSuccess)
        assert rsp.get(key).value == 1


FAST_NOTIFY = ReplicatorSettings(notify_subscribers_interval=0.05)


async def test_adapter_subscribe_receives_changed():
    async with single_node(FAST_NOTIFY) as node:
        c = spawn_counter(node, GCounterKey("counter2"))
        probe: TestProbe[int] = node.probe()

        async def cached_is(n: int) -> None:
            c.tell(GetCachedValue(probe.ref))
            assert await probe.receive_message() == n

        c.tell(Increment())
        c.tell(Increment())
        await probe.await_assert(lambda: cached_is(2))
        c.tell(Increment())
        await probe.await_assert(lambda: cached_is(3))


async def test_adapter_unsubscribe_stops_changed():
    async with single_node(FAST_NOTIFY) as node:
        c = spawn_counter(node, GCounterKey("counter2"))
        probe: TestProbe[int] = node.probe()
        c.tell(Unsubscribe())
        c.tell(Increment())
        c.tell(GetValue(probe.ref))
        await probe.expect_message(1)  # the replicator has the increment
        node.replicator.tell(Replicator.FlushChanges())
        c.tell(GetValue(probe.ref))  # a round trip, after the flush's Changed would have come
        await probe.expect_message(1)
        c.tell(GetCachedValue(probe.ref))
        await probe.expect_message(0)  # the old value, not 1


type _Ask = Callable[[ReplicatorMessageAdapter[object, GCounter], GCounterKey], None]


@pytest.mark.parametrize(
    ("ask", "timed_out"),
    [
        (
            lambda a, k: a.ask_get(
                lambda r: Replicator.Get(k, Replicator.ReadLocal(), r), lambda rsp: rsp
            ),
            Replicator.GetFailure,
        ),
        (
            lambda a, k: a.ask_update(
                lambda r: Replicator.Update(
                    k, GCounter(), Replicator.WriteLocal(), r, lambda c: c
                ),
                lambda rsp: rsp,
            ),
            Replicator.UpdateTimeout,
        ),
        (
            lambda a, k: a.ask_delete(
                lambda r: Replicator.Delete(k, Replicator.WriteLocal(), r), lambda rsp: rsp
            ),
            Replicator.DeleteFailure,
        ),
    ],
)
async def test_adapter_maps_an_unexpected_ask_timeout_to_a_failure_for_the_key(
    ask: _Ask, timed_out: Callable[[GCounterKey], object]
):
    async with ActorTestKit() as kit:
        silent: TestProbe[Replicator.Command] = kit.create_test_probe()  # never replies
        replies: TestProbe[object] = kit.create_test_probe()
        key = GCounterKey("k")

        def asker(ctx: ActorContext[object]) -> Behavior[object]:
            ask(ReplicatorMessageAdapter(ctx, silent.ref, 0.05), key)

            def forward(msg: object) -> Behavior[object]:
                replies.ref.tell(msg)
                return Behaviors.same

            return Behaviors.receive_message(forward)

        kit.spawn(Behaviors.setup(asker))
        await silent.receive_message()
        await replies.expect_message(timed_out(key))
