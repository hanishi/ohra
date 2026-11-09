from dataclasses import dataclass

from ohra import ActorRef, Behavior, Behaviors, remotable
from ohra.testkit import Direction, MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

sample = MultiNodeConfig()
node1 = sample.role("node1")
node2 = sample.role("node2")


@remotable
@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


def ponger() -> Behavior[Ping]:
    @Behaviors.receive_message
    def on_message(msg: Ping) -> Behavior[Ping]:
        msg.reply_to.tell("pong")
        return Behaviors.same

    return on_message


@multi_node_spec(sample)
async def test_send_to_and_receive_from_a_remote_node(spec: MultiNodeSpec):
    await spec.enter_barrier("startup")

    if spec.is_node(node1):
        await spec.enter_barrier("deployed")
        probe: TestProbe[str] = spec.create_test_probe()
        remote_ponger = spec.system.resolve(spec.node(node2) + "/user/ponger")
        remote_ponger.tell(Ping(probe.ref))
        await probe.expect_message("pong", 10.0)

    if spec.is_node(node2):
        spec.spawn(ponger(), "ponger")
        await spec.enter_barrier("deployed")

    await spec.enter_barrier("finished")


faulty = MultiNodeConfig()
first = faulty.role("first")
second = faulty.role("second")
faulty.test_transport(on=True)


@multi_node_spec(faulty)
async def test_a_blackhole_drops_messages_until_pass_through(spec: MultiNodeSpec):
    probe: TestProbe[str] = spec.create_test_probe()
    if spec.is_node(first):
        spec.spawn(ponger(), "ponger")
    await spec.enter_barrier("deployed")
    remote_ponger = spec.system.resolve(spec.node(first) + "/user/ponger")

    if spec.is_node(second):
        remote_ponger.tell(Ping(probe.ref))
        await probe.expect_message("pong")
    await spec.enter_barrier("connected")

    if spec.is_node(first):
        await spec.test_conductor.blackhole(second, first, Direction.BOTH)
    await spec.enter_barrier("blackholed")

    if spec.is_node(second):
        remote_ponger.tell(Ping(probe.ref))
        await probe.expect_no_message(0.5)
    await spec.enter_barrier("checked")

    if spec.is_node(first):
        await spec.test_conductor.pass_through(second, first, Direction.BOTH)
    await spec.enter_barrier("passed")

    if spec.is_node(second):
        remote_ponger.tell(Ping(probe.ref))
        await probe.expect_message("pong")
    await spec.enter_barrier("done")


@multi_node_spec(faulty)
async def test_a_node_that_exits_leaves_the_barriers(spec: MultiNodeSpec):
    await spec.enter_barrier("started")
    if spec.is_node(first):
        await spec.test_conductor.exit(second, 0)
        assert await spec.test_conductor.get_nodes() == (first,)
    await spec.enter_barrier("after-exit")
