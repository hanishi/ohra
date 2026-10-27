"""Node "greeter": listens on 127.0.0.1:2552 and answers Greet from any node.

uv run python examples/remoting/greeter_node.py          # terminal 1
uv run python examples/remoting/client_node.py           # terminal 2
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from messages import Greet, Greeted  # noqa: E402

from ohra import ActorContext, ActorSystem, Behavior, Behaviors  # noqa: E402


@Behaviors.receive
def greeter(ctx: ActorContext[Greet], msg: Greet) -> Behavior[Greet]:
    ctx.log.info("greeting %s for %s", msg.name, msg.reply_to.path)
    # reply_to is a RemoteRef
    msg.reply_to.tell(Greeted(f"hello {msg.name}", served_by=ctx.self.path))
    return Behaviors.same


async def main() -> None:
    async with ActorSystem(greeter, "greeter", address=("127.0.0.1", 2552)) as system:
        print(f"listening as {system.path}; ctrl-c to stop")
        await system.when_terminated()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")
    with __import__("contextlib").suppress(KeyboardInterrupt):
        asyncio.run(main())
