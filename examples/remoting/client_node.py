"""Node "client": resolves the greeter by path on another node, tells it, asks it.

uv run python examples/remoting/client_node.py [port]    # after greeter_node.py is running
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from messages import Greet, Greeted  # noqa: E402

from ohra import ActorRef, ActorSystem, Behaviors, ask  # noqa: E402

GREETER = "ohra://greeter@127.0.0.1:2552/user"


async def main(port: int) -> None:
    async with ActorSystem(Behaviors.ignore, "client", address=("127.0.0.1", port)) as system:
        greeter: ActorRef[Greet] = system.resolve(GREETER)  # a RemoteRef: no connection yet
        greeter.tell(Greet("fire-and-forget", system.ignore_ref))
        # the reply ref crosses the wire too
        reply: Greeted = await ask(greeter, lambda r: Greet("pekko", r), timeout=3)
        print(f"{reply.text!r} served by {reply.served_by}")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 2553))
