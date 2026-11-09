"""Controller, after Apache Pekko's ControllerSpec
(remote-tests/src/test/scala/org/apache/pekko/remote/testconductor/ControllerSpec.scala)."""

from __future__ import annotations

import asyncio

from ohra.testkit.conductor import Controller, RoleName

A = RoleName("a")
B = RoleName("b")


async def test_publish_its_nodes() -> None:
    controller = Controller(1, 5.0)
    await controller.node_info(A)
    await controller.node_info(B)
    assert controller.get_nodes() == {A, B}
    controller.close()
    await asyncio.sleep(0)
