"""pytest fixtures for the test kit, as Pekko's ScalaTestWithActorTestKit.

Enable with ``pytest_plugins = ["ohra.testkit.pytest_plugin"]`` in ``conftest.py``. The
``testkit`` fixture gives each test its own ``ActorTestKit``, named after the test;
``@pytest.mark.manual_time`` puts it on the explicitly triggered scheduler, and the
``manual_time`` fixture is its ``ManualTime``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import cast

import pytest
import pytest_asyncio

from .actor_testkit import ActorTestKit
from .manual_time import ManualTime


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "manual_time: run the test kit on the explicitly triggered scheduler"
    )


def _item(request: pytest.FixtureRequest) -> pytest.Item:
    return cast(pytest.Item, request.node)  # pyright: ignore[reportUnknownMemberType]


@pytest_asyncio.fixture
async def testkit(request: pytest.FixtureRequest) -> AsyncGenerator[ActorTestKit]:
    node = _item(request)
    manual = node.get_closest_marker("manual_time") is not None
    async with ActorTestKit(name=node.name, manual_time=manual) as kit:
        yield kit


@pytest.fixture
def manual_time(request: pytest.FixtureRequest) -> ManualTime:
    if _item(request).get_closest_marker("manual_time") is None:
        pytest.fail("the manual_time fixture needs @pytest.mark.manual_time on the test")
    kit: ActorTestKit = request.getfixturevalue("testkit")
    return ManualTime(kit.system)
