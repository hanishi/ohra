"""Ported from Pekko's ``scaladsl/LeaseProviderSpec.scala`` (coordination/src/test).

Cases, with their Scala lines: load lease implementation (81), defaults for timeouts (98), same
instance for the same name, config path and owner (105), different instance for a different
name (111) and owner (117), missing lease-class (123), unknown lease-class (129).

Differences:
- registration replaces the config blocks (D7);
- ``LeaseB``'s ``ActorSystem`` constructor parameter is a closure;
- "missing lease-class" becomes "not registered", and raises ``ValueError``;
- "unknown lease-class" becomes "the factory raises";
- ``LeaseProviderTest.java`` is not ported.

The ohra-only tests follow the ported ones: a factory that raises is called again, ``register``
after ``get_lease``, one instance per implementation, one provider per system, the exports.
"""

import logging
from collections.abc import Callable
from typing import Any

import pytest

import ohra
import ohra.coordination
import ohra.coordination.lease as lease_module
from ohra import ActorSystem
from ohra.coordination.lease import (
    Lease,
    LeaseProvider,
    LeaseSettings,
    TimeoutSettings,
)
from ohra.testkit import ActorTestKit


class LeaseA(Lease):
    async def acquire(
        self, lease_lost_callback: Callable[[BaseException | None], None] | None = None
    ) -> bool:
        return False

    async def release(self) -> bool:
        return False

    def check_lease(self) -> bool:
        return False


class LeaseB(LeaseA):
    def __init__(self, settings: LeaseSettings, system: ActorSystem[Any]) -> None:
        super().__init__(settings)
        self.system = system


def setup(system: ActorSystem[Any]) -> LeaseProvider:
    provider = LeaseProvider(system)
    provider.register(
        "lease-a",
        lambda s: LeaseA(s),
        timeout_settings=TimeoutSettings(1.0, 100.0, 2.0),
        lease_config={"key1": "value1"},
    )
    provider.register(
        "lease-b",
        lambda s: LeaseB(s, system),
        timeout_settings=TimeoutSettings(1.0, 120.0, 2.0),
        lease_config={"key2": "value2"},
    )

    def unknown(settings: LeaseSettings) -> Lease:
        raise ImportError("foo.wrong.ClassName")

    provider.register("lease-unknown", unknown)
    provider.register("lease-fallback-to-defaults", lambda s: LeaseA(s))
    return provider


async def test_load_lease_implementation() -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        a = provider.get_lease("a", "lease-a", "owner1")
        assert isinstance(a, LeaseA)
        assert a.settings.lease_name == "a"
        assert a.settings.owner_name == "owner1"
        assert a.settings.lease_config["key1"] == "value1"
        assert a.settings.timeout_settings.heartbeat_timeout == 100.0
        assert a.settings.timeout_settings.heartbeat_interval == 1.0
        assert a.settings.timeout_settings.operation_timeout == 2.0
        b = provider.get_lease("b", "lease-b", "owner2")
        assert isinstance(b, LeaseB)
        assert b.settings.lease_name == "b"
        assert b.settings.owner_name == "owner2"
        assert b.settings.lease_config["key2"] == "value2"


async def test_load_defaults_for_timeouts_if_not_specified() -> None:
    async with ActorTestKit() as testkit:
        lease = setup(testkit.system).get_lease("a", "lease-fallback-to-defaults", "owner1")
        timeouts = lease.settings.timeout_settings
        assert timeouts.operation_timeout == 5.0
        assert timeouts.heartbeat_timeout == 120.0
        assert timeouts.heartbeat_interval == 12.0


async def test_return_same_instance_for_same_lease_name_config_path_and_owner() -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        assert provider.get_lease("a2", "lease-a", "owner1") is provider.get_lease(
            "a2", "lease-a", "owner1"
        )


async def test_return_different_instance_for_different_lease_name() -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        assert provider.get_lease("a3", "lease-a", "owner1") is not provider.get_lease(
            "a3b", "lease-a", "owner1"
        )


async def test_return_different_instance_for_different_owner_name() -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        assert provider.get_lease("a4", "lease-a", "owner1") is not provider.get_lease(
            "a4", "lease-a", "owner2"
        )


async def test_throw_if_missing_lease_class_config(caplog: pytest.LogCaptureFixture) -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="no lease implementation registered"):
                provider.get_lease("x", "lease-missing", "owner1")
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


async def test_throw_if_unknown_lease_class_config(caplog: pytest.LogCaptureFixture) -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ImportError):
                provider.get_lease("x", "lease-unknown", "owner1")
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1
        assert errors[0].name == "ohra.coordination.lease"
        assert "Invalid lease configuration" in errors[0].getMessage()


async def test_a_factory_that_raises_is_called_again() -> None:
    async with ActorTestKit() as testkit:
        provider = LeaseProvider(testkit.system)
        calls: list[int] = []

        def flaky(settings: LeaseSettings) -> Lease:
            calls.append(1)
            if len(calls) == 1:
                raise ImportError("first call")
            return LeaseA(settings)

        provider.register("flaky", flaky)
        with pytest.raises(ImportError):
            provider.get_lease("n", "flaky", "o")
        assert isinstance(provider.get_lease("n", "flaky", "o"), LeaseA)
        assert len(calls) == 2


async def test_register_after_get_lease_keeps_the_cached_lease() -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        first = provider.get_lease("n", "lease-a", "o")
        provider.register("lease-a", lambda s: LeaseB(s, testkit.system))
        assert provider.get_lease("n", "lease-a", "o") is first
        assert isinstance(provider.get_lease("m", "lease-a", "o"), LeaseB)


async def test_different_implementations_of_one_name_are_different_instances() -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        assert provider.get_lease("n", "lease-a", "o") is not provider.get_lease(
            "n", "lease-fallback-to-defaults", "o"
        )


async def test_one_provider_per_system() -> None:
    async with ActorTestKit() as testkit:
        provider = setup(testkit.system)
        assert LeaseProvider(testkit.system) is provider
        async with ActorTestKit() as other:
            assert LeaseProvider(other.system) is not provider
            with pytest.raises(ValueError, match="no lease implementation registered"):
                LeaseProvider(other.system).get_lease("a", "lease-a", "o")


def test_the_names_are_importable_from_ohra_and_ohra_coordination() -> None:
    for name in (
        "Lease",
        "LeaseProvider",
        "LeaseSettings",
        "TimeoutSettings",
        "LeaseUsageSettings",
        "LeaseError",
        "LeaseTimeoutError",
    ):
        assert getattr(ohra, name) is getattr(ohra.coordination, name)
        assert getattr(ohra, name) is getattr(lease_module, name)
