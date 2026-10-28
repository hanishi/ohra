"""Pekko's TestKitSettings, with the defaults of its reference.conf."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TestKitSettings:
    """Durations are seconds. Every default is multiplied by ``timefactor`` where it is used,
    as Pekko dilates them when it reads the config; raise it on a slow CI machine."""

    __test__ = False  # not a pytest test class

    timefactor: float = 1.0
    single_expect_default: float = 3.0
    expect_no_message_default: float = 0.1
    default_timeout: float = 5.0
    system_shutdown_default: float = 10.0
    throw_on_shutdown_timeout: bool = True

    def dilated(self, d: float) -> float:
        return d * self.timefactor

    @property
    def single_expect_default_dilated(self) -> float:
        return self.dilated(self.single_expect_default)

    @property
    def expect_no_message_default_dilated(self) -> float:
        return self.dilated(self.expect_no_message_default)

    @property
    def default_timeout_dilated(self) -> float:
        return self.dilated(self.default_timeout)

    @property
    def system_shutdown_default_dilated(self) -> float:
        return self.dilated(self.system_shutdown_default)


def seconds(d: float) -> str:
    """A duration as the failure messages print it: ``3s``, ``0.05s``."""
    return f"{d:g}s"
