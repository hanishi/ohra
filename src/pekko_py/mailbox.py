"""Mailbox selection (org.apache.pekko.actor.typed.MailboxSelector). Pass it to ``spawn``."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

type Overflow = Literal["drop_new", "drop_old", "fail"]


class MailboxOverflowError(Exception):
    """Raised by ``tell`` on a full mailbox with ``overflow="fail"``."""


@dataclass(frozen=True, slots=True)
class MailboxSelector:
    """``capacity=None`` is unbounded. On overflow: drop the new message to dead letters
    (Pekko's behavior), drop the oldest queued one, or raise in the sender."""

    capacity: int | None = None
    overflow: Overflow = "drop_new"

    @staticmethod
    def default() -> MailboxSelector:
        return MailboxSelector()

    @staticmethod
    def bounded(capacity: int, overflow: Overflow = "drop_new") -> MailboxSelector:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        return MailboxSelector(capacity, overflow)
