"""Messages both nodes share. @remotable registers each type so it can cross the wire as JSON."""

from dataclasses import dataclass

from ohra import ActorRef, remotable


@remotable
@dataclass(frozen=True)
class Greet:
    name: str
    reply_to: ActorRef["Greeted"]


@remotable
@dataclass(frozen=True)
class Greeted:
    text: str
    served_by: str
