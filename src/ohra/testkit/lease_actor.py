"""Pekko's ``TestLeaseActor``, ``TestLeaseActorClient`` and ``TestLeaseActorClientExt``
(``coordination/src/test/.../TestLeaseActor.scala`` 38-131): a lease whose acquire and release
requests wait in an actor until a test actions them."""

from __future__ import annotations

import logging
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..behavior import Behavior, Behaviors
from ..context import ActorContext
from ..coordination.lease import Lease, LeaseSettings
from ..patterns import ask
from ..ref import ActorRef
from ..supervision import SupervisorStrategy
from ..system import ActorSystem

log = logging.getLogger("ohra.testkit.lease_actor")


class TestLeaseActor:
    """Pekko's ``TestLeaseActor`` and its companion (``TestLeaseActor.scala`` 38-79): the
    messages are nested here, and ``behavior()`` is ``props()``."""

    __test__ = False  # not a pytest test class

    @dataclass(frozen=True)
    class Acquire:
        """``Acquire`` (``TestLeaseActor.scala`` 43)."""

        owner: str

    @dataclass(frozen=True)
    class Release:
        """``Release`` (``TestLeaseActor.scala`` 44)."""

        owner: str

    type LeaseRequest = Acquire | Release

    @dataclass(frozen=True)
    class Create:
        """``Create`` (``TestLeaseActor.scala`` 45)."""

        lease_name: str
        owner_name: str

    @dataclass(frozen=True)
    class GetRequests:
        """``GetRequests`` (``TestLeaseActor.scala`` 47); ``reply_to`` stands in for
        ``sender()``."""

        reply_to: ActorRef[TestLeaseActor.LeaseRequests]

    @dataclass(frozen=True)
    class LeaseRequests:
        """``LeaseRequests`` (``TestLeaseActor.scala`` 48), newest first."""

        requests: tuple[TestLeaseActor.LeaseRequest, ...]

    @dataclass(frozen=True)
    class ActionRequest:
        """``ActionRequest`` (``TestLeaseActor.scala`` 49): ``result`` is a bool or the
        exception to fail the request with (Pekko's ``Status.Failure``)."""

        request: TestLeaseActor.LeaseRequest
        result: bool | BaseException

    @dataclass(frozen=True)
    class Request:
        """A lease request with its ``reply_to``: ohra's stand-in for classic ``sender()``
        on a ``LeaseRequest``."""

        request: TestLeaseActor.LeaseRequest
        reply_to: ActorRef[bool | BaseException]

    type Command = Create | Request | GetRequests | ActionRequest

    @staticmethod
    def behavior() -> Behavior[TestLeaseActor.Command]:
        """``props()`` (``TestLeaseActor.scala`` 40-41) with the ``receive`` of 57-79. It is
        supervised with a restart, the classic default, so a restart forgets the requests."""

        def receive(
            requests: tuple[
                tuple[ActorRef[bool | BaseException], TestLeaseActor.LeaseRequest], ...
            ],
        ) -> Behavior[TestLeaseActor.Command]:
            def on_message(
                _ctx: ActorContext[TestLeaseActor.Command], msg: TestLeaseActor.Command
            ) -> Behavior[TestLeaseActor.Command]:
                match msg:
                    case TestLeaseActor.Create(lease_name, owner_name):
                        log.info("Lease created with name %s ownerName %s", lease_name, owner_name)
                        return Behaviors.same
                    case TestLeaseActor.Request(request, reply_to):
                        log.info("Lease request %s from %s", request, reply_to)
                        return receive(((reply_to, request), *requests))
                    case TestLeaseActor.GetRequests(reply_to):
                        reply_to.tell(TestLeaseActor.LeaseRequests(tuple(r for _, r in requests)))
                        return Behaviors.same
                    case TestLeaseActor.ActionRequest(request, result):
                        for snd, req in requests:
                            if req == request:
                                log.info("Actioning request %s to %s", req, result)
                                snd.tell(result)
                                return receive(tuple(p for p in requests if p[1] != request))
                        raise RuntimeError(
                            f"unknown request to action: {request}. Requests: {requests}"
                        )

            return Behaviors.receive(on_message)

        return Behaviors.supervise(receive(())).on_failure(SupervisorStrategy.restart())


# ponytail: a client holds its system, so an entry lives as long as the process; test-only
_client_exts: weakref.WeakKeyDictionary[ActorSystem[Any], TestLeaseActorClientExt] = (
    weakref.WeakKeyDictionary()
)


class TestLeaseActorClientExt:
    """Pekko's ``TestLeaseActorClientExt`` (``TestLeaseActor.scala`` 81-101):
    ``TestLeaseActorClientExt(system)`` is the system's one instance, holding the lease actor."""

    __test__ = False  # not a pytest test class

    def __new__(cls, system: ActorSystem[Any]) -> TestLeaseActorClientExt:
        ext = _client_exts.get(system)
        if ext is None:
            ext = object.__new__(cls)
            ext._lease_actor = None
            _client_exts[system] = ext
        return ext

    _lease_actor: ActorRef[TestLeaseActor.Command] | None

    def get_lease_actor(self) -> ActorRef[TestLeaseActor.Command]:
        """``getLeaseActor`` (``TestLeaseActor.scala`` 94-98); Pekko throws
        ``IllegalStateException``."""
        if self._lease_actor is None:
            raise RuntimeError("LeaseActorRef must be set first")
        return self._lease_actor

    def set_actor_lease(self, client: ActorRef[TestLeaseActor.Command]) -> None:
        """``setActorLease`` (``TestLeaseActor.scala`` 100)."""
        self._lease_actor = client


class TestLeaseActorClient(Lease):
    """Pekko's ``TestLeaseActorClient`` (``TestLeaseActor.scala`` 103-131): every acquire and
    release is asked of the lease actor, with a timeout of 100 s."""

    __test__ = False  # not a pytest test class

    def __init__(self, settings: LeaseSettings, system: ActorSystem[Any]) -> None:
        super().__init__(settings)
        self.lease_actor = TestLeaseActorClientExt(system).get_lease_actor()
        log.info("lease created %s", settings)
        self.lease_actor.tell(TestLeaseActor.Create(settings.lease_name, settings.owner_name))

    async def _ask(self, request: TestLeaseActor.LeaseRequest) -> bool:
        """``(leaseActor ? request).mapTo[Boolean]`` (``TestLeaseActor.scala`` 119-126): an
        exception reply is raised."""
        reply = await ask(
            self.lease_actor, lambda reply_to: TestLeaseActor.Request(request, reply_to), 100.0
        )
        if isinstance(reply, BaseException):
            raise reply
        return reply

    async def acquire(
        self, lease_lost_callback: Callable[[BaseException | None], None] | None = None
    ) -> bool:
        """``acquire()`` and ``acquire(callback)`` (``TestLeaseActor.scala`` 119-121 and
        129-130); the callback is ignored, as in Pekko."""
        return await self._ask(TestLeaseActor.Acquire(self.settings.owner_name))

    async def release(self) -> bool:
        """``release`` (``TestLeaseActor.scala`` 123-125)."""
        return await self._ask(TestLeaseActor.Release(self.settings.owner_name))

    def check_lease(self) -> bool:
        """``checkLease`` (``TestLeaseActor.scala`` 127)."""
        return False
