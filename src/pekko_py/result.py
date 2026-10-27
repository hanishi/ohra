"""Scala-style Try for pipe_to_self and ctx.ask: ``match r: case Success(v): ... case Failure(e):
...``"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Success[R]:
    value: R


@dataclass(frozen=True, slots=True)
class Failure:
    exception: Exception


type Try[R] = Success[R] | Failure
