"""Pekko's ``org.apache.pekko.util`` internals that ohra ports: the recency and frequency lists
the entity passivation strategies track entities with. Internal API, as in Pekko."""

from collections.abc import Callable

type Clock = Callable[[], float]
"""Pekko's ``Clock`` (Clock.scala 55-68): the current time in seconds. Pekko's
``earlierTime(duration)`` is ``clock() - duration`` at each call site, and ``NanoClock`` is
``time.monotonic``."""
