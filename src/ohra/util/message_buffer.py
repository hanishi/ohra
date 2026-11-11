"""Port of Pekko's ``MessageBufferMap`` (actor/.../util/MessageBuffer.scala 184-310).

A typed message has no sender, so a buffer holds the messages only and is a plain list: ohra
has no ``MessageBuffer`` class. ``drop`` (Scala 233-241) is not ported, since nothing calls it
once Pekko's ``dropBufferFor`` is gone.
"""

from collections.abc import Hashable, Iterator, Sequence


class MessageBufferMap[I: Hashable]:
    """MessageBuffer.scala 184-310: message buffers by id, in the order the ids were added."""

    def __init__(self) -> None:
        self._buffers: dict[I, list[object]] = {}

    @property
    def is_empty(self) -> bool:
        """Scala 194."""
        return not self._buffers

    @property
    def non_empty(self) -> bool:
        """Scala 201."""
        return not self.is_empty

    @property
    def size(self) -> int:
        """Scala 208: the number of ids."""
        return len(self._buffers)

    @property
    def total_size(self) -> int:
        """Scala 215-222: the number of messages."""
        return sum(len(b) for b in self._buffers.values())

    def add(self, id: I) -> None:
        """Scala 235: an empty buffer for ``id`` unless it has one."""
        self._buffers.setdefault(id, [])

    def append(self, id: I, message: object) -> None:
        """Scala 246."""
        self._buffers.setdefault(id, []).append(message)

    def remove(self, id: I) -> None:
        """Scala 255."""
        self._buffers.pop(id, None)

    def contains(self, id: I) -> bool:
        """Scala 280."""
        return id in self._buffers

    def get_or_empty(self, id: I) -> Sequence[object]:
        """Scala 290: the buffer for ``id``, or an empty one."""
        return self._buffers.get(id, [])

    def items(self) -> Iterator[tuple[I, Sequence[object]]]:
        """Scala 302, ``foreach``."""
        return iter(self._buffers.items())
