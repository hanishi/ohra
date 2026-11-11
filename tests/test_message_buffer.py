"""Ported from Apache Pekko's MessageBufferSpec (actor-tests/src/test/scala/org/apache/pekko/util/
MessageBufferSpec.scala), the ``MessageBufferMap`` cases:

- support contains, add, append and remove (146)
- handle multiple message buffers (165)

Not ported: the ``MessageBuffer`` cases (27-125), since ohra has no ``MessageBuffer`` class: a
buffer is a plain sequence. The senders are dropped, since a typed message has no sender.
"""

from ohra.util.message_buffer import MessageBufferMap


def test_support_contains_add_append_and_remove() -> None:
    m = MessageBufferMap[str]()
    assert not m.contains("id1")
    assert not m.get_or_empty("id1")
    assert m.total_size == 0
    m.add("id1")
    assert m.contains("id1")
    assert not m.get_or_empty("id1")
    assert m.total_size == 0
    m.append("id1", "m1")
    assert m.contains("id1")
    assert m.get_or_empty("id1")
    assert m.total_size == 1
    m.remove("id1")
    assert not m.contains("id1")
    assert not m.get_or_empty("id1")
    assert m.total_size == 0


def test_handle_multiple_message_buffers() -> None:
    m = MessageBufferMap[str]()
    for message in ("m11", "m12"):
        m.append("id1", message)
    for message in ("m21", "m22"):
        m.append("id2", message)
    assert m.total_size == 4
    assert m.size == 2
    assert m.non_empty
    seen = "".join(f"{i}->{x}:" for i, buf in m.items() for x in buf)
    assert seen == "id1->m11:id1->m12:id2->m21:id2->m22:"
