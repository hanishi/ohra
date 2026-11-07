"""Rough estimate in bytes of some serialized data elements. Used when creating gossip messages."""

LONG_VALUE = 8
ADDRESS = 50
UNIQUE_ADDRESS = ADDRESS + LONG_VALUE
