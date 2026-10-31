"""Pigeonhole: an embedded, single-file, wide-column store (BigTable's data model, SQLite's
deployment model). See the Python guide: docs/guide/python.md."""

from pigeonhole._native import *  # noqa: F401,F403
from pigeonhole._native import __version__  # noqa: F401

import enum as _enum
from datetime import timedelta as _timedelta


class Durability(_enum.IntEnum):
    """How durable a commit is before it returns (see the Durability guide)."""

    NONE = 0
    """In memory only: lost on a crash."""
    BUFFERED = 1
    """Written to the OS: survives a process crash."""
    GROUP_SYNC = 2
    """Synced with other commits in a group: survives power loss (the default)."""
    SYNC = 3
    """Synced by this commit alone."""


Key = bytes | bytearray | memoryview | str
"""A row key, qualifier or byte value: bytes-like, or str (as UTF-8)."""
Value = Key | int | float
"""A cell value: bytes-like or str (a byte value), int (an i64), float (an f64)."""
Seconds = _timedelta | int | float
"""A duration: a timedelta, or seconds."""
