"""Pigeonhole: an embedded, single-file, wide-column store (BigTable's data model, SQLite's
deployment model). See the Python guide: docs/guide/python.md."""

from pigeonhole._native import *  # noqa: F401,F403
from pigeonhole._native import __version__  # noqa: F401

from datetime import timedelta as _timedelta

Key = bytes | bytearray | memoryview | str
"""A row key, qualifier or byte value: bytes-like, or str (as UTF-8)."""
Value = Key | int | float
"""A cell value: bytes-like or str (a byte value), int (an i64), float (an f64)."""
Seconds = _timedelta | int | float
"""A duration: a timedelta, or seconds."""
