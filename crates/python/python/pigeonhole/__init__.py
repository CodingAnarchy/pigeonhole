"""Pigeonhole: an embedded, single-file, wide-column store (BigTable's data model, SQLite's
deployment model). See the Python guide: docs/guide/python.md."""

from pigeonhole._native import *  # noqa: F401,F403
from pigeonhole._native import __version__  # noqa: F401

import array as _array
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


class Element(_enum.StrEnum):
    """A vector family's element type."""

    F32 = "f32"
    """32-bit floats."""
    I8 = "i8"
    """Signed bytes."""
    BIT = "bit"
    """Packed bits (Hamming distance)."""
    F16 = "f16"
    """IEEE half-precision floats: written as f32 (rounded to nearest-even), read back
    widened to f32."""
    BF16 = "bf16"
    """bfloat16: written as f32 (rounded to nearest-even), read back widened to f32."""


class Metric(_enum.StrEnum):
    """How a vector family compares vectors."""

    L2 = "l2"
    COSINE = "cosine"
    INNER_PRODUCT = "inner_product"
    HAMMING = "hamming"


Key = bytes | bytearray | memoryview | str
"""A row key, qualifier or byte value: bytes-like, or str (as UTF-8)."""
Value = Key | int | float
"""A cell value: bytes-like or str (a byte value), int (an i64), float (an f64)."""
Vector = list[float] | list[int] | bytes | bytearray | memoryview | _array.array
"""A vector as written. For an f32 or i8 family: a float32 (or float64) or int8 buffer, such
as a NumPy array or array.array, or a sequence of numbers; bytes are refused. For a bit
family: the packed bits, bytes-like."""
Seconds = _timedelta | int | float
"""A duration: a timedelta, or seconds."""
