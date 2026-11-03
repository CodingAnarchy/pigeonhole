"""Long-term agent memory over a Pigeonhole database (D311): the `pigeonhole-memory` store.

See the Python guide's "Agent memory" section (docs/guide/python.md) and the store's guide
(docs/guide/agent-memory.md).
"""

import enum as _enum
from collections.abc import Callable as _Callable
from collections.abc import Sequence as _Sequence

from pigeonhole._native import _memory as _native

Store = _native.Store
ReadStore = _native.ReadStore
MemoryId = _native.MemoryId
Memory = _native.Memory
EmbeddingModel = _native.EmbeddingModel
Weights = _native.Weights
ForgetPolicy = _native.ForgetPolicy
ConsolidatePolicy = _native.ConsolidatePolicy
NearDedup = _native.NearDedup
Remembered = _native.Remembered
Marked = _native.Marked
Link = _native.Link
Source = _native.Source
StoredMemory = _native.StoredMemory
StoreStats = _native.StoreStats
Signal = _native.Signal
Signals = _native.Signals
RecallHit = _native.RecallHit
RecallStats = _native.RecallStats
ForgetPass = _native.ForgetPass
ConsolidatePass = _native.ConsolidatePass
ModelMismatch = _native.ModelMismatch
ConsolidateError = _native.ConsolidateError


class Kind(_enum.StrEnum):
    """A memory's kind."""

    EPISODIC = "episodic"
    """Something that happened (the default)."""
    SEMANTIC = "semantic"
    """A fact or preference."""
    PROCEDURAL = "procedural"
    """How to do something."""


class LinkType(_enum.IntEnum):
    """The store's own link types. An application's are ints from 16 to 255."""

    DERIVED_FROM = 1
    """A memory drawn from another (a summary from its sources)."""
    SUPERSEDES = 2
    """Made by `Store.supersede`."""
    SIMILAR_TO = 3
    """Near duplicates (near dedup's `link` policy)."""


FIRST_CUSTOM_LINK_TYPE = 16
"""The first link type an application may use."""

Id = MemoryId | str
"""A memory id: a `MemoryId`, or its text form."""
LinkSpec = tuple[Id, int, float]
"""A link to write: (the linked memory, a link type, a weight from 0 to 1)."""
Consolidator = _Callable[[list[StoredMemory]], _Sequence[Memory]]
"""A consolidator: folds a group of episodic memories into memories to keep."""
Namespaces = str | _Sequence[str]
