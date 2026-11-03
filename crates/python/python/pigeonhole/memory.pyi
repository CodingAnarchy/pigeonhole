"""Long-term agent memory over a Pigeonhole database (D311): the `pigeonhole-memory` store.

The Python API's snapshot for `pigeonhole.memory` (ICR 0102, ICR 0107): `mypy.stubtest`
checks these stubs against the built module in CI.
"""

import enum
from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import Literal, final

from pigeonhole import Database, Element, InvalidArgument, MergeFailed, Reader, Seconds, Snapshot, Transaction

class Kind(enum.StrEnum):
    """A memory's kind."""

    EPISODIC = "episodic"
    SEMANTIC = "semantic"
    PROCEDURAL = "procedural"

class LinkType(enum.IntEnum):
    """The store's own link types. An application's are ints from 16 to 255."""

    DERIVED_FROM = 1
    SUPERSEDES = 2
    SIMILAR_TO = 3

FIRST_CUSTOM_LINK_TYPE: int

Id = MemoryId | str
"""A memory id: a `MemoryId`, or its text form."""
LinkSpec = tuple[Id, int, float]
"""A link to write: (the linked memory, a link type, a weight from 0 to 1)."""
Consolidator = Callable[[list[StoredMemory]], Sequence[Memory]]
"""A consolidator: folds a group of episodic memories into memories to keep."""

class ModelMismatch(InvalidArgument):
    """The store is pinned to another embedding model than the one named."""

class ConsolidateError(MergeFailed):
    """The consolidator failed, which stopped the pass: `consolidated` is the pass so far
    (its groups stay committed), and `__cause__` the consolidator's exception."""

    consolidated: ConsolidatePass

@final
class MemoryId:
    """A memory's id. `str()` gives its text form, which `MemoryId(text)` parses."""

    def __new__(cls, s: str) -> MemoryId: ...
    @staticmethod
    def from_bytes(b: bytes) -> MemoryId: ...
    def to_bytes(self) -> bytes: ...
    @property
    def namespace(self) -> str: ...
    @property
    def created_us(self) -> int: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...
    def __lt__(self, other: MemoryId, /) -> bool: ...
    def __le__(self, other: MemoryId, /) -> bool: ...
    def __gt__(self, other: MemoryId, /) -> bool: ...
    def __ge__(self, other: MemoryId, /) -> bool: ...
    def __hash__(self) -> int: ...

@final
class EmbeddingModel:
    """The embedding model a store is pinned to: its id, dimension and element."""

    def __new__(cls, id: str, dim: int, element_: Element | str) -> EmbeddingModel: ...
    @property
    def id(self) -> str: ...
    @property
    def dim(self) -> int: ...
    @property
    def element(self) -> Literal["f32", "i8", "bit", "f16", "bf16", "unknown"]: ...
    def __eq__(self, other: object, /) -> bool: ...

@final
class Weights:
    """Recall's signal weights: every one 1 by default, `consolidated` 0.5."""

    def __new__(
        cls,
        *,
        vector: float | None = None,
        keyword: float | None = None,
        recency: float | None = None,
        importance: float | None = None,
        uses: float | None = None,
        consolidated: float | None = None,
    ) -> Weights: ...
    @property
    def vector(self) -> float: ...
    @property
    def keyword(self) -> float: ...
    @property
    def recency(self) -> float: ...
    @property
    def importance(self) -> float: ...
    @property
    def uses(self) -> float: ...
    @property
    def consolidated(self) -> float: ...
    def __eq__(self, other: object, /) -> bool: ...

@final
class ForgetPolicy:
    """A store's forget policy; the defaults are the crate's."""

    def __new__(
        cls,
        *,
        min_retention: float | None = None,
        max_age: Mapping[Kind | Literal["episodic", "semantic", "procedural"], Seconds] | None = None,
        half_life: Seconds | None = None,
        use_weight: float | None = None,
        max_per_pass: int | None = None,
        interval: Seconds | None = None,
    ) -> ForgetPolicy: ...
    @property
    def min_retention(self) -> float | None: ...
    @property
    def max_age(self) -> list[tuple[str, timedelta]]: ...
    @property
    def half_life(self) -> timedelta: ...
    @property
    def use_weight(self) -> float: ...
    @property
    def max_per_pass(self) -> int: ...
    @property
    def interval(self) -> timedelta: ...
    def __eq__(self, other: object, /) -> bool: ...

@final
class ConsolidatePolicy:
    """How `Store.consolidate` picks and folds groups."""

    def __new__(
        cls,
        *,
        min_age: Seconds | None = None,
        group_similarity: float | None = None,
        min_group: int | None = None,
        max_group: int | None = None,
        max_groups: int | None = None,
        sources: Literal["keep", "delete", "supersede"] | None = None,
    ) -> ConsolidatePolicy: ...
    @property
    def min_age(self) -> timedelta: ...
    @property
    def group_similarity(self) -> float: ...
    @property
    def min_group(self) -> int: ...
    @property
    def max_group(self) -> int: ...
    @property
    def max_groups(self) -> int: ...
    @property
    def sources(self) -> Literal["keep", "delete", "supersede"]: ...
    def __eq__(self, other: object, /) -> bool: ...

@final
class NearDedup:
    """A store's near-duplicate dedup."""

    def __new__(
        cls, similarity: float, *, policy: Literal["link", "skip", "merge"] = "link"
    ) -> NearDedup: ...
    @property
    def similarity(self) -> float: ...
    @property
    def policy(self) -> Literal["link", "skip", "merge"]: ...
    def __eq__(self, other: object, /) -> bool: ...

@final
class Memory:
    """A memory to write: what a consolidator returns."""

    def __new__(
        cls,
        text: str,
        *,
        embedding: Sequence[float] | None = None,
        kind: Kind | Literal["episodic", "semantic", "procedural"] | None = None,
        importance: float | None = None,
        source: str | None = None,
        session: str | None = None,
        links: Sequence[LinkSpec] | None = None,
        pinned: bool = False,
    ) -> Memory: ...
    @property
    def text(self) -> str: ...

@final
class Remembered:
    """What `remember` did."""

    @property
    def id(self) -> MemoryId: ...
    @property
    def outcome(self) -> Literal["new", "merged", "similar", "unknown"]: ...

@final
class Marked:
    """What `mark_used` did: `missing` are ids that no longer exist."""

    @property
    def marked(self) -> int: ...
    @property
    def missing(self) -> list[MemoryId]: ...

@final
class Link:
    """A link: the linked memory, its type and weight."""

    @property
    def id(self) -> MemoryId: ...
    @property
    def type(self) -> int: ...
    @property
    def weight(self) -> float: ...

@final
class Source:
    """Where a memory came from, and when it was first seen (µs)."""

    @property
    def name(self) -> str: ...
    @property
    def at_us(self) -> int: ...

@final
class StoredMemory:
    """A stored memory."""

    @property
    def id(self) -> MemoryId: ...
    @property
    def text(self) -> str: ...
    @property
    def kind(self) -> Literal["episodic", "semantic", "procedural", "unknown"]: ...
    @property
    def importance(self) -> float: ...
    @property
    def uses(self) -> int: ...
    @property
    def last_used_us(self) -> int | None: ...
    @property
    def session(self) -> str | None: ...
    @property
    def pinned(self) -> bool: ...
    @property
    def consolidated(self) -> bool: ...
    @property
    def superseded_by(self) -> MemoryId | None: ...
    @property
    def sources(self) -> list[Source]: ...
    @property
    def links(self) -> list[Link]: ...
    @property
    def backlinks(self) -> list[Link]: ...
    @property
    def embedding(self) -> list[float] | None: ...

@final
class StoreStats:
    """`verify`'s counts."""

    @property
    def memories(self) -> int: ...
    @property
    def no_content(self) -> int: ...
    @property
    def stale_dedup(self) -> int: ...
    @property
    def broken_links(self) -> int: ...
    @property
    def missing_embeddings(self) -> int: ...
    @property
    def dangling_superseded(self) -> int: ...
    @property
    def text_index_inconsistent(self) -> bool: ...
    def is_consistent(self) -> bool: ...

@final
class Signal:
    """One signal of a hit: its rank among the candidates that have it, and its value."""

    @property
    def rank(self) -> int: ...
    @property
    def value(self) -> float: ...

@final
class Signals:
    """A hit's signals, each `None` when the hit lacks it."""

    @property
    def vector(self) -> Signal | None: ...
    @property
    def keyword(self) -> Signal | None: ...
    @property
    def recency(self) -> Signal | None: ...
    @property
    def importance(self) -> Signal | None: ...
    @property
    def uses(self) -> Signal | None: ...

@final
class RecallHit:
    """One recalled memory."""

    @property
    def id(self) -> MemoryId: ...
    @property
    def text(self) -> str: ...
    @property
    def kind(self) -> Literal["episodic", "semantic", "procedural", "unknown"]: ...
    @property
    def importance(self) -> float: ...
    @property
    def created_us(self) -> int: ...
    @property
    def superseded_by(self) -> MemoryId | None: ...
    @property
    def score(self) -> float: ...
    @property
    def signals(self) -> Signals | None: ...
    @property
    def embedding(self) -> list[float] | None: ...

@final
class RecallStats:
    """What a recall did."""

    @property
    def candidates(self) -> int: ...
    @property
    def truncated(self) -> bool: ...

@final
class ForgetPass:
    """A forget pass's counts, and where to resume."""

    @property
    def examined(self) -> int: ...
    @property
    def deleted(self) -> int: ...
    @property
    def protected(self) -> int: ...
    @property
    def resume(self) -> MemoryId | None: ...

@final
class ConsolidatePass:
    """A consolidation pass's counts, and where to resume."""

    @property
    def groups(self) -> int: ...
    @property
    def committed(self) -> int: ...
    @property
    def skipped(self) -> int: ...
    @property
    def created(self) -> int: ...
    @property
    def merged(self) -> int: ...
    @property
    def sources(self) -> int: ...
    @property
    def resume(self) -> MemoryId | None: ...

Namespaces = str | Sequence[str]

@final
class Store:
    """A memory store in the writer process. Close it (or use `with`) when done;
    `Database.close()` closes the stores opened on it first."""

    @staticmethod
    def create(
        db: Database,
        name: str,
        *,
        embedding: EmbeddingModel | None = None,
        forget: ForgetPolicy | None = None,
        episodic_ttl: Seconds | None = None,
        near_dedup: NearDedup | None = None,
    ) -> Store: ...
    @staticmethod
    def open(db: Database, name: str, *, model: str | None) -> Store: ...
    @staticmethod
    def open_reader(reader: Reader, name: str, *, model: str | None) -> ReadStore: ...
    def close(self) -> None: ...
    def __enter__(self) -> Store: ...
    def __exit__(
        self,
        ty: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...
    @property
    def embedding_model(self) -> EmbeddingModel | None: ...
    def remember(
        self,
        ns: str,
        text: str,
        *,
        embedding: Sequence[float] | None = None,
        kind: Kind | Literal["episodic", "semantic", "procedural"] | None = None,
        importance: float | None = None,
        source: str | None = None,
        session: str | None = None,
        links: Sequence[LinkSpec] | None = None,
        pinned: bool = False,
    ) -> Remembered: ...
    def remember_in(
        self,
        txn: Transaction,
        ns: str,
        text: str,
        *,
        embedding: Sequence[float] | None = None,
        kind: Kind | Literal["episodic", "semantic", "procedural"] | None = None,
        importance: float | None = None,
        source: str | None = None,
        session: str | None = None,
        links: Sequence[LinkSpec] | None = None,
        pinned: bool = False,
    ) -> Remembered: ...
    def delete(self, id: Id) -> bool: ...
    def delete_in(self, txn: Transaction, id: Id) -> bool: ...
    def link(self, from_: Id, to: Id, link_type: int, weight: float = 1.0) -> None: ...
    def supersede(self, old: Id, new: Id) -> None: ...
    def mark_used(self, ids: Sequence[Id]) -> Marked: ...
    def get(self, id: Id) -> StoredMemory | None: ...
    def verify(self) -> StoreStats: ...
    def recall(
        self,
        namespaces: Namespaces,
        *,
        text: str | None = None,
        embedding: Sequence[float] | None = None,
        k: int | None = None,
        candidates: int | None = None,
        byte_budget: int | None = None,
        snapshot: Snapshot | None = None,
        now_us: int | None = None,
        half_life: Seconds | None = None,
        fusion: Literal["rrf", "linear"] = "rrf",
        weights: Weights | None = None,
        include_superseded: bool = False,
        session: str | None = None,
        with_signals: bool = False,
        with_embeddings: bool = False,
    ) -> list[RecallHit]: ...
    def recall_with_stats(
        self,
        namespaces: Namespaces,
        *,
        text: str | None = None,
        embedding: Sequence[float] | None = None,
        k: int | None = None,
        candidates: int | None = None,
        byte_budget: int | None = None,
        snapshot: Snapshot | None = None,
        now_us: int | None = None,
        half_life: Seconds | None = None,
        fusion: Literal["rrf", "linear"] = "rrf",
        weights: Weights | None = None,
        include_superseded: bool = False,
        session: str | None = None,
        with_signals: bool = False,
        with_embeddings: bool = False,
    ) -> tuple[list[RecallHit], RecallStats]: ...
    def forget(
        self,
        *,
        policy: ForgetPolicy | None = None,
        after: Id | None = None,
        now_us: int | None = None,
    ) -> ForgetPass: ...
    def forget_in(
        self, txn: Transaction, id: Id, now_us: int
    ) -> Literal["deleted", "protected", "not_due", "gone", "unknown"]: ...
    def forget_policy(self) -> ForgetPolicy | None: ...
    def set_forget_policy(self, policy: ForgetPolicy | None) -> None: ...
    def near_dedup(self) -> NearDedup | None: ...
    def set_near_dedup(self, near: NearDedup | None) -> None: ...
    def near_duplicate(
        self, ns: str, text: str, *, embedding: Sequence[float] | None = None
    ) -> tuple[MemoryId, float] | None: ...
    def consolidate(
        self,
        ns: str,
        consolidator: Consolidator,
        *,
        policy: ConsolidatePolicy | None = None,
        after: Id | None = None,
        now_us: int | None = None,
    ) -> ConsolidatePass: ...

@final
class ReadStore:
    """A memory store in a reader process: recall, `get` and `verify`."""

    def close(self) -> None: ...
    def __enter__(self) -> ReadStore: ...
    def __exit__(
        self,
        ty: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...
    @property
    def embedding_model(self) -> EmbeddingModel | None: ...
    def get(self, id: Id) -> StoredMemory | None: ...
    def verify(self) -> StoreStats: ...
    def recall(
        self,
        namespaces: Namespaces,
        *,
        text: str | None = None,
        embedding: Sequence[float] | None = None,
        k: int | None = None,
        candidates: int | None = None,
        byte_budget: int | None = None,
        snapshot: Snapshot | None = None,
        now_us: int | None = None,
        half_life: Seconds | None = None,
        fusion: Literal["rrf", "linear"] = "rrf",
        weights: Weights | None = None,
        include_superseded: bool = False,
        session: str | None = None,
        with_signals: bool = False,
        with_embeddings: bool = False,
    ) -> list[RecallHit]: ...
    def recall_with_stats(
        self,
        namespaces: Namespaces,
        *,
        text: str | None = None,
        embedding: Sequence[float] | None = None,
        k: int | None = None,
        candidates: int | None = None,
        byte_budget: int | None = None,
        snapshot: Snapshot | None = None,
        now_us: int | None = None,
        half_life: Seconds | None = None,
        fusion: Literal["rrf", "linear"] = "rrf",
        weights: Weights | None = None,
        include_superseded: bool = False,
        session: str | None = None,
        with_signals: bool = False,
        with_embeddings: bool = False,
    ) -> tuple[list[RecallHit], RecallStats]: ...
