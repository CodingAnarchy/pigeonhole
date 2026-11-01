"""Pigeonhole: an embedded, single-file, wide-column store.

The Python API's snapshot (ICR 0102): `mypy.stubtest` checks these stubs against the built
module in CI.
"""

import array
import enum
from collections.abc import Callable, Iterator
from datetime import timedelta
from os import PathLike
from types import TracebackType
from typing import Any, Literal, TypeVar, final

__version__: str

Key = bytes | bytearray | memoryview | str
"""A row key, qualifier or byte value: bytes-like, or str (as UTF-8)."""
Value = Key | int | float
"""A cell value: bytes-like or str (a byte value), int (an i64), float (an f64)."""
Seconds = timedelta | int | float

_T = TypeVar("_T")

class Durability(enum.IntEnum):
    """How durable a commit is before it returns."""

    NONE = 0
    BUFFERED = 1
    GROUP_SYNC = 2
    SYNC = 3

class Element(enum.StrEnum):
    """A vector family's element type."""

    F32 = "f32"
    I8 = "i8"
    BIT = "bit"

class Metric(enum.StrEnum):
    """How a vector family compares vectors."""

    L2 = "l2"
    COSINE = "cosine"
    INNER_PRODUCT = "inner_product"
    HAMMING = "hamming"

@final
class VectorSpec:
    """A vector family's spec."""

    def __new__(cls, dim: int, element: Element | str, metric: Metric | str) -> VectorSpec: ...
    @property
    def dim(self) -> int: ...
    @property
    def element(self) -> Literal["f32", "i8", "bit", "unknown"]: ...
    @property
    def metric(self) -> Literal["l2", "cosine", "inner_product", "hamming", "unknown"]: ...

Vector = list[float] | list[int] | bytes | bytearray | memoryview | array.array[Any]
"""A vector as written: a buffer (a NumPy array, array.array) or a sequence of the family's
element type; bits: packed, bytes-like."""

@final
class Family:
    """A column family's options."""

    def __new__(
        cls,
        *,
        max_versions: int | None = None,
        ttl: Seconds | None = None,
        bloom_bits: int | None = None,
        block_size: int | None = None,
        blob_threshold: int | None = None,
        compression: Literal["lz4", "zstd", "none"] | None = None,
        zstd_level: int | None = None,
    ) -> Family: ...
    @staticmethod
    def counter(*, max_versions: int | None = None, ttl: Seconds | None = None) -> Family: ...
    @staticmethod
    def vector(
        dim: int,
        element: Element | str,
        metric: Metric | str,
        *,
        max_versions: int | None = None,
        ttl: Seconds | None = None,
    ) -> Family: ...
    @staticmethod
    def aggregate(
        kind: Literal[
            "sum",
            "min",
            "max",
            "hll",
            "bit_or",
            "bit_and",
            "bit_xor",
            "min_f64",
            "max_f64",
            "sum_f64",
            "mean_var",
            "histogram",
            "quantiles",
            "bloom",
        ],
        *,
        bounds: list[float] | None = None,
        mantissa_bits: int | None = None,
        max_buckets: int | None = None,
        bits: int | None = None,
        hashes: int | None = None,
        max_versions: int | None = None,
        ttl: Seconds | None = None,
    ) -> Family: ...

@final
class FamilyInfo:
    """A family's stored options (`Table.family_info`)."""

    @property
    def name(self) -> str: ...
    @property
    def counter(self) -> bool: ...
    @property
    def aggregation(self) -> str | None: ...
    @property
    def max_versions(self) -> int: ...
    @property
    def ttl(self) -> timedelta | None: ...
    @property
    def bloom_bits(self) -> int: ...
    @property
    def blob_threshold(self) -> int: ...
    @property
    def block_size(self) -> int: ...
    @property
    def compression(self) -> Literal["lz4", "zstd", "none", "unknown"]: ...
    @property
    def zstd_level(self) -> int | None: ...
    @property
    def merge_operator(self) -> str | None: ...
    @property
    def cache_priority(self) -> Literal["low", "normal", "high", "unknown"]: ...
    @property
    def compaction(self) -> Literal["leveled", "tiered", "fifo-by-time", "unknown"]: ...
    @property
    def vector(self) -> VectorSpec | None: ...
    @property
    def family(self) -> Family: ...

@final
class Condition:
    """A condition for `Mutation.commit_if`."""

    @staticmethod
    def absent(family: str, qualifier: Key) -> Condition: ...
    @staticmethod
    def exists(family: str, qualifier: Key) -> Condition: ...
    @staticmethod
    def equals(family: str, qualifier: Key, value: Key) -> Condition: ...

@final
class Cell:
    """One version of a cell."""

    @property
    def value(self) -> bytes: ...
    @property
    def ts(self) -> int: ...
    @property
    def typed(self) -> bytes | int | float | array.array[Any] | None:
        """The value as bytes, int, float, or for a vector cell what `as_vector` returns;
        None for a value type this binding doesn't know."""
    def as_vector(self) -> array.array[Any] | bytes | None: ...
    def as_int(self) -> int | None: ...
    def as_u64(self) -> int | None: ...
    def as_float(self) -> float | None: ...
    def hll_estimate(self) -> int | None: ...
    def sum_f64(self) -> float | None: ...
    def mean_var(self) -> tuple[int, float, float] | None: ...
    def histogram_counts(self) -> list[int] | None: ...
    def quantile(self, q: float) -> float | None: ...
    def bloom_contains(self, element: int | Key) -> bool | None: ...
    def __eq__(self, other: object, /) -> bool: ...

@final
class Row:
    """A row's key and cells; iterating gives `(family, qualifier, cell)`."""

    @property
    def key(self) -> bytes: ...
    def get(self, family: str, qualifier: Key) -> Cell | None: ...
    def cells(self) -> list[tuple[str, bytes, Cell]]: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[tuple[str, bytes, Cell]]: ...

@final
class Snapshot:
    """A consistent view of the database; release it, or use it as a context manager."""

    @property
    def seqno(self) -> int: ...
    def release(self) -> None: ...
    def __enter__(self) -> Snapshot: ...
    def __exit__(
        self,
        ty: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...

@final
class ScanIter:
    """The rows of a scan, in key order."""

    def __iter__(self) -> ScanIter: ...
    def __next__(self) -> Row: ...
    def close(self) -> None: ...
    def __enter__(self) -> ScanIter: ...
    def __exit__(
        self,
        ty: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...

@final
class Hop:
    """One hop's new nodes, sorted; with parents=True, each node's parent index."""

    @property
    def nodes(self) -> list[bytes]: ...
    @property
    def parents(self) -> list[int] | None: ...
    def __len__(self) -> int: ...

@final
class Traversal:
    """A traversal's hops, the sorted seeds (hop 1's parents index them), and its counts."""

    @property
    def hops(self) -> list[Hop]: ...
    @property
    def seeds(self) -> list[bytes]: ...
    @property
    def rows(self) -> int: ...
    @property
    def edges(self) -> int: ...
    @property
    def hops_cut(self) -> int: ...
    @property
    def cut_by_max_nodes(self) -> bool: ...

@final
class RowPosition:
    """Where a walk of a row stopped; its bytes are stable."""

    def to_bytes(self) -> bytes: ...
    @staticmethod
    def from_bytes(b: Key) -> RowPosition: ...
    def __eq__(self, other: object, /) -> bool: ...

@final
class Pages:
    """A wide row's pages, each read when it is asked for."""

    @property
    def position(self) -> RowPosition | None: ...
    def __iter__(self) -> Pages: ...
    def __next__(self) -> Row: ...

@final
class Mutation:
    """Writes to one row, committed atomically."""

    def put(self, family: str, qualifier: Key, value: Value, *, ts: int | None = None) -> Mutation: ...
    def put_u64(self, family: str, qualifier: Key, value: int, *, ts: int | None = None) -> Mutation: ...
    def put_vector(self, family: str, qualifier: Key, vector: Vector, *, ts: int | None = None) -> Mutation: ...
    def incr(self, family: str, qualifier: Key, delta: int, *, ts: int | None = None) -> Mutation: ...
    def aggregate(
        self, family: str, qualifier: Key, value: int | float | Key, *, ts: int | None = None
    ) -> Mutation: ...
    def delete(
        self, family: str | None = None, qualifier: Key | None = None, *, ts: int | None = None
    ) -> Mutation: ...
    def durability(self, durability: Durability) -> Mutation: ...
    def commit(self) -> int: ...
    def commit_if(self, condition: Condition) -> int | None: ...

@final
class Table:
    """A table: reads, and with a writer handle, writes."""

    @property
    def name(self) -> str: ...
    def families(self) -> list[str]: ...
    def family_info(self, family: str) -> FamilyInfo | None: ...
    def get(
        self, row: Key, family: str, qualifier: Key, *, snapshot: Snapshot | None = None
    ) -> Cell | None: ...
    def row(
        self,
        row: Key,
        *,
        families: list[str] | None = None,
        qualifier_prefix: Key | None = None,
        versions: int = 1,
        column_limit: int | None = None,
        snapshot: Snapshot | None = None,
    ) -> Row | None: ...
    def scan(
        self,
        prefix: Key | None = None,
        *,
        start: Key | None = None,
        end: Key | None = None,
        families: list[str] | None = None,
        qualifier_prefix: Key | None = None,
        versions: int = 1,
        limit: int | None = None,
        column_limit: int | None = None,
        snapshot: Snapshot | None = None,
        chunk: int = 256,
    ) -> ScanIter: ...
    def multi_get(
        self,
        keys: list[Key],
        *,
        families: list[str] | None = None,
        qualifier_prefix: Key | None = None,
        versions: int = 1,
        column_limit: int | None = None,
        snapshot: Snapshot | None = None,
    ) -> list[Row]: ...
    def traverse(
        self,
        seeds: list[Key],
        family: str,
        *,
        hops: int = 1,
        per_node_limit: int | None = None,
        hop_limit: int | None = None,
        max_nodes: int | None = None,
        parents: bool = False,
        qualifier_prefix: Key | None = None,
        snapshot: Snapshot | None = None,
        parallel: int | None = None,
    ) -> Traversal: ...
    def pages(
        self,
        row: Key,
        size: int,
        *,
        families: list[str] | None = None,
        qualifier_prefix: Key | None = None,
        versions: int = 1,
        snapshot: Snapshot | None = None,
        after: RowPosition | None = None,
    ) -> Pages: ...
    def mutate(self, row: Key) -> Mutation: ...
    def put(
        self,
        row: Key,
        family: str,
        qualifier: Key,
        value: Value,
        *,
        ts: int | None = None,
        durability: Durability | None = None,
    ) -> int: ...
    def incr(
        self,
        row: Key,
        family: str,
        qualifier: Key,
        delta: int,
        *,
        ts: int | None = None,
        durability: Durability | None = None,
    ) -> int: ...
    def delete(
        self,
        row: Key,
        family: str | None = None,
        qualifier: Key | None = None,
        *,
        ts: int | None = None,
        durability: Durability | None = None,
    ) -> int: ...

@final
class TableBuilder:
    """Creates or opens a table."""

    def family(self, name: str, options: Family | None = None) -> TableBuilder: ...
    def create(self) -> Table: ...
    def create_if_missing(self) -> Table: ...
    def open(self) -> Table: ...

@final
class WriteBatch:
    """Writes to any rows and tables, committed atomically."""

    def put(
        self, table: Table, row: Key, family: str, qualifier: Key, value: Value, *, ts: int | None = None
    ) -> WriteBatch: ...
    def put_u64(
        self, table: Table, row: Key, family: str, qualifier: Key, value: int, *, ts: int | None = None
    ) -> WriteBatch: ...
    def put_vector(
        self, table: Table, row: Key, family: str, qualifier: Key, vector: Vector, *, ts: int | None = None
    ) -> WriteBatch: ...
    def incr(
        self, table: Table, row: Key, family: str, qualifier: Key, delta: int, *, ts: int | None = None
    ) -> WriteBatch: ...
    def aggregate(
        self,
        table: Table,
        row: Key,
        family: str,
        qualifier: Key,
        value: int | float | Key,
        *,
        ts: int | None = None,
    ) -> WriteBatch: ...
    def delete(
        self,
        table: Table,
        row: Key,
        family: str | None = None,
        qualifier: Key | None = None,
        *,
        ts: int | None = None,
    ) -> WriteBatch: ...
    def commit(self, *, durability: Durability | None = None) -> int: ...
    def __len__(self) -> int: ...
    def __enter__(self) -> WriteBatch: ...
    def __exit__(
        self,
        ty: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...

@final
class Transaction:
    """An optimistic transaction."""

    def get(self, table: Table, row: Key, family: str, qualifier: Key) -> Cell | None: ...
    def get_row(self, table: Table, row: Key, families: list[str]) -> Row | None: ...
    def put(
        self, table: Table, row: Key, family: str, qualifier: Key, value: Value, *, ts: int | None = None
    ) -> Transaction: ...
    def put_u64(
        self, table: Table, row: Key, family: str, qualifier: Key, value: int, *, ts: int | None = None
    ) -> Transaction: ...
    def put_vector(
        self, table: Table, row: Key, family: str, qualifier: Key, vector: Vector, *, ts: int | None = None
    ) -> Transaction: ...
    def incr(
        self, table: Table, row: Key, family: str, qualifier: Key, delta: int, *, ts: int | None = None
    ) -> Transaction: ...
    def aggregate(
        self,
        table: Table,
        row: Key,
        family: str,
        qualifier: Key,
        value: int | float | Key,
        *,
        ts: int | None = None,
    ) -> Transaction: ...
    def delete(
        self,
        table: Table,
        row: Key,
        family: str | None = None,
        qualifier: Key | None = None,
        *,
        ts: int | None = None,
    ) -> Transaction: ...
    def commit(self, *, durability: Durability | None = None) -> int: ...
    def rollback(self) -> None: ...
    def __enter__(self) -> Transaction: ...
    def __exit__(
        self,
        ty: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...

@final
class Database:
    """The open database, as the writer process."""

    def table(self, name: str) -> TableBuilder: ...
    def tables(self) -> list[str]: ...
    def drop_table(self, name: str) -> None: ...
    def snapshot(self, *, durable: bool = False) -> Snapshot: ...
    def write_batch(self) -> WriteBatch: ...
    def transaction(self) -> Transaction: ...
    def transact(
        self,
        f: Callable[[Transaction], _T],
        *,
        max_attempts: int | None = None,
        deadline: Seconds | None = None,
        durability: Durability | None = None,
    ) -> _T: ...
    def flush(self) -> None: ...
    def compact(self) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Database: ...
    def __exit__(
        self,
        ty: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...

@final
class Reader:
    """The database, as a reader process of its running writer."""

    def table(self, name: str) -> Table: ...
    def tables(self) -> list[str]: ...
    def snapshot(self, *, durable: bool = False) -> Snapshot: ...
    def close(self) -> None: ...
    def __enter__(self) -> Reader: ...
    def __exit__(
        self,
        ty: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool: ...

def open(
    path: str | PathLike[str],
    *,
    create: bool = False,
    shards: int | None = None,
    durability: Durability | None = None,
    block_cache: int | None = None,
    row_cache: int | None = None,
    memtable_budget: int | None = None,
    shm_dir: str | PathLike[str] | None = None,
    read_only_fallback: bool | None = None,
) -> Database: ...
def open_reader(
    path: str | PathLike[str],
    *,
    shm_dir: str | PathLike[str] | None = None,
    block_cache: int | None = None,
    row_cache: int | None = None,
) -> Reader: ...

class Error(Exception):
    """A Pigeonhole error: `code` is the stable `ErrorCode` name, `code_num` its number."""

    code: str
    code_num: int

class Io(Error): ...
class Corruption(Error): ...
class WriterLocked(Error): ...
class ShmVersionMismatch(Error): ...
class ShmUnavailable(Error): ...
class UnsupportedFormat(Error): ...
class NetworkFilesystem(Error): ...
class TableNotFound(Error): ...
class TableExists(Error): ...
class FamilyNotFound(Error): ...
class FamilyExists(Error): ...
class UnknownMergeOperator(Error): ...
class MergeFailed(Error): ...
class Conflict(Error): ...
class ReadOnly(Error): ...
class KeyTooLarge(Error): ...
class ValueTooLarge(Error): ...
class NoSpace(Error): ...
class InvalidArgument(Error): ...
class Unsupported(Error): ...
class Closed(Error): ...
class NoReaderSlot(Error): ...
class RecordTooLarge(Error): ...
class Busy(Error): ...
class SnapshotExpired(Error): ...
class WouldDeadlock(Error): ...
class BatchTooLarge(Error): ...
class OutcomeUnknown(Error): ...
class NoWriter(Error): ...
class UnsupportedFeature(Error): ...
