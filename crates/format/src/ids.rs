//! Identifier newtypes and other vocabulary shared by every layer.

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident($inner:ty)) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[repr(transparent)]
        pub struct $name(pub $inner);
    };
}

id_type!(
    /// A table, unique within one database file. Assigned by the engine, never reused.
    TableId(u32)
);
id_type!(
    /// A column family, unique across the whole database file (not just its table), so a
    /// `(TabletId, FamilyId)` pair names exactly one LSM tree. Never reused.
    FamilyId(u32)
);
id_type!(
    /// A tablet: a contiguous row range of one table. Never reused; a split retires the
    /// parent id and creates two new ones.
    TabletId(u64)
);
id_type!(
    /// An SST, unique within the database file. Never reused.
    SstId(u64)
);
id_type!(
    /// A logical blob file (a sequence of blob extents). Never reused.
    BlobFileId(u32)
);
id_type!(
    /// A WAL stream (`data.phdb-wal-N` has stream id `N`). Independent of shard numbers.
    StreamId(u32)
);
id_type!(
    /// The id of one cross-shard commit, carried by its PREPARE and COMMIT records.
    CommitId(u64)
);

/// A global MVCC sequence number. Every commit gets one; `0` is never assigned.
pub type Seqno = u64;

/// A cell timestamp. By convention microseconds since the Unix epoch (see decision D11);
/// TTL arithmetic assumes that unit.
pub type Timestamp = u64;

/// Monotonic version of the manifest; bumped by every manifest commit.
pub type ManifestVersion = u64;

/// A position in one WAL stream: `(segment epoch << 32) | byte offset within the segment`.
/// Ordered: a larger `Lsn` was written later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Builds an `Lsn` from a segment epoch and an offset within that segment.
    pub fn new(epoch: u32, offset: u32) -> Self {
        todo!()
    }

    /// The segment epoch this position falls in.
    pub fn epoch(self) -> u32 {
        todo!()
    }

    /// The byte offset within the segment.
    pub fn offset(self) -> u32 {
        todo!()
    }
}

/// How durable a commit must be before it returns. Ordered from weakest to strongest.
///
/// Resolution order: per-call override, then the writer default, then [`Durability::GroupSync`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
pub enum Durability {
    /// Memtable only. Survives nothing past the last flush.
    None = 0,
    /// Handed to the kernel with `write()`, no fsync. Survives a process crash.
    Buffered = 1,
    /// fsync shared with every committer in the same group. Survives power loss.
    #[default]
    GroupSync = 2,
    /// A dedicated fsync, never batched. Survives power loss.
    Sync = 3,
}
