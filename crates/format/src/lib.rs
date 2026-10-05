//! On-disk and shared-memory byte layouts for Pigeonhole: pure encode/decode, no I/O.
//!
//! Every function here is a pure transformation between bytes and values; nothing touches a
//! file, a thread or a clock. [`FORMAT.md`](https://github.com/CodingAnarchy/pigeonhole/blob/main/FORMAT.md)
//! is the normative byte-level specification; this crate is its executable form.
//!
//! The crate also hosts the small vocabulary types every layer shares (ids, [`Durability`],
//! [`Cursor`]) because it is the only crate every other crate depends on.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

pub mod blob;
pub mod block;
mod bytes;
pub mod checksum;
pub mod compress;
pub mod cursor;
pub mod error;
pub mod filter;
pub mod ids;
pub mod key;
pub mod manifest;
pub mod scan;
pub mod shm;
pub mod sst;
pub mod superblock;
pub mod value;
pub mod varint;
pub mod version;
pub mod wal;

pub use cursor::Cursor;
pub use error::{Error, Result};
pub use ids::{
    BlobFileId, Durability, FamilyId, Lsn, ManifestVersion, Seqno, SstId, StreamId, TableId,
    TabletId, Timestamp,
};
pub use key::{Kind, decode_key, encode_key};
pub use version::{FormatVersion, ShmLayoutVersion};

/// Size of a page in the main file, in bytes. Every extent and superblock is page-aligned.
pub const PAGE_SIZE: usize = 4096;
