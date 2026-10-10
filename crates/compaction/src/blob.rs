//! Blob separation and blob GC (FORMAT §7, issue #33).
//!
//! - [`BlobSink`] appends separated values to fresh blob files, allocating their extents from
//!   the pager. A compaction separates every kept put whose payload is longer than the
//!   family's `blob_threshold`; a blob GC job also copies the values still live in the blob
//!   files it empties.
//! - [`BlobFetch`] lets the shared [`CellResolver`](crate::CellResolver) test a value
//!   predicate on a separated value (decision D77 as amended by blob separation).
//! - [`pick_blob_gc`] chooses the blob files worth emptying from their live-byte counts.
//!
//! A blob record holds the stored value the pointer replaced, tag byte included, so a read
//! returns the record's bytes as the stored value without copying.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use pigeonhole_cache::Cell;
use pigeonhole_format::BlobFileId;
use pigeonhole_format::blob::BLOB_RECORD_HEADER_LEN;
use pigeonhole_format::key::Kind;
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::value::{BlobPointer, ValueTag};
use pigeonhole_pager::Pager;
use pigeonhole_sst::BlobWriter;

use crate::Result;
use crate::job::NewBlobFile;

/// Smallest and largest blob extent (size classes 0 and 10).
const MIN_EXTENT: u64 = 64 << 10;
const MAX_EXTENT: u64 = 64 << 20;
/// Largest extent a sink asks for unless a value needs more: a file's extents all have one
/// size class and only a one-extent file can be trimmed, so its last extent's unused tail
/// (less than this) is the space a file wastes.
const MAX_DEFAULT_EXTENT: u64 = 1 << 20;

/// Bytes of a separated value's stored form: the tag byte and the 16-byte pointer.
pub const BLOB_STORED_LEN: usize = 1 + 16;

/// Whether a put's stored value is separated under `threshold` (the family's
/// `blob_threshold`; `u32::MAX` disables separation): a `Bytes` value whose payload (the
/// bytes after the tag) is longer than the threshold. Typed values (at most nine bytes)
/// stay inline whatever the threshold, so a counter's base is never a pointer.
///
/// ```
/// use pigeonhole_compaction::separates;
/// use pigeonhole_format::key::Kind;
///
/// assert!(separates(Kind::Put, &[0; 6], 4)); // tag + 5 payload bytes > 4
/// assert!(!separates(Kind::Put, &[0; 5], 4));
/// assert!(!separates(Kind::Merge, &[0; 6], 4)); // operands stay inline
/// assert!(!separates(Kind::Put, &[0; 6], u32::MAX));
/// ```
pub fn separates(kind: Kind, stored: &[u8], threshold: u32) -> bool {
    kind == Kind::Put
        && threshold != u32::MAX
        && stored.split_first().is_some_and(|(&tag, payload)| {
            tag == ValueTag::Bytes as u8 && payload.len() as u64 > u64::from(threshold)
        })
}

/// The pointer inside a separated stored value, if `stored` is one.
pub fn blob_pointer(stored: &[u8]) -> Option<BlobPointer> {
    match stored.split_first() {
        Some((&tag, ptr)) if tag == ValueTag::Blob as u8 => BlobPointer::decode(ptr).ok(),
        _ => None,
    }
}

/// The stored form of a separated value: the `Blob` tag and the pointer.
pub fn encode_blob_stored(ptr: &BlobPointer) -> [u8; BLOB_STORED_LEN] {
    let mut out = [0u8; BLOB_STORED_LEN];
    out[0] = ValueTag::Blob as u8;
    out[1..].copy_from_slice(&ptr.encode());
    out
}

/// Counts the blob pointer of one SST entry into `refs` (kept sorted by blob file): a put
/// whose stored value is a pointer adds [`record_bytes`] of its length to its file. Writers
/// call it for every entry they add, so an SST's references (`Edit::SstBlobRefs`, #240)
/// are exact.
///
/// ```
/// use pigeonhole_compaction::{encode_blob_stored, note_blob_ref};
/// use pigeonhole_format::BlobFileId;
/// use pigeonhole_format::key::{Kind, encode_key};
/// use pigeonhole_format::value::BlobPointer;
///
/// let mut key = Vec::new();
/// encode_key(&mut key, b"r", b"q", 10, 1, Kind::Put).unwrap();
/// let ptr = BlobPointer { blob_file: BlobFileId(3), len: 100, offset: 0 };
/// let mut refs = Vec::new();
/// note_blob_ref(&mut refs, &key, &encode_blob_stored(&ptr));
/// note_blob_ref(&mut refs, &key, b"\x00inline");
/// assert_eq!(refs, [(BlobFileId(3), 116)]);
/// ```
pub fn note_blob_ref(refs: &mut Vec<(BlobFileId, u64)>, key: &[u8], stored: &[u8]) {
    if stored.first() != Some(&(ValueTag::Blob as u8)) {
        return;
    }
    if !pigeonhole_format::key::split_suffix(key).is_ok_and(|(_, _, _, k)| k == Kind::Put) {
        return;
    }
    let Some(ptr) = blob_pointer(stored) else {
        return;
    };
    let bytes = record_bytes(ptr.len);
    match refs.binary_search_by_key(&ptr.blob_file, |r| r.0) {
        Ok(i) => refs[i].1 += bytes,
        Err(i) => refs.insert(i, (ptr.blob_file, bytes)),
    }
}

/// Bytes a value of `len` stored bytes takes in its blob file: the record header and the
/// value (what `blob_live_delta` and `NewBlobFile::total_bytes` count, decision D80).
pub fn record_bytes(len: u32) -> u64 {
    BLOB_RECORD_HEADER_LEN as u64 + u64::from(len)
}

/// The size class of the smallest extent holding `bytes`.
fn class_for(bytes: u64) -> u8 {
    let mut class = 0u8;
    while class < 10 && (MIN_EXTENT << class) < bytes {
        class += 1;
    }
    class
}

/// The blob file being written.
#[derive(Debug)]
struct OpenFile {
    id: BlobFileId,
    writer: BlobWriter,
    /// Extents added so far, to give back if the sink is abandoned.
    extents: Vec<ExtentRef>,
    /// Payload bytes per extent.
    payload: u64,
    /// Logical bytes appended.
    bytes: u64,
}

/// Writes separated values into new blob files: extents come from the pager, ids from the
/// engine's blob id allocator. A file is cut once it holds `file_bytes`, or before a value
/// more than four extents long (the next file takes larger extents; `spread_values` sets
/// how many). A file of one extent
/// is trimmed to its length (`Pager::trim`, D128). Nothing is synced: the
/// root commit that publishes the files covers them, and until then they are unreferenced
/// space that an aborted job abandons (D8).
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::AtomicU32;
/// use pigeonhole_cache::BlockCache;
/// use pigeonhole_compaction::BlobSink;
/// use pigeonhole_io::VfsRef;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_pager::Pager;
/// use pigeonhole_sst::BlobReader;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let vfs: VfsRef = SimVfs::new(1);
/// let pager = Arc::new(Pager::create(&vfs, "/db".as_ref())?);
/// let mut sink = BlobSink::new(Arc::clone(&pager), Arc::new(AtomicU32::new(1)), 64 << 10, 1 << 20);
/// let mut value = vec![0u8]; // a `Bytes` stored value: tag, then payload
/// value.extend_from_slice(&[7; 10_000]);
/// let ptr = sink.append(&value)?;
/// let files = sink.finish()?;
/// assert_eq!(files[0].total_bytes, 16 + 10_001);
///
/// let r = BlobReader::new(pager.file().clone(), files[0].id, files[0].extents.clone(),
///     Arc::new(BlockCache::new(1 << 20, 1)));
/// assert_eq!(&r.read(&ptr)?[..], &value[..]);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct BlobSink {
    pager: Arc<Pager>,
    blob_ids: Arc<AtomicU32>,
    extent_bytes: u64,
    file_bytes: u64,
    /// Extents one value may span before a file takes a larger size class.
    value_extents: u64,
    open: Option<OpenFile>,
    done: Vec<NewBlobFile>,
}

impl BlobSink {
    /// A sink whose files take extents of about `extent_bytes` (rounded up to a size class,
    /// 64 KiB to 1 MiB, larger only for a value that needs it) and are cut near
    /// `file_bytes`.
    pub fn new(
        pager: Arc<Pager>,
        blob_ids: Arc<AtomicU32>,
        extent_bytes: u64,
        file_bytes: u64,
    ) -> Self {
        Self {
            pager,
            blob_ids,
            extent_bytes: extent_bytes.clamp(MIN_EXTENT, MAX_DEFAULT_EXTENT),
            file_bytes: file_bytes.max(1),
            value_extents: 4,
            open: None,
            done: Vec::new(),
        }
    }

    /// Lets one value span up to `extents` extents (default 4) before a file takes a larger
    /// size class. Only a file of one extent is trimmed, so a file's waste is the unused
    /// tail of its last extent: more extents per value bound it more tightly, for a value
    /// much larger than the sink's extents, at the cost of a longer extent list.
    pub fn spread_values(mut self, extents: u32) -> Self {
        self.value_extents = u64::from(extents.max(1));
        self
    }

    /// Appends one stored value (tag byte included) and returns its pointer.
    pub fn append(&mut self, stored: &[u8]) -> Result<BlobPointer> {
        let len = stored.len() as u64;
        if let Some(f) = &self.open
            && (f.bytes >= self.file_bytes || len > f.payload.saturating_mul(self.value_extents))
        {
            self.cut()?;
        }
        if self.open.is_none() {
            let class = class_for(
                self.extent_bytes
                    .max(len.div_ceil(self.value_extents))
                    .min(MAX_EXTENT),
            );
            let id = BlobFileId(self.blob_ids.fetch_add(1, Ordering::Relaxed));
            self.open = Some(OpenFile {
                id,
                writer: BlobWriter::new(self.pager.data_file().clone(), id, class),
                extents: Vec::new(),
                payload: (MIN_EXTENT << class)
                    - pigeonhole_format::blob::BLOB_EXTENT_HEADER_LEN as u64,
                bytes: 0,
            });
        }
        let Some(f) = &mut self.open else {
            unreachable!("opened above");
        };
        while f.writer.needs_extent(stored.len()) {
            let extent = self.pager.allocate(f.payload + 1)?;
            f.extents.push(extent);
            f.writer.add_extent(extent)?;
        }
        let ptr = f.writer.append(stored)?;
        f.bytes += record_bytes(ptr.len);
        Ok(ptr)
    }

    /// Bytes appended so far (every file).
    pub fn bytes(&self) -> u64 {
        self.done.iter().map(|f| f.total_bytes).sum::<u64>()
            + self.open.as_ref().map_or(0, |f| f.bytes)
    }

    fn cut(&mut self) -> Result<()> {
        let Some(f) = self.open.take() else {
            return Ok(());
        };
        if f.bytes == 0 {
            return Ok(());
        }
        let (mut extents, total_bytes) = f.writer.finish()?;
        if let [only] = extents.as_mut_slice() {
            // One extent: no other extent's size class to match, so give back its tail.
            *only = self.pager.trim(
                *only,
                pigeonhole_format::blob::BLOB_EXTENT_HEADER_LEN as u64 + total_bytes,
            );
        }
        self.done.push(NewBlobFile {
            id: f.id,
            extents,
            total_bytes,
        });
        Ok(())
    }

    /// Finishes the open file and returns every file written.
    pub fn finish(mut self) -> Result<Vec<NewBlobFile>> {
        if let Err(e) = self.cut() {
            self.abandon();
            return Err(e);
        }
        Ok(std::mem::take(&mut self.done))
    }

    /// Gives every extent written so far back to the pager.
    pub fn abandon(&mut self) {
        if let Some(f) = self.open.take() {
            for e in f.extents {
                self.pager.abandon(e);
            }
        }
        for f in self.done.drain(..) {
            for e in f.extents {
                self.pager.abandon(e);
            }
        }
    }
}

/// Reads separated values for the resolver's value predicates.
///
/// The resolver's error type is its cursor's, so a fetch cannot fail through it: an
/// implementation that cannot read a value returns `None` (the predicate then does not
/// match) and keeps the error for its caller to report after the resolver returns.
pub trait BlobFetch: Send + Sync + fmt::Debug {
    /// The stored value `ptr` names (tag byte included), or `None` if it cannot be read.
    fn fetch(&self, ptr: &BlobPointer) -> Option<Cell>;
}

/// What the blob GC picker knows about one blob file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobFileStat {
    /// Id.
    pub id: BlobFileId,
    /// Bytes written (`NewBlobFile::total_bytes`).
    pub total_bytes: u64,
    /// Bytes still referenced.
    pub live_bytes: u64,
}

/// The blob files of one family worth emptying: those at least half garbage, with at least
/// `min_garbage` garbage bytes, oldest first. A file with no live bytes needs no GC (the
/// engine drops it when its count reaches zero).
///
/// ```
/// use pigeonhole_compaction::{BlobFileStat, pick_blob_gc};
/// use pigeonhole_format::BlobFileId;
///
/// let stat = |id, total_bytes, live_bytes| BlobFileStat { id: BlobFileId(id), total_bytes, live_bytes };
/// let files = [stat(1, 100, 40), stat(2, 100, 90), stat(3, 100, 0)];
/// assert_eq!(pick_blob_gc(&files, 10), vec![BlobFileId(1)]);
/// ```
pub fn pick_blob_gc(files: &[BlobFileStat], min_garbage: u64) -> Vec<BlobFileId> {
    let mut out: Vec<BlobFileId> = files
        .iter()
        .filter(|f| {
            let garbage = f.total_bytes.saturating_sub(f.live_bytes);
            f.live_bytes > 0 && garbage >= min_garbage.max(1) && garbage >= f.live_bytes
        })
        .map(|f| f.id)
        .collect();
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_cache::BlockCache;
    use pigeonhole_io::VfsRef;
    use pigeonhole_io::sim::SimVfs;
    use pigeonhole_sst::BlobReader;

    fn sink(extent: u64, file: u64) -> (Arc<Pager>, BlobSink) {
        let vfs: VfsRef = SimVfs::new(3);
        let pager = Arc::new(Pager::create(&vfs, "/db".as_ref()).unwrap());
        let s = BlobSink::new(
            Arc::clone(&pager),
            Arc::new(AtomicU32::new(5)),
            extent,
            file,
        );
        (pager, s)
    }

    fn value(len: usize, fill: u8) -> Vec<u8> {
        let mut v = vec![0u8];
        v.resize(len, fill);
        v
    }

    #[test]
    fn a_spread_value_wastes_at_most_one_small_extent() {
        // 5 MiB and a bit: four extents per value take 2 MiB extents (3 MiB unused); 256
        // keep the sink's 1 MiB ones, with less than one of them unused.
        let len = (5 << 20) + 123;
        for (spread, extent_len, waste) in [(4, 2 << 20, 3 << 20), (256, 1 << 20, 1 << 20)] {
            let (pager, s) = sink(1 << 20, u64::MAX);
            let mut s = s.spread_values(spread);
            let v = value(len, 9);
            let p = s.append(&v).unwrap();
            let files = s.finish().unwrap();
            assert_eq!(files.len(), 1);
            let extents = &files[0].extents;
            assert!(extents.iter().all(|e| e.len() == extent_len), "{extents:?}");
            let held: u64 = extents.iter().map(|e| e.len()).sum();
            assert!(
                held - files[0].total_bytes < waste,
                "{spread}: {held} bytes held"
            );
            let r = BlobReader::new(
                pager.file().clone(),
                files[0].id,
                extents.clone(),
                Arc::new(BlockCache::new(1 << 20, 1)),
            );
            assert_eq!(&r.read(&p).unwrap()[..], &v[..]);
        }
    }

    #[test]
    fn files_are_cut_at_their_size_and_read_back() {
        let (pager, mut s) = sink(64 << 10, 100_000);
        let values: Vec<Vec<u8>> = (0..12).map(|i| value(20_000 + i * 7, i as u8)).collect();
        let ptrs: Vec<BlobPointer> = values.iter().map(|v| s.append(v).unwrap()).collect();
        let total = s.bytes();
        let files = s.finish().unwrap();
        assert!(files.len() >= 2, "{files:?}");
        assert_eq!(files.iter().map(|f| f.total_bytes).sum::<u64>(), total);
        let cache = Arc::new(BlockCache::new(1 << 20, 1));
        for (v, p) in values.iter().zip(&ptrs) {
            let f = files.iter().find(|f| f.id == p.blob_file).unwrap();
            let r = BlobReader::new(
                pager.file().clone(),
                f.id,
                f.extents.clone(),
                Arc::clone(&cache),
            );
            assert_eq!(&r.read(p).unwrap()[..], &v[..]);
        }
    }

    #[test]
    fn a_large_value_opens_a_file_with_larger_extents() {
        let (_pager, mut s) = sink(64 << 10, 64 << 20);
        s.append(&value(5_000, 1)).unwrap();
        s.append(&value(1 << 20, 2)).unwrap();
        let files = s.finish().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].extents[0].size_class, 0);
        assert!(files[1].extents[0].size_class >= 2, "{files:?}");
    }

    #[test]
    fn abandon_frees_every_extent() {
        let (pager, mut s) = sink(64 << 10, 50_000);
        let initial = pager.stats().allocated_bytes;
        for i in 0..6 {
            s.append(&value(30_000, i)).unwrap();
        }
        assert!(pager.stats().allocated_bytes > initial);
        s.abandon();
        assert_eq!(pager.stats().allocated_bytes, initial);
        assert!(s.finish().unwrap().is_empty());
    }

    #[test]
    fn separation_rule() {
        assert!(separates(Kind::Put, &value(10, 0), 8));
        assert!(!separates(Kind::Put, &value(9, 0), 8));
        let ptr = encode_blob_stored(&BlobPointer {
            blob_file: BlobFileId(1),
            len: 100,
            offset: 0,
        });
        assert!(!separates(Kind::Put, &ptr, 0));
        assert!(!separates(
            Kind::Put,
            &[ValueTag::I64 as u8, 1, 2, 3, 4, 5, 6, 7, 8],
            0
        ));
        assert_eq!(blob_pointer(&ptr).unwrap().len, 100);
        assert!(blob_pointer(&value(17, 0)).is_none());
    }
}
