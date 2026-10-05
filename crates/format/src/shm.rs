//! Shared-memory region layout. See `FORMAT.md` §11.
//!
//! ```text
//! [header 4 KiB][watermarks: 64 B x shards][view area: 2 buffers][reader slots: 64 B x n][arenas]
//! ```
//!
//! Fields that change while the region is live are accessed as atomics at the offsets given
//! here; `pigeonhole-shm` does the atomic access, this module only fixes the numbers.
//!
//! ```
//! use pigeonhole_format::shm::{ARENA_ALIGN, HEADER_LEN, ShmHeader};
//!
//! let h = ShmHeader::layout([1; 16], 4, 128, 4 << 20, 64 << 20, 66, 1234);
//! assert_eq!(h.arenas_off % ARENA_ALIGN as u64, 0);
//! let mut page = [0u8; HEADER_LEN];
//! h.encode(&mut page);
//! assert_eq!(ShmHeader::decode(&page).unwrap(), h);
//! ```

use crate::bytes::{Reader, le_u32, le_u64, put_at};
use crate::version::SHM_MAGIC;
use crate::{Error, FamilyId, ManifestVersion, ShmLayoutVersion, TableId, TabletId};

/// Magic of the directory region.
pub const DIRECTORY_MAGIC: [u8; 8] = *b"PHDBSHMD";

/// Size of the region header.
pub const HEADER_LEN: usize = 4096;
/// Size of one shard's watermark line.
pub const WATERMARK_STRIDE: usize = 64;
/// Size of one reader slot.
pub const READER_SLOT_LEN: usize = 64;
/// Alignment of each shard arena (so NUMA binding works on whole pages).
pub const ARENA_ALIGN: usize = 2 * 1024 * 1024;

/// Byte offsets of the header fields.
pub mod header {
    /// `[u8; 8]` [`SHM_MAGIC`](crate::version::SHM_MAGIC).
    pub const MAGIC: usize = 0;
    /// `u32` layout version.
    pub const LAYOUT_VERSION: usize = 8;
    /// `u32` header length (4096).
    pub const HEADER_LEN: usize = 12;
    /// `u64` total region length.
    pub const REGION_LEN: usize = 16;
    /// `[u8; 16]` database id from the superblock.
    pub const DB_ID: usize = 24;
    /// `u64` this region's generation (also in its name).
    pub const GENERATION: usize = 40;
    /// `u32` atomic: 0 initializing, 1 ready, 2 abandoned (a newer generation replaced it;
    /// attached processes must re-attach through the directory).
    pub const STATE: usize = 48;
    /// `u32` shard count.
    pub const SHARD_COUNT: usize = 52;
    /// `u32` reader slot count.
    pub const READER_SLOT_COUNT: usize = 56;
    /// `u32` length of one view buffer.
    pub const VIEW_BUFFER_LEN: usize = 60;
    /// `u64` atomic: current manifest version.
    pub const MANIFEST_VERSION: usize = 64;
    /// `u64` atomic: `(view_version << 1) | buffer_index` of the current view. View versions
    /// start at 1; 0 means none published yet.
    pub const VIEW_POINTER: usize = 72;
    /// `u32` writer process id.
    pub const WRITER_PID: usize = 80;
    /// `u64` writer process start time (opaque, platform-defined).
    pub const WRITER_START_TIME: usize = 88;
    /// `u64` offset of the watermark table.
    pub const WATERMARKS_OFF: usize = 96;
    /// `u64` offset of the view area.
    pub const VIEWS_OFF: usize = 104;
    /// `u64` offset of the reader-slot table.
    pub const READER_SLOTS_OFF: usize = 112;
    /// `u64` offset of shard 0's arena.
    pub const ARENAS_OFF: usize = 120;
    /// `u64` length of each shard arena.
    pub const ARENA_LEN: usize = 128;
    /// `u64` atomic: next global seqno to reserve.
    pub const NEXT_SEQNO: usize = 136;
    /// `u64` device number of the main file.
    pub const FILE_DEVICE: usize = 144;
    /// `u64` inode (file index on Windows) of the main file.
    pub const FILE_INODE: usize = 152;
}

/// Byte offsets within a watermark line.
pub mod watermark {
    /// `u64` atomic: the lowest seqno this shard holds unapplied: the minimum of its current
    /// group's lower bound and every cross-shard commit seqno it coordinates and has not yet
    /// released; `u64::MAX` when idle.
    pub const PENDING: usize = 0;
}

/// Byte offsets within a reader slot.
pub mod reader_slot {
    /// `u32` atomic: 0 free, 1 claiming, 2 active.
    pub const STATE: usize = 0;
    /// `u32` process id.
    pub const PID: usize = 4;
    /// `u64` process start time.
    pub const START_TIME: usize = 8;
    /// `u64` atomic: pinned snapshot seqno, 0 if none.
    pub const PINNED_SEQNO: usize = 16;
    /// `u64` atomic: pinned view version, 0 if none.
    pub const PINNED_VIEW: usize = 24;
    /// `u64` generation the slot was claimed under.
    pub const GENERATION: usize = 32;
}

/// The immutable part of the header, written once when the region is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmHeader {
    /// Layout version.
    pub layout_version: ShmLayoutVersion,
    /// Total region length.
    pub region_len: u64,
    /// Database id.
    pub db_id: [u8; 16],
    /// Shards (one arena and one watermark line each).
    pub shard_count: u32,
    /// Reader slots.
    pub reader_slot_count: u32,
    /// Length of each of the two view buffers.
    pub view_buffer_len: u32,
    /// Offset of the watermark table.
    pub watermarks_off: u64,
    /// Offset of the view area.
    pub views_off: u64,
    /// Offset of the reader-slot table.
    pub reader_slots_off: u64,
    /// Offset of shard 0's arena.
    pub arenas_off: u64,
    /// Length of each arena.
    pub arena_len: u64,
    /// Device of the main file.
    pub file_device: u64,
    /// Inode of the main file.
    pub file_inode: u64,
}

impl ShmHeader {
    /// Computes a layout for the given sizes, aligning each area.
    pub fn layout(
        db_id: [u8; 16],
        shard_count: u32,
        reader_slot_count: u32,
        view_buffer_len: u32,
        arena_len: u64,
        file_device: u64,
        file_inode: u64,
    ) -> Self {
        // Saturating arithmetic: an absurd request yields a layout `decode` rejects rather
        // than a panic.
        let align = |v: u64, a: u64| v.div_ceil(a).saturating_mul(a);
        let arena_len = align(arena_len, ARENA_ALIGN as u64);
        let watermarks_off = HEADER_LEN as u64;
        let views_off = watermarks_off + WATERMARK_STRIDE as u64 * u64::from(shard_count);
        let reader_slots_off = align(views_off + 2 * u64::from(view_buffer_len), 64);
        let arenas_off = align(
            reader_slots_off + READER_SLOT_LEN as u64 * u64::from(reader_slot_count),
            ARENA_ALIGN as u64,
        );
        let region_len =
            arenas_off.saturating_add(arena_len.saturating_mul(u64::from(shard_count)));
        Self {
            layout_version: ShmLayoutVersion::CURRENT,
            region_len,
            db_id,
            shard_count,
            reader_slot_count,
            view_buffer_len,
            watermarks_off,
            views_off,
            reader_slots_off,
            arenas_off,
            arena_len,
            file_device,
            file_inode,
        }
    }

    /// Encodes the immutable fields (atomic fields are left zero). `generation`, `writer_pid`
    /// and `writer_start_time` are not part of this struct; `pigeonhole-shm` writes them at
    /// their [`header`] offsets.
    pub fn encode(&self, out: &mut [u8; HEADER_LEN]) {
        out.fill(0);
        let o = &mut out[..];
        put_at(o, header::MAGIC, &SHM_MAGIC);
        put_at(
            o,
            header::LAYOUT_VERSION,
            &self.layout_version.0.to_le_bytes(),
        );
        put_at(o, header::HEADER_LEN, &(HEADER_LEN as u32).to_le_bytes());
        put_at(o, header::REGION_LEN, &self.region_len.to_le_bytes());
        put_at(o, header::DB_ID, &self.db_id);
        put_at(o, header::SHARD_COUNT, &self.shard_count.to_le_bytes());
        put_at(
            o,
            header::READER_SLOT_COUNT,
            &self.reader_slot_count.to_le_bytes(),
        );
        put_at(
            o,
            header::VIEW_BUFFER_LEN,
            &self.view_buffer_len.to_le_bytes(),
        );
        put_at(
            o,
            header::WATERMARKS_OFF,
            &self.watermarks_off.to_le_bytes(),
        );
        put_at(o, header::VIEWS_OFF, &self.views_off.to_le_bytes());
        put_at(
            o,
            header::READER_SLOTS_OFF,
            &self.reader_slots_off.to_le_bytes(),
        );
        put_at(o, header::ARENAS_OFF, &self.arenas_off.to_le_bytes());
        put_at(o, header::ARENA_LEN, &self.arena_len.to_le_bytes());
        put_at(o, header::FILE_DEVICE, &self.file_device.to_le_bytes());
        put_at(o, header::FILE_INODE, &self.file_inode.to_le_bytes());
    }

    /// Decodes and validates magic, layout version and offsets against `region_len`.
    ///
    /// The layout version must equal [`ShmLayoutVersion::CURRENT`] exactly. The areas must
    /// appear in order without overlapping, arenas must be [`ARENA_ALIGN`]-aligned and sized,
    /// and everything must lie within `region_len`.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        const WHAT: &str = "shm header";
        let Some(b) = bytes.get(..HEADER_LEN) else {
            return Err(Error::Truncated { what: WHAT });
        };
        if b[..8] != SHM_MAGIC {
            return Err(Error::BadMagic { what: WHAT });
        }
        let layout_version = ShmLayoutVersion(le_u32(b, header::LAYOUT_VERSION));
        if layout_version != ShmLayoutVersion::CURRENT {
            return Err(Error::UnsupportedVersion {
                what: WHAT,
                found: layout_version.0,
            });
        }
        let h = Self {
            layout_version,
            region_len: le_u64(b, header::REGION_LEN),
            db_id: b[header::DB_ID..header::DB_ID + 16]
                .try_into()
                .expect("16 bytes"),
            shard_count: le_u32(b, header::SHARD_COUNT),
            reader_slot_count: le_u32(b, header::READER_SLOT_COUNT),
            view_buffer_len: le_u32(b, header::VIEW_BUFFER_LEN),
            watermarks_off: le_u64(b, header::WATERMARKS_OFF),
            views_off: le_u64(b, header::VIEWS_OFF),
            reader_slots_off: le_u64(b, header::READER_SLOTS_OFF),
            arenas_off: le_u64(b, header::ARENAS_OFF),
            arena_len: le_u64(b, header::ARENA_LEN),
            file_device: le_u64(b, header::FILE_DEVICE),
            file_inode: le_u64(b, header::FILE_INODE),
        };
        let fits = |off: u64, n: u64, stride: u64, next: u64| {
            n.checked_mul(stride)
                .and_then(|len| off.checked_add(len))
                .is_some_and(|end| end <= next)
        };
        let ok = le_u32(b, header::HEADER_LEN) as usize == HEADER_LEN
            && h.watermarks_off >= HEADER_LEN as u64
            && fits(
                h.watermarks_off,
                h.shard_count.into(),
                WATERMARK_STRIDE as u64,
                h.views_off,
            )
            && fits(h.views_off, 2, h.view_buffer_len.into(), h.reader_slots_off)
            && fits(
                h.reader_slots_off,
                h.reader_slot_count.into(),
                READER_SLOT_LEN as u64,
                h.arenas_off,
            )
            && h.arenas_off.is_multiple_of(ARENA_ALIGN as u64)
            && h.arena_len.is_multiple_of(ARENA_ALIGN as u64)
            && fits(
                h.arenas_off,
                h.shard_count.into(),
                h.arena_len,
                h.region_len,
            );
        if !ok {
            return Err(Error::Corrupt { what: WHAT });
        }
        Ok(h)
    }
}

/// Name of the directory region: `phdb-` followed by 16 lowercase hex digits of
/// `xxh3_64(device LE ++ inode LE)` of the main file, identical on every process however it
/// spells the path. The directory is one page that records the current generation; its layout
/// never changes, so it is never rebuilt (decision D27).
pub fn directory_name(device: u64, inode: u64) -> String {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&device.to_le_bytes());
    id[8..].copy_from_slice(&inode.to_le_bytes());
    format!("phdb-{:016x}", crate::checksum::xxh3_64(&id))
}

/// Name of the region for `generation`: the directory name, `-`, and the generation in
/// lowercase hex (at most 31 bytes, macOS's `shm_open` limit). A rebuilt region always has a
/// new name, so an old mapping that some process still holds (Windows keeps named mappings
/// alive while any handle is open) is never mistaken for the new one.
///
/// The 31-byte bound holds for generations below `2^36` (nine hex digits).
///
/// ```
/// use pigeonhole_format::shm::{directory_name, region_name};
///
/// let dir = directory_name(66, 1234);
/// assert_eq!(dir.len(), 21);
/// assert_eq!(region_name(66, 1234, 0x2a), format!("{dir}-2a"));
/// ```
pub fn region_name(device: u64, inode: u64, generation: u64) -> String {
    format!("{}-{generation:x}", directory_name(device, inode))
}

/// Byte offsets within the directory region (4096 bytes, layout fixed forever).
pub mod directory {
    /// `[u8; 8]` magic `PHDBSHMD`.
    pub const MAGIC: usize = 0;
    /// `u32` directory format (always 1).
    pub const FORMAT: usize = 8;
    /// `u64` atomic: generation of the current region (0 = none yet).
    pub const GENERATION: usize = 16;
    /// `u32` atomic: layout version of the current region.
    pub const LAYOUT_VERSION: usize = 24;
    /// Size of the directory region.
    pub const LEN: usize = 4096;
}

/// One tablet in a published view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewTablet {
    /// Tablet.
    pub tablet: TabletId,
    /// Table.
    pub table: TableId,
    /// Owning shard.
    pub shard: u16,
    /// Inclusive start row (unescaped); empty means unbounded.
    pub start: Vec<u8>,
    /// Exclusive end row; `None` means unbounded.
    pub end: Option<Vec<u8>>,
}

/// One memtable in a published view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewMemtable {
    /// Tablet.
    pub tablet: TabletId,
    /// Family.
    pub family: FamilyId,
    /// Shard whose arena holds it.
    pub shard: u16,
    /// 0 for the active memtable, then 1, 2, ... for frozen ones, newest first.
    pub age: u8,
    /// Offset of the memtable header within the shard arena.
    pub root: u32,
}

/// A view as published in shared memory: what a reader process needs to read the writer's
/// memtables and know which manifest version goes with them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ViewRecord {
    /// View version (strictly increasing, starting at 1).
    pub view_version: u64,
    /// Manifest version this view's SST set comes from.
    pub manifest_version: ManifestVersion,
    /// The tablet map.
    pub tablets: Vec<ViewTablet>,
    /// Every memtable a reader must consult.
    pub memtables: Vec<ViewMemtable>,
}

impl ViewRecord {
    /// Encoded length, so the writer can check it against the view buffer before publishing.
    pub fn encoded_len(&self) -> usize {
        VIEW_HEADER_LEN
            + self
                .tablets
                .iter()
                .map(ViewTablet::encoded_len)
                .sum::<usize>()
            + VIEW_MEMTABLE_LEN * self.memtables.len()
    }

    /// Encodes with a CRC32C so a reader can detect a torn copy.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let start = out.len();
        out.extend_from_slice(&self.view_version.to_le_bytes());
        out.extend_from_slice(&self.manifest_version.to_le_bytes());
        out.extend_from_slice(&(self.encoded_len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.tablets.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.memtables.len() as u32).to_le_bytes());
        out.extend_from_slice(&[0; 4]); // crc, filled below
        for t in &self.tablets {
            let end = t.end.as_deref().unwrap_or_default();
            out.extend_from_slice(&t.tablet.0.to_le_bytes());
            out.extend_from_slice(&t.table.0.to_le_bytes());
            out.extend_from_slice(&t.shard.to_le_bytes());
            out.extend_from_slice(&u16::from(t.end.is_some()).to_le_bytes());
            out.extend_from_slice(&(t.start.len() as u32).to_le_bytes());
            out.extend_from_slice(&(end.len() as u32).to_le_bytes());
            out.extend_from_slice(&t.start);
            out.extend_from_slice(end);
            out.resize(start + (out.len() - start).next_multiple_of(8), 0);
        }
        for m in &self.memtables {
            out.extend_from_slice(&m.tablet.0.to_le_bytes());
            out.extend_from_slice(&m.family.0.to_le_bytes());
            out.extend_from_slice(&m.shard.to_le_bytes());
            out.push(m.age);
            out.push(0);
            out.extend_from_slice(&m.root.to_le_bytes());
            out.extend_from_slice(&[0; 4]);
        }
        let crc = crate::checksum::crc32c(&out[start..]);
        out[start + 28..start + 32].copy_from_slice(&crc.to_le_bytes());
    }

    /// Decodes and verifies a copied view buffer. `bytes` may extend past the record (a whole
    /// view buffer); `byte_len` says where it ends.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        const WHAT: &str = "shm view record";
        let Some(h) = bytes.get(..VIEW_HEADER_LEN) else {
            return Err(Error::Truncated { what: WHAT });
        };
        let byte_len = le_u32(h, 16) as usize;
        let tablet_count = le_u32(h, 20) as usize;
        let memtable_count = le_u32(h, 24) as usize;
        if byte_len < VIEW_HEADER_LEN {
            return Err(Error::Corrupt { what: WHAT });
        }
        let Some(rec) = bytes.get(..byte_len) else {
            return Err(Error::Truncated { what: WHAT });
        };
        let crc = crate::checksum::crc32c_append(
            crate::checksum::crc32c_append(crate::checksum::crc32c(&rec[..28]), &[0; 4]),
            &rec[32..],
        );
        if crc != le_u32(h, 28) {
            return Err(Error::Checksum { what: WHAT });
        }
        let body = byte_len - VIEW_HEADER_LEN;
        // Bound the allocations by what the record can hold.
        if tablet_count > body / VIEW_TABLET_FIXED_LEN || memtable_count > body / VIEW_MEMTABLE_LEN
        {
            return Err(Error::Corrupt { what: WHAT });
        }
        let mut r = Reader::new(&rec[VIEW_HEADER_LEN..], WHAT);
        let mut tablets = Vec::with_capacity(tablet_count);
        for _ in 0..tablet_count {
            let tablet = TabletId(r.u64()?);
            let table = TableId(r.u32()?);
            let shard = r.u16()?;
            let flags = r.u16()?;
            let start_len = r.u32()? as usize;
            let end_len = r.u32()? as usize;
            let start = r.take(start_len)?.to_vec();
            let end_bytes = r.take(end_len)?;
            let end = if flags & 1 != 0 {
                Some(end_bytes.to_vec())
            } else if end_len == 0 {
                None
            } else {
                return Err(Error::Corrupt { what: WHAT });
            };
            r.take(r.pos().next_multiple_of(8) - r.pos())?;
            tablets.push(ViewTablet {
                tablet,
                table,
                shard,
                start,
                end,
            });
        }
        let mut memtables = Vec::with_capacity(memtable_count);
        for _ in 0..memtable_count {
            let tablet = TabletId(r.u64()?);
            let family = FamilyId(r.u32()?);
            let shard = r.u16()?;
            let age = r.u8()?;
            r.u8()?;
            let root = r.u32()?;
            r.u32()?;
            memtables.push(ViewMemtable {
                tablet,
                family,
                shard,
                age,
                root,
            });
        }
        if r.remaining() != 0 {
            return Err(Error::Corrupt { what: WHAT });
        }
        Ok(Self {
            view_version: le_u64(h, 0),
            manifest_version: le_u64(h, 8),
            tablets,
            memtables,
        })
    }
}

/// Size of the fixed part of a view record.
const VIEW_HEADER_LEN: usize = 32;
/// Size of the fixed part of a view tablet entry (before its row bytes and padding).
const VIEW_TABLET_FIXED_LEN: usize = 24;
/// Size of a view memtable entry.
const VIEW_MEMTABLE_LEN: usize = 24;

impl ViewTablet {
    fn encoded_len(&self) -> usize {
        let rows = self.start.len() + self.end.as_ref().map_or(0, Vec::len);
        (VIEW_TABLET_FIXED_LEN + rows).next_multiple_of(8)
    }
}

/// Memtable layout inside a shard arena. Offsets are `u32`, relative to the arena base; `0` is
/// null (the first 64 bytes of every arena are reserved).
pub mod memtable {
    /// Maximum skiplist height.
    pub const MAX_HEIGHT: usize = 16;
    /// Size of the memtable header.
    pub const HEADER_LEN: usize = 64;
    /// Header: `u32` [`MEMTABLE_MAGIC`](crate::version::MEMTABLE_MAGIC).
    pub const H_MAGIC: usize = 0;
    /// Header: `u16` layout version (1).
    pub const H_VERSION: usize = 4;
    /// Header: `u8` flags (bit 0: frozen).
    pub const H_FLAGS: usize = 6;
    /// Header: `u32` offset of the head node (height [`MAX_HEIGHT`]).
    pub const H_HEAD: usize = 8;
    /// Header: `u32` atomic entry count.
    pub const H_COUNT: usize = 12;
    /// Header: `u64` atomic bytes allocated.
    pub const H_BYTES: usize = 16;
    /// Header: `u64` atomic largest seqno inserted.
    pub const H_MAX_SEQNO: usize = 24;
    /// Header: `u64` smallest seqno inserted.
    pub const H_MIN_SEQNO: usize = 32;
    /// Node: `u32` internal key length.
    pub const N_KEY_LEN: usize = 0;
    /// Node: `u32` stored value length.
    pub const N_VALUE_LEN: usize = 4;
    /// Node: `u8` height.
    pub const N_HEIGHT: usize = 8;
    /// Node: tower of `height` atomic `u32` next offsets, starting here; key bytes then value
    /// bytes follow the tower. Nodes are 4-byte aligned.
    pub const N_TOWER: usize = 12;
}
