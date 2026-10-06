//! Store runners and the hand-written wide-column key encoding the key-value
//! comparison runners share.

pub(crate) mod pigeonhole;

#[cfg(feature = "fjall")]
pub(crate) mod fjall;
#[cfg(feature = "rocksdb")]
pub(crate) mod rocksdb;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

use crate::BenchOp;
#[cfg(any(feature = "rocksdb", feature = "fjall"))]
use crate::workload::FAMILIES;

/// Bloom filter bits per key: Pigeonhole's default (`Family::bloom_bits`), which the
/// RocksDB runner matches. fjall builds filters by default; SQLite has none.
pub const BLOOM_BITS: u8 = 10;

/// Memory every store may use, so no engine wins by caching more than another.
///
/// | Store | Write buffer | Read cache |
/// |---|---|---|
/// | Pigeonhole | `memtable_budget` (per shard; one table is on one shard) | `block_cache` |
/// | RocksDB | `write_buffer_size` | LRU block cache |
/// | fjall | `max_memtable_size` | `cache_size` |
/// | SQLite | — | page cache (`cache_size`) of `write_buffer + cache` |
///
/// The default is 256 MiB of each: Pigeonhole's default block cache, and a write
/// buffer raised from its 64 MiB default because all data stays in memtables until the
/// engine flushes to SSTs (#37).
///
/// ```
/// use pigeonhole_bench::MemoryBudget;
///
/// let m = MemoryBudget::default();
/// assert_eq!(m.write_buffer, 256 << 20);
/// assert_eq!(m.to_string(), "write_buffer=256MiB cache=256MiB");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudget {
    /// Bytes of in-memory write buffer (memtables).
    pub write_buffer: u64,
    /// Bytes of read cache (block or page cache).
    pub cache: u64,
}

impl Default for MemoryBudget {
    fn default() -> Self {
        Self {
            write_buffer: 256 << 20,
            cache: 256 << 20,
        }
    }
}

impl std::fmt::Display for MemoryBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "write_buffer={}MiB cache={}MiB",
            self.write_buffer >> 20,
            self.cache >> 20
        )
    }
}

/// `buffered` or `sync`, for `describe()`.
pub(crate) fn durability(sync: bool) -> &'static str {
    if sync { "sync" } else { "buffered" }
}

/// What one operation read: cells and value bytes. Runners return it so tests can check
/// that every store answers the same workload with the same data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Touched {
    pub(crate) cells: u64,
    pub(crate) bytes: u64,
}

impl Touched {
    pub(crate) fn cell(&mut self, value: &[u8]) {
        self.cells += 1;
        self.bytes += value.len() as u64;
        std::hint::black_box(value);
    }
}

/// A runner whose operations report what they read.
pub(crate) trait Counted {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String>;
}

/// The value written back by a read-modify-write: the old value with its first byte
/// incremented, or 8 zero bytes when the cell is missing. The same in every runner.
pub(crate) fn modified(old: Option<&[u8]>) -> Vec<u8> {
    match old {
        Some(v) if !v.is_empty() => {
            let mut v = v.to_vec();
            v[0] = v[0].wrapping_add(1);
            v
        }
        Some(v) => v.to_vec(),
        None => vec![0; 8],
    }
}

/// Wide-column key for a sorted key-value store:
/// `escape(row) ++ [0x00, 0x01] ++ [family index] ++ qualifier`.
///
/// `escape` turns each `0x00` in the row into `0x00 0xFF`, so the terminator `0x00 0x01`
/// sorts below any continuation and the byte order of encoded keys is (row, family,
/// qualifier) order. Families are numbered by their index in [`FAMILIES`].
#[cfg(any(feature = "rocksdb", feature = "fjall", test))]
pub(crate) mod keys {
    pub(crate) fn row_prefix(row: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(row.len() + 2);
        for &b in row {
            out.push(b);
            if b == 0 {
                out.push(0xFF);
            }
        }
        out.extend_from_slice(&[0x00, 0x01]);
        out
    }

    pub(crate) fn cell(row: &[u8], family: u8, qualifier: &[u8]) -> Vec<u8> {
        let mut out = row_prefix(row);
        out.push(family);
        out.extend_from_slice(qualifier);
        out
    }

    /// Length of the encoded row prefix (through the terminator) of `key`.
    pub(crate) fn row_prefix_len(key: &[u8]) -> Option<usize> {
        let mut i = 0;
        while i + 1 < key.len() {
            if key[i] == 0 {
                match key[i + 1] {
                    0x01 => return Some(i + 2),
                    0xFF => i += 2,
                    _ => return None,
                }
            } else {
                i += 1;
            }
        }
        None
    }
}

#[cfg(any(feature = "rocksdb", feature = "fjall"))]
pub(crate) fn family_id(family: &str) -> Result<u8, String> {
    FAMILIES
        .iter()
        .position(|f| *f == family)
        .map(|i| i as u8)
        .ok_or_else(|| format!("unknown family {family}"))
}

/// Walks sorted `(key, value)` pairs from a seek to `keys::row_prefix(start)`, counting
/// cells of the first `len` rows.
#[cfg(any(feature = "rocksdb", feature = "fjall"))]
pub(crate) fn scan_rows<K: AsRef<[u8]>, V: AsRef<[u8]>, E: std::fmt::Display>(
    iter: impl Iterator<Item = Result<(K, V), E>>,
    len: u32,
) -> Result<Touched, String> {
    let mut t = Touched::default();
    let mut rows = 0u32;
    let mut current: Option<Vec<u8>> = None;
    for item in iter {
        let (k, v) = item.map_err(|e| e.to_string())?;
        let k = k.as_ref();
        let p = keys::row_prefix_len(k).ok_or("malformed key")?;
        if current.as_deref() != Some(&k[..p]) {
            if rows == len {
                break;
            }
            rows += 1;
            current = Some(k[..p].to_vec());
        }
        t.cell(v.as_ref());
    }
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::keys::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn encoding_preserves_order(
            a in proptest::collection::vec(prop_oneof![Just(0u8), Just(1u8), Just(0xFFu8), any::<u8>()], 0..8),
            b in proptest::collection::vec(prop_oneof![Just(0u8), Just(1u8), Just(0xFFu8), any::<u8>()], 0..8),
            fa in 0u8..4, fb in 0u8..4,
            qa in proptest::collection::vec(any::<u8>(), 0..4),
            qb in proptest::collection::vec(any::<u8>(), 0..4),
        ) {
            let ka = cell(&a, fa, &qa);
            let kb = cell(&b, fb, &qb);
            prop_assert_eq!(ka.cmp(&kb), (&a, fa, &qa).cmp(&(&b, fb, &qb)));
            prop_assert_eq!(row_prefix_len(&ka), Some(row_prefix(&a).len()));
            // A seek to a row prefix lands at or before every cell of rows >= that row.
            if a <= b {
                prop_assert!(row_prefix(&a) <= kb);
            }
        }
    }
}

/// Every store must read the same cells for the same workload, or the comparison is
/// meaningless.
#[cfg(all(test, any(feature = "rocksdb", feature = "sqlite", feature = "fjall")))]
mod agreement {
    use super::{Counted, Touched};
    use crate::{PigeonholeRunner, Runner, Workload, WorkloadConfig, WorkloadKind};

    fn trace<R: Runner + Counted>(mut r: R, kind: WorkloadKind, tag: &str) -> Vec<Touched> {
        let dir = std::env::temp_dir().join(format!(
            "phdb-bench-agree-{tag}-{}-{}",
            kind.name(),
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut config = WorkloadConfig::smoke(kind);
        config.operations = 1_000;
        let mut w = Workload::new(config);
        r.open(&dir).unwrap();
        for op in w.load_ops() {
            r.execute(&op).unwrap();
        }
        let out = w
            .run_ops()
            .map(|op| r.execute_counted(&op).unwrap())
            .collect();
        r.close().unwrap();
        std::fs::remove_dir_all(&dir).ok();
        out
    }

    fn agree<R: Runner + Counted>(make: impl Fn() -> R, tag: &str) {
        for kind in WorkloadKind::ALL {
            let want = trace(
                PigeonholeRunner::default()
                    .shards(1)
                    .memtable_budget(32 << 20),
                kind,
                &format!("ph-for-{tag}"),
            );
            let got = trace(make(), kind, tag);
            let read: u64 = want.iter().map(|t| t.cells).sum();
            assert!(
                read > 0 || kind == WorkloadKind::SkewedMultiShard,
                "{kind:?} reads nothing"
            );
            for (i, (w, g)) in want.iter().zip(&got).enumerate() {
                assert_eq!(
                    w, g,
                    "{kind:?}: op {i} differs between pigeonhole and {tag}"
                );
            }
        }
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_reads_what_pigeonhole_reads() {
        agree(crate::RocksDbRunner::default, "rocksdb");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_reads_what_pigeonhole_reads() {
        agree(crate::SqliteRunner::default, "sqlite");
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn fjall_reads_what_pigeonhole_reads() {
        agree(crate::FjallRunner::default, "fjall");
    }
}
