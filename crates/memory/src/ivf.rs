//! IVF recall for large namespaces (D311 T8, #1342, ICR 0107): a store created with
//! [`StoreOptions::ivf`](crate::StoreOptions::ivf) keeps a `pigeonhole-vector` index of its
//! memories' embeddings beside `emb:v`, keyed by the memory's row key, and recall searches it
//! for a namespace of at least [`IvfOptions::min_namespace_len`] memories with an embedding,
//! filtered to the namespace by its row-key prefix.

use pigeonhole::{Snapshot, Transaction};
use pigeonhole_vector::{Index as VIndex, ReadIndex as VReadIndex};

use crate::error::{Error, Result, corrupt};
use crate::layout::{EMBEDDED, STATS, count_row};
use crate::recall::Rows;

/// How a store's IVF recall works (D311 T8): set at create with
/// [`StoreOptions::ivf`](crate::StoreOptions::ivf), fixed after.
///
/// ```
/// use pigeonhole_memory::IvfOptions;
///
/// let o = IvfOptions::new().min_namespace_len(50_000).nprobe(32);
/// assert_eq!(o.get_min_namespace_len(), 50_000);
/// assert_eq!(o.get_nprobe(), Some(32));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct IvfOptions {
    min_namespace_len: u64,
    nprobe: Option<u32>,
    target_list_len: Option<u32>,
}

impl Default for IvfOptions {
    fn default() -> Self {
        IvfOptions {
            min_namespace_len: 10_000,
            nprobe: None,
            target_list_len: None,
        }
    }
}

impl IvfOptions {
    /// The defaults: the index for namespaces of 10,000 memories with an embedding or more,
    /// and the vector index's own `nprobe` and list length.
    pub fn new() -> Self {
        Self::default()
    }

    /// A namespace with fewer memories with an embedding than `n` is recalled exactly,
    /// as in a store without the index (T5: exact search stays under 15 ms at p99 up to a
    /// few thousand per namespace). At least 1.
    pub fn min_namespace_len(mut self, n: u64) -> Self {
        self.min_namespace_len = n;
        self
    }

    /// The lists a recall probes first (`pigeonhole_vector::IndexOptions::nprobe`), unless a
    /// recall sets its own ([`Recall::nprobe`](crate::Recall::nprobe)).
    pub fn nprobe(mut self, n: u32) -> Self {
        self.nprobe = Some(n);
        self
    }

    /// The index's target list length (`pigeonhole_vector::IndexOptions::target_list_len`).
    pub fn target_list_len(mut self, n: u32) -> Self {
        self.target_list_len = Some(n);
        self
    }

    /// See [`min_namespace_len`](Self::min_namespace_len).
    pub fn get_min_namespace_len(&self) -> u64 {
        self.min_namespace_len
    }

    /// See [`nprobe`](Self::nprobe): `None` for the vector index's default.
    pub fn get_nprobe(&self) -> Option<u32> {
        self.nprobe
    }

    /// See [`target_list_len`](Self::target_list_len): `None` for the vector index's
    /// default.
    pub fn get_target_list_len(&self) -> Option<u32> {
        self.target_list_len
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.min_namespace_len == 0 {
            return Err(Error::InvalidArgument(
                "an IVF min_namespace_len is at least 1".into(),
            ));
        }
        if self.nprobe == Some(0) || self.target_list_len == Some(0) {
            return Err(Error::InvalidArgument(
                "an IVF nprobe or target_list_len is at least 1".into(),
            ));
        }
        Ok(())
    }

    /// `G:meta:ivf`: a version byte (1), `min_namespace_len` (8 bytes), then `nprobe` and
    /// `target_list_len` (4 bytes each, 0 for the index's default), little endian.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut b = vec![1u8];
        b.extend_from_slice(&self.min_namespace_len.to_le_bytes());
        b.extend_from_slice(&self.nprobe.unwrap_or(0).to_le_bytes());
        b.extend_from_slice(&self.target_list_len.unwrap_or(0).to_le_bytes());
        b
    }

    pub(crate) fn decode(b: &[u8]) -> Result<IvfOptions> {
        let bad = || corrupt("the store's IVF options");
        if b.len() != 17 || b[0] != 1 {
            return Err(bad());
        }
        let min = u64::from_le_bytes(b[1..9].try_into().map_err(|_| bad())?);
        let n = u32::from_le_bytes(b[9..13].try_into().map_err(|_| bad())?);
        let t = u32::from_le_bytes(b[13..17].try_into().map_err(|_| bad())?);
        Ok(IvfOptions {
            min_namespace_len: min,
            nprobe: (n != 0).then_some(n),
            target_list_len: (t != 0).then_some(t),
        })
    }
}

/// The store's IVF index's name.
pub(crate) fn ivf_name(store: &str) -> String {
    format!("mem_{store}_ivf")
}

/// A seek into a probed list costs about this many scored cells: an estimate, not fitted
/// (perf287 on #1342 and #1424). R1 showed a prefixed search seeks, and the T8 run's cap
/// never bit. A prefixed search widens to at most `max(nprobe, count / SEEK_CELLS)` lists
/// before recall searches the namespace exactly instead.
pub(crate) const SEEK_CELLS: u64 = 64;

/// The index, in the writer process or a reader's.
#[derive(Clone)]
pub(crate) enum Ivf {
    W(VIndex, IvfOptions),
    R(VReadIndex, IvfOptions),
}

impl Ivf {
    pub(crate) fn options(&self) -> IvfOptions {
        match self {
            Ivf::W(_, o) | Ivf::R(_, o) => *o,
        }
    }

    /// The `n` nearest memory rows under `prefix` (`{ns}\0m`) at `s`, or `None` when the
    /// widening hit its cap first: the namespace is sparse among the probed lists, and an
    /// exact search of it is cheaper (perf287's per-list fallback on #1342).
    pub(crate) fn nearest(
        &self,
        s: &Snapshot,
        prefix: &[u8],
        q: &[f32],
        n: usize,
        count: u64,
        nprobe: Option<u32>,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let o = self.options();
        let first = nprobe.or(o.nprobe);
        let run = |search: pigeonhole_vector::Search<'_>| -> Result<_> {
            let search = match first {
                Some(p) => search.nprobe(p),
                None => search,
            };
            // Unset, the index's own `nprobe` is the first probe. `max_probe` never goes below
            // the first probe, so the floor of 1 here means "the index's `nprobe`" (perf287's
            // nit on #1424; `IndexOptions` doesn't expose `nprobe`).
            let first = first.unwrap_or(1);
            let cap = u32::try_from(count / SEEK_CELLS)
                .unwrap_or(u32::MAX)
                .max(first);
            Ok(search
                .id_prefix(prefix)
                .max_probe(cap)
                .snapshot(s)
                .run_with_stats()?)
        };
        let (hits, stats) = match self {
            Ivf::W(i, _) => run(i.search_f32(q, n))?,
            Ivf::R(i, _) => run(i.search_f32(q, n))?,
        };
        if hits.len() < n && stats.lists_probed < stats.lists {
            return Ok(None);
        }
        Ok(Some(hits.into_iter().map(|h| h.id).collect()))
    }
}

/// The namespace counter at `s`: memories with an embedding (0 when absent).
pub(crate) fn count(rows: &Rows<'_>, s: &Snapshot, ns: &str) -> Result<u64> {
    let n = rows.counter_at(s, &count_row(ns), STATS, EMBEDDED)?;
    Ok(n.unwrap_or(0).max(0) as u64)
}

/// The counter's blind increment in a write (no read: concurrent remembers don't conflict
/// through it, simp724 on #1342).
pub(crate) fn counted(txn: &mut Transaction, t: &pigeonhole::Table, ns: &str, by: i64) {
    txn.incr(t, &count_row(ns), STATS, EMBEDDED, by);
}
