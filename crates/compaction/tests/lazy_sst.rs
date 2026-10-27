//! D288 PR 2: lazy SST cursors ([`SstReader::iter_lazy`]) under a [`MergingCursor`]. A lazy
//! SST cursor lands without filtering, and parks before a data block it would load (on the
//! separator of the block it leaves, or the target of a seek past it); the merge settles it
//! only up to the next source's key. The merge must yield exactly what the eager merge
//! yields, through every move, and an SST with nothing admissible near the merge's position
//! must read no block past it.

use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_compaction::MergingCursor;
use pigeonhole_format::Cursor;
use pigeonhole_format::key::{Kind, SUFFIX_LEN, encode_key};
use pigeonhole_format::manifest::FamilyOptions;
use pigeonhole_format::scan::{QualifierFilter, ScanFilter};
use pigeonhole_format::{FamilyId, SstId, TableId, TabletId};
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_pager::Pager;
use pigeonhole_sst::{ReadOptions, SstIter, SstReader, SstWriter, SstWriterOptions};
use proptest::prelude::*;

const ROWS: usize = 12;
const QUALIFIERS: [&[u8]; 4] = [b"x", b"y", b"yz", b"z"];
const KEYS: usize = ROWS * 4 * 2;

/// An internal key: twelve rows, four qualifiers, two timestamps. Small blocks then put
/// block boundaries inside rows and columns.
fn key(n: usize) -> Vec<u8> {
    let row = format!("r{:02}", n % ROWS);
    let q = QUALIFIERS[(n / ROWS) % 4];
    let ts = 1 + ((n / (ROWS * 4)) % 2) as u64;
    let mut k = Vec::new();
    encode_key(&mut k, row.as_bytes(), q, ts, 1, Kind::Put).unwrap();
    k
}

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

struct Store {
    pager: Pager,
    cache: Arc<BlockCache>,
    next_id: u64,
}

impl Store {
    fn new(cache_bytes: usize) -> Self {
        let vfs: VfsRef = SimVfs::new(288);
        Self {
            pager: Pager::create(&vfs, "/db/lazy.phdb".as_ref()).unwrap(),
            cache: Arc::new(BlockCache::new(cache_bytes, 1)),
            next_id: 1,
        }
    }

    /// Sorted `entries` as one SST of small blocks.
    fn sst(&mut self, entries: &Entries) -> Arc<SstReader> {
        let family = FamilyOptions::default().block_size(128);
        let bytes: usize = entries.iter().map(|(k, v)| k.len() + v.len() + 16).sum();
        let extent = self
            .pager
            .allocate((bytes as u64 * 2).max(64 << 10))
            .unwrap();
        let opts = SstWriterOptions::for_family(&family, TableId(1), FamilyId(1), TabletId(1));
        let id = self.next_id;
        self.next_id += 1;
        let mut w = SstWriter::new(self.pager.file().clone(), extent, SstId(id), opts);
        for (k, v) in entries {
            w.add(k, v).unwrap();
        }
        let meta = w.finish().unwrap();
        Arc::new(
            SstReader::open(
                self.pager.file().clone(),
                &meta,
                self.cache.clone(),
                Priority::Normal,
            )
            .unwrap(),
        )
    }
}

fn sources() -> impl Strategy<Value = Vec<Entries>> {
    prop::collection::vec(prop::collection::btree_set(0..KEYS, 0..40), 1..=4).prop_map(|srcs| {
        srcs.into_iter()
            .enumerate()
            .map(|(s, set): (usize, BTreeSet<usize>)| {
                let mut e: Entries = set
                    .into_iter()
                    // Mostly small values (several entries per block, so a move often
                    // lands in the same block), some large (blocks of one or two).
                    .map(|n| (key(n), vec![s as u8; if n % 5 == 0 { 40 } else { 2 }]))
                    .collect();
                e.sort();
                e
            })
            .collect()
    })
}

fn filters() -> impl Strategy<Value = ScanFilter> {
    let qualifiers = prop_oneof![
        Just(QualifierFilter::All),
        Just(QualifierFilter::Prefix(b"y".to_vec())),
        Just(QualifierFilter::Prefix(b"z".to_vec())),
        Just(QualifierFilter::Range(
            Bound::Included(b"y".to_vec()),
            Bound::Unbounded
        )),
        Just(QualifierFilter::Range(
            Bound::Excluded(b"x".to_vec()),
            Bound::Excluded(b"z".to_vec())
        )),
    ];
    let time = prop_oneof![Just(None), Just(Some((2, 2))), Just(Some((1, 1)))];
    (qualifiers, time).prop_map(|(qualifiers, time_range)| {
        let mut f = ScanFilter::all();
        f.qualifiers = qualifiers;
        f.time_range = time_range;
        f
    })
}

#[derive(Debug, Clone)]
enum Op {
    First,
    Seek(usize),
    /// To the larger of this key and the current one.
    SeekForward(usize),
    Next,
    SkipRow,
    /// Past the current entry's column: only the top moves.
    SkipColumn,
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            1 => Just(Op::First),
            2 => (0..KEYS).prop_map(Op::Seek),
            3 => (0..KEYS).prop_map(Op::SeekForward),
            8 => Just(Op::Next),
            3 => Just(Op::SkipRow),
            3 => Just(Op::SkipColumn),
        ],
        1..60,
    )
}

fn at<C: Cursor>(m: &MergingCursor<C>) -> Option<(Vec<u8>, Vec<u8>)> {
    m.valid().then(|| (m.key().to_vec(), m.value().to_vec()))
}

fn apply<C: Cursor>(m: &mut MergingCursor<C>, op: &Op, floor: Option<&[u8]>) -> Result<(), C::Error>
where
    C::Error: std::fmt::Debug,
{
    match op {
        Op::First => m.seek_to_first(),
        Op::Seek(n) => m.seek(&key(*n)),
        Op::SeekForward(n) => {
            let mut target = key(*n);
            if let Some(k) = floor {
                target = target.max(k.to_vec());
            }
            m.seek_forward(&target)
        }
        Op::Next => m.next(),
        Op::SkipRow => m.skip_row(),
        // The column of the eager merge's current entry (the floor), which both are on.
        Op::SkipColumn => match floor {
            Some(k) => m
                .skip_column(&k[..k.len() - SUFFIX_LEN])
                .map(|moved| assert!(moved)),
            None => Ok(()),
        },
    }
}

fn check(sources: Vec<Entries>, filter: ScanFilter, ops: &[Op]) -> Result<(), TestCaseError> {
    let mut store = Store::new(1 << 20);
    let ssts: Vec<_> = sources.iter().map(|e| store.sst(e)).collect();
    let mut eager = MergingCursor::new(
        ssts.iter()
            .map(|s| s.iter(filter.clone(), ReadOptions::default()))
            .collect::<Vec<SstIter>>(),
    );
    let mut lazy = MergingCursor::new(
        ssts.iter()
            .map(|s| s.iter_lazy(filter.clone(), ReadOptions::default()))
            .collect::<Vec<SstIter<true>>>(),
    );
    eager.seek_to_first().unwrap();
    lazy.seek_to_first().unwrap();
    prop_assert_eq!(at(&lazy), at(&eager), "after the first seek");
    for (step, op) in ops.iter().enumerate() {
        // Both from the eager merge's position, so the targets are the same.
        let floor = at(&eager).map(|(k, _)| k);
        apply(&mut eager, op, floor.as_deref()).unwrap();
        apply(&mut lazy, op, floor.as_deref()).unwrap();
        prop_assert_eq!(at(&lazy), at(&eager), "after step {} ({:?})", step, op);
        if let Some(c) = lazy.current() {
            prop_assert!(c.settled(), "unsettled top at step {}", step);
        }
    }
    while eager.valid() {
        prop_assert_eq!(at(&lazy), at(&eager));
        eager.next().unwrap();
        lazy.next().unwrap();
    }
    prop_assert!(!lazy.valid());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(600))]

    /// Every move of a merge over lazy SST cursors gives what the eager merge gives, with
    /// block boundaries inside rows and columns, and seek hints and row skips that cross
    /// blocks.
    #[test]
    fn a_lazy_sst_merge_yields_what_the_eager_merge_yields(
        sources in sources(),
        filter in filters(),
        ops in ops(),
    ) {
        check(sources, filter, &ops)?;
    }
}

/// A lone lazy cursor, settled with no bound, walks as the eager one does (a merge of one).
#[test]
fn a_lazy_cursor_alone_settles_fully() {
    let mut store = Store::new(1 << 20);
    let entries: Entries = (0..KEYS).map(|n| (key(n), vec![7; 30])).collect::<Vec<_>>();
    let mut entries = entries;
    entries.sort();
    let sst = store.sst(&entries);
    let mut filter = ScanFilter::all();
    filter.qualifiers = QualifierFilter::Prefix(b"z".to_vec());
    let mut eager = sst.iter(filter.clone(), ReadOptions::default());
    let mut lazy = MergingCursor::new(vec![sst.iter_lazy(filter, ReadOptions::default())]);
    eager.seek_to_first().unwrap();
    lazy.seek_to_first().unwrap();
    let mut n = 0;
    while eager.valid() {
        assert_eq!(lazy.key(), eager.key());
        eager.next().unwrap();
        lazy.next().unwrap();
        n += 1;
    }
    assert!(!lazy.valid());
    assert_eq!(n, ROWS * 2);
}

/// #711's shape over SSTs: one SST holds the scan's few admitted cells, another many blocks of
/// cells the filter rejects, interleaved with them. Eagerly the second walks (and reads) all
/// its blocks at the scan's first step; lazily it reads only the blocks the merge reaches.
#[test]
fn an_sst_with_nothing_admissible_reads_no_block_past_the_merge_frontier() {
    // No cache: every block a cursor loads is a file read.
    let mut store = Store::new(0);
    let row = |r: usize| format!("r{r:04}");
    let k = |r: usize, q: &[u8]| {
        let mut k = Vec::new();
        encode_key(&mut k, row(r).as_bytes(), q, 1, 1, Kind::Put).unwrap();
        k
    };
    // The wanted cells: rows 0 and 1 only.
    let wanted: Entries = vec![(k(0, b"b"), vec![1; 8]), (k(1, b"b"), vec![1; 8])];
    // The rest: 400 rows of `a` cells, about four per block.
    let other: Entries = (0..400).map(|r| (k(r, b"a"), vec![2; 24])).collect();
    let (wanted, other) = (store.sst(&wanted), store.sst(&other));
    let mut filter = ScanFilter::all();
    filter.qualifiers = QualifierFilter::Prefix(b"b".to_vec());
    let reads = |lazy: bool| {
        let ((), n) = pigeonhole_sst::counting_file_reads(|| {
            let seen = if lazy {
                let mut m = MergingCursor::new(vec![
                    wanted.iter_lazy(filter.clone(), ReadOptions::default()),
                    other.iter_lazy(filter.clone(), ReadOptions::default()),
                ]);
                m.seek_to_first().unwrap();
                let first = m.key().to_vec();
                m.next().unwrap();
                (first, m.key().to_vec())
            } else {
                let mut m = MergingCursor::new(vec![
                    wanted.iter(filter.clone(), ReadOptions::default()),
                    other.iter(filter.clone(), ReadOptions::default()),
                ]);
                m.seek_to_first().unwrap();
                let first = m.key().to_vec();
                m.next().unwrap();
                (first, m.key().to_vec())
            };
            assert_eq!(seen, (k(0, b"b"), k(1, b"b")));
        });
        n
    };
    let (eager, lazy) = (reads(false), reads(true));
    assert!(eager > 80, "the eager merge read {eager} blocks");
    assert!(
        lazy <= 10,
        "the lazy merge read {lazy} blocks (eager {eager})"
    );
}
