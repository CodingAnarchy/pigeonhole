//! `MergingCursor` against a naive merge: each source keeps its own position and the
//! merge is on the smallest `(key, source index)`. Random sources (one, two, three and more,
//! with keys shared across sources) and random operations: seeks, forward seeks, steps and
//! row skips.

use std::collections::BTreeSet;

use pigeonhole_compaction::{MergingCursor, VecCursor};
use pigeonhole_format::Cursor;
use pigeonhole_format::key::{Kind, encode_key, row_prefix_len};
use proptest::prelude::*;

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// The reference: per-source positions, the merge on the smallest `(key, index)`.
struct Naive {
    sources: Vec<Entries>,
    pos: Vec<usize>,
}

impl Naive {
    fn key(&self, i: usize) -> Option<&[u8]> {
        self.sources[i].get(self.pos[i]).map(|(k, _)| &k[..])
    }

    fn top(&self) -> Option<usize> {
        (0..self.sources.len())
            .filter_map(|i| self.key(i).map(|k| (k, i)))
            .min()
            .map(|(_, i)| i)
    }

    fn current(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.top().map(|i| self.sources[i][self.pos[i]].clone())
    }

    fn lower_bound(&self, i: usize, target: &[u8]) -> usize {
        self.sources[i].partition_point(|(k, _)| &k[..] < target)
    }

    fn seek_to_first(&mut self) {
        self.pos.iter_mut().for_each(|p| *p = 0);
    }

    fn seek(&mut self, target: &[u8]) {
        for i in 0..self.sources.len() {
            self.pos[i] = self.lower_bound(i, target);
        }
    }

    fn seek_forward(&mut self, target: &[u8]) {
        for i in 0..self.sources.len() {
            if self.key(i).is_some_and(|k| k < target) {
                self.pos[i] = self.lower_bound(i, target);
            }
        }
    }

    fn next(&mut self) {
        if let Some(i) = self.top() {
            self.pos[i] += 1;
        }
    }

    fn skip_row(&mut self) {
        let Some(i) = self.top() else {
            return;
        };
        let key = self.sources[i][self.pos[i]].0.clone();
        let row = &key[..row_prefix_len(&key).unwrap()];
        for i in 0..self.sources.len() {
            while self.key(i).is_some_and(|k| k.starts_with(row)) {
                self.pos[i] += 1;
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    First,
    Seek(usize),
    /// To the larger of this key and the current one.
    SeekForward(usize),
    Next,
    SkipRow,
}

/// An internal key from a small space, so sources share keys.
fn key(n: usize) -> Vec<u8> {
    let row = [b"a", b"b", b"c"][n % 3];
    let q = [b"x", b"y"][(n / 3) % 2];
    let ts = 1 + ((n / 6) % 3) as u64;
    let seqno = 1 + ((n / 18) % 2) as u64;
    let mut k = Vec::new();
    encode_key(&mut k, row, q, ts, seqno, Kind::Put).unwrap();
    k
}

const KEYS: usize = 36;

fn sources() -> impl Strategy<Value = Vec<Entries>> {
    prop::collection::vec(prop::collection::btree_set(0..KEYS, 0..14), 1..=6).prop_map(|srcs| {
        srcs.into_iter()
            .enumerate()
            .map(|(s, set): (usize, BTreeSet<usize>)| {
                let mut entries: Entries = set
                    .into_iter()
                    .map(|n| (key(n), vec![s as u8, n as u8]))
                    .collect();
                entries.sort();
                entries
            })
            .collect()
    })
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            1 => Just(Op::First),
            2 => (0..KEYS).prop_map(Op::Seek),
            3 => (0..KEYS).prop_map(Op::SeekForward),
            8 => Just(Op::Next),
            2 => Just(Op::SkipRow),
        ],
        1..60,
    )
}

fn check(sources: Vec<Entries>, ops: &[Op]) -> Result<(), TestCaseError> {
    let mut naive = Naive {
        pos: vec![0; sources.len()],
        sources: sources.clone(),
    };
    let mut m = MergingCursor::new(sources.into_iter().map(VecCursor::new).collect());
    m.seek_to_first().unwrap();
    for (step, op) in ops.iter().enumerate() {
        match op {
            Op::First => {
                m.seek_to_first().unwrap();
                naive.seek_to_first();
            }
            Op::Seek(n) => {
                m.seek(&key(*n)).unwrap();
                naive.seek(&key(*n));
            }
            Op::SeekForward(n) => {
                let mut target = key(*n);
                if let Some((k, _)) = naive.current() {
                    target = target.max(k);
                }
                m.seek_forward(&target).unwrap();
                naive.seek_forward(&target);
            }
            Op::Next => {
                m.next().unwrap();
                naive.next();
            }
            Op::SkipRow => {
                m.skip_row().unwrap();
                naive.skip_row();
            }
        }
        let got = m.valid().then(|| (m.key().to_vec(), m.value().to_vec()));
        prop_assert_eq!(&got, &naive.current(), "after step {} ({:?})", step, op);
        let current = m.current().map(|c| (c.key().to_vec(), c.value().to_vec()));
        prop_assert_eq!(current, got, "current() at step {}", step);
    }
    // Drain: the rest in order, ties by source.
    while m.valid() {
        prop_assert_eq!(
            Some((m.key().to_vec(), m.value().to_vec())),
            naive.current()
        );
        m.next().unwrap();
        naive.next();
    }
    prop_assert_eq!(naive.current(), None);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn merging_cursor_matches_a_naive_merge(sources in sources(), ops in ops()) {
        check(sources, &ops)?;
    }
}

/// Equal keys across sources come out once per source, the earlier source first, with
/// two sources and with more.
#[test]
fn equal_keys_come_out_in_source_order() {
    for n in [2usize, 3, 5] {
        let sources: Vec<Entries> = (0..n)
            .map(|s| (0..4).map(|k| (key(k), vec![s as u8, k as u8])).collect())
            .collect();
        let mut m = MergingCursor::new(sources.into_iter().map(VecCursor::new).collect());
        m.seek_to_first().unwrap();
        let mut got = Vec::new();
        while m.valid() {
            got.push((m.value()[1], m.value()[0]));
            m.next().unwrap();
        }
        let mut want: Vec<(u8, u8)> = (0..4u8)
            .flat_map(|k| (0..n as u8).map(move |s| (k, s)))
            .collect();
        want.sort_by_key(|&(k, s)| (key(usize::from(k)), s));
        assert_eq!(got, want, "{n} sources");
    }
}
