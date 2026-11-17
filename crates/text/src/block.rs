//! A packed block of postings (#1585, ICR 0143, index layout version 2): the value of one
//! `postb` cell.
//!
//! A block holds up to [`MAX_POSTINGS`] of one term's postings in document order, so a walk
//! pays the engine's per-cell cost once a block and not once a posting. Its bytes:
//!
//! | part | content |
//! |---|---|
//! | header | [`FORMAT`], the posting count `n` (varint), flags (bit 0: positions) |
//! | bound | the competitive pairs: a count, then that many `(tf, len)` varint pairs |
//! | ids | `n` ids, each front-coded against the one before: shared length, suffix length, suffix |
//! | `tf` | `n` varints |
//! | `len` | `n` varints |
//! | positions | with the flag: `n` byte lengths, then the lists (ascending LEB128 deltas) |
//!
//! **The bound.** The competitive pairs are the postings no other posting in the block
//! matches or beats on both a higher frequency and a shorter document. BM25's frequency
//! factor rises with frequency and falls with length whatever the corpus's average length
//! is, so the block's best factor is the best over those pairs: an exact bound for any
//! statistics, a cluster's global ones included (#1722).

use crate::error::{Error, Result};
use crate::maxscore::Bm25;

/// The block format this build writes and reads.
pub(crate) const FORMAT: u8 = 1;
/// The most postings a block holds.
pub(crate) const MAX_POSTINGS: usize = 128;
/// A block is closed once its value reaches this many bytes (ai1249 on the design): a
/// document with a large frequency carries long position lists, and a value should stay well
/// under a data block. One posting larger than this is a block alone.
pub(crate) const MAX_BYTES: usize = 4096;

const HAS_POSITIONS: u8 = 1;

fn corrupt(what: &str) -> Error {
    Error::Corruption(format!("a posting block {what}"))
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// A varint at `at`, which it moves past. At most 5 bytes: every number in a block fits
/// 32 bits.
#[inline]
fn varint(bytes: &[u8], at: &mut usize) -> Result<u32> {
    let mut v = 0u32;
    for shift in [0u32, 7, 14, 21, 28] {
        let Some(&b) = bytes.get(*at) else {
            return Err(corrupt("is cut short"));
        };
        *at += 1;
        if shift == 28 && b > 0x0f {
            return Err(corrupt("holds a number over 32 bits"));
        }
        v |= u32::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(corrupt("holds a number over 32 bits"))
}

/// One posting going into a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Posting<'a> {
    pub(crate) id: &'a [u8],
    pub(crate) tf: u32,
    pub(crate) len: u32,
    /// Its encoded position list ([`encode_positions`](crate::layout::encode_positions));
    /// empty in an index without positions.
    pub(crate) positions: &'a [u8],
}

/// Builds blocks from postings pushed in ascending id order.
#[derive(Debug, Default)]
pub(crate) struct Builder {
    positions: bool,
    n: usize,
    /// The sections, kept apart until the block is closed.
    ids: Vec<u8>,
    last_id: Vec<u8>,
    tfs: Vec<u8>,
    lens: Vec<u8>,
    position_lens: Vec<u8>,
    position_bytes: Vec<u8>,
    /// Every posting's `(tf, len)`, for the competitive pairs.
    pairs: Vec<(u32, u32)>,
}

impl Builder {
    pub(crate) fn new(positions: bool) -> Self {
        Builder {
            positions,
            ..Self::default()
        }
    }

    /// The postings in the block being built.
    pub(crate) fn len(&self) -> usize {
        self.n
    }

    fn bytes(&self) -> usize {
        self.ids.len()
            + self.tfs.len()
            + self.lens.len()
            + self.position_lens.len()
            + self.position_bytes.len()
    }

    /// Whether the block is full: the next posting starts another.
    pub(crate) fn full(&self) -> bool {
        self.n >= MAX_POSTINGS || self.bytes() >= MAX_BYTES
    }

    /// Adds `p`, whose id is above the last one pushed.
    pub(crate) fn push(&mut self, p: Posting<'_>) {
        debug_assert!(self.n == 0 || self.last_id.as_slice() < p.id, "ids ascend");
        debug_assert!(p.tf > 0, "a block holds no tombstone");
        let shared = self
            .last_id
            .iter()
            .zip(p.id)
            .take_while(|(a, b)| a == b)
            .count();
        put_varint(&mut self.ids, shared as u64);
        put_varint(&mut self.ids, (p.id.len() - shared) as u64);
        self.ids.extend_from_slice(&p.id[shared..]);
        self.last_id.truncate(shared);
        self.last_id.extend_from_slice(&p.id[shared..]);
        put_varint(&mut self.tfs, u64::from(p.tf));
        put_varint(&mut self.lens, u64::from(p.len));
        if self.positions {
            put_varint(&mut self.position_lens, p.positions.len() as u64);
            self.position_bytes.extend_from_slice(p.positions);
        }
        self.pairs.push((p.tf, p.len));
        self.n += 1;
    }

    /// Closes the block: its qualifier (the last id) and its value. `None` when it is
    /// empty. The builder is ready for the next block.
    pub(crate) fn finish(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.n == 0 {
            return None;
        }
        let size = self.bytes();
        let competitive = competitive(&mut self.pairs);
        let mut out = Vec::with_capacity(size + 8 + 4 * competitive.len());
        out.push(FORMAT);
        put_varint(&mut out, self.n as u64);
        out.push(if self.positions { HAS_POSITIONS } else { 0 });
        put_varint(&mut out, competitive.len() as u64);
        for &(tf, len) in competitive {
            put_varint(&mut out, u64::from(tf));
            put_varint(&mut out, u64::from(len));
        }
        for part in [
            &self.ids,
            &self.tfs,
            &self.lens,
            &self.position_lens,
            &self.position_bytes,
        ] {
            out.extend_from_slice(part);
        }
        let qualifier = std::mem::take(&mut self.last_id);
        for part in [
            &mut self.ids,
            &mut self.tfs,
            &mut self.lens,
            &mut self.position_lens,
            &mut self.position_bytes,
        ] {
            part.clear();
        }
        self.pairs.clear();
        self.n = 0;
        Some((qualifier, out))
    }
}

/// The competitive pairs of `pairs`, which it reorders: those no other pair matches or
/// beats on both a frequency at least as high and a length at least as short. Highest
/// frequency first.
fn competitive(pairs: &mut Vec<(u32, u32)>) -> &[(u32, u32)] {
    // Highest frequency first, the shortest length first among equals: a pair is then
    // competitive exactly when it is shorter than every pair before it.
    pairs.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut kept = 0;
    let mut shortest = u32::MAX;
    for i in 0..pairs.len() {
        let (tf, len) = pairs[i];
        if kept == 0 || len < shortest {
            shortest = len;
            pairs[kept] = (tf, len);
            kept += 1;
        }
    }
    pairs.truncate(kept);
    pairs
}

/// A decoded block: its postings as columns, in buffers reused from one block to the next.
#[derive(Debug, Default)]
pub(crate) struct Block {
    n: usize,
    /// The ids, one after another, and where each ends.
    ids: Vec<u8>,
    id_ends: Vec<u32>,
    tf: Vec<u32>,
    len: Vec<u32>,
    /// Where each posting's position list ends in `positions`; empty without positions.
    position_ends: Vec<u32>,
    positions: Vec<u8>,
}

impl Block {
    /// A block's posting count and its bound under `bm`, from its header alone: the largest
    /// BM25 frequency factor any of its postings has (the module doc). A walk passes a
    /// block whose bound can't reach the top `k` without decoding it.
    pub(crate) fn peek_bound(bytes: &[u8], bm: &Bm25) -> Result<(usize, f64)> {
        match bytes.first() {
            Some(&FORMAT) => {}
            Some(other) => {
                return Err(Error::Mismatch(format!(
                    "a posting block has format {other}, and this build reads format {FORMAT}"
                )));
            }
            None => return Err(corrupt("is empty")),
        }
        let mut at = 1;
        let n = varint(bytes, &mut at)? as usize;
        // The flags.
        at += 1;
        let pairs = varint(bytes, &mut at)? as usize;
        if n == 0 || n > MAX_POSTINGS || pairs == 0 || pairs > n {
            return Err(corrupt("has no postings or no bound"));
        }
        let mut bound = 0.0f64;
        for _ in 0..pairs {
            let tf = varint(bytes, &mut at)?;
            let len = varint(bytes, &mut at)?;
            bound = bound.max(bm.tf_factor(f64::from(tf), f64::from(len)));
        }
        Ok((n, bound))
    }

    /// Decodes `bytes` into the block, replacing what it held.
    pub(crate) fn decode(&mut self, bytes: &[u8]) -> Result<()> {
        self.decode_parts(bytes, true)
    }

    /// [`decode`](Self::decode) without the positions, for a reader of postings only (a
    /// term's walk): the position lists are most of a block's bytes, and are neither copied
    /// nor checked. [`positions`](Self::positions) is then empty for every posting.
    pub(crate) fn decode_postings(&mut self, bytes: &[u8]) -> Result<()> {
        self.decode_parts(bytes, false)
    }

    fn decode_parts(&mut self, bytes: &[u8], positions: bool) -> Result<()> {
        self.n = 0;
        self.ids.clear();
        self.id_ends.clear();
        self.tf.clear();
        self.len.clear();
        self.position_ends.clear();
        self.positions.clear();
        match bytes.first() {
            Some(&FORMAT) => {}
            Some(other) => {
                return Err(Error::Mismatch(format!(
                    "a posting block has format {other}, and this build reads format {FORMAT}"
                )));
            }
            None => return Err(corrupt("is empty")),
        }
        let mut at = 1;
        let n = varint(bytes, &mut at)? as usize;
        if n == 0 || n > MAX_POSTINGS {
            return Err(corrupt("holds no postings, or too many"));
        }
        let Some(&flags) = bytes.get(at) else {
            return Err(corrupt("is cut short"));
        };
        at += 1;
        let pairs = varint(bytes, &mut at)? as usize;
        if pairs == 0 || pairs > n {
            return Err(corrupt("has no bound"));
        }
        for _ in 0..pairs {
            // The bound: read from the header alone ([`peek_bound`](Self::peek_bound)).
            varint(bytes, &mut at)?;
            varint(bytes, &mut at)?;
        }
        let mut last = 0usize;
        for _ in 0..n {
            let shared = varint(bytes, &mut at)? as usize;
            let suffix = varint(bytes, &mut at)? as usize;
            if shared > self.ids.len() - last {
                return Err(corrupt("has an id sharing more than the one before it"));
            }
            let Some(tail) = bytes.get(at..at + suffix) else {
                return Err(corrupt("is cut short"));
            };
            at += suffix;
            let start = self.ids.len();
            self.ids.extend_from_within(last..last + shared);
            self.ids.extend_from_slice(tail);
            // Ascending, so a seek's "first id at or after" is a scan forward.
            if self.n > 0 && self.ids[last..start] >= self.ids[start..] {
                return Err(corrupt("has ids out of order"));
            }
            last = start;
            self.id_ends.push(self.ids.len() as u32);
            self.n += 1;
        }
        for _ in 0..n {
            let tf = varint(bytes, &mut at)?;
            if tf == 0 {
                return Err(corrupt("holds a posting with no frequency"));
            }
            self.tf.push(tf);
        }
        for _ in 0..n {
            self.len.push(varint(bytes, &mut at)?);
        }
        if !positions {
            return Ok(());
        }
        if flags & HAS_POSITIONS != 0 {
            let mut end = 0u32;
            for _ in 0..n {
                end = end
                    .checked_add(varint(bytes, &mut at)?)
                    .ok_or_else(|| corrupt("has position lists too long"))?;
                self.position_ends.push(end);
            }
            let Some(lists) = bytes.get(at..at + end as usize) else {
                return Err(corrupt("is cut short"));
            };
            at += end as usize;
            self.positions.extend_from_slice(lists);
        }
        if at != bytes.len() {
            return Err(corrupt("has bytes past its end"));
        }
        Ok(())
    }

    /// The postings it holds.
    pub(crate) fn len(&self) -> usize {
        self.n
    }

    /// Posting `i`'s document id.
    #[inline]
    pub(crate) fn id(&self, i: usize) -> &[u8] {
        let start = if i == 0 {
            0
        } else {
            self.id_ends[i - 1] as usize
        };
        &self.ids[start..self.id_ends[i] as usize]
    }

    /// Posting `i`'s frequency and document length.
    #[inline]
    pub(crate) fn posting(&self, i: usize) -> (u32, u32) {
        (self.tf[i], self.len[i])
    }

    /// Posting `i`'s encoded position list; empty in a block without positions.
    #[inline]
    pub(crate) fn positions(&self, i: usize) -> &[u8] {
        if self.position_ends.is_empty() {
            return &[];
        }
        let start = if i == 0 {
            0
        } else {
            self.position_ends[i - 1] as usize
        };
        &self.positions[start..self.position_ends[i] as usize]
    }

    /// The first posting at or after `id`, from posting `from` on: its index, or the
    /// block's length when every id left is below `id`.
    pub(crate) fn seek(&self, from: usize, id: &[u8]) -> usize {
        (from..self.n).find(|&i| self.id(i) >= id).unwrap_or(self.n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Owned postings with ascending ids, some sharing long prefixes and some none.
    fn postings(rng: &mut Rng, n: usize, positions: bool) -> Vec<(Vec<u8>, u32, u32, Vec<u8>)> {
        let mut ids: Vec<Vec<u8>> = (0..n)
            .map(|_| match rng.below(4) {
                0 => format!("d{:08}", rng.below(1 << 20)).into_bytes(),
                1 => rng.next().to_be_bytes()[..1 + rng.below(8) as usize].to_vec(),
                2 => format!("tenant-{}/{}", rng.below(3), rng.below(1 << 30)).into_bytes(),
                _ => Vec::from_iter((0..rng.below(20)).map(|_| rng.below(256) as u8)),
            })
            .collect();
        ids.sort();
        ids.dedup();
        ids.into_iter()
            .map(|id| {
                let most = if rng.below(10) == 0 { 5000 } else { 6 };
                let tf = 1 + rng.below(most) as u32;
                let len = tf + rng.below(200) as u32;
                let ps: Vec<u32> = if positions {
                    let mut at = 0;
                    (0..tf.min(40))
                        .map(|_| {
                            at += 1 + rng.below(300) as u32;
                            at
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                (id, tf, len, crate::layout::encode_positions(&ps))
            })
            .collect()
    }

    /// Cuts `all` into blocks as pack does: a block is closed when full.
    fn blocks(all: &[(Vec<u8>, u32, u32, Vec<u8>)], positions: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut b = Builder::new(positions);
        let mut out = Vec::new();
        for (id, tf, len, ps) in all {
            if b.full() {
                out.extend(b.finish());
            }
            b.push(Posting {
                id,
                tf: *tf,
                len: *len,
                positions: if positions { ps } else { &[] },
            });
        }
        out.extend(b.finish());
        assert_eq!(b.len(), 0);
        assert!(b.finish().is_none());
        out
    }

    #[test]
    fn postings_come_back_as_they_went_in_whatever_the_ids() {
        for seed in 1..=40u64 {
            let mut rng = Rng(seed);
            for positions in [false, true] {
                let n = 1 + rng.below(700) as usize;
                let all = postings(&mut rng, n, positions);
                let mut block = Block::default();
                let mut read = Vec::new();
                let mut last: Option<Vec<u8>> = None;
                for (qualifier, value) in blocks(&all, positions) {
                    block.decode(&value).unwrap();
                    assert!(block.len() <= MAX_POSTINGS, "seed {seed}");
                    // A value passes the byte cap by at most its last posting.
                    assert_eq!(
                        qualifier,
                        block.id(block.len() - 1),
                        "seed {seed}: the qualifier is the last id"
                    );
                    assert!(
                        last.as_deref().is_none_or(|l| l < block.id(0)),
                        "seed {seed}: blocks ascend"
                    );
                    last = Some(qualifier);
                    for i in 0..block.len() {
                        let (tf, len) = block.posting(i);
                        read.push((block.id(i).to_vec(), tf, len, block.positions(i).to_vec()));
                    }
                }
                let want: Vec<_> = all
                    .iter()
                    .map(|(id, tf, len, ps)| {
                        (
                            id.clone(),
                            *tf,
                            *len,
                            if positions { ps.clone() } else { Vec::new() },
                        )
                    })
                    .collect();
                assert_eq!(read, want, "seed {seed}, positions {positions}");
            }
        }
    }

    #[test]
    fn a_block_closes_at_its_posting_count_or_its_byte_cap() {
        let mut rng = Rng(7);
        // Short postings: the count closes the block.
        let small: Vec<_> = (0..300u32)
            .map(|i| (format!("d{i:05}").into_bytes(), 1, 10, Vec::new()))
            .collect();
        let cut = blocks(&small, false);
        assert_eq!(cut.len(), 3, "128, 128 and 44");
        // Long position lists: the bytes do, well before 128 postings.
        let long: Vec<_> = (0..40u32)
            .map(|i| {
                let ps: Vec<u32> = (0..600).map(|p| p * 3 + rng.below(3) as u32).collect();
                (
                    format!("d{i:05}").into_bytes(),
                    600,
                    2000,
                    crate::layout::encode_positions(&ps),
                )
            })
            .collect();
        let cut = blocks(&long, true);
        assert!(cut.len() >= 5, "{} blocks", cut.len());
        let mut block = Block::default();
        for (_, value) in &cut {
            block.decode(value).unwrap();
            // Closed by the first posting that takes it to the cap.
            assert!(value.len() < MAX_BYTES + 2000, "{}", value.len());
        }
    }

    #[test]
    fn the_bound_is_the_best_factor_in_the_block_for_any_statistics() {
        for seed in 1..=40u64 {
            let mut rng = Rng(seed);
            let n = 1 + rng.below(128) as usize;
            let all = postings(&mut rng, n, false);
            let cut = blocks(&all[..all.len().min(MAX_POSTINGS)], false);
            let mut block = Block::default();
            block.decode(&cut[0].1).unwrap();
            for _ in 0..20 {
                let bm = Bm25 {
                    n: 1000.0,
                    avglen: 1.0 + rng.below(400) as f64,
                    k1: 0.5 + rng.below(20) as f64 / 10.0,
                    b: rng.below(11) as f64 / 10.0,
                };
                let best = (0..block.len())
                    .map(|i| {
                        let (tf, len) = block.posting(i);
                        bm.tf_factor(f64::from(tf), f64::from(len))
                    })
                    .fold(0.0, f64::max);
                let (n, bound) = Block::peek_bound(&cut[0].1, &bm).unwrap();
                assert_eq!(n, block.len());
                assert_eq!(
                    bound.to_bits(),
                    best.to_bits(),
                    "seed {seed}: the bound is some posting's own factor, and the best"
                );
            }
        }
    }

    #[test]
    fn a_seek_finds_the_first_id_at_or_after() {
        let all: Vec<_> = ["a1", "a3", "b", "b7", "c"]
            .iter()
            .map(|id| (id.as_bytes().to_vec(), 1, 5, Vec::new()))
            .collect();
        let cut = blocks(&all, false);
        let mut block = Block::default();
        block.decode(&cut[0].1).unwrap();
        for (id, from, want) in [
            (&b""[..], 0, 0),
            (b"a1", 0, 0),
            (b"a2", 0, 1),
            (b"b", 0, 2),
            (b"b", 3, 3),
            (b"b8", 0, 4),
            (b"c", 0, 4),
            (b"d", 0, 5),
        ] {
            assert_eq!(block.seek(from, id), want, "{id:?} from {from}");
        }
    }

    #[test]
    fn damaged_bytes_are_refused_and_never_panic() {
        let mut rng = Rng(11);
        let all = postings(&mut rng, 60, true);
        let (_, value) = blocks(&all, true).remove(0);
        let mut block = Block::default();
        block.decode(&value).unwrap();
        // Every prefix is cut short.
        for end in 0..value.len() {
            assert!(block.decode(&value[..end]).is_err(), "prefix {end}");
        }
        // A byte past the end.
        let mut more = value.clone();
        more.push(0);
        assert!(block.decode(&more).is_err());
        // Another format is a mismatch, not corruption.
        let mut other = value.clone();
        other[0] = FORMAT + 1;
        assert!(matches!(block.decode(&other), Err(Error::Mismatch(_))));
        // Any one byte changed: an error or some other block, never a panic.
        for i in 0..value.len() {
            let mut bad = value.clone();
            bad[i] ^= 1 << rng.below(8);
            let _ = block.decode(&bad);
        }
        // And it still decodes the good bytes afterwards.
        block.decode(&value).unwrap();
        assert_eq!(block.len(), value[1] as usize);
    }
}
