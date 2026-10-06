//! Helpers shared by the integration tests: a seeded generator of internal keys and stored
//! values, and a full check of a reader against the entries it should hold.
#![allow(dead_code)]

use pigeonhole_format::{Cursor, Kind, encode_key};
use pigeonhole_memtable::MemtableReader;

pub type Entry = (Vec<u8>, Vec<u8>);

/// xorshift64*, enough for test data.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }
}

/// The seed from `PIGEONHOLE_SEED`, or one from the clock; printed so a failure can be
/// repeated.
pub fn seed() -> u64 {
    let seed = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(1, |d| d.as_nanos() as u64)
        });
    eprintln!("seed = {seed} (set PIGEONHOLE_SEED={seed} to repeat)");
    seed
}

/// A random cell key with this seqno (so keys are unique) and a random stored value. Rows
/// are short and drawn from a small alphabet, so prefixes and zero bytes (escaped) collide.
pub fn random_entry(rng: &mut Rng, seqno: u64) -> Entry {
    let row: Vec<u8> = (0..1 + rng.below(6))
        .map(|_| [0u8, 1, b'a', b'b', 0xff][rng.below(5)])
        .collect();
    let qual_len = rng.below(3);
    let qual = rng.bytes(qual_len);
    let ts = rng.below(4) as u64;
    let kind = [Kind::Put, Kind::Merge, Kind::CellDelete][rng.below(3)];
    let mut key = Vec::new();
    encode_key(&mut key, &row, &qual, ts, seqno, kind).unwrap();
    let value = if kind == Kind::CellDelete {
        Vec::new()
    } else {
        let mut v = vec![0u8];
        let len = rng.below(48);
        v.extend(rng.bytes(len));
        v
    };
    (key, value)
}

/// `n` entries with seqnos `1..=n`, in insertion order.
pub fn entries(seed: u64, n: usize) -> Vec<Entry> {
    let mut rng = Rng::new(seed);
    (1..=n as u64).map(|s| random_entry(&mut rng, s)).collect()
}

/// Everything the cursor yields, in order.
pub fn scan(reader: &MemtableReader) -> Vec<Entry> {
    let mut it = reader.iter();
    it.seek_to_first().unwrap();
    let mut out = Vec::new();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    out
}

/// The reader holds exactly `expected` (sorted): a full scan matches, every key seeks to
/// itself, and random targets seek to their lower bound.
pub fn verify(reader: &MemtableReader, expected: &[Entry], rng: &mut Rng) {
    assert_eq!(reader.len(), expected.len());
    let seen = scan(reader);
    assert_eq!(seen.len(), expected.len());
    assert!(seen == expected, "scan differs from the expected entries");
    let mut it = reader.iter();
    for (k, v) in expected {
        it.seek(k).unwrap();
        assert!(it.valid());
        assert_eq!(it.key(), &k[..]);
        assert_eq!(it.value(), &v[..]);
    }
    for i in 0..200 {
        let (target, _) = random_entry(rng, i);
        it.seek(&target).unwrap();
        let lower = expected.iter().find(|(k, _)| k >= &target);
        match lower {
            Some((k, v)) => {
                assert!(it.valid());
                assert_eq!(it.key(), &k[..]);
                assert_eq!(it.value(), &v[..]);
            }
            None => assert!(!it.valid()),
        }
    }
}
