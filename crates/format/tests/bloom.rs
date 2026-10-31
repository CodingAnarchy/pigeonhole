//! D298 (FORMAT §3.6): bloom filter states. No false negatives; a false-positive rate near
//! `(1 − e^(−kn/m))^k`; merges in any grouping give the bytes of one-shot adding (sparse to
//! dense included) and match an independent encoder; bytes and `i64` elements hash apart;
//! malformed states are refused; and a fold costs the size of its operands, not `m` per
//! operand (perf287's P1 on #1224). Seeded; the seed prints on failure.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use pigeonhole_format::bloom::{self, BloomParams};
use pigeonhole_format::hll::{hash_bytes, hash_i64};

/// Counts this thread's allocations and bytes while `COUNTING` is set.
struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

fn count(bytes: usize) {
    if COUNTING.with(Cell::get) {
        ALLOCS.with(|a| {
            let (n, b) = a.get();
            a.set((n + 1, b + bytes as u64));
        });
    }
}

// SAFETY: forwards to the system allocator; the counter is thread-local and never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        // SAFETY: the caller's contract, passed on.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        count(new);
        // SAFETY: the caller's contract, passed on.
        unsafe { System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocations `f` makes on this thread, and their bytes.
fn allocations(f: impl FnOnce()) -> (u64, u64) {
    ALLOCS.with(|a| a.set((0, 0)));
    COUNTING.with(|c| c.set(true));
    f();
    COUNTING.with(|c| c.set(false));
    ALLOCS.with(Cell::get)
}

/// FORMAT §3.6 from scratch: the positions, a bitmap, then the canonical form.
fn reference(p: BloomParams, hashes: &[u64]) -> Vec<u8> {
    let m = 1usize << p.log2_bits;
    let mut bitmap = vec![0u8; m / 8];
    for &h in hashes {
        let (h1, h2) = (h & 0xffff_ffff, (h >> 32) | 1);
        for i in 0..u64::from(p.hashes) {
            let j = (h1.wrapping_add(i.wrapping_mul(h2)) % m as u64) as usize;
            bitmap[j / 8] |= 1 << (j % 8);
        }
    }
    let set: Vec<u32> = (0..m as u32)
        .filter(|&j| bitmap[j as usize / 8] >> (j % 8) & 1 == 1)
        .collect();
    let mut out = vec![6, p.log2_bits, p.hashes];
    if 4 * set.len() < m / 8 {
        out.push(0);
        set.iter()
            .for_each(|j| out.extend_from_slice(&j.to_le_bytes()));
    } else {
        out.push(1);
        out.extend_from_slice(&bitmap);
    }
    out
}

fn element(p: BloomParams, h: u64) -> Vec<u8> {
    let mut s = Vec::new();
    bloom::append_element(&mut s, p, h);
    s
}

/// xorshift64*, seeded.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn state(p: BloomParams, hashes: &[u64]) -> Vec<u8> {
    let mut s = Vec::new();
    for &h in hashes {
        bloom::add(&mut s, p, h).unwrap();
    }
    s
}

#[test]
fn no_false_negatives_and_the_expected_false_positive_rate() {
    let p = BloomParams {
        log2_bits: 14, // 16384 bits
        hashes: 4,
    };
    let n = 2000u64;
    let s = state(p, &(0..n as i64).map(hash_i64).collect::<Vec<_>>());
    assert!((0..n as i64).all(|v| bloom::contains(&s, hash_i64(v)).unwrap()));
    let probes = 20_000i64;
    let fp = (n as i64..n as i64 + probes)
        .filter(|&v| bloom::contains(&s, hash_i64(v)).unwrap())
        .count() as f64
        / probes as f64;
    let (k, m) = (4.0, 16384.0);
    let expected = (1.0 - (-k * n as f64 / m).exp()).powf(k);
    assert!(
        (fp - expected).abs() < 0.5 * expected,
        "fp {fp} vs {expected}"
    );
    let fill = bloom::fill(&s).unwrap();
    assert!(
        (fill - (1.0 - (-k * n as f64 / m).exp())).abs() < 0.02,
        "fill {fill}"
    );
}

#[test]
fn merges_in_any_grouping_give_the_same_bytes() {
    for seed in 1..=200u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let p = BloomParams {
            log2_bits: 7 + rng.below(4) as u8,
            hashes: 1 + rng.below(5) as u8,
        };
        let hs: Vec<u64> = (0..1 + rng.below(80))
            .map(|_| hash_i64(rng.next() as i64 % 500))
            .collect();
        let one = state(p, &hs);
        bloom::validate(&one, p).unwrap();
        assert_eq!(one, reference(p, &hs), "seed {seed}");
        let mut parts: Vec<Vec<u8>> = hs
            .chunks(1 + rng.below(5) as usize)
            .map(|c| state(p, c))
            .collect();
        while parts.len() > 1 {
            let a = parts.swap_remove(rng.below(parts.len() as u64) as usize);
            let k = rng.below(parts.len() as u64) as usize;
            bloom::merge(&mut parts[k], p, &a).unwrap();
        }
        assert_eq!(parts.pop().unwrap(), one, "seed {seed}");
        // Idempotent.
        let mut twice = one.clone();
        bloom::merge(&mut twice, p, &one).unwrap();
        assert_eq!(twice, one, "seed {seed}");
    }
}

#[test]
fn bytes_and_i64_elements_hash_apart() {
    // The domain tag: the bytes of an i64's little-endian form are another element.
    for v in [0i64, 1, -1, 1 << 40] {
        assert_ne!(hash_i64(v), hash_bytes(&v.to_le_bytes()), "{v}");
    }
    let p = BloomParams {
        log2_bits: 16,
        hashes: 3,
    };
    let s = state(p, &[hash_bytes(b"alice"), hash_bytes(b"bob")]);
    assert!(bloom::contains(&s, hash_bytes(b"alice")).unwrap());
    assert!(!bloom::contains(&s, hash_bytes(b"carol")).unwrap());
}

#[test]
fn malformed_states_are_refused() {
    let p = BloomParams {
        log2_bits: 8,
        hashes: 2,
    };
    let s = state(p, &[hash_i64(1)]);
    // Other parameters; an empty sparse list; unsorted or out-of-range bits; a dense form
    // that should be sparse; a truncated bitmap.
    let other = BloomParams { hashes: 3, ..p };
    assert!(bloom::validate(&s, other).is_err());
    assert!(bloom::validate(&[6, 8, 2, 0], p).is_err());
    let mut unsorted = vec![6, 8, 2, 0];
    unsorted.extend_from_slice(&5u32.to_le_bytes());
    unsorted.extend_from_slice(&3u32.to_le_bytes());
    assert!(bloom::validate(&unsorted, p).is_err());
    let mut out_of_range = vec![6, 8, 2, 0];
    out_of_range.extend_from_slice(&256u32.to_le_bytes());
    assert!(bloom::validate(&out_of_range, p).is_err());
    let mut dense_few = vec![6, 8, 2, 1];
    dense_few.extend_from_slice(&[0u8; 32]);
    dense_few[4] = 0b11;
    assert!(bloom::validate(&dense_few, p).is_err());
    assert!(bloom::validate(&[6, 8, 2, 1, 0xff], p).is_err());
    assert!(
        bloom::contains(&[7, 8, 2, 0], 1).is_err(),
        "another encoding"
    );
}

#[test]
fn a_sparse_fold_at_the_largest_size_never_builds_the_bitmap() {
    // 1,000 one-element operands into a 2^24-bit family (a 2 MiB bitmap): the union stays
    // sparse (7,000 bits, 28 KB), and the fold's allocations are its own list's growth.
    let p = BloomParams {
        log2_bits: 24,
        hashes: 7,
    };
    let operands: Vec<Vec<u8>> = (0..1_000).map(|v| element(p, hash_i64(v))).collect();
    let mut acc = operands[0].clone();
    let (_, bytes) = allocations(|| {
        for o in &operands[1..] {
            bloom::merge(&mut acc, p, o).unwrap();
        }
    });
    assert_eq!(acc[3], 0, "still sparse");
    assert!(bytes < 256 << 10, "{bytes} bytes allocated");
    let mut one = Vec::with_capacity(64);
    let (n, _) = allocations(|| {
        bloom::append_element(&mut one, p, hash_i64(5));
        assert!(bloom::contains(&acc, hash_i64(999)).unwrap());
    });
    assert_eq!(n, 0, "an element or a lookup allocated");
    let hs: Vec<u64> = (0..1_000).map(hash_i64).collect();
    assert_eq!(acc, state(p, &hs));
}

#[test]
fn folding_into_a_dense_filter_allocates_nothing() {
    let p = BloomParams {
        log2_bits: 12,
        hashes: 4,
    };
    let mut acc = state(p, &(0..400).map(hash_i64).collect::<Vec<_>>());
    assert_eq!(acc[3], 1, "dense");
    let operands: Vec<Vec<u8>> = (400..900)
        .map(|v| element(p, hash_i64(v)))
        .chain([state(p, &(900..1300).map(hash_i64).collect::<Vec<_>>())])
        .collect();
    let mut tagged = vec![0u8];
    tagged.extend_from_slice(&acc);
    let (n, _) = allocations(|| {
        for o in &operands {
            bloom::merge(&mut acc, p, o).unwrap();
            bloom::merge_at(&mut tagged, 1, p, o).unwrap();
        }
    });
    assert_eq!(n, 0, "a dense fold allocated");
    assert_eq!(acc, state(p, &(0..1300).map(hash_i64).collect::<Vec<_>>()));
    assert_eq!(tagged[1..], acc[..]);
}

#[test]
fn a_malformed_sparse_accumulator_is_refused_by_a_merge() {
    let p = BloomParams {
        log2_bits: 10,
        hashes: 2,
    };
    let mut unsorted = vec![6, 10, 2, 0];
    unsorted.extend_from_slice(&5u32.to_le_bytes());
    unsorted.extend_from_slice(&3u32.to_le_bytes());
    assert!(bloom::merge(&mut unsorted, p, &element(p, hash_i64(1))).is_err());
    let mut out_of_range = vec![6, 10, 2, 0];
    out_of_range.extend_from_slice(&1024u32.to_le_bytes());
    let dense = state(p, &(0..200).map(hash_i64).collect::<Vec<_>>());
    assert!(bloom::merge(&mut out_of_range, p, &dense).is_err());
    let mut short = vec![6, 10, 2, 1, 0xff];
    assert!(bloom::merge(&mut short, p, &element(p, hash_i64(1))).is_err());
}

#[test]
fn hash_bytes_is_xxh3_over_the_tagged_bytes_and_short_ones_do_not_allocate() {
    let long: Vec<u8> = (0..400u32).map(|i| (i * 7) as u8).collect();
    for len in [0, 1, 8, 254, 255, 256, 400] {
        let b = &long[..len];
        let tagged = [&[0x01][..], b].concat();
        let want = pigeonhole_format::checksum::xxh3_64(&tagged);
        let (n, _) = allocations(|| assert_eq!(hash_bytes(b), want, "len {len}"));
        assert_eq!(n, u64::from(len >= 256), "len {len}");
    }
}
