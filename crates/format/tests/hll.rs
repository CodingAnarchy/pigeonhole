//! HyperLogLog sketches (D294): the encoding against an independent reference, merges
//! (commutative, associative, idempotent, canonical across the sparse/dense boundary), the
//! estimator's accuracy and pinned values, malformed input, and the in-place merge's
//! allocations (perf287's S2 on D294).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use pigeonhole_format::hll::{self, DENSE_LEN, MAX_SPARSE, REGISTERS};

/// Counts this thread's allocations while `COUNTING` is set.
struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

// SAFETY: forwards to the system allocator; the counter is thread-local and never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCS.with(|a| a.set(a.get() + 1));
        }
        // SAFETY: the caller's contract, passed on.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCS.with(|a| a.set(a.get() + 1));
        }
        // SAFETY: the caller's contract, passed on.
        unsafe { System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocations `f` makes on this thread.
fn allocations(f: impl FnOnce()) -> u64 {
    ALLOCS.with(|a| a.set(0));
    COUNTING.with(|c| c.set(true));
    f();
    COUNTING.with(|c| c.set(false));
    ALLOCS.with(Cell::get)
}

/// The reference: plain registers, encoded canonically without the crate's merge.
#[derive(Clone)]
struct Reference([u8; REGISTERS]);

impl Reference {
    fn new() -> Self {
        Self([0; REGISTERS])
    }

    fn add(&mut self, v: i64) {
        let (i, r) = hll::register_of(v);
        let slot = &mut self.0[usize::from(i)];
        *slot = (*slot).max(r);
    }

    fn union(&mut self, other: &Reference) {
        for (a, b) in self.0.iter_mut().zip(other.0) {
            *a = (*a).max(b);
        }
    }

    fn bytes(&self) -> Vec<u8> {
        let set: Vec<(usize, u8)> = (0..REGISTERS)
            .filter(|&i| self.0[i] != 0)
            .map(|i| (i, self.0[i]))
            .collect();
        if set.len() <= MAX_SPARSE {
            let mut out = vec![1, 12, 0, 0];
            for (i, r) in set {
                out.extend_from_slice(&(((i as u32) << 8) | u32::from(r)).to_le_bytes());
            }
            out
        } else {
            let mut out = vec![1, 12, 1, 0];
            let mut bits = vec![0u8; 3072];
            for (i, r) in set {
                for b in 0..6 {
                    if r >> b & 1 == 1 {
                        let bit = 6 * i + b;
                        bits[bit / 8] |= 1 << (bit % 8);
                    }
                }
            }
            out.extend_from_slice(&bits);
            out
        }
    }
}

fn element(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    hll::encode_element(&mut out, v);
    out
}

/// A sketch of `elements`, merged one at a time.
fn sketch(elements: impl IntoIterator<Item = i64>) -> Vec<u8> {
    let mut it = elements.into_iter();
    let mut acc = element(it.next().expect("one element"));
    for v in it {
        hll::merge(&mut acc, &element(v)).unwrap();
    }
    acc
}

fn reference(elements: impl IntoIterator<Item = i64>) -> Reference {
    let mut r = Reference::new();
    for v in elements {
        r.add(v);
    }
    r
}

#[test]
fn one_element_is_eight_sparse_bytes() {
    let s = element(42);
    assert_eq!(s.len(), 8);
    assert_eq!(&s[..4], &[1, 12, 0, 0]);
    assert_eq!(s, reference([42]).bytes());
    hll::validate(&s).unwrap();
    assert_eq!(hll::estimate(&s).unwrap(), 1);
}

#[test]
fn merges_match_the_reference_across_the_dense_boundary() {
    // Sizes around the 767-register boundary, and well past it.
    for n in [
        1, 2, 100, 760, 765, 766, 767, 768, 769, 770, 800, 2000, 50_000,
    ] {
        let elements: Vec<i64> = (0..n).map(|i| i * 7919 - 3).collect();
        let s = sketch(elements.iter().copied());
        let r = reference(elements.iter().copied());
        assert_eq!(s, r.bytes(), "{n} elements");
        let set = r.0.iter().filter(|&&x| x != 0).count();
        assert_eq!(
            s.len() == DENSE_LEN,
            set > MAX_SPARSE,
            "{n} elements, {set} set"
        );
        hll::validate(&s).unwrap();
    }
}

#[test]
fn merge_is_commutative_associative_and_idempotent() {
    let sets: [Vec<i64>; 4] = [
        (0..300).collect(),
        (200..1500).collect(),
        (5_000..5_010).collect(),
        (-40_000..-30_000).collect(),
    ];
    let sketches: Vec<Vec<u8>> = sets.iter().map(|s| sketch(s.iter().copied())).collect();
    let mut all = Reference::new();
    for s in &sets {
        all.union(&reference(s.iter().copied()));
    }
    let want = all.bytes();
    // Every order, folding left.
    let orders = [[0, 1, 2, 3], [3, 2, 1, 0], [1, 3, 0, 2], [2, 0, 3, 1]];
    for order in orders {
        let mut acc = sketches[order[0]].clone();
        for &k in &order[1..] {
            hll::merge(&mut acc, &sketches[k]).unwrap();
        }
        assert_eq!(acc, want, "order {order:?}");
        // Idempotent: merging any part again changes nothing.
        for s in &sketches {
            hll::merge(&mut acc, s).unwrap();
        }
        assert_eq!(acc, want);
    }
    // Associative: (a ∪ b) ∪ (c ∪ d) is the same.
    let mut ab = sketches[0].clone();
    hll::merge(&mut ab, &sketches[1]).unwrap();
    let mut cd = sketches[2].clone();
    hll::merge(&mut cd, &sketches[3]).unwrap();
    hll::merge(&mut ab, &cd).unwrap();
    assert_eq!(ab, want);
}

#[test]
fn merge_at_works_behind_a_prefix() {
    // The merge operator's stored values carry a tag byte first.
    let mut acc = vec![0u8];
    acc.extend_from_slice(&element(1));
    for v in 2..2_000 {
        hll::merge_at(&mut acc, 1, &element(v)).unwrap();
    }
    assert_eq!(acc[0], 0);
    assert_eq!(&acc[1..], reference(1..2_000).bytes());
}

#[test]
fn the_estimate_is_within_three_sigma() {
    // 1.04 / sqrt(4096) = 1.625%; 3σ ≈ 4.9%. Five disjoint element sets per size.
    for n in [
        1u64, 2, 3, 5, 8, 13, 16, 100, 1_000, 10_000, 100_000, 1_000_000,
    ] {
        for set in 0..5i64 {
            let base = set * 10_000_000_000 + 17;
            let r = reference((0..n as i64).map(|i| base + i * 3));
            let e = hll::estimate(&r.bytes()).unwrap();
            let off = (e as f64 - n as f64).abs();
            assert!(
                off <= (0.049 * n as f64).max(1.0),
                "n {n}, set {set}: estimate {e} ({:.2}%)",
                100.0 * off / n as f64
            );
        }
    }
    assert_eq!(
        hll::estimate(&reference(std::iter::empty()).bytes()).ok(),
        None,
        "no registers: not a sketch"
    );
}

#[test]
fn estimates_are_pinned() {
    // Exact values on fixed sketches, so the arithmetic can't drift between builds or
    // platforms (CI runs this on x86_64 and aarch64).
    let pinned: [(i64, u64); 4] = [
        (10, 10),
        (1_000, PIN_1K),
        (100_000, PIN_100K),
        (2_000_000, PIN_2M),
    ];
    for (n, want) in pinned {
        let got = hll::estimate(&reference(0..n).bytes()).unwrap();
        assert_eq!(got, want, "{n} elements");
    }
}

const PIN_1K: u64 = 996;
const PIN_100K: u64 = 98_734;
const PIN_2M: u64 = 2_028_851;

#[test]
fn malformed_sketches_are_refused() {
    let good = element(5);
    let dense = sketch(0..5_000);
    let bad: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("short header", vec![1, 12, 0]),
        ("encoding", [&[2u8, 12, 0, 0][..], &good[4..]].concat()),
        ("precision", [&[1u8, 14, 0, 0][..], &good[4..]].concat()),
        (
            "representation",
            [&[1u8, 12, 2, 0][..], &good[4..]].concat(),
        ),
        ("reserved", [&[1u8, 12, 0, 1][..], &good[4..]].concat()),
        ("no entries", vec![1, 12, 0, 0]),
        ("partial entry", good[..7].to_vec()),
        (
            "rank 0",
            [&good[..4], &(7u32 << 8).to_le_bytes()[..]].concat(),
        ),
        (
            "rank 54",
            [&good[..4], &((7u32 << 8) | 54).to_le_bytes()[..]].concat(),
        ),
        (
            "index out of range",
            [&good[..4], &((4096u32 << 8) | 1).to_le_bytes()[..]].concat(),
        ),
        (
            "out of order",
            [
                &good[..4],
                &((9u32 << 8) | 1).to_le_bytes()[..],
                &((3u32 << 8) | 1).to_le_bytes()[..],
            ]
            .concat(),
        ),
        (
            "duplicate",
            [
                &good[..4],
                &((9u32 << 8) | 1).to_le_bytes()[..],
                &((9u32 << 8) | 2).to_le_bytes()[..],
            ]
            .concat(),
        ),
        ("dense length", dense[..DENSE_LEN - 1].to_vec()),
        (
            "dense, not canonical",
            reference(0..10).bytes().pipe_dense(),
        ),
    ];
    // As the accumulator too, merged with a small sketch, one that pushes a sparse union past
    // 767 registers, and a dense one: an error, never a panic (perf287's S1 on #1200).
    let wide = sketch(0..700);
    for (what, s) in bad {
        assert!(hll::validate(&s).is_err(), "{what} validated");
        assert!(hll::estimate(&s).is_err(), "{what} estimated");
        let mut acc = good.clone();
        assert!(hll::merge(&mut acc, &s).is_err(), "{what} merged");
        for other in [&good, &wide, &dense] {
            let mut acc = s.clone();
            let merged = hll::merge(&mut acc, other).and_then(|()| hll::validate(&acc));
            // A dense accumulator with few registers set may become a canonical dense sketch
            // once merged with a dense one: no panic is what matters there.
            if what != "dense, not canonical" {
                assert!(merged.is_err(), "{what} as the accumulator merged");
            }
        }
    }
}

trait PipeDense {
    fn pipe_dense(self) -> Vec<u8>;
}

impl PipeDense for Vec<u8> {
    /// The same registers in the dense form, which is not canonical for a small sketch.
    fn pipe_dense(self) -> Vec<u8> {
        let mut out = vec![1, 12, 1, 0];
        let mut bits = vec![0u8; 3072];
        for e in self[4..].chunks(4) {
            let e = u32::from_le_bytes([e[0], e[1], e[2], e[3]]);
            let (i, r) = ((e >> 8) as usize, e as u8);
            for b in 0..6 {
                if r >> b & 1 == 1 {
                    let bit = 6 * i + b;
                    bits[bit / 8] |= 1 << (bit % 8);
                }
            }
        }
        out.extend_from_slice(&bits);
        out
    }
}

#[test]
fn folding_into_a_dense_sketch_allocates_nothing() {
    let mut acc = sketch(0..5_000);
    assert_eq!(acc.len(), DENSE_LEN);
    let operands: Vec<Vec<u8>> = (10_000..10_500).map(element).collect();
    let n = allocations(|| {
        for o in &operands {
            hll::merge(&mut acc, o).unwrap();
        }
        let _ = hll::estimate(&acc).unwrap();
    });
    assert_eq!(n, 0, "a dense fold allocated");
}

#[test]
fn a_sparse_fold_allocates_at_most_once() {
    // An accumulator that starts as one element (its exact 8 bytes), as the resolver's does,
    // grows through sparse and into dense with one reservation.
    let operands: Vec<Vec<u8>> = (0..3_000).map(element).collect();
    let mut acc = Vec::with_capacity(8);
    acc.extend_from_slice(&operands[0]);
    let n = allocations(|| {
        for o in &operands[1..] {
            hll::merge(&mut acc, o).unwrap();
        }
    });
    assert_eq!(acc.len(), DENSE_LEN);
    assert!(n <= 1, "{n} allocations");
}
