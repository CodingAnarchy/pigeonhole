//! D298 (FORMAT §3.8): roaring sets against `BTreeSet<u32>`. Reads (length, membership,
//! iteration, intersection and union sizes) and the set algebra match the reference across
//! the array/bitmap boundary; encodings are canonical and pinned; operands combine in commit
//! order and apply onto a base as the reference does; malformed sets and operands are
//! refused. Seeded; the seed prints on failure.

use std::collections::BTreeSet;

use pigeonhole_format::roaring::{self, SetRef};

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

fn encode(s: &BTreeSet<u32>) -> Vec<u8> {
    let mut out = Vec::new();
    roaring::encode_sorted(&mut out, s.iter().copied());
    out
}

/// A random set: a few dense or sparse clusters, so containers land on both sides of 4096.
fn random_set(rng: &mut Rng) -> BTreeSet<u32> {
    let mut s = BTreeSet::new();
    for _ in 0..1 + rng.below(4) {
        let key = rng.below(6) as u32;
        let n = [0, 3, 100, 4095, 4096, 4097, 9000, 60_000][rng.below(8) as usize];
        for _ in 0..n {
            s.insert(key << 16 | rng.below(65_536) as u32);
        }
    }
    if rng.below(4) == 0 {
        s.insert(u32::MAX);
    }
    s
}

#[test]
fn reads_and_algebra_match_a_btreeset() {
    for seed in 1..=60u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let (x, y) = (random_set(&mut rng), random_set(&mut rng));
        let (bx, by) = (encode(&x), encode(&y));
        let (a, b) = (SetRef::parse(&bx).unwrap(), SetRef::parse(&by).unwrap());
        assert_eq!(a.len(), x.len() as u64, "seed {seed}");
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            x.iter().copied().collect::<Vec<_>>()
        );
        for _ in 0..200 {
            let v = if rng.below(2) == 0 {
                rng.below(6 << 16) as u32
            } else {
                *x.iter()
                    .nth(rng.below(x.len().max(1) as u64) as usize)
                    .unwrap_or(&0)
            };
            assert_eq!(a.contains(v), x.contains(&v), "seed {seed}: {v}");
        }
        let both: Vec<u32> = x.intersection(&y).copied().collect();
        assert_eq!(a.intersection(&b).collect::<Vec<_>>(), both, "seed {seed}");
        assert_eq!(a.intersection_len(&b), both.len() as u64, "seed {seed}");
        assert_eq!(a.union_len(&b), x.union(&y).count() as u64, "seed {seed}");
        let mut u = Vec::new();
        roaring::union(&mut u, &a, &b);
        assert_eq!(
            u,
            encode(&x.union(&y).copied().collect()),
            "seed {seed}: union"
        );
        let mut d = Vec::new();
        roaring::difference(&mut d, &a, &b);
        assert_eq!(
            d,
            encode(&x.difference(&y).copied().collect()),
            "seed {seed}: diff"
        );
    }
}

#[test]
fn encodings_are_pinned() {
    // The RoaringFormatSpec bytes of {1, 5, 70000}: cookie 12346, two containers (keys 0 and
    // 1, cardinalities 2 and 1), offsets 24 and 28, then the arrays.
    let mut s = Vec::new();
    roaring::encode_sorted(&mut s, [1, 5, 70_000]);
    assert_eq!(
        s,
        [
            0x3a, 0x30, 0, 0, 2, 0, 0, 0, // cookie, n
            0, 0, 1, 0, 1, 0, 0, 0, // key 0 card 2, key 1 card 1
            24, 0, 0, 0, 28, 0, 0, 0, // offsets
            1, 0, 5, 0, // container 0
            0x70, 0x11, // 70000 - 65536 = 4464
        ]
    );
}

#[test]
fn operands_combine_in_commit_order_and_apply_onto_a_base() {
    for seed in 1..=200u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        // A run of adds and removes of a few elements, in commit order, onto a base.
        let base: BTreeSet<u32> = (0..rng.below(20)).map(|_| rng.below(40) as u32).collect();
        let ops: Vec<(bool, u32)> = (0..1 + rng.below(12))
            .map(|_| (rng.below(2) == 0, rng.below(40) as u32))
            .collect();
        let mut want = base.clone();
        for &(add, v) in &ops {
            if add {
                want.insert(v);
            } else {
                want.remove(&v);
            }
        }
        let operand = |&(add, v): &(bool, u32)| {
            let mut o = Vec::new();
            if add {
                roaring::encode_operand(&mut o, &[v], &[]);
            } else {
                roaring::encode_operand(&mut o, &[], &[v]);
            }
            o
        };
        // Combine in any grouping that keeps commit order, then apply once.
        let mut parts: Vec<Vec<u8>> = ops.iter().map(operand).collect();
        while parts.len() > 1 {
            let k = rng.below(parts.len() as u64 - 1) as usize;
            let newer = parts.remove(k + 1);
            let mut out = Vec::new();
            roaring::combine_operands(&mut out, &parts[k], &newer).unwrap();
            parts[k] = out;
        }
        let mut got = Vec::new();
        roaring::apply(&mut got, Some(&encode(&base)), &parts[0]).unwrap();
        assert_eq!(got, encode(&want), "seed {seed}");
        // With no base, the adds that survive.
        let mut no_base = BTreeSet::new();
        for &(add, v) in &ops {
            if add {
                no_base.insert(v);
            } else {
                no_base.remove(&v);
            }
        }
        let mut got = Vec::new();
        roaring::apply(&mut got, None, &parts[0]).unwrap();
        assert_eq!(got, encode(&no_base), "seed {seed}: no base");
    }
}

#[test]
fn malformed_sets_and_operands_are_refused() {
    let mut s = Vec::new();
    roaring::encode_sorted(&mut s, [1, 5, 70_000]);
    let bad = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut b = s.clone();
        f(&mut b);
        assert!(SetRef::parse(&b).is_err(), "{b:?}");
    };
    bad(&|b| b[0] = 0x3b); // cookie
    bad(&|b| b.push(0)); // trailing byte
    bad(&|b| {
        b.pop();
    }); // truncated
    bad(&|b| b[8] = 1); // keys out of order
    bad(&|b| b[16] = 25); // offset not consecutive
    bad(&|b| b[24..28].copy_from_slice(&[5, 0, 1, 0])); // unsorted array
    // A bitmap of no more than 4096 elements must be an array.
    let mut dense = vec![0x3a, 0x30, 0, 0, 1, 0, 0, 0, 0, 0, 0xff, 0x0f, 16, 0, 0, 0];
    let mut words = vec![0u8; 8192];
    for x in 0..4096 {
        words[x / 8] |= 1 << (x % 8);
    }
    dense.extend_from_slice(&words);
    assert!(SetRef::parse(&dense).is_err(), "a 4096-element bitmap");
    // A bitmap whose bits disagree with its cardinality.
    let mut wrong = s.clone();
    wrong.clear();
    roaring::encode_sorted(&mut wrong, 0..5000);
    // The bitmap starts after the 8-byte header and one container's key, cardinality and
    // offset: clear elements 0 to 7.
    wrong[16] = 0;
    assert!(SetRef::parse(&wrong).is_err(), "cardinality");
    // Operands: an element both added and removed; truncated.
    let mut op = Vec::new();
    roaring::encode_operand(&mut op, &[3], &[3]);
    assert!(roaring::parse_operand(&op).is_err());
    assert!(roaring::parse_operand(&[9, 0, 0, 0]).is_err());
}
