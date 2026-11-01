//! Roaring sets of `u32`, the values of a `Set` aggregate family (decision D298; FORMAT
//! §3.8), and their operands.
//!
//! **Encoding:** the standard 32-bit [RoaringFormatSpec] without run containers, so CRoaring
//! and roaring-rs readers open a cell's bytes as they are:
//!
//! | Offset | Size | Field |
//! |---|---|---|
//! | 0 | 4 | cookie `12346` (u32 LE: no run containers) |
//! | 4 | 4 | `n`, the container count (u32 LE) |
//! | 8 | 4·n | per container: its key (the high 16 bits, u16 LE) and cardinality − 1 (u16 LE) |
//! | 8 + 4·n | 4·n | per container: its offset from the start of the set (u32 LE) |
//! | … | | the containers, in key order: an array of `c` sorted `u16` LE if `c` ≤ 4096, else a bitmap of 1024 `u64` LE (bit `x` in word `x / 64`, bit `x % 64`) |
//!
//! It is **canonical**: containers sorted by key, none empty, each an array exactly when it
//! holds at most 4096 elements, the offsets those of consecutive containers, and nothing
//! after the last. The empty set is the 8-byte header. A set that is not canonical is
//! malformed, so equal sets have equal bytes.
//!
//! **Operands:** an input to a `Set` family is a pair `(A, R)` of disjoint sets, the elements
//! added and removed: `A`'s length (u32 LE), `A`, then `R`. `set_add(x)` writes `({x}, ∅)`
//! and `set_remove(x)` writes `(∅, {x})`.
//! - **Combine** an older `(A1, R1)` with a newer `(A2, R2)`: `((A1 ∖ R2) ∪ A2, (R1 ∖ A2) ∪
//!   R2)`. Associative but not commutative: operands fold in commit order (D31).
//! - **Apply** onto a base `B`: `(B ∖ R) ∪ A`; with no base, `A`.
//!
//! [RoaringFormatSpec]: https://github.com/RoaringBitmap/RoaringFormatSpec
//!
//! ```
//! use pigeonhole_format::roaring::{self, SetRef};
//!
//! let mut a = Vec::new();
//! roaring::encode_sorted(&mut a, [1, 5, 70_000]);
//! let mut b = Vec::new();
//! roaring::encode_sorted(&mut b, [5, 9, 70_000]);
//! let (a, b) = (SetRef::parse(&a).unwrap(), SetRef::parse(&b).unwrap());
//! assert_eq!(a.len(), 3);
//! assert!(a.contains(70_000) && !a.contains(2));
//! assert_eq!(a.intersection_len(&b), 2);
//! assert_eq!(a.union_len(&b), 4);
//! assert_eq!(a.intersection(&b).collect::<Vec<_>>(), [5, 70_000]);
//! ```

use std::cmp::Ordering;
use std::fmt;

/// The cookie of a set without run containers.
pub const COOKIE: u32 = 12346;
/// The most elements an array container holds; a larger container is a bitmap.
pub const ARRAY_MAX: usize = 4096;
const HEADER: usize = 8;
const BITMAP_BYTES: usize = 8192;
const BITMAP_WORDS: usize = 1024;

/// A malformed set or operand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoaringError(pub &'static str);

impl fmt::Display for RoaringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for RoaringError {}

/// The result type of this module.
pub type Result<T> = std::result::Result<T, RoaringError>;

fn u16_at(b: &[u8], i: usize) -> u16 {
    u16::from_le_bytes([b[i], b[i + 1]])
}

fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(b[i..i + 4].try_into().expect("4 bytes"))
}

fn u64_at(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().expect("8 bytes"))
}

/// One container of a checked set.
#[derive(Debug, Clone, Copy)]
enum Container<'a> {
    /// Sorted `u16` LE.
    Array(&'a [u8]),
    /// 1024 `u64` LE words.
    Bitmap(&'a [u8]),
}

impl<'a> Container<'a> {
    fn contains(&self, low: u16) -> bool {
        match *self {
            Container::Array(a) => {
                let (mut lo, mut hi) = (0, a.len() / 2);
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    match u16_at(a, 2 * mid).cmp(&low) {
                        Ordering::Less => lo = mid + 1,
                        Ordering::Greater => hi = mid,
                        Ordering::Equal => return true,
                    }
                }
                false
            }
            Container::Bitmap(b) => {
                let w = u64_at(b, 8 * (usize::from(low) / 64));
                w >> (low % 64) & 1 == 1
            }
        }
    }

    /// The container's elements, ascending.
    fn lows(self) -> Lows<'a> {
        match self {
            Container::Array(a) => Lows::Array(a, 0),
            Container::Bitmap(b) => Lows::Bitmap(b, 0, u64_at(b, 0)),
        }
    }

    fn words(&self) -> [u64; BITMAP_WORDS] {
        let mut w = [0u64; BITMAP_WORDS];
        match *self {
            Container::Array(a) => {
                for i in 0..a.len() / 2 {
                    let x = u16_at(a, 2 * i);
                    w[usize::from(x) / 64] |= 1 << (x % 64);
                }
            }
            Container::Bitmap(b) => {
                for (i, word) in w.iter_mut().enumerate() {
                    *word = u64_at(b, 8 * i);
                }
            }
        }
        w
    }
}

/// Ascending elements of one container.
enum Lows<'a> {
    Array(&'a [u8], usize),
    /// Bytes, word index, the remaining bits of that word.
    Bitmap(&'a [u8], usize, u64),
}

impl Iterator for Lows<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        match self {
            Lows::Array(a, i) => {
                if 2 * *i >= a.len() {
                    return None;
                }
                *i += 1;
                Some(u16_at(a, 2 * (*i - 1)))
            }
            Lows::Bitmap(b, i, bits) => loop {
                if *bits != 0 {
                    let t = bits.trailing_zeros();
                    *bits &= *bits - 1;
                    return Some((*i * 64) as u16 + t as u16);
                }
                *i += 1;
                if *i >= BITMAP_WORDS {
                    return None;
                }
                *bits = u64_at(b, 8 * *i);
            },
        }
    }
}

/// A checked set, borrowing its bytes: reads without decoding the whole set.
#[derive(Debug, Clone, Copy)]
pub struct SetRef<'a> {
    bytes: &'a [u8],
    n: usize,
}

impl<'a> SetRef<'a> {
    /// Checks `bytes` is a canonical set. O(its size).
    pub fn parse(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < HEADER || u32_at(bytes, 0) != COOKIE {
            return Err(RoaringError("not a roaring set without run containers"));
        }
        let n = u32_at(bytes, 4) as usize;
        if n > 1 << 16 || bytes.len() < HEADER + 8 * n {
            return Err(RoaringError("roaring set truncated"));
        }
        let mut at = HEADER + 8 * n;
        let mut prev_key: Option<u16> = None;
        for i in 0..n {
            let key = u16_at(bytes, HEADER + 4 * i);
            let card = usize::from(u16_at(bytes, HEADER + 4 * i + 2)) + 1;
            if prev_key.is_some_and(|p| p >= key) {
                return Err(RoaringError("roaring containers not in key order"));
            }
            prev_key = Some(key);
            if u32_at(bytes, HEADER + 4 * n + 4 * i) as usize != at {
                return Err(RoaringError("roaring container offsets not consecutive"));
            }
            let len = if card <= ARRAY_MAX {
                2 * card
            } else {
                BITMAP_BYTES
            };
            let body = bytes
                .get(at..at + len)
                .ok_or(RoaringError("roaring set truncated"))?;
            if card <= ARRAY_MAX {
                if (1..card).any(|j| u16_at(body, 2 * (j - 1)) >= u16_at(body, 2 * j)) {
                    return Err(RoaringError("roaring array not strictly increasing"));
                }
            } else {
                let ones: usize = (0..BITMAP_WORDS)
                    .map(|w| u64_at(body, 8 * w).count_ones() as usize)
                    .sum();
                if ones != card {
                    return Err(RoaringError("roaring bitmap cardinality mismatch"));
                }
            }
            at += len;
        }
        if at != bytes.len() {
            return Err(RoaringError("bytes after a roaring set"));
        }
        Ok(Self { bytes, n })
    }

    /// The set's bytes.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    fn key(&self, i: usize) -> u16 {
        u16_at(self.bytes, HEADER + 4 * i)
    }

    fn card(&self, i: usize) -> usize {
        usize::from(u16_at(self.bytes, HEADER + 4 * i + 2)) + 1
    }

    fn container(&self, i: usize) -> Container<'a> {
        let at = u32_at(self.bytes, HEADER + 4 * self.n + 4 * i) as usize;
        let card = self.card(i);
        if card <= ARRAY_MAX {
            Container::Array(&self.bytes[at..at + 2 * card])
        } else {
            Container::Bitmap(&self.bytes[at..at + BITMAP_BYTES])
        }
    }

    fn find(&self, key: u16) -> Option<usize> {
        let (mut lo, mut hi) = (0, self.n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key(mid).cmp(&key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// The number of elements: O(containers).
    pub fn len(&self) -> u64 {
        (0..self.n).map(|i| self.card(i) as u64).sum()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Whether `x` is in the set: a search over containers, then within one.
    pub fn contains(&self, x: u32) -> bool {
        self.find((x >> 16) as u16)
            .is_some_and(|i| self.container(i).contains(x as u16))
    }

    /// The elements, ascending.
    pub fn iter(&self) -> Iter<'a> {
        Iter {
            set: *self,
            i: 0,
            lows: None,
        }
    }

    /// The elements of both sets, ascending: container by container.
    pub fn intersection<'b>(&self, other: &SetRef<'b>) -> Intersection<'a, 'b> {
        Intersection {
            a: *self,
            b: *other,
            i: 0,
            j: 0,
            lows: None,
        }
    }

    /// `|self ∩ other|`, container by container without materialising either.
    pub fn intersection_len(&self, other: &SetRef<'_>) -> u64 {
        let (mut i, mut j, mut n) = (0, 0, 0u64);
        while i < self.n && j < other.n {
            match self.key(i).cmp(&other.key(j)) {
                Ordering::Less => i += 1,
                Ordering::Greater => j += 1,
                Ordering::Equal => {
                    n += container_and_len(self.container(i), other.container(j));
                    i += 1;
                    j += 1;
                }
            }
        }
        n
    }

    /// `|self ∪ other|`.
    pub fn union_len(&self, other: &SetRef<'_>) -> u64 {
        self.len() + other.len() - self.intersection_len(other)
    }
}

fn container_and_len(a: Container<'_>, b: Container<'_>) -> u64 {
    match (a, b) {
        (Container::Bitmap(x), Container::Bitmap(y)) => (0..BITMAP_WORDS)
            .map(|w| u64::from((u64_at(x, 8 * w) & u64_at(y, 8 * w)).count_ones()))
            .sum(),
        (Container::Array(_), Container::Bitmap(_)) => {
            a.lows().filter(|&x| b.contains(x)).count() as u64
        }
        (Container::Bitmap(_), Container::Array(_)) => container_and_len(b, a),
        (Container::Array(_), Container::Array(_)) => {
            let (mut x, mut y, mut n) = (a.lows().peekable(), b.lows().peekable(), 0u64);
            while let (Some(&p), Some(&q)) = (x.peek(), y.peek()) {
                match p.cmp(&q) {
                    Ordering::Less => {
                        x.next();
                    }
                    Ordering::Greater => {
                        y.next();
                    }
                    Ordering::Equal => {
                        n += 1;
                        x.next();
                        y.next();
                    }
                }
            }
            n
        }
    }
}

/// A set's elements, ascending ([`SetRef::iter`]).
pub struct Iter<'a> {
    set: SetRef<'a>,
    i: usize,
    lows: Option<(u32, Lows<'a>)>,
}

impl Iterator for Iter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        loop {
            if let Some((high, lows)) = &mut self.lows
                && let Some(low) = lows.next()
            {
                return Some(*high | u32::from(low));
            }
            if self.i >= self.set.n {
                return None;
            }
            self.lows = Some((
                u32::from(self.set.key(self.i)) << 16,
                self.set.container(self.i).lows(),
            ));
            self.i += 1;
        }
    }
}

/// The elements of two sets, ascending ([`SetRef::intersection`]).
pub struct Intersection<'a, 'b> {
    a: SetRef<'a>,
    b: SetRef<'b>,
    i: usize,
    j: usize,
    /// The current common container's elements.
    lows: Option<(u32, std::vec::IntoIter<u16>)>,
}

impl Iterator for Intersection<'_, '_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        loop {
            if let Some((high, lows)) = &mut self.lows
                && let Some(low) = lows.next()
            {
                return Some(*high | u32::from(low));
            }
            if self.i >= self.a.n || self.j >= self.b.n {
                return None;
            }
            match self.a.key(self.i).cmp(&self.b.key(self.j)) {
                Ordering::Less => self.i += 1,
                Ordering::Greater => self.j += 1,
                Ordering::Equal => {
                    let (x, y) = (self.a.container(self.i), self.b.container(self.j));
                    let common: Vec<u16> = match (x, y) {
                        (Container::Bitmap(_), Container::Bitmap(_)) => {
                            let (p, q) = (x.words(), y.words());
                            let w: Vec<u64> = p.iter().zip(q).map(|(a, b)| a & b).collect();
                            bits_to_lows(&w)
                        }
                        (Container::Array(_), _) => x.lows().filter(|&l| y.contains(l)).collect(),
                        (_, Container::Array(_)) => y.lows().filter(|&l| x.contains(l)).collect(),
                    };
                    self.lows = Some((u32::from(self.a.key(self.i)) << 16, common.into_iter()));
                    self.i += 1;
                    self.j += 1;
                }
            }
        }
    }
}

fn bits_to_lows(words: &[u64]) -> Vec<u16> {
    let mut out = Vec::new();
    for (i, &w) in words.iter().enumerate() {
        let mut bits = w;
        while bits != 0 {
            out.push((i * 64) as u16 + bits.trailing_zeros() as u16);
            bits &= bits - 1;
        }
    }
    out
}

/// Writes the canonical set of `sorted`, which must be strictly increasing, to `out`.
pub fn encode_sorted(out: &mut Vec<u8>, sorted: impl IntoIterator<Item = u32>) {
    let mut containers: Vec<(u16, Out<'_>)> = Vec::new();
    for x in sorted {
        let (key, low) = ((x >> 16) as u16, x as u16);
        match containers.last_mut() {
            Some((k, Out::Array(lows))) if *k == key => {
                debug_assert!(lows.last().is_none_or(|&l| l < low), "strictly increasing");
                lows.push(low);
            }
            _ => containers.push((key, Out::Array(vec![low]))),
        }
    }
    for (_, c) in &mut containers {
        if let Out::Array(lows) = c
            && lows.len() > ARRAY_MAX
        {
            *c = Out::Bitmap(lows_to_words(lows));
        }
    }
    encode_containers(out, &containers);
}

/// A result container: one of an input's, as it is, or a new array or bitmap. A new one
/// holds at least one element, and an array at most 4096 ([`Out::normalized`]).
enum Out<'a> {
    Raw(Container<'a>, usize),
    Array(Vec<u16>),
    Bitmap(Box<[u64; BITMAP_WORDS]>),
}

impl Out<'_> {
    fn card(&self) -> usize {
        match self {
            Out::Raw(_, c) => *c,
            Out::Array(a) => a.len(),
            Out::Bitmap(w) => w.iter().map(|x| x.count_ones() as usize).sum(),
        }
    }

    /// `None` if empty; an array if it holds at most 4096.
    fn normalized(self) -> Option<Self> {
        match self {
            Out::Array(a) if a.is_empty() => None,
            Out::Array(a) if a.len() > ARRAY_MAX => Some(Out::Bitmap(lows_to_words(&a))),
            Out::Bitmap(w) => match w.iter().map(|x| x.count_ones() as usize).sum::<usize>() {
                0 => None,
                n if n <= ARRAY_MAX => Some(Out::Array(bits_to_lows(&w[..]))),
                _ => Some(Out::Bitmap(w)),
            },
            other => Some(other),
        }
    }
}

fn lows_to_words(lows: &[u16]) -> Box<[u64; BITMAP_WORDS]> {
    let mut w = Box::new([0u64; BITMAP_WORDS]);
    for &l in lows {
        w[usize::from(l) / 64] |= 1 << (l % 64);
    }
    w
}

fn encode_containers(out: &mut Vec<u8>, containers: &[(u16, Out<'_>)]) {
    let n = containers.len();
    out.reserve(HEADER + 8 * n);
    out.extend_from_slice(&COOKIE.to_le_bytes());
    out.extend_from_slice(&(n as u32).to_le_bytes());
    let cards: Vec<usize> = containers.iter().map(|(_, c)| c.card()).collect();
    for ((key, _), card) in containers.iter().zip(&cards) {
        out.extend_from_slice(&key.to_le_bytes());
        out.extend_from_slice(&((card - 1) as u16).to_le_bytes());
    }
    let mut at = HEADER + 8 * n;
    for card in &cards {
        out.extend_from_slice(&(at as u32).to_le_bytes());
        at += if *card <= ARRAY_MAX {
            2 * card
        } else {
            BITMAP_BYTES
        };
    }
    for (_, c) in containers {
        match c {
            Out::Raw(Container::Array(b) | Container::Bitmap(b), _) => out.extend_from_slice(b),
            Out::Array(lows) => lows
                .iter()
                .for_each(|l| out.extend_from_slice(&l.to_le_bytes())),
            Out::Bitmap(w) => w
                .iter()
                .for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
        }
    }
}

/// Two sorted arrays merged: their union, or the first's elements not in the second.
fn merge_arrays(x: &[u8], y: &[u8], union: bool) -> Vec<u16> {
    let (n, m) = (x.len() / 2, y.len() / 2);
    let mut out = Vec::with_capacity(if union { n + m } else { n });
    let (mut i, mut j) = (0, 0);
    while i < n {
        let p = u16_at(x, 2 * i);
        while j < m && u16_at(y, 2 * j) < p {
            if union {
                out.push(u16_at(y, 2 * j));
            }
            j += 1;
        }
        if j < m && u16_at(y, 2 * j) == p {
            j += 1;
            if union {
                out.push(p);
            }
        } else {
            out.push(p);
        }
        i += 1;
    }
    if union {
        (j..m).for_each(|k| out.push(u16_at(y, 2 * k)));
    }
    out
}

/// Container-wise `a ∪ b` (`union`) or `a ∖ b`: a container on one side only is copied as
/// it is; two arrays merge as sorted lists; a bitmap only where one side is one.
fn combine(out: &mut Vec<u8>, a: &SetRef<'_>, b: &SetRef<'_>, union: bool) {
    let mut containers: Vec<(u16, Out<'_>)> = Vec::with_capacity(a.n + b.n);
    let (mut i, mut j) = (0, 0);
    while i < a.n || j < b.n {
        let ka = (i < a.n).then(|| a.key(i));
        let kb = (j < b.n).then(|| b.key(j));
        match (ka, kb) {
            (Some(x), Some(y)) if x == y => {
                let (p, q) = (a.container(i), b.container(j));
                let r = match (p, q) {
                    (Container::Array(x), Container::Array(y)) => {
                        Out::Array(merge_arrays(x, y, union))
                    }
                    (Container::Array(x), Container::Bitmap(_)) if !union => Out::Array(
                        (0..x.len() / 2)
                            .map(|k| u16_at(x, 2 * k))
                            .filter(|&l| !q.contains(l))
                            .collect(),
                    ),
                    _ => {
                        let mut w = Box::new(p.words());
                        let v = q.words();
                        for k in 0..BITMAP_WORDS {
                            w[k] = if union { w[k] | v[k] } else { w[k] & !v[k] };
                        }
                        Out::Bitmap(w)
                    }
                };
                if let Some(r) = r.normalized() {
                    containers.push((x, r));
                }
                i += 1;
                j += 1;
            }
            (Some(x), y) if y.is_none_or(|y| x < y) => {
                containers.push((x, Out::Raw(a.container(i), a.card(i))));
                i += 1;
            }
            (_, Some(y)) => {
                if union {
                    containers.push((y, Out::Raw(b.container(j), b.card(j))));
                }
                j += 1;
            }
            _ => unreachable!("one side has a container"),
        }
    }
    encode_containers(out, &containers);
}

/// Writes `a ∪ b` to `out`.
pub fn union(out: &mut Vec<u8>, a: &SetRef<'_>, b: &SetRef<'_>) {
    combine(out, a, b, true);
}

/// Writes `a ∖ b` to `out`.
pub fn difference(out: &mut Vec<u8>, a: &SetRef<'_>, b: &SetRef<'_>) {
    combine(out, a, b, false);
}

/// The empty set.
pub fn empty() -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER);
    encode_containers(&mut out, &[]);
    out
}

/// Writes the operand `(adds, removes)` (sorted, strictly increasing, disjoint) to `out`.
pub fn encode_operand(out: &mut Vec<u8>, adds: &[u32], removes: &[u32]) {
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    encode_sorted(out, adds.iter().copied());
    let a_len = (out.len() - at - 4) as u32;
    out[at..at + 4].copy_from_slice(&a_len.to_le_bytes());
    encode_sorted(out, removes.iter().copied());
}

/// A checked operand: its add and remove sets, disjoint.
pub fn parse_operand(bytes: &[u8]) -> Result<(SetRef<'_>, SetRef<'_>)> {
    if bytes.len() < 4 {
        return Err(RoaringError("set operand truncated"));
    }
    let a_len = u32_at(bytes, 0) as usize;
    let rest = &bytes[4..];
    if rest.len() < a_len {
        return Err(RoaringError("set operand truncated"));
    }
    let (a, r) = (
        SetRef::parse(&rest[..a_len])?,
        SetRef::parse(&rest[a_len..])?,
    );
    if a.intersection_len(&r) != 0 {
        return Err(RoaringError("a set operand adds and removes one element"));
    }
    Ok((a, r))
}

/// Combines the operands `older` and `newer` (commit order) into `out`:
/// `((A1 ∖ R2) ∪ A2, (R1 ∖ A2) ∪ R2)`.
pub fn combine_operands(out: &mut Vec<u8>, older: &[u8], newer: &[u8]) -> Result<()> {
    let ((a1, r1), (a2, r2)) = (parse_operand(older)?, parse_operand(newer)?);
    let (mut t, mut a) = (Vec::new(), Vec::new());
    difference(&mut t, &a1, &r2);
    union(&mut a, &SetRef::parse(&t).expect("canonical"), &a2);
    t.clear();
    difference(&mut t, &r1, &a2);
    let mut r = Vec::new();
    union(&mut r, &SetRef::parse(&t).expect("canonical"), &r2);
    out.extend_from_slice(&(a.len() as u32).to_le_bytes());
    out.extend_from_slice(&a);
    out.extend_from_slice(&r);
    Ok(())
}

/// Applies the operand `op` onto `base` (none: the empty set) into `out`: `(B ∖ R) ∪ A`.
pub fn apply(out: &mut Vec<u8>, base: Option<&[u8]>, op: &[u8]) -> Result<()> {
    let (a, r) = parse_operand(op)?;
    let Some(base) = base else {
        out.extend_from_slice(a.as_bytes());
        return Ok(());
    };
    let base = SetRef::parse(base)?;
    let mut t = Vec::new();
    difference(&mut t, &base, &r);
    union(out, &SetRef::parse(&t).expect("canonical"), &a);
    Ok(())
}

/// A set being built in place (a fold's accumulator): containers decoded once, updated per
/// element, encoded once ([`MutSet::encode`]). A fold of many operands costs their elements,
/// not the accumulated set per operand.
#[derive(Debug, Default)]
pub struct MutSet {
    keys: Vec<u16>,
    containers: Vec<MutContainer>,
}

#[derive(Debug)]
enum MutContainer {
    /// Sorted, distinct.
    Array(Vec<u16>),
    /// Kept a bitmap until [`MutSet::encode`] decides, so churn around 4096 doesn't convert
    /// back and forth.
    Bitmap(Box<[u64; BITMAP_WORDS]>),
}

impl MutSet {
    /// The empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// `set`, decoded.
    pub fn from_set(set: &SetRef<'_>) -> Self {
        let mut m = Self::default();
        for i in 0..set.n {
            m.keys.push(set.key(i));
            m.containers.push(match set.container(i) {
                Container::Array(a) => {
                    MutContainer::Array((0..a.len() / 2).map(|k| u16_at(a, 2 * k)).collect())
                }
                c @ Container::Bitmap(_) => MutContainer::Bitmap(Box::new(c.words())),
            });
        }
        m
    }

    /// Adds `x`.
    pub fn insert(&mut self, x: u32) {
        let (key, low) = ((x >> 16) as u16, x as u16);
        let i = match self.keys.binary_search(&key) {
            Ok(i) => i,
            Err(i) => {
                self.keys.insert(i, key);
                self.containers.insert(i, MutContainer::Array(Vec::new()));
                i
            }
        };
        match &mut self.containers[i] {
            MutContainer::Array(a) => {
                if let Err(at) = a.binary_search(&low) {
                    a.insert(at, low);
                    if a.len() > ARRAY_MAX {
                        self.containers[i] = MutContainer::Bitmap(lows_to_words(a));
                    }
                }
            }
            MutContainer::Bitmap(w) => w[usize::from(low) / 64] |= 1 << (low % 64),
        }
    }

    /// Removes `x`.
    pub fn remove(&mut self, x: u32) {
        let (key, low) = ((x >> 16) as u16, x as u16);
        let Ok(i) = self.keys.binary_search(&key) else {
            return;
        };
        match &mut self.containers[i] {
            MutContainer::Array(a) => {
                if let Ok(at) = a.binary_search(&low) {
                    a.remove(at);
                }
            }
            MutContainer::Bitmap(w) => w[usize::from(low) / 64] &= !(1 << (low % 64)),
        }
    }

    /// Adds every element of `set`.
    pub fn insert_all(&mut self, set: &SetRef<'_>) {
        set.iter().for_each(|x| self.insert(x));
    }

    /// Removes every element of `set`.
    pub fn remove_all(&mut self, set: &SetRef<'_>) {
        set.iter().for_each(|x| self.remove(x));
    }

    /// Appends the canonical encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let containers: Vec<(u16, Out<'_>)> = self
            .keys
            .iter()
            .zip(&self.containers)
            .filter_map(|(&k, c)| {
                let o = match c {
                    MutContainer::Array(a) => Out::Array(a.clone()),
                    MutContainer::Bitmap(w) => Out::Bitmap(w.clone()),
                };
                o.normalized().map(|o| (k, o))
            })
            .collect();
        encode_containers(out, &containers);
    }
}

/// Applies `operands` (in commit order) onto `base` (none: the empty set) into `out`, in
/// one pass over a decoded accumulator: `acc |= A_i; acc -= R_i`.
pub fn apply_all<'a>(
    out: &mut Vec<u8>,
    base: Option<&[u8]>,
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> Result<()> {
    let mut acc = match base {
        Some(b) => MutSet::from_set(&SetRef::parse(b)?),
        None => MutSet::new(),
    };
    for op in operands {
        let (a, r) = parse_operand(op)?;
        acc.insert_all(&a);
        acc.remove_all(&r);
    }
    acc.encode(out);
    Ok(())
}

/// Combines `operands` (in commit order) into one operand in `out`, in one pass:
/// `A = (A − R_i) ∪ A_i`, `R = (R − A_i) ∪ R_i`.
pub fn combine_all<'a>(
    out: &mut Vec<u8>,
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> Result<()> {
    let (mut adds, mut removes) = (MutSet::new(), MutSet::new());
    for op in operands {
        let (a, r) = parse_operand(op)?;
        adds.remove_all(&r);
        adds.insert_all(&a);
        removes.remove_all(&a);
        removes.insert_all(&r);
    }
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    adds.encode(out);
    let a_len = (out.len() - at - 4) as u32;
    out[at..at + 4].copy_from_slice(&a_len.to_le_bytes());
    removes.encode(out);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_container_turns_into_a_bitmap_past_4096() {
        let mut s = Vec::new();
        encode_sorted(&mut s, 0..4096);
        assert_eq!(s.len(), 8 + 8 + 2 * 4096);
        let mut d = Vec::new();
        encode_sorted(&mut d, 0..4097);
        assert_eq!(d.len(), 8 + 8 + 8192);
        assert_eq!(SetRef::parse(&d).unwrap().len(), 4097);
        assert_eq!(empty().len(), 8);
        assert!(SetRef::parse(&empty()).unwrap().is_empty());
    }
}
