//! A set family's cell value, read in place ([`SetRef`], D298).

use pigeonhole_format::roaring;

/// A set of `u32`s, borrowed from a cell of a set family
/// ([`Aggregation::Set`](crate::Aggregation::Set)) without decoding it: membership is a
/// search, the size comes from its containers' counts, and intersections and unions of two
/// cells go container by container. The bytes are a standard 32-bit roaring bitmap
/// ([`as_bytes`](Self::as_bytes)), which CRoaring and roaring-rs read as they are.
///
/// ```
/// use pigeonhole::{Aggregation, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db
///     .table("graph")?
///     .family("out", Family::aggregate(Aggregation::Set))
///     .create_if_missing()?;
/// t.mutate(b"a").set_add("out", b"", 1).set_add("out", b"", 7).commit()?;
/// t.mutate(b"b").set_add("out", b"", 7).set_add("out", b"", 9).commit()?;
/// t.mutate(b"a").set_remove("out", b"", 1).commit()?;
/// let (a, b) = (t.get(b"a", "out", b"")?.unwrap(), t.get(b"b", "out", b"")?.unwrap());
/// let (a, b) = (a.as_set().unwrap(), b.as_set().unwrap());
/// assert_eq!(a.iter().collect::<Vec<_>>(), [7]);
/// assert_eq!(a.intersection_len(&b), 1);
/// assert_eq!(a.union_len(&b), 2);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SetRef<'a>(roaring::SetRef<'a>);

impl<'a> SetRef<'a> {
    /// The set in `bytes`, a canonical roaring bitmap without run containers (as a set
    /// family stores one); `None` if they are not one.
    pub fn from_bytes(bytes: &'a [u8]) -> Option<Self> {
        roaring::SetRef::parse(bytes).ok().map(Self)
    }

    /// The set's bytes: a standard roaring bitmap.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.0.as_bytes()
    }

    /// The number of elements.
    pub fn len(&self) -> u64 {
        self.0.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `x` is in the set.
    pub fn contains(&self, x: u32) -> bool {
        self.0.contains(x)
    }

    /// The elements, ascending.
    pub fn iter(&self) -> SetIter<'a> {
        SetIter(self.0.iter())
    }

    /// The elements of both sets, ascending.
    pub fn intersection<'b>(&self, other: &SetRef<'b>) -> SetIntersection<'a, 'b> {
        SetIntersection(self.0.intersection(&other.0))
    }

    /// The size of the intersection of the two sets, without building it.
    pub fn intersection_len(&self, other: &SetRef<'_>) -> u64 {
        self.0.intersection_len(&other.0)
    }

    /// The size of the union of the two sets, without building it.
    pub fn union_len(&self, other: &SetRef<'_>) -> u64 {
        self.0.union_len(&other.0)
    }
}

/// A set's elements, ascending ([`SetRef::iter`]).
pub struct SetIter<'a>(roaring::Iter<'a>);

impl Iterator for SetIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        self.0.next()
    }
}

/// The elements of two sets, ascending ([`SetRef::intersection`]).
pub struct SetIntersection<'a, 'b>(roaring::Intersection<'a, 'b>);

impl Iterator for SetIntersection<'_, '_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        self.0.next()
    }
}
