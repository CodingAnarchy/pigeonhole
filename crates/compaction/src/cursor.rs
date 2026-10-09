//! Cursor combinators: the k-way merge, the scan filter for sources without pushdown, and an
//! in-memory cursor for tests and examples.

use pigeonhole_format::Cursor;
use pigeonhole_format::key::{compare, row_prefix_len};
use pigeonhole_format::scan::ScanFilter;

/// A k-way merge of cursors into one ordered cursor. Sources are ordered newest first; since
/// internal keys are unique (they contain the seqno), ties cannot occur (if they do, the
/// earlier source wins and both entries are yielded).
///
/// A binary min-heap of source indices: `next` is one source step plus `O(log k)` key
/// comparisons, and nothing allocates after the first seek. The heap remembers the smaller
/// child of its top, so a step that leaves the top source on top (a run of one source's
/// entries, such as a column's versions in one memtable) costs one comparison, and one that
/// does not costs no more than a plain sift. With two sources every step is one comparison.
/// After an error the cursor must be re-seeked before use.
///
/// ```
/// use pigeonhole_compaction::{MergingCursor, VecCursor};
/// use pigeonhole_format::Cursor;
///
/// let newer = VecCursor::new(vec![(b"b".to_vec(), b"2".to_vec())]);
/// let older = VecCursor::new(vec![(b"a".to_vec(), b"1".to_vec()), (b"c".to_vec(), b"3".to_vec())]);
/// let mut m = MergingCursor::new(vec![newer, older]);
/// m.seek_to_first().unwrap();
/// let mut keys = Vec::new();
/// while m.valid() {
///     keys.push(m.key().to_vec());
///     m.next().unwrap();
/// }
/// assert_eq!(keys, [b"a", b"b", b"c"]);
/// ```
#[derive(Debug)]
pub struct MergingCursor<C> {
    sources: Vec<C>,
    /// Indices of the valid sources, a min-heap by (key, index).
    heap: Vec<usize>,
    /// Scratch for `skip_row`.
    row: Vec<u8>,
    /// The heap position (1 or 2) of the smaller child of the top, when there is one.
    runner: usize,
}

impl<C: Cursor> MergingCursor<C> {
    /// Merges `sources`.
    pub fn new(sources: Vec<C>) -> Self {
        let heap = Vec::with_capacity(sources.len());
        Self {
            sources,
            heap,
            row: Vec::new(),
            runner: 0,
        }
    }

    /// The source the cursor is on (to pin its current value without copying), or `None` if
    /// the cursor is not valid.
    pub fn current(&self) -> Option<&C> {
        self.heap.first().map(|&i| &self.sources[i])
    }

    /// The sources, in the order given to [`MergingCursor::new`].
    pub fn sources(&self) -> &[C] {
        &self.sources
    }

    /// The source list, to refill in place between uses (a point get per call keeps one
    /// cursor and allocates nothing for the merge); call [`MergingCursor::reset`] after
    /// changing it.
    pub fn sources_mut(&mut self) -> &mut Vec<C> {
        &mut self.sources
    }

    /// Puts the cursor in the state [`MergingCursor::new`] gives it over its current sources
    /// (unpositioned), keeping its allocations.
    ///
    /// ```
    /// use pigeonhole_compaction::{MergingCursor, VecCursor};
    /// use pigeonhole_format::Cursor;
    ///
    /// let mut m = MergingCursor::new(Vec::new());
    /// for _ in 0..2 {
    ///     m.sources_mut().clear();
    ///     m.sources_mut().push(VecCursor::new(vec![(b"b".to_vec(), b"2".to_vec())]));
    ///     m.sources_mut().push(VecCursor::new(vec![(b"a".to_vec(), b"1".to_vec())]));
    ///     m.reset();
    ///     m.seek_to_first().unwrap();
    ///     assert_eq!(m.key(), b"a");
    /// }
    /// ```
    pub fn reset(&mut self) {
        // Every field, so that one added later is reset too.
        let Self {
            sources,
            heap,
            row,
            runner,
        } = self;
        heap.clear();
        heap.reserve(sources.len());
        row.clear();
        *runner = 0;
    }

    fn less(&self, a: usize, b: usize) -> bool {
        match compare(self.sources[a].key(), self.sources[b].key()) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => a < b,
            std::cmp::Ordering::Greater => false,
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        let n = self.heap.len();
        loop {
            let l = 2 * i + 1;
            if l >= n {
                return;
            }
            let r = l + 1;
            let c = if r < n && self.less(self.heap[r], self.heap[l]) {
                r
            } else {
                l
            };
            if !self.less(self.heap[c], self.heap[i]) {
                return;
            }
            self.heap.swap(c, i);
            i = c;
        }
    }

    /// Rebuilds the heap from every valid source.
    fn rebuild(&mut self) {
        self.heap.clear();
        for (i, s) in self.sources.iter().enumerate() {
            if s.valid() {
                self.heap.push(i);
            }
        }
        for i in (0..self.heap.len() / 2).rev() {
            self.sift_down(i);
        }
        self.set_runner();
    }

    /// Finds the smaller child of the top after the heap changed (0 when there is none).
    fn set_runner(&mut self) {
        self.runner = match self.heap.len() {
            0 | 1 => 0,
            2 => 1,
            _ => 1 + usize::from(self.less(self.heap[2], self.heap[1])),
        };
    }
}

impl<C: Cursor> Cursor for MergingCursor<C> {
    type Error = C::Error;

    fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    fn key(&self) -> &[u8] {
        self.sources[self.heap[0]].key()
    }

    fn value(&self) -> &[u8] {
        self.sources[self.heap[0]].value()
    }

    fn seek_to_first(&mut self) -> Result<(), C::Error> {
        self.heap.clear();
        for s in &mut self.sources {
            s.seek_to_first()?;
        }
        self.rebuild();
        Ok(())
    }

    fn seek(&mut self, target: &[u8]) -> Result<(), C::Error> {
        self.heap.clear();
        for s in &mut self.sources {
            s.seek(target)?;
        }
        self.rebuild();
        Ok(())
    }

    /// Moves only the sources still behind `target`: past a column with many versions that
    /// is usually the one source holding them (a memtable), not every SST of the read.
    fn seek_forward(&mut self, target: &[u8]) -> Result<(), C::Error> {
        let mut moved = false;
        for &i in &self.heap {
            if self.sources[i].key() < target {
                self.sources[i].seek_forward(target)?;
                moved = true;
            }
        }
        if moved {
            self.rebuild();
        }
        Ok(())
    }

    fn next(&mut self) -> Result<(), C::Error> {
        let Some(&top) = self.heap.first() else {
            return Ok(());
        };
        self.sources[top].next()?;
        if !self.sources[top].valid() {
            let last = self.heap.len() - 1;
            self.heap.swap(0, last);
            self.heap.pop();
            self.sift_down(0);
        } else {
            // Only the top moved: it stays while it is below the smaller of its children,
            // one comparison; otherwise that child takes its place and it sinks from there.
            let r = self.runner;
            if r == 0 || self.less(top, self.heap[r]) {
                return Ok(());
            }
            self.heap.swap(0, r);
            self.sift_down(r);
        }
        self.set_runner();
        Ok(())
    }

    fn skip_row(&mut self) -> Result<(), C::Error> {
        if !self.valid() {
            return Ok(());
        }
        let Ok(n) = row_prefix_len(self.key()) else {
            return self.next();
        };
        self.row.clear();
        let top = self.heap[0];
        self.row.extend_from_slice(&self.sources[top].key()[..n]);
        // Only the sources positioned inside the row move; each skips with its own row-start
        // table where it has one.
        let mut moved = false;
        for &i in &self.heap {
            if self.sources[i].key().starts_with(&self.row) {
                self.sources[i].skip_row()?;
                moved = true;
            }
        }
        if moved {
            self.rebuild();
        }
        Ok(())
    }
}

/// Applies a [`ScanFilter`] to any cursor with [`ScanFilter::admits`], so sources without
/// built-in pushdown (memtables) filter exactly like `SstIter` does. Excluded qualifiers are
/// skipped with a seek to [`ScanFilter::next_admissible`], not stepped over.
///
/// ```
/// use pigeonhole_compaction::{FilteredCursor, VecCursor};
/// use pigeonhole_format::Cursor;
/// use pigeonhole_format::key::{Kind, encode_key};
/// use pigeonhole_format::scan::{QualifierFilter, ScanFilter};
///
/// let key = |q: &[u8]| {
///     let mut k = Vec::new();
///     encode_key(&mut k, b"row", q, 1, 1, Kind::Put).unwrap();
///     k
/// };
/// let src = VecCursor::new(vec![(key(b"a:1"), b"\x00x".to_vec()), (key(b"b:1"), b"\x00y".to_vec())]);
/// let mut filter = ScanFilter::all();
/// filter.qualifiers = QualifierFilter::Prefix(b"b:".to_vec());
/// let mut c = FilteredCursor::new(src, filter);
/// c.seek_to_first().unwrap();
/// assert_eq!(c.key(), &key(b"b:1")[..]);
/// c.next().unwrap();
/// assert!(!c.valid());
/// ```
#[derive(Debug)]
pub struct FilteredCursor<C> {
    inner: C,
    filter: ScanFilter,
    all: bool,
    hint: Vec<u8>,
}

impl<C: Cursor> FilteredCursor<C> {
    /// Wraps `inner`.
    pub fn new(inner: C, filter: ScanFilter) -> Self {
        Self {
            all: filter.is_all(),
            inner,
            filter,
            hint: Vec::new(),
        }
    }

    /// The wrapped cursor (to pin its current value).
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Moves forward to the first admitted entry at or after the current one.
    fn settle(&mut self) -> Result<(), C::Error> {
        if self.all {
            return Ok(());
        }
        while self.inner.valid() && !self.filter.admits(self.inner.key()) {
            self.hint.clear();
            if !self
                .filter
                .next_admissible(self.inner.key(), &mut self.hint)
            {
                // Nothing later in this row can be admitted.
                self.inner.skip_row()?;
            } else if self.hint.as_slice() > self.inner.key() {
                self.inner.seek(&self.hint)?;
            } else {
                self.inner.next()?;
            }
        }
        Ok(())
    }
}

impl<C: Cursor> Cursor for FilteredCursor<C> {
    type Error = C::Error;

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn seek_to_first(&mut self) -> Result<(), C::Error> {
        self.inner.seek_to_first()?;
        self.settle()
    }

    fn seek(&mut self, target: &[u8]) -> Result<(), C::Error> {
        self.inner.seek(target)?;
        self.settle()
    }

    fn next(&mut self) -> Result<(), C::Error> {
        self.inner.next()?;
        self.settle()
    }

    fn skip_row(&mut self) -> Result<(), C::Error> {
        self.inner.skip_row()?;
        self.settle()
    }
}

/// An in-memory sorted cursor over owned `(internal key, stored value)` pairs: the mock
/// source for tests and examples of the layers above. Never fails.
///
/// ```
/// use pigeonhole_compaction::VecCursor;
/// use pigeonhole_format::Cursor;
///
/// let mut c = VecCursor::new(vec![(b"b".to_vec(), vec![]), (b"a".to_vec(), vec![])]);
/// c.seek(b"aa").unwrap(); // entries are sorted on construction
/// assert_eq!(c.key(), b"b");
/// ```
#[derive(Debug, Clone, Default)]
pub struct VecCursor {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl VecCursor {
    /// A cursor over `entries`, sorted by key. Unpositioned until a seek.
    pub fn new(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        entries.sort();
        let pos = entries.len();
        Self { entries, pos }
    }
}

impl Cursor for VecCursor {
    type Error = crate::Error;

    fn valid(&self) -> bool {
        self.pos < self.entries.len()
    }

    fn key(&self) -> &[u8] {
        &self.entries[self.pos].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.pos].1
    }

    fn seek_to_first(&mut self) -> crate::Result<()> {
        self.pos = 0;
        Ok(())
    }

    fn seek(&mut self, target: &[u8]) -> crate::Result<()> {
        self.pos = self.entries.partition_point(|(k, _)| k.as_slice() < target);
        Ok(())
    }

    fn next(&mut self) -> crate::Result<()> {
        if self.pos < self.entries.len() {
            self.pos += 1;
        }
        Ok(())
    }
}
