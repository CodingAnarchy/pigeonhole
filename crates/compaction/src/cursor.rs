//! Cursor combinators: the k-way merge, the scan filter for sources without pushdown, and an
//! in-memory cursor for tests and examples.

use pigeonhole_format::Cursor;
use pigeonhole_format::key::{compare, row_prefix_len};
use pigeonhole_format::scan::{CompiledFilter, ScanFilter, Verdict};

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
/// **Lazy sources (D288).** A source may be unsettled
/// ([`Cursor::settled`]): its key is then only a lower bound on its next entry, and it keeps
/// its place in the heap by that bound. After every move the merge settles its top up to the
/// key of the runner (the top's smaller child), until the top is settled or the heap is
/// empty. Each round either settles the top, exhausts it, or moves it strictly past the
/// runner's key (a source parks only strictly above its bound), so the rounds end. The top
/// is then the true smallest entry: every other source's next entry is at or above its own
/// bound, which is at or above the runner's. A source never walks further ahead of the merge
/// than one entry, or one seek of its filter, past the runner.
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
    /// The source at the top of the heap (`heap[0]`), kept beside it so `key` and `value`
    /// read it without a bounds check on the heap; 0 when the heap is empty.
    top: usize,
    /// Whether a source may be unsettled (D288), so the top needs settling after a move:
    /// derived from the sources ([`Cursor::may_park`]) at every rebuild of the heap, so it
    /// cannot be missed. A merge of eager sources tests only this.
    lazy: bool,
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
            top: 0,
            lazy: false,
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
            top,
            lazy,
        } = self;
        heap.clear();
        heap.reserve(sources.len());
        row.clear();
        *runner = 0;
        *top = 0;
        *lazy = false;
    }

    // Always inlined: the lazy settle's extra call site (D288) otherwise moved it out of
    // line in every hot caller (+0.8% on the unfiltered row reads).
    #[inline(always)]
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
        let mut lazy = false;
        for (i, s) in self.sources.iter().enumerate() {
            lazy |= s.may_park();
            if s.valid() {
                self.heap.push(i);
            }
        }
        self.lazy = lazy;
        for i in (0..self.heap.len() / 2).rev() {
            self.sift_down(i);
        }
        self.set_runner();
    }

    /// Finds the smaller child of the top after the heap changed (0 when there is none).
    /// Always inlined: the lazy settle's call site (D288) otherwise moved it out of line.
    #[inline(always)]
    fn set_runner(&mut self) {
        self.top = self.heap.first().copied().unwrap_or(0);
        self.runner = match self.heap.len() {
            0 | 1 => 0,
            2 => 1,
            _ => 1 + usize::from(self.less(self.heap[2], self.heap[1])),
        };
    }

    /// Settles the top after a move, in a lazy merge (D288); one predictable test otherwise,
    /// and one more in a lazy merge whose top landed on an admitted entry.
    #[inline]
    fn settle_top(&mut self) -> Result<(), C::Error> {
        if self.lazy && !self.heap.is_empty() && !self.sources[self.top].settled() {
            self.settle_top_lazy()
        } else {
            Ok(())
        }
    }

    /// [`Self::settle_top`] for the once-per-read moves (seeks, row skips): only the flag
    /// test inline, so an eager merge's seek stays as small as before (point gets).
    #[inline(always)]
    fn settle_top_after_seek(&mut self) -> Result<(), C::Error> {
        if self.lazy {
            self.settle_top_lazy()
        } else {
            Ok(())
        }
    }

    /// Settles the top up to the runner's key until the top is settled or the heap is empty
    /// (the rounds end: see the type's docs).
    #[cold]
    fn settle_top_lazy(&mut self) -> Result<(), C::Error> {
        while let Some(&t) = self.heap.first() {
            if self.sources[t].settled() {
                return Ok(());
            }
            if self.runner == 0 {
                self.sources[t].settle_until(None)?;
            } else {
                let r = self.heap[self.runner];
                // Two distinct sources: the top moves up to the runner's key.
                let (top, runner) = if t < r {
                    let (a, b) = self.sources.split_at_mut(r);
                    (&mut a[t], &b[0])
                } else {
                    let (a, b) = self.sources.split_at_mut(t);
                    (&mut b[0], &a[r])
                };
                top.settle_until(Some(runner.key()))?;
            }
            let r = self.runner;
            if !self.sources[t].valid() {
                let last = self.heap.len() - 1;
                self.heap.swap(0, last);
                self.heap.pop();
                self.sift_down(0);
            } else if r == 0 || self.less(t, self.heap[r]) {
                // Settled at or below the runner's key: the heap is unchanged. (Parked, the
                // top is strictly past the runner's key, so this needs no settled test.)
                continue;
            } else {
                // Parked past the runner's key, or tied with an earlier source: the runner
                // takes its place, as after `Cursor::next`.
                self.heap.swap(0, r);
                self.sift_down(r);
            }
            self.set_runner();
        }
        Ok(())
    }
}

impl<C: Cursor> Cursor for MergingCursor<C> {
    type Error = C::Error;

    fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    fn key(&self) -> &[u8] {
        self.sources[self.top].key()
    }

    fn value(&self) -> &[u8] {
        self.sources[self.top].value()
    }

    fn seek_to_first(&mut self) -> Result<(), C::Error> {
        self.heap.clear();
        for s in &mut self.sources {
            s.seek_to_first()?;
        }
        self.rebuild();
        self.settle_top_after_seek()
    }

    fn seek(&mut self, target: &[u8]) -> Result<(), C::Error> {
        self.heap.clear();
        for s in &mut self.sources {
            s.seek(target)?;
        }
        self.rebuild();
        self.settle_top_after_seek()
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
        self.settle_top_after_seek()
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
                return self.settle_top();
            }
            self.heap.swap(0, r);
            self.sift_down(r);
        }
        self.set_runner();
        self.settle_top()
    }

    /// Only the top source moves, then the heap settles as after [`Cursor::next`].
    #[inline]
    fn skip_column(&mut self, column: &[u8]) -> Result<bool, C::Error> {
        let Some(&top) = self.heap.first() else {
            return Ok(false);
        };
        if !self.sources[top].skip_column(column)? {
            return Ok(false);
        }
        if !self.sources[top].valid() {
            let last = self.heap.len() - 1;
            self.heap.swap(0, last);
            self.heap.pop();
            self.sift_down(0);
        } else {
            let r = self.runner;
            if r == 0 || self.less(top, self.heap[r]) {
                self.settle_top()?;
                return Ok(true);
            }
            self.heap.swap(0, r);
            self.sift_down(r);
        }
        self.set_runner();
        self.settle_top()?;
        Ok(true)
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
        self.settle_top_after_seek()
    }
}

/// Applies a [`ScanFilter`] to any cursor, compiled ([`ScanFilter::compile`]) as `SstIter`
/// compiles it, so sources without built-in pushdown (memtables) filter exactly like
/// `SstIter` does. Excluded qualifiers are skipped with a seek to the filter's hint, not
/// stepped over.
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
    filter: CompiledFilter,
    all: bool,
    hint: Vec<u8>,
    /// Lazy mode (D288): a move only lands, and a merge settles the cursor when it reaches
    /// the merge's top. False for an unfiltered cursor, which has nothing to settle.
    lazy: bool,
    /// In lazy mode, whether the current entry is not admitted (a lower bound only).
    parked: bool,
    /// While parked, the current entry's verdict (with its hint in `hint`), so settling
    /// does not check it again.
    verdict: Verdict,
}

impl<C: Cursor> FilteredCursor<C> {
    /// Wraps `inner`.
    pub fn new(inner: C, filter: ScanFilter) -> Self {
        Self {
            all: filter.is_all(),
            filter: filter.into_compiled(),
            inner,
            hint: Vec::new(),
            lazy: false,
            parked: false,
            verdict: Verdict::Admit,
        }
    }

    /// Wraps `inner` in lazy mode (D288), for a source under a [`MergingCursor`]: a move
    /// lands on the next entry without filtering, and the merge settles the cursor
    /// ([`Cursor::settle_until`]) only when it reaches the merge's top, up to the next
    /// source's key. A source with nothing admissible near the merge's position then stops
    /// there instead of walking ahead to its next admitted entry, however far that is.
    /// Unsettled entries must not be read on their own: use it only under a merge.
    ///
    /// ```
    /// use pigeonhole_compaction::{FilteredCursor, MergingCursor, VecCursor};
    /// use pigeonhole_format::Cursor;
    /// use pigeonhole_format::key::{Kind, encode_key};
    /// use pigeonhole_format::scan::{QualifierFilter, ScanFilter};
    ///
    /// let key = |row: &[u8], q: &[u8]| {
    ///     let mut k = Vec::new();
    ///     encode_key(&mut k, row, q, 1, 1, Kind::Put).unwrap();
    ///     k
    /// };
    /// let mut filter = ScanFilter::all();
    /// filter.qualifiers = QualifierFilter::Prefix(b"b".to_vec());
    /// let wanted = VecCursor::new(vec![(key(b"r1", b"b"), vec![]), (key(b"r2", b"b"), vec![])]);
    /// // Nothing admissible: an eager filter would walk all of it on the first seek.
    /// let other = VecCursor::new((0..100u8).map(|i| (key(&[b's', i], b"a"), vec![])).collect());
    /// let mut m = MergingCursor::new(vec![
    ///     FilteredCursor::lazy(wanted, filter.clone()),
    ///     FilteredCursor::lazy(other, filter),
    /// ]);
    /// m.seek_to_first().unwrap();
    /// assert_eq!(m.key(), &key(b"r1", b"b")[..]);
    /// // The other source parked on its first entry, past the merge's position.
    /// assert!(!m.sources()[1].settled());
    /// ```
    pub fn lazy(inner: C, filter: ScanFilter) -> Self {
        let mut c = Self::new(inner, filter);
        c.lazy = !c.all;
        c
    }

    /// The wrapped cursor (to pin its current value).
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Moves forward to the first admitted entry at or after the current one. Inline, so
    /// a cursor with nothing to filter (every memtable source of an unfiltered read) pays a
    /// check per step, not a call.
    #[inline]
    fn settle(&mut self) -> Result<(), C::Error> {
        if self.all {
            return Ok(());
        }
        if self.lazy {
            self.land();
            return Ok(());
        }
        self.settle_filtered()
    }

    /// Lazy mode: checks the entry a move landed on, once. Admitted, the cursor is settled;
    /// otherwise it parks there and keeps the verdict for [`Cursor::settle_until`].
    #[inline]
    fn land(&mut self) {
        self.parked = false;
        if self.inner.valid() {
            let v = self.filter.check(self.inner.key(), &mut self.hint);
            if v != Verdict::Admit {
                self.parked = true;
                self.verdict = v;
            }
        }
    }

    fn settle_filtered(&mut self) -> Result<(), C::Error> {
        while self.inner.valid() {
            match self.filter.check(self.inner.key(), &mut self.hint) {
                Verdict::Admit => break,
                // Nothing later in this row can be admitted.
                Verdict::SkipRow => self.inner.skip_row()?,
                Verdict::Seek => self.inner.seek(&self.hint)?,
                Verdict::Next => self.inner.next()?,
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

    #[inline]
    fn settled(&self) -> bool {
        !self.parked
    }

    #[inline]
    fn may_park(&self) -> bool {
        self.lazy
    }

    /// The eager filter's loop, parking on the first entry strictly past `bound`. The entry
    /// it is parked on was checked when the cursor landed there: its verdict is acted on
    /// without a second check.
    fn settle_until(&mut self, bound: Option<&[u8]>) -> Result<(), C::Error> {
        if !self.parked {
            return Ok(());
        }
        let mut verdict = self.verdict;
        loop {
            if let Some(b) = bound
                && compare(self.inner.key(), b) == std::cmp::Ordering::Greater
            {
                self.verdict = verdict;
                return Ok(());
            }
            match verdict {
                Verdict::Admit => break,
                Verdict::SkipRow => self.inner.skip_row()?,
                Verdict::Seek => self.inner.seek(&self.hint)?,
                Verdict::Next => self.inner.next()?,
            }
            if !self.inner.valid() {
                break;
            }
            verdict = self.filter.check(self.inner.key(), &mut self.hint);
        }
        self.parked = false;
        Ok(())
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

    fn seek_forward(&mut self, target: &[u8]) -> Result<(), C::Error> {
        // The inner cursor's own forward seek (a memtable's finger search), not a full one.
        self.inner.seek_forward(target)?;
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

    #[inline]
    fn skip_column(&mut self, column: &[u8]) -> Result<bool, C::Error> {
        if !self.inner.skip_column(column)? {
            return Ok(false);
        }
        self.settle()?;
        Ok(true)
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
