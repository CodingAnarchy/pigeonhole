//! The ordered cursor every sorted source implements (memtables, blocks, SSTs, merges).

/// A positioned, forward-only cursor over `(internal key, stored value)` pairs in key order.
///
/// Implemented by `MemIter`, `SstIter` and the merging cursors above them. Static dispatch
/// only: generic code takes `C: Cursor`, and the engine wraps heterogeneous sources in an enum,
/// so the per-entry path has no virtual calls. Keys and values borrow from the cursor and are
/// valid until it next moves.
pub trait Cursor {
    /// Error produced while moving (I/O, corruption).
    type Error;

    /// Whether the cursor is on an entry. `key` and `value` may only be called when true.
    fn valid(&self) -> bool;

    /// The current internal key.
    fn key(&self) -> &[u8];

    /// The current stored value (empty for deletes).
    fn value(&self) -> &[u8];

    /// Positions on the first entry.
    fn seek_to_first(&mut self) -> Result<(), Self::Error>;

    /// Positions on the first entry whose key is `>= target`.
    fn seek(&mut self, target: &[u8]) -> Result<(), Self::Error>;

    /// Advances one entry.
    fn next(&mut self) -> Result<(), Self::Error>;

    /// Positions on the first entry whose key is `>= target`, where `target` is at or past
    /// the current key (a forward seek). The default seeks; a merging cursor overrides it to
    /// move only its sources still behind `target`.
    ///
    /// On a cursor that is not valid (unpositioned, or exhausted by an earlier move) the
    /// result depends on the implementation: the default and a memtable cursor position as
    /// [`Cursor::seek`] does, but a merging cursor stays exhausted, even for a `target`
    /// before the key it was exhausted from. Use `seek` to position such a cursor.
    fn seek_forward(&mut self, target: &[u8]) -> Result<(), Self::Error> {
        self.seek(target)
    }

    /// Moves past the current entry and possibly more entries of `column` (an internal key
    /// without its suffix), never past an entry outside it, and returns `true`; or returns
    /// `false` without moving, when the cursor has no faster way than [`Cursor::next`]. The
    /// cursor must be valid and on an entry of `column`. A memtable with a stale-tail index
    /// overrides it to pass a column's superseded versions in one jump (D194, ICR 0013); the
    /// default returns `false`.
    fn skip_column(&mut self, column: &[u8]) -> Result<bool, Self::Error> {
        let _ = column;
        Ok(false)
    }

    /// Whether the current entry is one the cursor yields, or only a lower bound on the next
    /// entry it yields (D288). Only a cursor in a lazy mode ([`Cursor::may_park`]) is ever
    /// unsettled, and only a merging cursor that knows it may be reads one: it settles a
    /// source with [`Cursor::settle_until`] when the source reaches its top. The default is
    /// always settled.
    #[inline]
    fn settled(&self) -> bool {
        true
    }

    /// Moves an unsettled cursor forward to the next entry it yields, but parks (stays
    /// unsettled) on the first entry whose key is strictly greater than `bound`, by
    /// [`crate::key::compare`]; `None` settles fully. An entry equal to `bound` is checked, not
    /// parked on, so a merge that settles its top up to the next source's key always either
    /// settles it or passes that key. The default does nothing (always settled).
    fn settle_until(&mut self, bound: Option<&[u8]>) -> Result<(), Self::Error> {
        let _ = bound;
        Ok(())
    }

    /// Whether the cursor may be unsettled after a move (a lazy filtered source, D288): a
    /// merging cursor over it must settle its top. The default never is.
    #[inline]
    fn may_park(&self) -> bool {
        false
    }

    /// Advances past every remaining entry of the current row. Sources with a row-start table
    /// override this to skip without decoding; the default steps with [`Cursor::next`].
    fn skip_row(&mut self) -> Result<(), Self::Error> {
        if !self.valid() {
            return Ok(());
        }
        let Ok(n) = crate::key::row_prefix_len(self.key()) else {
            return self.next();
        };
        // The row prefix ends with a terminator that never occurs inside an escaped row, so
        // a key starting with it belongs to the same row.
        let row = self.key()[..n].to_vec();
        loop {
            self.next()?;
            if !self.valid() || !self.key().starts_with(&row) {
                return Ok(());
            }
        }
    }
}
