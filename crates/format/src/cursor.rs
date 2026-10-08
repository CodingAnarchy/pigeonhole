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
    fn seek_forward(&mut self, target: &[u8]) -> Result<(), Self::Error> {
        self.seek(target)
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
