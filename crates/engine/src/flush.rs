//! The flush seam: where frozen memtables go.
//!
//! Milestone A has no SST writer, so the only backend retains frozen memtables in every view
//! (they are never flushed, and their WAL streams are never checkpointed). Milestone B adds
//! the SST backend behind the same seam: a flush task writes the memtable through
//! `pigeonhole_sst::SstWriter` into a pager extent, sends `AddSst` and `SetFlushed` edits to
//! the manifest task, and retires the memtable once the edit is durable.

/// How frozen memtables are persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FlushBackend {
    /// Keep frozen memtables in memory (and in every view) until the SST backend exists.
    #[default]
    Retain,
}

impl FlushBackend {
    /// Whether a frozen memtable can be persisted and retired.
    pub(crate) fn persists(self) -> bool {
        match self {
            FlushBackend::Retain => false,
        }
    }
}
