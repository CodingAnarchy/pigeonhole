//! Memory events from the change feed (D311 T9, #1343, ICR 0107): a [`Watch`] tells a
//! process, the writer or a reader, that memories arrived, changed or went, without polling.
//! It reads the database's change feed (D310), which must be on and follow the store's
//! table (`mem_{name}`), and turns each commit's changes to memory rows into
//! [`MemoryEvent`]s.

use std::time::{Duration, Instant};

use pigeonhole::{ChangeKind, Commit, Feed, FeedBuilder, FeedToken, Snapshot, Value};

use crate::error::{Error, Result};
use crate::layout::{
    CONTENT, MEMORY_TAG, META, MemoryId, SUMMARY, SUPERSEDED_BY, TEXT, check_namespace,
};

/// What happened to a memory in one commit ([`MemoryCommit::events`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MemoryEvent {
    /// The memory was written: a `remember` (or `remember_in`) of a new memory.
    Remembered(MemoryId),
    /// A consolidation pass wrote the memory as a summary of its group
    /// ([`Store::consolidate`](crate::Store::consolidate)).
    Summarized(MemoryId),
    /// The memory was superseded by `by` (its `superseded_by` was set). `by` itself, an
    /// existing memory, sees an `Updated` for the link.
    Superseded {
        /// The memory superseded.
        id: MemoryId,
        /// The memory that supersedes it.
        by: MemoryId,
    },
    /// The memory was deleted (`delete`, a forget pass, consolidation's `Delete`).
    Deleted(MemoryId),
    /// Something else changed on the memory: an exact-dedup merge into it, `mark_used`, a
    /// link to or from it, a source added, its supersede undone, or a consolidation taking
    /// it in.
    Updated(MemoryId),
}

impl MemoryEvent {
    /// The memory the event is about.
    pub fn id(&self) -> &MemoryId {
        match self {
            MemoryEvent::Remembered(id)
            | MemoryEvent::Summarized(id)
            | MemoryEvent::Superseded { id, .. }
            | MemoryEvent::Deleted(id)
            | MemoryEvent::Updated(id) => id,
        }
    }
}

/// One commit's memory events, in the order the commit first touched each memory: one event
/// per memory, the strongest of what the commit did to it (`Deleted`, then `Remembered` or
/// `Summarized`, then `Superseded`, then `Updated`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MemoryCommit {
    /// The commit's sequence number: the order commits arrive in.
    pub seqno: u64,
    /// The commit's timestamp, µs since the Unix epoch.
    pub timestamp_us: u64,
    /// The events, at least one.
    pub events: Vec<MemoryEvent>,
}

/// Where a [`Watch`] is, to continue from later ([`WatchBuilder::resume`]), in this open of
/// the database or a later one: the change feed's token.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatchToken(FeedToken);

impl WatchToken {
    /// The token's bytes, to store.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// A token from stored bytes. A token that isn't this database's fails at
    /// [`WatchBuilder::open`].
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        WatchToken(FeedToken::from_bytes(bytes))
    }
}

/// A [`Watch`] to open ([`Store::watch`](crate::Store::watch),
/// [`ReadStore::watch`](crate::ReadStore::watch)). With none of `from_snapshot` and `resume`,
/// it starts with the commits after [`open`](Self::open).
#[derive(Debug)]
#[must_use = "a watch does nothing until .open()"]
pub struct WatchBuilder {
    feed: FeedBuilder,
    prefixes: Vec<Vec<u8>>,
    bad: Option<Error>,
}

impl WatchBuilder {
    pub(crate) fn new(feed: FeedBuilder, namespaces: &[&str]) -> WatchBuilder {
        let mut bad = None;
        let mut prefixes = Vec::with_capacity(namespaces.len());
        for ns in namespaces {
            match check_namespace(ns) {
                Ok(()) => prefixes.push([ns.as_bytes(), MEMORY_TAG].concat()),
                Err(e) => {
                    bad.get_or_insert(e);
                }
            }
        }
        prefixes.sort();
        prefixes.dedup();
        // One namespace: the feed filters rows itself. Several: by prefix here.
        let feed = match prefixes.as_slice() {
            [one] => feed.prefix(one),
            _ => feed,
        };
        WatchBuilder {
            feed,
            prefixes,
            bad,
        }
    }

    /// Starts after the commits `snapshot` sees: read the store at `snapshot`, then watch,
    /// and nothing is missed or seen twice.
    pub fn from_snapshot(mut self, snapshot: &Snapshot) -> Self {
        self.feed = self.feed.from_snapshot(snapshot);
        self
    }

    /// Continues after the last commit a watch delivered before `token` was taken
    /// ([`Watch::token`]).
    pub fn resume(mut self, token: &WatchToken) -> Self {
        self.feed = self.feed.resume(&token.0);
        self
    }

    /// Delivers a commit once it's visible to reads, not only once it's durable (the
    /// default): earlier by up to a sync, but a power loss can undo a `Buffered` commit it
    /// delivered (`pigeonhole::FeedBuilder::visible`).
    pub fn visible(mut self) -> Self {
        self.feed = self.feed.visible();
        self
    }

    /// Subscribes. `InvalidArgument` for a namespace that isn't one, or when the database's
    /// change feed is off or doesn't follow the store's table (`Options::change_feed`,
    /// `Options::feed_tables`).
    pub fn open(self) -> Result<Watch> {
        if let Some(e) = self.bad {
            return Err(e);
        }
        Ok(Watch {
            feed: self.feed.open()?,
            prefixes: self.prefixes,
        })
    }
}

/// A subscription to a store's memory events (D311 T9): each commit that touched a memory,
/// once, in commit order, as a [`MemoryCommit`]. Commits that touched no memory (another
/// table's, the store's own bookkeeping rows) are passed over.
///
/// In a reader process, call [`next`](Self::next) on a thread of its own: it blocks.
#[derive(Debug)]
pub struct Watch {
    feed: Feed,
    /// `{ns}\0m` for each namespace watched; empty for every namespace.
    prefixes: Vec<Vec<u8>>,
}

impl Watch {
    /// The next commit with memory events, waiting up to `timeout`; `None` if none came in
    /// time. `FeedExpired` once the history the watch needs is gone (`oldest_retained` says
    /// what's left): read the store at a fresh snapshot, then watch
    /// [`from_snapshot`](WatchBuilder::from_snapshot) it.
    pub fn next(&mut self, timeout: Duration) -> Result<Option<MemoryCommit>> {
        let deadline = Instant::now().checked_add(timeout);
        loop {
            let left = deadline.map_or(timeout, |d| d.saturating_duration_since(Instant::now()));
            let Some(c) = self.feed.next(left)? else {
                return Ok(None);
            };
            if let Some(m) = events(c, &self.prefixes) {
                return Ok(Some(m));
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(None);
            }
        }
    }

    /// The next commit with memory events if one is ready, else `None`. Never waits.
    pub fn try_next(&mut self) -> Result<Option<MemoryCommit>> {
        while let Some(c) = self.feed.try_next()? {
            if let Some(m) = events(c, &self.prefixes) {
                return Ok(Some(m));
            }
        }
        Ok(None)
    }

    /// Where the watch is: [`WatchBuilder::resume`] continues after the last commit it
    /// delivered, or passed over.
    pub fn token(&self) -> WatchToken {
        WatchToken(self.feed.token())
    }

    /// Once the watch expired: the oldest commit seqno the feed history still holds, when
    /// known.
    pub fn oldest_retained(&self) -> Option<u64> {
        self.feed.oldest_retained()
    }
}

/// What each memory row of a commit saw, in the order the commit first touched it.
#[derive(Default)]
struct Seen {
    text_put: bool,
    summary: bool,
    content_deleted: bool,
    superseded_by: Option<Vec<u8>>,
}

/// A commit's memory events, or `None` when it has none (rows outside `prefixes`, when any,
/// and every row that isn't a memory's are skipped).
fn events(c: &Commit, prefixes: &[Vec<u8>]) -> Option<MemoryCommit> {
    let mut rows: Vec<(Vec<u8>, Seen)> = Vec::new();
    for ch in c.changes() {
        let row = ch.row();
        if !prefixes.is_empty() && !prefixes.iter().any(|p| row.starts_with(p)) {
            continue;
        }
        if MemoryId::from_row(row).is_none() {
            continue;
        }
        let at = match rows.iter().position(|(r, _)| r.as_slice() == row) {
            Some(i) => i,
            None => {
                rows.push((row.to_vec(), Seen::default()));
                rows.len() - 1
            }
        };
        let seen = &mut rows[at].1;
        match (ch.family(), ch.kind()) {
            (CONTENT, ChangeKind::Put) if ch.qualifier() == TEXT => seen.text_put = true,
            (CONTENT, ChangeKind::DeleteFamily) => seen.content_deleted = true,
            (META, ChangeKind::Put) if ch.qualifier() == SUMMARY => seen.summary = true,
            (META, ChangeKind::Put) if ch.qualifier() == SUPERSEDED_BY => {
                if let Some(Value::Bytes(b)) = ch.value() {
                    seen.superseded_by = Some(b.to_vec());
                }
            }
            _ => {}
        }
    }
    let events: Vec<MemoryEvent> = rows
        .into_iter()
        .filter_map(|(row, seen)| {
            let id = MemoryId::from_row(&row)?;
            Some(if seen.content_deleted {
                MemoryEvent::Deleted(id)
            } else if seen.text_put && seen.summary {
                MemoryEvent::Summarized(id)
            } else if seen.text_put {
                MemoryEvent::Remembered(id)
            } else if let Some(by) = seen.superseded_by.as_deref().and_then(MemoryId::from_row) {
                MemoryEvent::Superseded { id, by }
            } else {
                MemoryEvent::Updated(id)
            })
        })
        .collect();
    (!events.is_empty()).then(|| MemoryCommit {
        seqno: c.seqno(),
        timestamp_us: c.timestamp(),
        events,
    })
}
