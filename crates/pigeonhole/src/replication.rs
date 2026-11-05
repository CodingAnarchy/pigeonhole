//! Replication apply (D315 PR 3, ICR 0114): what a replica applies, taken from the primary's
//! feed with the primary's ids. Behind `__unstable-replication`, hidden: not part of the API.

use pigeonhole_engine::{CatalogEvent, FamilyId, TableId};

use crate::feed::StreamPosition;
use crate::{ChangeKind, FeedEvent, FeedToken};

/// One event of a primary's feed, as a replica applies it ([`Feed::replicated`]): a commit's
/// changes with the primary's table and family ids, or a catalog change with its ids; the
/// feed's position before and after it, so the runs a replica applies chain; and the feed's
/// token right after it, to store with what it applied. Not part of the stable API.
///
/// [`Feed::replicated`]: crate::Feed::replicated
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicatedEvent {
    pub(crate) before: StreamPosition,
    pub(crate) after: StreamPosition,
    pub(crate) token: FeedToken,
    pub(crate) body: Body,
}

/// What a [`ReplicatedEvent`] holds.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Body {
    Commit {
        seqno: u64,
        ts: u64,
        changes: Vec<ReplicatedChange>,
    },
    Catalog(CatalogEvent),
}

/// One change of a replicated commit, as the primary logged it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplicatedChange {
    pub(crate) table: TableId,
    pub(crate) family: FamilyId,
    pub(crate) kind: ChangeKind,
    pub(crate) row: Vec<u8>,
    pub(crate) qualifier: Vec<u8>,
    pub(crate) ts: u64,
    /// The stored value, its tag byte first; empty for deletes.
    pub(crate) stored: Vec<u8>,
}

impl ReplicatedEvent {
    pub(crate) fn new(
        event: &FeedEvent,
        (before, after): (StreamPosition, StreamPosition),
        token: FeedToken,
    ) -> Self {
        let body = match event {
            FeedEvent::Commit(c) => Body::Commit {
                seqno: c.seqno(),
                ts: c.timestamp(),
                changes: c
                    .changes()
                    .iter()
                    .map(|ch| {
                        let (table, family) = ch.ids();
                        ReplicatedChange {
                            table,
                            family,
                            kind: ch.kind(),
                            row: ch.row().to_vec(),
                            qualifier: ch.qualifier().to_vec(),
                            ts: ch.timestamp(),
                            stored: ch.stored().to_vec(),
                        }
                    })
                    .collect(),
            },
            FeedEvent::Catalog(change) => Body::Catalog(change.raw().clone()),
        };
        Self {
            before,
            after,
            token,
            body,
        }
    }

    /// Whether it is a catalog change (applied with `apply_catalog`, between runs of commits).
    pub fn is_catalog(&self) -> bool {
        matches!(self.body, Body::Catalog(_))
    }

    /// The primary feed's token right after this event: what a replica stores with it.
    pub fn token(&self) -> &FeedToken {
        &self.token
    }
}
