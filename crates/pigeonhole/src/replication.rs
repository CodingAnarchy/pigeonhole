//! Replication apply (D315 PR 3, ICR 0114): what a replica applies, taken from the primary's
//! feed with the primary's ids. Behind `__unstable-replication`, hidden: not part of the API.

use std::collections::HashSet;

use pigeonhole_engine::{CatalogEvent, FamilyId, TableId, WriteBatch};
use pigeonhole_format::Kind;

use crate::feed::StreamPosition;
use crate::{ChangeKind, Error, ErrorCode, FeedEvent, FeedToken, Pigeonhole, Result};

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

/// What a replica stores with each apply (D315 PR 3), in its state cell: the primary feed's
/// position after the last event applied, and the token right after it (none until the first
/// event is applied: the first apply records where it starts before it applies anything,
/// so a crash in its middle resumes there).
#[derive(Debug, Clone, PartialEq)]
struct State {
    /// The primary's database id (simp724 on #1436): a replica follows only this database.
    /// Zero in a version 1 cell, written before it was recorded.
    primary: [u8; 16],
    after: StreamPosition,
    token: Option<FeedToken>,
}

/// The state cell's encoding version.
const STATE_VERSION: u8 = 2;

impl State {
    fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(18 + self.token.as_ref().map_or(0, |t| t.as_bytes().len()));
        out.push(STATE_VERSION);
        out.extend_from_slice(&self.primary);
        out.extend_from_slice(&self.after.seqno.to_le_bytes());
        out.extend_from_slice(&self.after.catalog.to_le_bytes());
        match &self.token {
            Some(t) => {
                out.push(1);
                out.extend_from_slice(t.as_bytes());
            }
            None => out.push(0),
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let bad = || Error::new(ErrorCode::Corruption, "a replica's state cell is malformed");
        let (&[version], rest) = bytes.split_first_chunk::<1>().ok_or_else(bad)?;
        // Version 1 had no primary id (D315 PR 3, before #1436's bootstrap).
        let (primary, rest) = match version {
            1 => ([0; 16], rest),
            2 => {
                let (id, rest) = rest.split_first_chunk::<16>().ok_or_else(bad)?;
                (*id, rest)
            }
            _ => return Err(bad()),
        };
        let (seqno, rest) = rest.split_first_chunk::<8>().ok_or_else(bad)?;
        let (catalog, rest) = rest.split_first_chunk::<8>().ok_or_else(bad)?;
        let (&[has_token], token) = rest.split_first_chunk::<1>().ok_or_else(bad)?;
        Ok(Self {
            primary,
            after: StreamPosition {
                seqno: u64::from_le_bytes(*seqno),
                catalog: u64::from_le_bytes(*catalog),
            },
            token: match has_token {
                0 if token.is_empty() => None,
                1 => Some(FeedToken::from_bytes(token)),
                _ => return Err(bad()),
            },
        })
    }
}

/// A cell at one timestamp, as one commit may write it once: table, family, row, qualifier
/// and timestamp.
type Cell<'a> = (TableId, FamilyId, &'a [u8], &'a [u8], u64);

/// The engine kind of a change.
fn logged_kind(kind: ChangeKind) -> Kind {
    match kind {
        ChangeKind::Put => Kind::Put,
        ChangeKind::Merge => Kind::Merge,
        ChangeKind::DeleteCell => Kind::CellDelete,
        ChangeKind::DeleteColumn => Kind::ColumnDelete,
        ChangeKind::DeleteFamily => Kind::FamilyDelete,
    }
}

/// The primary database a token belongs to (zeros for a token that isn't a feed of every
/// table's, which a replicated event never carries).
fn primary_of(token: &FeedToken) -> [u8; 16] {
    pigeonhole_engine::all_tables_token_position(token.as_bytes()).map_or([0; 16], |(id, _, _)| id)
}

fn invalid(message: String) -> Error {
    Error::new(ErrorCode::InvalidArgument, message)
}

impl Pigeonhole {
    /// Makes this database, a bootstrap copy of a primary (its [`backup_at`](Self::backup_at)
    /// of a snapshot), a replica that follows the primary from `token`: the token of a feed of
    /// every table opened `from_snapshot` on that same snapshot (#1436, ICR 0125). The first
    /// [`apply_replicated`](Self::apply_replicated) chains on it. Refused on a replica, and on
    /// a promoted one. Not part of the stable API.
    #[doc(hidden)]
    pub fn begin_replication(&self, token: &FeedToken) -> Result<()> {
        let (primary, seqno, catalog) =
            pigeonhole_engine::all_tables_token_position(token.as_bytes())?;
        let state = State {
            primary,
            after: StreamPosition { seqno, catalog },
            token: Some(token.clone()),
        };
        self.engine().begin_replication(&state.encode(), true)?;
        Ok(())
    }

    /// Turns this replica into a primary at the last event it applied (#1436, ICR 0125):
    /// returns the primary's token stored with that event (`None` if it applied none). It then
    /// takes writes, and its own feed goes on; an apply after it is refused. Stop following
    /// the old primary first, and stop the old primary's writers: nothing fences it. Not part
    /// of the stable API.
    #[doc(hidden)]
    pub fn promote(&self) -> Result<Option<FeedToken>> {
        let token = self.replicated_token()?;
        self.engine().promote()?;
        Ok(token)
    }

    /// The primary feed's token a replica stored with the last event it applied (D315 PR 3):
    /// resume the primary's feed from it ([`FeedBuilder::resume`](crate::FeedBuilder::resume))
    /// to go on from there. `None` before the first event is applied. Not part of the stable
    /// API.
    #[doc(hidden)]
    pub fn replicated_token(&self) -> Result<Option<FeedToken>> {
        Ok(self.replication_state()?.and_then(|s| s.token))
    }

    fn replication_state(&self) -> Result<Option<State>> {
        self.engine()
            .replication_state()?
            .map(|b| State::decode(&b))
            .transpose()
    }

    /// Where an apply starting at `before` stands: the stored state, which must end there
    /// (`run[0].before == stored.after`: anything else is a gap or an overlap). The first
    /// apply, with none stored, needs a database with no tables of its own, and records
    /// `before` as where it starts.
    fn chain(&self, before: StreamPosition, primary: [u8; 16]) -> Result<()> {
        match self.replication_state()? {
            // Another database's history (#1436): a replica follows only its primary.
            Some(s) if s.primary != [0; 16] && s.primary != primary => Err(invalid(
                "the events are another database's than the replica's primary".to_owned(),
            )),
            Some(s) if s.after == before => Ok(()),
            Some(s) => Err(invalid(format!(
                "a replicated run must start where the last apply ended: it starts at \
                 {before:?}, the last apply ended at {:?} (a gap or an overlap)",
                s.after
            ))),
            None => {
                if !self.engine().tables().is_empty() {
                    return Err(invalid(
                        "a replica's first apply needs a database with no tables of its own"
                            .to_owned(),
                    ));
                }
                let start = State {
                    primary,
                    after: before,
                    token: None,
                };
                self.engine().begin_replication(&start.encode(), false)?;
                Ok(())
            }
        }
    }

    /// Applies a run of a primary's commits (D315 PR 3), in order, with `token` (the last
    /// event's, [`ReplicatedEvent::token`]) stored in the same commit as the run's last change,
    /// so what a replica applied and the token it resumes from never part. The run must start
    /// where the last apply ended and each event where the one before it ended; it holds
    /// commits only (a catalog change goes through [`apply_catalog`](Self::apply_catalog)).
    ///
    /// One atomic `GroupSync` commit, unless two of the run's commits write the same cell
    /// at the same timestamp (one commit can't hold both), or the run is larger than a
    /// commit should carry (a quarter of one shard's memtable share, at least 64 KiB): it
    /// then splits there, between primary commits, each part committed with the token of its
    /// own last event, so a crash between parts resumes after the last one committed. Refused with `InvalidArgument`: an empty run, a catalog
    /// change in it, a gap or an overlap, or a `token` that isn't the last event's. Not part
    /// of the stable API.
    #[doc(hidden)]
    pub fn apply_replicated(&self, run: &[ReplicatedEvent], token: &FeedToken) -> Result<()> {
        let (Some(first), Some(last)) = (run.first(), run.last()) else {
            return Err(invalid("an empty replicated run".to_owned()));
        };
        if run.iter().any(ReplicatedEvent::is_catalog) {
            return Err(invalid(
                "a catalog change in a replicated run: apply it with apply_catalog".to_owned(),
            ));
        }
        if let Some(w) = run.windows(2).find(|w| w[1].before != w[0].after) {
            return Err(invalid(format!(
                "a replicated run's events must chain: one ends at {:?}, the next starts at {:?}",
                w[0].after, w[1].before
            )));
        }
        if last.token != *token {
            return Err(invalid(
                "the token to store must be the run's last event's".to_owned(),
            ));
        }
        self.chain(first.before, primary_of(&first.token))?;
        let mut batch = WriteBatch::new();
        let mut cells: HashSet<Cell<'_>> = HashSet::new();
        let mut max_ts = 0;
        let mut part_end: Option<&ReplicatedEvent> = None;
        let cap = self.engine().max_replicated_part_bytes();
        let mut part_bytes = 0;
        for event in run {
            let Body::Commit { changes, .. } = &event.body else {
                unreachable!("checked above");
            };
            let bytes: usize = changes
                .iter()
                .map(|c| c.row.len() + c.qualifier.len() + c.stored.len() + 32)
                .sum();
            // A cell and timestamp this part already writes, or a part this event would take
            // past the byte cap (simp724's S1): commit the part first. One event is never
            // split: a primary commit applies whole.
            if let Some(end) = part_end
                && (part_bytes + bytes > cap
                    || changes.iter().any(|c| {
                        cells.contains(&(c.table, c.family, &c.row[..], &c.qualifier[..], c.ts))
                    }))
            {
                self.commit_part(std::mem::take(&mut batch), end, max_ts)?;
                cells.clear();
                max_ts = 0;
                part_bytes = 0;
            }
            part_bytes += bytes;
            for c in changes {
                batch.push_logged(
                    c.table,
                    c.family,
                    logged_kind(c.kind),
                    &c.row,
                    &c.qualifier,
                    c.ts,
                    &c.stored,
                )?;
                cells.insert((c.table, c.family, &c.row[..], &c.qualifier[..], c.ts));
                max_ts = max_ts.max(c.ts);
            }
            part_end = Some(event);
        }
        let end = part_end.expect("a non-empty run");
        if batch.is_empty() {
            // Commits with nothing this database stores (every change was another's): only
            // the state moves.
            let state = State {
                primary: primary_of(&end.token),
                after: end.after,
                token: Some(end.token.clone()),
            };
            self.engine().store_replication_state(&state.encode())?;
            return Ok(());
        }
        self.commit_part(batch, end, max_ts)
    }

    fn commit_part(&self, batch: WriteBatch, end: &ReplicatedEvent, max_ts: u64) -> Result<()> {
        let state = State {
            primary: primary_of(&end.token),
            after: end.after,
            token: Some(end.token.clone()),
        };
        self.engine()
            .apply_replicated(batch, &state.encode(), max_ts)?;
        Ok(())
    }

    /// Applies a catalog change of a primary's feed (D315 PR 3), with the primary's ids,
    /// between runs of commits, then stores `token` (the event's). It must start where the
    /// last apply ended. The change and the token are two commits (the manifest's, then the
    /// state cell's): after a crash between them the change is applied again, and one already
    /// applied as it stands is passed over. Not part of the stable API.
    #[doc(hidden)]
    pub fn apply_catalog(&self, event: &ReplicatedEvent, token: &FeedToken) -> Result<()> {
        let Body::Catalog(change) = &event.body else {
            return Err(invalid(
                "apply_catalog takes a catalog change: apply commits with apply_replicated"
                    .to_owned(),
            ));
        };
        if event.token != *token {
            return Err(invalid("the token to store must be the event's".to_owned()));
        }
        self.chain(event.before, primary_of(&event.token))?;
        self.engine().apply_catalog(&change.edits)?;
        let state = State {
            primary: primary_of(&event.token),
            after: event.after,
            token: Some(event.token.clone()),
        };
        self.engine().store_replication_state(&state.encode())?;
        Ok(())
    }
}
