//! Replication apply (D315 PR 3, ICR 0114): what a replica applies, taken from the primary's
//! feed with the primary's ids. Behind `__unstable-replication`, hidden: not part of the API.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use pigeonhole_engine::{CatalogEvent, Engine, FamilyId, TableId, WriteBatch};
use pigeonhole_format::Kind;

use crate::feed::StreamPosition;
use crate::{ChangeKind, Error, ErrorCode, FeedEvent, FeedToken, Pigeonhole, Result};

/// A range move's feed retention hold ([`Pigeonhole::hold_feed_retention`], D331): kept
/// while it lives. Not part of the stable API.
#[doc(hidden)]
pub use pigeonhole_engine::FeedRetentionHold;

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
        /// Feed-silent (D331, ICR 0141): a replica applies it silent too. Here, in the
        /// commit's room under the catalog variant, so an event is no larger than before
        /// (#2055's replicate-apply).
        silent: bool,
        /// A spanning commit's xid (D329 2b): a replica applies it tagged, so its own feed, and
        /// a promoted replica's, carries it as the primary's does.
        xid: Option<std::sync::Arc<[u8]>>,
    },
    Catalog(CatalogEvent),
    /// A range drop (D331): rows `[start, end)` of `table` dropped, with the primary's
    /// table id. Body tag 2; never silent. Boxed, so every event keeps its size (#2089's
    /// callgrind).
    RangeDrop(Box<RangeDrop>),
}

/// A range drop a primary's feed delivers to a replica's follower (D331,
/// [`FeedEvent::RangeDrop`](crate::FeedEvent::RangeDrop)): rows `[start, end)` of a table,
/// gone outright at `seqno`. Not part of the stable API.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct RangeDrop {
    pub(crate) seqno: u64,
    pub(crate) table: TableId,
    pub(crate) start: Vec<u8>,
    pub(crate) end: Option<Vec<u8>>,
}

impl RangeDrop {
    pub(crate) fn from_engine(ev: &pigeonhole_engine::RangeDropEvent) -> Self {
        Self {
            seqno: ev.seqno,
            table: ev.table,
            start: ev.start.clone(),
            end: ev.end.clone(),
        }
    }

    /// The drop's seqno, which applies nothing.
    pub fn seqno(&self) -> u64 {
        self.seqno
    }

    /// Whether the drop removes any of rows `[start, end)` of `table` (`end` `None`: to the
    /// table's end): a range move's catch-up stream ends on one that does (D331 PR 4b).
    pub fn overlaps(&self, table: &crate::Table, start: &[u8], end: Option<&[u8]>) -> bool {
        table.core.info.id == self.table
            && end.is_none_or(|e| self.start.as_slice() < e)
            && self.end.as_deref().is_none_or(|e| start < e)
    }
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
        silent: bool,
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
                silent,
                xid: c.xid.clone(),
            },
            FeedEvent::Catalog(change) => Body::Catalog(change.raw().clone()),
            FeedEvent::RangeDrop(d) => Body::RangeDrop(Box::new(d.clone())),
        };
        Self {
            before,
            after,
            token,
            body,
        }
    }

    /// Whether it is a feed-silent commit (D331, ICR 0141): a range move's copy or cleanup,
    /// which a replica applies silent so its own feed matches its primary's.
    pub fn is_silent(&self) -> bool {
        matches!(self.body, Body::Commit { silent: true, .. })
    }

    /// The spanning commit's xid, if it is one's share (D329 2b).
    pub fn xid(&self) -> Option<&[u8]> {
        match &self.body {
            Body::Commit { xid, .. } => xid.as_deref(),
            Body::Catalog(_) | Body::RangeDrop(_) => None,
        }
    }

    /// Whether it is a catalog change (applied with `apply_catalog`, between runs of commits).
    pub fn is_catalog(&self) -> bool {
        matches!(self.body, Body::Catalog(_))
    }

    /// A range drop (D331): apply it with
    /// [`Pigeonhole::apply_range_drop`](crate::Pigeonhole::apply_range_drop).
    pub fn is_range_drop(&self) -> bool {
        matches!(self.body, Body::RangeDrop(_))
    }

    /// The primary feed's token right after this event: what a replica stores with it.
    pub fn token(&self) -> &FeedToken {
        &self.token
    }

    /// The primary's commit seqno the feed stood at after this event: what a replica that
    /// applied it has applied through (#1436's lag).
    pub fn primary_seqno(&self) -> u64 {
        self.after.seqno
    }

    /// The event's bytes, for a replica in another process (#1436: `phdb serve`'s
    /// `Replication.Follow`): versioned, opaque to the transport. [`decode`](Self::decode)
    /// reads them back. A feed-silent commit is version 2 (D331), every other event version
    /// 1; an older replica refuses a version 2 event rather than apply it as an ordinary
    /// write. A range drop (D331 PR 4a) is body tag 2 inside version 1, which builds before
    /// it refuse as an unknown body, halting their follower at the first drop: upgrade a
    /// primary's replicas before the primary (feed1248's S1 on #2089).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self.xid() {
            // A spanning commit's share (D329 2b): version 3, its flags, then the xid. Only
            // then: versions 1 and 2 are what they were, byte for byte.
            Some(xid) => {
                out.push(FLAGS_EVENT_VERSION);
                out.push(u8::from(self.is_silent()) | FLAG_XID);
                put_bytes(&mut out, xid);
            }
            None => out.push(if self.is_silent() {
                SILENT_EVENT_VERSION
            } else {
                EVENT_VERSION
            }),
        }
        for v in [
            self.before.seqno,
            self.before.catalog,
            self.after.seqno,
            self.after.catalog,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        put_bytes(&mut out, self.token.as_bytes());
        match &self.body {
            Body::Commit {
                seqno, ts, changes, ..
            } => {
                out.push(0);
                out.extend_from_slice(&seqno.to_le_bytes());
                out.extend_from_slice(&ts.to_le_bytes());
                out.extend_from_slice(&(changes.len() as u32).to_le_bytes());
                for c in changes {
                    out.extend_from_slice(&c.table.0.to_le_bytes());
                    out.extend_from_slice(&c.family.0.to_le_bytes());
                    out.push(logged_kind(c.kind) as u8);
                    out.extend_from_slice(&c.ts.to_le_bytes());
                    put_bytes(&mut out, &c.row);
                    put_bytes(&mut out, &c.qualifier);
                    put_bytes(&mut out, &c.stored);
                }
            }
            Body::Catalog(ev) => {
                out.push(1);
                for v in [ev.seqno_ceiling, ev.timestamp, ev.sequence] {
                    out.extend_from_slice(&v.to_le_bytes());
                }
                let mut edits = Vec::new();
                for e in &ev.edits {
                    e.encode(&mut edits);
                }
                out.extend_from_slice(&(ev.edits.len() as u32).to_le_bytes());
                put_bytes(&mut out, &edits);
                out.extend_from_slice(&(ev.names.len() as u32).to_le_bytes());
                for (table, name) in &ev.names {
                    out.extend_from_slice(&table.0.to_le_bytes());
                    put_bytes(&mut out, name.as_bytes());
                }
            }
            Body::RangeDrop(d) => {
                out.push(2);
                out.extend_from_slice(&d.seqno.to_le_bytes());
                // WAL type 7's range encoding (FORMAT §10.3), checked when it was written.
                pigeonhole_format::wal::DroppedRange::encode(
                    &mut out,
                    d.table,
                    &d.start,
                    d.end.as_deref(),
                )
                .expect("a range the engine logged");
            }
        }
        out
    }

    /// An event [`encode`](Self::encode) wrote. `InvalidArgument` for anything else, or for a
    /// newer encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader(bytes);
        let (silent, xid) = match r.u8()? {
            EVENT_VERSION => (false, None),
            SILENT_EVENT_VERSION => (true, None),
            FLAGS_EVENT_VERSION => {
                let flags = r.u8()?;
                if flags & !(FLAG_SILENT | FLAG_XID) != 0 {
                    return Err(invalid(
                        "a replicated event with flags this build doesn't know".to_owned(),
                    ));
                }
                // Version 3 is written only with an xid (versions 1 and 2 cover the rest):
                // one without isn't this build's (feed1248's N3 on #2076).
                if flags & FLAG_XID == 0 {
                    return Err(invalid(
                        "a version 3 replicated event without an xid".to_owned(),
                    ));
                }
                let xid = if flags & FLAG_XID != 0 {
                    let x = r.bytes()?;
                    if x.is_empty() || x.len() > 255 {
                        return Err(invalid("a replicated event's xid".to_owned()));
                    }
                    Some(std::sync::Arc::<[u8]>::from(x))
                } else {
                    None
                };
                (flags & FLAG_SILENT != 0, xid)
            }
            _ => return Err(invalid("not a replicated event of this version".to_owned())),
        };
        let before = StreamPosition {
            seqno: r.u64()?,
            catalog: r.u64()?,
        };
        let after = StreamPosition {
            seqno: r.u64()?,
            catalog: r.u64()?,
        };
        let token = FeedToken::from_bytes(r.bytes()?);
        let body = match r.u8()? {
            0 => {
                let (seqno, ts) = (r.u64()?, r.u64()?);
                let n = r.u32()?;
                let mut changes = Vec::with_capacity(n.min(1 << 16) as usize);
                for _ in 0..n {
                    let table = TableId(r.u32()?);
                    let family = FamilyId(r.u32()?);
                    let kind = Kind::from_u8(r.u8()?)
                        .map_err(|_| invalid("a replicated change's kind".to_owned()))?;
                    let ts = r.u64()?;
                    changes.push(ReplicatedChange {
                        table,
                        family,
                        kind: ChangeKind::of(kind),
                        row: r.bytes()?.to_vec(),
                        qualifier: r.bytes()?.to_vec(),
                        ts,
                        stored: r.bytes()?.to_vec(),
                    });
                }
                Body::Commit {
                    seqno,
                    ts,
                    changes,
                    silent,
                    xid,
                }
            }
            1 => {
                // A catalog change is never a spanning commit's share: never version 3.
                if xid.is_some() {
                    return Err(invalid("a catalog change in a flagged event".to_owned()));
                }
                let mut ev = CatalogEvent::default();
                ev.seqno_ceiling = r.u64()?;
                ev.timestamp = r.u64()?;
                ev.sequence = r.u64()?;
                let n = r.u32()?;
                let mut edits = r.bytes()?;
                for _ in 0..n {
                    let (e, used) = pigeonhole_format::manifest::Edit::decode(edits)
                        .map_err(|_| invalid("a replicated catalog edit".to_owned()))?;
                    ev.edits.push(e);
                    edits = &edits[used..];
                }
                for _ in 0..r.u32()? {
                    let table = TableId(r.u32()?);
                    let name = std::str::from_utf8(r.bytes()?)
                        .map_err(|_| invalid("a replicated table name".to_owned()))?;
                    ev.names.push((table, name.to_owned()));
                }
                Body::Catalog(ev)
            }
            2 => Body::RangeDrop(Box::new(decode_drop(bytes, &mut r)?)),
            _ => return Err(invalid("a replicated event's body".to_owned())),
        };
        if silent && matches!(body, Body::Catalog(_)) {
            return Err(invalid("a feed-silent catalog change".to_owned()));
        }
        if !r.0.is_empty() {
            return Err(invalid(
                "trailing bytes after a replicated event".to_owned(),
            ));
        }
        Ok(Self {
            before,
            after,
            token,
            body,
        })
    }
}

/// A range drop's body (tag 2), the rest of `r`, out of the commits' way (#2089's
/// callgrind). Written in the plain version only: keyed on the event's version byte itself,
/// not the flags one version reads, so a later flags form (D329 PR 2b's version 3) is refused
/// too (feed1248's M1 on #2089).
#[cold]
#[inline(never)]
fn decode_drop(event: &[u8], r: &mut Reader<'_>) -> Result<RangeDrop> {
    if event.first() != Some(&EVENT_VERSION) {
        return Err(invalid(
            "a range drop in a version other than the plain one".to_owned(),
        ));
    }
    let seqno = r.u64()?;
    let range = pigeonhole_format::wal::DroppedRange::decode(r.0)
        .map_err(|_| invalid("a replicated range drop".to_owned()))?;
    r.0 = &[];
    let (table, start, end) = range.fields();
    Ok(RangeDrop {
        seqno,
        table,
        start: start.to_vec(),
        end: end.map(<[u8]>::to_vec),
    })
}

/// [`ReplicatedEvent::encode`]'s version.
const EVENT_VERSION: u8 = 1;

/// [`ReplicatedEvent::encode`]'s version for a feed-silent commit (D331, ICR 0141): version 1's
/// fields, read only by builds that apply the mark.
const SILENT_EVENT_VERSION: u8 = 2;

/// [`ReplicatedEvent::encode`]'s version for a spanning commit's share (D329 2b, numbers.md):
/// a flags byte after the version ([`FLAG_SILENT`], [`FLAG_XID`]; any other bit refused),
/// then the xid, then version 1's fields. Written only for an event with an xid.
const FLAGS_EVENT_VERSION: u8 = 3;
/// Version 3's flag for a feed-silent commit.
const FLAG_SILENT: u8 = 1;
/// Version 3's flag for an xid that follows.
const FLAG_XID: u8 = 2;

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

/// Reads [`ReplicatedEvent::encode`]'s fields.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (head, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or_else(|| invalid("a truncated replicated event".to_owned()))?;
        self.0 = rest;
        Ok(*head)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take::<1>()?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take()?))
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        if self.0.len() < n {
            return Err(invalid("a truncated replicated event".to_owned()));
        }
        let (b, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(b)
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

/// Commits one part of a replicated run with the state after its last event, which it
/// returns.
fn commit_part(
    engine: &Engine,
    batch: WriteBatch,
    end: &ReplicatedEvent,
    max_ts: u64,
) -> Result<State> {
    let state = State {
        primary: primary_of(&end.token),
        after: end.after,
        token: Some(end.token.clone()),
    };
    // A part holds one spanning commit's share or none (the split above), so its last
    // event's xid is the whole part's (D329 2b).
    engine.apply_replicated_with(batch, &state.encode(), max_ts, end.xid())?;
    Ok(state)
}

/// The replica state `engine`'s cell holds; `None` on a database that isn't a replica.
fn state_of(engine: &Engine) -> Result<Option<State>> {
    #[cfg(test)]
    tests::CELL_READS.with(|n| n.set(n.get() + 1));
    engine
        .replication_state()?
        .map(|b| State::decode(&b))
        .transpose()
}

/// A replica's state as the last apply through a handle stored it (#2264), on the [`Db`]
/// every clone of the handle shares: each apply chains on it instead of reading the state
/// cell. The cell sits at one fixed timestamp, so every version written since the last
/// flush is one group a read resolves whole, and reading it per apply cost time quadratic
/// in the applies between flushes.
///
/// It is set only after an apply's commits all succeed, emptied by an apply that fails,
/// a bootstrap ([`Pigeonhole::begin_replication`]) and a promote, and kept per engine: a
/// swapped-in engine (a recovery's reopen, a re-bootstrap's
/// [`replace_with`](Pigeonhole::replace_with)) never matches what an earlier one stored, so
/// its first apply reads its own cell. A swap doesn't take the lock: a recovery's swap may
/// come while an apply holding it waits on the failed engine.
///
/// [`Db`]: crate::db::Db
#[derive(Debug, Default)]
pub(crate) struct StateCache(Mutex<Option<Known>>);

impl StateCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Known>> {
        // A panic inside an apply leaves the slot empty (it was taken): nothing to undo.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What [`StateCache`] holds: the state, and the engine it was stored through.
#[derive(Debug)]
struct Known {
    /// Held weakly: the allocation outlives the engine, so no later engine shares its
    /// address while this is here.
    engine: Weak<Engine>,
    state: State,
}

impl Known {
    /// The state, if it was stored through `engine`.
    fn on(self, engine: &Arc<Engine>) -> Option<State> {
        std::ptr::eq(self.engine.as_ptr(), Arc::as_ptr(engine)).then_some(self.state)
    }
}

/// A cell at one timestamp, as one commit may write it once: table, family, row, qualifier
/// and timestamp.
type Cell<'a> = (TableId, FamilyId, &'a [u8], &'a [u8], u64);

/// The engine kind of a change.
pub(crate) fn logged_kind(kind: ChangeKind) -> Kind {
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
    /// Bytes this database's files have read and written since open, as the engine counts
    /// them at its files (D332, ICR 0142): on-disk sizes, readahead included, syncs not. An
    /// estimate for comparing databases on one node, since reads the OS page cache served
    /// count too. A reader instance (`open_reader`) counts nothing. Not part of the stable
    /// API.
    #[doc(hidden)]
    pub fn device_bytes(&self) -> (u64, u64) {
        self.engine().device_bytes()
    }

    /// Every tablet's row range, SST bytes and write load (D332, ICR 0142): what a cluster
    /// balancer splits a partition by. A shared SST's bytes are divided among its tablets;
    /// the write load is the engine's moving average of rows written per balancer interval
    /// (0 with `tablet_changes` off). Not part of the stable API.
    #[doc(hidden)]
    pub fn tablets(&self) -> Vec<TabletLoad> {
        self.engine()
            .tablets()
            .into_iter()
            .map(|t| TabletLoad {
                table: t.table,
                start: t.start,
                end: t.end,
                bytes: t.bytes,
                write_load: t.write_load,
            })
            .collect()
    }

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
        // The first apply reads the state stored here (#2264).
        let mut slot = self.db.replica_state.lock();
        *slot = None;
        self.engine().begin_replication(&state.encode(), true)?;
        Ok(())
    }

    /// Replaces this database's file with the one at `side` (#1436: a replica's
    /// re-bootstrap swaps a fresh copy of its primary in), as a recovery swaps engines
    /// (D265): every handle and clone then reads the new file, and operations in flight on
    /// the old engine fail with `Closed` or retry on the new one. The path is never left
    /// unlocked (simp724's sequence):
    /// 1. `side` is opened and locked (writer byte and presence) first, and must have been
    ///    closed cleanly (`InvalidArgument` otherwise): its own WAL isn't replayed after the
    ///    rename, so a commit only it held would be lost;
    /// 2. the current engine closes, keeping its locks;
    /// 3. the old copy's WAL and sidecar files are removed;
    /// 4. `side` is renamed over the path, and `side`'s own WAL and sidecar files (from an
    ///    open of it under its own name, all checkpointed) are removed;
    /// 5. the old locks are released, and the old file's shm region removed unless a reader
    ///    process is still attached to it;
    /// 6. the new engine opens on `side`'s locks and is swapped in.
    ///
    /// Refused while an automatic recovery runs. If a step after 2 fails, the handle is
    /// left closed (`Closed`): reopen the path. Not part of the stable API.
    #[doc(hidden)]
    pub fn replace_with(&self, side: impl AsRef<std::path::Path>) -> Result<()> {
        let db = &self.db;
        db.check_open()?;
        if db.supervision().is_some_and(|s| s.recovering()) {
            return Err(Error::new(
                ErrorCode::Busy,
                "a recovery is running: replace the file after it",
            ));
        }
        let options = db.reopen_options()?;
        let path = db.path().to_path_buf();
        let vfs = std::sync::Arc::clone(&options.vfs);
        let side = side.as_ref();
        let adopted = pigeonhole_engine::Handover::adopt(&vfs, side, &path, options.allow_fuse)?;
        let old = db.engine_arc();
        let (_closed, held) = old.close_for_reopen();
        pigeonhole_engine::remove_side_files(&vfs, &path)?;
        let io = |e| Error::from(pigeonhole_engine::Error::Io(e));
        vfs.rename(side, &path).map_err(io)?;
        let dir = match path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => std::path::Path::new("."),
        };
        vfs.sync_dir(dir).map_err(io)?;
        // The side file's own WAL streams and sidecar, from an open of it under its own name
        // (the bootstrap's `begin_replication`): checkpointed by its clean close, which
        // `adopt` checked, so nothing in them is needed (simp724's A1 on #1583).
        pigeonhole_engine::remove_side_files(&vfs, side)?;
        // The old file is gone from the path: its shm region goes too, unless a reader
        // process is still attached to it (simp724's A3 on #1583).
        if let Some(held) = held {
            old.retire(held)?;
        }
        let engine =
            pigeonhole_engine::Engine::open_with(&path, options, adopted).map_err(|(e, _)| e)?;
        drop(db.swap_engine(engine));
        drop(old);
        Ok(())
    }

    /// Turns this replica into a primary at the last event it applied (#1436, ICR 0125):
    /// returns the primary's token stored with that event (`None` if it applied none). It then
    /// takes writes, and its own feed goes on; an apply after it is refused. Stop following
    /// the old primary first, and stop the old primary's writers: nothing fences it. If it fails after
    /// dropping the state (its bit's commit failed), the database is already no replica: reopen
    /// it to finish. Not part of the stable API.
    #[doc(hidden)]
    pub fn promote(&self) -> Result<Option<FeedToken>> {
        // Under the state cache's lock, which it leaves empty (#2264): no apply runs beside
        // it, and none after it chains on a state the promote dropped.
        let mut slot = self.db.replica_state.lock();
        *slot = None;
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

    /// The durable feed's frontier: every commit at or below it is durable at its level and in
    /// its stream file, so a feed that isn't `visible` can deliver it (#1436: what a primary
    /// tells its replicas they can have, cli692's F5 on #1583). An atomic read. Not part of
    /// the stable API.
    #[doc(hidden)]
    pub fn replication_frontier(&self) -> u64 {
        self.engine().feed_frontier(true)
    }

    /// The digest of this database's catalog (#1853 part C2): what its primary's feed of every
    /// table reports (`Feed::catalog_digest`) at the position a replica applied through,
    /// unless a schema change never reached the feed. That holds for a replica bootstrapped
    /// from a copy ([`begin_replication`](Self::begin_replication)), whose id counters are
    /// the primary's. One that started empty at its first apply never matches: its counters
    /// miss every id the primary allocated before it (feed1248's N2 on #1878). Not part of
    /// the stable API.
    #[doc(hidden)]
    pub fn replication_catalog_digest(&self) -> u64 {
        self.engine().catalog_digest()
    }

    /// The primary's commit seqno a replica applied through (#1436's lag): `None` on a
    /// database that isn't a replica. Not part of the stable API.
    #[doc(hidden)]
    pub fn replicated_seqno(&self) -> Result<Option<u64>> {
        Ok(self.replication_state()?.map(|s| s.after.seqno))
    }

    fn replication_state(&self) -> Result<Option<State>> {
        state_of(&self.engine())
    }

    /// Runs one apply under the state cache's lock (#2264), on one engine throughout, so
    /// applies run one at a time and each checks and commits on the same file. `apply` is
    /// handed the state the last apply on that engine stored, when this handle knows it
    /// (`None`: read the cell), and returns the state it stored, which the cache then holds.
    /// On an error the cache holds nothing, so the next apply reads the cell: the truth
    /// whether or not the failed commit landed.
    fn applying(&self, apply: impl FnOnce(&Engine, Option<State>) -> Result<State>) -> Result<()> {
        let mut slot = self.db.replica_state.lock();
        let engine = self.db.engine_arc();
        let known = slot.take().and_then(|k| k.on(&engine));
        let state = apply(&engine, known)?;
        *slot = Some(Known {
            engine: Arc::downgrade(&engine),
            state,
        });
        Ok(())
    }

    /// Where an apply starting at `before` stands: the stored state (`known`, or read from
    /// the cell), which must end there (`run[0].before == stored.after`: anything else is a
    /// gap or an overlap). The first apply, with none stored, needs a database with no
    /// tables of its own, and records `before` as where it starts.
    fn chain(
        engine: &Engine,
        known: Option<State>,
        before: StreamPosition,
        primary: [u8; 16],
    ) -> Result<()> {
        let stored = match known {
            Some(s) => Some(s),
            None => state_of(engine)?,
        };
        match stored {
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
                if !engine.tables().is_empty() {
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
                engine.begin_replication(&start.encode(), false)?;
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
        self.applying(|engine, known| {
            Self::chain(engine, known, first.before, primary_of(&first.token))?;
            Self::apply_run(engine, run)
        })
    }

    /// [`apply_replicated`](Self::apply_replicated)'s commits, once the run chains: the
    /// state its last part stored.
    fn apply_run(engine: &Engine, run: &[ReplicatedEvent]) -> Result<State> {
        let mut batch = WriteBatch::new();
        let mut cells: HashSet<Cell<'_>> = HashSet::new();
        let mut max_ts = 0;
        let mut part_end: Option<&ReplicatedEvent> = None;
        let cap = engine.max_replicated_part_bytes();
        let mut part_bytes = 0;
        for event in run {
            // A range drop in the run is refused here, at no cost to a run of commits (#2089's
            // callgrind). A part committed before it stays, with its own token: the replica
            // has applied the run up to there, and resumes from it.
            let Body::Commit { changes, .. } = &event.body else {
                return Err(invalid(
                    "a range drop in a replicated run: apply it with apply_range_drop".to_owned(),
                ));
            };
            let bytes: usize = changes
                .iter()
                .map(|c| c.row.len() + c.qualifier.len() + c.stored.len() + 32)
                .sum();
            // A cell and timestamp this part already writes, or a part this event would take
            // past the byte cap (simp724's S1), or a feed-silent event after an ordinary one
            // or the other way round (D331: one commit is silent or not): commit the part
            // first. One event is never split: a primary commit applies whole.
            if let Some(end) = part_end
                && (part_bytes + bytes > cap
                    || end.is_silent() != event.is_silent()
                    // One spanning commit's share per part, as its xid tags the whole batch.
                    || end.xid() != event.xid()
                    || changes.iter().any(|c| {
                        cells.contains(&(c.table, c.family, &c.row[..], &c.qualifier[..], c.ts))
                    }))
            {
                commit_part(engine, std::mem::take(&mut batch), end, max_ts)?;
                cells.clear();
                max_ts = 0;
                part_bytes = 0;
            }
            part_bytes += bytes;
            // A primary's silent commit stays silent here: the replica's own feed skips it as
            // the primary's does, and a promoted replica's feed is the primary's.
            if event.is_silent() {
                batch.mark_feed_silent();
            }
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
            engine.store_replication_state(&state.encode())?;
            return Ok(state);
        }
        commit_part(engine, batch, end, max_ts)
    }

    /// Raises the timestamp floor to at least `ts`, durably (D331, ICR 0141): a range move's
    /// target, before it serves the range, so a write it timestamps later sorts after every
    /// version it copied, whatever the clocks. Refused on a replica. Not part of the stable
    /// API.
    #[doc(hidden)]
    pub fn raise_ts_floor(&self, ts: u64) -> Result<()> {
        self.engine().raise_ts_floor(ts)?;
        Ok(())
    }

    /// Holds the change feed's history from now on (D331, ICR 0141): a feed token from now,
    /// or of a snapshot opened after it (not one opened before), resumes until the hold is
    /// dropped or the database retains more than `cap_bytes` of history, when it lapses. A
    /// range move's catch-up then finds the source's history from its copy's first
    /// snapshot, or learns it can't. `InvalidArgument` without the change feed. Not part of
    /// the stable API.
    #[doc(hidden)]
    pub fn hold_feed_retention(
        &self,
        cap_bytes: u64,
    ) -> Result<pigeonhole_engine::FeedRetentionHold> {
        Ok(self.engine().hold_feed_retention(cap_bytes)?)
    }

    /// A batch of changes written exactly as another database logged them (D331 PR 4, ICR
    /// 0141): a range move's target writes the source's copy and catch-up so, with the
    /// source's kinds, timestamps and stored values, named by table and family (the target's
    /// ids aren't the source's). It holds logged changes only, so it never mixes with
    /// ordinary writes. Refused at commit on a replica. Not part of the stable API.
    #[doc(hidden)]
    pub fn logged_batch(&self) -> LoggedBatch<'_> {
        LoggedBatch {
            db: self,
            batch: WriteBatch::new(),
            error: None,
            xid: None,
        }
    }

    /// Drops rows `[start, end)` of `table` (`end` `None`: to the table's end) outright,
    /// with no delete markers (D331 PR 4, ICR 0141): a range move's source at its cleanup,
    /// its target at an abort. A later write of the range at an older timestamp is visible,
    /// as if the rows had never been there, and a live snapshot still reads them. The caller
    /// keeps the range unserved while it runs, and runs it again after a crash until it
    /// returns. Feed-silent. `ReadOnly` on a replica; `Busy` to retry; `InvalidArgument`
    /// for an end at or before the start. Not part of the stable API.
    #[doc(hidden)]
    pub fn drop_range(&self, table: &crate::Table, start: &[u8], end: Option<&[u8]>) -> Result<()> {
        crate::write::check_table(&self.db, table)?;
        match self.engine().drop_range(table.core.info.id, start, end) {
            // The engine refuses while an externally prepared transaction is in doubt: name
            // them, so an operator can decide one stuck there (simp724).
            Err(pigeonhole_engine::Error::Busy) if !self.engine().in_doubt().is_empty() => {
                let xids: Vec<String> = self
                    .engine()
                    .in_doubt()
                    .iter()
                    .map(|(xid, _)| String::from_utf8_lossy(xid).into_owned())
                    .collect();
                Err(Error::new(
                    ErrorCode::Busy,
                    format!(
                        "a range drop waits for the transactions in doubt here to be decided: {}",
                        xids.join(", ")
                    ),
                ))
            }
            r => Ok(r?),
        }
    }

    /// Applies a range drop a primary's feed delivered (D331): the same rows go from this
    /// replica's own tablets, then `token` (the event's) is stored. It must start where the
    /// last apply ended. The drop and the token are two steps: after a crash between them
    /// the drop runs again, which changes nothing. Not part of the stable API.
    #[doc(hidden)]
    pub fn apply_range_drop(&self, event: &ReplicatedEvent, token: &FeedToken) -> Result<()> {
        let Body::RangeDrop(d) = &event.body else {
            return Err(invalid(
                "apply_range_drop takes a range drop: apply commits with apply_replicated"
                    .to_owned(),
            ));
        };
        if event.token != *token {
            return Err(invalid("the token to store must be the event's".to_owned()));
        }
        self.applying(|engine, known| {
            Self::chain(engine, known, event.before, primary_of(&event.token))?;
            engine.apply_range_drop(d.table, &d.start, d.end.as_deref())?;
            let state = State {
                primary: primary_of(&event.token),
                after: event.after,
                token: Some(event.token.clone()),
            };
            engine.store_replication_state(&state.encode())?;
            Ok(state)
        })
    }

    /// One chunk of rows `[after, end)` of `table` at `snapshot`, for a range move's copy
    /// (D331 PR 4a, ICR 0141): exactly what a compaction would write for them there (every
    /// version and marker a reader at the snapshot could need; a counter or aggregate
    /// family's operands folded into their base), named by family, for
    /// [`LoggedBatch::push`]. It ends at a row boundary once it holds `max_bytes`; its
    /// `next` is the row to ask for next, `None` once the range is done. Reads without
    /// filling the block cache. Not part of the stable API.
    #[doc(hidden)]
    pub fn copy_range(
        &self,
        snapshot: &crate::Snapshot,
        table: &crate::Table,
        after: &[u8],
        end: Option<&[u8]>,
        max_bytes: usize,
    ) -> Result<CopiedChunk> {
        crate::write::check_table(&self.db, table)?;
        let chunk =
            self.engine()
                .copy_range(&snapshot.inner, table.core.info.id, after, end, max_bytes)?;
        let info = table.core.current_info();
        let mut cells = Vec::with_capacity(chunk.cells.len());
        for c in chunk.cells {
            let Some(family) = info.families.iter().find(|f| f.id == c.family) else {
                return Err(Error::new(
                    ErrorCode::FamilyNotFound,
                    format!("family {} of table {:?}", c.family.0, info.name),
                ));
            };
            cells.push(CopiedCell {
                family: family.name.clone(),
                kind: ChangeKind::of(c.kind),
                row: c.row,
                qualifier: c.qualifier,
                ts: c.ts,
                stored: c.stored,
            });
        }
        Ok(CopiedChunk {
            cells,
            next: chunk.next,
        })
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
        self.applying(|engine, known| {
            Self::chain(engine, known, event.before, primary_of(&event.token))?;
            engine.apply_catalog(&change.edits)?;
            let state = State {
                primary: primary_of(&event.token),
                after: event.after,
                token: Some(event.token.clone()),
            };
            engine.store_replication_state(&state.encode())?;
            Ok(state)
        })
    }
}

/// One chunk of a range copy ([`Pigeonhole::copy_range`]). Not part of the stable API.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct CopiedChunk {
    /// Its cells, in row order, each row whole.
    pub cells: Vec<CopiedCell>,
    /// The row to ask for next; `None`: the range is done.
    pub next: Option<Vec<u8>>,
}

impl CopiedChunk {
    /// The chunk as the commits a target writes it in, in order: one commit keeps only the
    /// last mutation of a cell and timestamp (D34), so a cell the copy holds twice at one
    /// timestamp (a value beside a delete kept with it) starts a new commit, and the
    /// entries apply in the source's seqno order, oldest first, as they're listed.
    ///
    /// **Applying a chunk again is safe** (simp724), so a target writes its progress row in
    /// the chunk's last commit only, and after a crash between two of its commits copies the
    /// whole chunk again, at a fresh snapshot:
    /// - an aggregate cell held whole is a put of its folded state, which writes the same
    ///   again;
    /// - a value kept beside its delete comes after the delete, which masks what the first
    ///   attempt wrote before the value is written again;
    /// - a fold that failed comes as its base (a put) before its operands, which then apply
    ///   to that fresh base, not on top of the first attempt's.
    ///
    /// A standard family's operands under a named operator (D41: they run across
    /// timestamps) are the exception: they apply twice, so their chunk must commit once.
    pub fn batches(&self) -> Vec<&[CopiedCell]> {
        type Key<'a> = (&'a str, &'a [u8], &'a [u8], u64, bool);
        let mut out = Vec::new();
        let mut start = 0;
        let mut seen: std::collections::HashSet<Key<'_>> = std::collections::HashSet::new();
        for (i, c) in self.cells.iter().enumerate() {
            // A family marker is keyed apart from the family's cells (D34).
            let key = (
                c.family.as_str(),
                &c.row[..],
                &c.qualifier[..],
                c.ts,
                c.kind == ChangeKind::DeleteFamily,
            );
            if !seen.insert(key) {
                out.push(&self.cells[start..i]);
                start = i;
                seen.clear();
                seen.insert(key);
            }
        }
        if start < self.cells.len() {
            out.push(&self.cells[start..]);
        }
        out
    }
}

/// One cell of a range copy, as its source logged it, for [`LoggedBatch::push`]. Not part
/// of the stable API.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct CopiedCell {
    /// The family's name.
    pub family: String,
    /// A put (a counter or aggregate family's folded base too), a merge, or a delete.
    pub kind: ChangeKind,
    /// The row.
    pub row: Vec<u8>,
    /// The qualifier; empty for a family delete.
    pub qualifier: Vec<u8>,
    /// The timestamp.
    pub ts: u64,
    /// The stored value, tag byte first; empty for a delete.
    pub stored: Vec<u8>,
}

/// [`Pigeonhole::logged_batch`]'s batch. Errors (a table of another database, a family
/// the table lacks, a key too large) surface at [`commit`](Self::commit), and the first
/// one wins. Not part of the stable API.
#[doc(hidden)]
#[derive(Debug)]
pub struct LoggedBatch<'a> {
    db: &'a Pigeonhole,
    batch: WriteBatch,
    error: Option<Error>,
    /// The commit's tag (D336), if [`tag_xid`](Self::tag_xid) set one.
    xid: Option<Vec<u8>>,
}

impl LoggedBatch<'_> {
    /// Adds one change as its source logged it: its kind, timestamp and stored value (the
    /// tag byte first, as [`ReplicatedEvent`]s and raw reads carry it; empty for a delete).
    /// A family `table` lacks is an error (`FamilyNotFound`), never a dropped change.
    #[allow(clippy::too_many_arguments)]
    pub fn push(
        &mut self,
        table: &crate::Table,
        row: &[u8],
        family: &str,
        kind: ChangeKind,
        qualifier: &[u8],
        ts: u64,
        stored: &[u8],
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let r = crate::write::check_table(&self.db.db, table)
            .and_then(|()| table.core.family_id(family))
            .and_then(|family| {
                Ok(self.batch.push_logged(
                    table.core.info.id,
                    family,
                    logged_kind(kind),
                    row,
                    qualifier,
                    ts,
                    stored,
                )?)
            });
        if let Err(e) = r {
            self.error = Some(e);
        }
        self
    }

    /// Marks the batch feed-silent: committed, durable and replicated, but never delivered
    /// to a change-feed subscription.
    pub fn mark_feed_silent(&mut self) -> &mut Self {
        self.batch.mark_feed_silent();
        self
    }

    /// Tags the commit with `xid` (D336, ICR 0147): an old owner's tail commit replayed into
    /// a partition's new owner carries its origin, which its change-feed events deliver
    /// ([`feed::Commit::xid`](crate::feed::Commit::xid)). The tag is a spanning commit's
    /// (D329 2b, [`Transaction::tag_xid`](crate::Transaction::tag_xid)): 1–255 bytes, checked
    /// at commit. Hidden: not part of the API, outside the semver promise.
    #[cfg(feature = "__unstable-tail-replay")]
    #[doc(hidden)]
    pub fn tag_xid(&mut self, xid: &[u8]) -> &mut Self {
        self.xid = Some(xid.to_vec());
        self
    }

    /// The changes added so far.
    pub fn len(&self) -> usize {
        self.batch.len()
    }

    /// No change added yet.
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Commits the batch atomically. `ReadOnly` on a replica; `InvalidArgument` when empty.
    pub fn commit(self) -> Result<crate::CommitInfo> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let info = self
            .db
            .engine()
            .commit_logged_with(self.batch, None, self.xid.as_deref())?;
        Ok(crate::CommitInfo::from_engine(info))
    }
}

/// One tablet as [`Pigeonhole::tablets`] reports it (D332, ICR 0142).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TabletLoad {
    /// Its table's name.
    pub table: String,
    /// Inclusive start row; empty is unbounded.
    pub start: Vec<u8>,
    /// Exclusive end row; `None` is unbounded.
    pub end: Option<Vec<u8>>,
    /// Bytes of the SSTs it references, a shared SST's divided among its tablets.
    pub bytes: u64,
    /// Rows written to it per balancer interval, a moving average.
    pub write_load: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell as Counter;
    use std::path::Path;
    use std::time::Duration;

    use crate::{Family, FeedRetention, Options};

    thread_local! {
        /// State cell reads on this thread ([`state_of`]).
        pub(super) static CELL_READS: Counter<u64> = const { Counter::new(0) };
    }

    fn reads() -> u64 {
        CELL_READS.with(Counter::get)
    }

    fn options(vfs: &std::sync::Arc<pigeonhole_io::sim::SimVfs>) -> Options {
        Options::default()
            .vfs(std::sync::Arc::clone(vfs) as _)
            .shards(2)
            .pin_threads(false)
            .change_feed(FeedRetention::bytes(16 << 20))
    }

    /// The events `feed` has ready now.
    fn ready(feed: &mut crate::Feed) -> Vec<ReplicatedEvent> {
        let mut out = Vec::new();
        while feed
            .next_event(Duration::from_millis(200))
            .unwrap()
            .is_some()
        {
            out.push(feed.replicated().expect("an event was delivered"));
        }
        out
    }

    /// Applies each event alone, through the call for its kind.
    fn apply(replica: &Pigeonhole, events: &[ReplicatedEvent]) -> Result<()> {
        for e in events {
            if e.is_catalog() {
                replica.apply_catalog(e, e.token())?;
            } else {
                replica.apply_replicated(std::slice::from_ref(e), e.token())?;
            }
        }
        Ok(())
    }

    fn put(primary: &Pigeonhole, rows: std::ops::Range<u32>) {
        let t = primary
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        for i in rows {
            t.mutate(format!("r{i}").as_bytes())
                .put("f", b"q", b"v")
                .commit()
                .unwrap();
        }
    }

    /// #2264: applies chain on the state the last one stored without reading the cell, and
    /// every case that may leave that state stale reads the cell again: a failed apply, an
    /// engine swapped in by a recovery, a re-bootstrap's `replace_with` (a file at another
    /// position, where a stale state would refuse the right run and take the wrong one), and
    /// a promote.
    #[test]
    fn applies_chain_on_the_last_stored_state_and_every_reset_reads_the_cell() {
        let vfs = pigeonhole_io::sim::SimVfs::new(2264);
        let primary = Pigeonhole::open("/p/data.phdb", options(&vfs)).unwrap();
        let mut feed = primary.feed().open().unwrap();
        let replica = Pigeonhole::open("/r/data.phdb", options(&vfs)).unwrap();
        put(&primary, 0..2);
        apply(&replica, &ready(&mut feed)).unwrap();

        // Steady state: no reads, through this handle or a clone (one cache per database).
        put(&primary, 2..8);
        let events = ready(&mut feed);
        assert_eq!(events.len(), 6);
        let before = reads();
        apply(&replica, &events[..3]).unwrap();
        apply(&replica.clone(), &events[3..]).unwrap();
        assert_eq!(
            reads(),
            before,
            "an apply chains on what the last one stored"
        );

        // A failed apply (an overlap) empties the cache: the next one reads the cell.
        put(&primary, 8..9);
        let next = ready(&mut feed);
        let e = apply(&replica, &events[5..]).unwrap_err();
        assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
        let before = reads();
        apply(&replica, &next).unwrap();
        assert_eq!(reads(), before + 1, "after a failure, the cell is read");
        let before = reads();
        put(&primary, 9..10);
        apply(&replica, &ready(&mut feed)).unwrap();
        assert_eq!(reads(), before, "and the cache holds again");

        // A recovery's swap: the new engine's first apply reads its own cell.
        let old = replica.db.engine_arc();
        let (closed, handover) = old.close_for_reopen();
        closed.unwrap();
        let reopened = pigeonhole_engine::Engine::open_with(
            Path::new("/r/data.phdb"),
            options(&vfs).to_engine().unwrap(),
            handover.unwrap(),
        )
        .map_err(|(e, _)| e)
        .unwrap();
        drop((replica.db.swap_engine(reopened), old));
        put(&primary, 10..11);
        let before = reads();
        apply(&replica, &ready(&mut feed)).unwrap();
        assert_eq!(reads(), before + 1, "a swapped-in engine's cell is read");

        // A re-bootstrap: a copy of the primary at a later snapshot, swapped in. The stale
        // state would refuse the copy's next event as a gap, and chain the old feed's.
        put(&primary, 11..14);
        let stale = ready(&mut feed);
        let snap = primary.durable_snapshot().unwrap();
        primary.backup_at(&snap, "/r/side.phdb").unwrap();
        let mut fresh = primary.feed().from_snapshot(&snap).open().unwrap();
        let start = fresh.token();
        drop(snap);
        let side = Pigeonhole::open("/r/side.phdb", options(&vfs)).unwrap();
        side.begin_replication(&start).unwrap();
        side.close().unwrap();
        replica.replace_with("/r/side.phdb").unwrap();
        let e = apply(&replica, &stale[..1]).unwrap_err();
        assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
        put(&primary, 14..16);
        let after = ready(&mut fresh);
        assert!(!after.is_empty());
        apply(&replica, &after).unwrap();
        assert_eq!(replica.replicated_token().unwrap(), Some(fresh.token()));

        // A promote: the next apply finds no state, and is refused.
        put(&primary, 16..17);
        let later = ready(&mut fresh);
        replica.promote().unwrap();
        let before = reads();
        let e = apply(&replica, &later).unwrap_err();
        assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
        assert_eq!(reads(), before + 1, "after a promote, the cell is read");
        drop((feed, fresh));
        replica.close().unwrap();
        primary.close().unwrap();
    }

    #[test]
    fn a_silent_spanning_share_round_trips_as_version_3() {
        // D329 2b: version 3's flags carry both marks; no producer makes such a commit today
        // (a range move's copy is silent, a spanning commit's share is tagged), but the
        // encoding keeps them apart and a replica would apply it silent and tagged.
        let xid: std::sync::Arc<[u8]> = std::sync::Arc::from(&b"p1:h2-aa"[..]);
        let e = ReplicatedEvent {
            before: StreamPosition {
                seqno: 1,
                catalog: 0,
            },
            after: StreamPosition {
                seqno: 2,
                catalog: 0,
            },
            token: FeedToken::from_bytes(b"t"),
            body: Body::Commit {
                seqno: 2,
                ts: 5,
                changes: Vec::new(),
                silent: true,
                xid: Some(xid),
            },
        };
        let bytes = e.encode();
        assert_eq!(
            (bytes[0], bytes[1]),
            (FLAGS_EVENT_VERSION, FLAG_SILENT | FLAG_XID)
        );
        let back = ReplicatedEvent::decode(&bytes).unwrap();
        assert!(back.is_silent());
        assert_eq!(back.xid(), Some(&b"p1:h2-aa"[..]));
        assert_eq!(back.encode(), bytes, "it round-trips");
    }
}
