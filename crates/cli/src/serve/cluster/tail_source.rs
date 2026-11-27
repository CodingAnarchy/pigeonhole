//! D336 PR 3a (#2148, §4, Amendment 2's M2): an old owner's side of a tail replay,
//! `Cluster.ReadTail`.
//!
//! After a failover, the old owner's file of P may hold commits past the fork (the tail) that
//! no replica applied. P's new owner reads them here and replays them (`tail_replay.rs`). The
//! old node opens the file only when asked, as a **tail source**: no lease, no gate, no client
//! call, and never because of the map.
//!
//! - **Who's served:** only for a PENDING tail of P in this node's map that names this node as
//!   the old owner, with the request's `from_history_epoch` and `from_epoch`; only once that
//!   map places P elsewhere at an epoch above `from_epoch`; and only while P isn't open here
//!   in any other role (cli692's M2). Otherwise `FailedPrecondition`, which the caller
//!   retries.
//! - **The file:** P's own `DIR/P.phdb`, at identity epoch `from_epoch`, never a replica
//!   copy's (one still following an owner has replication state, which an old owner's file
//!   doesn't).
//! - **The open:** writable, since only that recovers a crashed owner's WAL, with the least
//!   memory a shard takes and no caches: it reads its own feed once, and writes nothing past
//!   recovery. Its first step after the engine's open is the feed retention hold (the
//!   coordinator's ruling on PR 1: nothing in between prunes), then the feed from the token.
//!   A token retention already passed answers `FailedPrecondition` naming the tail lost: a
//!   part of a tail would leave a hole in each key's order.
//! - **Its life:** one stream at a time. It stays open for [`TAIL_SOURCE_IDLE`] after its last
//!   stream ends (a resumed or repeated read finds it recovered), then closes. A stream whose
//!   reader takes nothing for that long ends too.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::{Node, identity, stamp};
use crate::serve::errors::ApiError;
use crate::serve::proto::{self, read_tail_event::Event};

/// How long a tail source stays open after its last stream ends, and how long a stream waits
/// for a reader that takes nothing (the coordinator, 2026-11-27).
pub(crate) const TAIL_SOURCE_IDLE: Duration = Duration::from_secs(60);

/// How long the source waits on its feed for the next commit before it calls the tail ended:
/// nothing writes the file, so once recovery is read, an empty wait is the end.
const QUIET: Duration = Duration::from_millis(200);

/// The tail sources open on this node, by partition.
#[derive(Debug, Default)]
pub(crate) struct TailSources {
    open: Mutex<BTreeMap<String, Slot>>,
    /// Each kept file's last durable seqno, by (partition, history epoch, epoch), once a
    /// source open recovered it: what `Status` reports as `tail_kept` (Amendment 2's M3).
    ends: Mutex<BTreeMap<(String, u64, u64), u64>>,
}

/// A partition's tail source.
#[derive(Debug)]
enum Slot {
    /// Being opened, or streaming: a second reader is refused until it's back.
    Busy { history_epoch: u64, epoch: u64 },
    /// Open and idle since `since`.
    Idle { source: Source, since: Instant },
}

/// An open tail source: the old file and the retention hold taken first.
#[derive(Debug)]
struct Source {
    history_epoch: u64,
    epoch: u64,
    db: pigeonhole::Pigeonhole,
    /// Dropped with the database, never before.
    _hold: pigeonhole::replication::FeedRetentionHold,
}

impl TailSources {
    fn open(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Slot>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether partition `id` is open here as a tail source, or being opened as one: the
    /// node's other opens, reconcile and `drop-partition` leave its file alone then.
    pub(crate) fn holds(&self, id: &str) -> bool {
        self.open().contains_key(id)
    }

    /// The tail source of `id` as `Status` shows it: (history epoch, epoch, its file's bytes
    /// once open).
    pub(crate) fn shown(&self, id: &str) -> Option<(u64, u64, Option<u64>)> {
        match self.open().get(id)? {
            Slot::Busy {
                history_epoch,
                epoch,
            } => Some((*history_epoch, *epoch, None)),
            Slot::Idle { source, .. } => Some((
                source.history_epoch,
                source.epoch,
                Some(super::status::used_bytes(&source.db)),
            )),
        }
    }

    /// A kept file's last durable seqno, once a source open recovered it.
    pub(crate) fn end(&self, id: &str, history_epoch: u64, epoch: u64) -> Option<u64> {
        self.ends
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(id.to_owned(), history_epoch, epoch))
            .copied()
    }

    fn set_end(&self, id: &str, history_epoch: u64, epoch: u64, seqno: u64) {
        self.ends
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((id.to_owned(), history_epoch, epoch), seqno);
    }

    /// Takes `id`'s idle source for (`history_epoch`, `epoch`), or marks it being opened:
    /// `Ok(None)` for the caller to open it. Another stream's, or a source of another file,
    /// is refused.
    fn take(&self, id: &str, history_epoch: u64, epoch: u64) -> Result<Option<Source>, ApiError> {
        let mut open = self.open();
        match open.remove(id) {
            None => {}
            Some(Slot::Idle { source, .. })
                if source.history_epoch == history_epoch && source.epoch == epoch =>
            {
                open.insert(
                    id.to_owned(),
                    Slot::Busy {
                        history_epoch,
                        epoch,
                    },
                );
                return Ok(Some(source));
            }
            Some(Slot::Idle { source, .. }) => {
                // Another file's source, idle: an old owner holds one file of P, so this
                // can only be a stale request. It closes; the caller's check below decides.
                let _ = source.db.close();
            }
            Some(busy @ Slot::Busy { .. }) => {
                open.insert(id.to_owned(), busy);
                return Err(ApiError::unavailable(format!(
                    "partition {id}'s old file is being read as a tail source already: one \
                     replay at a time; retry once it ends"
                )));
            }
        }
        open.insert(
            id.to_owned(),
            Slot::Busy {
                history_epoch,
                epoch,
            },
        );
        Ok(None)
    }

    /// Puts `source` back, idle from now, and arms its close after [`TAIL_SOURCE_IDLE`].
    fn give_back(self: &Arc<Self>, id: &str, source: Source) {
        let since = Instant::now();
        self.open()
            .insert(id.to_owned(), Slot::Idle { source, since });
        let (me, id) = (Arc::clone(self), id.to_owned());
        std::thread::Builder::new()
            .name("phdb-tail-source-idle".into())
            .spawn(move || {
                std::thread::sleep(TAIL_SOURCE_IDLE);
                me.close_idle(&id, since);
            })
            .map_or_else(
                |e| eprintln!("phdb serve: the tail source's idle timer didn't start: {e}"),
                drop,
            );
    }

    /// Closes `id`'s source if it's still idle since `since`.
    fn close_idle(&self, id: &str, since: Instant) {
        let mut open = self.open();
        if matches!(open.get(id), Some(Slot::Idle { since: s, .. }) if *s == since)
            && let Some(Slot::Idle { source, .. }) = open.remove(id)
        {
            drop(open);
            if let Err(e) = source.db.close() {
                eprintln!("phdb serve: partition {id}'s tail source didn't close cleanly: {e}");
            }
        }
    }

    /// Forgets `id`'s busy mark after an open that failed.
    fn release(&self, id: &str) {
        let mut open = self.open();
        if matches!(open.get(id), Some(Slot::Busy { .. })) {
            open.remove(id);
        }
    }
}

/// `Cluster.ReadTail`'s stream: its thread's messages, then the end or an error. The thread
/// holds a `--max-streams` place until it ends.
#[derive(Debug)]
pub(crate) struct TailStream(mpsc::Receiver<Result<proto::ReadTailEvent, ApiError>>);

impl futures_core::Stream for TailStream {
    type Item = Result<proto::ReadTailEvent, ApiError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

impl Node {
    /// `Cluster.ReadTail` (D336 §4): this node's old file of the partition, from the token,
    /// as a stream of its commits and then its end.
    pub(crate) async fn read_tail(
        self: &Arc<Self>,
        r: proto::ReadTailRequest,
        slot: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<TailStream, ApiError> {
        let id = r.partition.clone();
        self.may_read_tail(&r)?;
        if r.token.is_empty() {
            return Err(ApiError::invalid(
                "ReadTail needs the tail's token: the failover event's fork token, or the \
                 replay mark's",
            ));
        }
        let sources = Arc::clone(&self.tail_sources);
        let held = sources.take(&id, r.from_history_epoch, r.from_epoch)?;
        let source = match held {
            Some(s) => s,
            None => {
                let node = Arc::clone(self);
                let r2 = r.clone();
                let opened = tokio::task::spawn_blocking(move || node.open_tail_source(&r2))
                    .await
                    .map_err(|e| ApiError::internal(format!("the tail source's open: {e}")))
                    .and_then(|o| o);
                match opened {
                    Ok(s) => s,
                    Err(e) => {
                        sources.release(&id);
                        return Err(e);
                    }
                }
            }
        };
        // Checked again with the source in hand: the map may have moved on while it opened.
        if let Err(e) = self.may_read_tail(&r) {
            sources.give_back(&id, source);
            return Err(e);
        }
        let feed = match source
            .db
            .feed()
            .resume(&pigeonhole::FeedToken::from_bytes(r.token.to_vec()))
            .open()
        {
            Ok(f) => f,
            Err(e) => {
                sources.give_back(&id, source);
                return Err(match e.code() {
                    pigeonhole::ErrorCode::FeedExpired => ApiError::failed_precondition(format!(
                        "partition {id}'s tail is lost: its old file's change feed no longer \
                         holds the history from the requested token (retention pruned its \
                         start), so no part of it is replayed. Discard the tail \
                         (phdb cluster replay-tail {id} --discard)"
                    )),
                    _ => ApiError::from_engine(&e).for_read(),
                });
            }
        };
        let (tx, rx) = mpsc::channel(4);
        let (after, token) = (r.after_seqno, r.token.to_vec());
        std::thread::Builder::new()
            .name("phdb-tail-source".into())
            .spawn(move || {
                let mut feed = feed;
                // Every commit: a range move's feed-silent copy and cleanup too, which the
                // replay skips by name (§7's S2).
                feed.include_silent();
                let outcome = pump(&source.db, feed, &tx, after, &token);
                if let Err(e) = outcome {
                    let _ = tx.try_send(Err(e));
                }
                sources.give_back(&id, source);
                drop(slot);
            })
            .map_err(|e| ApiError::internal(format!("starting the tail source's thread: {e}")))?;
        Ok(TailStream(rx))
    }

    /// Whether this node may serve `r` now (Amendment 2's M2): its map holds a PENDING tail of
    /// the partition naming this node and the file, places the partition elsewhere above the
    /// file's epoch, and the partition isn't open here in another role.
    fn may_read_tail(&self, r: &proto::ReadTailRequest) -> Result<(), ApiError> {
        use proto::{FailoverKind as K, TailState as T};
        let id = &r.partition;
        let map = self.get_map()?;
        if !map.failovers.iter().any(|e| {
            e.kind() == K::Partition
                && e.partition == *id
                && e.from == self.id
                && e.tail_state() == T::Pending
                && e.from_history_epoch == r.from_history_epoch
                && e.from_epoch == r.from_epoch
        }) {
            return Err(ApiError::failed_precondition(format!(
                "this node's map (version {}) has no PENDING tail of partition {id:?} naming \
                 this node's file at history epoch {} and epoch {}",
                map.map_version, r.from_history_epoch, r.from_epoch
            )));
        }
        match map.partitions.iter().find(|p| p.id == *id) {
            Some(p) if p.node != self.id && p.epoch > r.from_epoch => {}
            _ => {
                return Err(ApiError::failed_precondition(format!(
                    "node {:?} hasn't taken a map that places partition {id:?} elsewhere above \
                     epoch {} yet (it has version {}): retry",
                    self.id, r.from_epoch, map.map_version
                )));
            }
        }
        let open_here = self.fences.is_opening(id)
            || self.fences.gate(id).is_some()
            || self.server.get().and_then(|o| o.shared.upgrade()).is_some_and(|s| {
                s.router.cluster.as_ref().is_some_and(|c| {
                    c.parts.load().contains_key(id) || c.replicas.load().contains_key(id)
                })
            });
        if open_here {
            return Err(ApiError::failed_precondition(format!(
                "partition {id:?} is still open on node {:?} in another role (a fenced owner, \
                 or a copy): it serves as a tail source once that closes; retry",
                self.id
            )));
        }
        Ok(())
    }

    /// Opens this node's old file of `r.partition` as a tail source: the engine's open (with
    /// its WAL recovery), the retention hold, then the checks that it's the file the tail
    /// names.
    fn open_tail_source(&self, r: &proto::ReadTailRequest) -> Result<Source, ApiError> {
        let id = &r.partition;
        let server = self
            .server
            .get()
            .ok_or_else(|| ApiError::unsupported("this node opens no file while it serves"))?;
        let cap = self
            .range_copies()
            .map(|c| c.limits().1)
            .ok_or_else(|| ApiError::partition_unavailable("the server is stopping"))?;
        let cluster_id = self.get_map()?.cluster_id;
        if stamp::marker(&self.dir, id).exists() || stamp::staged_any(&self.dir, id) {
            return Err(ApiError::failed_precondition(format!(
                "partition {id}'s file here has an install in progress: it's no old owner's \
                 file to read a tail from"
            )));
        }
        let path = self.dir.join(format!("{id}.phdb"));
        if !path.exists() {
            return Err(ApiError::not_found(format!(
                "node {:?} has no {id}.phdb: the old owner's file is gone, so its tail can't be \
                 replayed. Discard it (phdb cluster replay-tail {id} --discard)",
                self.id
            )));
        }
        let source = open_source(&path, &server.served, cap, r.from_history_epoch)?;
        let refuse = |source: Source, why: String| {
            let _ = source.db.close();
            Err(ApiError::failed_precondition(format!(
                "{} isn't the old owner's file of partition {id:?} the tail names (epoch {}): \
                 {why}",
                path.display(),
                r.from_epoch
            )))
        };
        match identity::read(&source.db) {
            Ok(Some(i))
                if i.cluster_id == cluster_id
                    && i.partition_id == *id
                    && i.epoch == r.from_epoch => {}
            Ok(Some(i)) => {
                return refuse(
                    source,
                    format!(
                        "it says partition {:?} of cluster {:?} at epoch {}",
                        i.partition_id, i.cluster_id, i.epoch
                    ),
                );
            }
            Ok(None) => return refuse(source, "it has no identity row".into()),
            Err(e) => return refuse(source, e),
        }
        // A replica copy's file (still following an owner) holds another history's commits
        // in its own numbering: never a tail (Amendment 2's M2).
        match source.db.replicated_token() {
            Ok(None) => {}
            Ok(Some(_)) => return refuse(source, "it's a replica copy".into()),
            Err(e) => return refuse(source, format!("its replication state: {e}")),
        }
        self.tail_sources.set_end(
            id,
            r.from_history_epoch,
            r.from_epoch,
            source.db.replication_frontier(),
        );
        Ok(source)
    }
}

/// The engine's open of an old file at `path` (of history `history_epoch`) with `served`'s
/// options, the least memory one shard takes and no caches, then the feed retention hold
/// (`cap` bytes, as a range move's source takes): the hold before anything reads the feed.
fn open_source(
    path: &Path,
    served: &pigeonhole::Options,
    cap: u64,
    history_epoch: u64,
) -> Result<Source, ApiError> {
    use crate::serve::placement::{MEMTABLE_FLOOR, Memory, Share};
    let share = Share {
        shards: 1,
        compaction: 0,
        confined: false,
    };
    // A file whose feed recorded a shard count keeps it (ICR 0132), whatever `share` says.
    let options = super::open::options_for(served, share, None, None, false);
    let options = Memory::with_caches(1, MEMTABLE_FLOOR, 0).apply(options);
    let db = super::open_retrying(path, &options)
        .map_err(|e| ApiError::from_engine(&e).for_read())?;
    let hold = match db.hold_feed_retention(cap) {
        Ok(h) => h,
        Err(e) => {
            let _ = db.close();
            return Err(match e.code() {
                pigeonhole::ErrorCode::InvalidArgument => ApiError::failed_precondition(format!(
                    "{} has no change feed, so it has no tail to read",
                    path.display()
                )),
                _ => ApiError::from_engine(&e).for_read(),
            });
        }
    };
    let epoch = match identity::read(&db) {
        Ok(Some(i)) => i.epoch,
        _ => 0,
    };
    Ok(Source {
        history_epoch,
        epoch,
        db,
        _hold: hold,
    })
}

/// Each table's last drop in the tail from the stream's start, as the seqno of its catalog
/// change: a commit below it wrote an incarnation the old owner itself dropped (S5).
type Drops = std::collections::HashMap<String, u64>;

/// Reads the tail once from `token`, for [`Drops`]. A tail is the few commits no replica
/// took, so reading it twice costs little, and it's what tells a write to a table dropped
/// and made again inside the tail from one to the table made again.
fn drops(db: &pigeonhole::Pigeonhole, token: &[u8]) -> Result<Drops, ApiError> {
    let engine = |e: pigeonhole::Error| ApiError::from_engine(&e).for_read();
    let mut feed = db
        .feed()
        .resume(&pigeonhole::FeedToken::from_bytes(token.to_vec()))
        .open()
        .map_err(engine)?;
    feed.include_silent();
    let mut out = Drops::new();
    while let Some(event) = feed.next_event(QUIET).map_err(engine)? {
        if let pigeonhole::FeedEvent::Catalog(c) = event {
            for edit in c.edits() {
                if let pigeonhole::CatalogEdit::TableDropped { table, .. } = edit {
                    out.insert(table.clone(), c.seqno());
                }
            }
        }
    }
    Ok(out)
}

/// The source's loop: each event past `after` in order, by name, then the end. Ends early,
/// `Ok`, when the reader goes away.
fn pump(
    db: &pigeonhole::Pigeonhole,
    mut feed: pigeonhole::Feed,
    tx: &mpsc::Sender<Result<proto::ReadTailEvent, ApiError>>,
    after: u64,
    token: &[u8],
) -> Result<(), ApiError> {
    use pigeonhole::FeedEvent as F;
    let engine = |e: pigeonhole::Error| ApiError::from_engine(&e).for_read();
    let dropped = drops(db, token)?;
    // The old file's schema records as they end: what a table that the tail doesn't drop
    // was made as. A table with none (made by hand) is no table the map places: 0.
    let records = super::schema::records(db).map_err(ApiError::internal)?;
    loop {
        if tx.is_closed() {
            return Ok(());
        }
        let Some(event) = feed.next_event(QUIET).map_err(engine)? else {
            let end = proto::ReadTailEvent {
                event: Some(Event::End(proto::TailEnd {
                    last_seqno: db.replication_frontier(),
                })),
            };
            send_within(tx, end)?;
            return Ok(());
        };
        let mut commit = proto::TailCommit::default();
        match event {
            F::Commit(c) => {
                commit.seqno = c.seqno();
                commit.timestamp = c.timestamp();
                commit.tag = c.xid().map(|x| x.to_vec()).unwrap_or_default().into();
                for ch in c.changes() {
                    let t = ch.table();
                    if !commit.created.contains_key(t) {
                        let created = match dropped.get(t) {
                            Some(&d) if c.seqno() < d => 0,
                            _ => records.get(t).copied().unwrap_or(0),
                        };
                        commit.created.insert(t.to_owned(), created);
                    }
                    commit.changes.push(proto::RawChange {
                        table: t.to_owned(),
                        family: ch.family().to_owned(),
                        kind: super::range_copy::logged_kind(ch.kind()),
                        row: ch.row().to_vec().into(),
                        qualifier: ch.qualifier().to_vec().into(),
                        timestamp: ch.timestamp(),
                        stored: ch.stored().to_vec().into(),
                    });
                }
            }
            F::Catalog(c) => {
                commit.seqno = c.seqno();
                commit.set_skip(proto::TailSkip::Catalog);
            }
            F::RangeDrop(d) => {
                commit.seqno = d.seqno();
                commit.set_skip(proto::TailSkip::RangeDrop);
            }
            _ => {
                return Err(ApiError::internal(
                    "the old file's feed delivered an event this build doesn't know",
                ));
            }
        }
        commit.silent = feed.delivered_silent();
        if commit.seqno <= after {
            continue;
        }
        commit.token = feed.token().as_bytes().to_vec().into();
        let msg = proto::ReadTailEvent {
            event: Some(Event::Commit(commit)),
        };
        if !send_within(tx, msg)? {
            return Ok(());
        }
    }
}

/// How long the source waits between tries while its reader's buffer is full.
const SEND_RETRY: Duration = Duration::from_millis(5);

/// Sends `msg`, waiting at most [`TAIL_SOURCE_IDLE`] for room: `false` if the reader went
/// away. On this thread alone, as the follower's send is (no runtime timer).
fn send_within(
    tx: &mpsc::Sender<Result<proto::ReadTailEvent, ApiError>>,
    msg: proto::ReadTailEvent,
) -> Result<bool, ApiError> {
    let deadline = Instant::now() + TAIL_SOURCE_IDLE;
    let mut item = Ok(msg);
    loop {
        match tx.try_send(item) {
            Ok(()) => return Ok(true),
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
            Err(mpsc::error::TrySendError::Full(back)) => {
                if Instant::now() >= deadline {
                    return Err(ApiError::deadline(
                        "the replay took nothing from its tail source for TAIL_SOURCE_IDLE (60 \
                         s); it resumes from its mark",
                    ));
                }
                item = back;
                std::thread::sleep(SEND_RETRY);
            }
        }
    }
}
