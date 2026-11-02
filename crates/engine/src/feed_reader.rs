//! The change feed's reader in the writer process (D310 PR 3, #1248; rev 3 §2.4–§2.6).
//!
//! A [`FeedReader`] merges every WAL stream's records into whole commits in seqno order, up
//! to the feed's frontier ([`Engine::feed_frontier`](crate::Engine::feed_frontier)). It
//! reads the stream files (through [`FeedCursor`]), never the writer's memory: by the M1
//! invariant (concurrency.md, the written-hold protocol) every record at or below the
//! frontier is in its file, so a poll that loads the frontier and then reads every stream to
//! its end has every commit up to it.
//!
//! - **Keys.** A single is ordered by its seqno, a cross-shard commit by its Decision's apply
//!   seqno; its PREPAREs are held by id until the Decision is read (FORMAT §10.3, D263).
//! - **An aborted round's PREPARE** is dropped once its own stream's checkpoint passed it
//!   with no Decision read (simp724's rule): a checkpoint passes a PREPARE only once it is
//!   aborted, or decided and settled, and then its Decision was in its file before the
//!   checkpoint moved. The checkpoints are loaded before the frontier. The stream's floor
//!   is the backstop.
//! - **The mark rule** (§2.2): a record counts only in a segment whose session fed every
//!   table the feed follows, within sessions that chain with no other root commit between
//!   them (the sidecar, FORMAT §10.4).
//! - **Tokens** fall between seqnos (§2.5): the last delivered seqno, and per stream where to
//!   read again and how far the delivered records reached.
//! - **Expiry** ([`Expiry`]) is sticky.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pigeonhole_format::feed::{FeedEntry, FeedHeader, FeedTables, decode_entries};
use pigeonhole_format::wal::{BatchRef, WalRecord};
use pigeonhole_format::{FamilyId, Kind, Lsn, Seqno, StreamId, TableId, Timestamp};
use pigeonhole_io::OpenOptions;
use pigeonhole_wal::{FeedCursor, Step};

use crate::shard::{Shared, Wake};
use crate::{Error, Result};

/// The token's format version.
const TOKEN_VERSION: u8 = 1;

/// How long a waiting reader sleeps when the frontier is held by the change feed's own
/// holds (a lazy flush or background sync to come) or a stream's next segment header is not
/// on disk yet: the wait is bounded, never a spin.
const HELD_BACKOFF: Duration = Duration::from_millis(1);

/// Where a feed starts.
#[derive(Debug, Clone)]
pub enum FeedStart {
    /// With the commits after the frontier at subscription.
    Now,
    /// With the oldest commit retained history still holds for the feed's tables. Under load
    /// that history may be recycled before the reader reaches it, which expires the feed at
    /// once; [`FeedStart::After`] from a snapshot is the way to bootstrap.
    Oldest,
    /// With the commits after `seqno` (a snapshot's seqno).
    After(Seqno),
    /// Where a token left off ([`FeedReader::token`]).
    Token(Vec<u8>),
}

/// What a feed follows.
#[derive(Debug, Clone)]
pub struct FeedSpec {
    /// The tables (at least one), each fed by the session.
    pub tables: Vec<TableId>,
    /// Only mutations of rows starting with these bytes (empty: every row).
    pub prefix: Vec<u8>,
    /// The durable frontier (the default, §2.4 S3) rather than the visible one.
    pub durable: bool,
    /// Where to start.
    pub start: FeedStart,
}

/// One mutation of a delivered commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// Its table.
    pub table: TableId,
    /// Its family.
    pub family: FamilyId,
    /// Put, Merge, or one of the delete kinds.
    pub kind: Kind,
    /// Its timestamp: explicit, or the commit's.
    pub ts: Timestamp,
    /// The row.
    pub row: Vec<u8>,
    /// The qualifier (empty for a family delete).
    pub qualifier: Vec<u8>,
    /// The stored value (FORMAT §3), with a separated value resolved; empty for deletes.
    pub value: Vec<u8>,
}

/// One whole commit (§2.5): every mutation of it the feed follows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Commit {
    /// The seqno it applied at.
    pub seqno: Seqno,
    /// Its commit timestamp.
    pub ts: Timestamp,
    /// Its mutations in the feed's tables and prefix, in record order (shares in their
    /// participants' order).
    pub changes: Vec<Change>,
}

/// Why a feed ended (§2.6): the feed can't deliver what follows its position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expiry {
    /// What happened, for people.
    pub reason: String,
    /// The oldest seqno history still holds, when known.
    pub oldest_retained: Option<Seqno>,
}

/// What [`FeedReader::try_next`] found.
#[derive(Debug)]
pub enum Next<'a> {
    /// The next whole commit.
    Commit(&'a Commit),
    /// Nothing past the frontier yet.
    Pending,
    /// The feed has expired (and stays so).
    Expired(&'a Expiry),
}

/// A mutation as read, before its commit's timestamp is known.
#[derive(Debug, Clone)]
struct Raw {
    table: TableId,
    family: FamilyId,
    kind: Kind,
    ts: Option<Timestamp>,
    row: Vec<u8>,
    qualifier: Vec<u8>,
    value: Vec<u8>,
}

/// A record as read, owned.
enum Item {
    Single {
        seqno: Seqno,
        ts: Timestamp,
        changes: Vec<Raw>,
    },
    Prepare {
        id: Seqno,
        changes: Vec<Raw>,
    },
    Decision {
        id: Seqno,
        apply: Seqno,
        ts: Timestamp,
        participants: Vec<StreamId>,
    },
    Other,
}

/// A record read but not yet delivered, by key.
#[derive(Debug)]
enum Pending {
    Single {
        ts: Timestamp,
        changes: Vec<Raw>,
        stream: usize,
        start: Lsn,
        end: Lsn,
    },
    Decided {
        id: Seqno,
        ts: Timestamp,
        participants: Vec<StreamId>,
        stream: usize,
        start: Lsn,
        end: Lsn,
    },
}

/// A PREPARE waiting for its Decision.
#[derive(Debug)]
struct Held {
    stream: usize,
    start: Lsn,
    end: Lsn,
    changes: Vec<Raw>,
    /// Read in a segment outside the feed's history (the mark rule): delivering it expires.
    outside: bool,
}

/// One stream's reading state.
#[derive(Debug)]
struct StreamRead {
    cursor: FeedCursor,
    /// Just past the last delivered (or skipped) record of this stream.
    delivered_end: Lsn,
    /// From a token: the delivered records reached here, so the stream must too.
    must_reach: Option<Lsn>,
}

/// The sessions whose segments the feed's history runs through (the mark rule).
#[derive(Debug)]
struct History {
    sessions: Vec<u64>,
}

impl History {
    fn holds(&self, session: u64) -> bool {
        session != 0 && self.sessions.contains(&session)
    }
}

/// Distinguishes feed readers' entries in the watermark registries from commits' and
/// shards' (`watermark_waiters`): the top two bits set.
static NEXT_KEY: AtomicU64 = AtomicU64::new(0);

/// A thread-unparking waker for a blocking wait.
struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// A subscription to the committed changes of some tables (D310): see the module docs.
pub struct FeedReader {
    shared: Arc<Shared>,
    tables: Vec<TableId>,
    prefix: Vec<u8>,
    durable: bool,
    streams: Vec<StreamRead>,
    history: History,
    /// Every commit with a key at or below this was delivered, skipped or precedes the
    /// start.
    delivered: Seqno,
    /// The frontier as of the last poll.
    frontier: Seqno,
    /// The newest session of any record read (the token's anchor).
    session: u64,
    ready: BTreeMap<Seqno, Pending>,
    held: HashMap<Seqno, Vec<Held>>,
    out: Commit,
    expired: Option<Expiry>,
    key: u64,
}

impl std::fmt::Debug for FeedReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedReader")
            .field("tables", &self.tables)
            .field("durable", &self.durable)
            .field("delivered", &self.delivered)
            .field("frontier", &self.frontier)
            .field("expired", &self.expired)
            .finish_non_exhaustive()
    }
}

/// What one poll left.
enum Polled {
    Commit,
    Pending { held: bool },
    Expired,
}

fn expiry(reason: impl Into<String>) -> Expiry {
    Expiry {
        reason: reason.into(),
        oldest_retained: None,
    }
}

impl FeedReader {
    pub(crate) fn open(shared: Arc<Shared>, spec: FeedSpec) -> Result<FeedReader> {
        let Some(feed) = shared.feed.clone() else {
            return Err(Error::InvalidArgument(
                "the change feed is off: open with it on to subscribe".into(),
            ));
        };
        let mut tables = spec.tables;
        tables.sort_unstable();
        tables.dedup();
        if tables.is_empty() {
            return Err(Error::InvalidArgument(
                "a change feed follows at least one table".into(),
            ));
        }
        if let Some(t) = tables.iter().find(|&&t| !feed.tables.contains(t)) {
            return Err(Error::InvalidArgument(format!(
                "table {} is not fed: name it in the feed's tables at open",
                t.0
            )));
        }
        let history = read_history(&shared, feed.session, &tables)?;
        let key = (3 << 62) | NEXT_KEY.fetch_add(1, Ordering::Relaxed);
        let db_id = shared.pager.db_id();
        let mut this = FeedReader {
            tables,
            prefix: spec.prefix,
            durable: spec.durable,
            streams: Vec::with_capacity(shared.shards),
            history,
            delivered: 0,
            frontier: 0,
            session: 0,
            ready: BTreeMap::new(),
            held: HashMap::new(),
            out: Commit::default(),
            expired: None,
            key,
            shared,
        };
        // Where each stream starts, and the bound below which nothing is delivered.
        let starts: Vec<(Lsn, Option<Lsn>)> = match spec.start {
            FeedStart::Token(bytes) => match this.decode_token(&bytes, db_id)? {
                Ok(starts) => starts,
                Err(e) => {
                    this.expired = Some(e);
                    return Ok(this);
                }
            },
            start => {
                let bound = match start {
                    FeedStart::Now => this
                        .shared
                        .shm
                        .visible_seqno()
                        .max(this.shared.shm.written_seqno(this.durable))
                        .max(feed.checkpoint_key()),
                    FeedStart::After(seqno) => seqno,
                    FeedStart::Oldest => {
                        // The largest bound every stream's retained history serves.
                        (0..this.shared.shards)
                            .map(|s| feed.oldest_bound(s, floor(&feed, s)))
                            .max()
                            .unwrap_or(0)
                    }
                    FeedStart::Token(_) => unreachable!("handled above"),
                };
                this.delivered = bound;
                let mut starts = Vec::with_capacity(this.shared.shards);
                for s in 0..this.shared.shards {
                    match feed.start_at(s, bound, floor(&feed, s)) {
                        Some(at) => starts.push((at, None)),
                        None => {
                            this.expired = Some(expiry(format!(
                                "history past seqno {bound} is no longer retained"
                            )));
                            return Ok(this);
                        }
                    }
                }
                starts
            }
        };
        for (s, (at, must_reach)) in starts.into_iter().enumerate() {
            let cursor = FeedCursor::open(
                &this.shared.vfs,
                &this.shared.path,
                StreamId(s as u32),
                db_id,
                at,
            )?;
            this.streams.push(StreamRead {
                cursor,
                delivered_end: at,
                must_reach,
            });
        }
        Ok(this)
    }

    /// The next whole commit, without waiting.
    pub fn try_next(&mut self) -> Result<Next<'_>> {
        let polled = self.poll()?;
        Ok(self.lend(polled))
    }

    /// The next whole commit, waiting up to `timeout` for one. Waits on the watermark the
    /// feed follows (never spins): a commit wakes it once its seqno passes; where the feed's
    /// own holds keep the frontier back, it looks again every millisecond.
    pub fn wait(&mut self, timeout: Duration) -> Result<Next<'_>> {
        let deadline = Instant::now() + timeout;
        loop {
            let polled = self.poll()?;
            let Polled::Pending { held } = polled else {
                return Ok(self.lend(polled));
            };
            let now = Instant::now();
            if now >= deadline {
                return Ok(Next::Pending);
            }
            let mut park = deadline - now;
            if held {
                park = park.min(HELD_BACKOFF);
            } else {
                let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
                let target = self.frontier.saturating_add(1);
                let waiters = if self.durable {
                    &self.shared.durable_waiters
                } else {
                    &self.shared.waiters
                };
                waiters.register(self.key, target, Wake::Task(waker));
                // A publish that missed the registration is seen here.
                if self.shared.shm.written_seqno(self.durable) >= target {
                    continue;
                }
                // The feed's holds may pass the watermark later than the publish that
                // wakes this: bound the sleep by the hold's interval.
                if let Some(feed) = &self.shared.feed {
                    let hold = if self.durable {
                        feed.sync_interval_nanos
                    } else {
                        feed.flush_interval_nanos
                    };
                    park = park.min(Duration::from_nanos(hold));
                }
            }
            std::thread::park_timeout(park);
        }
    }

    fn lend(&self, polled: Polled) -> Next<'_> {
        match polled {
            Polled::Commit => Next::Commit(&self.out),
            Polled::Pending { .. } => Next::Pending,
            Polled::Expired => Next::Expired(self.expired.as_ref().expect("set")),
        }
    }

    /// The frontier as of the last poll.
    pub fn frontier(&self) -> Seqno {
        self.frontier
    }

    /// The last delivered seqno: every commit at or below it was delivered (or precedes the
    /// feed's start).
    pub fn delivered(&self) -> Seqno {
        self.delivered
    }

    /// Where this feed is, to resume it with [`FeedStart::Token`]: just past the last
    /// commit [`FeedReader::try_next`] returned, never inside one (§2.5).
    pub fn token(&self) -> Vec<u8> {
        let mut t = Vec::with_capacity(1 + 16 + 4 + 8 + 8 + 16 * self.streams.len());
        t.push(TOKEN_VERSION);
        t.extend_from_slice(&self.shared.pager.db_id());
        t.extend_from_slice(&(self.streams.len() as u32).to_le_bytes());
        t.extend_from_slice(&self.delivered.to_le_bytes());
        t.extend_from_slice(&self.session.to_le_bytes());
        for (s, st) in self.streams.iter().enumerate() {
            t.extend_from_slice(&self.hint(s).0.to_le_bytes());
            t.extend_from_slice(&st.delivered_end.0.to_le_bytes());
        }
        t
    }

    /// Where stream `s` must be read again from to deliver everything after `delivered`:
    /// the earliest record of it read but not delivered (held or pending), else the
    /// cursor.
    fn hint(&self, s: usize) -> Lsn {
        let mut at = self.streams[s].cursor.position();
        for p in self.ready.values() {
            let (stream, start) = match p {
                Pending::Single { stream, start, .. } | Pending::Decided { stream, start, .. } => {
                    (*stream, *start)
                }
            };
            if stream == s {
                at = at.min(start);
            }
        }
        for h in self.held.values().flatten() {
            if h.stream == s {
                at = at.min(h.start);
            }
        }
        at
    }

    /// Decodes a token into each stream's start and must-reach position, or the reason it
    /// can't resume.
    #[allow(clippy::type_complexity)]
    fn decode_token(
        &mut self,
        bytes: &[u8],
        db_id: [u8; 16],
    ) -> Result<std::result::Result<Vec<(Lsn, Option<Lsn>)>, Expiry>> {
        let bad = || Error::InvalidArgument("not a change feed token".into());
        let (&version, rest) = bytes.split_first().ok_or_else(bad)?;
        if version != TOKEN_VERSION || rest.len() < 16 + 4 + 8 + 8 {
            return Err(bad());
        }
        let u64_at = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
        if rest[..16] != db_id {
            return Err(Error::InvalidArgument(
                "the token is another database's".into(),
            ));
        }
        let streams = u32::from_le_bytes(rest[16..20].try_into().unwrap()) as usize;
        let delivered = u64_at(rest, 20);
        let session = u64_at(rest, 28);
        let per = &rest[36..];
        if per.len() != streams * 16 {
            return Err(bad());
        }
        self.delivered = delivered;
        self.session = session;
        if streams != self.shared.shards {
            return Ok(Err(expiry(format!(
                "the shard count changed ({streams} to {})",
                self.shared.shards
            ))));
        }
        if session != 0 && !self.history.holds(session) {
            return Ok(Err(expiry(
                "another session wrote without feeding these tables since the token",
            )));
        }
        let feed = self.shared.feed.clone().expect("checked at open");
        let mut starts = Vec::with_capacity(streams);
        for s in 0..streams {
            let hint = Lsn(u64_at(per, s * 16));
            let reached = Lsn(u64_at(per, s * 16 + 8));
            if hint.epoch() != 0 && hint.epoch() < floor(&feed, s) {
                return Ok(Err(expiry("retention passed the token")));
            }
            starts.push((hint, (reached > hint).then_some(reached)));
        }
        Ok(Ok(starts))
    }

    fn poll(&mut self) -> Result<Polled> {
        if self.expired.is_some() {
            return Ok(Polled::Expired);
        }
        // A commit already read and below the frontier goes first.
        if self.emit()? {
            return Ok(Polled::Commit);
        }
        if self.expired.is_some() {
            return Ok(Polled::Expired);
        }
        let feed = self.shared.feed.clone().expect("checked at open");
        // simp724's order: the checkpoints, then the frontier, then the heads.
        let checkpoints: Vec<Lsn> = (0..self.streams.len())
            .map(|s| feed.marks(s).map_or(Lsn::default(), |m| m.checkpoint()))
            .collect();
        let frontier = self.shared.shm.written_seqno(self.durable);
        let mut caught_up = true;
        for s in 0..self.streams.len() {
            let head = feed.marks(s).map_or(0, |m| m.head());
            if !self.read_stream(s)? {
                return Ok(Polled::Expired);
            }
            // A stream whose cursor is behind its head waits for the next segment's header
            // (held back until its predecessor's sync, FORMAT §10.1 rule 1).
            if self.streams[s].cursor.epoch() < head {
                caught_up = false;
            }
        }
        self.drop_aborted(&checkpoints, &feed);
        if !caught_up {
            return Ok(Polled::Pending { held: true });
        }
        self.frontier = self.frontier.max(frontier);
        if self.emit()? {
            return Ok(Polled::Commit);
        }
        if self.expired.is_some() {
            return Ok(Polled::Expired);
        }
        // Held by the feed's own holds: the visible watermark may be past the frontier.
        let watermark = if self.durable {
            self.shared.shm.durable_seqno()
        } else {
            self.shared.shm.visible_seqno()
        };
        Ok(Polled::Pending {
            held: watermark > self.frontier,
        })
    }

    /// Reads stream `s` to its readable end. Returns `false` once the feed expired.
    fn read_stream(&mut self, s: usize) -> Result<bool> {
        loop {
            let before = self.streams[s].cursor.position();
            let Some((start, end, session, item)) = self.read_one(s)? else {
                return Ok(self.expired.is_none());
            };
            // A token's delivered records must still be there (a torn tail or a restart
            // that lost them expires it, M2).
            if let Some(m) = self.streams[s].must_reach {
                if start.epoch() != m.epoch() && before.epoch() == m.epoch() && before < m {
                    self.expired = Some(expiry(
                        "records the feed delivered were lost (a torn tail or a writer restart)",
                    ));
                    return Ok(false);
                }
                if end >= m {
                    self.streams[s].must_reach = None;
                }
            }
            let inside = self.history.holds(session);
            if inside {
                self.session = self.session.max(session);
            }
            match item {
                Item::Single { seqno, ts, changes } => {
                    if seqno <= self.delivered {
                        let st = &mut self.streams[s];
                        st.delivered_end = st.delivered_end.max(end);
                        continue;
                    }
                    if !inside {
                        self.expired = Some(gap(seqno));
                        return Ok(false);
                    }
                    self.ready.insert(
                        seqno,
                        Pending::Single {
                            ts,
                            changes,
                            stream: s,
                            start,
                            end,
                        },
                    );
                }
                Item::Prepare { id, changes } => {
                    self.held.entry(id).or_default().push(Held {
                        stream: s,
                        start,
                        end,
                        changes,
                        outside: !inside,
                    });
                }
                Item::Decision {
                    id,
                    apply,
                    ts,
                    participants,
                } => {
                    if apply <= self.delivered {
                        self.held.remove(&id);
                        let st = &mut self.streams[s];
                        st.delivered_end = st.delivered_end.max(end);
                        continue;
                    }
                    if !inside {
                        self.expired = Some(gap(apply));
                        return Ok(false);
                    }
                    self.ready.insert(
                        apply,
                        Pending::Decided {
                            id,
                            ts,
                            participants,
                            stream: s,
                            start,
                            end,
                        },
                    );
                }
                Item::Other => {
                    // A format 2 COMMIT is never in a fed session's segments (only this
                    // build writes feed marks); any other type is newer than this build.
                    if inside {
                        return Err(Error::Corruption(
                            "a change feed segment holds a record this build does not write".into(),
                        ));
                    }
                }
            }
        }
    }

    /// Stream `s`'s next record as an owned item, its mutations filtered to the feed's
    /// tables and prefix; `None` at the readable end, or once its segment is gone (which
    /// expires the feed).
    fn read_one(&mut self, s: usize) -> Result<Option<(Lsn, Lsn, u64, Item)>> {
        let (tables, prefix) = (&self.tables, &self.prefix);
        let filter = |batch: BatchRef<'_>| -> Result<Vec<Raw>> {
            let mut out = Vec::new();
            for m in batch.iter() {
                let m = m?;
                // The table first, then the row: a mutation outside is skipped by its
                // lengths, never copied (§2.7).
                if tables.binary_search(&m.table).is_err() || !m.row.starts_with(prefix) {
                    continue;
                }
                out.push(Raw {
                    table: m.table,
                    family: m.family,
                    kind: m.kind,
                    ts: m.ts,
                    row: m.row.to_vec(),
                    qualifier: m.qualifier.to_vec(),
                    value: m.value.to_vec(),
                });
            }
            Ok(out)
        };
        let (start, end, session, record) = match self.streams[s].cursor.step()? {
            Step::Record {
                start,
                end,
                session,
                record,
            } => (start, end, session, record),
            Step::End => return Ok(None),
            Step::Gone => {
                self.expired = Some(expiry(
                    "retention passed the feed: a segment it was reading was recycled",
                ));
                return Ok(None);
            }
        };
        let item = match record {
            WalRecord::Batch {
                seqno,
                commit_ts,
                batch,
            } => Item::Single {
                seqno,
                ts: commit_ts,
                changes: filter(batch)?,
            },
            WalRecord::Prepare { seqno, batch, .. } => Item::Prepare {
                id: seqno,
                changes: filter(batch)?,
            },
            WalRecord::Decision {
                seqno,
                apply_seqno,
                commit_ts,
                participants,
            } => Item::Decision {
                id: seqno,
                apply: apply_seqno,
                ts: commit_ts,
                participants: participants.iter().collect(),
            },
            _ => Item::Other,
        };
        Ok(Some((start, end, session, item)))
    }

    /// Drops held PREPAREs of rounds that ended without a Decision: their stream's
    /// checkpoint (loaded before the frontier) passed them and no Decision for them was read;
    /// or their segment fell below the stream's floor.
    fn drop_aborted(&mut self, checkpoints: &[Lsn], feed: &crate::feed::Feed) {
        let decided: std::collections::HashSet<Seqno> = self
            .ready
            .values()
            .filter_map(|p| match p {
                Pending::Decided { id, .. } => Some(*id),
                Pending::Single { .. } => None,
            })
            .collect();
        self.held.retain(|id, shares| {
            if decided.contains(id) {
                return true;
            }
            shares.retain(|h| {
                let passed = checkpoints.get(h.stream).is_some_and(|&c| c > h.start);
                let gone = h.start.epoch() < floor(feed, h.stream);
                !(passed || gone)
            });
            !shares.is_empty()
        });
    }

    /// Delivers the smallest pending commit at or below the frontier into `out`. Returns
    /// whether it did; a commit with nothing the feed follows is passed over.
    fn emit(&mut self) -> Result<bool> {
        loop {
            let Some((&key, _)) = self.ready.first_key_value() else {
                return Ok(false);
            };
            if key > self.frontier {
                return Ok(false);
            }
            let (_, p) = self.ready.pop_first().expect("checked");
            let (ts, raws) = match p {
                Pending::Single {
                    ts,
                    changes,
                    stream,
                    end,
                    ..
                } => {
                    let st = &mut self.streams[stream];
                    st.delivered_end = st.delivered_end.max(end);
                    (ts, changes)
                }
                Pending::Decided {
                    id,
                    ts,
                    participants,
                    stream,
                    end,
                    ..
                } => {
                    let st = &mut self.streams[stream];
                    st.delivered_end = st.delivered_end.max(end);
                    let shares = self.held.remove(&id).unwrap_or_default();
                    // Every participant's PREPARE was in its file before the Decision was
                    // written, so it has been read; one missing was lost under the reader.
                    let complete = participants
                        .iter()
                        .all(|p| shares.iter().any(|h| h.stream == p.0 as usize));
                    if !complete || shares.iter().any(|h| h.outside) {
                        self.expired = Some(gap(key));
                        return Ok(false);
                    }
                    let mut raws = Vec::new();
                    for h in shares {
                        let st = &mut self.streams[h.stream];
                        st.delivered_end = st.delivered_end.max(h.end);
                        raws.extend(h.changes);
                    }
                    (ts, raws)
                }
            };
            self.delivered = key;
            if raws.is_empty() {
                continue;
            }
            let view = self.shared.view.load();
            self.out.seqno = key;
            self.out.ts = ts;
            self.out.changes.clear();
            for r in raws {
                let value = match view.ssts.read_blob(&r.value) {
                    Ok(Some(cell)) => cell.to_vec(),
                    Ok(None) => r.value,
                    // The blob GC horizon follows the oldest retained segment, not readers: a
                    // file gone under a slow reader means retention passed it (S1).
                    Err(Error::Corruption(_)) => {
                        self.expired = Some(expiry(
                            "a blob value the feed was delivering is gone: retention passed the feed",
                        ));
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                };
                self.out.changes.push(Change {
                    table: r.table,
                    family: r.family,
                    kind: r.kind,
                    ts: r.ts.unwrap_or(ts),
                    row: r.row,
                    qualifier: r.qualifier,
                    value,
                });
            }
            return Ok(true);
        }
    }
}

fn gap(key: Seqno) -> Expiry {
    expiry(format!(
        "history has a gap before seqno {key}: a session that did not feed these tables wrote the database"
    ))
}

/// Stream `s`'s floor: no segment of a smaller epoch is kept.
fn floor(feed: &crate::feed::Feed, s: usize) -> u32 {
    feed.marks(s).map_or(0, |m| m.floor())
}

/// The sessions whose segments hold the feed's history: the current one, and each one
/// before it while it fed every table in `tables`, had the same stream count, and chains
/// (its `opened_at` is the previous session's last recorded `sequence`, with no feed-off
/// open between).
fn read_history(shared: &Shared, current: u64, tables: &[TableId]) -> Result<History> {
    let path = crate::feed::sidecar_path(&shared.path);
    let file = shared.vfs.open(&path, OpenOptions::read())?;
    let len = file.len()?;
    let mut bytes = vec![0u8; len as usize];
    file.read_at(&mut bytes, 0)?;
    let entries = match FeedHeader::decode(&bytes) {
        Ok(h) if h.db_id == shared.pager.db_id() => decode_entries(&bytes[FeedHeader::LEN..]).0,
        _ => Vec::new(),
    };
    // (id, feeds the tables, streams, chained to the previous)
    let mut sessions: Vec<(u64, bool, u32, bool)> = Vec::new();
    let mut last_seq: Option<u64> = None;
    let mut broken = true;
    for e in entries {
        match e {
            FeedEntry::Session {
                session,
                opened_at,
                streams,
                tables: fed,
                ..
            } => {
                let feeds = tables.iter().all(|&t| match &fed {
                    FeedTables::All => true,
                    FeedTables::Only(_) => fed.contains(t),
                });
                let chained = !broken && last_seq == Some(opened_at);
                sessions.push((session, feeds, streams, chained));
                last_seq = Some(opened_at);
                broken = false;
            }
            FeedEntry::Root { session, sequence } => {
                if sessions.last().is_some_and(|s| s.0 == session) {
                    last_seq = Some(sequence);
                }
            }
            FeedEntry::Unfed { .. } => broken = true,
            FeedEntry::Unknown(_) => {}
        }
    }
    let mut run = Vec::new();
    let Some(pos) = sessions.iter().rposition(|s| s.0 == current) else {
        return Ok(History {
            sessions: vec![current],
        });
    };
    let streams = sessions[pos].2;
    for (i, s) in sessions[..=pos].iter().enumerate().rev() {
        if !s.1 || s.2 != streams {
            break;
        }
        run.push(s.0);
        if !s.3 || i == 0 {
            break;
        }
    }
    Ok(History { sessions: run })
}
