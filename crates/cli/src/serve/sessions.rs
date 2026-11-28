//! The session table (D313 §3): transactions and pinned snapshots held on the server between
//! requests, each under an unguessable id bound to the token that opened it.
//!
//! - **One call at a time on a transaction** (B2): a call takes the `Transaction` out of its
//!   entry under a short lock, never held across an `.await`, and a guard puts it back when
//!   the call ends, even if the call's future is dropped. A second call meanwhile gets
//!   `SessionBusy`, never a queue. Snapshot reads take `&Snapshot`, so they run in parallel.
//! - **Leases:** each session has an idle lease, renewed at a call's start and end, and a hard
//!   lifetime. A session with a call in flight never expires; it does once the call ends.
//!   A stream at a pinned snapshot (a scan's pages) is the exception: its next page past the
//!   hard lifetime fails, so a slowly read stream can't keep the snapshot past
//!   `--snapshot-lifetime-ms` (audit120's S-S3), and the snapshot goes when the stream ends.
//!   Expiry is checked on every call as well as by the reaper, so a lease is exact whatever
//!   the reaper's period.
//! - **The tombstone** (B1): once `Commit` has run, the session holds its outcome, and a
//!   repeated `Commit` answers it until the tombstone expires. A transaction that expires
//!   before any `Commit` leaves a marker for one idle lease, which answers
//!   `TransactionExpired`; after that, the id is `NotFound`.
//! - **Caps:** sessions in total and per token: open transactions, pinned snapshots and MCP
//!   sessions, what holds a snapshot, a read set or memory. A tombstone or a marker is a
//!   small record kept only for a retry, so it gives its slot back and counts against
//!   [`MAX_RECORDS`] per node instead, the oldest going first past it (#2253: counted in
//!   the caps, they capped a token at ~2 commits a second).
//! - **Remote MCP sessions** (`Mcp-Session-Id`, D313 §7 and ai1249's M1) live here too, with
//!   leases of their own: bound to their token, never expired with a request in flight
//!   (any number at once, unlike a transaction; the session caps its own), and counted in
//!   the caps.

use super::auth::Principal;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once, PoisonError, Weak};
use std::time::{Duration, Instant};

use pigeonhole::{Durability, Snapshot, Transaction};

use super::errors::ApiError;

/// The session limits (`--txn-idle-ms` and the rest).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Leases {
    pub txn_idle: Duration,
    pub txn_lifetime: Duration,
    pub snapshot_idle: Duration,
    pub snapshot_lifetime: Duration,
    pub max_sessions: usize,
    pub max_per_token: usize,
    /// A remote MCP session's idle lease and lifetime (`--mcp-session-idle-ms`,
    /// `--mcp-session-lifetime-ms`).
    pub mcp_idle: Duration,
    pub mcp_lifetime: Duration,
}

type Id = [u8; 16];

/// A recorded commit outcome: the `Commit`'s answer, which a spanning transaction's carries
/// its parts in (D329).
pub(crate) type Outcome = Result<super::proto::TxnCommitResponse, ApiError>;

enum State {
    /// An open transaction: `None` while a call has it out.
    Txn(Option<Transaction>),
    /// `Commit` received and running.
    Committing,
    /// The tombstone: the commit's outcome.
    Done(Outcome),
    /// Expired before any `Commit`: answers `TransactionExpired` until it expires itself.
    Expired,
    Snapshot(Snapshot),
    /// A remote MCP session.
    Mcp(Arc<super::mcp::McpSession>),
}

struct Entry {
    principal: Principal,
    state: State,
    /// Calls in flight: at most one on a transaction.
    busy: usize,
    /// The transaction's durability, for its commit.
    durability: Option<Durability>,
    /// Whether a `TxnWrite` buffered writes in the transaction: its `Commit` is then a
    /// write, which a fenced partition refuses (D317 ticket 6).
    wrote: bool,
    renewed: Instant,
    idle: Duration,
    hard: Instant,
    opened: Instant,
    /// A spanning transaction's id, its xid (D329): on its home's table (the home session)
    /// and on each branch partition's (the branch). Such a session is found only by this
    /// exact text, never by the plain `<partition>:<32 hex>` form of the same key.
    xid: Option<Arc<str>>,
    /// A spanning transaction's home session: what each partition did (`None` while a call
    /// has it out, as the transaction).
    span: Option<Span>,
    /// What a range move's in-doubt watch needs of its writes (D331 PR 5): out of the entry
    /// with the transaction.
    watched: Watched,
    /// Whether it holds a slot in the caps: until it becomes a record (a tombstone or a
    /// marker, [`Table::become_record`]), which counts against [`MAX_RECORDS`] instead.
    counted: bool,
}

/// The records (tombstones and markers) a node keeps at most, across its tables (#2253):
/// past it, a table drops its own oldest. Sized to cover a whole default `--txn-idle-ms`
/// (30 s) at about 4,000 commits a second (#2221 measured 2,500 to 2,900 per node), so a
/// repeated `Commit` answers its outcome for the whole lease below that rate. A record is a
/// few hundred bytes: some tens of MB at most.
pub(crate) const MAX_RECORDS: usize = 1 << 17;

/// A transaction's writes as D331 PR 5's in-doubt watch sees them (simp724's rule): the
/// partition's watch sequence at its first write, and the moves whose watched ranges it
/// wrote. Its branch's prepare records both
/// ([`RangeGate::prepared`](super::ranges::RangeGate::prepared)).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Watched {
    pub(crate) first: Option<u64>,
    pub(crate) moves: Vec<String>,
}

/// A spanning transaction's coordinator state (D329), kept in its home session's entry:
/// what each partition it touched did. Taken out and put back with the transaction, so it
/// is found, checked against the caller, renewed and dropped with the session, under one
/// lock (audit120's M1 on #2062).
#[derive(Debug, Default, Clone)]
pub(crate) struct Span {
    pub(crate) parts: std::collections::BTreeMap<String, Touched>,
}

/// What one partition did in a spanning transaction, as its home sees it.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Touched {
    pub(crate) read: bool,
    pub(crate) wrote: bool,
    /// When the branch was last opened or renewed on its owner (`None`: not opened, or the
    /// home).
    pub(crate) renewed: Option<Instant>,
}

impl Entry {
    fn renew(&mut self, now: Instant) {
        self.renewed = now;
    }

    fn lapsed(&self, now: Instant) -> bool {
        self.busy == 0 && (now >= self.hard || now.duration_since(self.renewed) >= self.idle)
    }
}

/// A node's session counts (`--max-sessions`, `--max-sessions-per-token`), shared by every
/// table on it: one per partition on a cluster node (D317 ticket 8a), whose caps stay the
/// node's. The total and each principal's count are under one lock (cli692's Q2), taken
/// inside a table's lock and never the other way round.
pub(crate) struct Caps(Mutex<Counts>, AtomicU64);

impl Default for Caps {
    fn default() -> Self {
        Self(
            Mutex::new(Counts {
                max_records: MAX_RECORDS,
                ..Counts::default()
            }),
            AtomicU64::new(0),
        )
    }
}

#[derive(Default)]
struct Counts {
    total: usize,
    per_principal: HashMap<Principal, usize>,
    /// Records (tombstones and markers) on every table: out of `total`, bounded by
    /// `max_records` ([`MAX_RECORDS`]; smaller in a test of the bound).
    records: usize,
    max_records: usize,
}

impl Caps {
    fn lock(&self) -> MutexGuard<'_, Counts> {
        // Every change under it is a pair of counter steps, done together.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts a session for `principal`, or refuses it at either cap.
    fn take(&self, principal: Principal, leases: &Leases) -> Result<(), ApiError> {
        let mut c = self.lock();
        if c.total >= leases.max_sessions {
            return Err(ApiError::overloaded(
                "the server is at its session limit (--max-sessions)",
            ));
        }
        let mine = c.per_principal.entry(principal).or_insert(0);
        if *mine >= leases.max_per_token {
            if *mine == 0 {
                c.per_principal.remove(&principal);
            }
            return Err(ApiError::overloaded(
                "this token is at its session limit (--max-sessions-per-token)",
            ));
        }
        *mine += 1;
        c.total += 1;
        Ok(())
    }

    /// A session of `principal`'s ended.
    fn give(&self, principal: Principal) {
        Self::give_in(&mut self.lock(), principal);
    }

    fn give_in(c: &mut Counts, principal: Principal) {
        c.total = c.total.saturating_sub(1);
        if let Some(n) = c.per_principal.get_mut(&principal) {
            *n -= 1;
            if *n == 0 {
                c.per_principal.remove(&principal);
            }
        }
    }

    /// A session of `principal`'s became a record: its slot goes back, and it counts as a
    /// record. Whether the node is then past [`MAX_RECORDS`].
    fn record(&self, principal: Principal) -> bool {
        let mut c = self.lock();
        Self::give_in(&mut c, principal);
        c.records += 1;
        c.records > c.max_records
    }

    /// A record went (its lease, or [`MAX_RECORDS`]). Whether the node is still past it.
    fn unrecord(&self) -> bool {
        let mut c = self.lock();
        c.records = c.records.saturating_sub(1);
        c.records > c.max_records
    }

    /// Caps whose record bound is `max` (a test of the bound: [`MAX_RECORDS`] is too many
    /// to reach in one).
    #[cfg(test)]
    pub(crate) fn with_max_records(max: usize) -> Self {
        let caps = Self::default();
        caps.lock().max_records = max;
        caps
    }

    /// Records dropped for [`MAX_RECORDS`] before their lease ended, since the node started:
    /// each one a repeated `Commit` (or call on an expired transaction) answered `NotFound`
    /// sooner than its lease.
    pub(crate) fn records_dropped(&self) -> u64 {
        self.1.load(Ordering::Relaxed)
    }
}

struct Table {
    map: HashMap<Id, Entry>,
    /// The node's counts, which this table's sessions are part of.
    caps: Arc<Caps>,
    /// This table's records in the order they became records, oldest first: what goes past
    /// [`MAX_RECORDS`]. An id here may be gone already (its lease ended), and is passed over.
    records: VecDeque<Id>,
}

impl Table {
    fn remove(&mut self, id: &Id) -> Option<Entry> {
        let e = self.map.remove(id)?;
        if e.counted {
            self.caps.give(e.principal);
        } else {
            self.caps.unrecord();
        }
        Some(e)
    }

    /// Session `id` became a record (a tombstone or a marker, #2253): it gives its slot in
    /// the caps back and counts against [`MAX_RECORDS`]. Past that, this table drops its own
    /// oldest records, never `id` itself, so a node keeps at most the bound plus one record
    /// per table, and no table reaches into another's lock. The table that pays is the one
    /// committing, whichever filled the bound (cli692 on #2257): a busy partition drops its
    /// own retries' answers sooner, and a quiet one's age out on their lease.
    fn become_record(&mut self, id: &Id) {
        let Some(e) = self.map.get_mut(id) else {
            return;
        };
        if !e.counted {
            return;
        }
        e.counted = false;
        let mut over = self.caps.record(e.principal);
        // Ids whose lease ended since: the queue stays about as long as the records.
        while let Some(old) = self.records.front() {
            if self.map.get(old).is_some_and(|e| !e.counted) {
                break;
            }
            self.records.pop_front();
        }
        self.records.push_back(*id);
        while over {
            let Some(old) = self.records.front().copied() else {
                break;
            };
            if old == *id {
                break;
            }
            self.records.pop_front();
            if !self.map.get(&old).is_some_and(|e| !e.counted) {
                continue;
            }
            self.map.remove(&old);
            self.caps.1.fetch_add(1, Ordering::Relaxed);
            over = self.caps.unrecord();
        }
    }

    /// Expires `id` if its lease has lapsed: an open transaction becomes its marker (the
    /// transaction is dropped, releasing its snapshot), anything else goes. Returns whether
    /// it expired.
    fn expire(&mut self, id: &Id, now: Instant, leases: &Leases) -> bool {
        let Some(e) = self.map.get_mut(id) else {
            return false;
        };
        if !e.lapsed(now) {
            return false;
        }
        if matches!(e.state, State::Txn(_)) {
            e.state = State::Expired;
            e.renewed = now;
            e.idle = leases.txn_idle;
            e.hard = now + leases.txn_idle;
            self.become_record(id);
        } else {
            self.remove(id);
        }
        true
    }
}

/// The session table.
pub(crate) struct Sessions {
    table: Mutex<Table>,
    leases: Leases,
    reaper: Once,
    /// On a cluster node, the partition its sessions are on (D317 ticket 3): ids are
    /// `<partition>:<32 hex>`, so any node routes a call on one without a lookup.
    partition: Option<String>,
}

impl std::fmt::Debug for Sessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sessions")
            .field("leases", &self.leases)
            .finish_non_exhaustive()
    }
}

/// What a session is, as [`Sessions::peek`] finds it.
pub(crate) enum Peek {
    Live,
    Done(Outcome),
    Gone,
    /// A spanning transaction's entry that another principal owns (#2077, audit120's S5 on
    /// #2076): a client's `Outcome` answers it `NotFound` and reads nothing further.
    Foreign,
}

/// What a transaction call found. Moved once, from the table to the call, never stored, so
/// the guard (which holds the transaction by value) isn't boxed.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Taken {
    /// The transaction, out of its entry for this call.
    Open(TxnGuard),
    /// The tombstone's outcome.
    Done(Outcome),
}

/// What a snapshot's open sessions and tombstones look like, for `/metrics`.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Stats {
    pub transactions: usize,
    pub snapshots: usize,
    pub tombstones: usize,
    pub mcp: usize,
    pub oldest: Duration,
}

impl Sessions {
    /// A session table: a single node's (`partition` `None`, ids 32 hex), or a cluster
    /// partition's, whose ids name it (D317 ticket 3: `<partition>:<32 hex>`). It counts
    /// against `caps`, which every table on the node shares (D317 ticket 8a): the caps stay
    /// the node's.
    pub(crate) fn sharing(leases: Leases, partition: Option<String>, caps: Arc<Caps>) -> Arc<Self> {
        Arc::new(Self {
            table: Mutex::new(Table {
                map: HashMap::new(),
                caps,
                records: VecDeque::new(),
            }),
            leases,
            reaper: Once::new(),
            partition,
        })
    }

    /// Session `id` as a client sees it.
    fn text(&self, id: &Id) -> String {
        match &self.partition {
            Some(p) => format!("{p}:{}", hex(id)),
            None => hex(id),
        }
    }

    /// A client's session id as this table's key: `None` for any other form, or another
    /// partition's (cli692's S4 on ticket 3: `NotFound` either way).
    fn parse(&self, text: &str) -> Option<Id> {
        match &self.partition {
            Some(p) => {
                let (q, hex) = partition_of(text)?;
                if q == p { parse_id(hex) } else { None }
            }
            None => parse_id(text),
        }
    }

    /// A transaction call's id as this table's key, and the xid its entry must carry: a
    /// spanning transaction's id (D329, `<home>:h<epoch>-<32 hex>`) is its home's session
    /// on the home's table and a branch on any other partition's, so it's taken whatever its
    /// prefix; a plain id is [`parse`](Self::parse)'s.
    fn parse_txn<'a>(&self, text: &'a str) -> Option<(Id, Option<&'a str>)> {
        if self.partition.is_some()
            && let Some((_, _, hex)) = spanning_parts(text)
        {
            return Some((parse_id(hex)?, Some(text)));
        }
        Some((self.parse(text)?, None))
    }

    fn lock(&self) -> MutexGuard<'_, Table> {
        // A panic while the lock was held left the table consistent: every change under it
        // is a single insert, remove or field store.
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a session for `principal`, within the caps.
    fn insert(
        self: &Arc<Self>,
        principal: Principal,
        state: State,
        durability: Option<Durability>,
        idle: Duration,
        lifetime: Duration,
    ) -> Result<String, ApiError> {
        let id = new_id()?;
        self.insert_at(id, principal, state, durability, idle, lifetime, None, None)?;
        Ok(self.text(&id))
    }

    /// [`insert`](Self::insert) under key `id`, with a spanning transaction's `xid`.
    #[allow(clippy::too_many_arguments)]
    fn insert_at(
        self: &Arc<Self>,
        id: Id,
        principal: Principal,
        state: State,
        durability: Option<Durability>,
        idle: Duration,
        lifetime: Duration,
        xid: Option<Arc<str>>,
        span: Option<Span>,
    ) -> Result<(), ApiError> {
        self.start_reaper();
        let now = Instant::now();
        let mut t = self.lock();
        if t.map.contains_key(&id) {
            return Err(ApiError::internal("a session id is taken twice"));
        }
        t.caps.take(principal, &self.leases)?;
        t.map.insert(
            id,
            Entry {
                principal,
                state,
                busy: 0,
                durability,
                wrote: false,
                renewed: now,
                idle,
                hard: now + lifetime,
                opened: now,
                xid,
                span,
                watched: Watched::default(),
                counted: true,
            },
        );
        Ok(())
    }

    /// Holds `txn` for `principal` and returns its id.
    pub(crate) fn begin(
        self: &Arc<Self>,
        principal: Principal,
        txn: Transaction,
        durability: Option<Durability>,
    ) -> Result<String, ApiError> {
        self.insert(
            principal,
            State::Txn(Some(txn)),
            durability,
            self.leases.txn_idle,
            self.leases.txn_lifetime,
        )
    }

    /// Holds a spanning transaction's home share (D329) for `principal` and returns its id,
    /// `<partition>:h<history_epoch>-<32 hex>`, which is also its xid.
    pub(crate) fn begin_spanning(
        self: &Arc<Self>,
        principal: Principal,
        txn: Transaction,
        durability: Option<Durability>,
        history_epoch: u64,
    ) -> Result<String, ApiError> {
        let Some(p) = &self.partition else {
            return Err(ApiError::internal("a spanning transaction off a cluster"));
        };
        // The first 48 bits are the begin time in ms (the rest random): its home tells a
        // decision record GC'd past retention from one never written (D329 2b).
        let mut id = new_id()?;
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        id[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
        let xid: Arc<str> = format!("{p}:h{history_epoch}-{}", hex(&id)).into();
        self.insert_at(
            id,
            principal,
            State::Txn(Some(txn)),
            durability,
            self.leases.txn_idle,
            self.leases.txn_lifetime,
            Some(Arc::clone(&xid)),
            Some(Span::default()),
        )?;
        Ok(xid.to_string())
    }

    /// Opens spanning transaction `xid`'s branch here for `principal` (D329), or with
    /// `renew` renews the open one: the home's first call on this partition opens it, and
    /// its later calls renew it (S4). A renew finds the branch or answers
    /// `TransactionExpired`, and an open finds none or answers `NotFound`, so the home never
    /// silently reopens a branch at a newer snapshot with what it read and wrote gone
    /// (audit120's M2 on #2062).
    pub(crate) fn begin_branch(
        self: &Arc<Self>,
        principal: Principal,
        open: impl FnOnce() -> Result<Transaction, ApiError>,
        xid: &str,
        renew: bool,
    ) -> Result<(), ApiError> {
        let gone = || ApiError::not_found("no such transaction");
        let key = spanning_parts(xid)
            .and_then(|(_, _, hex)| parse_id(hex))
            .ok_or_else(|| ApiError::invalid("not a spanning transaction's id"))?;
        {
            let now = Instant::now();
            let mut t = self.lock();
            t.expire(&key, now, &self.leases);
            match t.map.get_mut(&key) {
                Some(e) if e.xid.as_deref() == Some(xid) && e.principal == principal => {
                    return match e.state {
                        State::Txn(_) if renew => {
                            e.renew(now);
                            Ok(())
                        }
                        State::Txn(_) | State::Committing => Err(gone()),
                        State::Expired => Err(ApiError::transaction_expired()),
                        _ => Err(gone()),
                    };
                }
                Some(_) => return Err(gone()),
                None if renew => return Err(ApiError::transaction_expired()),
                None => {}
            }
        }
        self.insert_at(
            key,
            principal,
            State::Txn(Some(open()?)),
            None,
            // Twice the home's: the home renews it at a call once it's half an idle lease
            // old, and calls may come up to an idle lease apart (audit120's second pass).
            self.leases.txn_idle * 2,
            self.leases.txn_lifetime,
            Some(xid.into()),
            None,
        )
    }

    /// What session `id` is now, without taking it (D329's `Outcome` and resolver): open or
    /// committing (`Live`), its recorded outcome, or gone. A spanning id is looked up by its
    /// exact text. `principal` is the client asking, whose own entry alone answers: another
    /// principal's spanning entry is [`Peek::Foreign`] (#2077), and its plain one
    /// [`Peek::Gone`]. `None` is the node's own call (the resolver), which a spanning entry
    /// answers whoever its client is.
    pub(crate) fn peek(&self, id: &str, principal: Option<Principal>) -> Peek {
        let Some((key, xid)) = self.parse_txn(id) else {
            return Peek::Gone;
        };
        let now = Instant::now();
        let mut t = self.lock();
        t.expire(&key, now, &self.leases);
        let Some(e) = t.map.get(&key).filter(|e| e.xid.as_deref() == xid) else {
            return Peek::Gone;
        };
        match principal {
            Some(p) if e.principal != p && xid.is_some() => return Peek::Foreign,
            Some(p) if e.principal != p => return Peek::Gone,
            None if xid.is_none() => return Peek::Gone,
            _ => {}
        }
        match &e.state {
            State::Txn(_) | State::Committing => Peek::Live,
            State::Done(o) => Peek::Done(o.clone()),
            State::Expired | State::Snapshot(_) | State::Mcp(_) => Peek::Gone,
        }
    }

    /// `--txn-lifetime-ms`: the longest a transaction lives.
    pub(crate) fn txn_lifetime(&self) -> Duration {
        self.leases.txn_lifetime
    }

    /// Takes branch `xid` out for the home node's own call (D329: its prepare or abort),
    /// whoever the client is. Only a spanning transaction's entry answers.
    pub(crate) fn take_branch(self: &Arc<Self>, xid: &str) -> Result<Taken, ApiError> {
        let gone = || ApiError::not_found("no such transaction");
        let key = spanning_parts(xid)
            .and_then(|(_, _, hex)| parse_id(hex))
            .ok_or_else(gone)?;
        let principal = {
            let now = Instant::now();
            let mut t = self.lock();
            t.expire(&key, now, &self.leases);
            t.map
                .get(&key)
                .filter(|e| e.xid.as_deref() == Some(xid))
                .ok_or_else(gone)?
                .principal
        };
        self.take(xid, principal)
    }

    /// Pins `snapshot` for `principal` and returns its id.
    pub(crate) fn open_snapshot(
        self: &Arc<Self>,
        principal: Principal,
        snapshot: Snapshot,
    ) -> Result<String, ApiError> {
        self.insert(
            principal,
            State::Snapshot(snapshot),
            None,
            self.leases.snapshot_idle,
            self.leases.snapshot_lifetime,
        )
    }

    /// Takes transaction `id` out for one call by `principal`.
    pub(crate) fn take(
        self: &Arc<Self>,
        id: &str,
        principal: Principal,
    ) -> Result<Taken, ApiError> {
        let gone = || ApiError::not_found("no such transaction");
        let (key, xid) = self.parse_txn(id).ok_or_else(gone)?;
        let now = Instant::now();
        let mut t = self.lock();
        t.expire(&key, now, &self.leases);
        let e = t
            .map
            .get_mut(&key)
            .filter(|e| e.principal == principal && e.xid.as_deref() == xid)
            .ok_or_else(gone)?;
        match &mut e.state {
            State::Txn(slot) => {
                let Some(txn) = slot.take() else {
                    return Err(ApiError::session_busy());
                };
                e.busy = 1;
                e.renew(now);
                Ok(Taken::Open(TxnGuard {
                    sessions: Arc::clone(self),
                    id: key,
                    txn: Some(txn),
                    durability: e.durability,
                    wrote: e.wrote,
                    span: e.span.take(),
                    watched: std::mem::take(&mut e.watched),
                    ended: false,
                    if_dropped: None,
                }))
            }
            State::Committing => Err(ApiError::session_busy()),
            State::Done(outcome) => {
                let outcome = outcome.clone();
                e.renew(now);
                Ok(Taken::Done(outcome))
            }
            State::Expired => Err(ApiError::transaction_expired()),
            State::Snapshot(_) | State::Mcp(_) => Err(gone()),
        }
    }

    /// Holds a remote MCP session for `principal` and returns its id.
    pub(crate) fn open_mcp(
        self: &Arc<Self>,
        principal: Principal,
        session: Arc<super::mcp::McpSession>,
    ) -> Result<String, ApiError> {
        self.insert(
            principal,
            State::Mcp(session),
            None,
            self.leases.mcp_idle,
            self.leases.mcp_lifetime,
        )
    }

    /// MCP session `id`, for one request by `principal`: the request holds it busy (so it doesn't
    /// expire under the request) until the guard drops. Another token's id, a malformed one
    /// and an expired one are all `NotFound`, which `/mcp` answers 404: the client starts a
    /// new session.
    pub(crate) fn mcp(
        self: &Arc<Self>,
        id: &str,
        principal: Principal,
    ) -> Result<McpGuard, ApiError> {
        let gone = || ApiError::not_found("no such MCP session (ended or expired)");
        let key = self.parse(id).ok_or_else(gone)?;
        let now = Instant::now();
        let mut t = self.lock();
        t.expire(&key, now, &self.leases);
        let e = t
            .map
            .get_mut(&key)
            .filter(|e| e.principal == principal)
            .ok_or_else(gone)?;
        let State::Mcp(session) = &e.state else {
            return Err(gone());
        };
        let session = Arc::clone(session);
        e.busy += 1;
        e.renew(now);
        Ok(McpGuard {
            sessions: Arc::clone(self),
            id: key,
            session,
        })
    }

    /// Ends MCP session `id` (`DELETE /mcp`). Requests already running finish.
    pub(crate) fn end_mcp(&self, id: &str, principal: Principal) -> Result<(), ApiError> {
        let gone = || ApiError::not_found("no such MCP session (ended or expired)");
        let key = self.parse(id).ok_or_else(gone)?;
        let mut t = self.lock();
        match t.map.get(&key) {
            Some(e) if e.principal == principal && matches!(e.state, State::Mcp(_)) => {
                t.remove(&key);
                Ok(())
            }
            _ => Err(gone()),
        }
    }

    /// Snapshot `id`, for one read by `principal`: the read holds the session busy (so it doesn't
    /// expire under the read) until the guard drops.
    pub(crate) fn snapshot(
        self: &Arc<Self>,
        id: &str,
        principal: Principal,
    ) -> Result<SnapshotGuard, ApiError> {
        let gone = || ApiError::not_found("no such snapshot (released or expired)");
        let key = self.parse(id).ok_or_else(gone)?;
        let now = Instant::now();
        let mut t = self.lock();
        t.expire(&key, now, &self.leases);
        let e = t
            .map
            .get_mut(&key)
            .filter(|e| e.principal == principal)
            .ok_or_else(gone)?;
        let State::Snapshot(snapshot) = &e.state else {
            return Err(gone());
        };
        let snapshot = snapshot.clone();
        e.busy += 1;
        e.renew(now);
        Ok(SnapshotGuard {
            sessions: Arc::clone(self),
            id: key,
            snapshot,
        })
    }

    /// Releases snapshot `id` now. Reads already running finish on their own handle; later
    /// ones get `NotFound`.
    pub(crate) fn release(&self, id: &str, principal: Principal) -> Result<(), ApiError> {
        let gone = || ApiError::not_found("no such snapshot (released or expired)");
        let key = self.parse(id).ok_or_else(gone)?;
        let mut t = self.lock();
        match t.map.get(&key) {
            Some(e) if e.principal == principal && matches!(e.state, State::Snapshot(_)) => {
                t.remove(&key);
                Ok(())
            }
            _ => Err(gone()),
        }
    }

    /// Drops every session not in a call: the database under them was replaced (#1436's
    /// re-bootstrap), so their transactions and snapshots are of a closed engine. A session
    /// in a call fails it (`Closed`) and goes when it lapses. Later calls on a dropped one
    /// find no session, and begin again.
    pub(crate) fn drop_idle(&self) {
        let mut t = self.lock();
        let idle: Vec<Id> = t
            .map
            .iter()
            .filter(|(_, e)| e.busy == 0)
            .map(|(id, _)| *id)
            .collect();
        for id in &idle {
            t.remove(id);
        }
    }

    /// Expires every lapsed session as of `now`; sessions with a call in flight are skipped.
    /// Returns how many expired.
    pub(crate) fn sweep(&self, now: Instant) -> usize {
        let mut t = self.lock();
        let lapsed: Vec<Id> = t
            .map
            .iter()
            .filter(|(_, e)| e.lapsed(now))
            .map(|(id, _)| *id)
            .collect();
        lapsed
            .iter()
            .filter(|id| t.expire(id, now, &self.leases))
            .count()
    }

    /// D331 PR 6: the spanning transactions homed on this partition still open (their home
    /// sessions, `--txn-lifetime-ms` bounds them): an emptied partition stays in the map
    /// while any is.
    pub(crate) fn live_homes(&self) -> usize {
        let Some(p) = &self.partition else {
            return 0;
        };
        let t = self.lock();
        t.map
            .values()
            .filter(|e| {
                matches!(e.state, State::Txn(_) | State::Committing)
                    && e.xid
                        .as_deref()
                        .and_then(spanning_parts)
                        .is_some_and(|(home, _, _)| home == p.as_str())
            })
            .count()
    }

    pub(crate) fn stats(&self, now: Instant) -> Stats {
        let t = self.lock();
        let mut s = Stats::default();
        for e in t.map.values() {
            match e.state {
                State::Txn(_) | State::Committing => s.transactions += 1,
                State::Snapshot(_) => s.snapshots += 1,
                State::Done(_) | State::Expired => s.tombstones += 1,
                State::Mcp(_) => s.mcp += 1,
            }
            // The oldest session is about what holds back version GC: an MCP session holds
            // no snapshot, and lives for hours.
            if !matches!(e.state, State::Done(_) | State::Expired | State::Mcp(_)) {
                s.oldest = s.oldest.max(now.duration_since(e.opened));
            }
        }
        s
    }

    /// Starts the reaper once, on the first session (the table is built before the runtime).
    /// It holds the table weakly and ends with it.
    fn start_reaper(self: &Arc<Self>) {
        let weak: Weak<Self> = Arc::downgrade(self);
        let period = (self
            .leases
            .txn_idle
            .min(self.leases.snapshot_idle)
            .min(self.leases.mcp_idle)
            / 4)
        .clamp(Duration::from_millis(10), Duration::from_secs(1));
        self.reaper.call_once(|| {
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(async move {
                    let mut tick = tokio::time::interval(period);
                    loop {
                        tick.tick().await;
                        let Some(sessions) = weak.upgrade() else {
                            return;
                        };
                        sessions.sweep(Instant::now());
                    }
                });
            }
        });
    }

    /// Puts a call's transaction back, or (with `None`) records that the call lost it.
    fn put_back(
        &self,
        id: &Id,
        txn: Option<Transaction>,
        wrote: bool,
        span: Option<Span>,
        watched: Watched,
    ) {
        let now = Instant::now();
        let mut t = self.lock();
        let mut lost = false;
        if let Some(e) = t.map.get_mut(id) {
            e.busy = 0;
            e.wrote |= wrote;
            e.span = span;
            e.watched = watched;
            e.renew(now);
            lost = txn.is_none();
            e.state = match txn {
                Some(txn) => State::Txn(Some(txn)),
                // A call that panicked with the transaction lost it before any Commit: nothing
                // was written.
                None => State::Expired,
            };
        }
        if lost {
            t.become_record(id);
        }
    }

    fn finish(&self, id: &Id, outcome: Outcome) {
        let now = Instant::now();
        let mut t = self.lock();
        if let Some(e) = t.map.get_mut(id) {
            e.busy = 0;
            e.state = State::Done(outcome);
            e.renew(now);
            e.hard = e.hard.max(now + e.idle);
            t.become_record(id);
        }
    }

    /// The node's records dropped for [`MAX_RECORDS`] (every table shares one count).
    pub(crate) fn records_dropped(&self) -> u64 {
        self.lock().caps.records_dropped()
    }

    fn remove(&self, id: &Id) {
        self.lock().remove(id);
    }

    fn end_read(&self, id: &Id) {
        let now = Instant::now();
        let mut t = self.lock();
        let past = t.map.get_mut(id).is_some_and(|e| {
            e.busy = e.busy.saturating_sub(1);
            e.renew(now);
            now >= e.hard
        });
        // Past its hard lifetime (a long stream's), the session goes now rather than at the
        // reaper's next pass (audit120's S-S3).
        if past {
            t.expire(id, now, &self.leases);
        }
    }

    /// Whether the session is there and within its hard lifetime: a stream's next page asks.
    fn alive(&self, id: &Id) -> bool {
        let now = Instant::now();
        self.lock().map.get(id).is_some_and(|e| now < e.hard)
    }
}

/// A transaction out of its entry for one call. Dropped, it puts the transaction back (a
/// call whose future was dropped, a client gone, leaves the session as it found it).
pub(crate) struct TxnGuard {
    sessions: Arc<Sessions>,
    id: Id,
    txn: Option<Transaction>,
    durability: Option<Durability>,
    /// The entry's `wrote`, and set by [`TxnGuard::wrote_now`]: put back with the
    /// transaction.
    wrote: bool,
    /// A spanning transaction's coordinator state, out of its entry with the transaction.
    span: Option<Span>,
    /// The entry's [`Watched`], put back with the transaction.
    watched: Watched,
    ended: bool,
    /// What ends the transaction if the guard is dropped while armed
    /// ([`TxnGuard::end_if_dropped`]).
    if_dropped: Option<(Arc<std::sync::atomic::AtomicBool>, EndDropped)>,
}

/// Ends a transaction whose call was dropped part-way ([`TxnGuard::end_if_dropped`]): given
/// its coordinator state, it does what must follow elsewhere and returns the outcome the
/// session records.
pub(crate) type EndDropped = Box<dyn FnOnce(Option<Span>) -> ApiError + Send>;

impl TxnGuard {
    /// From here on, a guard dropped while the returned flag is set doesn't put the
    /// transaction back: it ends it, and `end`'s error is the session's recorded outcome
    /// (#2157). A spanning transaction's home sets the flag once a call has sent rows to a
    /// branch: a client that goes away after that must not find the session open, or its
    /// retry would send the rows again. Clear the flag once the call has nothing left that
    /// can be cut short. A guard that ends some other way (a commit, an abort) never calls
    /// `end`.
    pub(crate) fn end_if_dropped(&mut self, end: EndDropped) -> Arc<std::sync::atomic::AtomicBool> {
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.if_dropped = Some((Arc::clone(&armed), end));
        armed
    }

    pub(crate) fn txn(&mut self) -> &mut Transaction {
        self.txn
            .as_mut()
            .expect("a guard holds its transaction until it ends")
    }

    /// The transaction, lent to a blocking call; `restore` returns it. A guard dropped without
    /// it (the call panicked) expires the session: the transaction is lost, uncommitted.
    pub(crate) fn lend(&mut self) -> Transaction {
        self.txn
            .take()
            .expect("a guard holds its transaction until it ends")
    }

    pub(crate) fn restore(&mut self, txn: Transaction) {
        self.txn = Some(txn);
    }

    /// Records that this call buffered writes in the transaction.
    pub(crate) fn wrote_now(&mut self) {
        self.wrote = true;
    }

    /// Whether the transaction has buffered writes from a `TxnWrite`.
    pub(crate) fn wrote(&self) -> bool {
        self.wrote
    }

    /// Records a write for D331 PR 5's in-doubt watch: `seq`, the partition's watch sequence
    /// read before its range state, if it's the first; and the moves whose watched ranges
    /// it touched.
    pub(crate) fn note_write(&mut self, seq: u64, moves: Vec<String>) {
        self.watched.first.get_or_insert(seq);
        for m in moves {
            if !self.watched.moves.contains(&m) {
                self.watched.moves.push(m);
            }
        }
    }

    /// What the in-doubt watch has seen of the transaction's writes.
    pub(crate) fn watched(&self) -> &Watched {
        &self.watched
    }

    /// A spanning transaction's coordinator state (D329): `None` for any other session.
    pub(crate) fn span(&mut self) -> Option<&mut Span> {
        self.span.as_mut()
    }

    /// Starts the commit: the session answers `SessionBusy` until the ticket records the
    /// outcome, and the tombstone after.
    pub(crate) fn commit(mut self) -> (Transaction, Option<Durability>, CommitTicket) {
        let txn = self.lend();
        self.ended = true;
        {
            let mut t = self.sessions.lock();
            if let Some(e) = t.map.get_mut(&self.id) {
                e.state = State::Committing;
            }
        }
        let ticket = CommitTicket {
            sessions: Arc::clone(&self.sessions),
            id: self.id,
            done: false,
        };
        (txn, self.durability, ticket)
    }

    /// Ends the transaction without writing.
    pub(crate) fn abort(mut self) {
        self.ended = true;
        drop(self.txn.take());
        self.sessions.remove(&self.id);
    }
}

impl Drop for TxnGuard {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        // A call cut short after it sent rows elsewhere (#2157): the transaction ends here,
        // uncommitted, and the session records why.
        if let Some((armed, end)) = self.if_dropped.take()
            && armed.load(std::sync::atomic::Ordering::Acquire)
        {
            let outcome = end(self.span.take());
            drop(self.txn.take());
            self.sessions.finish(&self.id, Err(outcome));
            return;
        }
        {
            self.sessions.put_back(
                &self.id,
                self.txn.take(),
                self.wrote,
                self.span.take(),
                std::mem::take(&mut self.watched),
            );
        }
    }
}

/// A running commit's place in the session: it records the outcome as the tombstone. Dropped
/// without one (its task ended early), the outcome is unknown, and the tombstone says so.
pub(crate) struct CommitTicket {
    sessions: Arc<Sessions>,
    id: Id,
    done: bool,
}

impl CommitTicket {
    pub(crate) fn finish(mut self, outcome: Outcome) {
        self.done = true;
        self.sessions.finish(&self.id, outcome);
    }
}

impl Drop for CommitTicket {
    fn drop(&mut self) {
        if !self.done {
            self.sessions.finish(
                &self.id,
                Err(ApiError::outcome_unknown(
                    "the commit's task ended before its outcome",
                )),
            );
        }
    }
}

/// A read at a pinned snapshot: the session stays busy while it's held.
pub(crate) struct SnapshotGuard {
    sessions: Arc<Sessions>,
    id: Id,
    snapshot: Snapshot,
}

impl SnapshotGuard {
    pub(crate) fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// For a stream's next page: `NotFound` once the snapshot was released or is past its
    /// hard lifetime, `--snapshot-lifetime-ms` (pages already running finish; later ones
    /// don't start).
    pub(crate) fn check(&self) -> Result<(), ApiError> {
        if self.sessions.alive(&self.id) {
            Ok(())
        } else {
            Err(ApiError::not_found(
                "no such snapshot (released or expired)",
            ))
        }
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        self.sessions.end_read(&self.id);
    }
}

/// A request on a remote MCP session: the session stays busy while it's held.
pub(crate) struct McpGuard {
    sessions: Arc<Sessions>,
    id: Id,
    session: Arc<super::mcp::McpSession>,
}

impl McpGuard {
    pub(crate) fn session(&self) -> &Arc<super::mcp::McpSession> {
        &self.session
    }
}

impl Drop for McpGuard {
    fn drop(&mut self) {
        self.sessions.end_read(&self.id);
    }
}

/// 128 random bits, as 32 hex characters: an id nobody can guess (a backup download's).
pub(crate) fn random_hex() -> Result<String, ApiError> {
    Ok(hex(&new_id()?))
}

/// 128 random bits from the operating system (rustls's ring provider).
fn new_id() -> Result<Id, ApiError> {
    let mut id = [0u8; 16];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut id)
        .map_err(|_| ApiError::internal("the system's random source failed"))?;
    Ok(id)
}

fn hex(id: &Id) -> String {
    use std::fmt::Write as _;
    id.iter().fold(String::with_capacity(32), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// A cluster session id's partition and the rest (D317 ticket 3). Partition ids never hold
/// `:` (`[a-z0-9_-]`), so the last one splits it.
pub(crate) fn partition_of(id: &str) -> Option<(&str, &str)> {
    id.rsplit_once(':')
}

/// A spanning transaction's id's parts (D329): `<home>:h<history_epoch>-<32 hex>` gives the
/// home, the epoch and the hex.
pub(crate) fn spanning_parts(id: &str) -> Option<(&str, u64, &str)> {
    let (home, rest) = partition_of(id)?;
    let (epoch, hex) = rest.strip_prefix('h')?.split_once('-')?;
    if hex.len() != 32 || epoch.is_empty() || !epoch.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((home, epoch.parse().ok()?, hex))
}

/// When spanning transaction `id` began, in ms since the epoch (its id's first 48 bits).
pub(crate) fn spanning_begun_ms(id: &str) -> Option<u64> {
    let (_, _, hex) = spanning_parts(id)?;
    u64::from_str_radix(hex.get(..12)?, 16).ok()
}

fn parse_id(s: &str) -> Option<Id> {
    if s.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test's principal: token `n`'s digest, as far as the session table can tell.
    fn p(n: u8) -> Principal {
        Principal::from_digest([n; 32])
    }
    use pigeonhole::{Family, Options, Pigeonhole};

    fn leases() -> Leases {
        Leases {
            txn_idle: Duration::from_secs(30),
            txn_lifetime: Duration::from_secs(300),
            snapshot_idle: Duration::from_secs(60),
            snapshot_lifetime: Duration::from_secs(600),
            max_sessions: 4,
            max_per_token: 3,
            mcp_idle: Duration::from_secs(60),
            mcp_lifetime: Duration::from_secs(600),
        }
    }

    fn db(name: &str) -> (Pigeonhole, std::path::PathBuf) {
        let dir = crate::test_dir::fresh(&format!("phdb-sessions-{name}-"));
        let db = Pigeonhole::open(
            dir.join("s.phdb"),
            Options::default().create_if_missing(true).shards(1),
        )
        .unwrap();
        db.table("t")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        (db, dir)
    }

    fn open(taken: Result<Taken, ApiError>) -> TxnGuard {
        match taken {
            Ok(Taken::Open(g)) => g,
            Ok(Taken::Done(_)) => panic!("a tombstone"),
            Err(e) => panic!("{e:?}"),
        }
    }

    /// Begins and commits a transaction for `who` on `s`, recording a committed outcome.
    fn committed(s: &Arc<Sessions>, db: &Pigeonhole, who: Principal) -> String {
        let id = s.begin(who, db.transaction().unwrap(), None).unwrap();
        let (txn, _, ticket) = open(s.take(&id, who)).commit();
        drop(txn);
        ticket.finish(Ok(Default::default()));
        id
    }

    /// #2253: a committed transaction's tombstone gives its slot back, so one token commits
    /// far past its cap (3 here) within one idle lease, and every tombstone still answers its
    /// outcome. The caps still hold what's open: a fourth open transaction is refused.
    #[test]
    fn a_token_commits_past_its_cap_and_each_tombstone_answers() {
        let (db, dir) = db("records");
        let s = Sessions::sharing(leases(), None, Arc::default());
        let ids: Vec<String> = (0..40).map(|_| committed(&s, &db, p(1))).collect();
        for id in &ids {
            assert!(
                matches!(s.take(id, p(1)), Ok(Taken::Done(Ok(_)))),
                "the outcome of {id}"
            );
        }
        let held: Vec<String> = (0..3)
            .map(|_| s.begin(p(1), db.transaction().unwrap(), None).unwrap())
            .collect();
        let e = s.begin(p(1), db.transaction().unwrap(), None).unwrap_err();
        assert_eq!(e.code, "Overloaded", "{e:?}");
        assert_eq!(s.stats(Instant::now()).tombstones, 40);
        assert_eq!(s.records_dropped(), 0);
        drop(held);
        drop(s);
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #2253: past the node's record bound (4 here), a table drops its own oldest record
    /// first: its id then answers `NotFound`, the newer ones their outcomes, and the drop is
    /// counted. Shared caps bound records across a node's tables.
    #[test]
    fn past_the_record_bound_the_oldest_tombstone_goes_first() {
        let (db, dir) = db("bound");
        let caps = Arc::new(Caps::with_max_records(4));
        let s = Sessions::sharing(leases(), None, Arc::clone(&caps));
        let ids: Vec<String> = (0..6).map(|_| committed(&s, &db, p(1))).collect();
        for id in &ids[..2] {
            let e = s.take(id, p(1)).err().expect("dropped for the bound");
            assert_eq!(e.code, "NotFound", "{id}: {e:?}");
        }
        for id in &ids[2..] {
            assert!(
                matches!(s.take(id, p(1)), Ok(Taken::Done(Ok(_)))),
                "the outcome of {id}"
            );
        }
        assert_eq!(s.records_dropped(), 2);
        assert_eq!(s.stats(Instant::now()).tombstones, 4);
        drop(s);
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// cli692's N2 on #2257: two tables sharing a node's caps (a cluster node's partitions).
    /// Past the bound (2 here), the table committing drops its own oldest, never the other
    /// table's records and never its newest: a node holds at most the bound plus one per
    /// table.
    #[test]
    fn tables_sharing_caps_each_drop_only_their_own_records() {
        let (db, dir) = db("shared");
        let caps = Arc::new(Caps::with_max_records(2));
        let a = Sessions::sharing(leases(), Some("pa".into()), Arc::clone(&caps));
        let b = Sessions::sharing(leases(), Some("pb".into()), Arc::clone(&caps));
        let a_ids: Vec<String> = (0..2).map(|_| committed(&a, &db, p(1))).collect();
        // b's first: over the bound, but it's b's newest, so it stays.
        let b1 = committed(&b, &db, p(1));
        // b's second: b drops its own oldest (b1), not a's.
        let b2 = committed(&b, &db, p(1));
        for id in &a_ids {
            assert!(
                matches!(a.take(id, p(1)), Ok(Taken::Done(Ok(_)))),
                "a's record {id} kept"
            );
        }
        assert_eq!(
            b.take(&b1, p(1)).err().map(|e| e.code),
            Some("NotFound".into())
        );
        assert!(matches!(b.take(&b2, p(1)), Ok(Taken::Done(Ok(_)))));
        let now = Instant::now();
        assert_eq!(
            a.stats(now).tombstones + b.stats(now).tombstones,
            3,
            "bound + 1"
        );
        assert_eq!(caps.records_dropped(), 1);
        drop((a, b));
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #2253: an expired transaction's marker gives its slot back too: the token opens as
    /// many again at once, while the marker still answers `TransactionExpired`.
    #[test]
    fn an_expired_transactions_marker_holds_no_slot() {
        let (db, dir) = db("marker");
        let mut l = leases();
        l.txn_idle = Duration::from_millis(1);
        let s = Sessions::sharing(l, None, Arc::default());
        let gone: Vec<String> = (0..3)
            .map(|_| s.begin(p(1), db.transaction().unwrap(), None).unwrap())
            .collect();
        std::thread::sleep(Duration::from_millis(20));
        // Each call checks expiry first: the transaction becomes its marker.
        for id in &gone {
            let e = s.take(id, p(1)).err().expect("expired");
            assert_eq!(e.code, "TransactionExpired", "{e:?}");
        }
        let again: Vec<String> = (0..3)
            .map(|_| s.begin(p(1), db.transaction().unwrap(), None).unwrap())
            .collect();
        assert_eq!(again.len(), 3);
        drop(s);
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_second_call_on_a_transaction_is_busy_until_the_first_ends() {
        let (db, dir) = db("busy");
        let s = Sessions::sharing(leases(), None, Arc::default());
        let id = s.begin(p(1), db.transaction().unwrap(), None).unwrap();
        let first = open(s.take(&id, p(1)));
        let Err(e) = s.take(&id, p(1)) else {
            panic!("a second call got the transaction")
        };
        assert_eq!(e.code, "SessionBusy");
        // Dropping the call's guard (its future dropped) puts the transaction back.
        drop(first);
        let again = open(s.take(&id, p(1)));
        // And a commit makes every call busy until its outcome is recorded.
        let (txn, _, ticket) = again.commit();
        assert_eq!(s.take(&id, p(1)).err().unwrap().code, "SessionBusy");
        drop(txn);
        drop(ticket);
        let Ok(Taken::Done(Err(e))) = s.take(&id, p(1)) else {
            panic!("the tombstone")
        };
        assert_eq!(
            e.code, "OutcomeUnknown",
            "a ticket dropped without an outcome"
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ids_are_bound_to_their_token() {
        let (db, dir) = db("token");
        let s = Sessions::sharing(leases(), None, Arc::default());
        let id = s.begin(p(1), db.transaction().unwrap(), None).unwrap();
        assert_eq!(id.len(), 32);
        assert_eq!(s.take(&id, p(2)).err().unwrap().code, "NotFound");
        assert_eq!(s.take("not an id", p(1)).err().unwrap().code, "NotFound");
        let snap = s.open_snapshot(p(1), db.snapshot().unwrap()).unwrap();
        assert_eq!(s.snapshot(&snap, p(2)).err().unwrap().code, "NotFound");
        assert_eq!(
            s.take(&snap, p(1)).err().unwrap().code,
            "NotFound",
            "not a txn"
        );
        assert_eq!(s.release(&snap, p(2)).err().unwrap().code, "NotFound");
        drop(open(s.take(&id, p(1))));
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn caps_count_every_session_per_token_and_in_total() {
        let (db, dir) = db("caps");
        let s = Sessions::sharing(leases(), None, Arc::default());
        for _ in 0..3 {
            s.begin(p(1), db.transaction().unwrap(), None).unwrap();
        }
        let e = s.begin(p(1), db.transaction().unwrap(), None).unwrap_err();
        assert!(e.message.contains("per-token"), "{e:?}");
        s.open_snapshot(p(2), db.snapshot().unwrap()).unwrap();
        let e = s.open_snapshot(p(3), db.snapshot().unwrap()).unwrap_err();
        assert!(e.message.contains("--max-sessions)"), "{e:?}");
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// D317 ticket 8a: a node's partitions' tables count against the node's caps together,
    /// and a session ended in one frees a place for another's.
    #[test]
    fn tables_sharing_caps_count_together() {
        let (db, dir) = db("shared-caps");
        let caps = Arc::new(Caps::default());
        let p1 = Sessions::sharing(leases(), Some("p1".into()), Arc::clone(&caps));
        let p3 = Sessions::sharing(leases(), Some("p3".into()), Arc::clone(&caps));
        // Token 1's cap is 3, across both partitions.
        let a = p1.begin(p(1), db.transaction().unwrap(), None).unwrap();
        p1.begin(p(1), db.transaction().unwrap(), None).unwrap();
        p3.begin(p(1), db.transaction().unwrap(), None).unwrap();
        let e = p3.begin(p(1), db.transaction().unwrap(), None).unwrap_err();
        assert!(e.message.contains("per-token"), "{e:?}");
        // The node's total is 4, across both partitions.
        p3.open_snapshot(p(2), db.snapshot().unwrap()).unwrap();
        let e = p1.open_snapshot(p(3), db.snapshot().unwrap()).unwrap_err();
        assert!(e.message.contains("--max-sessions)"), "{e:?}");
        // An abort in p1 frees a place, which p3 takes.
        open(p1.take(&a, p(1))).abort();
        p3.begin(p(1), db.transaction().unwrap(), None).unwrap();
        // Ids stay their own partition's.
        assert_eq!(p3.take(&a, p(1)).err().unwrap().code, "NotFound");
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_reaper_skips_a_session_with_a_call_in_flight() {
        let (db, dir) = db("reaper");
        let s = Sessions::sharing(leases(), None, Arc::default());
        let id = s.begin(p(1), db.transaction().unwrap(), None).unwrap();
        let call = open(s.take(&id, p(1)));
        // The call outlasts the idle lease and the hard lifetime: the reaper leaves it.
        let late = Instant::now() + Duration::from_secs(400);
        assert_eq!(s.sweep(late), 0);
        drop(call);
        // Renewed at the call's end, it expires once its lease lapses again, into a marker
        // that answers TransactionExpired (no Commit was received).
        assert_eq!(s.sweep(Instant::now()), 0, "just renewed");
        assert_eq!(s.sweep(Instant::now() + Duration::from_secs(31)), 1);
        assert_eq!(s.take(&id, p(1)).err().unwrap().code, "TransactionExpired");
        // The marker expires after one more idle lease: then the id is unknown.
        assert_eq!(s.sweep(Instant::now() + Duration::from_secs(62)), 1);
        assert_eq!(s.take(&id, p(1)).err().unwrap().code, "NotFound");

        // A read at a snapshot holds it the same way.
        let snap = s.open_snapshot(p(1), db.snapshot().unwrap()).unwrap();
        let read = s.snapshot(&snap, p(1)).unwrap();
        assert_eq!(s.sweep(Instant::now() + Duration::from_secs(700)), 0);
        drop(read);
        assert_eq!(s.sweep(Instant::now() + Duration::from_secs(61)), 1);
        assert_eq!(s.snapshot(&snap, p(1)).err().unwrap().code, "NotFound");
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_spanning_id_finds_its_home_session_and_its_branches_by_its_exact_text() {
        // D329: `<home>:h<epoch>-<32 hex>` is the home's session on the home's table and a
        // branch on another partition's, found only by that exact text and never by the
        // plain form of its key, nor by another home's prefix on the same key.
        let (db, dir) = db("spanning");
        let caps: Arc<Caps> = Arc::default();
        let home = Sessions::sharing(leases(), Some("p2".into()), Arc::clone(&caps));
        let branch = Sessions::sharing(leases(), Some("p1".into()), caps);
        let xid = home
            .begin_spanning(p(1), db.transaction().unwrap(), None, 7)
            .unwrap();
        let (h, epoch, hex) = spanning_parts(&xid).unwrap();
        assert_eq!((h, epoch, hex.len()), ("p2", 7, 32));
        assert!(matches!(home.take(&xid, p(1)), Ok(Taken::Open(_))));
        // Not by its plain form, nor another principal.
        assert_eq!(
            home.take(&format!("p2:{hex}"), p(1)).err().unwrap().code,
            "NotFound"
        );
        assert_eq!(home.take(&xid, p(2)).err().unwrap().code, "NotFound");
        // A branch: opened, renewed by a second begin, taken by the home's own call.
        branch
            .begin_branch(p(1), || Ok(db.transaction().unwrap()), &xid, false)
            .unwrap();
        branch
            .begin_branch(p(1), || panic!("not opened twice"), &xid, true)
            .unwrap();
        // Opened again (the home thinks it new), it's refused: never two snapshots.
        assert_eq!(
            branch
                .begin_branch(p(1), || panic!("not reopened"), &xid, false)
                .unwrap_err()
                .code,
            "NotFound"
        );
        assert!(matches!(branch.take(&xid, p(1)), Ok(Taken::Open(_))));
        assert!(matches!(branch.take_branch(&xid), Ok(Taken::Open(_))));
        // Another home's prefix on the same key is another transaction: not found.
        let forged = xid.replacen("p2:", "p3:", 1);
        assert_eq!(branch.take(&forged, p(1)).err().unwrap().code, "NotFound");
        assert_eq!(branch.take_branch(&forged).err().unwrap().code, "NotFound");
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_tombstone_never_answers_transaction_expired() {
        let (db, dir) = db("tomb");
        let s = Sessions::sharing(leases(), None, Arc::default());
        let id = s.begin(p(1), db.transaction().unwrap(), None).unwrap();
        let (txn, _, ticket) = open(s.take(&id, p(1))).commit();
        let info = txn.commit().unwrap();
        ticket.finish(Ok(super::super::proto::TxnCommitResponse {
            commit: Some(super::super::convert::commit_info(info)),
            parts: Vec::new(),
        }));
        // Past its idle lease the tombstone goes, straight to NotFound: never
        // TransactionExpired, which would read as "not committed".
        assert!(matches!(s.take(&id, p(1)), Ok(Taken::Done(Ok(c)))
            if c.commit.as_ref().is_some_and(|c| c.seqno == info.seqno)));
        assert_eq!(s.sweep(Instant::now() + Duration::from_secs(31)), 1);
        assert_eq!(s.take(&id, p(1)).err().unwrap().code, "NotFound");
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_spanning_session_is_foreign_to_another_principal_and_any_to_the_nodes_own_call() {
        // #2077 (audit120's S5 on #2076): a client sees only its own spanning transaction;
        // another principal's is `Foreign` (so its `Outcome` stops at NotFound), while the
        // node's own call (`None`, the resolver) finds it whoever its client is. Live, and
        // as a lost commit's tombstone (OutcomeUnknown), which settles from the decision row
        // for its own client only. A plain id of another principal stays `Gone`.
        let (db, dir) = db("peek");
        let s = Sessions::sharing(leases(), Some("p2".into()), Arc::default());
        let xid = s
            .begin_spanning(p(1), db.transaction().unwrap(), None, 7)
            .unwrap();
        assert!(matches!(s.peek(&xid, Some(p(1))), Peek::Live));
        assert!(matches!(s.peek(&xid, None), Peek::Live));
        assert!(matches!(s.peek(&xid, Some(p(2))), Peek::Foreign));
        let (txn, _, ticket) = open(s.take(&xid, p(1))).commit();
        drop(txn);
        ticket.finish(Err(ApiError::outcome_unknown(
            "the commit point's answer was lost",
        )));
        let unknown = |k: Peek| matches!(k, Peek::Done(Err(e)) if e.code == "OutcomeUnknown");
        assert!(unknown(s.peek(&xid, Some(p(1)))));
        assert!(unknown(s.peek(&xid, None)));
        assert!(matches!(s.peek(&xid, Some(p(2))), Peek::Foreign));
        let plain = s.begin(p(1), db.transaction().unwrap(), None).unwrap();
        assert!(matches!(s.peek(&plain, Some(p(1))), Peek::Live));
        assert!(matches!(s.peek(&plain, Some(p(2))), Peek::Gone));
        assert!(matches!(s.peek(&plain, None), Peek::Gone));
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_stream_at_a_pinned_snapshot_ends_at_its_hard_lifetime() {
        // audit120's S-S3: a stream's read kept the session busy, and a busy session never
        // lapsed, so a scan paged slowly held its snapshot past --snapshot-lifetime-ms.
        let (db, dir) = db("lifetime");
        let mut l = leases();
        l.snapshot_lifetime = Duration::from_millis(100);
        let s = Sessions::sharing(l, None, Arc::default());
        let snap = s.open_snapshot(p(1), db.snapshot().unwrap()).unwrap();
        let read = s.snapshot(&snap, p(1)).unwrap();
        // A page every 30 ms, with the reaper running: well inside the idle lease.
        let started = Instant::now();
        while read.check().is_ok() {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the stream never ended"
            );
            std::thread::sleep(Duration::from_millis(30));
            s.sweep(Instant::now());
        }
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert_eq!(read.check().unwrap_err().code, "NotFound");
        assert_eq!(
            s.stats(Instant::now()).snapshots,
            1,
            "held while the stream runs"
        );
        drop(read);
        assert_eq!(
            s.stats(Instant::now()).snapshots,
            0,
            "gone once the stream ends"
        );
        assert_eq!(s.snapshot(&snap, p(1)).err().unwrap().code, "NotFound");
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_released_snapshot_finishes_its_running_reads_only() {
        let (db, dir) = db("release");
        let s = Sessions::sharing(leases(), None, Arc::default());
        let snap = s.open_snapshot(p(1), db.snapshot().unwrap()).unwrap();
        let read = s.snapshot(&snap, p(1)).unwrap();
        s.release(&snap, p(1)).unwrap();
        assert_eq!(
            read.check().unwrap_err().code,
            "NotFound",
            "a stream's next page"
        );
        let _ = read.snapshot().seqno();
        drop(read);
        assert_eq!(s.snapshot(&snap, p(1)).err().unwrap().code, "NotFound");
        assert!(parse_id(&hex(&[0xab; 16])).is_some_and(|id| id == [0xab; 16]));
        // A cluster node's ids name its partition (D317 ticket 3); a single node's don't, and
        // neither takes the other's form or another partition's id (cli692's S4).
        let one = Sessions::sharing(leases(), None, Arc::default());
        let p2 = Sessions::sharing(leases(), Some("p2".into()), Arc::default());
        let h = hex(&[0xab; 16]);
        assert_eq!(p2.text(&[0xab; 16]), format!("p2:{h}"));
        assert_eq!(p2.parse(&format!("p2:{h}")), Some([0xab; 16]));
        assert_eq!(one.parse(&h), Some([0xab; 16]));
        for bad in [
            h.clone(),
            format!("p1:{h}"),
            format!("p2:{h}0"),
            format!("p2:p2:{h}"),
        ] {
            assert_eq!(p2.parse(&bad), None, "{bad}");
        }
        assert_eq!(one.parse(&format!("p2:{h}")), None);
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
