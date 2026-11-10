//! The session table (D313 §3): transactions and pinned snapshots held on the server between
//! requests, each under an unguessable id bound to the token that opened it.
//!
//! - **One call at a time on a transaction** (B2): a call takes the `Transaction` out of its
//!   entry under a short lock, never held across an `.await`, and a guard puts it back when
//!   the call ends, even if the call's future is dropped. A second call meanwhile gets
//!   `SessionBusy`, never a queue. Snapshot reads take `&Snapshot`, so they run in parallel.
//! - **Leases:** each session has an idle lease, renewed at a call's start and end, and a hard
//!   lifetime. A session with a call in flight never expires; it does once the call ends.
//!   Expiry is checked on every call as well as by the reaper, so a lease is exact whatever
//!   the reaper's period.
//! - **The tombstone** (B1): once `Commit` has run, the session holds its outcome, and a
//!   repeated `Commit` answers it until the tombstone expires. A transaction that expires
//!   before any `Commit` leaves a marker for one idle lease, which answers
//!   `TransactionExpired`; after that, the id is `NotFound`.
//! - **Caps:** sessions in total and per token, tombstones and markers counted in.
//! - **Remote MCP sessions** (`Mcp-Session-Id`, D313 §7 and ai1249's M1) live here too, with
//!   leases of their own: bound to their token, never expired with a request in flight
//!   (any number at once, unlike a transaction; the session caps its own), and counted in
//!   the caps.

use super::auth::Principal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Once, PoisonError, Weak};
use std::time::{Duration, Instant};

use pigeonhole::{CommitInfo, Durability, Snapshot, Transaction};

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

/// A recorded commit outcome.
pub(crate) type Outcome = Result<CommitInfo, ApiError>;

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
    token: Principal,
    state: State,
    /// Calls in flight: at most one on a transaction.
    busy: usize,
    /// The transaction's durability, for its commit.
    durability: Option<Durability>,
    renewed: Instant,
    idle: Duration,
    hard: Instant,
    opened: Instant,
}

impl Entry {
    fn renew(&mut self, now: Instant) {
        self.renewed = now;
    }

    fn lapsed(&self, now: Instant) -> bool {
        self.busy == 0 && (now >= self.hard || now.duration_since(self.renewed) >= self.idle)
    }
}

#[derive(Default)]
struct Table {
    map: HashMap<Id, Entry>,
    per_token: HashMap<Principal, usize>,
}

impl Table {
    fn remove(&mut self, id: &Id) -> Option<Entry> {
        let e = self.map.remove(id)?;
        if let Some(n) = self.per_token.get_mut(&e.token) {
            *n -= 1;
            if *n == 0 {
                self.per_token.remove(&e.token);
            }
        }
        Some(e)
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
}

impl std::fmt::Debug for Sessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sessions")
            .field("leases", &self.leases)
            .finish_non_exhaustive()
    }
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
    pub(crate) fn new(leases: Leases) -> Arc<Self> {
        Arc::new(Self {
            table: Mutex::new(Table::default()),
            leases,
            reaper: Once::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Table> {
        // A panic while the lock was held left the table consistent: every change under it
        // is a single insert, remove or field store.
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a session for `token`, within the caps.
    fn insert(
        self: &Arc<Self>,
        token: Principal,
        state: State,
        durability: Option<Durability>,
        idle: Duration,
        lifetime: Duration,
    ) -> Result<String, ApiError> {
        self.start_reaper();
        let id = new_id()?;
        let now = Instant::now();
        let mut t = self.lock();
        if t.map.len() >= self.leases.max_sessions {
            return Err(ApiError::overloaded(
                "the server is at its session limit (--max-sessions)",
            ));
        }
        let mine = t.per_token.get(&token).copied().unwrap_or(0);
        if mine >= self.leases.max_per_token {
            return Err(ApiError::overloaded(
                "this token is at its session limit (--max-sessions-per-token)",
            ));
        }
        *t.per_token.entry(token).or_insert(0) += 1;
        t.map.insert(
            id,
            Entry {
                token,
                state,
                busy: 0,
                durability,
                renewed: now,
                idle,
                hard: now + lifetime,
                opened: now,
            },
        );
        Ok(hex(&id))
    }

    /// Holds `txn` for `token` and returns its id.
    pub(crate) fn begin(
        self: &Arc<Self>,
        token: Principal,
        txn: Transaction,
        durability: Option<Durability>,
    ) -> Result<String, ApiError> {
        self.insert(
            token,
            State::Txn(Some(txn)),
            durability,
            self.leases.txn_idle,
            self.leases.txn_lifetime,
        )
    }

    /// Pins `snapshot` for `token` and returns its id.
    pub(crate) fn open_snapshot(
        self: &Arc<Self>,
        token: Principal,
        snapshot: Snapshot,
    ) -> Result<String, ApiError> {
        self.insert(
            token,
            State::Snapshot(snapshot),
            None,
            self.leases.snapshot_idle,
            self.leases.snapshot_lifetime,
        )
    }

    /// Takes transaction `id` out for one call by `token`.
    pub(crate) fn take(self: &Arc<Self>, id: &str, token: Principal) -> Result<Taken, ApiError> {
        let gone = || ApiError::not_found("no such transaction");
        let key = parse_id(id).ok_or_else(gone)?;
        let now = Instant::now();
        let mut t = self.lock();
        t.expire(&key, now, &self.leases);
        let e = t
            .map
            .get_mut(&key)
            .filter(|e| e.token == token)
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
                    ended: false,
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

    /// Holds a remote MCP session for `token` and returns its id.
    pub(crate) fn open_mcp(
        self: &Arc<Self>,
        token: Principal,
        session: Arc<super::mcp::McpSession>,
    ) -> Result<String, ApiError> {
        self.insert(
            token,
            State::Mcp(session),
            None,
            self.leases.mcp_idle,
            self.leases.mcp_lifetime,
        )
    }

    /// MCP session `id`, for one request by `token`: the request holds it busy (so it doesn't
    /// expire under the request) until the guard drops. Another token's id, a malformed one
    /// and an expired one are all `NotFound`, which `/mcp` answers 404: the client starts a
    /// new session.
    pub(crate) fn mcp(self: &Arc<Self>, id: &str, token: Principal) -> Result<McpGuard, ApiError> {
        let gone = || ApiError::not_found("no such MCP session (ended or expired)");
        let key = parse_id(id).ok_or_else(gone)?;
        let now = Instant::now();
        let mut t = self.lock();
        t.expire(&key, now, &self.leases);
        let e = t
            .map
            .get_mut(&key)
            .filter(|e| e.token == token)
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
    pub(crate) fn end_mcp(&self, id: &str, token: Principal) -> Result<(), ApiError> {
        let gone = || ApiError::not_found("no such MCP session (ended or expired)");
        let key = parse_id(id).ok_or_else(gone)?;
        let mut t = self.lock();
        match t.map.get(&key) {
            Some(e) if e.token == token && matches!(e.state, State::Mcp(_)) => {
                t.remove(&key);
                Ok(())
            }
            _ => Err(gone()),
        }
    }

    /// Snapshot `id`, for one read by `token`: the read holds the session busy (so it doesn't
    /// expire under the read) until the guard drops.
    pub(crate) fn snapshot(
        self: &Arc<Self>,
        id: &str,
        token: Principal,
    ) -> Result<SnapshotGuard, ApiError> {
        let gone = || ApiError::not_found("no such snapshot (released or expired)");
        let key = parse_id(id).ok_or_else(gone)?;
        let now = Instant::now();
        let mut t = self.lock();
        t.expire(&key, now, &self.leases);
        let e = t
            .map
            .get_mut(&key)
            .filter(|e| e.token == token)
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
    pub(crate) fn release(&self, id: &str, token: Principal) -> Result<(), ApiError> {
        let gone = || ApiError::not_found("no such snapshot (released or expired)");
        let key = parse_id(id).ok_or_else(gone)?;
        let mut t = self.lock();
        match t.map.get(&key) {
            Some(e) if e.token == token && matches!(e.state, State::Snapshot(_)) => {
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
    fn put_back(&self, id: &Id, txn: Option<Transaction>) {
        let now = Instant::now();
        let mut t = self.lock();
        if let Some(e) = t.map.get_mut(id) {
            e.busy = 0;
            e.renew(now);
            e.state = match txn {
                Some(txn) => State::Txn(Some(txn)),
                // A call that panicked with the transaction lost it before any Commit: nothing
                // was written.
                None => State::Expired,
            };
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
        }
    }

    fn remove(&self, id: &Id) {
        self.lock().remove(id);
    }

    fn end_read(&self, id: &Id) {
        let now = Instant::now();
        let mut t = self.lock();
        if let Some(e) = t.map.get_mut(id) {
            e.busy = e.busy.saturating_sub(1);
            e.renew(now);
        }
    }

    fn alive(&self, id: &Id) -> bool {
        self.lock().map.contains_key(id)
    }
}

/// A transaction out of its entry for one call. Dropped, it puts the transaction back (a
/// call whose future was dropped, a client gone, leaves the session as it found it).
pub(crate) struct TxnGuard {
    sessions: Arc<Sessions>,
    id: Id,
    txn: Option<Transaction>,
    durability: Option<Durability>,
    ended: bool,
}

impl TxnGuard {
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
        if !self.ended {
            self.sessions.put_back(&self.id, self.txn.take());
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

    /// For a stream's next page: `NotFound` once the snapshot was released (pages already
    /// running finish; later ones don't start).
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
        [n; 32]
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
        let dir = std::env::temp_dir().join(format!("phdb-sessions-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
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

    #[test]
    fn a_second_call_on_a_transaction_is_busy_until_the_first_ends() {
        let (db, dir) = db("busy");
        let s = Sessions::new(leases());
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
        let s = Sessions::new(leases());
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
        let s = Sessions::new(leases());
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

    #[test]
    fn the_reaper_skips_a_session_with_a_call_in_flight() {
        let (db, dir) = db("reaper");
        let s = Sessions::new(leases());
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
    fn a_tombstone_never_answers_transaction_expired() {
        let (db, dir) = db("tomb");
        let s = Sessions::new(leases());
        let id = s.begin(p(1), db.transaction().unwrap(), None).unwrap();
        let (txn, _, ticket) = open(s.take(&id, p(1))).commit();
        let info = txn.commit().unwrap();
        ticket.finish(Ok(info));
        // Past its idle lease the tombstone goes, straight to NotFound: never
        // TransactionExpired, which would read as "not committed".
        assert!(matches!(s.take(&id, p(1)), Ok(Taken::Done(Ok(c))) if c.seqno == info.seqno));
        assert_eq!(s.sweep(Instant::now() + Duration::from_secs(31)), 1);
        assert_eq!(s.take(&id, p(1)).err().unwrap().code, "NotFound");
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_released_snapshot_finishes_its_running_reads_only() {
        let (db, dir) = db("release");
        let s = Sessions::new(leases());
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
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
