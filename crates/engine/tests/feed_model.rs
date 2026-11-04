//! The change feed's model check (D310 PR 3a; rev 3 §5): seeded runs of single-shard and
//! cross-shard commits at every durability, flushes, clean reopens and power losses, with a
//! durable and a visible subscriber that resume from tokens. Checked at every poll:
//!
//! - every delivered commit is one the run made, whole (every mutation of it, nothing else),
//!   at its seqno, in seqno order, once per delivery since the token it resumed from;
//! - nothing is skipped: every surviving commit between two deliveries, and at or below the
//!   frontier, was delivered (M1 at every frontier);
//! - a durable subscriber never delivers a commit a power loss then undoes, and resumes
//!   across every reopen and power loss without expiring (retention is never passed here);
//! - a visible one may expire after a power loss, and then starts over.
//!
//! The run is single-threaded (application-owned shards driven by hand), so a seed replays.
//!
//! A limit: on the simulator's clock the feed's background sync runs at the next kick (its
//! timer gives up on a clock that doesn't move, PR 2), so a commit is rarely visible,
//! written and still unsynced when the power goes. A durable subscriber following the
//! visible frontier by mistake isn't caught here; the engine's
//! `nothing_the_durable_frontier_passed_is_lost_to_a_power_loss` checks the frontier itself.
//! `PIGEONHOLE_SEED` and `PIGEONHOLE_SEEDS` pick the seeds (the sweep's `-f test=feed_model`).

mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Commit, Engine, EngineOptions, EngineShard, FamilyOptions, FeedOptions, FeedReader, FeedSpec,
    FeedStart, Next, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, Seqno, TableId};
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::{CrashKind, SimVfs};

const DB: &str = "/db/data.phdb";
/// The writer's process, and a reader process whose subscribers follow the writer's feed
/// through shared memory (D310 PR 4).
const WRITER: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};
const READER: ProcessId = ProcessId {
    pid: 2,
    start_time: 1,
};

fn seeds() -> Vec<u64> {
    let n: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let base: u64 = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    (base..base + n).collect()
}

/// A small deterministic generator (the run's own choices; the VFS has its own).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn options(vfs: &Arc<SimVfs>, shards: usize) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), shards, 16 << 20);
    let mut f = FeedOptions::default();
    // Far more than a run writes: nothing is ever recycled under a reader here.
    f.retention_bytes = 64 << 20;
    o.feed = Some(f);
    o
}

thread_local! {
    /// The run's VFS while deferred I/O is on (`PIGEONHOLE_DEFERRED_IO=1`): the shard loops
    /// below complete one deferred operation, chosen by the seed, per turn, as a device
    /// finishes I/O on its own. Syncs and rollovers then land between the feed's polls, and a
    /// power loss finds more in flight.
    static DEFERRED: std::cell::RefCell<Option<Arc<SimVfs>>> = const { std::cell::RefCell::new(None) };
}

/// Completes one deferred operation if deferred I/O is on; whether one completed.
fn complete_one() -> bool {
    DEFERRED.with(|d| d.borrow().as_ref().is_some_and(|v| v.complete_io()))
}

fn drive<F: Future + Unpin>(shards: &mut [EngineShard], mut pending: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
            return r;
        }
        complete_one();
    }
    panic!("never resolved");
}

fn settle(shards: &mut [EngineShard]) {
    for _ in 0..100_000 {
        let mut more = false;
        for s in shards.iter_mut() {
            more |= s.run_once(u64::MAX);
        }
        more |= complete_one();
        if !more {
            return;
        }
    }
    panic!("the shards never settled");
}

/// One commit the run made: its mutations as `(table, row)`, and its seqno once answered.
#[derive(Debug, Clone)]
struct Made {
    rows: Vec<(TableId, Vec<u8>)>,
    seqno: Seqno,
    durability: Durability,
}

/// A subscriber and what it was delivered since its token.
struct Sub {
    durable: bool,
    /// In the reader process (D310 PR 4), not the writer's.
    process: bool,
    /// Keeps no token: subscribes `FeedStart::Oldest` each time, after every reopen too
    /// (#1465: from the oldest earlier session still kept, its first single's seqno less one).
    oldest: bool,
    reader: Option<FeedReader>,
    token: Option<Vec<u8>>,
    /// (seqno, tag) delivered, in order.
    delivered: Vec<(Seqno, u32)>,
    /// The seqno its stream of deliveries is past (its start, or its token's).
    last: Seqno,
}

struct Run {
    seed: u64,
    vfs: Arc<SimVfs>,
    shards_n: usize,
    tables: [Arc<TableInfo>; 2],
    db: Option<Arc<Engine>>,
    /// The reader process's engine (`Engine::open_reader`), while one runs.
    reader_db: Option<Arc<Engine>>,
    shards: Vec<EngineShard>,
    /// Every commit that survives (or may), by tag.
    made: BTreeMap<u32, Made>,
    subs: Vec<Sub>,
    /// Durable subscribers expired by a power loss inside fix A's window.
    window_expiries: u32,
    /// Commits a power loss undid.
    lost_total: u32,
}

fn tag_of(c: &Commit, seed: u64) -> u32 {
    let tags: HashSet<u32> = c
        .changes
        .iter()
        .map(|ch| {
            std::str::from_utf8(&ch.row)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| panic!("seed {seed}: a row the run never wrote: {c:?}"))
        })
        .collect();
    assert_eq!(
        tags.len(),
        1,
        "seed {seed}: a commit mixing two commits: {c:?}"
    );
    tags.into_iter().next().unwrap()
}

impl Run {
    fn db(&self) -> &Arc<Engine> {
        self.db.as_ref().expect("open")
    }

    fn open(&mut self) {
        let (db, shards) =
            Engine::open_application_owned(Path::new(DB), options(&self.vfs, self.shards_n))
                .unwrap_or_else(|e| panic!("seed {}: reopen: {e}", self.seed));
        self.db = Some(db);
        self.shards = shards;
        settle(&mut self.shards);
    }

    /// Opens the reader process's engine on the running writer.
    fn open_reader(&mut self) {
        self.vfs.enter_process(READER);
        let r = Engine::open_reader(Path::new(DB), options(&self.vfs, self.shards_n))
            .unwrap_or_else(|e| panic!("seed {}: reader open: {e}", self.seed));
        self.vfs.enter_process(WRITER);
        self.reader_db = Some(r);
    }

    /// Subscribes with `spec` in subscriber `i`'s process.
    fn feed_for(&self, i: usize, spec: FeedSpec) -> FeedReader {
        let process = self.subs[i].process;
        let db = if process {
            self.vfs.enter_process(READER);
            self.reader_db.as_ref().expect("a reader process")
        } else {
            self.db()
        };
        let r = db.feed(spec);
        self.vfs.enter_process(WRITER);
        r.unwrap_or_else(|e| panic!("seed {}: subscribe (process {process}): {e}", self.seed))
    }

    fn spec(&self, durable: bool, start: FeedStart) -> FeedSpec {
        FeedSpec {
            tables: self.tables.iter().map(|t| t.id).collect(),
            prefix: Vec::new(),
            durable,
            start,
        }
    }

    /// (Re)subscribes `i`: from its token if it has one, else from now.
    fn subscribe(&mut self, i: usize) {
        let start = match &self.subs[i].token {
            Some(t) => FeedStart::Token(t.clone()),
            None if self.subs[i].oldest => FeedStart::Oldest,
            None => FeedStart::Now,
        };
        let spec = self.spec(self.subs[i].durable, start);
        let r = self.feed_for(i, spec);
        let sub = &mut self.subs[i];
        if sub.token.is_none() {
            sub.last = r.delivered();
            sub.delivered.clear();
        } else {
            // Deliveries after the token come again (at least once, whole commits).
            let last = r.delivered();
            sub.delivered.retain(|&(s, _)| s <= last);
            sub.last = last;
        }
        sub.reader = Some(r);
    }

    fn commit(&mut self, tag: u32, cross: bool, durability: Durability) {
        let pc = self.submit(tag, cross, durability);
        let info = drive(&mut self.shards, pc)
            .unwrap_or_else(|e| panic!("seed {}: commit {tag}: {e}", self.seed));
        self.made.get_mut(&tag).unwrap().seqno = info.seqno;
    }

    /// Submits commit `tag` and records it, with no seqno until it's answered.
    fn submit(
        &mut self,
        tag: u32,
        cross: bool,
        durability: Durability,
    ) -> pigeonhole_engine::PendingCommit {
        let mut wb = WriteBatch::new();
        let row = tag.to_string().into_bytes();
        let mut rows = Vec::new();
        let n = if cross { 2 } else { 1 };
        for t in &self.tables[(tag as usize) % (3 - n)..][..n] {
            wb.put(
                t.id,
                t.families[0].id,
                &row,
                b"q",
                None,
                ValueRef::Bytes(&row),
            )
            .unwrap();
            rows.push((t.id, row.clone()));
        }
        let pc = self.db().submit(wb, Some(durability)).unwrap();
        self.made.insert(
            tag,
            Made {
                rows,
                seqno: 0,
                durability,
            },
        );
        pc
    }

    /// A burst of `None` and `Buffered` commits, a few shard turns (the visible subscriber
    /// reading meanwhile, with no settle, so nothing syncs them), then a power loss.
    fn burst_then_crash(&mut self, rng: &mut Rng, first_tag: u32) {
        let n = 1 + rng.below(6) as u32;
        let mut pending = Vec::new();
        for tag in first_tag..first_tag + n {
            let durability = if rng.below(2) == 0 {
                Durability::None
            } else {
                Durability::Buffered
            };
            let cross = self.shards_n > 1 && rng.below(3) == 0;
            pending.push((tag, Box::pin(self.submit(tag, cross, durability))));
        }
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..rng.below(24) {
            for s in self.shards.iter_mut() {
                s.run_once(u64::MAX);
            }
            for (tag, pc) in &mut pending {
                if let Poll::Ready(Ok(info)) = pc.as_mut().poll(&mut cx) {
                    self.made.get_mut(tag).unwrap().seqno = info.seqno;
                }
            }
        }
        // Both subscribers read what they can now, without the shards running (no settle,
        // so no sync): a durable one must take nothing a power loss can undo.
        let seed = self.seed;
        let vfs = Arc::clone(&self.vfs);
        for sub in &mut self.subs {
            let Some(r) = sub.reader.as_mut() else {
                continue;
            };
            if sub.process {
                vfs.enter_process(READER);
            }
            // A reader-process feed whose writer restarted since gets `SnapshotExpired` (its
            // next poll resubscribes): it reads nothing here.
            while let Ok(Next::Commit(c)) = r.try_next() {
                let tag = tag_of(c, seed);
                if let Some(m) = self.made.get_mut(&tag)
                    && m.seqno == 0
                {
                    m.seqno = c.seqno;
                }
                sub.delivered.push((c.seqno, tag));
            }
            vfs.enter_process(WRITER);
        }
        drop(pending);
        self.crash_and_reopen();
    }

    /// Polls every subscriber until it has nothing more, checking each delivery.
    fn poll(&mut self) {
        for i in 0..self.subs.len() {
            loop {
                settle(&mut self.shards);
                let seed = self.seed;
                let sub = &mut self.subs[i];
                let Some(r) = sub.reader.as_mut() else { break };
                if sub.process {
                    self.vfs.enter_process(READER);
                }
                let next = r.try_next();
                self.vfs.enter_process(WRITER);
                let next = match next {
                    Ok(next) => next,
                    // A reader process's feed after its writer restarted: resubscribe from
                    // its own token on the new writer (D310 PR 4, M2).
                    Err(pigeonhole_engine::Error::SnapshotExpired) if sub.process => {
                        sub.token = (!sub.oldest).then(|| r.token());
                        sub.reader = None;
                        self.subscribe(i);
                        continue;
                    }
                    Err(e) => panic!("seed {seed}: try_next (process {}): {e}", sub.process),
                };
                let c = match next {
                    Next::Commit(c) => c.clone(),
                    Next::Pending => {
                        // M1 at this frontier: every surviving commit at or below it past
                        // the subscriber's start was delivered.
                        let frontier = r.frontier();
                        let got: HashSet<u32> = sub.delivered.iter().map(|d| d.1).collect();
                        for (tag, m) in &self.made {
                            if m.seqno != 0 && m.seqno > sub.last && m.seqno <= frontier {
                                assert!(
                                    got.contains(tag),
                                    "seed {seed}: commit {tag} (seqno {}) at or below the \
                                     frontier {frontier} never delivered (durable {})",
                                    m.seqno,
                                    sub.durable
                                );
                            }
                        }
                        break;
                    }
                    Next::Expired(e) => {
                        panic!(
                            "seed {seed}: expired without cause (durable {}): {e:?}",
                            sub.durable
                        )
                    }
                    Next::Catalog(e) => {
                        panic!("seed {seed}: a catalog event in a per-table feed: {e:?}")
                    }
                };
                let tag = tag_of(&c, seed);
                let m = self
                    .made
                    .get_mut(&tag)
                    .unwrap_or_else(|| panic!("seed {seed}: commit {tag} delivered, never made"));
                // A commit unanswered at a power loss is learned at its first delivery.
                if m.seqno == 0 {
                    m.seqno = c.seqno;
                }
                assert_eq!(
                    c.seqno, m.seqno,
                    "seed {seed}: commit {tag} at another seqno"
                );
                let mut rows: Vec<_> = c
                    .changes
                    .iter()
                    .map(|ch| (ch.table, ch.row.clone()))
                    .collect();
                rows.sort();
                let mut want = m.rows.clone();
                want.sort();
                assert_eq!(rows, want, "seed {seed}: commit {tag} not whole");
                let prev = sub.delivered.last().map_or(sub.last, |d| d.0);
                assert!(c.seqno > prev, "seed {seed}: {} after {prev}", c.seqno);
                // Nothing surviving between the two was skipped.
                for (t, m) in &self.made {
                    assert!(
                        !(m.seqno != 0 && m.seqno > prev && m.seqno < c.seqno),
                        "seed {seed}: commit {t} (seqno {}, {:?}) skipped between {prev} and {} \
                         (durable {}, process {}; delivered tail {:?}, last {})",
                        m.seqno,
                        m.durability,
                        c.seqno,
                        sub.durable,
                        sub.process,
                        &sub.delivered[sub.delivered.len().saturating_sub(4)..],
                        sub.last
                    );
                }
                sub.delivered.push((c.seqno, tag));
            }
        }
    }

    fn take_tokens(&mut self) {
        for sub in &mut self.subs {
            if let Some(r) = &sub.reader
                && !sub.oldest
            {
                sub.token = Some(r.token());
            }
        }
    }

    fn close_and_reopen(&mut self) {
        self.poll();
        self.take_tokens();
        // The reader process lives on: its feeds find their attachment stale at their next
        // poll and resubscribe from their own tokens.
        for sub in &mut self.subs {
            if !sub.process {
                sub.reader = None;
            }
        }
        let db = self.db.take().unwrap();
        // A clean close: drive it with the shards.
        let shards = std::mem::take(&mut self.shards);
        close_app_owned(db, shards);
        self.open();
        for i in 0..self.subs.len() {
            if !self.subs[i].process {
                self.subscribe(i);
            }
        }
    }

    fn crash_and_reopen(&mut self) {
        // No poll first: polling settles the shards, and on the simulator's frozen clock that
        // runs the lazy flush and the background sync, which would leave nothing to lose.
        self.take_tokens();
        // What each durable subscriber delivered must survive.
        let durable_delivered: Vec<u32> = self
            .subs
            .iter()
            .filter(|s| s.durable)
            .flat_map(|s| s.delivered.iter().map(|d| d.1))
            .collect();
        for sub in &mut self.subs {
            sub.reader = None;
        }
        // A root commit whose sidecar sync hasn't landed: its Root entry may be lost with the
        // power, which expires every token (the window fix A leaves, the safe direction).
        let root_unsynced = self.db().feed_sidecar_sync_pending();
        // A power loss ends every process, the reader's too.
        self.reader_db = None;
        self.vfs.crash(CrashKind::Power);
        self.shards.clear();
        self.db = None;
        self.open();
        self.open_reader();
        // Which commits survived: a power loss may undo `None` and `Buffered` ones.
        let mut lost = Vec::new();
        for (tag, m) in &self.made {
            // All or nothing, a cross-shard commit included.
            let present: Vec<bool> = m
                .rows
                .iter()
                .map(|(t, row)| {
                    let table = self.tables.iter().find(|x| x.id == *t).unwrap();
                    self.db()
                        .get_latest(*t, table.families[0].id, row, b"q")
                        .unwrap()
                        .is_some()
                })
                .collect();
            assert!(
                present.iter().all(|&p| p == present[0]),
                "seed {}: commit {tag} partly survived: {present:?}",
                self.seed
            );
            let there = present[0];
            if !there {
                assert!(
                    matches!(m.durability, Durability::None | Durability::Buffered),
                    "seed {}: a {:?} commit {tag} lost",
                    self.seed,
                    m.durability
                );
                lost.push(*tag);
            }
        }
        self.lost_total += lost.len() as u32;
        // The reopen's own work commits a root before any token resumes: the replayed
        // memtables flushed (simp724's Q1 on #1316). The session's Root entries must chain
        // from its `opened_at`, so a durable token still continues below.
        let flush = self.db().submit_flush().unwrap();
        drive(&mut self.shards, flush).unwrap();
        for tag in &lost {
            assert!(
                !durable_delivered.contains(tag),
                "seed {}: the durable feed delivered commit {tag}, which a power loss undid",
                self.seed
            );
            self.made.remove(tag);
        }
        for i in 0..self.subs.len() {
            let durable = self.subs[i].durable;
            let Some(token) = self.subs[i].token.clone() else {
                self.subscribe(i);
                continue;
            };
            let mut r = self.feed_for(i, self.spec(durable, FeedStart::Token(token)));
            if self.subs[i].process {
                self.vfs.enter_process(READER);
            }
            let next = r.try_next();
            self.vfs.enter_process(WRITER);
            let expired = match next.unwrap() {
                Next::Expired(e) => Some(e.reason.clone()),
                _ => None,
            };
            drop(r);
            if let Some(reason) = expired {
                // A visible feed may have delivered what was lost; a durable one never
                // expires across a power loss (call 4: everything it delivered survived).
                assert!(
                    // Fix A's window: a lost Root entry breaks the session chain, which
                    // shows at the token ("another session wrote") or at the first record
                    // past it ("a session that did not feed these tables wrote").
                    !durable
                        || (root_unsynced
                            && (reason.contains("another session wrote")
                                || reason.contains("did not feed these tables"))),
                    "seed {}: the durable feed expired after a power loss: {reason} \
                     (a sidecar sync in flight: {root_unsynced})",
                    self.seed
                );
                self.window_expiries += u32::from(durable);
                self.subs[i].token = None;
            } else if !durable && self.subs[i].delivered.iter().any(|d| lost.contains(&d.1)) {
                self.subs[i].token = None;
            }
            self.subscribe(i);
        }
    }
}

fn close_app_owned(db: Arc<Engine>, mut shards: Vec<EngineShard>) {
    let closer = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.close())
    };
    while !closer.is_finished() {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        complete_one();
        std::thread::yield_now();
    }
    closer.join().unwrap().unwrap();
}

fn run(seed: u64) {
    let vfs = SimVfs::new(seed);
    let deferred = std::env::var("PIGEONHOLE_DEFERRED_IO").is_ok_and(|v| v == "1");
    DEFERRED.with(|d| *d.borrow_mut() = deferred.then(|| Arc::clone(&vfs)));
    let mut rng = Rng(seed);
    let shards_n = 1 + rng.below(3) as usize;
    vfs.enter_process(WRITER);
    let deferred = std::env::var("PIGEONHOLE_DEFERRED_IO").is_ok_and(|v| v == "1");
    DEFERRED.with(|d| *d.borrow_mut() = deferred.then(|| Arc::clone(&vfs)));
    let tables = {
        let db = Engine::open(Path::new(DB), options(&vfs, shards_n)).unwrap();
        let f = || vec![("f".into(), FamilyOptions::default())];
        let a = db.create_table("a", &f()).unwrap();
        let b = db.create_table("b", &f()).unwrap();
        db.close().unwrap();
        [a, b]
    };
    // After the setup engine (threaded) closed: from here the run's own loops step the
    // device.
    if deferred {
        vfs.set_deferred_io(true);
    }
    let mut run = Run {
        seed,
        vfs,
        shards_n,
        tables,
        db: None,
        reader_db: None,
        shards: Vec::new(),
        made: BTreeMap::new(),
        // A durable and a visible subscriber in the writer, and the same in a reader process;
        // and two that subscribe from the oldest each time (#1465): a durable one in the
        // writer and a visible one in the reader process.
        subs: [
            (true, false, false),
            (false, false, false),
            (true, true, false),
            (false, true, false),
            (true, false, true),
            (false, true, true),
        ]
        .into_iter()
        .map(|(durable, process, oldest)| Sub {
            durable,
            process,
            oldest,
            reader: None,
            token: None,
            delivered: Vec::new(),
            last: 0,
        })
        .collect(),
        window_expiries: 0,
        lost_total: 0,
    };
    run.open();
    run.open_reader();
    for i in 0..run.subs.len() {
        run.subscribe(i);
    }
    let levels = [
        Durability::None,
        Durability::Buffered,
        Durability::GroupSync,
        Durability::Sync,
    ];
    let mut reopens = HashMap::new();
    for tag in 0..240u32 {
        let durability = levels[rng.below(4) as usize];
        let cross = run.shards_n > 1 && rng.below(3) == 0;
        run.commit(tag, cross, durability);
        match rng.below(40) {
            0..=7 => run.poll(),
            8 => {
                let flush = run.db().submit_flush().unwrap();
                drive(&mut run.shards, flush).unwrap();
            }
            9 => {
                // Resume one subscriber from a token taken now, in this session.
                run.poll();
                let i = rng.below(run.subs.len() as u64) as usize;
                // A subscriber without tokens subscribes from the oldest again instead.
                run.subs[i].token = match run.subs[i].oldest {
                    true => None,
                    false => run.subs[i].reader.as_ref().map(|r| r.token()),
                };
                run.subs[i].reader = None;
                run.subscribe(i);
            }
            10 => {
                eprintln!("seed {seed}: clean reopen at commit {tag}");
                *reopens.entry("clean").or_insert(0) += 1;
                run.close_and_reopen();
            }
            11 => {
                eprintln!("seed {seed}: power loss at commit {tag}");
                *reopens.entry("power").or_insert(0) += 1;
                run.crash_and_reopen();
            }
            12 | 13 => {
                eprintln!("seed {seed}: a burst, then a power loss, at commit {tag}");
                *reopens.entry("burst+power").or_insert(0) += 1;
                run.burst_then_crash(&mut rng, 10_000 + tag * 8);
            }
            _ => {}
        }
    }
    // Everything synced, then every surviving commit is delivered to both.
    let flush = run.db().submit_flush().unwrap();
    drive(&mut run.shards, flush).unwrap();
    let mut wb = WriteBatch::new();
    let t = &run.tables[0];
    wb.put(
        t.id,
        t.families[0].id,
        b"999999",
        b"q",
        None,
        ValueRef::Bytes(b"x"),
    )
    .unwrap();
    let pc = run.db().submit(wb, Some(Durability::Sync)).unwrap();
    let last = drive(&mut run.shards, pc).unwrap();
    run.made.insert(
        999_999,
        Made {
            rows: vec![(t.id, b"999999".to_vec())],
            seqno: last.seqno,
            durability: Durability::Sync,
        },
    );
    run.poll();
    for sub in &run.subs {
        assert_eq!(
            sub.delivered.last().map(|d| d.0),
            Some(last.seqno),
            "seed {seed}: the last commit not delivered (durable {}, process {})",
            sub.durable,
            sub.process
        );
    }
    eprintln!(
        "seed {seed}: {} shards, {} commits kept, {} lost, reopens {reopens:?}, window \
         expiries {}",
        run.shards_n,
        run.made.len(),
        run.lost_total,
        run.window_expiries
    );
    let db = run.db.take().unwrap();
    for sub in &mut run.subs {
        sub.reader = None;
    }
    run.vfs.enter_process(READER);
    if let Some(r) = run.reader_db.take() {
        r.close().unwrap();
    }
    run.vfs.enter_process(WRITER);
    close_app_owned(db, std::mem::take(&mut run.shards));
}

#[test]
fn the_feed_matches_the_model() {
    for seed in seeds() {
        run(seed);
    }
}
