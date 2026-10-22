//! #694 PR 1: black-box checks that optimistic transactions are serializable over what they
//! read, through the public API, with real threads (so real interleavings), no reference
//! model and no engine internals:
//! - `histories_are_serializable`: concurrent read-modify-write, blind-write and read-only
//!   transactions over a few hot keys spread across shards. Every write stores a unique
//!   value, so each read names the write it saw. The history must have no read of an
//!   aborted or unknown write (G1a), every attempt's reads must come from one snapshot (even
//!   one that then conflicts), and the dependency graph of the committed transactions (ww
//!   by commit seqno, wr, rw) must be acyclic: no lost update, no write skew (G0, G1c, G2).
//! - `bank_transfers_conserve_money` and `write_skew_is_prevented`: the two classic
//!   invariants, checked at every snapshot a transaction read.
//! - `a_cross_shard_transaction_crashed_at_every_step_is_all_or_nothing`: a power loss
//!   after each mutating I/O of a cross-shard transaction's commit; after reopen the
//!   transaction is wholly there or wholly absent, and there if it was acknowledged.
//!
//! Each test runs `PIGEONHOLE_SEEDS` seeds from `PIGEONHOLE_SEED` (default 2 from 1); the
//! sweeps run hundreds. A failure prints its seed. Thread scheduling is not replayable, so
//! a seed fixes the workload, not the interleaving.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use pigeonhole::{Durability, ErrorCode, Family, Options, Pigeonhole, Table};

fn seeds() -> std::ops::Range<u64> {
    let n: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let base: u64 = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    base..base + n
}

/// Runs `f` per seed, naming the seed when it panics.
fn for_seeds(f: impl Fn(u64)) {
    for seed in seeds() {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(seed)));
        if let Err(e) = r {
            eprintln!("failed at seed {seed} (PIGEONHOLE_SEED={seed} PIGEONHOLE_SEEDS=1)");
            std::panic::resume_unwind(e);
        }
    }
}

/// xorshift64*.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A temporary directory removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("pigeonhole-serial-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const SHARDS: usize = 4;
/// Keys live in this many tables, so they spread over the shards (one tablet each).
const TABLES: usize = 4;

fn options() -> Options {
    Options::default()
        .shards(SHARDS)
        .pin_threads(false)
        .tablet_changes(false)
        .memtable_budget(32 << 20)
        .block_cache(8 << 20)
        .durability(Durability::Buffered)
}

/// The tables keys live in, and which table and row key `k` is.
struct Keys {
    tables: Vec<Table>,
}

impl Keys {
    fn create(db: &Pigeonhole) -> Self {
        let tables = (0..TABLES)
            .map(|t| {
                db.table(&format!("t{t}"))
                    .unwrap()
                    .family("f", Family::default().max_versions(1))
                    .create()
                    .unwrap()
            })
            .collect();
        Keys { tables }
    }
    fn of(&self, k: usize) -> (&Table, Vec<u8>) {
        (&self.tables[k % TABLES], format!("key{k:04}").into_bytes())
    }
}

/// A transaction's read, or a write's value: eight bytes, little-endian.
fn decode(v: &[u8]) -> u64 {
    u64::from_le_bytes(v.try_into().expect("an 8-byte value"))
}

#[derive(Debug, Clone)]
enum Outcome {
    Committed(u64),
    Conflict,
    /// Any other error (`Busy` past the stall timeout): treated as not committed, but a
    /// write it made must then never be read.
    Failed(String),
}

#[derive(Debug, Clone)]
struct Attempt {
    worker: usize,
    reads: Vec<(usize, u64)>,
    writes: Vec<(usize, u64)>,
    outcome: Outcome,
}

/// A unique value: the writer and a counter; worker `usize::MAX` is the initial load.
fn unique(worker: usize, n: u64) -> u64 {
    ((worker as u64 + 1) << 40) | n
}

#[test]
fn histories_are_serializable() {
    for_seeds(|seed| {
        let dir = TempDir::new(&format!("hist-{seed}"));
        let db = Arc::new(Pigeonhole::open(dir.0.join("h.phdb"), options()).unwrap());
        let keys = Arc::new(Keys::create(&db));
        let nkeys = 12;
        // The initial values, committed before any transaction.
        let mut init_seqno = 0;
        for k in 0..nkeys {
            let (t, row) = keys.of(k);
            init_seqno = t
                .mutate(&row)
                .put("f", b"v", &(k as u64).to_le_bytes())
                .commit()
                .unwrap()
                .seqno
                .max(init_seqno);
        }
        let shards: std::collections::HashSet<_> = (0..nkeys)
            .map(|k| {
                let (t, row) = keys.of(k);
                t.shard_of(&row)
            })
            .collect();
        assert!(
            shards.len() > 1,
            "keys on one shard: no cross-shard transactions"
        );

        let workers = 8;
        let per_worker = 120;
        let start = Arc::new(Barrier::new(workers));
        let threads: Vec<_> = (0..workers)
            .map(|w| {
                let (db, keys, start) = (Arc::clone(&db), Arc::clone(&keys), Arc::clone(&start));
                std::thread::spawn(move || {
                    let mut rng = Rng::new(seed * 1_000 + w as u64);
                    let mut history = Vec::new();
                    let mut n = 0;
                    start.wait();
                    for _ in 0..per_worker {
                        history.push(attempt(&db, &keys, nkeys, w, &mut n, &mut rng));
                    }
                    history
                })
            })
            .collect();
        let history: Vec<Attempt> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();

        // The final value of each key, for the last version check.
        let mut last = BTreeMap::new();
        for k in 0..nkeys {
            let (t, row) = keys.of(k);
            let v = t
                .get(&row, "f", b"v")
                .unwrap()
                .expect("every key is present");
            last.insert(k, decode(v.value()));
        }
        check_history(&history, nkeys, init_seqno, &last);
        // Anything but a commit or a conflict (`Busy` past the stall timeout) is rare.
        let failed: Vec<&str> = history
            .iter()
            .filter_map(|a| match &a.outcome {
                Outcome::Failed(e) => Some(e.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            failed.len() * 20 <= history.len(),
            "seed {seed}: {failed:?}"
        );
        let committed = history
            .iter()
            .filter(|a| matches!(a.outcome, Outcome::Committed(_)))
            .count();
        assert!(
            committed * 4 > history.len(),
            "too few commits: {committed}"
        );
        drop(keys);
        Arc::try_unwrap(db).ok().unwrap().close().unwrap();
    });
}

/// One transaction: a read-modify-write (half), a read-only one (a quarter) or a blind
/// write with reads of other keys (a quarter), over 1 to 3 keys.
fn attempt(
    db: &Pigeonhole,
    keys: &Keys,
    nkeys: usize,
    worker: usize,
    n: &mut u64,
    rng: &mut Rng,
) -> Attempt {
    let kind = rng.below(4);
    let mut picked = Vec::new();
    while picked.len() < 1 + rng.below(3) as usize {
        let k = rng.below(nkeys as u64) as usize;
        if !picked.contains(&k) {
            picked.push(k);
        }
    }
    let mut txn = db.transaction().unwrap();
    let mut reads = Vec::new();
    let mut writes = Vec::new();
    let read_keys: &[usize] = if kind == 3 { &picked[1..] } else { &picked };
    for &k in read_keys {
        let (t, row) = keys.of(k);
        match txn.get(t, &row, "f", b"v") {
            Ok(Some(c)) => reads.push((k, decode(c.value()))),
            Ok(None) => panic!("key {k} read as absent"),
            Err(e) => {
                return Attempt {
                    worker,
                    reads,
                    writes,
                    outcome: Outcome::Failed(format!("read: {e}")),
                };
            }
        }
    }
    let write_keys: &[usize] = match kind {
        0 | 1 => &picked,
        2 => &[],
        _ => &picked[..1],
    };
    for &k in write_keys {
        *n += 1;
        let v = unique(worker, *n);
        let (t, row) = keys.of(k);
        txn.put(t, &row, "f", b"v", &v.to_le_bytes());
        writes.push((k, v));
    }
    let outcome = match txn.commit() {
        Ok(info) => Outcome::Committed(info.seqno),
        Err(e) if e.code() == ErrorCode::Conflict => Outcome::Conflict,
        Err(e) => Outcome::Failed(e.to_string()),
    };
    Attempt {
        worker,
        reads,
        writes,
        outcome,
    }
}

/// Checks a history as described at the top of the file. Node 0 is the initial load.
fn check_history(history: &[Attempt], nkeys: usize, init_seqno: u64, last: &BTreeMap<usize, u64>) {
    // Who wrote each value, and at what seqno (committed writers only).
    let mut writer: HashMap<u64, (usize, u64)> = HashMap::new();
    for k in 0..nkeys {
        writer.insert(k as u64, (0, init_seqno));
    }
    let mut aborted_values: HashMap<u64, usize> = HashMap::new();
    for (i, a) in history.iter().enumerate() {
        for &(_, v) in &a.writes {
            match a.outcome {
                Outcome::Committed(s) => {
                    writer.insert(v, (i + 1, s));
                }
                _ => {
                    aborted_values.insert(v, i + 1);
                }
            }
        }
    }
    // Each key's versions in commit order.
    let mut versions: Vec<Vec<(u64, u64, usize)>> = vec![Vec::new(); nkeys]; // (seqno, value, node)
    for (k, vs) in versions.iter_mut().enumerate() {
        vs.push((init_seqno, k as u64, 0));
    }
    for (i, a) in history.iter().enumerate() {
        if let Outcome::Committed(s) = a.outcome {
            for &(k, v) in &a.writes {
                versions[k].push((s, v, i + 1));
            }
        }
    }
    for (k, vs) in versions.iter_mut().enumerate() {
        vs.sort();
        for w in vs.windows(2) {
            assert!(w[0].0 != w[1].0, "key {k}: two writes at seqno {}", w[0].0);
        }
        assert_eq!(
            vs.last().unwrap().1,
            last[&k],
            "key {k}: the final value is not the last committed write"
        );
    }
    let position = |k: usize, v: u64| versions[k].iter().position(|x| x.1 == v);

    // G1a, and each attempt's reads from one snapshot (committed or not).
    for (i, a) in history.iter().enumerate() {
        let (mut lo, mut hi) = (0u64, u64::MAX);
        for &(k, v) in &a.reads {
            if let Some(node) = aborted_values.get(&v) {
                panic!(
                    "G1a: attempt {} read key {k}'s value {v:#x} written by aborted attempt {node}: {a:?}",
                    i + 1
                );
            }
            let Some(p) = position(k, v) else {
                panic!(
                    "attempt {} read key {k}'s value {v:#x}, which no commit wrote: {a:?}",
                    i + 1
                );
            };
            lo = lo.max(versions[k][p].0);
            if let Some(next) = versions[k].get(p + 1) {
                hi = hi.min(next.0);
            }
        }
        assert!(
            lo < hi,
            "attempt {}'s reads come from no single snapshot (worker {}): {a:?}",
            i + 1,
            a.worker
        );
    }

    // The dependency graph of committed attempts (and the initial load).
    let nodes = history.len() + 1;
    let mut edges: Vec<Vec<(usize, &'static str, usize)>> = vec![Vec::new(); nodes]; // (to, kind, key)
    for (k, vs) in versions.iter().enumerate() {
        for w in vs.windows(2) {
            edges[w[0].2].push((w[1].2, "ww", k));
        }
    }
    for (i, a) in history.iter().enumerate() {
        if !matches!(a.outcome, Outcome::Committed(_)) {
            continue;
        }
        let node = i + 1;
        for &(k, v) in &a.reads {
            let p = position(k, v).unwrap();
            let w = versions[k][p].2;
            if w != node {
                edges[w].push((node, "wr", k));
            }
            if let Some(next) = versions[k].get(p + 1)
                && next.2 != node
            {
                edges[node].push((next.2, "rw", k));
            }
        }
    }
    if let Some(cycle) = find_cycle(&edges) {
        let described: Vec<String> = cycle
            .iter()
            .map(|(from, kind, key, to)| {
                let a = if *from == 0 {
                    "init".to_owned()
                } else {
                    format!("{:?}", history[from - 1])
                };
                format!("  {from} -{kind}(key {key})-> {to}: {a}")
            })
            .collect();
        panic!(
            "dependency cycle (not serializable):\n{}",
            described.join("\n")
        );
    }
}

/// A cycle as `(from, kind, key, to)` steps, if the graph has one (iterative DFS).
fn find_cycle(
    edges: &[Vec<(usize, &'static str, usize)>],
) -> Option<Vec<(usize, &'static str, usize, usize)>> {
    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        New,
        Open,
        Done,
    }
    let mut mark = vec![Mark::New; edges.len()];
    let mut parent: Vec<Option<(usize, &'static str, usize)>> = vec![None; edges.len()];
    for root in 0..edges.len() {
        if mark[root] != Mark::New {
            continue;
        }
        let mut stack = vec![(root, 0usize)];
        mark[root] = Mark::Open;
        while let Some(&mut (node, ref mut next)) = stack.last_mut() {
            if let Some(&(to, kind, key)) = edges[node].get(*next) {
                *next += 1;
                match mark[to] {
                    Mark::New => {
                        mark[to] = Mark::Open;
                        parent[to] = Some((node, kind, key));
                        stack.push((to, 0));
                    }
                    Mark::Open => {
                        // `to` is on the stack: walk back from `node` to it.
                        let mut cycle = vec![(node, kind, key, to)];
                        let mut at = node;
                        while at != to {
                            let (p, k, key) = parent[at].expect("on the stack");
                            cycle.push((p, k, key, at));
                            at = p;
                        }
                        cycle.reverse();
                        return Some(cycle);
                    }
                    Mark::Done => {}
                }
            } else {
                mark[node] = Mark::Done;
                stack.pop();
            }
        }
    }
    None
}

#[test]
fn the_cycle_check_finds_a_lost_update() {
    // The checker itself: two read-modify-writes that both read the initial value and both
    // committed form a cycle (rw both ways), whatever their seqnos.
    let history = vec![
        Attempt {
            worker: 0,
            reads: vec![(0, 0)],
            writes: vec![(0, unique(0, 1))],
            outcome: Outcome::Committed(10),
        },
        Attempt {
            worker: 1,
            reads: vec![(0, 0)],
            writes: vec![(0, unique(1, 1))],
            outcome: Outcome::Committed(11),
        },
    ];
    let last = BTreeMap::from([(0, unique(1, 1))]);
    let r = std::panic::catch_unwind(|| check_history(&history, 1, 1, &last));
    let msg = *r.unwrap_err().downcast::<String>().unwrap();
    assert!(msg.contains("dependency cycle"), "{msg}");
}

#[test]
fn bank_transfers_conserve_money() {
    for_seeds(|seed| {
        let dir = TempDir::new(&format!("bank-{seed}"));
        let db = Arc::new(Pigeonhole::open(dir.0.join("b.phdb"), options()).unwrap());
        let keys = Arc::new(Keys::create(&db));
        let accounts = 10usize;
        let start_balance = 1_000u64;
        for k in 0..accounts {
            let (t, row) = keys.of(k);
            t.mutate(&row)
                .put("f", b"v", &start_balance.to_le_bytes())
                .commit()
                .unwrap();
        }
        let total = start_balance * accounts as u64;
        let workers = 6;
        let start = Arc::new(Barrier::new(workers));
        let threads: Vec<_> = (0..workers)
            .map(|w| {
                let (db, keys, start) = (Arc::clone(&db), Arc::clone(&keys), Arc::clone(&start));
                std::thread::spawn(move || {
                    let mut rng = Rng::new(seed * 7_919 + w as u64);
                    let mut commits = 0;
                    start.wait();
                    for _ in 0..150 {
                        let mut txn = db.transaction().unwrap();
                        if rng.below(4) == 0 {
                            // An audit: every account at one snapshot sums to the total,
                            // whether or not the (read-only) commit then succeeds.
                            let mut sum = 0;
                            for k in 0..accounts {
                                let (t, row) = keys.of(k);
                                sum +=
                                    decode(txn.get(t, &row, "f", b"v").unwrap().unwrap().value());
                            }
                            assert_eq!(
                                sum, total,
                                "seed {seed}: an audit saw money appear or vanish"
                            );
                            let _ = txn.commit();
                            continue;
                        }
                        let a = rng.below(accounts as u64) as usize;
                        let b = (a + 1 + rng.below(accounts as u64 - 1) as usize) % accounts;
                        let (ta, ra) = keys.of(a);
                        let (tb, rb) = keys.of(b);
                        let ba = decode(txn.get(ta, &ra, "f", b"v").unwrap().unwrap().value());
                        let bb = decode(txn.get(tb, &rb, "f", b"v").unwrap().unwrap().value());
                        let amount = rng.below(ba.min(50) + 1);
                        txn.put(ta, &ra, "f", b"v", &(ba - amount).to_le_bytes());
                        txn.put(tb, &rb, "f", b"v", &(bb + amount).to_le_bytes());
                        match txn.commit() {
                            Ok(_) => commits += 1,
                            Err(e) if e.code() == ErrorCode::Conflict => {}
                            Err(e) => panic!("seed {seed}: transfer: {e}"),
                        }
                    }
                    commits
                })
            })
            .collect();
        let commits: u32 = threads.into_iter().map(|t| t.join().unwrap()).sum();
        assert!(commits > 0, "seed {seed}: no transfer committed");
        let sum: u64 = (0..accounts)
            .map(|k| {
                let (t, row) = keys.of(k);
                decode(t.get(&row, "f", b"v").unwrap().unwrap().value())
            })
            .sum();
        assert_eq!(sum, total, "seed {seed}: the final balances");
        drop(keys);
        Arc::try_unwrap(db).ok().unwrap().close().unwrap();
    });
}

#[test]
fn write_skew_is_prevented() {
    // "At least one doctor stays on call": each transaction reads both of a pair and takes
    // its own doctor off call only if both are on. Under snapshot isolation two such
    // transactions could both commit and leave nobody; serializable transactions cannot.
    for_seeds(|seed| {
        let dir = TempDir::new(&format!("skew-{seed}"));
        let db = Arc::new(Pigeonhole::open(dir.0.join("s.phdb"), options()).unwrap());
        let keys = Arc::new(Keys::create(&db));
        let pairs = 6usize;
        // Doctors 2p and 2p+1 (in different tables, so mostly on different shards).
        let on_call = |keys: &Keys| {
            for d in 0..2 * pairs {
                let (t, row) = keys.of(d);
                t.mutate(&row)
                    .put("f", b"v", &1u64.to_le_bytes())
                    .commit()
                    .unwrap();
            }
        };
        for round in 0..20 {
            on_call(&keys);
            let start = Arc::new(Barrier::new(pairs * 2));
            let threads: Vec<_> = (0..pairs * 2)
                .map(|d| {
                    let (db, keys, start) =
                        (Arc::clone(&db), Arc::clone(&keys), Arc::clone(&start));
                    std::thread::spawn(move || {
                        let p = d / 2;
                        let mut txn = db.transaction().unwrap();
                        let mut on = 0;
                        for x in [2 * p, 2 * p + 1] {
                            let (t, row) = keys.of(x);
                            on += decode(txn.get(t, &row, "f", b"v").unwrap().unwrap().value());
                        }
                        start.wait();
                        if on == 2 {
                            let (t, row) = keys.of(d);
                            txn.put(t, &row, "f", b"v", &0u64.to_le_bytes());
                        }
                        // A conflict is the expected way for the second of a pair to lose.
                        let _ = txn.commit();
                    })
                })
                .collect();
            for t in threads {
                t.join().unwrap();
            }
            for p in 0..pairs {
                let on: u64 = [2 * p, 2 * p + 1]
                    .iter()
                    .map(|&d| {
                        let (t, row) = keys.of(d);
                        decode(t.get(&row, "f", b"v").unwrap().unwrap().value())
                    })
                    .sum();
                assert!(
                    on >= 1,
                    "seed {seed} round {round}: write skew left pair {p} with nobody on call"
                );
            }
        }
        drop(keys);
        Arc::try_unwrap(db).ok().unwrap().close().unwrap();
    });
}

#[test]
fn a_cross_shard_transaction_crashed_at_every_step_is_all_or_nothing() {
    use pigeonhole_io::sim::{FaultPlan, SimVfs};
    for_seeds(|seed| {
        let sim_options = |vfs: &Arc<SimVfs>| {
            Options::default()
                .vfs(Arc::clone(vfs) as _)
                .shards(SHARDS)
                .tablet_changes(false)
                .memtable_budget(8 << 20)
                .wal_segment_size(256 << 10)
        };
        let mut crashed_runs = 0;
        for step in 1..400u64 {
            let vfs = SimVfs::new(seed * 10_000 + step);
            let db = Pigeonhole::open("/db/x.phdb", sim_options(&vfs)).unwrap();
            let keys = Keys::create(&db);
            // Two keys on different shards, read and written by one transaction.
            let (a, b) = (0..16)
                .flat_map(|x| (0..16).map(move |y| (x, y)))
                .find(|&(x, y)| {
                    x != y
                        && keys.of(x).0.shard_of(&keys.of(x).1)
                            != keys.of(y).0.shard_of(&keys.of(y).1)
                })
                .expect("two keys on different shards");
            for k in [a, b] {
                let (t, row) = keys.of(k);
                t.mutate(&row)
                    .put("f", b"v", &1u64.to_le_bytes())
                    .durability(Durability::Sync)
                    .commit()
                    .unwrap();
            }
            let planned = vfs.mutating_ops() + step;
            let mut plan = FaultPlan::none();
            plan.torn_writes = true;
            plan.reorder_unsynced = true;
            plan.crash_after_ops = Some(planned);
            vfs.set_faults(plan);
            let committed = (|| {
                let mut txn = db.transaction()?;
                for k in [a, b] {
                    let (t, row) = keys.of(k);
                    let v = decode(txn.get(t, &row, "f", b"v")?.unwrap().value());
                    txn.put(t, &row, "f", b"v", &(v + 1).to_le_bytes());
                }
                txn.commit_with(Durability::Sync)
            })()
            .is_ok();
            // The power loss fired inside the commit if the I/O count reached it.
            let crashed = vfs.mutating_ops() >= planned;
            // Close (a crashed instance fails to; the close may also reach the crash point),
            // then reopen with no faults.
            drop(keys);
            let _ = db.close();
            vfs.set_faults(FaultPlan::none());
            let db = Pigeonhole::open("/db/x.phdb", sim_options(&vfs))
                .unwrap_or_else(|e| panic!("seed {seed} step {step}: reopen: {e}"));
            let keys = Keys::create_or_open(&db);
            let values: Vec<u64> = [a, b]
                .iter()
                .map(|&k| {
                    let (t, row) = keys.of(k);
                    decode(t.get(&row, "f", b"v").unwrap().unwrap().value())
                })
                .collect();
            assert!(
                values == [1, 1] || values == [2, 2],
                "seed {seed} step {step}: half a transaction survived: {values:?}"
            );
            if committed {
                assert_eq!(
                    values,
                    [2, 2],
                    "seed {seed} step {step}: an acknowledged Sync transaction was lost"
                );
            }
            db.close().unwrap();
            if crashed {
                crashed_runs += 1;
            }
            if committed && !crashed {
                // The commit finished before the planned crash point: every step is covered.
                break;
            }
        }
        assert!(
            crashed_runs > 3,
            "seed {seed}: the matrix crashed only {crashed_runs} runs"
        );
    });
}

impl Keys {
    /// The tables of an existing database.
    fn create_or_open(db: &Pigeonhole) -> Self {
        let tables = (0..TABLES)
            .map(|t| db.table(&format!("t{t}")).unwrap().open().unwrap())
            .collect();
        Keys { tables }
    }
}
