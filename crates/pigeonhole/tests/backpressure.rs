//! Async commit backpressure (D210): a shard holds at most half `memtable_budget` of async
//! commits submitted and not yet resolved; a commit over the bound waits, before it
//! submits, until earlier ones resolve, and waiters go in arrival order. Blocking commits
//! are not held. Each test drives one application-owned shard by hand, so a "stopped"
//! shard is one this thread does not run.
#![cfg(feature = "async")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pigeonhole::nonblocking::CommitFuture;
use pigeonhole::{Durability, Family, Options, Pigeonhole, Shard, Table};
use pigeonhole_io::sim::SimVfs;

/// 4 MiB arenas: a 2 MiB bound per shard.
const BOUND: u64 = 2 << 20;

fn options(vfs: &Arc<SimVfs>) -> Options {
    Options::default()
        .vfs(Arc::clone(vfs) as _)
        .shards(1)
        .tablet_changes(false)
        .durability(Durability::Buffered)
        .memtable_budget(4 << 20)
        .wal_segment_size(4 << 20)
}

/// Opens an application-owned database and creates its table with a helper thread driving
/// the shard; this thread then drives it by hand.
fn open(seed: u64) -> (Pigeonhole, Shard, Table) {
    let vfs = SimVfs::new(seed);
    let (db, mut shards) =
        Pigeonhole::open_application_owned("/db/bp.phdb", options(&vfs)).unwrap();
    let mut shard = shards.remove(0);
    let stop = Arc::new(AtomicBool::new(false));
    let driver = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                shard.run_once(Duration::from_millis(1));
            }
            shard
        })
    };
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    stop.store(true, Ordering::Release);
    (db, driver.join().unwrap(), t)
}

fn poll<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    Pin::new(fut).poll(&mut Context::from_waker(Waker::noop()))
}

fn close(db: Pigeonhole, mut shard: Shard) {
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(Duration::from_millis(1));
    }
}

/// A batch of `cells` cells of `len` bytes on `row`.
fn commit(t: &Table, row: &str, cells: usize, len: usize) -> CommitFuture {
    let mut m = t.mutate(row.as_bytes());
    for c in 0..cells {
        m = m.put("f", format!("q{c:05}").as_bytes(), &vec![1u8; len]);
    }
    m.commit_async()
}

/// The #149 measurement as a test: 200,000 async commits against a shard that is not
/// running. The shard holds at most the bound; the rest wait in their futures, and once
/// the shard runs they all commit, in the order they were made.
#[test]
fn a_stopped_shard_holds_at_most_the_bound_and_every_waiter_commits_in_order() {
    let (db, mut shard, t) = open(2101);
    let n = 200_000;
    let mut futs: Vec<CommitFuture> = (0..n)
        .map(|i| commit(&t, &format!("r{i:06}"), 1, 256))
        .collect();
    // Polled newest first: arrival order decides admission, not poll order.
    for f in futs.iter_mut().rev() {
        assert!(poll(f).is_pending());
    }
    let held = db.async_commit_inflight()[0];
    assert!(
        held <= BOUND && held > BOUND - 1024,
        "the stopped shard holds {held} bytes against a bound of {BOUND}"
    );
    // About 3,600 commits fit the bound. Each round polls the oldest unresolved ones (a
    // window wider than that), oldest first, as an executor polls woken tasks: an admitted
    // waiter wakes the next, which is polled next.
    let mut seqnos = vec![0u64; n];
    let (mut next, mut rounds) = (0, 0);
    while next < n {
        shard.run_once(Duration::from_millis(1));
        let end = (next + 8_192).min(n);
        for i in next..end {
            if seqnos[i] == 0
                && let Poll::Ready(r) = poll(&mut futs[i])
            {
                seqnos[i] = r.unwrap().seqno;
            }
        }
        while next < n && seqnos[next] != 0 {
            next += 1;
        }
        assert!(db.async_commit_inflight()[0] <= BOUND);
        rounds += 1;
        assert!(rounds < 20_000, "{} commits never resolved", n - next);
    }
    assert!(
        seqnos.windows(2).all(|w| w[0] < w[1]),
        "waiters were submitted out of arrival order"
    );
    assert_eq!(db.async_commit_inflight()[0], 0, "every charge returned");
    assert_eq!(t.scan_prefix(b"r").iter().unwrap().count(), n);
    close(db, shard);
}

/// A small commit that would fit does not overtake a larger one waiting ahead of it.
#[test]
fn a_small_commit_waits_behind_an_earlier_waiting_one() {
    let (db, mut shard, t) = open(2102);
    // 1.5 MiB in flight, then 1 MiB (does not fit) and 1 KiB (would fit).
    let mut a = commit(&t, "a", 150, 10 << 10);
    let mut b = commit(&t, "b", 100, 10 << 10);
    let mut s = commit(&t, "s", 1, 1 << 10);
    assert!(poll(&mut s).is_pending());
    assert!(poll(&mut b).is_pending());
    let a_bytes = db.async_commit_inflight()[0];
    assert!(a_bytes > 1_500_000 && a_bytes < BOUND, "{a_bytes}");
    let (mut ia, mut ib, mut is) = (None, None, None);
    for _ in 0..100_000 {
        shard.run_once(Duration::from_millis(1));
        if ia.is_none()
            && let Poll::Ready(r) = poll(&mut a)
        {
            ia = Some(r.unwrap().seqno);
        }
        // `s` is polled before `b` every round, and still goes after it.
        if is.is_none()
            && let Poll::Ready(r) = poll(&mut s)
        {
            is = Some(r.unwrap().seqno);
        }
        if ib.is_none()
            && let Poll::Ready(r) = poll(&mut b)
        {
            ib = Some(r.unwrap().seqno);
        }
        if ia.is_some() && ib.is_some() && is.is_some() {
            break;
        }
    }
    let (ia, ib, is) = (ia.unwrap(), ib.unwrap(), is.unwrap());
    assert!(ia < ib && ib < is, "a {ia}, b {ib}, s {is}");
    close(db, shard);
}

/// Blocking commits take no admission: one made while async commits wait behind a full
/// bound is submitted at once and applied before them.
#[test]
fn a_blocking_commit_is_not_held_behind_waiting_async_ones() {
    let (db, mut shard, t) = open(2103);
    let mut full = commit(&t, "full", 190, 10 << 10);
    let mut waiting = commit(&t, "waiting", 100, 10 << 10);
    assert!(poll(&mut waiting).is_pending());
    let held = db.async_commit_inflight()[0];
    let blocking = {
        let t = t.clone();
        std::thread::spawn(move || {
            t.mutate(b"blocking")
                .put("f", b"q", b"v")
                .commit()
                .unwrap()
                .seqno
        })
    };
    // The blocking commit is charged nothing.
    let mut blocking_seqno = None;
    let (mut full_seqno, mut waiting_seqno) = (None, None);
    for _ in 0..100_000 {
        assert!(db.async_commit_inflight()[0] <= held.max(BOUND));
        shard.run_once(Duration::from_millis(1));
        if full_seqno.is_none()
            && let Poll::Ready(r) = poll(&mut full)
        {
            full_seqno = Some(r.unwrap().seqno);
        }
        if waiting_seqno.is_none()
            && let Poll::Ready(r) = poll(&mut waiting)
        {
            waiting_seqno = Some(r.unwrap().seqno);
        }
        if blocking_seqno.is_none() && blocking.is_finished() {
            blocking_seqno = Some(0);
        }
        if full_seqno.is_some() && waiting_seqno.is_some() && blocking_seqno.is_some() {
            break;
        }
    }
    let blocking = blocking.join().unwrap();
    assert!(
        blocking < waiting_seqno.unwrap(),
        "the blocking commit ({blocking}) waited behind the async one ({:?})",
        waiting_seqno
    );
    close(db, shard);
}

/// A future dropped while it waits for room submits nothing, and the next waiter moves up.
#[test]
fn a_dropped_waiting_commit_writes_nothing() {
    let (db, mut shard, t) = open(2104);
    let mut full = commit(&t, "full", 190, 10 << 10);
    let mut dropped = commit(&t, "dropped", 100, 10 << 10);
    let mut next = commit(&t, "next", 1, 16);
    assert!(poll(&mut dropped).is_pending());
    assert!(poll(&mut next).is_pending());
    drop(dropped);
    let (mut f, mut x) = (false, false);
    for _ in 0..100_000 {
        shard.run_once(Duration::from_millis(1));
        f |= poll(&mut full).is_ready();
        x |= poll(&mut next).is_ready();
        if f && x {
            break;
        }
    }
    assert!(f && x);
    assert!(t.get(b"dropped", "f", b"q00000").unwrap().is_none());
    assert!(t.get(b"next", "f", b"q00000").unwrap().is_some());
    assert_eq!(db.async_commit_inflight()[0], 0);
    close(db, shard);
}
