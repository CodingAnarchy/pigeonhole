//! #694 PR 3: `Pigeonhole::transact` and `RetryPolicy` (ICR 0060), and the transaction
//! counters in `EngineMetrics` (ICR 0061).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pigeonhole::{Error, ErrorCode, Family, Options, Pigeonhole, RetryPolicy, Table};
use pigeonhole_io::sim::SimVfs;

fn db() -> (Pigeonhole, Table) {
    let vfs = SimVfs::new(694);
    let db = Pigeonhole::open(
        "/db/t.phdb",
        Options::default()
            .vfs(vfs as _)
            .shards(2)
            .memtable_budget(4 << 20)
            .wal_segment_size(256 << 10),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    t.mutate(b"n").put("f", b"q", b"0").commit().unwrap();
    (db, t)
}

fn number(txn: &mut pigeonhole::Transaction, t: &Table, row: &[u8]) -> pigeonhole::Result<u64> {
    let cell = txn.get(t, row, "f", b"q")?;
    Ok(cell.map_or(0, |c| {
        std::str::from_utf8(c.value()).unwrap().parse().unwrap()
    }))
}

#[test]
fn a_conflicting_attempt_is_retried_and_counted() {
    let (db, t) = db();
    let attempts = AtomicU32::new(0);
    let (value, info) = db
        .transact(&RetryPolicy::default(), |txn| {
            let n = number(txn, &t, b"n")?;
            if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                // Another writer changes what the first attempt read.
                t.mutate(b"n").put("f", b"q", b"10").commit()?;
            }
            txn.put(&t, b"n", "f", b"q", (n + 1).to_string().as_bytes());
            Ok::<_, Error>(n)
        })
        .unwrap();
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
    assert_eq!(value, 10, "the retry read the interfering write");
    assert!(info.seqno > 0);
    assert_eq!(t.get(b"n", "f", b"q").unwrap().unwrap().value(), b"11");
    let m = db.engine_metrics();
    assert_eq!(
        (
            m.transactions,
            m.transaction_conflicts,
            m.transaction_retries
        ),
        (1, 1, 1)
    );
    db.close().unwrap();
}

#[test]
fn retrying_stops_at_the_attempt_cap_or_the_deadline() {
    let (db, t) = db();
    // Every attempt is interfered with.
    for policy in [
        RetryPolicy::default().max_attempts(3),
        RetryPolicy::default()
            .unlimited_attempts()
            .deadline(Duration::from_millis(30))
            .backoff(Duration::from_millis(1), Duration::from_millis(5)),
    ] {
        let attempts = AtomicU32::new(0);
        let err = db
            .transact(&policy, |txn| {
                attempts.fetch_add(1, Ordering::Relaxed);
                let n = number(txn, &t, b"n")?;
                t.mutate(b"n").put("f", b"q", b"99").commit()?;
                txn.put(&t, b"n", "f", b"q", (n + 1).to_string().as_bytes());
                Ok::<_, Error>(())
            })
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Conflict, "{policy:?}");
        let n = attempts.load(Ordering::Relaxed);
        assert!(n >= 2, "{policy:?}: {n} attempts");
        if format!("{policy:?}").contains("max_attempts: Some(3)") {
            assert_eq!(n, 3);
        }
    }
    db.close().unwrap();
}

/// An application's own error type, giving up a transaction.
#[derive(Debug, PartialEq)]
enum AppError {
    Overdrawn,
    Db(ErrorCode),
}

impl From<Error> for AppError {
    fn from(e: Error) -> Self {
        AppError::Db(e.code())
    }
}

#[test]
fn the_applications_own_error_ends_the_call() {
    let (db, t) = db();
    let attempts = AtomicU32::new(0);
    let err = db
        .transact(&RetryPolicy::default(), |txn| {
            attempts.fetch_add(1, Ordering::Relaxed);
            let n = number(txn, &t, b"n")?;
            txn.put(&t, b"n", "f", b"q", b"-1");
            if n < 5 {
                return Err(AppError::Overdrawn);
            }
            Ok(())
        })
        .unwrap_err();
    assert_eq!(err, AppError::Overdrawn);
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
    assert_eq!(
        t.get(b"n", "f", b"q").unwrap().unwrap().value(),
        b"0",
        "nothing committed"
    );
    // Nothing was committed and nothing counted as a transaction.
    let m = db.engine_metrics();
    assert_eq!((m.transactions, m.transaction_retries), (0, 0));
    // A conflict that runs out of attempts reaches the application through its `From`.
    let err = db
        .transact(&RetryPolicy::default().max_attempts(1), |txn| {
            let n = number(txn, &t, b"n")?;
            t.mutate(b"n").put("f", b"q", b"3").commit()?;
            txn.put(&t, b"n", "f", b"q", (n + 1).to_string().as_bytes());
            Ok::<_, AppError>(())
        })
        .unwrap_err();
    assert_eq!(err, AppError::Db(ErrorCode::Conflict));
    db.close().unwrap();
}

#[test]
fn concurrent_increments_lose_no_update() {
    let (db, t) = db();
    let (db, t) = (Arc::new(db), Arc::new(t));
    let threads = 6;
    let each = 40;
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let (db, t) = (Arc::clone(&db), Arc::clone(&t));
            std::thread::spawn(move || {
                for _ in 0..each {
                    db.transact(&RetryPolicy::default(), |txn| {
                        let n = number(txn, &t, b"n")?;
                        txn.put(&t, b"n", "f", b"q", (n + 1).to_string().as_bytes());
                        Ok::<_, Error>(())
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let total = (threads * each) as u64;
    assert_eq!(
        t.get(b"n", "f", b"q").unwrap().unwrap().value(),
        total.to_string().as_bytes()
    );
    let m = db.engine_metrics();
    assert_eq!(m.transactions, total);
    // Every conflict was retried (and nothing else failed).
    assert_eq!(m.transaction_conflicts, m.transaction_retries);
    drop(t);
    Arc::try_unwrap(db).ok().unwrap().close().unwrap();
}

#[test]
fn the_policy_bounds_its_pauses() {
    let p = RetryPolicy::default();
    assert_eq!(p.delay(1), Duration::ZERO);
    let p = p.backoff(Duration::from_micros(10), Duration::from_micros(80));
    for attempt in 2..40 {
        let bound = Duration::from_micros((10u64 << (attempt - 2).min(10)).min(80));
        for _ in 0..20 {
            assert!(p.delay(attempt) < bound, "attempt {attempt}");
        }
    }
}

#[cfg(feature = "async")]
#[test]
fn an_async_transaction_commit_is_counted() {
    let (db, t) = db();
    let mut txn = db.transaction().unwrap();
    let _ = txn.get(&t, b"n", "f", b"q").unwrap();
    t.mutate(b"n").put("f", b"q", b"5").commit().unwrap();
    txn.put(&t, b"n", "f", b"q", b"6");
    let err = futures_executor_block_on(txn.commit_async()).unwrap_err();
    assert_eq!(err.code(), ErrorCode::Conflict);
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"n", "f", b"q", b"7");
    futures_executor_block_on(txn.commit_async()).unwrap();
    let m = db.engine_metrics();
    assert_eq!((m.transactions, m.transaction_conflicts), (1, 1));
    db.close().unwrap();
}

/// A minimal executor: polls with a thread-parking waker.
#[cfg(feature = "async")]
fn futures_executor_block_on<F: std::future::Future>(f: F) -> F::Output {
    use std::task::{Context, Poll, Wake, Waker};
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut f = std::pin::pin!(f);
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::park(),
        }
    }
}

#[test]
fn next_pause_is_the_whole_decision() {
    // What `transact` decides, reachable without its closure (C ABI, async callers).
    let p = RetryPolicy::default()
        .max_attempts(3)
        .deadline(Duration::from_millis(50))
        .backoff(Duration::from_micros(10), Duration::from_micros(40));
    let none = Duration::ZERO;
    assert!(p.next_pause(ErrorCode::Conflict, 1, none).is_some());
    assert!(p.next_pause(ErrorCode::SnapshotExpired, 2, none).is_some());
    assert_eq!(p.next_pause(ErrorCode::Conflict, 3, none), None, "the cap");
    assert_eq!(
        p.next_pause(ErrorCode::Conflict, 1, Duration::from_millis(50)),
        None,
        "the deadline"
    );
    assert_eq!(p.next_pause(ErrorCode::Busy, 1, none), None);
    assert!(
        p.clone()
            .retry_busy(true)
            .next_pause(ErrorCode::Busy, 1, none)
            .is_some()
    );
    assert_eq!(p.next_pause(ErrorCode::Io, 1, none), None);
    // A pause never runs past the deadline.
    let near = p
        .next_pause(ErrorCode::Conflict, 2, Duration::from_micros(49_995))
        .unwrap();
    assert!(near <= Duration::from_micros(5));
    assert!(p.retries_code(ErrorCode::Conflict) && !p.retries_code(ErrorCode::Corruption));
}
