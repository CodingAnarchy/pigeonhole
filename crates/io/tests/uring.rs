//! The io_uring backend's own cases (#402), beyond the parity suite: many submitting threads
//! at once, completions dropped before they resolve, teardown with operations in flight, and
//! the last handle dropped on the reaper thread. Linux only; each fails where io_uring is
//! unavailable, so a run that should cover the backend cannot pass without it.
#![cfg(all(target_os = "linux", not(miri)))]

mod common;

use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::Duration;

use common::Backend;
use pigeonhole_io::{ErrorKind, IoBuf, OpenOptions};

/// One test at a time: every ring registers buffers against the user's locked-memory limit
/// (RLIMIT_MEMLOCK), which rings of tests running side by side would exhaust, leaving the
/// tests that check registration with plain buffers.
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A buffer of `len` bytes holding `byte` at every position.
fn filled(len: usize, byte: u8) -> IoBuf {
    let mut buf = IoBuf::zeroed(len);
    buf.fill(byte);
    buf
}

#[test]
fn many_threads_submit_at_once() {
    let _serial = serial();
    let b = Backend::uring("uring-many-threads");
    let f = b.create("f");
    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let f = Arc::clone(&f);
            thread::spawn(move || {
                for i in 0..250u64 {
                    let at = (t * 250 + i) * 512;
                    f.submit_write(filled(512, (t * 31 + i) as u8), at)
                        .wait()
                        .unwrap();
                }
                f.submit_sync_data().wait().unwrap();
                for i in 0..250u64 {
                    let at = (t * 250 + i) * 512;
                    let got = f.submit_read(IoBuf::zeroed(512), at).wait().unwrap();
                    assert!(got.iter().all(|&x| x == (t * 31 + i) as u8));
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(f.len().unwrap(), 8 * 250 * 512);
}

#[test]
fn many_operations_in_flight_overflow_the_queue() {
    let _serial = serial();
    // More than the ring's 256 entries at once: submission waits for room, nothing is lost.
    let b = Backend::uring("uring-overflow");
    let f = b.create("f");
    let pending: Vec<_> = (0..2000u64)
        .map(|i| f.submit_write(filled(64, i as u8), i * 64))
        .collect();
    for c in pending {
        c.wait().unwrap();
    }
    let all = f.submit_read(IoBuf::zeroed(2000 * 64), 0).wait().unwrap();
    for (i, chunk) in all.chunks(64).enumerate() {
        assert!(chunk.iter().all(|&x| x == i as u8), "chunk {i}");
    }
}

#[test]
fn a_dropped_completion_still_completes_its_operation() {
    let _serial = serial();
    let b = Backend::uring("uring-dropped");
    let f = b.create("f");
    for i in 0..100u64 {
        drop(f.submit_write(filled(4096, 7), i * 4096));
    }
    // A sync submitted after them completes after them too only if they were not lost.
    f.submit_sync_data().wait().unwrap();
    let mut seen = 0;
    for _ in 0..1000 {
        if f.len().unwrap() == 100 * 4096 {
            seen += 1;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(seen, 1, "the dropped writes never landed");
    let got = f.submit_read(IoBuf::zeroed(100 * 4096), 0).wait().unwrap();
    assert!(got.iter().all(|&x| x == 7));
}

#[test]
fn large_reads_and_writes_complete_in_full() {
    let _serial = serial();
    let b = Backend::uring("uring-large");
    let f = b.create("f");
    let len = 9 << 20;
    let mut buf = IoBuf::zeroed(len);
    for (i, x) in buf.iter_mut().enumerate() {
        *x = (i % 251) as u8;
    }
    f.submit_write(buf, 4096).wait().unwrap();
    let got = f.submit_read(IoBuf::zeroed(len), 4096).wait().unwrap();
    assert!(got.iter().enumerate().all(|(i, &x)| x == (i % 251) as u8));
}

#[test]
fn a_read_past_the_end_fails_with_unexpected_eof() {
    let _serial = serial();
    let b = Backend::uring("uring-eof");
    let f = b.create("f");
    f.submit_write(filled(1000, 1), 0).wait().unwrap();
    // Straddling the end: the first part reads, the rest finds the end.
    let err = f.submit_read(IoBuf::zeroed(4096), 500).wait().unwrap_err();
    assert_eq!(err.kind, ErrorKind::UnexpectedEof);
    // Wholly past it.
    let err = f
        .submit_read(IoBuf::zeroed(10), 1 << 20)
        .wait()
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::UnexpectedEof);
}

#[test]
fn errors_arrive_through_the_completion() {
    let _serial = serial();
    let b = Backend::uring("uring-errors");
    drop(b.create("f"));
    // A read-only handle: the kernel refuses the write (EBADF).
    let f = b.vfs.open(&b.path("f"), OpenOptions::read()).unwrap();
    let err = f.submit_write(filled(10, 1), 0).wait().unwrap_err();
    assert_eq!(err.kind, ErrorKind::Other, "{err}");
}

#[test]
fn dropping_the_backend_with_operations_in_flight_completes_them() {
    let _serial = serial();
    let b = Backend::uring("uring-teardown");
    let f = b.create("f");
    let pending: Vec<_> = (0..64u64)
        .map(|i| f.submit_write(filled(4096, 3), i * 4096))
        .collect();
    // The backend and the file handle go while the writes may still be in flight; their
    // completions still resolve, and the ring is torn down only after them.
    let Backend { vfs, .. } = b;
    drop(vfs);
    drop(f);
    for c in pending {
        c.wait().unwrap();
    }
}

#[test]
fn the_last_handle_can_go_on_the_reaper_thread() {
    let _serial = serial();
    // A continuation holding the only file handle runs on the reaper thread: dropping that
    // handle there must not make the reaper wait for itself.
    let b = Backend::uring("uring-last-on-reaper");
    let f = b.create("f");
    let (tx, rx) = mpsc::channel();
    let held = Arc::clone(&f);
    let c = f.submit_sync_data().map(move |r| {
        drop(held);
        r
    });
    drop(f);
    let Backend { vfs, .. } = b;
    drop(vfs);
    thread::spawn(move || {
        tx.send(c.wait()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("the completion resolved")
        .unwrap();
}

// ---- a thread's own ring (#402 PR 3) ----

use pigeonhole_io::{own_io_in_flight, own_io_waker, reap_own_io};

#[test]
fn an_attached_thread_completes_its_own_io_by_reaping() {
    let _serial = serial();
    let b = Backend::uring("uring-own-ring");
    b.vfs.attach_thread();
    assert!(own_io_waker().is_some(), "the kernel offers a thread ring");
    let f = b.create("f");
    let write = f.submit_write(filled(4096, 9), 0);
    // This thread's I/O now: it completes only when the thread reaps or waits on it.
    assert!(own_io_in_flight() || write.is_ready());
    while !write.is_ready() {
        reap_own_io(Some(Duration::from_millis(10)));
    }
    write.wait().unwrap();
    assert!(!own_io_in_flight());
    // A blocked wait on the owner thread reaps its ring itself.
    let got = f.submit_read(IoBuf::zeroed(4096), 0).wait().unwrap();
    assert!(got.iter().all(|&x| x == 9));
    f.submit_sync_data().wait().unwrap();
}

#[test]
fn a_waker_ends_a_ring_wait_from_another_thread() {
    let _serial = serial();
    let b = Backend::uring("uring-own-wake");
    b.vfs.attach_thread();
    let waker = own_io_waker().expect("a thread ring");
    let started = std::time::Instant::now();
    let w = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        waker.wake();
    });
    // Nothing in flight: only the wake (or the long timeout) ends this wait.
    reap_own_io(Some(Duration::from_secs(10)));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the wake did not end the wait"
    );
    w.join().unwrap();
}

#[test]
fn a_thread_ending_with_io_in_flight_completes_it() {
    let _serial = serial();
    let b = Backend::uring("uring-own-exit");
    let f = b.create("f");
    let vfs = Arc::clone(&b.vfs);
    let g = Arc::clone(&f);
    let pending = thread::spawn(move || {
        vfs.attach_thread();
        (0..32u64)
            .map(|i| g.submit_write(filled(4096, 4), i * 4096))
            .collect::<Vec<_>>()
    })
    .join()
    .unwrap();
    // The thread's ring went with it, after completing what was in flight.
    for c in pending {
        c.wait().unwrap();
    }
    let got = f.submit_read(IoBuf::zeroed(32 * 4096), 0).wait().unwrap();
    assert!(got.iter().all(|&x| x == 4));
}

#[test]
fn a_non_waiting_reap_takes_finished_completions() {
    let _serial = serial();
    // A shard's turn reaps without waiting: completions the kernel has finished must be
    // taken there, not only by a waiting reap (an application-owned loop may never wait).
    let b = Backend::uring("uring-own-nowait");
    b.vfs.attach_thread();
    let f = b.create("f");
    let write = f.submit_write(filled(4096, 5), 0);
    for _ in 0..2000 {
        if write.is_ready() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
        reap_own_io(None);
    }
    assert!(
        write.is_ready(),
        "a non-waiting reap never took the completion"
    );
    write.wait().unwrap();
}

// ---- registered buffers (#402 PR 4) ----

#[test]
fn read_bufs_are_registered_and_fixed_io_round_trips() {
    let _serial = serial();
    let b = Backend::uring("uring-fixed-shared");
    let f = b.create("f");
    // The shared ring's pool (this thread has no ring of its own).
    let mut w = f.read_buf(8192);
    assert!(w.is_registered(), "the kernel accepted the registered pool");
    w.fill(0x6B);
    f.submit_write(w, 4096).wait().unwrap();
    let r = f.submit_read(f.read_buf(8192), 4096).wait().unwrap();
    assert!(r.is_registered());
    assert!(r.iter().all(|&x| x == 0x6B));
    // Kept beyond its I/O: copied out, the slot goes back.
    let kept = r.detached();
    assert!(!kept.is_registered() && kept.iter().all(|&x| x == 0x6B));
}

#[test]
fn an_attached_thread_reads_into_its_own_rings_slots() {
    let _serial = serial();
    let b = Backend::uring("uring-fixed-thread");
    b.vfs.attach_thread();
    let f = b.create("f");
    let mut w = f.read_buf(4096);
    assert!(w.is_registered());
    w.fill(0x21);
    f.submit_write(w, 0).wait().unwrap();
    let r = f.submit_read(f.read_buf(4096), 0).wait().unwrap();
    assert!(r.iter().all(|&x| x == 0x21));
    // A buffer from another ring's pool goes as a plain buffer, and works.
    let other = thread::spawn({
        let f = Arc::clone(&f);
        move || f.read_buf(4096)
    })
    .join()
    .unwrap();
    assert!(other.is_registered());
    let r = f.submit_read(other, 0).wait().unwrap();
    assert!(r.iter().all(|&x| x == 0x21));
}

#[test]
fn a_dry_pool_falls_back_to_plain_buffers() {
    let _serial = serial();
    let b = Backend::uring("uring-fixed-dry");
    let f = b.create("f");
    f.submit_write(filled(4096, 1), 0).wait().unwrap();
    // Take slots until the pool runs dry (as many as the kernel accepted, at most 16).
    let mut held = Vec::new();
    loop {
        let b = f.read_buf(4096);
        if !b.is_registered() {
            break;
        }
        held.push(b);
        assert!(held.len() <= 16, "a pool larger than its slots");
    }
    assert!(!held.is_empty(), "the kernel accepted the registered pool");
    let plain = f.read_buf(4096);
    assert!(!plain.is_registered(), "every slot is taken");
    let r = f.submit_read(plain, 0).wait().unwrap();
    assert!(r.iter().all(|&x| x == 1));
    // Longer than a slot: plain too.
    assert!(!f.read_buf(1 << 20).is_registered());
    drop(held);
    assert!(f.read_buf(4096).is_registered(), "slots came back");
}

#[test]
fn ring_stats_count_rings_and_pools() {
    let _serial = serial();
    let b = Backend::uring("uring-ring-stats");
    let f = b.create("f");
    let stats = |rings, pooled| {
        let s = b
            .vfs
            .ring_stats()
            .expect("an io_uring backend reports its rings");
        assert_eq!((s.rings, s.pooled), (rings, pooled), "{s:?}");
    };
    // A ring registers its pool on the first read that can use one, not before.
    stats(1, 0);
    assert!(!f.read_buf(1 << 20).is_registered(), "longer than a slot");
    stats(1, 0);
    drop(f.read_buf(4096));
    stats(1, 1);
    let vfs = Arc::clone(&b.vfs);
    let g = Arc::clone(&f);
    let (tx, rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let t = thread::spawn(move || {
        vfs.attach_thread();
        let before = vfs.ring_stats().unwrap();
        drop(g.read_buf(4096));
        tx.send((before, vfs.ring_stats().unwrap())).unwrap();
        done_rx.recv().unwrap();
    });
    let (attached, used) = rx.recv().unwrap();
    assert_eq!(
        (attached.rings, attached.pooled),
        (2, 1),
        "the thread's own ring, no pool yet"
    );
    // One test at a time, with the budget at half the limit: both fit a default 8 MiB.
    assert_eq!((used.rings, used.pooled), (2, 2), "both rings got pools");
    done_tx.send(()).unwrap();
    t.join().unwrap();
    stats(1, 1);
}

// ---- the completion fd (#408) ----

use pigeonhole_io::own_io_fd;

/// Whether `fd` turns readable within `ms` milliseconds.
fn readable(fd: i32, ms: i32) -> bool {
    let mut p = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one live `pollfd`.
    unsafe { libc::poll(&mut p, 1, ms) == 1 }
}

#[test]
fn the_completion_fd_turns_readable_before_any_reap() {
    // An application's event loop waits on the fd, not in the ring. A ring that runs its
    // completion work only when its owner enters it (DEFER_TASKRUN) must still signal the
    // fd when that work is queued, or such a loop would sleep through its own I/O.
    let _serial = serial();
    let b = Backend::uring("uring-own-fd");
    b.vfs.attach_thread();
    let fd = own_io_fd().expect("a thread ring offers a completion fd");
    let f = b.create("f");
    reap_own_io(None);
    assert!(!readable(fd, 0), "nothing in flight yet");
    let write = f.submit_write(filled(4096, 3), 0);
    assert!(
        readable(fd, 5000),
        "the fd did not turn readable for a completion nobody reaped"
    );
    while !write.is_ready() {
        reap_own_io(Some(Duration::from_millis(10)));
    }
    write.wait().unwrap();
    // A reap resets it; another thread (no ring of its own) has none.
    reap_own_io(None);
    assert!(!readable(fd, 0), "the reap reset it");
    assert!(thread::spawn(own_io_fd).join().unwrap().is_none());
}
