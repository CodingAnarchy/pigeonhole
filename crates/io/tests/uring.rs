//! The io_uring backend's own cases (#402), beyond the parity suite: many submitting threads
//! at once, completions dropped before they resolve, teardown with operations in flight, and
//! the last handle dropped on the reaper thread. Linux only; each fails where io_uring is
//! unavailable, so a run that should cover the backend cannot pass without it.
#![cfg(all(target_os = "linux", not(miri)))]

mod common;

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use common::Backend;
use pigeonhole_io::{ErrorKind, IoBuf, OpenOptions};

/// A buffer of `len` bytes holding `byte` at every position.
fn filled(len: usize, byte: u8) -> IoBuf {
    let mut buf = IoBuf::zeroed(len);
    buf.fill(byte);
    buf
}

#[test]
fn many_threads_submit_at_once() {
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
    let b = Backend::uring("uring-errors");
    drop(b.create("f"));
    // A read-only handle: the kernel refuses the write (EBADF).
    let f = b.vfs.open(&b.path("f"), OpenOptions::read()).unwrap();
    let err = f.submit_write(filled(10, 1), 0).wait().unwrap_err();
    assert_eq!(err.kind, ErrorKind::Other, "{err}");
}

#[test]
fn dropping_the_backend_with_operations_in_flight_completes_them() {
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
