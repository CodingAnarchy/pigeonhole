//! D292 (#749): a submitted read the page cache holds is copied on the submitting thread
//! (`preadv2` with `RWF_NOWAIT`) and resolves before `submit_read` returns; a miss, a short
//! read, a read over the length cap, a file system that refuses the flag or an open breaker
//! goes the submitted way as before. Linux only. Each backend has its own switch and
//! counters, so the cases run side by side.
#![cfg(all(target_os = "linux", not(miri)))]

mod common;

use common::Backend;
use pigeonhole_io::{FileRef, IoBuf, NowaitReads, own_io_in_flight, reap_own_io};

/// A file of `len` bytes whose byte at `i` is `i % 251`, written through the page cache.
fn pattern_file(b: &Backend, len: usize) -> FileRef {
    let f = b.create("f");
    let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    f.write_at(&bytes, 0).expect("write");
    f
}

fn pattern(offset: u64, len: usize) -> Vec<u8> {
    (offset..offset + len as u64)
        .map(|i| (i % 251) as u8)
        .collect()
}

fn stats(b: &Backend) -> NowaitReads {
    b.vfs
        .nowait_reads()
        .expect("the Linux backends have inline reads")
}

fn backends(tag: &str) -> [Backend; 2] {
    [Backend::pread(tag), Backend::uring(tag)]
}

/// Whether this backend's file system refused the flag (the podman VM's overlay mount
/// does): the hit cases then check only that reads stay correct and nothing retries.
fn refused(b: &Backend) -> bool {
    stats(b).refused > 0
}

#[test]
fn a_read_the_page_cache_holds_resolves_inline_with_the_same_bytes() {
    for b in backends("nowait-hit") {
        let f = pattern_file(&b, 64 * 1024);
        for offset in [0u64, 4096, 12_345, 60 * 1024] {
            let before = stats(&b);
            let done = f.submit_read(f.read_buf(4096), offset);
            let ready = done.is_ready();
            let got = done.wait().expect("read");
            assert_eq!(
                &got[..],
                &pattern(offset, 4096)[..],
                "{} at {offset}",
                b.name
            );
            let after = stats(&b);
            if refused(&b) {
                assert!(
                    after.refused <= 1,
                    "{}: a handle tried again after a refusal",
                    b.name
                );
                continue;
            }
            assert_eq!(
                after.hits,
                before.hits + 1,
                "{} at {offset}: not a hit",
                b.name
            );
            assert_eq!(after.misses, before.misses, "{} at {offset}", b.name);
            assert!(
                ready,
                "{} at {offset}: a hit did not resolve inline",
                b.name
            );
        }
    }
}

#[test]
fn a_read_past_the_end_fails_as_it_did_and_counts_a_timed_miss() {
    for b in backends("nowait-eof") {
        let f = pattern_file(&b, 64 * 1024);
        b.vfs.set_nowait_reads(false);
        let off = f
            .submit_read(f.read_buf(4096), 62 * 1024)
            .wait()
            .expect_err("a read past the end fails")
            .kind;
        b.vfs.set_nowait_reads(true);
        let before = stats(&b);
        let on = f
            .submit_read(f.read_buf(4096), 62 * 1024)
            .wait()
            .expect_err("a read past the end fails")
            .kind;
        assert_eq!(on, off, "{}", b.name);
        let after = stats(&b);
        assert_eq!(
            after.hits, before.hits,
            "{}: a short read was taken as a hit",
            b.name
        );
        if !refused(&b) {
            assert_eq!(after.misses, before.misses + 1, "{}", b.name);
            assert_eq!(
                after.miss_buckets.iter().sum::<u64>(),
                after.misses,
                "{}: every miss lands in a bucket",
                b.name
            );
        }
    }
}

/// Drops the page cache's copy of `len` bytes at `offset` (0: to the end) of the file at
/// `path` (clean pages: the file was synced).
fn evict(path: &std::path::Path, offset: i64, len: i64) {
    use std::os::fd::AsRawFd;
    let f = std::fs::File::open(path).expect("open");
    // SAFETY: an advisory call on a descriptor open for the call.
    let r = unsafe { libc::posix_fadvise(f.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED) };
    assert_eq!(r, 0, "posix_fadvise");
}

#[test]
fn a_read_the_page_cache_dropped_is_a_timed_miss() {
    for b in backends("nowait-cold") {
        let f = pattern_file(&b, 64 * 1024);
        f.sync_data().expect("sync");
        // The whole file: xfs caches it in large folios, which a partial range can't drop.
        evict(&b.path("f"), 0, 0);
        let before = stats(&b);
        let got = f
            .submit_read(f.read_buf(16 * 1024), 24 * 1024)
            .wait()
            .expect("read");
        assert_eq!(&got[..], &pattern(24 * 1024, 16 * 1024)[..], "{}", b.name);
        let after = stats(&b);
        if after.refused > 0 {
            continue;
        }
        assert_eq!(
            after.hits, before.hits,
            "{}: an uncached read was a hit",
            b.name
        );
        assert_eq!(after.misses, before.misses + 1, "{}", b.name);
        assert_eq!(
            after.miss_buckets.iter().sum::<u64>(),
            after.misses,
            "{}",
            b.name
        );
        assert!(
            after.miss_nanos > before.miss_nanos,
            "{}: the miss is timed",
            b.name
        );
    }
}

#[test]
fn switched_off_every_read_is_submitted() {
    for b in backends("nowait-off") {
        let f = pattern_file(&b, 64 * 1024);
        b.vfs.set_nowait_reads(false);
        let before = stats(&b);
        let got = f.submit_read(f.read_buf(4096), 4096).wait().expect("read");
        assert_eq!(&got[..], &pattern(4096, 4096)[..], "{}", b.name);
        let after = stats(&b);
        assert!(!after.on, "{}", b.name);
        assert_eq!(
            (after.hits, after.misses),
            (before.hits, before.misses),
            "{}",
            b.name
        );
    }
}

#[test]
fn a_read_over_the_length_cap_is_submitted() {
    for b in backends("nowait-cap") {
        let f = pattern_file(&b, 1 << 20);
        let before = stats(&b);
        let got = f
            .submit_read(f.read_buf(512 << 10), 4096)
            .wait()
            .expect("read");
        assert_eq!(&got[..], &pattern(4096, 512 << 10)[..], "{}", b.name);
        let after = stats(&b);
        assert_eq!(
            (after.hits, after.misses),
            (before.hits, before.misses),
            "{}",
            b.name
        );
        // At the cap it is still tried.
        let got = f
            .submit_read(f.read_buf(256 << 10), 0)
            .wait()
            .expect("read");
        assert_eq!(&got[..], &pattern(0, 256 << 10)[..], "{}", b.name);
        if !refused(&b) {
            assert_eq!(stats(&b).hits, after.hits + 1, "{}", b.name);
        }
    }
}

#[test]
fn misses_with_no_hit_open_the_files_breaker() {
    for b in backends("nowait-break") {
        let f = pattern_file(&b, 64 * 1024);
        if refused(&b) {
            continue;
        }
        // Reads past the end come up short: misses, with no hit between them.
        let before = stats(&b);
        for _ in 0..64 {
            let _ = f.submit_read(f.read_buf(4096), 62 * 1024).wait();
        }
        if refused(&b) {
            continue;
        }
        let after = stats(&b);
        assert_eq!(after.misses, before.misses + 64, "{}", b.name);
        assert_eq!(
            after.breaks,
            before.breaks + 1,
            "{}: 64 misses open it",
            b.name
        );
        // Open: a cached read is submitted without trying, still with the same bytes.
        let got = f.submit_read(f.read_buf(4096), 0).wait().expect("read");
        assert_eq!(&got[..], &pattern(0, 4096)[..], "{}", b.name);
        let open = stats(&b);
        assert_eq!(
            (open.hits, open.misses),
            (after.hits, after.misses),
            "{}",
            b.name
        );
        // Another file of the same backend keeps trying.
        let g = b.create("g");
        g.write_at(&[5; 4096], 0).expect("write");
        let got = g.submit_read(g.read_buf(4096), 0).wait().expect("read");
        assert!(got.iter().all(|&x| x == 5));
        assert_eq!(
            stats(&b).hits,
            open.hits + 1,
            "{}: the breaker is per file",
            b.name
        );
    }
}

#[test]
fn a_hit_beside_a_threads_own_ring_io_leaves_that_io_to_complete() {
    // perf287's N4: a thread with operations in flight on its own ring takes a hit inline;
    // the hit doesn't touch the ring, and the operations complete as before.
    let b = Backend::uring("nowait-own-ring");
    let f = pattern_file(&b, 64 * 1024);
    let vfs = std::sync::Arc::clone(&b.vfs);
    let (before, after, in_flight_before, in_flight_after, ready, write) =
        std::thread::spawn(move || {
            vfs.attach_thread();
            let mut buf = IoBuf::zeroed(4096);
            buf.fill(9);
            let write = f.submit_write(buf, 128 * 1024);
            let in_flight_before = own_io_in_flight();
            let before = vfs.nowait_reads().unwrap();
            let read = f.submit_read(f.read_buf(4096), 8192);
            let ready = read.is_ready();
            let in_flight_after = own_io_in_flight();
            let after = vfs.nowait_reads().unwrap();
            let got = read.wait().expect("read");
            assert_eq!(&got[..], &pattern(8192, 4096)[..]);
            let write = write.wait();
            while own_io_in_flight() {
                reap_own_io(Some(std::time::Duration::from_millis(10)));
            }
            (
                before,
                after,
                in_flight_before,
                in_flight_after,
                ready,
                write,
            )
        })
        .join()
        .unwrap();
    write.expect("the ring's write completes");
    assert!(
        in_flight_before,
        "the write is not on the thread's own ring"
    );
    assert!(in_flight_after, "the hit drained or disturbed the ring");
    if after.refused == 0 {
        assert_eq!(after.hits, before.hits + 1, "the read was not a hit");
        assert!(ready, "the hit did not resolve inline");
    }
}
