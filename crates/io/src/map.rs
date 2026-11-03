//! Writable file mappings (D312, [`File::map_write`](crate::File::map_write)).

#[cfg(any(target_os = "linux", test))]
use std::ptr::NonNull;

#[cfg(any(target_os = "linux", test))]
use crate::{Error, ErrorKind, Result};

/// Copies `bytes` to `at` within the `len` bytes at `base`, after checking they fit. The one
/// place a mapped store's `unsafe` lives: the real mapping calls it, and Miri runs it over heap
/// memory (`tests`).
///
/// # Safety
/// `base..base + len` must be valid for writes for the call, and nothing else may read or write
/// it during the call.
#[cfg(any(target_os = "linux", test))]
pub(crate) unsafe fn store(base: NonNull<u8>, len: usize, at: usize, bytes: &[u8]) -> Result<()> {
    if at.checked_add(bytes.len()).is_none_or(|end| end > len) {
        return Err(Error::new(ErrorKind::Other, "a store beyond the mapping"));
    }
    // SAFETY: `[at, at + bytes.len())` is inside `base..base + len` (checked above), which the
    // caller guarantees is writable and unaliased; `bytes` is a separate borrow, so the two
    // can't overlap.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), base.as_ptr().add(at), bytes.len());
    }
    Ok(())
}

/// A mapping shared between its [`FileWriteMap`] and the populate helper ([`populate`]),
/// unmapped when the last of them lets go.
#[cfg(target_os = "linux")]
#[derive(Debug)]
struct Shared(crate::os::Mapping);

// SAFETY: plain shared memory, owned here. Its bytes are written only through
// `FileWriteMap::write(&mut self)`, so by one thread at a time; the populate helper only passes
// the range to `madvise`, which never reads or writes a byte of it, and everything else reads
// only the pointer and length.
#[cfg(target_os = "linux")]
unsafe impl Send for Shared {}
// SAFETY: as for `Send`: shared references expose only the pointer, the length and `madvise`.
#[cfg(target_os = "linux")]
unsafe impl Sync for Shared {}

/// A real file's shared mapping ([`crate::os::map_range`]). Creating one hands the mapping to
/// the populate helper, which faults its pages in writable in the background (D312, owner call
/// 3), so the stream's stores mostly find them faulted already.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct FileWriteMap {
    mapping: std::sync::Arc<Shared>,
}

#[cfg(target_os = "linux")]
impl FileWriteMap {
    pub(crate) fn new(mapping: crate::os::Mapping) -> Self {
        let mapping = std::sync::Arc::new(Shared(mapping));
        populate::queue(&mapping);
        Self { mapping }
    }
}

#[cfg(target_os = "linux")]
impl crate::WriteMap for FileWriteMap {
    fn len(&self) -> usize {
        self.mapping.0.len()
    }

    fn write(&mut self, at: usize, bytes: &[u8]) -> Result<()> {
        // SAFETY: the mapping is live while `self` holds it, and `&mut self` excludes every
        // other store through it; the populate helper never reads or writes its bytes (only
        // `madvise`). Other processes see the same page-cache pages, as with `write_at`; they
        // read through the file, never through Rust references to this memory.
        unsafe { store(self.mapping.0.ptr(), self.mapping.0.len(), at, bytes) }
    }
}

/// The populate helper (D312, owner call 3): one thread per process, started by the first
/// mapped segment, that faults each new mapping in writable (`MADV_POPULATE_WRITE`) in
/// 2 MiB steps, ahead of the stream's stores. Off the stream's thread: a 64 MiB segment takes
/// milliseconds to populate, which the writer would otherwise pay page by page in its tail.
/// It holds the mapping while it works, so a rollover can't unmap it underneath; it skips the
/// rest of a mapping its stream has already dropped. On a kernel without the advice (before
/// 5.14) it stops for good, and the stores fault as before.
#[cfg(target_os = "linux")]
mod populate {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc::{Sender, channel};
    use std::sync::{Arc, Mutex, OnceLock};

    use super::Shared;

    /// Bytes per `madvise`: short steps, so a dropped mapping is let go soon.
    const STEP: usize = 2 << 20;

    static HELPER: OnceLock<Option<Mutex<Sender<Arc<Shared>>>>> = OnceLock::new();
    /// The kernel refused the advice: populating is off for the process.
    static UNSUPPORTED: AtomicBool = AtomicBool::new(false);
    /// Bytes populated, for tests (an inspection counter).
    pub(super) static POPULATED: AtomicU64 = AtomicU64::new(0);

    /// Hands `mapping` to the helper, starting it the first time. A helper that couldn't start
    /// leaves the mapping as it is.
    pub(super) fn queue(mapping: &Arc<Shared>) {
        if UNSUPPORTED.load(Ordering::Relaxed) {
            return;
        }
        let helper = HELPER.get_or_init(|| {
            let (tx, rx) = channel::<Arc<Shared>>();
            std::thread::Builder::new()
                .name("pigeonhole-io-populate".into())
                .spawn(move || {
                    while let Ok(m) = rx.recv() {
                        run(&m);
                    }
                })
                .ok()
                .map(|_| Mutex::new(tx))
        });
        if let Some(tx) = helper {
            let _ = tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .send(Arc::clone(mapping));
        }
    }

    /// Whether the kernel refused the advice (tests skip their check then).
    #[cfg(test)]
    pub(super) fn unsupported() -> bool {
        UNSUPPORTED.load(Ordering::Relaxed)
    }

    fn run(m: &Arc<Shared>) {
        let len = m.0.len();
        let mut at = 0;
        // Only while the stream still holds it: once it rolled over, the rest is never stored.
        while at < len && Arc::strong_count(m) > 1 && !UNSUPPORTED.load(Ordering::Relaxed) {
            let n = STEP.min(len - at);
            match crate::os::populate_write(&m.0, at, n) {
                Ok(()) => {
                    POPULATED.fetch_add(n as u64, Ordering::Relaxed);
                }
                Err(unsupported) => {
                    if unsupported {
                        UNSUPPORTED.store(true, Ordering::Relaxed);
                    }
                    return;
                }
            }
            at += n;
        }
    }
}

/// Bytes the populate helper has faulted in so far (an inspection counter for tests).
#[cfg(all(target_os = "linux", test))]
pub(crate) fn populated_bytes() -> u64 {
    populate::POPULATED.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::store;
    use std::ptr::NonNull;

    /// The store Miri checks: over a heap buffer, in bounds and at its edges, and refused past
    /// the end without touching memory.
    #[test]
    fn a_store_copies_in_bounds_and_refuses_past_the_end() {
        let mut buf = vec![0u8; 64];
        let base = NonNull::new(buf.as_mut_ptr()).unwrap();
        // SAFETY: `buf` is 64 writable bytes, borrowed by nothing else during the calls.
        unsafe {
            store(base, 64, 0, b"head").unwrap();
            store(base, 64, 60, b"tail").unwrap();
            store(base, 64, 64, b"").unwrap();
            assert!(store(base, 64, 61, b"tail").is_err());
            assert!(store(base, 64, usize::MAX, b"x").is_err());
        }
        assert_eq!(&buf[..4], b"head");
        assert_eq!(&buf[60..], b"tail");
        assert!(buf[4..60].iter().all(|&b| b == 0));
    }

    /// D312 owner call 3: a new mapping is populated in the background while it's held, and
    /// stores made meanwhile land in the file. A kernel without `MADV_POPULATE_WRITE` (before
    /// 5.14) populates nothing, which is the fallback.
    #[cfg(target_os = "linux")]
    #[test]
    #[cfg_attr(
        miri,
        ignore = "a real file's mapping and madvise: foreign calls Miri can't run"
    )]
    fn a_held_mapping_is_populated_in_the_background() {
        use crate::WriteMap;
        use std::io::{Read, Seek};
        let dir = std::env::temp_dir().join(format!("phio-populate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg");
        let len = 8usize << 20;
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(len as u64).unwrap();
        let before = super::populated_bytes();
        let mapping = crate::os::map_range(&f, 0, len).unwrap().unwrap();
        let mut map = super::FileWriteMap::new(mapping);
        map.write(len - 4, b"last").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while super::populated_bytes() < before + len as u64
            && std::time::Instant::now() < deadline
            && !super::populate::unsupported()
        {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        if !super::populate::unsupported() {
            assert!(super::populated_bytes() >= before + len as u64);
        }
        map.write(0, b"first").unwrap();
        drop(map);
        let mut got = vec![0u8; len];
        f.rewind().unwrap();
        f.read_exact(&mut got).unwrap();
        assert_eq!(&got[..5], b"first");
        assert_eq!(&got[len - 4..], b"last");
        assert!(
            got[5..len - 4].iter().all(|&b| b == 0),
            "populating writes nothing"
        );
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }
}
