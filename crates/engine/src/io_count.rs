//! A database's device bytes (D332, ICR 0142): the bytes its files read and write, counted
//! where every one of its I/Os passes, a wrapper around the [`Vfs`] it opened with.
//!
//! [`CountingVfs`] opens [`CountingFile`]s, which forward every method of [`File`] to the
//! backend's file and add the buffer length of each read and write to the database's
//! [`IoCounters`]. Every method is forwarded, the defaulted ones too (perf287's must on
//! D332 PR 1): a default taken by mistake would silently change the backend's behaviour (its
//! registered buffers, nowait reads, ring statistics or the simulator's clock). The tests
//! compare each defaulted getter through the wrapper with the backend's.
//!
//! What it counts: the length asked for, at submission, so a short read at the end of a file
//! and a failed read count in full; the readahead actually issued; a write's buffer, once,
//! when the engine writes it. Not syncs, writeback hints, allocations or punches. Reads the
//! OS page cache serves count too: these are the engine's own estimate of its device bytes,
//! for comparing databases on one node, never a disk utilisation.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use pigeonhole_io::{
    Completion, File, FileIdentity, FileRef, IoBuf, Locality, LockMode, NowaitReads, OpenOptions,
    ProcessId, Result, RingStats, SharedOpen, SharedRegion, Vfs, VfsRef,
};

/// Stripes per counter: concurrent I/Os from different threads add to different cache lines.
const STRIPES: usize = 16;

#[derive(Default)]
#[repr(align(64))]
struct Stripe {
    read: AtomicU64,
    written: AtomicU64,
}

/// One database's read and written bytes, striped by thread. Summed only when read
/// ([`IoCounters::totals`]), never on the I/O path.
pub(crate) struct IoCounters {
    stripes: [Stripe; STRIPES],
}

impl Default for IoCounters {
    fn default() -> Self {
        Self {
            stripes: std::array::from_fn(|_| Stripe::default()),
        }
    }
}

impl fmt::Debug for IoCounters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (read, written) = self.totals();
        f.debug_struct("IoCounters")
            .field("read", &read)
            .field("written", &written)
            .finish()
    }
}

/// This thread's stripe, picked once per thread.
fn stripe() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static MINE: usize = NEXT.fetch_add(1, Ordering::Relaxed) % STRIPES;
    }
    MINE.with(|s| *s)
}

impl IoCounters {
    #[inline]
    fn read(&self, bytes: usize) {
        self.stripes[stripe()]
            .read
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    #[inline]
    fn written(&self, bytes: usize) {
        self.stripes[stripe()]
            .written
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Bytes read and written since open.
    pub(crate) fn totals(&self) -> (u64, u64) {
        self.stripes.iter().fold((0, 0), |(r, w), s| {
            (
                r + s.read.load(Ordering::Relaxed),
                w + s.written.load(Ordering::Relaxed),
            )
        })
    }
}

/// A [`Vfs`] whose files count their bytes into one [`IoCounters`].
#[derive(Debug)]
pub(crate) struct CountingVfs {
    inner: VfsRef,
    counters: Arc<IoCounters>,
}

impl CountingVfs {
    pub(crate) fn wrap(inner: VfsRef, counters: Arc<IoCounters>) -> VfsRef {
        Arc::new(Self { inner, counters })
    }
}

impl Vfs for CountingVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        let inner = self.inner.open(path, opts)?;
        Ok(Arc::new(CountingFile {
            inner,
            counters: Arc::clone(&self.counters),
        }))
    }
    fn remove(&self, path: &Path) -> Result<()> {
        self.inner.remove(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to)
    }
    fn exists(&self, path: &Path) -> Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        self.inner.list_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.inner.sync_dir(dir)
    }
    fn submit_sync_dir(&self, dir: &Path) -> Completion<()> {
        self.inner.submit_sync_dir(dir)
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos()
    }
    fn clock_is_simulated(&self) -> bool {
        self.inner.clock_is_simulated()
    }
    fn current_process(&self) -> ProcessId {
        self.inner.current_process()
    }
    fn attach_thread(&self) {
        self.inner.attach_thread();
    }
    fn ring_stats(&self) -> Option<RingStats> {
        self.inner.ring_stats()
    }
    fn set_nowait_reads(&self, on: bool) {
        self.inner.set_nowait_reads(on);
    }
    fn nowait_reads(&self) -> Option<NowaitReads> {
        self.inner.nowait_reads()
    }
    fn random_u64(&self) -> u64 {
        self.inner.random_u64()
    }
}

/// A backend's file that counts its reads and writes.
#[derive(Debug)]
pub(crate) struct CountingFile {
    inner: FileRef,
    counters: Arc<IoCounters>,
}

impl File for CountingFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.counters.read(buf.len());
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.counters.written(buf.len());
        self.inner.write_at(buf, offset)
    }
    fn direct_align(&self) -> Option<usize> {
        self.inner.direct_align()
    }
    fn read_buf(&self, len: usize) -> IoBuf {
        self.inner.read_buf(len)
    }
    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion {
        self.counters.read(buf.len());
        self.inner.submit_read(buf, offset)
    }
    fn submit_reads_ahead(&self, reads: Vec<(IoBuf, u64)>) -> Vec<Completion> {
        self.counters.read(reads.iter().map(|(b, _)| b.len()).sum());
        self.inner.submit_reads_ahead(reads)
    }
    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        self.counters.written(buf.len());
        self.inner.submit_write(buf, offset)
    }
    fn start_writeback(&self, offset: u64, len: u64) -> Result<()> {
        self.inner.start_writeback(offset, len)
    }
    fn sync_data(&self) -> Result<()> {
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> Completion<()> {
        self.inner.submit_sync_data()
    }
    fn sync_all(&self) -> Result<()> {
        self.inner.sync_all()
    }
    fn submit_sync_all(&self) -> Completion<()> {
        self.inner.submit_sync_all()
    }
    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
    fn is_empty(&self) -> Result<bool> {
        self.inner.is_empty()
    }
    fn set_len(&self, len: u64) -> Result<()> {
        self.inner.set_len(len)
    }
    fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        self.inner.allocate(offset, len)
    }
    fn punch_hole(&self, offset: u64, len: u64) -> Result<()> {
        self.inner.punch_hole(offset, len)
    }
    fn lock(&self, byte: u64, mode: LockMode) -> Result<()> {
        self.inner.lock(byte, mode)
    }
    fn unlock(&self, byte: u64) -> Result<()> {
        self.inner.unlock(byte)
    }
    fn holds(&self, byte: u64) -> Option<LockMode> {
        self.inner.holds(byte)
    }
    fn identity(&self) -> Result<FileIdentity> {
        self.inner.identity()
    }
    fn is_local(&self) -> Result<bool> {
        self.inner.is_local()
    }
    fn locality(&self) -> Result<Locality> {
        self.inner.locality()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_io::sim::SimVfs;

    /// perf287's must: every defaulted method answers through the wrapper as the backend
    /// does, on the simulator and on the OS backend.
    fn same_defaults(raw: &VfsRef, dir: &Path) {
        let counters = Arc::new(IoCounters::default());
        let wrapped = CountingVfs::wrap(Arc::clone(raw), Arc::clone(&counters));
        assert_eq!(wrapped.clock_is_simulated(), raw.clock_is_simulated());
        assert_eq!(wrapped.ring_stats().is_some(), raw.ring_stats().is_some());
        assert_eq!(
            wrapped.nowait_reads().is_some(),
            raw.nowait_reads().is_some()
        );
        let opts = OpenOptions::read_write_create();
        let path = dir.join("counted");
        let f = wrapped.open(&path, opts).unwrap();
        let g = raw.open(&path, opts).unwrap();
        assert_eq!(f.direct_align(), g.direct_align());
        assert_eq!(f.locality().unwrap(), g.locality().unwrap());
        assert_eq!(f.read_buf(4096).capacity(), g.read_buf(4096).capacity());
        f.write_at(&[7; 100], 0).unwrap();
        let mut buf = [0; 60];
        f.read_at(&mut buf, 10).unwrap();
        assert_eq!(buf, [7; 60]);
        assert_eq!(counters.totals(), (60, 100));
        let mut out = IoBuf::zeroed(50);
        out.fill(1);
        let done = f.submit_write(out, 100);
        done.wait().unwrap();
        let back = f.submit_read(f.read_buf(30), 100);
        back.wait().unwrap();
        assert_eq!(counters.totals(), (90, 150));
        // Readahead counts every buffer it issues.
        for c in f.submit_reads_ahead(vec![(f.read_buf(20), 0), (f.read_buf(10), 40)]) {
            c.wait().unwrap();
        }
        assert_eq!(counters.totals(), (120, 150));
        // A read past the end, short or failed, counts the length asked for.
        let mut far = [0; 40];
        let _ = f.read_at(&mut far, 140);
        assert_eq!(counters.totals(), (160, 150));
        // Syncs, lengths and punches count nothing.
        f.sync_data().unwrap();
        f.len().unwrap();
        assert_eq!(counters.totals(), (160, 150));
    }

    #[test]
    fn the_wrapper_forwards_every_default_and_counts_reads_and_writes() {
        let sim: VfsRef = SimVfs::new(1);
        same_defaults(&sim, Path::new("/"));
        let dir = std::env::temp_dir().join(format!(
            "phdb-io-count-{}-{}",
            std::process::id(),
            sim.random_u64()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let os: VfsRef = pigeonhole_io::pread::PreadVfs::new(0);
        same_defaults(&os, &dir);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
