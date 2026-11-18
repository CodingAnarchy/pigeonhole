//! A database's device bytes (D332, ICR 0142): the bytes its files read and write.
//!
//! [`CountingVfs`] wraps the [`Vfs`] the database opened with. Each file it opens is the
//! backend's own, given the database's counters ([`File::count_into`]): the backend counts
//! each read's and write's buffer length at its entry points. No `File` wraps another, so a
//! commit's WAL write pays a load, a branch and an add, not an extra call (the file wrapper
//! this replaced cost the commit paths about 50 instructions in #2063's callgrind run;
//! perf287 agreed to reverse their one-wrapper choice).
//!
//! What it counts (pigeonhole-io's `count` module): the length asked for, at submission, so
//! a short read at the end of a file and a failed read count in full; the readahead actually
//! issued; a write's buffer, once. Not syncs, writeback hints, allocations or punches, nor
//! what a file moved before it was given the counters. Reads the OS page cache serves count
//! too: these are the engine's own estimate of its device bytes, for comparing databases on
//! one node, never a disk utilisation.
//!
//! [`File::count_into`]: pigeonhole_io::File::count_into

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole_io::{
    Completion, FileRef, IoCounters, NowaitReads, OpenOptions, ProcessId, Result, RingStats,
    SharedOpen, SharedRegion, Vfs, VfsRef,
};

/// A [`Vfs`] whose files count their bytes into one [`IoCounters`]. Every method forwards
/// to the backend's, the defaulted ones too (perf287's must on D332 PR 1: a default taken by
/// mistake would silently change the backend's behaviour); `open` also gives the file the
/// counters.
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
        let file = self.inner.open(path, opts)?;
        file.count_into(Arc::clone(&self.counters));
        Ok(file)
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

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_io::IoBuf;
    use pigeonhole_io::sim::SimVfs;

    /// perf287's musts: the wrapper's defaulted `Vfs` getters answer as the backend's, and a
    /// file it opens counts exactly the bytes of each read and write entry point, once, with
    /// none missed or counted twice (perf287 on #2063).
    fn counts_exactly(raw: &VfsRef, dir: &Path) {
        let counters = Arc::new(IoCounters::new());
        let wrapped = CountingVfs::wrap(Arc::clone(raw), Arc::clone(&counters));
        assert_eq!(wrapped.clock_is_simulated(), raw.clock_is_simulated());
        assert_eq!(wrapped.ring_stats().is_some(), raw.ring_stats().is_some());
        assert_eq!(
            wrapped.nowait_reads().is_some(),
            raw.nowait_reads().is_some()
        );
        let f = wrapped
            .open(&dir.join("counted"), OpenOptions::read_write_create())
            .unwrap();
        f.write_at(&[7; 100], 0).unwrap();
        let mut buf = [0; 60];
        f.read_at(&mut buf, 10).unwrap();
        assert_eq!(buf, [7; 60]);
        assert_eq!(counters.totals(), (60, 100));
        let mut out = IoBuf::zeroed(50);
        out.fill(1);
        f.submit_write(out, 100).wait().unwrap();
        f.submit_read(f.read_buf(30), 100).wait().unwrap();
        assert_eq!(counters.totals(), (90, 150));
        // Readahead counts every buffer it issues, once.
        for c in f.submit_reads_ahead(vec![(f.read_buf(20), 0), (f.read_buf(10), 40)]) {
            c.wait().unwrap();
        }
        assert_eq!(counters.totals(), (120, 150));
        // A read past the end, short or failed, counts the length asked for.
        let mut far = [0; 40];
        let _ = f.read_at(&mut far, 140);
        assert_eq!(counters.totals(), (160, 150));
        // Syncs and lengths count nothing.
        f.sync_data().unwrap();
        f.len().unwrap();
        assert_eq!(counters.totals(), (160, 150));
        // The same counters given again change nothing.
        f.count_into(Arc::clone(&counters));
        f.write_at(&[1; 10], 0).unwrap();
        assert_eq!(counters.totals(), (160, 160));
    }

    #[test]
    fn every_entry_point_counts_its_bytes_once() {
        let sim: VfsRef = SimVfs::new(1);
        counts_exactly(&sim, Path::new("/"));
        let dir = std::env::temp_dir().join(format!(
            "phdb-io-count-{}-{}",
            std::process::id(),
            sim.random_u64()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let os: VfsRef = pigeonhole_io::pread::PreadVfs::new(0);
        counts_exactly(&os, &dir);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The same on io_uring, whose `submit_reads_ahead` is its own (Linux: the VM or CI).
    #[cfg(target_os = "linux")]
    #[test]
    fn every_entry_point_counts_its_bytes_once_on_io_uring() {
        let Ok(uring) = pigeonhole_io::uring::UringVfs::new() else {
            return;
        };
        let dir = std::env::temp_dir().join(format!(
            "phdb-io-count-uring-{}-{}",
            std::process::id(),
            uring.random_u64()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let uring: VfsRef = uring;
        counts_exactly(&uring, &dir);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
