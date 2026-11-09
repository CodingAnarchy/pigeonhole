//! `Vectors`' IVF indexes (D313 ticket 7, ICR 0100): the open handles, by name, and the
//! rebalancing threads of the indexes that rebalance in the background.
//!
//! - **One handle per index**, opened on first use (on the blocking pool: an open reads the
//!   index's metadata) and kept until the server closes. The crate shares an index's centroid
//!   cache and its queue of lists to rebalance across handles in a process, so one is enough.
//! - **The server runs each background index's rebalancer itself,** on a thread of its own
//!   (`phdb-serve-rebalance`), instead of the thread `Index::open` would start: the server
//!   then knows the thread, and its shutdown waits for it ([`Indexes::close`]) before it
//!   closes the database. An index that rebalances inline needs no thread.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use pigeonhole::Pigeonhole;
use pigeonhole_vector::{Index, IndexOptions, Rebalance, Rebalancer};

use super::errors::ApiError;

/// How long a rebalancer works between looks at whether the index closed.
const SLICE: Duration = Duration::from_millis(50);

/// How long a rebalancer with nothing to do waits before it looks again.
const IDLE: Duration = Duration::from_secs(1);

pub(crate) struct Indexes {
    db: Pigeonhole,
    open: Mutex<super::handles::Handles<Index>>,
    rebalancers: Mutex<Vec<JoinHandle<()>>>,
    /// Rebalancer threads running, for tests to read.
    running: Arc<AtomicUsize>,
    /// Set by [`close`](Indexes::close), under `open`'s lock: no index opens after it, so a
    /// call still queued on the blocking pool at shutdown can't start a rebalancer nothing
    /// joins (ai1249's M2 on #1573).
    closed: AtomicBool,
    /// Where the rebalancer threads run: off the shards' CPUs (perf287's B2 on 7b2).
    placement: Option<Arc<super::placement::Placement>>,
    /// Set on a replica (#1436): a background index's rebalancer isn't started, since its
    /// writes would be refused; its lists stay marked for after a promote.
    paused: AtomicBool,
}

impl std::fmt::Debug for Indexes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Indexes")
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Indexes {
    pub(crate) fn new(db: Pigeonhole, placement: Option<Arc<super::placement::Placement>>) -> Self {
        Self {
            placement,
            db,
            open: Mutex::new(super::handles::Handles::default()),
            rebalancers: Mutex::new(Vec::new()),
            running: Arc::default(),
            closed: AtomicBool::new(false),
            paused: AtomicBool::new(false),
        }
    }

    fn handles(&self) -> std::sync::MutexGuard<'_, super::handles::Handles<Index>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The index `name` if this server has it open: no I/O, so on any thread.
    pub(crate) fn cached(&self, name: &str) -> Option<Arc<Index>> {
        self.handles().get(name).cloned()
    }

    /// The index `name`, opened if this server hasn't yet. Sync: call it on the blocking pool.
    /// The open runs outside the cache's lock, so a cached lookup never waits behind another
    /// request's I/O (ai1249's M1 on #1573); two first calls racing for one name both open
    /// it, and the first to finish is kept. An open that a [`forget`](Self::forget)
    /// overtook opens again, since it may have read the catalog from before it (#1632).
    pub(crate) fn get(&self, name: &str) -> Result<Arc<Index>, ApiError> {
        loop {
            let generation = {
                let open = self.handles();
                if let Some(i) = open.get(name) {
                    return Ok(Arc::clone(i));
                }
                open.generation()
            };
            self.refuse_if_closed()?;
            let (index, rebalancer) = Index::open_with_rebalancer(&self.db, name)?;
            if let Some(kept) = self.keep(name, index, rebalancer, generation)? {
                return Ok(kept);
            }
        }
    }

    /// Creates the index `name`, or with `if_missing` opens an existing one, whatever its
    /// spec and options: `if_missing` compares neither. Sync, as `get`, and outside the
    /// cache's lock too.
    pub(crate) fn create(
        &self,
        name: &str,
        spec: pigeonhole::VectorSpec,
        options: IndexOptions,
        if_missing: bool,
    ) -> Result<(), ApiError> {
        if if_missing && self.cached(name).is_some() {
            return Ok(());
        }
        self.refuse_if_closed()?;
        let generation = self.handles().generation();
        let created = Index::create_with_rebalancer(&self.db, name, spec, options);
        let (index, rebalancer) = match created {
            Ok(r) => r,
            Err(e) if if_missing && e.code() == pigeonhole::ErrorCode::TableExists => {
                Index::open_with_rebalancer(&self.db, name)?
            }
            Err(e) => return Err(e.into()),
        };
        // Overtaken by a forget: the next call opens it afresh.
        self.keep(name, index, rebalancer, generation)?;
        Ok(())
    }

    fn refuse_if_closed(&self) -> Result<(), ApiError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(closed());
        }
        Ok(())
    }

    /// Keeps `index` under `name`, unless another call kept one first (then its handle is
    /// returned, and this one is dropped with its rebalancer), and runs the rebalancer of a
    /// background index it keeps on a thread of its own. Under the cache's lock, which
    /// `close` takes to set `closed`: after `close`, nothing is kept or started.
    ///
    /// `None`, keeping nothing, if a [`forget`](Self::forget) came after `generation` (the
    /// one the open started at): the handle may be of the catalog from before it (#1632).
    fn keep(
        &self,
        name: &str,
        index: Index,
        rebalancer: Rebalancer,
        generation: u64,
    ) -> Result<Option<Arc<Index>>, ApiError> {
        let mut open = self.handles();
        if self.closed.load(Ordering::Acquire) {
            return Err(closed());
        }
        if !open.current(generation) {
            return Ok(None);
        }
        if let Some(kept) = open.get(name) {
            return Ok(Some(Arc::clone(kept)));
        }
        if matches!(index.options().rebalance_mode(), Rebalance::Background)
            && !self.paused.load(Ordering::Acquire)
        {
            let running = Arc::clone(&self.running);
            running.fetch_add(1, Ordering::Relaxed);
            let index_name = name.to_owned();
            let placement = self.placement.clone();
            let spawned = std::thread::Builder::new()
                .name("phdb-serve-rebalance".into())
                .spawn(move || {
                    if let Some(p) = placement {
                        p.enter();
                    }
                    rebalance(rebalancer, &index_name);
                    running.fetch_sub(1, Ordering::Relaxed);
                });
            match spawned {
                Ok(h) => self
                    .rebalancers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(h),
                // Lists stay marked: the next open (or an inline write) rebalances them.
                Err(e) => {
                    self.running.fetch_sub(1, Ordering::Relaxed);
                    eprintln!("phdb serve: index {name}: no rebalancing thread: {e}");
                }
            }
        }
        // Current, checked above under this same lock.
        Ok(open.keep(generation, name, Arc::new(index)))
    }

    /// Drops every handle, so the next call opens its index again: the catalog changed under
    /// the server (a replica applied a primary's change, or swapped in a new copy, #1436).
    /// A rebalancer sees its index close at its next look and ends; a handle a call still
    /// holds keeps its index open until the call ends.
    ///
    /// It doesn't wait for those rebalancers (a call holding a handle would hold this
    /// caller, the replica's loop, too): it reaps the threads that have ended, so their
    /// handles don't pile up over many changes (cli692's F3 on #1583).
    ///
    /// An open running across it keeps nothing, and opens again (#1632).
    pub(crate) fn forget(&self) {
        self.handles().forget();
        let mut threads = self
            .rebalancers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (ended, running): (Vec<_>, Vec<_>) = std::mem::take(&mut *threads)
            .into_iter()
            .partition(JoinHandle::is_finished);
        *threads = running;
        drop(threads);
        for t in ended {
            let _ = t.join();
        }
    }

    /// Whether background rebalancers start for the indexes opened from now on: not on a
    /// replica, again after its promote (#1436).
    pub(crate) fn pause_rebalancing(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
    }

    /// Drops every handle and waits for the rebalancing threads to end: each sees its index
    /// close at its next look (at most one split or merge later). The server's shutdown calls
    /// this before it closes the database. A handle a call still holds keeps its index open
    /// until the call ends.
    pub(crate) fn close(&self) {
        {
            let mut open = self.handles();
            self.closed.store(true, Ordering::Release);
            open.forget();
        }
        let threads = std::mem::take(
            &mut *self
                .rebalancers
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for t in threads {
            let _ = t.join();
        }
    }

    /// Rebalancer threads running now (an inspection helper for tests).
    #[cfg(test)]
    pub(crate) fn rebalancing(&self) -> usize {
        self.running.load(Ordering::Relaxed)
    }
}

fn closed() -> ApiError {
    ApiError::from_code(
        pigeonhole::ErrorCode::Closed,
        "the server is closing: its vector indexes are closed",
    )
}

/// A background index's rebalancer, run until its index closes: the loop the crate's
/// rustdoc gives. An error leaves the list marked for a later call.
fn rebalance(mut r: Rebalancer, name: &str) {
    while !r.closed() {
        match r.run_once(SLICE) {
            Ok(true) => {}
            Ok(false) => r.wait(IDLE),
            Err(e) => {
                eprintln!("phdb serve: index {name}: rebalancing: {e}");
                r.wait(IDLE);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Indexes;
    use pigeonhole::{Element, Metric, Options, Pigeonhole, VectorSpec};
    use pigeonhole_vector::{Index, IndexOptions};

    /// #1632: an open that a `forget` overtook (a replica's catalog change, or a swap)
    /// keeps nothing, and the next call opens the index afresh.
    #[test]
    fn an_open_overtaken_by_a_forget_keeps_nothing() {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = Pigeonhole::open(dir.join("i.phdb"), Options::default().shards(1)).unwrap();
        let spec = VectorSpec::new(2, Element::F32, Metric::L2).unwrap();
        drop(Index::create(&db, "v", spec, IndexOptions::new()).unwrap());
        let indexes = Indexes::new(db.clone(), None);
        // The steps of `get`, with a forget between the open and the keep.
        let started = indexes.handles().generation();
        let (index, rebalancer) = Index::open_with_rebalancer(&db, "v").unwrap();
        indexes.forget();
        assert!(
            indexes
                .keep("v", index, rebalancer, started)
                .unwrap()
                .is_none(),
            "a stale open is not kept"
        );
        assert!(indexes.cached("v").is_none());
        // Opened afresh, it is kept.
        let kept = indexes.get("v").unwrap();
        assert!(std::sync::Arc::ptr_eq(&kept, &indexes.cached("v").unwrap()));
        drop(kept);
        indexes.close();
        db.close().unwrap();
    }
}
