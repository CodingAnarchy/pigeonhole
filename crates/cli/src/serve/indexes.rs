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

use std::collections::HashMap;
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
    open: Mutex<HashMap<String, Arc<Index>>>,
    rebalancers: Mutex<Vec<JoinHandle<()>>>,
    /// Rebalancer threads running, for tests to read.
    running: Arc<AtomicUsize>,
    /// Set by [`close`](Indexes::close), under `open`'s lock: no index opens after it, so a
    /// call still queued on the blocking pool at shutdown can't start a rebalancer nothing
    /// joins (ai1249's M2 on #1573).
    closed: AtomicBool,
    /// Where the rebalancer threads run: off the shards' CPUs (perf287's B2 on 7b2).
    placement: Option<Arc<super::placement::Placement>>,
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
            open: Mutex::new(HashMap::new()),
            rebalancers: Mutex::new(Vec::new()),
            running: Arc::default(),
            closed: AtomicBool::new(false),
        }
    }

    fn handles(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Index>>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The index `name` if this server has it open: no I/O, so on any thread.
    pub(crate) fn cached(&self, name: &str) -> Option<Arc<Index>> {
        self.handles().get(name).cloned()
    }

    /// The index `name`, opened if this server hasn't yet. Sync: call it on the blocking pool.
    /// The open runs outside the cache's lock, so a cached lookup never waits behind another
    /// request's I/O (ai1249's M1 on #1573); two first calls racing for one name both open
    /// it, and the first to finish is kept.
    pub(crate) fn get(&self, name: &str) -> Result<Arc<Index>, ApiError> {
        if let Some(i) = self.cached(name) {
            return Ok(i);
        }
        self.refuse_if_closed()?;
        let (index, rebalancer) = Index::open_with_rebalancer(&self.db, name)?;
        self.keep(name, index, rebalancer)
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
        let created = Index::create_with_rebalancer(&self.db, name, spec, options);
        let (index, rebalancer) = match created {
            Ok(r) => r,
            Err(e) if if_missing && e.code() == pigeonhole::ErrorCode::TableExists => {
                Index::open_with_rebalancer(&self.db, name)?
            }
            Err(e) => return Err(e.into()),
        };
        self.keep(name, index, rebalancer)?;
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
    fn keep(
        &self,
        name: &str,
        index: Index,
        rebalancer: Rebalancer,
    ) -> Result<Arc<Index>, ApiError> {
        let mut open = self.handles();
        if self.closed.load(Ordering::Acquire) {
            return Err(closed());
        }
        if let Some(kept) = open.get(name) {
            return Ok(Arc::clone(kept));
        }
        if matches!(index.options().rebalance_mode(), Rebalance::Background) {
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
        let index = Arc::new(index);
        open.insert(name.to_owned(), Arc::clone(&index));
        Ok(index)
    }

    /// Drops every handle and waits for the rebalancing threads to end: each sees its index
    /// close at its next look (at most one split or merge later). The server's shutdown calls
    /// this before it closes the database. A handle a call still holds keeps its index open
    /// until the call ends.
    pub(crate) fn close(&self) {
        {
            let mut open = self.handles();
            self.closed.store(true, Ordering::Release);
            open.clear();
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
