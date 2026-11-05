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
use std::sync::atomic::{AtomicUsize, Ordering};
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
}

impl std::fmt::Debug for Indexes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Indexes")
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Indexes {
    pub(crate) fn new(db: Pigeonhole) -> Self {
        Self {
            db,
            open: Mutex::new(HashMap::new()),
            rebalancers: Mutex::new(Vec::new()),
            running: Arc::default(),
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
    pub(crate) fn get(&self, name: &str) -> Result<Arc<Index>, ApiError> {
        let mut open = self.handles();
        if let Some(i) = open.get(name) {
            return Ok(Arc::clone(i));
        }
        let (index, rebalancer) = Index::open_with_rebalancer(&self.db, name)?;
        Ok(self.keep(&mut open, name, index, rebalancer))
    }

    /// Creates the index `name`, or with `if_missing` opens an existing one. Sync, as `get`.
    pub(crate) fn create(
        &self,
        name: &str,
        spec: pigeonhole::VectorSpec,
        options: IndexOptions,
        if_missing: bool,
    ) -> Result<(), ApiError> {
        let mut open = self.handles();
        if if_missing && open.contains_key(name) {
            return Ok(());
        }
        let created = Index::create_with_rebalancer(&self.db, name, spec, options);
        let (index, rebalancer) = match created {
            Ok(r) => r,
            Err(e) if if_missing && e.code() == pigeonhole::ErrorCode::TableExists => {
                Index::open_with_rebalancer(&self.db, name)?
            }
            Err(e) => return Err(e.into()),
        };
        self.keep(&mut open, name, index, rebalancer);
        Ok(())
    }

    /// Keeps `index` under `name`, and runs its rebalancer on a thread of its own when it
    /// rebalances in the background.
    fn keep(
        &self,
        open: &mut HashMap<String, Arc<Index>>,
        name: &str,
        index: Index,
        rebalancer: Rebalancer,
    ) -> Arc<Index> {
        if matches!(index.options().rebalance_mode(), Rebalance::Background) {
            let running = Arc::clone(&self.running);
            running.fetch_add(1, Ordering::Relaxed);
            let index_name = name.to_owned();
            let spawned = std::thread::Builder::new()
                .name("phdb-serve-rebalance".into())
                .spawn(move || {
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
        index
    }

    /// Drops every handle and waits for the rebalancing threads to end: each sees its index
    /// close at its next look (at most one split or merge later). The server's shutdown calls
    /// this before it closes the database. A handle a call still holds keeps its index open
    /// until the call ends.
    pub(crate) fn close(&self) {
        self.handles().clear();
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
