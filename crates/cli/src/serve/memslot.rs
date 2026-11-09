//! The memory store a server serves (`--memory`), opened when a call needs it and opened
//! again after the database changes under it (#1618).
//!
//! A replica's database is swapped whole by a re-bootstrap, and its catalog changes as it
//! applies the primary's: the store a slot opened reads the old copy's text and IVF indexes
//! and pin. So [`MemSlot::clear`] (called by the service's `tables_changed`, and so by
//! `engine_replaced`) drops it, and the next call opens it afresh.
//!
//! - **Mode:** a writer opens the store, creating it if missing. A replica (until its promote)
//!   only opens it, with no thread of the crate's ([`MemCore::open_in`]), and "missing" is an
//!   answer: the store may arrive with a later catalog change.
//! - **The generation (cli692's D1):** an open takes a while, and a clear can land during
//!   it. Every clear bumps the generation; an open installs its result only if the generation
//!   is the one it started at, and otherwise opens again. So a store created on the primary
//!   during an open is never cached as missing, nor a dropped one's handles kept.
//! - **What's cached** until the next clear: the open store, "missing", or a failure of the
//!   server's own configuration (a pin mismatch, cli692's D4). Any other failure is answered
//!   and not kept, so the next call tries again.
//! - **Order (cli692's D2):** `clear` is synchronous, and the replica calls it before it
//!   publishes the seqno it applied, so a read waiting for that seqno opens after the clear.

use std::sync::{Arc, Condvar, Mutex, PoisonError};

use pigeonhole::Pigeonhole;

use super::errors::ApiError;
use crate::exit::Failure;
use crate::memcore::{Create, Embedder, MemCore, OpenError};

/// What a slot holds once an open has finished.
#[derive(Clone)]
pub(crate) enum Entry {
    Core(Arc<MemCore>),
    /// The database has no store of the slot's name (a replica only).
    Missing,
    /// The open failed for a reason a retry won't fix.
    Failed(ApiError),
}

struct State {
    /// Bumped by every [`MemSlot::clear`].
    generation: u64,
    /// Whether the slot opens as the writer (creating a missing store).
    writer: bool,
    /// An open is running (one at a time; the others wait for it).
    opening: bool,
    entry: Option<Entry>,
}

/// Ends an open that unwound: the slot isn't opening any more, and its waiters try again.
struct Opening<'a> {
    slot: &'a MemSlot,
    armed: bool,
}

impl Drop for Opening<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.slot.state().opening = false;
            self.slot.opened.notify_all();
        }
    }
}

/// The memory store `--memory` names, as the service holds it.
pub(crate) struct MemSlot {
    db: Pigeonhole,
    name: String,
    embedder: Option<Arc<Embedder>>,
    /// The model the store must be pinned to when there's no embedder (`--memory-model`).
    expected: Option<String>,
    create: Arc<Create>,
    budget: (u64, u64),
    state: Mutex<State>,
    opened: Condvar,
    /// How many times the database was swapped under the slot: a watch of the store ends
    /// when it changes.
    swaps: tokio::sync::watch::Sender<u64>,
    /// A hold seam (AGENTS.md), and nothing else: run once after an open, before its result
    /// is installed, where `tests::an_open_overtaken_by_a_clear_opens_again` lands a create
    /// and a clear during an open (D1). Unset, it does nothing.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    after_open: Mutex<Option<Box<dyn FnOnce(&MemSlot) + Send>>>,
}

impl MemSlot {
    /// A slot for the store `name`, opening as the writer or (`writer` false) as a replica.
    /// Nothing is opened yet.
    pub(crate) fn new(
        db: &Pigeonhole,
        name: &str,
        embedder: Option<Embedder>,
        expected: Option<&str>,
        create: Arc<Create>,
        budget: (u64, u64),
        writer: bool,
    ) -> Self {
        Self {
            db: db.clone(),
            name: name.to_owned(),
            embedder: embedder.map(Arc::new),
            expected: expected.map(str::to_owned),
            create,
            budget,
            state: Mutex::new(State {
                generation: 0,
                writer,
                opening: false,
                entry: None,
            }),
            opened: Condvar::new(),
            swaps: tokio::sync::watch::Sender::new(0),
            #[cfg(test)]
            after_open: Mutex::new(None),
        }
    }

    /// The store's name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// [`new`](Self::new), opened now as a server starts it: a writer opens the store,
    /// creating it if missing; a replica opens it only if its copy has it (so a pin mismatch
    /// fails the start, cli692's D3 on #1618), and otherwise when a call first needs it.
    /// Returns the slot and whether the store was created.
    pub(crate) fn start(
        db: &Pigeonhole,
        name: &str,
        embedder: Option<Embedder>,
        expected: Option<&str>,
        create: Arc<Create>,
        budget: (u64, u64),
        writer: bool,
    ) -> Result<(Self, bool), Failure> {
        let slot = Self::new(db, name, embedder, expected, create, budget, writer);
        let exists = slot.exists();
        if !(writer || exists) {
            return Ok((slot, false));
        }
        let create = writer.then_some(&*slot.create);
        match MemCore::open_in(db, name, slot.embedder.clone(), expected, create, budget) {
            Ok(Some(core)) => slot.state().entry = Some(Entry::Core(Arc::new(core))),
            Ok(None) => {}
            Err(OpenError::Pin(f) | OpenError::Create(f)) => return Err(f),
            Err(OpenError::Store(e)) => {
                return Err(Failure::coded(
                    e.code(),
                    format!("memory store {name}: {e}"),
                ));
            }
        }
        Ok((slot, writer && !exists))
    }

    /// Whether the database has the store now.
    pub(crate) fn exists(&self) -> bool {
        self.db.tables().contains(&format!("mem_{}", self.name))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What the slot holds, if an open has finished since the last clear.
    pub(crate) fn cached(&self) -> Option<Entry> {
        self.state().entry.clone()
    }

    /// What the slot holds, opening the store if nothing is cached (it blocks: call it on
    /// the blocking pool). One open runs at a time; a call that finds one running waits for
    /// it. An open that a clear overtook is run again (D1).
    pub(crate) fn get(&self) -> Result<Entry, ApiError> {
        let mut state = self.state();
        loop {
            if let Some(entry) = &state.entry {
                return Ok(entry.clone());
            }
            if state.opening {
                state = self
                    .opened
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            }
            state.opening = true;
            let (generation, writer) = (state.generation, state.writer);
            drop(state);
            // A panic in the open (or the create closure) mustn't leave the slot opening for
            // good, with every later call parked behind it (cli692's R1 on #1639).
            let mut guard = Opening {
                slot: self,
                armed: true,
            };
            let opened = self.open(writer);
            #[cfg(test)]
            {
                let hold = self
                    .after_open
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                if let Some(hold) = hold {
                    hold(self);
                }
            }
            guard.armed = false;
            state = self.state();
            state.opening = false;
            self.opened.notify_all();
            if state.generation != generation {
                // The catalog or the engine changed during the open: its result may be of
                // the old one. Open again.
                continue;
            }
            let entry = opened?;
            state.entry = Some(entry.clone());
            return Ok(entry);
        }
    }

    /// Opens the store as the writer or a replica: the entry to cache, or an error to answer
    /// without caching it.
    fn open(&self, writer: bool) -> Result<Entry, ApiError> {
        let create = writer.then_some(&*self.create);
        match MemCore::open_in(
            &self.db,
            &self.name,
            self.embedder.clone(),
            self.expected.as_deref(),
            create,
            self.budget,
        ) {
            Ok(Some(core)) => Ok(Entry::Core(Arc::new(core))),
            Ok(None) => Ok(Entry::Missing),
            // The server's configuration: a retry won't change it (D4).
            Err(OpenError::Pin(f) | OpenError::Create(f)) => Ok(Entry::Failed(
                ApiError::failed_precondition(f.message().to_owned()),
            )),
            Err(OpenError::Store(e)) => Err(ApiError::from_code(
                e.code(),
                &format!("memory store {}: {e}", self.name),
            )),
        }
    }

    /// Drops what the slot holds: the next call opens the store again. Synchronous, so a
    /// caller that publishes progress after it (the replica's applied seqno) publishes it
    /// only once the slot is cleared (D2).
    pub(crate) fn clear(&self) {
        let mut state = self.state();
        state.generation += 1;
        state.entry = None;
    }

    /// The swaps a watch of the store must end at. Take it before the store the watch reads:
    /// a swap after it ends the watch, and a swap before it has cleared the slot already.
    pub(crate) fn swaps(&self) -> tokio::sync::watch::Receiver<u64> {
        self.swaps.subscribe()
    }

    /// The database was swapped under the slot (a replica's re-bootstrap): what it holds goes,
    /// and then every watch of the old copy's store ends.
    pub(crate) fn replaced(&self) {
        self.clear();
        self.swaps.send_modify(|n| *n += 1);
    }

    /// The server was promoted: from now on the slot opens as the writer.
    pub(crate) fn promote(&self) {
        let mut state = self.state();
        state.writer = true;
        state.generation += 1;
        state.entry = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pigeonhole::{Options, Pigeonhole};

    use super::{Entry, MemSlot};
    use crate::memcore::{Create, MemCore};

    /// A test's directory, removed when dropped.
    struct Dir(std::path::PathBuf);

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn db() -> (Dir, Pigeonhole) {
        static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let dir = Dir(std::env::temp_dir().join(format!(
            "phdb-memslot-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )));
        std::fs::create_dir_all(&dir.0).unwrap();
        let db = Pigeonhole::open(
            dir.0.join("t.phdb"),
            Options::default().create_if_missing(true),
        )
        .unwrap();
        (dir, db)
    }

    fn no_model() -> Arc<Create> {
        Arc::new(|_| Ok(pigeonhole_memory::StoreOptions::new()))
    }

    /// A replica's slot for the store `m`, expecting `expected`'s pin.
    fn replica_slot(db: &Pigeonhole, expected: Option<&str>) -> MemSlot {
        MemSlot::new(
            db,
            "m",
            None,
            expected,
            no_model(),
            (1 << 20, 8 << 20),
            false,
        )
    }

    /// Creates the store `m` (no model) as a primary's writes would.
    fn create_store(db: &Pigeonhole) {
        let core = MemCore::open_in(db, "m", None, None, Some(&*no_model()), (1, 1)).unwrap();
        assert!(core.is_some());
    }

    /// cli692's D1: the store arrives (and the catalog change clears the slot) while an open
    /// that found it missing is still running. That open's "missing" is of the old catalog,
    /// so it isn't kept: the slot opens again and finds the store.
    #[test]
    fn an_open_overtaken_by_a_clear_opens_again() {
        let (_dir, db) = db();
        let slot = replica_slot(&db, None);
        let created = db.clone();
        *slot.after_open.lock().unwrap() = Some(Box::new(move |slot: &MemSlot| {
            create_store(&created);
            slot.clear();
        }));
        match slot.get().unwrap() {
            Entry::Core(_) => {}
            Entry::Missing => panic!("an open from before the store arrived was kept"),
            Entry::Failed(e) => panic!("{}", e.message),
        }
        assert!(matches!(slot.cached(), Some(Entry::Core(_))));
    }

    /// cli692's D4: "missing" and a pin mismatch are kept until the next clear (a call
    /// doesn't open again), and a mismatch is the server's configuration:
    /// `FailedPrecondition`.
    #[test]
    fn missing_and_a_pin_mismatch_are_kept_until_a_clear() {
        let (_dir, db) = db();
        let slot = replica_slot(&db, None);
        assert!(slot.cached().is_none(), "nothing opened yet");
        assert!(matches!(slot.get().unwrap(), Entry::Missing));
        assert!(matches!(slot.cached(), Some(Entry::Missing)));
        // The store arrives: still "missing" until the catalog change's clear.
        create_store(&db);
        assert!(matches!(slot.get().unwrap(), Entry::Missing));
        slot.clear();
        assert!(slot.cached().is_none());
        assert!(matches!(slot.get().unwrap(), Entry::Core(_)));

        // A store with no model, where the replica names one.
        let pinned = replica_slot(&db, Some("other-model"));
        let Entry::Failed(e) = pinned.get().unwrap() else {
            panic!("a pin mismatch opened");
        };
        assert_eq!(e.code, "FailedPrecondition", "{}", e.message);
        assert!(e.message.contains("other-model"), "{}", e.message);
        assert!(matches!(pinned.cached(), Some(Entry::Failed(_))));
    }

    /// A panic during an open (cli692's R1 on #1639) leaves the slot able to open: the next
    /// call opens, rather than waiting for good on an open that ended.
    #[test]
    fn a_panic_during_an_open_leaves_the_slot_able_to_open() {
        let (_dir, db) = db();
        let slot = replica_slot(&db, None);
        *slot.after_open.lock().unwrap() = Some(Box::new(|_: &MemSlot| panic!("in the open")));
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| slot.get()));
        assert!(unwound.is_err());
        assert!(matches!(slot.get().unwrap(), Entry::Missing));
    }

    /// A promote's slot opens as the writer: a store that's missing is created.
    #[test]
    fn a_promoted_slot_creates_a_missing_store() {
        let (_dir, db) = db();
        let slot = replica_slot(&db, None);
        assert!(matches!(slot.get().unwrap(), Entry::Missing));
        slot.promote();
        assert!(slot.cached().is_none(), "a promote drops what was kept");
        assert!(matches!(slot.get().unwrap(), Entry::Core(_)));
        assert!(slot.exists());
    }
}
