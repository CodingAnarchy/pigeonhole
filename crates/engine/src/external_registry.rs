//! The registry of transactions prepared for an external coordinator (D329, ICR 0140),
//! `Shared::external`: which xids are reserved by a prepare in flight or in doubt here, and
//! the in-doubt limits.
//!
//! - **A prepare** reserves its xid ([`reserve`](ExternalRegistry::reserve)) before it
//!   submits, under the lock that checks for the xid and the limits, so two prepares of one
//!   xid can't both log, and concurrent prepares can't pass a limit together.
//! - **The coordinator** replaces the reservation with the xid's entry
//!   ([`install`](ExternalRegistry::install)) once every share is logged, before it answers
//!   the caller, so a `decide` after the prepare returned always finds it. It removes the
//!   entry once decided ([`remove`](ExternalRegistry::remove)).
//! - **A prepare that didn't log** drops its reservation
//!   ([`unreserve`](ExternalRegistry::unreserve)); an installed entry stays.
//! - **`decide`** looks the xid up ([`lookup`](ExternalRegistry::lookup)) and submits to the
//!   coordinator after the lock is released. A reservation is not in doubt yet (`Busy`, ask
//!   again). Two decides of one xid both reach the coordinator, whose shard thread orders
//!   them: the second finds it decided.
//!
//! One lock, `entries`, a leaf: nothing is sent or waited on under it. The limits are fixed
//! at open. Loom-modelled (`loom_tests`).

#[cfg(engine_loom)]
use loom::sync::Mutex;
#[cfg(not(engine_loom))]
use std::sync::Mutex;

use std::collections::BTreeMap;
use std::sync::PoisonError;

use pigeonhole_format::Seqno;

use crate::error::{Error, Result};
use pigeonhole_runtime::ShardId;

/// An xid in doubt: its commit's id, the shard that holds its decision, when it was
/// prepared (the engine's monotonic clock), and its batches' bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExternalEntry {
    pub id: Seqno,
    pub coordinator: ShardId,
    pub prepared_at: u64,
    pub bytes: u64,
}

/// What [`ExternalRegistry::lookup`] finds for an xid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lookup {
    /// Neither reserved nor in doubt: decided already, or never prepared.
    Absent,
    /// A prepare of it is in flight.
    Reserved,
    InDoubt(ExternalEntry),
}

/// A slot: a prepare's reservation (its bytes), or an entry in doubt.
#[derive(Debug, Clone, Copy)]
enum Slot {
    Reserved { bytes: u64 },
    InDoubt(ExternalEntry),
}

impl Slot {
    fn bytes(&self) -> u64 {
        match self {
            Slot::Reserved { bytes } => *bytes,
            Slot::InDoubt(e) => e.bytes,
        }
    }
}

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct ExternalRegistry {
    entries: Mutex<BTreeMap<Vec<u8>, Slot>>,
    /// The most xids reserved or in doubt, and the most bytes of their batches; 0 for none
    /// (`EngineOptions::in_doubt_limits`, fixed at open).
    limits: (usize, u64),
}

impl Default for ExternalRegistry {
    fn default() -> Self {
        Self::new((0, 0))
    }
}

impl ExternalRegistry {
    /// An empty registry with the in-doubt `limits` (`EngineOptions::in_doubt_limits`).
    pub(crate) fn new(limits: (usize, u64)) -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            limits,
        }
    }

    fn entries(&self) -> impl std::ops::DerefMut<Target = BTreeMap<Vec<u8>, Slot>> + '_ {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reserves `xid` for a prepare of `bytes`. `InvalidArgument` if it is reserved or in
    /// doubt already; `Busy` past a limit (prepares in flight count). The byte limit counts
    /// this prepare's own bytes, so nothing reserved or in doubt passes it, except that a
    /// prepare alone is always admitted: one larger than the limit could otherwise never
    /// prepare (perf2's N2 on #2000).
    pub(crate) fn reserve(&self, xid: &[u8], bytes: u64) -> Result<()> {
        let mut entries = self.entries();
        if entries.contains_key(xid) {
            return Err(Error::InvalidArgument(
                "this xid is prepared here already: decide it first".to_owned(),
            ));
        }
        let (max_shares, max_bytes) = self.limits;
        let held: u64 = entries.values().map(Slot::bytes).sum();
        let over_bytes = max_bytes > 0 && !entries.is_empty() && held + bytes > max_bytes;
        if (max_shares > 0 && entries.len() >= max_shares) || over_bytes {
            return Err(Error::Busy);
        }
        entries.insert(xid.to_vec(), Slot::Reserved { bytes });
        Ok(())
    }

    /// Drops `xid`'s reservation, if it is still one: a prepare that logged every share
    /// left its entry in its place.
    pub(crate) fn unreserve(&self, xid: &[u8]) {
        let mut entries = self.entries();
        if matches!(entries.get(xid), Some(Slot::Reserved { .. })) {
            entries.remove(xid);
        }
    }

    /// Records `xid` in doubt (the coordinator, every share logged; recovery's reinstall),
    /// in place of its reservation if any.
    pub(crate) fn install(&self, xid: &[u8], entry: ExternalEntry) {
        self.entries().insert(xid.to_vec(), Slot::InDoubt(entry));
    }

    /// Forgets `xid` once decided.
    pub(crate) fn remove(&self, xid: &[u8]) {
        self.entries().remove(xid);
    }

    /// What is known of `xid` here.
    pub(crate) fn lookup(&self, xid: &[u8]) -> Lookup {
        match self.entries().get(xid) {
            None => Lookup::Absent,
            Some(Slot::Reserved { .. }) => Lookup::Reserved,
            Some(Slot::InDoubt(e)) => Lookup::InDoubt(*e),
        }
    }

    /// Every xid in doubt, with its entry (reservations aren't).
    pub(crate) fn in_doubt(&self) -> Vec<(Vec<u8>, ExternalEntry)> {
        self.entries()
            .iter()
            .filter_map(|(xid, s)| match s {
                Slot::InDoubt(e) => Some((xid.clone(), *e)),
                Slot::Reserved { .. } => None,
            })
            .collect()
    }

    /// Whether any xid is in doubt.
    pub(crate) fn any_in_doubt(&self) -> bool {
        self.entries()
            .values()
            .any(|s| matches!(s, Slot::InDoubt(_)))
    }
}

#[cfg(all(test, not(engine_loom)))]
mod tests {
    use super::ExternalRegistry;
    use crate::error::Error;

    #[test]
    fn the_byte_limit_counts_the_prepares_own_bytes_and_admits_one_alone() {
        // perf2's N2 on #2000.
        let r = ExternalRegistry::new((0, 10));
        r.reserve(b"big", 25).unwrap();
        assert!(matches!(r.reserve(b"small", 1), Err(Error::Busy)));
        r.unreserve(b"big");
        r.reserve(b"a", 6).unwrap();
        assert!(
            matches!(r.reserve(b"b", 5), Err(Error::Busy)),
            "6 + 5 is over 10"
        );
        r.reserve(b"c", 4).unwrap();
    }
}

#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::{ExternalEntry, ExternalRegistry, Lookup};
    use crate::error::Error;
    use loom::sync::Arc;
    use loom::thread;
    use pigeonhole_runtime::ShardId;

    fn entry(id: u64) -> ExternalEntry {
        ExternalEntry {
            id,
            coordinator: ShardId(0),
            prepared_at: 0,
            bytes: 1,
        }
    }

    /// A prepare as `Inner::prepare_external` runs it against the registry: reserve, then
    /// (every share logged) the coordinator installs the entry, then the reservation is
    /// dropped if still one. `Ok(true)` if it prepared.
    fn prepare(r: &ExternalRegistry, xid: &[u8], id: u64, logs: bool) -> Result<bool, Error> {
        r.reserve(xid, 1)?;
        if logs {
            r.install(xid, entry(id));
        }
        r.unreserve(xid);
        Ok(logs)
    }

    #[test]
    fn loom_two_prepares_of_one_xid_never_both_log() {
        // Two prepares of xid "x" race. In every interleaving at most one reserves and logs;
        // the other is refused (`InvalidArgument`), or, if it came after the first had
        // logged, refused too: "x" is in doubt. The entry left is the winner's.
        loom::model(|| {
            let r = Arc::new(ExternalRegistry::default());
            let spawn = |id| {
                let r = Arc::clone(&r);
                thread::spawn(move || prepare(&r, b"x", id, true))
            };
            let (a, b) = (spawn(1), spawn(2));
            let (a, b) = (a.join().unwrap(), b.join().unwrap());
            let won: Vec<u64> = [(a, 1), (b, 2)]
                .into_iter()
                .filter_map(|(res, id)| match res {
                    Ok(true) => Some(id),
                    Err(Error::InvalidArgument(_)) => None,
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(won.len(), 1, "exactly one prepare logs");
            assert_eq!(r.lookup(b"x"), Lookup::InDoubt(entry(won[0])));
        });
    }

    #[test]
    fn loom_a_decide_racing_a_prepare_never_sees_it_decided_while_it_is_in_doubt() {
        // A decide looks "x" up while its prepare runs. It sees it absent (before the
        // reservation), reserved (`Busy`: ask again), or in doubt with the prepare's id:
        // never absent once in doubt, so a decide after the prepare answered can't miss it.
        // A prepare that fails (logs nothing) leaves nothing behind.
        loom::model(|| {
            let r = Arc::new(ExternalRegistry::default());
            let p = {
                let r = Arc::clone(&r);
                thread::spawn(move || {
                    prepare(&r, b"x", 7, true).unwrap();
                    prepare(&r, b"y", 8, false).unwrap();
                })
            };
            let first = r.lookup(b"x");
            let second = r.lookup(b"x");
            assert!(
                matches!(
                    first,
                    Lookup::Absent | Lookup::Reserved | Lookup::InDoubt(_)
                ),
                "{first:?}"
            );
            if let Lookup::InDoubt(e) = first {
                assert_eq!(e.id, 7);
                assert_eq!(second, first, "once in doubt it stays until decided");
            }
            p.join().unwrap();
            assert_eq!(r.lookup(b"x"), Lookup::InDoubt(entry(7)));
            assert_eq!(r.lookup(b"y"), Lookup::Absent);
            assert_eq!(r.in_doubt().len(), 1);
        });
    }

    #[test]
    fn loom_two_decides_and_the_coordinators_removal() {
        // Two decides of "x" look it up while the coordinator, having decided it, removes
        // it. Each sees it in doubt or absent; one that saw it absent never sees it again,
        // and neither ever sees a reservation.
        loom::model(|| {
            let r = Arc::new(ExternalRegistry::default());
            r.install(b"x", entry(3));
            let decide = || {
                let r = Arc::clone(&r);
                thread::spawn(move || {
                    let a = r.lookup(b"x");
                    let b = r.lookup(b"x");
                    assert!(!matches!(a, Lookup::Reserved) && !matches!(b, Lookup::Reserved));
                    if a == Lookup::Absent {
                        assert_eq!(b, Lookup::Absent);
                    }
                })
            };
            let (d1, d2) = (decide(), decide());
            r.remove(b"x");
            d1.join().unwrap();
            d2.join().unwrap();
            assert!(!r.any_in_doubt());
        });
    }

    #[test]
    fn loom_concurrent_prepares_never_pass_the_limit_together() {
        // A limit of one xid: two prepares of different xids race, and at most one is in
        // doubt or reserved at a time, so one of them is refused `Busy` unless the other
        // failed and dropped its reservation first.
        loom::model(|| {
            let r = Arc::new(ExternalRegistry::new((1, 0)));
            let spawn = |xid: &'static [u8], id| {
                let r = Arc::clone(&r);
                thread::spawn(move || prepare(&r, xid, id, true))
            };
            let (a, b) = (spawn(b"a", 1), spawn(b"b", 2));
            let res = [a.join().unwrap(), b.join().unwrap()];
            let logged = res.iter().filter(|r| matches!(r, Ok(true))).count();
            let busy = res.iter().filter(|r| matches!(r, Err(Error::Busy))).count();
            assert_eq!((logged, busy), (1, 1), "{res:?}");
            assert_eq!(r.in_doubt().len(), 1);
        });
    }
}
