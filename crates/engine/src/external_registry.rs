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
//! - **A read hold** (`hold_reads`, ICR 0140 amendment 1) counts against the share limit
//!   while it is live ([`reserve_hold`](ExternalRegistry::reserve_hold),
//!   [`release_hold`](ExternalRegistry::release_hold)), so holds and prepares can't pass it
//!   together. Its token comes from the reservations' counter, so it is unique too.
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

/// A slot: a prepare's reservation (its bytes, and the token that prepare holds), or an
/// entry in doubt.
#[derive(Debug, Clone, Copy)]
enum Slot {
    Reserved { bytes: u64, token: u64 },
    InDoubt(ExternalEntry),
}

impl Slot {
    fn bytes(&self) -> u64 {
        match self {
            Slot::Reserved { bytes, .. } => *bytes,
            Slot::InDoubt(e) => e.bytes,
        }
    }
}

/// The registry's state, under its one lock.
#[derive(Debug, Default)]
struct Table {
    slots: BTreeMap<Vec<u8>, Slot>,
    /// The last reservation's token: each one is new, so a prepare's late `unreserve` can't
    /// drop a later prepare's reservation of the same xid (perf2's S3 on #2000). A read
    /// hold's token comes from it too.
    last_token: u64,
    /// Read holds live now (ICR 0140 amendment 1): counted with `slots` against the share
    /// limit.
    holds: usize,
}

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct ExternalRegistry {
    entries: Mutex<Table>,
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
            entries: Mutex::new(Table::default()),
            limits,
        }
    }

    fn table(&self) -> impl std::ops::DerefMut<Target = Table> + '_ {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reserves `xid` for a prepare of `bytes`. `InvalidArgument` if it is reserved or in
    /// doubt already; `Busy` past a limit (prepares in flight count). The byte limit counts
    /// this prepare's own bytes, so nothing reserved or in doubt passes it, except that a
    /// prepare alone is always admitted: one larger than the limit could otherwise never
    /// prepare (perf2's N2 on #2000).
    /// The answer is the reservation's token, for [`unreserve`](Self::unreserve).
    pub(crate) fn reserve(&self, xid: &[u8], bytes: u64) -> Result<u64> {
        let mut table = self.table();
        let entries = &mut table.slots;
        if entries.contains_key(xid) {
            return Err(Error::InvalidArgument(
                "this xid is prepared here already: decide it first".to_owned(),
            ));
        }
        let (max_shares, max_bytes) = self.limits;
        let held: u64 = entries.values().map(Slot::bytes).sum();
        let over_bytes = max_bytes > 0 && !entries.is_empty() && held + bytes > max_bytes;
        if (max_shares > 0 && entries.len() + table.holds >= max_shares) || over_bytes {
            return Err(Error::Busy);
        }
        table.last_token += 1;
        let token = table.last_token;
        table
            .slots
            .insert(xid.to_vec(), Slot::Reserved { bytes, token });
        Ok(token)
    }

    /// Counts a read hold against the share limit (`Busy` past it) and answers its token.
    pub(crate) fn reserve_hold(&self) -> Result<u64> {
        let mut table = self.table();
        let max_shares = self.limits.0;
        if max_shares > 0 && table.slots.len() + table.holds >= max_shares {
            return Err(Error::Busy);
        }
        table.holds += 1;
        table.last_token += 1;
        Ok(table.last_token)
    }

    /// A read hold ended (released, or its engine closed): it no longer counts.
    pub(crate) fn release_hold(&self) {
        let mut table = self.table();
        table.holds = table.holds.saturating_sub(1);
    }

    /// Read holds live now: a range drop waits while any is (D331, #2089 landing after
    /// #2076).
    pub(crate) fn holds(&self) -> usize {
        self.table().holds
    }

    /// Drops `xid`'s reservation `token`, if it is still that one: a prepare that logged
    /// every share left its entry in its place, and a later prepare's reservation of the
    /// same xid has a token of its own.
    pub(crate) fn unreserve(&self, xid: &[u8], token: u64) {
        let mut table = self.table();
        if matches!(table.slots.get(xid), Some(Slot::Reserved { token: t, .. }) if *t == token) {
            table.slots.remove(xid);
        }
    }

    /// Records `xid` in doubt (the coordinator, every share logged; recovery's reinstall),
    /// in place of its reservation if any.
    pub(crate) fn install(&self, xid: &[u8], entry: ExternalEntry) {
        self.table()
            .slots
            .insert(xid.to_vec(), Slot::InDoubt(entry));
    }

    /// Forgets `xid` once decided.
    pub(crate) fn remove(&self, xid: &[u8]) {
        self.table().slots.remove(xid);
    }

    /// What is known of `xid` here.
    pub(crate) fn lookup(&self, xid: &[u8]) -> Lookup {
        match self.table().slots.get(xid) {
            None => Lookup::Absent,
            Some(Slot::Reserved { .. }) => Lookup::Reserved,
            Some(Slot::InDoubt(e)) => Lookup::InDoubt(*e),
        }
    }

    /// Every xid in doubt, with its entry (reservations aren't).
    pub(crate) fn in_doubt(&self) -> Vec<(Vec<u8>, ExternalEntry)> {
        self.table()
            .slots
            .iter()
            .filter_map(|(xid, s)| match s {
                Slot::InDoubt(e) => Some((xid.clone(), *e)),
                Slot::Reserved { .. } => None,
            })
            .collect()
    }

    /// Whether any xid is in doubt.
    pub(crate) fn any_in_doubt(&self) -> bool {
        self.table()
            .slots
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
        let big = r.lookup(b"big");
        assert_eq!(big, super::Lookup::Reserved);
        r.remove(b"big");
        r.reserve(b"a", 6).unwrap();
        assert!(
            matches!(r.reserve(b"b", 5), Err(Error::Busy)),
            "6 + 5 is over 10"
        );
        r.reserve(b"c", 4).unwrap();
    }

    #[test]
    fn read_holds_count_against_the_share_limit_with_prepares() {
        // ICR 0140 amendment 1: a live hold takes a share's place in the limit.
        let r = ExternalRegistry::new((2, 0));
        let a = r.reserve_hold().unwrap();
        r.reserve(b"x", 1).unwrap();
        assert!(matches!(r.reserve_hold(), Err(Error::Busy)));
        assert!(matches!(r.reserve(b"y", 1), Err(Error::Busy)));
        r.release_hold();
        let b = r.reserve_hold().unwrap();
        assert_ne!(a, b, "each hold's token is new");
        assert_eq!(r.holds(), 1);
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
        let token = r.reserve(xid, 1)?;
        if logs {
            r.install(xid, entry(id));
        }
        r.unreserve(xid, token);
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
    fn loom_a_read_hold_and_a_prepare_never_pass_the_limit_together() {
        // ICR 0140 amendment 1: with a limit of one share, a read hold and a prepare race.
        // At most one is admitted; the other answers `Busy`. Once the winner is done, the
        // limit is free again.
        loom::model(|| {
            let r = Arc::new(ExternalRegistry::new((1, 0)));
            let h = {
                let r = Arc::clone(&r);
                thread::spawn(move || r.reserve_hold().is_ok())
            };
            let p = r.reserve(b"x", 1);
            let held = h.join().unwrap();
            match p {
                Ok(_) => assert!(!held, "both passed the limit"),
                Err(Error::Busy) => assert!(held, "both refused"),
                Err(e) => panic!("{e:?}"),
            }
            if held {
                r.release_hold();
            } else {
                r.remove(b"x");
            }
            assert!(r.reserve_hold().is_ok());
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
    fn loom_a_late_unreserve_never_drops_a_later_prepares_reservation() {
        // perf2's S3 on #2000. Prepare A of "x" logged (its entry installed) but hasn't run
        // its `unreserve` yet; meanwhile "x" is decided (removed) and prepare B reserves it.
        // A's late `unreserve` must leave B's reservation, so a third prepare of "x" is still
        // refused while B is in flight.
        loom::model(|| {
            let r = Arc::new(ExternalRegistry::default());
            let a = r.reserve(b"x", 1).unwrap();
            r.install(b"x", entry(1));
            let late = {
                let r = Arc::clone(&r);
                thread::spawn(move || r.unreserve(b"x", a))
            };
            r.remove(b"x");
            let b = r.reserve(b"x", 1);
            late.join().unwrap();
            if b.is_ok() {
                assert_eq!(r.lookup(b"x"), Lookup::Reserved, "B's reservation stands");
                assert!(matches!(r.reserve(b"x", 1), Err(Error::InvalidArgument(_))));
            }
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
