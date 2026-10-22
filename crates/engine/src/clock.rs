//! The engine's one seqno allocator (D271): every seqno a writer reserves (a group's, a
//! cross-shard commit's id, its decision's) comes from here, in the writer's process, and
//! shm `NEXT_SEQNO` mirrors it (`ShmRegion::advance_next_seqno`, a `fetch_max` after each
//! reservation) for reader processes.
//!
//! D271's default timestamps come from the same reservation in a later step; this step only
//! moves the seqnos.

use std::sync::{Mutex, PoisonError};

use pigeonhole_format::Seqno;
use pigeonhole_shm::ShmRegion;

/// The next seqno to reserve, under one lock.
#[derive(Debug, Default)]
pub(crate) struct Reservation {
    next: Mutex<Seqno>,
}

impl Reservation {
    /// Starts at `next` (at open, once replay has raised shm `NEXT_SEQNO` past every
    /// recovered seqno).
    pub(crate) fn seed(&self, next: Seqno) {
        *self.next.lock().unwrap_or_else(PoisonError::into_inner) = next;
    }

    /// The next seqno to reserve: a lower bound for every later reservation (FORMAT §11.3's
    /// pending watermark), and the seqno ceiling the manifest records.
    pub(crate) fn next_seqno(&self) -> Seqno {
        *self.next.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reserves `n` consecutive seqnos and returns the first, then raises the shm mirror past
    /// them. The caller published its pending watermark first, no higher than the result.
    pub(crate) fn reserve(&self, n: u64, shm: &ShmRegion) -> Seqno {
        let first = {
            let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
            let first = *next;
            *next += n;
            first
        };
        shm.advance_next_seqno(first + n);
        first
    }
}
