# 0063: `ShmRegion::advance_next_seqno`, the mirror of the writer's seqno allocator

**Status:** Approved (owner, via the coordinator, 2026-10-22; D271, #854). Additive.

## Change

**`pigeonhole-shm`:** `ShmRegion::advance_next_seqno(&self, to: Seqno)` raises shm `NEXT_SEQNO` to at least `to`, with a `fetch_max(AcqRel)`.

`reserve_seqnos` stays. The engine no longer calls it, but shm's own tests do, and readers are unaffected.

## Why

D271 moves seqno reservation into the writer process (`engine::clock::Reservation`, one allocator for seqnos and default timestamps), and shm `NEXT_SEQNO` becomes a mirror that readers keep reading for `visible_seqno`.
- **Never ahead.** The mirror must never pass a reservation. A reader whose `visible_seqno` passed a seqno before that seqno's pending watermark was published would see an unapplied commit. A `fetch_add` of the difference can overshoot when two shards race; a `fetch_max` of the reservation's end cannot.
- **Publish order kept** (FORMAT §11.3). The writer publishes its pending watermark (a lower bound at or below the seqno it is about to reserve), reserves, then raises the mirror. So a reader that sees the mirror at `M` also sees the watermarks published before the reservations below `M`.
- **No layout change.**

## Callers

- **`pigeonhole-engine`, `clock.rs`:** `Reservation::{reserve_group, reserve_id, reserve_decision}` raise the mirror after each reservation.
- **`pigeonhole-engine`, `engine.rs`:** open's catch-up raises it past every recovered seqno, then seeds the allocator from it.
