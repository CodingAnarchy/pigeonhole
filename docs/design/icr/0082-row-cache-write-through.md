# 0082: the row cache's write-through hooks (D290)

**Status:** Approved (coordinator, 2026-10-27; #708, D290). Additive only: new methods, plus one field on a `#[non_exhaustive]` struct. Nothing existing changes signature.

## Change

- **`pigeonhole-shm`:** `RowEpochTable::note_write_prev(&self, hash: u64, seqno: u64) -> u64`.
  - It is `note_write` returning the slot's value before the raise.
  - Only the writer calls it, from `Shard::apply` with the row cache on. `note_write` stays for the cache-off path.
- **`pigeonhole-cache`:** `RowCache::update(&self, family: u64, row: &[u8], from: u64, to: u64, edit: impl FnOnce(&mut Vec<u8>) -> bool) -> bool`.
  - It moves the row cached at epoch `from` to `to`, editing its encoding in place under the shard's lock.
  - It keeps the entry's queue position and hit count. A held `RowHandle` keeps the old encoding: the entry is copied first while one is held.
  - `edit` returning `false` drops the entry.
- **`pigeonhole-cache`:** `RowCache::stale_unused(&self, family: u64, row: &[u8], epoch: u64) -> bool`.
  - Whether the row is cached at another epoch and was never read there. A get then skips its fill (D290's get-fill safeguard).
- **`pigeonhole-engine`:** `Engine::row_cache_write_throughs(&self) -> u64`, the entries a commit moved forward since open.
  - It's a method rather than a `RowCacheStats` field, because that struct isn't `#[non_exhaustive]`.
- **`pigeonhole`:** `RowCacheStats::write_throughs` (the struct is `#[non_exhaustive]`).

## Why

D290's write-through (#708 c6455731499, reviewed in c6455806598):
- The shard that applies a commit needs the row's previous watermark `p` from the same `fetch_max` that raises it.
- It then moves the family row cached at `p` forward to the commit's seqno without re-encoding or re-inserting it.
- The counter is for the box A/B, which reports write-throughs beside the shard's turn time.

## Callers

- **`note_write_prev`:** `pigeonhole_engine::row_cache::RowEpochs::note_write_prev`, from `Shard::apply`.
- **`RowCache::update`:** `RowCaches::write_through`, from `Shard::follow_commit`.
- **`row_cache_write_throughs`:** `pigeonhole::RowCacheStats::from_engine`. The bench's `Stalls::row_cache_write_throughs` reads it through that.
- `cargo semver-checks` sees added methods and a field on a non-exhaustive struct (minor).
