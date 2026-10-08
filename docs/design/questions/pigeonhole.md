# Questions: counter families (#274, D179)

Raised while implementing D179 across pigeonhole, engine, compaction, format and sim.

## Proposed decision: the family kind is stored, not derived from the operator
A family's kind is a new `FamilyOptions::kind` (`FamilyKind::{Standard, Counter}`), appended as one byte to the family options in the manifest (`PutFamily`). Edit bodies already ignore trailing bytes, so the change is additive within format version 1: a body that ends before the byte reads as `Standard`; an unknown value is `Corrupt`. FORMAT.md §9.2 says so. A golden file of a 0.1.0 snapshot (`manifest_snapshot_0_1.bin`) pins the old encoding.

Deriving the kind from `merge_operator == "pigeonhole.i64_add"` was rejected: every 0.1.0 family stores that name, so every existing family would have become a counter family and refused byte puts.

**Interim behavior:** as above. `Family::counter()` stores kind `Counter` with `pigeonhole.i64_add`; the engine refuses a counter kind with any other operator (`InvalidArgument`). The kind is fixed at creation (an existing family keeps its stored options).

## Proposed decision: 0.1.0 families keep 0.1.0 semantics
A family written by 0.1.0 has the `i64` operator and kind `Standard`. It keeps exactly its 0.1.0 behavior: `incr` writes at the commit timestamp, runs of operands fold across timestamps at read (D41), byte puts are allowed (and fail at read under an `incr`, `MergeFailed`).

The issue suggested treating such a family as a counter family for reads, with new `incr`s at the fixed timestamp. That breaks reads: per-timestamp resolution of old commit-timestamp operands returns one increment per version instead of the sum, and a 0.1.0 base `put_i64` at commit timestamp T would sit above every later increment at timestamp 0, so the counter would stop moving.

**Interim behavior:** as above; `incr_at` on such a family is refused (buckets are for counter families). `Family::default().merge_operator("pigeonhole.i64_add")` creates one. CHANGELOG has the migration note (copy counters with `get` + `put_i64` into a counter family).

## Proposed decision: in a counter family a delete hides only earlier writes (by seqno)
D179 says deletes "work normally". With the fixed timestamp they cannot keep timestamp scope: a `delete_column` at commit timestamp T covers timestamp 0 forever, so every later `incr` would stay hidden until a compaction purged the marker. Following Bigtable, a delete in a counter family hides the entries in its timestamp scope with a **lower seqno** only. Same-commit writes are not hidden (equal seqno; D34 still collapses same-cell writes). Snapshots read the operands they see, ordered by seqno.

**Interim behavior:** `ResolveOptions::counter` / `GcConfig::counter` / `ModelFamily::counter` implement it; ordinary families are unchanged.

## Proposed decision: counter-family compaction never changes reads
Consequences of seqno-scoped deletes in compaction (`gc.rs`, `decide_counter`):
- An entry is dropped when a delete in its own stripe with a higher seqno covers it; a delete is redundant next to a wider one in its stripe with a higher seqno; bottommost purges of stripe-0 deletes below `min_ts_above` are unchanged.
- Two operands are combined only if no other source of the slot can hold a delete with a seqno between theirs: a delete there would hide the older but not the newer. `GcPolicy::other_sources` lists the slot's non-input SSTs (with key ranges) and memtables / prepared shares (unbounded keys) that may hold a seqno at or below the newest input seqno; `None` means unknown (no combining). Sources entirely above the inputs are left out, so in practice a hot counter compacts to one entry.
- Versions beyond `max_versions` are **never purged**: a later delete of a newer bucket, or its expiry under a TTL, must show the older one again (found by the compaction proptest; seeds kept in `counter_versions_outlive_newer_expired_buckets`). Reads apply the limit; space is bounded by the TTL. Follow-up decision: #284.

So `Model::purge` is a no-op for counter families.

## Q: a counter family with a TTL
TTL applies per bucket (`ts + ttl <= now`), so the fixed timestamp 0 would expire at once. **Interim behavior:** in a counter family with a TTL, a put or operand without a timestamp fails with `InvalidArgument` (use `incr_at` / `put_i64_at`). Alternative: exempt the fixed timestamp from the TTL.

## Q: two increments of one counter in one mutation
D34 collapses same-cell writes in a commit to the last one, so `.incr(c, 1).incr(c, 2)` adds 2. This was already true in 0.1.0 (both at the commit timestamp). Counters could combine same-commit operands instead. **Interim behavior:** unchanged (D34), documented on `RowMutation::incr`.

## Q: `#[non_exhaustive]` on option structs
Adding `FamilyOptions::kind` is semver-breaking (exhaustive public struct), hence the 0.2.0 bump. `#[non_exhaustive]` on `FamilyOptions` (and `ModelFamily`) would make later fields additive, but also forbids `FamilyOptions { .., ..Default::default() }` outside the defining crate, which the engine, compaction and test crates use in dozens of places; they would need constructors or builder methods first. **Interim behavior:** not added in #274; proposal: add builder-style setters to `FamilyOptions`, migrate callers, then mark it `#[non_exhaustive]` (with `ModelFamily`) before 1.0. (`ResolveOptions` and `GcPolicy` are already non-exhaustive.)

## Note: model-check harnesses and arena slots
The engine and public harnesses keep four families per table; a new `counters` test target in each runs the same suites with `f`, a 0.1.0-style `counter`, and the counter families `sum` and `sum_ttl` (the family set is chosen by `env!("CARGO_CRATE_NAME")`). Six families overflowed a tablet-off shard's arena (16 slots), which stalls or hangs: #283.
