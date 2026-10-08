# Compaction questions (Phase 2)

## Proposed decision: fold counter operands across timestamps (#34; amends D73)
D73 deferred this to an owner decision; the proposal there is implemented here and needs confirming.

**Interim behavior:** a bottommost compaction folds operands across timestamps in a column when the family has a merge operator and no TTL, and every input entry of the column is below `min_ts_above`. Folding covers only the column's *plain prefix*: its kept entries, newest first, up to the first one that fails any of these:
- it is a put or operand;
- it is in stripe 0 (visible at every read point: no live snapshot is older than it);
- no delete or marker covers it at any read point.

In that prefix, each run of operands becomes one operand at the newest one's key. Folded onto the put right below it, the run becomes one put at that key. Three cases are left unfolded:
- A base the operator refuses stays below the folded operands (#21: the read still fails with `MergeFailed`).
- A blob base is never folded away, so no blob accounting is needed.
- An operand the operator refuses ends folding for the rest of the column, so a read that failed before still fails. Without this, the tail below it could fold onto the base and turn `MergeFailed` into a value.

The run is streamed: one accumulator and one key, never a buffer of the column, so a counter with millions of increments costs O(1) memory.
- **Why these conditions.** In the prefix every read point sees the same versions, and nothing above the inputs reaches into a run (it is all newer than `min_ts_above`). Entries a snapshot sees differently, entries a kept delete hides at some read point, and TTL expiry would each split a run in some view.
- **Accepted consequence (the reason this is an owner decision).** A later write with an explicit timestamp inside a folded span sees the fold, not the individual increments. Default-timestamp writes are unaffected while the clock moves forward, and `incr` cannot take an explicit timestamp, so this concerns `put_at` and `delete_cell` only. Take a base `B@t0` and increments `d1..dn` at `t1 < … < tn`, folded to `Put(B+Σd)@tn`:

  | Later write | Without the fold | After the fold |
  |---|---|---|
  | `delete_cell(tk)`, 0 < k < n | B + Σ(i≠k) dᵢ | B + Σd (hides nothing) |
  | `delete_cell(t0)` | Σd (base gone) | B + Σd |
  | `delete_cell(tn)` | B + Σ(i<n) dᵢ | **the whole counter hidden** (the next version below t0, or nothing) |
  | `put_at(V, T)`, t0 < T ≤ t(n−1) | V + Σ(tᵢ>T) dᵢ | B + Σd (V becomes an older version) |
  | `put_at(V, tn)` | V | V |

  The options: (a) accept this (implemented here), (b) don't fold, so counters grow without bound; (c) fold only in families that opt in.
- **Model.** `pigeonhole_sim::Model::purge` gains the same rule as step 3. It walks the column's input entries newest first, skips puts and operands that contribute to no version at any read point (the store drops them), and stops at the first delete, or put or operand that is not in stripe 0 or is covered by an input delete or marker. It then folds each version's part in that prefix, and removes the skipped entries. The engine and public model oracles therefore stay strict across later explicit-timestamp writes. The compaction read-preservation proptest checks exactly that: it purges, then flushes later writes with explicit older timestamps and cell deletes. It passed 10,000 cases.
