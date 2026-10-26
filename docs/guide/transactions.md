# Transactions

A `Transaction` reads at one snapshot, buffers writes, and commits them atomically across rows, tables and shards. It's optimistic: nothing is locked while it runs, and the commit checks that nothing it read has changed since its snapshot. If something has, the commit fails with `ErrorCode::Conflict` and writes nothing, and you run the whole transaction again. `Pigeonhole::transact` does that for you.

For one row, a conditional write (`RowMutation::commit_if`, see [Getting started](getting-started.md#conditional-writes-and-transactions)) is cheaper: it checks and writes on the row's shard with no read set.

## Running a transaction

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("txn.phdb"), Options::default().shards(2))?;
# let bank = pigeonhole::doc_support::table(&db, "bank", &["acct"])?;
# bank.mutate(b"alice").put_i64("acct", b"balance", 10).commit()?;
# bank.mutate(b"bob").put_i64("acct", b"balance", 0).commit()?;
let balance = |txn: &mut Transaction, who: &[u8]| -> Result<i64> {
    Ok(txn.get(&bank, who, "acct", b"balance")?.and_then(|c| c.as_i64()).unwrap_or(0))
};
let (moved, _commit) = db.transact(&RetryPolicy::default(), |txn| {
    let (a, b) = (balance(txn, b"alice")?, balance(txn, b"bob")?);
    let amount = a.min(5);
    txn.put_i64(&bank, b"alice", "acct", b"balance", a - amount)
        .put_i64(&bank, b"bob", "acct", b"balance", b + amount);
    Ok::<_, pigeonhole::Error>(amount)
})?;
assert_eq!(moved, 5);
# Ok::<(), pigeonhole::Error>(())
```

**How `transact` works:**
- **Each attempt gets a fresh transaction.** The closure reads current data every time, and it may run more than once, so keep side effects that mustn't repeat outside it.
- **What it retries:** a `Conflict`, or a snapshot expired by an automatic recovery (`SnapshotExpired`). Pauses grow exponentially, with full jitter, so competing transactions spread out. A pause below 100 µs yields the core instead of sleeping, because an OS sleep that short overshoots.
- **When it stops:** at `RetryPolicy::max_attempts` or past its `deadline` (30 s by default), returning the last error.
- **Giving up:** the closure may return your own error type, as long as `From<pigeonhole::Error>` is implemented for it. Returning it ends the call with nothing committed. That's how you give up, for example on an overdrawn account.
- **Async code** uses its own timer with the same policy: `RetryPolicy::next_pause(code, attempt, elapsed)` gives the pause before the next attempt, or `None` to give up.

## What a transaction reads, and what conflicts

**Reads:**
- **`get(table, row, family, qualifier)`** reads one cell.
- **`get_row(table, row, families)`** reads a row: the newest version of each column of the families named, or of every family when none are named.

Both read at the transaction's snapshot, and every read is recorded.

**Writes:** every `RowMutation` operation is available, with the table and row first: `put`, `put_at`, `put_i64`, `put_f64`, `incr`, `merge`, `delete_cell`, `delete_column`, `delete_family` and `delete_row`. They're buffered, and invisible to the transaction's own reads.

**What conflicts depends on how you read** (D275):
- **`get` is tracked per cell.** The transaction conflicts with a commit that writes that cell (a put, `incr`, `merge`, `delete_cell` or `delete_column` of it) or deletes its family or row. Reading `alice/acct:balance` doesn't conflict with a concurrent write to `alice/acct:nickname`.
- **`get_row` is tracked per family it read.** A commit that changes *any* column of those families in that row makes the transaction conflict, even a column it didn't use. Prefer `get` when a transaction depends on only a few columns.

**A row or cell that wasn't there counts as read too:** a commit that creates it makes the transaction conflict.

**Scans aren't available inside a transaction** (planned after 1.0). A scan outside it isn't part of what's checked.

## Isolation

Transactions are **serializable over what they read with `get` and `get_row`**: the result is as if the committed transactions ran one at a time, in commit order.
- **Lost update is prevented:** two transactions that read and increment the same balance can't both commit.
- **Write skew is prevented:** in "at least one doctor stays on call", two transactions that each check both doctors and take one off can't both commit. Under snapshot isolation they could.
- **Not covered:** rows a transaction never read. If a decision depends on "no row with this prefix exists", read the rows that would conflict (for example a counter or index row you maintain), since a scan isn't tracked.
- **Plain commits count too.** Non-transaction commits (`RowMutation::commit`, `WriteBatch`) are ordered with transactions. A transaction conflicts with any commit that touched its reads, not only with other transactions.

**Read-only transactions:**
- **They never conflict.** One that writes nothing commits at once, with no shard round, because its reads all come from one snapshot. `Ok` means what it read was consistent as of that snapshot, not that it's still current at commit.
- **To confirm a value still holds when you commit,** write something in the transaction, or use `commit_if` on the cell.
- **The commit names the snapshot's seqno** with `Durability::None`. Other commits may share that seqno.
- **Its reads are visible, not necessarily durable.** For reads of only durable data, use `Pigeonhole::durable_snapshot`.

## What a transaction costs

- **Reads cost the same as outside a transaction,** plus one read-set entry per cell (`get`) or per family (`get_row`).
- **A transaction whose reads and writes all fall on one shard** commits like a single commit: one log record, no two-phase commit.
- **One that spans shards** commits with two-phase commit: a PREPARE on each shard involved, including shards it only read from, then a decision. It costs about two commits' latency, and more at `Sync`.
- **Conflicts cost a full retry.** `EngineMetrics::transactions`, `transaction_conflicts` and `transaction_retries` show the conflict rate and what retries cost. On a few hot rows, a high conflict rate means the work is serializing on them: split the hot rows, or use a counter family (`incr` is a blind write and doesn't conflict unless you also read the cell).
- **Under heavy contention on the same rows, every conflict costs a retry.** The `transfer` benchmark (#708) compares this with RocksDB's own transactions: its `OptimisticTransactionDB` is the judged baseline, and its pessimistic `TransactionDB` is reported beside it (D274). There's no lock mode in 1.0 (D272).

## Errors

| Code | Meaning | Do |
|---|---|---|
| `Conflict` | Something the transaction read changed before it committed; nothing was written. | Run it again (`transact` does). |
| `SnapshotExpired` | An automatic recovery replaced the engine the transaction began on. Not committed. | Run it again (`transact` does). |
| `Busy` | No room within `write_stall_timeout`; not committed. | Back off and retry (`RetryPolicy::retry_busy`). |
| `OutcomeUnknown` | A shard died or the database closed while the commit was in flight; it may or may not have committed. | Read back to find out. Never retry blindly (`transact` doesn't). |
| `ReadOnly` | A reader process or a read-only open can't run transactions. | Use the writer. |
