# 0003: `pigeonhole-sim` model: fallible reads, timestamp-only cell deletes, family order

**Status:** Approved (coordinator, decisions audit, 2026-10-06). Implements owner decisions D38 and D39 and audit item C2 (D41).

## Change

1. **Additive.** `ModelError::MergeFailed(String)` (the family name) and three fallible read methods, `Model::try_get`, `Model::try_read_row` and `Model::try_scan`, with the same arguments as `get`, `read_row` and `scan` and their results wrapped in `Result<_, ModelError>`. A read fails when a version it returns folds merge operands onto a base put whose value is not an 8-byte `i64`. The existing `get`, `read_row` and `scan` keep their signatures and panic in that case, like `commit` versus `try_commit`. A put with no operand folded onto it is returned as written whatever its length.
2. **Behavior (D38).** A `DeleteCell` at timestamp `T` hides every version at `T` whatever its seqno, including a put or merge operand at `T` committed after the delete. It used to hide only older-seqno entries.
3. **Behavior (D39).** `read_row` and `scan` return a row's families in creation order (the order `create_table` first saw each family), or in the caller's order when `families` is non-empty; a family listed twice or not in the table is skipped. Within a family, cells stay ordered by qualifier, then timestamp descending. They used to be ordered by family name.

## Why

(1) The user guide promises `MergeFailed` for a counter whose base is not an `i64` (errors.md, data-modeling.md); the model silently counted it as 0. (2) and (3) are owner decisions from the audit.

## Callers

- `crates/sim/tests/common` (the toy store uses the model as its index, so store and model change together; the checker compares the two). Updated in the same PR.
- `pigeonhole-engine` and `pigeonhole` full-stack suites: not written yet. Their test adapters compare engine reads against the model directly; the engine already iterates families in creation order (`engine.rs`), so no normalization is needed.
- `ModelError` is matched exhaustively nowhere outside `pigeonhole-sim`.
