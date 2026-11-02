### Added
- Set families (`Aggregation::Set`, D298, ICR 0096): a cell holds a set of `u32`s as a standard roaring bitmap (FORMAT §3.8), which CRoaring and roaring-rs read as stored. Write with `set_add` and `set_remove` (and their `_at` forms) on `RowMutation`, `WriteBatch` and `Transaction`. Read in place with `Cell::as_set()` and `SetRef` (`len`, `contains`, `iter`, `intersection_len`, `intersection`, `union_len`). `phdb` takes `"aggregation": "set"`, and its shell's `aggregate` takes `+x`/`x` to add and `-x` to remove.

### Changed
- Reads keep a folded set value for later reads when it is at most 256 KiB. Other aggregate kinds keep the 4 KiB limit (D298).
