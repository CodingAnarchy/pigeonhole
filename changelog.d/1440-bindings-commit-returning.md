### Added
- Python and Ruby: increments that return the counter's value after the commit (D314, ICR 0113, #1440).
  - **Python:** `Mutation.commit_returning()` returns an `Incremented` (`value(family, qualifier, ts=None)`, `r[family, qualifier]` or `r[family, qualifier, ts]`, iteration over the keys, `cells()` and `seqno`), and `Table.incr_returning(...)` returns the new value.
  - **Ruby:** `Mutation#commit_returning` returns a frozen `Pigeonhole::Incremented` (`value(family, qualifier, ts: nil)`, `to_h`, `size` and `seqno`), and `Table#incr_returning(...)` returns the new value, with the GVL released.
  - **`ReturnedValueError`** (both bindings) is raised when the commit applied but reading a value after it failed: the increment landed, so don't retry it. A failed commit keeps its usual exception.
