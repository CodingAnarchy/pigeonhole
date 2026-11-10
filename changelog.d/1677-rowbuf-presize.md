### Changed
- A point read (`RowRead::read`, `read_async`) sizes the row it returns from the largest row it has read from the table, rather than growing its buffers from empty: a warmed read of a 33-cell row makes 8 allocator calls instead of growing by doubling, and glibc's `realloc` was the top allocation site on read-heavy work (#1558).
