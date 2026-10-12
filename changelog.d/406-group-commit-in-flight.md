### Added
- `phdb-bench --in-flight N` sets the commits each shard's thread keeps in flight in an inline run (`RunOptions::in_flight`, default 16; `GroupCommit::in_flight`). The gate window also runs the D213 group commit at 4 and 8 in flight, and its report shows that trade-off beside the judged 16 row (D226). A group-commit result at another count is reported, not judged.
