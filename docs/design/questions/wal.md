# WAL: open questions and proposed decisions

## Proposed decision: amend D35 — an open writes one frame per stream, and spares wait for use (#143)
D35 zero-filled the first slot of every stream at open, and the engine prepared `spare_segments` more right away. At the defaults (64 MiB segments, two spares) every open wrote 192 MiB per shard, with one shard per CPU, and the clean close deleted it all. Measured on a 10-core Mac: 454 ms and 2.5 GiB written per open and close of a tiny database (51 ms and 256 MiB with one shard).

**Decision:**
- `WalStream::create` and `Recovery::into_stream` start the first segment in a slot added past the file's end. The slot is sparse and reads as zeros, so it is not zero-filled. The header is written and synced together with the new length (`sync_all`). A recycled slot is used as is, as before. A *blank* slot found at recovery is still zero-filled first: frames of a segment whose header write was torn can carry the epoch the new segment takes.
- The price: the first segment's fdatasyncs also allocate its blocks, which D35's spares avoid for every later segment. A writer pays that only until its first rollover.
- `WalStream::create_all` creates several streams, submits every stream's `sync_all` before waiting on any (new `File::submit_sync_all`: pread runs it on its pool, `SimVfs` inline), and syncs the directory once. Opening N shards then costs about one sync, not N.
- The engine runs `SpareSegments::prepare` only once its stream is half way through the segment it opened in (or past it). A small database never zero-fills a spare. A busy one has half a segment of writes to prepare spares before its first rollover needs one.
- Segment size stays 64 MiB: a smaller default would lower the D16 value limit, which is `min(segment payload, …)`.

**Interim behavior:** as described. Measured after (same machine, same tiny database): open 14 ms median at 1, 4 and 10 shards; 0.11–0.39 MiB written per open and close; about 1 MiB of disk held while open.
