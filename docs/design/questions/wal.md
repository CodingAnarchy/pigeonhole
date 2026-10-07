# WAL: open questions and proposed decisions

## Proposed decision: amend D35 — an open writes one frame per stream, and spares wait for use (#143)
D35 zero-filled the first slot of every stream at open, and the engine prepared `spare_segments` more right away. At the defaults (64 MiB segments, two spares) every open wrote 192 MiB per shard, with one shard per CPU, and the clean close deleted it all. Measured on a 10-core Mac: 454 ms and 2.5 GiB written per open and close of a tiny database (51 ms and 256 MiB with one shard).

**Decision:**
- `WalStream::create` and `Recovery::into_stream` start the first segment in a slot added past the file's end. The slot was never written, so it reads as zeros and is not zero-filled. The header is written and synced together with the new length (`sync_all`).
  - On Linux the slot is allocated with `fallocate` (unwritten extents, metadata only). Its space is reserved, so a full disk fails the open rather than poisoning the stream at a later append. The zero-read guarantee also holds on filesystems that could otherwise expose stale freed blocks in a delayed-allocation hole.
  - Elsewhere the slot is a sparse extension. APFS has no unwritten extents: `F_PREALLOCATE` plus the length change physically wrote the whole slot (measured: 64 MiB per shard per open, 156 ms to open 10 shards against 14 ms sparse), and APFS holes read as zeros by design. On a full disk such a stream can still fail at an append and poison, as an inline grow at rollover already could.
- A recycled slot is used as is, as before. A *blank* slot is still zero-filled first: one found at recovery, or bytes past the stream's known slots (a failed spare preparation that grew the file). Frames of a segment whose header write was torn can carry the epoch the new segment takes.
- Fragments carry only an epoch and a CRC, no database id or salt. A reopen after a clean close starts epoch-1 streams at the same offsets as the deleted previous session's files. A filesystem that could expose stale freed blocks in a never-written allocated range after a crash would break the zero-read assumption. Allocated, unwritten ranges read as zeros on the local filesystems Pigeonhole supports (D37 refuses network filesystems). A per-database salt in fragments would remove the assumption: #176 (Phase 4).
- The price: the first segment's fdatasyncs also convert its unwritten blocks, which D35's zero-filled spares avoid for every later segment. A writer pays that only until its first rollover.
- `WalStream::create_all` creates several streams, submits every stream's `sync_all` before waiting on any (new `File::submit_sync_all`: pread runs it on its pool, `SimVfs` inline), and syncs the directory once. Opening N shards then costs about one sync, not N.
- The engine runs `SpareSegments::prepare` only once its stream is half way through the segment it opened in (or past it). A small database never zero-fills a spare. A busy one has half a segment of writes to prepare spares before its first rollover needs one.
- Segment size stays 64 MiB: a smaller default would lower the D16 value limit, which is `min(segment payload, …)`.

**Interim behavior:** as described. Measured after (same machine, same tiny database): open 14 ms median at 1, 4 and 10 shards; 0.11–0.39 MiB written per open and close; about 1 MiB of disk held while open.
