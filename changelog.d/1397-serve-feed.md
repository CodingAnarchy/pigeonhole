### Added
- **`phdb serve`: `Feed.Subscribe`** (D313 ticket 5, ICR 0112, #1397): the change feed as a gRPC server stream, or over HTTP as NDJSON or server-sent events (each event's `id:` is its token, so `Last-Event-ID` resumes).
  - Streams on the same table set share one engine subscription: the shard threads wake one reader for it, and the WAL is read once, whatever the number of subscribers.
  - A stream that starts from a token, a snapshot or the oldest commit, or falls behind, reads on its own until it catches up. Its events and tokens are the same either way.
  - Quiet streams send progress events and keepalives.
  - New flags: `--max-streams` (256), `--max-feed-hubs` (32), `--max-catch-up` (16) and `--feed-ring-bytes` (256 MiB, in all).
  - Each stream buffers up to 64 events (256 KiB at most) its client hasn't taken, so events that are ready go out together. Under load, this halves the server CPU each subscriber costs per commit. A client connection carries at most 128 streams: open another connection for more.
