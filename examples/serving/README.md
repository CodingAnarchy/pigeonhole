# examples/serving

The reference pattern for sharing one Pigeonhole database across processes and hosts: one process owns the database file and serves a small HTTP API, and workers anywhere call it. Read it with the guide's page, [`docs/guide/serving.md`](../../docs/guide/serving.md), which explains each part and its failure handling.

- `src/lib.rs`: the server (axum on Tokio): jobs with leases and fencing tokens, transfers in `transact` with an operation id, and the change feed as server-sent events.
- `src/client.rs`: the workers' HTTP client.
- `src/bin/server.rs`, `src/bin/worker.rs`: the two programs.
- `tests/serving.rs`: the scenarios against a real server on loopback, including concurrent workers, a stalled worker fenced off, and a watch resumed after a reconnect.

```sh
cargo run -p pigeonhole-example-serving --bin serving-server -- jobs.phdb 127.0.0.1:7070
cargo run -p pigeonhole-example-serving --bin serving-worker -- http://127.0.0.1:7070 worker-1
cargo test -p pigeonhole-example-serving
```

It's an example to read and copy, not a published crate.
