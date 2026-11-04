### Added
- **`phdb serve`** (D313 ticket 3a, ICR 0112, #1397), in a build with the `serve` feature: one process serves a database over gRPC and HTTP/JSON on one port (default `127.0.0.1:7448`).
  - This build serves `Data`'s single-call RPCs: `Get`, `MultiGet`, `ReadRow` (paged by bytes), `Mutate`, `CheckAndMutate`, `Batch` and `MutateReturning`. Each write runs as its own task, so a client that disconnects loses only the reply.
  - **Security:** TLS, bearer tokens with `read`, `write` and `admin` scopes from a file or the environment, and a non-loopback address refused without both unless explicitly opted out.
  - **Limits and lifecycle:** request size, in-flight bytes and concurrency limits, with overload refused at once as `Overloaded`; `/v1/health`, `/metrics`, and a graceful shutdown that closes the database.
  - **Threads:** on Linux, the network threads run off the shards' CPUs.
