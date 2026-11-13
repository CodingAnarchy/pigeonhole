# Serving a database: `phdb serve`

> **Status:** this page describes `main`. `phdb serve` is new in 1.2.0 (D313, #1397), behind the `serve` feature. [ICR 0112](../design/icr/0112-network-api.md) is the full contract.

`phdb serve` owns one database file and serves it to other processes and hosts:
- gRPC and HTTP/JSON on one port;
- remote MCP at `/mcp`;
- a change feed, read replicas, backups and Prometheus metrics.

Use it when several containers or hosts need the same data and a generic API suits them. When your operations are few and known, a server of your own may fit better: [Sharing a database across processes and hosts](serving.md) shows that pattern. When the other processes are on the same host and only read, [reader processes](readers.md) open the file directly, with no server at all.

## Running it
**Install** a `phdb` built with the `serve` feature:

```sh
cargo install pigeonhole-cli --features serve
# With an embedding model run in the server, for agent memory (below):
cargo install pigeonhole-cli --features serve,embed-local
```

**Start** it on a database file. `--create` makes the file if it's missing. For a quick try, add `--no-wal-preallocate`, so the start doesn't write each shard's log in full first (see "Start-up writes the logs in full" below):

```sh
phdb serve app.phdb --create
```

```text
phdb serve: serving app.phdb on 127.0.0.1:7448 (no TLS, no tokens; 8 network, 2 feed, 1 feed hub and 10 blocking threads on 10 free CPUs, unpinned; 0 shard CPUs left out, 0 compaction CPUs not)
```

- **The address:** by default `127.0.0.1:7448`; `--listen ADDR:PORT` sets another. gRPC (`/pigeonhole.v1.Data/Get`) and HTTP/JSON (`POST /v1/Data/Get`) share the port.
- **The startup line** says what the server runs with: TLS, tokens, its threads, and where they run ([Threads and placement](#threads-and-placement)).
- **Stopping:** Ctrl-C or SIGTERM. In-flight requests get `--shutdown-grace-ms` to finish, and then the server closes the database cleanly.
- **The shards:** `--shards` sets the count. The default is half the physical cores, which leaves the other half for the network side. It applies on every start, a restart included, since the count isn't stored in the file. A database with a change feed is the exception: it keeps the count its feed recorded, because another count ends the feed's history (`--shards` with another count reshards it on purpose).
- **Start-up writes the logs in full** (#1621, ICR 0130). Before it accepts connections, `serve` gives each shard's write-ahead log every slot it holds when busy, zero-filled and synced. That's 5 slots of 64 MiB a shard at the defaults (two for unflushed writes, two spares and the slot in use), plus the change feed's retention spread over the shards (at least 2 slots a shard with the feed on). Otherwise each log grows while the server runs, and its zero-fill lengthens commits' tail latency for as long as it grows.
  - **Disk:** slots × 64 MiB per shard, from the start. At the defaults that's 320 MiB a shard: about 1.25 GiB on 4 shards and 5 GiB on 16. With the feed on, a shard holds at least 7 slots (about 1.75 GiB on 4 shards, 7 GiB on 16). With `--change-feed 4294967296` (4 GiB) on 16 shards, it's 9 slots a shard, about 9 GiB.
  - **Time:** the open writes and syncs all of it, every shard's log in parallel, which takes a few seconds on NVMe before `serving ... on` appears. A database whose feed is on keeps its logs across a clean stop, so a restart adds no slot and only zero-fills again the ones the last run never used. Without the feed the logs are removed at a clean stop, and the next start writes them again.
  - **`--no-wal-preallocate`** turns this off: the logs start at one slot a shard and grow on demand. Use it for a small or development database, tests, or a server with little write traffic, where the disk and the start-up time aren't worth it.

### Security
- **TLS:** `--tls-cert` and `--tls-key` take PEM files; every connection is then TLS.
- **Tokens:** `--token-file`, or the `PHDB_SERVE_TOKENS` environment variable, holds one token per line: `read|write|admin [ns=a,b] <token>`, or `sha256:<hex>` for the token's digest. Requests carry `Authorization: Bearer <token>`. Tokens are never a flag's value.
  - With both set, the server takes the file's tokens and the variable's.
  - **Tokens are read once, at start.** To add, change or retire a token, change the file (or the variable) and restart the server. To rotate without refusing anyone, list the old and the new token, restart, move the clients over, then remove the old one and restart again.
  - **A token file or variable that is set but holds no token fails the start,** rather than serving without authentication.
- **The bind rule:** an address other hosts can reach needs both TLS and tokens, or the server refuses to start. `--insecure-no-tls-no-auth` overrides that for a trusted private network, except on a cluster across hosts, which needs TLS whatever it says ([Running a cluster](#running-a-cluster)). A loopback address needs neither.

A token file:

```text
# scope [ns=...] token, or sha256:<hex> of the token. Lines starting with # are comments.
admin  sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
write  ns=team-a  team-a-agent-token
read   reporting-token
```

- **Scopes:**
  - `read` reads, opens snapshots, and runs transactions without writes.
  - `write` adds writes, `CreateTable`, the index writes and the memory writes (`Remember`, `MarkUsed`, `Supersede`, `Link` and `Forget`).
  - `admin` adds `DropTable`, the maintenance calls, backups, `Promote`, and following as a replica.
  - `cluster` is only for node-to-node calls ([Clusters](#clusters)).
- **`ns=a,b`** limits a token to agent memory in those namespaces ([Agent memory](#agent-memory)). It reaches only the `Memory` service and `/mcp`'s memory tools: every other call (data, transactions and snapshots, schema, the change feed, graphs, vectors, text, admin, `/metrics` and `/mcp`'s data tools) answers `PermissionDenied`, and `/mcp` doesn't list the data tools to it.

A server other hosts reach:

```sh
phdb serve /var/lib/phdb/app.phdb --create --listen 0.0.0.0:7448 \
  --tls-cert /etc/phdb/cert.pem --tls-key /etc/phdb/key.pem \
  --token-file /etc/phdb/tokens --backup-dir /var/lib/phdb/backups
```

## Clients
### HTTP/JSON
Every method is `POST /v1/{Service}/{Method}`, with the request message as its JSON body. The names are the proto's (`proto/pigeonhole/v1/*.proto`).

The JSON is proto3's mapping:
- **Bytes are base64.** Keys, qualifiers and values are bytes: `user:1` is `dXNlcjox`.
- **64-bit integers are strings** (`"seqno": "1"`).
- **Enums are their names** (`"DURABILITY_SYNC"`).
- **A field at its default is left out** of a reply: read an absent field as `0`, `false`, `""` or empty.

Add `-H "Authorization: Bearer $TOKEN"` to each call when the server has tokens.

```sh
# A table with one family.
curl -s localhost:7448/v1/Schema/CreateTable -H 'Content-Type: application/json' \
  -d '{"name": "users", "families": [{"name": "profile"}]}'

# Put profile:name = "Ada" in row user:1.
curl -s localhost:7448/v1/Data/Mutate -H 'Content-Type: application/json' -d '{
  "writes": {"row": {"table": "users", "row": "dXNlcjox"},
             "writes": [{"put": {"family": "profile", "qualifier": "bmFtZQ==", "value": {"bytes": "QWRh"}}}]}}'
# {"commit":{"seqno":"1","durability":"DURABILITY_GROUP_SYNC"}}

# Read it back.
curl -s localhost:7448/v1/Data/Get -H 'Content-Type: application/json' \
  -d '{"table": "users", "row": "dXNlcjox", "family": "profile", "qualifier": "bmFtZQ=="}'
# {"cell":{"family":"profile","qualifier":"bmFtZQ==","timestamp":"1794333866442931","value":"QWRh"}}

# Scan the rows under user: (one JSON page per line, or SSE with Accept: text/event-stream).
curl -s localhost:7448/v1/Data/Scan -H 'Content-Type: application/json' \
  -d '{"table": "users", "range": {"prefix": "dXNlcjo="}}'
# {"rows":[{"key":"dXNlcjox","cells":[...]}],"bytes":"29"}
```

An error is its status with an `ErrorDetail` body:

```sh
curl -s localhost:7448/v1/Data/Get -H 'Content-Type: application/json' \
  -d '{"table": "nope", "row": "dXNlcjox", "family": "profile", "qualifier": "bmFtZQ=="}'
# 404: {"code":"TableNotFound","code_num":8,"message":"no such table \"nope\""}
```

**Shortcuts:**
- `GET /v1/health` needs no token.
- `GET /v1/tables` lists the tables.
- `GET /metrics` is the Prometheus text, with read scope.

### gRPC
- **Clients:** generate one from `proto/pigeonhole/v1/*.proto` (package `pigeonhole.v1`) with your language's gRPC toolchain: tonic and prost in Rust, grpcio-tools in Python, protoc-gen-go in Go. The server has no reflection service, so tools such as grpcurl need the proto files (`-import-path proto -proto pigeonhole/v1/data.proto`).
- **Errors** carry a `google.rpc.Status` detail with the same `ErrorDetail` ([Errors and retries](#errors-and-retries)).
- **Don't configure automatic retries for writes.** Reads may be retried; a write may be retried only when its error says `retry_safe`.
- **Streams:** one HTTP/2 connection carries at most 128 streams at once ([Limits](#limits)). Feed clients should turn on adaptive flow-control windows ([The change feed](#the-change-feed)).

### Remote MCP
`/mcp` serves `phdb mcp`'s tools over MCP's Streamable HTTP transport: [the agent guide's remote section](ai-agents.md#remote-mcp-phdb-serve) has the client configuration, scopes and sessions. Its own flags are `--mcp-max-result-bytes`, `--mcp-max-value-bytes`, `--mcp-session-idle-ms`, `--mcp-session-lifetime-ms`, `--mcp-session-requests` and `--mcp-session-cache-bytes`. A `feed_poll` with `wait_ms` waits for its first commit without holding a blocking thread, under a `--max-streams` place. Inside a JSON-RPC batch, `feed_poll` doesn't wait: it returns what's ready.

## Reads, writes and sessions
- **Scans** stream pages of up to `page_bytes` each (default 1 MiB). Over gRPC, `Data.Scan` is a server stream. Over HTTP, `POST /v1/Data/Scan` answers one JSON page per line (`application/x-ndjson`), or server-sent events with `Accept: text/event-stream`. A scan that fails partway ends with a final error message: an NDJSON line `{"error": ...}`, or an SSE `event: error`. Each page's `next` resumes the scan later, from any server process. The scan is complete only when the last page's `next` is empty; a page budget too small for anything ends the stream with an empty page and a position, so resume with a larger `page_bytes`. Each page reads at its own snapshot: a row is never torn, but rows can change between pages. A client that stops reading for `--stream-idle-ms` (default 30 s) gets `DeadlineExceeded`, and resumes from the last page's `next`.
- **Transactions** (`Transactions.Begin`, `Get`, `GetRow`, `Write`, `Commit`, `Abort`) are held on the server between calls, under an opaque `txn_id` bound to the token that began them:
  - **One call at a time.** A second call while one runs gets `SessionBusy`; send it again when the first answers.
  - **`Commit`** runs to its outcome even if the client goes away. Sent again, it answers the first one's outcome and commits nothing twice. After `Commit`, other calls answer `TransactionFinished`.
  - **Leases:** a transaction idle for `--txn-idle-ms` (30 s), or older than `--txn-lifetime-ms` (5 min), expires. Before any `Commit`, that's `TransactionExpired`: nothing was written, so begin again. A call in flight never expires.
  - **`NotFound` for a `txn_id` you sent `Commit` to** (after a restart, or once its record expires) means the outcome is unknown: check an operation id the transaction wrote before retrying ([recovery](recovery.md)).
  - A read token may begin, read and commit a transaction without writes; writing needs a write token.
- **Pinned snapshots** (`Snapshots.Open`, `Release`) make reads agree across requests: pass the `snapshot_id` as `GetRequest.snapshot_id`, or as `Selection.snapshot_id` for `MultiGet`, `ReadRow` and `Scan`. Each read renews the snapshot's lease (`--snapshot-idle-ms`, 60 s; at most `--snapshot-lifetime-ms`, 10 min). A held snapshot holds back version and blob GC, so release it when done. Reads already running when it's released finish; later ones, and a scan's later pages, get `NotFound`. So do a scan's pages past `--snapshot-lifetime-ms`: a scan read slowly ends there with `NotFound`, resumable from its last page's `next` at a new snapshot, and the snapshot goes when the scan ends.

### Errors and retries
- **Errors** carry ICR 0112's stable code: a gRPC status detail, or the HTTP JSON body (`{"code": "TableNotFound", ...}`). Retry a write only when the error says `retry_safe`. A bare transport error means its outcome is unknown. A read may be retried; one that failed on `Io` says so (`retry_from: REQUEST`). `MergeFailed` doesn't: a merge operator that fails on stored data fails the same way again. `Unavailable` (503 with `Retry-After`, not applied, retry-safe) means a service the server depends on failed or can't be reached: today `Memory`'s embedding endpoint (see Agent memory below), or, on a replica, a memory store the replica doesn't have yet. `FailedPrecondition` (409, not applied, don't retry) means the server's role refuses the call: `Admin.Promote` on a server that isn't a replica or on one whose first bootstrap hasn't finished, a replica asked for a bootstrap copy or to be followed (see [Replicas](#replicas)), or a `DropTable` of the served memory store's tables.

The rules a client keys on (D313 B3, [ICR 0112's error table](../design/icr/0112-network-api.md#errors)):

| The response | The request was | Retry |
|---|---|---|
| `retry_safe: true` with `retry_from: REQUEST` (`RETRY_FROM_REQUEST` on the wire) (`Busy`, `Overloaded`, `SessionBusy`, `Closed`, `ReplicaBehind`, `Unavailable`, `DeadlineExceeded`, and a read's `Io`) | not applied | send it again, after `Retry-After` where given |
| `retry_safe: true` with `retry_from: BEGIN` (`Conflict`, `SnapshotExpired`, `TransactionExpired`) | not applied | run the transaction again from `Begin` |
| `retry_safe: false` with a validation, schema, scope or role code (`InvalidArgument`, `TableNotFound`, `PermissionDenied`, `ReadOnly`, `FailedPrecondition`, ...) | not applied | no: fix the request |
| `retry_safe: false` with a 503 the server can't help (`WriterLocked` and the other codes for a database it can't open) | not applied | no: the server's database needs attention |
| `retry_safe: false` with any code, from a `Vectors.BulkLoad` that had taken vectors (its message says the vectors before it were loaded, or may have been) | part applied: the vectors before the failure, or some of them | no: `Insert` the rest, or drop the index and load again (a load needs an empty index) |
| `TransactionFinished` | that call did nothing | no: send `Commit` for the transaction's outcome |
| `FeedExpired` | (a read) | no: subscribe again from now |
| A write's `OutcomeUnknown`, `Io`, `NoSpace`, `MergeFailed`, `Corruption` or `Internal`, or `NotFound` for a `txn_id` you sent `Commit` to | **maybe applied** | check an operation id the write committed ([Recovery](recovery.md)) |
| No `ErrorDetail` at all: a dropped connection, or a bare gRPC `UNAVAILABLE` or HTTP 502/503/504 from a proxy | **maybe applied**, for a write | as above. A read may be sent again. |

**A proxy in front of the server must not retry writes.** Many retry a 503 or a reset connection by default, which replays a write whose outcome is unknown.

## Schema, admin and backups
### Schema
- `Schema.ListTables` (also `GET /v1/tables`) and `DescribeTable` need read scope, `CreateTable` write scope, and `DropTable` admin scope (it drops the data).
- `CreateTable` takes each family's options as `FamilySpec`, and with `if_missing` opens an existing table and adds only the families it lacks (adding families is additive, so write scope does it too).
- `DescribeTable` returns every option as stored, so its output creates the same table again.

### Admin
- `Admin.Health` needs no token;
- `Info` (path, sizes, tables, version) and `Metrics` (the `/metrics` text) need read scope;
- `Flush`, `Compact`, `Backup`, `BackupDownload` and `Promote` need admin scope, as does `Replication.Follow` (gRPC only), which replicas use.

### Backups
- **Backups** need `--backup-dir`; without it they're off.
  - `Admin.Backup` writes a consistent backup inside it, at a path relative to it that doesn't exist yet and doesn't lead out of it through a link.
  - `Admin.BackupDownload` streams one: over gRPC as chunks ending with the file's SHA-256, over HTTP as the file with its id and SHA-256 in headers (`x-pigeonhole-backup-id`, `x-pigeonhole-backup-sha256`). Its copy waits in `<backup-dir>/.phdb-serve-downloads` (private to the server) for 5 minutes after the last read, so a broken download resumes from an offset (over HTTP, `Range: bytes=<offset>-` and `?backup_id=`). At most two copies are kept.
  - **Allow for the silence before the first byte:** the server backs up and hashes first. A stream that ends without its final chunk (the one with the size and SHA-256) is incomplete: resume it. Check the SHA-256 before relying on the copy.
  - **One at a time:** backups run one at a time by default (`--max-backups`), off the threads reads use.
  - **Taking one and putting it back:**

    ```sh
    phdb serve app.phdb --backup-dir /var/backups/phdb     # admin token, if the server has tokens

    # A backup kept on the server: /var/backups/phdb/nightly/app.phdb
    curl -s localhost:7448/v1/Admin/Backup -H 'Content-Type: application/json' \
      -d '{"path": "nightly/app.phdb"}'
    # {"bytes":"524288"}

    # Or a backup streamed to you, with its id and SHA-256 in the headers.
    curl -s -D headers.txt -o copy.phdb localhost:7448/v1/Admin/BackupDownload \
      -H 'Content-Type: application/json' -d '{}'
    # x-pigeonhole-backup-id: 1f2a3fd9...   x-pigeonhole-backup-sha256: a03d5a8f...
    sha256sum copy.phdb                                    # must equal the header

    # A broken download goes on from the bytes you have (206), within 5 minutes.
    curl -s -o rest.part 'localhost:7448/v1/Admin/BackupDownload?backup_id=1f2a3fd9...' \
      -H 'Content-Type: application/json' -H 'Range: bytes=1000-' -d '{}'
    ```

    A backup is a whole database in one file. **To restore,** stop the server, put the copy where the database was (or anywhere), and start `phdb serve` on it. A copy taken from a replica needs `phdb promote` first ([Replicas](#replicas)). A backup holds the data, not the change feed's history, and it is a database of its own: a feed token from the original answers `InvalidArgument` ("the token is another database's") on the restored file, so start its subscribers again from a snapshot, with `--change-feed` on.

## Graphs, vectors and text
- **Graph and vector reads** run on the request's task, holding no blocking thread, and take a `snapshot_id` as the other reads do.
  - `Graph.Traverse` walks one edge family from its seeds, hop by hop: a node is a row, an edge a column whose qualifier is the destination's row key. `hops` is at most 64 (0 is 1), and `per_node_limit`, `hop_limit` and `max_nodes` bound it (0 for each is the library's default). Its reply is byte-budgeted like a `MultiGet`'s: `page_bytes` of nodes (a node counts its key and 16 bytes, plus 4 with `parents`), 0 for the server's default of 1 MiB, capped at 8 MiB. The hop the budget cuts keeps its in-budget prefix, the traversal ends there, and `stats.cut_by_budget` says so. The edge selection is `qualifier_prefix` or bounds, a time range and a value filter. With `parents`, each hop gives each node's source as an index into the hop before (the seeds, for hop 1).
  - `Vectors.Nearest` is the exact top `k` of one vector family over a key range (`range`) or a key list (`keys`), nearest first by the family's metric. `k` is 1 to 10,000. The query is an `f32`, `i8` or bit vector: query an `f16` or `bf16` family with `f32` components, as in Rust, since the server refuses an `f16` or `bf16` query. The result is cut to `page_bytes` by D308's measure (each neighbour counts its row, its qualifier and 16 bytes), 0 for the server's default of 1 MiB, capped at 8 MiB, and `cut_by_budget` says it was. A ranking can't resume where it stopped: ask again with a smaller `k` or a narrower source.
- **The IVF vector index** (`pigeonhole-vector`, ICR 0100): `Vectors.CreateIndex` (a spec, and the index options; `if_missing` opens an existing one as it is, without comparing its spec or options to the request's), `Insert`, `Delete`, `Search` (the `k` nearest ids, at most 10,000, with `nprobe`, `id_prefix`, `max_probe`, and `stats` on request) and `Verify`.
  - A vector is in the index's element type: all five work (`f32`, `i8`, bit, `f16`, `bf16`). Another type, or another dimension, is `InvalidArgument`.
  - The server keeps one handle per index, opened on first use. `Insert` and `Delete` are writes, so they answer their real outcome even if the client goes away. `Search` holds no blocking thread once the index is open.
  - An index created with `REBALANCE_BACKGROUND` splits and merges its lists on a server thread of its own (`phdb-serve-rebalance`); the server's shutdown waits for it before it closes the database, and a call that arrives during shutdown gets `Closed`. With `REBALANCE_INLINE` (the library's default), the writes rebalance as they go.
  - A `CreateIndex` that committed just before the server closed can still answer `Closed`: retry it with `if_missing`.
  - **Dropping an index's table** (`Schema.DropTable` on its name) drops the server's handles too (#1588). The next call on the name answers `TableNotFound`, a background index's rebalancer stops, and an index created again under the name is the one served, `if_missing` included.
  - **`Vectors.BulkLoad` fills an empty index** faster than one `Insert` per vector. It's a client stream: the first message names the index and how many vectors follow at most (`expected`, which sizes the lists and bounds the load), and every message carries a batch of items (`id` and `vector`), all in one element type. Over HTTP the body is NDJSON: one `VectorBulkLoadRequest` per line, in the JSON mapping, with or without `Content-Length`.
    - **A failed load leaves the index loaded up to the failure, and rebalanced.** That covers a vector of another element type or dimension (the server checks each against the index before the load takes it), a stream that breaks off, and a server shutting down (`Closed`, "interrupted": the load stops taking vectors at its next message, then writes and rebalances what it took, which for a large load can run past the shutdown grace period). Every vector before the failure is written and searchable. A failure inside the index's own writes (an I/O error, say) is the exception: the batches still waiting are dropped, so only some of the vectors before it may be there (`Vectors.Verify` counts them). So once it has taken a vector, its error says `retry_safe: false` whatever its code (a stall's `DeadlineExceeded`, a shutdown's `Closed`, an `Overloaded`): sending it again would find the index no longer empty.
    - **The index must be empty,** so a failed load can't simply be resumed with `BulkLoad`: `Insert` the rest, or drop the index and load again.
    - Each message (each line over HTTP) holds its share of `--inflight-bytes` until the load takes its last vector, and the server reads one message ahead of the load at most. A client that sends faster than the index loads is held back by the transport's flow control, not buffered. A line longer than `--max-request-bytes` is refused.
    - **The load holds a working set of its own, outside `--inflight-bytes` and bounded by `--bulk-load-bytes`** (1 GiB). `expected` sets the lists: `expected` / target list length, at least 1. The target is the index's (1,000 by default), scaled down so twice it fits a list's 4 MiB row: 682 at 768-dim `f32`, 341 at 1,536-dim. The load clusters a sample of its first max(50,000, lists) vectors before it writes any, then keeps up to 255 vectors per list until each list's batch of 256 is written. So it holds at most the larger of:
      - **the sample:** max(50,000, lists) × (a vector's bytes + its clustering copy, `f32` but for a bit index + twice its id + about 96 bytes);
      - **the pending batches:** lists × 255 × (a vector's bytes + twice its id + about 96 bytes).

      A vector's bytes are dimensions × 4 for `f32`; × 1 for `i8`; and dimensions / 8 for bits. A half index (`f16`, `bf16`) is counted at `f32`'s size, whichever element type the client sends, since it takes `f32` vectors too. Ids are counted at 32 bytes up front.
    - **A load over `--bulk-load-bytes` is refused before it starts,** with `InvalidArgument` naming the bytes it needs. Raise `--bulk-load-bytes` to that, or load in parts: `Insert` the rest after a smaller load. At the default 1 GiB:
      - **768-dim `f32`** (lists of 682): the sample is about 315 MB, and each list's pending batch about 0.8 MB, so up to about 1,300 lists: about 888,000 vectors. A million needs about 1.2 GB.
      - **1,536-dim `f32`** (lists of 341): the sample is about 622 MB, and each list's pending batch about 1.6 MB, so up to about 670 lists: about 227,000 vectors. A million needs about 4.7 GB.
    - **`expected` is an upper bound:** a load stops once it has sent what its lists hold before they split, lists × twice the target (about twice `expected`), with `InvalidArgument`, the vectors before it loaded. Declaring too few (0, say) would otherwise grow one list without bound. `Insert` the rest (the index is no longer empty), or drop it and load again with the right `expected`.
    - **Not counted in `--bulk-load-bytes`: the split after a load.** The rebalance after a load reads a list whole, so a load whose vectors all fall in one list (a write client sending one vector repeated, say) makes it read the whole load: up to about twice `expected`'s vectors. At 768-dim `f32` under the default 1 GiB that's about 5.5 GB, and about 2.8 GB at 1,536-dim, outside the flag. Size the host for it, or give `write` tokens only to clients you trust with that, until [#1824](https://github.com/CodingAnarchy/pigeonhole/issues/1824) (1.3.0) splits a list without reading it whole.
    - **A running load reserves its worst case** from the node's `--bulk-load-bytes` until it ends, succeeded or failed. A load whose ids are longer than 32 bytes grows its reservation as they come, and stops there, the vectors before it loaded, if that's over the flag or other loads hold the rest.
    - **One load runs at a time per database;** another waits for it, up to the request timeout or its own deadline. A load whose client sends nothing for `--stream-idle-ms` while the load waits for it ends with `DeadlineExceeded`, the vectors before it loaded, so a stalled client can't hold the slot. `/metrics` shows the running load's phase, the vectors and bytes handed to it, and its reservation.
    - The load runs on a thread of its own (`phdb-serve-bulk-load`), off the shards' cores where the engine pinned them, as the rebalancers do. Its writes still fill memtables quickly, so other writers can meet room and pacing stalls while it runs. The server's shutdown waits for a running load to write what it received.
- **The BM25 text index** (`pigeonhole-text`, ICR 0105): `Text.CreateIndex` (the source column, as a table, family and qualifier, and the index options; `if_missing` opens an existing one as it is), `Put` (writes the document's text to the source column and indexes it, in one transaction), `Delete`, `Search`, `Rebuild` and `Verify`.
  - `Search` takes the query syntax (`syntax`), or `clauses` in order. A clause's `term` (and a fuzzy clause's `term`) is matched as given, after the index's normalisation, so `March` misses an index of lowercased text. Its `text`, and a phrase's `text`, are tokenised as documents are, as the syntax's words and `"..."~N` phrases are. A repeated clause is refused, since it would count once and shift the clause indexes that `highlight`'s matches report.
  - `k` is 1 to 10,000. With `highlight`, each hit carries its text and every match's byte range, by the request's clause index. `id_prefix` keeps only ids under a prefix.
  - `Put` and `Delete` are writes, so they answer their real outcome even if the client goes away. `Search` and `Verify` run on the blocking pool. `Rebuild` rereads the whole source column, so it runs on a thread of its own (`phdb-serve-text-rebuild`, off the shards' cores), one at a time: a second is `Overloaded`. The server's shutdown waits for a running rebuild to finish before it closes the database, even past `--shutdown-grace-ms`: a rebuild can't stop partway.
  - Dropping a text index's table drops the server's handle the same way (#1588): the next call on the name answers `TableNotFound`, and an index created again under it is the one served.

## Agent memory
- **Agent memory** (`pigeonhole-memory`, ICR 0107): with `--memory NAME`, the server opens (or creates) that store at startup (a replica opens it and never creates it: see [Replicas](#replicas)), with `phdb mcp`'s memory flags (`--embed-url`, `--embed-local`, `--memory-model`, `--memory-dim`, and so on), and serves it as `Memory`: `Remember`, `Recall`, `MarkUsed`, `Supersede`, `Link`, `Forget` and `Watch`. A fleet of agents then shares one store. Without `--memory`, these calls answer `Unsupported`. `Schema.DropTable` of the served store's own tables (`mem_NAME`, its text index's and its IVF index's: `Store::table_names`) answers `FailedPrecondition` while the server serves it (#1651).
  - **Namespaces:** a token line with `ns=a,b` (`write ns=team-a <token>`) reaches only those namespaces, on every call: the namespaces a call names, the namespace each memory id carries (`MarkUsed`, `Supersede`, `Link`, `Forget`, a link's target), and a watch's. A call that names one outside them is refused whole with `PermissionDenied`, never trimmed, and a scoped token must name its namespaces to watch. A token with `ns=` reaches nothing but the memory calls: every other call is `PermissionDenied` ([Security](#security)). Without `ns=`, a token reaches every namespace and the whole database, so a server is one trust domain: run a server or a store per tenant if tokens can't be scoped.
  - **Scopes:** `Recall` and `Watch` need read scope; the others write (`Forget` included: forgetting is ordinary agent behaviour, and `ns=` bounds its reach).
  - **Embeddings:** with `--embed-url`, the server embeds the text and refuses an embedding sent with it. Each embedding call waits at most `--embed-timeout-ms` (10 s, shorter than `phdb mcp`'s 30 s), and memory calls run at most `--memory-calls` at once (half the blocking threads by default), so a slow embedding endpoint can't take every blocking thread. An endpoint that's unreachable (refused, or no connection within half of `--embed-timeout-ms`, at most 3 s) or failing (5xx, 408, 429) answers `Unavailable` (retry-safe); one that takes longer than `--embed-timeout-ms`, `DeadlineExceeded`; one that refuses the text (400, 413, 422), `InvalidArgument`; and any other answer, or a vector of the wrong dimension, `Internal`, since that's the server's `--embed-url`, key or model. With `--embed-local` (a `phdb` built with the `embed-local` feature), the model runs in the server, on at most 4 threads placed where the network threads are, off the shards' CPUs. `--embed-timeout-ms` doesn't bound it, since it's CPU work, but `--memory-calls` still bounds how many embed at once. candle also runs large steps on a process-wide pool of its own, which only the environment places: with the shards pinned and `CANDLE_CPU_MASK` unset, while either of `CANDLE_CPU_MASK` and `CANDLE_NUM_THREADS` is unset, the server prints the `export` line to restart with: its placement's CPUs for the mask, and the embedder's thread count for the size. Without either flag, a store pinned to a model takes the caller's embeddings: `f32`, or `f16`/`bf16` widened.
  - **Recall** returns at most `k` (1 to 1,000) hits within `byte_budget`: 1 MiB when it sets none, 8 MiB at most. `with_signals` adds each ranking signal's score, and `with_embeddings` the stored vectors.
  - **Ids** are a memory's byte form (`MemoryId::to_bytes`), base64 in JSON. Deleting a memory that superseded another brings the other back into recall.
  - **`Watch`** streams new memories in the namespaces named, with a progress event for commits that bring none; each event's `token` resumes a later watch (`resume`, or `Last-Event-ID` over SSE). A token is a commit boundary: every event of one commit carries the same token, which points after the whole commit, so resume from it only once you've handled all of that commit's events. It holds a `--max-streams` slot and ends when the client takes nothing for `--stream-idle-ms`.
  - The writes answer their real outcome even if the client goes away.

## The change feed
- **The change feed** (`Feed.Subscribe`) streams committed changes of some tables, in commit order, once each.
  - **Turn the feed on with `--change-feed BYTES`:** the server then keeps up to that many bytes of history for subscribers (and replicas). Without the flag, a database whose feed is already on keeps it, and one whose feed is off answers `InvalidArgument` (400, "the change feed is off: open with it on to subscribe").
  - **The request** names `tables` (at least one), and optionally a row `prefix`, `visible` (deliver at visibility rather than durability), and one start: `from_oldest`, `from_snapshot` (a pinned snapshot's id) or `resume` (an earlier event's `token`). With no start, the stream begins at the next commit.

    ```sh
    phdb serve app.phdb --change-feed 67108864        # keep up to 64 MiB of history

    # Everything the history holds, then new commits as they land (one JSON event per line).
    curl -sN localhost:7448/v1/Feed/Subscribe -H 'Content-Type: application/json' \
      -d '{"tables": ["users"], "from_oldest": true}'
    # {"token":"AcLz...","commit":{"seqno":"1","timestamp":"...","changes":[{"table":"users","family":"profile","kind":"CHANGE_KIND_PUT","row":"dXNlcjox","qualifier":"bmFtZQ==","timestamp":"...","value":{"bytes":"QWRh"}}]}}

    # Later, from any process: go on after the last event you handled.
    curl -sN localhost:7448/v1/Feed/Subscribe -H 'Content-Type: application/json' \
      -d '{"tables": ["users"], "resume": "AcLz..."}'
    ```

    Keep the last handled event's `token` with your own state, and send it as `resume` after a disconnect. `FeedExpired` means the history no longer reaches back to the token: read the tables again (a scan at a pinned snapshot, then `from_snapshot` with its id) and go on from there.
  - **Feed clients should turn on HTTP/2 adaptive flow control** (tonic's `Endpoint::http2_adaptive_window(true)`; grpc-go and grpc-java estimate the bandwidth-delay product unless a fixed window is set). With fixed windows, events queue in flight ahead of each subscriber. At 16 subscribers that was about 18 ms of delivery lag, which adaptive windows took to 0.5 ms p50 (perf287 on #1397). Larger fixed windows made it worse. `--http2-send-buffer` bounds what the server queues per stream.
  - Over gRPC it's a server stream. Over HTTP it's NDJSON, or server-sent events whose `id:` is each event's token, so an SSE client's `Last-Event-ID` resumes it.
  - Every event's `token` resumes right after it, from any server process. A stream that delivers nothing for about 10 s, while its position moved past commits its `prefix` skipped, sends a `progress` event with the new token.
  - Streams on the same tables share one subscription in the server: the database wakes and reads for it once, whatever the number of subscribers.
  - **Over gRPC, each commit is encoded once for every stream with no `prefix`,** and those streams send the same bytes. A stream with a `prefix` gets its own encoding of the rows that match. The HTTP feed (NDJSON or SSE) still renders each event as JSON per subscriber, so many subscribers cost more over HTTP than over gRPC.
  - **Each stream buffers up to 64 events, or 256 KiB of encoded events, whichever is less,** that its client hasn't taken yet. An event larger than 256 KiB is still sent, alone. A server holds at most `--max-streams` × 256 KiB of these buffers (64 MiB at the default 256 streams). This memory is separate from `--feed-ring-bytes` and `--inflight-bytes`. A client that takes nothing fills its buffer; after `--stream-idle-ms` more it gets the buffered events, then `DeadlineExceeded`, and resumes from the last event's token.

## Limits
- `--max-request-bytes` (16 MiB): the largest request.
- **Aggregate reads:** a read's `AggregateQuery` asks at most 1,024 quantiles and at most 1,024 bloom probes (`bloom_i64` and `bloom_bytes` together); more is `InvalidArgument`. A set family's members come back only with `set_members: true`. Only an aggregate family's cells have an aggregate view, and a page counts each view (its answers, a histogram's counts, a set's members) against its `page_bytes`, so a page of many sketches asked many questions ends early. A single cell's view, like a single column, can still take a page over its budget.
- `--engine-memory` (unset: the library's defaults, 64 MiB of memtable per shard, a 256 MiB block cache and a 64 MiB row cache): the database's memtables, block cache and row cache, in bytes. The memtables take theirs first (at most 64 MiB and at least 128 KiB per shard, and a smaller arena means a smaller largest batch), then the caches at 4:1. A size that can't cover the memtables at 128 KiB a shard is refused at start.
- `--inflight-bytes` (256 MiB): request and reply bytes held at once. A request's share is reserved before its body is read: from `Content-Length` over HTTP, and from the message header over gRPC. A gRPC request's token is checked before any of it is read, and its message header must arrive within `--request-timeout-ms` (or the client's shorter `grpc-timeout`), else `DeadlineExceeded`. Scan pages count too (below).
  - **Unary read replies count as well** (#1567):
    - **Which:** `ReadRow`, `MultiGet`, `Graph.Traverse` and `Vectors.Nearest` (by their `page_bytes`, 1 MiB when 0, up to 8 MiB), `Memory.Recall` (by its byte budget) and `Get`.
    - **Large pages reserve first:** a read asking for more than the default page (1 MiB) reserves its whole `page_bytes` before it reads, twice that for HTTP/JSON, which is larger than the protobuf. So `--inflight-bytes` ÷ `page_bytes` caps how many such reads build at once: under a 64 MiB budget, 8 MiB pages run 8 at a time, while the server's memory stays within the budget. Ask for smaller pages for more concurrency.
    - **Default pages charge after the build:** a read within the default page builds its reply first.
      - A reply of 64 KiB or less takes no share: it goes to the connection whole.
      - A larger one takes exactly its size if it's free. If it isn't, the read drops its reply, reserves its page as above, and reads again. `pigeonhole_serve_reply_rereads_total` counts these.
      - `Get` and a recall within the default page wait for their size instead of reading again.
      - So what's built before it's counted is at most `--max-concurrent` × 1 MiB (of protobuf; its JSON is larger).
    - **Trimmed and held:** a reply keeps only its own size (its encoded message, or its rendered JSON), and holds it until it has been written to the connection.
    - **Refused:** a read whose share can't be had within `--request-timeout-ms` gets `Overloaded` (503 with `Retry-After`), having read nothing. So a burst of large reads pushes back rather than growing the server's memory.
  - **A client that takes nothing of its reply** for `--stream-idle-ms` loses it: the server drops the reply's unsent part and gives its share back. The client's stream then ends with `DeadlineExceeded` (over gRPC) or a cut-off body (over HTTP/1.1) once it reads again or its connection closes. Meanwhile the server holds at most about 64 KiB of that reply.
  - **What isn't counted:** the feed streams' buffers, the shared feed ring (`--feed-ring-bytes`), each connection's own memory (`--max-connections`), and a `/mcp` reply, which is bounded by `--mcp-max-result-bytes` a tool call instead. So request and reply bytes together stay within `--inflight-bytes`, and those come on top.
- **HTTP/1.1 bodies of 64 KiB or less are read before anything else,** even before a request is shed or its token checked, within `--request-timeout-ms`. Past that deadline the server answers `DeadlineExceeded` (504, as every deadline in `phdb serve` does, rather than HTTP's 408) and closes the connection. The read counts toward the request's one deadline, so a slow body leaves the rest of the request less time. So every answer, an early refusal included, leaves the connection open for the next request. A larger body is reserved from `Content-Length` and read later. A refusal that comes first (`Overloaded`, `Unauthenticated`, `RequestTooLarge`) then answers with the body still on its way, and the server drops the connection, which can reset it under the answer. A client sending large bodies should send `Expect: 100-continue`: a refusal then arrives before any of the body is sent, and the server closes the connection cleanly, with no reset. HTTP/2 (gRPC) is unaffected: a refused stream leaves the connection alone. Each HTTP/1.1 connection holds at most 64 KiB in that read, and `--max-connections` bounds how many there are.
- `--max-concurrent` (1,024 requests): requests at once in the whole server, gRPC, HTTP/JSON and `/mcp` together; more are refused at once with `Overloaded`, never queued. Health checks (`GET /v1/health`, `Admin.Health`) aren't counted, so they answer under overload; `Admin.Health` takes no token and an empty message, and reserves nothing.
- **`--max-connections`** (half the soft open-file limit, `ulimit -n`, at most 4096; #1604): open TCP connections, HTTP/1.1 and HTTP/2 alike, from accept until they close, a TLS handshake included.
  - **Past the cap,** a new connection is accepted and closed at once, before anything is read. A gRPC client sees `UNAVAILABLE`, which its retry policy handles, and an HTTP client a connection error it can retry. If the client already sent bytes, the close is a reset ("connection reset by peer"), not an error in the server.
  - **The default** leaves the other half of the descriptors for the database (its file, WAL segments, a backup being written, the embedding endpoint's and a replica's connections). For example, 128 under macOS's default limit of 256, or 512 under Linux's usual 1024. Setting `--max-connections` above half the limit prints a warning at startup: raise `ulimit -n` with it.
  - **Memory per connection:** up to 64 KiB for the HTTP/1.1 pre-read (above). An HTTP/2 connection holds its flow-control windows and up to `--http2-send-buffer` per stream.
  - **A connection that sends nothing** within `--request-timeout-ms` of connecting is closed, and an HTTP/1.1 request's headers must arrive within it too, so silent or slow sockets can't hold the slots.
  - **An idle HTTP/1.1 keep-alive connection** is closed once `--request-timeout-ms` passes with no next request, which frees its slot. A client that reuses connections sees the close and opens another.
  - **An idle gRPC channel holds its slot:** an HTTP/2 connection with no calls answers the server's pings, so it counts until the client closes it. With many idle gRPC clients, raise the cap, or give the clients an idle timeout.
  - **`/metrics`:** `pigeonhole_serve_connections` (open now), `pigeonhole_serve_connections_max`, `pigeonhole_serve_connections_refused_total` and `pigeonhole_serve_accept_errors_total`. Out of descriptors (`EMFILE`), accepting waits for a connection to close, a second at most, and logs a line a second at most.
- `--max-scans` (64): scans streaming at once, refused past that the same way. A scan's pages count against `--inflight-bytes` until they're handed to the connection, and at most half the blocking threads read scan pages at once.
- `--stream-idle-ms` (30,000): how long a scan waits for its client to read a page, a feed stream with a full buffer waits for its client to take an event, or a `Vectors.BulkLoad` waits for its client's next message, before ending the stream.
- `--bulk-load-bytes` (1 GiB): what the node's `Vectors.BulkLoad`s hold at most, outside `--inflight-bytes`: a load's sample while it clusters it, then its lists' pending batches. A load whose worst case is more is refused before it starts ([Graphs, vectors and text](#graphs-vectors-and-text) has the sizing).
- `--max-streams` (256): feed streams at once. `--max-feed-hubs` (32): distinct table sets subscribed at once. Past either, a subscription gets `Overloaded`.
- **128 streams per connection:** one HTTP/2 connection carries at most 128 streams at once (scans, feed streams and calls alike). A client that opens more on one connection doesn't get an error: the extra streams wait until earlier ones end, and a feed stream may never end. Open another connection for each 128 concurrent streams. With tonic, that means another `Channel`, because one `Channel` is one connection.
- `--max-catch-up` (16): feed streams reading their own history at once (from a token, a snapshot or the oldest commit, or after falling behind); the rest wait their turn.
- `--feed-ring-bytes` (256 MiB): the recent commits the shared feeds keep for their streams, in all, shared out over the table sets subscribed. A commit is kept as its encoded event, which carries its resume token (about 45 + 16 × shards bytes), plus about 110 bytes; a stream with a `prefix` filters that event. With 100-byte values and 4 shards, a commit costs about 380 bytes. Each table set's share is `--feed-ring-bytes` over the sets subscribed, so with many sets, raise it to keep the seconds of commits a stream can fall behind and still catch up from the ring. This is outside `--inflight-bytes`; `/metrics` reports what's in use.
- `--backup-dir`: where `Admin.Backup` writes, and where downloads are kept for resumes. It must exist; put it on a disk with room for a copy or two of the database, not tmpfs. There's no default: without it, backups are off. `--max-backups` (1) runs that many at once.
- `--max-sessions` (1,024) and `--max-sessions-per-token` (64): transactions, pinned snapshots and committed transactions' records held at once. Past either, `Begin` and `Open` get `Overloaded`.
- `--request-timeout-ms`: the longest wait for those limits. A gRPC client's shorter `grpc-timeout` wins, and running out of it is `DeadlineExceeded`.
- A write the server has submitted always answers its real outcome.

## Threads and placement
- **Threads:** `--shards` sets the shard count (default: half the physical cores), and `--compaction-threads` the threads dedicated to flushes and compactions (default: a quarter of the physical cores this process may run on, at least 1, at most one per shard). On Linux the network threads run on the CPUs the shards don't use; `--io-threads` and `--blocking-threads` override their counts. The change feed's delivery runs on threads of its own (`phdb-serve-feed`, `--feed-threads`, by default a quarter of those CPUs, at least 1, taken from the network threads' default), so a put never waits behind feed streams for a worker. They're placed as the network threads are. The shared subscriptions (hubs) that read the engine's feed run on threads of their own (`phdb-serve-feed-hub`, `--feed-hub-threads`, by default a quarter as many as `--feed-threads`, at least 1), on the same CPUs, so a hub never waits behind its streams' delivery (#1397). A hub with a single stream runs on the feed threads beside it instead (#1689). **The default keeps writes first.** Each hub thread past it costs the writes on the same CPUs: put p50 rose 13–38% at moderate fan-out load, and CPU per delivered event rose with it. When saturated, delivery wasn't better either: at 16 table sets × 16 streams, more hub threads delivered 15–21% fewer events. With 3 feed threads, 16 table sets with 2 streams each, on the AX102 (g76c on #1725):

  | `--feed-hub-threads` | put p50 at 30K puts/s | at 55K puts/s (saturated): puts | events delivered | lag p99 |
  |--:|--:|--:|--:|--:|
  | 1 (the default) | 145 µs | 176 µs p50 | 50% | 15.7 s |
  | 2 | 164 µs | 1.85 ms p50 | 100% | 0.27 s |
  | 3 | 175 µs | 524 ms p50 | 96% | 28 ms |

  Raise it on a feed-heavy server with CPUs to spare, whose subscribers matter more than its writes' latency. The feed dump (below) shows hubs' engine feeds falling behind the database.

  **At saturation, writes come first.** When the feed side can't deliver every commit, the writes still get the rate they ask for, and feed streams fall behind and catch up. `pigeonhole_serve_feed_fell_behind_total` and the streams' lag show it.
  - On the AX102 at 16 table sets × 16 streams and 55K puts/s, about 14M events/s didn't fit in 3 feed threads and 1 hub thread. More hub threads delivered less.
  - Size the feed side for headroom. #1737 tracks scaling it.
  - Before #1734, the masked server did the opposite: it held writes to about 33K of the 55K puts/s and delivered every event, or switched between the two. The network threads now also run on the shards' SMT siblings (Placement, below), so the writes no longer queue behind delivery for a CPU.

  Each stream's framing still runs on its connection's network thread.

  **When to raise `--io-threads`** (#1745). The default is the free cores' CPUs less the feed threads. Since #1734 the network threads also run on the shards' SMT siblings (Placement, below), and the default doesn't count those, so on a node whose network threads are the limit, part of their mask sits idle.
  - **Raise it on a node whose feed side keeps up:** no feeds, or feeds whose delivery has headroom, meaning `pigeonhole_serve_feed_fell_behind_total` isn't rising and no `phdb-serve-feed-hub` thread runs near a full core. Raise it to the CPUs the network threads may run on, less the feed threads: the startup line's free CPUs, plus the SMT siblings named at the end of the line, minus its feed threads.
    - That tail is there only with the mask on (`--affinity on`) and SMT. Without SMT the default already fills the mask, so there's nothing to raise.
    - In perf287's g83, at 512 callers on ycsb-a and ycsb-c with no feeds:
      - on the AX102 single node, 13 network threads instead of 9 gave about 23% more ops/s (ycsb-a 345K → 425K);
      - on an 8-core node, 10 instead of 6 gave 34–37% more;
      - p99 was lower in both.

    The cost:
    - CPU per op rose 12–18% in g83, and up to 37% on one row of an earlier single-round run (8bio), partly limited by its client;
    - a mixed read/put row's p99.9 rose 25–35%, with its p99 level or lower;
    - at 16 table sets × 16 streams and 20K puts/s, put p50 rose about 45 µs and put p99 about 12% (1.63 → 1.84 ms, two rounds each), with subscriber lag unchanged.
  - **Don't raise it on a node whose feed side saturates,** where `pigeonhole_serve_feed_fell_behind_total` rises or a hub thread runs at a full core. At 16 × 16 streams and 55K puts/s on the AX102, 13 network threads held puts to 38–48K/s, with a put p99 of seconds, in 4 of 4 runs (perf287's g83 and g84). 11 threads fell short in 1 of 2 runs, and even their good run had a put p99 of 0.84 s against the default's 0.12–0.14 s.
    - The extra workers queued for CPU behind each other: about one core's worth of run-queue wait, against 0.04 with the default. The feed and hub threads weren't short of CPU.
    - The default keeps the writes first there. [#1737](https://github.com/CodingAnarchy/pigeonhole/issues/1737) tracks scaling the feed side.
- **Placement** (Linux, D313 R1): where the engine pinned its shard threads (the default, `Options::pin_threads`), the server's threads get an affinity mask that keeps them off the shards' CPUs: every partition's shards on a cluster node. The compaction threads' CPUs stay in the mask: on one node, leaving them out too cost the network side about three times its throughput (g72 on #1703). The startup line counts both. A client thread preempting a pinned shard thread raised `Buffered` put p99.9 (#1206). The startup line says which mask the server built, after its thread counts (the shard and compaction CPU counts follow it):
  - **`off the shards' cores`:** the network workers run on every CPU but the shards' own: the free cores and the shards' SMT siblings. The line then ends with "network threads also on the shards' N SMT siblings (CPUs)", the whole list. The feed and feed hub threads, the blocking threads (scans, `ReadRow`, `MultiGet`, text and vector calls, `/mcp`) and the background threads (below) stay on the free cores only, since a busy thread on a shard's sibling doubled the shard's p99 (#1244).
    - The siblings are there for the writes: with only the free cores, 256 feed streams' delivery filled them and the network threads waited for a CPU, so puts were held to 33K/s while the siblings sat idle (#1734).
    - The feed threads stay off the siblings. On them as well, subscriber lag p99 at 16 × 16 streams and 20K puts/s rose from 5.6 ms to 34 ms. On the free cores it was 8.3 ms, at the same put rate when saturated and with a lower put p99 (perf287's g82 on #1738).
    - Without SMT there are no siblings, and every thread gets the free cores;
  - **`unmasked (too few free cores)`:** fewer than two CPUs are left once the shards' cores and their SMT siblings are taken out: a small host, or a `--shards` near the core count. Each shard's driver thread takes a core, so at one shard per core none is left. The threads then get no mask, and the server says why on a line of its own. On the shards' SMT siblings they cost the shards more than they do left to the OS (g70d). Lower `--shards` to get the mask back;
  - **`unpinned`:** no shard is pinned, and the threads run where the OS puts them. That's macOS (no hard affinity), a host with too few free CPUs, a CPU quota narrower than the affinity set, or an OS that refused the pin. `/metrics` counts the engine's threads pinned and not (`pigeonhole_threads{pigeonhole_thread_pinned}`).
  - **`unmasked (--affinity off)`:** the shards are pinned, but the server's threads get no mask.
- **`--affinity off`** (default `on`) keeps the same thread counts but sets no mask, so the network, feed, feed hub and blocking threads run where the OS puts them. The shards stay pinned: that's `Options::pin_threads`, not this flag.
  - **Leave it on for writes:** in the measurements below, the mask raised gRPC `Mutate` throughput 25% at 21% less CPU per write, and cut `Mutate`'s p99.9 in mixed workloads by 1.5–4.5×.
  - **Deployments dominated by whole-row reads or scans should use `--affinity off`.** With the mask, `ReadRow` ran 16% slower over gRPC and 26% slower over HTTP, and HTTP `Scan` 15% slower: with few writes, the shards' cores sit idle, and the mask keeps those reads off them. Point `Get`s, and the read-only YCSB C, were level or better with the mask.
  - **Turn it off on a host shared with other work,** where the server shouldn't claim every CPU outside the shards.
- **The server's background threads** run on the free cores only, off the shards' cores and their siblings: an index's rebalancer (`phdb-serve-rebalance`), a bulk load (`phdb-serve-bulk-load`) and a text rebuild (`phdb-serve-text-rebuild`). So do the embedder's threads with `--embed-local` ([Agent memory](#agent-memory)).

- **The shards' poll:** a shard thread sleeps as soon as its queue is empty (`--shard-spin 0`, the default). The library's embedded default polls up to a millisecond first, which saves a wakeup per commit, but behind a network round trip the wakeup is small, and on a busy server (about 55,000 puts a second over 4 shards on the reference machine, perf287's g51) the poll was a quarter to half of the CPU per put. At low rates it costs little. `--shard-spin US` sets a fixed poll in microseconds (at most 10,000), and `--shard-spin adaptive` restores the library's default.

**Measured with and without the mask** (D313 ticket 9b, perf287's g70e2; [the full table](https://github.com/CodingAnarchy/pigeonhole/issues/1397#issuecomment-6780991941)):
- **The setup:**
  - **Machine:** the reference machine (AX102), with 8 shards (the default). The server ran on 12 cores under `taskset`, 8 holding the shards and 4 free: "6 network, 2 feed and 8 blocking threads on 8 free CPUs, off the shards' cores". The client ran on 4 cores of its own.
  - **Load:** `crates/cli/examples/serve_load.rs` at its defaults: 100,000 rows of 100 B, 64 callers, 8 gRPC connections, 20 s per row after a 5 s warm-up, and writes at `GroupSync`.
  - **Arms:** mask on against `--affinity off`, two runs each, rotated. The values are the two runs' means.
- **Reading it:** "on → off" for each. p99.9 is the row's operation, or its `Mutate` in the YCSB mixes.

| Transport | Mix | ops/s | CPU µs per op | p99.9 ms |
|---|---|--:|--:|--:|
| gRPC | `Get` | 315K → 297K | 18.6 → 19.5 | 0.49 → 0.52 |
| gRPC | `ReadRow` | 177K → 210K | 39.5 → 36.7 | 0.83 → 0.72 |
| gRPC | `Mutate` | 265K → 211K | 38.8 → 49.2 | 0.82 → 0.99 |
| gRPC | `Batch` (10 rows) | 166K → 165K | 67.5 → 73.4 | 4.6 → 5.1 |
| gRPC | `Scan` (10 rows) | 10K → 9K | 443 → 470 | 7.9 → 8.9 |
| gRPC | YCSB A (50% writes) | 226K → 205K | 37.8 → 42.5 | 0.96 → 1.47 |
| gRPC | YCSB B (5% writes) | 232K → 220K | 26.9 → 28.2 | 0.83 → 1.59 |
| gRPC | YCSB C (reads) | 239K → 232K | 24.9 → 25.3 | 0.65 → 0.66 |
| HTTP | `Get` | 202K → 212K | 29.6 → 28.1 | 0.79 → 0.73 |
| HTTP | `ReadRow` | 105K → 142K | 69.6 → 75.9 | 1.00 → 0.87 |
| HTTP | `Mutate` | 268K → 241K | 41.8 → 45.7 | 1.32 → 1.24 |
| HTTP | `Batch` (10 rows) | 131K → 138K | 72.8 → 81.6 | 7.1 → 6.5 |
| HTTP | `Scan` (10 rows) | 5.5K → 6.5K | 705 → 703 | 14.5 → 13.3 |
| HTTP | YCSB A | 232K → 223K | 38.1 → 39.6 | 0.89 → 2.29 |
| HTTP | YCSB B | 235K → 232K | 27.1 → 27.7 | 0.92 → 4.2 |
| HTTP | YCSB C | 242K → 251K | 24.8 → 23.7 | 0.66 → 0.63 |

- **Writes gain from the mask, and reads that could use the idle shard cores lose.** `Batch` is the one write row whose p99 rose with the mask (gRPC 1.11 against 0.82 ms). The engine's compaction threads, pinned to three of the four free cores and at about 2.5 cores busy in those rows, likely contributed; p99.9 couldn't separate them (#1703).
- **These are a fresh server's numbers.** A server restarted before #1699 reopened with one shard per core, and its mask fell back to the shards' SMT siblings, where the mask mostly hurt (section 2 of the write-up). Since #1699 a restart keeps the default, and since #1697 a host with too few free cores gets no mask rather than the siblings.

## Metrics
- **`GET /v1/health`** answers without a token. `GET /metrics` serves Prometheus text with read scope: request counts by RPC and outcome, each RPC's durations as a histogram (service time: from the decoded request to its answer, on every path, so the wait for the request's body, an HTTP/1.1 pre-read included, isn't in it; a scan's runs to the end of its stream), open sessions by kind, the oldest open session's age, the feed's shared subscriptions, their memory, and the streams that fell behind or are catching up, and a running bulk load's phase, vectors and bytes. After those come the engine's metrics for the served database, under the names and labels the [observability guide](observability.md#the-metrics) lists (`pigeonhole_commits_total`, `pigeonhole_write_stall_time_seconds_total` and the rest, each with `pigeonhole_db_name`), with its alerts and dashboard. On Unix, `kill -USR1 <pid>` makes the server write its feed hubs to stderr (it doesn't stop the server): the hub thread count, then for each hub, how far its engine feed is behind the database, its ring against its share of `--feed-ring-bytes`, and its streams on the ring (entries behind its head), on their own catch-up streams, and waiting for a `--max-catch-up` slot.

Serve's own metrics, before the engine's:

| Metric | Type | What it is |
|---|---|---|
| `pigeonhole_serve_requests_total{rpc, outcome}` | counter | Requests by RPC (`Data.Get`) and outcome: `ok`, or the error's code (`Overloaded`, `TableNotFound`, ...). |
| `pigeonhole_serve_request_duration_seconds{rpc}` | histogram | Each RPC's service time, from the decoded request to its answer (a stream's to its end). Buckets from 100 µs to 10 s. |
| `pigeonhole_serve_shed_total` | counter | Requests refused at `--max-concurrent`, before they ran. |
| `pigeonhole_serve_replies_lost_total` | counter | Writes that finished after their client went away. The write's outcome went to the log instead. |
| `pigeonhole_serve_feed_idle_drops_total` | counter | Feed subscriptions ended at `--stream-idle-ms`, because the client left an event untaken that long (on a cluster: took nothing that long). Each one also ends its `Feed.Subscribe` with `DeadlineExceeded`. A cluster subscription counts once, on the node it entered. |
| `pigeonhole_serve_connections`, `pigeonhole_serve_connections_max` | gauge | Open connections, and `--max-connections`. |
| `pigeonhole_serve_connections_refused_total` | counter | Connections closed at the cap. |
| `pigeonhole_serve_accept_errors_total` | counter | Failed accepts, such as running out of file descriptors (`EMFILE`). |
| `pigeonhole_serve_inflight_reply_bytes` | gauge | Read replies' bytes charged to `--inflight-bytes` now. |
| `pigeonhole_serve_reply_idle_drops_total` | counter | Replies dropped because their client took nothing for `--stream-idle-ms`. |
| `pigeonhole_serve_reply_rereads_total` | counter | Reads run again under a reservation, because their built reply's share wasn't free. |
| `pigeonhole_serve_sessions{kind}` | gauge | Open sessions: `transaction`, `snapshot`, `tombstone` (a committed transaction's record) and `mcp`. |
| `pigeonhole_serve_oldest_session_seconds` | gauge | The age of the oldest open transaction or snapshot. Either holds back version and blob GC. |
| `pigeonhole_serve_feed_hubs` | gauge | Shared feed subscriptions (one per table set). |
| `pigeonhole_serve_feed_ring_bytes` | gauge | Their rings' bytes, against `--feed-ring-bytes`. |
| `pigeonhole_serve_feed_catching_up` | gauge | Feed streams reading their own history now. |
| `pigeonhole_serve_feed_catch_up_waits_total` | counter | Feed streams that waited for a `--max-catch-up` slot. |
| `pigeonhole_serve_feed_fell_behind_total`, `pigeonhole_serve_feed_rejoined_total` | counter | Feed streams that fell off a ring onto their own history, and that caught up and rejoined it. |
| `pigeonhole_serve_feed_hub_threads` | gauge | The threads that read the engine's feed into the shared subscriptions (`--feed-hub-threads`). |
| `pigeonhole_serve_feed_solo_hubs` | gauge | Shared subscriptions with a single stream, running on the feed threads beside it. |
| `pigeonhole_serve_feed_hub_moves_total` | counter | Shared subscriptions that moved between the feed threads and the hub threads as their second stream joined or their last but one left. |
| `pigeonhole_serve_feed_hub_failures_total` | counter | Shared subscriptions that failed. Their streams end with the error. |
| `pigeonhole_serve_bulk_load_phase` | gauge | A `Vectors.BulkLoad`'s phase: 0 none, 1 receiving, 2 writing and rebalancing. |
| `pigeonhole_serve_bulk_load_vectors`, `pigeonhole_serve_bulk_load_bytes` | gauge | The vectors and message bytes handed to the running load. |
| `pigeonhole_serve_bulk_load_reserved_bytes` | gauge | What the running load reserved of `--bulk-load-bytes`: its worst case. |
| `pigeonhole_serve_partition_unpinned_shard_threads{pigeonhole_partition}`, `pigeonhole_serve_partition_unpinned_compaction_threads{pigeonhole_partition}` | gauge | On a cluster node, each partition's engine threads left unpinned: shard drivers and compaction threads. Above 0 on Linux, the partition's threads can share CPUs with another's or with the network side's. |
| `pigeonhole_serve_partition_memtable_bytes{pigeonhole_partition}`, `pigeonhole_serve_partition_block_cache_bytes{pigeonhole_partition}`, `pigeonhole_serve_partition_row_cache_bytes{pigeonhole_partition}` | gauge | On a cluster node, each partition's engine memory as resolved from `--engine-memory` and the map's `memory_bytes`. |
| `pigeonhole_replica_following`, `pigeonhole_replica_applied_primary_seqno`, `pigeonhole_replica_lag_commits`, `pigeonhole_replica_lag_seconds` | gauge | On a replica only ([Replicas](#replicas)). |

The engine's metrics follow, under the [observability guide's](observability.md#the-metrics) names and labels, with its alerts and its Grafana dashboard.

**What to alert on,** beside the engine's ([Observability](observability.md#what-to-alert-on)). "For" is the alert rule's `for:`.

| Alert | Query | For | Why |
|---|---|---|---|
| Requests are shed | `rate(pigeonhole_serve_shed_total[5m]) / (rate(pigeonhole_serve_shed_total[5m]) + sum(rate(pigeonhole_serve_requests_total[5m]))) > 0.01` | 5m | More than 1% of requests arrive past `--max-concurrent` and are refused before they run. Shed requests aren't in `pigeonhole_serve_requests_total`, so the ratio adds them back. Raise the limit if the machine has room, or add capacity. |
| Overloaded | `sum(rate(pigeonhole_serve_requests_total{outcome="Overloaded"}[5m])) / sum(rate(pigeonhole_serve_requests_total[5m])) > 0.01` | 5m | More than 1% of the requests that ran were refused by a limit: in-flight bytes, sessions, streams or feed hubs. The [limits](#limits) say which. |
| Connections near the cap | `pigeonhole_serve_connections > 0.9 * pigeonhole_serve_connections_max` | 5m | New connections will be closed at once. Raise `--max-connections` and `ulimit -n` together. |
| Out of descriptors | `rate(pigeonhole_serve_accept_errors_total[5m]) > 0` | | Accepting is failing, usually `EMFILE`. |
| A feed subscription failed | `increase(pigeonhole_serve_feed_hub_failures_total[15m]) > 0` | | Its subscribers' streams ended. They resubscribe from their last token. |
| Feed streams stay behind | `pigeonhole_serve_feed_catching_up > 0` | 15m | Streams reading their own history for a quarter of an hour aren't catching up: subscribers slower than the commits. Raise `--feed-ring-bytes`, or look at the slow clients. Streams falling off a ring and rejoining it (`pigeonhole_serve_feed_fell_behind_total` and `_rejoined_total`) is normal under bursts, so don't alert on those. A rising `pigeonhole_serve_feed_catch_up_waits_total` means `--max-catch-up` is the bottleneck. |
| A snapshot held long | `pigeonhole_serve_oldest_session_seconds > 300` | | A client holds a pinned snapshot past 5 minutes, holding back GC. Transactions end at `--txn-lifetime-ms` (5 minutes), so only snapshots (up to `--snapshot-lifetime-ms`, 10 minutes) reach it. |
| A replica isn't following | `pigeonhole_replica_following == 0` | 5m | It's retrying, or its primary is unreachable. `Admin.Info` says why. The `for:` lets a bootstrap or re-bootstrap finish. |
| A replica is behind | `pigeonhole_replica_lag_seconds > 30` | 5m | Pick the threshold from how stale its reads may be. |
| Latency | `histogram_quantile(0.99, sum by (le, rpc) (rate(pigeonhole_serve_request_duration_seconds_bucket{rpc!~"Data.Scan\|Feed.Subscribe\|Memory.Watch\|Admin.Backup\|Admin.BackupDownload\|Vectors.BulkLoad\|Text.Rebuild"}[5m]))) > 0.05` | 10m | The server's p99 by RPC. Streams are timed to their end, and the backup, bulk-load and rebuild calls run long by design, so they're left out. Pick the threshold from your own baseline. `histogram_quantile` interpolates within a bucket (bounds 100 µs, 250 µs, 500 µs, 1 ms, 2.5 ms, ... 10 s), so a p99 between two bounds is an estimate. |

## Replicas
A replica server serves reads of a copy that follows a primary server's commits (#1436). Use one to move reads off the primary, to keep a warm copy on another machine, or to take backups there.

```sh
# The primary: the change feed on (retention is how far a replica may fall behind), and
# a backup directory (a replica's first copy is a backup download).
phdb serve primary.phdb --listen 0.0.0.0:7448 --tls-cert cert.pem --tls-key key.pem \
  --token-file tokens --backup-dir /var/lib/phdb/backups --change-feed 4294967296

# The replica, on another machine: an admin token of the primary's in a file of its own.
phdb serve replica.phdb --create --listen 0.0.0.0:7448 --tls-cert cert.pem --tls-key key.pem \
  --token-file tokens --replica-of https://primary.example:7448 --replica-token-file primary-token
```

- **Setup.** `--replica-of` takes the primary's URL (`http://` or `https://`, checked against the system's roots: a primary whose certificate comes from a private CA needs that CA installed in the replica host's trust store, since a replica has no CA flag). The replica sends the token from `--replica-token-file` (or `PHDB_REPLICA_TOKEN`), which needs admin scope on the primary. The primary needs `--backup-dir`, and its change feed on (`--change-feed`, or a database whose feed is already on).
- **Bootstrap.** A new replica downloads a copy of the primary at one durable snapshot (`Admin.BackupDownload` with `replica`), checks its SHA-256, and swaps it in for its own file. It then follows the primary from exactly that snapshot (`Replication.Follow`). The download resumes if the connection breaks. A restarted replica doesn't download again: it goes on from the last commit it applied.
- **What it serves.** Every read, snapshot and feed of its own works as on the primary, and so do IVF index searches and `Vectors.Verify` on the indexes the primary made. A background index doesn't rebalance on a replica (the primary's rebalancing arrives as its commits); it starts after a promote. Writes, schema changes and index writes (`Vectors.CreateIndex`, `Insert`, `Delete`) fail with `ReadOnly` (HTTP 409, gRPC `FAILED_PRECONDITION`), and the message names the primary to send them to. `Flush` and `Compact` are local maintenance and work.
- **Agent memory on a replica** (`--memory NAME` with `--replica-of`, #1618): the replica serves the store's `Recall` and `Watch` from what it has applied, and `x-pigeonhole-min-primary-seqno` works for them as for any read. `Remember`, `MarkUsed`, `Supersede`, `Link` and `Forget` fail with `ReadOnly`, naming the primary.
  - **It never creates the store:**
    - A replica whose copy has the store opens it at startup, so a pin mismatch with `--embed-url` or `--memory-model` stops `phdb serve` at once.
    - Otherwise it opens the store when it arrives from the primary. Until then, `Recall` and `Watch` answer `ReplicaBehind` while the replica is still starting or bootstrapping, and `Unavailable` once it follows the primary, since the primary may not have created the store yet.
    - A store that arrives pinned to another model answers `FailedPrecondition`.
  - **No background writers:** the store's forgetter and IVF rebalancer don't run on a replica; the primary's work arrives as its commits.
  - **A re-bootstrap:** the server opens the store again on the new copy. An open `Watch` ends with an error (`FeedExpired` or `InvalidArgument`: its token is the old copy's), as feed streams do, so recall and watch again without the token.
  - **A promote:** the store is opened as the writer at once, created if missing, with the `--memory-*` creation flags (they're used only then). If that fails, the promote still succeeds, and `Admin.Info`'s `last_error` says why. `Admin.Promote` answers once that open is done, so with `--embed-url` it can wait up to `--embed-timeout-ms` for the endpoint's dimension probe.
  - **`--embed-url` on a replica** embeds recall's query text.
- **The consistency model:**
  - **A consistent prefix.** A replica holds the primary's commits up to some point, in the primary's order, and never part of one: a read, a pinned snapshot or a feed on the replica sees the primary as it was after some commit.
  - **Durable commits only.** A replica receives a commit once it is durable on the primary, so it never holds a commit the primary could lose to a crash. With `Buffered` writes on the primary, commits arrive after the primary's next sync.
  - **Monotonic reads** on one replica: they go forward in time. Across replicas, or between the primary and a replica, they don't, and a client that wrote to the primary may not see its write on a replica yet.
  - **Read your own writes** with the `x-pigeonhole-min-primary-seqno` header (gRPC metadata, or an HTTP header): the `seqno` of your commit on the primary (`CommitInfo.seqno`). The replica answers once it has applied the primary's commits through it. If it hasn't within the request's deadline, it fails with `ReplicaBehind` (503 / `UNAVAILABLE`, retry-safe): retry, or read from the primary. The deadline is `--request-timeout-ms` from the request's arrival, waiting for the in-flight budget included. A gRPC client whose own deadline is shorter gets `DEADLINE_EXCEEDED` instead, usually from its own library first; both are retry-safe. While the replica bootstraps the read fails at once. A replica that can't reach its primary answers `ReplicaBehind` at the deadline, and a promotion answers every waiting read at once. It applies to reads, including `Transactions.Begin` and `Snapshots.Open`, whose snapshot then holds the write; `Admin.Info` and `Metrics` answer at once. A primary ignores the header, so a client can send it everywhere. A `Buffered` commit reaches replicas after the primary's next sync, so a read waiting for one waits for that sync. The wait holds the request's place under `--max-concurrent`.
  - **The primary's timestamps and ids,** so cells read the same on both. Local seqnos (`CommitInfo.seqno`, feed tokens) are the replica's own.
  - **Schema changes arrive in commit order:** a table created after a commit appears on the replica after it.
- **Lag.** `Admin.Info`'s `replica` section reports the replica's `state`: `starting` (before its first step), `bootstrapping`, `following`, `re-bootstrapping`, `retrying` (with `last_error`), or `stopped` (a promote failed after it stopped following: `last_error` says why, and another `Promote` may succeed). Once promoted, the server is a primary and `Admin.Info` has no `replica` section, the primary's seqno it applied through, the primary's durable frontier, `lag_commits` (the difference: durable commits not applied yet; commits the primary hasn't synced aren't counted, since a replica can't have them) and `lag_ms` (how long since it was last caught up, 0 when it is). `bootstrap_phase` says which step a bootstrap is at (`connecting`, `waiting for the primary's copy`, `downloading`, `opening the copy` or `swapping`), and `bootstrap_bytes` how much of the copy has arrived; a bootstrap that failed keeps its last step beside `last_error` until the next one starts. A download that stalls (no chunk for `--stream-idle-ms`) breaks, shows why in `last_error`, and resumes from its offset on a new connection. The primary answers at once, then sends a heartbeat every second while it writes its copy, so no wait depends on how long the copy takes. A replica waits up to 3 seconds (or `--stream-idle-ms`, if longer) for the answer and between heartbeats, and up to `--stream-idle-ms` for each chunk of the copy's data. A break during the copy resumes with the same copy, never a new one. Only gRPC sends heartbeats: an HTTP `Admin/BackupDownload` sends nothing until the copy is written and hashed, because its size and SHA-256 go out as headers, and it holds its `--max-streams` slot while it waits. `/metrics` has the same as `pigeonhole_replica_following`, `pigeonhole_replica_applied_primary_seqno`, `pigeonhole_replica_lag_commits` and `pigeonhole_replica_lag_seconds`.
- **Throughput.** A replica applies the primary's commits in runs, one commit of its own per run: at most `--replica-run` (256) primary commits, or 8 MiB. A caught-up replica applies each arrival at once. One that is behind fills runs, so it catches up faster than the primary wrote.
- **Retention, and falling behind.** The primary doesn't track its replicas: its feed retention (`--change-feed`) decides how long a replica may be down or behind. Size it to the write rate times the longest replica outage you want to ride out; the primary's `/metrics` reports the feed's retained bytes. A replica that falls off the end, or whose primary's URL now serves another database, bootstraps again: it downloads a new copy while serving its old one (stale, with `lag_ms` growing), then swaps it in. Requests during the swap get `Unavailable` (retry them), and transactions, pinned snapshots and feed streams open on the old copy end. A feed token from the old copy is another database's to the new one: a stream on it, or a resume from it, ends with `InvalidArgument` ("the token is another database's"), so subscribe again from now or from a snapshot.
- **Promotion.** `Admin.Promote` (admin scope) on a running replica, or `phdb promote <file>` on a stopped one's file, makes it a primary at the last commit it applied: it stops following and takes writes, and its own feed tokens stay valid. It returns the primary's token it stopped at. **Nothing fences the old primary:** stop it (or its writers) before promoting, or the two diverge. A promote before the replica's first bootstrap has finished is refused with `FailedPrecondition`, and the replica goes on bootstrapping: promote it once `Admin.Info` shows it following. A promoted database can't follow again, and `phdb serve --replica-of` refuses to start on it (or on any database with tables that isn't a replica's) rather than replace it; start a new replica from it instead.
- **Backups of a replica** (`Admin.Backup`, `phdb backup`) are ordinary copies, a good way to take backups off the primary. A copy restored and opened as a primary needs `phdb promote` first. A replica can't be another replica's source: replicas follow the primary.

## Clusters
**In progress (D317, ICR 0117).** A cluster splits one database across several `phdb serve` nodes by key range. Each partition is a whole database file, served by one node.
- **What works now:**
  - any node takes any call and forwards what it doesn't hold;
  - single-row reads and writes, and batches within one partition;
  - transactions and snapshots in one partition;
  - scans, multi-gets, nearest searches and traversals across partitions;
  - change feeds across partitions;
  - schema through the cluster: tables and families live in the map, made and dropped through any node;
  - fencing, and relocation onto a node that holds the partition's files;
  - whole moves (`phdb cluster move`, `relocate --copy`), which keep the partition's change-feed history;
  - backups and restore;
  - several partitions per node.
- **Still to come:** the IVF and full-text indexes (building them as well as searching them), agent memory and `/mcp` aren't served by a cluster node in this release (#1722, 1.3.0); [How calls are routed](#how-calls-are-routed) lists every call a node refuses. Each section below says which of its parts are still to come.

### How a cluster is laid out
- **Partitions are whole database files.** A partition holds key ranges of one or more tables. Its file records which cluster and partition it is, and its *epoch* (`__phdb_cluster`, hidden from `ListTables`).
- **The map** says, in two layers:
  - which partition holds each key range of each table, given as sorted *split keys*. Range *i* is `[split i-1, split i)`, the first starting at the empty key and the last open-ended. **This layer is fixed once a range holds data.** Moving a range between partitions comes in 1.3.0 (#1499), so choose split keys up front ([Choosing split keys](#choosing-split-keys-and-hot-ranges)).
  - which node serves each partition. This changes when a partition is moved to another node, with a higher epoch.
- **The map has a version,** raised by every change. A node refuses a map no newer than its own, and every node keeps a copy (`DIR/cluster-map.json`).
- **Ids:** pick `cluster_id` once (a UUID is a good choice). Ids are `[a-z0-9_-]`, up to 64 characters.

### Running a cluster
1. **Stamp each partition's file** before its node starts: `phdb cluster init-partition DIR/P.phdb --cluster-id C --partition P`. `DIR` is the node's cluster directory, and the file must be named `<partition>.phdb`. The file is stamped at epoch 1, the epoch a first map gives a partition; `--epoch N` stamps another.
2. **Tables** are made through the cluster once it runs (see [Schema through the cluster](#schema-through-the-cluster)): `Schema.CreateTable` with a `placement`. You can also place a table in the first map below and create its families through the cluster afterwards. Tables made by hand in a partition's file (`phdb shell DIR/P.phdb -c "create users profile"`), as before schema came to the cluster, still work: the map takes one over when it places a table of that name.
3. **Write the map** as JSON (`ClusterMap`):

   ```json
   {
     "cluster_id": "c1",
     "map_version": 1,
     "nodes": [
       {"id": "a", "address": "10.0.0.1:7448"},
       {"id": "b", "address": "10.0.0.2:7448"}
     ],
     "partitions": [
       {"id": "p1", "node": "a", "epoch": 1},
       {"id": "p2", "node": "b", "epoch": 1}
     ],
     "tables": [
       {"name": "users", "splits": [{"text": "m"}], "partitions": ["p1", "p2"]}
     ]
   }
   ```

   `users` rows below `m` are in `p1`, and the rest in `p2`. A split key that isn't UTF-8 is written `{"raw": "<base64>"}`. A table with no `splits` is one range, in one partition.

   Put the first map in every node's `DIR` as `cluster-map.json` before the nodes start: a node opens the partition its map gives it only at start. `phdb cluster show-map --file MAP.json` checks that it parses.
4. **Start each node:** `phdb serve --cluster DIR --node-id ID`. One server runs per `DIR`.
   - **Across hosts, TLS is required:** a node without TLS refuses a map naming an address that isn't loopback, `--insecure-no-tls-no-auth` or not. Peers and `phdb cluster` connect to each node at its `address` in the map, so its certificate must be valid for that host: a DNS name in the map is simplest, and a bare IP needs an IP subject alternative name. A node on another host then starts as `phdb serve --cluster DIR --node-id a --listen 0.0.0.0:7448 --tls-cert a.pem --tls-key a.key --token-file tokens --cluster-token-file cluster.token --cluster-ca ca.pem` (the last only for a private CA).
   - `--cluster-token-file` is the token a node presents to the others. List it as `cluster <token>` in every node's `--token-file`. The `cluster` scope is only for node-to-node calls.
   - To rotate it, list the old and new tokens on every node, then switch each node's `--cluster-token-file`. A node reads its tokens at start, so each of those steps is a restart of each node.
   - `--cluster-ca PEM` names the CA certificates a node trusts for its peers' TLS, beside the system's roots. A cluster whose certificates come from a private CA needs it on every node, or node-to-node calls fail their handshake.
   - `--cluster-push-timeout-ms` (2,000) is how long a map change waits for each other node to take the new map. A node that doesn't in time pulls it later.
   - **Every node lists the same tokens** (`--token-file`). A call forwarded to another node runs as your token there, with the scope and namespaces that node lists for it, and sessions and feeds are bound to it.
5. **Change the map** on a running cluster with `phdb cluster set-map MAP.json --node URL`, a compare-and-set against the current version. Raise `map_version` in the file.
   - **The map's writer** is the node with the smallest id in the map, and it alone changes the map, so two changes can't make two different maps of one version (D317 amendment 1). `set-map` reads the map from `--node` and sends yours to the writer. A `SetMap` sent straight to another node answers `WrongPartition`, naming the writer.
   - The writer validates the map, keeps it, and pushes it to the others.
   - A node that misses the push pulls the map when it next hears from a node that has it, or at its own start.
   - A map may add tables, partitions and nodes, and give a partition another node with a higher epoch. A table's split keys and partitions are fixed once set.
   - A partition the map gives a running node opens there when the node takes the map, and serves at once ([Fencing and relocation](#fencing-and-relocation)). Until it has opened, the node answers calls for it with `PartitionUnavailable` ("isn't open here").
   - **When the writer is down,** no map change and no schema call can be made. `phdb cluster set-map MAP.json --node URL --force` sets a map at another node:
     - Its version goes above every node `phdb cluster` can reach.
     - The map must name a writer that's up: remove the down node from it, or let another id be the smallest. Give the down node's partitions to other nodes with a higher epoch.
     - When the old writer comes back, it takes the forced map from the others before it changes anything, and follows it.
     - A partition whose epoch the forced map raises starts a new history, as a promotion does: its change-feed tokens from before answer `FeedExpired`.
     - No partition's epoch may go back, or stay the same with another node, against the receiving node's map or any node's that `phdb cluster` reaches. Give each moved partition an epoch above every node's. The report lists the nodes it couldn't reach as `unchecked`; read their map files (`phdb cluster show-map --file`) if they may be newer.
     - **Only for a writer that's down.** A writer that's up but cut off from you can still change the map, and a forced map then loses that change. Check that the writer's process is stopped first.
6. **Check it:** `phdb cluster status --node URL` shows every node's map version side by side, so a push that hasn't reached a node shows, and how many of each node's partitions are open with the identity the map expects. A row per partition gives its state on its node: `opening` (a map just gave it here, and its calls answer `PartitionUnavailable` until it's open), `serving`, `fenced (operator)`, `fenced (awaiting peer map)` or `closed`. `phdb cluster show-map (--node URL | --file DIR/cluster-map.json)` prints a map.
7. **Give `phdb cluster` its token.** Every `phdb cluster` command that talks to a node takes these beside `--node`:
   - `--token-file FILE`: a file holding the bearer token alone. Without the flag, the `PHDB_TOKEN` environment variable. A token is never a flag's value. The same token goes to every node a command talks to (`move`, `backup`, `restore` and `status` reach each node at the map's address), so list it on every node.
   - `--ca PEM`: CA certificates to trust beside the system's roots, for nodes whose certificates come from a private CA.
   - **Scopes:** `status` and `show-map` need `read`. Every command that changes something needs `admin`: `set-map`, `fence`, `unfence`, `close`, `drop-partition`, `relocate`, `move`, `backup` and `restore`.
   - A cluster on addresses other hosts reach requires TLS and tokens ([Security](#security)), so there every command needs a token: `phdb cluster status --node https://10.0.0.1:7448 --token-file admin.token`. The examples in this section leave the two flags out.
   - `init-partition` and `show-map --file` work on local files and take neither.

**Flags:** a cluster node takes `--shard-spin`, `--no-wal-preallocate`, `--shm-dir`, `--change-feed`, `--backup-dir` and `--max-backups` as a single node does, for each of its partitions. Each partition writes its logs in full when it opens, including a partition the map attaches while the node serves; `--change-feed` is per partition ([Partitions per node](#partitions-per-node) has the disk this takes). `--shards`, `--compaction-threads` and `--engine-memory` are the node's totals, split across its partitions ([Partitions per node](#partitions-per-node)). `--max-uploads` (1) caps the partition files it takes in at once (`Admin.PartitionUpload`), for a restore or a move by file copy. An upload is staged in `DIR/.phdb-incoming` until `phdb cluster relocate` or `phdb cluster restore` installs it. An upload doesn't check for free space first: a full disk fails it, and it resumes once there's room. `--memory`, `--create` and `--replica-of` are refused until the steps that bring them.

**Metrics:** a cluster node's `/metrics` and `Admin.Metrics` label each partition's series `pigeonhole_partition="<id>"`: the engine's, and the feed fan-out's and bulk loads'. The node's own (connections, requests, sessions) carry no label. Each partition also has its unpinned engine threads (`pigeonhole_serve_partition_unpinned_shard_threads`, `…_unpinned_compaction_threads`) and its engine memory (`pigeonhole_serve_partition_memtable_bytes`, `…_block_cache_bytes`, `…_row_cache_bytes`).

### How calls are routed
- **Any node takes any call.** It serves the call if it holds the row's partition, and otherwise forwards it to the node that does. A client needs no map. One that wants to skip the hop can read the map (`Cluster.GetMap`) and call the owner directly.
- **Served by row:** `Get`, `ReadRow`, `Mutate`, `CheckAndMutate`, `MutateReturning` and counters. Also a `Batch` whose rows are all in one partition, which commits atomically as on a single node.
- **A batch across partitions** is refused with `CrossPartition` before anything is written. The error names each partition and its first row, so you can split the batch.
- **Where a node's map is older:** `WrongPartition` is handled between nodes. A node with an older map pulls the newer one and forwards again.
- **Refused with `Unsupported`** on a cluster node in this release:
  - every IVF and full-text index call, not only the searches: `Vectors.CreateIndex`, `Insert`, `Delete`, `BulkLoad`, `Search` and `Verify`, and `Text.CreateIndex`, `Put`, `Delete`, `Search`, `Rebuild` and `Verify` (#1722, 1.3.0). For vectors, `Vectors.Nearest` works and fans out across the partitions;
  - `Admin.Info`, `Admin.Flush`, `Admin.Compact` and `Admin.Promote`. `Admin.Metrics`, `Backup` and `BackupDownload` work ([Backup and restore](#backup-and-restore));
  - every `Memory` call (a cluster node refuses `--memory` at start) and `/mcp`.
- **A node holds every partition its map gives it,** opened at start in id order after checking each file's identity row against the map. One that fails to open stops the node from starting. A node the map gives no partition only routes.
- **Errors a cluster adds:**
  - `PartitionUnavailable` (503): the row's node can't be reached and nothing was sent to it. Safe to retry, writes included.
  - A forwarded write whose answer was lost after it was sent answers `OutcomeUnknown`, as on a single node. Check before you retry.
  - `CrossPartition` (409): the request can't succeed as written. Split it.
  - `Fenced` (409): the write's partition is fenced, and nothing was written. Send it again once the partition is unfenced or has moved ([Fencing and relocation](#fencing-and-relocation)).

### Schema through the cluster
Tables and their families live in the cluster map (D317 ticket 5). `Schema.CreateTable`, `DropTable`, `ListTables`, `DescribeTable` and `GET /v1/tables` work through any node, with a single node's scopes: `write` to create (`if_missing` included), `admin` to drop, `read` to list and describe. Every node makes its partitions' files follow the map.
- **A create names its placement:** `placement` gives the table's split keys and partitions (or a placement group), as the map would. In a request the split keys are bytes, so base64 in JSON, not the map file's `{"text": ...}` form:

  ```sh
  # orders: rows below "m" (bQ==) in p1, the rest in p2.
  curl -s localhost:7448/v1/Schema/CreateTable -H 'Content-Type: application/json' -d '{
    "name": "orders", "families": [{"name": "f"}],
    "placement": {"splits": ["bQ=="], "partitions": ["p1", "p2"]}}'
  ```

  Without one, a create on a cluster is refused unless the map already places the table. `if_missing` adds the families a table lacks; an existing family keeps its options, even when the request gives others, as on a single node.
- **A drop** removes the table from the map, and each partition drops it. Sessions and feeds on the table end as they do on a single node.
- **The map's writer makes every change.** A create or drop through another node is forwarded to it ([Running a cluster](#running-a-cluster)). Concurrent changes through different nodes all land, one after another. A forwarded create or drop whose answer is lost after it was sent answers `OutcomeUnknown`: a create with `if_missing` is safe to send again, and a drop that went through answers `TableNotFound` the second time.
- **The window:** the writer sets the map and pushes it, and each node brings each of its partitions' files up to date before it routes by the new map. Until a node has, a call through it can answer `TableNotFound`. Nothing was applied, so retry it. A schema call returns once the writer has set the map and pushed it, not once every node has reconciled: the reply's `unreconciled` lists partitions whose node the push missed, which catch up when their node pulls the map. A partition its operator fenced takes no schema change until `unfence` (below), and isn't listed there. `phdb cluster status` shows each partition's reconcile (its `reconciled_version`).
- **Tables made by hand** in a partition's file stay. One the map doesn't place is left alone, and `Cluster.Status` lists it as unmapped. A create through the map under its name takes it over, data included.
- **Listing:** `ListTables` and `DescribeTable` answer from the map, so they show every table of the cluster and never a partition's own `__phdb_` tables.

### Transactions and snapshots
Transactions and pinned snapshots run in one partition, on its node.
- **`Begin` and `Snapshots.Open` must name the partition** in `route`, as a row (`table`, `row`) or a partition id. That holds on every cluster, even one with a single partition, so a client keeps working when the map gains one.
- **The ids they answer name the partition** (`p2:...`). Any node routes the calls on them, and none of them are held by the node you called.
- **A row outside the partition** (a transaction's read or write, or a read at the snapshot) is refused with `CrossPartition` before anything is written.
- **A `Commit` whose answer was lost** answers `OutcomeUnknown`. Send the same `Commit` again, through any node: it answers the first outcome.
- **`Traverse` and `Nearest` at a snapshot** aren't supported on a cluster.
- **Tokens:** a session belongs to the token that opened it, on every node. A call for a token the partition's node doesn't list is refused with `PermissionDenied`. `--max-sessions` and `--max-sessions-per-token` count on each node separately.

### Reads across partitions
Scans and multi-gets read across the partitions, in key order, and return what one database holding all of them would. So do nearest searches and traversals.
- **Scans:** each page holds one partition's rows, and each partition reads at its own snapshot, so rows in different partitions aren't from one point in time. A row is never torn.
- **Multi-gets:** a page is exactly what a single node would return for the same keys and `page_bytes`.
- **Memory:** a read across partitions counts its partitions' pages against `--inflight-bytes` on the node you called (its entry node), from before its first partition is read until its reply is written. The nodes that hold its other partitions serve their parts with no budget of their own, so reads through every node can't wait on each other. Those parts count once, at their entry node. A node serves at most 16 forwarded calls at once, so its memory for other nodes' parts is bounded by 16 pages (8 MiB each at most), not by its own `--inflight-bytes`. When the budget is full it waits for room, and answers `Overloaded` past its deadline, as a single node's reply does. The budget is about `page_bytes` for a multi-get or a traversal, and twice that for a nearest search, with more taken only when it's free (to read the next partition ahead, or several partitions at once). A traversal reads each hop's edges in pages of its own `page_bytes`, so a hub with more edges than that takes more round trips. A traversal whose visited nodes leave less of the budget than one of its pages ends there, with `cut_by_budget` set.
- **Resume** from a page's `next` through any node. A cluster's positions are its own: one from a single node, or one sent to a single node, is refused with `InvalidArgument`.
- **A partition that can't be reached,** or whose node takes the connection and never answers, fails the read with `PartitionUnavailable` (by its deadline, in the second case). With `allow_partial: true`, the read skips it and names its key range in `missing`: a scan on its next page (or a last, empty one), a multi-get on its reply.
- **At a snapshot** (`Selection.snapshot_id`), a scan or multi-get reads the snapshot's partition only. Its range or keys must stay within that partition, otherwise `CrossPartition`. A multi-get at a snapshot pages as its partition's node does.
- **`Nearest` and `Traverse`** return a single node's answer (D317 ticket 4b):
  - **`Nearest`:** the exact top `k` over every partition its source meets. Up to `--fanout-forwards` (8) partitions answer at once.
  - **`Traverse`:** each hop's edges are read from every partition its frontier reaches, then ranked by the same rules a single node uses. So hops, `parents` and `stats` are the same.
  - **Snapshots:** a traversal reads each hop at its own time, and each partition at its own snapshot, rather than at one snapshot for the whole traversal.
  - **Latency:** each hop is a round trip to the partitions' nodes, so a deep traversal of a graph spread across nodes costs hops × that round trip. Keep a graph's tables in one partition where you can.
  - **With `allow_partial`:** a `Nearest` result is exact over the partitions reached. A traversal skips an unreachable partition's frontier nodes, so it doesn't follow their edges, and the later hops and the ranking are exact only over what was reached. `missing` names what was skipped.
- **The IVF and full-text indexes** (`Vectors.CreateIndex`, `Vectors.Search`, `Text.Search` and the rest) aren't supported on a cluster in this release (#1722, 1.3.0). For vectors, `Nearest` fans out.

### Feeds
`Feed.Subscribe` works the same through any node. The node you call follows every partition that holds the subscription's tables, opening a single-node subscription on each partition's node, and merges them into one stream. With a `prefix`, only the partitions whose key ranges can hold such rows are followed.

#### Setup
- Turn the feed on in every partition with `--change-feed BYTES` on its node, as on a single node. A partition whose feed was on keeps it without the flag.
- Every node lists the same tokens (`--token-file`), as [Running a cluster](#running-a-cluster) says. A subscription's part on another node is opened for your token there, and a node that doesn't list it refuses the part with `PermissionDenied`.

#### What arrives
- **Order:** each partition's commits arrive in its commit order, so each row's do. There's no order across partitions: a commit on one partition can arrive after a later one on another.
- **`partition`** on each event names where it came from. A commit's `seqno` is its partition's own, comparable only with seqnos of the same partition.
- **`partition_state`** events report a partition that went away or came back (see [When a partition's node is down](#when-a-partitions-node-is-down)).
- **Progress events** keep a quiet stream's position fresh, as on a single node.

#### Tokens and resuming

A cluster token holds every followed partition's position, so it's bigger than a single node's: about 150 bytes for each partition's shards. So on a subscription over several partitions, only checkpoints carry a token. A checkpoint is a commit every N commits, a progress event after a quiet interval, or a last progress event before the stream ends with an error. The other events have an empty `token`.

| Partitions × shards each | Checkpoint size | One every |
|---|---|---|
| 2 × 16 | about 310 B | 3 commits |
| 16 × 4 | about 1 KB | 7 commits |
| 16 × 16 | about 2.4 KB | 16 commits |
| 64 × 16 | about 9.5 KB | 64 commits |

That keeps the token at about 150 bytes per event on average. Past about 64 partitions × 16 shards a checkpoint comes every 64 commits, and costs more per event. A one-partition subscription has a token on every event, as a single node does. `checkpoint_every: N` (1–64) sets the spacing yourself; 1 gives every event a token, at the full size.

Resume from the last token you stored. The commits after that checkpoint may arrive again, at most N in all. The loop that drops them is: store the token and, per partition, the last seqno you handled, together with the work they fed.

```text
for each event:
    if event.commit and event.commit.seqno <= last_seen[event.partition]: skip it  (a repeat)
    handle it
    if event.commit: last_seen[event.partition] = event.commit.seqno
    if event.token:  token = event.token
    store (token, last_seen) with what you handled
to resume: Subscribe { resume: token, skip_through: last_seen as [{partition, seqno}] }
```

- **With `skip_through`,** the server passes over the commits you already handled, so nothing arrives twice. It refuses a skip past what a checkpoint can leave behind (more than 64 commits in a partition) with `InvalidArgument`, rather than skip commits you never saw.
- **Without it,** the `seqno` check drops the repeats.
- **Over SSE,** events without a token carry no `id:`. So `Last-Event-ID` reconnects from the last checkpoint, and the `seqno` check applies.
- **Through any node:** a token resumes through any node of the cluster. It doesn't resume on a single node, and a single node's token doesn't resume here (`InvalidArgument`).

#### When a partition's node is down
- **By default:**
  - a subscription that can't open one of its partitions fails with `PartitionUnavailable` naming it;
  - a stream whose partition goes away ends with `PartitionUnavailable`, after a last checkpoint. Resume once the node is back.
- **With `allow_partial: true`,** the stream carries on with the other partitions:
  - A `partition_state` event (`available: false`, with the error's code in `reason`) says which partition is missing. It's sent at the start for one unavailable then, and once when it goes.
  - The node tries the partition again every second, each try bounded by `--request-timeout-ms`, without holding up the others. When it's back, a `partition_state` event says so, and its commits continue from where they stopped, with none skipped.
  - A partition that was down since a subscription "from now" began starts at its own "now" when it comes back. One subscribed "from the oldest" starts at its oldest commit.
  - An expired position, or a refusal such as a revoked token, still ends the stream.
- **A partition moved to another node** (a newer map, from `phdb cluster move` or `relocate`: [Fencing and relocation](#fencing-and-relocation)) is opened again where the map then puts it, through any node, with nothing skipped, unless the move started a new history ([Expiry and what isn't supported](#expiry-and-what-isnt-supported)). The new node may still be opening it: the subscription waits for that, up to `--request-timeout-ms`, before it counts the partition unavailable, so with `allow_partial` a moved partition's `partition_state` event can come that much later.

#### Limits
- **Stream slots:** a subscription holds a `--max-streams` place on the node it entered, and one on each followed partition's node. An entry node that serves one of the partitions itself holds two.
- **Memory:** it buffers up to 256 KiB on each partition's node, so partitions × 256 KiB at most, outside `--inflight-bytes`, like a single node's feed. The entry node buffers nothing of its own: it merges the partitions' events as its client reads them.
- **Stalled subscribers:** a subscriber that stops reading ends after `--stream-idle-ms`, as on a single node. Its `--max-streams` places, its partitions' streams and its connections between nodes are freed then, and it's counted in `pigeonhole_serve_feed_idle_drops_total`. Until then it holds only its own connections between nodes, never one that forwarded calls or other streams use. A subscriber that keeps reading keeps its subscription however long nothing happens: on a cluster the limit counts the client's reads, not the events it gets, so it ends a client that stopped reading, never one that's waiting for a commit.

#### Expiry and what isn't supported
- **Expired tokens:**
  - A token can't resume after a partition it follows starts a new history: a `relocate` without `--copy` (`--old-unreachable` included) does, as a restore does, and so does a `set-map --force` that raises its epoch ([Running a cluster](#running-a-cluster)). `move` and `relocate --copy` copy the files whole and keep the history, so tokens resume across them ([Fencing and relocation](#fencing-and-relocation)).
  - It can't resume after the map changed which partitions the subscription covers either. That happens only when a new table joins a subscription of every table.
  - Both answer `FeedExpired`: subscribe again.
- **After a restore:** a token from before it answers `FeedExpired`. Each restored partition is at a new epoch, and its history starts at the restore ([Backup and restore](#backup-and-restore)).
- **Not supported:** a start from a snapshot isn't supported on a cluster subscription. A one-partition version is #1701.
- **Positions:** a token's `seqno`s are partition-local. There's no cluster-wide position, so compare progress per partition.

### Fencing and relocation
A node writes a partition only while its newest map names it for the partition at the file's epoch, and the partition isn't fenced (D317). `phdb cluster move` does a whole move. The commands after it are its steps, for doing one by hand or for a node that's down.

- **`phdb cluster move P --to ID --node URL [--drop-source] [--timeout SECS]`** (admin scope) moves P to node `ID` with its files copied whole, so its change-feed history carries over: tokens taken before the move resume after it, through any node.
  - **Its steps:**
    1. It fences and closes P on the node serving it, and waits for the close to hash its files.
    2. It copies each file to `ID`: the main file, every WAL file a clean close keeps, and the feed's.
    3. It stamps P there at the next epoch, and checks the installed set against the closed one: each file's name, size and SHA-256.
    4. It checks once more that P is still closed on the old node with exactly the files it compared, then sets the map (`relocate --copy`), and waits for `ID` to serve P. If P was unfenced and closed again there meanwhile, its files changed: the map stays as it was, and a rerun copies the new files.
  - **A mismatch** stops the move before the map changes, so the old node still holds P: run the move again to copy the files again, or `unfence` P to serve it there again (a peer's map must still give P to that node: see `unfence` below).
  - **Downtime:** writes answer `Fenced` from the fence until the map moves. Reads answer `PartitionUnavailable` from the close until `ID` serves, which is about as long as copying every file, one after another. Both grow with the files' size. With a change feed, that's the WAL files a clean close keeps for the feed's retention, so a long retention means a longer move. The report gives both durations and each file's size.
  - **`--timeout SECS`** (at least 1) bounds each of its two waits: the old node hashing its closed files, and `ID` serving P. A wait that runs out ends the move with exit 1. The error names the step, the node's last `status` line for P, and where that leaves P:
    - **The hash:** the map still gives P to the old node, closed there. Run the move again to go on waiting, or `unfence` P there.
    - **Serving:** the map already gives P to `ID`, so check that node.

    The copy has no limit, since it takes as long as the files do. Without `--timeout`, a wait goes on as long as it takes, and says every 10 s what it's waiting for.
  - **Run it again** to resume a move that stopped. A move that's finished answers that the map already gives P to `ID`.
    - A rerun before the map changed copies and checks the files again, whatever an earlier run left on `ID`: a copy still staged there is picked up from what arrived, and one already installed is sent whole and installed over the earlier one, which moves aside (`<file>.replaced-<time>`). So if you unfenced P on the old node between two runs, the second run moves the writes it took since.
  - **The old node's files stay** unless you pass `--drop-source`. With it, they move aside (`drop-partition`, below) once the copy has checked out and `ID` serves P. A rerun that finds the map already changed drops nothing; its report gives the `drop-partition` command.
  - **The old node must answer.** For a node that's down, bring its partitions back on another from a backup with `relocate --old-unreachable` ([A lost node](#a-lost-node), below). That starts a new feed history.

- **`phdb cluster fence P --node URL`** (admin scope) fences P on the node that serves it.
  - Its writes then answer `Fenced` through every node, until it's unfenced, closed and moved, or the node restarts with it unfenced.
  - Reads go on, and so do its flushes and compactions.
  - **Schema changes wait too:** a table created or dropped through the cluster meanwhile reaches P only at `unfence`, so reads of the new table's range in P answer `TableNotFound` until then. `phdb cluster status` shows P's `reconciled_version` behind the map.
  - The command answers once the writes admitted before the fence have finished. Past 30 s it answers `FailedPrecondition`, naming how many still run.
  - The fence is kept in `DIR/fenced.json`, so it outlasts a restart.
  - A fence is the copy's it was set on: when P's files are later installed on the node again (a move back), the fence is gone, and P serves there once a peer's map confirms it.
- **A restarted node** opens every partition fenced, and unfences each one once a peer's map, at least as new as its own, still names it for the partition at the file's epoch. A node whose peers don't answer keeps them fenced: `phdb cluster status` shows `fenced (awaiting peer map)`, and `fenced (operator)` for a fence of yours. A one-node cluster unfences on its own map.
- **`phdb cluster unfence P --node URL`** lifts either fence. It's refused unless the node's map names that node for P at its file's epoch, and a peer agrees.
  - **The peers' newest map must confirm it,** whichever fence it is: the command asks every peer first, and lifts the fence only if the newest map among those that answer is at least as new as the node's own and still names the node for P at that epoch. One peer agreeing isn't enough: a peer that missed the same move holds the same old map. A peer that doesn't answer can't be counted, so if the only node that knows of a move is down, the others still confirm: check `phdb cluster status` on every node before you unfence the old node of a move. The node's own map isn't enough: it may have been down, or missed a push, while a move gave P to another node. That is most likely on the old node of a move, which the move fenced. A newer map a peer answers with is taken by the node at once. A cluster of one node confirms on its own map.
  - **`--force`** lifts it without a peer's map. Use it only when no peer can answer and you know P hasn't moved. If a peer's newer map gave P to another node, both would then take writes.
  - **Don't unfence P during a relocation.** Between `relocate`'s check and its map change, the old node's map still names it, so an unfence there resumes its writes.
- **`phdb cluster close P --node URL`** closes a fenced partition. Its database closes, so its files are final, and its calls answer `PartitionUnavailable`. A call already running on it ends the same way.
  - A close keeps the partition's fence in `DIR/fenced.json`, whichever fence it was: a closed partition stays fenced across a restart until `unfence`.
  - The partition's feed hubs end, and their clients resume from their tokens. A cluster subscription with `allow_partial` doesn't end: it reports the partition unavailable (`partition_state`) and goes on, with nothing skipped, when the partition serves again, here or where a move put it.
  - The command answers at once. `phdb cluster status` then lists the files with their SHA-256 once they're hashed.
  - A failed hash shows in the partition's error, and `close` again retries it.
  - `unfence` reopens a closed partition, once its checks pass: a refused unfence leaves it closed.
- **Copy the closed files to the new node** with `Admin.PartitionUpload`, taking each from the old node with `Admin.BackupDownload`'s `fenced_file` (its name in `status`'s list). They're staged in the new node's `DIR/.phdb-incoming`, and nothing serves them yet.
- **`phdb cluster relocate P --to ID --node URL`** moves P to node `ID`, whose staging holds its files. (`--node` is the node the command talks to, as in every `phdb cluster` command.)
  - It checks that P is fenced or closed on the node serving it. `--old-unreachable` skips that check, for a node whose process you've stopped.
  - The new node installs the files and takes the next epoch. Then the map gives it P, and it serves at once.
  - **If you once fenced P on the new node** (it held P before, and P moved away), a relocation that installs staged files there clears that fence, since the files are another copy. One that installs nothing, onto the copy the node kept, leaves it: P opens `fenced (operator)`, safe but taking no writes, until `phdb cluster unfence P` there.
  - An install interrupted by a crash finishes the next time the node is stamped or opens P.
  - Run again once done, `relocate` answers that the map already gives P to that node. Run again after a map change that didn't land, it stamps again (the file is already at the new epoch, so that does nothing) and sets the map.
  - `--old-unreachable` still asks the old node briefly, and refuses if it answers and serves P.
  - **`--copy`** says the staged files are P's own, copied whole from the node serving it, as `move` copies them. P then keeps its feed history, and tokens from before resume on the new node.
    - **It's checked:** P must be closed on the old node. The set the new node installs at the stamp must be the old node's closed files, each file's name, size and SHA-256, and just before the map changes the old node must still hold P closed with those files. Otherwise `relocate` refuses and the map stays as it was. That includes a stamp that installs nothing, such as files already stamped by an earlier run, since there's nothing to check. The error names `phdb cluster move`, which copies the closed files itself.
    - Without `--copy`, the move starts a new history (a promotion, a restore), and older tokens answer `FeedExpired`.
    - `--copy` is refused with `--old-unreachable`: a copy is taken from that node's closed files.
    - Use it only for files copied whole from the partition as it closed. Tokens taken before the move then name changes the new files have.
    - **`--old-unreachable` can lose acknowledged writes:** nothing compares the new node's files with a node that doesn't answer. Writes the old node acknowledged after your copy was taken aren't in it. Use it only when that node is gone, with the newest copy you have.
- <a id="a-lost-node"></a>**A lost node: bring its partition back from a backup.** With the old node gone, a partition's files can only come from a backup (`phdb cluster backup` or `Admin.Backup`). For each partition P it held:
  1. **Check the epoch.** The backup's copy must be at P's current epoch in the map: `phdb cluster show-map` gives the map's, and the backup's `manifest.json` the copy's. They match unless P was moved, relocated or restored after the backup.
  2. **Copy the backup's `P.phdb` into the new node's `DIR`,** as `P.phdb`. The new node must not hold P already. A running node holds `DIR/.lock`, not its files, so the copy can go in while it runs.
  3. **Run `phdb cluster relocate P --to ID --old-unreachable --node URL`,** with `--node` a node that answers. The new node stamps the copy at the next epoch, the map gives it P, and it serves.
  - **What you lose:** the writes to P after the backup. P starts a new feed history, so tokens from before answer `FeedExpired`.
  - **If the lost node was the map's writer** (the smallest id), set a map without it first with `set-map --force` ([Running a cluster](#running-a-cluster)), then relocate.
  - **If the epochs don't match,** nothing in this release puts the older copy back for P alone: `relocate` takes only a copy at the map's epoch, and `phdb cluster restore` restores every partition. Restoring the whole cluster from the backup is the way back. A restore of one partition (`phdb cluster restore --partition P`) is [#1832](https://github.com/CodingAnarchy/pigeonhole/issues/1832), 1.3.0.
- **`phdb cluster drop-partition P --node URL`** moves P's files on the old node into `DIR/.phdb-dropped/<P>-<epoch>-<time>/`. It's refused while the map still places P on that node, or while P is open there, so close it first.
  - Nothing is deleted, and nothing is freed until you delete that directory, once you've checked the move.
- **A node opens a partition while it serves** when a map gives it one. Its share of the node's `--shards` and `--compaction-threads` is the share it would get at start for the new set of partitions, and the partitions already open keep theirs.
  - **To add a new partition to a running node,** its file must be in the node's `DIR` before the map gives it the partition. `phdb cluster init-partition` can't write into a running node's `DIR`, since the node holds `DIR/.lock`: stamp the file in another directory and move its files into `DIR`, or upload it (`Admin.PartitionUpload`) and use `phdb cluster relocate`.
  - **Memory:** it gets what the open partitions leave of `--engine-memory`, never more. A node that started with partitions gave them all of it, so there the open is refused, saying so. **Restart that node alone:** the map already gives it P, and its start splits `--engine-memory` across every partition, P included. A node that held no partition, the usual move target, gives P all of it. Without `--engine-memory`, every partition takes the library's defaults, as at start.
  - It opens on the free cores, off every open partition's shard cores and their SMT siblings. If its shards don't fit there, the open is refused, naming how many would run unpinned, rather than put on a sibling.
  - **The server's threads then move off the new shards' CPUs** (or back onto a closed partition's), and a log line names each runtime's threads against its CPUs. Their counts stay as the server started them, since a runtime can't be resized. A background thread already running (a backup, a bulk load, an index build, a replica's follow) keeps its CPUs until it ends, and the new partition's own engine threads keep the CPUs it opened on. After a partition opens, the network, feed and blocking threads share fewer CPUs, and run more of them per CPU than a node started with that partition would. Restart the node to size them again.
- **Errors:** `Fenced` (409, gRPC `FAILED_PRECONDITION`) means the write wasn't applied because its partition is fenced. Send it again once the partition is unfenced or has moved: a node that takes it reroutes to the new owner.

### Backup and restore
- **`phdb cluster backup DIR --node URL`** backs up every partition, each through the node the map gives it, into a new or empty `DIR`. It writes `<partition>.phdb` for each partition, `map.json` and `manifest.json`.
  - Each node needs `--backup-dir`, and the token admin scope, as for `Admin.Backup`.
  - `--durable` copies only what each writer holds durable.
  - `--parallel N` (4) copies that many partitions at once.
  - A broken download resumes.
- **Not one point in time.** Each copy is a consistent snapshot of its own partition, and the set is not one point in time across partitions. A commit on one partition can be in the backup while an earlier one on another isn't. A table created during the backup can have rows in one partition's copy and none in another's.
- **Checked against the map.** The backup checks each copy's cluster, partition and epoch against the map. A copy that differs fails it, naming both, for example `the copy is partition "p2" of cluster "c1" at epoch 2, but the map says partition "p2" of cluster "c1" at epoch 1`. That means the map changed during the backup, or the node holds another file. Run it again once the map is settled.
- **A map that moves meanwhile:** a partition added, moved or relocated during the backup fails it: `the map changed during the backup`. Tables created or dropped meanwhile don't, but a table dropped and created again under its name does (`table users was dropped and created again`): some copies may hold the old table and others the new. `map.json` is the map at the end, which places them as the copies hold them.
- **A backup is complete only with its `manifest.json`,** which is written last. Copies are `<partition>.phdb.partial` until checked.
- **One partition's backup:** a node's `Admin.Backup` and `BackupDownload` back up one partition it holds, named by `partition`, and aren't forwarded. Another node's partition is `WrongPartition`.
- **`phdb cluster restore DIR --node URL`** puts a complete backup back. Each partition's copy goes to the node the backup's `map.json` gives it, is installed there at a new epoch, and the backup's map is then set. **Writes since the backup are lost.**
  - **First, fence every partition** (`phdb cluster fence`), **and close each one on the node it's restored to** (`phdb cluster close`). A partition that serves anywhere, or is open on the node it goes to, refuses the restore before anything changes, naming the command to run.
  - **Checks before any change:** every copy's size and SHA-256 against `manifest.json`, the map's partitions against the backup's, and every node: the node each partition goes to must answer. So must the node the live map gives it, unless `--old-unreachable` (its process is stopped).
  - **Then, per partition:** the copy is uploaded (`Admin.PartitionUpload`, resumed after a break; `--parallel N` (4) uploads that many at once), then the node installs it at epoch `e + 1`, one partition after another. `e` is the highest of the backup's, the backup map's and the live map's epoch for the partition, so a node still holding an older file can't write into the restored cluster.
  - **Last, the map:** the backup's map is set by the forced path (as `set-map --force`): its version goes above every reachable node's, and every table is placed exactly as the backup has it. Its writer, the node with the smallest id, must answer. A table created after the backup is dropped, with its data, and one dropped since comes back.
  - **Then served:** each partition opens on its node with the map, under the fence you set before, and the restore lifts that fence once it's open. It waits up to a minute for each, and otherwise says which isn't serving yet.
  - **`--map FILE`** installs an edited copy of the backup's map instead, for example with nodes at new addresses or other nodes holding the partitions. It must have the backup's partitions and exactly the backup's tables.
  - **`--cluster-id ID`** restores under another cluster id, onto nodes of no cluster yet, beside a live cluster: every file is stamped with it. A node in another cluster refuses the restore. A `--map` file still names the backup's cluster id: the restore replaces it with `ID`, and refuses a map naming another. A node of no cluster yet is `phdb serve --cluster DIR --node-id ID` on a `DIR` with no `cluster-map.json`.
  - **History starts over.** A restored partition is its backup's main file alone. Its WAL and feed files are moved aside to `<file>.replaced-<time>`, never deleted: delete them once the restore is checked. Feed tokens from before the restore answer `FeedExpired`.
  - **Run it again** after a failure. `DIR/restore-progress.json` records each partition installed and the map once set, and goes when the restore is done. A run again skips a partition already installed at its new epoch (no second upload), and if the map is already set and still places every partition so, it only lifts the fences. An unfinished upload resumes. If `DIR` can't be written to (a read-only snapshot), the restore warns and goes on without the file, and a run again after a failure redoes the whole restore.
    - A node restarted between the stamps and the map still starts: a partition whose file is stamped at an epoch above its map's is left unopened, with a line in the log, and `phdb cluster status` shows both epochs. The run again reaches it and sets the map, which opens it. A file behind the map's epoch, or another cluster's, still stops the node.
  - **A node that held a partition and doesn't get it back** (the restored map, or `--map`, places it elsewhere) keeps its copy open and fenced, with its memory and CPUs, and its files. The report lists those nodes (`close_and_drop_on`). On each, `phdb cluster close P`, then `phdb cluster drop-partition P` once the restore is checked.

### Partitions per node
A node serves every partition its map gives it (D317 ticket 8a). **In the runs below, several partitions on one node cost nothing measurable (up to 8 partitions) and didn't change its capacity (up to 4), as long as the node's totals were set.** Capacity follows the node's totals and its network threads, not the number of partitions.

**Measured** (perf287's box runs, #1482), with 20M rows of 100 B split evenly by key range and two rounds at each N:
- **At a fixed load** (set by the client, so not capacity): one node on 12 cores, `--shards 8 --compaction-threads 4 --engine-memory 1073741824` (1 GiB), with 1, 2, 4 and 8 partitions and 64 callers.
  - ycsb-a at Buffered, ycsb-a at GroupSync and zipf ycsb-c: throughput and p99 at every N fell within the run-to-run noise at N=1. That noise was 4% in throughput for ycsb-a at Buffered.
  - The worst cell against one partition in the same round was 2 partitions at GroupSync: 2% fewer ops/s, and 4% more p99 on Mutate.
- **At saturation:** one node on 8 cores, `--shards 4 --compaction-threads 2 --engine-memory 1073741824`, with 1, 2 and 4 partitions and 512 callers.
  - Capacity was the same at every N: about 270–280K ops/s for ycsb-a at Buffered (p99 about 4.4–5.2 ms) and about 305–333K ops/s for ycsb-c.
  - Rounds differed by up to 8%, more than any N did.
  - What saturated was the node's network threads (6 on that node, each close to 100% of a CPU), not its shards. The network side is node-wide, so splitting the node doesn't change it.

**To get that:**
- **Set `--engine-memory`,** or each partition takes the library's full caches.
- **Keep `--shards` a multiple of the partition count.** A remainder gives the first partitions one shard more than the rest.
- **Past `--compaction-threads` partitions, the compaction CPUs are oversubscribed.** With several partitions, every partition's compaction threads run on the node's compaction CPUs, and each partition keeps at least one thread. The run measured 8 partitions on 4.

**A node's budget is shared by its partitions:**
- **`--shards` and `--compaction-threads` are the node's totals,** split across its partitions in id order, at least one shard each. A partition whose change feed recorded a shard count keeps it.
- **The shards pin first:** with several partitions, every partition's shards are pinned before any compaction thread, and the compaction threads share the node's compaction CPUs. So the network side keeps the same CPUs as with one partition. Before 8a's placement and node-total split (g74), four partitions on one node gave 25–34% less throughput than one; with them, the runs above found no measurable difference: up to eight partitions at a fixed load, and up to four at saturation.
- **`--engine-memory` is the node's total too.** The map's `memory_bytes` for a partition is taken first, and the rest is split equally across the node's other partitions. Set it when a node holds several partitions, or each takes the library's full defaults.
- **The startup lines** give each partition's shard and compaction threads (pinned, confined or unpinned) and its memory. On Linux, a partition with unpinned threads is a warning. `/metrics` has the same per partition.
- **`--change-feed` is not split:** each partition keeps the whole retention over its own shards, so a node's feed disk is its partition count times the retention. With WAL preallocation (unless `--no-wal-preallocate`), that disk is written when the partitions open, not reached over time. For example, with `--change-feed 4294967296` (4 GiB), a 16-shard node writes about 9 GiB of log at start as one database, and about 37 GiB as 8 partitions of 2 shards. A node-total feed budget is planned for 1.3.0 ([#1786](https://github.com/CodingAnarchy/pigeonhole/issues/1786)).
- **Sessions and feeds:** `--max-sessions`, `--max-sessions-per-token`, `--feed-ring-bytes`, `--max-feed-hubs` and `--max-catch-up` count across all of a node's partitions. A feed with tables on two of a node's partitions holds two hubs there.

**What a partition costs** even so: each is a whole database engine, with costs that don't shrink with its share of the load:
- at least one shard's memtable arena (64 MiB by default);
- its own WAL, so its own group syncs: the same load over more partitions means more syncs, each covering fewer commits;
- its own block and row caches, so a busy partition can't use an idle one's;
- its own flush and compaction.

A few partitions per node leave room to move one to another node later.

### Choosing split keys and hot ranges
A table's split keys are fixed once its ranges hold data, until range moves in 1.3.0 (#1499). Choose them from how your keys are shaped:
- **Tenant prefixes** (`tenant/…`): split at tenant boundaries. A tenant's rows stay in one partition, so its batches and transactions stay in one, and a scan of one tenant reads one partition. A big tenant can be given a partition of its own.
- **Hashed prefixes** (a byte or two of a hash in front of the natural key): split evenly over the prefix space for an even spread of rows and load. The cost is key order: a scan in natural order reads every partition.
- **Time-bucketed keys:** don't lead with the time. With fixed split keys every new write lands in the last range, so one partition takes all the writes. Put a tenant or hashed prefix first and the time after it.

**A range that turns out hot:** until ranges can move, give its partition a node of its own or a bigger one, by moving the partition ([Fencing and relocation](#fencing-and-relocation)).

## Performance
> **Coming with D313 ticket 9b:** the serve bench's numbers on the reference machine: puts, gets and scans over gRPC and HTTP/JSON, and the change feed's delivery at many subscribers, with and without the placement mask.

## Next
- [Observability](observability.md): the engine's metrics, alerts and dashboard.
- [Using Pigeonhole from AI agents](ai-agents.md): `phdb mcp`, and remote MCP's client configuration.
- [Recovery](recovery.md): `OutcomeUnknown`, and operation ids.
- [Change feed](change-feed.md): tokens, retention and delivery.
