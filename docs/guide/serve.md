# Serving a database: `phdb serve`

> **Status:** this page describes 1.3. `phdb serve` is new in 1.2.0 (D313, #1397), behind the `serve` feature. [ICR 0112](../design/icr/0112-network-api.md) is the full contract.

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
- **The shards:** `--shards` sets the count. The default is half the physical cores this process may run on (its CPU affinity and quota: under `taskset` or in a container, not the host's), which leaves the other half for the network side. It applies on every start, a restart included, since the count isn't stored in the file. So a restart under another affinity mask or CPU quota opens a database without a change feed with that count (a reshard, harmless without a feed). A database with a change feed is the exception: it keeps the count its feed recorded, because another count ends the feed's history (`--shards` with another count reshards it on purpose).
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
  - `write` adds writes, `CreateTable`, the index writes, the memory writes (`Remember`, `MarkUsed`, `Supersede`, `Link` and `Forget`) and `Transactions.Outcome`, which can make a transaction's outcome final.
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

# The 5 vectors of family emb nearest a query, among qualifiers starting with "img:".
curl -s localhost:7448/v1/Vectors/Nearest -H 'Content-Type: application/json' -d '{
  "table": "docs", "family": "emb", "k": 5, "qualifiers": {"prefix": "aW1nOg=="},
  "query": {"element": "ELEMENT_F32", "dim": 2, "data": "AAAAAAAAgD8="}}'
# {"neighbours":[{"row":"...","qualifier":"aW1nOjE=","distance":0.5,"timestamp":"..."}]}
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
`/mcp` serves `phdb mcp`'s tools over MCP's Streamable HTTP transport: [the agent guide's remote section](ai-agents.md#remote-mcp-phdb-serve) has the client configuration, scopes and sessions. Its own flags are `--mcp-max-result-bytes`, `--mcp-max-value-bytes`, `--mcp-session-idle-ms`, `--mcp-session-lifetime-ms`, `--mcp-session-requests`, `--mcp-session-cache-bytes` and `--mcp-blocking`.
- **Each tool call is a service call** (#1915), made as the session's token, within its request's timeout. It's checked, routed and refused as the same gRPC or `/v1` call would be: `read_row` is `Data.ReadRow`, `scan` `Data.Scan`, `write` `Data.Batch`, `create_table` `Schema.CreateTable`, `nearest` `Vectors.Nearest`, `traverse` `Graph.Traverse` and `feed_poll` `Feed.Subscribe`. The memory tools are `Memory` calls too: `memory_remember` is `Remember` (with `supersedes`), `memory_recall` `Recall`, and so on. Each takes its memory place (`--memory-calls`) as the RPC does, so a `/mcp` message holds only its own place while it waits for one. One difference from `phdb mcp`: a `memory_forget` that fails part-way answers the error (on a cluster, `OutcomeUnknown` naming the partitions whose outcome isn't known), not a partial list. The deletes before it stand, and retrying is safe.
- **On a cluster node** (1.3.0, #1915), `/mcp` is served by every node that holds a partition, and its tools reach every partition as the RPCs do: a `scan` or `nearest` fans out, a `write` across partitions is refused with `CrossPartition`, and a `feed_poll`'s token resumes through any node. A node that only routes answers `/mcp` with `Unsupported`. The memory tools are listed on a node started with `--memory` only, since a recall across partitions runs with that node's memory flags ([Agent memory on a cluster](#agent-memory-on-a-cluster)).
- **`--mcp-blocking`** (default 8): the `/mcp` messages running at once, across sessions. Each runs on a blocking thread of its own, outside `--blocking-threads`, whose permits its tools' calls take; the server keeps this many blocking threads more. One past the limit waits for a place until its request times out, then gets `Overloaded`.
- **`feed_poll`** with `wait_ms` waits for its first commit before its message takes a place, so a waiting poll holds neither a place nor a blocking thread. Its subscription holds a `--max-streams` place. Inside a JSON-RPC batch, or with no `wait_ms`, it doesn't wait: it returns what's ready, which on a server means what arrives within a moment of asking.
- **Cursors** name their tool and that call's arguments: a cursor from another call is refused. A `read_row` cursor is the server's own: one from `phdb mcp`, or from a server before 1.3, is refused with a usage error, so start that row's walk again without it. A `scan` cursor (a row key) and a `traverse` cursor carry over.

## Reads, writes and sessions
- **`Data.Batch`** commits writes to several rows, across tables, atomically. It takes every write `Mutate` takes: puts of bytes, `i64`, `f64`, `u64` and vectors (checked against the family's spec first), increments, merges, aggregate inputs and deletes. It refused `u64` and vector puts and aggregate inputs before 1.3 (#1915). On a cluster mid-upgrade, a node still on the older build refuses those writes in a batch with `InvalidArgument`.
- **Scans** stream pages of up to `page_bytes` each (default 1 MiB). Over gRPC, `Data.Scan` is a server stream. Over HTTP, `POST /v1/Data/Scan` answers one JSON page per line (`application/x-ndjson`), or server-sent events with `Accept: text/event-stream`. A scan that fails partway ends with a final error message: an NDJSON line `{"error": ...}`, or an SSE `event: error`. Each page's `next` resumes the scan later, from any server process. The scan is complete only when the last page's `next` is empty; a page budget too small for anything ends the stream with an empty page and a position, so resume with a larger `page_bytes`. Each page reads at its own snapshot: a row is never torn, but rows can change between pages. A client that stops reading for `--stream-idle-ms` (default 30 s) gets `DeadlineExceeded`, and resumes from the last page's `next`.
- **Transactions** (`Transactions.Begin`, `Get`, `GetRow`, `Write`, `Commit`, `Abort`, and from 1.3.0 `Outcome`, which asks what became of one) are held on the server between calls, under an opaque `txn_id` bound to the token that began them:
  - **One call at a time.** A second call while one runs gets `SessionBusy`; send it again when the first answers.
  - **`Commit`** runs to its outcome even if the client goes away. Sent again, it answers the first one's outcome and commits nothing twice. After `Commit`, other calls answer `TransactionFinished`.
  - **Leases:** a transaction idle for `--txn-idle-ms` (30 s), or older than `--txn-lifetime-ms` (5 min), expires. Before any `Commit`, that's `TransactionExpired`: nothing was written, so begin again. A call in flight never expires.
  - **`NotFound` for a `txn_id` you sent `Commit` to** (after a restart, or once its record expires) means the outcome is unknown: check an operation id the transaction wrote before retrying ([recovery](recovery.md)).
  - A read token may begin, read and commit a transaction without writes; writing needs a write token.
- **Pinned snapshots** (`Snapshots.Open`, `Release`) make reads agree across requests: pass the `snapshot_id` as `GetRequest.snapshot_id`, or as `Selection.snapshot_id` for `MultiGet`, `ReadRow` and `Scan`. Each read renews the snapshot's lease (`--snapshot-idle-ms`, 60 s; at most `--snapshot-lifetime-ms`, 10 min). A held snapshot holds back version and blob GC, so release it when done. Reads already running when it's released finish; later ones, and a scan's later pages, get `NotFound`. So do a scan's pages past `--snapshot-lifetime-ms`: a scan read slowly ends there with `NotFound`, resumable from its last page's `next` at a new snapshot, and the snapshot goes when the scan ends.

### Errors and retries
- **Errors** carry ICR 0112's stable code: a gRPC status detail, or the HTTP JSON body (`{"code": "TableNotFound", ...}`). Retry a write only when the error says `retry_safe`. A bare transport error means its outcome is unknown. A read may be retried; one that failed on `Io` says so (`retry_from: REQUEST`). `MergeFailed` doesn't: a merge operator that fails on stored data fails the same way again. `Unavailable` (503 with `Retry-After`, not applied, retry-safe) means a service the server depends on failed or can't be reached: today `Memory`'s embedding endpoint (see Agent memory below), or, on a replica, a memory store the replica doesn't have yet. `FailedPrecondition` (409, not applied, don't retry) means the server's role refuses the call: `Admin.Promote` on a server that isn't a replica or on one whose first bootstrap hasn't finished, a replica asked for a bootstrap copy or to be followed (see [Replicas](#replicas)), a `DropTable` of the served memory store's tables, or a `DropTable` of a text index's source table.

The rules a client keys on ([D313](../design/decisions/phase-3.md#d313), [ICR 0112's error table](../design/icr/0112-network-api.md#errors)):

| The response | The request was | Retry |
|---|---|---|
| `retry_safe: true` with `retry_from: REQUEST` (`RETRY_FROM_REQUEST` on the wire) (`Busy`, `Overloaded`, `SessionBusy`, `Closed`, `ReplicaBehind`, `Unavailable`, `DeadlineExceeded`, and a read's `Io`) | not applied | send it again, after `Retry-After` where given |
| `retry_safe: true` with `retry_from: BEGIN` (`Conflict`, `SnapshotExpired`, `TransactionExpired`) | not applied | run the transaction again from `Begin` |
| `retry_safe: false` with a validation, schema, scope or role code (`InvalidArgument`, `TableNotFound`, `PermissionDenied`, `ReadOnly`, `FailedPrecondition`, ...) | not applied | no: fix the request |
| `retry_safe: false` with a 503 the server can't help (`WriterLocked` and the other codes for a database it can't open) | not applied | no: the server's database needs attention |
| `retry_safe: false` with any code, from a `Vectors.BulkLoad` that had taken vectors (its message says the vectors before it were loaded, or may have been) | part applied: the vectors before the failure, or some of them | no: `Insert` the rest, or drop the index and load again (a load needs an empty index) |
| `TransactionFinished` | that call did nothing | no: send `Commit` for the transaction's outcome |
| `FeedExpired` | (a read) | no: subscribe again from now |
| `FeedRerouted` (503, `retry_safe`) | (a read) | yes: resume from the last token the stream sent, which it sent just before it ended. A cluster subscription ends so when a range move gives one of its tables a partition it didn't read (1.3.0, D331) |
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
- `Flush`, `Compact`, `Backup`, `BackupDownload` and `Promote` need admin scope, as does `Replication.Follow` (gRPC only), which replicas use. On a cluster node, a node's cluster token may also download a partition's closed set or a replica's bootstrap (a move the map's writer runs, 1.3.0).
- **On a cluster node** (#1852), `Info`, `Flush` and `Compact` cover the partitions the node holds, answered there and never forwarded:
  - `Info` lists each partition the map gives the node (`partitions`: its id, file, size, tables, and its state, its fence reason and whether it awaits its peers, as `phdb cluster status` shows them) and names the map's other partitions (`other_partitions`). Its `path` is the node's cluster directory, `file_bytes` the partitions' files together, and `tables` the map's.
  - `Flush` and `Compact` act on the partitions `Info` lists that are open (serving or fenced), one after another, or on the one `partition` names. All of them are tried even when one fails, and the answer then carries the first failure's code and names every partition that failed. A copy the map moved away that's still open here (fenced until it's closed) isn't one of them. A partition another node holds is `WrongPartition`, and one closed or not open here is `PartitionUnavailable`. On a single node, `partition` must be empty.

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
    - **`qualifiers`** (a `QualifierSelection`, #1915) ranks only the vectors at the qualifiers it selects: a `prefix`, `start` and `end` bounds, or both, as a read's `Selection` applies its qualifier fields. Absent, every qualifier of the family is ranked. On a cluster node, each partition's part applies it. During a rolling upgrade, a cluster search with `qualifiers` is refused (`FailedPrecondition`, naming the node) while a node holding part of it runs a build without them (its `Status` lacks `nearest_qualifiers`). Finish the upgrade, or search without `qualifiers`. A node that doesn't answer is unavailable, as for any read: with `allow_partial` its partitions are skipped and named in `missing`, otherwise the search fails with `PartitionUnavailable`. The entry node remembers a node's answer for 30 seconds, and a node that didn't answer for 5, so a search while a node is down doesn't wait for it each time.
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
    - **The load holds a working set of its own, outside `--inflight-bytes` and bounded by `--bulk-load-bytes`** (1 GiB). `expected` sets the lists: `expected` / target list length, at least 1. The target is the index's (1,000 by default), scaled down so twice it fits a list's 4 MiB row: 682 at 768-dim `f32`, 341 at 1,536-dim. A half index (`f16`, `bf16`) counts two bytes a dimension (682 at 1,536-dim), and `i8` one. An index keeps the target it was created with: a half index created before [#1882](https://github.com/CodingAnarchy/pigeonhole/issues/1882)'s fix was scaled at one byte a dimension. Above about 1,048 dimensions, a half index created since has lists half as long, so a search at the same `nprobe` examines about half as many vectors and recalls less: double `nprobe` to probe as many as before. The load clusters a sample of its first max(50,000, lists) vectors before it writes any, then keeps up to 255 vectors per list until each list's batch of 256 is written. So it holds at most the larger of:
      - **the sample:** max(50,000, lists) × (a vector's bytes + its clustering copy, `f32` but for a bit index + twice its id + about 96 bytes);
      - **the pending batches:** lists × 255 × (a vector's bytes + twice its id + about 96 bytes).

      A vector's bytes are dimensions × 4 for `f32`; × 1 for `i8`; and dimensions / 8 for bits. A half index (`f16`, `bf16`) is counted at `f32`'s size, whichever element type the client sends, since it takes `f32` vectors too. Ids are counted at 32 bytes up front.
    - **A load over `--bulk-load-bytes` is refused before it starts,** with `InvalidArgument` naming the bytes it needs. Raise `--bulk-load-bytes` to that, or load in parts: `Insert` the rest after a smaller load. At the default 1 GiB:
      - **768-dim `f32`** (lists of 682): the sample is about 315 MB, and each list's pending batch about 0.8 MB, so up to about 1,300 lists: about 888,000 vectors. A million needs about 1.2 GB.
      - **1,536-dim `f32`** (lists of 341): the sample is about 622 MB, and each list's pending batch about 1.6 MB, so up to about 670 lists: about 227,000 vectors. A million needs about 4.7 GB.
    - **`expected` is an upper bound:** a load stops once it has sent what its lists hold before they split, lists × twice the target (about twice `expected`), with `InvalidArgument`, the vectors before it loaded. Declaring too few (0, say) would otherwise grow one list without bound. `Insert` the rest (the index is no longer empty), or drop it and load again with the right `expected`.
    - **The split after a load stays small, however the vectors fall.** The rebalance after a load splits a list longer than it reads whole from a sample, and moves it a page at a time ([#1824](https://github.com/CodingAnarchy/pigeonhole/issues/1824)), so even a load whose vectors all fall in one list (a write client sending one vector repeated, say) holds about 16 MiB at most, outside the flag, whatever the element and dimension: a split reads a list whole while that list's vectors and ids, as the split holds them (`f16`, `bf16` and `i8` widened to `f32`), fit in 16 MiB, or while the list is within four split lengths, if that is more. Such a load still takes longer, since each split reads the list it splits.
    - **A running load reserves its worst case** from the node's `--bulk-load-bytes` until it ends, succeeded or failed. A load whose ids are longer than 32 bytes grows its reservation as they come, and stops there, the vectors before it loaded, if that's over the flag or other loads hold the rest.
    - **One load runs at a time per database;** another waits for it, up to the request timeout or its own deadline. A load whose client sends nothing for `--stream-idle-ms` while the load waits for it ends with `DeadlineExceeded`, the vectors before it loaded, so a stalled client can't hold the slot. `/metrics` shows the running load's phase, the vectors and bytes handed to it, and its reservation.
      - **The idle bound counts silence, not progress.** A client that sends a byte now and then is never idle, and holds the load's place as long as it keeps sending. Two loads on one HTTP/2 connection share its flow-control window: one waiting for the slot can fill the window with its unread messages, and the running one's client then can't send, so it may end at `--stream-idle-ms` with "the client sent nothing". Send each load on a connection of its own, or keep `--stream-idle-ms` at or above `--request-timeout-ms`, so the waiter gives up first (#1848).
    - The load runs on a thread of its own (`phdb-serve-bulk-load`), off the shards' cores where the engine pinned them, as the rebalancers do. Its writes still fill memtables quickly, so other writers can meet room and pacing stalls while it runs. The server's shutdown waits for a running load to write what it received.
- **The BM25 text index** (`pigeonhole-text`, ICR 0105): `Text.CreateIndex` (the source column, as a table, family and qualifier, and the index options; `if_missing` opens an existing one as it is), `Put` (writes the document's text to the source column and indexes it, in one transaction), `Delete`, `Search`, `Rebuild` and `Verify`.
  - `Search` takes the query syntax (`syntax`), or `clauses` in order. A clause's `term` (and a fuzzy clause's `term`) is matched as given, after the index's normalisation, so `March` misses an index of lowercased text. Its `text`, and a phrase's `text`, are tokenised as documents are, as the syntax's words and `"..."~N` phrases are. A repeated clause is refused, since it would count once and shift the clause indexes that `highlight`'s matches report.
  - `k` is 1 to 10,000. With `highlight`, each hit carries its text and every match's byte range, by the request's clause index. `id_prefix` keeps only ids under a prefix.
  - `Put` and `Delete` are writes, so they answer their real outcome even if the client goes away. `Search` and `Verify` run on the blocking pool. `Rebuild` rereads the whole source column, so it runs on a thread of its own (`phdb-serve-text-rebuild`, off the shards' cores), one at a time: a second is `Overloaded`. A rebuild always starts from the first row. It stops partway at a shutdown, answering `Closed` with how far it got, and resumes on its own when the server starts again (logged as it starts and ends); the documents it did stand. A client's `Text.Rebuild` while such a resume runs waits for it to stop at its next document, then starts over. A server built before 1.3 ignores a stopped rebuild's cursor: its own rebuild doesn't clear it, so the next 1.3 start resumes from it, rereading rows already done (#1756).
  - **`--text-pack` packs the text indexes in the background** (1.3, ICR 0143; off by default). Packing moves a term's postings into blocks, which makes a search of a common word several times faster ([Packing an index](text-search.md#packing-an-index)). **A packed index can't be opened by a 1.2 build, so start a server with `--text-pack` only once you won't roll back to 1.2.** Without the flag a server never packs, and no RPC does.
    - A thread (`phdb-serve-text-pack`, off the shards' cores) passes over the indexes at the start and then every minute. A pass takes only what was written since the last one, a chunk at a time, so searches and writes go on beside it.
    - It shares the rebuild's place, and ranks last for it: it stops at its next chunk for a client's `Text.Rebuild` and for a stopped rebuild resuming (on any partition of the node), and carries on afterwards. It leaves an index whose rebuild is stopped part-way until that has finished, and stops at a shutdown, carrying on from where it was at the next start.
    - **The first pack of an index waits while a reader process is attached** to the file, since the file doesn't record which release a reader runs. It is logged and counted once (`pigeonhole_serve_text_pack_refused_total`) and tried again each pass. Close the readers (1.3 readers can attach again afterwards).
    - Stopping the flag later doesn't unpack anything: the index stays packed, and new writes stay unpacked. On a packed index a delete, or an update that drops a term, leaves a small marker over the packed posting, which only a pack removes, so without the flag the index table grows with churn. Pack again now and then (`Index::pack`, or a server started with the flag) when they have built up ([Packing an index](text-search.md#packing-an-index)).
  - Dropping a text index's table drops the server's handle the same way (#1588): the next call on the name answers `TableNotFound`, and an index created again under it is the one served.
  - **A text index's source table can't be dropped while the index is there** (#1916, from 1.3.0). `Schema.DropTable` of it answers `FailedPrecondition` (409, not applied), naming every index whose source column is in it, and also while a `Text.CreateIndex` over it is still building. Drop the indexes first, then the table. Before 1.3.0 the drop succeeded and left the index behind: it could no longer write, rebuild or find its documents, and a new table of the same name became its source. A cluster refuses the drop the same way.

## Agent memory
- **Agent memory** (`pigeonhole-memory`, ICR 0107): with `--memory NAME`, the server opens (or creates) that store at startup (a replica opens it and never creates it: see [Replicas](#replicas)), with `phdb mcp`'s memory flags (`--embed-url`, `--embed-local`, `--memory-model`, `--memory-dim`, and so on), and serves it as `Memory`: `Remember`, `Recall`, `MarkUsed`, `Supersede`, `Link`, `Forget` and `Watch`. A fleet of agents then shares one store. Without `--memory`, these calls answer `Unsupported`. On a cluster, each partition holds the store's namespaces in its range ([Agent memory on a cluster](#agent-memory-on-a-cluster)). `Schema.DropTable` of the served store's own tables (`mem_NAME`, its text index's and its IVF index's: `Store::table_names`) answers `FailedPrecondition` while the server serves it (#1651).
  - **Namespaces:** a token line with `ns=a,b` (`write ns=team-a <token>`) reaches only those namespaces, on every call: the namespaces a call names, the namespace each memory id carries (`MarkUsed`, `Supersede`, `Link`, `Forget`, a link's target), and a watch's. A call that names one outside them is refused whole with `PermissionDenied`, never trimmed, and a scoped token must name its namespaces to watch. A token with `ns=` reaches nothing but the memory calls: every other call is `PermissionDenied` ([Security](#security)). Without `ns=`, a token reaches every namespace and the whole database, so a server is one trust domain: run a server or a store per tenant if tokens can't be scoped.
  - **Scopes:** `Recall` and `Watch` need read scope; the others write (`Forget` included: forgetting is ordinary agent behaviour, and `ns=` bounds its reach).
  - **What `/mcp`'s memory tools answer, in the RPCs too** (1.3.0, #1915): `Remember` takes `supersedes`, the memory the new one corrects (in its namespace). The store then skips near dedup against what it holds (a correction is near what it corrects; exact dedup stays), stores the memory, and supersedes the old one. If that last step fails, the reply is still a success: `id` is the stored memory and `supersede_error` says why, so the old memory is still live: retry with `Supersede`. `Recall` answers `truncated` when its byte budget cut hits that would have ranked, and each kept superseded hit's `superseded_by`. `MarkUsed` and `Forget` answer the ids the store doesn't hold in `missing`.
  - **Embeddings:** with `--embed-url`, the server embeds the text and refuses an embedding sent with it. Each embedding call waits at most `--embed-timeout-ms` (10 s, shorter than `phdb mcp`'s 30 s), and memory calls run at most `--memory-calls` at once (half the blocking threads by default), so a slow embedding endpoint can't take every blocking thread. An endpoint that's unreachable (refused, or no connection within half of `--embed-timeout-ms`, at most 3 s) or failing (5xx, 408, 429) answers `Unavailable` (retry-safe); one that takes longer than `--embed-timeout-ms`, `DeadlineExceeded`; one that refuses the text (400, 413, 422), `InvalidArgument`; and any other answer, or a vector of the wrong dimension, `Internal`, since that's the server's `--embed-url`, key or model. With `--embed-local` (a `phdb` built with the `embed-local` feature), the model runs in the server, on at most 4 threads placed where the network threads are, off the shards' CPUs. `--embed-timeout-ms` doesn't bound it, since it's CPU work, but `--memory-calls` still bounds how many embed at once. candle also runs large steps on a process-wide pool of its own, which only the environment sizes and places. While `CANDLE_NUM_THREADS` or `CANDLE_CPU_MASK` is unset, the server warns at startup with the `export` line to restart with, and a gauge shows it (see [Embedding placement](#embedding-placement)). Without either flag, a store pinned to a model takes the caller's embeddings: `f32`, or `f16`/`bf16` widened.
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
  - **Feed clients should turn on HTTP/2 adaptive flow control** (tonic's `Endpoint::http2_adaptive_window(true)`; grpc-go and grpc-java estimate the bandwidth-delay product unless a fixed window is set). With fixed windows, events queue in flight ahead of each subscriber. At 16 subscribers that was about 18 ms of delivery lag, which adaptive windows took to 0.5 ms p50 (measured on the reference machine, a 16-core AX102; #1397). Larger fixed windows made it worse. `--http2-send-buffer` bounds what the server queues per stream.
  - Over gRPC it's a server stream. Over HTTP it's NDJSON, or server-sent events whose `id:` is each event's token, so an SSE client's `Last-Event-ID` resumes it.
  - Every event's `token` resumes right after it, from any server process. A stream that delivers nothing for about 10 s, while its position moved past commits its `prefix` skipped, sends a `progress` event with the new token.
  - Streams on the same tables share one subscription in the server: the database wakes and reads for it once, whatever the number of subscribers.
  - **Over gRPC, each commit is encoded once for every stream with no `prefix`,** and those streams send the same bytes. A stream with a `prefix` gets its own encoding of the rows that match. The HTTP feed (NDJSON or SSE) still renders each event as JSON per subscriber, so many subscribers cost more over HTTP than over gRPC.
  - **Each stream buffers up to 256 KiB of encoded events** that its client hasn't taken yet, each event counted as at least 64 bytes (so at most 4,096 events). An event larger than 256 KiB is still sent, alone. A server holds at most `--max-streams` × 256 KiB of these buffers (64 MiB at the default 256 streams). This memory is separate from `--feed-ring-bytes` and `--inflight-bytes`. A client that takes nothing fills its buffer; after `--stream-idle-ms` more it gets the buffered events, then `DeadlineExceeded`, and resumes from the last event's token.

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
- `--bulk-load-bytes` (1 GiB): what the node's `Vectors.BulkLoad`s hold at most, outside `--inflight-bytes`: a load's sample while it clusters it, then its lists' pending batches. A load whose worst case is more is refused before it starts ([Graphs, vectors and text](#graphs-vectors-and-text) has the sizing). On a cluster, each partition's part of a load reserves the whole load's worst case, so a node serving several partitions of one index may need it raised ([Indexes on a cluster](#indexes-on-a-cluster)).
- `--max-streams` (256): feed streams at once. `--max-feed-hubs` (32): distinct table sets subscribed at once. Past either, a subscription gets `Overloaded`.
- **128 streams per connection:** one HTTP/2 connection carries at most 128 streams at once (scans, feed streams and calls alike). A client that opens more on one connection doesn't get an error: the extra streams wait until earlier ones end, and a feed stream may never end. Open another connection for each 128 concurrent streams. With tonic, that means another `Channel`, because one `Channel` is one connection.
- `--max-catch-up` (16): feed streams reading their own history at once (from a token, a snapshot or the oldest commit, or after falling behind); the rest wait their turn.
- **Range moves (cluster):**
  - `--move-rate-bytes` (64 MiB/s): the node's copy bandwidth, shared by every range move copying from its partitions. A move's catch-up afterwards isn't throttled.
  - `--move-retention-cap` (4 GiB): the most change-feed history a range move keeps on its source while it copies and catches up. Past it, the move's hold lapses (`phdb cluster status` shows it) and the move must be aborted.
  - `--max-move-followers` (2): range moves copying from a node's partitions at once. A further move's copy is refused (`Overloaded`, counted in `pigeonhole_cluster_move_copies_refused_total`) until one ends. Each move's catch-up costs its source CPU for every commit it makes while the move runs, so this cap bounds that cost: about 2 µs of the source's CPU per put per move (about +3% with 1, +6% at the default 2 and +12% with 4, at 20,000 puts/s on the reference machine), with each move's catch-up lag at 1 ms or less.
- `--feed-ring-bytes` (256 MiB): the recent commits the shared feeds keep for their streams, in all, shared out over the table sets subscribed. A commit is kept as its encoded event, which carries its resume token (about 45 + 16 × shards bytes), plus about 110 bytes; a stream with a `prefix` filters that event. With 100-byte values and 4 shards, a commit costs about 380 bytes. Each table set's share is `--feed-ring-bytes` over the sets subscribed, so with many sets, raise it to keep the seconds of commits a stream can fall behind and still catch up from the ring. This is outside `--inflight-bytes`; `/metrics` reports what's in use.
- `--backup-dir`: where `Admin.Backup` writes, and where downloads are kept for resumes. It must exist; put it on a disk with room for a copy or two of the database, not tmpfs. There's no default: without it, backups are off. `--max-backups` (1) runs that many at once.
- `--max-sessions` (1,024) and `--max-sessions-per-token` (64): transactions, pinned snapshots and committed transactions' records held at once. Past either, `Begin` and `Open` get `Overloaded`.
- `--request-timeout-ms`: the longest wait for those limits. A gRPC client's shorter `grpc-timeout` wins, and running out of it is `DeadlineExceeded`.
- A write the server has submitted always answers its real outcome.

## Threads and placement
- **Threads:** `--shards` sets the shard count (default: half the physical cores this process may run on), and `--compaction-threads` the threads dedicated to flushes and compactions (default: a quarter of the physical cores this process may run on, at least 1, at most one per shard). On Linux the network threads run on the CPUs the shards don't use; `--io-threads` and `--blocking-threads` override their counts. Sync calls run on the blocking threads, which stay off the shards' SMT siblings, except for `ReadRow` pages within the default 1 MiB budget while the threads are masked: those run on threads of their own (`phdb-serve-lite`, as many as the network threads), on the network threads' CPUs, siblings included, and a page that finds them all busy runs on the blocking threads instead (#1880). The change feed's delivery runs on threads of its own (`phdb-serve-feed`, `--feed-threads`, by default a quarter of those CPUs, at least 1, taken from the network threads' default), so a put never waits behind feed streams for a worker. They run on the free cores only, off the shards' SMT siblings ([below](#threads-and-placement)). The shared subscriptions (hubs) that read the engine's feed run on threads of their own (`phdb-serve-hub`, `--feed-hub-threads`, by default a quarter as many as `--feed-threads`, at least 1), on the same CPUs, so a hub never waits behind its streams' delivery (#1397). A hub with a single stream runs on the feed threads beside it instead (#1689). **The default keeps writes first.** Each hub thread past it costs the writes on the same CPUs: put p50 rose 13–38% at moderate fan-out load, and CPU per delivered event rose with it. When saturated, delivery wasn't better either: at 16 table sets × 16 streams, more hub threads delivered 15–21% fewer events. With 3 feed threads, 16 table sets with 2 streams each, on the reference machine, a 16-core AX102 (#1725):

  | `--feed-hub-threads` | put p50 at 30K puts/s | at 55K puts/s (saturated): puts | events delivered | lag p99 |
  |--:|--:|--:|--:|--:|
  | 1 (the default) | 145 µs | 176 µs p50 | 50% | 15.7 s |
  | 2 | 164 µs | 1.85 ms p50 | 100% | 0.27 s |
  | 3 | 175 µs | 524 ms p50 | 96% | 28 ms |

  Raise it on a feed-heavy server with CPUs to spare, whose subscribers matter more than its writes' latency. The feed dump (below) shows hubs' engine feeds falling behind the database.

  **Waiting to batch (#1901).** A hub that keeps up reads its feed once per commit, so under steady writes each hub thread can stay busy waking. `--feed-hub-coalesce-ticks N` makes a hub that just read a small batch (a few commits) wait N timer ticks (1 ms each) before reading again, so the commits arriving meanwhile are read together.
  - **The cost:** up to N to N+1 ms more lag, and only under steady writes. A commit after a quiet gap is read at once.
  - **Clients:** `--feed-client-coalesce-ticks N` does the same for each subscriber's stream.
  - **Default:** 1 tick for hubs on Linux, 0 on Windows, macOS and other systems; 0 for clients everywhere. On the reference machine, a Linux server (16 table sets × 16 streams, 2 hub threads, g114 on #1938):
    - At 20K puts/s, the hub threads went from 1.92 cores to 0.69, the feed threads from 2.7 to 1.0, and CPU per delivered event fell 42%. Feed lag p99 rose from 1.4 ms to about 3.5 ms.
    - At 2K puts/s, CPU per delivered event fell 39%, and lag p99 rose from 0.6 ms to 2.2–2.8 ms.
    - **For the lowest lag,** set `--feed-hub-coalesce-ticks 0`. At 20K puts/s, its lag p99 was 0.7 ms, at about 3% more CPU per event than before #1901.
  - **A tick lasts longer outside Linux.** The wait is a tokio timer, and how long a 1-tick wait really lasts depends on the OS timer under it. It also depends on whether anything else wakes the hub threads meanwhile, as other busy hubs do:

    | OS | hub threads otherwise idle: p50 (p99) | hub threads busy: p50 |
    |---|--:|--:|
    | Linux | 2.1 ms (2.1 ms) | 2.0 ms |
    | Windows | 13.6 ms (16 ms) | 2.0 ms |
    | macOS | 3–10 ms (up to 30 ms) | 2.0 ms |

    Measured with tokio 1.54.1 on GitHub's runners and an Apple-silicon Mac (#1938). Windows' timer ticks every 15.6 ms, and macOS coalesces timer wakes. A server with few hubs would pay about 14–16 ms of lag per wait on Windows, and 3–10 ms (up to 30 ms) on macOS, so the default doesn't wait there. **On Windows or macOS,** a server with many busy hubs can opt in with `--feed-hub-coalesce-ticks 1`.
  - **Batched notifies:** whatever these are set to, a hub tells its streams about new commits once per batch it reads, not once per commit.

  **At saturation, writes come first.** When the feed side can't deliver every commit, the writes still get the rate they ask for, and feed streams fall behind and catch up. `pigeonhole_serve_feed_fell_behind_total` and the streams' lag show it.
  - On the AX102 at 16 table sets × 16 streams and 55K puts/s, about 14M events/s didn't fit in 3 feed threads and 1 hub thread. More hub threads delivered less.
  - Size the feed side for headroom. #1737 tracks scaling it.
  - **A server that's mostly feed delivery** can raise `--feed-threads` and `--feed-hub-threads`, but it gives up write rate for it. Since #2012, a stream hands its connection a batch per ring pass, and more threads now deliver more. Same AX102 load, both arms with #2012:

    | | default (3 feed, 1 hub) | `--feed-threads 4 --feed-hub-threads 2` |
    |---|--:|--:|
    | puts/s (55K offered) | 54.7K | 49.2–49.6K |
    | put p99, from each put's due time | 38–116 ms | 8.4–9.2 s |
    | put service p99 | 0.55–0.72 ms | 1.4–1.5 ms |
    | events delivered per stream | 2.56–2.61M | 4.43–4.47M |
    | feed lag p99 | 49–52 s | 0.2–0.36 s |
    | streams that fell behind | 0 | 0 |

    With the raised counts, writes no longer get the rate they ask for. The 8–9 s is a client queue that kept growing. The default keeps writes first.
  - **A cluster subscription's members also hand off each of their hub's ring passes as one batch (#2014).** It cut CPU per put by 16% and network cycles per delivered event by 12% on the same AX102 load, served as a one-node cluster with default counts. Alone, it cost a slightly higher put service tail: p99 a median 16% higher and p99.9 14% higher, because one poll of a connection could send many batches before yielding the network worker that the puts share.
  - **Since #2027, a connection yields its network worker every 128 events at most**, as it did before batching. In the same session against the build before #2014, on the same load, the batched hand-off kept its saving: CPU per put −15%, network cycles per delivered event −11%. Its put service p99 and p99.9 were back within that build's range: p99 458–553 µs against 461–664 µs, and p99.9 1,643–1,902 µs against 1,487–2,114 µs. The single-node rows (s55) moved the same way.
  - Before #1734, the masked server did the opposite: it held writes to about 33K of the 55K puts/s and delivered every event, or switched between the two. The network threads now also run on the shards' SMT siblings (Placement, below), so the writes no longer queue behind delivery for a CPU.

  Each stream's framing still runs on its connection's network thread.

  **When to raise `--io-threads`** (#1745). The default is the free cores' CPUs less the feed threads. Since #1734 the network threads also run on the shards' SMT siblings (Placement, below), and the default doesn't count those, so on a node whose network threads are the limit, part of their mask sits idle.
  - **Raise it on a node whose feed side keeps up:** no feeds, or feeds whose delivery has headroom, meaning `pigeonhole_serve_feed_fell_behind_total` isn't rising and no `phdb-serve-hub` thread runs near a full core. Raise it to the CPUs the network threads may run on, less the feed threads: the startup line's free CPUs, plus the SMT siblings named at the end of the line, minus its feed threads.
    - That tail is there only with the mask on (`--affinity on`) and SMT. Without SMT the default already fills the mask, so there's nothing to raise.
    - On the reference machine, at 512 callers on ycsb-a and ycsb-c with no feeds:
      - on the AX102 single node, 13 network threads instead of 9 gave about 23% more ops/s (ycsb-a 345K → 425K);
      - on an 8-core node, 10 instead of 6 gave 34–37% more;
      - p99 was lower in both.

    The cost:
    - CPU per op rose 12–18% in those runs, and up to 37% on one row of an earlier single-round run, partly limited by its client;
    - a mixed read/put row's p99.9 rose 25–35%, with its p99 level or lower;
    - at 16 table sets × 16 streams and 20K puts/s, put p50 rose about 45 µs and put p99 about 12% (1.63 → 1.84 ms, two rounds each), with subscriber lag unchanged.
  - **Don't raise it on a node whose feed side saturates,** where `pigeonhole_serve_feed_fell_behind_total` rises or a hub thread runs at a full core. At 16 × 16 streams and 55K puts/s on the AX102, 13 network threads held puts to 38–48K/s, with a put p99 of seconds, in 4 of 4 runs. 11 threads fell short in 1 of 2 runs, and even their good run had a put p99 of 0.84 s against the default's 0.12–0.14 s.
    - The extra workers queued for CPU behind each other: about one core's worth of run-queue wait, against 0.04 with the default. The feed and hub threads weren't short of CPU.
    - The default keeps the writes first there. [#1737](https://github.com/CodingAnarchy/pigeonhole/issues/1737) tracks scaling the feed side.
- **Placement** (Linux, [D313](../design/decisions/phase-3.md#d313)): where the engine pinned its shard threads (the default, `Options::pin_threads`), the server's threads get an affinity mask that keeps them off the shards' CPUs: every partition's shards on a cluster node. The compaction threads' CPUs stay in the mask: on one node, leaving them out too cost the network side about three times its throughput (#1703). The startup line counts both. A client thread preempting a pinned shard thread raised `Buffered` put p99.9 (#1206). The startup line says which mask the server built, after its thread counts (the shard and compaction CPU counts follow it):
  - **`off the shards' cores`:** the network workers run on every CPU but the shards' own: the free cores and the shards' SMT siblings. The line then ends with "network threads also on the shards' N SMT siblings (CPUs)", the whole list. The feed and feed hub threads, the blocking threads (scans, `ReadRow`, `MultiGet`, text and vector calls, `/mcp`) and the background threads (below) stay on the free cores only, since a busy thread on a shard's sibling doubled the shard's p99 (#1244).
    - The siblings are there for the writes: with only the free cores, 256 feed streams' delivery filled them and the network threads waited for a CPU, so puts were held to 33K/s while the siblings sat idle (#1734).
    - The feed threads stay off the siblings. On them as well, subscriber lag p99 at 16 × 16 streams and 20K puts/s rose from 5.6 ms to 34 ms. On the free cores it was 8.3 ms, at the same put rate when saturated and with a lower put p99 (#1738).
    - Without SMT there are no siblings, and every thread gets the free cores;
  - **`unmasked (too few free cores)`:** fewer than two CPUs are left once the shards' cores and their SMT siblings are taken out: a small host, or a `--shards` near the core count. Each shard's driver thread takes a core, so at one shard per core none is left. The threads then get no mask, and the server says why on a line of its own. On the shards' SMT siblings they cost the shards more than they do left to the OS. Lower `--shards` to get the mask back;
  - **`unpinned`:** no shard is pinned, and the threads run where the OS puts them. That's macOS (no hard affinity), a host with too few free CPUs, a CPU quota narrower than the affinity set, or an OS that refused the pin. `/metrics` counts the engine's threads pinned and not (`pigeonhole_threads{pigeonhole_thread_pinned}`).
  - **`unmasked (--affinity off)`:** the shards are pinned, but the server's threads get no mask.
- **`--affinity off`** (default `on`) keeps the same thread counts but sets no mask, so the network, feed, feed hub and blocking threads run where the OS puts them. The shards stay pinned: that's `Options::pin_threads`, not this flag.
  - **Leave it on for writes:** in the measurements below, the mask raised gRPC `Mutate` throughput 25% at 21% less CPU per write, and cut `Mutate`'s p99.9 in mixed workloads by 1.5–4.5×.
  - **Deployments dominated by whole-row reads or scans should use `--affinity off`.** With the mask, `ReadRow` ran 16% slower over gRPC and 26% slower over HTTP, and HTTP `Scan` 15% slower: with few writes, the shards' cores sit idle, and the mask keeps those reads off them. Point `Get`s, and the read-only YCSB C, were level or better with the mask.
  - **Turn it off on a host shared with other work,** where the server shouldn't claim every CPU outside the shards.
- **The server's background threads** run on the free cores only, off the shards' cores and their siblings: an index's rebalancer (`phdb-serve-rebalance`), a bulk load (`phdb-serve-bulk-load`), a text rebuild (`phdb-serve-text-rebuild`), a text pack (`phdb-serve-text-pack`), a backup (`phdb-serve-backup`) and a replica's follow (`phdb-serve-follow`). So do the embedder's threads with `--embed-local` ([Agent memory](#agent-memory)).

- **The shards' poll:** a shard thread sleeps as soon as its queue is empty (`--shard-spin 0`, the default). The library's embedded default polls up to a millisecond first, which saves a wakeup per commit, but behind a network round trip the wakeup is small, and on a busy server (about 55,000 puts a second over 4 shards on the reference machine, a 16-core AX102) the poll was a quarter to half of the CPU per put. At low rates it costs little. `--shard-spin US` sets a fixed poll in microseconds (at most 10,000), and `--shard-spin adaptive` restores the library's default.

**Measured with and without the mask** ([the full table](https://github.com/CodingAnarchy/pigeonhole/issues/1397#issuecomment-6780991941)):
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

### Embedding placement
With `--embed-local`, the embedding model runs in the server, and two pools of threads do the work.
- **The embedder's own pool:** at most 4 threads, placed on the free cores as the blocking threads are, off the shards' CPUs and their SMT siblings. It runs the matrix products, the bulk of the cost.
- **candle's process-wide pool:** the model runtime (candle 0.11) runs large element-wise steps, those of long texts and batches, on threads of its own. The server can't size or place them once it's running: candle reads two environment variables once, at its first use.
  - **`CANDLE_NUM_THREADS`** sizes the pool, less one. Unset, the size comes from `RAYON_NUM_THREADS`, else on Linux from the CPU count of `CANDLE_CPU_MASK`, else from the physical cores.
  - **`CANDLE_CPU_MASK`** (Linux) places it, given as a CPU list such as `2-7,10`. Unset, its threads run anywhere, the shards' CPUs included.

**What it costs to leave them unset:**
- **Placement:** during a remember of long texts or a batch, candle's threads compete with the shards for their CPUs, which shows as commit and read latency on the shards.
- **The count:** a thread per core spins briefly on every large step.
- **On Linux:** without `CANDLE_CPU_MASK` (or `RAYON_NUM_THREADS`), candle re-reads `/proc/cpuinfo` on every matrix product, about 47 ms per call on a 16-core x86 server, five times the work itself ([huggingface/candle#4070](https://github.com/huggingface/candle/issues/4070)).

**What the server does:** while either variable is unset, `phdb serve` warns at startup and names the line to export:
- `CANDLE_NUM_THREADS` set to the embedder's own thread count;
- `CANDLE_CPU_MASK` set to the free cores' CPUs (the list the server's warning gives, not the network threads', which include the shards' SMT siblings), or the process's own when the shards aren't pinned.

The gauge `pigeonhole_serve_embed_env_unset{variable=...}` reads 1 for each variable still unset, so a dashboard can catch it. Export them and restart:

```sh
export CANDLE_NUM_THREADS=4 CANDLE_CPU_MASK=2-7,10   # the values the warning names
phdb serve ... --memory agents --embed-local
```

**Why the server doesn't set them itself:** setting them would take `unsafe` code, or running itself again with the variables added. Setting `CANDLE_NUM_THREADS=1`, which puts all of candle's work on the embedder's own placed threads, was measured and declined: it costs 25–50% of the throughput of batches and long texts (#2061).

## Metrics
- **`GET /v1/health`** answers without a token. `GET /metrics` serves Prometheus text with read scope: request counts by RPC and outcome, each RPC's durations as a histogram (service time: from the decoded request to its answer, on every path, so the wait for the request's body, an HTTP/1.1 pre-read included, isn't in it; a scan's runs to the end of its stream), open sessions by kind, the oldest open session's age, the feed's shared subscriptions, their memory, and the streams that fell behind or are catching up, and a running bulk load's phase, vectors and bytes. After those come the engine's metrics for the served database, under the names and labels the [observability guide](observability.md#the-metrics) lists (`pigeonhole_commits_total`, `pigeonhole_write_stall_time_seconds_total` and the rest, each with `pigeonhole_db_name`), with its alerts and dashboard. On Unix, `kill -USR1 <pid>` makes the server write its feed hubs to stderr (it doesn't stop the server): the hub thread count, then for each hub, how far its engine feed is behind the database, its ring against its share of `--feed-ring-bytes`, and its streams on the ring (entries behind its head), on their own catch-up streams, and waiting for a `--max-catch-up` slot. A cluster node then writes its shared feed merges: for each, its tables, prefix length, visibility and token (`token #N`, its line in the token file, never the token), its ring, and its subscribers on the ring, on their own merges and waiting for a slot. It also writes how many subscribers run their own merge, by reason.

Serve's own metrics, before the engine's:

| Metric | Type | What it is |
|---|---|---|
| `pigeonhole_serve_requests_total{rpc, outcome}` | counter | Requests by RPC (`Data.Get`) and outcome: `ok`, or the error's code (`Overloaded`, `TableNotFound`, ...). |
| `pigeonhole_serve_request_duration_seconds{rpc}` | histogram | Each RPC's service time, from the decoded request to its answer (a stream's to its end). Buckets from 100 µs to 10 s. |
| `pigeonhole_serve_shed_total` | counter | Requests refused at `--max-concurrent`, before they ran. |
| `pigeonhole_serve_replies_lost_total` | counter | Writes that finished after their client went away. The write's outcome went to the log instead. |
| `pigeonhole_serve_feed_idle_drops_total` | counter | Feed subscriptions ended at `--stream-idle-ms`, because the client left an event untaken that long. Each one also ends its `Feed.Subscribe` with `DeadlineExceeded`. A cluster subscription counts once, on the node it entered. |
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
| `pigeonhole_serve_feed_cluster_merges` | gauge | Cluster feed merges running now on this node's feed threads (`phdb-serve-feed`): one per shared merge, and one per subscriber running its own. |
| `pigeonhole_serve_feed_cluster_hub_members` | gauge | Cluster feed subscriptions on a shared merge now (#1798). |
| `pigeonhole_serve_feed_cluster_fell_behind_total` | counter | Cluster feed subscriptions that fell behind their shared merge's ring and went on alone. |
| `pigeonhole_serve_feed_cluster_rejoined_total` | counter | Cluster feed subscriptions that caught up and rejoined their shared merge. |
| `pigeonhole_serve_feed_cluster_catch_up_waits_total` | counter | Cluster feed subscriptions that fell behind and waited for a `--max-catch-up` slot. |
| `pigeonhole_serve_feed_hub_failures_total` | counter | Shared subscriptions that failed. Their streams end with the error. |
| `pigeonhole_serve_embed_env_unset` | gauge | With `--embed-local`: 1 for each of candle's variables (`variable="CANDLE_NUM_THREADS"`, `"CANDLE_CPU_MASK"`) the server started without, as its startup warning says ([Embedding placement](#embedding-placement)). The mask's is always 0 off Linux, where the mask doesn't apply. |
| `pigeonhole_serve_bulk_load_phase` | gauge | A `Vectors.BulkLoad`'s phase: 0 none, 1 receiving, 2 writing and rebalancing. |
| `pigeonhole_serve_bulk_load_vectors`, `pigeonhole_serve_bulk_load_bytes` | gauge | The vectors and message bytes handed to the running load. |
| `pigeonhole_serve_bulk_load_reserved_bytes` | gauge | What the running load reserved of `--bulk-load-bytes`: its worst case. |
| `pigeonhole_serve_text_pack_passes_total`, `pigeonhole_serve_text_pack_postings_total`, `pigeonhole_serve_text_pack_refused_total`, `pigeonhole_serve_text_pack_conflicts_total` | counter | With `--text-pack` only: the background pack's finished passes over the text indexes (a pass that left an index for later still counts), the postings it moved into blocks, the indexes it couldn't pack (a first pack with a reader process attached, or an error: counted and logged once for an index, when it starts failing, not once a pass), and the packs that stopped because writers kept changing the postings under them (the next pass carries on; one that grows every pass means an index too busy to pack). On a cluster node, per partition. |
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
| Latency | `histogram_quantile(0.99, sum by (le, rpc) (rate(pigeonhole_serve_request_duration_seconds_bucket{rpc!~"Data.Scan\|Feed.Subscribe\|Memory.Watch\|Admin.Backup\|Admin.BackupDownload\|Vectors.BulkLoad\|Text.Rebuild\|Replication.Follow"}[5m]))) > 0.05` | 10m | The server's p99 by RPC. Streams are timed to their end (a replica's `Replication.Follow` included), and the backup, bulk-load and rebuild calls run long by design, so they're left out. Pick the threshold from your own baseline. `histogram_quantile` interpolates within a bucket (bounds 100 µs, 250 µs, 500 µs, 1 ms, 2.5 ms, ... 10 s), so a p99 between two bounds is an estimate. |

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
- **Retention, and falling behind.** The primary doesn't track its replicas: its feed retention (`--change-feed`) decides how long a replica may be down or behind. Size it to the write rate times the longest replica outage you want to ride out; the primary's `/metrics` reports the feed's retained bytes. A replica that falls off the end, whose primary's URL now serves another database, or whose tables are no longer its primary's (a schema change the primary's feed didn't record, such as one made while it was rolled back to 1.1.x: the replica compares its tables' digest with the one the primary sends, or an apply is refused), bootstraps again: it downloads a new copy while serving its old one (stale, with `lag_ms` growing), then swaps it in. Requests during the swap get `Unavailable` (retry them), and transactions, pinned snapshots and feed streams open on the old copy end. A feed token from the old copy is another database's to the new one: a stream on it, or a resume from it, ends with `InvalidArgument` ("the token is another database's"), so subscribe again from now or from a snapshot.
- **Promotion.** `Admin.Promote` (admin scope) on a running replica, or `phdb promote <file>` on a stopped one's file, makes it a primary at the last commit it applied: it stops following and takes writes, and its own feed tokens stay valid. It returns the primary's token it stopped at. **Nothing fences the old primary:** stop it (or its writers) before promoting, or the two diverge. A promote before the replica's first bootstrap has finished is refused with `FailedPrecondition`, and the replica goes on bootstrapping: promote it once `Admin.Info` shows it following. A promoted database can't follow again, and `phdb serve --replica-of` refuses to start on it (or on any database with tables that isn't a replica's) rather than replace it; start a new replica from it instead.
- **Backups of a replica** (`Admin.Backup`, `phdb backup`) are ordinary copies, a good way to take backups off the primary. A copy restored and opened as a primary needs `phdb promote` first. A replica can't be another replica's source: replicas follow the primary.

## Clusters
**New in 1.2.0 (D317, ICR 0117).** A cluster splits one database across several `phdb serve` nodes by key range. Each partition is a whole database file, served by one node.
- **What it does:**
  - any node takes any call and forwards what it doesn't hold;
  - single-row reads and writes, and batches within one partition;
  - transactions and snapshots in one partition, and from 1.3.0 [transactions across partitions](#transactions-across-partitions);
  - scans, multi-gets, nearest searches and traversals across partitions;
  - change feeds across partitions;
  - schema through the cluster: tables and families live in the map, made and dropped through any node;
  - fencing, and relocation onto a node that holds the partition's files;
  - whole moves (`phdb cluster move`, `relocate --copy`), which keep the partition's change-feed history (until the node it moved to restarts, which gives it that node's share of `--change-feed`);
  - backups and restore;
  - several partitions per node;
  - from 1.3.0, [read replicas](#read-replicas-in-a-cluster) of a partition, and two things that are off until you turn them on: [automatic failover](#automatic-failover) and [the cluster balancer](#the-cluster-balancer).
- **Its limits,** detailed in the sections below:
  - a cluster node builds the IVF and full-text indexes, writes to them, and runs `Vectors.Search`, `Vectors.BulkLoad`, `Text.Search`, `Text.Rebuild` and both kinds' `Verify` across an index's partitions ([Indexes on a cluster](#indexes-on-a-cluster), #1722); agent memory and `/mcp` are served across partitions too. [How calls are routed](#how-calls-are-routed) lists every call a node refuses;
  - a table's split keys change only through a range move once its ranges hold data ([Moving and splitting ranges](#moving-and-splitting-ranges));
  - at feed saturation (256 subscribers at 55,000 puts/s), a cluster's writers no longer wait much longer than a single node's. The [Performance](#performance) table's 256-subscriber rows at 55,000 puts/s, measured at the 1.3.0 release candidate, have a one-node cluster's put p99 at 16–64 ms against a single file's 32–42 ms. Before shared merges (#1798), batched hand-off (#2012, #2020) and its yield to puts (#2031), it was 0.55–1.46 s;
  - [read replicas](#read-replicas-in-a-cluster) follow their partition's owner and serve reads that allow staleness, across partitions too. A replica is made the owner by hand (`phdb cluster promote`), or by the cluster itself once you turn failover on ([Automatic failover](#automatic-failover)). A cluster node refuses `--replica-of` and `Admin.Promote`.

### How a cluster is laid out
- **Partitions are whole database files.** A partition holds key ranges of one or more tables. Its file records which cluster and partition it is, and its *epoch* (`__phdb_cluster`, hidden from `ListTables`).
- **The map** says, in two layers:
  - which partition holds each key range of each table, given as sorted *split keys*. Range *i* is `[split i-1, split i)`, the first starting at the empty key and the last open-ended. **Once a range holds data, this layer changes only through a range move** ([Moving and splitting ranges](#moving-and-splitting-ranges)), which copies the range: a map that reshapes it otherwise is refused. So still choose split keys up front ([Choosing split keys](#choosing-split-keys-and-hot-ranges)).
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
   - **Protect the cluster token as you protect an admin token.** It reads and controls every partition: it can push maps, forward any data call, stamp files, and (from 1.3.0) fence, unfence, close, copy and drop partitions on any node. The map's writer uses it to run moves (the balancer, D332) and range moves (D331). So anyone holding it can relocate or drop partitions. Keep the file readable only by the server's user, and never give it to a client.
   - To rotate it, list the old and new tokens on every node, then switch each node's `--cluster-token-file`. A node reads its tokens at start, so each of those steps is a restart of each node.
   - `--cluster-ca PEM` names the CA certificates a node trusts for its peers' TLS, beside the system's roots. A cluster whose certificates come from a private CA needs it on every node, or node-to-node calls fail their handshake.
   - `--cluster-push-timeout-ms` (2,000) is how long a map change waits for each other node to take the new map. A node that doesn't in time pulls it later.
   - **Every node lists the same tokens** (`--token-file`). A call forwarded to another node runs as your token there, with the scope and namespaces that node lists for it, and sessions and feeds are bound to it.
5. **Change the map** on a running cluster with `phdb cluster set-map MAP.json --node URL`, a compare-and-set against the current version. Raise `map_version` in the file.
   - **`OutcomeUnknown` from `set-map` (1.3.0, D330):** the writer installed your map, but its writer lease ran out before it pushed it. The map may still reach the cluster when the writer renews its lease, or be replaced by an elected one. Read the map (`phdb cluster show-map`) before setting it again: the same file a second time is refused with "isn't newer" if it did land.
   - **The map's writer** is the node with the smallest id in the map (from the first election or forced map on, the node the map names as `writer`), and it alone changes the map, so two changes can't make two different maps of one version (D317 amendment 1). `set-map` reads the map from `--node` and sends yours to the writer. A `SetMap` sent straight to another node answers `WrongPartition`, naming the writer.
   - **Change the nodes and move partitions in separate maps.** A map that adds or removes nodes is refused if it also gives a partition to another node or a higher epoch (`FailedPrecondition`, naming the partition): a partition's owner is confirmed by a majority of one set of nodes (D317 rule 3). Set the node change, then the move. With `--force`, keep them apart too.
   - **A node change must reach most of the old nodes and most of the new ones before the map changes again.** `set-map` waits a few push timeouts for that. Until it's so, every other `set-map`, `move`, `relocate` and schema change is refused (`FailedPrecondition`), naming how many of the nodes before and after hold it and which don't: bring them up, or set a map with `--force`.
   - **`--change-feed` is the node's total from 1.3.0** (#1786). In 1.2, each partition kept the whole `--change-feed`, so a node with P partitions kept P times it, and wrote that much at start with preallocation. From 1.3.0, the node splits it across its partitions by their shards, as `--engine-memory` is split.
     - **On the first 1.3 start, each partition keeps about its shard share of the history it kept under 1.2:** 1/P when the partitions have equal shards. The oldest history goes at that open. A subscriber, or a feed token, whose position is further back than the history now kept gets `FeedExpired` once: subscribe again, from now or from the oldest history kept. The start logs each partition whose history shrinks, with its retention before and after.
     - **To keep as much history as 1.2 did,** start with `--change-feed` multiplied by the node's shards ÷ the smallest partition's shards (× P when the partitions have equal shards). Size it before the upgrade, from the write rate and how far behind your subscribers may fall.
     - **A single node is unchanged:** its total is its one database's.
     - **A partition opened while the node serves** takes its share of the total. One moved there with its history (`move`, `relocate --copy`) keeps at least the history it had until the node restarts. One restored alone (`restore --partition`) starts a new history and takes its share. The node can hold more than `--change-feed` until a restart re-splits it, and the open logs that.
   - **Finish an upgrade to 1.3.0 before creating an index on a cluster** ([Indexes on a cluster](#indexes-on-a-cluster)). A 1.2 node drops `indexes` from the map it stores, and would then read as divergent. So the writer adds the first index placement only once every node answers `Status` with `indexes` in `map_features`, and otherwise refuses with `FailedPrecondition`, naming the nodes that don't. Don't roll a node back to 1.2 once the map has one: a forced map from a 1.3 node (`set-map --force`) recovers it.
   - **Finish an upgrade to 1.3.0 before placing a memory store** ([Agent memory on a cluster](#agent-memory-on-a-cluster)). A 1.2 node drops `memory` from the map it stores, and would then read as divergent. So the writer adds it only once every node answers `Status` with `memory` in `map_features`, and otherwise refuses with `FailedPrecondition`, naming the nodes that don't (an older build, or no answer). Don't roll a node back to 1.2 once the map has one: a forced map from a 1.3 node (`set-map --force`) recovers it.
   - **Leases during the upgrade** (D326, #2047): upgrade one node at a time, in any order. A 1.3 node counts each 1.2 node holding the map as granting its leases, so partitions keep serving throughout, under 1.2's guarantee until every node runs 1.3 ([Fencing and relocation](#fencing-and-relocation)).
   - **Finish an upgrade to 1.3.0 before changing the nodes.** A 1.2 node stores the map without the nodes before the change (`previous_nodes`), so the writer's next push of that version reads as a different map. Each side then marks the other divergent, that node never counts as holding the change, and partitions it would confirm stay fenced. A forced map (`set-map --force`) recovers it, since it clears `previous_nodes`.
   - **Replica copies before their owner** (D328, D331): a range move's cleanup or abort drops a range outright, and that drop reaches a copy as an event a copy built before 1.3.0 doesn't know. Its follower halts there, and the copy stops being fresh. Range moves need every node on 1.3.0 anyway (the map's `range_moves` feature), so finish the upgrade before moving ranges.
   - The writer validates the map, keeps it, and pushes it to the others.
   - A node that misses the push pulls the map when it next hears from a node that has it, or at its own start.
   - A map may add tables, partitions and nodes, and give a partition another node with a higher epoch. A table's split keys and partitions are fixed once set.
   - A partition the map gives a running node opens there when the node takes the map, and serves once a majority of the nodes hold that map ([Fencing and relocation](#fencing-and-relocation)). Until it has opened, the node answers calls for it with `PartitionUnavailable` ("isn't open here").
   - **When the writer is down,** no map change and no schema call can be made. With the map's `failover` on and a majority of the map's nodes up, another node is elected writer, about half a minute after the old one's last renewal with the defaults ([Automatic failover](#automatic-failover)), and nothing here is needed. Otherwise `phdb cluster set-map MAP.json --node URL --force` sets a map at another node:
     - Its version goes above every node `phdb cluster` can reach.
     - The map must name a writer that's up: remove the down node from it, or let another id be the smallest. Give the down node's partitions to other nodes with a higher epoch.
     - When the old writer comes back, it takes the forced map from the others before it changes anything, and follows it.
     - A partition whose epoch the forced map raises starts a new history, as a promotion does: its change-feed tokens from before answer `FeedExpired`.
     - No partition's epoch may go back, or stay the same with another node, against the receiving node's map or any node's that `phdb cluster` reaches. Give each moved partition an epoch above every node's. The report lists the nodes it couldn't reach as `unchecked`; read their map files (`phdb cluster show-map --file`) if they may be newer.
     - **Leave the memory placement as it is** ([Agent memory on a cluster](#agent-memory-on-a-cluster)). A forced map isn't held to the rule that fixes it, but no store moves with it. Each partition's store stays in its file as it was: a namespace a forced map places elsewhere is then routed to a partition whose store doesn't have its memories (one is created empty there), and its old memories stay unread where they were. A forced map that drops the placement makes every memory call answer `Unsupported`, and the stores stay in their files.
     - **Check `phdb cluster status` before forcing a range back onto a partition it moved from** ([Moving and splitting ranges](#moving-and-splitting-ranges)). A forced map isn't held to the rule that fixes the split keys. While the move's rows are kept, the old source answers the range `PartitionUnavailable` rather than serve its old copy. Once they're pruned, it serves the range as empty: its copy was dropped at the move's CLEANING, and the rows are on the move's target.
     - **Only for a writer that's down.** A writer that's up but cut off from you can still change the map, and a forced map then loses that change. Check that the writer's process is stopped first.
6. **Check it:** `phdb cluster status --node URL` shows every node's map version side by side, so a push that hasn't reached a node shows, and how many of each node's partitions are open with the identity the map expects. A row per partition gives its state on its node: `opening` (a map just gave it here, and its calls answer `PartitionUnavailable` until it's open), `serving`, `fenced (operator)`, `fenced (awaiting peer map)`, `fenced (operator, awaiting peer map)` (both: a partition you fenced that has no lease since it opened), `fenced (lease lapsed)` (its renewals stopped reaching a majority) or `closed`. A serving partition's `lease_remaining_ms` is the time left on its lease, and a node's `grace_remaining_ms` its start grace (D326). `phdb cluster show-map (--node URL | --file DIR/cluster-map.json)` prints a map.
7. **Give `phdb cluster` its token.** Every `phdb cluster` command that talks to a node takes these beside `--node`:
   - `--token-file FILE`: a file holding the bearer token alone. Without the flag, the `PHDB_TOKEN` environment variable. A token is never a flag's value. The same token goes to every node a command talks to (`move`, `backup`, `restore` and `status` reach each node at the map's address), so list it on every node.
   - `--ca PEM`: CA certificates to trust beside the system's roots, for nodes whose certificates come from a private CA.
   - **Scopes:** `status`, `show-map` and `balancer status` need `read`. Every command that changes something needs `admin`: `set-map`, `fence`, `unfence`, `close`, `drop-partition`, `relocate`, `move` (and `move --abort`), `promote`, `txn decide`, `balancer pause`, `resume`, `pin` and `unpin`, `backup` and `restore`.
   - A cluster on addresses other hosts reach requires TLS and tokens ([Security](#security)), so there every command needs a token: `phdb cluster status --node https://10.0.0.1:7448 --token-file admin.token`. The examples in this section leave the two flags out.
   - `init-partition` and `show-map --file` work on local files and take neither.

**Flags:** a cluster node takes `--shard-spin`, `--no-wal-preallocate`, `--shm-dir`, `--change-feed`, `--backup-dir` and `--max-backups` as a single node does, for each of its partitions. Each partition writes its logs in full when it opens, including a partition the map attaches while the node serves. `--shards`, `--compaction-threads`, `--engine-memory` and `--change-feed` are the node's totals, split across its partitions ([Partitions per node](#partitions-per-node)); `--change-feed` by shards, from 1.3.0 (#1786). `--max-uploads` (1) caps the partition files it takes in at once (`Admin.PartitionUpload`), for a restore or a move by file copy. An upload is staged in `DIR/.phdb-incoming` until `phdb cluster relocate` or `phdb cluster restore` installs it. An upload doesn't check for free space first: a full disk fails it, and it resumes once there's room. `--memory` and its flags make each partition the map places the store on open a store of its own ([Agent memory on a cluster](#agent-memory-on-a-cluster)). A cluster node refuses `--replica-of` (a partition's replicas are listed in the map: [Read replicas in a cluster](#read-replicas-in-a-cluster)) and `--create` (a partition's file is made with `phdb cluster init-partition`).

**Metrics:** a cluster node's `/metrics` and `Admin.Metrics` label each partition's series `pigeonhole_partition="<id>"`: the engine's, and the feed fan-out's and bulk loads'. The node's own (connections, requests, sessions) carry no label. Each partition also has its unpinned engine threads (`pigeonhole_serve_partition_unpinned_shard_threads`, `…_unpinned_compaction_threads`) and its engine memory (`pigeonhole_serve_partition_memtable_bytes`, `…_block_cache_bytes`, `…_row_cache_bytes`).

### How calls are routed
- **Any node takes any call.** It serves the call if it holds the row's partition, and otherwise forwards it to the node that does. A client needs no map. One that wants to skip the hop can read the map (`Cluster.GetMap`) and call the owner directly.
- **Served by row:** `Get`, `ReadRow`, `Mutate`, `CheckAndMutate`, `MutateReturning` and counters. Also a `Batch` whose rows are all in one partition, which commits atomically as on a single node.
- **A batch across partitions** is refused with `CrossPartition` before anything is written. The error names each partition and its first row, so you can split the batch.
- **Where a node's map is older:** `WrongPartition` is handled between nodes. A node with an older map pulls the newer one and forwards again. A fanned-out read's part whose partition a newer map dropped since the read began answers `WrongPartition` too: read the map again and retry.
- **Refused with `Unsupported`** on a cluster node in this release:
  - `Admin.Promote`. `Admin.Info`, `Flush` and `Compact` cover the node's own partitions ([Admin](#admin)), and `Admin.Metrics`, `Backup` and `BackupDownload` work ([Backup and restore](#backup-and-restore));
  - `/mcp` on a node that only routes ([Remote MCP](#remote-mcp)).
- **A node holds every partition its map gives it,** opened at start in id order after checking each file's identity row against the map. One that fails to open stops the node from starting. A node the map gives no partition only routes.
- **Errors a cluster adds:**
  - `PartitionUnavailable` (503): the row's node can't be reached and nothing was sent to it. Safe to retry, writes included.
  - A forwarded write whose answer was lost after it was sent answers `OutcomeUnknown`, as on a single node. Check before you retry.
  - `CrossPartition` (409): the request can't succeed as written. Split it.
  - `Fenced` (409): the write's partition is fenced, and nothing was written. Send it again once the partition is unfenced or has moved ([Fencing and relocation](#fencing-and-relocation)).

### Agent memory on a cluster
**New in 1.3.0 (#1915, ICR 0139).** Each partition the map places the memory store on holds a whole store of its own (its memories, its keyword index and its IVF index) for the namespaces in its range. A namespace lives in one partition.
- **Place it in the map:** `"memory": {"store": "notes", "splits": [{"text": "m"}], "partitions": ["p1", "p2"]}` puts namespaces below `m` on p1 and the rest on p2. Split keys are namespaces, and none may contain `\0` (no namespace does). The placement is fixed once set: range moves move a table's ranges, not a memory store's.
- **Start every node with `--memory notes`** and the same memory flags. Each partition the map places the store on opens it, created with the `--memory-*` options if missing, and checked against the node's model pin, `--memory-dim` and `--memory-normalization`. A partition whose store doesn't match answers its memory calls with `FailedPrecondition`, and `phdb cluster status` lists why among its reconcile errors; the node's other partitions serve. So does a partition the map places the store on whose node was started without `--memory`, naming the flag. A node started with `--memory` while the map places no store opens one when a map places it. A map that places another store refuses the start.
- **Use one embedder backend on every node** (all `--embed-url` with the same endpoint and model, or all `--embed-local` with the same model). The model pin checks the model's name, dimension and normalisation, not the numbers each backend computes.
- **Routing:** any node takes a memory call and forwards it to the partition holding its namespace. `Remember` goes by its namespace, `Link` and `Supersede` by their first memory's, and `MarkUsed`, `Forget` and `Recall` by every id's or namespace's. The owner checks the token's scope and `ns=` namespaces again.
- **A recall across partitions** (its namespaces on more than one) runs in steps from the node you called:
  - each partition returns its candidates, which that node ranks together;
  - a graph expansion (`expand_hops`) then walks one hop at a time;
  - the partitions holding the best hits return them.

  It returns what one store holding every namespace would, except for the keyword signal: each partition's BM25 statistics are its own. That holds only while nothing writes to those namespaces, since each step reads each partition at a snapshot of its own. It costs two round trips, plus one per hop of expansion. It names at most 64 namespaces, and any partition that can't be reached fails it (`PartitionUnavailable`). The node you called checks the byte budget against its own `--memory-*` limits and embeds the text once for every partition, so it needs `--memory` with the same memory flags as the others: without it, a recall across partitions answers `Unsupported`, while a recall whose namespaces are on one partition is forwarded and served there.
- **`MarkUsed` and `Forget` across partitions** are split, one call per partition, sent together, and their counts summed. If a part fails, the error names the partitions that applied, with their counts. If a part may have applied, the error is `OutcomeUnknown`. Retrying the whole call is safe either way: `Forget` skips what's gone, and `MarkUsed`'s counts are advisory.
- **`Memory.Watch` across partitions** watches each partition holding its namespaces (every partition for a watch of every namespace) and merges their events on the node you called, which needs no `--memory` for it.
  - **Order:** each partition's events come in its commit order, so each namespace's do; there's no order across partitions. A commit's memories come together, never with another partition's between them.
  - **Tokens:** every event carries a token for the whole watch, which resumes it through any node, as on a single node: resume once that event's commit is handled. A token resumes only the watch it came from (the same namespaces), on the same cluster; a single node's token, or another watch's, answers `InvalidArgument`.
  - **Leases:** each partition's events are delivered only while its node holds the partition's lease, as a cluster subscription's are ([When a partition's node is down](#when-a-partitions-node-is-down)). A partition with no lease yet holds the watch back, and if it gets none within the request timeout, the watch doesn't start (`PartitionUnavailable`, naming it); one whose lease lapses, or that closes or moves, ends it (`PartitionUnavailable`, or `WrongPartition` for a move), as does any partition that can't be reached. Watch again from the last token.
  - **`--max-streams`:** a watch holds one place on the node you called, and one on each partition's node, as a cluster subscription does.
  - **Scope:** a token limited to some namespaces (`ns=`) names its namespaces, as on a single node; the node you called and each partition's node both check them.
- **`/mcp`'s memory tools** work through any node that holds a partition and was started with `--memory` ([Remote MCP](#remote-mcp)). They rely on the fields above (`supersedes`, `supersede_error`, `truncated`, `superseded_by`, `missing`), which a node built before 1.3.0 doesn't serve: it would ignore `supersedes` and leave the rest empty, a wrong answer rather than a short one. So during a rolling upgrade, a `/mcp` memory call, and a `Remember` with `supersedes` from any client, first asks each node owning a partition it reaches for the `memory_tool_fields` feature (cached for 30 s a node). A node that answers without it refuses the call with `FailedPrecondition` naming it: finish the upgrade. One that doesn't answer is unavailable (`PartitionUnavailable`), as for any call to it. Plain RPCs that read the other fields aren't refused: until every node runs 1.3.0, an empty `truncated`, `superseded_by` or `missing` from a node's part doesn't mean none.
- **In the background:** each partition's forget policy and IVF rebalancing run on the node serving it, under the partition's fence: a fenced partition runs neither.

### Indexes on a cluster
An IVF or BM25 index on a cluster (#1722, ICR 0138) is an **index placement** in the map. Each partition it's placed on holds a whole index of its own over the ids in its range.
- **Where its ids are:**
  - **A text index follows its source table.** A document's id is its source row's key, so its postings are always in that row's partition.
  - **A vector index either follows a table** (`follows`: its ids are that table's row keys, the usual case of embeddings beside rows) **or has ranges of its own** (`placement`: split keys and partitions, as a table's). A create on a cluster gives exactly one of them; a single node refuses both (`InvalidArgument`).
- **`Vectors.CreateIndex` and `Text.CreateIndex`** work through any node, as `Schema.CreateTable` does: the map's writer adds the placement, and each node creates the index on its partitions when it takes the map. `if_missing` answers an existing index as it is. The placement is fixed once written. A text index's `source_family` must be one of the followed table's families (`InvalidArgument` otherwise), and `Schema.CreateTable` of an index's name answers `TableExists`.
- **Right after a create,** a write can answer `TableNotFound` from a partition that hasn't taken the new map yet. Nothing was written, so retry until every partition has it.
- **Listing:** `Schema.ListTables` and `DescribeTable` on a cluster show the map's tables, not its indexes. `Cluster.GetMap` lists the indexes (`indexes`).
- **A text index fills in the background on a cluster:** the create returns once the map has it, and each partition indexes its source rows a document at a time, as a resumed `Text.Rebuild` does (stopping at a fence, carrying on after). Writes through the index are indexed at once.
- **`--text-pack` is per node:** a node started with it packs the text indexes of the partitions it owns, each under that partition's lease (a fence stops it at its next chunk, and it carries on after). A replica copy never packs; it receives the blocks through replication, so its node must run 1.3 too. To pack a whole cluster's indexes, start every node with the flag.
- **Writes route by id:** `Vectors.Insert` and `Delete`, and `Text.Put` and `Delete`, go to the partition holding the id, through any node.
- **Dropping:** `Schema.DropTable` of the index's name drops it on every partition. A table an index follows can't be dropped while the index is there (`FailedPrecondition`, naming the indexes), as on a single node (#1916).
- **`Vectors.Search`** goes through any node to each partition whose range meets `id_prefix` (every partition of the index without one), and the node you called keeps the first `k` by distance, then id.
  - **`nprobe` and `max_probe` are per partition.** Each partition's index has its own lists, trained on its own vectors, and each probes `nprobe` of them, so a search over P partitions probes up to P × `nprobe` lists. Recall and cost both rise with P. Splitting one `nprobe` across the partitions loses recall instead: on the reference machine, a single index at `nprobe` 32 recalled 0.969 at 9.8 ms, and 8 partitions at `nprobe` 8 recalled 0.967 at about 1.6 ms with 8.7 ms of CPU in all, while 8 partitions at `nprobe` 1 recalled 0.53 (#1722's g108).
  - **With every list probed,** the search is exact over every row, as on a single node.
  - **Ties at the `k`th distance** are each partition's choice, so across partitions they're ordered by id only among the hits returned.
  - **`stats`** sums the partitions' `lists_probed` and `lists`.
  - **A partition that can't be reached** fails the search with `PartitionUnavailable`, and the error names it. There's no `allow_partial` for it yet. A search whose `id_prefix` lies in reachable partitions doesn't need the others.
- **`Vectors.Verify` and `Text.Verify`** check every partition's index and sum the counts. A partition whose check fails, or that can't be reached, fails the call, and the error names it. The sums are the partitions' counts added up, so a term found in two partitions counts twice in `terms`.
- **`Text.Search`** goes through any node and scores as one index holding every partition's documents would: the same hits and scores, by BM25 over the whole index's statistics. It runs in two rounds.
  - **Round 1:** every partition of the index sends its statistics for the query (its document count and length, each term's document frequency, each fuzzy term's expansions), read at one snapshot. Every partition is asked, whatever `id_prefix` says, since a search's statistics cover the whole index, as on a single node. The node you called adds them up.
  - **Round 2:** each partition `id_prefix` meets (every one without a prefix) ranks its documents with those statistics, and the node you called keeps the first `k` by score, then id.
  - **An index on one partition** answers in one round: its own statistics are the whole index's.
  - **The cost** is one more round trip to every partition of the index. On the reference machine, round 1 took 9–23 µs of compute for terms and phrases, 0.24–0.41 ms for `~1` fuzzy terms and 2.3–3.8 ms for `~2`, plus 0.14–0.37 ms for the round trip at 2–8 partitions (#1722's g108).
  - **Skewed data:** a search's latency follows the partition with the most matching documents, so split a large tenant's range rather than adding partitions.
  - **The rounds run at different times.** A document written between them is ranked with the statistics round 1 read, so it scores as if those statistics had included it; a term only it holds scores as one no document holds. Every partition scores with the same numbers, so the ranking stays consistent with itself. A single node reads both at one snapshot.
  - **Fuzzy terms after a cut:** each partition keeps a fuzzy term's `max_expansions` best expansions (50), and the node you called keeps the best of their union. When any partition cut, the expansions can differ from one index's: a term every partition left out can be the one a single index would keep first. `cut_clauses` names the clauses whose fuzzy terms were cut, here or on a single node.
  - **A partition that can't answer,** because its node can't be reached or its index is still filling, fails the search with `PartitionUnavailable` or `Unavailable`, naming it. That holds even when `id_prefix` is in other partitions: the search needs every partition's statistics.
  - **With `allow_partial: true`,** the search goes on without it and sets `partial`. Missing from round 1, it scores with the statistics of the partitions that answered, so the scores are not the whole index's. Missing from round 2 (or from round 1, when `id_prefix` meets it), its ranges are named in `missing`, and its hits are left out.
- **`Text.Rebuild`** goes through any node to every partition of the index. Each partition rebuilds its own index as a single node does, and the counts are summed.
  - **One at a time per node:** a node rebuilds one text index at a time, so the partitions one node serves rebuild in turn, and different nodes' at once.
  - **A partition that fails or can't be reached** fails the call, naming it. The partitions rebuilt by then stand, and a rebuild already running on another node goes on. Rebuilding again is safe.
  - **A rebuild that outlives the deadline keeps running.** A part that runs past the request's deadline (`--request-timeout-ms`), on the node you called or another, answers `OutcomeUnknown`, naming its partition and any of that node's partitions that hadn't started yet, and its rebuild goes on. The forward doesn't wait longer: an unbounded one would hold the client and a connection for as long as the rebuild takes.
    - **To tell when it's done:** call `Text.Rebuild` again. While that node is still rebuilding, its partition answers `Overloaded` ("already rebuilding"), and the call retries it after the `RetryInfo` delay. Once that node is free, the call rebuilds again and answers the counts. `Text.Verify` shows the progress without rebuilding: its `source_mismatch` falls to 0 once every source row is indexed as it stands.
    - **A retry is safe:** a rebuild reads the source and rewrites the index to match it, so running it again changes nothing that's already right. It does reread the whole source.
  - **Searches go on during a rebuild,** strict ones included: the index stays whole while it's rebuilt. Only a new index's background fill answers `Unavailable`.
- **`Vectors.BulkLoad`** goes through any node, which splits the stream by id: each vector goes to the partition its id's range is in.
  - **Each partition loads its own part** as a single node does, into its own empty index. A partition none of the ids fall in isn't touched, and its index stays empty.
  - **`expected` applies to every partition:** a partition's share isn't known until the load has been read, and each partition's load stops at about twice its `expected`. So every partition takes the whole load's value.
    - Each partition's lists are sized for the whole load, so each list holds less than on a single node, until later writes split or merge them.
    - Each partition's load reserves the whole load's worst case from its node's `--bulk-load-bytes`, so a node needs room for that once for every partition of the index it serves, whether or not the load's ids reach them all. A node that hasn't room refuses the load at once with `InvalidArgument`, saying how much it needs: the node you called before it reads anything, another node as its first part opens (the vectors other partitions took by then stay).
  - **One slow partition slows the whole load,** as a slow loader does on a single node: a message's vectors all go out before the next is read. A partition left waiting meanwhile is kept alive within `--stream-idle-ms`, so give the flag the same value on every node.
  - **A partition that fails or can't be reached** fails the load, naming it. The other partitions stop with the vectors they took, and the error comes once they have all stopped, as a single node's comes once its load has, so a load sent right after finds their places free. When any partition may have taken vectors, the error says the index may be loaded in part (`retry_safe: false`): `Insert` the rest, or drop the index and load again.
  - **Every node serving a partition of the index must run 1.3.0 or later.** A load with one that doesn't is refused with `Unsupported`, naming it, before anything is read.

### Schema through the cluster
Tables and their families live in the cluster map ([D317](../design/decisions/phase-3.md#d317)). `Schema.CreateTable`, `DropTable`, `ListTables`, `DescribeTable` and `GET /v1/tables` work through any node, with a single node's scopes: `write` to create (`if_missing` included), `admin` to drop, `read` to list and describe. Every node makes its partitions' files follow the map.
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

### Transactions across partitions
**New in 1.3.0 (D329, ICR 0140).** A transaction begun with `BeginRequest.spans_partitions` reads and writes rows of any partition, and commits them atomically.
- **Its home** is the partition `route` names. Each other partition it touches is a branch on that partition's owner. The commit is a two-phase commit, and its commit point is a decision row on the home.
- **A commit that writes** runs at `GroupSync` or stronger. `TxnCommitResponse.parts` lists each partition's share, the home first.
- **A read-only one** writes nothing: each partition holds its reads until a release round confirms every hold lasted, or answers `Conflict`.
- **Every node** must run a build that keeps the `spanning_txns` map feature, and `--txn-max-branches` (16) bounds the branches of one transaction. Without the opt-in, a row of another partition is still `CrossPartition`.
- **In doubt.** If the home fails during a commit, a branch may hold its share prepared, its outcome unknown. It's still readable as before the commit. A write to a row the share wrote or read doesn't wait for the outcome: it's refused at once, a single write with `Busy` and a transaction's commit or prepare with `Conflict`. Both are retry-safe, and they go on being refused until the share is decided.
  - Each node's resolver asks the home once a share has been in doubt for `--txn-resolve-delay` (default: the request timeout), and decides it by the answer.
  - It asks up to eight homes at once, each home's shares in turn. A home that gives no answer (its node down or too slow) is asked once a pass, and its other shares wait for the next one, so one dead home doesn't hold up the others or the collection of decided records.
  - A branch that applies a commit, by its resolver or by `txn decide --commit`, then tells the home, so the home's record can be collected after `--txn-outcome-retention`. The branch's node keeps that ack in memory and retries it until the home takes it. If that node restarts first, or the home's own record of a branch that answered its decide fails, the ack is lost. So once a committed record is past `--txn-outcome-retention`, the home's sweep sends each branch that never acked the commit's decide again: a branch that answers holds nothing of it in doubt (a share still in doubt is applied then), so its ack is recorded and the record is collected. A branch whose node doesn't answer keeps the record until it does (#2098).
  - With no decision recorded and the home's history unchanged, the home records an abort first, so a late commit can't follow.
  - Across a change of the home's history (a restore, a relocate without `--copy`, a promotion), the answer is unknown, and the share stays in doubt with that reason.
  - `phdb cluster status` shows how many shares each partition holds in doubt and the oldest's age; with `--format json` it lists each one's xid, age and why it's waiting.
  - `--txn-max-in-doubt` and `--txn-max-in-doubt-wal-bytes` (16 MiB) bound what a partition holds in doubt.
- **`Transactions.Outcome`** (write scope) answers what became of a transaction, through any node: in progress, committed (with its parts), aborted, unknown (its home's history changed), or not found once its record has gone.
  - **A commit that wrote** is answered from its decision row on the home. The row is kept for `--txn-outcome-retention` (24 h) from the time of the decision, and a commit's row also stays until every branch has acknowledged it.
  - **A read-only one** has no decision row. It's answered committed only while its session's record lasts: one `--txn-idle-ms` after its `Commit`, on the home's node, and not across that node's restart. After that, the home answers aborted: the first ask records it, for a retention, and the answer is not found once that record is collected. Either way it wrote nothing. Rely on `Commit`'s own answer, not on `Outcome`, for whether its reads held.
  - **Not found, past retention.** From a full `--txn-outcome-retention` after a transaction began, a missing decision row may be one that was collected, so the home answers not found rather than presume an abort.
  - **Only its own client's** (#2077). While a transaction's session or its record lasts, another token's `Outcome` on its id is not found, and nothing is read or recorded for it. After that, the decision row doesn't say whose transaction it was, so the decided outcome is answered to any write-scope token that has the id: the 80 random bits in its id are the guard.
- **Deciding by hand.** `phdb cluster txn decide <xid> --partition P --commit|--abort` decides a share in doubt.
  - It's refused while the home can settle the share (it answers committed, aborted or in progress), since the resolver does that then.
  - It goes ahead without `--force` when the home can't answer, answers unknown (its history changed), or answers not found. That last case is a share still in doubt a full `--txn-outcome-retention` after its transaction began: the resolver can't settle it, so decide it by hand. Check the transaction's other partitions, or an operation id it wrote ([recovery](recovery.md)), for which way it went.
  - `--force` decides anyway: use it only for a home that will never settle it, since it can break the transaction's atomicity.
- **The change feed:** each partition's commit of a spanning transaction carries the transaction's id (`FeedEvent.xid`), the same on every share, so a subscriber can group them. Other commits carry none.
- **Upgrading:** a replica copy applies a spanning commit's share with its id. A build before 1.3.0 refuses that event rather than apply it without the id. Every node keeps `spanning_txns` before the first spanning transaction, so a cluster's own copies are on a build that reads it.

### Reads across partitions
Scans and multi-gets read across the partitions, in key order, and return what one database holding all of them would. So do nearest searches and traversals.
- **Scans:** each page holds one partition's rows, and each partition reads at its own snapshot, so rows in different partitions aren't from one point in time. A row is never torn.
- **Multi-gets:** a page is exactly what a single node would return for the same keys and `page_bytes`.
- **Memory:** a read across partitions counts its partitions' pages against `--inflight-bytes` on the node you called (its entry node), from before its first partition is read until its reply is written. The nodes that hold its other partitions serve their parts with no budget of their own, so reads through every node can't wait on each other. Those parts count once, at their entry node. A node serves at most 16 forwarded calls at once, so its memory for other nodes' parts is bounded by 16 pages (8 MiB each at most), not by its own `--inflight-bytes`. When the budget is full it waits for room, and answers `Overloaded` past its deadline, as a single node's reply does. The budget is about `page_bytes` for a multi-get or a traversal, and twice that for a nearest search, with more taken only when it's free (to read the next partition ahead, or several partitions at once). A traversal reads each hop's edges in pages of its own `page_bytes`, so a hub with more edges than that takes more round trips. A traversal whose visited nodes leave less of the budget than one of its pages ends there, with `cut_by_budget` set.
- **Resume** from a page's `next` through any node. A cluster's positions are its own: one from a single node, or one sent to a single node, is refused with `InvalidArgument`.
- **A partition that can't be reached,** or whose node takes the connection and never answers, fails the read with `PartitionUnavailable` (by its deadline, in the second case). With `allow_partial: true`, the read skips it and names its key range in `missing`: a scan on its next page (or a last, empty one), a multi-get on its reply.
- **At a snapshot** (`Selection.snapshot_id`), a scan or multi-get reads the snapshot's partition only. Its range or keys must stay within that partition, otherwise `CrossPartition`. A multi-get at a snapshot pages as its partition's node does.
- **`Nearest` and `Traverse`** return a single node's answer ([D317](../design/decisions/phase-3.md#d317)):
  - **`Nearest`:** the exact top `k` over every partition its source meets. Up to `--fanout-forwards` (8) partitions answer at once.
  - **`Traverse`:** each hop's edges are read from every partition its frontier reaches, then ranked by the same rules a single node uses. So hops, `parents` and `stats` are the same.
  - **Snapshots:** a traversal reads each hop at its own time, and each partition at its own snapshot, rather than at one snapshot for the whole traversal.
  - **Latency:** each hop is a round trip to the partitions' nodes, so a deep traversal of a graph spread across nodes costs hops × that round trip. Keep a graph's tables in one partition where you can.
  - **With `allow_partial`:** a `Nearest` result is exact over the partitions reached. A traversal skips an unreachable partition's frontier nodes, so it doesn't follow their edges, and the later hops and the ranking are exact only over what was reached. `missing` names what was skipped.
- **Index searches:** `Vectors.Search` fans out across an index's partitions, with `nprobe` per partition ([Indexes on a cluster](#indexes-on-a-cluster)). `Text.Search` scores as one index would, over every partition's statistics.

### Feeds
`Feed.Subscribe` works the same through any node. The node you call follows every partition that holds the subscription's tables, opening a single-node subscription on each partition's node, and merges them into one stream. With a `prefix`, only the partitions whose key ranges can hold such rows are followed.

#### Setup
- Turn the feed on in every partition with `--change-feed BYTES` on its node: the node's total, split across its partitions by their shards (#1786). A partition whose feed was on keeps it without the flag.
- Every node lists the same tokens (`--token-file`), as [Running a cluster](#running-a-cluster) says. A subscription's part on another node is opened for your token there, and a node that doesn't list it refuses the part with `PermissionDenied`.

#### What arrives
- **Order:** each partition's commits arrive in its commit order, so each row's do. There's no order across partitions: a commit on one partition can arrive after a later one on another.
- **`partition`** on each event names where it came from. A commit's `seqno` is its partition's own, comparable only with seqnos of the same partition.
- **`partition_state`** events report a partition that went away or came back (see [When a partition's node is down](#when-a-partitions-node-is-down)).
- **Progress events** keep a quiet stream's position fresh, as on a single node.
- **A subscription from now** gets every commit made after the call returns.
  - From a partition the entry node serves itself, it gets nothing committed before the call, as on a single node.
  - From another node's partition, it may also get commits made shortly before the call. When the entry node's feed threads are saturated, its first commits can be older than the call by that backlog.

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
  - a stream whose partition the map drops (a forced map, or one that drops it once no table is placed on it) ends with `PartitionUnavailable` too, its message saying the map no longer lists the partition. A new subscription follows the tables as the map now places them.
- **With `allow_partial: true`,** the stream carries on with the other partitions:
  - A `partition_state` event (`available: false`, with the error's code in `reason`) says which partition is missing. It's sent at the start for one unavailable then, and once when it goes.
  - The node tries the partition again every second, each try bounded by `--request-timeout-ms`, without holding up the others. When it's back, a `partition_state` event says so, and its commits continue from where they stopped, with none skipped.
  - A partition that was down since a subscription "from now" began starts at its own "now" when it comes back. One subscribed "from the oldest" starts at its oldest commit.
  - A partition the map drops is reported the same way, and tried again in case a later map lists it.
  - An expired position, or a refusal such as a revoked token, still ends the stream.
- **A partition's lease (D326):** a partition's events are delivered only while its node holds the partition's lease, its catch-up from a position included.
  - **A node whose lease ran out, or whose lease rounds stopped reaching a majority of the map's nodes** (it's cut off), ends the partition's streams with `PartitionUnavailable` within the lease, and refuses new ones. With `allow_partial`, that's a `partition_state` event, and the partition is tried again every second where the map puts it; without, the subscription ends, and you resume from your token.
  - **A node that hasn't held the lease yet,** but reaches a majority (a move's new node waiting for the old node's lease to end), keeps a subscription open and delivers nothing until the lease starts. A subscription resumed through it then goes on with nothing skipped.
  - A subscribe isn't a read: it's refused only in the first case, never for want of a lease alone.
- **A partition moved to another node** (a newer map, from `phdb cluster move` or `relocate`: [Fencing and relocation](#fencing-and-relocation)) is opened again where the map then puts it, through any node, with nothing skipped, unless the move started a new history ([Expiry and what isn't supported](#expiry-and-what-isnt-supported)). The new node may still be opening it: the subscription waits for that, up to `--request-timeout-ms`, before it counts the partition unavailable, so with `allow_partial` a moved partition's `partition_state` event can come that much later.

#### Limits
- **Stream slots:** a subscription holds a `--max-streams` place on the node it entered, and one on each followed partition's node. An entry node that serves one of the partitions itself holds two.
- **Memory:** it buffers up to 256 KiB on each partition's node, so partitions × 256 KiB at most, outside `--inflight-bytes`, like a single node's feed. The entry node merges the partitions' events on its feed threads (`phdb-serve-feed`, not the threads that serve requests) and buffers up to the client's own 256 KiB, as a single node's stream does.
- **Shared merges** (#1798): subscriptions from now that enter the same node with the same token and the same request share one merge there.
  - The same request means the same tables, prefix, visibility, `allow_partial` and `checkpoint_every`.
  - A shared merge keeps a ring of its merged events, as a single node's hub does. Its ring takes a share of `--feed-ring-bytes`, and a node has at most `--max-feed-hubs` shared merges, counted apart from its partitions' hubs. Past that, a new request runs a merge of its own.
  - The partition hubs it reads from on the entry node take a share of `--feed-ring-bytes` too, as any hub does. While shared merges are their only readers, they run on the hub threads (`phdb-serve-hub`), so the feed threads are left to merge and deliver.
  - Starts from a token, from the oldest commit, or with `skip_through` run a merge of their own until they catch up, then join the shared one.
  - The saving needs subscribers that share a token and a request. Each token or request of its own costs about what a subscription did before.
- **Falling behind a shared merge:** a subscriber more than its ring behind goes on alone from its last checkpoint, with nothing skipped or repeated, and rejoins once caught up.
  - Going on alone takes a `--max-catch-up` slot, as a single node's catch-up stream does.
  - With no slot free, it waits, delivering nothing, until one is free or the client goes away. `--stream-idle-ms` doesn't end the wait: it counts only events the client left untaken.
  - If its shared merge ends meanwhile, it gives the slot back and goes on alone, joining the request's next shared merge once one runs.
- **Stalled subscribers:** a subscriber that stops reading ends after `--stream-idle-ms`, as on a single node: it gets the events queued for it, then `DeadlineExceeded`, and resumes from the last event's token (#2021). Its `--max-streams` places, its partitions' streams and its connections between nodes are freed then, and it's counted in `pigeonhole_serve_feed_idle_drops_total`. Until then it holds only its own connections between nodes, never one that forwarded calls or other streams use. A subscriber that keeps reading keeps its subscription however long nothing happens: the limit counts how long an event waits untaken, so it ends a client that stopped reading, never one that's waiting for a commit. On a feed with no commits, a client that stopped reading is first left an event by the next quiet progress event (about 10 s), then ended `--stream-idle-ms` after that, as on a single node.

#### Expiry and what isn't supported
- **Expired tokens:**
  - A token can't resume after a partition it follows starts a new history: a `relocate` without `--copy` (`--old-unreachable` included) does, as a restore does, and so do a promotion of a replica, by hand or by [automatic failover](#automatic-failover), and a `set-map --force` that raises its epoch ([Running a cluster](#running-a-cluster)). `move` and `relocate --copy` copy the files whole and keep the history, so tokens resume across them ([Fencing and relocation](#fencing-and-relocation)).
  - It can't resume after the map dropped a partition the token reads either: a forced map, or one that drops a partition once no table is placed on it.
  - Both answer `FeedExpired`: subscribe again.
- **Range moves (1.3.0, D331)** don't expire tokens, while the map still lists the partitions they read. A key's events stay in order across a move: its last events on the old partition come before its first on the new one.
  - **A subscription that reads the new partition already** holds back that partition's events from the hand-over on until its read of the old partition has passed the move's last commit. While held, the new partition's other ranges wait too.
  - **A subscription that doesn't read it yet** ends with `FeedRerouted`, right after a checkpoint. Its resume from that token reads the new partition from the hand-over on.
  - **A move that takes all of a table from a partition:** the old partition still serves the table up to the move's last commit, then drops out of the subscription, both to a subscription running across the move and to a token from before it. A new subscription `from_oldest` after such a move's hand-over reads the table only on the partitions that hold it from then on, so the old partition's retained history of it isn't delivered. To build state from scratch, subscribe from now first, then read the table (a scan), then apply the subscription's events over it, keeping each cell's newest timestamp so an event the scan already saw changes nothing. A snapshot start isn't supported on a cluster subscription.
  - **Shared subscriptions:** the end reaches every member of a shared one at once. They resume unshared until the subscription resumes or a new shared merge opens (#2067).
  - **A quiet old partition** releases a hold only at its next progress event, so up to its quiet interval late.
- **After a restore:** a token from before it answers `FeedExpired`. Each restored partition is at a new epoch, and its history starts at the restore ([Backup and restore](#backup-and-restore)).
- **Not supported:** a start from a snapshot isn't supported on a cluster subscription. A one-partition version is #1701.
- **Positions:** a token's `seqno`s are partition-local. There's no cluster-wide position, so compare progress per partition.

### Fencing and relocation
A node writes a partition only while its newest map names it for the partition at the file's epoch, it holds the partition's lease, and the partition isn't fenced (D317, D326). `phdb cluster move` does a whole move. The commands after it are its steps, for doing one by hand or for a node that's down.

- **Leases (1.3.0, D326).** A partition serves on its node, reads and writes, only while a majority of the map's nodes, the node itself included, grant it a lease. Each grant runs for `--cluster-lease-ms` (default 10000) on the granting node's clock. The node renews every third of a lease.
  - **A node cut off from the majority stops serving within the lease.** Its writes answer `Fenced` ("its lease ran out") and its reads `PartitionUnavailable`. A write whose commit returned after the lease ended answers `OutcomeUnknown`: it may or may not be kept, so read it back from the partition's node before you retry. Once a renewal reaches a majority again, it serves again.
  - **No second node can serve P while the first's lease runs:** a node that granted P to one node withholds it from every other until that grant runs out. So after `relocate --old-unreachable`, or a forced map that keeps the nodes, the new node serves only once the old node's lease has ended, up to one lease later. That holds while a majority of the map's nodes include nodes that granted the old lease: a forced map that replaces most of the nodes promises nothing of it (below).
  - **A planned move** (`phdb cluster move`) closes P on the old node, which then releases P's lease at every node of the map, so the new node serves without waiting for it. A release that's lost costs the new node a wait: until the old node's next renewal asks without P, up to a third of a lease, or until its grant runs out. It never lets both serve.
  - **A node that starts grants no lease for a lease and 1% more** (its grants were in memory). A whole cluster restarting serves that much later. `phdb cluster status` shows each node's `grace_remaining_ms`.
  - **Two nodes:** a majority is both. With one down, the other stops serving within the lease, since it can't tell "the other node is down" from "I'm cut off". Run three nodes, so one can be down, or a single node (`phdb serve` without `--cluster`). To bring a two-node cluster back on one node, set a one-node map with `set-map --force`.
  - **`set-map --force` across a change of nodes promises nothing about the old majority:** it skips the wait for the old nodes. If the nodes it dropped are alive on the other side of a cut, both sides can serve. Use it only for nodes that are gone.
  - **Clocks:** no node compares its clock with another's; each times its own grants and leases on a clock that counts time the machine was suspended. A clock may run up to 1% fast or slow. A VM its host pauses without the guest's clock catching up breaks that: don't run cluster nodes on hosts that pause VMs without adjusting their clocks.
  - **The map's writer holds a lease too (1.3.0, D330).** From the first writer term on, it changes the map only while a majority grants it. With the map's `failover` on (off by default, set with `set-map`), a writer whose lease has been out for `--cluster-failover-after-ms` (default twice `--cluster-lease-ms`) is replaced by an elected one; `phdb cluster status` shows the writer, its term and its lease. A forced map raises the term, so a writer it replaced can't change the map when it comes back.
  - **Partition failover (1.3.0, D330).** With the map's `failover` on, the writer promotes a down partition's freshest replica on its own: [Automatic failover](#automatic-failover).
  - **Partition load (1.3.0, D332).** `phdb cluster status` shows each partition's `load_cpu`, `load_device` and `load` (the larger), exponential averages over `--balance-window` (seconds, default 300, 30 to 86400: each 30 s reading weighs `1 − e^(−30/window)`), for the cluster balancer. Every node folds them once a second, whether or not anyone reads them.
    - `load_cpu` is the partition's share of the node's cores: its shards' busy time, or their group-sync time if larger, plus sampled request CPU (one call in 16 per thread). The sampled CPU includes the reads' work on the server's read pools. CPU outside its service calls isn't counted (request decoding and encoding, framing, sockets).
    - `load_device` is its bytes per second at its files over the node's ceiling: the highest one-second rate of the same counters, summed over the node's partitions, within the window. It's per node and measured, with no flag. These are the engine's own estimates, for every file of the partition (metadata such as the manifest included): reads the OS page cache served count too. A newly opened partition's counts start from its open, so its open doesn't count toward the ceiling.
    - **`load_device` is 0 while the partition's rate is under 1 MiB/s** (from 1.3.1, #2174), however close that rate runs to the ceiling. A node that only trickles (its map, lease and decision writes, and Status reads) has a ceiling near its partitions' own rates, so up to 1.3.0 a lone idle partition read near 1.0: too hot to the balancer, which took the pass's first skip reason, and never cold enough to merge. `load_cpu` needs no such floor: it's a share of the node's cores, not of its own peak.
    - Use them to compare partitions, not as absolute CPU or disk use.
  - **Partition size (D332).** `phdb cluster status` shows each open partition's `file_bytes`: the bytes its file holds on its node, which is the file's length less the space no live or retired extent holds. The balancer measures partitions by it, and a split's or move's fit check uses it. It falls after a delete, or a split's or move's cleanup, once the freed space is reclaimed (a retired extent counts until no reader's view still holds it).
    - **Changed in 1.3.1 (#2150).** Up to 1.3.0, `file_bytes` was the file's length. That never shrinks while the partition is served: freed space stays inside the file, and D333 punches it as holes with the length kept, until `phdb compact` truncates the tail.
    - So a partition split for size looked just as big after its split's cleanup, and the balancer split it again every dwell. A partition that shrank was never merged.
    - Scripts that read `file_bytes` as the file's length should take the length from the file system, or from `Admin.Info`'s partition `file_bytes`, which is still the length.
    - **Pause the balancer for a rolling upgrade from 1.3.0** (`phdb cluster balancer pause`, then `resume` once every node runs 1.3.1). Mid-upgrade, a 1.3.0 node reports its partitions' lengths and a 1.3.1 node what their files hold, so the nodes not yet upgraded look larger: a 1.3.1 writer would move their partitions onto the upgraded nodes, and a 1.3.0 writer still splits again after a split's cleanup.
  - **The cluster balancer (1.3.0, D332). Off by default.** The map's writer can move whole partitions to even out the nodes' size and load: [The cluster balancer](#the-cluster-balancer).
  - `--cluster-lease-ms` must be at least 3 times `--cluster-push-timeout-ms`, and not 0. **Every node must take the same value:** a node refuses to grant a lease to one whose `--cluster-lease-ms` differs (`FailedPrecondition`, naming both values, in the asking node's log), since a node with a longer lease than its peers' could serve past their grants. Change it on every node by restarting them one at a time only if a majority keeps one value throughout; otherwise stop them all and start them with the new value.
  - **During a rolling upgrade from 1.2** (#2047), a 1.2 node doesn't grant leases (it answers `Cluster.Lease` with `Unimplemented`). A 1.3 node counts each 1.2 node that holds the round's map as granting (a node whose `Status` lacks the `leases` feature), so the first node upgraded serves as soon as it restarts, as a 1.2 node would, and logs once per such peer that it does. A 1.3 node that reports `leases` but whose `Cluster.Lease` answers `Unimplemented` counts as nothing, and the log says to check what sits between the nodes: a proxy or ingress in front of a node must route every `/pigeonhole.v1.Cluster/` call, `Lease` and `Release` included. While any node runs 1.2, the guarantee is 1.2's (a majority holds the map naming the node), not the lease's: a 1.2 node withholds nothing, so after `relocate --old-unreachable` or a forced map, an old owner on the other side of a cut may serve until its lease runs out. A 1.2 node that doesn't answer counts as nothing, as any node does: a 1.3 node cut off still stops within its lease. Finish the upgrade before relying on the lease, and don't `relocate --old-unreachable` mid-upgrade unless the old node is known to be down.
  - **Backups aren't gated by the lease:** `phdb cluster backup`, `Admin.Backup` and `Admin.BackupDownload` copy a partition whether or not its node holds the lease. A copy of a fenced or closed partition is what a move or a recovery needs.

- **`phdb cluster move P --to ID --node URL [--drop-source] [--timeout SECS]`** (admin scope) moves P to node `ID` with its files copied whole, so its change-feed history carries over: tokens taken before the move resume after it, through any node, until the node it moved to restarts. The partition keeps at least the history it came with until then, even past that node's `--change-feed` share; a restart gives it its share.
  - **Its steps:**
    1. It fences and closes P on the node serving it, and waits for the close to hash its files.
    2. It copies each file to `ID`: the main file, every WAL file a clean close keeps, and the feed's.
    3. It stamps P there at the next epoch, and checks the installed set against the closed one: each file's name, size and SHA-256.
    4. It checks once more that P is still closed on the old node with exactly the files it compared, then sets the map (`relocate --copy`). It waits for a majority of the nodes to hold the new map, so the old node can't be confirmed again on the older one (D317 rule 3), then for `ID` to serve P. If P was unfenced and closed again there meanwhile, its files changed: the map stays as it was, and a rerun copies the new files.
  - **A mismatch** stops the move before the map changes, so the old node still holds P: run the move again to copy the files again, or `unfence` P to serve it there again (a peer's map must still give P to that node: see `unfence` below).
  - **Downtime:** writes answer `Fenced` from the fence until the map moves. Reads answer `PartitionUnavailable` from the close until `ID` serves, which is about as long as copying every file, one after another. Both grow with the files' size. With a change feed, that's the WAL files a clean close keeps for the feed's retention, so a long retention means a longer move. The report gives both durations and each file's size.
  - **`--timeout SECS`** (at least 1) bounds each of its waits: the old node hashing its closed files, a majority of the nodes taking the new map, and `ID` serving P. A wait that runs out ends the move with exit 1. The error names the step, the node's last `status` line for P, and where that leaves P:
    - **The hash:** the map still gives P to the old node, closed there. Run the move again to go on waiting, or `unfence` P there.
    - **The map on a majority:** the map is set; a node that missed it takes it from a peer. Check the nodes that don't answer, and run the move again to go on waiting.
    - **Serving:** the map already gives P to `ID`, so check that node.

    The copy has no limit on its length, since it takes as long as the files do. It does stop if the old node sends nothing of a file for 60 s, or if `ID` takes nothing of one for 60 s: run the move again, and it resumes from the bytes `ID` holds. It also stops if `ID` takes a whole file and then gives no answer for 60 s plus the file's size at 8 MiB/s (about 3 h 34 min for 100 GiB), the time allowed for its sync and hash of the file: run the move again. Without `--timeout`, a wait goes on as long as it takes, and says every 10 s what it's waiting for.
  - **The map records the move while it runs (1.3.0, D332).** `phdb cluster status` lists it under the partition's `moves`, with no table: COPYING, HANDING_OVER (stamped and checked), HANDED_OVER (the map gave P to `ID`) and CLEANING (`ID` serves). The record goes when the move ends. A stopped move is therefore visible, and nothing else can move P meanwhile.
    - **`phdb cluster move P --abort`** ends a recorded move before the map gives P to `ID`. It removes the record and moves the copy on `ID` aside. P stays on its node, closed and fenced as the move left it: `phdb cluster unfence P` serves it there again. Past that point it's refused, naming the phase: run the move again to finish it.
    - **A failover** of P before that point (its node went down mid-move) ends the move in the same map.
    - **A whole restore** (`phdb cluster restore DIR`) clears the record. `restore --partition P` is refused while a move names P: abort or finish the move first. A copy the move staged or stamped on `ID` stays there, outside the map: move it aside with `phdb cluster drop-partition P --node <ID>`. So does a failover's end of the move.
    - **Mixed versions:** in a cluster with a 1.2 node, the move runs unrecorded, as before, and the report's `recorded` says so.
  - **Run it again** to resume a move that stopped. A move that's finished answers that the map already gives P to `ID`.
    - A rerun before the map changed copies and checks the files again, whatever an earlier run left on `ID`: a copy still staged there is picked up from what arrived, and one already installed is sent whole and installed over the earlier one, which moves aside (`<file>.replaced-<time>`). So if you unfenced P on the old node between two runs, the second run moves the writes it took since.
  - **The old node's files stay** unless you pass `--drop-source`. With it, they move aside (`drop-partition`, below) once the copy has checked out and `ID` serves P. A rerun that finds the map already changed drops nothing; its report gives the `drop-partition` command.
  - **The old node must answer.** For a node that's down, promote one of the partition's replicas (`phdb cluster promote`, or [automatic failover](#automatic-failover)), or bring it back on another node from a backup with `relocate --old-unreachable` ([A lost node](#a-lost-node), below). Either starts a new feed history.

- **`phdb cluster fence P --node URL`** (admin scope; a node's cluster token too, for a move the map's writer runs) fences P on the node that serves it.
  - Its writes then answer `Fenced` through every node, until it's unfenced, closed and moved, or the node restarts with it unfenced.
  - Reads go on while the node holds P's lease, and so do its flushes and compactions.
  - **Schema changes wait too:** a table created or dropped through the cluster meanwhile reaches P only at `unfence`, so reads of the new table's range in P answer `TableNotFound` until then. `phdb cluster status` shows P's `reconciled_version` behind the map.
  - The command answers once the writes admitted before the fence have finished, and P's change feed reports them durable. For a `Buffered` or `None` write, that comes at the feed's next background sync (`sync_interval`). Past 30 s it answers `FailedPrecondition`, naming how many still run, or how far the feed's durable frontier is behind.
  - The fence is kept in `DIR/fenced.json`, so it outlasts a restart.
  - A fence is the copy's it was set on: when P's files are later installed on the node again (a move back), the fence is gone, and P serves there once a peer's map confirms it.
- **A restarted node** opens every partition without a lease, and serves each one once a majority of the map's nodes, itself included, hold the newest map, it still names the node for the partition at the file's epoch (D317 rule 3), and they grant its lease (D326). The node asks its peers, takes a newer map if one answers with it, and gives its map to any peer that's behind before counting it. A newer map that moved the partition keeps it from serving. A partition a map gives a running node (a move's new node) waits the same way. In a three-node cluster one node may be down; in a two-node cluster both must answer. A node whose peers don't answer serves nothing: `phdb cluster status` shows `fenced (awaiting peer map)`, and `fenced (operator, awaiting peer map)` for a fence of yours, which becomes `fenced (operator)` once the lease is granted. `phdb cluster unfence` asks the peers in either case (#1829). A one-node cluster leases to itself, after its start grace.
- **`phdb cluster unfence P --node URL`** lifts either fence. It's refused unless the node's map names that node for P at its file's epoch, and the peers' newest map agrees.
  - **The peers' newest map must confirm it,** whichever fence it is: the command asks every peer first, and lifts the fence only if the newest map among those that answer is at least as new as the node's own and still names the node for P at that epoch. One peer agreeing isn't enough: a peer that missed the same move holds the same old map. A peer that doesn't answer can't be counted, so if the only node that knows of a move is down, the others still confirm: check `phdb cluster status` on every node before you unfence the old node of a move. The node's own map isn't enough: it may have been down, or missed a push, while a move gave P to another node. That is most likely on the old node of a move, which the move fenced. A newer map a peer answers with is taken by the node at once. A cluster of one node confirms on its own map.
  - **`--force`** lifts the operator's fence without asking the peers. It never makes P serve without a lease (1.3.0, D326): that still takes a majority of the map's nodes. Before 1.3.0 it also lifted the wait for the peers, and a node whose peer's newer map gave P to another node then took writes alongside it.
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
    - **`--old-unreachable` can lose acknowledged writes:** nothing compares the new node's files with a node that doesn't answer. Writes the old node acknowledged after your copy was taken aren't in it. Use it only when that node is gone, with the newest copy you have.
- <a id="a-lost-node"></a>**A lost node: bring its partition back from a backup.** A partition with a read replica comes back with far less loss by promoting the replica (`phdb cluster promote P --to ID --old-unreachable`: [Read replicas in a cluster](#read-replicas-in-a-cluster)), and with failover on the cluster does that itself when one of P's replicas qualifies. Without a replica, the files can only come from a backup (`phdb cluster backup` or `Admin.Backup`). For each such partition P the node held:
  1. **Check the epoch.** The backup's copy must be at P's current epoch in the map: `phdb cluster show-map` gives the map's, and the backup's `manifest.json` the copy's. They match unless P was moved, relocated or restored after the backup.
  2. **Copy the backup's `P.phdb` into the new node's `DIR`,** as `P.phdb`. The new node must not hold P already. A running node holds `DIR/.lock`, not its files, so the copy can go in while it runs.
  3. **Run `phdb cluster relocate P --to ID --old-unreachable --node URL`,** with `--node` a node that answers. The new node stamps the copy at the next epoch, the map gives it P, and it serves.
  - **What you lose:** the writes to P after the backup. P starts a new feed history, so tokens from before answer `FeedExpired`.
  - **If the lost node was the map's writer,** set a map without it first with `set-map --force` ([Running a cluster](#running-a-cluster)), then promote or relocate: both need a live writer. With the map's `failover` on, another node has been elected writer instead, and no forced map is needed.
  - **If the epochs don't match,** `relocate` takes only a copy at the map's epoch: use `phdb cluster restore DIR --partition P --to ID --old-unreachable` instead ([below](#restore-one-partition)), which takes the backup's older copy.
- **`phdb cluster drop-partition P --node URL`** moves P's files on the old node into `DIR/.phdb-dropped/<P>-<epoch>-<time>/`. It's refused while the map still places P on that node, or while P is open there, so close it first.
  - Nothing is deleted, and nothing is freed until you delete that directory, once you've checked the move.
- **A node opens a partition while it serves** when a map gives it one. Its share of the node's `--shards` and `--compaction-threads` is the share it would get at start for the new set of partitions, and the partitions already open keep theirs. The server's thread masks are recomputed off the new shards' CPUs. Its runtimes' threads take the new masks at their next park or task, and every other thread is moved at once, except the engines' own pinned shard and compaction threads (#1762). Those others include the engines' unpinned threads, a backup, a bulk load, a replica's follow, and candle's pool, whose `CANDLE_CPU_MASK` is read only once.
  - **To add a new partition to a running node,** its file must be in the node's `DIR` before the map gives it the partition. `phdb cluster init-partition` can't write into a running node's `DIR`, since the node holds `DIR/.lock`: stamp the file in another directory and move its files into `DIR`, or upload it (`Admin.PartitionUpload`) and use `phdb cluster relocate`.
  - **Memory:** `--engine-memory` is split again for the new set of partitions, as a restart would split it (#1761). The partitions already open keep their memtables, which can't be resized while they're open, and their block and row caches shrink to what their new shares leave after them, before P's engine opens, so the node never holds more than `--engine-memory`. P gets the rest. A log line gives each partition's caches before and after. When a partition closes, the others' caches grow back to their shares.
    - **Refused only** when the open partitions' memtables leave too little for P's: that happens when `--engine-memory` was so tight that memtables took all of it. The message says so. **Then restart that node alone:** the map already gives it P, and its start splits `--engine-memory` across every partition with smaller memtables.
    - **A shrink is a budget, not an instant drop in memory use:** blocks an open SST pins (its top index and filters) stay cached, and a log line names a partition whose pins are over its new block cache share. A blob file already open keeps the cache limit it opened with.
    - A node that held no partition, the usual move target, gives P all of it. Without `--engine-memory`, every partition takes the library's defaults, as at start.
  - It opens on the free cores, off every open partition's shard cores and their SMT siblings. If its shards don't fit there, the open is refused, naming how many would run unpinned, rather than put on a sibling.
  - **Its compaction threads** run on the node's compaction CPUs, as at start: the CPUs reserved after all the shards on a node that started with several partitions, or the open partitions' compaction CPUs on one that started with one. Before #1888 they were pinned on the free cores, where the request threads run.
  - **The server's threads then move off the new shards' CPUs** (or back onto a closed partition's), and a log line names each runtime's threads against its CPUs. Their counts stay as the server started them, since a runtime can't be resized. Every other thread, a background one already running included (a backup, a bulk load, an index build, a replica's follow), is moved at once, as above, and the new partition's own pinned engine threads keep the CPUs it opened on. After a partition opens, the network, feed and blocking threads share fewer CPUs, and run more of them per CPU than a node started with that partition would. Restart the node to size them again.
- **Errors:** `Fenced` (409, gRPC `FAILED_PRECONDITION`) means the write wasn't applied because its partition is fenced or has no lease. Send it again once the partition is unfenced or has moved: a node that takes it reroutes to the new owner. A read on a partition without a lease answers `PartitionUnavailable`, and a write whose commit outran its lease `OutcomeUnknown` (D326).

### Read replicas in a cluster
**New in 1.3.0 (D328, ICR 0117).** A partition can have read replicas: whole copies on other nodes that follow its owner and serve reads that accept a bounded staleness. One can be made the owner by hand with `phdb cluster promote` (below), or by the cluster itself when its owner goes down ([Automatic failover](#automatic-failover)).
- **Placing them.** List the nodes in the partition's `replicas` in the map, through `phdb cluster set-map`, in order of preference. Each must be a node of the map, not the partition's own node, and listed once. A node removed from the map must be dropped from every `replicas` list first. Changing `replicas` isn't a move: the partition's epoch and owner stay as they are. Every node must run a build that knows replicas (it reports the `replicas` map feature) before a map can add the first one.
- **The owner's change feed must be on** (`--change-feed` on the owner's node). A replica follows the owner's feed, so without it the replica can't start: its status says `OWNER_FEED_OFF`, and it tries again until the feed is on. The owner's feed retention is also how long a replica may be down or behind before it must copy the whole partition again, as for a [single node's replica](#replicas): size it for the longest replica outage you want to ride out.
- **What a node does with one.** A node listed in P's `replicas` opens `DIR/P.phdb` as P's replica copy (made empty if missing), downloads a copy of P from its owner, then follows the owner's commits. A map that drops the node closes the copy; the file stays until `phdb cluster drop-partition P` moves it aside. A replica copy takes no writes of its own, holds no lease and is never fenced. It counts in the node's budgets (`--shards`, `--compaction-threads`, `--engine-memory`) like a partition it owns.
- **Following the owner.** The replica dials whichever node the map names as P's owner, and dials again after a break. An owner serves `Replication.Follow` only to a node listed in P's `replicas`, so nothing outside the map can follow a cluster partition (from 1.3.0; a cluster node never took `--replica-of`). The owner serves a replica only while it holds P's lease, so an owner cut off from the cluster stops feeding its replicas within the lease. After `phdb cluster move` or `relocate --copy`, the replica goes on from where it was on the new owner. After a relocate without `--copy`, a restore or a promotion (by hand or by failover), which give P a new history, it copies the partition again from the new owner. It serves no reads meanwhile, since its old copy may hold writes the new owner never had: a read that allows staleness goes to the next replica or the owner until the new copy follows.
- **Watching them.** `phdb cluster status` shows each copy as a replica row (`role` replica): the owner it follows, its applied and the owner's seqnos, `replica_lag_ms` (how long since the owner last said it was caught up, on the replica's clock; an owner sends that at least once a second while idle, so a stopped replica's lag grows at once) and `replica_state`: `BOOTSTRAPPING`, `FOLLOWING`, `BEHIND` (retrying after an error, given in `error`), `STOPPED` or `OWNER_FEED_OFF`. The node's own partitions show `role` owner. `Admin.Info` lists each copy with a `replica` section as a single node's replica has, and `/metrics` carries the same four replica gauges as a [single node's replica](#replicas), each labelled `pigeonhole_partition`.
- **Moving a replica** is a map change: drop the node from `replicas` and add another, which copies the partition again.
- **Reading from replicas.** A read is the owner's unless the client allows staleness: send `x-pigeonhole-read-replica-max-lag-ms: N` (gRPC metadata, or an HTTP header) with a read: `Get`, `ReadRow`, `MultiGet`, `Scan`, `Vectors.Nearest` or `Graph.Traverse`. A read across partitions takes each partition's part from that partition's replicas first.
  - **Which replica:** the node you called tries P's replicas in the map's order, then the owner, all within the request's deadline. Each replica serves the read only if it's **fresh within N ms**: it heard from the owner, and had applied everything the owner had made durable when that message left, at most N ms ago on its own clock. Otherwise it answers `ReplicaBehind`, and the next one is tried. A replica found behind or unreachable is skipped for 2 s.
  - **What it promises:** a consistent read of P as of some committed point of P's history, at most N ms behind what the owner had durably committed (plus one heartbeat, up to a second, and the network's delay). It never shows a write that wasn't committed, and never one from a deposed owner past its lease.
    - A replica whose stream never pauses (it can't keep up with the owner) counts its age from its last pause, so it reads as aging even when it's only a message or two behind.
  - **Read your own writes:** send `x-pigeonhole-min-primary-seqno: S` too, with your write's `CommitInfo.seqno`. A replica waits, within the deadline, until it has applied S, else the read goes on to the next replica or the owner. A seqno names one partition, so a read across partitions with it is refused (`InvalidArgument`).
  - **What served it:** the reply's `x-pigeonhole-served-by` header is `replica` or `owner`, and `x-pigeonhole-served-seqno` the seqno it read at (the replica's applied seqno, or the owner's visible seqno, after the read). Send that back as `x-pigeonhole-min-primary-seqno` for reads that never go back in time. A seqno belongs to one history of the partition: after a relocate without `--copy`, a restore or a promotion, the partition's seqnos start again, so a seqno you kept from before names nothing in the new one. Drop it, and take the next read's.
  - **Scans:** each page is checked against the bound before it's read and before it's sent. A replica that falls behind partway leaves the scan, and the partition's pages go on from the last page's position on the next replica or the owner: no row is skipped or repeated. Each page's `served_seqno` is the seqno that page was read at, since a scan's pages can come from different replicas and partitions; the served headers are for single-partition reads only.
  - **Never from a replica:** writes, transactions, snapshots and reads at a snapshot, subscriptions, schema and admin calls.
  - **Not promised:** that two reads see the same or a later state (they may hit different replicas) unless you send `served-seqno` back, or any order across partitions.
- **Promoting a replica** (`phdb cluster promote P --to ID --node URL`): node ID, which holds a replica of P, becomes P's owner. Use it when P's node is lost, or to move P with no copy.
  - **First,** P must be fenced or closed on its owner (`phdb cluster fence`, `close`), or the owner's process stopped (`--old-unreachable`, which still checks briefly that it doesn't answer and serve P).
  - **With `--old-unreachable`, it also checks that the owner hasn't come back** (#2152), as automatic failover does ([The old owner](#the-old-owner)).
    - It asks every node for its grants of P, before its first map and again once that map is on a majority. Grants still running from before the owner stopped are waited out first, for up to two leases. It stops if any node still grants P's lease to the old owner after that (the owner renews it, so it's alive), if that map misses a majority, or if too few nodes answer to tell. An old owner that answers and holds no lease of P settles that last question itself.
    - It also stops if the two surveys are more than a lease apart (from `--cluster-lease-ms`, which each node reports). A node from before 1.3.1 doesn't report its lease; if none does, the promote goes on without that bound and prints a warning naming the nodes.
    - **When it stops:** on the owner's grant, too few answers, or surveys too far apart, the promote doesn't start, or is abandoned before its stamp with ID back in P's replicas. On a first map short of a majority, it stays pending, unstamped. Either way, run it again once the reason is gone.
  - **What it does:**
    1. It sets a map without ID in P's replicas, so ID closes its copy.
    2. It makes the copy a primary at the last commit it applied, and stamps it at the next epoch: above the map's, and above every epoch a promote of P recorded. A rerun that resumes its own pending promote to the same node keeps that promote's epoch.
    3. It sets the map that gives ID the partition at that epoch, with a **new change-feed history**: feed tokens from before don't resume, and replicas of P copy it again.
    4. It waits until ID serves P, as `move` does, so P takes reads and writes there when the command returns. `--timeout SECS` bounds that wait; the error then names ID's last `status` line for P, and the map already gives ID the partition. With `--old-unreachable` the wait can include up to one lease (`--cluster-lease-ms`) while the old owner's grants run out, so give a `--timeout` longer than that.
  - **What's lost:** commits the old owner acknowledged after the copy's last one. The report gives `applied_seqno` (the copy's), `owner_durable_seqno` (the owner's, as the copy last heard), and `lost_commits`, their difference. `lost_commits_counted` says how far to trust it.
    - **With the owner fenced by you** (`phdb cluster fence`), it still streams its feed. The promote first fences it again, which answers once the owner's feed reports every write durable. Then it waits up to 3 s for the copy to hear that and catch up. The count is then exact, and usually 0. If either step runs out, the count is a lower bound.
    - **With the owner fenced otherwise** (its lease lapsed, or it awaits its peers' map), the count is a lower bound.
    - **With the owner closed,** the count is a lower bound: the copy last heard the owner's durable seqno before the close.
    - **With `--old-unreachable`,** it can't ask the old owner, so `lost_commits` says the loss is unknown.
  - **Refused:** a copy from before P's current history (it copies P again first; promote it once it follows), and an owner that still serves P.
  - **Run it again** to finish one that stopped: it carries on from the closed or stamped copy. That includes a stop between making the copy a primary and stamping it: ID's node keeps the promote's intent beside the file (`P.phdb.promoting`) until the stamp is done, and the rerun only stamps it. With failover on, the map's writer finishes such a promote itself. If the map changed in between (a `set-map`, relocate or another promote), the rerun is refused, so it can't replace what the partition took since. Check the map, then move ID's file aside (`phdb cluster drop-partition P --node <ID>`).
  - **The old owner afterwards** keeps its files, and serves nothing for P once it holds the new map. Leases (D326) keep it from serving P once ID does. To have it follow ID as a replica: `phdb cluster drop-partition P --node <old owner>` (its files go aside, so copy off anything you want from its tail first), then add it to P's `replicas` with `set-map`. Its unreplicated commits are gone either way.
- **Moving P onto a node that holds its replica** (`move`, `relocate`, `restore --partition`): the command first sets a map without that node in P's replicas and waits for it to close its copy. The copy's file goes aside when P's files are installed there.

### Automatic failover
**New in 1.3.0 (D330, ICR 0117). Off by default.** With the map's `failover` on, the cluster replaces a partition's owner that has gone down with one of its [read replicas](#read-replicas-in-a-cluster), and replaces a map writer that has gone down with an elected one. Nobody has to run `phdb cluster promote` or `set-map --force`.

**Failover can lose acknowledged writes.** Replication is asynchronous, so the replica that takes over may not have every commit the old owner acknowledged ([What can be lost](#what-can-be-lost)). Turn it on only where that's acceptable.

#### Turning it on
- **In the map:** set `"failover": true` with `phdb cluster set-map`. It's a map field, so every node agrees on it and a new writer inherits it.
- **One partition out:** set that partition's `"failover": "FAILOVER_MODE_OFF"`. The map's flag is the master switch: with it off, a partition's own `ON` does nothing.
- **What it needs:**
  - three or more nodes, since everything here takes a majority of the map's nodes;
  - at least one replica for each partition that should fail over;
  - every node on a build that keeps the `writer_terms` map feature (1.3.0). Until then the map can't carry `failover`.
- **Flags, on every node:**
  - `--cluster-failover-after-ms` is the margin A: how long a lease must have been out before the cluster acts. The default, 0, means twice `--cluster-lease-ms`.
  - `--cluster-failover-max-lag-ms` refuses a replica staler than that (default 0: no limit beyond the freshness rule below). That rule already refuses a replica more than a heartbeat (1 s) stale, so only a value under 1000 changes anything.
- **With it off,** a down partition stays down until an operator promotes a replica, and a down writer until a forced map. `phdb cluster status` still reports the down partition, with "failover off".

#### What fails over, and when
Two things can fail over. Both wait for the same two times: the lease L (`--cluster-lease-ms`, default 10 s) and the margin A (default 2·L).

- **A partition, when its owner goes down.** The writer is one of every partition's grantors, so it sees each owner's renewals. P is **down** once the writer's own grant of P to its owner has been out, unrenewed, for A.
  - The writer then asks every node at once (a survey, bounded by the push timeout). It fails P over only if no other node still grants P's lease to the owner, and the owner doesn't answer as serving P.
  - It promotes the freshest replica that qualifies, by the same steps as `phdb cluster promote`. One promote runs at a time.
- **The writer, when it goes down.** A node starts an election once its grant of the writer lease has been out for A and every node it reaches reports no writer lease.
  - Smaller ids go first: each live node with a smaller id gets one more A before a larger one tries.
  - The candidate first asks whether a majority would vote (a pre-vote), then takes the votes at a higher writer term, and sets the newest map that majority holds, naming itself.
  - The new writer then fails over the old writer's partitions as above.

**Which replica.** Of P's replicas, the writer takes the one with the highest applied seqno (then the map's order) whose copy:
- is `FOLLOWING` or `BEHIND`;
- is on a node that holds the writer's own map;
- is inside P's current history (not still copying after a restore or an earlier promotion);
- had heard from the owner no earlier than a heartbeat (1 s) before the owner's last renewal, and is within `--cluster-failover-max-lag-ms` if set.

**What isn't failed over.** `phdb cluster status` lists each down partition the writer leaves alone, with the reason (`failover_blocked`):
- failover is off, for the map or for that partition;
- it has no replicas, or none qualifies;
- its owner answers and serves it;
- the operator fenced or closed it on its node. A lease that ran out because of that is the operator's doing;
- another node still grants its lease to the owner ("node X still grants its lease"). The owner is alive and only the writer is cut off from it;
- the writer has seen no renewal from the owner since the writer's node started, so it can't tell how fresh a replica is. The reason says to promote by hand (`phdb cluster promote P --to <replica> --old-unreachable`).

A blocked partition is looked at again when its reason can have ended: when the grant it waits on runs out, on a map change, or after A.

#### The timeline
With the defaults (L = 10 s, A = 20 s), counted from the owner's last renewal that reached a majority. Renewals go every third of a lease, so from the failure itself it's up to L/3 less.

| After the last renewal | What happens |
|---|---|
| up to L (10 s) | The owner's lease runs out. An owner that's cut off but alive stops serving P by then. |
| L + A (30 s) | The writer counts P down and surveys the nodes. |
| a few seconds more | The promote: a map that records it pending, the stamp of the replica's copy, the map that gives it P. Then the new owner's lease round. |
| about 30–35 s | P serves on the new owner. |

- **The formula:** L + A + the survey + the promote + one lease round. The margin is why the new owner never waits on the old one's lease: the old grants ran out a whole A earlier.
- **A down writer takes longer.** The election comes first, about L + A after the writer's last renewal, plus one A for each smaller-id node that's alive but didn't win. The new writer then counts each partition's silence from a lease after it took office, so the old writer's own partitions serve again after about 2·(L + A): a little over a minute with the defaults.
- **Shorter:** lower `--cluster-lease-ms` (it must stay at least 3 times `--cluster-push-timeout-ms`, and the same on every node) or `--cluster-failover-after-ms`. A margin under one lease gives a slow owner less time to renew before it's replaced.

#### What a client sees
Any node still takes any call, before, during and after.
- **While P is down:**
  - A call for P answers `PartitionUnavailable` when its node can't be reached. Nothing was sent, so it's safe to retry, writes included.
  - An owner that's alive but cut off from the majority answers `Fenced` to writes and `PartitionUnavailable` to reads once its lease has run out. A write whose commit returned after the lease ended answers `OutcomeUnknown`: read it back before you retry.
  - Reads that allow staleness (`x-pigeonhole-read-replica-max-lag-ms`) go on from P's replicas for as long as the bound allows. The replicas hear nothing new, so a read with a bound of N ms stops being served about N ms after the owner went. The replica being promoted stops serving reads when the promote starts, at about L + A, whatever the bound.
  - Feed streams of P end with `PartitionUnavailable`, or a `partition_state` event with `allow_partial` ([When a partition's node is down](#when-a-partitions-node-is-down)).
  - Other partitions aren't affected.
- **Once P serves on its new owner:**
  - Calls go there through any node. Retry what answered `PartitionUnavailable`.
  - **P has a new history,** as after a manual promotion:
    - change-feed tokens for P from before answer `FeedExpired`: read the tables again and subscribe from there;
    - a seqno you kept for read-your-own-writes names nothing in the new history: drop it;
    - P's other replicas copy it again from the new owner, serving their old copy meanwhile.
  - Open transactions and pinned snapshots on P ended with its old node. Begin again.
  - A write the old owner acknowledged may be missing. Nothing tells the client which.
- **While the writer is down,** data calls go on as before. No map change and no schema call (`CreateTable`, `DropTable`) can be made until a writer is elected.
- **Spanning transactions** ([Transactions across partitions](#transactions-across-partitions)) whose home was P: their home's history changed, so `Outcome` answers unknown and their shares on other partitions stay in doubt with that reason. Decide them with `phdb cluster txn decide`.

#### What can be lost
- **The known part:** the commits the owner had made durable and the promoted copy hadn't applied. The map's event records it as `primary_seqno − applied_seqno`.
- **The rest:** commits the owner acknowledged in the last heartbeat (up to 1 s) before it was cut off, and those in the lease that followed. A cut-off owner stops acknowledging within L.
- **To bound it more tightly,** set `--cluster-failover-max-lag-ms` under 1000: a failover to a staler replica is refused, and P stays down for an operator. A replica more than a heartbeat (1 s) staler than the owner's last renewal is refused whatever the flag says. Nothing in 1.3.0 makes a commit wait for a replica.
- **In-doubt transaction shares** on the old owner are lost with its history. The event lists them, as of the owner's last renewal. After a failover of a partition that homes spanning transactions, check every node's in-doubt list (`phdb cluster status`).

#### The old owner
- **Cut off but alive,** it stops serving P within the lease. No other node serves P until the old owner's grants have run out, and a failover comes a whole A after that, so the two never serve P at once.
- **If it comes back before the failover,** it renews its lease and serves again. The writer sees the renewal and leaves P alone.
- **If it comes back while the failover runs** (#2152), it depends on how far the promote got.
  - **Before the promote's first map reached a majority:** the promote finds the owner's new grant, stops before it stamps the replica, and is abandoned. The replica goes back into P's `replicas`, and the owner serves.
  - **After that,** that map records the promote pending, and no node grants P's lease to the old owner while it's there. So the owner waits, fenced, taking no writes. If the promote finishes, the owner finds P elsewhere, as below. If it can't (the replica stops answering before its stamp), the writer abandons it once the owner answers, and the owner serves again.
  - Either way, no write the owner acknowledges is lost to the promote.
- **During a rolling upgrade to 1.3.1,** a 1.3.0 node grants the lease whatever the map's pending promotes. Until every node runs 1.3.1, an owner that comes back after the promote's checks and before its last map can still take writes the promote then loses.
- **If it comes back after,** it takes the newer map, finds P on another node and serves nothing for it. Its file stays at the old epoch.
  - It doesn't rejoin on its own. Copy off anything you need from its unreplicated tail, drop the file (`phdb cluster drop-partition P --node <old owner>`), then add the node to P's `replicas` with `set-map`. It copies P again from the new owner.
- **An old writer that comes back** takes the elected map, which has a higher term, and follows it. The nodes that voted refuse every map and writer lease below the new term, so the old writer can't change the map at its old one.

#### Seeing it
- **`phdb cluster status`** shows:
  - the writer, its term and its lease, and whether `failover` is on;
  - `failovers`: the map's last 16 finished events and every pending one. A partition's event has its old and new node, the writer and its term, `applied_seqno` and `primary_seqno`, the in-doubt shares, the epoch it stamped and its state (pending, done or abandoned). An election's event names the old and new writer;
  - `failover_blocked`: each down partition that isn't failed over, and why.
- **`/metrics`:** `pigeonhole_cluster_failovers_total{partition}` and `pigeonhole_cluster_writer_term`. Every node shows the same values, since they come from the map.
- **The writer's log** has a line for each failover and election.

#### Two cases that settle themselves
- **A node that lost an election.** A candidate can pass its pre-vote and then lose the vote itself, for example when it's cut off between the two. It has recorded its own vote, so it refuses every map of the lower term the cluster is still on.
  - **What you see:** that node's partitions stay fenced and it routes by an old map, for up to about a lease after it's reachable again, and usually only a lease round or two. The other nodes go on serving and renewing.
  - **What ends it:** the live writer notices a node that stays behind, reads the term it voted for, and holds an election of its own at one term above. It wins, since every node already grants it the writer lease, and the stranded node takes that map. `failovers` shows a writer event from the writer to itself.
  - Nothing for the operator to do.
- **A promote that didn't finish.** If the writer stops after stamping the replica it promotes and before the map, the promote stays pending in the map.
  - **While P is still down,** the next writer finishes it: same replica, same epoch.
  - **If the owner comes back and serves first,** the writer abandons the promote. It can't finish while the old owner holds a lease.
  - **What's left is an orphaned copy** on the node it stamped. That copy serves nothing, and a node doesn't open it as a replica. `failover_blocked` names it: "an unfinished promote stamped node X at epoch E: drop that copy".
  - **To get it back as a replica:**
    1. Run `phdb cluster drop-partition P --node <X>` on that node.
    2. Add the node to P's `replicas` with `set-map`. If it's already listed, set the map again with no other change, or wait for the next map change: the node opens the replica only when a map that lists it lands after the drop.
    3. It then downloads a fresh copy from P's owner.
  - **Epochs:** every later change of P's epoch (a relocation, a move, a range move's hand-over) goes above the epoch the promote stamped, so that copy is never taken for P's history.

#### What it can't do
- **A lost majority.** Nothing can be granted, so nothing fails over and no writer is elected: a minority can't tell a dead majority from one it's cut off from. Recovery is still `set-map --force` with a map without the lost nodes ([Running a cluster](#running-a-cluster)).
- **Two nodes.** A majority is both, so with one down nothing fails over.
- **A replica cut off with its owner.** If P's owner and its only replica are on the same side of a split, away from the majority, there's nothing to promote.
- **Rejoin the old owner.** It becomes a replica again only by the steps above.
- **Lose nothing.** See [What can be lost](#what-can-be-lost).

### The cluster balancer
**New in 1.3.0 (D332, ICR 0117). Off by default.** With the balancer on, the map's writer moves whole partitions off a node that holds more than its share of the cluster's size or load, splits partitions too big or too hot, and merges small, cold ones, one action at a time. It reacts only to imbalance that lasts: a node must stay over its bound for minutes before anything moves, and what a move touched then rests for half an hour.

#### Turning it on
- **`phdb cluster balancer resume --node URL`** turns it on (the map's `"balancer": "BALANCER_MODE_ON"`), from off or paused. You can also set the field with `phdb cluster set-map`.
- **What it needs:** every node on a build that keeps the `balancer` map feature (1.3.0). Until then the map can't turn it on.
- **`phdb cluster balancer pause --node URL`** (PAUSED) lets a move in flight finish and starts no other. Setting the map's `balancer` to OFF does the same, and the balancer does nothing more.
- The mode, the bounds and the pins are map fields, so every node agrees on them and a new writer keeps them.

#### What it does
Every 30 s, the writer reads each node's open partitions (their size, which is `file_bytes` in `phdb cluster status`, and their load: the "Partition load" note under [Fencing and relocation](#fencing-and-relocation)), and compares each node with the mean of the nodes that answered:
- **A node's share is per unit of its weight.** Give a bigger machine a bigger `weight` in the map's `nodes` (an integer; 0 or unset counts as 1). A node with weight 2 is expected to hold twice the bytes and load of a node with weight 1.
- **A node is over its bound** when its size or load per unit of weight is above `size_ratio` or `load_ratio` (default 1.25) times the mean.
- **The move:** once a node has been over its bound for `passes` passes in a row (default 10, so about 5 minutes), the writer moves one of its partitions to the node that brings both closest to the mean.
  - The move never takes the target over its own bound, on size or on load, whichever measure the source is over on. The target is the coldest node, by that measure, that stays within both. Up to 1.3.0 only the source's measure was checked, so a load move could take its target over on size, or the reverse, and once the dwell ended a move took it back (#2158). The move never puts a partition on a node that holds one of its replicas.
  - Ties go to the smaller partition.
  - A partition whose copy wouldn't finish within the dwell, reckoned at the writer's `--move-rate-bytes` (64 MiB/s by default), isn't moved. At the defaults that's over about 112 GiB, beyond `partition_max_bytes`, so it matters only with a short dwell or a low rate.
- **It keeps going on the same node** while that node stays above `1 + 0.4 × (ratio − 1)` (1.1 at the default ratio), so it doesn't stop just under the bound and start again a pass later.
- **The rest (`dwell_ms`, default 30 minutes):** the partition moved, and both nodes, aren't moved from or to again until it ends. With the defaults, the cluster changes at most a few times an hour.
- **One move at a time, cluster-wide.** Nothing starts while any move is in flight, an operator's `phdb cluster move` or range move included.
- **A node that didn't answer** is neither a source nor a target, and its count of passes starts again.
- **Nor is a node with a partition it gave no numbers for** (from 1.3.1, #2158): a partition the map places on it that is closed, opening, or not open there. Its sums would leave that partition out and make the node look lighter and colder than it is, and the partition itself small and cold. The pass's `state` names it when nothing else is to say ("skip: node a's numbers are incomplete: partition p3 isn't open there"). So a partition left closed on purpose (`phdb cluster close`) keeps its node out of balancing until it's opened again or dropped.

**A balancer move is a [`phdb cluster move`](#fencing-and-relocation)** run by the writer with its cluster token, and recorded in the map under an id starting `bal-`:
- **During the move:** the partition is fenced and closed on its old node, its files are copied whole and it opens on the new one. Writes answer `Fenced` and reads `PartitionUnavailable` for about as long as the copy takes: retry them.
  - **How long:** the copy runs as fast as the disks and network allow; a whole move isn't held to a rate. As a planning figure, at 64 MiB/s that's about 16 s per GiB, so about 8.5 minutes for a 32 GiB partition.
  - **Where an outage of that length matters,** keep the balancer paused or off and move partitions by hand in a window, or pin the ones that must stay up.
- **The old node keeps its copy.** The partition's files stay there, closed, as after a `phdb cluster move` without `--drop-source`. A move off a node that was over its size bound frees no disk on it until you run `phdb cluster drop-partition` there.
- **Open transactions and pinned snapshots** on that partition end with the move. Clients begin them again once it serves on its new node.
- **Kept:** the partition's history, so change-feed tokens resume and spanning transactions homed there keep their outcomes. A partition with prepared spanning-transaction shares still in doubt is never moved.
- **Protect the cluster token** as you protect an admin token ([Securing a cluster](#running-a-cluster)): it's what the writer moves partitions with.

#### What it leaves alone
- **A pinned partition.** `phdb cluster balancer pin P --node URL` keeps P on its node; `unpin` lets the balancer act on it again. An operator's pin never expires.
- **A partition that keeps moving.** One moved whole `max_moves` times (default 3) within `bounce_window_ms` (default 24 h) is pinned by the balancer, with the reason "bouncing". That pin expires after another bounce window. An operator's `pin` or `unpin` replaces it. Splits and merges don't count: a split doesn't add to the partition's count, nor start it again.
- **A pinned range.** A table's `pinned_ranges` in the map (each a `start` and `end` split key, half-open) keeps those keys where they are. A partition holding one is never moved.
- **A partition in any move,** or with spanning-transaction shares in doubt.

#### Splits and merges
The balancer also judges partitions too big (over `partition_max_bytes`, default 32 GiB) or too hot (over `partition_max_load`, default 0.5 of a node), and small, cold ones (under `partition_min_bytes`, default 4 GiB, and cold: under a tenth of its node's mean load, or under a hundredth of `partition_max_load` whatever the mean, as in an idle cluster). It splits and merges them with the online split and merge (D331), as `phdb cluster split` and `merge` do, recorded in the map under an id starting `bal-`. Splits come first in a pass, then whole moves, then merges.
- **A split** cuts one table of the partition P at a tablet boundary, never inside a pinned range:
  - **Where:** the boundary that halves P's load, each tablet's load being the larger of its share of P's reads (sampled) and of its writes. With no load measured, it halves P's bytes. If the best cut leaves over 80% on one side (a hot key, or a narrow hot range), P is reported as unsplittable, and a whole move is considered instead.
  - **Which partition is looked at:** each pass asks one partition's node for its tablets, the one furthest over its bound among those that could split now. A partition with no cut (a hot key, or no tablet boundary in its own ranges), whose node didn't answer, or whose split couldn't start, is set aside for a dwell, so the next one over gets its turn; `state` names each partition set aside, why, and for how long.
  - **What moves:** from the cut to the end of P's range of that table that holds it. P's other ranges, and its other tables, stay.
  - **Where to:** a new partition on the coldest node holding none of P's replicas. It's named `<P>-<v>`, with `v` the map version of the change that records the split: for example `p3-1742`, and `p3-1742-1800` for a later split of that one. A name is never used twice, even after a merge drops its partition, so an old change-feed token or history can't name the new one. A split whose name would be over 64 characters, or is taken, is skipped, with that reason.
  - **The copy** runs at the source's `--move-rate-bytes`. One that wouldn't finish within the dwell at that rate isn't started.
  - **During it:** P serves throughout the copy and catch-up. At the hand-over, writes to the moving range are refused briefly (`Fenced`, retry-safe), and then go to the new partition. Once the map gives the range away, P's epoch rises, so its lease takes a round to come back: for that moment P's other writes are refused too (nothing written, retry-safe), and one in flight may answer `Unknown` (it may or may not be kept: read it back before retrying, as on any lease change).
  - **History:** P keeps its own `moves_history`, and gains a SPLIT entry; the new partition starts with none. The SPLIT event names the table and the key.
- **A merge** of S into T joins S only with a neighbour (one holding a range next to one of S's), and only while the two together stay under **half** of `partition_max_bytes` and `partition_max_load`. So a merge never makes a partition the next pass would split.
  - **How:** each of S's ranges is a range move into T, and then S is dropped.
  - **Interrupted part-way** (the writer changed, or a range move aborted), some of S's ranges are in T and S holds the rest. That's a valid layout: the next pass judges S again, and merges the rest.
  - **An S emptied but not yet dropped** (something still needs it, such as a change feed's tail, kept at most 24 h after the hand-over; see [Merging partitions](#merging-partitions)): each pass tries its drop again, and `state` says what S waits for. Meanwhile S is neither moved nor merged into, and the balancer goes on with other actions. A drop that stopped (say on a fence it left, for an operator to lift) is named in `state` the same way, and tried again each pass.
  - **A stopped balancer merge finished by hand:** if you run the rest with `phdb cluster merge S --into T`, S is still marked as the balancer's, so the balancer drops it once emptied, as the command itself would. That's safe either way.
  - Neither side of a merge was split within the last four dwells, so a split isn't merged straight back. A split aborted before its hand-over counts too: P keeps its SPLIT entry, so P isn't merged for those four dwells either.
- **Never split or merged:** a table in a co-location group, or one an index follows. Those partitions move only whole.

#### The bounds
Set them in the map's `balance` with `set-map`. Each field left at 0 takes its default:

| Field | Default | Meaning |
|---|---|---|
| `size_ratio`, `load_ratio` | 1.25 | A node's bound, as a multiple of the mean per unit of weight. Must be above 1. |
| `passes` | 10 | Passes in a row (30 s each) a condition must hold before an action. |
| `dwell_ms` | 1,800,000 (30 min) | How long what an action touched rests. |
| `max_moves`, `bounce_window_ms` | 3, 86,400,000 (24 h) | A partition moved this often within the window is pinned. |
| `partition_max_bytes` | 32 GiB | Over this, a partition is too big (a split). |
| `partition_max_load` | 0.5 | Over this share of a node, a partition is too hot (a split). At most 1. |
| `partition_min_bytes` | an eighth of `partition_max_bytes` (4 GiB) | Under this, and cold, a partition is small (a merge). At most a quarter of `partition_max_bytes`. |

#### Seeing it
- **`phdb cluster balancer status --node URL`** shows:
  - the mode and the writer;
  - `state`: the writer's last decision and why. Examples: "skip: within bounds", "skip: node a is over its bound, but no partition on it can move within the bounds", "skip: node a is over its bound (1.43x the mean size): 4 of 10 passes so far", "skip: node a is over its bound (1.43x the mean size), but rests 1200 s more" (from 1.3.1, #2158; before, both showed as "within bounds"), "moving p3 from a to c (bal-…): node a is at 1.43x the mean size", "skip: p3's best split leaves 91% on one side: a hot key or a narrow hot range, unsplittable", "paused" (followed, like a skip, by any emptied partition's removal that waits or stopped), "waiting: move … is in flight";
  - the pinned partitions, and whether the balancer or an operator pinned each;
  - the bounds in force;
  - the last 32 events: each move, split and merge, the balancer's pins and their expiry, and each abort, with its partition, nodes, reason and time (and a split's table and key).
- **A pass that does nothing changes nothing in the map.** Its reason is only in `state`, not an event.
- **Each partition's `moves_history`** in the map keeps its last 8 balancer moves, splits, merges and pins.
- **`/metrics` on the writer:**
  - `pigeonhole_cluster_balancer_actions_total{kind}`: `move`, `split`, `merge`, `pin` (a pin or its expiry) and `abort`. It counts from the writer's start;
  - `pigeonhole_cluster_node_load_ratio{node}`: each node's load per unit of weight over the mean, at the last pass.

#### When the writer changes
A new writer, after an election or a forced map, keeps the mode, bounds and pins from the map. Its counts of passes start again, so it waits out `passes` before acting.
- **A balancer move the old writer left behind:**
  - **Before its commit point** (the map still gives the partition to its old node), the new writer aborts it: the target's copy goes aside, the partition serves on its old node again, and an ABORT event says so. If the old node can't be told to serve it again just then (it doesn't answer, or its peers haven't confirmed the map yet), the balancer tries again each pass and decides nothing else meanwhile; `state` reads "waiting: P is still fenced and closed after aborted move …". That retry is the writer's own, and only for the fence its move set: it stops if the writer restarts, if another node becomes the writer, or if P's node or epoch changes. **How you notice:** after the ABORT event, `phdb cluster status` still shows P closed on its old node. Run `phdb cluster unfence P` there.
  - **Past it,** the new writer finishes the move.
- **A balancer split, or one range of a merge, that stopped** (the old writer left it behind, or the writer's own run of it stopped):
  - **Before it was handed over,** the new writer aborts it: the range serves from P (or S) again, and a split's new partition, still empty, is removed from the map. An ABORT event says so.
  - **From the hand-over on,** the new writer finishes it.
  - The rest of an interrupted merge waits for a later pass (above).
- **With the balancer off (not paused),** such a move stays recorded. A paused balancer still aborts or finishes it. Run it again with `phdb cluster move`, or end it with `phdb cluster move P --abort`.
- **The writer's lease bounds the balancer's moves at every term:** a writer cut off from the majority can't advance one.

### Backup and restore
- **`phdb cluster backup DIR --node URL`** backs up every partition, each through the node the map gives it, into a new or empty `DIR`. It writes `<partition>.phdb` for each partition, `map.json` and `manifest.json`.
  - Each node needs `--backup-dir`, and the token admin scope, as for `Admin.Backup`.
  - `--durable` copies only what each writer holds durable.
  - `--parallel N` (4) copies that many partitions at once.
  - A broken download resumes from the bytes written, on a new connection. A node that sends nothing for 60 s, with its connection up, counts as a break. Three breaks in a row with no new bytes fail the backup with the first one's reason; a break after progress starts the count again, so a long copy over a poor link still finishes. A copy that is gone from the node when the download resumes starts again from nothing, within the same three tries.
- **Not one point in time.** Each copy is a consistent snapshot of its own partition, and the set is not one point in time across partitions. A commit on one partition can be in the backup while an earlier one on another isn't. A table created during the backup can have rows in one partition's copy and none in another's.
- **Checked against the map.** The backup checks each copy's cluster, partition and epoch against the map. A copy that differs fails it, naming both, for example `the copy is partition "p2" of cluster "c1" at epoch 2, but the map says partition "p2" of cluster "c1" at epoch 1`. That means the map changed during the backup, or the node holds another file. Run it again once the map is settled.
- **A map that moves meanwhile:** a partition added, moved or relocated during the backup fails it: `the map changed during the backup`. Tables created or dropped meanwhile don't, but a table dropped and created again under its name does (`table users was dropped and created again`): some copies may hold the old table and others the new. `map.json` is the map at the end, which places them as the copies hold them.
- **A backup is complete only with its `manifest.json`,** which is written last. Copies are `<partition>.phdb.partial` until checked.
- **One partition's backup:** a node's `Admin.Backup` and `BackupDownload` back up one partition it holds, named by `partition`, and aren't forwarded. Another node's partition is `WrongPartition`.
- **`phdb cluster restore DIR --node URL`** puts a complete backup back. Each partition's copy goes to the node the backup's `map.json` gives it, is installed there at a new epoch, and the backup's map is then set. **Writes since the backup are lost.**
  - **First, fence every partition** (`phdb cluster fence`), **and close each one on the node it's restored to** (`phdb cluster close`). A partition that serves anywhere, is open on the node it goes to, or is given to a node that holds its read replica (the owner and a replica changed places since the backup, or `--map` says so), refuses the restore before anything changes, naming the command to run. For a replica: set a map without that node in the partition's `replicas`, and move its copy aside (`phdb cluster drop-partition`).
  - **Checks before any change:** every copy's size and SHA-256 against `manifest.json`, the map's partitions against the backup's, and every node: the node each partition goes to must answer. So must the node the live map gives it, unless `--old-unreachable` (its process is stopped).
  - **Then, per partition:** the copy is uploaded (`Admin.PartitionUpload`, resumed after a break; `--parallel N` (4) uploads that many at once). A node that takes nothing of the copy for 60 s, with its connection up, counts as a break. A node that takes the whole copy and then gives no answer for 60 s plus the copy's size at 8 MiB/s (about 3 h 34 min for 100 GiB) fails the restore there, with no resume: run the restore again once the node is seen to. Then the node installs it at epoch `e + 1`, one partition after another. `e` is the highest of the backup's, the backup map's and the live map's epoch for the partition, so a node still holding an older file can't write into the restored cluster.
  - **Last, the map:** the backup's map is set by the forced path (as `set-map --force`): its version goes above every reachable node's, and every table is placed exactly as the backup has it. Its writer, the node with the smallest id, must answer. A table created after the backup is dropped, with its data, and one dropped since comes back.
  - **Then served:** each partition opens on its node with the map, under the fence you set before, and the restore lifts that fence once it's open. It waits up to a minute for each, and otherwise says which isn't serving yet.
  - **`--map FILE`** installs an edited copy of the backup's map instead, for example with nodes at new addresses or other nodes holding the partitions. It must have the backup's partitions and exactly the backup's tables and indexes.
  - **`--cluster-id ID`** restores under another cluster id, onto nodes of no cluster yet, beside a live cluster: every file is stamped with it. A node in another cluster refuses the restore. A `--map` file still names the backup's cluster id: the restore replaces it with `ID`, and refuses a map naming another. A node of no cluster yet is `phdb serve --cluster DIR --node-id ID` on a `DIR` with no `cluster-map.json`.
  - **History starts over.** A restored partition is its backup's main file alone. Its WAL and feed files are moved aside to `<file>.replaced-<time>`, never deleted: delete them once the restore is checked. Feed tokens from before the restore answer `FeedExpired`.
  - **Run it again** after a failure. `DIR/restore-progress.json` records each partition installed and the map once set, and goes when the restore is done. A run again skips a partition already installed at its new epoch (no second upload), and if the map is already set and still places every partition so, it only lifts the fences. An unfinished upload resumes. If `DIR` can't be written to (a read-only snapshot), the restore warns and goes on without the file, and a run again after a failure redoes the whole restore.
    - A node restarted between the stamps and the map still starts: a partition whose file is stamped at an epoch above its map's is left unopened, with a line in the log, and `phdb cluster status` shows both epochs. The run again reaches it and sets the map, which opens it. A file behind the map's epoch, or another cluster's, still stops the node.
  - **A node that held a partition and doesn't get it back** (the restored map, or `--map`, places it elsewhere) keeps its copy open and fenced, with its memory and CPUs, and its files. The report lists those nodes (`close_and_drop_on`). On each, `phdb cluster close P`, then `phdb cluster drop-partition P` once the restore is checked.
  - **The new epoch passes every copy a node reports,** not only the maps' epochs: a move or relocation that failed after its stamp can leave a file past the map's epoch, and the restored copy's history must not share it.
- <a id="restore-one-partition"></a>**`phdb cluster restore DIR --partition P [--to ID] [--old-unreachable] --node URL`** puts one partition back from a backup into the live cluster, alone (#1832): for a lost partition, or one whose data must go back, even after P was moved, relocated or restored since the backup.
  - **What changes:** only P. Its copy goes to `--to ID` (default: the node the live map gives it), is installed there at a new epoch, past every copy of P any node reports, and the live map then gives P that node at that epoch. The other partitions, and every table, stay as the live map has them.
  - **What you lose:** P's writes since the backup. How often you back up bounds that loss. P starts a new feed history: a token with a position in P answers `FeedExpired`, so subscribe again. The other partitions keep their writes, so P is now older than they are. No batch or plain transaction spans partitions (`CrossPartition`), but a [transaction across partitions](#transactions-across-partitions) does: one that committed after the backup keeps its writes in the other partitions and loses P's, and a share of one still in doubt elsewhere whose home was P stays in doubt (P's history changed), for `phdb cluster txn decide`. An invariant your application keeps across partitions may not hold either.
  - **First, fence P** (`phdb cluster fence`), **and close it on the node it goes to** (`phdb cluster close`), as for a whole restore. The node the live map gives P must answer, unless `--old-unreachable` (its process is stopped).
  - **The tables:** the live map's. When P opens, its node reconciles the copy's schema to them, as for any partition: a family added since the backup is added, a table created since comes back empty, and one dropped since goes. So does an index (a text index then fills from its source rows; a vector index stays empty until written). The report lists them (`tables_empty_since_backup`, `tables_dropped_since_backup`). A table or index whose key range on P changed since the backup (its splits, or the partitions it's on) refuses the restore, since no reconcile moves rows between partitions: restore the whole cluster, or put the splits back first. A split moved elsewhere in the table doesn't refuse it.
  - **The map** is set as a relocation sets it: checked against the live map, never forced, so a map changed meanwhile refuses it, and a run again goes on. The restore then waits for a majority of the nodes to hold the map, as `phdb cluster move` does, and lifts P's fence once it's open.
  - **Where the copy goes:** the upload is staged in the node's `DIR/.phdb-incoming`. The install then moves P's files there aside (`.replaced-<time>`, never deleted) and renames the copy in, with a marker that a stamp or an open finishes after a crash: P is either its old files or the backup's copy, never part of each. When `--to` is the node that holds P now, P must be closed there first.
  - **Run it again** after a failure. `DIR/restore-progress-P.json` (apart from a whole restore's file) records the node and epoch of the install before it starts, and the map once set. A run again works out the epoch again from the live map: it reuses an earlier install only if that epoch still fits, and with the map set and still placing P so, only waits and lifts the fence.
  - **`--map`, `--cluster-id` and `--parallel`** don't go with `--partition`: the live cluster is the backup's, and only P's placement changes. A partition the live map no longer lists isn't restored this way: restore the whole cluster, or add it back to the map first.
  - **If `--to` moved P,** its old node keeps its copy, closed or fenced, with its files. The report lists it (`close_and_drop_on`).
  - **With `--old-unreachable`,** the new epoch passes only the copies of P the nodes that answer report. A copy on the node that doesn't answer, stamped past the map by a failed move or relocation, can share the restored epoch, but it never serves: the map gives P to `--to`. Drop it there (`phdb cluster drop-partition`) once that node is back.

### Partitions per node
A node serves every partition its map gives it ([D317](../design/decisions/phase-3.md#d317)). **In the runs below, several partitions on one node cost nothing measurable (up to 8 partitions) and didn't change its capacity (up to 4), as long as the node's totals were set.** Capacity follows the node's totals and its network threads, not the number of partitions.

**Measured** on the reference machine, a 16-core AX102 (#1482), with 20M rows of 100 B split evenly by key range and two rounds at each N:
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
- **The shards pin first:** with several partitions, every partition's shards are pinned before any compaction thread, and the compaction threads share the node's compaction CPUs. So the network side keeps the same CPUs as with one partition. Before this placement and node-total split, four partitions on one node gave 25–34% less throughput than one; with them, the runs above found no measurable difference: up to eight partitions at a fixed load, and up to four at saturation.
- **`--engine-memory` is the node's total too.** The map's `memory_bytes` for a partition is taken first, and the rest is split equally across the node's other partitions. Set it when a node holds several partitions, or each takes the library's full defaults.
- **The startup lines** give each partition's shard and compaction threads (pinned, confined or unpinned) and its memory. On Linux, a partition with unpinned threads is a warning. `/metrics` has the same per partition.
- **`--change-feed` is split by shards** (#1786): partition i keeps `--change-feed` × its shards ÷ the node's shards, so every shard on the node keeps the same history, and the node's feed disk is what one database of all its shards would keep. A partition whose feed recorded another shard count keeps that count, so its shards each keep more or less history than the others', within the same node total. With `--change-feed 4294967296` (4 GiB), a 16-shard node writes about 9 GiB of log at start, as one database or as 8 partitions of 2 shards (1.2 wrote about 37 GiB for the 8 partitions).
  - **Each partition keeps at least two WAL segments (64 MiB) a shard,** so a total below that across the partitions is kept as more, and the start says so.
  - **Fixed while open:** a partition opened while the node serves takes its share for the new set, the partitions already open keep theirs, and one that arrived with more history (a move) keeps it. The node can then keep more than `--change-feed` until a restart re-splits it, and the open logs that. A restart that gives a partition a smaller share drops its oldest history: a token behind it answers `FeedExpired`, and the start logs it.
- **Sessions and feeds:** `--max-sessions`, `--max-sessions-per-token`, `--feed-ring-bytes`, `--max-feed-hubs` and `--max-catch-up` count across all of a node's partitions. A feed with tables on two of a node's partitions holds two hubs there.
  - `--max-feed-hubs` also caps the node's shared cluster feed merges ([Limits](#limits)). Those are counted on their own, apart from the partitions' hubs, so a node can have up to that many of each.
  - A partition's hub whose only readers are shared merges takes a share of `--feed-ring-bytes` like any other. So a node running shared merges over its own partitions splits the budget over both: 16 shared merges over one local partition's 16 hubs make 32 rings, each a thirty-second of it.

**What a partition costs** even so: each is a whole database engine, with costs that don't shrink with its share of the load:
- at least one shard's memtable arena (64 MiB by default);
- its own WAL, so its own group syncs: the same load over more partitions means more syncs, each covering fewer commits;
- its own block and row caches, so a busy partition can't use an idle one's;
- its own flush and compaction.

A few partitions per node leave room to move one to another node later.

### Choosing split keys and hot ranges
Once a table's ranges hold data, its split keys change only through a range move ([Moving and splitting ranges](#moving-and-splitting-ranges)), which copies the range. So choose them from how your keys are shaped:
- **Tenant prefixes** (`tenant/…`): split at tenant boundaries. A tenant's rows stay in one partition, so its batches and transactions stay in one, and a scan of one tenant reads one partition. A big tenant can be given a partition of its own.
- **Hashed prefixes** (a byte or two of a hash in front of the natural key): split evenly over the prefix space for an even spread of rows and load. The cost is key order: a scan in natural order reads every partition.
- **Time-bucketed keys:** don't lead with the time. With fixed split keys every new write lands in the last range, so one partition takes all the writes. Put a tenant or hashed prefix first and the time after it.

**A range that turns out hot:** split it onto a new partition on another node (`phdb cluster split`), or move it to a less busy partition (`phdb cluster move-range`): [Moving and splitting ranges](#moving-and-splitting-ranges). Or give its whole partition a node of its own or a bigger one, by moving the partition ([Fencing and relocation](#fencing-and-relocation)).

### Moving and splitting ranges
From 1.3.0 (D331), a key range of a table moves from one partition to another while both serve:
- **`phdb cluster move-range TABLE --from S --to T [--start K] [--end K] [--skip-fit-check]`** moves `[K_start, K_end)` of `TABLE` from partition `S` to partition `T`. The range must lie inside one of `S`'s ranges of the table; without `--start` or `--end` it runs from that range's start or to its end.
- **`phdb cluster move-range --resume ID`** goes on with move `ID` from the phase the map records it at, to its end. Use it for a move or split whose command stopped, or one the balancer started, when all you have is its id (`phdb cluster status` lists it).
- **`phdb cluster split TABLE --from S --key K --to N [--id ID] [--skip-fit-check]`** moves `S`'s range of `TABLE` from `K` to its end onto a new partition on node `N` (`--node` is the URL of the node the command talks to, as for every `phdb cluster` command). The new partition's id is `ID`, or by default `S-<v>`, where v is the map version of the change that records the split (`p3-41`, say). It's fixed for the split's life, and never reused. If that id is taken or isn't a valid partition id, the split is refused: give one with `--id`. `N` creates its file empty first, then the map adds it. `K` can't be the range's start: that's a move of the whole range.

**Before it starts, each one checks that the copy fits** on `S`'s node:
- **The copy time** is the range's bytes at `S`'s node's `--move-rate-bytes`. The command can't measure one range's bytes, so it counts all of `S`'s (`file_bytes` in `phdb cluster status`, an upper bound).
- **The writes during the copy** are `S`'s device bytes written a second, over that time. Its flushes and compactions count too, so this overestimates what `S`'s change-feed hold keeps. The rate is `write_bytes_rate` in `phdb cluster status`, reported after each partition's first load interval (30 s); the command waits for it.
- **If they pass `--move-retention-cap`, the command refuses**, and the message says it used the whole-partition bound. A small range in a big partition fits anyway: run it again with `--skip-fit-check`. The cluster balancer measures the range's own bytes and never skips the check.
- **If `S`'s node is already copying `--max-move-followers` moves, the command refuses with `Busy`.** That's safe to retry once one of them finishes.
- **A command run again for a move the map already records** skips the check and goes on.

Both run on the map's writer, through its own map changes, and print each phase as it starts:
1. **COPYING:** `T` copies the range from snapshots of `S`, at most `--move-rate-bytes` from `S`'s node, while `S` serves it as before.
2. **CATCHING_UP:** `T` applies `S`'s writes to the range from its change history, until it's within about a second of `S`.
3. **HANDING_OVER:** `S` refuses writes to the range (`Fenced`, reason `RANGE_MOVING`, retry-safe; nothing was written) until `T` has applied the last of them, usually well under a second. Reads go on. A transaction begun before then that writes on `S`, a spanning one's branch included, is refused at its commit or prepare (`Fenced`, retry-safe): retry it.
   - **A transaction share prepared on `S` that may touch the range** holds the move at CATCHING_UP until its home decides it, so the move never cuts it off. If the fence's drain (the writes and shares already running on the range) lasts more than about 30 s, the move aborts and the range takes writes again.
   - **The whole of HANDING_OVER is bounded** (1.3.1, #2160). A running move aborts if its target hasn't applied through the fence's seqno within 60 s, and the range takes writes again.
   - **A move that stops here** (its command killed or past `--timeout`) leaves the range's writes refused until the map's writer aborts it, about 2 minutes after it first saw the move at HANDING_OVER, or until you run the move again or end it with `phdb cluster move-range --abort ID`.
4. **HANDED_OVER:** the map gives the range to `T`. This is the commit point: from here the move only goes forward. `S`'s epoch rises in the same change, so its lease takes a round to come back. Meanwhile `S`'s other writes are refused, and are safe to retry. One in flight may answer `Unknown`.
5. **CLEANING:** `T` serves the range, and `S` drops its copy. Then the move's record leaves the map.

**Rerun and abort:**
- **A rerun of the same command goes on** from the phase the map records, after a crash or a timeout. A different move of a partition already moving is refused, naming the move in flight.
- **`phdb cluster move-range --abort ID`** stops move `ID` (a split's too) before its commit point: `T` drops what it copied, `S` serves the range as before, and a split's new partition leaves the map with the record, its file moved aside into `.phdb-dropped/` once it's empty (never deleted). Replica copies of the split's partition, if an operator added any before the abort, are closed by the map and left on disk: `phdb cluster status` lists them as a partition the map doesn't have, and `drop-partition` moves them aside. A range move's target that's down when the abort lands cleans up when it comes back. A split's record stays ABORTING until its new partition has dropped what it copied: if its node is down, rerun the abort once it's back. Past the commit point the abort is refused: rerun the move to finish it.
- **A failover of `S` or `T` before the hand-over aborts the move.** From HANDING_OVER until the move is done, a failover or a promote of either partition is refused.

**Limits:**
- **Every node runs 1.3.0** (the map features `range_moves` and `move_aborting` in `phdb cluster status`).
- **One move per partition at a time,** and none of a table in a co-location group or one an index follows.
- **`--max-move-followers`** moves copy from one node's partitions at once; a further one waits (`Overloaded`). `--move-retention-cap` bounds the change history a move keeps on `S`: a copy that would outrun it is refused before it starts, and one that does anyway must be aborted.

**What to watch:** `phdb cluster status` shows each move's phase on `S` and `T`, the bytes copied, how far `T`'s catch-up is behind, what `T` waits for (an earlier abort's drop of an overlapping range) and why a move stopped, if it did. `T`'s `/metrics` has `pigeonhole_cluster_move_follower_lag_ms{move}`, how far behind `S`'s commits each move's catch-up applies; `S`'s has `pigeonhole_cluster_move_copies_refused_total`.

**Backups and restores:** a whole restore ([Backup and restore](#backup-and-restore)) ends every range move in flight: the backup's map has no move record, and each restored file is clipped to that map's ranges, so a range a move had half copied isn't served twice. `restore --partition` is refused while a move names the partition.

**Change feeds:** a subscription to the table goes on across the move, in per-key order: it reads the range from `S` until the hand-over, then from `T`. A token from before a move's hand-over still resumes while the move's rows are kept. Once they're pruned, such a token answers `FeedExpired` on any copy of `S` or `T`: open the feed again. A subscription from the oldest position starts, on each of `S` and `T`, past a pruned move's history of the table there, so it never replays a range out of order.
- **When a move's rows are pruned:** each node prunes its own partitions' rows, about once a minute. It prunes once the move has left the map, every node holds a map from after the hand-over, the transaction lifetime plus the request timeout have passed, and the partition's change-feed retention has passed the hand-over.
- **An emptied source's rows:** a partition the map no longer places anything on (no table range, index or memory store, as a merge's source once its last range has moved) takes no writes, so its change-feed retention, kept by bytes, never passes the hand-over. Its moved-away rows go instead 24 hours after each move's hand-over, under the same other conditions, so older moves' rows may go as soon as it empties. The 24 hours are fixed. A token from before the move's last event on that partition then answers `FeedExpired`; one that had read up to it resumes and reads on from the target.
- **After a merge removes a partition:** a token that names it answers `FeedExpired`: subscribe again. That holds even for a token that had read to the merge's end, so a consumer that caught up and then stalled for more than the 24 hours must subscribe again too. A partition still listed but not served answers `PartitionUnavailable`, which is worth retrying.
- **Resuming through a node whose map is behind:** a token can name a partition, such as a split's new one, that the node you resume through hasn't heard of yet. That node first pulls a newer map from the others. If none has one yet, the resume answers `PartitionUnavailable`: retry it. It doesn't answer `FeedExpired`.

### Merging partitions
From 1.3.0 (D331), **`phdb cluster merge S --into T [--skip-fit-check] [--timeout SECS]`** merges partition `S` into partition `T` under writes, then removes `S`:
1. **Each of `S`'s ranges moves into `T`,** one after another, as `move-range` moves a range: same phases, same fit check (here `S`'s whole file is the exact size), same refusals. A hand-over that leaves `T` with two neighbouring ranges of a table joins them into one.
2. **Then `S` leaves the map** once nothing still needs it, and its files are moved aside into `.phdb-dropped/` on its node and on each replica node (never deleted).

**Refused:** a partition that holds an index's placement or the agent memory store's (move it whole with `phdb cluster move`), and a table in a co-location group or one an index follows.

**Run it again to go on.** There's no merge record: the map's ranges are the state. A rerun after a stop, a timeout or a crash resumes or aborts the range move it left (by its id), then moves whatever `S` still holds. A half-done merge is a valid map, so you can also leave it.

**When `S` is emptied but stays listed.** `S` keeps serving, holding no range, while something still needs it. The merge then ends with `source: waiting` and says what. So does `phdb cluster status` (`drop_waits` on `S`'s partition on its node). In order:
- **Its change feed's tail:** a subscriber's token from before a hand-over may still resume from `S`. `S`'s moved-away row for that move is pruned 24 hours after the hand-over, or sooner if `S`'s feed retention passes it. A resume after that answers `FeedExpired`: open the feed again.
- **Transaction outcomes homed on `S`:** kept until every branch has acknowledged and `--txn-outcome-retention` has passed. A branch node that restarted before it acknowledged leaves the record for ever (#2098). Until `merge --force` (a follow-up, [#2132](https://github.com/CodingAnarchy/pigeonhole/issues/2132)), such an `S` stays listed.
- **Spanning transactions homed on `S` still open:** bounded by `--txn-lifetime-ms`. An emptied `S` refuses a new commit point (`Conflict`, retry-safe).
- **A share in doubt,** of `S`'s own or on any node with its home at `S`: its resolver settles it through `S`. For a share whose branch never acknowledged and whose node is gone, decide it with `phdb cluster txn decide`.

Run the merge again later; it removes `S` once the list is empty.

**The removal,** each step re-checked:
- **Checks:** `S`'s owner copy must be serving under its lease at the map's epoch, with nothing waiting.
- **Fence, then remove:** the merge fences `S` and checks again. The map change that removes `S` applies only if the map hasn't changed since the first read, so a failover between the two never removes a live `S`. On a changed map, the merge lifts its fence and starts again, up to 5 times.
- **Then drop:** it closes `S` and drops its files on its node and each replica node.
- **A stop that leaves `S` fenced** says so, naming the node and `phdb cluster unfence S --node URL`. The merge never lifts a fence it didn't just set: lift it, then run merge again.
- **A stop after `S` left the map** leaves files the map doesn't have. `phdb cluster status` shows each as "the map has no such partition", on every node that holds one. A rerun of the merge finds and drops only those files, of this cluster and named `S`, and prints each node and file. A split's new file, created before the change that records it, shows as "the map has no such partition yet" and is left alone. A node that doesn't answer, or a copy still open or still listed there, is left too: the merge's `detail` says what's not done, and a later rerun goes on with it.
- **`drop-partition` is refused on a node the map lists as one of the partition's replicas.** The exception is the copy an unfinished promote left there, which failover's message tells you to drop.

**After the removal:**
- **A feed token naming `S`** answers `FeedExpired`, even one that had read `S` to its end: subscribe again. A token taken after the subscription passed the merge doesn't name `S`.
- **A share in doubt homed at `S`** can't appear from a live node, since the removal waits for every one. A restore of a branch partition from a backup taken before the merge can bring one back. Its resolver keeps asking, and `phdb cluster status` lists the share with the reason "the cluster map here (version …) doesn't list the transaction's home partition": decide it with `phdb cluster txn decide`.
- **`S`'s id can be used again** by a later partition. An old transaction id or feed token naming `S` never reads or decides the new partition's data. A feed token answers `FeedExpired` ("the partition was dropped and created again"): subscribe again. A transaction is refused at its commit, and writes nothing.

## Performance
These numbers are for `phdb serve` on one machine, serving a single database file and also as a one-node cluster. All rows use a fixed offered write rate. They show latency and feed delivery at that rate, not peak throughput.

**Setup**

- **Machine:** AMD Ryzen 9 7950X3D (16 cores, 32 hardware threads), 128 GB RAM, local NVMe SSD, Linux.
- **CPU split:** the server is pinned to 20 hardware threads (10 cores) and the load generator to the other 12.
- **Server:** 4 shards. Memory, compaction and thread settings are the defaults: 9 network threads, 3 feed threads, 1 feed hub thread and 12 blocking threads. Change feed is on.
- **Transport:** gRPC over HTTP/2, plaintext TCP on loopback, no authentication.
- **Writes:**
  - 16 concurrent writers send `Data.Mutate` (one cell each, 100-byte values) at the default durability (group sync).
  - Puts follow a fixed schedule. A put's latency counts from when it was due, so a server that falls behind can't hide the delay.
- **Subscribers:**
  - `Feed.Subscribe` streams use the durable feed, 64 streams per connection.
  - "256 subscribers" means 16 subscriptions to different table sets with 16 streams each. "32 subscribers" means 16 subscriptions with 2 streams each.
- **Duration:** each row is a 30 s put loop, except the 256-subscriber rows at 55,000 puts/s, which are 90 s.
- **Versions:** the two 256-subscriber rows at 55,000 puts/s are the 1.3.0 release candidate's: three runs each, alternating between the cluster and the single file, in one session. Every other row was measured on 1.2, in an earlier session.

**Mutate latency and feed delivery**

| offered puts/s | subscribers | server | put p50 | put p99 | put p99.9 | commits delivered per subscriber | feed lag p50 | feed lag p99 |
|---|---|---|---|---|---|---|---|---|
| 20,000 | 0 | single file | 0.09–0.10 ms | 0.18–0.20 ms | 0.24–0.25 ms | | | |
| 20,000 | 0 | one-node cluster | 0.09–0.10 ms | 0.18–0.20 ms | 0.23–0.24 ms | | | |
| 20,000 | 1 | single file | 0.10–0.11 ms | 0.18–0.20 ms | 0.23–1.2 ms | all | 0.1 ms | 0.2 ms |
| 20,000 | 1 | one-node cluster | 0.11 ms | 0.18–0.19 ms | 0.24–0.25 ms | all | 0.1 ms | 0.2 ms |
| 20,000 | 256 | single file | 0.14–0.15 ms | 1.5–1.7 ms | 2.9–3.5 ms | all | 1.4–2.0 ms | 12–48 ms |
| 20,000 | 256 | one-node cluster | 0.15–0.17 ms | 2.0–2.3 ms | 4.0–9.7 ms | all | 1.2–1.3 ms | 21–122 ms |
| 55,000 | 0 | single file | 0.15–0.16 ms | 0.24–0.27 ms | 3.1–4.5 ms | | | |
| 55,000 | 0 | one-node cluster | 0.15–0.16 ms | 0.25–0.32 ms | 8.4–19 ms | | | |
| 55,000 | 1 | single file | 0.16–0.17 ms | 0.27–0.36 ms | 5.6–6.1 ms | all | 0.2 ms | 0.3 ms |
| 55,000 | 1 | one-node cluster | 0.17 ms | 0.30–0.31 ms | 5.6–5.8 ms | all | 0.2 ms | 0.3 ms |
| 55,000 | 32 | single file | 0.17–0.18 ms | 0.71–1.1 ms | 11–14 ms | 56–58% within the run | 4–6 s | 11–17 s |
| 55,000 | 32 | one-node cluster | 0.18 ms | 0.67–1.1 ms | 9.4–13 ms | 51–52% within the run | 6–7 s | 18 s |
| 55,000 | 256 | single file | 0.20 ms | 32–42 ms | 53–100 ms | 50–51% within the run | 17 s | 52 s |
| 55,000 | 256 | one-node cluster | 0.20–0.21 ms | 16–64 ms | 44–122 ms | 52–55% within the run | 10–17 s | 49–52 s |

**Reading the table**

- **Every row sustained its offered rate:** 20,000 or about 54,750 puts/s.
- **At 20,000 puts/s, every subscriber keeps up**, even at 256. Writers' p99 rises from about 0.2 ms to 1.5–2.3 ms with 256 subscribers.
- **At 55,000 puts/s, one subscriber keeps up**, but 32 or 256 do not.
  - Delivery trails the writes by seconds, so each subscriber received only about half of the run's commits before it ended.
  - Subscribers that fall behind catch up from the log rather than being dropped.
- **A one-node cluster matches a single file** up to 32 subscribers at 55,000 puts/s.
- **With 256 subscribers at 55,000 puts/s, the cluster's writers wait about as long as a single file's:** put p99 of 16–64 ms against 32–42 ms, and p99.9 of 44–122 ms against 53–100 ms, over three runs each. The cluster's runs spread wider. Before 1.3.0 the cluster's put p99 at this load was 0.55–1.46 s; shared merges (#1798), batched hand-off (#2012, #2020) and its yield to puts (#2031) closed that gap.
- **Feed lag** is measured from a put's send time to its arrival at a subscriber, so it includes the put's own latency.

## Next
- [Observability](observability.md): the engine's metrics, alerts and dashboard.
- [Using Pigeonhole from AI agents](ai-agents.md): `phdb mcp`, and remote MCP's client configuration.
- [Recovery](recovery.md): `OutcomeUnknown`, and operation ids.
- [Change feed](change-feed.md): tokens, retention and delivery.
