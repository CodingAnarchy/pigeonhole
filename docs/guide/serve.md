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
phdb serve: serving app.phdb on 127.0.0.1:7448 (no TLS, no tokens; 8 network, 2 feed and 10 blocking threads on 10 free CPUs, unpinned)
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
- **The bind rule:** an address other hosts can reach needs both TLS and tokens, or the server refuses to start. `--insecure-no-tls-no-auth` overrides that for a trusted private network. A loopback address needs neither.

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
- **`ns=a,b`** limits a token to those agent-memory namespaces ([Agent memory](#agent-memory)).

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
`/mcp` serves `phdb mcp`'s tools over MCP's Streamable HTTP transport: [the agent guide's remote section](ai-agents.md#remote-mcp-phdb-serve) has the client configuration, scopes and sessions. Its own flags are `--mcp-max-result-bytes`, `--mcp-max-value-bytes`, `--mcp-session-idle-ms`, `--mcp-session-lifetime-ms`, `--mcp-session-requests` and `--mcp-session-cache-bytes`.

## Reads, writes and sessions
- **Scans** stream pages of up to `page_bytes` each (default 1 MiB). Over gRPC, `Data.Scan` is a server stream. Over HTTP, `POST /v1/Data/Scan` answers one JSON page per line (`application/x-ndjson`), or server-sent events with `Accept: text/event-stream`. A scan that fails partway ends with a final error message: an NDJSON line `{"error": ...}`, or an SSE `event: error`. Each page's `next` resumes the scan later, from any server process. The scan is complete only when the last page's `next` is empty; a page budget too small for anything ends the stream with an empty page and a position, so resume with a larger `page_bytes`. Each page reads at its own snapshot: a row is never torn, but rows can change between pages. A client that stops reading for `--stream-idle-ms` (default 30 s) gets `DeadlineExceeded`, and resumes from the last page's `next`.
- **Transactions** (`Transactions.Begin`, `Get`, `GetRow`, `Write`, `Commit`, `Abort`) are held on the server between calls, under an opaque `txn_id` bound to the token that began them:
  - **One call at a time.** A second call while one runs gets `SessionBusy`; send it again when the first answers.
  - **`Commit`** runs to its outcome even if the client goes away. Sent again, it answers the first one's outcome and commits nothing twice. After `Commit`, other calls answer `TransactionFinished`.
  - **Leases:** a transaction idle for `--txn-idle-ms` (30 s), or older than `--txn-lifetime-ms` (5 min), expires. Before any `Commit`, that's `TransactionExpired`: nothing was written, so begin again. A call in flight never expires.
  - **`NotFound` for a `txn_id` you sent `Commit` to** (after a restart, or once its record expires) means the outcome is unknown: check an operation id the transaction wrote before retrying ([recovery](recovery.md)).
  - A read token may begin, read and commit a transaction without writes; writing needs a write token.
- **Pinned snapshots** (`Snapshots.Open`, `Release`) make reads agree across requests: pass the `snapshot_id` as `GetRequest.snapshot_id`, or as `Selection.snapshot_id` for `MultiGet`, `ReadRow` and `Scan`. Each read renews the snapshot's lease (`--snapshot-idle-ms`, 60 s; at most `--snapshot-lifetime-ms`, 10 min). A held snapshot holds back version and blob GC, so release it when done. Reads already running when it's released finish; later ones, and a scan's later pages, get `NotFound`.

### Errors and retries
- **Errors** carry ICR 0112's stable code: a gRPC status detail, or the HTTP JSON body (`{"code": "TableNotFound", ...}`). Retry a write only when the error says `retry_safe`. A bare transport error means its outcome is unknown. A read may be retried; one that failed on `Io` says so (`retry_from: REQUEST`). `MergeFailed` doesn't: a merge operator that fails on stored data fails the same way again. `Unavailable` (503 with `Retry-After`, not applied, retry-safe) means a service the server depends on failed or can't be reached: today `Memory`'s embedding endpoint (see Agent memory below), or, on a replica, a memory store the replica doesn't have yet. `FailedPrecondition` (409, not applied, don't retry) means the server's role refuses the call: `Admin.Promote` on a server that isn't a replica, a replica asked for a bootstrap copy or to be followed (see [Replicas](#replicas)), or a `DropTable` of the served memory store's tables.

The rules a client keys on (D313 B3, [ICR 0112's error table](../design/icr/0112-network-api.md#errors)):

| The response | The request was | Retry |
|---|---|---|
| `retry_safe: true` with `retry_from: REQUEST` (`Busy`, `Overloaded`, `SessionBusy`, `Closed`, `ReplicaBehind`, `Unavailable`, `DeadlineExceeded`, and a read's `Io`) | not applied | send it again, after `Retry-After` where given |
| `retry_safe: true` with `retry_from: BEGIN` (`Conflict`, `SnapshotExpired`, `TransactionExpired`) | not applied | run the transaction again from `Begin` |
| `retry_safe: false` with a validation, schema, scope or role code (`InvalidArgument`, `TableNotFound`, `PermissionDenied`, `ReadOnly`, `FailedPrecondition`, ...) | not applied | no: fix the request |
| `retry_safe: false` with a 503 the server can't help (`WriterLocked` and the other codes for a database it can't open) | not applied | no: the server's database needs attention |
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
  - `Admin.BackupDownload` streams one: over gRPC as chunks ending with the file's SHA-256, over HTTP as the file with its id and SHA-256 in headers. Its copy waits in `<backup-dir>/.phdb-serve-downloads` (private to the server) for 5 minutes after the last read, so a broken download resumes from an offset (over HTTP, `Range: bytes=<offset>-` and `?backup_id=`). At most two copies are kept.
  - **Allow for the silence before the first byte:** the server backs up and hashes first. A stream that ends without its final chunk (the one with the size and SHA-256) is incomplete: resume it. Check the SHA-256 before relying on the copy.
  - **One at a time:** backups run one at a time by default (`--max-backups`), off the threads reads use.

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
  - **`Vectors.BulkLoad` fills an empty index** faster than one `Insert` per vector. It's a client stream: the first message names the index and about how many vectors follow (`expected`, which only sizes the lists), and every message carries a batch of items (`id` and `vector`), all in one element type. Over HTTP the body is NDJSON: one `VectorBulkLoadRequest` per line, in the JSON mapping, with or without `Content-Length`.
    - **A failed load leaves the index loaded up to the failure, and rebalanced.** That covers a vector of another element type or dimension (the server checks each against the index before the load takes it), a stream that breaks off, and a server shutting down (`Closed`, "interrupted": the load stops taking vectors at its next message, then writes and rebalances what it took, which for a large load can run past the shutdown grace period). Every vector before the failure is written and searchable.
    - **The index must be empty,** so a failed load can't simply be resumed with `BulkLoad`: `Insert` the rest, or drop the index and load again.
    - Each message (each line over HTTP) holds its share of `--inflight-bytes` until the load takes its last vector, and the server reads one message ahead of the load at most. A client that sends faster than the index loads is held back by the transport's flow control, not buffered. A line longer than `--max-request-bytes` is refused.
    - **The load holds a working set of its own, outside `--inflight-bytes`.** It clusters a sample of the first max(50,000, `expected` / target list length) vectors before it writes any (about 300 MB at 1,536 `f32` dimensions), then keeps up to 256 vectors per list until each list's batch is written. One load runs at a time per server; another waits for it, up to the request timeout or its own deadline. `/metrics` shows the running load's phase and the vectors and bytes handed to it.
    - The load runs on a thread of its own (`phdb-serve-bulk-load`), off the shards' cores where the engine pinned them, as the rebalancers do. Its writes still fill memtables quickly, so other writers can meet room and pacing stalls while it runs. The server's shutdown waits for a running load to write what it received.
- **The BM25 text index** (`pigeonhole-text`, ICR 0105): `Text.CreateIndex` (the source column, as a table, family and qualifier, and the index options; `if_missing` opens an existing one as it is), `Put` (writes the document's text to the source column and indexes it, in one transaction), `Delete`, `Search`, `Rebuild` and `Verify`.
  - `Search` takes the query syntax (`syntax`), or `clauses` in order. A clause's `term` (and a fuzzy clause's `term`) is matched as given, after the index's normalisation, so `March` misses an index of lowercased text. Its `text`, and a phrase's `text`, are tokenised as documents are, as the syntax's words and `"..."~N` phrases are. A repeated clause is refused, since it would count once and shift the clause indexes that `highlight`'s matches report.
  - `k` is 1 to 10,000. With `highlight`, each hit carries its text and every match's byte range, by the request's clause index. `id_prefix` keeps only ids under a prefix.
  - `Put` and `Delete` are writes, so they answer their real outcome even if the client goes away. `Search` and `Verify` run on the blocking pool. `Rebuild` rereads the whole source column, so it runs on a thread of its own (`phdb-serve-text-rebuild`, off the shards' cores), one at a time: a second is `Overloaded`. The server's shutdown waits for a running rebuild to finish before it closes the database, even past `--shutdown-grace-ms`: a rebuild can't stop partway.
  - Dropping a text index's table drops the server's handle the same way (#1588): the next call on the name answers `TableNotFound`, and an index created again under it is the one served.

## Agent memory
- **Agent memory** (`pigeonhole-memory`, ICR 0107): with `--memory NAME`, the server opens (or creates) that store at startup (a replica opens it and never creates it: see [Replicas](#replicas)), with `phdb mcp`'s memory flags (`--embed-url`, `--embed-local`, `--memory-model`, `--memory-dim`, and so on), and serves it as `Memory`: `Remember`, `Recall`, `MarkUsed`, `Supersede`, `Link`, `Forget` and `Watch`. A fleet of agents then shares one store. Without `--memory`, these calls answer `Unsupported`. `Schema.DropTable` of the served store's own tables (`mem_NAME`, its text index's and its IVF index's: `Store::table_names`) answers `FailedPrecondition` while the server serves it (#1651).
  - **Namespaces:** a token line with `ns=a,b` (`write ns=team-a <token>`) reaches only those namespaces, on every call: the namespaces a call names, the namespace each memory id carries (`MarkUsed`, `Supersede`, `Link`, `Forget`, a link's target), and a watch's. A call that names one outside them is refused whole with `PermissionDenied`, never trimmed, and a scoped token must name its namespaces to watch. Without `ns=`, a token reaches every namespace, so a server is one trust domain: run a server or a store per tenant if tokens can't be scoped.
  - **Scopes:** `Recall` and `Watch` need read scope; the others write (`Forget` included: forgetting is ordinary agent behaviour, and `ns=` bounds its reach).
  - **Embeddings:** with `--embed-url`, the server embeds the text and refuses an embedding sent with it. Each embedding call waits at most `--embed-timeout-ms` (10 s, shorter than `phdb mcp`'s 30 s), and memory calls run at most `--memory-calls` at once (half the blocking threads by default), so a slow embedding endpoint can't take every blocking thread. An endpoint that's unreachable (refused, or no connection within half of `--embed-timeout-ms`, at most 3 s) or failing (5xx, 408, 429) answers `Unavailable` (retry-safe); one that takes longer than `--embed-timeout-ms`, `DeadlineExceeded`; one that refuses the text (400, 413, 422), `InvalidArgument`; and any other answer, or a vector of the wrong dimension, `Internal`, since that's the server's `--embed-url`, key or model. With `--embed-local` (a `phdb` built with the `embed-local` feature), the model runs in the server, on at most 4 threads placed where the network threads are, off the shards' CPUs. `--embed-timeout-ms` doesn't bound it, since it's CPU work, but `--memory-calls` still bounds how many embed at once. candle also runs large steps on a process-wide pool of its own, which only the environment places: with the shards pinned and `CANDLE_CPU_MASK` unset, while either of `CANDLE_CPU_MASK` and `CANDLE_NUM_THREADS` is unset, the server prints the `export` line to restart with: its placement's CPUs for the mask, and the embedder's thread count for the size. Without either flag, a store pinned to a model takes the caller's embeddings: `f32`, or `f16`/`bf16` widened.
  - **Recall** returns at most `k` (1 to 1,000) hits within `byte_budget`: 1 MiB when it sets none, 8 MiB at most. `with_signals` adds each ranking signal's score, and `with_embeddings` the stored vectors.
  - **Ids** are a memory's byte form (`MemoryId::to_bytes`), base64 in JSON. Deleting a memory that superseded another brings the other back into recall.
  - **`Watch`** streams new memories in the namespaces named, with a progress event for commits that bring none; each event's `token` resumes a later watch (`resume`, or `Last-Event-ID` over SSE). A token is a commit boundary: every event of one commit carries the same token, which points after the whole commit, so resume from it only once you've handled all of that commit's events. It holds a `--max-streams` slot and ends when the client takes nothing for `--stream-idle-ms`.
  - The writes answer their real outcome even if the client goes away.

## The change feed
- **The change feed** (`Feed.Subscribe`) streams committed changes of some tables, in commit order, once each (the database needs `Options::change_feed` on for them).
  - **Feed clients should turn on HTTP/2 adaptive flow control** (tonic's `Endpoint::http2_adaptive_window(true)`; grpc-go and grpc-java estimate the bandwidth-delay product unless a fixed window is set). With fixed windows, events queue in flight ahead of each subscriber. At 16 subscribers that was about 18 ms of delivery lag, which adaptive windows took to 0.5 ms p50 (perf287 on #1397). Larger fixed windows made it worse. `--http2-send-buffer` bounds what the server queues per stream.
  - Over gRPC it's a server stream. Over HTTP it's NDJSON, or server-sent events whose `id:` is each event's token, so an SSE client's `Last-Event-ID` resumes it.
  - Every event's `token` resumes right after it, from any server process. A stream that delivers nothing for about 10 s, while its position moved past commits its `prefix` skipped, sends a `progress` event with the new token.
  - Streams on the same tables share one subscription in the server: the database wakes and reads for it once, whatever the number of subscribers.
  - **Over gRPC, each commit is encoded once for every stream with no `prefix`,** and those streams send the same bytes. A stream with a `prefix` gets its own encoding of the rows that match. The HTTP feed (NDJSON or SSE) still renders each event as JSON per subscriber, so many subscribers cost more over HTTP than over gRPC.
  - **Each stream buffers up to 64 events, or 256 KiB of encoded events, whichever is less,** that its client hasn't taken yet. An event larger than 256 KiB is still sent, alone. A server holds at most `--max-streams` × 256 KiB of these buffers (64 MiB at the default 256 streams). This memory is separate from `--feed-ring-bytes` and `--inflight-bytes`. A client that takes nothing fills its buffer; after `--stream-idle-ms` more it gets the buffered events, then `DeadlineExceeded`, and resumes from the last event's token.

## Limits
- `--max-request-bytes` (16 MiB): the largest request.
- `--inflight-bytes` (256 MiB): request and reply bytes held at once. A request's share is reserved before its body is read: from `Content-Length` over HTTP, and from the message header over gRPC. Scan pages count too (below).
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
  - **What isn't counted:** the feed streams' buffers, the shared feed ring (`--feed-ring-bytes`) and each connection's own memory (`--max-connections`). So request and reply bytes together stay within `--inflight-bytes`, and those come on top.
- **HTTP/1.1 bodies of 64 KiB or less are read before anything else,** even before a request is shed or its token checked, within `--request-timeout-ms`. Past that deadline the server answers `DeadlineExceeded` (504, as every deadline in `phdb serve` does, rather than HTTP's 408) and closes the connection. The read counts toward the request's one deadline, so a slow body leaves the rest of the request less time. So every answer, an early refusal included, leaves the connection open for the next request. A larger body is reserved from `Content-Length` and read later. A refusal that comes first (`Overloaded`, `Unauthenticated`, `RequestTooLarge`) then answers with the body still on its way, and the server drops the connection, which can reset it under the answer. A client sending large bodies should send `Expect: 100-continue`: a refusal then arrives before any of the body is sent, and the server closes the connection cleanly, with no reset. HTTP/2 (gRPC) is unaffected: a refused stream leaves the connection alone. Each HTTP/1.1 connection holds at most 64 KiB in that read, and `--max-connections` bounds how many there are.
- `--max-concurrent` (1,024 requests): more are refused at once with `Overloaded`, never queued.
- **`--max-connections`** (half the soft open-file limit, `ulimit -n`, at most 4096; #1604): open TCP connections, HTTP/1.1 and HTTP/2 alike, from accept until they close, a TLS handshake included.
  - **Past the cap,** a new connection is accepted and closed at once, before anything is read. A gRPC client sees `UNAVAILABLE`, which its retry policy handles, and an HTTP client a connection error it can retry. If the client already sent bytes, the close is a reset ("connection reset by peer"), not an error in the server.
  - **The default** leaves the other half of the descriptors for the database (its file, WAL segments, a backup being written, the embedding endpoint's and a replica's connections). For example, 128 under macOS's default limit of 256, or 512 under Linux's usual 1024. Setting `--max-connections` above half the limit prints a warning at startup: raise `ulimit -n` with it.
  - **Memory per connection:** up to 64 KiB for the HTTP/1.1 pre-read (above). An HTTP/2 connection holds its flow-control windows and up to `--http2-send-buffer` per stream.
  - **A connection that sends nothing** within `--request-timeout-ms` of connecting is closed, and an HTTP/1.1 request's headers must arrive within it too, so silent or slow sockets can't hold the slots.
  - **An idle HTTP/1.1 keep-alive connection** is closed once `--request-timeout-ms` passes with no next request, which frees its slot. A client that reuses connections sees the close and opens another.
  - **An idle gRPC channel holds its slot:** an HTTP/2 connection with no calls answers the server's pings, so it counts until the client closes it. With many idle gRPC clients, raise the cap, or give the clients an idle timeout.
  - **`/metrics`:** `pigeonhole_serve_connections` (open now), `pigeonhole_serve_connections_max`, `pigeonhole_serve_connections_refused_total` and `pigeonhole_serve_accept_errors_total`. Out of descriptors (`EMFILE`), accepting waits for a connection to close, a second at most, and logs a line a second at most.
- `--max-scans` (64): scans streaming at once, refused past that the same way. A scan's pages count against `--inflight-bytes` until they're handed to the connection, and at most half the blocking threads read scan pages at once.
- `--stream-idle-ms` (30,000): how long a scan waits for its client to read a page, or a feed stream with a full buffer waits for its client to take an event, before ending the stream.
- `--max-streams` (256): feed streams at once. `--max-feed-hubs` (32): distinct table sets subscribed at once. Past either, a subscription gets `Overloaded`.
- **128 streams per connection:** one HTTP/2 connection carries at most 128 streams at once (scans, feed streams and calls alike). A client that opens more on one connection doesn't get an error: the extra streams wait until earlier ones end, and a feed stream may never end. Open another connection for each 128 concurrent streams. With tonic, that means another `Channel`, because one `Channel` is one connection.
- `--max-catch-up` (16): feed streams reading their own history at once (from a token, a snapshot or the oldest commit, or after falling behind); the rest wait their turn.
  - **While every catch-up slot is busy, a slow stream keeps its place on the ring** (#1726). Pushed off then, it would only queue for a slot, delivering nothing, and its own history read would cost the server more per event than the ring. So the shared subscription holds the commits that stream still needs and reads no further from the database meanwhile. The subscription falls behind the database, not the writes: no write waits for it.
  - **The hold ends** as soon as the stream takes its events or a slot frees, after at most 1 s of the stream taking nothing, or once the subscription is more than half the change feed's history behind, so it never runs off the history's end. Its other streams wait for it only that long.
  - **On `/metrics`:** `pigeonhole_serve_feed_paced_waits_total` and `_paced_hubs`, and the holds that ended with the stream falling off anyway, `pigeonhole_serve_feed_paced_released_total{reason}`.
- `--feed-ring-bytes` (256 MiB): the recent commits the shared feeds keep for their streams, in all, shared out over the table sets subscribed. A commit is kept as its encoded event, which carries its resume token (about 45 + 16 × shards bytes), plus about 110 bytes; a stream with a `prefix` filters that event. With 100-byte values and 4 shards, a commit costs about 380 bytes. Each table set's share is `--feed-ring-bytes` over the sets subscribed, so with many sets, raise it to keep the seconds of commits a stream can fall behind and still catch up from the ring. This is outside `--inflight-bytes`; `/metrics` reports what's in use.
- `--backup-dir`: where `Admin.Backup` writes, and where downloads are kept for resumes. It must exist; put it on a disk with room for a copy or two of the database, not tmpfs. There's no default: without it, backups are off. `--max-backups` (1) runs that many at once.
- `--max-sessions` (1,024) and `--max-sessions-per-token` (64): transactions, pinned snapshots and committed transactions' records held at once. Past either, `Begin` and `Open` get `Overloaded`.
- `--request-timeout-ms`: the longest wait for those limits. A gRPC client's shorter `grpc-timeout` wins, and running out of it is `DeadlineExceeded`.
- A write the server has submitted always answers its real outcome.

## Threads and placement
- **Threads:** `--shards` sets the shard count (default: half the physical cores). On Linux the network threads run on the CPUs the shards don't use; `--io-threads` and `--blocking-threads` override their counts. The change feed's delivery runs on threads of its own (`phdb-serve-feed`, `--feed-threads`, by default a quarter of those CPUs, at least 1, taken from the network threads' default), so a put never waits behind feed streams for a worker. They're placed as the network threads are. The shared subscriptions (hubs) that read the engine's feed run on threads of their own (`phdb-serve-feed-hub`, `--feed-hub-threads`, by default a quarter as many as `--feed-threads`, at least 1), on the same CPUs, so a hub never waits behind its streams' delivery (#1397). A hub with a single stream runs on the feed threads beside it instead (#1689). **The default keeps writes first.** Each hub thread past it costs the writes on the same CPUs: put p50 rose 13–38% at moderate fan-out load, and CPU per delivered event rose with it. When saturated, delivery wasn't better either: at 16 table sets × 16 streams, more hub threads delivered 15–21% fewer events. With 3 feed threads, 16 table sets with 2 streams each, on the AX102 (g76c on #1725):

  | `--feed-hub-threads` | put p50 at 30K puts/s | at 55K puts/s (saturated): puts | events delivered | lag p99 |
  |--:|--:|--:|--:|--:|
  | 1 (the default) | 145 µs | 176 µs p50 | 50% | 15.7 s |
  | 2 | 164 µs | 1.85 ms p50 | 100% | 0.27 s |
  | 3 | 175 µs | 524 ms p50 | 96% | 28 ms |

  Raise it on a feed-heavy server with CPUs to spare, whose subscribers matter more than its writes' latency. The feed dump (below) shows hubs' engine feeds falling behind the database.

  Each stream's framing still runs on its connection's network thread.
- **Placement** (Linux, D313 R1): where the engine pinned its shard threads (the default, `Options::pin_threads`), the network, feed and blocking threads get an affinity mask of the other CPUs. A client thread preempting a pinned shard thread raised `Buffered` put p99.9 (#1206), and one on a shard's SMT sibling doubled p99 (#1244). The startup line's last part says which mask the server built:
  - **`off the shards' cores`:** every CPU but the shards' and their SMT siblings;
  - **`unmasked (too few free cores)`:** fewer than two CPUs are left once the shards' cores and their SMT siblings are taken out: a small host, or a `--shards` near the core count. Each shard's driver thread takes a core, so at one shard per core none is left. The threads then get no mask, and the server says why on a line of its own. On the shards' SMT siblings they cost the shards more than they do left to the OS (g70d). Lower `--shards` to get the mask back;
  - **`unpinned`:** no shard is pinned, and the threads run where the OS puts them. That's macOS (no hard affinity), a host with too few free CPUs, a CPU quota narrower than the affinity set, or an OS that refused the pin. `/metrics` counts the engine's threads pinned and not (`pigeonhole_threads{pigeonhole_thread_pinned}`).
  - **`unmasked (--affinity off)`:** the shards are pinned, but the server's threads get no mask.
- **`--affinity off`** (default `on`) keeps the same thread counts but sets no mask, so the network, feed and blocking threads run where the OS puts them. The shards stay pinned: that's `Options::pin_threads`, not this flag.
  - **Leave it on for writes:** in the measurements below, the mask raised gRPC `Mutate` throughput 25% at 21% less CPU per write, and cut `Mutate`'s p99.9 in mixed workloads by 1.5–4.5×.
  - **Deployments dominated by whole-row reads or scans should use `--affinity off`.** With the mask, `ReadRow` ran 16% slower over gRPC and 26% slower over HTTP, and HTTP `Scan` 15% slower: with few writes, the shards' cores sit idle, and the mask keeps those reads off them. Point `Get`s, and the read-only YCSB C, were level or better with the mask.
  - **Turn it off on a host shared with other work,** where the server shouldn't claim every CPU outside the shards.
- **The server's background threads** run off the shards' cores too: an index's rebalancer (`phdb-serve-rebalance`), a bulk load (`phdb-serve-bulk-load`) and a text rebuild (`phdb-serve-text-rebuild`). So do the embedder's threads with `--embed-local` ([Agent memory](#agent-memory)).

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
| `pigeonhole_serve_feed_paced_waits_total`, `pigeonhole_serve_feed_paced_hubs` | counter, gauge | Shared subscriptions that held a slow stream's place on the ring while every `--max-catch-up` slot was busy, and those holding now (#1726). |
| `pigeonhole_serve_feed_paced_released_total{reason}` | counter | Holds that ended with the slow stream falling off anyway: `slot` (a catch-up slot freed), `hold` (it took nothing for 1 s) or `retention` (the subscription was more than half the feed's history behind). |
| `pigeonhole_serve_feed_hub_failures_total` | counter | Shared subscriptions that failed. Their streams end with the error. |
| `pigeonhole_serve_bulk_load_phase` | gauge | A `Vectors.BulkLoad`'s phase: 0 none, 1 receiving, 2 writing and rebalancing. |
| `pigeonhole_serve_bulk_load_vectors`, `pigeonhole_serve_bulk_load_bytes` | gauge | The vectors and message bytes handed to the running load. |
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

- **Setup.** `--replica-of` takes the primary's URL (`http://` or `https://`, checked against the system's roots). The replica sends the token from `--replica-token-file` (or `PHDB_REPLICA_TOKEN`), which needs admin scope on the primary. The primary needs `--backup-dir`, and its change feed on (`--change-feed`, or a database whose feed is already on).
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
- **Lag.** `Admin.Info`'s `replica` section reports the replica's `state` (`bootstrapping`, `following`, `re-bootstrapping`, or `retrying` with `last_error`), the primary's seqno it applied through, the primary's durable frontier, `lag_commits` (the difference: durable commits not applied yet; commits the primary hasn't synced aren't counted, since a replica can't have them) and `lag_ms` (how long since it was last caught up, 0 when it is). `bootstrap_phase` says which step a bootstrap is at (`connecting`, `waiting for the primary's copy`, `downloading`, `opening the copy` or `swapping`), and `bootstrap_bytes` how much of the copy has arrived; a bootstrap that failed keeps its last step beside `last_error` until the next one starts. A download that stalls (no chunk for `--stream-idle-ms`) breaks, shows why in `last_error`, and resumes from its offset on a new connection. The primary answers at once, then sends a heartbeat every second while it writes its copy, so no wait depends on how long the copy takes. A replica waits up to 3 seconds (or `--stream-idle-ms`, if longer) for the answer and between heartbeats, and up to `--stream-idle-ms` for each chunk of the copy's data. A break during the copy resumes with the same copy, never a new one. Only gRPC sends heartbeats: an HTTP `Admin/BackupDownload` sends nothing until the copy is written and hashed, because its size and SHA-256 go out as headers, and it holds its `--max-streams` slot while it waits. `/metrics` has the same as `pigeonhole_replica_following`, `pigeonhole_replica_applied_primary_seqno`, `pigeonhole_replica_lag_commits` and `pigeonhole_replica_lag_seconds`.
- **Throughput.** A replica applies the primary's commits in runs, one commit of its own per run: at most `--replica-run` (256) primary commits, or 8 MiB. A caught-up replica applies each arrival at once. One that is behind fills runs, so it catches up faster than the primary wrote.
- **Retention, and falling behind.** The primary doesn't track its replicas: its feed retention (`--change-feed`) decides how long a replica may be down or behind. Size it to the write rate times the longest replica outage you want to ride out; the primary's `/metrics` reports the feed's retained bytes. A replica that falls off the end, or whose primary's URL now serves another database, bootstraps again: it downloads a new copy while serving its old one (stale, with `lag_ms` growing), then swaps it in. Requests during the swap get `Unavailable` (retry them), and transactions, pinned snapshots and feed streams open on the old copy end. A feed token from the old copy is another database's to the new one: a stream on it, or a resume from it, ends with `InvalidArgument` ("the token is another database's"), so subscribe again from now or from a snapshot.
- **Promotion.** `Admin.Promote` (admin scope) on a running replica, or `phdb promote <file>` on a stopped one's file, makes it a primary at the last commit it applied: it stops following and takes writes, and its own feed tokens stay valid. It returns the primary's token it stopped at. **Nothing fences the old primary:** stop it (or its writers) before promoting, or the two diverge. A promoted database can't follow again, and `phdb serve --replica-of` refuses to start on it (or on any database with tables that isn't a replica's) rather than replace it; start a new replica from it instead.
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
  - backups.
- **Still to come:** moving a partition between nodes, several partitions per node, and restore. The index searches across partitions aren't supported on a cluster in this release (#1722, 1.3.0). Each section below says which of its parts are still to come.

### How a cluster is laid out
- **Partitions are whole database files.** A partition holds key ranges of one or more tables. Its file records which cluster and partition it is, and its *epoch* (`__phdb_cluster`, hidden from `ListTables`).
- **The map** says, in two layers:
  - which partition holds each key range of each table, given as sorted *split keys*. Range *i* is `[split i-1, split i)`, the first starting at the empty key and the last open-ended. **This layer is fixed once a range holds data.** Moving a range between partitions comes in 1.3.0 (#1499), so choose split keys up front ([Choosing split keys](#choosing-split-keys-and-hot-ranges)).
  - which node serves each partition. This changes when a partition is moved to another node, with a higher epoch.
- **The map has a version,** raised by every change. A node refuses a map no newer than its own, and every node keeps a copy (`DIR/cluster-map.json`).
- **Ids:** pick `cluster_id` once (a UUID is a good choice). Ids are `[a-z0-9_-]`, up to 64 characters.

### Running a cluster
1. **Stamp each partition's file** before its node starts: `phdb cluster init-partition DIR/P.phdb --cluster-id C --partition P`. `DIR` is the node's cluster directory, and the file must be named `<partition>.phdb`.
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
   - `--cluster-token-file` is the token a node presents to the others. List it as `cluster <token>` in every node's `--token-file`. The `cluster` scope is only for node-to-node calls.
   - To rotate it, list the old and new tokens on every node, then switch each node's `--cluster-token-file`.
   - **Every node lists the same tokens** (`--token-file`). A call forwarded to another node runs as your token there, with the scope and namespaces that node lists for it, and sessions and feeds are bound to it.
5. **Change the map** on a running cluster with `phdb cluster set-map MAP.json --node URL`, a compare-and-set against the current version. Raise `map_version` in the file.
   - **The map's writer** is the node with the smallest id in the map, and it alone changes the map, so two changes can't make two different maps of one version (D317 amendment 1). `set-map` reads the map from `--node` and sends yours to the writer. A `SetMap` sent straight to another node answers `WrongPartition`, naming the writer.
   - The writer validates the map, keeps it, and pushes it to the others.
   - A node that misses the push pulls the map when it next hears from a node that has it, or at its own start.
   - A map may add tables, partitions and nodes, and give a partition another node with a higher epoch. A table's split keys and partitions are fixed once set.
   - A partition added to a running node's map, or given to it, isn't opened there until the node restarts: nodes open their partitions at start, for now. Until then the node answers calls for it with `PartitionUnavailable` ("isn't open here").
   - **When the writer is down,** no map change and no schema call can be made. `phdb cluster set-map MAP.json --node URL --force` sets a map at another node:
     - Its version goes above every node `phdb cluster` can reach.
     - The map must name a writer that's up: remove the down node from it, or let another id be the smallest. Give the down node's partitions to other nodes with a higher epoch.
     - When the old writer comes back, it takes the forced map from the others before it changes anything, and follows it.
     - **Only for a writer that's down.** A writer that's up but cut off from you can still change the map, and a forced map then loses that change. Check that the writer's process is stopped first.
6. **Check it:** `phdb cluster status --node URL` shows every node's map version side by side, so a push that hasn't reached a node shows, and how many of each node's partitions are open with the identity the map expects. `phdb cluster show-map (--node URL | --file DIR/cluster-map.json)` prints a map.

**Flags:** a cluster node takes `--shard-spin`, `--shm-dir`, `--change-feed`, `--backup-dir` and `--max-backups` as a single node does, for its partition. `--memory`, `--create` and `--replica-of` are refused until the steps that bring them.

**Metrics:** a cluster node's `/metrics` and `Admin.Metrics` label each partition's series `pigeonhole_partition="<id>"`: the engine's, and the feed fan-out's and bulk loads'. The node's own (connections, requests, sessions) carry no label.

### How calls are routed
- **Any node takes any call.** It serves the call if it holds the row's partition, and otherwise forwards it to the node that does. A client needs no map. One that wants to skip the hop can read the map (`Cluster.GetMap`) and call the owner directly.
- **Served by row:** `Get`, `ReadRow`, `Mutate`, `CheckAndMutate`, `MutateReturning` and counters. Also a `Batch` whose rows are all in one partition, which commits atomically as on a single node.
- **A batch across partitions** is refused with `CrossPartition` before anything is written. The error names each partition and its first row, so you can split the batch.
- **Where a node's map is older:** `WrongPartition` is handled between nodes. A node with an older map pulls the newer one and forwards again.
- **Refused with `Unsupported`:** the index searches (`Vectors.Search`, `Text.Search`), which aren't supported on a cluster in this release (for vectors, use `Vectors.Nearest`, which fans out), and, naming the step that brings each, `Info` and `/mcp`.
- **A node holds one partition** for now, the one its map gives it, opened at start after checking the file's identity row against the map. A node the map gives no partition only routes.
- **Errors a cluster adds:**
  - `PartitionUnavailable` (503): the row's node can't be reached and nothing was sent to it. Safe to retry, writes included.
  - A forwarded write whose answer was lost after it was sent answers `OutcomeUnknown`, as on a single node. Check before you retry.
  - `CrossPartition` (409): the request can't succeed as written. Split it.

### Schema through the cluster
Tables and their families live in the cluster map (D317 ticket 5). `Schema.CreateTable`, `DropTable`, `ListTables`, `DescribeTable` and `GET /v1/tables` work through any node, with a single node's scopes: `write` to create (`if_missing` included), `admin` to drop, `read` to list and describe. Every node makes its partitions' files follow the map.
- **A create names its placement:** `placement` gives the table's split keys and partitions (or a placement group), as the map would. Without one, a create on a cluster is refused unless the map already places the table. `if_missing` adds the families a table lacks; an existing family keeps its options, even when the request gives others, as on a single node.
- **A drop** removes the table from the map, and each partition drops it. Sessions and feeds on the table end as they do on a single node.
- **The map's writer makes every change.** A create or drop through another node is forwarded to it ([Running a cluster](#running-a-cluster)). Concurrent changes through different nodes all land, one after another. A forwarded create or drop whose answer is lost after it was sent answers `OutcomeUnknown`: a create with `if_missing` is safe to send again, and a drop that went through answers `TableNotFound` the second time.
- **The window:** the writer sets the map and pushes it, and each node brings its partition's file up to date before it routes by the new map. Until a node has, a call through it can answer `TableNotFound`. Nothing was applied, so retry it. The reply's `unreconciled` lists partitions whose node the push missed. `phdb cluster status` shows each partition's state.
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
- **Resume** from a page's `next` through any node. A cluster's positions are its own: one from a single node, or one sent to a single node, is refused with `InvalidArgument`.
- **A partition that can't be reached** fails the read with `PartitionUnavailable`. With `allow_partial: true`, the read skips it and names its key range in `missing`: a scan on its next page (or a last, empty one), a multi-get on its reply.
- **At a snapshot** (`Selection.snapshot_id`), a scan or multi-get reads the snapshot's partition only. Its range or keys must stay within that partition, otherwise `CrossPartition`. A multi-get at a snapshot pages as its partition's node does.
- **`Nearest` and `Traverse`** return a single node's answer (D317 ticket 4b):
  - **`Nearest`:** the exact top `k` over every partition its source meets. Up to `--fanout-forwards` (8) partitions answer at once.
  - **`Traverse`:** each hop's edges are read from every partition its frontier reaches, then ranked by the same rules a single node uses. So hops, `parents` and `stats` are the same.
  - **Snapshots:** a traversal reads each hop at its own time, and each partition at its own snapshot, rather than at one snapshot for the whole traversal.
  - **Latency:** each hop is a round trip to the partitions' nodes, so a deep traversal of a graph spread across nodes costs hops × that round trip. Keep a graph's tables in one partition where you can.
  - **With `allow_partial`:** a `Nearest` result is exact over the partitions reached. A traversal skips an unreachable partition's frontier nodes, so it doesn't follow their edges, and the later hops and the ranking are exact only over what was reached. `missing` names what was skipped.
- **The index searches** (`Vectors.Search`, `Text.Search`) aren't supported on a cluster in this release (#1722, 1.3.0). For vectors, `Nearest` fans out.

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
- **A partition moved to another node** (a newer map, once moves come with D317's ticket 6) is opened again where the map then puts it, through any node, with nothing skipped.

#### Limits
- **Stream slots:** a subscription holds a `--max-streams` place on the node it entered, and one on each followed partition's node. An entry node that serves one of the partitions itself holds two.
- **Memory:** it buffers up to 256 KiB on its entry node and on each partition's node, so (partitions + 1) × 256 KiB at most, outside `--inflight-bytes`, like a single node's feed.
- **Stalled subscribers:** a subscriber that stops reading ends after `--stream-idle-ms`, as on a single node. Until then it holds only its own connections between nodes, never one that forwarded calls or other streams use.

#### Expiry and what isn't supported
- **Expired tokens:**
  - A token can't resume after a partition it follows was relocated onto a copy with another history. [Fencing and relocation](#fencing-and-relocation) will say which moves those are.
  - It can't resume after the map changed which partitions the subscription covers either. That happens only when a new table joins a subscription of every table.
  - Both answer `FeedExpired`: subscribe again.
- **After a restore:** restore comes with a later step, and [Backup and restore](#backup-and-restore) will say what a restored partition does to older tokens.
- **Not supported:** a start from a snapshot isn't supported on a cluster subscription. A one-partition version is #1701.
- **Positions:** a token's `seqno`s are partition-local. There's no cluster-wide position, so compare progress per partition.

### Fencing and relocation
**Comes with D317's ticket 6.** Moving a partition to another node, fencing a partition's old node so it can't write, and dropping a partition are being built. This section will describe them, including which kinds of move keep a feed's tokens.

Until then, keep each partition on the node it started on. A node checks its partition file's epoch against the map at start, and nothing yet stops an old node from writing while a new one takes over.

### Backup and restore
- **`phdb cluster backup DIR --node URL`** backs up every partition, each through the node the map gives it, into a new or empty `DIR`. It writes `<partition>.phdb` for each partition, `map.json` and `manifest.json`.
  - Each node needs `--backup-dir`, and the token admin scope, as for `Admin.Backup`.
  - `--durable` copies only what each writer holds durable.
  - `--parallel N` (4) copies that many partitions at once.
  - A broken download resumes.
- **Not one point in time.** Each copy is a consistent snapshot of its own partition, and the set is not one point in time across partitions. A commit on one partition can be in the backup while an earlier one on another isn't. A table created during the backup can have rows in one partition's copy and none in another's.
- **Checked against the map.** The backup checks each copy's cluster, partition and epoch against the map. A copy that differs fails it, naming both, for example `the copy is partition "p2" of cluster "c1" at epoch 2, but the map says partition "p2" of cluster "c1" at epoch 1`. That means the map changed during the backup, or the node holds another file. Run it again once the map is settled.
- **A map that moves meanwhile:** a partition added, moved or relocated during the backup fails it: `the map changed during the backup`. Tables created or dropped meanwhile don't. `map.json` is the map at the end, which places them as the copies hold them.
- **A backup is complete only with its `manifest.json`,** which is written last. Copies are `<partition>.phdb.partial` until checked.
- **One partition's backup:** a node's `Admin.Backup` and `BackupDownload` back up one partition it holds, named by `partition`, and aren't forwarded. Another node's partition is `WrongPartition`.
- **Restore comes with a later step.** It will put each partition's file back on the node the backup's map (or an edited copy of it) gives it, fenced so that the old cluster, if it still runs, can't write into the restored one ([Fencing and relocation](#fencing-and-relocation) will say how).

### Partitions per node
Each node holds one partition for now. Several partitions per node come with D317's ticket 8a. The recommendation for how many comes from a box run at the same node totals with 1, 2, 4 and 8 partitions.

**Plan for one partition per node until then.** An early box run (perf287's g74) points the same way: at the same totals, four partitions on one node gave 25–34% less throughput and 30–75% higher p99 than one. Each partition is a whole database engine, with costs that don't shrink with its share of the load:
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
