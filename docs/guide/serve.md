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
- `--feed-ring-bytes` (256 MiB): the recent commits the shared feeds keep for their streams, in all, shared out over the table sets subscribed. A commit is kept as its encoded event, which carries its resume token (about 45 + 16 × shards bytes), plus about 110 bytes; a stream with a `prefix` filters that event. With 100-byte values and 4 shards, a commit costs about 380 bytes. Each table set's share is `--feed-ring-bytes` over the sets subscribed, so with many sets, raise it to keep the seconds of commits a stream can fall behind and still catch up from the ring. This is outside `--inflight-bytes`; `/metrics` reports what's in use.
- `--backup-dir`: where `Admin.Backup` writes, and where downloads are kept for resumes. It must exist; put it on a disk with room for a copy or two of the database, not tmpfs. There's no default: without it, backups are off. `--max-backups` (1) runs that many at once.
- `--max-sessions` (1,024) and `--max-sessions-per-token` (64): transactions, pinned snapshots and committed transactions' records held at once. Past either, `Begin` and `Open` get `Overloaded`.
- `--request-timeout-ms`: the longest wait for those limits. A gRPC client's shorter `grpc-timeout` wins, and running out of it is `DeadlineExceeded`.
- A write the server has submitted always answers its real outcome.

## Threads and placement
- **Threads:** `--shards` sets the shard count (default: half the physical cores). On Linux the network threads run on the CPUs the shards don't use; `--io-threads` and `--blocking-threads` override their counts. The change feed's delivery runs on threads of its own (`phdb-serve-feed`, `--feed-threads`, by default a quarter of those CPUs, at least 1, taken from the network threads' default), so a put never waits behind feed streams for a worker. They're placed as the network threads are. The shared subscriptions (hubs) that read the engine's feed run on threads of their own (`phdb-serve-feed-hub`, `--feed-hub-threads`, by default as many as `--feed-threads`), on the same CPUs, so a hub never waits behind its streams' delivery and falls behind the database (#1397). Each busy hub keeps a thread busy: with one hub thread, 16 hubs at 55K commits/s fell 13.8 s behind and delivered about half their events (#1719). Raise `--feed-hub-threads` when the feed dump (below) shows hubs' engine feeds falling behind the database as more table sets are subscribed. A hub with a single stream runs on the feed threads beside it instead, which saves a handoff between thread pools on every commit (#1689). It moves to the hub threads as soon as a second stream joins, and back once it has had one stream for a second. Each stream's framing still runs on its connection's network thread.
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
**In progress (D317, ICR 0117).** One database split across several nodes by key range: each partition is a whole database file on a node. A node keeps and distributes the partition map. It serves single-row reads and writes, transactions and snapshots for the whole cluster, forwarding each to the node that owns its row or partition. It fans scans, multi-gets, nearest searches and traversals out across the partitions, and serves change feeds across them. It backs up every partition through its node. Index searches, schema changes and restore come in the next steps, and this section grows with them.

- **`phdb serve --cluster DIR --node-id ID`** runs a node.
  - `DIR` holds the node's map (`cluster-map.json`) and its partitions' files (`<partition>.phdb`).
  - One server per `DIR`.
  - `--cluster-token-file` is the token the node presents to the others. List it as `cluster <token>` in every node's `--token-file`; the `cluster` scope is only for node-to-node calls.
  - To rotate it, list the old and new tokens on every node, then switch each node's `--cluster-token-file`.
- **`phdb cluster init-partition DIR/P.phdb --cluster-id C --partition P`** stamps a partition file with who it is. Run it before the node starts.
- **`phdb cluster set-map MAP.json --node URL`** sets the map: a compare-and-set against the node's version.
  - The node validates the map, keeps it, and pushes it to the others.
  - A node that misses the push pulls the map when it next hears from a node that has it, or at its own start.
  - A map may add tables, partitions and nodes, and move a partition to another node with a higher epoch. A table's split keys and partitions are fixed once set.
- **`phdb cluster status --node URL`** shows every node's map version side by side.
- **`phdb cluster show-map (--node URL | --file DIR/cluster-map.json)`** prints a map.
- **`phdb cluster backup DIR --node URL`** backs up every partition, each through the node the map gives it, into a new or empty `DIR`. It writes `<partition>.phdb` for each partition, `map.json` and `manifest.json` (D317 ticket 8b).
  - **Each copy is a consistent snapshot of its own partition. The set is not one point in time across partitions.** A commit on one partition can be in the backup while an earlier one on another isn't.
  - Each node needs `--backup-dir` (admin scope, as for `Admin.Backup`). `--durable` copies only what each writer holds durable. `--parallel N` (4) copies that many partitions at once. A broken download resumes.
  - The backup checks each copy against the map: its cluster, partition and epoch. A copy that differs fails it with a message naming both, for example `the copy is partition "p2" of cluster "c1" at epoch 2, but the map says partition "p2" of cluster "c1" at epoch 1`. That means the map changed during the backup, or the node holds another file. Run it again once the map is settled.
  - A partition added, moved or relocated while it runs also fails it: `the map changed during the backup`. Tables created or dropped meanwhile don't. `map.json` is the map at the end, which places them as the copies hold them.
  - The backup is complete only with its `manifest.json`, which is written last.
  - Restoring comes with a later step.
- **Choosing ids:** pick `cluster_id` once (a UUID is a good choice). Ids are `[a-z0-9_-]`, up to 64 characters.
- **Serving data:** any node takes any call. It serves the call if its partition holds the row, and otherwise forwards it to the node that does. A client needs no map; one that wants to skip the hop can read the map (`Cluster.GetMap`) and call the owner directly.
  - **What's served:** `Get`, `ReadRow`, `Mutate`, `CheckAndMutate`, `MutateReturning` and counters, by their row. Also a `Batch` whose rows are all in one partition, which commits atomically as on a single node.
  - **A batch across partitions** is refused with `CrossPartition` before anything is written. The error names each partition and its first row, so you can split the batch.
  - **Scans and multi-gets** read across the partitions, in key order, and return what one database holding all of them would (D317 ticket 4a):
    - **Scans:** each page holds one partition's rows, and each partition reads at its own snapshot, so rows in different partitions aren't from one point in time. A row is never torn.
    - **Multi-gets:** a page is exactly what a single node would return for the same keys and `page_bytes`.
    - **Resume** from a page's `next` through any node. A cluster's positions are its own: one from a single node, or one sent to a single node, is refused with `InvalidArgument`.
    - **A partition that can't be reached** fails the read with `PartitionUnavailable`. With `allow_partial: true`, the read skips it and names its key range in `missing`: a scan on its next page (or a last, empty one), a multi-get on its reply.
    - **At a snapshot** (`Selection.snapshot_id`), a scan or multi-get reads the snapshot's partition only. Its range or keys must stay within that partition, otherwise `CrossPartition`. A multi-get at a snapshot pages as its partition's node does.
  - **`Nearest` and `Traverse`** read across the partitions too, and return a single node's answer (D317 ticket 4b):
    - **`Nearest`:** the exact top `k` over every partition its source meets. Up to `--fanout-forwards` (8) partitions answer at once.
    - **`Traverse`:** each hop's edges are read from every partition its frontier reaches, then ranked by the same rules a single node uses. So hops, `parents` and `stats` are the same.
    - **Snapshots:** a traversal reads each hop at its own time, and each partition at its own snapshot, rather than at one snapshot for the whole traversal.
    - **Latency:** each hop is a round trip to the partitions' nodes, so a deep traversal of a graph spread across nodes costs hops × that round trip. Keep a graph's tables in one partition where you can.
    - **With `allow_partial`:** a `Nearest` result is exact over the partitions reached. A traversal skips an unreachable partition's frontier nodes, so it doesn't follow their edges, and the later hops and the ranking are exact only over what was reached. `missing` names what was skipped.
  - **Transactions and pinned snapshots** run in one partition, on its node. `Begin` and `Snapshots.Open` must name it in `route`, as a row (`table`, `row`) or a partition id, on every cluster, even one with a single partition. The ids they answer name the partition (`p2:...`), so any node routes the calls on them, and none of them are held by the node you called.
    - **A row outside the partition** (a transaction's read or write, or a read at the snapshot) is refused with `CrossPartition` before anything is written.
    - **`Traverse` and `Nearest` at a snapshot** aren't supported on a cluster.
    - **Tokens:** a session belongs to the token that opened it, on every node. So every node must list the same tokens; a call for a token the partition's node doesn't list is refused with `PermissionDenied`. `--max-sessions` and `--max-sessions-per-token` count on each node separately.
  - **Refused with `Unsupported`,** naming the step that brings each: the index searches (`Vectors.Search`, `Text.Search`), schema calls (`ListTables` included), `Info` and `/mcp`. `Admin.Backup` and `BackupDownload` back up one partition the node holds, named by `partition`, and aren't forwarded: another node's is `WrongPartition`.
  - **A node with one partition per node:** a node opens the partition its map gives it at start, and checks the file's identity row against the map. Several partitions per node, and moving a partition, come later. A node the map gives no partition only routes.
  - **Flags:** a cluster node takes `--shard-spin`, `--shm-dir`, `--change-feed`, `--backup-dir` and `--max-backups` as a single node does, for its partition. `--memory`, `--create` and `--replica-of` are refused until the steps that bring them.
  - **Metrics:** a cluster node's `/metrics` and `Admin.Metrics` label each partition's series `pigeonhole_partition="<id>"`: the engine's, and the feed fan-out's and bulk loads'. The node's own (connections, requests, sessions) carry no label.
  - **Tables are created in each partition's file** before its node starts, for now: `phdb` against the file, after `init-partition`. Schema through the cluster comes later.
- **Change feeds:** `Feed.Subscribe` through any node follows every partition that holds the subscription's tables (only those whose key ranges can hold its prefix), and merges them into one stream. Turn the feed on in each partition with `--change-feed`.
  - **Order:** each partition's commits arrive in its commit order, so each row's do. There's no order across partitions: a commit on one can arrive after a later one on another.
  - **Partition and seqno:** each event names its `partition`. A commit's `seqno` is that partition's own, and is comparable only with seqnos of the same partition.
  - **Tokens are checkpoints.** A cluster token holds every partition's position, so on a subscription over several partitions only some events carry one; the others have an empty `token`. A one-partition subscription, or one with `checkpoint_every: 1`, has a token on every event, as on a single node.
  - **Resuming:** resume from the last checkpoint you've stored. You may get again the commits that came after it, up to 64 in all (`checkpoint_every`, if you set it).
  - **The client loop**: store the last token and, per partition, the last seqno you've handled, with the work they fed:

    ```text
    for each event:
        if event.commit and event.commit.seqno <= last_seen[event.partition]: skip it  (a repeat)
        handle it
        if event.commit: last_seen[event.partition] = event.commit.seqno
        if event.token:  token = event.token
        store (token, last_seen) with what you handled
    to resume: Subscribe { resume: token, skip_through: last_seen as [{partition, seqno}] }
    ```

    With `skip_through` the server passes over the repeats itself, so nothing arrives twice. Without it, the `seqno` check drops them. Over SSE, events without a token carry no `id:`, so `Last-Event-ID` reconnects from the last checkpoint and the `seqno` check applies.
  - **What a token costs:** a checkpoint every N commits keeps the token at about 150 bytes per event on average:

    | Partitions × shards each | Checkpoint size | One every |
    |---|---|---|
    | 2 × 16 | about 310 B | 3 commits |
    | 16 × 4 | about 1 KB | 7 commits |
    | 16 × 16 | about 2.4 KB | 16 commits |
    | 64 × 16 | about 9.5 KB | 64 commits |

    Past about 64 partitions × 16 shards, a checkpoint comes every 64 commits and costs more per event.
  - **`allow_partial: true`** keeps the stream going when a partition's node can't be reached. A `partition_state` event says which partition is missing, and again when it's back. Its commits then continue from where they left off, with none skipped. Without it, the stream ends with `PartitionUnavailable` naming the partition; resume once the node is back.
  - **Limits:** a subscription holds a `--max-streams` place on the node it entered, and one on each partition's node. An entry node that also serves one of its partitions holds two. It buffers up to 256 KiB on its entry node and on each partition's node: (partitions + 1) × 256 KiB at most, outside `--inflight-bytes`, like a single node's feed.
  - **Expiry:** a token can't resume after its partitions' map changed under it (a new table in a subscription of every table), or after a partition was relocated: `FeedExpired`, subscribe again.
  - A start from a snapshot isn't supported on a cluster subscription.
- **Errors a cluster adds:**
  - `PartitionUnavailable` (503): the row's node can't be reached and nothing was sent to it. Safe to retry, writes included.
  - A forwarded write whose answer was lost after it was sent answers `OutcomeUnknown`, as on a single node. Check before you retry.
  - `WrongPartition` is handled between nodes: a node with an older map pulls the newer one and forwards again.

## Performance
> **Coming with D313 ticket 9b:** the serve bench's numbers on the reference machine: puts, gets and scans over gRPC and HTTP/JSON, and the change feed's delivery at many subscribers, with and without the placement mask.

## Next
- [Observability](observability.md): the engine's metrics, alerts and dashboard.
- [Using Pigeonhole from AI agents](ai-agents.md): `phdb mcp`, and remote MCP's client configuration.
- [Recovery](recovery.md): `OutcomeUnknown`, and operation ids.
- [Change feed](change-feed.md): tokens, retention and delivery.
