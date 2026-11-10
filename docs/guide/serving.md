# Sharing a database across processes and hosts

> **Status:** this page describes `main`. It's the first step of #1397 (D313): a reference pattern you run and own. [`phdb serve`](cli.md#phdb-serve), a supported server speaking gRPC, HTTP/JSON and remote MCP (`/mcp`), is in progress for 1.2.0, behind the `serve` feature.

Pigeonhole is embedded: one process opens the file and writes it. When several containers or hosts need the same data (a job queue, workflow state, coordination between workers), **one process owns the database and serves it over the network, and the others call it.** This page shows that pattern with a runnable example, [`examples/serving`](../../examples/serving), and covers what makes it correct under failures:
- paging that resumes anywhere;
- writes that happen once;
- leases with fencing;
- transactions that are safe to retry;
- a watch stream that resumes after a disconnect.

It ends with security, backups and the limits.

## When this fits
- **Shared state for a fleet:** job queues, leases, workflow state, counters, and agents sharing memory.
- **One machine's throughput is enough.** Every write crosses the network to one process. That's fine for coordination state, and it isn't a distributed database.
- **One node is acceptable.** The owner is a single point of failure, with no replication or failover. Recovery is backups and restore (below).

If the processes are on one host and only need to read, you may not need a server at all: [reader processes](readers.md) open the same file read-only.

## The shape: business operations, not a database protocol
The example's server exposes the operations its workers need (create a job, claim it, complete it, transfer between accounts), not a generic "get, put, scan" API. Each endpoint runs its logic **next to the data, with the whole Pigeonhole API in-process**:
- `transact`'s retry loop runs on the server, so no client implements optimistic concurrency.
- A rule like "a done job can't be claimed" is checked in one place.
- Each operation is one round trip.

A generic remote API is what [`phdb serve`](cli.md#phdb-serve) is. Owning your server is the better fit when your operations are few and known.

```sh
# One terminal: the server owns jobs.phdb and listens on loopback.
cargo run -p pigeonhole-example-serving --bin serving-server -- jobs.phdb 127.0.0.1:7070

# Another: queue a few jobs.
for i in 1 2 3; do
  curl -s localhost:7070/jobs -H 'content-type: application/json' -d "{\"id\":\"job:$i\",\"spec\":\"resize\"}"
done

# Any number of workers, on any host that can reach the server.
cargo run -p pigeonhole-example-serving --bin serving-worker -- http://127.0.0.1:7070 worker-1
```

`examples/serving/src/lib.rs` is the server, about 750 lines with comments. `src/client.rs` is the worker's client, and `tests/serving.rs` runs the scenarios below against a real server.

## Reads that page
`GET /jobs?after=&bytes=` returns a page cut to a byte budget, with the position to continue from (the scan's [byte budget](scans-and-filters.md#byte-budgets), `read_within`, D308):
- **The position is stateless.** It's `ScanPosition::to_bytes`, sent as hex. Any server process, now or after a restart, resumes from it, so the server keeps no cursor per client.
- **A page can end inside a row.** The next page then starts with the rest of that row: the same row key again, with the columns that didn't fit. Merge by key if you need whole rows, or treat the list as advisory, as the worker does. Acting on a stale or partial entry costs at most a refused claim, because claims are conditional writes.
- **Each page reads as of its own request.** For a listing that must be consistent across pages, read the pages at one snapshot, held on the server for the duration.

## Writes that happen once
A create that a client retries after a lost reply mustn't create twice. `POST /jobs` is `commit_if` on the job's status column being absent, so a second create of the same id is a 409 (`Exists`), not a duplicate:

```rust,ignore
let absent = Condition::Absent { family: "state".into(), qualifier: b"status".to_vec() };
app.jobs.mutate(id).put("spec", b"body", spec).put("state", b"status", b"queued")
    .commit_if_async(&absent).await?; // None: the job already exists
```

## Leases, with a fencing token
A worker claims a job, so that no other worker runs it at the same time. If the worker dies, the claim must expire on its own.
- **Claim:** `commit_if(Absent lease:holder)` writes the lease, in a family with a TTL (`Family::default().ttl(..)`). An expired lease reads as absent, so an abandoned job becomes claimable again with no cleanup process.
- **The lease's value is a fencing token** unique to this claim: `worker/claim_id`, where the worker makes the claim id, a random id per claim. The worker gets the token back from the claim.
- **A claim can be retried.** If a claim's reply is lost, the worker doesn't know whether it holds the job, so it sends the same claim again, with the same claim id. A claim that finds its own token already in the lease returns success with it; any other token is `409 Claimed`. Without the worker's claim id, the retry would be refused by its own invisible lease, and the job would sit held by nobody who knows it until the TTL (simp724's L1 on #1491). `Client::claim` does this.
- **Every later write for the job is conditioned on the lease still holding that exact token.** That covers renewing it and completing the job (`commit_if(Value lease:holder == token)`).
- **Renewal is a fresh put of the same token,** so the TTL counts from the renewal. A renewal fails once the TTL has passed, even if nobody else claimed the job, because the expired cell is invisible to the condition. The worker must then stop and treat the job as lost.
- **Renew at least every third of the TTL,** and treat a renewal that fails *or gets no answer* as lost. A renewal whose outcome is unknown may not have extended the claim, and the safe reading is "stop".

**Why the token matters.** A TTL alone doesn't stop a slow holder. A worker paused past its TTL (a GC pause, a stalled VM) still believes it holds the job, while another worker has claimed and finished it. Without fencing, the slow worker's "complete" would overwrite the newer result. With the token, its write is refused (`409 Lost`), and the result stays the new holder's.

**What the token doesn't fence.** It fences writes that go through the database. A side effect outside it (sending an email, taking a payment, calling another service) isn't fenced: a stalled worker can still do it after its claim expired. So before each such step, renew and check the renewal. Better, pass the token to the downstream system and have it refuse a token older than the newest it has seen, which is the general fencing rule.

```rust,no_run
# fn main() -> Result<(), pigeonhole_example_serving::ClientError> {
use pigeonhole_example_serving::Client;

let client = Client::new("http://127.0.0.1:7070", None);
if let Some(claim) = client.claim("job:1", "worker-7")? {
    // ... do the work, calling client.renew(..) at least every TTL/3, and stopping if a
    // renewal returns false or an error ...
    if !client.complete("job:1", &claim, "resized")? {
        // The claim expired and someone else took the job: our result was not recorded.
    }
}
# Ok(())
# }
```

A condition tests one column, so "is the job still queued?" is checked right after the claim lands. A claim on a done job gives the lease straight back.

## Transactions, and retries that are safe
`POST /transfer` moves money between two accounts in a [transaction](transactions.md). It runs in `transact` **on the server**, which retries a `Conflict` with backoff, so concurrent transfers serialize correctly and the client never sees the conflict.

What the client *does* need to handle is not knowing whether a request committed. The reply can be lost after the commit (a timeout, a dropped connection, a proxy error), or the database itself can answer `OutcomeUnknown`. Retrying blindly would transfer twice. So **the request carries an operation id, and the transaction records it** alongside the money it moves:
- **First, the transaction checks the op id.** If it's recorded, the transfer already happened, and the server answers `applied: false` without moving anything.
- **The check and the transfer are one transaction.** A retry therefore sees either "not applied" (and applies it) or "already applied". It's never applied twice.
- The same idea covers single writes: [recovery](recovery.md) writes an operation id in the same mutation.

**Which errors say a write didn't happen.** Retry a write as it is only when the response *says* it wrote nothing:
- the API's JSON error body with a stable code like `Busy` (503, with `Retry-After`), `Conflict`, or a validation error;
- or, for anything else, when the request is idempotent, like the transfer with its op id.

A timeout, a connection error, or a 502/503/504 **without** the API's body came from something in between, and says nothing about whether the write committed. `Client::transfer` retries those only because the op id makes it safe.

```rust,no_run
# fn main() -> Result<(), pigeonhole_example_serving::ClientError> {
# let client = pigeonhole_example_serving::Client::new("http://127.0.0.1:7070", None);
// The same op id on every retry: at most one of the attempts moves the money.
let applied = client.transfer("invoice-2041", "alice", "bob", 30, 5)?;
# Ok(())
# }
```

## Watching for changes
`GET /events` streams every commit to `jobs` as server-sent events, over the [change feed](change-feed.md). The database must be opened with the feed on (`Options::change_feed`).
- **Each event's `id` is the feed's position after it** (`FeedToken`, as hex). An SSE client that reconnects sends it as `Last-Event-ID`, and the stream continues exactly after the last event it saw: none skipped, none repeated.
- **Positions are opaque.** A resumed stream's ids are positions too, but don't compare them byte for byte with the first stream's.
- **A client that stays away longer than the feed's retention** gets one last `error` event, `FeedExpired`. It then re-reads the state it needs and subscribes from now.
- **The server holds one feed per connected client.** That's fine for a handful of watchers. For many, share one subscription in the server and fan it out, which is what `phdb serve` will do (D313).

## Threads
The example serves on Tokio with axum.
- **Commits, gets, row reads and the feed** use Pigeonhole's [async forms](async.md) (`commit_if_async`, `get_async`, `read_async`, `FeedStream`). They hold no thread while they wait.
- **The calls that are sync-only** (`transact`, `read_within`) run on Tokio's blocking pool (`spawn_blocking`).
- **Opening and closing** happen once, around serving.

## Security
- **The example's server refuses a non-loopback address unless a token is set** (`SERVING_TOKEN`, from the environment, never a flag, so it doesn't show in process listings or shell history). Every request must then carry `Authorization: Bearer <token>`, compared in constant time.
- **The token alone isn't enough off-host.** Put TLS in front: a reverse proxy (nginx, Caddy, Envoy) terminating TLS for the server on loopback is the simple setup.
- **Configure the proxy not to retry writes:** many retry a 503 or a reset connection by default, which would replay a write whose outcome is unknown.
- **Separate tokens for readers and writers** are a small extension of the example's middleware. `phdb serve` will have them built in.

## Backups and restore
- **Take a backup of the live database** with `phdb backup <file> <dest>`, or with `Pigeonhole::backup` from an admin endpoint of your own. It's a consistent copy at a point in time, taken while writes continue ([`phdb backup`](cli.md#phdb-backup-and-phdb-compact)).
- **Copy it off the host.** With one node, the backup is the recovery story.
- **To restore,** stop the server, put the copy in place, and start the server again. Commits after the backup's point are lost, so take backups as often as you can afford to lose work.

## The limits
- **One writer node,** so no failover or high availability of your own server. Backup and restore is the recovery story. `phdb serve` has read replicas with manual promotion ([Replicas](cli.md#replicas)); partitioning across nodes (#1482, D317) is in progress ([Clusters](cli.md#clusters)).
- **The file is on the server's local disk.** Network filesystems are refused at open.
- **Throughput is one machine's.**
- **Your server is your API.** Versioning, auth beyond a token, and rate limits are yours to add, or to get from `phdb serve` once it lands.

## Next
- [Transactions](transactions.md): what conflicts, and `transact`'s retry policy.
- [Change feed](change-feed.md): retention, tokens and delivery.
- [Recovery](recovery.md): `OutcomeUnknown`, and operation ids.
