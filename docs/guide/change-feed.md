# Change feed

> **Status:** this guide describes `main`. The current release on crates.io is 1.1.0, and [`changelog.d/`](../../changelog.d/README.md) lists what `main` adds after it. The change feed (D310) is new in 1.1.0. Code samples run as doctests (lines starting with `#` are hidden setup).

A change feed delivers the committed changes of some tables to a subscriber: whole commits, once each, in commit order. It's resumable from a stored token, in the same open of the database or a later one. Use it to keep a cache, a search index or another store in step with a database, or to drive work from what was written. The writer never waits for subscribers. A commit costs the same whether ten feeds follow the table or none does.

## Turning it on
The feed reads the database's write-ahead log, so the log has to keep history past the point where it would otherwise recycle it. Turn that on when the database opens, with the history's size:

```rust
use pigeonhole::{FeedRetention, Options, Pigeonhole};
use std::time::Duration;

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let options = Options::default()
    // Keep up to 1 GiB of committed history, and none older than an hour.
    .change_feed(FeedRetention::bytes(1 << 30).age(Duration::from_secs(3600)));
let db = Pigeonhole::open(dir.join("app.phdb"), options)?;
# db.close()?;
# Ok(())
# }
```

- **`feed_tables([..])`** limits the history to some tables. By default every table is fed.
- **`DatabaseInfo::feed`** reports the retention and what it costs on disk: the retained log bytes, and blob bytes kept for it.
- **An open that doesn't mention the feed inherits it.** The next open with neither `change_feed` nor `change_feed_off()` feeds the same tables with the same retention and age. With `shards` unset, it also keeps the same shard count. So a tool that opens the file between two runs of your application (`phdb load`, say) keeps your feed's history.
- **`change_feed_off()`** turns the feed off for an open, which ends its history: a token from before can't resume past that open. So does opening with an explicit, different `shards(n)`, which reshards the database.
- **`phdb`'s read-only commands** (`info`, `dump`, `backup`, `shell --read-only`) open read-only, so they leave the history whole too.

## Subscribing
`Table::feed()` builds a subscription. `next(timeout)` waits for the next commit, and `try_next()` takes one if it's ready:

```rust
use pigeonhole::{ChangeKind, Family, FeedRetention, Options, Pigeonhole};
use std::time::Duration;

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(
    dir.join("app.phdb"),
    Options::default().change_feed(FeedRetention::bytes(64 << 20)),
)?;
let users = db.table("users")?.family("p", Family::default()).create_if_missing()?;
let mut feed = users.feed().open()?;
users.mutate(b"user:1").put("p", b"name", b"ada").commit()?;

let commit = feed.next(Duration::from_secs(5))?.expect("the commit");
for change in commit.changes() {
    assert_eq!(change.table(), "users");
    if change.kind() == ChangeKind::Put {
        assert_eq!((change.row(), change.value().is_some()), (&b"user:1"[..], true));
    }
}
# drop(feed);
# db.close()?;
# Ok(())
# }
```

- **A `Commit`** has its `seqno` (the order commits arrive in, as `Snapshot::seqno` counts) and its `timestamp`. Its `changes` are in the order the commit wrote them.
- **A `Change`** names its table and family, and has its `kind` (`Put`, `Merge`, or one of `DeleteCell`, `DeleteColumn` and `DeleteFamily`), row, qualifier and timestamp. `value()` types the value as reads type it (`None` for deletes). A row delete arrives as one `DeleteFamily` per family.
- **Whole commits:** a commit across several tables or shards arrives as one commit, never in pieces. `FeedBuilder::table(&t)` adds more tables to the same feed, and `prefix(..)` keeps only the rows under a key prefix. A commit with nothing the feed follows is passed over.

## Where it starts
- **After now** (the default): the commits after `open()`.
- **`from_snapshot(&snapshot)`:** the commits after those the snapshot sees. This is how a consumer starts from a known state without missing or repeating anything: read the snapshot, then follow the feed.
- **`from_oldest()`:** the oldest history kept, across reopens too: it starts at the oldest earlier open whose history every shard still keeps whole. Under load that history can be recycled before the feed reaches it, which expires the feed at once, so prefer a snapshot.
- **`resume(&token)`:** after the last commit a feed delivered before it gave that token.

## A feed of every table
`Pigeonhole::feed()` follows every table, including tables created after it opened, as one stream of the whole database's commits. It needs the change feed on for every table: no `feed_tables` list in this open or in any earlier open its history spans. `next` gives its commits. `next_event` gives the same commits as `FeedEvent::Commit`, and the changes to the tables themselves as `FeedEvent::Catalog`, each in its place in commit order:

```rust
use pigeonhole::{CatalogEdit, Family, FeedEvent, FeedRetention, Options, Pigeonhole};
use std::time::Duration;

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(
    dir.join("app.phdb"),
    Options::default().change_feed(FeedRetention::bytes(64 << 20)),
)?;
let mut feed = db.feed().open()?;
let users = db.table("users")?.family("p", Family::default()).create()?;
users.mutate(b"user:1").put("p", b"name", b"ada").commit()?;

let wait = Duration::from_secs(5);
match feed.next_event(wait)?.expect("the create") {
    FeedEvent::Catalog(change) => match &change.edits()[0] {
        CatalogEdit::TableCreated { table, families, .. } => {
            assert_eq!((table.as_str(), families[0].name.as_str()), ("users", "p"));
        }
        other => panic!("{other:?}"),
    },
    other => panic!("{other:?}"),
}
match feed.next_event(wait)?.expect("the commit") {
    FeedEvent::Commit(commit) => assert_eq!(commit.changes()[0].table(), "users"),
    other => panic!("{other:?}"),
}
# drop(feed);
# db.close()?;
# Ok(())
# }
```

- **A `CatalogChange`** holds the edits one catalog change made: `TableCreated` (with every family's options, as `FamilyInfo`), `FamilyAdded` and `TableDropped`, each naming its table. Its `seqno` places it: every commit below it came before the change, every commit at or above it comes after.
- **Starts:** a feed opened now, or `from_snapshot`, passes over the changes its start already sees, so the tables that exist are not created again. A token resumes just past the last event, a commit or a catalog change.
- **Tokens:** a feed of every table resumes only from a token of a feed of every table. A table's feed may resume from one: it follows part of what that feed did, from the same position.
- **Names:** a change to a table dropped before the feed reached it comes with empty names, unless the feed delivered the table's creation.
- **A sidecar failure** (the feed's `-feed` file can't be written) stops this session from recording catalog changes. A feed of every table in the writer's process expires at once. In a reader process, it waits at the next catalog change's place in commit order until the database reopens. Feeds of some tables carry on. Every token taken since expires at the next open, as for any sidecar failure. Writes go on.
- **Async:** `nonblocking::FeedEventStream` is `FeedStream` for events, and `Feed::poll_next_event` is the same one level down.

## Tokens
Take `feed.token()` after handling a commit, and store it with what the commit fed, ideally in the same write. `FeedToken::as_bytes` and `FeedToken::from_bytes` round-trip it. Resuming continues exactly after the last commit delivered, so a crash between handling a commit and storing its token delivers that commit again, never skips it. Make your handling idempotent, or store the token atomically with its effect.

```rust
use pigeonhole::{Family, FeedRetention, FeedToken, Options, Pigeonhole};
use std::time::Duration;

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
# let path = dir.join("app.phdb");
let options = || Options::default().change_feed(FeedRetention::bytes(64 << 20));
let db = Pigeonhole::open(&path, options())?;
let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
let mut feed = t.feed().open()?;
t.mutate(b"a").put("f", b"q", b"1").commit()?;
feed.next(Duration::from_secs(5))?.expect("a");
let stored: Vec<u8> = feed.token().as_bytes().to_vec(); // keep it with your state
t.mutate(b"b").put("f", b"q", b"2").commit()?;
drop(feed);
db.close()?;

let db = Pigeonhole::open(&path, options())?;
let t = db.table("t")?.open()?;
let mut feed = t.feed().resume(&FeedToken::from_bytes(stored)).open()?;
let c = feed.next(Duration::from_secs(5))?.expect("b");
assert_eq!(c.changes()[0].row(), b"b");
# drop(feed);
# db.close()?;
# Ok(())
# }
```

## Durable or visible
By default a feed delivers a commit once it's durable at its commit's level. So nothing it delivered is ever undone, by a crash or a power loss: what you derived from it stays true.
- **`visible()`** delivers a commit as soon as reads see it. That's sooner, by up to a sync, but a power loss can undo a `Buffered` or `None` commit a visible feed delivered, and the feed's next resume then expires.
- **A fed table's `Durability::None` commits** reach the feed within 10 ms through a lazy write, even when nothing stronger follows them. A durable feed also waits for a sync to cover them. That sync is a background one after 100 ms (1 s on macOS) when no other commit's sync does.

## When a feed expires
A feed that can't continue without a gap fails with `ErrorCode::FeedExpired`, and stays failed. It never skips silently. This happens when:
- retention recycled the history it needs: raise `FeedRetention` if a slow consumer falls behind;
- the database was opened since with the feed off, without these tables, or with another explicit shard count;
- a power loss took commits a `visible()` feed had delivered;
- a short window after any metadata commit (such as a flush's, a checkpoint's, a compaction's, a schema change's or a shrink's) before the feed's record of it is safe: a process crash before the record is written (a single write, microseconds), or a power loss before it is synced.

Once a feed has expired, `Feed::oldest_retained` says how far back the history it needed still goes, when that's known. To recover, rebuild what the feed maintained from a snapshot and start a new feed `from_snapshot` of it.

## Async
`nonblocking::FeedStream` wraps a feed as a `Stream` of owned commits for any executor. It's woken, never spinning, when the frontier can move, and it yields an error once (`FeedExpired`, `Closed`) and then ends. `Feed::poll_next` is the same, one level down.

```rust
use futures_core::Stream;
use pigeonhole::nonblocking::FeedStream;
use pigeonhole::{Family, FeedRetention, Options, Pigeonhole};

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(
    dir.join("app.phdb"),
    Options::default().change_feed(FeedRetention::bytes(64 << 20)),
)?;
let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
let mut stream = FeedStream::new(t.feed().open()?);
t.mutate(b"row").put("f", b"q", b"v").commit()?;
# use pigeonhole::doc_support::block_on;
let next = std::future::poll_fn(|cx| std::pin::Pin::new(&mut stream).poll_next(cx));
let commit = block_on(next).expect("a feed doesn't end on its own")?;
assert_eq!(commit.changes()[0].row(), b"row");
# drop(stream);
# db.close()?;
# Ok(())
# }
```

The async form is for the writer process. In a reader process, `poll_next` is `Unsupported`: run the blocking `next` on a thread of its own (#1407 tracks an async form).

## Reader processes
A [reader process](readers.md) follows the writer's feed too: `ReadTable::feed()` gives the same `Feed`, with every start, from the writer's shared memory and its log files. The writer does no per-commit work for it. The reader polls with a short backoff (50 µs, doubling to 5 ms while nothing arrives). So after a quiet spell, a reader-process feed sees the next commit within about 5 ms of the writer's.

Across a writer restart, the feed follows the new writer from its own token, so a durable feed continues. While no writer runs, `next` fails with `NoWriter`, or with `SnapshotExpired` while a new one is starting. Neither is permanent: call `next` again on the same feed.

## Python and `phdb mcp`
- **Python** has the same feed: `Table.feed(...)`, `Feed.next(timeout=...)` with the GIL released, iteration, and `Feed.token()` (see [Python](python.md)).
- **`phdb mcp`** gives agents a stateless `feed_poll` tool. It returns whole commits and a token to pass back, so an agent can follow a database without holding a connection open (see [Using Pigeonhole from AI agents](ai-agents.md)).

## Cost
- **Writers:** a fed commit costs the same as an unfed one on the commit path. The feed's work happens once per log segment, retention pass and checkpoint, and for `None` commits in the lazy write.
- **Disk:** retention keeps up to the configured bytes of log, plus any blob files that history still points to (`DatabaseInfo::feed` reports both). The log stops growing once it holds the retention: old history is recycled, never blocking a writer.
- **Subscribers:** each delivered change is copied out of the log, and its value resolved if it was stored separately.

### What a feed costs
These numbers were measured on a Hetzner AX102: an AMD Ryzen 9 7950X3D (16 cores), an NVMe drive with power-loss protection, XFS, Linux 6.8 and io_uring. The workload was `phdb-bench ycsb-a`: 1 million rows of 100-byte values, half reads and half updates, from **one client thread** at `Durability::Buffered`. The figures are medians of 6 runs (D310, #1444, #1450).

| Setup | Writer throughput vs the feed off | Delivery latency, p50 / p99 |
|---|--:|--:|
| Feed on, no subscriber | 0.985x | – |
| One durable subscriber (the default) | 0.977x | 57 / 104 ms |
| One `visible()` subscriber | 0.953x | 42 / 126 µs |
| One durable subscriber, `GroupSync` commits | – | 1.1 / 1.5 ms |
| One reader-process subscriber (durable), idle | 0.006 CPU-seconds/s in the reader | 102 / 106 ms |

- **Durable delivery waits for a sync.** Under `Buffered` that's the feed's background sync, after 100 ms (1 s on macOS). For low latency use `visible()`, or commit at `GroupSync`, where each group's own sync covers its commits.
- **The `GroupSync` row was measured before #1450,** so only its latency is shown. In it, the feed never added a sync of its own beside the 500,000 group syncs.
- **An idle reader process keeps polling** with its 5 ms backoff. That costs about 0.6% of a core (0.010 CPU-s/s at 50 commits/s).

## Next
[Reader processes](readers.md) · [Async](async.md) · [Durability](durability.md) · [Errors](errors.md)
