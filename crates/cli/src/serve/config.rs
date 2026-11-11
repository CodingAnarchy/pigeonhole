//! `phdb serve`'s flags, and the checks made before anything is opened: the bind rule (D313
//! §6) and the limits' defaults (§4).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub(crate) use super::sessions::Leases;

/// `--affinity`: whether the server's own threads are masked off the shards' cores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Affinity {
    /// Masked off the shards' cores (the default).
    On,
    /// No mask: the same threads, where the OS puts them.
    Off,
}

/// `phdb serve`'s arguments.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct ServeArgs {
    /// The database file (`*.phdb`). Not with `--cluster`, whose directory holds the
    /// partitions' files.
    #[arg(required_unless_present = "cluster", conflicts_with = "cluster")]
    pub file: Option<PathBuf>,
    /// Create the database file if it does not exist.
    #[arg(long)]
    pub create: bool,
    /// The address to serve gRPC and HTTP/JSON on, together (ICR 0112's one port).
    #[arg(long, value_name = "ADDR:PORT", default_value = "127.0.0.1:7448")]
    pub listen: SocketAddr,
    /// The server's certificate chain, PEM. With `--tls-key`, every connection is TLS.
    #[arg(long, value_name = "PEM", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// The server's private key, PEM.
    #[arg(long, value_name = "PEM", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// The bearer tokens: one per line, `read|write|admin [ns=a,b] <token>` (or
    /// `sha256:<hex>` for the token's digest). Also from `PHDB_SERVE_TOKENS`, in the same
    /// format. Never a flag's value: tokens don't belong in process listings.
    #[arg(long, value_name = "FILE")]
    pub token_file: Option<PathBuf>,
    /// Serve a non-loopback address without TLS and tokens. Anyone who can reach the port
    /// can then read and write everything. For trusted private networks only.
    #[arg(long)]
    pub insecure_no_tls_no_auth: bool,
    /// The database's shard count (default: half the physical cores, leaving the rest for the
    /// network side, on every open; a database with a change feed keeps the count the feed
    /// recorded). A count other than the feed's reshards it and ends the feed's history.
    #[arg(long, value_name = "N")]
    pub shards: Option<usize>,
    /// The largest request body, in bytes.
    #[arg(long, value_name = "BYTES", default_value_t = 16 << 20)]
    pub max_request_bytes: usize,
    /// The bytes of request bodies held at once, across all requests: a request that can't
    /// get its share within its deadline is refused (`Overloaded`).
    #[arg(long, value_name = "BYTES", default_value_t = 256 << 20)]
    pub inflight_bytes: usize,
    /// Requests served at once; more are refused at once (`Overloaded`), not queued.
    #[arg(long, value_name = "N", default_value_t = 1024)]
    pub max_concurrent: usize,
    /// Open connections at once, HTTP/1.1 and HTTP/2 alike; one past this is closed as soon
    /// as it's accepted. Default: half the open-file limit (`ulimit -n`), at most 4096.
    #[arg(long, value_name = "N")]
    pub max_connections: Option<usize>,
    /// Scans streaming at once; more are refused at once (`Overloaded`). A scan's stream
    /// outlives its request's slot in `--max-concurrent`, so it has its own cap.
    #[arg(long, value_name = "N", default_value_t = 64)]
    pub max_scans: usize,
    /// Feed streams (`Feed.Subscribe`) at once; more are refused at once (`Overloaded`).
    #[arg(long, value_name = "N", default_value_t = 256)]
    pub max_streams: usize,
    /// Distinct table sets subscribed at once: each is one engine subscription, shared by
    /// every stream on it. A stream on another set past this is refused (`Overloaded`).
    #[arg(long, value_name = "N", default_value_t = 32)]
    pub max_feed_hubs: usize,
    /// Feed streams reading their own catch-up feed at once (one that started from a token
    /// or a snapshot, or fell behind the shared one); the rest wait their turn.
    #[arg(long, value_name = "N", default_value_t = 16)]
    pub max_catch_up: usize,
    /// On a cluster node: forwards one fanned-out read (`Nearest`, a traversal's hop) has
    /// running at once to other nodes (D317 §7). The peers' own limits protect them.
    #[arg(long, value_name = "N", default_value_t = 8)]
    pub fanout_forwards: usize,
    /// Bytes of recent commits the shared feeds keep for their streams, in all: each table
    /// set's share is this over the sets subscribed. Each commit counts its encoded event, which
    /// holds its resume token (about 45 + 16 × shards bytes), and about 110 bytes more.
    /// Outside `--inflight-bytes`.
    #[arg(long, value_name = "BYTES", default_value_t = 256 << 20)]
    pub feed_ring_bytes: usize,
    /// How long a request may wait for the server's limits, in milliseconds. A write the
    /// server has submitted always answers its real outcome, however long it takes.
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub request_timeout_ms: u64,
    /// How long a scan waits for its client to take a page, in milliseconds. Past that, the
    /// stream ends with `DeadlineExceeded`, and the client resumes from its last page's
    /// `next`: a client that stops reading doesn't hold its scan slot and a page forever.
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub stream_idle_ms: u64,
    /// How long a transaction may sit idle between calls, in milliseconds, before it expires
    /// (`TransactionExpired`, if no `Commit` arrived).
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub txn_idle_ms: u64,
    /// The longest a transaction lives, in milliseconds, however busy.
    #[arg(long, value_name = "MS", default_value_t = 300_000)]
    pub txn_lifetime_ms: u64,
    /// How long a pinned snapshot may sit idle, in milliseconds: each read at it renews it.
    #[arg(long, value_name = "MS", default_value_t = 60_000)]
    pub snapshot_idle_ms: u64,
    /// The longest a pinned snapshot lives, in milliseconds. A held snapshot holds back
    /// compaction's version GC and blob GC.
    #[arg(long, value_name = "MS", default_value_t = 600_000)]
    pub snapshot_lifetime_ms: u64,
    /// Sessions (transactions, pinned snapshots, and the tombstones of committed
    /// transactions) held at once.
    #[arg(long, value_name = "N", default_value_t = 1024)]
    pub max_sessions: usize,
    /// Sessions one token may hold at once.
    #[arg(long, value_name = "N", default_value_t = 64)]
    pub max_sessions_per_token: usize,
    /// Threads for the network side (default: the CPUs the shards don't use, at least 2).
    /// With `--blocking-threads`'s default, the two together can be twice those CPUs: the
    /// blocking threads compete only while blocking calls run.
    #[arg(long, value_name = "N")]
    pub io_threads: Option<usize>,
    /// Threads for blocking calls (default: the CPUs the shards don't use, at least 2).
    #[arg(long, value_name = "N")]
    pub blocking_threads: Option<usize>,
    /// Threads that deliver the change feed to its streams, a runtime of their own so feed
    /// delivery never holds a request's worker (default: a quarter of the CPUs the shards
    /// don't use, at least 1, taken from `--io-threads`' default).
    #[arg(long, value_name = "N")]
    pub feed_threads: Option<usize>,
    /// Threads that read the engine's change feed into the shared subscriptions (hubs), a
    /// runtime of their own so a hub never waits behind its streams' delivery (default: a
    /// quarter of `--feed-threads`, at least 1). More let many busy subscriptions keep up,
    /// at a cost to the writes' latency on the same CPUs (#1719). A hub with a single stream
    /// runs on the feed threads beside it instead (#1689).
    #[arg(long, value_name = "N")]
    pub feed_hub_threads: Option<usize>,
    /// Whether the network, feed and blocking threads get a CPU mask off the shards' cores
    /// (Linux, where the engine pinned its shards). `off` keeps the same thread counts but
    /// leaves the threads where the OS puts them: for a host shared with other work, where
    /// the server shouldn't claim the CPUs (D313's R1, ICR 0131).
    #[arg(long, value_enum, value_name = "on|off", default_value_t = Affinity::On)]
    pub affinity: Affinity,
    /// The bytes one HTTP/2 stream may queue for its client beyond the client's window
    /// (default: hyper's 400 KiB; at least 16 KiB, one frame). Smaller bounds a feed
    /// stream's queued delivery.
    #[arg(long, value_name = "BYTES", value_parser = clap::value_parser!(u64).range(16_384..))]
    pub http2_send_buffer: Option<u64>,
    /// How long a shard thread polls its queue before it sleeps, in microseconds, or
    /// `adaptive` for the library's default (D198). The server's default is 0, no poll: a
    /// network round trip dwarfs the wakeup a poll saves, and on a busy server the poll was
    /// about a quarter to half of the CPU per put (D198 amendment 4, #1644).
    #[arg(long, value_name = "US|adaptive", default_value = "0")]
    pub shard_spin: ShardSpin,
    /// How long in-flight requests may take to finish at shutdown, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 10_000)]
    pub shutdown_grace_ms: u64,
    /// The memory store to serve as `Memory` (D313 ticket 8a), with `phdb mcp`'s flags.
    #[command(flatten)]
    pub memory: crate::args::MemoryArgs,
    /// The longest one embedding call to `--embed-url` may take, in milliseconds: shorter
    /// than `phdb mcp`'s 30 s, since a server's blocking threads wait on it.
    #[arg(
        long,
        value_name = "MS",
        default_value_t = 10_000,
        requires = "embed_url"
    )]
    pub embed_timeout_ms: u64,
    /// Memory calls at once (default: half the blocking threads, at least 1).
    #[arg(long, value_name = "N", requires = "memory")]
    pub memory_calls: Option<usize>,
    /// Where `Admin.Backup` writes, and (in `.phdb-serve-downloads`, the server's own, mode
    /// 0700) where `Admin.BackupDownload` keeps its copies for resumes. It must exist.
    /// Without it the server takes no backups. Put it on a disk with room for a copy or two
    /// of the database, not tmpfs.
    #[arg(long, value_name = "DIR")]
    pub backup_dir: Option<PathBuf>,
    /// Backups running at once (`Backup` and fresh downloads), each on a thread of its own;
    /// more are refused (`Overloaded`).
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub max_backups: usize,
    /// Turn the change feed on, keeping up to this many bytes of history: `Feed.Subscribe`
    /// and replicas (`Replication.Follow`) read it. Without it, a database whose feed is on
    /// keeps it (the open inherits it). A replica that falls further behind than this
    /// bootstraps again.
    #[arg(long, value_name = "BYTES")]
    pub change_feed: Option<u64>,
    /// Don't size each shard's write-ahead log at start-up. By default `serve` gives every
    /// shard's log the slots it holds when busy before it accepts connections (about 320 MiB
    /// a shard at the defaults, plus the change feed's retention), written and synced at the
    /// open, so the logs never grow while commits wait on them (#1621, ICR 0130). With this
    /// flag they grow on demand instead: less disk and a faster start, but a slower commit
    /// tail while they grow.
    #[arg(long)]
    pub no_wal_preallocate: bool,
    /// Serve this database as a replica of the primary server at this URL (`http://` or
    /// `https://`): it bootstraps from the primary's backup, then follows its durable
    /// commits, and refuses writes until `Admin.Promote`. Reads see a consistent prefix of
    /// the primary's commits. The primary needs `--backup-dir` and a change feed of every
    /// table.
    #[arg(long, value_name = "URL")]
    pub replica_of: Option<String>,
    /// The bearer token the replica sends the primary (admin scope there), alone in the file.
    /// Also from `PHDB_REPLICA_TOKEN`. Never a flag's value.
    #[arg(long, value_name = "FILE", requires = "replica_of")]
    pub replica_token_file: Option<PathBuf>,
    /// A replica's run: the primary's commits it applies in one commit of its own, at most
    /// (or 8 MiB). A caught-up replica applies what arrived at once, whatever the count.
    #[arg(long, value_name = "N", default_value_t = super::replica::RUN_EVENTS, requires = "replica_of")]
    pub replica_run: usize,
    /// Serve as a node of a partitioned cluster (D317, ICR 0117): DIR holds the node's
    /// partition map (`cluster-map.json`) and its partitions' files (`<partition>.phdb`). One
    /// server per DIR. So far the node keeps and distributes the map (`phdb cluster`); serving
    /// the partitions' data comes with D317's next ticket.
    #[arg(long, value_name = "DIR", requires = "node_id")]
    pub cluster: Option<PathBuf>,
    /// This node's id in the cluster's map (`[a-z0-9_-]`, up to 64).
    #[arg(long, value_name = "ID", requires = "cluster")]
    pub node_id: Option<String>,
    /// The bearer token this node presents to the other nodes (`cluster` scope there), alone
    /// in the file. Also from `PHDB_CLUSTER_TOKEN`. Never a flag's value.
    #[arg(long, value_name = "FILE", requires = "cluster")]
    pub cluster_token_file: Option<PathBuf>,
    /// A PEM file of the CA certificates this node trusts for its peers' TLS, beside the
    /// system's roots.
    #[arg(long, value_name = "PEM", requires = "cluster")]
    pub cluster_ca: Option<PathBuf>,
    /// How long `SetMap` waits for each other node to take the map, in milliseconds. A node
    /// that doesn't pulls it later.
    #[arg(long, value_name = "MS", default_value_t = 2_000, requires = "cluster")]
    pub cluster_push_timeout_ms: u64,
    /// A remote MCP (`/mcp`) tool result's size, about, in bytes of JSON: a longer one is
    /// cut and returns a cursor, as with `phdb mcp --max-result-bytes`.
    #[arg(long, value_name = "BYTES", default_value_t = 32 << 10)]
    pub mcp_max_result_bytes: usize,
    /// The longest value a `/mcp` read returns whole by default, as with `phdb mcp
    /// --max-value-bytes`.
    #[arg(long, value_name = "BYTES", default_value_t = 4 << 10)]
    pub mcp_max_value_bytes: usize,
    /// How long a `/mcp` session may sit idle between requests, in milliseconds, before it
    /// ends (the client then gets 404 and starts a new session).
    #[arg(long, value_name = "MS", default_value_t = 30 * 60 * 1000)]
    pub mcp_session_idle_ms: u64,
    /// The longest a `/mcp` session lives, in milliseconds, however busy.
    #[arg(long, value_name = "MS", default_value_t = 24 * 60 * 60 * 1000)]
    pub mcp_session_lifetime_ms: u64,
    /// Requests one `/mcp` session may have running at once. One past it waits briefly,
    /// then gets a retryable error.
    #[arg(long, value_name = "N", default_value_t = 8)]
    pub mcp_session_requests: usize,
    /// The bytes one `/mcp` session may keep for its cursors (a cut traversal's result).
    /// A larger result isn't kept: its cursor runs the traversal again.
    #[arg(long, value_name = "BYTES", default_value_t = 8 << 20)]
    pub mcp_session_cache_bytes: usize,
}

impl ServeArgs {
    /// The database file: present unless `--cluster` (clap requires one of the two).
    pub(crate) fn db_file(&self) -> &std::path::Path {
        self.file
            .as_deref()
            .expect("clap requires FILE unless --cluster is given")
    }
}

/// What the flags leave the server to do.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub max_request_bytes: usize,
    pub inflight_bytes: usize,
    pub max_concurrent: usize,
    /// Open connections at once (`--max-connections`, #1604).
    pub max_connections: usize,
    pub max_scans: usize,
    pub max_streams: usize,
    pub max_feed_hubs: usize,
    pub max_catch_up: usize,
    /// A fanned-out read's forwards at once (`--fanout-forwards`, D317 ticket 4b).
    pub fanout_forwards: usize,
    pub feed_ring_bytes: usize,
    /// How long a quiet feed stream waits before a progress event (ICR 0112: about 10 s).
    pub feed_quiet: Duration,
    /// A feed client's buffer: events and their encoded bytes (not flags; tests shrink them).
    pub feed_client_events: usize,
    pub feed_client_bytes: usize,
    pub request_timeout: Duration,
    pub stream_idle: Duration,
    pub leases: Leases,
    /// The remote MCP tools' result limits and a session's cursor cache.
    pub mcp: crate::mcp::Limits,
    /// Requests one `/mcp` session may have running at once.
    pub mcp_session_requests: usize,
}

impl ServeArgs {
    pub(crate) fn limits(&self) -> Limits {
        Limits {
            max_request_bytes: self.max_request_bytes,
            inflight_bytes: self.inflight_bytes.max(self.max_request_bytes),
            max_concurrent: self.max_concurrent.max(1),
            max_connections: self
                .max_connections
                .unwrap_or_else(|| {
                    super::listener::default_max_connections(super::listener::open_file_limit())
                })
                .max(1),
            max_scans: self.max_scans.max(1),
            max_streams: self.max_streams.max(1),
            max_feed_hubs: self.max_feed_hubs.max(1),
            max_catch_up: self.max_catch_up.max(1),
            fanout_forwards: self.fanout_forwards.max(1),
            feed_ring_bytes: self.feed_ring_bytes.max(1 << 16),
            feed_quiet: Duration::from_secs(10),
            feed_client_events: super::feed::CLIENT_EVENTS,
            feed_client_bytes: super::feed::CLIENT_BYTES,
            request_timeout: Duration::from_millis(self.request_timeout_ms),
            stream_idle: Duration::from_millis(self.stream_idle_ms.max(1)),
            leases: Leases {
                txn_idle: Duration::from_millis(self.txn_idle_ms.max(1)),
                txn_lifetime: Duration::from_millis(self.txn_lifetime_ms.max(1)),
                snapshot_idle: Duration::from_millis(self.snapshot_idle_ms.max(1)),
                snapshot_lifetime: Duration::from_millis(self.snapshot_lifetime_ms.max(1)),
                max_sessions: self.max_sessions.max(1),
                max_per_token: self.max_sessions_per_token.max(1),
                mcp_idle: Duration::from_millis(self.mcp_session_idle_ms.max(1)),
                mcp_lifetime: Duration::from_millis(self.mcp_session_lifetime_ms.max(1)),
            },
            mcp: crate::mcp::Limits {
                result_bytes: self.mcp_max_result_bytes,
                value_bytes: self.mcp_max_value_bytes,
                feed_bytes: None,
                cache_bytes: self.mcp_session_cache_bytes,
            },
            mcp_session_requests: self.mcp_session_requests.max(1),
        }
    }
}

/// D313 §6's bind rule: an address other hosts can reach needs TLS and tokens, unless the
/// operator explicitly opts out. Loopback needs neither (local development, sidecars).
pub(crate) fn check_bind(
    listen: SocketAddr,
    tls: bool,
    tokens: bool,
    insecure: bool,
) -> Result<(), String> {
    if listen.ip().is_loopback() || insecure || (tls && tokens) {
        return Ok(());
    }
    let missing = match (tls, tokens) {
        (false, false) => {
            "TLS (--tls-cert, --tls-key) and tokens (--token-file or PHDB_SERVE_TOKENS)"
        }
        (false, true) => "TLS (--tls-cert, --tls-key)",
        _ => "tokens (--token-file or PHDB_SERVE_TOKENS)",
    };
    Err(format!(
        "refusing to serve {listen}, which other hosts can reach, without {missing}; \
         --insecure-no-tls-no-auth overrides this"
    ))
}

/// `--shard-spin`'s largest window: 10 ms.
const MAX_SHARD_SPIN_US: u64 = 10_000;

/// `--shard-spin`: the shard threads' poll before they sleep (D198 amendment 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardSpin {
    /// The library's default for the CPU profile (D198: adaptive under `Embedded`).
    Adaptive,
    /// A fixed window, in microseconds; 0 doesn't poll.
    Micros(u64),
}

impl std::str::FromStr for ShardSpin {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        if s == "adaptive" {
            return Ok(Self::Adaptive);
        }
        let us: u64 = s
            .parse()
            .map_err(|_| format!("{s:?}: microseconds, or `adaptive`"))?;
        // A poll past this is a typo, not a setting: it would hold a CPU per shard through
        // every gap of up to that long (cli692's N1 on #1680).
        if us > MAX_SHARD_SPIN_US {
            return Err(format!(
                "{us} µs: at most {MAX_SHARD_SPIN_US} µs (the library's own cap is 1 ms)"
            ));
        }
        Ok(Self::Micros(us))
    }
}

impl ShardSpin {
    /// Applies it to the engine's options.
    pub(crate) fn apply(self, options: pigeonhole::Options) -> pigeonhole::Options {
        match self {
            Self::Adaptive => options,
            Self::Micros(us) => options.shard_spin(Duration::from_micros(us)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ShardSpin, check_bind};

    #[test]
    fn a_reachable_address_needs_tls_and_tokens() {
        let public = "0.0.0.0:7448".parse().unwrap();
        let local = "127.0.0.1:7448".parse().unwrap();
        assert!(check_bind(local, false, false, false).is_ok());
        assert!(check_bind(public, true, true, false).is_ok());
        assert!(
            check_bind(public, false, false, true).is_ok(),
            "the explicit opt-out"
        );
        let e = check_bind(public, true, false, false).unwrap_err();
        assert!(e.contains("tokens") && !e.contains("TLS ("), "{e}");
        let e = check_bind(public, false, true, false).unwrap_err();
        assert!(
            e.contains("TLS") && e.contains("--insecure-no-tls-no-auth"),
            "{e}"
        );
        assert!(check_bind("[::1]:7448".parse().unwrap(), false, false, false).is_ok());
    }

    #[test]
    fn shard_spin_takes_microseconds_or_adaptive() {
        assert_eq!("0".parse::<ShardSpin>(), Ok(ShardSpin::Micros(0)));
        assert_eq!("200".parse::<ShardSpin>(), Ok(ShardSpin::Micros(200)));
        assert_eq!("adaptive".parse::<ShardSpin>(), Ok(ShardSpin::Adaptive));
        assert!("1ms".parse::<ShardSpin>().is_err());
        assert!("-1".parse::<ShardSpin>().is_err());
        assert_eq!("10000".parse::<ShardSpin>(), Ok(ShardSpin::Micros(10_000)));
        assert!("1000000".parse::<ShardSpin>().is_err());
    }
}
