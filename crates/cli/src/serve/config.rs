//! `phdb serve`'s flags, and the checks made before anything is opened: the bind rule (D313
//! §6) and the limits' defaults (§4).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub(crate) use super::sessions::Leases;

/// `phdb serve`'s arguments.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct ServeArgs {
    /// The database file (`*.phdb`).
    pub file: PathBuf,
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
    /// Shards for a database this command creates, or to reshard to (default: half the
    /// physical cores, leaving the rest for the network side).
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
    /// Bytes of recent commits the shared feeds keep for their streams, in all: each table
    /// set's share is this over the sets subscribed. Each commit counts its changes and its
    /// resume token (about 45 + 16 × shards bytes). Outside `--inflight-bytes`.
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
    /// don't use, but at least 4 where they leave 8 or more, taken from `--io-threads`'
    /// default).
    #[arg(long, value_name = "N")]
    pub feed_threads: Option<usize>,
    /// The bytes one HTTP/2 stream may queue for its client beyond the client's window
    /// (default: hyper's 400 KiB). Smaller bounds a feed stream's queued delivery.
    #[arg(long, value_name = "BYTES")]
    pub http2_send_buffer: Option<usize>,
    /// How long in-flight requests may take to finish at shutdown, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 10_000)]
    pub shutdown_grace_ms: u64,
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
}

/// What the flags leave the server to do.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub max_request_bytes: usize,
    pub inflight_bytes: usize,
    pub max_concurrent: usize,
    pub max_scans: usize,
    pub max_streams: usize,
    pub max_feed_hubs: usize,
    pub max_catch_up: usize,
    pub feed_ring_bytes: usize,
    /// How long a quiet feed stream waits before a progress event (ICR 0112: about 10 s).
    pub feed_quiet: Duration,
    /// A feed client's buffer: events and their encoded bytes (not flags; tests shrink them).
    pub feed_client_events: usize,
    pub feed_client_bytes: usize,
    pub request_timeout: Duration,
    pub stream_idle: Duration,
    pub leases: Leases,
}

impl ServeArgs {
    pub(crate) fn limits(&self) -> Limits {
        Limits {
            max_request_bytes: self.max_request_bytes,
            inflight_bytes: self.inflight_bytes.max(self.max_request_bytes),
            max_concurrent: self.max_concurrent.max(1),
            max_scans: self.max_scans.max(1),
            max_streams: self.max_streams.max(1),
            max_feed_hubs: self.max_feed_hubs.max(1),
            max_catch_up: self.max_catch_up.max(1),
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
            },
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

#[cfg(test)]
mod tests {
    use super::check_bind;

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
}
