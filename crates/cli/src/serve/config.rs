//! `phdb serve`'s flags, and the checks made before anything is opened: the bind rule (D313
//! §6) and the limits' defaults (§4).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

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
    /// How long a request may wait for the server's limits, in milliseconds. A write the
    /// server has submitted always answers its real outcome, however long it takes.
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub request_timeout_ms: u64,
    /// Threads for the network side (default: the CPUs the shards don't use, at least 2).
    /// With `--blocking-threads`'s default, the two together can be twice those CPUs: the
    /// blocking threads compete only while blocking calls run.
    #[arg(long, value_name = "N")]
    pub io_threads: Option<usize>,
    /// Threads for blocking calls (default: the CPUs the shards don't use, at least 2).
    #[arg(long, value_name = "N")]
    pub blocking_threads: Option<usize>,
    /// How long in-flight requests may take to finish at shutdown, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 10_000)]
    pub shutdown_grace_ms: u64,
}

/// What the flags leave the server to do.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub max_request_bytes: usize,
    pub inflight_bytes: usize,
    pub max_concurrent: usize,
    pub max_scans: usize,
    pub request_timeout: Duration,
}

impl ServeArgs {
    pub(crate) fn limits(&self) -> Limits {
        Limits {
            max_request_bytes: self.max_request_bytes,
            inflight_bytes: self.inflight_bytes.max(self.max_request_bytes),
            max_concurrent: self.max_concurrent.max(1),
            max_scans: self.max_scans.max(1),
            request_timeout: Duration::from_millis(self.request_timeout_ms),
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
