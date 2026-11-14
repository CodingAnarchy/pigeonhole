//! The hidden entry for `phdb serve`'s instruction-count shapes (#1828,
//! `examples/serveshapes.rs`): a single node's router, built by the construction
//! `serve::run` serves (`serve::single`), and nothing else. The caller drives it in
//! process, without a listener: the requests take the same middleware, handlers and
//! service as a connection's. Not part of any API (ICR 0086).
//!
//! What `serve_shared` adds when it serves is not here: the router has no feed or feed hub
//! runtimes (no feed or subscription call can run on it) and no placement masks on its
//! callers' threads. `examples/serveshapes.rs` drives only `Data` calls, which use neither.

use pigeonhole::Pigeonhole;

use crate::args::{Cli, Command};

/// A single node's router, from `phdb serve` arguments ([`router`]).
#[doc(hidden)]
pub struct Router {
    /// Every route `phdb serve` serves: gRPC and HTTP/JSON behind the concurrency limit,
    /// and health and metrics beside them.
    pub router: axum::Router,
    dbs: Vec<Pigeonhole>,
}

impl Router {
    /// Closes the database once the router's calls are done.
    pub fn close(self) -> Result<(), String> {
        drop(self.router);
        for db in self.dbs {
            db.close().map_err(|e| format!("close: {e}"))?;
        }
        Ok(())
    }
}

/// The router a single node serves with `args` (`phdb`, then `serve` and its arguments, as
/// on the command line), built by `serve::single` as `serve::run` builds it before it
/// listens. Fails on anything else, on a cluster node, and on a replica (`--replica-of`):
/// `serve_shared` starts a replica's follow loop, so its router here would refuse writes
/// and never follow.
#[doc(hidden)]
pub fn router<I, T>(args: I, stderr: &mut dyn std::io::Write) -> Result<Router, String>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    use clap::Parser;
    let cli = Cli::try_parse_from(args).map_err(|e| e.to_string())?;
    let Command::Serve(args) = &cli.command else {
        return Err("not `phdb serve`".to_owned());
    };
    if args.cluster.is_some() {
        return Err("a single node only, not --cluster".to_owned());
    }
    if args.replica_of.is_some() {
        return Err("a primary only, not --replica-of".to_owned());
    }
    let serving = super::single(&cli, args, stderr).map_err(|e| format!("{e:?}"))?;
    // The databases to close, as `serve_shared` finds them (#1862).
    let dbs = serving
        .shared
        .router
        .parts()
        .iter()
        .map(|p| p.db.clone())
        .collect();
    Ok(Router {
        router: super::http::router(serving.shared),
        dbs,
    })
}
