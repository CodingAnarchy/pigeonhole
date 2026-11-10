//! What the MCP tools reach the database through (D313 ticket 8c-2, §7 as amended): the
//! tools' parsing, paging, cursors, budgets and JSON stay in [`tools`](super::tools), and
//! their database calls go through a [`Backend`].
//!
//! - **`phdb mcp`** (stdio) uses [`Db`], the database it opened: the calls are the ones the
//!   tools always made.
//! - **`phdb serve`'s `/mcp`** (8c-3) gets its tables and makes its schema changes through
//!   the service layer's own functions, after one `Service::admit` shared with gRPC and
//!   HTTP. So a tool can't reach a table, or make a change, that the service would refuse
//!   (its scopes, the reserved `__phdb_` prefix, the served memory store's tables), and
//!   `phdb mcp` keeps none of the server's dependencies.

use pigeonhole::{Family, FamilyInfo, Pigeonhole, Table};

use crate::exit::Failure;
use crate::open::{Db, Handle};

/// The tools' database surface.
pub(crate) trait Backend: Send + Sync {
    /// How the database is open, for the tools' answers ("writer", "reader", ...).
    fn mode(&self) -> &'static str;
    /// Whether `feed_poll` belongs in `tools/list`: the writer's feed is on, or this is a
    /// reader process, which follows the writer's feed whatever its own `info` says (#1361).
    fn lists_feed(&self) -> bool;
    /// The tables, by name.
    fn tables(&self) -> Vec<String>;
    /// A table's families.
    fn families(&self, table: &str) -> pigeonhole::Result<Vec<FamilyInfo>>;
    /// A table to read (or follow the feed of).
    fn handle(&self, table: &str) -> Result<Handle, Failure>;
    /// The writer's database, for a write batch: `None` unless writes can succeed.
    fn writer(&self) -> Option<&Pigeonhole>;
    /// A table to write, as the writer: `None` unless writes can succeed.
    fn open_for_write(&self, table: &str) -> Option<pigeonhole::Result<Table>>;
    /// Creates `table` with `families`, or opens it if it exists: `None` unless writes can
    /// succeed.
    fn create_table(
        &self,
        table: &str,
        families: Vec<(String, Family)>,
    ) -> Option<pigeonhole::Result<Table>>;
    /// Closes the database (stdio, at the end of input); a no-op where the backend doesn't
    /// own it.
    fn close(self: Box<Self>) -> Result<(), Failure>;
}

impl Backend for Db {
    fn mode(&self) -> &'static str {
        Db::mode(self)
    }

    fn lists_feed(&self) -> bool {
        matches!(self, Db::Reader(_)) || self.info().is_ok_and(|i| i.feed.is_some())
    }

    fn tables(&self) -> Vec<String> {
        Db::tables(self)
    }

    fn families(&self, table: &str) -> pigeonhole::Result<Vec<FamilyInfo>> {
        Db::families(self, table)
    }

    fn handle(&self, table: &str) -> Result<Handle, Failure> {
        Db::handle(self, table)
    }

    fn writer(&self) -> Option<&Pigeonhole> {
        match self {
            Db::Writer(db) => Some(db),
            Db::Reader(_) => None,
        }
    }

    fn open_for_write(&self, table: &str) -> Option<pigeonhole::Result<Table>> {
        let db = Backend::writer(self)?;
        Some(db.table(table).and_then(|b| b.open()))
    }

    fn create_table(
        &self,
        table: &str,
        families: Vec<(String, Family)>,
    ) -> Option<pigeonhole::Result<Table>> {
        let db = Backend::writer(self)?;
        Some(db.table(table).and_then(|mut b| {
            for (name, family) in families {
                b = b.family(&name, family);
            }
            b.create_if_missing()
        }))
    }

    fn close(self: Box<Self>) -> Result<(), Failure> {
        Db::close(*self)
    }
}
