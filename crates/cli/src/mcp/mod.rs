//! `phdb mcp`: a Model Context Protocol server on standard input and output (#1249).
//!
//! The stdio transport: one JSON-RPC 2.0 message per line in each direction, and nothing
//! else on standard output (diagnostics go to standard error). The server answers
//! `initialize`, `ping`, `tools/list` and `tools/call` (and, with `--memory`,
//! `resources/list` and `resources/read` for `memory://guide`), ignores notifications, and
//! exits when its input ends, closing the database.
//!
//! A hand-written loop rather than an MCP SDK: the server is synchronous and needs four
//! methods, and the Rust SDKs bring an async runtime and a schema generator for them.
//!
//! The protocol itself, apart from the transport, is in [`protocol`] (D313 ticket 8c), which
//! `phdb serve`'s `/mcp` drives too; the tools are in [`tools`].

use std::io::{BufRead, Write};
use std::path::Path;

use crate::ExitStatus;
use crate::args::{Cli, MemoryArgs};
use crate::exit::Failure;
use crate::open::{self, Db};

mod backend;
mod cursor;
mod embed;
mod protocol;
// `phdb serve`'s memory core embeds through it (D313 ticket 8a), and so does `phdb mcp` (8b).
pub(crate) use embed::{EmbedFault, Endpoint};
mod memory;
mod tools;

/// Result-size limits (`--max-result-bytes`, `--max-value-bytes`).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// A result's size, about, in bytes of its JSON.
    pub result_bytes: usize,
    /// The longest value a read returns whole unless it asks for more.
    pub value_bytes: usize,
    /// `--change-feed`: turn the change feed on with this retention in bytes.
    pub feed_bytes: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    cli: &Cli,
    file: &Path,
    read_only: bool,
    create: bool,
    limits: Limits,
    memory_args: &MemoryArgs,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<ExitStatus, Failure> {
    let db = if read_only {
        open::reader_only(cli, file)?
    } else if create && !file.exists() {
        Db::Writer(open::for_write_with_feed(
            cli,
            file,
            true,
            limits.feed_bytes,
        )?)
    } else {
        open::for_read_with_feed(cli, file, limits.feed_bytes)?
    };
    let writable = !read_only && db.writable();
    let _ = writeln!(
        stderr,
        "phdb mcp: serving {} as {}{}",
        file.display(),
        db.mode(),
        if writable { "" } else { " (read-only)" }
    );
    let memory = match memory::Memory::open(&db, memory_args, writable, limits.result_bytes, stderr)
    {
        Ok(m) => m,
        Err(f) => {
            // Close what was opened; the failure is the one to report.
            let _ = db.close();
            return Err(f);
        }
    };
    let tools = tools::Tools::new(Box::new(db), writable, limits, memory);
    let served = serve(&tools, &mut std::io::stdin().lock(), stdout);
    let closed = tools.close();
    if let Err(e) = served {
        // A client that went away mid-reply is the end of the session, not a failure.
        if e.kind() != std::io::ErrorKind::BrokenPipe {
            return Err(Failure::io("mcp", &e));
        }
    }
    closed?;
    Ok(ExitStatus::SUCCESS)
}

/// Answers each line of `input` on `output` until the input ends: one session, over the
/// stdio transport.
fn serve(
    tools: &tools::Tools,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> std::io::Result<()> {
    let protocol = protocol::Core;
    let mut session = protocol::Session::default();
    let mut line = String::new();
    loop {
        line.clear();
        if input.read_line(&mut line)? == 0 {
            return Ok(());
        }
        if line.trim().is_empty() {
            continue;
        }
        if let Some(reply) = protocol.message(tools, &mut session, &line) {
            writeln!(output, "{reply}")?;
            output.flush()?;
        }
        // After the reply: tell a client that listed the tools when a call changed them.
        if let Some(note) = protocol.list_changed(tools, &mut session) {
            writeln!(output, "{note}")?;
            output.flush()?;
        }
    }
}
