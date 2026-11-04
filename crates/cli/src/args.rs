//! The command line: global flags and one subcommand per `phdb` command.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

/// phdb: inspect and maintain Pigeonhole databases.
///
/// Exit status: 0 success, 1 error, 2 usage, 3 corruption, 4 locked or busy, 5 not found,
/// 6 unsupported format or version.
#[derive(Debug, Parser)]
#[command(name = "phdb", version)]
pub(crate) struct Cli {
    /// Output format: `table` for people; `json` (one document) and `jsonl` (a header line,
    /// then one object per row) for programs; `csv` with a header line.
    /// `dump` defaults to `jsonl` and takes `jsonl`, `csv` or `arrow`; every other command
    /// defaults to `table`.
    #[arg(long, global = true, value_enum)]
    pub format: Option<Format>,

    /// Shared-memory directory, as the database's writer was opened with
    /// (`Options::shm_dir`).
    #[arg(long, global = true, value_name = "DIR")]
    pub shm_dir: Option<PathBuf>,

    /// Open as a reader process only: fail unless a writer process has the database open.
    #[arg(long, global = true, conflicts_with = "writer")]
    pub reader: bool,

    /// Open as the writer only: fail if a writer process has the database open.
    #[arg(long, global = true)]
    pub writer: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Format version, size and free space, and per table its tablets, families (with
    /// their stored options) and files.
    Info {
        /// The database file (`*.phdb`).
        file: PathBuf,
    },
    /// Export every cell (all retained versions) at one snapshot: `jsonl` (the default;
    /// `phdb load` reads it back), `csv`, or `arrow` (an Arrow IPC stream of one table), to
    /// standard output.
    Dump {
        /// The database file (`*.phdb`).
        file: PathBuf,
        /// Only this table (repeat for several).
        #[arg(long = "table", value_name = "TABLE")]
        tables: Vec<String>,
        /// Only rows starting with this prefix (UTF-8).
        #[arg(long)]
        prefix: Option<String>,
        /// Read at a durable snapshot: only commits durable at their level.
        #[arg(long)]
        durable: bool,
    },
    /// Load a `phdb dump --format jsonl` export: create its tables and families (an existing
    /// family keeps its stored options) and write every cell at its timestamp. Opens as the
    /// writer.
    Load {
        /// The database file (`*.phdb`).
        file: PathBuf,
        /// The dump to read; `-` or nothing reads standard input.
        input: Option<PathBuf>,
        /// Create the database file if it does not exist.
        #[arg(long)]
        create: bool,
    },
    /// Copy the database into one new file while it stays usable: by default only commits
    /// durable at their level (`--latest` adds the rest). With a writer process running, it
    /// copies from a reader process.
    Backup {
        /// The database file (`*.phdb`).
        file: PathBuf,
        /// The new file; it must not exist.
        dest: PathBuf,
        /// Copy everything committed, including commits not yet durable at their level.
        #[arg(long)]
        latest: bool,
        /// Check the copy afterwards (`phdb check`); exit 3 if it is damaged.
        #[arg(long)]
        check: bool,
    },
    /// Flush, then merge every level of every table into one (purging old versions, expired
    /// cells and deletes), with progress on standard error. Opens as the writer.
    Compact {
        /// The database file (`*.phdb`).
        file: PathBuf,
        /// Then release the file's free space at its end (`Pigeonhole::shrink`).
        #[arg(long)]
        shrink: bool,
    },
    /// An interactive shell (get, scan, put, incr, delete, create, snapshots; `\\help`):
    /// commands from `-c`, a script (`-f`), piped input, or a terminal.
    /// With a writer process running it is a read-only reader.
    Shell {
        /// The database file (`*.phdb`).
        file: PathBuf,
        /// Run this command, then exit (repeat for several, in order). The first failure
        /// stops with its exit status.
        #[arg(short = 'c', long = "command", value_name = "COMMAND")]
        commands: Vec<String>,
        /// Run the commands in this file, one per line, then exit.
        #[arg(short = 'f', long = "file", value_name = "SCRIPT")]
        script: Option<PathBuf>,
        /// Refuse writes even when phdb is the writer.
        #[arg(long)]
        read_only: bool,
    },
    /// Check the database for damage, offline: superblocks, manifest, extents, every SST
    /// block, every blob record and the WAL a crash left. Exits 3 if anything is damaged.
    Check {
        /// The database file (`*.phdb`).
        file: PathBuf,
        /// Also read every separated value through its pointer, and recount blob live bytes.
        #[arg(long)]
        deep: bool,
        /// List the file's regions (superblocks, manifest, SSTs, blob extents) instead of the
        /// findings, for `table` and `csv` output (`json` adds them as `regions`).
        #[arg(long)]
        map: bool,
        /// If a writer process has the database open, read every table through a reader
        /// process instead (a weaker check: only the data a scan reaches).
        #[arg(long)]
        online: bool,
    },
    /// A Model Context Protocol server on standard input and output, for AI agents: tools to
    /// list tables, read rows, scan, traverse graphs, write in atomic batches and create
    /// tables. Opens as `phdb shell` does, and holds the database until its input ends.
    Mcp {
        /// The database file (`*.phdb`).
        file: PathBuf,
        /// Open as a reader process (a writer process must have the database open): the
        /// write tools are left out.
        #[arg(long)]
        read_only: bool,
        /// Create the database file if it does not exist (not with `--read-only`).
        #[arg(long, conflicts_with = "read_only")]
        create: bool,
        /// A tool result's size, about, in bytes of JSON: a longer one is cut at a column
        /// boundary and returns a cursor to continue from.
        #[arg(long, value_name = "BYTES", default_value_t = 32 << 10)]
        max_result_bytes: usize,
        /// The longest value a read returns whole by default; a longer one is replaced by its
        /// length (a read may ask for more, up to `--max-result-bytes`).
        #[arg(long, value_name = "BYTES", default_value_t = 4 << 10)]
        max_value_bytes: usize,
        /// Turn the change feed on, keeping up to this many bytes of history (and serve
        /// `feed_poll`). Without it, a database whose application feeds keeps its feed, which
        /// `feed_poll` serves too: phdb's open inherits it.
        #[arg(long, value_name = "BYTES", conflicts_with = "read_only")]
        change_feed: Option<u64>,
        /// The agent memory tools (D311 T7).
        #[command(flatten)]
        memory: MemoryArgs,
    },
    /// Serve the database over the network (D313): gRPC and HTTP/JSON on one port (ICR
    /// 0112). Holds the database until stopped with Ctrl-C or SIGTERM, then closes it.
    #[cfg(feature = "serve")]
    Serve(Box<crate::serve::ServeArgs>),
}

/// `phdb mcp`'s agent memory options (D311 T7): a `pigeonhole-memory` store and the tools
/// over it.
#[derive(Debug, Clone, Default, clap::Args)]
#[command(next_help_heading = "Agent memory")]
pub(crate) struct MemoryArgs {
    /// Serve the memory tools over the store NAME (its tables are `mem_NAME` and
    /// `mem_NAME_text`), creating it if it is missing and the database is writable.
    #[arg(long, value_name = "NAME")]
    pub memory: Option<String>,
    /// A new store's embedding model id, when the client supplies the embeddings: the store
    /// is pinned to it, and refuses embeddings that name another model. With
    /// `--memory-dim`.
    #[arg(long, value_name = "ID", requires_all = ["memory", "memory_dim"], conflicts_with = "embed_url")]
    pub memory_model: Option<String>,
    /// A new store's embedding dimension, with `--memory-model`.
    #[arg(long, value_name = "N", requires = "memory_model")]
    pub memory_dim: Option<u32>,
    /// A new store's embedding element: f32, f16 (half the size) or bf16.
    #[arg(long, value_name = "ELEMENT", default_value = "f32", value_parser = ["f32", "f16", "bf16"], requires = "memory")]
    pub memory_element: String,
    /// A new store's text-index normalisation: nfc (the default), nfkc (folds ligatures and
    /// full-width forms, for text from PDFs, OCR or East Asian sources) or none. Fixed at
    /// create: for an existing store it must match the store's, or the server won't start.
    #[arg(long, value_name = "FORM", value_parser = ["nfc", "nfkc", "none"], requires = "memory")]
    pub memory_normalization: Option<String>,
    /// A new store's episodic memories are forgotten this many hours after they were made,
    /// unless pinned or a summary derives from them (the store's forget policy, D311 T2).
    #[arg(long, value_name = "HOURS", requires = "memory")]
    pub memory_episodic_ttl: Option<u64>,
    /// Compute the embeddings in the server through this OpenAI-compatible `/embeddings`
    /// endpoint (the full URL). The memory tools then take text only. Memory text and recall
    /// queries are sent to it.
    #[arg(long, value_name = "URL", requires_all = ["memory", "embed_model"])]
    pub embed_url: Option<String>,
    /// The endpoint's model name, sent with each request; a new store is pinned to it.
    #[arg(long, value_name = "NAME", requires = "embed_url")]
    pub embed_model: Option<String>,
    /// The environment variable holding the endpoint's API key, sent as a bearer token. The
    /// key itself is never a flag, and never logged.
    #[arg(long, value_name = "VAR", requires = "embed_url")]
    pub embed_key_env: Option<String>,
}

impl Cli {
    /// The output format: `--format`, else the command's default.
    pub(crate) fn format(&self) -> Format {
        self.format.unwrap_or(match self.command {
            Command::Dump { .. } => Format::Jsonl,
            _ => Format::Table,
        })
    }
}

/// The output format (`--format`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Format {
    /// Aligned text for people.
    Table,
    /// One JSON document.
    Json,
    /// A JSON header line, then one JSON object per row.
    Jsonl,
    /// Comma-separated values with a header line (RFC 4180).
    Csv,
    /// An Arrow IPC stream of one table's cells, one Arrow row per cell (`dump` only).
    Arrow,
}
