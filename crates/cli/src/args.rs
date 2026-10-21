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
    /// `dump` defaults to `jsonl` and takes `jsonl` or `csv`; every other command defaults
    /// to `table`.
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
    /// Check the database for damage, offline: superblocks, manifest, extents, every SST
    /// block, every blob record and the WAL a crash left. Exits 3 if anything is damaged.
    /// Export every cell (all retained versions) at one snapshot: `jsonl` (the default;
    /// `phdb load` reads it back) or `csv`, to standard output.
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
}
