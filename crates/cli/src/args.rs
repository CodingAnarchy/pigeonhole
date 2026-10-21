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
    #[arg(long, global = true, value_enum, default_value_t = Format::Table)]
    pub format: Format,

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
