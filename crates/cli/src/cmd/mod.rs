//! The commands. Each returns a [`Report`](crate::output::Report) or a
//! [`Failure`](crate::exit::Failure).

use std::io::Write;

use crate::args::{Cli, Command};
use crate::exit::Failure;
use crate::output::Report;

mod backup;
mod check;
mod compact;
pub(crate) mod dump;
pub(crate) mod info;
mod load;
pub(crate) mod schema;

/// What a command produced: a report to render, or output it wrote itself (`dump`
/// streams its rows), with the status to exit with.
pub(crate) enum Outcome {
    Report(Report),
    Streamed(crate::ExitStatus),
}

pub(crate) fn run(
    cli: Cli,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Outcome, Failure> {
    let report = match &cli.command {
        Command::Dump {
            file,
            tables,
            prefix,
            durable,
        } => {
            return dump::run(&cli, file, tables, prefix.as_deref(), *durable, stdout)
                .map(Outcome::Streamed);
        }
        Command::Shell {
            file,
            commands,
            script,
            read_only,
        } => {
            return crate::shell::run(
                &cli,
                file,
                commands,
                script.as_deref(),
                *read_only,
                stdout,
                stderr,
            )
            .map(Outcome::Streamed);
        }
        Command::Mcp {
            file,
            read_only,
            create,
            max_result_bytes,
            max_value_bytes,
        } => {
            let limits = crate::mcp::Limits {
                result_bytes: *max_result_bytes,
                value_bytes: *max_value_bytes,
            };
            return crate::mcp::run(&cli, file, *read_only, *create, limits, stdout, stderr)
                .map(Outcome::Streamed);
        }
        Command::Load {
            file,
            input,
            create,
        } => load::run(&cli, file, input.as_deref(), *create),
        Command::Backup {
            file,
            dest,
            latest,
            check,
        } => backup::run(&cli, file, dest, *latest, *check),
        Command::Compact { file, shrink } => compact::run(&cli, file, *shrink, stderr),
        Command::Info { file } => info::run(&cli, file),
        Command::Check {
            file,
            deep,
            map,
            online,
        } => check::run(&cli, file, *deep, *map, *online),
    }?;
    Ok(Outcome::Report(report))
}
