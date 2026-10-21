//! The commands. Each returns a [`Report`](crate::output::Report) or a
//! [`Failure`](crate::exit::Failure).

use std::io::Write;

use crate::args::{Cli, Command};
use crate::exit::Failure;
use crate::output::Report;

mod check;
mod dump;
mod info;
mod load;
pub(crate) mod schema;

/// What a command produced: a report to render, or output it wrote itself (`dump`
/// streams its rows), with the status to exit with.
pub(crate) enum Outcome {
    Report(Report),
    Streamed(crate::ExitStatus),
}

pub(crate) fn run(cli: Cli, stdout: &mut dyn Write) -> Result<Outcome, Failure> {
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
        Command::Load {
            file,
            input,
            create,
        } => load::run(&cli, file, input.as_deref(), *create),
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
