//! The commands. Each returns a [`Report`](crate::output::Report) or a
//! [`Failure`](crate::exit::Failure).

use crate::args::{Cli, Command};
use crate::exit::Failure;
use crate::output::Report;

mod check;
mod info;

pub(crate) fn run(cli: Cli) -> Result<Report, Failure> {
    match &cli.command {
        Command::Info { file } => info::run(&cli, file),
        Command::Check {
            file,
            deep,
            map,
            online,
        } => check::run(&cli, file, *deep, *map, *online),
    }
}
