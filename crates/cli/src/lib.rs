//! phdb: the command-line tool for Pigeonhole databases (#692).
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). The binary is the
//! product; this library exists so the commands can be tested in process. Its one public
//! item is [`run`]: the command language, the output shapes and the exit codes are
//! documented in the [CLI guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/cli.md),
//! not in this crate's API.
#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::io::Write;

mod args;
mod cmd;
mod exit;
mod open;
mod output;

pub use exit::ExitStatus;

/// Runs `phdb` with `args` (the program name first, as from [`std::env::args_os`]), writing
/// command output to `stdout` and errors to `stderr`, and returns the exit status the
/// process should end with.
pub fn run<I, T>(args: I, stdout: &mut dyn Write, stderr: &mut dyn Write) -> ExitStatus
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    use clap::Parser;
    let cli = match args::Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            let (out, status): (&mut dyn Write, _) = match e.kind() {
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => (stdout, ExitStatus::SUCCESS),
                _ => (stderr, ExitStatus::USAGE),
            };
            let _ = write!(out, "{}", e.render());
            return status;
        }
    };
    let format = cli.format;
    match cmd::run(cli) {
        Ok(report) => match output::render(&report, format, stdout) {
            Ok(()) => report.status,
            // A closed pipe (`phdb … | head`) is not an error worth a message.
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => report.status,
            Err(e) => {
                let e = exit::Failure::io("write output", &e);
                e.render(format, stderr);
                e.status()
            }
        },
        Err(e) => {
            e.render(format, stderr);
            e.status()
        }
    }
}
