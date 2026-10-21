//! The `phdb` command-line tool (#692).
#![forbid(unsafe_code)]

fn main() -> std::process::ExitCode {
    let status = pigeonhole_cli::run(
        std::env::args_os(),
        &mut std::io::stdout().lock(),
        &mut std::io::stderr().lock(),
    );
    status.into()
}
