//! The `phdb` command-line tool (#692).
#![forbid(unsafe_code)]

fn main() -> std::process::ExitCode {
    let status = pigeonhole_cli::run(
        std::env::args_os(),
        // Unlocked handles, which lock per write: a held lock blocks every other thread that
        // prints (an `eprintln!` from a `phdb serve` worker hung forever, #1599).
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    );
    status.into()
}
