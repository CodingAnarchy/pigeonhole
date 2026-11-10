//! The `phdb` command-line tool (#692).
#![forbid(unsafe_code)]

/// jemalloc for the `phdb` binary only (D322, #1558): jemalloc measured 9-21% more throughput
/// than glibc's malloc on read-heavy work at 16-32 threads (glibc's own paths and its
/// cross-thread frees). The library crates
/// choose no allocator; an application picks its own (the guide's "Allocator" section).
#[cfg(all(feature = "jemalloc", not(windows)))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
