//! Platform glue: every syscall the crate makes lives under this module.

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(crate) use unix::*;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::*;

/// What the OS says about a process id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Liveness {
    /// No such process (or a zombie).
    Dead,
    /// Running, with its start time if the platform reports one.
    Alive(Option<u64>),
}

/// Whether a recorded process (pid plus start time, 0 = unknown) is still running.
pub(crate) fn process_alive(pid: u32, start_time: u64) -> bool {
    match process_liveness(pid) {
        Liveness::Dead => false,
        Liveness::Alive(None) => true,
        Liveness::Alive(Some(now)) => start_time == 0 || now == start_time,
    }
}

/// Validates a shared-memory name: non-empty, no path separators or NULs.
pub(crate) fn check_shared_name(name: &str) -> crate::Result<()> {
    if name.is_empty() || name.contains(['/', '\\', '\0']) {
        return Err(crate::Error::new(
            crate::ErrorKind::Other,
            "invalid shared-memory name",
        ));
    }
    Ok(())
}
