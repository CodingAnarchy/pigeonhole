//! Exit statuses and how errors are reported.

use std::io::Write;

use pigeonhole::{Error, ErrorCode};

use crate::args::Format;

/// The status `phdb` exits with ([`run`](crate::run)'s result).
///
/// | Status | Meaning |
/// |---|---|
/// | 0 | Success |
/// | 1 | An error not listed below |
/// | 2 | Usage: bad arguments |
/// | 3 | Corruption |
/// | 4 | Locked or busy: another process holds the writer lock, or the database is too busy |
/// | 5 | Not found: the file, a table or a family |
/// | 6 | Unsupported: the file's format version, or a feature this system lacks |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExitStatus(u8);

impl ExitStatus {
    /// 0: success.
    pub const SUCCESS: Self = Self(0);
    /// 1: an error not listed below.
    pub const ERROR: Self = Self(1);
    /// 2: bad arguments.
    pub const USAGE: Self = Self(2);
    /// 3: corruption.
    pub const CORRUPTION: Self = Self(3);
    /// 4: locked or busy.
    pub const LOCKED: Self = Self(4);
    /// 5: the file, a table or a family is missing.
    pub const NOT_FOUND: Self = Self(5);
    /// 6: unsupported format version or feature.
    pub const UNSUPPORTED: Self = Self(6);

    /// The numeric status.
    pub fn code(self) -> u8 {
        self.0
    }
}

impl From<ExitStatus> for std::process::ExitCode {
    fn from(s: ExitStatus) -> Self {
        s.0.into()
    }
}

/// A command that failed: what to print and the status to exit with.
#[derive(Debug)]
pub(crate) struct Failure {
    /// The `ErrorCode` name, or a CLI name such as `FileNotFound` when none applies.
    code: String,
    /// The `ErrorCode` number; `None` for a CLI-side failure.
    code_num: Option<u32>,
    message: String,
    status: ExitStatus,
}

impl Failure {
    /// The database file does not exist.
    pub(crate) fn file_not_found(path: &std::path::Path) -> Self {
        Self {
            code: "FileNotFound".into(),
            code_num: None,
            message: format!("{}: no such file", path.display()),
            status: ExitStatus::NOT_FOUND,
        }
    }

    /// An I/O error outside the database (writing the output).
    pub(crate) fn io(what: &str, e: &std::io::Error) -> Self {
        Self {
            code: "Io".into(),
            code_num: Some(ErrorCode::Io as u32),
            message: format!("{what}: {e}"),
            status: ExitStatus::ERROR,
        }
    }

    /// A database error, with `context` (what phdb was doing) before its message.
    pub(crate) fn db(context: &str, e: &Error) -> Self {
        let code = e.code();
        Self {
            code: format!("{code:?}"),
            code_num: Some(code as u32),
            message: format!("{context}: {}", e.message()),
            status: status_of(code),
        }
    }

    pub(crate) fn status(&self) -> ExitStatus {
        self.status
    }

    /// Writes the failure to `out`: one line for people, or with `--format json`/`jsonl` a
    /// JSON object `{"phdb":1,"error":{"code","code_num","message"}}`.
    pub(crate) fn render(&self, format: Format, out: &mut dyn Write) {
        let _ = match format {
            Format::Json | Format::Jsonl => writeln!(
                out,
                "{}",
                serde_json::json!({
                    "phdb": 1,
                    "error": {"code": self.code, "code_num": self.code_num, "message": self.message},
                })
            ),
            Format::Table | Format::Csv => writeln!(out, "phdb: error: {}", self.message),
        };
    }
}

/// The exit status for a database error.
fn status_of(code: ErrorCode) -> ExitStatus {
    match code {
        ErrorCode::Corruption => ExitStatus::CORRUPTION,
        ErrorCode::WriterLocked | ErrorCode::Busy | ErrorCode::NoReaderSlot => ExitStatus::LOCKED,
        ErrorCode::TableNotFound | ErrorCode::FamilyNotFound => ExitStatus::NOT_FOUND,
        ErrorCode::UnsupportedFormat | ErrorCode::ShmVersionMismatch | ErrorCode::Unsupported => {
            ExitStatus::UNSUPPORTED
        }
        _ => ExitStatus::ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_follow_the_documented_table() {
        assert_eq!(status_of(ErrorCode::Corruption).code(), 3);
        assert_eq!(status_of(ErrorCode::WriterLocked).code(), 4);
        assert_eq!(status_of(ErrorCode::Busy).code(), 4);
        assert_eq!(status_of(ErrorCode::NoReaderSlot).code(), 4);
        assert_eq!(status_of(ErrorCode::TableNotFound).code(), 5);
        assert_eq!(status_of(ErrorCode::FamilyNotFound).code(), 5);
        assert_eq!(status_of(ErrorCode::UnsupportedFormat).code(), 6);
        assert_eq!(status_of(ErrorCode::ShmVersionMismatch).code(), 6);
        assert_eq!(status_of(ErrorCode::Unsupported).code(), 6);
        assert_eq!(status_of(ErrorCode::Io).code(), 1);
        assert_eq!(status_of(ErrorCode::Conflict).code(), 1);
    }
}
