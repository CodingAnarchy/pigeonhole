//! Errors: `Pigeonhole::Error < StandardError`, and one subclass per [`ErrorCode`] named as
//! the code, each instance carrying `code` (the name) and `code_num` (ICR 0111).

use magnus::{Error, ExceptionClass, RModule, Ruby, prelude::*};
use pigeonhole::ErrorCode;

/// Declares the codes: the module registration and the code → class-name mapping.
macro_rules! codes {
    ($($code:ident),* $(,)?) => {
        /// The class name for `code`, or `None` for a code this build doesn't know.
        fn name(code: ErrorCode) -> Option<&'static str> {
            match code {
                $(ErrorCode::$code => Some(stringify!($code)),)*
                _ => None,
            }
        }

        /// Defines `Pigeonhole::Error` and every code's subclass under `m`.
        pub(crate) fn register(ruby: &Ruby, m: RModule) -> Result<(), Error> {
            let base = m.define_error("Error", ruby.exception_standard_error())?;
            $(m.define_error(stringify!($code), base)?;)*
            Ok(())
        }
    };
}

codes!(
    Io,
    Corruption,
    WriterLocked,
    ShmVersionMismatch,
    ShmUnavailable,
    UnsupportedFormat,
    NetworkFilesystem,
    TableNotFound,
    TableExists,
    FamilyNotFound,
    FamilyExists,
    UnknownMergeOperator,
    MergeFailed,
    Conflict,
    ReadOnly,
    KeyTooLarge,
    ValueTooLarge,
    NoSpace,
    InvalidArgument,
    Unsupported,
    Closed,
    NoReaderSlot,
    RecordTooLarge,
    Busy,
    SnapshotExpired,
    WouldDeadlock,
    BatchTooLarge,
    OutcomeUnknown,
    NoWriter,
    UnsupportedFeature,
    FeedExpired,
);

/// `code`'s exception with `message`, its `@code` and `@code_num` set.
pub(crate) fn with_code(ruby: &Ruby, code: ErrorCode, message: &str) -> Error {
    let make = || -> Result<Error, Error> {
        let m: RModule = ruby.class_object().const_get("Pigeonhole")?;
        let class: ExceptionClass = m.const_get(name(code).unwrap_or("Error"))?;
        // `Pigeonhole::Error#initialize(message, code, code_num)` (lib/pigeonhole.rb).
        let e = class.new_instance((message, format!("{code:?}"), code as u32))?;
        Ok(Error::from(e))
    };
    make().unwrap_or_else(|e| e)
}

/// A library error as its Ruby exception.
pub(crate) fn to_rb(ruby: &Ruby, e: &pigeonhole::Error) -> Error {
    with_code(ruby, e.code(), e.message())
}

/// `Result` with library errors converted.
pub(crate) trait OrRb<T> {
    /// The library error, if any, as its Ruby exception.
    fn or_rb(self, ruby: &Ruby) -> Result<T, Error>;
}

impl<T> OrRb<T> for pigeonhole::Result<T> {
    fn or_rb(self, ruby: &Ruby) -> Result<T, Error> {
        self.map_err(|e| to_rb(ruby, &e))
    }
}

/// Runs the engine call `f` with the GVL released ([`crate::gvl::without_gvl`]), its
/// library error raised as the Ruby exception.
pub(crate) fn engine<F, R>(ruby: &Ruby, f: F) -> Result<R, Error>
where
    F: FnOnce() -> pigeonhole::Result<R> + Send,
    R: Send,
{
    crate::gvl::without_gvl(ruby, f)?.or_rb(ruby)
}

/// A plain `Pigeonhole::Error` (no code): a write handle used after it was committed,
/// discarded or rolled back (simp724's R4 on #697).
pub(crate) fn spent(ruby: &Ruby, message: &str) -> Error {
    let make = || -> Result<Error, Error> {
        let m: RModule = ruby.class_object().const_get("Pigeonhole")?;
        let class: ExceptionClass = m.const_get("Error")?;
        Ok(Error::from(class.new_instance((message,))?))
    };
    make().unwrap_or_else(|e| e)
}
