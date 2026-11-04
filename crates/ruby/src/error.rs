//! Errors: `Pigeonhole::Error < StandardError`, and one subclass per [`ErrorCode`] named as
//! the code, each instance carrying `code` (the name) and `code_num` (ICR 0111).

use magnus::{Error, ExceptionClass, RModule, Ruby, Value, prelude::*};
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

        /// Defines `Pigeonhole::Error`, every code's subclass, `SpentError` and
        /// `ReturnedValueError` under `m`,
        /// and `Pigeonhole::Native::ERROR_CODES` (each code's name and number) under `native`,
        /// which a test checks against `ErrorCode` itself (ai1249's P5 on #1399).
        pub(crate) fn register(ruby: &Ruby, m: RModule, native: RModule) -> Result<(), Error> {
            let base = m.define_error("Error", ruby.exception_standard_error())?;
            $(m.define_error(stringify!($code), base)?;)*
            m.define_error("SpentError", base)?;
            m.define_error("ReturnedValueError", base)?;
            let codes = ruby.hash_new();
            $(codes.aset(stringify!($code), ErrorCode::$code as u32)?;)*
            codes.freeze();
            native.const_set("ERROR_CODES", codes)?;
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

/// `Pigeonhole::SpentError` (an `Error` with no code): a write handle used after it was
/// committed, discarded or rolled back (simp724's R4 on #697; named, ai1249's P4 on #1399).
pub(crate) fn spent(ruby: &Ruby, message: &str) -> Error {
    uncoded(ruby, "SpentError", message)
}

/// A read that failed after commit `seqno` applied, as `Pigeonhole::ReturnedValueError`
/// (its `seqno`, and the read's error as its `code`, `code_num` and `cause`): a caller can
/// tell it from a failed commit, which a retry is safe after (perf2's S1 on #1473).
pub(crate) fn returned_value(ruby: &Ruby, seqno: u64, e: &pigeonhole::Error) -> Error {
    let cause = to_rb(ruby, e);
    let make = || -> Result<Error, Error> {
        let m: RModule = ruby.class_object().const_get("Pigeonhole")?;
        let class: ExceptionClass = m.const_get("ReturnedValueError")?;
        let code = e.code();
        let message = format!(
            "the commit (seqno {seqno}) succeeded, but reading the value after it failed: {}",
            e.message()
        );
        // `ReturnedValueError#initialize(message, code, code_num, seqno)` (lib/pigeonhole.rb).
        let exc = class.new_instance((message, format!("{code:?}"), code as u32, seqno))?;
        // Raised through `Kernel.raise` so the read's exception is its `cause`.
        let raised = match cause.value() {
            Some(c) => ruby
                .module_kernel()
                .funcall::<_, _, Value>("raise", (exc, magnus::kwargs!("cause" => c)))
                .err(),
            None => None,
        };
        Ok(raised.unwrap_or_else(|| Error::from(exc)))
    };
    make().unwrap_or_else(|e| e)
}

/// `Pigeonhole::<class>` with `message` and no code: the binding's own errors.
pub(crate) fn uncoded(ruby: &Ruby, class: &str, message: &str) -> Error {
    let make = || -> Result<Error, Error> {
        let m: RModule = ruby.class_object().const_get("Pigeonhole")?;
        let class: ExceptionClass = m.const_get(class)?;
        Ok(Error::from(class.new_instance((message,))?))
    };
    make().unwrap_or_else(|e| e)
}
