//! Errors: one Python exception per [`ErrorCode`], all subclasses of `pigeonhole.Error`, each
//! with the stable code's name (`code`) and number (`code_num`).

use pigeonhole::ErrorCode;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

pyo3::create_exception!(pigeonhole, Error, PyException, "A Pigeonhole error.");

pyo3::create_exception!(
    pigeonhole,
    ReturnedValueError,
    Error,
    "A counter's value couldn't be read after its commit applied (D314): the commit \
     succeeded, so don't retry it. `seqno` is the commit's; `code`, `code_num` and \
     `__cause__` are the read's error."
);

/// Declares an exception per code, the code → class mapping, and the module registration.
macro_rules! codes {
    ($($code:ident),* $(,)?) => {
        $(pyo3::create_exception!(pigeonhole, $code, Error, concat!("`ErrorCode::", stringify!($code), "`."));)*

        /// The exception class for `code`, or `Error` for a code this build doesn't know.
        fn class(py: Python<'_>, code: ErrorCode) -> Bound<'_, pyo3::types::PyType> {
            match code {
                $(ErrorCode::$code => py.get_type::<$code>(),)*
                _ => py.get_type::<Error>(),
            }
        }

        /// Adds `Error` and every code's class to `m`.
        pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
            let py = m.py();
            m.add("Error", py.get_type::<Error>())?;
            m.add("ReturnedValueError", py.get_type::<ReturnedValueError>())?;
            $(m.add(stringify!($code), py.get_type::<$code>())?;)*
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

/// `code`'s exception with `message`, its `code` and `code_num` set.
pub(crate) fn with_code(code: ErrorCode, message: &str) -> PyErr {
    Python::attach(|py| {
        let err = PyErr::from_type(class(py, code), message.to_owned());
        let value = err.value(py);
        // Setting attributes on a fresh exception instance can't fail.
        let _ = value.setattr("code", format!("{code:?}"));
        let _ = value.setattr("code_num", code as u32);
        err
    })
}

/// A library error as its Python exception.
pub(crate) fn to_py(e: pigeonhole::Error) -> PyErr {
    with_code(e.code(), e.message())
}

/// A read that failed after commit `seqno` applied, as `ReturnedValueError` with the read's
/// error as its cause (perf2's S1 on #1473): a caller can tell it from a failed commit,
/// which a retry is safe after.
pub(crate) fn returned_value(seqno: u64, e: pigeonhole::Error) -> PyErr {
    Python::attach(|py| {
        let code = e.code();
        let err = ReturnedValueError::new_err(format!(
            "the commit (seqno {seqno}) succeeded, but reading the value after it failed: {}",
            e.message()
        ));
        let value = err.value(py);
        // Setting attributes on a fresh exception instance can't fail.
        let _ = value.setattr("seqno", seqno);
        let _ = value.setattr("code", format!("{code:?}"));
        let _ = value.setattr("code_num", code as u32);
        err.set_cause(py, Some(to_py(e)));
        err
    })
}

/// `Result` with library errors converted.
pub(crate) trait OrPy<T> {
    fn or_py(self) -> PyResult<T>;
}

impl<T> OrPy<T> for pigeonhole::Result<T> {
    fn or_py(self) -> PyResult<T> {
        self.map_err(to_py)
    }
}
