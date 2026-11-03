//! Ruby arguments to Rust: byte strings, names, cell values, durations and durability (each
//! a [`TryConvert`] type, so the native methods take them directly), plus the binding's own
//! argument errors and lock helpers.

use std::borrow::Cow;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use magnus::{Error, Float, Integer, RArray, RString, Ruby, Symbol, TryConvert, Value, prelude::*};

use crate::table::Durability;

/// A row key, qualifier or byte value: a String's bytes as they are (any encoding).
pub(crate) struct Bytes(pub(crate) Vec<u8>);

impl TryConvert for Bytes {
    fn try_convert(val: Value) -> Result<Self, Error> {
        let ruby = Ruby::get_with(val);
        match RString::from_value(val) {
            Some(s) => Ok(Self(s.to_bytes().to_vec())),
            None => Err(type_error(
                &ruby,
                format!("expected a String (bytes), not {}", class_name(val)),
            )),
        }
    }
}

/// An optional byte string.
pub(crate) fn opt_bytes(b: Option<Bytes>) -> Option<Vec<u8>> {
    b.map(|b| b.0)
}

/// A name (a family, a table, a kind): a Symbol or a String.
pub(crate) struct Name(pub(crate) String);

impl TryConvert for Name {
    fn try_convert(val: Value) -> Result<Self, Error> {
        let ruby = Ruby::get_with(val);
        if let Some(sym) = Symbol::from_value(val) {
            return Ok(Self(sym.name()?.into_owned()));
        }
        if let Some(s) = RString::from_value(val) {
            return Ok(Self(s.to_string()?));
        }
        Err(type_error(
            &ruby,
            format!("expected a Symbol or a String, not {}", class_name(val)),
        ))
    }
}

/// An Array of names (`families:`).
pub(crate) fn names(a: Option<RArray>) -> Result<Option<Vec<String>>, Error> {
    let Some(a) = a else { return Ok(None) };
    let mut out = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        let v: Value = a.entry(isize::try_from(i).unwrap_or(isize::MAX))?;
        out.push(Name::try_convert(v)?.0);
    }
    Ok(Some(out))
}

/// A cell value as written: the Ruby class picks the cell type.
#[derive(Debug, Clone)]
pub(crate) enum Val {
    Bytes(Vec<u8>),
    I64(i64),
    F64(f64),
}

/// A `String` is bytes, an `Integer` an `i64` (`RangeError` past it), a `Float` an `f64`.
/// `true`, `false`, `nil` and other classes are refused (`TypeError`): a stored 1 is rarely
/// what `true` meant.
pub(crate) struct CellValue(pub(crate) Val);

impl TryConvert for CellValue {
    fn try_convert(v: Value) -> Result<Self, Error> {
        let ruby = Ruby::get_with(v);
        if let Some(s) = RString::from_value(v) {
            return Ok(Self(Val::Bytes(s.to_bytes().to_vec())));
        }
        if let Some(n) = Integer::from_value(v) {
            return n.to_i64().map(|n| Self(Val::I64(n))).map_err(|_| {
                range_error(
                    &ruby,
                    "an Integer value must fit in an i64 (put_u64 writes a u64)",
                )
            });
        }
        if let Some(x) = Float::from_value(v) {
            return Ok(Self(Val::F64(x.to_f64())));
        }
        Err(type_error(
            &ruby,
            format!(
                "a cell value is a String, an Integer or a Float, not {}",
                class_name(v)
            ),
        ))
    }
}

/// A duration: Numeric seconds, non-negative and finite.
pub(crate) struct Secs(pub(crate) Duration);

impl TryConvert for Secs {
    fn try_convert(v: Value) -> Result<Self, Error> {
        let ruby = Ruby::get_with(v);
        if v.is_kind_of(ruby.class_true_class()) || v.is_kind_of(ruby.class_false_class()) {
            return Err(type_error(
                &ruby,
                "a duration is Numeric seconds, not a boolean",
            ));
        }
        let s = f64::try_convert(v)?;
        Duration::try_from_secs_f64(s).map(Self).map_err(|_| {
            arg_error(
                &ruby,
                "a duration must be a non-negative, finite number of seconds",
            )
        })
    }
}

impl TryConvert for Durability {
    /// `:none`, `:buffered`, `:group_sync` or `:sync`.
    fn try_convert(v: Value) -> Result<Self, Error> {
        let ruby = Ruby::get_with(v);
        let Some(sym) = Symbol::from_value(v) else {
            return Err(type_error(
                &ruby,
                format!(
                    "durability is a Symbol (:none, :buffered, :group_sync or :sync), not {}",
                    class_name(v)
                ),
            ));
        };
        Ok(match sym.name()?.as_ref() {
            "none" => Self::None_,
            "buffered" => Self::Buffered,
            "group_sync" => Self::GroupSync,
            "sync" => Self::Sync,
            other => {
                return Err(arg_error(
                    &ruby,
                    format!("unknown durability :{other} (:none, :buffered, :group_sync or :sync)"),
                ));
            }
        })
    }
}

/// A binary (`ASCII-8BIT`) String holding `b`.
pub(crate) fn binary(ruby: &Ruby, b: &[u8]) -> RString {
    ruby.str_from_slice(b)
}

/// A frozen binary String, for a value inside a frozen `Data` result: mutating it would
/// change the result's `==` and `hash`.
pub(crate) fn frozen(ruby: &Ruby, b: &[u8]) -> RString {
    let s = binary(ruby, b);
    s.freeze();
    s
}

/// `v`'s class name, for messages.
pub(crate) fn class_name(v: Value) -> String {
    v.class().inspect()
}

/// `ArgumentError` with `msg`.
pub(crate) fn arg_error(ruby: &Ruby, msg: impl Into<Cow<'static, str>>) -> Error {
    Error::new(ruby.exception_arg_error(), msg)
}

/// `TypeError` with `msg`.
pub(crate) fn type_error(ruby: &Ruby, msg: impl Into<Cow<'static, str>>) -> Error {
    Error::new(ruby.exception_type_error(), msg)
}

/// `RangeError` with `msg`.
pub(crate) fn range_error(ruby: &Ruby, msg: impl Into<Cow<'static, str>>) -> Error {
    Error::new(ruby.exception_range_error(), msg)
}

/// Locks `m`, ignoring poisoning: a panic is re-raised as a Ruby exception, and each guarded
/// value stays consistent across one (every critical section is a take, a push or one
/// engine call that fails as a whole).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}
