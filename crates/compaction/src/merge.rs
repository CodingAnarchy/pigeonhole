//! Merge operators: the trait, the built-in `i64` add and the registry.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use pigeonhole_format::value::ValueTag;

/// A merge operator failure (bad operand encoding, overflow policy).
///
/// ```
/// use pigeonhole_compaction::{I64Add, MergeOperator};
///
/// let mut acc = b"\x00not a counter".to_vec();
/// let err = I64Add.finish(None, &mut acc).unwrap_err();
/// assert_eq!(err.operator, "pigeonhole.i64_add");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeError {
    /// Operator name.
    pub operator: String,
    /// What went wrong.
    pub message: String,
}

impl MergeError {
    pub(crate) fn new(operator: &str, message: &str) -> Self {
        Self {
            operator: operator.to_owned(),
            message: message.to_owned(),
        }
    }

    /// The error for merge operands in a family that has no operator.
    pub(crate) fn no_operator() -> Self {
        Self::new("", "merge operands in a family without a merge operator")
    }
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.operator.is_empty() {
            write!(f, "merge failed: {}", self.message)
        } else {
            write!(
                f,
                "merge operator {:?} failed: {}",
                self.operator, self.message
            )
        }
    }
}

impl std::error::Error for MergeError {}

/// A user-defined merge, applied at read and compaction time to operands written blind.
///
/// Identified by [`MergeOperator::name`], which is stored in the family's options in the
/// file, so a binary that registers a different operator under that name is the only way to
/// misinterpret the data. Values and operands are stored values (tag byte included).
///
/// Operators are **associative folds** (decision D31) so the resolver streams operands in
/// the order the cursor yields them (newest first) without buffering copies: the accumulator
/// starts as a copy of the newest operand, each older operand is folded in with
/// [`MergeOperator::merge`], and [`MergeOperator::finish`] applies the result to the base
/// value (the newest put below the operands) if one was found. Compaction that finds no base
/// keeps the accumulator as a single combined operand.
///
/// `older` passed to `merge` may itself be a combination of several operands (compaction
/// stores combined operands), which associativity makes equivalent.
///
/// ```
/// use pigeonhole_compaction::{MergeError, MergeOperator};
///
/// /// Concatenates byte values, oldest first.
/// #[derive(Debug)]
/// struct Append;
///
/// impl MergeOperator for Append {
///     fn name(&self) -> &str {
///         "example.append"
///     }
///     fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
///         // Both are `Bytes` values: tag 0x00, then the payload.
///         let newer = acc.split_off(1);
///         acc.extend_from_slice(&older[1..]);
///         acc.extend_from_slice(&newer);
///         Ok(())
///     }
///     fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
///         if let Some(base) = base {
///             let newer = acc.split_off(1);
///             acc.extend_from_slice(&base[1..]);
///             acc.extend_from_slice(&newer);
///         }
///         Ok(())
///     }
/// }
///
/// let mut acc = b"\x00c".to_vec();
/// Append.merge(&mut acc, b"\x00b").unwrap();
/// Append.finish(Some(b"\x00a"), &mut acc).unwrap();
/// assert_eq!(acc, b"\x00abc");
/// ```
pub trait MergeOperator: Send + Sync + fmt::Debug {
    /// Stable name, stored in the file. Built-ins use the `pigeonhole.` prefix.
    fn name(&self) -> &str;

    /// Folds `older` into `acc`, where `acc` holds the combination of every newer operand:
    /// afterwards `acc` is the combination of `older` followed by them.
    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError>;

    /// Applies the combined operands in `acc` to `base` (`None`: no value below them),
    /// leaving the resulting stored value in `acc`.
    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError>;
}

/// The `i64` in a stored `ValueTag::I64` value, or `None` if `stored` is anything else.
pub(crate) fn stored_i64(stored: &[u8]) -> Option<i64> {
    match stored {
        [tag, rest @ ..] if *tag == ValueTag::I64 as u8 => {
            <[u8; 8]>::try_from(rest).ok().map(i64::from_le_bytes)
        }
        _ => None,
    }
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.clear();
    out.push(ValueTag::I64 as u8);
    out.extend_from_slice(&v.to_le_bytes());
}

/// Built-in `i64` add (the `incr` operator): operands and values are `ValueTag::I64`; a
/// missing base counts as 0; overflow wraps. Name: `pigeonhole.i64_add`.
///
/// A base or operand that is not a stored `i64` (tag `0x01` and 8 bytes) is a
/// [`MergeError`], never 0 (decision D41).
///
/// ```
/// use pigeonhole_compaction::{I64Add, MergeOperator};
///
/// let i64v = |v: i64| [&[1u8][..], &v.to_le_bytes()].concat();
/// let mut acc = i64v(5); // newest operand
/// I64Add.merge(&mut acc, &i64v(2)).unwrap();
/// I64Add.finish(Some(&i64v(100)), &mut acc).unwrap();
/// assert_eq!(acc, i64v(107));
///
/// let mut acc = i64v(1);
/// assert!(I64Add.finish(Some(b"\x00abc"), &mut acc).is_err()); // not an i64 base
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct I64Add;

impl I64Add {
    const NAME: &'static str = "pigeonhole.i64_add";

    fn operand(v: &[u8]) -> Result<i64, MergeError> {
        stored_i64(v).ok_or_else(|| MergeError::new(Self::NAME, "operand is not an i64"))
    }
}

impl MergeOperator for I64Add {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        let sum = Self::operand(acc)?.wrapping_add(Self::operand(older)?);
        put_i64(acc, sum);
        Ok(())
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        let operands = Self::operand(acc)?;
        let base = match base {
            None => 0,
            Some(b) => stored_i64(b)
                .ok_or_else(|| MergeError::new(Self::NAME, "base value is not an i64"))?,
        };
        put_i64(acc, operands.wrapping_add(base));
        Ok(())
    }
}

/// Operators available to this process, by name. Built-ins are always present.
///
/// ```
/// use std::sync::Arc;
/// use pigeonhole_compaction::{I64Add, MergeRegistry};
///
/// let mut registry = MergeRegistry::new();
/// assert!(registry.get("pigeonhole.i64_add").is_some());
/// assert!(registry.get("example.append").is_none());
/// registry.register(Arc::new(I64Add)); // replaces the built-in with an equal one
/// assert_eq!(registry.get("pigeonhole.i64_add").unwrap().name(), "pigeonhole.i64_add");
/// ```
#[derive(Debug, Clone)]
pub struct MergeRegistry {
    ops: HashMap<String, Arc<dyn MergeOperator>>,
}

impl Default for MergeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeRegistry {
    /// A registry with the built-ins.
    pub fn new() -> Self {
        let mut r = Self {
            ops: HashMap::new(),
        };
        r.register(Arc::new(I64Add));
        r
    }

    /// Registers `op` under its name, replacing any previous one.
    pub fn register(&mut self, op: Arc<dyn MergeOperator>) {
        self.ops.insert(op.name().to_owned(), op);
    }

    /// Looks up an operator.
    pub fn get(&self, name: &str) -> Option<Arc<dyn MergeOperator>> {
        self.ops.get(name).cloned()
    }
}
