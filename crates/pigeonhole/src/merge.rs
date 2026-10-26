//! Merge operators: this crate's own trait and error (D282), adapted to the engine's.

use std::fmt;
use std::sync::Arc;

/// A merge operator failure (bad operand encoding, overflow policy), returned by a
/// [`MergeOperator`]. A read that hits it fails with
/// [`ErrorCode::MergeFailed`](crate::ErrorCode::MergeFailed).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MergeError {
    /// Operator name.
    pub operator: String,
    /// What went wrong.
    pub message: String,
}

impl MergeError {
    /// A failure of the operator named `operator`.
    pub fn new(operator: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            operator: operator.into(),
            message: message.into(),
        }
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

/// A user-defined merge, applied at read and compaction time to operands written blind
/// ([`RowMutation::merge`](crate::RowMutation::merge)). Register it with
/// [`Options::merge_operator`](crate::Options::merge_operator) and name it in a family
/// ([`Family::merge_operator`](crate::Family::merge_operator)).
///
/// Identified by [`MergeOperator::name`], which is stored in the family's options in the
/// file, so a binary that registers a different operator under that name is the only way to
/// misinterpret the data. Values and operands are stored values (tag byte included).
///
/// Operators are **associative folds**: the accumulator starts as a copy of the newest
/// operand, each older operand is folded in with [`MergeOperator::merge`], and
/// [`MergeOperator::finish`] applies the result to the base value (the newest put below the
/// operands) if one was found. Compaction that finds no base keeps the accumulator as a
/// single combined operand, so `older` passed to `merge` may itself combine several operands,
/// which associativity makes equivalent.
///
/// ```
/// use pigeonhole::{MergeError, MergeOperator};
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
    /// afterwards `acc` is the combination of `older` followed by them. On error the contents
    /// of `acc` are unspecified; callers never use them.
    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError>;

    /// Applies the combined operands in `acc` to `base` (`None`: no value below them),
    /// leaving the resulting stored value in `acc`. On error the contents of `acc` are
    /// unspecified.
    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError>;
}

/// A [`MergeOperator`] as the engine's trait: one more call through a pointer per fold.
#[derive(Debug)]
pub(crate) struct EngineMergeOperator(pub(crate) Arc<dyn MergeOperator>);

fn engine_error(e: MergeError) -> pigeonhole_engine::MergeError {
    pigeonhole_engine::MergeError {
        operator: e.operator,
        message: e.message,
    }
}

impl pigeonhole_engine::MergeOperator for EngineMergeOperator {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), pigeonhole_engine::MergeError> {
        self.0.merge(acc, older).map_err(engine_error)
    }

    fn finish(
        &self,
        base: Option<&[u8]>,
        acc: &mut Vec<u8>,
    ) -> Result<(), pigeonhole_engine::MergeError> {
        self.0.finish(base, acc).map_err(engine_error)
    }
}
