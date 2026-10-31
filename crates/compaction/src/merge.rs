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
    /// afterwards `acc` is the combination of `older` followed by them. On error the contents
    /// of `acc` are unspecified; callers never use them (compaction merges into a copy and
    /// keeps the operands apart, the resolver fails the read).
    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError>;

    /// Applies the combined operands in `acc` to `base` (`None`: no value below them),
    /// leaving the resulting stored value in `acc`. On error the contents of `acc` are
    /// unspecified.
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

/// Built-in `i64` minimum, the operator of a `FamilyKind::Min` family (D294): operands and
/// values are `ValueTag::I64`, and the result is the smallest of the operands and the base.
/// A missing base leaves the operands' minimum: the identity is no value, never 0. Name:
/// `pigeonhole.i64_min`.
///
/// A base or operand that is not a stored `i64` is a [`MergeError`], as for [`I64Add`].
///
/// ```
/// use pigeonhole_compaction::{I64Min, MergeOperator};
///
/// let i64v = |v: i64| [&[1u8][..], &v.to_le_bytes()].concat();
/// let mut acc = i64v(5); // newest operand
/// I64Min.merge(&mut acc, &i64v(-2)).unwrap();
/// I64Min.finish(None, &mut acc).unwrap();
/// assert_eq!(acc, i64v(-2));
/// I64Min.finish(Some(&i64v(-7)), &mut acc).unwrap();
/// assert_eq!(acc, i64v(-7));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct I64Min;

/// Built-in `i64` maximum, the operator of a `FamilyKind::Max` family (D294): as
/// [`I64Min`], keeping the largest. Name: `pigeonhole.i64_max`.
///
/// ```
/// use pigeonhole_compaction::{I64Max, MergeOperator};
///
/// let i64v = |v: i64| [&[1u8][..], &v.to_le_bytes()].concat();
/// let mut acc = i64v(5);
/// I64Max.merge(&mut acc, &i64v(9)).unwrap();
/// I64Max.finish(Some(&i64v(7)), &mut acc).unwrap();
/// assert_eq!(acc, i64v(9));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct I64Max;

/// [`I64Min`]'s and [`I64Max`]'s steps: `pick` chooses between two values.
fn select(
    name: &str,
    pick: fn(i64, i64) -> i64,
    acc: &mut Vec<u8>,
    other: Option<&[u8]>,
    what: &str,
) -> Result<(), MergeError> {
    let operands = stored_i64(acc).ok_or_else(|| MergeError::new(name, "operand is not an i64"))?;
    let Some(other) = other else {
        return Ok(());
    };
    let other = stored_i64(other).ok_or_else(|| MergeError::new(name, what))?;
    put_i64(acc, pick(operands, other));
    Ok(())
}

impl I64Min {
    const NAME: &'static str = "pigeonhole.i64_min";
}

impl MergeOperator for I64Min {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        select(
            Self::NAME,
            i64::min,
            acc,
            Some(older),
            "operand is not an i64",
        )
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        select(Self::NAME, i64::min, acc, base, "base value is not an i64")
    }
}

impl I64Max {
    const NAME: &'static str = "pigeonhole.i64_max";
}

impl MergeOperator for I64Max {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        select(
            Self::NAME,
            i64::max,
            acc,
            Some(older),
            "operand is not an i64",
        )
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        select(Self::NAME, i64::max, acc, base, "base value is not an i64")
    }
}

/// A bitwise operator over `u64` (D298), the operator of a `FamilyKind::BitOr`, `BitAnd` or
/// `BitXor` family: values and operands are `ValueTag::I64`, read as the `u64`'s bits. A
/// missing base leaves the operands' combination.
///
/// ```
/// use pigeonhole_compaction::{Bitwise, MergeOperator};
///
/// let v = |x: u64| [&[1u8][..], &x.to_le_bytes()].concat();
/// let mut acc = v(0b0101);
/// Bitwise::OR.merge(&mut acc, &v(0b0011)).unwrap();
/// assert_eq!(acc, v(0b0111));
/// let mut acc = v(0b0101);
/// Bitwise::XOR.finish(Some(&v(0b0110)), &mut acc).unwrap();
/// assert_eq!(acc, v(0b0011));
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Bitwise {
    name: &'static str,
    op: fn(u64, u64) -> u64,
}

impl Bitwise {
    /// `pigeonhole.u64_or`.
    pub const OR: Bitwise = Bitwise {
        name: "pigeonhole.u64_or",
        op: |a, b| a | b,
    };
    /// `pigeonhole.u64_and`.
    pub const AND: Bitwise = Bitwise {
        name: "pigeonhole.u64_and",
        op: |a, b| a & b,
    };
    /// `pigeonhole.u64_xor`.
    pub const XOR: Bitwise = Bitwise {
        name: "pigeonhole.u64_xor",
        op: |a, b| a ^ b,
    };

    fn combine(
        &self,
        acc: &mut Vec<u8>,
        other: Option<&[u8]>,
        what: &str,
    ) -> Result<(), MergeError> {
        let a =
            stored_i64(acc).ok_or_else(|| MergeError::new(self.name, "operand is not an i64"))?;
        let Some(other) = other else {
            return Ok(());
        };
        let b = stored_i64(other).ok_or_else(|| MergeError::new(self.name, what))?;
        put_i64(acc, (self.op)(a as u64, b as u64) as i64);
        Ok(())
    }
}

impl MergeOperator for Bitwise {
    fn name(&self) -> &str {
        self.name
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        self.combine(acc, Some(older), "operand is not an i64")
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        self.combine(acc, base, "base value is not an i64")
    }
}

/// The `f64` in a stored `ValueTag::F64` value that is not NaN, or `None`.
fn stored_f64(stored: &[u8]) -> Option<f64> {
    match stored {
        [tag, rest @ ..] if *tag == ValueTag::F64 as u8 => <[u8; 8]>::try_from(rest)
            .ok()
            .map(f64::from_le_bytes)
            .filter(|x| !x.is_nan()),
        _ => None,
    }
}

/// An `f64` minimum or maximum (D298), the operator of a `FamilyKind::MinF64` or `MaxF64`
/// family: values and operands are `ValueTag::F64`, never NaN, ordered by `f64::total_cmp`
/// (`-0.0 < +0.0`), so the result is exact and deterministic. A NaN or another tag is a
/// [`MergeError`].
///
/// ```
/// use pigeonhole_compaction::{F64Extreme, MergeOperator};
///
/// let v = |x: f64| [&[2u8][..], &x.to_le_bytes()].concat();
/// let mut acc = v(0.0);
/// F64Extreme::MIN.merge(&mut acc, &v(-0.0)).unwrap();
/// assert_eq!(acc, v(-0.0));
/// assert!(F64Extreme::MAX.merge(&mut acc, &v(f64::NAN)).is_err());
/// ```
#[derive(Debug, Clone, Copy)]
pub struct F64Extreme {
    name: &'static str,
    keep: std::cmp::Ordering,
}

impl F64Extreme {
    /// `pigeonhole.f64_min`.
    pub const MIN: F64Extreme = F64Extreme {
        name: "pigeonhole.f64_min",
        keep: std::cmp::Ordering::Less,
    };
    /// `pigeonhole.f64_max`.
    pub const MAX: F64Extreme = F64Extreme {
        name: "pigeonhole.f64_max",
        keep: std::cmp::Ordering::Greater,
    };

    fn combine(
        &self,
        acc: &mut Vec<u8>,
        other: Option<&[u8]>,
        what: &str,
    ) -> Result<(), MergeError> {
        let a =
            stored_f64(acc).ok_or_else(|| MergeError::new(self.name, "operand is not an f64"))?;
        let Some(other) = other else {
            return Ok(());
        };
        let b = stored_f64(other).ok_or_else(|| MergeError::new(self.name, what))?;
        if b.total_cmp(&a) == self.keep {
            acc.clear();
            acc.push(ValueTag::F64 as u8);
            acc.extend_from_slice(&b.to_le_bytes());
        }
        Ok(())
    }
}

impl MergeOperator for F64Extreme {
    fn name(&self) -> &str {
        self.name
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        self.combine(acc, Some(older), "operand is not an f64")
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        self.combine(acc, base, "base value is not an f64")
    }
}

/// Exact `f64` aggregation (D298), the operator of a `FamilyKind::SumF64` or `MeanVar`
/// family. Operands are `ValueTag::F64` inputs (finite) or `ValueTag::Bytes` states
/// (`pigeonhole_format::exact`); a merge or finish always leaves the canonical state, so a
/// bucket reads the same bytes however its inputs were grouped. A NaN or infinite input, a
/// malformed state or another tag is a [`MergeError`].
///
/// ```
/// use pigeonhole_compaction::{ExactF64, MergeOperator};
///
/// let f = |x: f64| [&[2u8][..], &x.to_le_bytes()].concat();
/// let mut acc = f(1e100);
/// ExactF64::SUM.merge(&mut acc, &f(1.0)).unwrap();
/// ExactF64::SUM.finish(Some(&f(-1e100)), &mut acc).unwrap();
/// assert_eq!(pigeonhole_format::exact::sum_read(&acc[1..]).unwrap(), 1.0);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ExactF64 {
    name: &'static str,
    mean_var: bool,
}

impl ExactF64 {
    /// `pigeonhole.f64_sum`.
    pub const SUM: ExactF64 = ExactF64 {
        name: "pigeonhole.f64_sum",
        mean_var: false,
    };
    /// `pigeonhole.f64_mean_var`.
    pub const MEAN_VAR: ExactF64 = ExactF64 {
        name: "pigeonhole.f64_mean_var",
        mean_var: true,
    };

    /// Folds the stored value `v` (an input or a state) into the raw state `state`.
    fn fold(&self, state: &mut Vec<u8>, v: &[u8]) -> Result<(), MergeError> {
        use pigeonhole_format::exact;
        let bad = |e: exact::StateError| MergeError::new(self.name, e.0);
        match v {
            [tag, rest @ ..] if *tag == ValueTag::F64 as u8 => {
                let x = <[u8; 8]>::try_from(rest)
                    .map(f64::from_le_bytes)
                    .map_err(|_| MergeError::new(self.name, "operand is not an f64"))?;
                if self.mean_var {
                    exact::mean_var_add(state, x).map_err(bad)
                } else {
                    exact::sum_add(state, x).map_err(bad)
                }
            }
            [tag, rest @ ..] if *tag == ValueTag::Bytes as u8 => {
                if self.mean_var {
                    exact::mean_var_validate(rest).map_err(bad)?;
                    exact::mean_var_merge(state, rest).map_err(bad)
                } else {
                    exact::sum_validate(rest).map_err(bad)?;
                    exact::sum_merge(state, rest).map_err(bad)
                }
            }
            _ => Err(MergeError::new(
                self.name,
                "operand is not an f64 or a state",
            )),
        }
    }

    /// `acc` as the state of `acc` and `other`, tagged `ValueTag::Bytes`. The state is built
    /// in a scratch buffer the thread keeps (perf287's N1 on #1220): a fold of many operands
    /// allocates nothing once the buffer has grown to the largest state.
    fn combine(&self, acc: &mut Vec<u8>, other: Option<&[u8]>) -> Result<(), MergeError> {
        thread_local! {
            static SCRATCH: std::cell::RefCell<Vec<u8>> =
                std::cell::RefCell::new(Vec::with_capacity(pigeonhole_format::exact::MEAN_VAR_MAX_LEN));
        }
        SCRATCH.with(|scratch| {
            let mut state = scratch.borrow_mut();
            state.clear();
            self.fold(&mut state, acc)?;
            if let Some(o) = other {
                self.fold(&mut state, o)?;
            }
            acc.clear();
            acc.push(ValueTag::Bytes as u8);
            acc.extend_from_slice(&state);
            Ok(())
        })
    }
}

impl MergeOperator for ExactF64 {
    fn name(&self) -> &str {
        self.name
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        self.combine(acc, Some(older))
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        self.combine(acc, base)
    }
}

/// A histogram's or quantile sketch's operator (D298), the operator of a
/// `FamilyKind::Histogram` or `Quantiles` family, built from the family's parameters
/// (`kind_params`): operands are `ValueTag::F64` inputs or `ValueTag::Bytes` states
/// (`pigeonhole_format::buckets`), and a merge or finish always leaves the canonical state.
/// A refused input (NaN; for quantiles any non-finite value), a malformed state or one of
/// other parameters, or another tag is a [`MergeError`].
///
/// ```
/// use pigeonhole_compaction::{Buckets, MergeOperator};
///
/// let f = |x: f64| [&[2u8][..], &x.to_le_bytes()].concat();
/// let op = Buckets::histogram(vec![0.0, 10.0]);
/// let mut acc = f(5.0);
/// op.merge(&mut acc, &f(-1.0)).unwrap();
/// op.finish(Some(&f(50.0)), &mut acc).unwrap();
/// let counts = pigeonhole_format::buckets::histogram_counts(&acc[1..], Some(3)).unwrap();
/// assert_eq!(counts, [1, 1, 1]);
/// ```
#[derive(Debug, Clone)]
pub struct Buckets {
    kind: BucketsKind,
}

#[derive(Debug, Clone)]
enum BucketsKind {
    Histogram(Arc<[f64]>),
    Quantiles(pigeonhole_format::buckets::QuantileParams),
}

impl Buckets {
    /// The name of the histogram operator.
    pub const HISTOGRAM: &'static str = "pigeonhole.f64_histogram";
    /// The name of the quantile sketch operator.
    pub const QUANTILES: &'static str = "pigeonhole.f64_quantiles";

    /// A histogram's operator over `bounds` (checked by the caller: finite, strictly
    /// increasing, 1 to 1023 of them).
    pub fn histogram(bounds: Vec<f64>) -> Self {
        Buckets {
            kind: BucketsKind::Histogram(bounds.into()),
        }
    }

    /// A quantile sketch's operator with `params` (checked by the caller).
    pub fn quantiles(params: pigeonhole_format::buckets::QuantileParams) -> Self {
        Buckets {
            kind: BucketsKind::Quantiles(params),
        }
    }

    fn label(&self) -> &'static str {
        match self.kind {
            BucketsKind::Histogram(_) => Self::HISTOGRAM,
            BucketsKind::Quantiles(_) => Self::QUANTILES,
        }
    }

    /// Folds the stored value `v` (an input or a state) into the raw state `state`.
    fn fold(&self, state: &mut Vec<u8>, v: &[u8]) -> Result<(), MergeError> {
        use pigeonhole_format::buckets;
        let bad = |e: buckets::BucketError| MergeError::new(self.label(), e.0);
        match v {
            [tag, rest @ ..] if *tag == ValueTag::F64 as u8 => {
                let x = <[u8; 8]>::try_from(rest)
                    .map(f64::from_le_bytes)
                    .map_err(|_| MergeError::new(self.label(), "operand is not an f64"))?;
                match &self.kind {
                    BucketsKind::Histogram(b) => buckets::histogram_add(state, b, x),
                    BucketsKind::Quantiles(p) => buckets::quantiles_add(state, *p, x),
                }
                .map_err(bad)
            }
            [tag, rest @ ..] if *tag == ValueTag::Bytes as u8 => match &self.kind {
                BucketsKind::Histogram(b) => buckets::histogram_merge(state, b, rest),
                BucketsKind::Quantiles(p) => buckets::quantiles_merge(state, *p, rest),
            }
            .map_err(bad),
            _ => Err(MergeError::new(
                self.label(),
                "operand is not an f64 or a state",
            )),
        }
    }

    /// `acc` as the state of `acc` and `other`, tagged `ValueTag::Bytes`, built in the
    /// thread's scratch buffer.
    fn combine(&self, acc: &mut Vec<u8>, other: Option<&[u8]>) -> Result<(), MergeError> {
        thread_local! {
            static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
        }
        SCRATCH.with(|scratch| {
            let mut state = scratch.borrow_mut();
            state.clear();
            self.fold(&mut state, acc)?;
            if let Some(o) = other {
                self.fold(&mut state, o)?;
            }
            acc.clear();
            acc.push(ValueTag::Bytes as u8);
            acc.extend_from_slice(&state);
            Ok(())
        })
    }
}

impl MergeOperator for Buckets {
    fn name(&self) -> &str {
        self.label()
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        self.combine(acc, Some(older))
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        self.combine(acc, base)
    }
}

/// Bloom filter union (D298), the operator of a `FamilyKind::Bloom` family, built from the
/// family's parameters (`kind_params`): operands and values are `ValueTag::Bytes` filter
/// states (`pigeonhole_format::bloom`; an element becomes its one-element state before the
/// WAL), and a merge is their bitwise OR in canonical form. It folds in place: a dense
/// accumulator takes each operand's bits without allocating, so a fold costs the size of its
/// operands, not `m` per operand. A state of other parameters, a malformed one or another tag
/// is a [`MergeError`].
///
/// ```
/// use pigeonhole_compaction::{BloomUnion, MergeOperator};
/// use pigeonhole_format::bloom::{self, BloomParams};
///
/// let p = BloomParams { log2_bits: 10, hashes: 3 };
/// let state = |v: i64| {
///     let mut s = vec![0u8]; // ValueTag::Bytes
///     bloom::append_element(&mut s, p, pigeonhole_format::hll::hash_i64(v));
///     s
/// };
/// let op = BloomUnion::new(p);
/// let mut acc = state(1);
/// op.merge(&mut acc, &state(2)).unwrap();
/// assert!(bloom::contains(&acc[1..], pigeonhole_format::hll::hash_i64(2)).unwrap());
/// ```
#[derive(Debug, Clone, Copy)]
pub struct BloomUnion {
    params: pigeonhole_format::bloom::BloomParams,
}

impl BloomUnion {
    /// The operator's name.
    pub const NAME: &'static str = "pigeonhole.bloom";

    /// The union for filters of `params` (checked by the caller).
    pub fn new(params: pigeonhole_format::bloom::BloomParams) -> Self {
        BloomUnion { params }
    }

    fn state<'a>(v: &'a [u8], what: &str) -> Result<&'a [u8], MergeError> {
        match v {
            [tag, rest @ ..] if *tag == ValueTag::Bytes as u8 => Ok(rest),
            _ => Err(MergeError::new(Self::NAME, what)),
        }
    }

    fn merge_into(&self, acc: &mut Vec<u8>, other: &[u8]) -> Result<(), MergeError> {
        Self::state(acc, "operand is not a bloom state")?;
        pigeonhole_format::bloom::merge_at(acc, 1, self.params, other)
            .map_err(|e| MergeError::new(Self::NAME, e.0))
    }
}

impl MergeOperator for BloomUnion {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        self.merge_into(acc, Self::state(older, "operand is not a bloom state")?)
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        if let Some(base) = base {
            self.merge_into(acc, Self::state(base, "base value is not a bloom state")?)?;
        }
        // The accumulator's own bits are checked here, once per fold.
        let state = Self::state(acc, "operand is not a bloom state")?;
        pigeonhole_format::bloom::validate(state, self.params)
            .map_err(|e| MergeError::new(Self::NAME, e.0))
    }
}

/// Built-in HyperLogLog union, the operator of a `FamilyKind::Hll` family (D294): operands
/// and values are `ValueTag::Bytes` sketches (`pigeonhole_format::hll`), and the result is
/// their register-wise maximum, in canonical form. It folds in place: no decode, no re-encode,
/// no allocation once the accumulator is dense. A missing base leaves the operands' union.
/// Name: `pigeonhole.hll`.
///
/// A base or operand that is not a well-formed sketch is a [`MergeError`].
///
/// ```
/// use pigeonhole_compaction::{HllUnion, MergeOperator};
/// use pigeonhole_format::hll;
///
/// let stored = |v: i64| {
///     let mut s = Vec::new();
///     hll::encode_element(&mut s, v);
///     [&[0u8][..], &s].concat() // the Bytes tag, then the sketch
/// };
/// let mut acc = stored(1);
/// HllUnion.merge(&mut acc, &stored(2)).unwrap();
/// HllUnion.finish(Some(&stored(3)), &mut acc).unwrap();
/// assert_eq!(hll::estimate(&acc[1..]).unwrap(), 3);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct HllUnion;

impl HllUnion {
    const NAME: &'static str = "pigeonhole.hll";

    fn sketch<'a>(v: &'a [u8], what: &str) -> Result<&'a [u8], MergeError> {
        match v {
            [tag, rest @ ..] if *tag == ValueTag::Bytes as u8 => Ok(rest),
            _ => Err(MergeError::new(Self::NAME, what)),
        }
    }

    fn merge_into(acc: &mut Vec<u8>, other: &[u8]) -> Result<(), MergeError> {
        Self::sketch(acc, "operand is not a sketch")?;
        pigeonhole_format::hll::merge_at(acc, 1, other)
            .map_err(|e| MergeError::new(Self::NAME, e.0))
    }
}

impl MergeOperator for HllUnion {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        Self::merge_into(acc, Self::sketch(older, "operand is not a sketch")?)
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        if let Some(base) = base {
            Self::merge_into(acc, Self::sketch(base, "base value is not a sketch")?)?;
        }
        // The accumulator's own registers are checked here, once per fold.
        let sketch = Self::sketch(acc, "operand is not a sketch")?;
        pigeonhole_format::hll::validate(sketch).map_err(|e| MergeError::new(Self::NAME, e.0))
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
        r.register(Arc::new(I64Min));
        r.register(Arc::new(I64Max));
        r.register(Arc::new(HllUnion));
        for op in [Bitwise::OR, Bitwise::AND, Bitwise::XOR] {
            r.register(Arc::new(op));
        }
        for op in [F64Extreme::MIN, F64Extreme::MAX] {
            r.register(Arc::new(op));
        }
        for op in [ExactF64::SUM, ExactF64::MEAN_VAR] {
            r.register(Arc::new(op));
        }
        // Their names only: a family of these kinds binds its own, from its parameters.
        r.register(Arc::new(Buckets::histogram(vec![0.0])));
        r.register(Arc::new(Buckets::quantiles(Default::default())));
        r.register(Arc::new(BloomUnion::new(
            pigeonhole_format::bloom::BloomParams {
                log2_bits: 6,
                hashes: 1,
            },
        )));
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
