//! Vectors (D303): vector families' specs, vectors written from Ruby Arrays (or a packed
//! String of bits), and vector cells read back as Arrays or a binary String.

use magnus::{Error, Float, Integer, RArray, RString, Ruby, Symbol, TryConvert, Value, prelude::*};
use pigeonhole::{Element, ErrorCode, Metric};

use crate::classes::VectorSpec;
use crate::convert::{arg_error, binary, class_name, type_error};
use crate::error::{OrRb, with_code};
use crate::gvl::without_gvl;

/// A Symbol's (or a String's) name, for the element and metric arguments.
fn sym_name(ruby: &Ruby, v: Value, what: &str) -> Result<String, Error> {
    if let Some(s) = Symbol::from_value(v) {
        return Ok(s.name()?.into_owned());
    }
    if let Some(s) = RString::from_value(v) {
        return s.to_string();
    }
    Err(type_error(
        ruby,
        format!("{what} is a Symbol, not {}", class_name(v)),
    ))
}

/// `:f32`, `:i8`, `:bit`, `:f16` or `:bf16`.
pub(crate) fn element(ruby: &Ruby, v: Value) -> Result<Element, Error> {
    Ok(match sym_name(ruby, v, "a vector element")?.as_str() {
        "f32" => Element::F32,
        "i8" => Element::I8,
        "bit" => Element::Bit,
        "f16" => Element::F16,
        "bf16" => Element::BF16,
        other => {
            return Err(arg_error(
                ruby,
                format!("unknown vector element :{other} (:f32, :i8, :bit, :f16 or :bf16)"),
            ));
        }
    })
}

/// `:l2`, `:cosine`, `:inner_product` or `:hamming`.
pub(crate) fn metric(ruby: &Ruby, v: Value) -> Result<Metric, Error> {
    Ok(match sym_name(ruby, v, "a metric")?.as_str() {
        "l2" => Metric::L2,
        "cosine" => Metric::Cosine,
        "inner_product" => Metric::InnerProduct,
        "hamming" => Metric::Hamming,
        other => {
            return Err(arg_error(
                ruby,
                format!("unknown metric :{other} (:l2, :cosine, :inner_product or :hamming)"),
            ));
        }
    })
}

fn element_name(e: Element) -> &'static str {
    match e {
        Element::F32 => "f32",
        Element::I8 => "i8",
        Element::Bit => "bit",
        Element::F16 => "f16",
        Element::BF16 => "bf16",
        _ => "unknown",
    }
}

fn metric_name(m: Metric) -> &'static str {
    match m {
        Metric::L2 => "l2",
        Metric::Cosine => "cosine",
        Metric::InnerProduct => "inner_product",
        Metric::Hamming => "hamming",
        _ => "unknown",
    }
}

/// A spec from Ruby arguments: `dim` elements of `element`, compared by `metric`.
pub(crate) fn spec(
    ruby: &Ruby,
    dim: u32,
    element: Value,
    metric: Value,
) -> Result<pigeonhole::VectorSpec, Error> {
    pigeonhole::VectorSpec::new(
        dim,
        self::element(ruby, element)?,
        self::metric(ruby, metric)?,
    )
    .or_rb(ruby)
}

impl VectorSpec {
    /// `VectorSpec.new(dim, element, metric)`.
    pub(crate) fn new(ruby: &Ruby, dim: u32, element: Value, metric: Value) -> Result<Self, Error> {
        spec(ruby, dim, element, metric).map(Self)
    }

    /// The dimension.
    pub(crate) fn dim(&self) -> u32 {
        self.0.dim()
    }

    /// The element type: `:f32`, `:i8`, `:bit`, `:f16` or `:bf16`.
    pub(crate) fn element(ruby: &Ruby, rb_self: &Self) -> Symbol {
        ruby.to_symbol(element_name(rb_self.0.element()))
    }

    /// The metric: `:l2`, `:cosine`, `:inner_product` or `:hamming`.
    pub(crate) fn metric(ruby: &Ruby, rb_self: &Self) -> Symbol {
        ruby.to_symbol(metric_name(rb_self.0.metric()))
    }

    /// Equal dimension, element and metric.
    pub(crate) fn eq(&self, other: Value) -> bool {
        <&VectorSpec>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    /// A hash consistent with `==`.
    pub(crate) fn hash(&self) -> i64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        (
            self.0.dim(),
            element_name(self.0.element()),
            metric_name(self.0.metric()),
        )
            .hash(&mut h);
        h.finish().cast_signed()
    }

    /// `#<Pigeonhole::VectorSpec dim=... element=:f32 metric=:l2>`.
    pub(crate) fn inspect(&self) -> String {
        format!(
            "#<Pigeonhole::VectorSpec dim={} element=:{} metric=:{}>",
            self.0.dim(),
            element_name(self.0.element()),
            metric_name(self.0.metric())
        )
    }
}

/// A vector as written: the family's element type decides how a Ruby value is read.
#[derive(Debug, Clone)]
pub(crate) enum Vector {
    F32(Vec<f32>),
    I8(Vec<i8>),
    Bits(Vec<u8>),
}

/// `v` as a vector of `element`:
/// - `f32`, `f16`, `bf16` (and an element this build doesn't name): an Array of Numerics,
///   each narrowed to `f32` (a half family rounds each to its element at the write, ties to
///   even, and refuses an overflow);
/// - `i8`: an Array of Integers in -128..=127;
/// - `bit`: the packed bits, a String.
///
/// A String is refused for `f32` and `i8`, and an Array for bits: the two forms don't
/// convert into each other silently. The family checks the dimension at commit.
pub(crate) fn vector(ruby: &Ruby, v: Value, element: Element) -> Result<Vector, Error> {
    if element == Element::Bit {
        return match RString::from_value(v) {
            Some(s) => Ok(Vector::Bits(s.to_bytes().to_vec())),
            None => Err(type_error(
                ruby,
                format!(
                    "a bit vector is its packed bits, a String, not {}",
                    class_name(v)
                ),
            )),
        };
    }
    let Some(a) = RArray::from_value(v) else {
        let want = if element == Element::I8 {
            "an i8 vector is an Array of Integers"
        } else {
            "an f32 vector is an Array of Numerics"
        };
        return Err(type_error(ruby, format!("{want}, not {}", class_name(v))));
    };
    if element == Element::I8 {
        let mut out = Vec::with_capacity(a.len());
        for x in a.into_iter() {
            let Some(n) = Integer::from_value(x) else {
                return Err(type_error(
                    ruby,
                    format!(
                        "an i8 vector's elements are Integers, not {}",
                        class_name(x)
                    ),
                ));
            };
            out.push(i8::try_from(n.to_i64()?).map_err(|_| {
                crate::convert::range_error(ruby, "an i8 vector's elements are in -128..=127")
            })?);
        }
        return Ok(Vector::I8(out));
    }
    let mut out = Vec::with_capacity(a.len());
    for x in a.into_iter() {
        if let Some(f) = Float::from_value(x) {
            out.push(f.to_f64() as f32);
        } else if x.is_kind_of(ruby.class_numeric()) {
            // An Integer, a Rational, or any other real Numeric, as `Float(x)` converts it.
            out.push(f64::try_convert(x)? as f32);
        } else {
            return Err(type_error(
                ruby,
                format!(
                    "an f32 vector's elements are Numerics, not {}",
                    class_name(x)
                ),
            ));
        }
    }
    Ok(Vector::F32(out))
}

/// The spec of `table`'s vector family `family` (`FamilyNotFound` for no such family, a
/// `TypeError` naming `what` for a family that isn't a vector family).
pub(crate) fn family_spec(
    ruby: &Ruby,
    table: &crate::table::Handle,
    family: &str,
    what: &str,
) -> Result<pigeonhole::VectorSpec, Error> {
    let (h, f) = (table.clone(), family.to_owned());
    let info = without_gvl(ruby, move || {
        crate::table::either!(&h, |t| t.family_info(&f))
    })?;
    let name = crate::table::either!(table, |t| t.name().to_owned());
    let info = info.ok_or_else(|| {
        with_code(
            ruby,
            ErrorCode::FamilyNotFound,
            &format!("table {name} has no family {family}"),
        )
    })?;
    info.vector.ok_or_else(|| {
        type_error(
            ruby,
            format!("{what} a vector family, and {family} is not one"),
        )
    })
}

/// A vector cell as Ruby: an Array of Floats (`f32`, and `f16` or `bf16` widened exactly),
/// an Array of Integers (`i8`), or the packed bits as a binary String.
pub(crate) fn to_rb(ruby: &Ruby, v: pigeonhole::VectorRef<'_>) -> Value {
    match v.element() {
        Element::Bit => binary(ruby, v.as_bytes()).as_value(),
        Element::I8 => ruby
            .ary_from_iter(v.as_bytes().iter().map(|b| i64::from(*b as i8)))
            .as_value(),
        _ => match v.to_f32_vec() {
            Some(x) => ruby.ary_from_iter(x.into_iter().map(f64::from)).as_value(),
            None => binary(ruby, v.as_bytes()).as_value(),
        },
    }
}
