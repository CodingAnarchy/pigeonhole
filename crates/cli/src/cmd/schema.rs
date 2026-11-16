//! Shared JSON shapes: a family's stored options (in `info`, `dump` and `load`), and bytes
//! (row keys, qualifiers and values in `dump` and `load`).

use std::time::Duration;

use pigeonhole::{
    Aggregation, BloomParams, Compaction, Compression, Element, Family, FamilyInfo,
    HistogramParams, Metric, Priority, QuantileParams, VectorRef, VectorSpec,
};
use serde_json::{Map, Value, json};

/// A family's stored options as JSON fields (`name` first).
pub(crate) fn family_json(f: &FamilyInfo) -> Map<String, Value> {
    let (compression, zstd_level) = match f.compression {
        Compression::None => ("none", None),
        Compression::Zstd(level) => ("zstd", Some(level)),
        Compression::Lz4 => ("lz4", None),
        _ => ("unknown", None),
    };
    let v = json!({
        "name": f.name,
        "counter": f.counter,
        "aggregation": f.aggregation.as_ref().map(|a| match a {
            Aggregation::Sum => "sum",
            Aggregation::Min => "min",
            Aggregation::Max => "max",
            Aggregation::Hll => "hll",
            Aggregation::BitOr => "bit_or",
            Aggregation::BitAnd => "bit_and",
            Aggregation::BitXor => "bit_xor",
            Aggregation::MinF64 => "min_f64",
            Aggregation::MaxF64 => "max_f64",
            Aggregation::SumF64 => "sum_f64",
            Aggregation::MeanVar => "mean_var",
            Aggregation::Histogram(_) => "histogram",
            Aggregation::Quantiles(_) => "quantiles",
            Aggregation::Bloom(_) => "bloom",
            Aggregation::Set => "set",
            _ => "unknown",
        }),
        // A histogram's bounds, a quantile sketch's parameters (D298).
        "aggregation_params": match &f.aggregation {
            Some(Aggregation::Histogram(h)) => json!({ "bounds": h.bounds() }),
            Some(Aggregation::Bloom(b)) => json!({ "bits": b.bits(), "hashes": b.hashes() }),
            Some(Aggregation::Quantiles(q)) => json!({
                "mantissa_bits": q.get_mantissa_bits(),
                "max_buckets": q.get_max_buckets(),
            }),
            _ => Value::Null,
        },
        // A vector family's shape (D303): what its every value is, and how it's compared.
        "vector": f.vector.as_ref().map(vector_spec_json),
        "max_versions": f.max_versions,
        "ttl_micros": f.ttl.map(|t| u64::try_from(t.as_micros()).unwrap_or(u64::MAX)),
        "bloom_bits": f.bloom_bits,
        "blob_threshold": f.blob_threshold,
        "compression": compression,
        "zstd_level": zstd_level,
        "block_size": f.block_size,
        "merge_operator": f.merge_operator,
        "cache_priority": match f.cache_priority {
            Priority::Low => "low",
            Priority::Normal => "normal",
            Priority::High => "high",
            _ => "unknown",
        },
        "compaction": match f.compaction {
            Compaction::Leveled => "leveled",
            Compaction::Tiered => "tiered",
            Compaction::FifoByTime => "fifo-by-time",
            _ => "unknown",
        },
    });
    match v {
        Value::Object(m) => m,
        _ => unreachable!("json! of an object literal"),
    }
}

/// A vector family's spec as JSON: `{"dim", "element", "metric"}`.
pub(crate) fn vector_spec_json(spec: &VectorSpec) -> Value {
    json!({
        "dim": spec.dim(),
        "element": element_name(spec.element()),
        "metric": match spec.metric() {
            Metric::L2 => "l2",
            Metric::InnerProduct => "inner_product",
            Metric::Cosine => "cosine",
            Metric::Hamming => "hamming",
            _ => "unknown",
        },
    })
}

/// An element type's name in JSON (D303, and #1263's halves).
pub(crate) fn element_name(e: Element) -> &'static str {
    match e {
        Element::F32 => "f32",
        Element::I8 => "i8",
        Element::Bit => "bit",
        Element::F16 => "f16",
        Element::BF16 => "bf16",
        _ => "unknown",
    }
}

fn element_from_name(name: &str) -> Option<Element> {
    Some(match name {
        "f32" => Element::F32,
        "i8" => Element::I8,
        "bit" => Element::Bit,
        "f16" => Element::F16,
        "bf16" => Element::BF16,
        _ => return None,
    })
}

/// The spec [`vector_spec_json`] wrote.
pub(crate) fn vector_spec_from_json(v: &Value) -> Result<VectorSpec, String> {
    let dim = v["dim"]
        .as_u64()
        .and_then(|d| u32::try_from(d).ok())
        .ok_or("a vector spec needs its dim")?;
    let element = v["element"]
        .as_str()
        .and_then(element_from_name)
        .ok_or("a vector spec's element is f32, i8, bit, f16 or bf16")?;
    let metric = match v["metric"].as_str() {
        Some("l2") => Metric::L2,
        Some("inner_product") => Metric::InnerProduct,
        Some("cosine") => Metric::Cosine,
        Some("hamming") => Metric::Hamming,
        _ => return Err("a vector spec's metric is l2, inner_product, cosine or hamming".into()),
    };
    VectorSpec::new(dim, element, metric).map_err(|e| e.to_string())
}

/// The family [`family_json`] describes: its name and a [`Family`] with those options.
/// Missing fields take `Family`'s defaults.
pub(crate) fn family_from_json(v: &Value) -> Result<(String, Family), String> {
    let def = family_def_from_json(v)?;
    let family = def.family();
    Ok((def.name, family))
}

/// A family's kind, as a [`FamilyDef`] holds it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FamilyKind {
    /// An ordinary family (perhaps naming a merge operator).
    Plain,
    /// An aggregate family; `Aggregation::Sum` is a counter family.
    Aggregate(Aggregation),
    /// A vector family (D303).
    Vector(VectorSpec),
}

/// A family's options, independent of where they come from (#1915 PR 3): parsed from
/// [`family_json`]'s shape (`create_table`, `load`), read from a database's
/// [`FamilyInfo`], or from a server's family spec. An option left `None` takes
/// [`Family`]'s default.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FamilyDef {
    pub(crate) name: String,
    pub(crate) kind: FamilyKind,
    pub(crate) max_versions: Option<u32>,
    pub(crate) ttl: Option<Duration>,
    pub(crate) bloom_bits: Option<u8>,
    pub(crate) blob_threshold: Option<u32>,
    pub(crate) compression: Option<Compression>,
    pub(crate) block_size: Option<u32>,
    pub(crate) merge_operator: Option<String>,
    pub(crate) cache_priority: Option<Priority>,
    pub(crate) compaction: Option<Compaction>,
}

impl FamilyDef {
    /// A family with every option at its default.
    pub(crate) fn new(name: String, kind: FamilyKind) -> Self {
        Self {
            name,
            kind,
            max_versions: None,
            ttl: None,
            bloom_bits: None,
            blob_threshold: None,
            compression: None,
            block_size: None,
            merge_operator: None,
            cache_priority: None,
            compaction: None,
        }
    }

    /// A database's family, every option set.
    pub(crate) fn from_info(f: &FamilyInfo) -> Self {
        let kind = match (&f.aggregation, &f.vector) {
            (Some(a), _) => FamilyKind::Aggregate(a.clone()),
            (None, Some(v)) => FamilyKind::Vector(*v),
            (None, None) => FamilyKind::Plain,
        };
        Self {
            name: f.name.clone(),
            kind,
            max_versions: Some(f.max_versions),
            ttl: f.ttl,
            bloom_bits: Some(f.bloom_bits),
            blob_threshold: Some(f.blob_threshold),
            compression: Some(f.compression),
            block_size: Some(f.block_size),
            merge_operator: f.merge_operator.clone(),
            cache_priority: Some(f.cache_priority),
            compaction: Some(f.compaction),
        }
    }

    /// The aggregation of an aggregate family.
    pub(crate) fn aggregation(&self) -> Option<&Aggregation> {
        match &self.kind {
            FamilyKind::Aggregate(a) => Some(a),
            _ => None,
        }
    }

    /// The spec of a vector family.
    pub(crate) fn vector(&self) -> Option<&VectorSpec> {
        match &self.kind {
            FamilyKind::Vector(v) => Some(v),
            _ => None,
        }
    }

    /// The builder that defines this family.
    pub(crate) fn family(&self) -> Family {
        let mut f = match &self.kind {
            FamilyKind::Plain => Family::default(),
            FamilyKind::Aggregate(Aggregation::Sum) => Family::counter(),
            FamilyKind::Aggregate(a) => Family::aggregate(a.clone()),
            FamilyKind::Vector(v) => Family::vector(*v),
        };
        if let Some(n) = self.max_versions {
            f = f.max_versions(n);
        }
        if let Some(t) = self.ttl {
            f = f.ttl(t);
        }
        if let Some(n) = self.bloom_bits {
            f = f.bloom_bits(n);
        }
        if let Some(n) = self.blob_threshold {
            f = f.blob_threshold(n);
        }
        if let Some(c) = self.compression {
            f = f.compression(c);
        }
        if let Some(n) = self.block_size {
            f = f.block_size(n);
        }
        if let Some(op) = &self.merge_operator {
            f = f.merge_operator(op);
        }
        if let Some(p) = self.cache_priority {
            f = f.cache_priority(p);
        }
        if let Some(c) = self.compaction {
            f = f.compaction(c);
        }
        f
    }

    /// [`family_json`]'s shape, for a family whose options are all set (a database's or a
    /// server's): equal to `family_json` of the same family.
    pub(crate) fn json(&self) -> Map<String, Value> {
        let (compression, zstd_level) = match self.compression {
            None => (None, None),
            Some(Compression::None) => (Some("none"), None),
            Some(Compression::Zstd(level)) => (Some("zstd"), Some(level)),
            Some(Compression::Lz4) => (Some("lz4"), None),
            Some(_) => (Some("unknown"), None),
        };
        let v = json!({
            "name": self.name,
            "counter": matches!(self.kind, FamilyKind::Aggregate(Aggregation::Sum)),
            "aggregation": self.aggregation().map(aggregation_name),
            "aggregation_params": match self.aggregation() {
                Some(Aggregation::Histogram(h)) => json!({ "bounds": h.bounds() }),
                Some(Aggregation::Bloom(b)) => json!({ "bits": b.bits(), "hashes": b.hashes() }),
                Some(Aggregation::Quantiles(q)) => json!({
                    "mantissa_bits": q.get_mantissa_bits(),
                    "max_buckets": q.get_max_buckets(),
                }),
                _ => Value::Null,
            },
            "vector": self.vector().map(vector_spec_json),
            "max_versions": self.max_versions,
            "ttl_micros": self.ttl.map(|t| u64::try_from(t.as_micros()).unwrap_or(u64::MAX)),
            "bloom_bits": self.bloom_bits,
            "blob_threshold": self.blob_threshold,
            "compression": compression,
            "zstd_level": zstd_level,
            "block_size": self.block_size,
            "merge_operator": self.merge_operator,
            "cache_priority": self.cache_priority.map(|p| match p {
                Priority::Low => "low",
                Priority::Normal => "normal",
                Priority::High => "high",
                _ => "unknown",
            }),
            "compaction": self.compaction.map(|c| match c {
                Compaction::Leveled => "leveled",
                Compaction::Tiered => "tiered",
                Compaction::FifoByTime => "fifo-by-time",
                _ => "unknown",
            }),
        });
        match v {
            Value::Object(m) => m,
            _ => unreachable!("json! of an object literal"),
        }
    }
}

/// An aggregation's name in JSON.
fn aggregation_name(a: &Aggregation) -> &'static str {
    match a {
        Aggregation::Sum => "sum",
        Aggregation::Min => "min",
        Aggregation::Max => "max",
        Aggregation::Hll => "hll",
        Aggregation::BitOr => "bit_or",
        Aggregation::BitAnd => "bit_and",
        Aggregation::BitXor => "bit_xor",
        Aggregation::MinF64 => "min_f64",
        Aggregation::MaxF64 => "max_f64",
        Aggregation::SumF64 => "sum_f64",
        Aggregation::MeanVar => "mean_var",
        Aggregation::Histogram(_) => "histogram",
        Aggregation::Quantiles(_) => "quantiles",
        Aggregation::Bloom(_) => "bloom",
        Aggregation::Set => "set",
        _ => "unknown",
    }
}

/// The family [`family_json`] describes, as a [`FamilyDef`]: missing fields are `None`.
pub(crate) fn family_def_from_json(v: &Value) -> Result<FamilyDef, String> {
    let name = v["name"]
        .as_str()
        .ok_or("a family without a name")?
        .to_owned();
    // `aggregation` (D294) names the kind; files dumped before it say `counter` only.
    let kind = match (v["aggregation"].as_str(), v["counter"].as_bool()) {
        (Some("sum"), _) | (None, Some(true)) => FamilyKind::Aggregate(Aggregation::Sum),
        (Some("min"), _) => FamilyKind::Aggregate(Aggregation::Min),
        (Some("max"), _) => FamilyKind::Aggregate(Aggregation::Max),
        (Some("hll"), _) => FamilyKind::Aggregate(Aggregation::Hll),
        (Some("bit_or"), _) => FamilyKind::Aggregate(Aggregation::BitOr),
        (Some("bit_and"), _) => FamilyKind::Aggregate(Aggregation::BitAnd),
        (Some("bit_xor"), _) => FamilyKind::Aggregate(Aggregation::BitXor),
        (Some("min_f64"), _) => FamilyKind::Aggregate(Aggregation::MinF64),
        (Some("max_f64"), _) => FamilyKind::Aggregate(Aggregation::MaxF64),
        (Some("sum_f64"), _) => FamilyKind::Aggregate(Aggregation::SumF64),
        (Some("mean_var"), _) => FamilyKind::Aggregate(Aggregation::MeanVar),
        (Some("histogram"), _) => {
            let bounds: Vec<f64> = v["aggregation_params"]["bounds"]
                .as_array()
                .ok_or_else(|| format!("family {name}: a histogram needs its bounds"))?
                .iter()
                .map(|b| {
                    b.as_f64()
                        .ok_or_else(|| format!("family {name}: a bound is not a number"))
                })
                .collect::<Result<_, _>>()?;
            let params = HistogramParams::new(bounds).map_err(|e| format!("family {name}: {e}"))?;
            FamilyKind::Aggregate(Aggregation::Histogram(params))
        }
        (Some("quantiles"), _) => {
            let p = &v["aggregation_params"];
            let mut q = QuantileParams::default();
            if let Some(n) = p["mantissa_bits"].as_u64() {
                q = q.mantissa_bits(
                    u8::try_from(n).map_err(|_| format!("family {name}: bad mantissa_bits"))?,
                );
            }
            if let Some(n) = p["max_buckets"].as_u64() {
                q = q.max_buckets(
                    u16::try_from(n).map_err(|_| format!("family {name}: bad max_buckets"))?,
                );
            }
            FamilyKind::Aggregate(Aggregation::Quantiles(q))
        }
        (Some("bloom"), _) => {
            let p = &v["aggregation_params"];
            let bits = p["bits"].as_u64().and_then(|n| u32::try_from(n).ok());
            let hashes = p["hashes"].as_u64().and_then(|n| u8::try_from(n).ok());
            let (Some(bits), Some(hashes)) = (bits, hashes) else {
                return Err(format!(
                    "family {name}: a bloom filter needs its bits and hashes"
                ));
            };
            let params =
                BloomParams::new(bits, hashes).map_err(|e| format!("family {name}: {e}"))?;
            FamilyKind::Aggregate(Aggregation::Bloom(params))
        }
        (Some("set"), _) => FamilyKind::Aggregate(Aggregation::Set),
        (Some(other), _) => return Err(format!("family {name}: unknown aggregation {other:?}")),
        (None, _) => match v.get("vector").filter(|s| !s.is_null()) {
            Some(spec) => FamilyKind::Vector(
                vector_spec_from_json(spec).map_err(|m| format!("family {name}: {m}"))?,
            ),
            None => FamilyKind::Plain,
        },
    };
    if v.get("vector").is_some_and(|s| !s.is_null())
        && (v["aggregation"].is_string() || v["counter"].as_bool() == Some(true))
    {
        return Err(format!(
            "family {name}: a vector family can't also be an aggregate"
        ));
    }
    let mut f = FamilyDef::new(name, kind);
    let name = &f.name.clone();
    let num = |key: &str| v[key].as_u64();
    let bad = |key: &str| format!("family {name}: bad {key}");
    if let Some(n) = num("max_versions") {
        f.max_versions = Some(u32::try_from(n).map_err(|_| bad("max_versions"))?);
    }
    if let Some(n) = num("ttl_micros") {
        f.ttl = Some(Duration::from_micros(n));
    }
    if let Some(n) = num("bloom_bits") {
        f.bloom_bits = Some(u8::try_from(n).map_err(|_| bad("bloom_bits"))?);
    }
    if let Some(n) = num("blob_threshold") {
        f.blob_threshold = Some(u32::try_from(n).map_err(|_| bad("blob_threshold"))?);
    }
    f.compression = match v["compression"].as_str() {
        None => None,
        Some("lz4") => Some(Compression::Lz4),
        Some("none") => Some(Compression::None),
        Some("zstd") => {
            let level = v["zstd_level"].as_i64().unwrap_or(3);
            Some(Compression::Zstd(
                i8::try_from(level).map_err(|_| bad("zstd_level"))?,
            ))
        }
        Some(_) => return Err(bad("compression")),
    };
    if let Some(n) = num("block_size") {
        f.block_size = Some(u32::try_from(n).map_err(|_| bad("block_size"))?);
    }
    if let Some(op) = v["merge_operator"].as_str() {
        f.merge_operator = Some(op.to_owned());
    }
    f.cache_priority = match v["cache_priority"].as_str() {
        None => None,
        Some("low") => Some(Priority::Low),
        Some("normal") => Some(Priority::Normal),
        Some("high") => Some(Priority::High),
        Some(_) => return Err(bad("cache_priority")),
    };
    f.compaction = match v["compaction"].as_str() {
        None => None,
        Some("leveled") => Some(Compaction::Leveled),
        Some("tiered") => Some(Compaction::Tiered),
        Some("fifo-by-time") => Some(Compaction::FifoByTime),
        Some(_) => return Err(bad("compaction")),
    };
    Ok(f)
}

/// Bytes as JSON: a string when they are valid UTF-8 (JSON escapes control characters, so
/// this round-trips), else `{"base64": "…"}`.
pub(crate) fn bytes_json(b: &[u8]) -> Value {
    match std::str::from_utf8(b) {
        Ok(s) => json!(s),
        Err(_) => json!({ "base64": base64_encode(b) }),
    }
}

/// The bytes [`bytes_json`] wrote.
pub(crate) fn bytes_from_json(v: &Value) -> Result<Vec<u8>, String> {
    match v {
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        Value::Object(m) => match m.get("base64").and_then(Value::as_str) {
            Some(b) => base64_decode(b).ok_or_else(|| "bad base64".to_owned()),
            None => Err("expected a string or {\"base64\": …}".into()),
        },
        _ => Err("expected a string or {\"base64\": …}".into()),
    }
}

/// A cell value as `dump` writes it and `load` and `phdb mcp` read it.
#[derive(Debug, PartialEq)]
pub(crate) enum JsonValue {
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
    /// A vector (D303): its element and dim as written, and its components.
    Vector {
        element: Element,
        dim: u32,
        components: Components,
    },
}

/// A JSON vector's components, by element.
#[derive(Debug, PartialEq)]
pub(crate) enum Components {
    /// `f32`, `f16` and `bf16`: the values, exact (a half widens exactly, and writing it back
    /// with `put_vector_f32` rounds to the same bits).
    Floats(Vec<f32>),
    /// `i8`.
    Ints(Vec<i8>),
    /// Bit vectors' packed bytes.
    Bits(Vec<u8>),
}

/// A vector value as JSON: `{"vector": {"dim", "element", "values": [...]}}`, or for a bit
/// vector `{"vector": {"dim", "element": "bit", "base64": "..."}}` (the spec's key order). Floats are written as
/// the `f64`s they widen to exactly, so a dump and load round-trips every bit; `None` for a
/// vector of an element this build doesn't name.
pub(crate) fn vector_json(v: &VectorRef<'_>) -> Value {
    let element = v.element();
    let body = match element {
        Element::Bit => json!({
            "dim": v.dim(),
            "element": "bit",
            "base64": base64_encode(v.as_bytes()),
        }),
        Element::I8 => json!({
            "dim": v.dim(),
            "element": "i8",
            "values": v.as_bytes().iter().map(|b| *b as i8).collect::<Vec<_>>(),
        }),
        _ => match v.to_f32_vec() {
            Some(xs) => json!({
                "dim": v.dim(),
                "element": element_name(element),
                "values": xs.iter().map(|x| f64::from(*x)).collect::<Vec<_>>(),
            }),
            None => json!({ "dim": v.dim(), "element": "unknown" }),
        },
    };
    json!({ "vector": body })
}

/// The vector [`vector_json`] wrote (the object under `"vector"`).
fn vector_from_json(v: &Value) -> Result<JsonValue, String> {
    let element = v["element"]
        .as_str()
        .and_then(element_from_name)
        .ok_or("a vector's element is f32, i8, bit, f16 or bf16")?;
    let dim = v["dim"]
        .as_u64()
        .and_then(|d| u32::try_from(d).ok())
        .ok_or("a vector needs its dim")?;
    let components = if element == Element::Bit {
        let b = v["base64"]
            .as_str()
            .and_then(base64_decode)
            .ok_or("a bit vector's bits are base64")?;
        Components::Bits(b)
    } else {
        let values = v["values"].as_array().ok_or("a vector needs its values")?;
        if element == Element::I8 {
            Components::Ints(
                values
                    .iter()
                    .map(|x| {
                        x.as_i64()
                            .and_then(|n| i8::try_from(n).ok())
                            .ok_or("an i8 vector's values are integers from -128 to 127")
                    })
                    .collect::<Result<_, _>>()?,
            )
        } else {
            Components::Floats(
                values
                    .iter()
                    .map(|x| {
                        x.as_f64()
                            .map(|f| f as f32)
                            .filter(|f| f.is_finite())
                            .ok_or("a vector's values are finite numbers")
                    })
                    .collect::<Result<_, _>>()?,
            )
        }
    };
    Ok(JsonValue::Vector {
        element,
        dim,
        components,
    })
}

/// The value [`value_json`](super::dump::value_json) wrote: `{"i64": n}`, `{"f64": x}`,
/// `{"f64_bits": n}`, or bytes ([`bytes_json`]).
pub(crate) fn value_from_json(v: &Value) -> Result<JsonValue, String> {
    if let Some(x) = v.get("vector") {
        return vector_from_json(x);
    }
    if let Some(x) = v.get("i64") {
        return x
            .as_i64()
            .map(JsonValue::I64)
            .ok_or_else(|| "i64 value: not an integer".into());
    }
    if let Some(x) = v.get("f64") {
        return x
            .as_f64()
            .map(JsonValue::F64)
            .ok_or_else(|| "f64 value: not a number".into());
    }
    if let Some(x) = v.get("f64_bits") {
        return x
            .as_u64()
            .map(|b| JsonValue::F64(f64::from_bits(b)))
            .ok_or_else(|| "f64_bits value: not a u64".into());
    }
    bytes_from_json(v).map(JsonValue::Bytes)
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 (RFC 4648) with padding.
pub(crate) fn base64_encode(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for chunk in b.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &c)| n | u32::from(c) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(B64[(n >> (18 - 6 * i) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

pub(crate) fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.as_bytes();
    if !s.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for chunk in s.chunks(4) {
        let pad = chunk.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 {
            return None;
        }
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            let v = if i >= 4 - pad {
                0
            } else {
                B64.iter().position(|&x| x == c)? as u32
            };
            n |= v << (18 - 6 * i);
        }
        let bytes = [(n >> 16) as u8, (n >> 8) as u8, n as u8];
        out.extend_from_slice(&bytes[..3 - pad]);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips() {
        for len in 0..40usize {
            let b: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            assert_eq!(
                base64_decode(&base64_encode(&b)).as_deref(),
                Some(&b[..]),
                "{len}"
            );
        }
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
        assert_eq!(base64_decode("TQ=="), Some(b"M".to_vec()));
        assert_eq!(base64_decode("T@=="), None);
        assert_eq!(base64_decode("TQ="), None);
    }

    #[test]
    fn bytes_are_strings_when_utf8() {
        assert_eq!(bytes_json(b"user:42\n"), json!("user:42\n"));
        assert_eq!(bytes_json(&[0xff, 0]), json!({"base64": "/wA="}));
        for b in [&b"abc"[..], &[0xff, 0, 1][..], b""] {
            assert_eq!(bytes_from_json(&bytes_json(b)).unwrap(), b);
        }
    }

    #[test]
    fn typed_values_round_trip() {
        use super::super::dump::value_json;
        use pigeonhole::Value as Cell;
        let cases = [
            (Cell::I64(-7), JsonValue::I64(-7)),
            (Cell::F64(0.5), JsonValue::F64(0.5)),
            (Cell::Bytes(b"hi"), JsonValue::Bytes(b"hi".to_vec())),
            (Cell::Bytes(&[0xff]), JsonValue::Bytes(vec![0xff])),
        ];
        for (cell, want) in cases {
            assert_eq!(value_from_json(&value_json(cell)).unwrap(), want);
        }
        let inf = value_from_json(&value_json(Cell::F64(f64::INFINITY))).unwrap();
        assert_eq!(inf, JsonValue::F64(f64::INFINITY));
        assert!(value_from_json(&json!({"i64": "x"})).is_err());
        assert!(value_from_json(&json!(3)).is_err());
    }
}
