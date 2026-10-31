//! Shared JSON shapes: a family's stored options (in `info`, `dump` and `load`), and bytes
//! (row keys, qualifiers and values in `dump` and `load`).

use std::time::Duration;

use pigeonhole::{Aggregation, Compaction, Compression, Family, FamilyInfo, Priority};
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
            _ => "unknown",
        }),
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

/// The family [`family_json`] describes: its name and a [`Family`] with those options.
/// Missing fields take `Family`'s defaults.
pub(crate) fn family_from_json(v: &Value) -> Result<(String, Family), String> {
    let name = v["name"]
        .as_str()
        .ok_or("a family without a name")?
        .to_owned();
    // `aggregation` (D294) names the kind; files dumped before it say `counter` only.
    let mut f = match (v["aggregation"].as_str(), v["counter"].as_bool()) {
        (Some("sum"), _) | (None, Some(true)) => Family::counter(),
        (Some("min"), _) => Family::aggregate(Aggregation::Min),
        (Some("max"), _) => Family::aggregate(Aggregation::Max),
        (Some("hll"), _) => Family::aggregate(Aggregation::Hll),
        (Some("bit_or"), _) => Family::aggregate(Aggregation::BitOr),
        (Some("bit_and"), _) => Family::aggregate(Aggregation::BitAnd),
        (Some("bit_xor"), _) => Family::aggregate(Aggregation::BitXor),
        (Some("min_f64"), _) => Family::aggregate(Aggregation::MinF64),
        (Some("max_f64"), _) => Family::aggregate(Aggregation::MaxF64),
        (Some(other), _) => return Err(format!("family {name}: unknown aggregation {other:?}")),
        (None, _) => Family::default(),
    };
    let num = |key: &str| v[key].as_u64();
    let bad = |key: &str| format!("family {name}: bad {key}");
    if let Some(n) = num("max_versions") {
        f = f.max_versions(u32::try_from(n).map_err(|_| bad("max_versions"))?);
    }
    if let Some(n) = num("ttl_micros") {
        f = f.ttl(Duration::from_micros(n));
    }
    if let Some(n) = num("bloom_bits") {
        f = f.bloom_bits(u8::try_from(n).map_err(|_| bad("bloom_bits"))?);
    }
    if let Some(n) = num("blob_threshold") {
        f = f.blob_threshold(u32::try_from(n).map_err(|_| bad("blob_threshold"))?);
    }
    f = match v["compression"].as_str() {
        None => f,
        Some("lz4") => f.compression(Compression::Lz4),
        Some("none") => f.compression(Compression::None),
        Some("zstd") => {
            let level = v["zstd_level"].as_i64().unwrap_or(3);
            f.compression(Compression::Zstd(
                i8::try_from(level).map_err(|_| bad("zstd_level"))?,
            ))
        }
        Some(_) => return Err(bad("compression")),
    };
    if let Some(n) = num("block_size") {
        f = f.block_size(u32::try_from(n).map_err(|_| bad("block_size"))?);
    }
    if let Some(op) = v["merge_operator"].as_str() {
        f = f.merge_operator(op);
    }
    f = match v["cache_priority"].as_str() {
        None => f,
        Some("low") => f.cache_priority(Priority::Low),
        Some("normal") => f.cache_priority(Priority::Normal),
        Some("high") => f.cache_priority(Priority::High),
        Some(_) => return Err(bad("cache_priority")),
    };
    f = match v["compaction"].as_str() {
        None => f,
        Some("leveled") => f.compaction(Compaction::Leveled),
        Some("tiered") => f.compaction(Compaction::Tiered),
        Some("fifo-by-time") => f.compaction(Compaction::FifoByTime),
        Some(_) => return Err(bad("compaction")),
    };
    Ok((name, f))
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

fn base64_decode(s: &str) -> Option<Vec<u8>> {
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
}
