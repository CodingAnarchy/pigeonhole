//! Continuation cursors: opaque strings an agent passes back unchanged to continue a result
//! cut at the size limit.
//!
//! A cursor is base64 of a kind byte, a fingerprint of the call's other arguments, and the
//! position. The fingerprint makes a cursor fail loudly when it comes back with different
//! arguments, instead of resuming some other read at a meaningless place. It is a hash of
//! the arguments with object keys sorted, so it is the same for the same call in this build;
//! cursors are not meant to outlive the server.

use std::hash::{DefaultHasher, Hash, Hasher};

use serde_json::{Map, Value};

use crate::cmd::schema::{base64_decode, base64_encode};

/// Where a cut result continues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cursor {
    /// `read_row`: after this column (a `RowPosition`'s bytes).
    Row(Vec<u8>),
    /// `scan`: at this row key, or after it.
    Scan { key: Vec<u8>, inclusive: bool },
    /// `traverse`: this hop's node at this index.
    Traverse { hop: u32, index: u64 },
}

const ROW: u8 = b'r';
const SCAN: u8 = b's';
const TRAVERSE: u8 = b't';

impl Cursor {
    /// The cursor for a call with `args` (its `cursor` argument is ignored).
    pub(crate) fn encode(&self, args: &Map<String, Value>) -> String {
        let mut b = Vec::new();
        match self {
            Cursor::Row(pos) => {
                b.push(ROW);
                b.extend_from_slice(&fingerprint(args).to_le_bytes());
                b.extend_from_slice(pos);
            }
            Cursor::Scan { key, inclusive } => {
                b.push(SCAN);
                b.extend_from_slice(&fingerprint(args).to_le_bytes());
                b.push(u8::from(*inclusive));
                b.extend_from_slice(key);
            }
            Cursor::Traverse { hop, index } => {
                b.push(TRAVERSE);
                b.extend_from_slice(&fingerprint(args).to_le_bytes());
                b.extend_from_slice(&hop.to_le_bytes());
                b.extend_from_slice(&index.to_le_bytes());
            }
        }
        base64_encode(&b)
    }

    /// The `cursor` argument of a call with `args`, if it has one.
    pub(crate) fn decode(args: &Map<String, Value>) -> Result<Option<Cursor>, String> {
        let Some(v) = args.get("cursor") else {
            return Ok(None);
        };
        let bad = || "cursor: not a cursor this server returned".to_owned();
        let s = v.as_str().ok_or_else(bad)?;
        let b = base64_decode(s).ok_or_else(bad)?;
        if b.len() < 9 {
            return Err(bad());
        }
        let (kind, rest) = (b[0], &b[9..]);
        let hash = u64::from_le_bytes(b[1..9].try_into().expect("8 bytes"));
        if hash != fingerprint(args) {
            return Err(
                "cursor: it belongs to a call with other arguments; repeat the call \
                        that returned it with only the cursor added"
                    .into(),
            );
        }
        Ok(Some(match kind {
            ROW => Cursor::Row(rest.to_vec()),
            SCAN if !rest.is_empty() && rest[0] <= 1 => Cursor::Scan {
                key: rest[1..].to_vec(),
                inclusive: rest[0] == 1,
            },
            TRAVERSE if rest.len() == 12 => Cursor::Traverse {
                hop: u32::from_le_bytes(rest[..4].try_into().expect("4 bytes")),
                index: u64::from_le_bytes(rest[4..].try_into().expect("8 bytes")),
            },
            _ => return Err(bad()),
        }))
    }
}

/// A hash of `args` without `cursor`, with object keys sorted.
fn fingerprint(args: &Map<String, Value>) -> u64 {
    fn feed(v: &Value, h: &mut DefaultHasher) {
        match v {
            Value::Object(m) => {
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                h.write_u8(b'{');
                for k in keys {
                    k.hash(h);
                    feed(&m[k], h);
                }
                h.write_u8(b'}');
            }
            Value::Array(a) => {
                h.write_u8(b'[');
                a.iter().for_each(|x| feed(x, h));
                h.write_u8(b']');
            }
            other => other.to_string().hash(h),
        }
    }
    let mut h = DefaultHasher::new();
    let mut keys: Vec<&String> = args.keys().filter(|k| *k != "cursor").collect();
    keys.sort();
    for k in keys {
        k.hash(&mut h);
        feed(&args[k], &mut h);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn args(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => unreachable!(),
        }
    }

    #[test]
    fn cursors_round_trip_with_their_call() {
        let call = args(json!({"table": "t", "limit": 5, "families": ["a", "b"]}));
        for c in [
            Cursor::Row(vec![1, 2, 3]),
            Cursor::Scan {
                key: b"k\xff".to_vec(),
                inclusive: true,
            },
            Cursor::Scan {
                key: Vec::new(),
                inclusive: false,
            },
            Cursor::Traverse { hop: 2, index: 9 },
        ] {
            let mut again = args(json!({"families": ["a", "b"], "limit": 5, "table": "t"}));
            again.insert("cursor".into(), c.encode(&call).into());
            assert_eq!(Cursor::decode(&again).unwrap(), Some(c));
        }
    }

    #[test]
    fn a_cursor_with_other_arguments_is_refused() {
        let call = args(json!({"table": "t", "limit": 5}));
        let cursor = Cursor::Traverse { hop: 0, index: 1 }.encode(&call);
        let other = args(json!({"table": "t", "limit": 6, "cursor": cursor}));
        assert!(
            Cursor::decode(&other)
                .unwrap_err()
                .contains("other arguments")
        );
        let junk = args(json!({"table": "t", "limit": 5, "cursor": "nope"}));
        assert!(Cursor::decode(&junk).is_err());
        assert_eq!(Cursor::decode(&call).unwrap(), None);
    }
}
