//! A cluster subscription's token (D317 ticket 7, ICR 0117): where each partition's part of
//! the subscription is, as that partition's own D310 token, with what the subscription is.
//!
//! - **What it holds:** the cluster's id, the map version it was made under, a hash of the
//!   subscription (its tables, prefix and visibility), and one component per partition:
//!   the partition's id, its epoch in the map, and its feed token.
//! - **Sent on checkpoints only** ([`super::feed`]): a full token on every event of a
//!   subscription over many partitions would be several times the size of the events.
//! - **Compact components:** a D310 token is fixed-width (16 bytes a shard stream). Its
//!   numbers are mostly small or close together, so a component carries it as varints and
//!   deltas. The layout is the engine's, read here without its help, so every compaction is
//!   checked by expanding it again: a token that doesn't come back byte for byte is carried
//!   raw. An engine change to the layout can make tokens larger, never wrong.

use prost::bytes::Bytes;
use prost::encoding::{decode_varint, encode_varint};

/// The first byte of a cluster token: never a D310 token's version (1 or 2), so a single
/// node's token sent to a cluster, or a cluster's to a single node, is recognised as such.
const MAGIC: u8 = 0xC7;
/// The cluster token's format version.
const VERSION: u8 = 1;

/// A component carried as the D310 token's own bytes.
const RAW: u8 = 0;
/// A component carried compacted ([`compact`]).
const COMPACT: u8 = 1;
/// A component with no position yet, which starts at its own "now" (its partition was
/// unavailable since a subscription from now began, so nothing of it was delivered).
const NOW: u8 = 2;
/// A component with no position yet, which starts at its oldest commit.
const OLDEST: u8 = 3;

/// Where a component is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum At {
    /// Nothing delivered yet, from a start at the partition's own now.
    Now,
    /// Nothing delivered yet, from a start at the partition's oldest commit.
    Oldest,
    /// The partition's own feed token, as its node gave it.
    Token(Bytes),
}

/// One partition's part of a cluster subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Component {
    pub partition: String,
    /// The partition's epoch in the map when the token was made: a relocation raises it.
    pub epoch: u64,
    pub at: At,
}

/// A cluster subscription's position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterToken {
    pub cluster_id: String,
    pub map_version: u64,
    /// [`subscription_hash`] of what was subscribed.
    pub subscription: u64,
    /// Sorted by partition.
    pub components: Vec<Component>,
}

impl ClusterToken {
    /// The token's bytes, each component compacted where that's exact.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32 + self.components.len() * 64);
        out.push(MAGIC);
        out.push(VERSION);
        put_bytes(&mut out, self.cluster_id.as_bytes());
        encode_varint(self.map_version, &mut out);
        out.extend_from_slice(&self.subscription.to_le_bytes());
        encode_varint(self.components.len() as u64, &mut out);
        for c in &self.components {
            put_bytes(&mut out, c.partition.as_bytes());
            encode_varint(c.epoch, &mut out);
            match &c.at {
                At::Now => out.push(NOW),
                At::Oldest => out.push(OLDEST),
                At::Token(token) => match compact(token) {
                    Some(small) => {
                        out.push(COMPACT);
                        put_bytes(&mut out, &small);
                    }
                    None => {
                        out.push(RAW);
                        put_bytes(&mut out, token);
                    }
                },
            }
        }
        out
    }

    /// A token from `bytes`; why not, for bytes that aren't a cluster token.
    pub(crate) fn decode(mut bytes: &[u8]) -> Result<Self, String> {
        let buf = &mut bytes;
        match (take(buf, 1)?, take(buf, 1)?) {
            ([MAGIC], [VERSION]) => {}
            ([MAGIC], [v]) => return Err(format!("a cluster feed token of version {v}")),
            _ => return Err("not a cluster feed token".to_owned()),
        }
        let cluster_id = text(take_bytes(buf)?)?;
        let map_version = varint(buf)?;
        let subscription = u64::from_le_bytes(take(buf, 8)?.try_into().expect("8 bytes"));
        let n = varint(buf)?;
        let mut components = Vec::new();
        for _ in 0..n {
            let partition = text(take_bytes(buf)?)?;
            let epoch = varint(buf)?;
            let at = match take(buf, 1)?[0] {
                RAW => At::Token(Bytes::copy_from_slice(take_bytes(buf)?)),
                COMPACT => At::Token(Bytes::from(expand(take_bytes(buf)?)?)),
                NOW => At::Now,
                OLDEST => At::Oldest,
                f => return Err(format!("a component of unknown form {f}")),
            };
            components.push(Component {
                partition,
                epoch,
                at,
            });
        }
        if !buf.is_empty() {
            return Err("trailing bytes".to_owned());
        }
        Ok(Self {
            cluster_id,
            map_version,
            subscription,
            components,
        })
    }
}

/// What a subscription is, as a token records it: its tables (sorted, without repeats), its
/// prefix and its visibility. FNV-1a, so it's the same in every build and process.
pub(crate) fn subscription_hash(tables: &[String], prefix: &[u8], visible: bool) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |bytes: &[u8]| {
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for t in tables {
        eat(&(t.len() as u64).to_le_bytes());
        eat(t.as_bytes());
    }
    eat(&(prefix.len() as u64).to_le_bytes());
    eat(prefix);
    eat(&[u8::from(visible)]);
    h
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    encode_varint(b.len() as u64, out);
    out.extend_from_slice(b);
}

fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8], String> {
    if buf.len() < n {
        return Err("truncated".to_owned());
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}

fn take_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let n = usize::try_from(varint(buf)?).map_err(|_| "a length too large".to_owned())?;
    take(buf, n)
}

fn varint(buf: &mut &[u8]) -> Result<u64, String> {
    decode_varint(buf).map_err(|e| e.to_string())
}

fn text(b: &[u8]) -> Result<String, String> {
    String::from_utf8(b.to_vec()).map_err(|_| "a name that isn't UTF-8".to_owned())
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// The D310 token's fixed part: its version, the database's id (16 bytes), the stream
/// count (`u32`), the last seqno delivered and the writer session (`u64` each).
const HEAD: usize = 1 + 16 + 4 + 8 + 8;

/// A D310 token compacted (perf287's form on ticket 7), or `None` when it isn't a layout
/// this build knows or doesn't come back exactly ([`expand`]):
/// - version and database id as they are;
/// - the stream count and the last seqno delivered as varints; the writer session as its 8
///   bytes (it's random, so a varint would be longer);
/// - each stream's `(hint, delivered_end)`: the hint as a zigzag varint delta from the
///   previous stream's hint, and the end as one from its hint (the same segment, usually);
/// - a feed of every table's catalog positions (version 2) as varints.
pub(crate) fn compact(raw: &[u8]) -> Option<Vec<u8>> {
    let version = *raw.first()?;
    if raw.len() < HEAD || !matches!(version, 1 | 2) {
        return None;
    }
    let u64_at = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().expect("8 bytes"));
    let streams = u32::from_le_bytes(raw[17..21].try_into().expect("4 bytes")) as usize;
    let tail = if version == 2 { 16 } else { 0 };
    if raw.len() != HEAD + streams * 16 + tail {
        return None;
    }
    let mut out = Vec::with_capacity(HEAD + streams * 4 + tail);
    out.extend_from_slice(&raw[..17]);
    encode_varint(streams as u64, &mut out);
    encode_varint(u64_at(21), &mut out);
    out.extend_from_slice(&raw[29..37]);
    let mut prev = 0u64;
    for s in 0..streams {
        let (hint, end) = (u64_at(HEAD + s * 16), u64_at(HEAD + s * 16 + 8));
        encode_varint(zigzag(hint.wrapping_sub(prev) as i64), &mut out);
        encode_varint(zigzag(end.wrapping_sub(hint) as i64), &mut out);
        prev = hint;
    }
    if version == 2 {
        let at = HEAD + streams * 16;
        encode_varint(u64_at(at), &mut out);
        encode_varint(u64_at(at + 8), &mut out);
    }
    // The check: the engine's layout, read without its help, must come back byte for byte.
    (expand(&out).ok()? == raw).then_some(out)
}

/// A compacted D310 token as the engine's bytes.
pub(crate) fn expand(small: &[u8]) -> Result<Vec<u8>, String> {
    let bad = |what: &str| format!("a compacted feed token with {what}");
    let mut buf = small;
    let head = take(&mut buf, 17).map_err(|_| bad("no header"))?;
    let version = head[0];
    if !matches!(version, 1 | 2) {
        return Err(bad("an unknown version"));
    }
    let streams = varint(&mut buf)?;
    let streams_u32 = u32::try_from(streams).map_err(|_| bad("too many streams"))?;
    let delivered = varint(&mut buf)?;
    let session = take(&mut buf, 8)?;
    // At least two bytes a stream remain: refuse a count the bytes can't hold before sizing
    // anything by it.
    if buf.len() < streams as usize * 2 {
        return Err(bad("fewer streams than it says"));
    }
    let tail = if version == 2 { 16 } else { 0 };
    let mut out = Vec::with_capacity(HEAD + streams as usize * 16 + tail);
    out.extend_from_slice(head);
    out.extend_from_slice(&streams_u32.to_le_bytes());
    out.extend_from_slice(&delivered.to_le_bytes());
    out.extend_from_slice(session);
    let mut prev = 0u64;
    for _ in 0..streams {
        let hint = prev.wrapping_add(unzigzag(varint(&mut buf)?) as u64);
        let end = hint.wrapping_add(unzigzag(varint(&mut buf)?) as u64);
        out.extend_from_slice(&hint.to_le_bytes());
        out.extend_from_slice(&end.to_le_bytes());
        prev = hint;
    }
    if version == 2 {
        out.extend_from_slice(&varint(&mut buf)?.to_le_bytes());
        out.extend_from_slice(&varint(&mut buf)?.to_le_bytes());
    }
    if !buf.is_empty() {
        return Err(bad("trailing bytes"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A D310 token of `version` with `streams` streams, its hints `epoch << 32 | offset`.
    fn d310(version: u8, streams: u32, epoch: u64) -> Vec<u8> {
        let mut t = vec![version];
        t.extend_from_slice(&[7u8; 16]);
        t.extend_from_slice(&streams.to_le_bytes());
        t.extend_from_slice(&123_456u64.to_le_bytes());
        t.extend_from_slice(&0x9e37_79b9_7f4a_7c15u64.to_le_bytes());
        for s in 0..u64::from(streams) {
            let hint = ((epoch + s) << 32) | (4096 * s + 100);
            t.extend_from_slice(&hint.to_le_bytes());
            t.extend_from_slice(&(hint + 37 * s).to_le_bytes());
        }
        if version == 2 {
            t.extend_from_slice(&9u64.to_le_bytes());
            t.extend_from_slice(&4u64.to_le_bytes());
        }
        t
    }

    #[test]
    fn a_feed_token_compacts_exactly_and_smaller() {
        for (version, streams, epoch) in [(1, 0, 0), (1, 4, 3), (1, 16, 2400), (2, 16, 1 << 20)] {
            let raw = d310(version, streams, epoch);
            let small = compact(&raw).expect("a known layout compacts");
            assert_eq!(expand(&small).unwrap(), raw, "v{version} x{streams}");
            assert!(
                small.len() < raw.len() || streams == 0,
                "{} < {}",
                small.len(),
                raw.len()
            );
        }
        // 16 streams an hour in (epochs ~2400): about 25 bytes a stream fewer than raw.
        let raw = d310(1, 16, 2400);
        assert!(compact(&raw).unwrap().len() <= 37 + 16 * 10, "{raw:?}");
    }

    #[test]
    fn a_token_this_build_cant_read_is_carried_raw_and_still_resumes() {
        // An unknown version, a length that doesn't match its stream count, and a later
        // version's extra bytes: none compacts, and the cluster token keeps each byte for byte.
        let mut longer = d310(1, 4, 1);
        longer.push(0);
        for raw in [vec![3u8; 70], d310(1, 4, 1)[..60].to_vec(), longer] {
            assert_eq!(compact(&raw), None, "{raw:?}");
            let t = ClusterToken {
                cluster_id: "c1".into(),
                map_version: 3,
                subscription: 9,
                components: vec![Component {
                    partition: "p1".into(),
                    epoch: 1,
                    at: At::Token(Bytes::from(raw.clone())),
                }],
            };
            assert_eq!(ClusterToken::decode(&t.encode()).unwrap(), t);
        }
    }

    #[test]
    fn a_cluster_token_round_trips_and_refuses_what_isnt_one() {
        let t = ClusterToken {
            cluster_id: "c1".into(),
            map_version: 12,
            subscription: subscription_hash(&["a".into(), "b".into()], b"x", true),
            components: (0..16)
                .map(|p| Component {
                    partition: format!("p{p:02}"),
                    epoch: p,
                    at: match p {
                        0 => At::Now,
                        1 => At::Oldest,
                        _ => At::Token(Bytes::from(d310(1, 16, 2400 + p))),
                    },
                })
                .collect(),
        };
        let bytes = t.encode();
        assert_eq!(ClusterToken::decode(&bytes).unwrap(), t);
        assert!(
            ClusterToken::decode(&d310(1, 4, 1)).is_err(),
            "a single node's token"
        );
        assert!(ClusterToken::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(ClusterToken::decode(&longer).is_err());
        // The subscription's identity: tables, prefix and visibility each count.
        let h = |t: &[&str], p: &[u8], v| {
            subscription_hash(&t.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(), p, v)
        };
        assert_ne!(h(&["a", "b"], b"", false), h(&["ab"], b"", false));
        assert_ne!(h(&["a"], b"", false), h(&["a"], b"", true));
        assert_ne!(h(&["a"], b"x", false), h(&["a"], b"", false));
    }
}
