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
//!   deltas. The layout is the engine's, read here without its help: a token whose length
//!   doesn't fit a layout this build knows is carried raw, and one that fits comes back byte
//!   for byte whatever its fields hold ([`compact`]). An engine change to the layout can
//!   make tokens larger, never wrong.

use prost::bytes::Bytes;
use prost::encoding::decode_varint;

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

/// `v` as a LEB128 varint, as `prost::encoding::encode_varint` writes it, straight into a
/// `Vec`: a one-partition subscription's checkpoint writes about a dozen of them on every
/// event (#1798 (b)).
#[inline]
fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    // Byte by byte: `push` inlines to a capacity check and a store, where a slice copy of
    // one to ten bytes is a call to `memcpy` (most of the checkpoint's time in #1798's
    // profile).
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_varint(b.len() as u64, out);
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

/// A subscription's last checkpoint, by part: the token's prefix (everything before the
/// components) and each component's bytes, each kept with the values it was encoded from and
/// rebuilt only when they differ (#1758's 2a). Keyed by value rather than invalidated where
/// things change, so a change it isn't told about can't leave a stale token: a checkpoint
/// equals the token built fresh from the same parts, whatever happened between
/// (`a_cached_token_equals_one_built_fresh_whatever_changes`).
#[derive(Debug, Default)]
pub(crate) struct TokenCache {
    /// The prefix, and the map version it was built for (the rest of it is fixed for the
    /// subscription).
    prefix: Vec<u8>,
    prefix_for: Option<u64>,
    parts: Vec<CachedPart>,
    scratch: Vec<u8>,
}

/// A part's component as [`TokenCache`] holds it: its bytes, and the epoch and position they
/// were built from (none yet).
#[derive(Debug, Default)]
struct CachedPart {
    made_from: Option<(u64, At)>,
    bytes: Vec<u8>,
}

impl TokenCache {
    /// The token of a subscription (`cluster_id`, `subscription`) at `map_version` whose
    /// parts are `components`, always the same parts in the same order, into `out`.
    pub(crate) fn encode<'a>(
        &mut self,
        cluster_id: &str,
        map_version: u64,
        subscription: u64,
        components: impl ExactSizeIterator<Item = (&'a str, u64, &'a At)>,
        out: &mut Vec<u8>,
    ) {
        let n = components.len();
        if self.prefix_for != Some(map_version) || self.parts.len() != n {
            self.prefix.clear();
            put_prefix(cluster_id, map_version, subscription, n, &mut self.prefix);
            self.prefix_for = Some(map_version);
            self.parts.resize_with(n, Default::default);
        }
        out.extend_from_slice(&self.prefix);
        // One part: its position moves with every event, so a cached component would never
        // be reused. It's built straight into `out` (#1798 (b)).
        if n == 1 {
            for (partition, epoch, at) in components {
                put_component(partition, epoch, at, out, &mut self.scratch);
            }
            return;
        }
        for ((partition, epoch, at), part) in components.zip(&mut self.parts) {
            if !matches!(&part.made_from, Some((e, a)) if *e == epoch && a == at) {
                part.bytes.clear();
                put_component(partition, epoch, at, &mut part.bytes, &mut self.scratch);
                part.made_from = Some((epoch, at.clone()));
            }
            out.extend_from_slice(&part.bytes);
        }
    }
}

/// A cluster token's bytes before its components.
fn put_prefix(cluster_id: &str, map_version: u64, subscription: u64, n: usize, out: &mut Vec<u8>) {
    out.push(MAGIC);
    out.push(VERSION);
    put_bytes(out, cluster_id.as_bytes());
    put_varint(map_version, out);
    out.extend_from_slice(&subscription.to_le_bytes());
    put_varint(n as u64, out);
}

/// One component's bytes; `scratch` holds a compacted token of 128 bytes or more on the way.
fn put_component(partition: &str, epoch: u64, at: &At, out: &mut Vec<u8>, scratch: &mut Vec<u8>) {
    put_bytes(out, partition.as_bytes());
    put_varint(epoch, out);
    match at {
        At::Now => out.push(NOW),
        At::Oldest => out.push(OLDEST),
        At::Token(token) => {
            // Compacted straight into `out` behind a one-byte length, patched after: a
            // compacted token is under 128 bytes at any likely stream count, so the copy
            // through `scratch` and its length varint are saved on every event (#1798 (b)).
            // A longer one moves its bytes up for the wider length.
            out.push(COMPACT);
            let len_at = out.len();
            out.push(0);
            if compact_into(token, out) {
                let n = out.len() - len_at - 1;
                if n < 0x80 {
                    out[len_at] = n as u8;
                } else {
                    scratch.clear();
                    scratch.extend_from_slice(&out[len_at + 1..]);
                    out.truncate(len_at);
                    put_bytes(out, scratch);
                }
            } else {
                out.truncate(len_at - 1);
                out.push(RAW);
                put_bytes(out, token);
            }
        }
    }
}

/// A D310 token compacted (perf287's form on ticket 7), appended to `out`; false, with `out`
/// as it was, for one whose length doesn't fit a layout this build knows:
/// - version and database id as they are;
/// - the stream count and the last seqno delivered as varints; the writer session as its 8
///   bytes (it's random, so a varint would be longer);
/// - each stream's `(hint, delivered_end)`: the hint as a zigzag varint delta from the
///   previous stream's hint, and the end as one from its hint (the same segment, usually);
/// - a feed of every table's catalog positions (version 2) as varints.
fn compact_into(raw: &[u8], out: &mut Vec<u8>) -> bool {
    let Some(&version) = raw.first() else {
        return false;
    };
    if raw.len() < HEAD || !matches!(version, 1 | 2) {
        return false;
    }
    let u64_at = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().expect("8 bytes"));
    let streams = u32::from_le_bytes(raw[17..21].try_into().expect("4 bytes")) as usize;
    let tail = if version == 2 { 16 } else { 0 };
    if raw.len() != HEAD + streams * 16 + tail {
        return false;
    }
    let from = out.len();
    out.extend_from_slice(&raw[..17]);
    put_varint(streams as u64, out);
    put_varint(u64_at(21), out);
    out.extend_from_slice(&raw[29..37]);
    let mut prev = 0u64;
    for s in 0..streams {
        let (hint, end) = (u64_at(HEAD + s * 16), u64_at(HEAD + s * 16 + 8));
        put_varint(zigzag(hint.wrapping_sub(prev) as i64), out);
        put_varint(zigzag(end.wrapping_sub(hint) as i64), out);
        prev = hint;
    }
    if version == 2 {
        let at = HEAD + streams * 16;
        put_varint(u64_at(at), out);
        put_varint(u64_at(at + 8), out);
    }
    // Once the length matches the layout, every field is read whole and each step (a
    // varint, a zigzag of a wrapping difference) is exact, so the bytes come back as they
    // were whatever they hold: a layout this build misreads makes a token larger, never
    // wrong. Checked here in debug builds (every test), and over random tokens by
    // `any_token_of_a_known_layout_comes_back_byte_for_byte`, rather than per event
    // (#1758: expanding it again was part of a checkpoint's cost on every event).
    debug_assert_eq!(
        expand(&out[from..]).as_deref(),
        Ok(raw),
        "a compaction that doesn't come back"
    );
    true
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
    if buf.len() < (streams_u32 as usize).saturating_mul(2) {
        return Err(bad("fewer streams than it says"));
    }
    let tail = if version == 2 { 16 } else { 0 };
    // Bounded by the check above: a stream is at least two bytes of `buf`.
    let mut out = Vec::with_capacity(HEAD + (streams_u32 as usize).saturating_mul(16) + tail);
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

    #[test]
    fn put_varint_writes_what_prost_writes() {
        // Every length's edges, and a seeded spread between them.
        let mut values = vec![0u64, u64::MAX];
        for bits in 1..64 {
            let v = 1u64 << bits;
            values.extend([v - 1, v, v + 1]);
        }
        let seed = 0x5eed_1798u64;
        let mut x = seed;
        for _ in 0..10_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            values.push(x >> (x % 64));
        }
        for v in values {
            let (mut ours, mut theirs) = (vec![0xAA], vec![0xAA]);
            put_varint(v, &mut ours);
            prost::encoding::encode_varint(v, &mut theirs);
            assert_eq!(ours, theirs, "{v} (seed {seed:#x})");
        }
    }

    /// A cluster token's bytes, appended to `out`, from its parts as a subscription holds them,
    /// so a checkpoint (one per event on a one-partition subscription) builds no
    /// [`ClusterToken`] and allocates nothing once `out` and `scratch` have grown (#1758).
    /// `scratch` holds each compacted component on its way.
    fn encode_into<'a>(
        cluster_id: &str,
        map_version: u64,
        subscription: u64,
        components: impl ExactSizeIterator<Item = (&'a str, u64, &'a At)>,
        out: &mut Vec<u8>,
        scratch: &mut Vec<u8>,
    ) {
        put_prefix(cluster_id, map_version, subscription, components.len(), out);
        for (partition, epoch, at) in components {
            put_component(partition, epoch, at, out, scratch);
        }
    }

    fn compact(raw: &[u8]) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        compact_into(raw, &mut out).then_some(out)
    }

    impl ClusterToken {
        fn encode(&self) -> Vec<u8> {
            let mut out = Vec::new();
            encode_into(
                &self.cluster_id,
                self.map_version,
                self.subscription,
                self.components
                    .iter()
                    .map(|c| (c.partition.as_str(), c.epoch, &c.at)),
                &mut out,
                &mut Vec::new(),
            );
            out
        }
    }

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
    fn a_cached_token_equals_one_built_fresh_whatever_changes() {
        // simp724's test for #1758's 2a: whatever happens to a subscription's parts between
        // checkpoints, the cached token is the one built fresh. Seeded (`PHDB_TOKEN_SEED`);
        // the seed and step print on failure.
        let seed: u64 = std::env::var("PHDB_TOKEN_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0x2a_1758);
        let mut x = seed;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for parts in [1usize, 3] {
            let mut cache = TokenCache::default();
            let mut map_version = 4u64;
            let mut comps: Vec<(String, u64, At)> =
                (0..parts).map(|i| (format!("p{i}"), 1, At::Now)).collect();
            for step in 0..3000 {
                let i = (next() % parts as u64) as usize;
                let what = match next() % 10 {
                    // A part's next position: mostly a token that compacts, sometimes one
                    // carried raw.
                    0..=4 => {
                        let raw = if next() % 5 == 0 {
                            (0..40).map(|_| next() as u8).collect()
                        } else {
                            d310(1, 4, next() % 5000)
                        };
                        comps[i].2 = At::Token(Bytes::from(raw));
                        "position"
                    }
                    // Moved, by copy (#1729): a reopen records the new epoch.
                    5 => {
                        comps[i].1 += 1;
                        "move"
                    }
                    // A reopen took a newer map.
                    6 => {
                        map_version += 1 + next() % 3;
                        "map"
                    }
                    // Back to no position (opened from its own now or oldest).
                    7 => {
                        comps[i].2 = if next() % 2 == 0 { At::Now } else { At::Oldest };
                        "reopen"
                    }
                    // skip_through: passed over by the part's node, nothing in the token.
                    _ => "skip_through",
                };
                let mut cached = Vec::new();
                cache.encode(
                    "c1",
                    map_version,
                    77,
                    comps.iter().map(|(p, e, a)| (p.as_str(), *e, a)),
                    &mut cached,
                );
                let mut fresh = Vec::new();
                encode_into(
                    "c1",
                    map_version,
                    77,
                    comps.iter().map(|(p, e, a)| (p.as_str(), *e, a)),
                    &mut fresh,
                    &mut Vec::new(),
                );
                assert_eq!(
                    cached, fresh,
                    "seed {seed}, {parts} parts, step {step} ({what} on part {i})"
                );
            }
        }
    }

    #[test]
    fn any_token_of_a_known_layout_comes_back_byte_for_byte() {
        // #1758: the per-event check is a debug assertion now, so this is the guarantee:
        // whatever a known layout's fields hold (random bytes, extremes), compacting then
        // expanding gives the token back. Seeded; the seed prints on failure.
        let seed: u64 = std::env::var("PHDB_TOKEN_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0x5eed_1758);
        let mut x = seed;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for round in 0..2000 {
            let version = if next() % 2 == 0 { 1u8 } else { 2 };
            let streams = (next() % 33) as u32;
            let mut raw = vec![version];
            for _ in 0..16 {
                raw.push(next() as u8);
            }
            raw.extend_from_slice(&streams.to_le_bytes());
            let words = 2 + 2 * streams as usize + if version == 2 { 2 } else { 0 };
            for _ in 0..words {
                let w = match next() % 4 {
                    0 => 0,
                    1 => u64::MAX,
                    _ => next(),
                };
                raw.extend_from_slice(&w.to_le_bytes());
            }
            let mut out = vec![0xAA];
            assert!(compact_into(&raw, &mut out), "seed {seed} round {round}");
            assert_eq!(out[0], 0xAA, "appended after what was there");
            assert_eq!(
                expand(&out[1..]).unwrap(),
                raw,
                "seed {seed} round {round}: {raw:?}"
            );
        }
        // One that doesn't compact leaves `out` as it was.
        let mut out = vec![1, 2];
        assert!(!compact_into(&[3u8; 70], &mut out));
        assert_eq!(out, [1, 2]);
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
    fn a_component_compacted_in_place_round_trips_on_both_sides_of_a_one_byte_length() {
        // #1798 (b): a component's compacted token is written straight into the output
        // behind a one-byte length, and one of 128 bytes or more moves up for the wider
        // length. Tokens around that edge, one part each, come back byte for byte.
        let mut seen = std::collections::BTreeSet::new();
        for streams in 0..40u32 {
            let raw = d310(2, streams, 1 << 20);
            let len = compact(&raw).expect("a known layout compacts").len();
            seen.insert(len >= 0x80);
            let t = ClusterToken {
                cluster_id: "c1".into(),
                map_version: 5,
                subscription: 9,
                components: vec![Component {
                    partition: "p1".into(),
                    epoch: 3,
                    at: At::Token(Bytes::from(raw)),
                }],
            };
            assert_eq!(
                ClusterToken::decode(&t.encode()).unwrap(),
                t,
                "{streams} streams, compacted to {len} bytes"
            );
        }
        assert_eq!(
            seen.len(),
            2,
            "lengths on both sides of 128 were tried: {seen:?}"
        );
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
