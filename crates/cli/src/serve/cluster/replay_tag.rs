//! D336 §8 (#2148): the tag a replayed tail commit carries, in the commit's D329 tag slot
//! (`LoggedBatch::tag_xid`, ICR 0147), so the new owner's feed names its origin and a later
//! replay of the same commit skips it.
//!
//! The tag is `0x00 'R' <version>`, the origin's history epoch and seqno (varints), then the
//! commit's original D329 xid, if it had one. No D329 xid starts with `0x00` (they're
//! `<home>:h<epoch>-<hex>`, and partition ids are `[a-z0-9_-]`), so a tag is either one or
//! the other. Every serve path that turns a commit's tag into `FeedEvent.xid` goes through
//! [`split`] (simp724's S1 on the design): the engine's `xid()` stays the raw tag.

/// The tag's first two bytes.
const MAGIC: [u8; 2] = [0x00, b'R'];

/// The tag's version (coordinator, 2026-11-27, numbers.md). 0 is reserved and never written,
/// so a zeroed tag never reads as a valid one; a version this build doesn't know is corrupt
/// ([`split`]), never guessed at.
pub(crate) const REPLAY_TAG_VERSION: u8 = 1;

/// The longest tag the engine takes (ICR 0147: 1–255 bytes).
const TAG_MAX: usize = 255;

/// Where a replayed commit came from: its first origin's history epoch and seqno there, kept
/// through later hops (§5: a commit replayed twice names its first origin).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Origin {
    pub(crate) history_epoch: u64,
    pub(crate) seqno: u64,
}

/// The tag for a commit replayed from `origin`, with its original D329 `xid` (empty for
/// none). `None` when the whole tag would pass the engine's 255 bytes, which no real xid
/// reaches (§8: at most 23 bytes of prefix and origin, an xid at most about 110).
pub(crate) fn encode(origin: Origin, xid: &[u8]) -> Option<Vec<u8>> {
    let mut tag = Vec::with_capacity(3 + 20 + xid.len());
    tag.extend_from_slice(&MAGIC);
    tag.push(REPLAY_TAG_VERSION);
    prost::encoding::encode_varint(origin.history_epoch, &mut tag);
    prost::encoding::encode_varint(origin.seqno, &mut tag);
    tag.extend_from_slice(xid);
    (tag.len() <= TAG_MAX).then_some(tag)
}

/// A tag that starts as a replay's but isn't one this build reads: a version it doesn't know
/// (0, which is never written, included) or a cut origin. The commit is refused as corrupt,
/// never read as having no origin or no xid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BadTag(pub(crate) String);

impl std::fmt::Display for BadTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a corrupt replay tag: {}", self.0)
    }
}

/// A commit's tag split into its replay origin, if it's a replay's, and its D329 xid (empty
/// for none).
pub(crate) fn split(tag: &[u8]) -> Result<(Option<Origin>, &[u8]), BadTag> {
    let Some(rest) = tag.strip_prefix(&MAGIC[..]) else {
        return Ok((None, tag));
    };
    let Some((&version, mut rest)) = rest.split_first() else {
        return Err(BadTag("no version".into()));
    };
    if version != REPLAY_TAG_VERSION {
        return Err(BadTag(format!(
            "version {version}, which this build doesn't read"
        )));
    }
    let (Ok(history_epoch), Ok(seqno)) = (
        prost::encoding::decode_varint(&mut rest),
        prost::encoding::decode_varint(&mut rest),
    ) else {
        return Err(BadTag("its origin is cut short".into()));
    };
    Ok((
        Some(Origin {
            history_epoch,
            seqno,
        }),
        rest,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_replay_tag_round_trips_with_and_without_an_xid() {
        let o = Origin {
            history_epoch: 7,
            seqno: u64::MAX,
        };
        for xid in [&b""[..], b"p1:h3-00ff"] {
            let tag = encode(o, xid).unwrap();
            assert_eq!(tag[2], REPLAY_TAG_VERSION);
            assert_eq!(split(&tag), Ok((Some(o), xid)));
        }
    }

    #[test]
    fn a_plain_xid_is_no_replay() {
        assert_eq!(split(b"p1:h3-00ff"), Ok((None, &b"p1:h3-00ff"[..])));
        assert_eq!(split(b""), Ok((None, &b""[..])));
    }

    /// The coordinator's rule: version 0 is reserved and never written, so a zeroed tag is
    /// refused rather than read as valid.
    #[test]
    fn version_zero_is_refused_as_corrupt() {
        assert_ne!(REPLAY_TAG_VERSION, 0);
        let e = split(&[0x00, b'R', 0, 1, 1]).unwrap_err();
        assert!(e.to_string().contains("version 0"), "{e}");
    }

    /// The coordinator's rule: a version this build doesn't know is corrupt, never guessed.
    #[test]
    fn an_unknown_version_is_refused_as_corrupt() {
        for v in [REPLAY_TAG_VERSION + 1, u8::MAX] {
            let e = split(&[0x00, b'R', v, 1, 1, b'x']).unwrap_err();
            assert!(e.to_string().contains(&format!("version {v}")), "{e}");
        }
    }

    #[test]
    fn a_cut_replay_tag_is_refused_as_corrupt() {
        assert!(split(&[0x00, b'R']).is_err());
        assert!(split(&[0x00, b'R', REPLAY_TAG_VERSION]).is_err());
        assert!(split(&[0x00, b'R', REPLAY_TAG_VERSION, 0x80]).is_err());
        assert!(split(&[0x00, b'R', REPLAY_TAG_VERSION, 1]).is_err());
    }

    #[test]
    fn a_tag_past_the_engine_limit_is_refused() {
        let o = Origin {
            history_epoch: u64::MAX,
            seqno: u64::MAX,
        };
        // 3 bytes of prefix and two 10-byte varints: 23, so an xid of 232 fits and 233 doesn't.
        assert_eq!(encode(o, &[b'x'; 232]).map(|t| t.len()), Some(255));
        assert_eq!(encode(o, &[b'x'; 233]), None);
    }
}
