//! A cluster read's position (D317 ticket 4, simp724's M1 on the 4a plan): which partition a
//! fanned-out `Scan` or `MultiGet` goes on in, and that partition's own position in it.
//!
//! A single node's `ScanPosition` and `KeysPosition` name the table by its id in one file, and
//! the partitions of a table are separate files with ids of their own: a position is only
//! valid on the partition that issued it. So the cluster's position wraps it with the
//! partition, and a page that ends at a partition's end points at the next partition's start
//! (`inner` `None`). An empty position still means only the read's true end.
//!
//! Bytes: `0xC1, kind ('S' a scan, 'K' a multi-get), partition length (u16 LE), partition,
//! then the partition's own position` (nothing for its start). A single node's positions
//! start with their version byte, 1, so neither is ever taken for the other: a cluster
//! position sent to a single node, or a single node's to a cluster read, is refused by name.

use crate::serve::errors::ApiError;

/// The first byte of a cluster position.
const MARK: u8 = 0xC1;
/// A single node's position's first byte (`pigeonhole::read::budget`'s version).
const NODE_VERSION: u8 = 1;

/// Which read a position resumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Scan,
    Keys,
}

impl Kind {
    fn byte(self) -> u8 {
        match self {
            Self::Scan => b'S',
            Self::Keys => b'K',
        }
    }

    fn what(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Keys => "multi-get",
        }
    }
}

/// Where a cluster read goes on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Position {
    pub partition: String,
    /// The partition's own position; `None` for its start.
    pub inner: Option<Vec<u8>>,
}

impl Position {
    pub(crate) fn to_bytes(&self, kind: Kind) -> Vec<u8> {
        let p = self.partition.as_bytes();
        let len = u16::try_from(p.len()).expect("a partition id is at most 64 bytes");
        let inner = self.inner.as_deref().unwrap_or_default();
        let mut b = Vec::with_capacity(4 + p.len() + inner.len());
        b.extend_from_slice(&[MARK, kind.byte()]);
        b.extend_from_slice(&len.to_le_bytes());
        b.extend_from_slice(p);
        b.extend_from_slice(inner);
        b
    }

    /// A cluster read's `after`: `None` when empty (from the start).
    pub(crate) fn from_bytes(b: &[u8], kind: Kind) -> Result<Option<Self>, ApiError> {
        if b.is_empty() {
            return Ok(None);
        }
        let bad = || {
            ApiError::invalid(format!(
                "not a cluster {} position: resume only from a page's `next`",
                kind.what()
            ))
        };
        match b[0] {
            MARK => {}
            NODE_VERSION => {
                return Err(ApiError::invalid(format!(
                    "a single node's {} position, sent to a cluster: a cluster read resumes \
                     only from its own pages' `next`",
                    kind.what()
                )));
            }
            _ => return Err(bad()),
        }
        if b.len() < 4 || b[1] != kind.byte() {
            return Err(bad());
        }
        let len = usize::from(u16::from_le_bytes([b[2], b[3]]));
        let partition = b.get(4..4 + len).ok_or_else(bad)?;
        let partition = std::str::from_utf8(partition)
            .map_err(|_| bad())?
            .to_owned();
        let rest = &b[4 + len..];
        Ok(Some(Self {
            partition,
            inner: (!rest.is_empty()).then(|| rest.to_vec()),
        }))
    }
}

/// On a single node: refuses a cluster read's position by name, rather than letting the
/// engine call it malformed (simp724 on the 4a plan).
pub(crate) fn refuse_on_a_single_node(after: &[u8]) -> Result<(), ApiError> {
    if after.first() == Some(&MARK) {
        return Err(ApiError::invalid(
            "a cluster node's position, sent to a single node (or to a partition directly): \
             resume it through the cluster",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Kind, Position, refuse_on_a_single_node};

    #[test]
    fn a_position_round_trips_and_is_never_taken_for_the_other_kind() {
        for p in [
            Position {
                partition: "p2".into(),
                inner: None,
            },
            Position {
                partition: "p1".into(),
                inner: Some(vec![1, b'S', 0, 9, 9]),
            },
        ] {
            for kind in [Kind::Scan, Kind::Keys] {
                let b = p.to_bytes(kind);
                assert_eq!(Position::from_bytes(&b, kind).unwrap(), Some(p.clone()));
                let other = if kind == Kind::Scan {
                    Kind::Keys
                } else {
                    Kind::Scan
                };
                assert!(Position::from_bytes(&b, other).is_err());
                assert!(refuse_on_a_single_node(&b).is_err());
            }
        }
        assert_eq!(Position::from_bytes(&[], Kind::Scan).unwrap(), None);
        // A single node's position is refused by name on a cluster, and passes on a node.
        let node = [1, b'S', 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let e = Position::from_bytes(&node, Kind::Scan).unwrap_err();
        assert!(e.message.contains("single node"), "{}", e.message);
        refuse_on_a_single_node(&node).unwrap();
        // Truncated or foreign bytes.
        for b in [&[0xC1][..], &[0xC1, b'S', 5, 0, b'p'], &[7, 7]] {
            assert!(Position::from_bytes(b, Kind::Scan).is_err(), "{b:?}");
        }
    }
}
