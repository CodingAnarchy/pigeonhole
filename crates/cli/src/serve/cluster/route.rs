//! Routing by the partition map (D317 §3, ticket 2): which partition holds a table's row,
//! and which node serves that partition.
//!
//! A [`Routes`] is built once per installed map and swapped whole with it, so the request
//! path reads it without a lock (perf287: `load()`'s guard, never `load_full()`). A
//! table's rows are split by its placement's split keys into half-open ranges, compared
//! as bytes: row `r` is in partition `i` where `splits[i-1] <= r < splits[i]`.

use std::collections::HashMap;

use super::map::split_key_bytes;
use crate::serve::proto::ClusterMap;

/// One table's placement: its split keys, as bytes, and the partition of each range.
#[derive(Debug)]
struct Placement {
    splits: Vec<Vec<u8>>,
    partitions: Vec<String>,
}

/// One partition's part of a table: its rows in `[start, end)` (`end` `None`: the table's
/// end).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartRange {
    pub partition: String,
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

impl PartRange {
    /// Whether the rows in `[lo, hi)` (`hi` `None`: no end) meet this range.
    pub(crate) fn meets(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        let below_end = self.end.as_deref().is_none_or(|e| lo < e);
        let above_start = hi.is_none_or(|h| self.start.as_slice() < h);
        below_end && above_start
    }

    /// Whether `row` is in this range.
    pub(crate) fn holds(&self, row: &[u8]) -> bool {
        self.start.as_slice() <= row && self.end.as_deref().is_none_or(|e| row < e)
    }
}

/// Where a partition is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Owner {
    pub node: String,
    pub address: String,
    pub epoch: u64,
}

/// The routing view of one map version.
#[derive(Debug)]
pub(crate) struct Routes {
    map_version: u64,
    tables: HashMap<String, Placement>,
    owners: HashMap<String, Owner>,
}

impl Routes {
    /// The routes of `map`, which [`super::map::validate`] accepted: every placement's
    /// partitions and every partition's node are in it, and its split keys are well formed.
    pub(crate) fn new(map: &ClusterMap) -> Self {
        let addresses: HashMap<&str, &str> = map
            .nodes
            .iter()
            .map(|n| (n.id.as_str(), n.address.as_str()))
            .collect();
        let owners = map
            .partitions
            .iter()
            .map(|p| {
                let address = addresses.get(p.node.as_str()).copied().unwrap_or_default();
                let owner = Owner {
                    node: p.node.clone(),
                    address: address.to_owned(),
                    epoch: p.epoch,
                };
                (p.id.clone(), owner)
            })
            .collect();
        let tables = map
            .tables
            .iter()
            .map(|t| {
                let placement = Placement {
                    // Validated: every split key has a form.
                    splits: t
                        .splits
                        .iter()
                        .filter_map(|k| split_key_bytes(k).ok())
                        .collect(),
                    partitions: t.partitions.clone(),
                };
                (t.name.clone(), placement)
            })
            .collect();
        Self {
            map_version: map.map_version,
            tables,
            owners,
        }
    }

    pub(crate) fn map_version(&self) -> u64 {
        self.map_version
    }

    /// The partition holding `table`'s row `row`; `None` for a table the map doesn't place.
    pub(crate) fn partition(&self, table: &str, row: &[u8]) -> Option<&str> {
        let p = self.tables.get(table)?;
        let i = p.splits.partition_point(|s| s.as_slice() <= row);
        p.partitions.get(i).map(String::as_str)
    }

    /// The partitions holding `table`'s rows from `lo` up to `hi` (inclusive when its flag
    /// says so; `None`: to the end), in key order; `None` for a table the map doesn't place.
    /// An empty range reaches `lo`'s partition only.
    pub(crate) fn span(
        &self,
        table: &str,
        lo: &[u8],
        hi: Option<(&[u8], bool)>,
    ) -> Option<Vec<&str>> {
        let p = self.tables.get(table)?;
        let first = p.splits.partition_point(|s| s.as_slice() <= lo);
        let last = match hi {
            None => p.partitions.len().saturating_sub(1),
            // The last row is `hi` itself: its partition.
            Some((hi, true)) => p.splits.partition_point(|s| s.as_slice() <= hi),
            // Every row is below `hi`: a range starting at `hi` isn't reached.
            Some((hi, false)) => p.splits.partition_point(|s| s.as_slice() < hi),
        };
        let mut out: Vec<&str> = Vec::new();
        for q in &p.partitions[first.min(p.partitions.len())
            ..=last.max(first).min(p.partitions.len().saturating_sub(1))]
        {
            if !out.contains(&q.as_str()) {
                out.push(q);
            }
        }
        Some(out)
    }

    /// `table`'s partitions in key order, each with its half-open range (D317 ticket 4's
    /// fan-out); `None` for a table the map doesn't place.
    pub(crate) fn ranges(&self, table: &str) -> Option<Vec<PartRange>> {
        let p = self.tables.get(table)?;
        Some(
            p.partitions
                .iter()
                .enumerate()
                .map(|(i, partition)| PartRange {
                    partition: partition.clone(),
                    start: if i == 0 {
                        Vec::new()
                    } else {
                        p.splits[i - 1].clone()
                    },
                    end: p.splits.get(i).cloned(),
                })
                .collect(),
        )
    }

    /// Where `partition` is served; `None` for a partition the map doesn't list.
    pub(crate) fn owner(&self, partition: &str) -> Option<&Owner> {
        self.owners.get(partition)
    }

    /// Whether a node of the map is at `address`.
    pub(crate) fn has_address(&self, address: &str) -> bool {
        self.owners.values().any(|o| o.address == address)
    }

    /// The partitions the map gives `node`.
    pub(crate) fn partitions_of<'a>(&'a self, node: &'a str) -> impl Iterator<Item = &'a str> {
        self.owners
            .iter()
            .filter(move |(_, o)| o.node == node)
            .map(|(id, _)| id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::{Node, Partition, SplitKey, TablePlacement, split_key};

    fn text(s: &str) -> SplitKey {
        SplitKey {
            key: Some(split_key::Key::Text(s.into())),
        }
    }

    fn raw(b: &[u8]) -> SplitKey {
        SplitKey {
            key: Some(split_key::Key::Raw(b.to_vec().into())),
        }
    }

    /// `users` over p1 | "g" | p2 | "m\xff" | p3, `orders` unsplit on p3; p1 on a, p2 and p3
    /// on b.
    fn map() -> ClusterMap {
        let part = |id: &str, node: &str, epoch| Partition {
            id: id.into(),
            node: node.into(),
            epoch,
        };
        ClusterMap {
            cluster_id: "c1".into(),
            map_version: 7,
            nodes: vec![
                Node {
                    id: "a".into(),
                    address: "127.0.0.1:7001".into(),
                },
                Node {
                    id: "b".into(),
                    address: "127.0.0.1:7002".into(),
                },
            ],
            partitions: vec![part("p1", "a", 1), part("p2", "b", 2), part("p3", "b", 1)],
            tables: vec![
                TablePlacement {
                    name: "users".into(),
                    group: String::new(),
                    families: Vec::new(),
                    splits: vec![text("g"), raw(b"m\xff")],
                    partitions: vec!["p1".into(), "p2".into(), "p3".into()],
                },
                TablePlacement {
                    name: "orders".into(),
                    group: String::new(),
                    families: Vec::new(),
                    splits: Vec::new(),
                    partitions: vec!["p3".into()],
                },
            ],
        }
    }

    #[test]
    fn a_row_routes_to_the_half_open_range_holding_it() {
        let r = Routes::new(&map());
        assert_eq!(r.map_version(), 7);
        // Below the first split, at it (a range starts at its split key), between, at the
        // raw split, and above the last.
        for (row, want) in [
            (&b""[..], "p1"),
            (b"a", "p1"),
            (b"f\xff", "p1"),
            (b"g", "p2"),
            (b"g\x00", "p2"),
            (b"m", "p2"),
            (b"m\xfe", "p2"),
            (b"m\xff", "p3"),
            (b"z", "p3"),
        ] {
            assert_eq!(r.partition("users", row), Some(want), "{row:?}");
        }
        assert_eq!(r.partition("orders", b"anything"), Some("p3"));
        assert_eq!(r.partition("nope", b"a"), None);
    }

    /// A range's partitions: an exclusive end at a split key stops before its range, an
    /// inclusive one reaches it (D317 ticket 3's snapshot scans).
    #[test]
    fn a_range_spans_the_partitions_its_rows_can_be_in() {
        let r = Routes::new(&map());
        let span = |lo: &[u8], hi: Option<(&[u8], bool)>| r.span("users", lo, hi).unwrap();
        assert_eq!(span(b"", None), ["p1", "p2", "p3"]);
        assert_eq!(span(b"a", Some((b"g", false))), ["p1"]);
        assert_eq!(span(b"a", Some((b"g", true))), ["p1", "p2"]);
        assert_eq!(span(b"g", Some((b"m\xff", false))), ["p2"]);
        assert_eq!(span(b"h", Some((b"z", false))), ["p2", "p3"]);
        assert_eq!(span(b"z", None), ["p3"]);
        // Empty: the start's partition only.
        assert_eq!(span(b"z", Some((b"a", false))), ["p3"]);
        assert_eq!(r.span("orders", b"", None).unwrap(), ["p3"]);
        assert!(r.span("nope", b"", None).is_none());
    }

    #[test]
    fn a_partition_routes_to_its_node_at_its_epoch() {
        let r = Routes::new(&map());
        assert_eq!(
            r.owner("p2"),
            Some(&Owner {
                node: "b".into(),
                address: "127.0.0.1:7002".into(),
                epoch: 2,
            })
        );
        assert_eq!(r.owner("p9"), None);
        let mut on_b: Vec<_> = r.partitions_of("b").collect();
        on_b.sort_unstable();
        assert_eq!(on_b, ["p2", "p3"]);
        assert_eq!(r.partitions_of("c").count(), 0);
    }
}
