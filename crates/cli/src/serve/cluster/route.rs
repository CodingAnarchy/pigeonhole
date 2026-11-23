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

impl Placement {
    /// Its ranges in key order, each with its partition: range `i` is
    /// `[splits[i-1], splits[i])`.
    fn ranges(&self) -> Vec<PartRange> {
        self.partitions
            .iter()
            .enumerate()
            .map(|(i, partition)| PartRange {
                partition: partition.clone(),
                start: if i == 0 {
                    Vec::new()
                } else {
                    self.splits[i - 1].clone()
                },
                end: self.splits.get(i).cloned(),
            })
            .collect()
    }

    /// The partitions holding ids that start with `prefix`, in key order and without
    /// repeats (an empty prefix: all of them). Range `i` is `[splits[i-1], splits[i])`: it
    /// holds such an id when it ends past the prefix and starts before the first key past
    /// every id with it.
    fn with_prefix(&self, prefix: &[u8]) -> Vec<&str> {
        let end = prefix_end(prefix);
        let mut out: Vec<&str> = Vec::new();
        for (i, partition) in self.partitions.iter().enumerate() {
            let starts_before_end = match (&end, i.checked_sub(1).map(|j| &self.splits[j])) {
                (Some(end), Some(lo)) => lo < end,
                _ => true,
            };
            let ends_past_prefix = self.splits.get(i).is_none_or(|hi| hi.as_slice() > prefix);
            if starts_before_end && ends_past_prefix && !out.contains(&partition.as_str()) {
                out.push(partition);
            }
        }
        out
    }
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
    /// The epoch P's current history began at (D317 ticket 6): a feed token from an epoch
    /// before it can't resume.
    pub history_epoch: u64,
    /// P's replicas (D328), in the map's order of preference: each node's id and address.
    /// None while a range move gates P's copies (D331 PR 3; see [`Routes::new`]).
    pub replicas: Vec<(String, String)>,
}

/// The routing view of one map version.
#[derive(Debug)]
pub(crate) struct Routes {
    cluster_id: String,
    map_version: u64,
    tables: HashMap<String, Placement>,
    owners: HashMap<String, Owner>,
    /// The memory store's name and its namespaces' placement (#1915), if the map has one.
    memory: Option<(String, Placement)>,
    /// Each index's ids' placement (#1722): its followed table's, or its own. Apart from
    /// `tables`: an index's own table holds rows that aren't keyed by its ids, so a data
    /// call on that table never routes by them.
    indexes: HashMap<String, Placement>,
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
        // D331 PR 3: a partition a move names as its target, or as its source from the
        // hand-over on, has its bounded reads served by its owner while the move is in the
        // map: its copies' range gates may refuse the range (a copy of the target serves it
        // only once it has applied the target's serving row), and a read refused there
        // mustn't count against a copy's health (cli692's F6). The copies' own gates still
        // refuse a read sent by a node with an older map.
        let moving = |id: &str| {
            // A NODE move (a whole relocation, D332 PR 2) leaves the copies serving reads.
            map.moves.iter().any(|m| {
                !super::map::node_move(m)
                    && (m.to == id || (m.from == id && super::map::committed(super::map::phase(m))))
            })
        };
        let owners = map
            .partitions
            .iter()
            .map(|p| {
                let address = addresses.get(p.node.as_str()).copied().unwrap_or_default();
                let owner = Owner {
                    node: p.node.clone(),
                    address: address.to_owned(),
                    epoch: p.epoch,
                    history_epoch: p.history_epoch,
                    replicas: p
                        .replicas
                        .iter()
                        .filter(|_| !moving(&p.id))
                        .map(|r| {
                            let address = addresses.get(r.as_str()).copied().unwrap_or_default();
                            (r.clone(), address.to_owned())
                        })
                        .collect(),
                };
                (p.id.clone(), owner)
            })
            .collect();
        let tables: HashMap<String, Placement> = map
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
        let memory = map.memory.as_ref().map(|m| {
            let placement = Placement {
                splits: m
                    .splits
                    .iter()
                    .filter_map(|k| split_key_bytes(k).ok())
                    .collect(),
                partitions: m.partitions.clone(),
            };
            (m.store.clone(), placement)
        });
        let indexes = map
            .indexes
            .iter()
            .map(|i| {
                let placement = match tables.get(i.follows.as_str()) {
                    Some(t) if !i.follows.is_empty() => Placement {
                        splits: t.splits.clone(),
                        partitions: t.partitions.clone(),
                    },
                    _ => Placement {
                        splits: i
                            .splits
                            .iter()
                            .filter_map(|k| split_key_bytes(k).ok())
                            .collect(),
                        partitions: i.partitions.clone(),
                    },
                };
                (i.name.clone(), placement)
            })
            .collect();
        Self {
            cluster_id: map.cluster_id.clone(),
            map_version: map.map_version,
            tables,
            owners,
            memory,
            indexes,
        }
    }

    /// The partition holding id `id` of index `index` (#1722); `None` for an index the map
    /// doesn't place.
    pub(crate) fn index_partition(&self, index: &str, id: &[u8]) -> Option<&str> {
        let p = self.indexes.get(index)?;
        let i = p.splits.partition_point(|s| s.as_slice() <= id);
        p.partitions.get(i).map(String::as_str)
    }

    /// The memory store the map places, if any (#1915).
    pub(crate) fn memory_store(&self) -> Option<&str> {
        self.memory.as_ref().map(|(s, _)| s.as_str())
    }

    /// The partition holding namespace `ns` of the memory store; `None` when the map places
    /// no store. Split keys have no `\0` (validated), so comparing the namespace itself
    /// places every row of it (`{ns}\0…`) in the same range.
    pub(crate) fn memory_partition(&self, ns: &str) -> Option<&str> {
        let (_, p) = self.memory.as_ref()?;
        let i = p.splits.partition_point(|s| s.as_slice() <= ns.as_bytes());
        p.partitions.get(i).map(String::as_str)
    }

    /// Every partition the memory store is placed on, in the placement's order; empty when
    /// the map places none.
    pub(crate) fn memory_partitions(&self) -> &[String] {
        self.memory
            .as_ref()
            .map_or(&[], |(_, p)| p.partitions.as_slice())
    }

    pub(crate) fn map_version(&self) -> u64 {
        self.map_version
    }

    pub(crate) fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// The partitions holding rows of `table` that start with `prefix`, in key order and
    /// without repeats (an empty prefix: all of the table's); `None` for a table the map
    /// doesn't place. Range `i` is `[splits[i-1], splits[i])`: it holds such a row when it
    /// ends past the prefix and starts before the first key past every row with it.
    pub(crate) fn partitions_of_table(&self, table: &str, prefix: &[u8]) -> Option<Vec<&str>> {
        Some(self.tables.get(table)?.with_prefix(prefix))
    }

    /// The partitions holding ids of index `index` that start with `prefix` (#1722), as
    /// [`partitions_of_table`](Self::partitions_of_table) gives a table's; `None` for an
    /// index the map doesn't place. A partition with several of the index's ranges is
    /// listed once: it holds one index.
    pub(crate) fn index_partitions(&self, index: &str, prefix: &[u8]) -> Option<Vec<&str>> {
        Some(self.indexes.get(index)?.with_prefix(prefix))
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
        Some(self.tables.get(table)?.ranges())
    }

    /// `index`'s id ranges in order, each with its partition (#1722 PR 2b: the ranges an
    /// `allow_partial` text search names as missing); `None` for an index the map doesn't
    /// place.
    pub(crate) fn index_ranges(&self, index: &str) -> Option<Vec<PartRange>> {
        Some(self.indexes.get(index)?.ranges())
    }

    /// Where `partition` is served; `None` for a partition the map doesn't list.
    pub(crate) fn owner(&self, partition: &str) -> Option<&Owner> {
        self.owners.get(partition)
    }

    /// Every node the map places a partition or a replica copy on, once each: `(id,
    /// address)`.
    pub(crate) fn nodes(&self) -> impl Iterator<Item = (String, String)> + '_ {
        let mut seen = std::collections::BTreeMap::new();
        for o in self.owners.values() {
            seen.entry(o.node.clone())
                .or_insert_with(|| o.address.clone());
            for (n, a) in &o.replicas {
                seen.entry(n.clone()).or_insert_with(|| a.clone());
            }
        }
        seen.into_iter()
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

    /// The partitions the map gives nodes other than `node`, sorted (`Admin.Info`, #1852).
    pub(crate) fn partitions_not_of(&self, node: &str) -> Vec<String> {
        let mut ids: Vec<String> = self
            .owners
            .iter()
            .filter(|(_, o)| o.node != node)
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// The map's tables, sorted (`Admin.Info` on a cluster node, #1852).
    pub(crate) fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }
}

/// The first key after every key starting with `prefix`; `None` when there's none (an
/// empty prefix, or one of `0xff` bytes only).
pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
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
            moves_history: Default::default(),
            pinned: Default::default(),
            history_epoch: 0,
            id: id.into(),
            node: node.into(),
            epoch,
            memory_bytes: 0,
            replicas: Vec::new(),
            failover: 0,
            failover_count: 0,
        };
        ClusterMap {
            balance: Default::default(),
            balancer: Default::default(),
            balancer_events: Default::default(),
            cluster_id: "c1".into(),
            map_version: 7,
            previous_nodes: Vec::new(),
            indexes: Vec::new(),
            memory: None,
            nodes: vec![
                Node {
                    weight: Default::default(),
                    id: "a".into(),
                    address: "127.0.0.1:7001".into(),
                },
                Node {
                    weight: Default::default(),
                    id: "b".into(),
                    address: "127.0.0.1:7002".into(),
                },
            ],
            partitions: vec![part("p1", "a", 1), part("p2", "b", 2), part("p3", "b", 1)],
            tables: vec![
                TablePlacement {
                    pinned_ranges: Default::default(),
                    name: "users".into(),
                    group: String::new(),
                    families: Vec::new(),
                    splits: vec![text("g"), raw(b"m\xff")],
                    partitions: vec!["p1".into(), "p2".into(), "p3".into()],
                    created: 0,
                },
                TablePlacement {
                    pinned_ranges: Default::default(),
                    name: "orders".into(),
                    group: String::new(),
                    families: Vec::new(),
                    splits: Vec::new(),
                    partitions: vec!["p3".into()],
                    created: 0,
                },
            ],
            writer: String::new(),
            writer_term: 0,
            failover: false,
            failover_rejoin: false,
            failovers: Vec::new(),
            moves: Vec::new(),
        }
    }

    /// #1722: an index's id routes by its followed table's ranges, or by its own; a data call
    /// on the index's own table never does (it isn't in `tables`).
    #[test]
    fn an_index_id_routes_by_the_table_it_follows_or_its_own_ranges() {
        use crate::serve::proto::IndexPlacement;
        let mut m = map();
        m.indexes = vec![
            IndexPlacement {
                name: "users_vec".into(),
                follows: "users".into(),
                ..Default::default()
            },
            IndexPlacement {
                name: "own".into(),
                splits: vec![text("k")],
                partitions: vec!["p3".into(), "p1".into()],
                ..Default::default()
            },
        ];
        let r = Routes::new(&m);
        for (id, want) in [(&b"a"[..], "p1"), (b"g", "p2"), (b"m\xff", "p3")] {
            assert_eq!(r.index_partition("users_vec", id), Some(want));
            assert_eq!(r.partition("users", id), Some(want));
        }
        assert_eq!(r.index_partition("own", b"j"), Some("p3"));
        assert_eq!(r.index_partition("own", b"k"), Some("p1"));
        assert_eq!(r.index_partition("nope", b"a"), None);
        assert_eq!(r.partition("users_vec", b"a"), None);
    }

    #[test]
    fn an_index_prefix_narrows_its_partitions_and_lists_each_once() {
        use crate::serve::proto::IndexPlacement;
        let mut m = map();
        m.indexes = vec![
            IndexPlacement {
                name: "users_vec".into(),
                follows: "users".into(),
                ..Default::default()
            },
            // Two ranges on p1, one on p3.
            IndexPlacement {
                name: "own".into(),
                splits: vec![text("f"), text("k")],
                partitions: vec!["p1".into(), "p3".into(), "p1".into()],
                ..Default::default()
            },
        ];
        let r = Routes::new(&m);
        assert_eq!(
            r.index_partitions("users_vec", b""),
            Some(vec!["p1", "p2", "p3"])
        );
        assert_eq!(r.index_partitions("users_vec", b"h"), Some(vec!["p2"]));
        assert_eq!(
            r.index_partitions("users_vec", b"m"),
            Some(vec!["p2", "p3"])
        );
        assert_eq!(r.index_partitions("own", b""), Some(vec!["p1", "p3"]));
        assert_eq!(r.index_partitions("own", b"g"), Some(vec!["p3"]));
        assert_eq!(r.index_partitions("own", b"z"), Some(vec!["p1"]));
        assert_eq!(r.index_partitions("nope", b""), None);
        // A table's own call never sees an index's placement.
        assert_eq!(r.partitions_of_table("own", b""), None);
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
    fn a_prefix_narrows_a_tables_partitions_to_the_ranges_holding_its_rows() {
        // D317 ticket 7: a feed with a prefix subscribes only where such rows can be.
        let r = Routes::new(&map());
        assert_eq!(r.cluster_id(), "c1");
        for (prefix, want) in [
            (&b""[..], &["p1", "p2", "p3"][..]),
            (b"a", &["p1"]),
            (b"f", &["p1"]),
            (b"g", &["p2"]),
            // Rows "m..." run from p2's "m" to p3's "m\xff...".
            (b"m", &["p2", "p3"]),
            (b"m\xfe", &["p2"]),
            (b"m\xff", &["p3"]),
            (b"z", &["p3"]),
            // No key follows every row of 0xff bytes: the last range only.
            (b"\xff\xff", &["p3"]),
        ] {
            assert_eq!(
                r.partitions_of_table("users", prefix).unwrap(),
                want,
                "{prefix:?}"
            );
        }
        assert_eq!(r.partitions_of_table("orders", b"x").unwrap(), ["p3"]);
        assert_eq!(r.partitions_of_table("nope", b""), None);
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
                history_epoch: 0,
                replicas: Vec::new(),
            })
        );
        assert_eq!(r.owner("p9"), None);
        let mut on_b: Vec<_> = r.partitions_of("b").collect();
        on_b.sort_unstable();
        assert_eq!(on_b, ["p2", "p3"]);
        assert_eq!(r.partitions_of("c").count(), 0);
    }

    // D331 PR 5: an aborting move's source is before its commit point, so its copies go on
    // serving its reads; only from HANDED_OVER do they stop.
    #[test]
    fn an_aborting_moves_source_keeps_its_copies_serving_reads() {
        use crate::serve::proto::{MoveKind, MovePhase, RangeMove};
        let at = |phase: MovePhase| {
            let mut m = map();
            m.partitions[0].replicas = vec!["b".into()];
            let mut r = RangeMove {
                id: "mv1".into(),
                kind: MoveKind::Range as i32,
                table: "users".into(),
                start: Some(text("a")),
                end: Some(text("c")),
                from: "p1".into(),
                to: "p2".into(),
                ..Default::default()
            };
            r.set_phase(phase);
            m.moves.push(r);
            Routes::new(&m)
        };
        assert!(
            !at(MovePhase::Aborting)
                .owner("p1")
                .unwrap()
                .replicas
                .is_empty()
        );
        assert!(
            !at(MovePhase::HandingOver)
                .owner("p1")
                .unwrap()
                .replicas
                .is_empty()
        );
        assert!(
            at(MovePhase::HandedOver)
                .owner("p1")
                .unwrap()
                .replicas
                .is_empty()
        );
    }
}
