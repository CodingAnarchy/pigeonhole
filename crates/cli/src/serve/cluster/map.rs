//! The partition map (D317 §1, ICR 0117): what makes a map valid, which changes one map may
//! make to another, and its canonical form.
//!
//! **Two layers with different rules.** Which partition holds a key range (`tables`) is
//! fixed once the range holds data: a change may add placements, never alter one. Which node
//! serves a partition (`partitions`) changes by relocation, and only with a higher epoch.
//! Everything here is pure, so the server, `phdb cluster` and the tests share it.

use std::collections::{HashMap, HashSet};
use std::net::ToSocketAddrs;

use crate::serve::proto::{ClusterMap, SplitKey, split_key};

/// The reserved table-name prefix: the cluster's own tables (`__phdb_cluster`), hidden from
/// and refused to user calls.
pub(crate) const RESERVED_PREFIX: &str = "__phdb_";

/// Why a map, or a change from one map to another, is refused: the operator reads it.
pub(crate) type Refusal = String;

/// A split key's bytes: `text` and `raw` forms of one key compare equal.
pub(crate) fn split_key_bytes(k: &SplitKey) -> Result<Vec<u8>, Refusal> {
    match &k.key {
        Some(split_key::Key::Text(t)) => Ok(t.as_bytes().to_vec()),
        Some(split_key::Key::Raw(b)) => Ok(b.to_vec()),
        None => Err("a split key has neither `text` nor `raw`".into()),
    }
}

/// Whether `id` may name a node or a partition: a partition id is also its file's name
/// (`<id>.phdb`), so ids are `[a-z0-9_-]`, 1 to 64 characters, not starting with `.` (which
/// the set excludes) or `-`.
pub(crate) fn id_ok(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && !id.starts_with('-')
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

/// Checks `map` on its own. `loopback_only`: the node dials its peers without TLS, so every
/// node it would dial must be on loopback, or its cluster token would cross a network in
/// the clear (cli692's M2 and R3 on D317 ticket 1).
pub(crate) fn validate(map: &ClusterMap, loopback_only: bool) -> Result<(), Refusal> {
    if !id_ok(&map.cluster_id) {
        return Err(format!(
            "cluster_id {:?} must be 1-64 of [a-z0-9_-]",
            map.cluster_id
        ));
    }
    if map.map_version == 0 {
        return Err("map_version must be at least 1".into());
    }
    let mut nodes = HashSet::new();
    for n in &map.nodes {
        if !id_ok(&n.id) {
            return Err(format!("node id {:?} must be 1-64 of [a-z0-9_-]", n.id));
        }
        if !nodes.insert(n.id.as_str()) {
            return Err(format!("node {:?} is listed twice", n.id));
        }
        if n.address.is_empty() {
            return Err(format!("node {:?} has no address", n.id));
        }
        if loopback_only && !loopback(&n.address) {
            return Err(format!(
                "node {:?}'s address {} isn't loopback: a node without TLS dials only nodes \
                 on this machine (add --tls-cert and --tls-key)",
                n.id, n.address
            ));
        }
    }
    let mut partitions = HashMap::new();
    for p in &map.partitions {
        if !id_ok(&p.id) {
            return Err(format!(
                "partition id {:?} must be 1-64 of [a-z0-9_-]",
                p.id
            ));
        }
        if !nodes.contains(p.node.as_str()) {
            return Err(format!(
                "partition {:?} is on node {:?}, which the map doesn't list",
                p.id, p.node
            ));
        }
        if p.epoch == 0 {
            return Err(format!("partition {:?}'s epoch must be at least 1", p.id));
        }
        if p.history_epoch > p.epoch {
            return Err(format!(
                "partition {:?}'s history_epoch {} is past its epoch {}",
                p.id, p.history_epoch, p.epoch
            ));
        }
        if partitions.insert(p.id.as_str(), p).is_some() {
            return Err(format!("partition {:?} is listed twice", p.id));
        }
    }
    let mut tables = HashSet::new();
    // A co-location group's single partition, and the table that set it.
    let mut groups: HashMap<&str, (&str, &str)> = HashMap::new();
    for t in &map.tables {
        if t.name.is_empty() {
            return Err("a table placement has no name".into());
        }
        if t.name.starts_with(RESERVED_PREFIX) {
            return Err(format!(
                "table {:?}: the {RESERVED_PREFIX} prefix is reserved",
                t.name
            ));
        }
        if !tables.insert(t.name.as_str()) {
            return Err(format!("table {:?} is placed twice", t.name));
        }
        let mut last: Option<Vec<u8>> = None;
        for k in &t.splits {
            let k = split_key_bytes(k).map_err(|e| format!("table {:?}: {e}", t.name))?;
            if k.is_empty() {
                return Err(format!(
                    "table {:?}: a split key is empty (the first range starts there already)",
                    t.name
                ));
            }
            if last.as_ref().is_some_and(|l| *l >= k) {
                return Err(format!(
                    "table {:?}: split keys must be distinct and in increasing order",
                    t.name
                ));
            }
            last = Some(k);
        }
        if t.partitions.len() != t.splits.len() + 1 {
            return Err(format!(
                "table {:?}: {} split key(s) make {} ranges, but {} partition(s) are given",
                t.name,
                t.splits.len(),
                t.splits.len() + 1,
                t.partitions.len()
            ));
        }
        for p in &t.partitions {
            if !partitions.contains_key(p.as_str()) {
                return Err(format!(
                    "table {:?} is placed on partition {:?}, which the map doesn't list",
                    t.name, p
                ));
            }
        }
        if !t.group.is_empty() {
            if !t.splits.is_empty() {
                return Err(format!(
                    "table {:?} is in co-location group {:?}, so it can't be split",
                    t.name, t.group
                ));
            }
            let p = t.partitions[0].as_str();
            match groups.get(t.group.as_str()) {
                Some(&(q, first)) if q != p => {
                    return Err(format!(
                        "co-location group {:?}: table {:?} is on partition {:?}, but {:?} is on {:?}",
                        t.group, t.name, p, first, q
                    ));
                }
                Some(_) => {}
                None => {
                    groups.insert(t.group.as_str(), (p, t.name.as_str()));
                }
            }
        }
        for f in &t.families {
            crate::serve::convert::family_in(f)
                .map_err(|e| format!("table {:?}, family {:?}: {}", t.name, f.name, e.message))?;
        }
    }
    Ok(())
}

/// Checks that a node at `old` may take `new` (valid) from a peer, whatever happened in the
/// versions between them: the same cluster and a newer version, nothing more. The change
/// rules ([`validate_change`]) hold one step at a time, not across a jump: a table dropped
/// in one version and created again with other splits in a later one, or a partition
/// dropped and added back at a lower epoch, would refuse a node that missed both steps,
/// for good (cli692's R1). They're enforced where a map is set (`SetMap`), against the
/// setter's current map; a pushed or pulled map is one that passed them there.
pub(crate) fn validate_catch_up(old: &ClusterMap, new: &ClusterMap) -> Result<(), Refusal> {
    if new.cluster_id != old.cluster_id {
        return Err(format!(
            "the map is for cluster {:?}, but this node is in cluster {:?}",
            new.cluster_id, old.cluster_id
        ));
    }
    if new.map_version <= old.map_version {
        return Err(format!(
            "map_version {} isn't newer than this node's {}",
            new.map_version, old.map_version
        ));
    }
    Ok(())
}

/// Checks that `new` may replace `old` (both valid) as one step, where a map is set: the
/// same cluster, a newer version, and only the changes D317 §1 allows. Placements are fixed
/// once written (re-splitting a range that holds data needs range moves, #1499); a
/// partition moves only with a higher epoch.
pub(crate) fn validate_change(old: &ClusterMap, new: &ClusterMap) -> Result<(), Refusal> {
    validate_catch_up(old, new)?;
    let new_tables: HashMap<&str, _> = new.tables.iter().map(|t| (t.name.as_str(), t)).collect();
    let mut used = HashSet::new();
    for t in &old.tables {
        let Some(n) = new_tables.get(t.name.as_str()) else {
            // Dropping a table is a schema change (ticket 5); its partitions stay.
            continue;
        };
        let keys = |t: &crate::serve::proto::TablePlacement| -> Vec<Vec<u8>> {
            t.splits
                .iter()
                .map(|k| split_key_bytes(k).unwrap_or_default())
                .collect()
        };
        if keys(t) != keys(n) || t.partitions != n.partitions {
            return Err(format!(
                "table {:?}'s split keys or partitions changed: a placement is fixed once \
                 written (moving ranges is later work, #1499)",
                t.name
            ));
        }
        if t.group != n.group {
            return Err(format!("table {:?}'s co-location group changed", t.name));
        }
        // D317 ticket 5: a table's incarnation and its families are fixed; families may only
        // be added (options are fixed at creation).
        if t.created != n.created {
            return Err(format!(
                "table {:?}'s created version changed: drop the table, then create it again",
                t.name
            ));
        }
        for f in &t.families {
            match n.families.iter().find(|g| g.name == f.name) {
                None => {
                    return Err(format!(
                        "table {:?}'s family {:?} went away: families are only added",
                        t.name, f.name
                    ));
                }
                Some(g) if g != f => {
                    return Err(format!(
                        "table {:?}'s family {:?} changed: a family's options are fixed at creation",
                        t.name, f.name
                    ));
                }
                Some(_) => {}
            }
        }
        used.extend(t.partitions.iter().map(String::as_str));
    }
    let new_partitions: HashMap<&str, _> =
        new.partitions.iter().map(|p| (p.id.as_str(), p)).collect();
    for p in &old.partitions {
        if !new_partitions.contains_key(p.id.as_str()) && used.contains(p.id.as_str()) {
            return Err(format!(
                "partition {:?} holds a table placement and can't be dropped",
                p.id
            ));
        }
    }
    validate_epochs(old, new)
}

/// Checks each partition `old` and `new` both have: its epoch never goes back, and it moves
/// to another node only with a higher epoch (D317 §2). So no epoch, and no history begun at
/// one (ticket 6), is used twice. Part of [`validate_change`]; a forced map (amendment 1)
/// passes it too, against the receiving node's map, and `phdb cluster set-map --force`
/// against every node's it reaches.
pub(crate) fn validate_epochs(old: &ClusterMap, new: &ClusterMap) -> Result<(), Refusal> {
    let new_partitions: HashMap<&str, _> =
        new.partitions.iter().map(|p| (p.id.as_str(), p)).collect();
    for p in &old.partitions {
        let Some(n) = new_partitions.get(p.id.as_str()) else {
            continue;
        };
        if n.epoch < p.epoch {
            return Err(format!(
                "partition {:?}'s epoch went back from {} to {}",
                p.id, p.epoch, n.epoch
            ));
        }
        if n.node != p.node && n.epoch == p.epoch {
            return Err(format!(
                "partition {:?} moved from node {:?} to {:?} without a new epoch \
                 (relocate it: D317 §2)",
                p.id, p.node, n.node
            ));
        }
    }
    Ok(())
}

/// `new`, a map being set over `old`, with each partition's `history_epoch` derived (D317
/// ticket 6): the one place it's decided, so a map built by hand or from an older one can't
/// carry a history across a relocation that broke it.
/// - **An epoch kept:** `old`'s, whatever `new` says.
/// - **An epoch raised:** the new epoch (a promotion or a restore starts another history),
///   unless `copies` names the partition (`relocate --copy`): then `old`'s, since its files
///   were copied whole. A copy must move to another node at exactly `old`'s epoch + 1.
/// - **A new partition:** its epoch. So a first map, derived over an empty `old`, starts
///   every history at its epoch.
///
/// Run after [`validate_change`] has passed the step, or, for a forced map, after
/// [`validate_epochs`].
pub(crate) fn with_history(
    old: &ClusterMap,
    new: &ClusterMap,
    copies: &[String],
) -> Result<ClusterMap, Refusal> {
    let before: HashMap<&str, _> = old.partitions.iter().map(|p| (p.id.as_str(), p)).collect();
    for c in copies {
        let moved = new.partitions.iter().find(|p| &p.id == c);
        match (before.get(c.as_str()), moved) {
            (Some(o), Some(n)) if n.epoch == o.epoch + 1 && n.node != o.node => {}
            _ => {
                return Err(format!(
                    "partition {c:?} is marked as copied, but this change doesn't move it to \
                     another node at its epoch + 1"
                ));
            }
        }
    }
    let mut out = new.clone();
    for p in &mut out.partitions {
        p.history_epoch = match before.get(p.id.as_str()) {
            Some(o) if p.epoch == o.epoch => o.history_epoch,
            Some(o) if copies.contains(&p.id) => o.history_epoch,
            _ => p.epoch,
        };
    }
    Ok(out)
}

/// The map with each split key in its canonical form: `text` when the bytes are UTF-8,
/// else `raw`. Two maps that say the same thing are equal after it.
pub(crate) fn canonical(map: &ClusterMap) -> ClusterMap {
    let mut m = map.clone();
    for t in &mut m.tables {
        for k in &mut t.splits {
            if let Ok(b) = split_key_bytes(k) {
                k.key = Some(match String::from_utf8(b) {
                    Ok(s) => split_key::Key::Text(s),
                    Err(e) => split_key::Key::Raw(e.into_bytes().into()),
                });
            }
        }
    }
    m
}

/// The map's canonical JSON: the map file's contents, `Cluster.GetMap`'s HTTP answer, and
/// what an equal-version comparison compares (cli692's M3).
pub(crate) fn canonical_json(map: &ClusterMap) -> String {
    let mut s = serde_json::to_string_pretty(&canonical(map)).expect("a map serializes");
    s.push('\n');
    s
}

/// Reads a map file's JSON (either split-key form).
pub(crate) fn parse_json(json: &str) -> Result<ClusterMap, Refusal> {
    serde_json::from_str(json).map_err(|e| format!("not a cluster map: {e}"))
}

/// Whether `address` (`host:port`) resolves only to loopback addresses.
fn loopback(address: &str) -> bool {
    match address.to_socket_addrs() {
        Ok(addrs) => {
            let addrs: Vec<_> = addrs.collect();
            !addrs.is_empty() && addrs.iter().all(|a| a.ip().is_loopback())
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::{Node, Partition, TablePlacement};

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

    /// Two nodes, three partitions; `users` split at "m" over p1 and p2, `orders` and its
    /// index table co-located on p3.
    fn map() -> ClusterMap {
        let node = |id: &str, port: u16| Node {
            id: id.into(),
            address: format!("127.0.0.1:{port}"),
        };
        let part = |id: &str, node: &str| Partition {
            history_epoch: 0,
            id: id.into(),
            node: node.into(),
            epoch: 1,
            memory_bytes: 0,
        };
        let table =
            |name: &str, group: &str, splits: Vec<SplitKey>, parts: &[&str]| TablePlacement {
                name: name.into(),
                group: group.into(),
                families: Vec::new(),
                splits,
                partitions: parts.iter().map(|p| (*p).into()).collect(),
                created: 0,
            };
        ClusterMap {
            cluster_id: "c1".into(),
            map_version: 1,
            nodes: vec![node("a", 7001), node("b", 7002)],
            partitions: vec![part("p1", "a"), part("p2", "b"), part("p3", "b")],
            tables: vec![
                table("users", "", vec![text("m")], &["p1", "p2"]),
                table("orders", "o", vec![], &["p3"]),
                table("orders_idx", "o", vec![], &["p3"]),
            ],
        }
    }

    fn refused(r: Result<(), Refusal>, says: &str) {
        let e = r.expect_err("refused");
        assert!(e.contains(says), "{e:?} doesn't say {says:?}");
    }

    #[test]
    fn a_valid_map_passes_and_its_json_round_trips() {
        let m = map();
        validate(&m, true).unwrap();
        let back = parse_json(&canonical_json(&m)).unwrap();
        assert_eq!(back, canonical(&m));
    }

    #[test]
    fn ids_are_file_name_safe() {
        for ok in ["p1", "a", "east_2-b", &"x".repeat(64)] {
            assert!(id_ok(ok), "{ok}");
        }
        for bad in [
            "",
            "P1",
            "../x",
            ".p",
            "-p",
            "p.1",
            "p 1",
            "con:",
            &"x".repeat(65),
        ] {
            assert!(!id_ok(bad), "{bad}");
        }
        let mut m = map();
        m.partitions[0].id = "../x".into();
        refused(validate(&m, true), "partition id");
        let mut m = map();
        m.nodes[0].id = "A".into();
        refused(validate(&m, true), "node id");
    }

    #[test]
    fn references_versions_and_epochs_are_checked() {
        let mut m = map();
        m.partitions[0].node = "zz".into();
        refused(validate(&m, true), "doesn't list");
        let mut m = map();
        m.tables[0].partitions[1] = "p9".into();
        refused(validate(&m, true), "doesn't list");
        let mut m = map();
        m.map_version = 0;
        refused(validate(&m, true), "map_version");
        let mut m = map();
        m.partitions[1].epoch = 0;
        refused(validate(&m, true), "epoch");
        let mut m = map();
        m.nodes.push(m.nodes[0].clone());
        refused(validate(&m, true), "twice");
        let mut m = map();
        m.tables.push(m.tables[0].clone());
        refused(validate(&m, true), "placed twice");
    }

    #[test]
    fn split_keys_are_increasing_and_match_the_partitions() {
        let mut m = map();
        m.tables[0].splits = vec![text("m"), text("c")];
        m.tables[0].partitions.push("p1".into());
        refused(validate(&m, true), "increasing");
        let mut m = map();
        // "m" as text and as raw bytes is one key twice.
        m.tables[0].splits = vec![text("m"), raw(b"m")];
        m.tables[0].partitions.push("p1".into());
        refused(validate(&m, true), "distinct");
        let mut m = map();
        m.tables[0].splits = vec![text("")];
        refused(validate(&m, true), "empty");
        let mut m = map();
        m.tables[0].partitions.pop();
        refused(validate(&m, true), "ranges");
        let mut m = map();
        m.tables[0].splits = vec![SplitKey { key: None }];
        refused(validate(&m, true), "neither");
        // Non-UTF-8 raw keys are fine, and the same partition may hold several ranges.
        let mut m = map();
        m.tables[0].splits = vec![raw(&[0xff, 0x00]), raw(&[0xff, 0x01])];
        m.tables[0].partitions = vec!["p1".into(), "p2".into(), "p1".into()];
        validate(&m, true).unwrap();
    }

    #[test]
    fn groups_are_unsplit_and_on_one_partition_and_the_prefix_is_reserved() {
        let mut m = map();
        m.tables[2].partitions = vec!["p1".into()];
        refused(validate(&m, true), "co-location group");
        let mut m = map();
        m.tables[1].splits = vec![text("k")];
        m.tables[1].partitions.push("p3".into());
        refused(validate(&m, true), "can't be split");
        let mut m = map();
        m.tables[0].name = "__phdb_cluster".into();
        refused(validate(&m, true), "reserved");
    }

    #[test]
    fn plaintext_clusters_dial_only_loopback() {
        let mut m = map();
        m.nodes[1].address = "192.0.2.7:7002".into();
        refused(validate(&m, true), "isn't loopback");
        validate(&m, false).unwrap();
        m.nodes[1].address = "localhost:7002".into();
        validate(&m, true).unwrap();
    }

    #[test]
    fn a_change_may_add_and_relocate_but_not_reshape() {
        let old = map();
        let next = || {
            let mut m = map();
            m.map_version = 2;
            m
        };
        // Adding a node, a partition and a table placement; relocating with a new epoch.
        let mut ok = next();
        ok.nodes.push(Node {
            id: "c".into(),
            address: "127.0.0.1:7003".into(),
        });
        ok.partitions.push(Partition {
            history_epoch: 0,
            id: "p4".into(),
            node: "c".into(),
            epoch: 1,
            memory_bytes: 0,
        });
        ok.partitions[0].node = "c".into();
        ok.partitions[0].epoch = 2;
        ok.tables.push(TablePlacement {
            name: "events".into(),
            group: String::new(),
            families: Vec::new(),
            splits: Vec::new(),
            partitions: vec!["p4".into()],
            created: 0,
        });
        validate(&ok, true).unwrap();
        validate_change(&old, &ok).unwrap();
        // A split key in the other form is the same placement.
        let mut same = next();
        same.tables[0].splits = vec![raw(b"m")];
        validate_change(&old, &same).unwrap();

        refused(validate_change(&old, &map()), "isn't newer");
        let mut m = next();
        m.cluster_id = "c2".into();
        refused(validate_change(&old, &m), "cluster");
        let mut m = next();
        m.tables[0].splits = vec![text("n")];
        refused(validate_change(&old, &m), "fixed once written");
        let mut m = next();
        m.tables[0].partitions = vec!["p2".into(), "p1".into()];
        refused(validate_change(&old, &m), "fixed once written");
        let mut m = next();
        m.tables[2].group = String::new();
        refused(validate_change(&old, &m), "group");
        let mut m = next();
        m.partitions[0].node = "b".into();
        refused(validate_change(&old, &m), "without a new epoch");
        let mut m = next();
        let mut old2 = map();
        old2.partitions[0].epoch = 3;
        m.partitions[0].epoch = 2;
        refused(validate_change(&old2, &m), "went back");
        let mut m = next();
        m.partitions.retain(|p| p.id != "p3");
        refused(validate_change(&old, &m), "can't be dropped");
    }

    #[test]
    fn a_node_catching_up_checks_only_the_cluster_and_the_order() {
        // v2 dropped `users` and p3, v3 created `users` again with other splits and added
        // p3 back at a lower epoch: each step was a valid change, the jump isn't one, and a
        // node at v1 must still reach v3 (cli692's R1).
        let mut old = map();
        old.partitions[2].epoch = 5;
        let mut v3 = map();
        v3.map_version = 3;
        v3.tables[0].splits = vec![text("q")];
        validate(&v3, true).unwrap();
        refused(validate_change(&old, &v3), "fixed once written");
        validate_catch_up(&old, &v3).unwrap();
        refused(validate_catch_up(&old, &map()), "isn't newer");
        let mut other = v3.clone();
        other.cluster_id = "c2".into();
        refused(validate_catch_up(&old, &other), "cluster");
    }

    #[test]
    fn a_partitions_history_is_kept_by_a_copy_and_restarted_by_any_other_relocation() {
        // D317 ticket 6: `history_epoch` is derived where a map is set, never taken from it.
        let mut old = map();
        old.partitions[0].epoch = 3;
        old.partitions[0].history_epoch = 2;
        let history = |m: &ClusterMap, id: &str| {
            m.partitions
                .iter()
                .find(|p| p.id == id)
                .map(|p| (p.epoch, p.history_epoch))
                .unwrap()
        };
        let mut next = old.clone();
        next.map_version = 2;
        // Whatever the new map says, an unchanged epoch keeps the old history.
        next.partitions[0].history_epoch = 0;
        next.partitions[1].history_epoch = 1;
        let kept = with_history(&old, &next, &[]).unwrap();
        assert_eq!(history(&kept, "p1"), (3, 2));
        assert_eq!(history(&kept, "p2"), (1, 0));

        // A relocation by copy to another node at epoch + 1 keeps it.
        let mut moved = next.clone();
        moved.partitions[0].node = "b".into();
        moved.partitions[0].epoch = 4;
        let copied = with_history(&old, &moved, &["p1".to_owned()]).unwrap();
        assert_eq!(history(&copied, "p1"), (4, 2));
        // The same move unmarked (a promotion, a restore) starts another history, even if
        // the map it came from claims the old one.
        moved.partitions[0].history_epoch = 2;
        let promoted = with_history(&old, &moved, &[]).unwrap();
        assert_eq!(history(&promoted, "p1"), (4, 4));
        // A new partition's history starts at its epoch.
        moved.partitions.push(Partition {
            id: "p4".into(),
            node: "a".into(),
            epoch: 5,
            memory_bytes: 0,
            history_epoch: 0,
        });
        assert_eq!(
            history(&with_history(&old, &moved, &[]).unwrap(), "p4"),
            (5, 5)
        );

        // The copy mark applies only to a move to another node at exactly epoch + 1.
        let mut far = next.clone();
        far.partitions[0].node = "b".into();
        far.partitions[0].epoch = 5;
        let mut same_node = next.clone();
        same_node.partitions[0].epoch = 4;
        for (bad, id) in [
            (&far, "p1"),
            (&same_node, "p1"),
            (&next, "p1"),
            (&next, "p9"),
        ] {
            let e = with_history(&old, bad, &[id.to_owned()]).unwrap_err();
            assert!(e.contains("marked as copied"), "{e}");
        }
    }

    #[test]
    fn a_history_can_t_begin_after_its_epoch() {
        let mut m = map();
        m.partitions[0].history_epoch = 2;
        refused(validate(&m, true), "past its epoch");
        m.partitions[0].epoch = 2;
        validate(&m, true).unwrap();
    }

    #[test]
    fn canonical_json_prints_text_keys_and_raw_bytes() {
        let mut m = map();
        m.tables[0].splits = vec![raw(b"m")];
        let json = canonical_json(&m);
        assert!(json.contains("\"text\": \"m\""), "{json}");
        m.tables[0].splits = vec![raw(&[0xff])];
        let json = canonical_json(&m);
        assert!(json.contains("\"raw\""), "{json}");
        // Equal maps in either form have equal canonical JSON.
        let mut a = map();
        a.tables[0].splits = vec![raw(b"m")];
        assert_eq!(canonical_json(&a), canonical_json(&map()));
    }
}
