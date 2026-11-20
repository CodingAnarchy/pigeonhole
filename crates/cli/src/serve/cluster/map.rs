//! The partition map (D317 §1, ICR 0117): what makes a map valid, which changes one map may
//! make to another, and its canonical form.
//!
//! **Two layers with different rules.** Which partition holds a key range (`tables`) is
//! fixed once the range holds data: a change may add placements, never alter one. Which node
//! serves a partition (`partitions`) changes by relocation, and only with a higher epoch.
//! Everything here is pure, so the server, `phdb cluster` and the tests share it.

use std::collections::{HashMap, HashSet};
use std::net::ToSocketAddrs;

use crate::serve::proto::{ClusterMap, MovePhase, RangeMove, SplitKey, TablePlacement, split_key};

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
    validate_failover_fields(map)?;
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
    // D330: a named writer is one of the map's nodes, and only an elected map (term 1 on)
    // names one.
    if (map.writer_term == 0) != map.writer.is_empty() {
        return Err(format!(
            "writer {:?} at writer_term {}: a map names its writer from term 1 on, and only \
             then",
            map.writer, map.writer_term
        ));
    }
    if !map.writer.is_empty() && !nodes.contains(map.writer.as_str()) {
        return Err(format!(
            "the map's writer {:?} isn't one of its nodes",
            map.writer
        ));
    }
    let mut partitions = HashMap::new();
    for p in &map.partitions {
        if !id_ok(&p.id) {
            return Err(format!(
                "partition id {:?} must be 1-64 of [a-z0-9_-]",
                p.id
            ));
        }
        if p.id == super::lease::WRITER_LEASE {
            return Err(format!(
                "partition id {:?} is reserved: it names the map writer's lease (D330)",
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
        // D328: its replicas are other nodes of the map, each once. A map that drops a node
        // must drop it from every partition's replicas too.
        let mut replicas = HashSet::new();
        for r in &p.replicas {
            if !nodes.contains(r.as_str()) {
                return Err(format!(
                    "partition {:?} has a replica on node {:?}, which the map doesn't list \
                     (a map that drops a node drops its replicas too)",
                    p.id, r
                ));
            }
            if *r == p.node {
                return Err(format!(
                    "partition {:?}'s owner, node {:?}, is listed as its replica too",
                    p.id, r
                ));
            }
            if !replicas.insert(r.as_str()) {
                return Err(format!(
                    "partition {:?} lists node {:?} as its replica twice",
                    p.id, r
                ));
            }
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
    if let Some(m) = &map.memory {
        validate_memory(m, &partitions, &tables)?;
    }
    validate_indexes(map, &partitions, &tables)?;
    validate_moves(map)?;
    Ok(())
}

/// A move's phase, `Unspecified` for a value this build doesn't know.
pub(crate) fn phase(m: &RangeMove) -> MovePhase {
    MovePhase::try_from(m.phase).unwrap_or(MovePhase::Unspecified)
}

/// D331: a move's range as bytes, `[start, end)`: an absent start is the table's first key
/// (the empty key), an absent end the table's end.
pub(crate) fn move_range(m: &RangeMove) -> Result<(Vec<u8>, Option<Vec<u8>>), Refusal> {
    let start = m.start.as_ref().map(split_key_bytes).transpose()?;
    let end = m.end.as_ref().map(split_key_bytes).transpose()?;
    Ok((start.unwrap_or_default(), end))
}

/// One range of a table, `(start, end, partition)`: `end` `None` for the last.
pub(crate) type PlacedRange<'t> = (Vec<u8>, Option<Vec<u8>>, &'t str);

/// A table's ranges in key order.
pub(crate) fn table_ranges(t: &TablePlacement) -> Vec<PlacedRange<'_>> {
    let keys: Vec<Vec<u8>> = t
        .splits
        .iter()
        .map(|k| split_key_bytes(k).unwrap_or_default())
        .collect();
    t.partitions
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let start = if i == 0 {
                Vec::new()
            } else {
                keys[i - 1].clone()
            };
            (start, keys.get(i).cloned(), p.as_str())
        })
        .collect()
}

/// Whether `[start, end)` lies inside one range of `t` that partition `p` holds.
pub(crate) fn inside(t: &TablePlacement, start: &[u8], end: Option<&[u8]>, p: &str) -> bool {
    table_ranges(t).iter().any(|(lo, hi, q)| {
        *q == p
            && lo.as_slice() <= start
            && match (end, hi) {
                (_, None) => true,
                (None, Some(_)) => false,
                (Some(e), Some(h)) => e <= h.as_slice(),
            }
    })
}

/// The indexes that follow `table` (#1722): a range of such a table can't move by key (a
/// text index's postings and a vector index's lists hold ids from both sides of any range),
/// so D331 refuses to move it in 1.3.0 (the coordinator, 2026-11-18; #2066 lifts it).
pub(crate) fn followed_by<'m>(map: &'m ClusterMap, table: &str) -> Vec<&'m str> {
    map.indexes
        .iter()
        .filter(|i| i.follows == table)
        .map(|i| i.name.as_str())
        .collect()
}

/// Whether range move `m`'s source, at `epoch` in a map, is still the history its copy
/// began on (cli692 on #2107): `from_epoch` before HANDED_OVER; from HANDED_OVER on, above
/// it, since the hand-over raises the source's epoch (to `max(from_epoch, floor) + 1`, as
/// [`validate_move_steps`] checks), and failover is blocked past the commit point, so the
/// hand-over's is the only raise there. The exact value isn't recomputed: a promote's floor
/// may leave the map once the epoch passed it. A record from before `from_epoch` (0) isn't
/// compared.
pub(crate) fn source_epoch_holds(m: &RangeMove, epoch: u64) -> bool {
    if m.from_epoch == 0 {
        return true;
    }
    if phase(m) >= MovePhase::HandedOver {
        epoch > m.from_epoch
    } else {
        epoch == m.from_epoch
    }
}

/// Whether `m` is a whole partition's relocation (D332 PR 2's `MOVE_KIND_NODE`), not a
/// range move: it gates no range, sends no barrier and hands nothing over, so every reader
/// of the map's moves that acts on a range skips it.
pub(crate) fn node_move(m: &RangeMove) -> bool {
    m.kind == crate::serve::proto::MoveKind::Node as i32
}

/// Checks a NODE move (D332 PR 2): no table or range; a listed partition, relocating to
/// another of the map's nodes; never at CATCHING_UP (nothing to catch up: P is closed on
/// its node while it copies, simp724's Q1). Before HANDED_OVER P is still on its node at
/// `from_epoch`; from it, on `to` past it. So any map that moves P otherwise drops the
/// record (simp724's M2), a failover's included.
fn validate_node_move(map: &ClusterMap, m: &RangeMove) -> Result<(), Refusal> {
    let id = &m.id;
    if !m.table.is_empty() || m.start.is_some() || m.end.is_some() {
        return Err(format!(
            "move {id:?} relocates a whole partition: it names no table or range"
        ));
    }
    let Some(p) = map.partitions.iter().find(|p| p.id == m.from) else {
        return Err(format!(
            "move {id:?} names partition {:?}, which the map doesn't list",
            m.from
        ));
    };
    if !map.nodes.iter().any(|n| n.id == m.to) {
        return Err(format!(
            "move {id:?} relocates {:?} to node {:?}, which the map doesn't list",
            m.from, m.to
        ));
    }
    let phase = phase(m);
    if phase == MovePhase::CatchingUp {
        return Err(format!(
            "move {id:?} relocates a whole partition, which has no CATCHING_UP phase"
        ));
    }
    if m.from_epoch == 0 || m.snapshot_seqno != 0 || m.final_seqno != 0 {
        return Err(format!(
            "move {id:?} relocates a whole partition: it records its epoch at COPYING \
             (from_epoch), and no seqnos"
        ));
    }
    let handed = phase >= MovePhase::HandedOver;
    let at = if handed {
        p.node == m.to && p.epoch > m.from_epoch
    } else {
        p.node != m.to && p.epoch == m.from_epoch
    };
    if !at {
        return Err(format!(
            "move {id:?} relocates {:?} from epoch {} to node {:?}, but the map has it on node \
             {:?} at epoch {}: {} (a map that moves the partition otherwise drops the record)",
            m.from,
            m.from_epoch,
            m.to,
            p.node,
            p.epoch,
            if handed {
                "from HANDED_OVER it's on the target past that epoch"
            } else {
                "before HANDED_OVER it's on its node at that epoch"
            }
        ));
    }
    let pair = (m.handed_over_term, m.handed_over_version);
    if handed != (pair != (0, 0)) || pair > (map.writer_term, map.map_version) {
        return Err(format!(
            "move {id:?}'s handed-over map (term {}, version {}) must be set from HANDED_OVER \
             on, only then, and not after this map",
            m.handed_over_term, m.handed_over_version
        ));
    }
    Ok(())
}

/// Checks the range moves (D331): each well formed, on a placed, ungrouped table no index
/// follows, between two listed partitions, inside a range of its source before the
/// hand-over and of its target from it; and each partition in at most one move, a NODE
/// move's included ([`validate_node_move`]).
fn validate_moves(map: &ClusterMap) -> Result<(), Refusal> {
    let tables: HashMap<&str, &TablePlacement> =
        map.tables.iter().map(|t| (t.name.as_str(), t)).collect();
    let partitions: HashSet<&str> = map.partitions.iter().map(|p| p.id.as_str()).collect();
    let mut ids = HashSet::new();
    let mut busy: HashMap<&str, &str> = HashMap::new();
    for m in &map.moves {
        if !id_ok(&m.id) {
            return Err(format!("move id {:?} must be 1-64 of [a-z0-9_-]", m.id));
        }
        if !ids.insert(m.id.as_str()) {
            return Err(format!("move {:?} is listed twice", m.id));
        }
        let id = &m.id;
        let phase = phase(m);
        if phase == MovePhase::Unspecified {
            return Err(format!("move {id:?} has no phase this build knows"));
        }
        if node_move(m) {
            validate_node_move(map, m)?;
            if let Some(other) = busy.insert(m.from.as_str(), id.as_str()) {
                return Err(format!(
                    "partition {:?} is in moves {other:?} and {id:?}: a partition is in one \
                     move at a time",
                    m.from
                ));
            }
            continue;
        }
        if !matches!(
            crate::serve::proto::MoveKind::try_from(m.kind),
            Ok(crate::serve::proto::MoveKind::Range | crate::serve::proto::MoveKind::Partition)
        ) {
            return Err(format!("move {id:?} has no kind this build knows"));
        }
        let Some(t) = tables.get(m.table.as_str()) else {
            return Err(format!(
                "move {id:?} is of table {:?}, which the map doesn't place",
                m.table
            ));
        };
        if !t.group.is_empty() {
            return Err(format!(
                "move {id:?}: table {:?} is in co-location group {:?}, which moves only whole \
                 (relocate its partition instead)",
                m.table, t.group
            ));
        }
        if let Some(i) = followed_by(map, &m.table).first() {
            return Err(format!(
                "move {id:?}: index {i:?} follows table {:?}, and a range of a table an index \
                 follows can't move in this version (#2066): drop the index, or move the \
                 whole partition",
                m.table
            ));
        }
        if m.from == m.to {
            return Err(format!(
                "move {id:?} is from partition {:?} to itself",
                m.from
            ));
        }
        for p in [&m.from, &m.to] {
            if !partitions.contains(p.as_str()) {
                return Err(format!(
                    "move {id:?} names partition {p:?}, which the map doesn't list"
                ));
            }
            if let Some(other) = busy.insert(p.as_str(), id.as_str()) {
                return Err(format!(
                    "partition {p:?} is in moves {other:?} and {id:?}: a partition is in one \
                     move at a time"
                ));
            }
        }
        let (start, end) = move_range(m).map_err(|e| format!("move {id:?}: {e}"))?;
        if end.as_ref().is_some_and(|e| *e <= start) {
            return Err(format!(
                "move {id:?}'s range is empty: its end isn't after its start"
            ));
        }
        let handed = phase >= MovePhase::HandedOver;
        let holder = if handed { &m.to } else { &m.from };
        if !inside(t, &start, end.as_deref(), holder) {
            return Err(format!(
                "move {id:?}'s range of table {:?} isn't inside one range partition {holder:?} \
                 holds{}",
                m.table,
                if handed {
                    " (from HANDED_OVER, the target holds it)"
                } else {
                    " (before HANDED_OVER, the source holds it)"
                }
            ));
        }
        if m.final_seqno != 0 && phase < MovePhase::HandingOver {
            return Err(format!("move {id:?} has a final_seqno before HANDING_OVER"));
        }
        let pair = (m.handed_over_term, m.handed_over_version);
        if handed != (pair != (0, 0)) || pair > (map.writer_term, map.map_version) {
            return Err(format!(
                "move {id:?}'s handed-over map (term {}, version {}) must be set from \
                 HANDED_OVER on, only then, and not after this map",
                m.handed_over_term, m.handed_over_version
            ));
        }
    }
    Ok(())
}

/// `t` with `[start, end)` given to partition `to` (D331's hand-over): the range's ends
/// become split keys where they aren't, and every range inside it goes to `to`.
pub(crate) fn handed_over(
    t: &TablePlacement,
    start: &[u8],
    end: Option<&[u8]>,
    to: &str,
) -> TablePlacement {
    // Each range's start: its split key as the table has it (none for the first), the
    // move's new ones in canonical form (cli692's N3).
    let mut cuts: Vec<(Vec<u8>, Option<SplitKey>, String)> = Vec::new();
    for (i, (lo, hi, p)) in table_ranges(t).into_iter().enumerate() {
        let own = i.checked_sub(1).map(|j| t.splits[j].clone());
        let mut points = vec![(lo.clone(), own)];
        // Cut this range at the move's ends where they fall strictly inside it.
        for k in [Some(start), end].into_iter().flatten() {
            if k > lo.as_slice() && hi.as_deref().is_none_or(|h| k < h) {
                points.push((k.to_vec(), Some(canonical_key(k))));
            }
        }
        points.sort_by(|x, y| x.0.cmp(&y.0));
        for (pt, key) in points {
            let moved = pt.as_slice() >= start && end.is_none_or(|e| pt.as_slice() < e);
            cuts.push((pt, key, if moved { to.to_owned() } else { p.to_owned() }));
        }
    }
    let mut out = t.clone();
    out.splits = cuts
        .iter()
        .skip(1)
        .filter_map(|(_, k, _)| k.clone())
        .collect();
    out.partitions = cuts.into_iter().map(|(_, _, p)| p).collect();
    out
}

/// A split key for `bytes` in canonical form: `text` when they're UTF-8, else `raw`.
fn canonical_key(bytes: &[u8]) -> SplitKey {
    SplitKey {
        key: Some(match std::str::from_utf8(bytes) {
            Ok(s) => split_key::Key::Text(s.to_owned()),
            Err(_) => split_key::Key::Raw(bytes.to_vec().into()),
        }),
    }
}

/// Checks the indexes' placements (#1722, ICR 0138): each a unique name that no table or
/// memory store table takes, its create request, and either a placed table it follows or
/// its own split keys and partitions (a vector index only: a text index always follows its
/// source table, whose rows `Text.Put` writes beside the postings).
fn validate_indexes(
    map: &ClusterMap,
    partitions: &HashMap<&str, &crate::serve::proto::Partition>,
    tables: &HashSet<&str>,
) -> Result<(), Refusal> {
    use crate::serve::proto::index_placement::Kind;
    let store_tables = map
        .memory
        .as_ref()
        .map(|m| pigeonhole_memory::Store::table_names(&m.store))
        .unwrap_or_default();
    let mut names = HashSet::new();
    for i in &map.indexes {
        let name = i.name.as_str();
        if name.is_empty() {
            return Err("an index placement has no name".into());
        }
        if name.starts_with(RESERVED_PREFIX) {
            return Err(format!(
                "index {name:?}: the {RESERVED_PREFIX} prefix is reserved"
            ));
        }
        if tables.contains(name) || store_tables.iter().any(|t| t == name) {
            return Err(format!(
                "index {name:?} has the name of a table the map places: an index is a table \
                 of its own"
            ));
        }
        if !names.insert(name) {
            return Err(format!("index {name:?} is placed twice"));
        }
        match &i.kind {
            None => return Err(format!("index {name:?} has no create request")),
            Some(Kind::Vector(r)) if r.name != name => {
                return Err(format!(
                    "index {name:?}'s create request names index {:?}",
                    r.name
                ));
            }
            Some(Kind::Text(r)) if r.name != name => {
                return Err(format!(
                    "index {name:?}'s create request names index {:?}",
                    r.name
                ));
            }
            Some(Kind::Text(r)) if i.follows != r.source_table => {
                return Err(format!(
                    "text index {name:?} must follow its source table {:?}: a document's \
                     postings are written beside its source row",
                    r.source_table
                ));
            }
            Some(_) => {}
        }
        if !i.follows.is_empty() {
            if !i.splits.is_empty() || !i.partitions.is_empty() {
                return Err(format!(
                    "index {name:?} follows table {:?}, so it takes that table's ranges and \
                     names no split keys or partitions of its own",
                    i.follows
                ));
            }
            if !tables.contains(i.follows.as_str()) {
                return Err(format!(
                    "index {name:?} follows table {:?}, which the map doesn't place",
                    i.follows
                ));
            }
            continue;
        }
        let mut last: Option<Vec<u8>> = None;
        for k in &i.splits {
            let k = split_key_bytes(k).map_err(|e| format!("index {name:?}: {e}"))?;
            if k.is_empty() {
                return Err(format!(
                    "index {name:?}: a split key is empty (the first range starts there already)"
                ));
            }
            if last.as_ref().is_some_and(|l| *l >= k) {
                return Err(format!(
                    "index {name:?}: split keys must be distinct and in increasing order"
                ));
            }
            last = Some(k);
        }
        if i.partitions.len() != i.splits.len() + 1 {
            return Err(format!(
                "index {name:?}: {} split key(s) make {} ranges, but {} partition(s) are given",
                i.splits.len(),
                i.splits.len() + 1,
                i.partitions.len()
            ));
        }
        if let Some(p) = i
            .partitions
            .iter()
            .find(|p| !partitions.contains_key(p.as_str()))
        {
            return Err(format!(
                "index {name:?} is placed on partition {p:?}, which the map doesn't list"
            ));
        }
    }
    Ok(())
}

/// Checks the memory store's placement (#1915): a store name whose tables no table placement
/// takes, and namespace split keys that, without `\0`, never fall inside a namespace's rows
/// (`{ns}\0…`): for such a key `k`, `k <= ns` exactly when `k <= ns\0…`.
fn validate_memory(
    m: &crate::serve::proto::MemoryPlacement,
    partitions: &HashMap<&str, &crate::serve::proto::Partition>,
    tables: &HashSet<&str>,
) -> Result<(), Refusal> {
    if m.store.is_empty() {
        return Err("the memory placement names no store".into());
    }
    if let Some(t) = pigeonhole_memory::Store::table_names(&m.store)
        .into_iter()
        .find(|t| tables.contains(t.as_str()))
    {
        return Err(format!(
            "table {t:?} is placed as a table, but it's memory store {:?}'s own",
            m.store
        ));
    }
    let mut last: Option<Vec<u8>> = None;
    for k in &m.splits {
        let k = split_key_bytes(k).map_err(|e| format!("memory store {:?}: {e}", m.store))?;
        if k.is_empty() {
            return Err(format!(
                "memory store {:?}: a split key is empty (the first range starts there already)",
                m.store
            ));
        }
        if k.contains(&0) {
            return Err(format!(
                "memory store {:?}: a split key contains \\0, so a namespace's rows could \
                 straddle it (namespaces can't contain \\0)",
                m.store
            ));
        }
        if last.as_ref().is_some_and(|l| *l >= k) {
            return Err(format!(
                "memory store {:?}: split keys must be distinct and in increasing order",
                m.store
            ));
        }
        last = Some(k);
    }
    if m.partitions.len() != m.splits.len() + 1 {
        return Err(format!(
            "memory store {:?}: {} split key(s) make {} ranges, but {} partition(s) are given",
            m.store,
            m.splits.len(),
            m.splits.len() + 1,
            m.partitions.len()
        ));
    }
    if let Some(p) = m
        .partitions
        .iter()
        .find(|p| !partitions.contains_key(p.as_str()))
    {
        return Err(format!(
            "memory store {:?} is placed on partition {p:?}, which the map doesn't list",
            m.store
        ));
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
    // D330: ordered by `(writer_term, map_version)`. A higher term is taken whatever its
    // version: an elected writer's map replaces one the old writer pushed to a minority.
    if (new.writer_term, new.map_version) <= (old.writer_term, old.map_version) {
        return Err(if new.writer_term < old.writer_term {
            format!(
                "the map is at writer term {}, below this node's {}: it's from a writer that \
                 has since been replaced (D330)",
                new.writer_term, old.writer_term
            )
        } else {
            format!(
                "map_version {} isn't newer than this node's {}",
                new.map_version, old.map_version
            )
        });
    }
    validate_pending_failovers(old, new)
}

/// How many done or abandoned failover events a map keeps (D330 §6); pending ones are
/// kept besides, however many.
pub(crate) const FAILOVER_EVENTS: usize = 16;

/// The most bytes of in-doubt xids an event or a lease part carries (simp724 on the D330
/// PR 2 plan's F7); the count stays exact.
pub(crate) const IN_DOUBT_BYTES: usize = 4096;

/// Whether `m` uses any of D330's failover fields: what needs `writer_terms` on every node.
pub(crate) fn uses_failover(m: &ClusterMap) -> bool {
    m.failover
        || !m.failovers.is_empty()
        || m.partitions.iter().any(|p| {
            p.failover() != crate::serve::proto::FailoverMode::Unspecified || p.failover_count > 0
        })
}

/// D330's failover fields in one map: no rejoin (the owner's Q4), at most
/// [`FAILOVER_EVENTS`] finished events, at most one pending promote per partition and
/// copy epoch, and in-doubt lists within [`IN_DOUBT_BYTES`].
fn validate_failover_fields(map: &ClusterMap) -> Result<(), Refusal> {
    use crate::serve::proto::{FailoverKind as K, FailoverState as S};
    if map.failover_rejoin {
        return Err(
            "failover_rejoin isn't supported in this release: an old owner rejoins P as a \
             replica only when the operator adds it to P's replicas (D330, the owner's Q4)"
                .into(),
        );
    }
    let finished = map
        .failovers
        .iter()
        .filter(|e| !super::promote::pinned(map, e))
        .count();
    if finished > FAILOVER_EVENTS {
        return Err(format!(
            "the map records {finished} finished failovers: it keeps the last \
             {FAILOVER_EVENTS}"
        ));
    }
    let mut pending = HashSet::new();
    for e in &map.failovers {
        let bytes: usize = e.in_doubt_xids.iter().map(String::len).sum();
        if bytes > IN_DOUBT_BYTES || (e.in_doubt_xids.len() as u64) > e.in_doubt_count {
            return Err(format!(
                "a failover event of partition {:?} lists {} in-doubt xids ({bytes} bytes) \
                 for a count of {}: at most {IN_DOUBT_BYTES} bytes, and no more than the count",
                e.partition,
                e.in_doubt_xids.len(),
                e.in_doubt_count
            ));
        }
        match (e.kind(), e.state()) {
            (K::Partition, S::Pending) => {
                if !pending.insert((e.partition.as_str(), e.to_epoch)) {
                    return Err(format!(
                        "two pending failovers of partition {:?} to epoch {}",
                        e.partition, e.to_epoch
                    ));
                }
            }
            (K::Partition, S::Done | S::Abandoned) | (K::Writer, S::Done) => {}
            (k, st) => {
                return Err(format!(
                    "a failover event of kind {k:?} can't be in state {st:?}"
                ));
            }
        }
    }
    Ok(())
}

/// The pending promotes of `old` still live (their partition's epoch below their
/// `to_epoch`) must reach `new` unchanged, finished or abandoned, never dropped or altered
/// (simp724's S2 follow-ups on the D330 PR 2 plan): their `to_epoch` is the floor every
/// later promote of the partition stamps above, so a copy a half-done promote stamped is
/// never taken for a new history. Finished means the map gives the partition the event's
/// node at its `to_epoch`; abandoned, that a later promote of it is recorded above it.
fn validate_pending_failovers(old: &ClusterMap, new: &ClusterMap) -> Result<(), Refusal> {
    use crate::serve::proto::{FailoverKind as K, FailoverState as S};
    let epoch_of = |m: &ClusterMap, id: &str| {
        m.partitions
            .iter()
            .find(|p| p.id == id)
            .map(|p| (p.node.clone(), p.epoch))
    };
    for e in old.failovers.iter().filter(|e| {
        e.kind() == K::Partition
            && e.state() == S::Pending
            && epoch_of(old, &e.partition).is_some_and(|(_, ep)| ep < e.to_epoch)
    }) {
        // A map that takes P past the pending epoch keeps its floor, so it may drop or close
        // the event (a restore stamps above every recorded promote's epoch).
        if epoch_of(new, &e.partition).is_some_and(|(_, ep)| ep > e.to_epoch) {
            continue;
        }
        let now = new.failovers.iter().find(|n| {
            n.kind() == K::Partition && n.partition == e.partition && n.to_epoch == e.to_epoch
        });
        let same = |n: &crate::serve::proto::FailoverEvent| {
            let mut a = n.clone();
            a.state = e.state;
            a == *e
        };
        let ok = match now {
            None => false,
            Some(n) if !same(n) => false,
            Some(n) => match n.state() {
                S::Pending => true,
                S::Done => epoch_of(new, &e.partition) == Some((e.to.clone(), e.to_epoch)),
                // Abandoned: a later promote of P recorded above it, or (#2070, simp724) P
                // kept by the owner it would have replaced, at its epoch, which came back
                // and serves. Either way the event stays, pinned, and keeps its floor.
                S::Abandoned => {
                    new.failovers.iter().any(|l| {
                        l.kind() == K::Partition
                            && l.partition == e.partition
                            && l.to_epoch > e.to_epoch
                    }) || epoch_of(new, &e.partition) == epoch_of(old, &e.partition)
                        && epoch_of(old, &e.partition).is_some_and(|(node, _)| node == e.from)
                }
                S::Unspecified => false,
            },
        };
        if !ok {
            return Err(format!(
                "the map drops or changes the pending failover of partition {:?} to node {:?} \
                 at epoch {}: keep `failovers` as `phdb cluster get-map` shows them (a promote \
                 may have stamped a copy at that epoch, D330)",
                e.partition, e.to, e.to_epoch
            ));
        }
        // No other move takes P to an epoch at or below the pending one's: a copy stamped
        // there would pass for the new history. Only the promote's own map gives P that
        // epoch (finished, above).
        let finished = now.is_some_and(|n| n.state() == S::Done);
        let (old_at, new_at) = (
            epoch_of(old, &e.partition).map(|(_, ep)| ep),
            epoch_of(new, &e.partition).map(|(_, ep)| ep),
        );
        if !finished
            && let Some(at) = new_at
            && Some(at) != old_at
            && at <= e.to_epoch
        {
            return Err(format!(
                "partition {:?} has a promote to node {:?} pending at epoch {}: a move must take \
                 it above that epoch, or finish the promote first (phdb cluster promote {} --to \
                 {}) (D330)",
                e.partition, e.to, e.to_epoch, e.partition, e.to
            ));
        }
    }
    // The floor binds every epoch change of P (#2070, simp724): a promote's `to_epoch`, in
    // any state, may have a copy stamped at it, so no map gives P that epoch or one below
    // it except that promote finishing. A hand-over, a relocation or a move takes
    // `max(epoch, floor) + 1` ([`super::promote::next_epoch`]).
    for p in &new.partitions {
        let Some((_, was)) = epoch_of(old, &p.id) else {
            continue;
        };
        let floor = super::promote::promote_floor(old, &p.id);
        if p.epoch == was || p.epoch > floor {
            continue;
        }
        let finishing = new.failovers.iter().any(|e| {
            e.kind() == K::Partition
                && e.partition == p.id
                && e.state() == S::Done
                && e.to == p.node
                && e.to_epoch == p.epoch
        });
        if !finishing {
            return Err(format!(
                "partition {:?} goes to epoch {}, at or below epoch {floor}, which a promote of \
                 it recorded: a copy may be stamped there. Take it to epoch {} (D330, #2070)",
                p.id,
                p.epoch,
                was.max(floor) + 1
            ));
        }
    }
    Ok(())
}

/// Whether `a` sorts after `b`: `(writer_term, map_version)` (D330).
pub(crate) fn newer(a: &ClusterMap, b: &ClusterMap) -> bool {
    (a.writer_term, a.map_version) > (b.writer_term, b.map_version)
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
            // D331: the one change a placement takes, a move's hand-over.
            let handover = new.moves.iter().find(|m| {
                !node_move(m)
                    && m.table == t.name
                    && phase(m) == MovePhase::HandedOver
                    && old
                        .moves
                        .iter()
                        .any(|o| o.id == m.id && phase(o) == MovePhase::HandingOver)
            });
            let expected = handover.and_then(|m| {
                let (start, end) = move_range(m).ok()?;
                Some(handed_over(t, &start, end.as_deref(), &m.to))
            });
            if !expected.is_some_and(|e| keys(&e) == keys(n) && e.partitions == n.partitions) {
                return Err(format!(
                    "table {:?}'s split keys or partitions changed: a placement is fixed once \
                     written, except by a range move's hand-over (D331), which gives exactly \
                     the move's range to its target",
                    t.name
                ));
            }
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
    // The memory placement is fixed once written, as a table's is (#1915): its namespaces'
    // memories and index entries are in its partitions' stores.
    if let Some(m) = &old.memory {
        // Split keys compare as bytes: `text` and `raw` forms of one key are the same key.
        let same = |n: &crate::serve::proto::MemoryPlacement| {
            let keys = |p: &crate::serve::proto::MemoryPlacement| -> Vec<Vec<u8>> {
                p.splits
                    .iter()
                    .map(|k| split_key_bytes(k).unwrap_or_default())
                    .collect()
            };
            n.store == m.store && n.partitions == m.partitions && keys(n) == keys(m)
        };
        if !new.memory.as_ref().is_some_and(same) {
            return Err(format!(
                "memory store {:?}'s placement changed or went away: it's fixed once written \
                 (moving namespaces is later work, #1499)",
                m.store
            ));
        }
        used.extend(m.partitions.iter().map(String::as_str));
    }
    // An index's placement is fixed once written, as a table's is (#1722): its partitions
    // hold whole indexes of their ranges. Dropping it (`DropTable` of its name) is a schema
    // change.
    let new_indexes: HashMap<&str, _> = new.indexes.iter().map(|i| (i.name.as_str(), i)).collect();
    for i in &old.indexes {
        let Some(n) = new_indexes.get(i.name.as_str()) else {
            continue;
        };
        let keys = |p: &crate::serve::proto::IndexPlacement| -> Vec<Vec<u8>> {
            p.splits
                .iter()
                .map(|k| split_key_bytes(k).unwrap_or_default())
                .collect()
        };
        if i.kind != n.kind
            || i.follows != n.follows
            || keys(i) != keys(n)
            || i.partitions != n.partitions
            || i.created != n.created
        {
            return Err(format!(
                "index {:?}'s placement or options changed: an index is fixed once written \
                 (drop it, then create it again)",
                i.name
            ));
        }
        used.extend(i.partitions.iter().map(String::as_str));
    }
    validate_move_steps(old, new)?;
    let new_partitions: HashMap<&str, _> =
        new.partitions.iter().map(|p| (p.id.as_str(), p)).collect();
    for p in &old.partitions {
        if !new_partitions.contains_key(p.id.as_str()) && used.contains(p.id.as_str()) {
            return Err(format!(
                "partition {:?} holds a table, index or memory placement and can't be dropped",
                p.id
            ));
        }
    }
    validate_epochs(old, new)
}

/// D330's pending promote of partition `p` in `map`, if one is live: a PARTITION event
/// still PENDING whose `to_epoch` is above the partition's epoch.
fn pending_promote<'m>(
    map: &'m ClusterMap,
    p: &str,
) -> Option<&'m crate::serve::proto::FailoverEvent> {
    use crate::serve::proto::{FailoverKind, FailoverState};
    let epoch = map.partitions.iter().find(|q| q.id == p)?.epoch;
    map.failovers.iter().find(|e| {
        e.kind() == FailoverKind::Partition
            && e.partition == p
            && e.state() == FailoverState::Pending
            && e.to_epoch > epoch
    })
}

/// D331: each move takes one step per map. A forced map (a recovery) skips these steps,
/// so it may drop a move at any phase, HANDED_OVER included (cli692's N2): the table layer
/// it carries says who holds the range then, and `validate` still holds it to a range its
/// holder holds. One appears at COPYING; one kept keeps its
/// range, partitions and kind and stays in its phase or goes to the next; one goes away
/// before HANDED_OVER (an abort) or from CLEANING (done), never in HANDED_OVER. The step to
/// HANDED_OVER names this map as the hand-over and raises the source's epoch (by one, or past a promote's floor: [`super::promote::next_epoch`]), on
/// the same node (S1).
fn validate_move_steps(old: &ClusterMap, new: &ClusterMap) -> Result<(), Refusal> {
    let before: HashMap<&str, &RangeMove> = old.moves.iter().map(|m| (m.id.as_str(), m)).collect();
    let after: HashSet<&str> = new.moves.iter().map(|m| m.id.as_str()).collect();
    for o in &old.moves {
        if !after.contains(o.id.as_str()) && phase(o) == MovePhase::HandedOver {
            return Err(format!(
                "move {:?} is past its commit point (HANDED_OVER): it goes on to CLEANING, \
                 and can't be removed",
                o.id
            ));
        }
    }
    for m in &new.moves {
        let id = &m.id;
        let Some(o) = before.get(id.as_str()) else {
            if phase(m) != MovePhase::Copying {
                return Err(format!("move {id:?} must start at COPYING"));
            }
            // Not while a promote of its source or target is pending (cli692's N1 on PR 2):
            // the promote's map moves the partition to another node and epoch under the
            // copy, and the move's hand-over raises the same epoch.
            // A NODE move's `to` is a node, not a partition.
            let parts = if node_move(m) { 1 } else { 2 };
            for p in [&m.from, &m.to].into_iter().take(parts) {
                if let Some(e) = pending_promote(new, p) {
                    return Err(format!(
                        "move {id:?} can't start while partition {p:?} has a pending promote \
                         (to node {:?} at epoch {}): finish or abandon it first",
                        e.to, e.to_epoch
                    ));
                }
            }
            continue;
        };
        if (o.kind, &o.table, &o.from, &o.to, o.from_epoch)
            != (m.kind, &m.table, &m.from, &m.to, m.from_epoch)
            || move_range(o)? != move_range(m)?
        {
            return Err(format!(
                "move {id:?}'s table, range, partitions or kind changed: a move keeps them"
            ));
        }
        let (was, now) = (phase(o) as i32, phase(m) as i32);
        // A NODE move has no CATCHING_UP: it goes from COPYING to HANDING_OVER (D332 PR 2).
        let skip =
            node_move(m) && phase(o) == MovePhase::Copying && phase(m) == MovePhase::HandingOver;
        if now != was && now != was + 1 && !skip {
            return Err(format!(
                "move {id:?} went from phase {:?} to {:?}: a move goes forward one phase per map",
                phase(o),
                phase(m)
            ));
        }
        if phase(m) == MovePhase::HandedOver && phase(o) == MovePhase::HandingOver {
            if (m.handed_over_term, m.handed_over_version) != (new.writer_term, new.map_version) {
                return Err(format!(
                    "move {id:?}'s hand-over map must be this map (term {}, version {})",
                    new.writer_term, new.map_version
                ));
            }
            // A NODE move's commit point is P's relocation (D332 PR 2): to `to`, at its next
            // epoch past any promote's floor, from the epoch it started at.
            if node_move(m) {
                let of = |map: &ClusterMap| {
                    map.partitions
                        .iter()
                        .find(|p| p.id == m.from)
                        .map(|p| (p.node.clone(), p.epoch))
                };
                let floor = super::promote::promote_floor(old, &m.from);
                match (of(old), of(new)) {
                    (Some((a, e)), Some((b, f)))
                        if a != m.to && e == m.from_epoch && b == m.to && f == e.max(floor) + 1 => {
                    }
                    _ => {
                        return Err(format!(
                            "move {id:?}'s HANDED_OVER map relocates partition {:?} from epoch \
                             {} to node {:?} at its next epoch (D332 PR 2)",
                            m.from, m.from_epoch, m.to
                        ));
                    }
                }
                continue;
            }
            let epoch = |map: &ClusterMap| {
                map.partitions
                    .iter()
                    .find(|p| p.id == m.from)
                    .map(|p| (p.node.clone(), p.epoch))
            };
            // By one, or past the epoch a promote of it recorded (#2070, simp724): never onto
            // a copy an abandoned promote stamped.
            let floor = super::promote::promote_floor(old, &m.from);
            match (epoch(old), epoch(new)) {
                (Some((a, e)), Some((b, f))) if a == b && f == e.max(floor) + 1 => {}
                _ => {
                    return Err(format!(
                        "move {id:?}'s hand-over raises its source {:?}'s epoch to one above \
                         both its epoch and every epoch a promote of it recorded, on the same \
                         node (D331's S1, #2070)",
                        m.from
                    ));
                }
            }
        } else if (o.handed_over_term, o.handed_over_version)
            != (m.handed_over_term, m.handed_over_version)
        {
            return Err(format!("move {id:?}'s hand-over map changed"));
        }
    }
    Ok(())
}

/// Whether `a` and `b` list the same node ids (in any order).
pub(crate) fn same_nodes(a: &ClusterMap, b: &ClusterMap) -> bool {
    fn ids(m: &ClusterMap) -> std::collections::BTreeSet<&str> {
        m.nodes.iter().map(|n| n.id.as_str()).collect()
    }
    ids(a) == ids(b)
}

/// A map set through the writer that changes the cluster's nodes may not also move a
/// partition or raise its epoch (D317 rule 3, as amended for #1760): a partition's owner
/// changes only in a map with the same nodes as the one before it, so a majority of those
/// nodes confirms one owner at a time. The refusal, naming the first such partition, or
/// `None`. Forced maps aren't held to it (a recovery's), nor maps a peer pushes.
pub(crate) fn moves_with_node_change(old: &ClusterMap, new: &ClusterMap) -> Option<String> {
    if same_nodes(old, new) {
        return None;
    }
    let old_parts: HashMap<&str, _> = old.partitions.iter().map(|p| (p.id.as_str(), p)).collect();
    let moved = new.partitions.iter().find(|p| {
        old_parts
            .get(p.id.as_str())
            .is_some_and(|o| o.node != p.node || o.epoch != p.epoch)
    })?;
    Some(format!(
        "a map that changes the cluster's nodes can't also move or re-epoch a partition \
         (partition {:?}): set the node change, then the move, as two maps (D317 rule 3: a \
         majority of one set of nodes confirms a partition's owner)",
        moved.id
    ))
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
///   were copied whole. A copy must move to another node at exactly `old`'s next epoch
///   ([`super::promote::next_epoch`]).
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
            (Some(o), Some(n))
                if n.epoch == o.epoch.max(super::promote::promote_floor(old, c)) + 1
                    && n.node != o.node => {}
            _ => {
                return Err(format!(
                    "partition {c:?} is marked as copied, but this change doesn't move it to \
                     another node at its next epoch (D330's floor included)"
                ));
            }
        }
    }
    // D331's S1: a hand-over raises its source's epoch with its files in place, open on the
    // same node, so the partition's history goes on.
    let handing: HashSet<&str> = new
        .moves
        .iter()
        .filter(|m| {
            !node_move(m)
                && phase(m) == MovePhase::HandedOver
                && old
                    .moves
                    .iter()
                    .any(|o| o.id == m.id && phase(o) == MovePhase::HandingOver)
        })
        .map(|m| m.from.as_str())
        .collect();
    let mut out = new.clone();
    for p in &mut out.partitions {
        p.history_epoch = match before.get(p.id.as_str()) {
            Some(o) if p.epoch == o.epoch => o.history_epoch,
            Some(o) if copies.contains(&p.id) => o.history_epoch,
            Some(o) if handing.contains(p.id.as_str()) && o.node == p.node => o.history_epoch,
            _ => p.epoch,
        };
    }
    Ok(out)
}

/// The map with each split key in its canonical form: `text` when the bytes are UTF-8,
/// else `raw`. Two maps that say the same thing are equal after it.
pub(crate) fn canonical(map: &ClusterMap) -> ClusterMap {
    let mut m = map.clone();
    let memory = m.memory.iter_mut().flat_map(|p| p.splits.iter_mut());
    let indexes = m.indexes.iter_mut().flat_map(|i| i.splits.iter_mut());
    let moves = m
        .moves
        .iter_mut()
        .flat_map(|r| r.start.iter_mut().chain(r.end.iter_mut()));
    for k in m
        .tables
        .iter_mut()
        .flat_map(|t| t.splits.iter_mut())
        .chain(memory)
        .chain(indexes)
        .chain(moves)
    {
        if let Ok(b) = split_key_bytes(k) {
            k.key = Some(match String::from_utf8(b) {
                Ok(s) => split_key::Key::Text(s),
                Err(e) => split_key::Key::Raw(e.into_bytes().into()),
            });
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
            replicas: Vec::new(),
            failover: 0,
            failover_count: 0,
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
            previous_nodes: Vec::new(),
            indexes: Vec::new(),
            memory: None,
            nodes: vec![node("a", 7001), node("b", 7002)],
            partitions: vec![part("p1", "a"), part("p2", "b"), part("p3", "b")],
            tables: vec![
                table("users", "", vec![text("m")], &["p1", "p2"]),
                table("orders", "o", vec![], &["p3"]),
                table("orders_idx", "o", vec![], &["p3"]),
            ],
            writer: String::new(),
            writer_term: 0,
            failover: false,
            failover_rejoin: false,
            failovers: Vec::new(),
            moves: Vec::new(),
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

    /// The memory store `notes`: namespaces below "m" on p1, the rest on p2.
    fn with_memory() -> ClusterMap {
        let mut m = map();
        m.memory = Some(crate::serve::proto::MemoryPlacement {
            store: "notes".into(),
            splits: vec![text("m")],
            partitions: vec!["p1".into(), "p2".into()],
        });
        m
    }

    #[test]
    fn a_memory_placement_takes_no_table_and_no_split_inside_a_namespace() {
        validate(&with_memory(), true).unwrap();
        let set = |f: &dyn Fn(&mut crate::serve::proto::MemoryPlacement)| {
            let mut m = with_memory();
            f(m.memory.as_mut().unwrap());
            m
        };
        refused(validate(&set(&|p| p.store.clear()), true), "names no store");
        // `{ns}\0…` rows: a key with `\0` could fall inside one namespace's.
        refused(
            validate(&set(&|p| p.splits = vec![raw(b"m\0x")]), true),
            "contains \\0",
        );
        refused(
            validate(&set(&|p| p.splits = vec![text("m"), text("c")]), true),
            "increasing order",
        );
        refused(
            validate(&set(&|p| p.splits = vec![text("")]), true),
            "split key is empty",
        );
        refused(
            validate(&set(&|p| p.partitions.push("p3".into())), true),
            "2 ranges, but 3",
        );
        refused(
            validate(&set(&|p| p.partitions[1] = "p9".into()), true),
            "doesn't list",
        );
        // The store's own tables can't be placed as tables.
        for t in ["mem_notes", "mem_notes_text", "mem_notes_ivf"] {
            let mut m = with_memory();
            m.tables[0].name = t.into();
            refused(validate(&m, true), "memory store \"notes\"'s own");
        }
    }

    /// Indexes (#1722): a vector and a text index following `users`, and a vector index of
    /// its own ids, split at "k" over p1 and p3.
    fn with_indexes() -> ClusterMap {
        use crate::serve::proto::{
            CreateTextIndexRequest, CreateVectorIndexRequest, IndexPlacement, index_placement,
        };
        let mut m = map();
        let vector = |name: &str| {
            Some(index_placement::Kind::Vector(CreateVectorIndexRequest {
                name: name.into(),
                ..Default::default()
            }))
        };
        m.indexes = vec![
            IndexPlacement {
                name: "users_vec".into(),
                kind: vector("users_vec"),
                follows: "users".into(),
                ..Default::default()
            },
            IndexPlacement {
                name: "users_text".into(),
                kind: Some(index_placement::Kind::Text(CreateTextIndexRequest {
                    name: "users_text".into(),
                    source_table: "users".into(),
                    ..Default::default()
                })),
                follows: "users".into(),
                ..Default::default()
            },
            IndexPlacement {
                name: "own_vec".into(),
                kind: vector("own_vec"),
                splits: vec![text("k")],
                partitions: vec!["p1".into(), "p3".into()],
                ..Default::default()
            },
        ];
        m
    }

    #[test]
    fn an_index_follows_a_placed_table_or_has_its_own_ranges() {
        use crate::serve::proto::index_placement::Kind;
        validate(&with_indexes(), true).unwrap();
        let set = |i: usize, f: &dyn Fn(&mut crate::serve::proto::IndexPlacement)| {
            let mut m = with_indexes();
            f(&mut m.indexes[i]);
            m
        };
        for (m, says) in [
            (set(0, &|i| i.name = String::new()), "no name"),
            (
                set(0, &|i| i.name = "users".into()),
                "has the name of a table",
            ),
            (set(0, &|i| i.name = "__phdb_x".into()), "reserved"),
            (set(1, &|i| i.name = "other".into()), "names index"),
            (set(0, &|i| i.kind = None), "no create request"),
            (set(0, &|i| i.follows = "nope".into()), "doesn't place"),
            (
                set(0, &|i| i.partitions = vec!["p1".into()]),
                "names no split keys or partitions",
            ),
            (
                set(1, &|i| i.follows = String::new()),
                "must follow its source",
            ),
            (
                set(2, &|i| i.partitions = vec!["p1".into()]),
                "partition(s) are given",
            ),
            (
                set(2, &|i| i.partitions = vec!["p1".into(), "p9".into()]),
                "doesn't list",
            ),
            (set(2, &|i| i.splits = vec![text("")]), "empty"),
        ] {
            refused(validate(&m, true), says);
        }
        // Two placements of one name.
        let mut m = with_indexes();
        let again = m.indexes[0].clone();
        m.indexes.push(again);
        refused(validate(&m, true), "placed twice");
        // A memory store's tables are taken too.
        let mut m = with_indexes();
        m.memory = Some(crate::serve::proto::MemoryPlacement {
            store: "notes".into(),
            splits: Vec::new(),
            partitions: vec!["p1".into()],
        });
        m.indexes[0].name = "mem_notes".into();
        if let Some(Kind::Vector(r)) = &mut m.indexes[0].kind {
            r.name = "mem_notes".into();
        }
        refused(validate(&m, true), "has the name of a table");
    }

    #[test]
    fn an_index_is_fixed_once_written_and_its_table_stays_while_it_follows() {
        let old = with_indexes();
        let next = |f: &dyn Fn(&mut ClusterMap)| {
            let mut m = with_indexes();
            m.map_version = 2;
            f(&mut m);
            m
        };
        validate_change(&old, &next(&|_| {})).unwrap();
        // A key in its other form is the same key.
        validate_change(&old, &next(&|m| m.indexes[2].splits = vec![raw(b"k")])).unwrap();
        // Dropping an index is a schema change.
        validate_change(&old, &next(&|m| m.indexes.remove(2).name.clear())).unwrap();
        for (why, change) in [
            (
                "re-split",
                &(|m: &mut ClusterMap| m.indexes[2].splits = vec![text("j")])
                    as &dyn Fn(&mut ClusterMap),
            ),
            ("moved", &|m: &mut ClusterMap| {
                m.indexes[2].partitions = vec!["p2".into(), "p3".into()];
            }),
            ("recreated", &|m: &mut ClusterMap| m.indexes[0].created = 9),
            ("other options", &|m: &mut ClusterMap| {
                m.indexes[0].kind = m.indexes[2].kind.clone();
            }),
        ] {
            let e = validate_change(&old, &next(change)).expect_err(why);
            assert!(e.contains("fixed once written"), "{why}: {e}");
        }
        // A table an index follows can't go while the index does (#1916's rule).
        refused(
            validate(&next(&|m| m.tables.retain(|t| t.name != "users")), true),
            "doesn't place",
        );
        // An index of its own ranges keeps its partitions in the map.
        let mut m = next(&|_| {});
        m.tables.retain(|t| t.group != "o");
        m.partitions.retain(|p| p.id != "p3");
        refused(
            validate_change(&old, &m),
            "holds a table, index or memory placement",
        );
    }

    #[test]
    fn a_memory_placement_is_fixed_once_written_and_holds_its_partitions() {
        let old = with_memory();
        let next = |f: &dyn Fn(&mut ClusterMap)| {
            let mut m = with_memory();
            m.map_version = 2;
            f(&mut m);
            m
        };
        // The same placement, a key in its other form included: allowed.
        validate_change(&old, &next(&|_| {})).unwrap();
        validate_change(
            &old,
            &next(&|m| m.memory.as_mut().unwrap().splits = vec![raw(b"m")]),
        )
        .unwrap();
        for (why, change) in [
            (
                "dropped",
                &(|m: &mut ClusterMap| m.memory = None) as &dyn Fn(&mut ClusterMap),
            ),
            ("re-split", &|m: &mut ClusterMap| {
                m.memory.as_mut().unwrap().splits = vec![text("k")];
            }),
            ("moved", &|m: &mut ClusterMap| {
                m.memory.as_mut().unwrap().partitions = vec!["p1".into(), "p3".into()];
            }),
            ("renamed", &|m: &mut ClusterMap| {
                m.memory.as_mut().unwrap().store = "x".into()
            }),
        ] {
            let e = validate_change(&old, &next(change)).expect_err(why);
            assert!(e.contains("fixed once written"), "{why}: {e}");
        }
        // A store placed on a partition keeps it in the map, as a table does.
        let mut m = next(&|_| {});
        m.tables.retain(|t| t.name != "users");
        m.partitions.retain(|p| p.id != "p1");
        refused(
            validate_change(&old, &m),
            "holds a table, index or memory placement",
        );
        // A first placement is a change like any other.
        validate_change(&map(), &next(&|_| {})).unwrap();
        // Both forms of a key print as one.
        let mut raw_form = with_memory();
        raw_form.memory.as_mut().unwrap().splits = vec![raw(b"m")];
        assert_eq!(canonical_json(&raw_form), canonical_json(&with_memory()));
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

    /// D331: `users` `[c, f)` from p1 to p2, at `phase`.
    fn moving(phase: MovePhase) -> ClusterMap {
        let mut m = map();
        m.moves.push(RangeMove {
            id: "mv1".into(),
            kind: crate::serve::proto::MoveKind::Range as i32,
            table: "users".into(),
            start: Some(text("c")),
            end: Some(text("f")),
            from: "p1".into(),
            to: "p2".into(),
            phase: phase as i32,
            ..Default::default()
        });
        m
    }

    #[test]
    fn a_move_names_a_movable_range_its_source_holds() {
        validate(&moving(MovePhase::Copying), true).unwrap();
        let bad = |f: &dyn Fn(&mut ClusterMap), says: &str| {
            let mut m = moving(MovePhase::Copying);
            f(&mut m);
            refused(validate(&m, true), says);
        };
        bad(&|m| m.moves[0].id = "Bad Id".into(), "move id");
        bad(&|m| m.moves[0].phase = 0, "no phase");
        bad(&|m| m.moves[0].kind = 0, "no kind");
        bad(&|m| m.moves[0].table = "nope".into(), "doesn't place");
        bad(
            &|m| {
                m.moves[0].table = "orders".into();
                m.moves[0].from = "p3".into();
            },
            "co-location group",
        );
        bad(&|m| m.moves[0].to = "p9".into(), "doesn't list");
        bad(&|m| m.moves[0].to = "p1".into(), "to itself");
        bad(&|m| m.moves[0].end = Some(text("c")), "range is empty");
        // Across p1's range's end (`m`), and a range p1 doesn't hold.
        bad(&|m| m.moves[0].end = Some(text("n")), "isn't inside");
        bad(&|m| m.moves[0].end = None, "isn't inside");
        bad(
            &|m| {
                m.moves[0].start = Some(text("p"));
                m.moves[0].end = None;
            },
            "isn't inside",
        );
        // Across two adjacent ranges that p1 holds both of (simp724's nit): still not inside
        // one.
        let mut both = moving(MovePhase::Copying);
        both.tables[0].partitions = vec!["p1".into(), "p1".into()];
        both.moves[0].end = Some(text("p"));
        refused(validate(&both, true), "isn't inside");
        bad(&|m| m.moves[0].final_seqno = 7, "final_seqno");
        bad(&|m| m.moves[0].handed_over_version = 1, "handed-over map");
        // One move per partition, as source or target.
        bad(
            &|m| {
                let mut two = m.moves[0].clone();
                two.id = "mv2".into();
                two.start = Some(text("g"));
                two.end = Some(text("h"));
                two.to = "p3".into();
                m.moves.push(two);
            },
            "one move at a time",
        );
        bad(&|m| m.moves.push(m.moves[0].clone()), "listed twice");
        // From HANDED_OVER the target holds the range, and the hand-over map is named.
        let mut m = moving(MovePhase::HandedOver);
        m.moves[0].handed_over_version = 1;
        refused(validate(&m, true), "inside one range partition \"p2\"");
        m.tables[0] = handed_over(&m.tables[0], b"c", Some(b"f"), "p2");
        validate(&m, true).unwrap();
        m.moves[0].handed_over_version = 2;
        refused(validate(&m, true), "not after this map");
    }

    #[test]
    fn a_table_an_index_follows_never_moves_even_in_a_forced_map() {
        // The coordinator, 2026-11-18: a text index's postings and a vector index's lists
        // hold ids from both sides of any range (#2066).
        let mut m = with_indexes();
        m.moves = moving(MovePhase::Copying).moves;
        refused(
            validate(&m, true),
            "index \"users_vec\" follows table \"users\"",
        );
    }

    #[test]
    fn a_hand_over_cuts_the_range_out_and_gives_it_to_the_target() {
        let t = &map().tables[0];
        let keys = |t: &TablePlacement| -> Vec<Vec<u8>> {
            t.splits
                .iter()
                .map(|k| split_key_bytes(k).unwrap())
                .collect()
        };
        // Inside p1's range: two new split keys.
        let h = handed_over(t, b"c", Some(b"f"), "p2");
        assert_eq!(keys(&h), [b"c".to_vec(), b"f".to_vec(), b"m".to_vec()]);
        assert_eq!(h.partitions, ["p1", "p2", "p1", "p2"]);
        // To the table's end from inside p2's range: one.
        let h = handed_over(t, b"p", None, "p1");
        assert_eq!(keys(&h), [b"m".to_vec(), b"p".to_vec()]);
        assert_eq!(h.partitions, ["p1", "p2", "p1"]);
        // A whole range: none.
        let h = handed_over(t, b"m", None, "p3");
        assert_eq!(keys(&h), [b"m".to_vec()]);
        assert_eq!(h.partitions, ["p1", "p3"]);
        // The table's own keys keep their form; new ones are canonical (cli692's N3).
        let mut raw_m = t.clone();
        raw_m.splits = vec![raw(b"m")];
        let h = handed_over(&raw_m, b"c", Some(b"f"), "p2");
        assert_eq!(h.splits, [text("c"), text("f"), raw(b"m")]);
        let h = handed_over(&raw_m, b"\xfe", Some(b"\xff"), "p1");
        assert_eq!(h.splits, [raw(b"m"), raw(b"\xfe"), raw(b"\xff")]);
    }

    #[test]
    fn a_forced_map_may_drop_a_handed_over_move_and_keeps_the_targets_placement() {
        // cli692's N2: a recovery's forced map skips the step rules, so it can drop a move
        // past its commit point; the placement it carries still names who holds the range,
        // and `validate` still holds it to that.
        let mut handed = moving(MovePhase::HandedOver);
        handed.map_version = 5;
        handed.moves[0].handed_over_version = 5;
        handed.tables[0] = handed_over(&handed.tables[0], b"c", Some(b"f"), "p2");
        handed.partitions[0].epoch = 2;
        validate(&handed, true).unwrap();
        let mut forced = handed.clone();
        forced.map_version = 6;
        forced.moves.clear();
        validate(&forced, true).unwrap();
        validate_catch_up(&handed, &forced).unwrap();
        validate_epochs(&handed, &forced).unwrap();
        assert_eq!(forced.tables[0].partitions, ["p1", "p2", "p1", "p2"]);
        // A change through the writer couldn't.
        refused(validate_change(&handed, &forced), "past its commit point");
    }

    /// cli692's N1 on PR 2: a move doesn't start while its source or target has a pending
    /// promote (D330); one already finished (its partition at the promote's epoch) is no bar.
    #[test]
    fn a_move_doesnt_start_while_its_source_or_target_has_a_pending_promote() {
        for p in ["p1", "p2"] {
            let epoch = map().partitions.iter().find(|q| q.id == p).unwrap().epoch;
            let mut old = map();
            old.failovers.push(pending(p, "c", epoch + 1));
            let mut new = moving(MovePhase::Copying);
            new.map_version = old.map_version + 1;
            new.failovers = old.failovers.clone();
            refused(validate_change(&old, &new), "pending promote");
            // Finished: the partition is at the promote's epoch.
            let mut old = map();
            old.failovers.push(pending(p, "c", epoch));
            new.failovers = old.failovers.clone();
            validate_change(&old, &new).unwrap();
        }
    }

    #[test]
    fn a_move_goes_one_phase_per_map_and_its_hand_over_is_the_one_reshape() {
        let at = |phase: MovePhase, version: u64| {
            let mut m = moving(phase);
            m.map_version = version;
            m
        };
        // It appears at COPYING, and only there.
        validate_change(&map(), &at(MovePhase::Copying, 2)).unwrap();
        refused(
            validate_change(&map(), &at(MovePhase::CatchingUp, 2)),
            "start at COPYING",
        );
        // Forward one phase at a time, or staying.
        validate_change(&at(MovePhase::Copying, 1), &at(MovePhase::Copying, 2)).unwrap();
        validate_change(&at(MovePhase::Copying, 1), &at(MovePhase::CatchingUp, 2)).unwrap();
        refused(
            validate_change(&at(MovePhase::Copying, 1), &at(MovePhase::HandingOver, 2)),
            "one phase per map",
        );
        refused(
            validate_change(&at(MovePhase::CatchingUp, 1), &at(MovePhase::Copying, 2)),
            "one phase per map",
        );
        // Its range, partitions and kind are fixed.
        let mut other = at(MovePhase::Copying, 2);
        other.moves[0].end = Some(text("e"));
        refused(
            validate_change(&at(MovePhase::Copying, 1), &other),
            "keeps them",
        );
        // Removed before the commit point (an abort), not at it.
        let mut gone = map();
        gone.map_version = 2;
        validate_change(&at(MovePhase::HandingOver, 1), &gone).unwrap();

        // The hand-over: this map named, the range given to p2, p1's epoch up by one.
        let before = at(MovePhase::HandingOver, 4);
        let mut after = at(MovePhase::HandedOver, 5);
        after.moves[0].handed_over_version = 5;
        after.tables[0] = handed_over(&after.tables[0], b"c", Some(b"f"), "p2");
        after.partitions[0].epoch = 2;
        validate_change(&before, &after).unwrap();
        // It keeps p1's history (cli692's M2), and p1 stays on its node.
        let derived = with_history(&before, &after, &[]).unwrap();
        assert_eq!(derived.partitions[0].history_epoch, 0);
        let mut wrong = after.clone();
        wrong.moves[0].handed_over_version = 4;
        refused(validate_change(&before, &wrong), "must be this map");
        let mut wrong = after.clone();
        wrong.partitions[0].epoch = 1;
        refused(validate_change(&before, &wrong), "raises its source");
        let mut wrong = after.clone();
        wrong.tables[0] = handed_over(&map().tables[0], b"c", Some(b"g"), "p2");
        refused(
            validate_change(&before, &wrong),
            "except by a range move's hand-over",
        );
        // The same reshape with no hand-over step is refused.
        let mut reshaped = map();
        reshaped.map_version = 2;
        reshaped.tables[0] = handed_over(&map().tables[0], b"c", Some(b"f"), "p2");
        refused(
            validate_change(&map(), &reshaped),
            "except by a range move's hand-over",
        );
        // Past the commit point it only goes on: never removed in HANDED_OVER; from CLEANING
        // it's done.
        let mut handed = after.clone();
        handed.map_version = 6;
        let mut gone = handed.clone();
        gone.map_version = 7;
        gone.moves.clear();
        refused(validate_change(&handed, &gone), "past its commit point");
        let mut cleaning = handed.clone();
        cleaning.map_version = 7;
        cleaning.moves[0].phase = MovePhase::Cleaning as i32;
        validate_change(&handed, &cleaning).unwrap();
        gone.map_version = 8;
        validate_change(&cleaning, &gone).unwrap();
        // A later step keeps the hand-over's map.
        let mut moved = cleaning.clone();
        moved.map_version = 8;
        moved.moves[0].handed_over_version = 7;
        refused(validate_change(&cleaning, &moved), "hand-over map changed");
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
            replicas: Vec::new(),
            failover: 0,
            failover_count: 0,
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
            replicas: Vec::new(),
            failover: 0,
            failover_count: 0,
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

    /// A pending promote of `p` to `to` at `to_epoch` (D330 PR 2).
    fn pending(p: &str, to: &str, to_epoch: u64) -> crate::serve::proto::FailoverEvent {
        let mut e = crate::serve::proto::FailoverEvent {
            partition: p.into(),
            from: "b".into(),
            to: to.into(),
            to_epoch,
            ..Default::default()
        };
        e.set_kind(crate::serve::proto::FailoverKind::Partition);
        e.set_state(crate::serve::proto::FailoverState::Pending);
        e
    }

    /// `m` one version on, with `f` applied.
    fn next(m: &ClusterMap, f: impl FnOnce(&mut ClusterMap)) -> ClusterMap {
        let mut n = m.clone();
        n.map_version += 1;
        f(&mut n);
        n
    }

    fn p3(m: &mut ClusterMap) -> &mut Partition {
        m.partitions.iter_mut().find(|p| p.id == "p3").unwrap()
    }

    #[test]
    fn a_pending_promote_is_kept_until_it_finishes_or_a_later_one_takes_over() {
        // simp724's S2 follow-ups on the D330 PR 2 plan: p3 (on b at epoch 1) has a promote
        // to a pending at epoch 2.
        use crate::serve::proto::FailoverState as S;
        let mut old = map();
        old.writer_term = 1;
        old.failovers.push(pending("p3", "a", 2));
        let says = "drops or changes the pending failover";
        refused(
            validate_catch_up(&old, &next(&old, |n| n.failovers.clear())),
            says,
        );
        refused(
            validate_catch_up(&old, &next(&old, |n| n.failovers[0].to_epoch = 3)),
            says,
        );
        // Done only with the move it names.
        refused(
            validate_catch_up(&old, &next(&old, |n| n.failovers[0].set_state(S::Done))),
            says,
        );
        let done = next(&old, |n| {
            n.failovers[0].set_state(S::Done);
            p3(n).node = "a".into();
            p3(n).epoch = 2;
        });
        validate_catch_up(&old, &done).unwrap();
        // No other move takes p3 to the pending epoch, or below it.
        refused(
            validate_catch_up(
                &old,
                &next(&old, |n| {
                    p3(n).node = "a".into();
                    p3(n).epoch = 2;
                }),
            ),
            "has a promote to node \"a\" pending at epoch 2",
        );
        // Abandoned for a later promote of p3, recorded above it; or (#2070, simp724) with p3
        // kept by the owner it would have replaced (b), at its epoch: it came back.
        validate_catch_up(
            &old,
            &next(&old, |n| n.failovers[0].set_state(S::Abandoned)),
        )
        .unwrap();
        let mut elsewhere = old.clone();
        elsewhere.failovers[0].from = "c".into();
        refused(
            validate_catch_up(
                &elsewhere,
                &next(&elsewhere, |n| n.failovers[0].set_state(S::Abandoned)),
            ),
            says,
        );
        validate_catch_up(
            &old,
            &next(&old, |n| {
                n.failovers[0].set_state(S::Abandoned);
                n.failovers.push(pending("p3", "b", 3));
            }),
        )
        .unwrap();
        // A map that takes p3 past the pending epoch (a restore's stamp) may drop it.
        validate_catch_up(
            &old,
            &next(&old, |n| {
                n.failovers.clear();
                p3(n).epoch = 3;
            }),
        )
        .unwrap();
        // Once p3's epoch passed it, the event is no floor: a map may drop it.
        let mut moved = old.clone();
        p3(&mut moved).epoch = 2;
        validate_catch_up(&moved, &next(&moved, |n| n.failovers.clear())).unwrap();
    }

    /// #2070 (simp724): a promote's `to_epoch`, in any state, is a floor for every epoch
    /// change of its partition: an abandoned promote's orphan copy, stamped there, never
    /// passes for a later history.
    #[test]
    fn an_abandoned_promotes_epoch_stays_a_floor_for_every_epoch_change() {
        use crate::serve::proto::FailoverState as S;
        // p3 on b at epoch 1; a promote to a at epoch 2 abandoned (b came back).
        let mut old = map();
        old.writer_term = 1;
        old.writer = "a".into();
        let mut e = pending("p3", "a", 2);
        e.set_state(S::Abandoned);
        old.failovers.push(e);
        assert_eq!(
            super::super::promote::next_epoch(&old, p3(&mut old.clone())),
            3
        );
        // A relocation onto the orphan's epoch is refused; one above it isn't.
        let says = "at or below epoch 2, which a promote of it recorded";
        refused(
            validate_catch_up(
                &old,
                &next(&old, |n| {
                    p3(n).node = "c".into();
                    p3(n).epoch = 2;
                }),
            ),
            says,
        );
        validate_catch_up(
            &old,
            &next(&old, |n| {
                p3(n).node = "c".into();
                p3(n).epoch = 3;
            }),
        )
        .unwrap();
        // A relocation by copy goes to the next epoch past the floor (with_history).
        let copied = next(&old, |n| {
            p3(n).node = "c".into();
            p3(n).epoch = 3;
        });
        with_history(&old, &copied, &["p3".into()]).unwrap();
        let onto = next(&old, |n| {
            p3(n).node = "c".into();
            p3(n).epoch = 2;
        });
        assert!(with_history(&old, &onto, &["p3".into()]).is_err());
        // The event stays pinned while p3's epoch is at or below it, whatever the count; past
        // it, it's an ordinary finished event.
        let mut m = old.clone();
        for i in 0..FAILOVER_EVENTS {
            let mut d = pending("p1", "a", 2 + i as u64);
            d.set_state(S::Done);
            super::super::promote::push_event(&mut m, d);
        }
        assert!(m.failovers.iter().any(|e| e.partition == "p3"), "pinned");
        validate(&m, true).unwrap();
        p3(&mut m).epoch = 3;
        let mut d = pending("p1", "a", 99);
        d.set_state(S::Done);
        super::super::promote::push_event(&mut m, d);
        assert!(!m.failovers.iter().any(|e| e.partition == "p3"), "pruned");
    }

    /// #2070 (ai1249's review): an abandoned promote's event stays pinned while a hand-over
    /// of its partition is in flight, though the hand-over took the partition past it: a
    /// source that crashed before its stamp checks its file against that floor when it opens.
    #[test]
    fn an_abandoned_promote_stays_pinned_while_a_hand_over_of_its_partition_is_in_flight() {
        use crate::serve::proto::FailoverState as S;
        let mut m = moving(MovePhase::HandedOver);
        m.partitions[0].epoch = 3;
        let mut e = pending("p1", "c", 2);
        e.set_state(S::Abandoned);
        m.failovers.push(e);
        let done = |i: u64| {
            let mut d = pending("p2", "a", 10 + i);
            d.set_state(S::Done);
            d
        };
        for i in 0..=FAILOVER_EVENTS as u64 {
            super::super::promote::push_event(&mut m, done(i));
        }
        assert!(
            m.failovers.iter().any(|e| e.partition == "p1"),
            "pinned while p1's hand-over is in flight"
        );
        // The move done (PR 5 removes it once the source stamped): the floor is p1's epoch.
        m.moves.clear();
        super::super::promote::push_event(&mut m, done(99));
        assert!(
            !m.failovers.iter().any(|e| e.partition == "p1"),
            "pruned once the hand-over is done"
        );
    }

    /// #2070: a hand-over after an abandoned promote never lands on the orphan's stamped
    /// epoch; it takes the source past it.
    #[test]
    fn a_hand_over_after_an_abandoned_promote_goes_past_its_epoch() {
        use crate::serve::proto::FailoverState as S;
        let at = |phase: MovePhase, version: u64| {
            let mut m = moving(phase);
            m.map_version = version;
            let mut e = pending("p1", "c", 2);
            e.set_state(S::Abandoned);
            m.failovers.push(e);
            m
        };
        let before = at(MovePhase::HandingOver, 4);
        let mut after = at(MovePhase::HandedOver, 5);
        after.moves[0].handed_over_version = 5;
        after.tables[0] = handed_over(&after.tables[0], b"c", Some(b"f"), "p2");
        // One above p1's epoch (1) is the orphan's: refused.
        after.partitions[0].epoch = 2;
        refused(
            validate_change(&before, &after),
            "at or below epoch 2, which a promote of it recorded",
        );
        // Past it: max(epoch, floor) + 1.
        after.partitions[0].epoch = 3;
        validate_change(&before, &after).unwrap();
        let derived = with_history(&before, &after, &[]).unwrap();
        assert_eq!(
            derived.partitions[0].history_epoch, 0,
            "the history goes on"
        );
    }

    /// D332 PR 2: a NODE move's record, `phdb cluster move`'s: p3 (on b at epoch 1)
    /// relocating to node a.
    fn node_move(phase: MovePhase, version: u64) -> ClusterMap {
        let mut m = map();
        m.map_version = version;
        let mut r = RangeMove {
            id: "node-1".into(),
            kind: crate::serve::proto::MoveKind::Node as i32,
            from: "p3".into(),
            to: "a".into(),
            from_epoch: 1,
            ..Default::default()
        };
        r.set_phase(phase);
        m.moves.push(r);
        m
    }

    #[test]
    fn a_node_moves_record_is_checked_and_goes_forward_with_the_relocation() {
        validate(&node_move(MovePhase::Copying, 2), true).unwrap();
        let bad = |f: &dyn Fn(&mut ClusterMap), says: &str| {
            let mut m = node_move(MovePhase::Copying, 2);
            f(&mut m);
            refused(validate(&m, true), says);
        };
        bad(
            &|m| m.moves[0].table = "users".into(),
            "names no table or range",
        );
        bad(
            &|m| m.moves[0].to = "z".into(),
            "which the map doesn't list",
        );
        bad(
            &|m| m.moves[0].from = "p9".into(),
            "which the map doesn't list",
        );
        bad(
            &|m| m.moves[0].set_phase(MovePhase::CatchingUp),
            "no CATCHING_UP phase",
        );
        bad(&|m| m.moves[0].from_epoch = 0, "from_epoch");
        // A map that moves p3 elsewhere with the record left (a failover's promote) drops it
        // instead (simp724's M2).
        bad(
            &|m| {
                p3(m).node = "b".into();
                p3(m).epoch = 2;
            },
            "drops the record",
        );
        // One move per partition, whatever the kind.
        bad(
            &|m| {
                let mut r = m.moves[0].clone();
                r.id = "node-2".into();
                m.moves.push(r);
            },
            "one move at a time",
        );

        // COPYING to HANDING_OVER directly: no CATCHING_UP.
        validate_change(&map(), &node_move(MovePhase::Copying, 2)).unwrap();
        validate_change(
            &node_move(MovePhase::Copying, 2),
            &node_move(MovePhase::HandingOver, 3),
        )
        .unwrap();
        // The commit point: the map relocating p3 to a at its next epoch, naming itself.
        let before = node_move(MovePhase::HandingOver, 3);
        let mut after = node_move(MovePhase::HandedOver, 4);
        after.moves[0].handed_over_version = 4;
        p3(&mut after).node = "a".into();
        p3(&mut after).epoch = 2;
        validate_change(&before, &after).unwrap();
        let mut wrong = after.clone();
        p3(&mut wrong).epoch = 3;
        refused(validate_change(&before, &wrong), "at its next epoch");
        // Removed before the commit point (an abort, or a failover's map), not at it.
        let mut gone = map();
        gone.map_version = 4;
        validate_change(&before, &gone).unwrap();
        let mut gone = after.clone();
        gone.map_version = 5;
        gone.moves.clear();
        refused(validate_change(&after, &gone), "past its commit point");
        // A NODE record gates no range and hands no table over.
        assert!(super::node_move(&after.moves[0]));
    }

    #[test]
    fn a_maps_failover_fields_are_checked() {
        let mut m = map();
        m.failover_rejoin = true;
        refused(validate(&m, true), "failover_rejoin");
        let mut m = map();
        m.failovers = vec![pending("p3", "a", 2), pending("p3", "a", 2)];
        refused(validate(&m, true), "two pending failovers");
        let mut m = map();
        for i in 0..=FAILOVER_EVENTS {
            let mut e = pending("p3", "a", 2 + i as u64);
            e.set_state(crate::serve::proto::FailoverState::Done);
            m.failovers.push(e);
        }
        refused(validate(&m, true), "finished failovers");
        m.failovers.remove(0);
        validate(&m, true).unwrap();
        let mut m = map();
        let mut e = pending("p3", "a", 2);
        e.in_doubt_xids = vec!["ab".repeat(IN_DOUBT_BYTES)];
        e.in_doubt_count = 1;
        m.failovers.push(e);
        refused(validate(&m, true), "in-doubt xids");
    }

    #[test]
    fn uses_failover_sees_every_failover_field() {
        assert!(!uses_failover(&map()));
        let mut m = map();
        m.failovers.push(pending("p3", "a", 2));
        assert!(uses_failover(&m));
        let mut m = map();
        p3(&mut m).set_failover(crate::serve::proto::FailoverMode::Off);
        assert!(uses_failover(&m));
        let mut m = map();
        p3(&mut m).failover_count = 1;
        assert!(uses_failover(&m));
    }
}
