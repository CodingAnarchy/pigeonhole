//! A partition file's identity (D317 §2, ICR 0117): one row of the reserved table
//! `__phdb_cluster`, naming the cluster, the partition and its epoch. It's kept in the file
//! so it travels with the data: a backup, a bootstrap copy or a replica carries it.

use std::path::Path;

use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, Snapshot};

/// The reserved table holding the identity row.
pub(crate) const TABLE: &str = "__phdb_cluster";
const FAMILY: &str = "id";
const ROW: &[u8] = b"partition";

/// Who a partition file says it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identity {
    pub cluster_id: String,
    pub partition_id: String,
    pub epoch: u64,
}

/// The identity row of `db`, or `None` for a file `init-partition` never stamped.
pub(crate) fn read(db: &Pigeonhole) -> Result<Option<Identity>, String> {
    read_as_of(db, None)
}

/// [`read`] as of `snapshot`: what a backup of that snapshot holds (D317 ticket 8b).
pub(crate) fn read_at(db: &Pigeonhole, snapshot: &Snapshot) -> Result<Option<Identity>, String> {
    read_as_of(db, Some(snapshot))
}

fn read_as_of(db: &Pigeonhole, snapshot: Option<&Snapshot>) -> Result<Option<Identity>, String> {
    let table = match db.table(TABLE).and_then(|t| t.open()) {
        Ok(t) => t,
        Err(e) if e.code() == ErrorCode::TableNotFound => return Ok(None),
        Err(e) => return Err(format!("read {TABLE}: {e}")),
    };
    let cell = |q: &str| -> Result<String, String> {
        let c = match snapshot {
            Some(snap) => table.get_at(snap, ROW, FAMILY, q.as_bytes()),
            None => table.get(ROW, FAMILY, q.as_bytes()),
        }
        .map_err(|e| format!("read {TABLE}: {e}"))?
        .ok_or_else(|| format!("{TABLE} has no {q}"))?;
        String::from_utf8(c.value().to_vec()).map_err(|_| format!("{TABLE}'s {q} isn't UTF-8"))
    };
    let epoch = cell("epoch")?;
    Ok(Some(Identity {
        cluster_id: cell("cluster_id")?,
        partition_id: cell("partition_id")?,
        epoch: epoch
            .parse()
            .map_err(|_| format!("{TABLE}'s epoch {epoch:?} isn't a number"))?,
    }))
}

/// Stamps `db` with `id`, creating the table. Refused when the file already says it's
/// another cluster's or another partition; the same identity again only updates the epoch,
/// which never falls.
pub(crate) fn write(db: &Pigeonhole, id: &Identity) -> Result<(), String> {
    if let Some(old) = read(db)? {
        if old.cluster_id != id.cluster_id || old.partition_id != id.partition_id {
            return Err(format!(
                "the file is already partition {:?} of cluster {:?}",
                old.partition_id, old.cluster_id
            ));
        }
        if id.epoch < old.epoch {
            return Err(format!(
                "the file is at epoch {}, and an epoch never falls",
                old.epoch
            ));
        }
    }
    put(db, id)
}

/// Restores `db`, a backup of cluster `from`'s partition, into `id`'s cluster (D317 ticket
/// 6's stamp with `restore_from_cluster_id`): the one place a file's cluster changes.
/// Refused unless the file says it's that partition of `from`.
pub(crate) fn restore(db: &Pigeonhole, id: &Identity, from: &str) -> Result<(), String> {
    match read(db)? {
        Some(old) if old.cluster_id == from && old.partition_id == id.partition_id => put(db, id),
        Some(old) => Err(format!(
            "the file is partition {:?} of cluster {:?}, not partition {:?} of cluster {from:?}",
            old.partition_id, old.cluster_id, id.partition_id
        )),
        None => Err("the file has no identity row: it isn't a cluster partition".into()),
    }
}

/// Writes `id` into `db`'s identity row, creating the table.
fn put(db: &Pigeonhole, id: &Identity) -> Result<(), String> {
    let table = db
        .table(TABLE)
        .map_err(|e| format!("{TABLE}: {e}"))?
        .family(FAMILY, Family::default())
        .create_if_missing()
        .map_err(|e| format!("create {TABLE}: {e}"))?;
    let epoch = id.epoch.to_string();
    table
        .mutate(ROW)
        .put(FAMILY, b"cluster_id", id.cluster_id.as_bytes())
        .put(FAMILY, b"partition_id", id.partition_id.as_bytes())
        .put(FAMILY, b"epoch", epoch.as_bytes())
        .commit()
        .map_err(|e| format!("write {TABLE}: {e}"))?;
    Ok(())
}

/// A split's mark on the partition file it created (D331 PR 5, audit120's four points):
/// the move that created it, and the map version that move's COPYING change carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SplitMark {
    pub move_id: String,
    pub version: u64,
}

const MARK_MOVE: &[u8] = b"split_move";
const MARK_VERSION: &[u8] = b"split_version";

/// `db`'s split mark, if a split's create made it and the mark wasn't cleared.
pub(crate) fn split_mark(db: &Pigeonhole) -> Result<Option<SplitMark>, String> {
    let table = match db.table(TABLE).and_then(|t| t.open()) {
        Ok(t) => t,
        Err(e) if e.code() == ErrorCode::TableNotFound => return Ok(None),
        Err(e) => return Err(format!("read {TABLE}: {e}")),
    };
    let cell = |q: &[u8]| -> Result<Option<String>, String> {
        table
            .get(ROW, FAMILY, q)
            .map_err(|e| format!("read {TABLE}: {e}"))?
            .map(|c| {
                String::from_utf8(c.value().to_vec())
                    .map_err(|_| format!("{TABLE}'s split mark isn't UTF-8"))
            })
            .transpose()
    };
    let (Some(move_id), Some(version)) = (cell(MARK_MOVE)?, cell(MARK_VERSION)?) else {
        return Ok(None);
    };
    Ok(Some(SplitMark {
        move_id,
        version: version
            .parse()
            .map_err(|_| format!("{TABLE}'s split version {version:?} isn't a number"))?,
    }))
}

/// Writes `mark` on `db`, whose identity row is there.
pub(crate) fn set_split_mark(db: &Pigeonhole, mark: &SplitMark) -> Result<(), String> {
    let table = db
        .table(TABLE)
        .and_then(|t| t.open())
        .map_err(|e| format!("{TABLE}: {e}"))?;
    let version = mark.version.to_string();
    table
        .mutate(ROW)
        .put(FAMILY, MARK_MOVE, mark.move_id.as_bytes())
        .put(FAMILY, MARK_VERSION, version.as_bytes())
        .commit()
        .map(drop)
        .map_err(|e| format!("write {TABLE}: {e}"))
}

/// Clears `db`'s split mark: its split finished, and the partition is like any other.
pub(crate) fn clear_split_mark(db: &Pigeonhole) -> Result<(), String> {
    let table = db
        .table(TABLE)
        .and_then(|t| t.open())
        .map_err(|e| format!("{TABLE}: {e}"))?;
    table
        .mutate(ROW)
        .delete_column(FAMILY, MARK_MOVE)
        .delete_column(FAMILY, MARK_VERSION)
        .commit()
        .map(drop)
        .map_err(|e| format!("write {TABLE}: {e}"))
}

/// Whether `db` holds no row outside the reserved tables (`__phdb_*`): what a split's
/// create made, or what its abort's drop left.
pub(crate) fn holds_no_rows(db: &Pigeonhole) -> Result<bool, String> {
    for name in db.tables() {
        if name.starts_with("__phdb") {
            continue;
        }
        let table = db
            .table(&name)
            .and_then(|t| t.open())
            .map_err(|e| format!("open {name}: {e}"))?;
        let mut rows = table
            .scan_prefix(b"")
            .iter()
            .map_err(|e| format!("scan {name}: {e}"))?;
        if rows
            .next_ref()
            .map_err(|e| format!("scan {name}: {e}"))?
            .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The split mark of the closed partition file at `path` (D331 PR 6: Status's line for a
/// file the map doesn't list).
pub(crate) fn split_mark_file(path: &Path) -> Result<Option<SplitMark>, String> {
    let db = Pigeonhole::open(path, Options::default().read_only(true))
        .map_err(|e| format!("open: {e}"))?;
    let mark = split_mark(&db);
    let _ = db.close();
    mark
}

/// The identity of the partition file at `path`, read through a read-only open: no partition
/// is open while ticket 1's node runs. Once ticket 2's node holds its partitions open for
/// writing, `Status` reads the identity from the open handle ([`read`]) instead, rather than
/// opening each file beside its writer (cli692's N2).
pub(crate) fn read_file(path: &Path) -> Result<Option<Identity>, String> {
    let db = Pigeonhole::open(path, Options::default().read_only(true))
        .map_err(|e| format!("open: {e}"))?;
    let id = read(&db);
    let _ = db.close();
    id
}
