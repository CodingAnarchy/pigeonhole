//! A partition file's identity (D317 §2, ICR 0117): one row of the reserved table
//! `__phdb_cluster`, naming the cluster, the partition and its epoch. It's kept in the file
//! so it travels with the data: a backup, a bootstrap copy or a replica carries it.

use std::path::Path;

use pigeonhole::{ErrorCode, Family, Options, Pigeonhole};

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
    let table = match db.table(TABLE).and_then(|t| t.open()) {
        Ok(t) => t,
        Err(e) if e.code() == ErrorCode::TableNotFound => return Ok(None),
        Err(e) => return Err(format!("read {TABLE}: {e}")),
    };
    let cell = |q: &str| -> Result<String, String> {
        let c = table
            .get(ROW, FAMILY, q.as_bytes())
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

/// The identity of the partition file at `path`, read through a read-only open (no partition
/// is open for writing while ticket 1's node runs).
pub(crate) fn read_file(path: &Path) -> Result<Option<Identity>, String> {
    let db = Pigeonhole::open(path, Options::default().read_only(true))
        .map_err(|e| format!("open: {e}"))?;
    let id = read(&db);
    let _ = db.close();
    id
}
