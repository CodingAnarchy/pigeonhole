use super::*;

/// A fault seam (tests): nodes cut off from their peers. A cut node's dials of its peers
/// fail, and so do its peers' dials of it, for maps, pushes and leases; its process runs on
/// and answers its own clients (D326's cut-off owner). Set through [`cut_node`].
#[cfg(test)]
static CUT_OFF: Mutex<Vec<Cut>> = Mutex::new(Vec::new());

/// One cut node: its directory, its address, and its cluster's node directories. Its
/// address matches only a dial from one of those directories, so an entry a finished or
/// panicked test left can't cut a later test's node that took the same port (#2095).
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct Cut {
    dir: PathBuf,
    address: String,
    cluster: Vec<PathBuf>,
}

/// Cuts the node in `dir` at `address` off from its peers in `cluster` (its cluster's node
/// directories) both ways, or joins it again (`off` false).
#[cfg(test)]
pub(crate) fn cut_node(dir: &Path, address: &str, cluster: &[PathBuf], off: bool) {
    let mut cut = CUT_OFF.lock().unwrap_or_else(PoisonError::into_inner);
    cut.retain(|c| !(c.dir == dir && c.address == address));
    if off {
        cut.push(Cut {
            dir: dir.to_owned(),
            address: address.to_owned(),
            cluster: cluster.to_vec(),
        });
    }
}

/// A fault seam (tests): the node in each directory can't reach the paired address, one way
/// (D330 PR 2's test 7: an owner cut off from the writer alone). List both ways to cut a
/// pair.
#[cfg(test)]
pub(crate) static CUT_BETWEEN: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before `nearest_qualifiers` (#1915 PR 3),
/// by cluster directory. `status`: its `Status` leaves the feature out. `parts`: its
/// partitions' nearest searches rank every qualifier, as such a build's would (with
/// `status` false, a peer that changed build within the entry node's features cache).
#[cfg(test)]
pub(crate) static OLD_NEAREST: Mutex<Vec<(PathBuf, OldNearest)>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before `memory_tool_fields` (#1915 PR 3b),
/// by cluster directory: their `Status` leaves the feature out.
#[cfg(test)]
pub(crate) static OLD_MEMORY: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before `ForwardBulkLoad` (#1722 PR 3), by
/// cluster directory: their `Status` leaves `index_bulk_load` out.
#[cfg(test)]
pub(crate) static OLD_BULK_LOAD: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before `ClusterMap.indexes` (#1722), by
/// cluster directory: their `Status` leaves `indexes` out.
#[cfg(test)]
pub(crate) static OLD_INDEXES: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A hold seam (tests): nodes whose `Status` doesn't answer while their cluster directory
/// is listed, as a stalled peer's wouldn't.
#[cfg(test)]
pub(crate) static HOLD_STATUS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests, #2073): nodes that hold no election while their cluster directory is
/// listed.
#[cfg(test)]
pub(crate) static NO_ELECTION: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests, #2073): candidates whose real vote requests reach no peer, past a
/// pre-vote that did: an election lost between the two rounds. Its own vote is recorded.
#[cfg(test)]
pub(crate) static DROP_VOTES: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests, #2073): writers that don't adopt a stranded node's term, so a test
/// can watch the cluster with the node still stranded.
#[cfg(test)]
pub(crate) static NO_ADOPT: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A hold seam (tests, #2159): an install on a node whose cluster directory is listed waits
/// after its term fence, before it persists the map, until the directory is unlisted. It
/// lists the directory in [`INSTALL_HELD`] once it waits.
#[cfg(test)]
pub(crate) static HOLD_INSTALL: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// The nodes whose install [`HOLD_INSTALL`] holds or held, by cluster directory.
#[cfg(test)]
pub(crate) static INSTALL_HELD: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// [`HOLD_INSTALL`]'s wait, in an install.
#[cfg(test)]
pub(crate) fn hold_install(dir: &Path) {
    if !seam(&HOLD_INSTALL, dir) {
        return;
    }
    INSTALL_HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(dir.to_owned());
    while seam(&HOLD_INSTALL, dir) {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// Whether the seam `list` names this node's directory `dir`.
#[cfg(test)]
pub(crate) fn seam(list: &Mutex<Vec<PathBuf>>, dir: &Path) -> bool {
    list.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|d| d == dir)
}

/// How [`OLD_NEAREST`] has a node act.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum OldNearest {
    /// `Status` leaves `nearest_qualifiers` out.
    Status,
    /// The node's partitions ignore `NearestRequest.qualifiers`.
    Parts,
}

/// Whether [`OLD_NEAREST`] has the node or partition at `path` act as `how`.
#[cfg(test)]
pub(crate) fn old_nearest(path: &Path, how: OldNearest) -> bool {
    OLD_NEAREST
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|(d, h)| *h == how && path.starts_with(d))
}

#[cfg(test)]
pub(super) fn cut_off(dir: &Path, address: &str) -> bool {
    CUT_OFF
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|c| c.dir == dir || (c.address == address && c.cluster.iter().any(|d| d == dir)))
        || CUT_BETWEEN
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(d, a)| d == dir && a == address)
}
