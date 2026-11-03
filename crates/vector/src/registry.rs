//! The per-process registry of open indexes (#1268 item 8, ICR 0110): a second
//! [`Index::open`](crate::Index::open) of an index this process already has open returns a
//! clone of the first handle, so they share one centroid cache, one queue of lists to
//! rebalance and, in background mode, one rebalancing thread.
//!
//! An entry is the index's table name, its shared state (weakly) and its application handles'
//! token (weakly). It matches an open only when:
//! - an application handle is still alive (the token upgrades): once every handle is gone, the
//!   next open starts afresh, whatever a rebalancer still holds for its last split;
//! - the database is the same open ([`Pigeonhole::same_database`]), not another file, a copy
//!   of it, or the same file reopened after a close;
//! - the table is still the one the entry opened: its options row still reads through the
//!   entry's table handle (a dropped table, or a closed database, refuses the read).

use std::sync::{Arc, Mutex, PoisonError, Weak};

use pigeonhole::Pigeonhole;

use crate::Result;
use crate::index::Inner;

struct Entry {
    name: String,
    inner: Weak<Inner>,
    user: Weak<()>,
}

impl Entry {
    fn dead(&self) -> bool {
        self.user.strong_count() == 0 || self.inner.strong_count() == 0
    }
}

/// Every index this process has open. Opens are rare, so it's one lock and a scan.
static OPEN: Mutex<Vec<Entry>> = Mutex::new(Vec::new());

/// The handle for `name` in `db`: the registered one, or one `open` makes and registers.
///
/// The lock is held across `open` (a few reads: the options and the centroids), so two
/// threads opening one index at once get one handle, not two. It also holds across the
/// stale-entry read. Nothing called under it takes it again: `open` doesn't, and no
/// `Inner` can drop under it, since an entry is used only while an application handle holds
/// its `Inner`.
///
/// **Reads only under this lock (simp724's R1 on #1392):** nothing called under it may
/// submit work to a shard and wait for it: no commit, transaction, reduce or traverse. In an
/// application-owned database that wait needs the application's driver thread, which may
/// itself be blocked here opening another index. Today `open` and the stale-entry read only
/// get and scan. If an open ever has to write, open outside the lock, take it again, and use a
/// live entry that appeared meanwhile instead of yours (no thread has started for yours yet).
pub(crate) fn open(
    db: &Pigeonhole,
    name: &str,
    open: impl FnOnce() -> Result<(Arc<Inner>, Arc<()>)>,
) -> Result<(Arc<Inner>, Arc<()>)> {
    let mut entries = OPEN.lock().unwrap_or_else(PoisonError::into_inner);
    entries.retain(|e| !e.dead());
    let found = entries.iter().position(|e| {
        e.name == name
            && e.inner
                .upgrade()
                .is_some_and(|inner| inner.db().same_database(db))
    });
    if let Some(at) = found {
        let e = &entries[at];
        if let (Some(inner), Some(user)) = (e.inner.upgrade(), e.user.upgrade())
            && inner.is_live()
        {
            return Ok((inner, user));
        }
        // Its table was dropped (and perhaps created again), or its handles went meanwhile.
        entries.swap_remove(at);
    }
    let (inner, user) = open()?;
    entries.push(Entry {
        name: name.to_owned(),
        inner: Arc::downgrade(&inner),
        user: Arc::downgrade(&user),
    });
    Ok((inner, user))
}
