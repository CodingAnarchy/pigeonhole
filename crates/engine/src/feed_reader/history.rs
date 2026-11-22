use super::*;

/// The sessions whose segments the feed's history runs through (the mark rule), newest
/// first ([`chain`]'s order).
#[derive(Debug)]
pub(super) struct History {
    pub(super) sessions: Vec<u64>,
}

impl History {
    pub(super) fn holds(&self, session: u64) -> bool {
        session != 0 && self.sessions.contains(&session)
    }

    /// Whether `session` came after `than` in the history: by the chain's order, never by
    /// value, since session ids are random (#1810). Any held session is later than none
    /// (0, a feed that has read nothing yet) and than one the history doesn't hold.
    pub(super) fn later(&self, session: u64, than: u64) -> bool {
        let at = |s: u64| self.sessions.iter().position(|&x| x == s);
        match (at(session), at(than)) {
            (Some(a), Some(b)) => a < b,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }
}

/// In a reader process: the writer's session and fed tables, from its entry in the sidecar
/// (the last session entry: a writer writes it before its shared memory exists).
/// `InvalidArgument` when the writer doesn't feed (its open recorded `fed: none`).
pub(super) fn writer_session(shared: &Shared) -> Result<(u64, FeedTables)> {
    let off = || {
        Error::InvalidArgument(
            "the writer's change feed is off: subscribe to a writer that feeds".into(),
        )
    };
    let entries = match crate::feed::read_sidecar(&shared.vfs, &shared.path, shared.pager.db_id()) {
        Ok(Some(entries)) => entries,
        Ok(None) => return Err(off()),
        Err(Error::Io(e)) if e.kind == pigeonhole_io::ErrorKind::NotFound => return Err(off()),
        Err(e) => return Err(e),
    };
    for e in entries.iter().rev() {
        match e {
            FeedEntry::Session {
                session,
                streams,
                tables,
                ..
            } => {
                if *streams as usize != shared.shards {
                    // A new writer with another shard count is starting: this attachment is
                    // the old one's (simp724's nit). Subscribe again once it runs.
                    return Err(Error::SnapshotExpired);
                }
                return Ok((*session, tables.clone()));
            }
            FeedEntry::Unfed { .. } => return Err(off()),
            _ => {}
        }
    }
    Err(off())
}

/// The sessions whose segments hold the feed's history: the current one, and each one
/// before it while it fed every table in `tables`, had the same stream count, and chains.
/// A session chains when its `opened_at` is the previous session's last recorded
/// `sequence` and every sequence between was recorded: the previous session's Root
/// entries run contiguously from its own `opened_at` (simp724 on fix A: a lost middle
/// entry could hide another session's root commits, so a gap breaks the chain), with no
/// feed-off open between.
pub(super) fn read_history(shared: &Shared, current: u64, tables: &[TableId]) -> Result<History> {
    let entries = crate::feed::read_sidecar(&shared.vfs, &shared.path, shared.pager.db_id())?
        .unwrap_or_default();
    Ok(History {
        sessions: chain(&entries, current, tables),
    })
}

/// [`read_history`]'s rule over the sidecar's entries.
pub(super) fn chain(entries: &[FeedEntry], current: u64, tables: &[TableId]) -> Vec<u64> {
    let mut sessions: Vec<Link> = Vec::new();
    let mut last_seq: Option<u64> = None;
    let mut broken = true;
    for e in entries {
        match e {
            FeedEntry::Session {
                session,
                opened_at,
                streams,
                tables: fed,
                ..
            } => {
                // A feed of every table (an empty list, D315) needs a session that fed every
                // table: one with a list may have left out any table created since.
                let feeds = if tables.is_empty() {
                    matches!(fed, FeedTables::All)
                } else {
                    tables.iter().all(|&t| fed.contains(t))
                };
                let chained = !broken && last_seq == Some(*opened_at);
                sessions.push(Link {
                    session: *session,
                    feeds,
                    streams: *streams,
                    chained,
                });
                last_seq = Some(*opened_at);
                broken = false;
            }
            FeedEntry::Root { session, sequence } => {
                let ours = sessions.last().is_some_and(|s| s.session == *session);
                if !ours || last_seq.map(|l| l + 1) != Some(*sequence) {
                    broken = true;
                }
                last_seq = Some(*sequence);
            }
            FeedEntry::Unfed { .. } => broken = true,
            // A catalog change (D315) is appended just before its root commit's Root entry,
            // which carries the chain: one of another session breaks it.
            FeedEntry::Catalog { session, .. } => {
                if sessions.last().is_none_or(|s| s.session != *session) {
                    broken = true;
                }
            }
            // A root commit's intent (D315 amendment) is overwritten by its Root entry once
            // the commit is durable, and an open resolves one a crash left at the end. One
            // still here is either the live session's commit in progress, after everything
            // the chain reads, or an older one no open could decide: fail closed.
            FeedEntry::RootIntent { .. } => broken = true,
            // A newer build's entry may mean a break: fail closed (simp724's nit on #1316).
            FeedEntry::Unknown(_) | _ => broken = true,
        }
    }
    let Some(pos) = sessions.iter().rposition(|s| s.session == current) else {
        return vec![current];
    };
    let streams = sessions[pos].streams;
    let mut run = Vec::new();
    for (i, s) in sessions[..=pos].iter().enumerate().rev() {
        if !s.feeds || s.streams != streams {
            break;
        }
        run.push(s.session);
        if !s.chained || i == 0 {
            break;
        }
    }
    run
}

/// One session in [`chain`]'s walk of the sidecar.
pub(super) struct Link {
    pub(super) session: u64,
    /// It fed every table the feed follows.
    pub(super) feeds: bool,
    pub(super) streams: u32,
    /// It chains to the session before it.
    pub(super) chained: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(session: u64, opened_at: u64) -> FeedEntry {
        FeedEntry::Session {
            session,
            opened_at,
            retention_bytes: 1 << 20,
            retention_age_secs: 0,
            streams: 2,
            tables: FeedTables::All,
        }
    }

    fn root(session: u64, sequence: u64) -> FeedEntry {
        FeedEntry::Root { session, sequence }
    }

    #[test]
    fn sessions_chain_only_through_contiguous_sequences() {
        let t = [TableId(1)];
        let whole = [
            session(1, 1),
            root(1, 2),
            root(1, 3),
            root(1, 4),
            session(2, 4),
            root(2, 5),
        ];
        assert_eq!(chain(&whole, 2, &t), [2, 1]);
        // A lost middle entry: the last still matches the next open, but the root commit at
        // 3 is unaccounted for, so session 1's history ends there (simp724 on fix A).
        let gap = [
            session(1, 1),
            root(1, 2),
            root(1, 4),
            session(2, 4),
            root(2, 5),
        ];
        assert_eq!(chain(&gap, 2, &t), [2]);
        // A lost last entry: the next open is ahead.
        let ahead = [session(1, 1), root(1, 2), session(2, 3)];
        assert_eq!(chain(&ahead, 2, &t), [2]);
        // A feed-off open between.
        let unfed = [
            session(1, 1),
            root(1, 2),
            FeedEntry::Unfed { sequence: 2 },
            session(2, 2),
        ];
        assert_eq!(chain(&unfed, 2, &t), [2]);
        // A session with another stream count, or not feeding the table, ends it too.
        let narrower = [
            FeedEntry::Session {
                session: 1,
                opened_at: 1,
                retention_bytes: 1 << 20,
                retention_age_secs: 0,
                streams: 2,
                tables: FeedTables::Only(vec![TableId(9)]),
            },
            session(2, 1),
        ];
        assert_eq!(chain(&narrower, 2, &t), [2]);
        // An entry this build doesn't know may be a break: it ends the chain too.
        let unknown = [
            session(1, 1),
            root(1, 2),
            FeedEntry::Unknown(0xee),
            session(2, 2),
        ];
        assert_eq!(chain(&unknown, 2, &t), [2]);
    }
}
