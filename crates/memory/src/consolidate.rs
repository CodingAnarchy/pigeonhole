//! Consolidation (D311 §4, T3): the application's [`Consolidator`] folds groups of episodic
//! memories into semantic ones, and each group commits in one conflict-checked transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use pigeonhole::ErrorCode;

use crate::error::{Error, Result};
use crate::layout::*;
use crate::store::{
    Kind, LinkType, Memory, Remembered, Store, StoredMemory, supersede_in, write_link,
};

/// The application's summariser: folds a group of episodic memories into zero or more
/// memories (D311 §4).
///
/// [`Store::consolidate`] calls it outside every transaction, with no lock held, so it may
/// call a model and take seconds. What it returns is written as [`Kind::Semantic`] unless it
/// says [`Kind::Procedural`], in the group's session, and with an embedding in a store pinned
/// to a model, as [`Store::remember`] requires. Returning nothing says nothing in the group
/// is worth keeping.
pub trait Consolidator: Send + Sync {
    /// Folds `group` (newest first, as ids sort) into memories. An `Err` stops the pass
    /// ([`Error::Consolidate`]).
    fn consolidate(
        &self,
        group: &[StoredMemory],
    ) -> std::result::Result<Vec<Memory>, Box<dyn std::error::Error + Send + Sync>>;
}

/// What a consolidation does with a group's memories once it commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SourcePolicy {
    /// Keeps them, marked consolidated: recall weights their importance by
    /// [`Weights::consolidated`](crate::Weights::consolidated), and they're never grouped
    /// again.
    #[default]
    Keep,
    /// Deletes them. Their provenance (`sources`) moves to the new memories first. A memory
    /// one of them superseded is superseded by the first new memory instead; with nothing
    /// returned, a source that superseded one is kept, marked consolidated.
    Delete,
    /// Supersedes them by the first new memory, so recall leaves them out unless
    /// [`include_superseded`](crate::Recall::include_superseded). With nothing returned,
    /// they're kept, marked consolidated.
    Supersede,
}

/// How [`Store::consolidate`] picks and folds groups. Unlike the forget policy, it isn't
/// stored: the [`Consolidator`] is the application's, so is when to run it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ConsolidatePolicy {
    min_age: Duration,
    group_similarity: f32,
    min_group: usize,
    max_group: usize,
    max_groups: usize,
    sources: SourcePolicy,
}

impl Default for ConsolidatePolicy {
    fn default() -> Self {
        ConsolidatePolicy {
            min_age: Duration::from_secs(3600),
            group_similarity: 0.8,
            min_group: 2,
            max_group: 16,
            max_groups: 100,
            sources: SourcePolicy::Keep,
        }
    }
}

impl ConsolidatePolicy {
    /// The defaults: groups of 2 to 16 episodic memories at least an hour old, similar at
    /// 0.8 cosine, at most 100 groups a pass, sources kept.
    pub fn new() -> Self {
        Self::default()
    }

    /// Only memories at least this old, from their creation: 1 hour by default.
    pub fn min_age(mut self, d: Duration) -> Self {
        self.min_age = d;
        self
    }

    /// The cosine similarity to a group's first memory (its seed) a memory needs to join
    /// the group: 0.8 by default, from −1 to 1. A store without an embedding model ignores
    /// it: its groups are a session's memories in creation order.
    pub fn group_similarity(mut self, s: f32) -> Self {
        self.group_similarity = s;
        self
    }

    /// The smallest group consolidated: 2 by default, so a memory with nothing like it is
    /// left alone. 1 hands single memories over too, to rewrite them. 0 is refused.
    pub fn min_group(mut self, n: usize) -> Self {
        self.min_group = n;
        self
    }

    /// The largest group: 16 by default.
    pub fn max_group(mut self, n: usize) -> Self {
        self.max_group = n;
        self
    }

    /// The most groups a pass consolidates: 100 by default.
    pub fn max_groups(mut self, n: usize) -> Self {
        self.max_groups = n;
        self
    }

    /// What happens to a group's memories: [`SourcePolicy::Keep`] by default.
    pub fn sources(mut self, p: SourcePolicy) -> Self {
        self.sources = p;
        self
    }

    /// The minimum age.
    pub fn get_min_age(&self) -> Duration {
        self.min_age
    }

    /// The grouping similarity.
    pub fn get_group_similarity(&self) -> f32 {
        self.group_similarity
    }

    /// The smallest group.
    pub fn get_min_group(&self) -> usize {
        self.min_group
    }

    /// The largest group.
    pub fn get_max_group(&self) -> usize {
        self.max_group
    }

    /// The most groups a pass consolidates.
    pub fn get_max_groups(&self) -> usize {
        self.max_groups
    }

    /// What happens to a group's memories.
    pub fn get_sources(&self) -> SourcePolicy {
        self.sources
    }

    fn check(&self) -> Result<()> {
        let bad = |m: &str| {
            Err(Error::InvalidArgument(format!(
                "a consolidate policy's {m}"
            )))
        };
        if !(-1.0..=1.0).contains(&self.group_similarity) {
            return bad("group_similarity must be from -1 to 1");
        }
        if self.min_group == 0 {
            return bad("min_group must be at least 1");
        }
        if self.max_group < self.min_group {
            return bad("max_group must be at least min_group");
        }
        if self.max_groups == 0 {
            return bad("max_groups must be at least 1");
        }
        Ok(())
    }
}

/// What a consolidation pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConsolidatePass {
    /// The groups formed.
    pub groups: u64,
    /// The groups committed.
    pub committed: u64,
    /// The groups skipped: a memory in one changed after grouping (deleted, consolidated,
    /// superseded, pinned, or given a source), or its commit conflicted. Never retried in
    /// place, since that would call the consolidator again; the next pass regroups.
    pub skipped: u64,
    /// New memories written.
    pub created: u64,
    /// Returned memories merged into an existing one (exact dedup), a group's own memory
    /// included.
    pub merged: u64,
    /// Group memories kept, deleted or superseded by their source policy.
    pub sources: u64,
    /// Where to resume ([`Consolidate::after`]) when the pass stopped short of the
    /// namespace's end: its scan reached its limit, or `max_groups` left groups for later.
    /// `None` once it looked at every candidate. Groups never span a resume position (cli692's
    /// A1 on #1386: without it, old memories that never group would hold every pass at the
    /// scan's limit). A group that was skipped (it changed, or its commit conflicted) has its
    /// seed at or before the resume position, so a resumed pass doesn't see it again: a pass
    /// from the start does.
    pub resume: Option<MemoryId>,
}

/// A consolidation pass ([`Store::consolidate`]): set what you need, then [`run`](Self::run).
#[must_use = "a pass does nothing until run"]
pub struct Consolidate<'a> {
    store: &'a Store,
    ns: &'a str,
    consolidator: &'a dyn Consolidator,
    policy: Option<&'a ConsolidatePolicy>,
    now_us: Option<u64>,
    after: Option<&'a MemoryId>,
}

impl<'a> Consolidate<'a> {
    /// Starts after `id`, where an earlier pass stopped ([`ConsolidatePass::resume`]).
    pub fn after(mut self, id: &'a MemoryId) -> Self {
        self.after = Some(id);
        self
    }

    /// The policy: [`ConsolidatePolicy::new`]'s defaults if not set.
    pub fn policy(mut self, policy: &'a ConsolidatePolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// The time `min_age` counts to, in µs since the Unix epoch: now by default.
    pub fn now_us(mut self, us: u64) -> Self {
        self.now_us = Some(us);
        self
    }

    /// Forms the groups, then for each one calls the consolidator and commits what it
    /// returned (D311 §4). Groups that commit stay committed if a later one fails.
    ///
    /// The consolidator's failures end the pass with [`Error::Consolidate`], which carries
    /// the pass so far: its own `Err`, a returned memory the store refuses, or one whose
    /// link names a memory that's gone (that group isn't committed). Another error (the
    /// database's) doesn't report the groups before it, which stay committed.
    ///
    /// What the consolidator returns skips near dedup ([`NearDedup`](crate::NearDedup)): a
    /// summary is meant to sit near its sources.
    pub fn run(self) -> Result<ConsolidatePass> {
        check_namespace(self.ns)?;
        if let Some(a) = self.after
            && a.namespace() != self.ns
        {
            return Err(Error::InvalidArgument(format!(
                "a resume position in namespace {:?}, for a pass over {:?}",
                a.namespace(),
                self.ns
            )));
        }
        let default = ConsolidatePolicy::default();
        let policy = self.policy.unwrap_or(&default);
        policy.check()?;
        let now = self.now_us.unwrap_or_else(now_us);
        let (groups, resume) = self.groups(policy, now)?;
        let mut pass = ConsolidatePass {
            groups: groups.len() as u64,
            resume,
            ..ConsolidatePass::default()
        };
        for group in groups {
            let out = match self.consolidator.consolidate(&group) {
                Ok(out) => out,
                Err(error) => return Err(Error::Consolidate { error, pass }),
            };
            let session = group[0].session.clone();
            let mut made = Vec::with_capacity(out.len());
            let mut texts = BTreeSet::new();
            let mut folded = 0;
            for (i, mut m) in out.into_iter().enumerate() {
                if m.kind != Kind::Procedural {
                    m.kind = Kind::Semantic;
                }
                m.session = session.clone();
                // The consolidator's output, not the caller's arguments (cli692's A2).
                if let Err(e) = self.store.check_memory(self.ns, &m) {
                    let error = format!("the consolidator's memory {i} was refused: {e}").into();
                    return Err(Error::Consolidate { error, pass });
                }
                // The transaction reads at its snapshot, so a second memory of the same
                // text wouldn't see the first's dedup row: fold it here, as dedup would.
                let norm = normalize(&m.text);
                if !norm.is_empty() && !texts.insert(norm) {
                    folded += 1;
                    continue;
                }
                made.push(m);
            }
            match self.commit(policy, &group, &made, now, &mut pass) {
                Ok(true) => pass.merged += folded,
                Ok(false) => {}
                // A returned link to a memory that's gone (M4), or another refusal of what the
                // consolidator returned (simp724's N1): its fault, with the pass so far.
                Err(e @ (Error::NotFound(_) | Error::InvalidArgument(_))) => {
                    return Err(Error::Consolidate {
                        error: Box::new(e),
                        pass,
                    });
                }
                Err(e) => return Err(e),
            }
        }
        Ok(pass)
    }

    /// The candidates in id order, which is newest first: episodic memories of the
    /// namespace at least `min_age` old that no consolidation took in, not superseded and
    /// not pinned. Then the groups: by session, then greedily by similarity to a seed (the
    /// first left in that order), in the order of their seeds. A group's members follow its
    /// seed, so a resume after a kept group's seed finds every group left out.
    fn groups(
        &self,
        p: &ConsolidatePolicy,
        now: u64,
    ) -> Result<(Vec<Vec<StoredMemory>>, Option<MemoryId>)> {
        let cutoff = now.saturating_sub(p.min_age.as_micros().min(u64::MAX as u128) as u64);
        // Enough for `max_groups` full groups, with room for those that don't fill.
        let cap = p.max_groups.saturating_mul(p.max_group).saturating_mul(4);
        let mut by_session: BTreeMap<Option<String>, Vec<StoredMemory>> = BTreeMap::new();
        let mut seen = 0usize;
        let mut last: Option<MemoryId> = None;
        let mut capped = false;
        let prefix = [self.ns.as_bytes(), MEMORY_TAG].concat();
        // The namespace's memory rows end where `{ns}\0m` does: before `{ns}\0n`.
        let mut end = prefix.clone();
        *end.last_mut().expect("the tag") += 1;
        // A rid starts with `u64::MAX − created_us`, big-endian, so memory rows sort newest
        // first, and the first old enough is at the cutoff's key: start there, or after the
        // resume position if that's further on.
        let old_enough = [prefix.as_slice(), &(u64::MAX - cutoff).to_be_bytes()].concat();
        let after = self.after.map(MemoryId::row).filter(|a| *a >= old_enough);
        let start = match &after {
            Some(a) => std::ops::Bound::Excluded(a.as_slice()),
            None => std::ops::Bound::Included(old_enough.as_slice()),
        };
        let mut it = self
            .store
            .table()
            .scan_bounds(start, std::ops::Bound::Excluded(end.as_slice()))
            .iter()?;
        while let Some(r) = it.next_ref()? {
            let Some(id) = MemoryId::from_row(r.key()) else {
                continue;
            };
            last = Some(id.clone());
            if id.created_us() > cutoff {
                continue;
            }
            let Some(m) = crate::store::read_memory(id, &r.to_owned())? else {
                continue;
            };
            if m.kind != Kind::Episodic || m.consolidated || m.superseded_by.is_some() || m.pinned {
                continue;
            }
            by_session.entry(m.session.clone()).or_default().push(m);
            seen += 1;
            if seen >= cap {
                capped = true;
                break;
            }
        }
        let similar = self.store.embedding_model().is_some();
        let mut groups = Vec::new();
        for (_, mut left) in by_session {
            while !left.is_empty() {
                let seed = left.remove(0);
                let mut group = vec![seed];
                let mut rest = Vec::with_capacity(left.len());
                for m in left {
                    let joins = group.len() < p.max_group
                        && (!similar
                            || match (&group[0].embedding, &m.embedding) {
                                (Some(a), Some(b)) => cosine(a, b) >= p.group_similarity,
                                _ => false,
                            });
                    if joins {
                        group.push(m);
                    } else {
                        rest.push(m);
                    }
                }
                left = rest;
                if group.len() >= p.min_group {
                    groups.push(group);
                }
            }
        }
        // In their seeds' order (newest first), across sessions.
        groups.sort_by(|a, b| a[0].id.cmp(&b[0].id));
        // Where the next pass resumes. Groups left out by `max_groups` have seeds after the
        // last kept one, and members after their seeds, so resuming after that seed finds
        // them. Otherwise it's where the scan stopped, if it stopped at its limit.
        let resume = if groups.len() > p.max_groups {
            groups.truncate(p.max_groups);
            groups.last().map(|g| g[0].id.clone())
        } else if capped {
            last
        } else {
            None
        };
        Ok((groups, resume))
    }

    /// One group's commit (S1): re-reads, the new memories, then the source policy, in one
    /// transaction that's skipped, never retried, on a change or a conflict.
    fn commit(
        &self,
        p: &ConsolidatePolicy,
        group: &[StoredMemory],
        made: &[Memory],
        now: u64,
        pass: &mut ConsolidatePass,
    ) -> Result<bool> {
        let store = self.store;
        let t = store.table();
        let mut txn = store.db().transaction()?;
        // 1. The re-reads: each memory's content (a delete), the flags a consolidation,
        //    supersede or pin sets, and its sources, whose union moves to the new memories
        //    (simp724's C1 and C4 on #1337: a merge that adds a source conflicts).
        let mut provenance: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        for m in group {
            let row = m.id.row();
            let changed = txn.get(t, &row, CONTENT, TEXT)?.is_none()
                || txn.get(t, &row, META, CONSOLIDATED)?.is_some()
                || txn.get(t, &row, META, SUPERSEDED_BY)?.is_some()
                || txn.get(t, &row, META, PINNED)?.is_some();
            if changed {
                pass.skipped += 1;
                return Ok(false);
            }
            if let Some(r) = txn.get_row(t, &row, &[SOURCES])? {
                for e in r.iter() {
                    let at = e.cell.as_u64().unwrap_or(0);
                    let first = provenance.entry(e.qualifier.to_vec()).or_insert(at);
                    *first = (*first).min(at);
                }
            }
        }
        // 2. The new memories, through remember's path (exact dedup included), each with the
        //    group's provenance.
        let in_group: BTreeSet<&MemoryId> = group.iter().map(|m| &m.id).collect();
        let mut results: Vec<MemoryId> = Vec::with_capacity(made.len());
        let mut counts = (0, 0);
        for m in made {
            let r = store.remember_with(&mut txn, self.ns, m, new_rid(now)?, now, None)?;
            match &r {
                Remembered::New(_) => counts.0 += 1,
                _ => counts.1 += 1,
            }
            let id = r.id().clone();
            for (name, at) in &provenance {
                let name = std::str::from_utf8(name).map_err(|_| corrupt_source())?;
                store.add_source(&mut txn, &id.row(), name, *at)?;
            }
            if !results.contains(&id) {
                results.push(id);
            }
        }
        // A returned memory that merged into one of the group's own (simp724's C2): that
        // memory is the result, consolidated, and the source policy applies to the others.
        let kept: Vec<&MemoryId> = results.iter().filter(|id| in_group.contains(id)).collect();
        for id in &kept {
            txn.put(t, &id.row(), META, CONSOLIDATED, b"1");
        }
        let others: Vec<&StoredMemory> =
            group.iter().filter(|m| !results.contains(&m.id)).collect();
        // 3. The links and the source policy.
        if p.sources != SourcePolicy::Delete {
            for r in &results {
                for m in &others {
                    write_link(
                        &mut txn,
                        t,
                        &r.row(),
                        &m.id.row(),
                        LinkType::DERIVED_FROM,
                        1.0,
                    );
                }
            }
        }
        let first = results.first();
        for m in &others {
            let row = m.id.row();
            match (p.sources, first) {
                (SourcePolicy::Supersede, Some(new)) => {
                    supersede_in(&mut txn, t, &row, &new.row(), now);
                }
                (SourcePolicy::Delete, Some(new)) => {
                    // What it superseded is superseded by the new memory instead (C3).
                    let older = superseded_by(&mut txn, store, &row)?;
                    store.delete_in(&mut txn, &m.id)?;
                    // delete_in reads at the snapshot, so it doesn't see a link a returned
                    // memory makes to this one in this transaction: removed here.
                    for r in &results {
                        txn.delete_column(t, &r.row(), LINKS, &row);
                    }
                    for (old, valid_to) in older {
                        supersede_in(&mut txn, t, &old, &new.row(), valid_to);
                    }
                }
                (SourcePolicy::Delete, None)
                    if superseded_by(&mut txn, store, &row)?.is_empty() =>
                {
                    store.delete_in(&mut txn, &m.id)?;
                    for r in &results {
                        txn.delete_column(t, &r.row(), LINKS, &row);
                    }
                }
                _ => {
                    txn.put(t, &row, META, CONSOLIDATED, b"1");
                }
            }
        }
        match txn.commit() {
            Ok(_) => {
                pass.committed += 1;
                pass.created += counts.0;
                pass.merged += counts.1;
                pass.sources += others.len() as u64;
                Ok(true)
            }
            Err(e) if e.code() == ErrorCode::Conflict => {
                pass.skipped += 1;
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// The memories `row` superseded and still names as their superseder, each with its
/// `valid_to`: read in the transaction, as `delete_in` reads them.
fn superseded_by(
    txn: &mut pigeonhole::Transaction,
    store: &Store,
    row: &[u8],
) -> Result<Vec<(Vec<u8>, u64)>> {
    let t = store.table();
    let mut targets = Vec::new();
    if let Some(r) = txn.get_row(t, row, &[LINKS])? {
        for e in r.iter() {
            if e.cell.value().first() == Some(&LinkType::SUPERSEDES.get()) {
                targets.push(e.qualifier.to_vec());
            }
        }
    }
    let mut out = Vec::new();
    for old in targets {
        if txn
            .get(t, &old, META, SUPERSEDED_BY)?
            .is_some_and(|c| c.value() == row)
        {
            let valid_to = txn
                .get(t, &old, META, VALID_TO)?
                .and_then(|c| c.as_u64())
                .unwrap_or(0);
            out.push((old, valid_to));
        }
    }
    Ok(out)
}

fn corrupt_source() -> Error {
    crate::error::corrupt("a source's name")
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let (mut ab, mut aa, mut bb) = (0.0f64, 0.0f64, 0.0f64);
    for (&x, &y) in a.iter().zip(b) {
        let (x, y) = (f64::from(x), f64::from(y));
        ab += x * y;
        aa += x * x;
        bb += y * y;
    }
    if aa == 0.0 || bb == 0.0 {
        return 0.0;
    }
    (ab / (aa.sqrt() * bb.sqrt())) as f32
}

impl Store {
    /// A consolidation pass over namespace `ns` with `consolidator` (D311 §4, T3): see
    /// [`Consolidate`]. Episodic memories are grouped by session, then by similarity; each
    /// group goes to the consolidator outside any transaction, and what it returns commits in
    /// one transaction per group, with [`LinkType::DERIVED_FROM`] links to the group and the
    /// group's provenance. A group whose memories changed meanwhile, or whose commit
    /// conflicts (another pass took it), is skipped, not retried.
    ///
    /// Runs on demand: unlike forgetting, there's no thread, since the consolidator is the
    /// application's.
    pub fn consolidate<'a>(
        &'a self,
        ns: &'a str,
        consolidator: &'a dyn Consolidator,
    ) -> Consolidate<'a> {
        Consolidate {
            store: self,
            ns,
            consolidator,
            policy: None,
            now_us: None,
            after: None,
        }
    }
}
