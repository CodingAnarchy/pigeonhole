use super::*;

/// A reader's read-ahead budget (D320): the bytes of records read but not delivered that it
/// keeps (`ready` and `held`). Past it, a pass keeps only the smallest keys.
pub(super) const READ_AHEAD_BYTES: u64 = 64 << 20;

/// What one entry of `ready` costs in its B-tree, with its start's entry in `starts`: each
/// key and value twice over, since a node of entries appended in order (keys only grow
/// there) is about half full (#1637).
pub(super) const READY_ENTRY: u64 =
    2 * (std::mem::size_of::<(Seqno, Pending)>() + START_ENTRY) as u64;

/// The same for one share in `held`.
pub(super) const HELD_ENTRY: u64 =
    2 * (std::mem::size_of::<((Seqno, usize), Held)>() + START_ENTRY) as u64;

/// One entry of a stream's `starts`.
pub(super) const START_ENTRY: usize = std::mem::size_of::<(Lsn, u32)>();

/// The read-ahead bytes a record kept in `ready` counts for: what it really holds, its map
/// entry and its allocations (#1637; a decided round's shares count in `held`).
pub(super) fn pending_bytes(p: &Pending) -> u64 {
    READY_ENTRY
        + match p {
            Pending::Single { changes, .. } => changes.memory(),
            Pending::Decided { participants, .. } => {
                alloc_size(participants.capacity() * std::mem::size_of::<StreamId>())
            }
        }
}

/// The read-ahead bytes a held share counts for, as for [`pending_bytes`].
pub(super) fn held_bytes(h: &Held) -> u64 {
    if h.external {
        return 0;
    }
    HELD_ENTRY + h.changes.memory()
}

/// The read-ahead cap's state (D320). A pass reads every stream to its end, as without a
/// cap, but keeps only records with keys at or below the cutoff `C`, which falls whenever
/// what's kept passes the budget: the largest keys go first. Every record with a key at or
/// below `C` read since the last rewind is kept, so the reader delivers up to the smaller
/// of the frontier and `C`. A PREPARE's key is its round's apply seqno, above its id (both
/// come from the one allocator, the id first), so a share counts at `id + 1`, a lower bound.
#[derive(Debug)]
pub(super) struct ReadAhead {
    /// The bytes kept now, over `ready` and `held`.
    pub(super) bytes: u64,
    /// The budget for the next eviction: `base`, doubled after a pass that left nothing to
    /// deliver below a cutoff under the frontier (liveness), and back to `base` on a delivery.
    pub(super) budget: u64,
    pub(super) base: u64,
    /// `C`: every record with a key at or below it read since the last rewind is kept.
    /// `Seqno::MAX` while nothing was passed over.
    pub(super) cutoff: Seqno,
    /// Per stream, the earliest start of a record passed over since the last rewind: where
    /// the stream must be read again from (and the token's hint at most this).
    pub(super) resume: Vec<Option<Lsn>>,
    /// Rounds passed over since the last rewind (their Decision skipped, or their shares or
    /// Decision evicted): their shares read later are passed over too, never held. Rule C
    /// would otherwise drop a held share whose Decision this pass skipped, and the Decision
    /// read after the rewind would then expire the feed. Only ids below the cutoff are kept
    /// (simp724's S1 on #1622): a share with an id at or above it is passed over by its id
    /// anyway, and the cutoff only falls until the rewind. So it holds only rounds in flight
    /// across the cutoff, not every round of a backlog.
    pub(super) skipped_rounds: std::collections::BTreeSet<Seqno>,
    /// The decided rounds in `ready`: id to apply seqno.
    pub(super) decided: HashMap<Seqno, Seqno>,
}

impl ReadAhead {
    pub(super) fn new(streams: usize, base: u64) -> Self {
        Self {
            bytes: 0,
            budget: base,
            base,
            cutoff: Seqno::MAX,
            resume: vec![None; streams],
            skipped_rounds: std::collections::BTreeSet::new(),
            decided: HashMap::new(),
        }
    }

    /// Stream `s`'s record at `start` was passed over.
    pub(super) fn skip(&mut self, s: usize, start: Lsn) {
        let r = &mut self.resume[s];
        *r = Some(r.map_or(start, |r| r.min(start)));
    }

    /// Lowers the cutoff to at most `c`, forgetting the passed-over rounds its id test now
    /// covers.
    pub(super) fn lower_cutoff(&mut self, c: Seqno) {
        if c < self.cutoff {
            self.cutoff = c;
            self.skipped_rounds.split_off(&c);
        }
    }

    /// Whether anything was passed over since the last rewind.
    pub(super) fn capped(&self) -> bool {
        self.cutoff != Seqno::MAX
    }
}

impl FeedReader {
    /// D320: reads every stream again from its hint (the earliest record not delivered,
    /// kept or passed over), dropping what's kept: the reads of a resume from this reader's
    /// token. Returns `false` once the feed expired (retention passed a hint).
    pub(super) fn rewind(&mut self) -> Result<bool> {
        let hints: Vec<Lsn> = (0..self.streams.len()).map(|s| self.hint(s)).collect();
        for (s, &at) in hints.iter().enumerate() {
            if at.epoch() != 0 && at.epoch() < self.floor(s) {
                self.expired = Some(expiry(
                    "retention passed the feed: a segment it was reading was recycled",
                ));
                return Ok(false);
            }
        }
        for (s, at) in hints.into_iter().enumerate() {
            self.streams[s].cursor = self.open_cursor(s, at)?;
            self.streams[s].index.trim(at);
        }
        self.ready.clear();
        self.held.clear();
        self.silent_seen.clear();
        self.xids.clear();
        self.starts = Starts::default();
        let a = &mut self.ahead;
        a.bytes = 0;
        a.cutoff = Seqno::MAX;
        a.resume.iter_mut().for_each(|r| *r = None);
        a.skipped_rounds.clear();
        a.decided.clear();
        self.read_through = None;
        Ok(true)
    }

    /// D320: evicts the largest-keyed records kept while the read-ahead is over its budget,
    /// or past the cutoff, lowering the cutoff below each. A held share counts at its id + 1
    /// (its round's apply seqno is above its id), and goes with every share of its round; a
    /// decided round goes with its shares. An external share (D329) never causes an eviction
    /// or lowers the cutoff, but once the cutoff has fallen, those held at or past it are
    /// passed over too ([`evict_external_past_cutoff`](Self::evict_external_past_cutoff)).
    /// Returns whether it evicted anything.
    ///
    /// Run after every record a pass keeps: the check stays inline and the eviction is out
    /// of line, so a reader that keeps up pays only the check (callgrind's feed-drain on
    /// x86, #1922: with the eviction in it, `trim` stopped being inlined).
    #[inline]
    pub(super) fn trim(&mut self) -> bool {
        let (top_ready, top_held) = self.trim_tops();
        if !self.trim_due(top_ready, top_held) {
            return false;
        }
        self.evict_over();
        true
    }

    /// The largest key kept, and the largest held share's id + 1 (not a share of a round
    /// whose id + 1 is at or below `delivered`, simp724's N1 on #1622: a rewind can re-read a
    /// delivered round's share without its Decision, and evicting it would put the cutoff at
    /// or below `delivered` for nothing; rule C drops it). External shares are left out
    /// (ICR 0140 E3): [`evict_external_past_cutoff`](Self::evict_external_past_cutoff) passes
    /// over those a fallen cutoff reaches.
    #[inline]
    pub(super) fn trim_tops(&self) -> (Option<Seqno>, Option<Seqno>) {
        let top_ready = self.ready.last_key_value().map(|(&k, _)| k);
        let top_held = self
            .held
            .range((self.delivered, 0)..)
            .rev()
            .find(|(_, h)| !h.external)
            .map(|(&(id, _), _)| id.saturating_add(1));
        (top_ready, top_held)
    }

    /// Whether [`trim`](Self::trim) has anything to evict.
    #[inline]
    pub(super) fn trim_due(&self, top_ready: Option<Seqno>, top_held: Option<Seqno>) -> bool {
        let c = self.ahead.cutoff;
        self.ahead.bytes > self.ahead.budget
            || top_ready.is_some_and(|k| k > c)
            || top_held.is_some_and(|k| k > c)
    }

    /// [`trim`](Self::trim)'s eviction, once it's due: the largest keys first, then the
    /// external shares held at or past where the cutoff ended (#2180).
    #[cold]
    #[inline(never)]
    pub(super) fn evict_over(&mut self) {
        // The cutoff falls only here: the streams' skip indexes are built from now on
        // (#1827), and a reader that never gets here pays nothing for them per record.
        self.indexing = true;
        loop {
            let (top_ready, top_held) = self.trim_tops();
            if !self.trim_due(top_ready, top_held) {
                break;
            }
            match (top_ready, top_held) {
                (Some(k), None) => self.evict_ready(k),
                (Some(k), Some(h)) if k >= h => self.evict_ready(k),
                (_, Some(h)) => {
                    let id = h - 1;
                    // A decided round's apply seqno is in `ready`, at least id + 1: the arms
                    // above evict it first, with its shares.
                    debug_assert!(!self.ahead.decided.contains_key(&id), "round {id} decided");
                    self.evict_round(id);
                    self.ahead.lower_cutoff(id);
                }
                (None, None) => break,
            }
        }
        self.evict_external_past_cutoff();
    }

    /// #2180: passes over the external shares held with an id at or past the cutoff, once it
    /// has fallen. An external share never causes an eviction or lowers the cutoff
    /// ([`trim_tops`](Self::trim_tops)), but one held before the cutoff fell to its id is no
    /// different from one read after, which is passed over by its id: its Decision is keyed
    /// past the cutoff, so a chunk jump may pass the Decision over (`Chunk::min_key`), and
    /// rule C would then drop the share as aborted and the Decision, read later, expire the
    /// feed as a gap. Passed over, the share is read again after the rewind.
    fn evict_external_past_cutoff(&mut self) {
        let cutoff = self.ahead.cutoff;
        let ids: Vec<Seqno> = self
            .held
            .range((cutoff, 0)..)
            .filter(|(_, h)| h.external)
            .map(|(&(id, _), _)| id)
            .collect();
        if !ids.is_empty() {
            // One per share (a round's shares on several streams count each).
            self.shared
                .feed_external_evictions
                .fetch_add(ids.len() as u64, Ordering::Relaxed);
        }
        for id in ids {
            self.evict_round(id);
        }
    }

    /// Evicts `ready`'s record at `key` (and a decided round's shares): passed over, to be
    /// read again after a rewind.
    pub(super) fn evict_ready(&mut self, key: Seqno) {
        let Some(p) = self.ready.remove(&key) else {
            return;
        };
        self.forget_ready(&p);
        let (stream, start) = p.at();
        self.ahead.skip(stream, start);
        match p {
            // A round's note goes with its shares (`remove_held`).
            Pending::Decided { id, .. } => self.evict_round(id),
            // A single's goes here: it's read again, and noted again, past the cutoff
            // (ai1249's nit on #2068).
            Pending::Single { .. } => {
                self.silent_seen.remove(&key);
                self.xids.remove(&key);
            }
        }
        self.ahead.lower_cutoff(key - 1);
    }

    /// Evicts round `id`'s held shares, and passes over any read later in this pass.
    pub(super) fn evict_round(&mut self, id: Seqno) {
        for h in self.remove_held(id) {
            self.ahead.skip(h.stream, h.start);
        }
        if id < self.ahead.cutoff {
            self.ahead.skipped_rounds.insert(id);
        }
    }
}
