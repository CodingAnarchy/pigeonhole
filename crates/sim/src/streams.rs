//! Per-stream crash recovery for the model (decisions D42, D83, D84).
//!
//! Durability is per WAL stream, one per shard. After a crash each stream keeps a prefix of
//! its records. A single-shard commit has one record in its stream and survives iff that
//! record does. A cross-shard commit has a PREPARE in every participant's stream and a COMMIT
//! in the coordinator's, and survives iff all of them do (D83); otherwise it is lost whole.
//! There is no global prefix across streams, so [`Model::crash_window`] only describes the
//! single-stream case.

use pigeonhole_format::{Durability, Timestamp};
use pigeonhole_io::sim::CrashKind;

use crate::model::{Model, ModelOp};

/// Where a commit's records live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitStreams {
    /// One record in this stream.
    Single(usize),
    /// A PREPARE in each participant's stream, then a COMMIT in the coordinator's. The
    /// coordinator may also be a participant, in which case its stream holds both records.
    Cross {
        /// Streams holding a PREPARE.
        participants: Vec<usize>,
        /// Stream holding the COMMIT.
        coordinator: usize,
    },
}

/// A model commit together with the WAL streams it wrote to, in commit order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCommit {
    /// The mutations.
    pub ops: Vec<ModelOp>,
    /// The commit timestamp.
    pub commit_ts: Timestamp,
    /// The durability the commit was acknowledged with ([`Durability::None`] if it was in
    /// flight at the crash).
    pub durability: Durability,
    /// The streams holding its records.
    pub streams: CommitStreams,
}

/// The records `commit` appends, as the streams they go to, in append order.
fn record_streams(commit: &StreamCommit) -> Vec<usize> {
    match &commit.streams {
        CommitStreams::Single(s) => vec![*s],
        CommitStreams::Cross {
            participants,
            coordinator,
        } => participants
            .iter()
            .copied()
            .chain(std::iter::once(*coordinator))
            .collect(),
    }
}

/// How many records each of `streams` streams holds once every commit is written; the
/// largest useful `survivors` entry per stream.
///
/// # Panics
/// If a commit names a stream `>= streams`.
pub fn stream_lengths(commits: &[StreamCommit], streams: usize) -> Vec<usize> {
    let mut len = vec![0; streams];
    for c in commits {
        for s in record_streams(c) {
            len[s] += 1;
        }
    }
    len
}

/// The commits recovered when stream `s` keeps its first `survivors[s]` records: indices into
/// `commits`, ascending.
///
/// ```
/// use pigeonhole_format::Durability;
/// use pigeonhole_sim::{CommitStreams, StreamCommit, recovered_commits};
///
/// let c = |streams| StreamCommit {
///     ops: vec![], commit_ts: 1, durability: Durability::Buffered, streams,
/// };
/// let commits = [
///     c(CommitStreams::Single(0)),                                  // 0: record 0 of stream 0
///     c(CommitStreams::Cross { participants: vec![0, 1], coordinator: 1 }),
///     c(CommitStreams::Single(1)),
/// ];
/// // Stream 1 lost its COMMIT (its second record): the cross-shard commit is lost whole,
/// // while commit 2 on the same stream is lost with the suffix.
/// assert_eq!(recovered_commits(&commits, &[2, 1]), vec![0]);
/// assert_eq!(recovered_commits(&commits, &[2, 3]), vec![0, 1, 2]);
/// ```
///
/// # Panics
/// If a commit names a stream `>= survivors.len()`.
pub fn recovered_commits(commits: &[StreamCommit], survivors: &[usize]) -> Vec<usize> {
    let mut next = vec![0usize; survivors.len()];
    let mut out = Vec::new();
    for (i, c) in commits.iter().enumerate() {
        let mut all = true;
        for s in record_streams(c) {
            all &= next[s] < survivors[s];
            next[s] += 1;
        }
        if all {
            out.push(i);
        }
    }
    out
}

/// Checks that every commit acknowledged at the floor level of `kind` or stronger (`Buffered`
/// after a process crash, `GroupSync` after power loss; D42, D84) is among `recovered`
/// (ascending indices, as [`recovered_commits`] returns). `Err` holds the first lost one.
///
/// ```
/// use pigeonhole_format::Durability;
/// use pigeonhole_io::sim::CrashKind;
/// use pigeonhole_sim::{CommitStreams, StreamCommit, check_acknowledged_survive};
///
/// let c = |durability| StreamCommit {
///     ops: vec![], commit_ts: 1, durability, streams: CommitStreams::Single(0),
/// };
/// let commits = [c(Durability::Buffered), c(Durability::Sync)];
/// // Only the Sync commit (index 1) survived: fine after power loss, where Buffered is not
/// // promised, but a lost acknowledged commit after a process crash.
/// assert_eq!(check_acknowledged_survive(&commits, &[1], CrashKind::Power), Ok(()));
/// assert_eq!(check_acknowledged_survive(&commits, &[1], CrashKind::Process), Err(0));
/// assert_eq!(check_acknowledged_survive(&commits, &[0], CrashKind::Power), Err(1));
/// ```
pub fn check_acknowledged_survive(
    commits: &[StreamCommit],
    recovered: &[usize],
    kind: CrashKind,
) -> Result<(), usize> {
    let floor = match kind {
        CrashKind::Process => Durability::Buffered,
        CrashKind::Power => Durability::GroupSync,
    };
    match commits
        .iter()
        .enumerate()
        .find(|(i, c)| c.durability >= floor && recovered.binary_search(i).is_err())
    {
        Some((i, _)) => Err(i),
        None => Ok(()),
    }
}

impl Model {
    /// Rebuilds a model from the commits that survived a crash, in order. `setup` creates the
    /// tables. Seqnos are renumbered from 1, so they match the recovered store's only if it
    /// also renumbers; compare reads at the latest snapshot.
    ///
    /// ```
    /// use pigeonhole_format::Durability;
    /// use pigeonhole_sim::{CommitStreams, Model, ModelFamily, ModelOp, StreamCommit};
    ///
    /// let put = |v: &[u8], s| StreamCommit {
    ///     ops: vec![ModelOp::Put {
    ///         table: "t".into(), row: b"r".to_vec(), family: "f".into(),
    ///         qualifier: b"q".to_vec(), ts: None, value: v.to_vec(),
    ///     }],
    ///     commit_ts: 10, durability: Durability::Sync, streams: CommitStreams::Single(s),
    /// };
    /// let commits = [put(b"a", 0), put(b"b", 1)];
    /// let m = Model::from_commits(
    ///     |m| m.create_table("t", vec![ModelFamily { name: "f".into(), ..Default::default() }]),
    ///     &commits[..1],
    /// );
    /// assert_eq!(m.get("t", b"r", "f", b"q", m.snapshot(), 20).unwrap().value, b"a");
    /// ```
    ///
    /// # Panics
    /// If a commit is invalid (see [`Model::commit`]).
    pub fn from_commits<'a>(
        setup: impl FnOnce(&mut Model),
        commits: impl IntoIterator<Item = &'a StreamCommit>,
    ) -> Model {
        let mut m = Model::new();
        setup(&mut m);
        for c in commits {
            m.commit(&c.ops, c.commit_ts, c.durability);
        }
        m
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn commit(durability: Durability, streams: CommitStreams) -> StreamCommit {
        StreamCommit {
            ops: Vec::new(),
            commit_ts: 1,
            durability,
            streams,
        }
    }

    /// Brute force: list every stream's records, cut each to its prefix, and keep a commit iff
    /// all its records are still listed.
    fn reference(commits: &[StreamCommit], survivors: &[usize]) -> Vec<usize> {
        let mut lists: Vec<Vec<(usize, usize)>> = vec![Vec::new(); survivors.len()];
        for (i, c) in commits.iter().enumerate() {
            match &c.streams {
                CommitStreams::Single(s) => lists[*s].push((i, 0)),
                CommitStreams::Cross {
                    participants,
                    coordinator,
                } => {
                    for p in participants {
                        lists[*p].push((i, 0));
                    }
                    lists[*coordinator].push((i, 1));
                }
            }
        }
        for (l, keep) in lists.iter_mut().zip(survivors) {
            l.truncate(*keep);
        }
        (0..commits.len())
            .filter(|&i| {
                let has = |s: usize, k: usize| lists[s].contains(&(i, k));
                match &commits[i].streams {
                    CommitStreams::Single(s) => has(*s, 0),
                    CommitStreams::Cross {
                        participants,
                        coordinator,
                    } => participants.iter().all(|p| has(*p, 0)) && has(*coordinator, 1),
                }
            })
            .collect()
    }

    fn arb_durability() -> impl Strategy<Value = Durability> {
        prop_oneof![
            Just(Durability::None),
            Just(Durability::Buffered),
            Just(Durability::GroupSync),
            Just(Durability::Sync),
        ]
    }

    /// Streams, commits (single or cross-shard) and one survivor draw per stream.
    fn arb_case() -> impl Strategy<Value = (Vec<StreamCommit>, Vec<usize>)> {
        (1usize..=8).prop_flat_map(|n| {
            let streams = prop_oneof![
                (0..n).prop_map(CommitStreams::Single),
                (proptest::collection::btree_set(0..n, 1..=n), 0..n).prop_map(
                    |(p, coordinator)| {
                        CommitStreams::Cross {
                            participants: p.into_iter().collect(),
                            coordinator,
                        }
                    }
                ),
            ];
            (
                proptest::collection::vec((arb_durability(), streams), 0..40),
                proptest::collection::vec(any::<u16>(), n),
            )
                .prop_map(move |(cs, draws)| {
                    let commits: Vec<_> = cs.into_iter().map(|(d, s)| commit(d, s)).collect();
                    let lens = stream_lengths(&commits, n);
                    let survivors = draws
                        .iter()
                        .zip(&lens)
                        .map(|(d, l)| usize::from(*d) % (l + 1))
                        .collect();
                    (commits, survivors)
                })
        })
    }

    proptest! {
        #[test]
        fn matches_brute_force((commits, survivors) in arb_case()) {
            prop_assert_eq!(
                recovered_commits(&commits, &survivors),
                reference(&commits, &survivors)
            );
        }

        #[test]
        fn full_streams_recover_everything_and_none_recovers_nothing(
            (commits, _) in arb_case(),
        ) {
            let n = commits
                .iter()
                .flat_map(record_streams)
                .max()
                .map_or(1, |m| m + 1);
            let full = stream_lengths(&commits, n);
            prop_assert_eq!(
                recovered_commits(&commits, &full),
                (0..commits.len()).collect::<Vec<_>>()
            );
            prop_assert!(recovered_commits(&commits, &vec![0; n]).is_empty());
        }

        /// Whatever survives, single-shard commits on one stream form a prefix of that stream.
        #[test]
        fn single_shard_survivors_are_a_stream_prefix((commits, survivors) in arb_case()) {
            let rec = recovered_commits(&commits, &survivors);
            for s in 0..survivors.len() {
                let on_s: Vec<bool> = commits
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.streams == CommitStreams::Single(s))
                    .map(|(i, _)| rec.contains(&i))
                    .collect();
                prop_assert!(on_s.windows(2).all(|w| w[0] || !w[1]));
            }
        }
    }

    #[test]
    fn acknowledged_check_flags_a_lost_commit() {
        let commits = [
            commit(Durability::Buffered, CommitStreams::Single(0)),
            commit(Durability::GroupSync, CommitStreams::Single(1)),
        ];
        // The GroupSync commit on stream 1 survives while the Buffered one on stream 0 is lost:
        // legal after power loss, a violation after a process crash.
        let rec = recovered_commits(&commits, &[0, 1]);
        assert_eq!(rec, vec![1]);
        assert_eq!(
            check_acknowledged_survive(&commits, &rec, CrashKind::Power),
            Ok(())
        );
        assert_eq!(
            check_acknowledged_survive(&commits, &rec, CrashKind::Process),
            Err(0)
        );
    }
}
