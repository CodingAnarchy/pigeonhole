//! Per-stream crash recovery for the model (decisions D42, D83, D84).
//!
//! Durability is per WAL stream, one per shard. After a crash each stream keeps a prefix of
//! its records. A single-shard commit has one record in its stream and survives iff that
//! record does. A cross-shard commit has a PREPARE in every participant's stream and a COMMIT
//! in the coordinator's, and survives iff all of them do (D83); otherwise it is lost whole.
//! There is no global prefix across streams, so [`Model::crash_window`] only describes the
//! single-stream case.

use std::collections::BTreeSet;

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

/// One record in a WAL stream, naming the commit it belongs to by index.
///
/// A cross-shard commit's PREPAREs and COMMIT need not be adjacent in their streams: the
/// engine logs the COMMIT only once every participant's PREPARE is durable, so other commits'
/// records may land in between.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamRecord {
    /// A single-shard commit's only record.
    Single(usize),
    /// A participant's PREPARE of cross-shard commit `.0`.
    Prepare(usize),
    /// The coordinator's COMMIT of a cross-shard commit.
    Commit {
        /// The commit.
        commit: usize,
        /// Streams that must hold its PREPARE (the COMMIT record names its participants).
        participants: Vec<usize>,
    },
}

/// Lays `commits` out as per-stream record lists, each commit's records appended in turn
/// (so a cross-shard commit's records are adjacent on a stream that holds several of them).
/// Callers with overlapping commits build the lists themselves in append order.
///
/// # Panics
/// If a commit names a stream `>= streams`.
pub fn commit_records(commits: &[StreamCommit], streams: usize) -> Vec<Vec<StreamRecord>> {
    let mut out = vec![Vec::new(); streams];
    for (i, c) in commits.iter().enumerate() {
        match &c.streams {
            CommitStreams::Single(s) => out[*s].push(StreamRecord::Single(i)),
            CommitStreams::Cross {
                participants,
                coordinator,
            } => {
                for p in participants {
                    out[*p].push(StreamRecord::Prepare(i));
                }
                out[*coordinator].push(StreamRecord::Commit {
                    commit: i,
                    participants: participants.clone(),
                });
            }
        }
    }
    out
}

/// How many records each of `streams` streams holds once every commit is written; the
/// largest useful `survivors` entry per stream.
///
/// # Panics
/// If a commit names a stream `>= streams`.
pub fn stream_lengths(commits: &[StreamCommit], streams: usize) -> Vec<usize> {
    commit_records(commits, streams)
        .iter()
        .map(Vec::len)
        .collect()
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
    recovered_from_records(&commit_records(commits, survivors.len()), survivors)
}

/// The commits recovered when stream `s` keeps its first `survivors[s]` of `streams[s]`, its
/// records in append order: commit indices, ascending. A single-shard commit survives iff its
/// record does; a cross-shard commit iff its COMMIT survives and every participant it names
/// still holds its PREPARE (D83). A commit whose COMMIT was never appended is lost. A
/// `survivors` entry past the end of its stream keeps the whole stream.
///
/// ```
/// use pigeonhole_sim::{StreamRecord::*, recovered_from_records};
///
/// // Commit 0 is cross-shard over streams 0 and 1, coordinated by stream 0; commit 1 is a
/// // single-shard commit that landed on stream 0 between 0's PREPARE and COMMIT.
/// let streams = [
///     vec![Prepare(0), Single(1), Commit { commit: 0, participants: vec![0, 1] }],
///     vec![Prepare(0)],
/// ];
/// assert_eq!(recovered_from_records(&streams, &[3, 1]), vec![0, 1]);
/// // The COMMIT is lost but commit 1 sits before it: only commit 1 survives.
/// assert_eq!(recovered_from_records(&streams, &[2, 1]), vec![1]);
/// // Stream 1 lost its PREPARE: commit 0 is lost whole although its COMMIT survived.
/// assert_eq!(recovered_from_records(&streams, &[3, 0]), vec![1]);
/// ```
pub fn recovered_from_records(streams: &[Vec<StreamRecord>], survivors: &[usize]) -> Vec<usize> {
    let kept = || {
        streams
            .iter()
            .zip(survivors)
            .enumerate()
            .flat_map(|(s, (recs, n))| recs.iter().take(*n).map(move |r| (s, r)))
    };
    let prepared: BTreeSet<(usize, usize)> = kept()
        .filter_map(|(s, r)| match r {
            StreamRecord::Prepare(c) => Some((*c, s)),
            _ => None,
        })
        .collect();
    kept()
        .filter_map(|(_, r)| match r {
            StreamRecord::Single(c) => Some(*c),
            StreamRecord::Commit {
                commit,
                participants,
            } if participants
                .iter()
                .all(|p| prepared.contains(&(*commit, *p))) =>
            {
                Some(*commit)
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
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
    ///     |m| m.create_table("t", vec![ModelFamily::new("f")]),
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
                .flat_map(|c| match &c.streams {
                    CommitStreams::Single(s) => vec![*s],
                    CommitStreams::Cross { participants, coordinator } => {
                        participants.iter().copied().chain([*coordinator]).collect()
                    }
                })
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

    /// Ground truth for a commit that may be cross-shard: its participants (empty for a
    /// single-shard commit), its coordinator or only stream, and its records' append order.
    type Truth = (Vec<usize>, usize);

    /// Random commits over `n` streams whose records are merged in a random order that keeps
    /// each commit's own order (PREPAREs, then COMMIT), so records of different commits
    /// interleave freely. Some commits are cut short, as when a crash caught them in flight:
    /// `cut` of their records were appended (the COMMIT only if all were).
    fn arb_interleaved() -> impl Strategy<Value = (Vec<Truth>, Vec<Vec<StreamRecord>>, Vec<usize>)>
    {
        (1usize..=8).prop_flat_map(|n| {
            let truth = prop_oneof![
                (0..n).prop_map(|s| (Vec::new(), s)),
                (proptest::collection::btree_set(0..n, 1..=n), 0..n)
                    .prop_map(|(p, c)| (p.into_iter().collect::<Vec<_>>(), c)),
            ];
            (
                proptest::collection::vec((truth, any::<bool>()), 0..30),
                proptest::collection::vec(any::<u16>(), 200),
                proptest::collection::vec(any::<u16>(), n),
            )
                .prop_map(move |(commits, picks, draws)| {
                    let mut queues: Vec<std::collections::VecDeque<(usize, StreamRecord)>> =
                        commits
                            .iter()
                            .enumerate()
                            .map(|(i, ((p, c), in_flight))| {
                                let mut q: std::collections::VecDeque<_> = if p.is_empty() {
                                    [(*c, StreamRecord::Single(i))].into()
                                } else {
                                    p.iter().map(|s| (*s, StreamRecord::Prepare(i))).collect()
                                };
                                if !p.is_empty() {
                                    q.push_back((
                                        *c,
                                        StreamRecord::Commit {
                                            commit: i,
                                            participants: p.clone(),
                                        },
                                    ));
                                }
                                if *in_flight {
                                    q.pop_back();
                                }
                                q
                            })
                            .collect();
                    let mut streams = vec![Vec::new(); n];
                    let mut picks = picks.into_iter().cycle();
                    loop {
                        let live: Vec<usize> = (0..queues.len())
                            .filter(|&i| !queues[i].is_empty())
                            .collect();
                        if live.is_empty() {
                            break;
                        }
                        let i = live[usize::from(picks.next().unwrap()) % live.len()];
                        let (s, r) = queues[i].pop_front().unwrap();
                        streams[s].push(r);
                    }
                    let survivors = draws
                        .iter()
                        .zip(&streams)
                        .map(|(d, l): (_, &Vec<_>)| usize::from(*d) % (l.len() + 1))
                        .collect();
                    (
                        commits.into_iter().map(|(t, _)| t).collect(),
                        streams,
                        survivors,
                    )
                })
        })
    }

    proptest! {
        /// Brute force against the ground truth, not the record kinds: a commit survives iff
        /// each of its PREPAREs (or its single record) and its COMMIT are in a kept prefix.
        #[test]
        fn interleaved_records_match_brute_force(
            (truth, streams, survivors) in arb_interleaved(),
        ) {
            let kept: Vec<Vec<&StreamRecord>> = streams
                .iter()
                .zip(&survivors)
                .map(|(l, n)| l.iter().take(*n).collect())
                .collect();
            let has = |s: usize, r: &StreamRecord| kept[s].contains(&r);
            let expect: Vec<usize> = truth
                .iter()
                .enumerate()
                .filter(|(i, (p, c))| {
                    if p.is_empty() {
                        has(*c, &StreamRecord::Single(*i))
                    } else {
                        p.iter().all(|s| has(*s, &StreamRecord::Prepare(*i)))
                            && has(*c, &StreamRecord::Commit { commit: *i, participants: p.clone() })
                    }
                })
                .map(|(i, _)| i)
                .collect();
            prop_assert_eq!(recovered_from_records(&streams, &survivors), expect);
        }

        /// Survivors per stream are a record prefix, so keeping more never loses a commit.
        #[test]
        fn longer_prefixes_recover_more((_, streams, survivors) in arb_interleaved()) {
            let base = recovered_from_records(&streams, &survivors);
            for s in 0..survivors.len() {
                let mut more = survivors.clone();
                more[s] += 1;
                let rec = recovered_from_records(&streams, &more);
                prop_assert!(base.iter().all(|c| rec.contains(c)));
            }
        }
    }

    #[test]
    fn commit_level_is_the_record_level_in_commit_order() {
        let commits = [
            commit(
                Durability::Sync,
                CommitStreams::Cross {
                    participants: vec![0, 1],
                    coordinator: 0,
                },
            ),
            commit(Durability::Sync, CommitStreams::Single(0)),
        ];
        let recs = commit_records(&commits, 2);
        assert_eq!(recs[0].len(), 3);
        assert_eq!(stream_lengths(&commits, 2), vec![3, 1]);
        for a in 0..=3 {
            for b in 0..=1 {
                assert_eq!(
                    recovered_commits(&commits, &[a, b]),
                    recovered_from_records(&recs, &[a, b])
                );
            }
        }
    }

    #[test]
    fn a_commit_never_logged_is_lost() {
        // Only PREPAREs were appended before the crash.
        let streams = [
            vec![StreamRecord::Prepare(0)],
            vec![StreamRecord::Prepare(0)],
        ];
        assert!(recovered_from_records(&streams, &[1, 1]).is_empty());
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
