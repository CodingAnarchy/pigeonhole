//! Transactions across partitions (D329 PR 2a): a transaction begun with `spans_partitions`
//! reads and writes rows of any partition, and commits them atomically. `Three::start`:
//! node a serves p1 (users < "m"), b serves p2 (users >= "m"), c only routes.

use super::*;

pub(super) async fn begin_spanning(port: u16, row: &str) -> Result<String, tonic::Status> {
    let r = super::super::proto::BeginRequest {
        durability: 0,
        route: route_row(row),
        spans_partitions: true,
    };
    Ok(txns(port).await.begin(r).await?.into_inner().txn_id)
}

pub(super) async fn commit_parts(
    port: u16,
    id: &str,
    rows: Vec<super::super::proto::RowWrites>,
) -> Result<super::super::proto::TxnCommitResponse, tonic::Status> {
    let r = super::super::proto::TxnCommitRequest {
        txn_id: id.into(),
        rows,
    };
    Ok(txns(port).await.commit(r).await?.into_inner())
}

async fn channel(port: u16) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

pub(super) async fn abort(port: u16, id: &str) -> Result<(), tonic::Status> {
    let r = super::super::proto::TxnAbortRequest { txn_id: id.into() };
    txns(port).await.abort(r).await.map(drop)
}

#[test]
fn a_spanning_transaction_commits_rows_of_two_partitions_atomically() {
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        // Home p2 (by "zed"), begun through c: its id is p2's, with the history epoch.
        let id = begin_spanning(c, "zed").await.unwrap();
        assert!(id.starts_with("p2:h"), "{id}");
        // A row of p1 (a branch) and one of p2 (the home), through different nodes.
        txn_write(a, &id, vec![put_row("alice", "1")], None)
            .await
            .unwrap();
        txn_write(b, &id, vec![put_row("yan", "1")], None)
            .await
            .unwrap();
        // Nothing is visible before the commit.
        assert_eq!(get_row(c, "alice").await.unwrap(), None);
        // The transaction reads its own branch's row... at its snapshot (a write isn't read
        // back before commit, as in-process), and a row through another node.
        assert_eq!(txn_get(c, &id, "bob").await.unwrap(), None);
        let reply = commit_parts(a, &id, vec![put_row("bob", "2")])
            .await
            .unwrap();
        let parts: Vec<(&str, bool)> = reply
            .parts
            .iter()
            .map(|p| (p.partition.as_str(), p.applied))
            .collect();
        assert_eq!(parts, [("p2", true), ("p1", true)], "{reply:?}");
        assert!(reply.parts.iter().all(|p| p.seqno > 0), "{reply:?}");
        assert_eq!(
            reply.commit.as_ref().map(|c| c.seqno),
            Some(reply.parts[0].seqno)
        );
        for (row, v) in [("alice", "1"), ("bob", "2"), ("yan", "1")] {
            assert_eq!(get_row(c, row).await.unwrap(), Some(v.as_bytes().to_vec()));
        }
        // A Commit sent again answers the first outcome, parts and all.
        let again = commit_parts(b, &id, Vec::new()).await.unwrap();
        assert_eq!(again, reply);
        // The branch's rows are free again: a plain write to them commits at once.
        put(c, "alice", "3").await;
    });
}

#[test]
fn without_the_opt_in_a_row_of_another_partition_is_still_cross_partition() {
    let t = Three::start(["", "", ""]);
    let [_, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin(c, "zed", None).await.unwrap();
        assert!(!id.contains(":h"), "{id}");
        let e = txn_write(c, &id, vec![put_row("alice", "1")], None)
            .await
            .unwrap_err();
        assert_eq!(code(&e), "CrossPartition", "{e}");
    });
}

#[test]
fn a_read_only_spanning_transaction_commits_with_no_writes_and_no_parts() {
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        put(c, "alice", "1").await;
        put(c, "zed", "1").await;
        let id = begin_spanning(a, "zed").await.unwrap();
        assert_eq!(txn_get(b, &id, "alice").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(txn_get(c, &id, "zed").await.unwrap(), Some(b"1".to_vec()));
        let reply = commit_parts(c, &id, Vec::new()).await.unwrap();
        assert!(reply.parts.is_empty(), "{reply:?}");
        // Its holds are gone: both rows take writes at once.
        put(c, "alice", "2").await;
        put(c, "zed", "2").await;
    });
}

#[test]
fn a_read_changed_on_a_branch_or_at_home_fails_the_commit_and_writes_nothing() {
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        put(c, "alice", "0").await;
        put(c, "zed", "0").await;
        // A branch's read changed: its prepare refuses, so the commit is a Conflict.
        let id = begin_spanning(c, "zed").await.unwrap();
        assert_eq!(txn_get(c, &id, "alice").await.unwrap(), Some(b"0".to_vec()));
        txn_write(c, &id, vec![put_row("yan", "1")], None)
            .await
            .unwrap();
        put(a, "alice", "changed").await;
        let e = commit_parts(b, &id, Vec::new()).await.unwrap_err();
        assert_eq!(code(&e), "Conflict", "{e}");
        assert_eq!(get_row(c, "yan").await.unwrap(), None);
        // The home's read changed: the commit point refuses, and the branch that prepared
        // is aborted, its row free.
        let id = begin_spanning(c, "zed").await.unwrap();
        assert_eq!(txn_get(c, &id, "zed").await.unwrap(), Some(b"0".to_vec()));
        txn_write(c, &id, vec![put_row("bob", "1")], None)
            .await
            .unwrap();
        put(b, "zed", "changed").await;
        let e = commit_parts(a, &id, Vec::new()).await.unwrap_err();
        assert_eq!(code(&e), "Conflict", "{e}");
        assert_eq!(get_row(c, "bob").await.unwrap(), None);
        put(c, "bob", "free").await;
    });
}

#[test]
fn an_aborted_spanning_transaction_leaves_its_branches_free() {
    let t = Three::start(["", "", ""]);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(
            a,
            &id,
            vec![put_row("alice", "1"), put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        abort(c, &id).await.unwrap();
        assert_eq!(get_row(c, "alice").await.unwrap(), None);
        assert_eq!(get_row(c, "zed").await.unwrap(), None);
        put(c, "alice", "2").await;
        // The aborted transaction is gone.
        let e = commit_parts(c, &id, Vec::new()).await.unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
    });
}

/// Whether node a's p1 holds spanning transaction `id`'s share in doubt.
fn in_doubt(t: &Three, id: &str) -> bool {
    t.dbs[0]
        .in_doubt()
        .iter()
        .any(|(xid, _)| xid.as_slice() == id.as_bytes())
}

async fn set_fence(port: u16, p: &str, on: bool) {
    let r = super::super::proto::PartitionRequest {
        partition: p.into(),
        ..Default::default()
    };
    let mut c = client(port).await;
    if on {
        c.fence(r).await.unwrap();
    } else {
        c.unfence(r).await.unwrap();
    }
}

/// Commits `id` through `port` in a task of its own, paused at `step` until the test lets
/// it go (`abandon`: as if the home died there).
async fn paused_commit(
    port: u16,
    id: &str,
    step: crate::serve::cluster::spanning::seams::Step,
) -> (
    crate::serve::cluster::spanning::seams::Pause,
    tokio::task::JoinHandle<Result<super::super::proto::TxnCommitResponse, tonic::Status>>,
) {
    let pause = crate::serve::cluster::spanning::seams::pause(id, step);
    let id = id.to_owned();
    let task = tokio::spawn(async move { commit_parts(port, &id, Vec::new()).await });
    pause.reached.notified().await;
    (pause, task)
}

#[test]
fn a_read_only_commit_whose_branch_changed_lease_term_before_its_release_is_a_conflict() {
    // audit120's condition 2: the release answers only under the term the hold was taken in.
    // A fence and unfence between them leaves p1 serving, at a new term: the hold's
    // continuity is gone, so the commit is a Conflict.
    use crate::serve::cluster::spanning::seams::Step;
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        put(c, "alice", "1").await;
        put(c, "zed", "1").await;
        let id = begin_spanning(b, "zed").await.unwrap();
        txn_get(c, &id, "alice").await.unwrap();
        txn_get(c, &id, "zed").await.unwrap();
        let (pause, commit) = paused_commit(c, &id, Step::Release).await;
        set_fence(a, "p1", true).await;
        set_fence(a, "p1", false).await;
        pause.go.send(false).unwrap();
        let e = commit.await.unwrap().unwrap_err();
        assert_eq!(code(&e), "Conflict", "{e}");
        // Unchanged, the same transaction shape commits.
        let id = begin_spanning(b, "zed").await.unwrap();
        txn_get(c, &id, "alice").await.unwrap();
        commit_parts(c, &id, Vec::new()).await.unwrap();
    });
}

#[test]
fn a_home_lost_before_its_commit_point_leaves_nothing_written_and_its_branch_in_doubt() {
    use crate::serve::cluster::spanning::seams::Step;
    let t = Three::start(["", "", ""]);
    let [_, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(
            c,
            &id,
            vec![put_row("alice", "1"), put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        let (pause, commit) = paused_commit(c, &id, Step::CommitPoint).await;
        pause.go.send(true).unwrap();
        let e = commit.await.unwrap().unwrap_err();
        assert_eq!(code(&e), "OutcomeUnknown", "{e}");
        // The home wrote nothing; p1's share is prepared and in doubt (no decision row:
        // 2b's resolver aborts it), its write not applied.
        assert_eq!(get_row(c, "zed").await.unwrap(), None);
        assert_eq!(get_row(c, "alice").await.unwrap(), None);
        assert!(in_doubt(&t, &id), "p1's share isn't in doubt");
    });
}

#[test]
fn a_home_lost_after_its_commit_point_has_committed_and_leaves_its_branch_in_doubt() {
    use crate::serve::cluster::spanning::seams::Step;
    let t = Three::start(["", "", ""]);
    let [_, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(
            c,
            &id,
            vec![put_row("alice", "1"), put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        let (pause, commit) = paused_commit(c, &id, Step::Decide).await;
        pause.go.send(true).unwrap();
        let e = commit.await.unwrap().unwrap_err();
        assert_eq!(code(&e), "OutcomeUnknown", "{e}");
        // The commit point committed: the home's row is there. p1's share waits in doubt
        // for 2b's resolver, which finds the decision row and commits it.
        assert_eq!(get_row(c, "zed").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(get_row(c, "alice").await.unwrap(), None);
        assert!(in_doubt(&t, &id), "p1's share isn't in doubt");
    });
}

#[test]
fn a_spanning_transaction_is_its_tokens_and_a_client_cant_call_the_branch_arms() {
    // cli692's S1 on the design: branch calls carry the client, so a branch is bound to its
    // token too; the prepare and decide arms are the nodes' own (cluster scope only).
    use super::super::proto::{BranchDecide, BranchPrepare, ForwardRequest, forward_request};
    let all = format!("write t0ken-a\nwrite t0ken-b\ncluster {NODE_TOKEN}\n");
    let t = Three::start([&all, &all, &all]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        let r = super::super::proto::BeginRequest {
            durability: 0,
            route: route_row("zed"),
            spans_partitions: true,
        };
        let id = txns(c)
            .await
            .begin(with(r, Some("t0ken-a")))
            .await
            .unwrap()
            .into_inner()
            .txn_id;
        txn_write(a, &id, vec![put_row("alice", "1")], Some("t0ken-a"))
            .await
            .unwrap();
        // Another token: not found, home or branch.
        for row in ["alice", "yan"] {
            let e = txn_write(b, &id, vec![put_row(row, "2")], Some("t0ken-b"))
                .await
                .unwrap_err();
            assert_eq!(code(&e), "NotFound", "{row}: {e}");
        }
        // Another token's Abort finds nothing, and changes nothing (audit120's M1).
        let r = super::super::proto::TxnAbortRequest { txn_id: id.clone() };
        let e = txns(b)
            .await
            .abort(with(r, Some("t0ken-b")))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
        // A client token can't prepare or decide a branch itself.
        for op in [
            forward_request::Op::BranchPrepare(BranchPrepare {
                xid: id.clone(),
                read_only: false,
            }),
            forward_request::Op::BranchDecide(BranchDecide {
                xid: id.clone(),
                commit: true,
                hold: 0,
                term: 0,
            }),
        ] {
            let req = ForwardRequest {
                partition: "p1".into(),
                op: Some(op),
                ..Default::default()
            };
            let e = super::super::proto::cluster_client::ClusterClient::new(channel(a).await)
                .forward(with(req, Some("t0ken-a")))
                .await
                .unwrap_err();
            assert_eq!(code(&e), "PermissionDenied", "{e}");
        }
        let r = super::super::proto::TxnCommitRequest {
            txn_id: id.clone(),
            rows: Vec::new(),
        };
        let reply = txns(b)
            .await
            .commit(with(r, Some("t0ken-a")))
            .await
            .unwrap()
            .into_inner();
        // The branch's write is in: the other token's Abort didn't touch it.
        let parts: Vec<&str> = reply.parts.iter().map(|p| p.partition.as_str()).collect();
        assert_eq!(parts, ["p2", "p1"], "{reply:?}");
        assert_eq!(cell(&t.dbs[0], "alice"), Some(b"1".to_vec()));
    });
}

/// `row`'s value in partition database `db`, read directly.
fn cell(db: &pigeonhole::Pigeonhole, row: &str) -> Option<Vec<u8>> {
    db.table("users")
        .unwrap()
        .open()
        .unwrap()
        .get(row.as_bytes(), "f", b"q")
        .unwrap()
        .map(|c| c.value().to_vec())
}

#[test]
fn a_repeated_commit_with_branch_rows_answers_its_first_outcome_and_writes_them_once() {
    // audit120's M3: the home's session is taken before a Commit's rows go anywhere, so a
    // repeat finds the tombstone and sends nothing to the branch.
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        let first = commit_parts(a, &id, vec![put_row("alice", "1"), put_row("zed", "1")])
            .await
            .unwrap();
        let again = commit_parts(b, &id, vec![put_row("alice", "2"), put_row("zed", "2")])
            .await
            .unwrap();
        assert_eq!(again, first);
        assert_eq!(get_row(c, "alice").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(get_row(c, "zed").await.unwrap(), Some(b"1".to_vec()));
        // The decision table stays hidden from the schema (audit120's S5).
        let mut s = super::super::proto::schema_client::SchemaClient::new(channel(c).await);
        let tables = s
            .list_tables(super::super::proto::ListTablesRequest {})
            .await
            .unwrap()
            .into_inner()
            .tables;
        assert!(
            tables.iter().all(|t| !t.starts_with("__phdb_")),
            "{tables:?}"
        );
    });
}

/// Spanning transaction `id`'s decision row in home partition file `db`.
fn decision_row(db: &pigeonhole::Pigeonhole, id: &str) -> Option<Vec<u8>> {
    use crate::serve::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, DECISIONS};
    let t = db.table(DECISIONS).ok()?.open().ok()?;
    t.get(id.as_bytes(), DECISION_FAMILY, DECISION_QUALIFIER)
        .ok()
        .flatten()
        .map(|c| c.value().to_vec())
}

/// Limits whose resolver asks about a share in doubt after 200 ms (D329 2b).
fn resolving() -> crate::serve::config::Limits {
    let mut l = crate::serve::server_tests::limits();
    l.txn_resolve_delay = Duration::from_millis(200);
    l
}

/// `Transactions.Outcome` of `id` through `port`.
async fn outcome(
    port: u16,
    id: &str,
) -> Result<super::super::proto::TxnOutcomeResponse, tonic::Status> {
    txns(port)
        .await
        .outcome(super::super::proto::TxnOutcomeRequest {
            txn_id: id.to_owned(),
        })
        .await
        .map(tonic::Response::into_inner)
}

/// `id` with its begin time set to `ms` (the first 48 bits of its hex) and, with `epoch`, its
/// home's history epoch replaced: an id the home never gave out.
fn forged(id: &str, ms: u64, epoch: Option<u64>) -> String {
    let (prefix, hex) = id.rsplit_once('-').unwrap();
    let prefix = match epoch {
        Some(e) => format!("{}h{e}", prefix.rsplit_once('h').unwrap().0),
        None => prefix.to_owned(),
    };
    format!("{prefix}-{:012x}{}", ms & ((1 << 48) - 1), &hex[12..])
}

#[test]
fn the_resolver_commits_a_branch_whose_home_reached_its_commit_point() {
    // D329 2b: the home died after its decision row committed. p1's share, in doubt, asks
    // the home (ResolveTxn), finds the row and commits; Outcome says so, with the parts.
    use crate::serve::cluster::spanning::seams::Step;
    use crate::serve::proto::TxnOutcomeState as St;
    let t = Three::start_limits(["", "", ""], resolving());
    let [_, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(
            c,
            &id,
            vec![put_row("alice", "1"), put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        let (pause, commit) = paused_commit(c, &id, Step::Decide).await;
        pause.go.send(true).unwrap();
        assert_eq!(code(&commit.await.unwrap().unwrap_err()), "OutcomeUnknown");
        // Out of doubt once its decision is logged, and visible once applied just after.
        eventually("the resolver to commit p1's share", async || {
            !in_doubt(&t, &id) && get_row(c, "alice").await.unwrap() == Some(b"1".to_vec())
        })
        .await;
        let o = outcome(c, &id).await.unwrap();
        assert_eq!(o.state(), St::Committed, "{o:?}");
        // p1 acked the commit it applied: the home's record marks it, for the GC.
        eventually("p1's ack on the home's decision row", async || {
            decision_row(&t.dbs[1], &id).is_some_and(|v| v.ends_with(b"\n+p1"))
        })
        .await;
    });
}

#[test]
fn the_resolver_aborts_a_branch_whose_home_never_reached_its_commit_point() {
    // D329 2b: no decision row, the home's history unchanged: the home writes an abort
    // marker (OCC), so a late commit can't follow, and p1's share aborts.
    use crate::serve::cluster::spanning::seams::Step;
    use crate::serve::proto::TxnOutcomeState as St;
    let t = Three::start_limits(["", "", ""], resolving());
    let [_, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(
            c,
            &id,
            vec![put_row("alice", "1"), put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        let (pause, commit) = paused_commit(c, &id, Step::CommitPoint).await;
        pause.go.send(true).unwrap();
        assert_eq!(code(&commit.await.unwrap().unwrap_err()), "OutcomeUnknown");
        eventually("the resolver to abort p1's share", async || {
            !in_doubt(&t, &id)
        })
        .await;
        assert_eq!(get_row(c, "alice").await.unwrap(), None);
        assert_eq!(get_row(c, "zed").await.unwrap(), None);
        let o = outcome(c, &id).await.unwrap();
        assert_eq!(o.state(), St::Aborted, "{o:?}");
        // p1 takes a plain write of alice at once: its share is gone.
        put(c, "alice", "2").await;
    });
}

#[test]
fn outcome_answers_in_progress_unknown_and_not_found() {
    // D329 2b's Outcome: a live transaction is in progress (nothing written); an id from
    // another history of its home is unknown; one begun before the retention horizon,
    // with no record, is not found (never presumed aborted: its record may have been GC'd).
    use crate::serve::proto::TxnOutcomeState as St;
    let t = Three::start(["", "", ""]);
    let [_, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(c, &id, vec![put_row("zed", "1")], None)
            .await
            .unwrap();
        let o = outcome(c, &id).await.unwrap();
        assert_eq!(o.state(), St::InProgress, "{o:?}");
        // Asking wrote nothing: the transaction still commits.
        commit_parts(c, &id, Vec::new()).await.unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let other = forged(&id, now, Some(999));
        let o = outcome(c, &other).await.unwrap();
        assert_eq!(o.state(), St::Unknown, "{o:?}");
        let old = forged(&id, 1, None);
        let e = outcome(c, &old).await.unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
    });
}

#[test]
fn an_absent_record_is_presumed_aborted_only_within_retention_of_the_begin() {
    // audit120's M1 on #2076: a committed, acked record can be collected from its commit
    // time plus retention, just after the begin plus retention. From then on an absent
    // record answers NotFound and writes nothing, so a collected commit is never
    // re-recorded as aborted; just inside, an absent one is presumed aborted, with a marker.
    use crate::serve::proto::TxnOutcomeState as St;
    let t = Three::start(["", "", ""]);
    let [_, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        commit_parts(c, &id, vec![put_row("zed", "1")])
            .await
            .unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let retention = 86_400_000;
        let past = forged(&id, now - retention - 1_000, None);
        let e = outcome(c, &past).await.unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
        assert_eq!(decision_row(&t.dbs[1], &past), None, "nothing written");
        let inside = forged(&id, now - retention + 60_000, None);
        let o = outcome(c, &inside).await.unwrap();
        assert_eq!(o.state(), St::Aborted, "{o:?}");
        assert!(
            decision_row(&t.dbs[1], &inside).is_some_and(|v| v.starts_with(b"aborted ")),
            "the abort marker"
        );
    });
}

/// `phdb cluster txn decide`'s call: decides `id`'s share on partition `p` through `port`.
async fn decide(
    port: u16,
    p: &str,
    id: &str,
    commit: bool,
    force: bool,
) -> Result<super::super::proto::LocalPartition, tonic::Status> {
    client(port)
        .await
        .decide_txn(super::super::proto::DecideTxnRequest {
            partition: p.into(),
            xid: id.into(),
            commit,
            force,
        })
        .await
        .map(tonic::Response::into_inner)
}

/// Node `port`'s status line for partition `p`.
async fn line(port: u16, p: &str) -> super::super::proto::LocalPartition {
    client(port)
        .await
        .status(StatusRequest {
            open_only: Default::default(),
        })
        .await
        .unwrap()
        .into_inner()
        .partitions
        .into_iter()
        .find(|lp| lp.id == p)
        .unwrap()
}

#[test]
fn a_share_in_doubt_shows_in_status_and_is_decided_by_hand_only_when_its_home_cant_answer() {
    // D329 2b: p1's status line lists the share in doubt. The operator's decide is refused
    // while the home answers (its resolver settles the share) unless forced; once the home
    // can't answer (its node down), it decides. A fence doesn't silence a home: it still
    // reads its decision rows.
    use crate::serve::cluster::spanning::seams::Step;
    let mut t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    let bob = t.rt.block_on(async {
        let lost = async |row: &str| {
            let id = begin_spanning(c, "zed").await.unwrap();
            txn_write(c, &id, vec![put_row(row, "1"), put_row("zed", "1")], None)
                .await
                .unwrap();
            let (pause, commit) = paused_commit(c, &id, Step::CommitPoint).await;
            pause.go.send(true).unwrap();
            assert_eq!(code(&commit.await.unwrap().unwrap_err()), "OutcomeUnknown");
            id
        };
        let id = lost("alice").await;
        let lp = line(a, "p1").await;
        assert_eq!(lp.in_doubt, 1, "{lp:?}");
        assert_eq!(lp.in_doubt_oldest_xid, id);
        assert_eq!(lp.in_doubt_shares[0].xid, id, "{lp:?}");
        let e = decide(a, "p1", &id, true, false).await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(in_doubt(&t, &id), "a refused decide leaves it");
        set_fence(b, "p2", true).await;
        let e = decide(a, "p1", &id, true, false).await.unwrap_err();
        assert_eq!(
            code(&e),
            "FailedPrecondition",
            "a fenced home still answers: {e}"
        );
        set_fence(b, "p2", false).await;
        // With the home answering, `force` decides (here: commit, the operator's call).
        decide(a, "p1", &id, true, true).await.unwrap();
        assert!(!in_doubt(&t, &id));
        assert_eq!(get_row(a, "alice").await.unwrap(), Some(b"1".to_vec()));
        lost("bob").await
    });
    // The home's node down: it can't answer, so the decide goes ahead unforced.
    drop(std::mem::replace(
        &mut t.stops[1],
        tokio::sync::oneshot::channel().0,
    ));
    t.rt.block_on(async {
        assert!(in_doubt(&t, &bob));
        let lp = decide(a, "p1", &bob, false, false).await.unwrap();
        assert_eq!(lp.in_doubt, 0, "{lp:?}");
        assert_eq!(get_row(a, "bob").await.unwrap(), None);
    });
}

#[test]
fn a_cluster_partition_refuses_a_follower_outside_its_map_replicas() {
    // D329 2b (the coordinator's option a): a cluster node serves `Replication.Follow` only
    // to a cluster-scope caller whose node id is one of the partition's map replicas. An
    // outside follower (`--replica-of` a cluster node), with or without a node id, is
    // refused with the reason, not served the partition's raw history.
    use crate::serve::proto::FollowRequest;
    use crate::serve::proto::replication_client::ReplicationClient;
    let t = Three::start(["", "", ""]);
    let [a, _, _] = t.ports;
    t.rt.block_on(async {
        for node_id in [None, Some("c"), Some("zz")] {
            let mut req = tonic::Request::new(FollowRequest::default());
            if let Some(id) = node_id {
                req.metadata_mut()
                    .insert(super::super::NODE_ID_HEADER, id.parse().unwrap());
            }
            let mut c = ReplicationClient::new(channel(a).await);
            let e = match c.follow(req).await {
                Err(e) => e,
                Ok(stream) => stream
                    .into_inner()
                    .message()
                    .await
                    .expect_err("a follower outside the map's replicas is served"),
            };
            assert_eq!(code(&e), "FailedPrecondition", "{node_id:?}: {e}");
            assert!(e.message().contains("map replicas"), "{e}");
        }
    });
}

/// `Transactions.Outcome` of `id` through `port`, as `token`.
async fn outcome_as(
    port: u16,
    id: &str,
    token: &str,
) -> Result<super::super::proto::TxnOutcomeResponse, tonic::Status> {
    let r = super::super::proto::TxnOutcomeRequest {
        txn_id: id.to_owned(),
    };
    txns(port)
        .await
        .outcome(with(r, Some(token)))
        .await
        .map(tonic::Response::into_inner)
}

#[test]
fn a_spanning_transactions_outcome_is_its_tokens_alone_while_its_session_lasts() {
    // #2077 (audit120's S5 on #2076, and their P1–P3 on the plan): another token's
    // `Outcome` on a live spanning transaction is NotFound and writes nothing, on the home
    // and through any other node, so it can't learn the outcome nor make the home record an
    // abort over it; the owner then commits. While the tombstone lasts, the owner reads the
    // outcome and the other token still gets NotFound, though the decision row exists. A
    // client's id dated in the future is NotFound with nothing written, and a client can't
    // send the resolver's ResolveTxn itself.
    use super::super::proto::{ForwardRequest, ResolveTxn, TxnOutcomeState as St, forward_request};
    let all = format!("write t0ken-a\nwrite t0ken-b\ncluster {NODE_TOKEN}\n");
    let t = Three::start([&all, &all, &all]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        let r = super::super::proto::BeginRequest {
            durability: 0,
            route: route_row("zed"),
            spans_partitions: true,
        };
        let id = txns(c)
            .await
            .begin(with(r, Some("t0ken-a")))
            .await
            .unwrap()
            .into_inner()
            .txn_id;
        txn_write(
            c,
            &id,
            vec![put_row("alice", "1"), put_row("zed", "1")],
            Some("t0ken-a"),
        )
        .await
        .unwrap();
        // The home (b) answers locally; a and c forward to it.
        for port in [b, a, c] {
            let e = outcome_as(port, &id, "t0ken-b").await.unwrap_err();
            assert_eq!(code(&e), "NotFound", "through {port}: {e}");
            assert_eq!(decision_row(&t.dbs[1], &id), None, "nothing written");
        }
        let o = outcome_as(c, &id, "t0ken-a").await.unwrap();
        assert_eq!(o.state(), St::InProgress, "{o:?}");
        // The other token's asks changed nothing: the owner commits both partitions.
        let r = super::super::proto::TxnCommitRequest {
            txn_id: id.clone(),
            rows: Vec::new(),
        };
        let reply = txns(c)
            .await
            .commit(with(r, Some("t0ken-a")))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.parts.len(), 2, "{reply:?}");
        assert_eq!(cell(&t.dbs[0], "alice"), Some(b"1".to_vec()));
        assert!(decision_row(&t.dbs[1], &id).is_some_and(|v| v.starts_with(b"committed ")));
        // The tombstone: the owner's outcome; the other token's NotFound, the row unread.
        let o = outcome_as(c, &id, "t0ken-a").await.unwrap();
        assert_eq!(o.state(), St::Committed, "{o:?}");
        assert_eq!(o.parts.len(), 2, "{o:?}");
        for port in [b, c] {
            let e = outcome_as(port, &id, "t0ken-b").await.unwrap_err();
            assert_eq!(code(&e), "NotFound", "through {port}: {e}");
        }
        // An id dated an hour ahead: no transaction, and nothing recorded for it.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let ahead = forged(&id, now + 3_600_000, None);
        let e = outcome_as(c, &ahead, "t0ken-b").await.unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
        assert_eq!(decision_row(&t.dbs[1], &ahead), None, "nothing written");
        // The resolver's call is the nodes' own: a client's token can't forward it.
        let req = ForwardRequest {
            partition: "p2".into(),
            op: Some(forward_request::Op::ResolveTxn(ResolveTxn {
                xid: id.clone(),
                acked: String::new(),
            })),
            ..Default::default()
        };
        let e = super::super::proto::cluster_client::ClusterClient::new(channel(b).await)
            .forward(with(req, Some("t0ken-b")))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "PermissionDenied", "{e}");
    });
}

/// A call of transaction `id` that pauses at `Step::Forwarded` (its branch rows sent) and
/// is cancelled there, as a client that goes away or whose deadline passes: the pause is
/// never released, so the call can only end by being dropped where it waits. Then `again`
/// is tried until the session answers for itself (the cancel has to reach the home's node
/// first), and its answer is returned.
async fn cancelled_after_forwarding<T, F, R, G, S>(
    id: &str,
    call: F,
    again: G,
) -> Result<R, tonic::Status>
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
    G: Fn() -> S,
    S: std::future::Future<Output = Result<R, tonic::Status>>,
    R: std::fmt::Debug,
{
    use crate::serve::cluster::spanning::seams::{self, Step};
    let pause = seams::pause(id, Step::Forwarded);
    let first = tokio::spawn(call);
    pause.reached.notified().await;
    first.abort();
    let _ = first.await;
    let mut second = again().await;
    let mut tries = 0;
    while let Err(e) = &second {
        if !matches!(code(e).as_str(), "SessionBusy" | "InProgress") || tries == 200 {
            break;
        }
        tries += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
        second = again().await;
    }
    assert!(
        !matches!(&second, Err(e) if matches!(code(e).as_str(), "SessionBusy" | "InProgress")),
        "the cancelled call never let go of the session: {second:?}"
    );
    drop(pause);
    second
}

#[test]
fn a_call_cancelled_after_its_branch_rows_went_out_ends_the_transaction() {
    // #2157: a `Commit` carries a counter's increment for a branch (p1) and a row for its
    // home (p2). The client goes away once the increment has reached the branch and before
    // the commit's round starts, then sends the same `Commit` again, as a client with no
    // answer does. Before the fix the session was put back open, the second `Commit` sent
    // the increment again and committed, and the counter read 2. Now the cancelled call
    // ends the transaction: the second `Commit` is told so (`Conflict`, retry the
    // transaction) and nothing was applied. The same for a `TxnWrite` cut short.
    let t = Three::start(["", "", ""]);
    let [_, _, c] = t.ports;
    t.rt.block_on(async {
        use super::several::{add_counter, counter, incr_row};
        add_counter(&t.ports, c).await;
        let rows = || vec![incr_row("alice", 1), put_row("zed", "1")];

        let id = begin_spanning(c, "zed").await.unwrap();
        let first = {
            let (id, rows) = (id.clone(), rows());
            async move { commit_parts(c, &id, rows).await }
        };
        let second = cancelled_after_forwarding(&id, first, || commit_parts(c, &id, rows())).await;
        let e = second.expect_err("the cancelled Commit ended the transaction");
        assert_eq!(code(&e), "Conflict", "{e}");
        assert!(e.message().contains("cancelled part-way"), "{e}");
        assert_eq!(counter(c, "alice").await, 0);
        assert_eq!(get_row(c, "zed").await.unwrap(), None);

        // A `TxnWrite` cut short the same way: its retry finds the transaction ended, and
        // so does a `Commit`.
        let id = begin_spanning(c, "zed").await.unwrap();
        let first = {
            let (id, rows) = (id.clone(), rows());
            async move { txn_write(c, &id, rows, None).await }
        };
        let second =
            cancelled_after_forwarding(&id, first, || txn_write(c, &id, rows(), None)).await;
        assert!(second.is_err(), "{second:?}");
        let e = commit_parts(c, &id, Vec::new()).await.unwrap_err();
        assert_eq!(code(&e), "Conflict", "{e}");
        assert_eq!(counter(c, "alice").await, 0);

        // A whole transaction after that applies the increment once.
        let id = begin_spanning(c, "zed").await.unwrap();
        commit_parts(c, &id, rows()).await.unwrap();
        assert_eq!(counter(c, "alice").await, 1);
    });
}

#[test]
fn a_transaction_ended_by_a_lost_branch_write_answer_says_it_didnt_commit() {
    // #2161: a home's write to a branch is taken there, but its answer is lost, which is
    // `OutcomeUnknown` for that one write. The home ends the transaction for it, before any
    // commit point, so nothing of it can commit: the write's caller, and then `Commit`, are
    // told so (`Conflict`, retry the transaction), not `OutcomeUnknown`, which sent a
    // client to `Outcome` for what was already certain. In a `TxnWrite` and in the rows a
    // `Commit` carries alike.
    use crate::serve::cluster::spanning::seams;
    let t = Three::start(["", "", ""]);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(c, &id, vec![put_row("zed", "1")], None)
            .await
            .unwrap();
        seams::lose_write_reply(&id);
        let e = txn_write(c, &id, vec![put_row("alice", "1")], None)
            .await
            .unwrap_err();
        assert_eq!(code(&e), "Conflict", "{e}");
        assert!(e.message().contains("nothing of it was committed"), "{e}");
        // The session's record says the same to its `Commit`.
        let e = commit_parts(c, &id, Vec::new()).await.unwrap_err();
        assert_eq!(code(&e), "Conflict", "{e}");
        assert_eq!(get_row(a, "alice").await.unwrap(), None);
        assert_eq!(get_row(c, "zed").await.unwrap(), None);
        assert!(!in_doubt(&t, &id));

        // The same loss on a row the `Commit` itself carries.
        let id = begin_spanning(c, "zed").await.unwrap();
        seams::lose_write_reply(&id);
        let e = commit_parts(c, &id, vec![put_row("alice", "2"), put_row("zed", "2")])
            .await
            .unwrap_err();
        assert_eq!(code(&e), "Conflict", "{e}");
        assert_eq!(get_row(a, "alice").await.unwrap(), None);
        assert_eq!(get_row(c, "zed").await.unwrap(), None);
    });
}

#[test]
fn a_commit_decided_by_hand_acks_its_home_so_its_record_can_be_collected() {
    // #2078 (audit120's nit on #2076, Q3 on the plan): the home is lost after its commit
    // point, so its row is COMMITTED and p1's share waits in doubt. The resolver waits an
    // hour here, so the operator decides it by hand (`--force`: the home still answers).
    // The decide returns at once; the resolver's next pass sends the ack it owes, and the
    // home's row then holds p1's ack line, so GC can collect it.
    use crate::serve::cluster::spanning::seams::Step;
    let mut limits = crate::serve::server_tests::limits();
    limits.txn_resolve_delay = Duration::from_secs(3600);
    let t = Three::start_limits(["", "", ""], limits);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(
            c,
            &id,
            vec![put_row("alice", "1"), put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        let (pause, commit) = paused_commit(c, &id, Step::Decide).await;
        pause.go.send(true).unwrap();
        assert_eq!(code(&commit.await.unwrap().unwrap_err()), "OutcomeUnknown");
        assert!(in_doubt(&t, &id));
        let row = decision_row(&t.dbs[1], &id).unwrap();
        assert!(!row.ends_with(b"\n+p1"), "not acked yet: {row:?}");
        decide(a, "p1", &id, true, true).await.unwrap();
        assert!(!in_doubt(&t, &id));
        assert_eq!(get_row(a, "alice").await.unwrap(), Some(b"1".to_vec()));
        // Sent by the resolver's next pass (every five seconds with this delay).
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let row = decision_row(&t.dbs[1], &id).unwrap();
            if row.ends_with(b"\n+p1") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the home's row never took p1's ack: {}",
                String::from_utf8_lossy(&row)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}

#[test]
fn a_commit_whose_ack_was_lost_is_asked_again_and_collected_past_retention() {
    // #2098: as in the test above, p1's share is decided by hand, but its ack is lost (a
    // restart of node a loses the queue it's owed from), so the home's row never takes it.
    // Once the row is past retention, the home's sweep sends p1 the commit's decide again;
    // p1 holds nothing in doubt, so it answers at once, its ack is recorded, and the same
    // sweep collects the row. p1 applied the commit once.
    use crate::serve::cluster::spanning::seams::{self, Step};
    use crate::serve::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, DECISIONS};
    let mut limits = crate::serve::server_tests::limits();
    limits.txn_resolve_delay = Duration::from_secs(3600);
    let retention = limits.txn_outcome_retention;
    let t = Three::start_limits(["", "", ""], limits);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        use super::several::{add_counter, counter, incr_row};
        add_counter(&t.ports, c).await;
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(
            c,
            &id,
            vec![incr_row("alice", 1), put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        let (pause, commit) = paused_commit(c, &id, Step::Decide).await;
        pause.go.send(true).unwrap();
        assert_eq!(code(&commit.await.unwrap().unwrap_err()), "OutcomeUnknown");
        assert!(in_doubt(&t, &id));
        seams::lose_ack(&id);
        decide(a, "p1", &id, true, true).await.unwrap();
        assert!(!in_doubt(&t, &id));
        assert_eq!(counter(c, "alice").await, 1);
        // The row as the commit left it, aged past retention: nothing acked it.
        let row = decision_row(&t.dbs[1], &id).unwrap();
        let text = String::from_utf8(row).unwrap();
        assert!(text.ends_with("\np1"), "{text:?}");
        let (head, branches) = text.split_once('\n').unwrap();
        let old = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - retention.as_millis() as u64
            - 1_000;
        let aged: Vec<&str> = head
            .split(' ')
            .map(|w| if w.starts_with("at") { "AT" } else { w })
            .collect();
        let aged = format!("{}\n{branches}", aged.join(" ")).replace("AT", &format!("at{old}"));
        t.dbs[1]
            .table(DECISIONS)
            .unwrap()
            .open()
            .unwrap()
            .mutate(id.as_bytes())
            .put(DECISION_FAMILY, DECISION_QUALIFIER, aged.as_bytes())
            .commit()
            .unwrap();
        eventually_within(
            Duration::from_secs(30),
            "the home's sweep collects the row",
            async || decision_row(&t.dbs[1], &id).is_none(),
        )
        .await;
        assert_eq!(counter(c, "alice").await, 1, "applied once");
    });
}

/// On a cluster with tokens (audit120's M1 on #2164): a spanning transaction homed on p2
/// (node b) whose home dies before its decides leaves p1's share (node a) in doubt. Node a's
/// resolver asks the home on another node, as the cluster itself, and decides it. With
/// `lose`, p1 is instead decided by hand with its ack lost, and the home's sweep (#2098) asks
/// p1 again on node a and collects the row. Either way the node's own calls are let through
/// on a cluster whose nodes hold tokens.
fn a_token_cluster_settles_a_share_across_nodes(lose: bool) {
    use crate::serve::cluster::spanning::seams::{self, Step};
    use crate::serve::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, DECISIONS};
    use crate::serve::proto::{BeginRequest, DecideTxnRequest, TxnCommitRequest};
    let tok = "t0ken-a";
    let all = format!("write {tok}\ncluster {NODE_TOKEN}\n");
    let mut limits = crate::serve::server_tests::limits();
    limits.txn_resolve_delay = if lose {
        Duration::from_secs(3600)
    } else {
        Duration::from_millis(500)
    };
    let retention = limits.txn_outcome_retention;
    let t = Three::start_limits([&all, &all, &all], limits);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        let r = BeginRequest {
            durability: 0,
            route: route_row("zed"),
            spans_partitions: true,
        };
        let id = txns(c)
            .await
            .begin(with(r, Some(tok)))
            .await
            .unwrap()
            .into_inner()
            .txn_id;
        txn_write(
            c,
            &id,
            vec![put_row("alice", "1"), put_row("zed", "1")],
            Some(tok),
        )
        .await
        .unwrap();
        let pause = seams::pause(&id, Step::Decide);
        let commit = {
            let id = id.clone();
            tokio::spawn(async move {
                txns(c)
                    .await
                    .commit(with(
                        TxnCommitRequest {
                            txn_id: id,
                            rows: Vec::new(),
                        },
                        Some(tok),
                    ))
                    .await
            })
        };
        pause.reached.notified().await;
        pause.go.send(true).unwrap();
        assert_eq!(code(&commit.await.unwrap().unwrap_err()), "OutcomeUnknown");
        assert!(in_doubt(&t, &id));
        if !lose {
            eventually_within(
                Duration::from_secs(30),
                "node a's resolver settles p1's share with the home on node b",
                async || !in_doubt(&t, &id),
            )
            .await;
            // The share leaves the in-doubt registry before its decision is written and
            // applied (the engine's `on_external_decide`), so its row shows a moment later.
            let users = t.dbs[0].table("users").unwrap().open().unwrap();
            eventually_within(
                Duration::from_secs(30),
                "p1 applied the commit",
                async || users.get(b"alice", "f", b"q").unwrap().is_some(),
            )
            .await;
            return;
        }
        seams::lose_ack(&id);
        client(a)
            .await
            .decide_txn(with(
                DecideTxnRequest {
                    partition: "p1".into(),
                    xid: id.clone(),
                    commit: true,
                    force: true,
                },
                Some(NODE_TOKEN),
            ))
            .await
            .unwrap();
        assert!(!in_doubt(&t, &id));
        let text = String::from_utf8(decision_row(&t.dbs[1], &id).unwrap()).unwrap();
        assert!(text.ends_with("\np1"), "{text:?}");
        let (head, branches) = text.split_once('\n').unwrap();
        let old = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - retention.as_millis() as u64
            - 1_000;
        let head: Vec<String> = head
            .split(' ')
            .map(|w| {
                if w.starts_with("at") {
                    format!("at{old}")
                } else {
                    w.to_owned()
                }
            })
            .collect();
        t.dbs[1]
            .table(DECISIONS)
            .unwrap()
            .open()
            .unwrap()
            .mutate(id.as_bytes())
            .put(
                DECISION_FAMILY,
                DECISION_QUALIFIER,
                format!("{}\n{branches}", head.join(" ")).as_bytes(),
            )
            .commit()
            .unwrap();
        eventually_within(
            Duration::from_secs(30),
            "the home's sweep asks p1 on node a and collects the row",
            async || decision_row(&t.dbs[1], &id).is_none(),
        )
        .await;
    });
}

#[test]
fn on_a_token_cluster_the_resolver_asks_a_home_on_another_node() {
    a_token_cluster_settles_a_share_across_nodes(false);
}

#[test]
fn on_a_token_cluster_the_sweep_asks_a_branch_on_another_node() {
    a_token_cluster_settles_a_share_across_nodes(true);
}

/// D331 PR 5 (simp724's M1 on the design, and the range-scoped in-doubt rule): a spanning
/// transaction's branch on the moving range is prepared before the move, and its home is
/// paused before the commit point, so the share stays in doubt on the source. The move
/// waits before the hand-over (no fence, no `fence_seqno`) for as long as it's in doubt.
/// Released, the transaction commits, the move goes on, and the target holds the decided
/// write.
#[test]
fn a_share_prepared_before_a_move_holds_it_until_the_home_decides() {
    use crate::serve::cluster::spanning::seams::Step;
    use crate::serve::proto::{GetMapRequest, MovePhase};
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        // Home p2 (by "zed"); its branch on p1 writes "d05", inside the moving [c, f).
        let id = begin_spanning(c, "zed").await.unwrap();
        txn_write(c, &id, vec![put_row("d05", "1"), put_row("zed", "1")], None)
            .await
            .unwrap();
        // At the home's own node: a forward would time out while the home is paused.
        let (pause, commit) = paused_commit(b, &id, Step::CommitPoint).await;
        let args = super::backup::args(a);
        let moving = tokio::spawn(async move {
            let spec = super::super::range_move::Spec {
                table: "users".into(),
                from: "p1".into(),
                to: "p2".into(),
                start: Some(b"c".to_vec()),
                end: Some(b"f".to_vec()),
            };
            super::super::range_move::run(&args, &spec, Some(Duration::from_secs(120)), true)
                .await
                .map(drop)
        });
        let phase = async || {
            client(a)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner()
                .moves
                .first()
                .map(super::super::map::phase)
        };
        eventually("the move reaches CATCHING_UP", async || {
            phase().await == Some(MovePhase::CatchingUp)
        })
        .await;
        // Held there while the share is in doubt.
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(phase().await, Some(MovePhase::CatchingUp));
        assert!(!moving.is_finished(), "the move ran past a share in doubt");
        pause.go.send(false).unwrap();
        commit.await.unwrap().unwrap();
        moving.await.unwrap().unwrap();
        assert_eq!(phase().await, None, "the move ended");
        // Read through the map, which gives "d05" to p2 now.
        assert_eq!(get_row(c, "d05").await.unwrap(), Some(b"1".to_vec()));
    });
}
