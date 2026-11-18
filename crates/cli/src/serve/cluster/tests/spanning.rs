//! Transactions across partitions (D329 PR 2a): a transaction begun with `spans_partitions`
//! reads and writes rows of any partition, and commits them atomically. `Three::start`:
//! node a serves p1 (users < "m"), b serves p2 (users >= "m"), c only routes.

use super::*;

async fn begin_spanning(port: u16, row: &str) -> Result<String, tonic::Status> {
    let r = super::super::proto::BeginRequest {
        durability: 0,
        route: route_row(row),
        spans_partitions: true,
    };
    Ok(txns(port).await.begin(r).await?.into_inner().txn_id)
}

async fn commit_parts(
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

async fn abort(port: u16, id: &str) -> Result<(), tonic::Status> {
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
        txns(b)
            .await
            .commit(with(r, Some("t0ken-a")))
            .await
            .unwrap();
    });
}
