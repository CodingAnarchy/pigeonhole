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
