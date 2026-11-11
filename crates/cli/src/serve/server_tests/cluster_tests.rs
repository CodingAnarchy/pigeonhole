use super::*;

// ---- Cluster (D317 ticket 1, ICR 0117) ----

#[test]
fn the_clusters_reserved_tables_are_hidden_and_refused() {
    // A partition's identity table (D317, ICR 0117) is the server's own: not listed, not
    // readable, and its prefix is refused to every schema call.
    let s = Server::start("", limits());
    let db = s.shared.router.service.part().unwrap().db.clone();
    super::cluster::identity::write(
        &db,
        &super::cluster::identity::Identity {
            cluster_id: "c1".into(),
            partition_id: "p1".into(),
            epoch: 1,
        },
    )
    .unwrap();
    let (_, list) = s.rpc("Schema/ListTables", &json!({}), None);
    assert_eq!(list["tables"], json!(["t"]), "{list}");
    for (rpc, body) in [
        (
            "Schema/CreateTable",
            json!({"name": "__phdb_x", "families": [{"name": "f"}]}),
        ),
        ("Schema/DropTable", json!({"name": "__phdb_cluster"})),
        ("Schema/DescribeTable", json!({"name": "__phdb_cluster"})),
        (
            "Data/Get",
            json!({"table": "__phdb_cluster", "row": "cGFydGl0aW9u", "family": "id", "qualifier": "ZXBvY2g="}),
        ),
    ] {
        let (status, e) = s.rpc(rpc, &body, None);
        assert_eq!(
            (status, e["code"].as_str()),
            (400, Some("InvalidArgument")),
            "{rpc}: {e}"
        );
    }
    // Still there for the cluster.
    assert_eq!(
        super::cluster::identity::read(&db)
            .unwrap()
            .map(|i| i.epoch),
        Some(1)
    );
}

#[test]
fn a_node_that_only_routes_answers_for_its_own_state_without_a_partition() {
    // D317 ticket 2: a cluster node the map gives no partition still has its node state
    // (connections, budgets, sessions, metrics); what needs a database answers that it has
    // none, and the node's own hooks pass over it.
    let svc = Service::routing_only(limits(), 2, Arc::new(Metrics::default()));
    assert!(svc.part_opt().is_none());
    let e = svc.part().err().expect("no partition");
    assert_eq!(
        (e.code.as_str(), e.grpc),
        ("Internal", tonic::Code::Internal)
    );
    assert!(svc.user_tables().is_empty());
    assert_eq!(svc.catalog(), 0);
    // A catalog change or an engine swap has nothing to drop here.
    svc.tables_changed();
    svc.engine_replaced();
    // The node's metrics, without the engine's.
    let text = svc.metrics_text(&[], &[&svc.sessions]);
    assert!(text.contains("pigeonhole_serve_requests_total"), "{text}");
    assert!(
        !text.contains("pigeonhole_async_sync_reads_total"),
        "{text}"
    );
}
