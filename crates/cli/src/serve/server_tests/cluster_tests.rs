use super::*;

// ---- Cluster (D317 ticket 1, ICR 0117) ----

#[test]
fn the_clusters_reserved_tables_are_hidden_and_refused() {
    // A partition's identity table (D317, ICR 0117) is the server's own: not listed, not
    // readable, and its prefix is refused to every schema call.
    let s = Server::start("", limits());
    let db = s.shared.router.service.db.clone();
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
