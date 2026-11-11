//! A cluster node's metrics (D317 ticket 8b): every per-partition series carries
//! `pigeonhole_partition`, node-wide ones don't, and each family's `# TYPE` comes once, as
//! the exposition format requires, through `Admin.Metrics` (answered by the router) as on
//! `/metrics`.

use std::collections::BTreeSet;

use super::*;
use crate::serve::proto::{Empty, admin_client::AdminClient};

#[test]
fn a_cluster_nodes_metrics_label_its_partition_once_per_family() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let text = rt.block_on(async {
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", ports[1]))
                .unwrap()
                .connect()
                .await
                .unwrap();
        AdminClient::new(channel)
            .metrics(Empty {})
            .await
            .unwrap()
            .into_inner()
            .text
    });
    let mut seen = BTreeSet::new();
    for line in text.lines().filter(|l| l.starts_with("# TYPE ")) {
        assert!(seen.insert(line), "{line} twice:\n{text}");
    }
    let labelled = |prefix: &str| {
        text.lines()
            .filter(|l| l.starts_with(prefix) && !l.starts_with('#'))
            .all(|l| l.contains("pigeonhole_partition=\"p2\""))
    };
    // The engine's (D323) and the serve feed fan-out's are the partition's.
    assert!(text.contains("pigeonhole_commits_total{"), "{text}");
    assert!(labelled("pigeonhole_commits_total"), "{text}");
    assert!(labelled("pigeonhole_serve_feed_hubs"), "{text}");
    // Connections are the node's.
    assert!(
        text.lines()
            .filter(|l| l.starts_with("pigeonhole_serve_") && l.contains("connection"))
            .all(|l| !l.contains("pigeonhole_partition")),
        "{text}"
    );
}
