//! `/mcp` on a cluster node (#1915 PR 3b, ICR 0139): the tools are service calls through
//! the router, so through any node they answer as one database does, refuse a write across
//! partitions as `Data.Mutate` does, and a `feed_poll`'s token resumes through another node
//! with no commit repeated or lost (simp724's M1).

use std::collections::BTreeSet;

use serde_json::{Value as Json, json};

use super::*;
use crate::serve::proto::MutateRequest;
use crate::serve::server_tests::mcp_tests::{call_at, session_at};

fn url(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

/// `tool` through the node at `port`, on a session of its own: its answer, which must not
/// be an error.
fn ok(port: u16, id: &str, tool: &str, args: Json) -> Json {
    let (out, err) = call_at(&url(port), None, id, tool, args);
    assert!(!err, "{tool} through {port}: {out}");
    out
}

/// The rows of a `scan` or `nearest` answer, in order.
fn rows(out: &Json, list: &str) -> Vec<String> {
    out[list]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["row"].as_str().unwrap().to_owned())
        .collect()
}

/// A put of `value` at `users`/`row`/`family`/`qualifier`, as `write` takes it.
fn put(row: &str, family: &str, qualifier: &str, value: Json) -> Json {
    json!({"op": "put", "table": "users", "row": row, "family": family,
           "qualifier": qualifier, "value": value})
}

fn vector(values: [f32; 4]) -> Json {
    json!({"vector": {"dim": 4, "element": "f32", "values": values}})
}

/// Rows `a1` (p1, on a), `h1` (p2, on b) and `t1` (p3, on c), each with a vector, chained by
/// edges `a1` → `h1` → `t1`, written through `/mcp` on a: each `write` routes to its row's
/// partition.
fn fill(port: u16) {
    let id = session_at(&url(port), None);
    // The test partitions hold `users` already, placed by hand in the map without families:
    // `create_table` adds them to the map, cluster-wide, as `Schema.CreateTable` does.
    let made = ok(
        port,
        &id,
        "create_table",
        json!({"table": "users", "families": [
            "f", "out",
            {"name": "emb", "vector": {"dim": 4, "element": "f32", "metric": "l2"}},
        ]}),
    );
    assert_eq!(made["families"].as_array().unwrap().len(), 3, "{made}");
    for (row, v, next) in [
        ("a1", [0.0, 0.0, 0.0, 1.0], Some("h1")),
        ("h1", [1.0, 0.0, 0.0, 0.0], Some("t1")),
        ("t1", [2.0, 0.0, 0.0, 0.0], None),
    ] {
        let mut ops = vec![
            put(row, "f", "q", json!(format!("v-{row}"))),
            put(row, "emb", "", vector(v)),
        ];
        if let Some(n) = next {
            ops.push(put(row, "out", n, json!("yes")));
        }
        ok(port, &id, "write", json!({"ops": ops}));
    }
}

#[test]
fn every_table_tool_answers_through_any_node_as_one_database_does() {
    bounded(Duration::from_secs(120), || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_stops, ports, _dbs) = three_nodes(&rt);
        fill(ports[0]);
        for &port in &ports {
            let id = session_at(&url(port), None);
            let tables = ok(port, &id, "list_tables", json!({}));
            assert!(tables.to_string().contains("\"users\""), "{tables}");
            for row in ["a1", "h1", "t1"] {
                let r = ok(
                    port,
                    &id,
                    "read_row",
                    json!({"table": "users", "row": row, "families": ["f"]}),
                );
                assert_eq!(r["cells"][0]["value"], format!("v-{row}"), "{r}");
            }
            // A scan across the three partitions, a row a page: its cursor carries on into
            // the next partition.
            let mut args = json!({"table": "users", "families": ["f"], "limit": 1});
            let mut seen = Vec::new();
            loop {
                let page = ok(port, &id, "scan", args.clone());
                seen.extend(rows(&page, "rows"));
                match page.get("cursor") {
                    Some(c) if !c.is_null() => args["cursor"] = c.clone(),
                    _ => break,
                }
            }
            assert_eq!(seen, ["a1", "h1", "t1"], "through {port}");
            let near = ok(
                port,
                &id,
                "nearest",
                json!({"table": "users", "family": "emb", "query": [1.9, 0.0, 0.0, 0.0],
                       "k": 2}),
            );
            assert_eq!(rows(&near, "neighbours"), ["t1", "h1"], "{near}");
            let walk = ok(
                port,
                &id,
                "traverse",
                json!({"table": "users", "family": "out", "seeds": ["a1"], "hops": 2}),
            );
            let hops: Vec<Json> = walk["hops"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["nodes"].clone())
                .collect();
            assert_eq!(hops, [json!(["h1"]), json!(["t1"])], "{walk}");
        }
        // One write's rows on two partitions: refused as `Data.Mutate` refuses it, and
        // nothing written.
        let id = session_at(&url(ports[2]), None);
        let (e, err) = call_at(
            &url(ports[2]),
            None,
            &id,
            "write",
            json!({"ops": [put("a2", "f", "q", json!("x")), put("t2", "f", "q", json!("x"))]}),
        );
        assert!(err, "{e}");
        assert_eq!(e["error"]["code"], "CrossPartition", "{e}");
        for row in ["a2", "t2"] {
            let r = ok(
                ports[1],
                &id_on(ports[1]),
                "read_row",
                json!({"table": "users", "row": row}),
            );
            assert_eq!(r["cells"], json!([]), "{row}: {r}");
        }
    });
}

fn id_on(port: u16) -> String {
    session_at(&url(port), None)
}

/// simp724's M1 on #1915 PR 3: on a cluster, a `feed_poll` ends between the merged feed's
/// checkpoints (`max_commits` 1), and its token, taken through another node, resumes with
/// no commit repeated or lost.
#[test]
fn a_feed_polls_token_resumes_through_another_node_with_no_commit_repeated_or_lost() {
    bounded(Duration::from_secs(120), || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_stops, ports, _dbs) = three_nodes(&rt);
        let first = ok(
            ports[0],
            &id_on(ports[0]),
            "feed_poll",
            json!({"tables": ["users"]}),
        );
        assert_eq!(first["commits"], json!([]), "{first}");
        let written: BTreeSet<String> = ["a1", "b1", "h1", "i1", "t1", "u1", "a3", "t3"]
            .iter()
            .map(|r| (*r).to_owned())
            .collect();
        rt.block_on(async {
            let mut data = data(ports[1]).await;
            for row in &written {
                data.mutate(MutateRequest {
                    replication: 0,
                    writes: Some(put_row(row, "v")),
                    durability: 0,
                })
                .await
                .unwrap();
            }
        });
        // One commit a poll, each through the next node, from the last one's token.
        let mut token = first["token"].clone();
        let mut got = Vec::new();
        for i in 0..64 {
            let port = ports[i % 3];
            let out = ok(
                port,
                &id_on(port),
                "feed_poll",
                json!({"tables": ["users"], "token": token, "max_commits": 1,
                       "wait_ms": 2000}),
            );
            for c in out["commits"].as_array().unwrap() {
                for ch in c["changes"].as_array().unwrap() {
                    got.push(ch["row"].as_str().unwrap().to_owned());
                }
            }
            token = out["token"].clone();
            if got.len() >= written.len() {
                break;
            }
        }
        let unique: BTreeSet<String> = got.iter().cloned().collect();
        assert_eq!(got.len(), unique.len(), "a commit repeated: {got:?}");
        assert_eq!(unique, written, "a commit lost: {got:?}");
        // Nothing after the last: a quiet poll through c answers no commit.
        let quiet = ok(
            ports[2],
            &id_on(ports[2]),
            "feed_poll",
            json!({"tables": ["users"], "token": token}),
        );
        assert_eq!(quiet["commits"], json!([]), "{quiet}");
    });
}

/// simp724's forwarded variant of the nested-pool deadlock (#1915 PR 3): two nodes, each
/// with as many `/mcp` messages in flight as its `--blocking-threads` (2), each held until
/// all four are, whose reads are of the other node's partition. Had a message held a pool
/// permit while its op ran, each node's permits would be held by its own messages while
/// their forwarded ops waited for the other's: no answer until the requests' timeout. With
/// the `/mcp` places, every read answers.
#[test]
fn mcp_messages_forwarding_to_each_other_on_full_nodes_all_answer() {
    bounded(Duration::from_secs(120), || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let listeners: Vec<std::net::TcpListener> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();
        let map = three_way(&[ports[0], ports[1], ports[2]]);
        let mut stops = Vec::new();
        let mut dbs = Vec::new();
        for ((l, id), p) in listeners
            .into_iter()
            .zip(["a", "b", "c"])
            .zip(["p1", "p2", "p3"])
        {
            let dir = test_dir();
            super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
            let limits = crate::serve::config::Limits {
                mcp_blocking: 2,
                ..crate::serve::server_tests::limits()
            };
            let (stop, db) = data_node_with(&rt, &dir, id, Some(p), l, limits);
            stops.push(stop);
            dbs.push(db.unwrap());
        }
        // Rows on p1 (a) and p2 (b).
        rt.block_on(async {
            for (port, row) in [(ports[0], "a1"), (ports[1], "h1")] {
                data(port)
                    .await
                    .mutate(MutateRequest {
                        replication: 0,
                        writes: Some(put_row(row, "v")),
                        durability: 0,
                    })
                    .await
                    .unwrap();
            }
        });
        // a's and b's messages wait for all four at the hold before their ops.
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let held: Vec<PathBuf> = dbs[..2].iter().map(|d| d.path().to_path_buf()).collect();
        crate::serve::mcp_backend::HOLD
            .lock()
            .unwrap()
            .extend(held.iter().map(|p| (p.clone(), Arc::clone(&barrier))));
        let reads: Vec<_> = [
            (ports[0], "h1"),
            (ports[0], "h1"),
            (ports[1], "a1"),
            (ports[1], "a1"),
        ]
        .into_iter()
        .map(|(port, row)| {
            let id = id_on(port);
            std::thread::spawn(move || {
                call_at(
                    &url(port),
                    None,
                    &id,
                    "read_row",
                    json!({"table": "users", "row": row, "families": ["f"]}),
                )
            })
        })
        .collect();
        let answers: Vec<_> = reads.into_iter().map(|r| r.join().unwrap()).collect();
        crate::serve::mcp_backend::HOLD
            .lock()
            .unwrap()
            .retain(|(p, _)| !held.contains(p));
        for (out, err) in answers {
            assert!(!err, "{out}");
            assert_eq!(out["cells"][0]["value"], "v", "{out}");
        }
        drop(stops);
        for db in dbs {
            let _ = db.close();
        }
    });
}
