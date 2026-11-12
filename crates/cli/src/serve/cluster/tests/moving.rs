//! `phdb cluster move` stopped after each step, as a killed process would be, then run
//! again: the rerun finishes the move from where it was, and drops the old node's files only
//! when it could check the copy itself. Otherwise it says where they are and how to drop
//! them once checked.

use super::*;
use crate::serve::cluster::moving::{STOP, Step};

/// Three nodes: a serves p1, b p2, c nothing. A row of p1's, through b.
struct Cluster {
    rt: tokio::runtime::Runtime,
    ports: [u16; 3],
    dirs: [PathBuf; 3],
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    dbs: Vec<pigeonhole::Pigeonhole>,
    args: super::super::cli::NodeArgs,
}

impl Cluster {
    fn start() -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let listeners: Vec<std::net::TcpListener> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
        let mut map = the_map(1, &ports);
        map.partitions.retain(|p| p.id != "p3");
        let dirs = [test_dir(), test_dir(), test_dir()];
        let mut stops = Vec::new();
        let mut dbs = Vec::new();
        for ((l, dir), (id, local)) in listeners.into_iter().zip(&dirs).zip([
            ("a", Some("p1")),
            ("b", Some("p2")),
            ("c", None),
        ]) {
            super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
            let (stop, db) = data_node_tokens(&rt, dir, id, local, l, "");
            stops.push(stop);
            dbs.extend(db);
        }
        let args = super::super::cli::NodeArgs {
            node: format!("http://127.0.0.1:{}", ports[1]),
            token_file: None,
            ca: None,
        };
        rt.block_on(put(ports[1], "alice", "1"));
        Self {
            rt,
            ports,
            dirs,
            stops,
            dbs,
            args,
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for stop in self.stops.drain(..) {
            let _ = stop.send(());
        }
        for db in self.dbs.drain(..) {
            let _ = db.close();
        }
    }
}

#[test]
fn a_move_stopped_after_any_step_finishes_when_run_again() {
    for step in [
        Step::Closed,
        Step::Copied,
        Step::Stamped,
        Step::Mapped,
        Step::Serving,
    ] {
        let t = Cluster::start();
        let [a, b, _] = t.ports;
        t.rt.block_on(async {
            STOP.lock().unwrap().push((t.args.node.clone(), step));
            let e = super::super::moving::run(&t.args, "p1", "c", true)
                .await
                .err()
                .unwrap_or_else(|| panic!("{step:?}: not stopped"));
            assert!(
                e.message().contains("stopped after"),
                "{step:?}: {}",
                e.message()
            );

            let r = super::super::moving::run(&t.args, "p1", "c", true)
                .await
                .unwrap_or_else(|e| panic!("{step:?}: the rerun failed: {}", e.message()));
            let doc = &r.document;
            let a_kept = t.dirs[0].join("p1.phdb").exists();
            match step {
                // Before the stamp: the rerun copies (what's left), stamps and checks the
                // copy itself, so it drops a's files.
                Step::Closed | Step::Copied => {
                    assert_eq!(doc["verified"], true, "{step:?}: {doc:?}");
                    let source = doc["source"].as_str().unwrap();
                    assert!(source.starts_with("moved aside"), "{step:?}: {source}");
                    assert!(!a_kept, "{step:?}: a's files went aside");
                    if step == Step::Copied {
                        // Every upload was complete: nothing sent again.
                        for f in doc["copied"].as_array().unwrap() {
                            assert_eq!(f["sent"], 0, "{step:?}: {f}");
                        }
                    }
                }
                // After the stamp: the move finishes, but its copy can't be checked again.
                Step::Stamped => {
                    assert_eq!(doc["verified"], false, "{step:?}: {doc:?}");
                    let source = doc["source"].as_str().unwrap();
                    assert!(source.contains("not re-verified"), "{source}");
                    assert!(source.contains("drop-partition p1"), "{source}");
                    assert!(a_kept, "{step:?}: a's files stay");
                }
                // After the map: done, and the advice names a's closed copy and its drop.
                Step::Mapped | Step::Serving => {
                    let done = doc["done"].as_str().unwrap();
                    assert!(done.contains("nothing was dropped"), "{step:?}: {done}");
                    assert!(
                        done.contains(&format!(
                            "phdb cluster drop-partition p1 --node http://127.0.0.1:{a}"
                        )),
                        "{step:?}: {done}"
                    );
                    assert!(a_kept, "{step:?}: a's files stay");
                }
            }
            // Whatever the step: c serves p1, with its rows and its history.
            eventually("c to serve p1", async || {
                get_row(b, "alice").await.ok().flatten() == Some(b"1".to_vec())
            })
            .await;
            let m = client(b)
                .await
                .get_map(crate::serve::proto::GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let p1 = m.partitions.iter().find(|p| p.id == "p1").unwrap();
            assert_eq!(
                (p1.node.as_str(), p1.epoch, p1.history_epoch),
                ("c", 2, 0),
                "{step:?}"
            );
        });
    }
}
