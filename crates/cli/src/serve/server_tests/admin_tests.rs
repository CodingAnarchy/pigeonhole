use super::*;

// ---- Schema (D313 ticket 6) ----

#[test]
fn schema_creates_describes_lists_and_drops_tables() {
    let s = Server::start("", limits());
    let spec = json!({"name": "m", "if_missing": false, "families": [
        {"name": "h", "aggregate": {"kind": "AGGREGATION_KIND_HISTOGRAM", "histogram_bounds": [1.0, 10.0, 100.0]}},
        {"name": "b", "aggregate": {"kind": "AGGREGATION_KIND_BLOOM", "bloom_bits": 1024, "bloom_hashes": 3}},
        {"name": "v", "vector": {"dim": 4, "element": "ELEMENT_F32", "metric": "METRIC_COSINE"}},
        {"name": "o", "max_versions": 3, "ttl_micros": "60000000", "bloom_bits": 10,
         "compression": {"codec": "COMPRESSION_CODEC_ZSTD", "zstd_level": 5},
         "cache_priority": "PRIORITY_HIGH", "compaction": "COMPACTION_STYLE_TIERED"}
    ]});
    let (status, created) = s.rpc("Schema/CreateTable", &spec, None);
    assert_eq!(status, 200, "{created}");
    let (_, described) = s.rpc("Schema/DescribeTable", &json!({"name": "m"}), None);
    assert_eq!(created, described);
    let fam = |n: &str| {
        described["families"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == n)
            .unwrap()
            .clone()
    };
    assert_eq!(
        fam("h")["aggregate"]["histogram_bounds"],
        json!([1.0, 10.0, 100.0])
    );
    assert_eq!(fam("b")["aggregate"]["bloom_hashes"], 3);
    assert_eq!(fam("v")["vector"]["metric"], "METRIC_COSINE");
    let o = fam("o");
    assert_eq!(
        (o["max_versions"].as_u64(), o["ttl_micros"].as_str()),
        (Some(3), Some("60000000"))
    );
    assert_eq!(o["compression"]["zstd_level"], 5);
    assert_eq!(
        (o["cache_priority"].as_str(), o["compaction"].as_str()),
        (Some("PRIORITY_HIGH"), Some("COMPACTION_STYLE_TIERED"))
    );
    // A described table creates again as itself: the spec round-trips.
    let mut again = described.clone();
    again["name"] = json!("m2");
    let (status, made) = s.rpc("Schema/CreateTable", &again, None);
    assert_eq!(status, 200, "{made}");
    let mut renamed = made.clone();
    renamed["name"] = json!("m");
    assert_eq!(renamed, described);
    // if_missing opens the table and adds only the families it lacks.
    let (status, grown) = s.rpc(
        "Schema/CreateTable",
        &json!({"name": "m", "if_missing": true, "families": [{"name": "o", "max_versions": 9}, {"name": "x"}]}),
        None,
    );
    assert_eq!(status, 200, "{grown}");
    let names: Vec<&str> = grown["families"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"x") && names.len() == 5, "{grown}");
    let o = grown["families"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "o")
        .unwrap();
    assert_eq!(
        o["max_versions"], 3,
        "an existing family keeps its stored options"
    );
    // Without if_missing, an existing table is TableExists.
    let (status, e) = s.rpc("Schema/CreateTable", &json!({"name": "m"}), None);
    assert_eq!(
        (status, e["code"].as_str()),
        (409, Some("TableExists")),
        "{e}"
    );
    // Listed, over the RPC and GET /v1/tables; then dropped.
    let (_, list) = s.rpc("Schema/ListTables", &json!({}), None);
    assert_eq!(list["tables"], json!(["m", "m2", "t"]));
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let got: Json = agent
        .get(format!("{}/v1/tables", s.url))
        .call()
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    assert_eq!(got, list);
    assert_eq!(
        s.rpc("Schema/DropTable", &json!({"name": "m2"}), None).0,
        200
    );
    let (status, e) = s.rpc("Schema/DescribeTable", &json!({"name": "m2"}), None);
    assert_eq!(
        (status, e["code"].as_str()),
        (404, Some("TableNotFound")),
        "{e}"
    );
    // A bad spec is InvalidArgument and creates nothing.
    let (status, e) = s.rpc(
        "Schema/CreateTable",
        &json!({"name": "bad", "families": [{"name": "a", "aggregate": {}}]}),
        None,
    );
    assert_eq!(
        (status, e["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{e}"
    );
    assert_eq!(
        s.rpc("Schema/DescribeTable", &json!({"name": "bad"}), None)
            .0,
        404
    );
}

#[test]
fn schema_changes_need_their_scope() {
    let s = Server::start("read r3ad\nwrite wr1te\nadmin adm1n\n", limits());
    let create = json!({"name": "n", "families": [{"name": "f"}]});
    let (status, e) = s.rpc("Schema/CreateTable", &create, Some("r3ad"));
    assert_eq!(
        (status, e["code"].as_str()),
        (403, Some("PermissionDenied")),
        "{e}"
    );
    assert_eq!(s.rpc("Schema/CreateTable", &create, Some("wr1te")).0, 200);
    assert_eq!(
        s.rpc("Schema/DescribeTable", &json!({"name": "n"}), Some("r3ad"))
            .0,
        200
    );
    // Dropping a table drops its data: admin only.
    let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "n"}), Some("wr1te"));
    assert_eq!(
        (status, e["code"].as_str()),
        (403, Some("PermissionDenied")),
        "{e}"
    );
    assert_eq!(
        s.rpc("Schema/DropTable", &json!({"name": "n"}), Some("adm1n"))
            .0,
        200
    );
}

#[test]
fn a_table_dropped_and_created_again_is_a_new_table_to_its_feeds() {
    // feed1248's R2 on #1518, deferred to ticket 6: hubs are keyed by the table's id, and
    // the service's handle for a name is replaced when it's dropped and created again, so
    // a subscriber after that follows the new table, never the old one's hub.
    let s = Server::start("", limits());
    let create = json!({"name": "r", "families": [{"name": "f"}]});
    assert_eq!(s.rpc("Schema/CreateTable", &create, None).0, 200);
    let put_r = |row: &str| {
        let (status, body) = s.post(
            "Mutate",
            &json!({"writes": {"row": {"table": "r", "row": b64(row.as_bytes())},
                "writes": [{"put": {"family": "f", "qualifier": b64(b"q"), "value": {"bytes": b64(b"v")}}}]}}),
            None,
        );
        assert_eq!(status, 200, "{body}");
        body["commit"]["seqno"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let mut old = direct_feed(&s, subscribe(&["r"], b""));
    let first = put_r("a");
    assert_eq!(
        drain(&s, &mut old, Duration::from_millis(500))
            .iter()
            .map(|(n, _, _)| *n)
            .collect::<Vec<_>>(),
        [first]
    );
    // Dropped and created again under the same name, while the old stream is open.
    assert_eq!(
        s.rpc("Schema/DropTable", &json!({"name": "r"}), None).0,
        200
    );
    assert_eq!(s.rpc("Schema/CreateTable", &create, None).0, 200);
    let mut new = direct_feed(&s, subscribe(&["r"], b""));
    let second = put_r("b");
    assert_eq!(
        drain(&s, &mut new, Duration::from_millis(500))
            .iter()
            .map(|(n, _, _)| *n)
            .collect::<Vec<_>>(),
        [second],
        "the new table's commits reach a new subscriber"
    );
    assert!(s.shared.router.service.part().unwrap().feeds.gauges().hubs >= 1);
    // The old stream never gets the new table's commit: it follows the dropped table, and
    // ends with an error rather than go on silently.
    let rest: Vec<_> =
        std::iter::from_fn(|| next_event(&s, &mut old, Duration::from_millis(500))).collect();
    assert!(
        rest.iter().all(|e| e
            .as_ref()
            .map_or(true, |e| commit_of(e).is_none_or(|(n, _)| n != second))),
        "{rest:?}"
    );
}

// ---- Admin (D313 ticket 6) ----

#[test]
fn admin_reports_maintains_and_answers_health_without_a_token() {
    let s = Server::start("read r3ad\nadmin adm1n\n", limits());
    // Health needs no token, over HTTP's alias, its RPC path, and gRPC.
    let (status, h) = s.rpc("Admin/Health", &json!({}), None);
    assert_eq!((status, h["status"].as_str()), (200, Some("ok")), "{h}");
    let mut admin = s.block_on(async {
        proto::admin_client::AdminClient::new(
            tonic::transport::Channel::from_shared(s.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap(),
        )
    });
    let h = s
        .block_on(admin.health(proto::Empty {}))
        .unwrap()
        .into_inner();
    assert_eq!(h.status, "ok");
    // Info and Metrics with read scope; maintenance with admin only.
    let (status, info) = s.rpc("Admin/Info", &json!({}), Some("r3ad"));
    assert_eq!(status, 200, "{info}");
    assert_eq!(info["tables"], json!(["t"]));
    assert_eq!(info["version"], env!("CARGO_PKG_VERSION"));
    assert!(info["path"].as_str().unwrap().ends_with("t.phdb"), "{info}");
    let (status, m) = s.rpc("Admin/Metrics", &json!({}), Some("r3ad"));
    assert_eq!(status, 200);
    assert!(
        m["text"]
            .as_str()
            .unwrap()
            .contains("pigeonhole_serve_requests_total"),
        "{m}"
    );
    for rpc in ["Admin/Flush", "Admin/Compact"] {
        let (status, e) = s.rpc(rpc, &json!({}), Some("r3ad"));
        assert_eq!(
            (status, e["code"].as_str()),
            (403, Some("PermissionDenied")),
            "{rpc}: {e}"
        );
        assert_eq!(s.rpc(rpc, &json!({}), Some("adm1n")).0, 200, "{rpc}");
    }
}

#[test]
fn a_backup_lands_inside_the_backup_dir_only() {
    let s = Server::start("", limits());
    assert_eq!(
        s.post(
            "Mutate",
            &put("r1", "f", "q", json!({"bytes": b64(b"v")})),
            None
        )
        .0,
        200
    );
    let (status, b) = s.rpc("Admin/Backup", &json!({"path": "one.phdb"}), None);
    assert_eq!(status, 200, "{b}");
    let file = s.dir.join("backups/one.phdb");
    assert_eq!(
        b["bytes"].as_str().unwrap().parse::<u64>().unwrap(),
        std::fs::metadata(&file).unwrap().len()
    );
    assert_eq!(rows_in_backup(&file), (1, true));
    // A subdirectory is made inside --backup-dir.
    assert_eq!(
        s.rpc("Admin/Backup", &json!({"path": "nightly/two.phdb"}), None)
            .0,
        200
    );
    assert_eq!(
        rows_in_backup(&s.dir.join("backups/nightly/two.phdb")),
        (1, true)
    );
    for (path, why) in [
        ("one.phdb", "exists"),
        ("../out.phdb", "escapes"),
        ("/tmp/x.phdb", "absolute"),
    ] {
        let (status, e) = s.rpc("Admin/Backup", &json!({"path": path}), None);
        assert_eq!(
            (status, e["code"].as_str()),
            (400, Some("InvalidArgument")),
            "{why}: {e}"
        );
    }
}

#[test]
fn a_fenced_file_streams_as_it_lies_from_any_offset() {
    // D317 ticket 6: a closed partition's file goes out raw, with its size and SHA-256 as
    // hashed at the close, for a relocation by copy. The cluster node looks the file up
    // (ticket 6's close records it); this is the stream it answers with.
    use sha2::{Digest, Sha256};
    let dir = test_dir();
    let db = Pigeonhole::open(
        dir.join("db.phdb"),
        Options::default().create_if_missing(true),
    )
    .unwrap();
    let service = Service::new(db, limits(), 2, Arc::new(Metrics::default()));
    // Two and a half chunks, not all the same byte.
    let bytes: Vec<u8> = (0..(5u32 << 19)).map(|i| (i % 251) as u8).collect();
    let path = dir.join("p1.phdb-feed");
    std::fs::write(&path, &bytes).unwrap();
    let file = proto::PartitionFile {
        name: "-feed".into(),
        bytes: bytes.len() as u64,
        sha256: Sha256::digest(&bytes).to_vec().into(),
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let fetch = |offset: u64, durable: bool| {
        let req = proto::BackupDownloadRequest {
            partition: "p1".into(),
            fenced_file: "-feed".into(),
            offset,
            durable,
            ..Default::default()
        };
        rt.block_on(async {
            let mut d = service
                .fenced_download(path.clone(), file.clone(), req)
                .await?;
            let mut out = Vec::new();
            while let Some(c) = d.chunks.recv().await {
                out.push(c?);
            }
            Ok::<_, errors::ApiError>(out)
        })
    };
    let all = fetch(0, false).unwrap();
    assert_eq!(all.len(), 3, "three chunks of at most 1 MiB");
    let got: Vec<u8> = all.iter().flat_map(|c| c.data.to_vec()).collect();
    assert_eq!(got, bytes);
    let last = all.last().unwrap();
    assert_eq!(last.total, bytes.len() as u64);
    assert_eq!(
        last.sha256, file.sha256,
        "the last chunk carries the file's SHA-256"
    );
    assert!(all[..2].iter().all(|c| c.total == 0 && c.sha256.is_empty()));
    // A resume from an offset is the rest of the file; the end itself is one empty last
    // chunk; past it is refused.
    let rest = fetch(1 << 20, false).unwrap();
    assert_eq!(rest[0].offset, 1 << 20);
    let tail: Vec<u8> = rest.iter().flat_map(|c| c.data.to_vec()).collect();
    assert_eq!(tail, bytes[1 << 20..]);
    let end = fetch(bytes.len() as u64, false).unwrap();
    assert_eq!((end.len(), end[0].data.len()), (1, 0));
    assert_eq!(end[0].sha256, file.sha256);
    assert!(fetch(bytes.len() as u64 + 1, false).is_err());
    // A file goes as it lies: no durable copy of it.
    assert_eq!(fetch(0, true).unwrap_err().code, "InvalidArgument");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_single_node_refuses_a_fenced_file() {
    let s = Server::start("", limits());
    let (status, body) = s.rpc(
        "Admin/BackupDownload",
        &json!({"fenced_file": "-feed"}),
        None,
    );
    assert_eq!(status, 400, "{body}");
    assert!(body.to_string().contains("cluster node's"), "{body}");
}

#[test]
fn a_backup_download_streams_resumes_and_checks_out() {
    use sha2::{Digest, Sha256};
    let s = Server::start("", limits());
    for i in 0..50u32 {
        let value = json!({"bytes": b64(&vec![b'v'; 4096])});
        assert_eq!(
            s.post("Mutate", &put(&format!("r{i:02}"), "f", "q", value), None)
                .0,
            200
        );
    }
    let mut admin = s.block_on(async {
        proto::admin_client::AdminClient::new(
            tonic::transport::Channel::from_shared(s.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap(),
        )
    });
    let chunks =
        |req: proto::BackupDownloadRequest,
         admin: &mut proto::admin_client::AdminClient<tonic::transport::Channel>| {
            s.block_on(async {
                let mut stream = admin.backup_download(req).await?.into_inner();
                let mut out = Vec::new();
                while let Some(c) = stream.message().await? {
                    out.push(c);
                }
                Ok::<_, tonic::Status>(out)
            })
        };
    let all = chunks(Default::default(), &mut admin).unwrap();
    let last = all.last().unwrap();
    let bytes: Vec<u8> = all.iter().flat_map(|c| c.data.to_vec()).collect();
    assert_eq!(last.total, bytes.len() as u64);
    assert_eq!(
        &last.sha256[..],
        &Sha256::digest(&bytes)[..],
        "the last chunk's SHA-256 is the file's"
    );
    assert!(
        all.iter()
            .all(|c| c.data.len() <= 1 << 20 && c.backup_id == last.backup_id)
    );
    let copy = s.dir.join("download.phdb");
    std::fs::write(&copy, &bytes).unwrap();
    assert_eq!(rows_in_backup(&copy).0, 50);
    // A resume from an offset gives the rest of the same file.
    let half = bytes.len() as u64 / 2;
    let rest = chunks(
        proto::BackupDownloadRequest {
            backup_id: last.backup_id.clone(),
            offset: half,
            ..Default::default()
        },
        &mut admin,
    )
    .unwrap();
    assert_eq!(rest[0].offset, half);
    let tail: Vec<u8> = rest.iter().flat_map(|c| c.data.to_vec()).collect();
    assert_eq!(tail, bytes[half as usize..]);
    // An id the server doesn't keep is NotFound.
    let e = chunks(
        proto::BackupDownloadRequest {
            backup_id: "0".repeat(32),
            ..Default::default()
        },
        &mut admin,
    )
    .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound, "{e:?}");
    // Over HTTP: the bytes, with the id and SHA-256 as headers; Range resumes, 206.
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let mut resp = agent
        .post(format!("{}/v1/Admin/BackupDownload", s.url))
        .send_json(json!({}))
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    let id = resp
        .headers()
        .get("x-pigeonhole-backup-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let sha = resp
        .headers()
        .get("x-pigeonhole-backup-sha256")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let body = resp
        .body_mut()
        .with_config()
        .limit(u64::MAX)
        .read_to_vec()
        .unwrap();
    let hex: String = Sha256::digest(&body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(hex, sha);
    let mut resp = agent
        .post(format!("{}/v1/Admin/BackupDownload?backup_id={id}", s.url))
        .header("Range", "bytes=100-")
        .send_json(json!({}))
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    let tail = resp
        .body_mut()
        .with_config()
        .limit(u64::MAX)
        .read_to_vec()
        .unwrap();
    assert_eq!(tail, body[100..]);
}

#[test]
fn racing_creates_and_drops_leave_the_cache_on_the_live_table() {
    // simp724's C1 on #1527: a create whose cache update ran after a drop and a newer create
    // could leave the name on the dropped table. Creates and drops are now serialized with
    // their cache updates (`Service::schema`), so whatever the order, a table that exists
    // takes writes. A smoke test only: the race's window (between a create's blocking call
    // and its cache update) is too small to hit without a hold, so it passes without the
    // lock too. The fix holds by construction.
    let s = Server::start("", limits());
    let service = &s.shared.router.service;
    let caller = Caller::anonymous();
    let create = || {
        ServiceOp::CreateTable(proto::CreateTableRequest {
            name: "c".into(),
            families: vec![proto::FamilySpec {
                name: "f".into(),
                ..Default::default()
            }],
            if_missing: true,
            placement: None,
        })
    };
    let drop_c = || ServiceOp::DropTable(proto::DropTableRequest { name: "c".into() });
    let put_c = || {
        ServiceOp::Mutate(proto::MutateRequest {
            writes: serde_json::from_value(json!({"row": {"table": "c", "row": b64(b"r")},
                "writes": [{"put": {"family": "f", "qualifier": b64(b"q"), "value": {"bytes": b64(b"v")}}}]}))
            .unwrap(),
            durability: 0,
        })
    };
    for round in 0..30 {
        s.block_on(async {
            let (a, b, c) = tokio::join!(
                service.call(create(), &caller, None),
                service.call(drop_c(), &caller, None),
                service.call(create(), &caller, None),
            );
            // A drop of a table not there yet fails; the creates always succeed.
            assert!(
                a.is_ok() && c.is_ok(),
                "round {round}: {:?} {:?}",
                a.err(),
                c.err()
            );
            let _ = b;
        });
        if service.part().unwrap().db.tables().iter().any(|t| t == "c") {
            let put = s.block_on(service.call(put_c(), &caller, None));
            assert!(
                put.is_ok(),
                "round {round}: the cache is on a dropped table: {:?}",
                put.err()
            );
        }
    }
}

/// The serve guide's `/metrics` table names exactly the metric families serve renders itself
/// (ticket 9c): a family added without a row, or a row for one that's gone, fails here. The
/// engine's families are `pigeonhole-otel`'s, checked against the observability guide there.
#[test]
fn the_serve_guide_lists_every_metric_serve_renders() {
    let guide = include_str!("../../../../../docs/guide/serve.md");
    let names = |text: &str, prefix: &str| -> std::collections::BTreeSet<String> {
        let mut out = std::collections::BTreeSet::new();
        let mut rest = text;
        while let Some(at) = rest.find(prefix) {
            let tail = &rest[at..];
            let end = tail
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(tail.len());
            // A histogram's series carry suffixes; the family is the name without them.
            let name = tail[..end]
                .trim_end_matches("_bucket")
                .trim_end_matches("_sum")
                .trim_end_matches("_count");
            out.insert(name.to_owned());
            rest = &tail[end..];
        }
        out
    };
    // Serve's own families: a primary's, then a replica's (rendered only on a replica). The
    // primary has a placement, as every `phdb serve` does: the hub thread gauge is its
    // (#1719).
    let dir = test_dir();
    let placed = Pigeonhole::open(
        dir.join("p.phdb"),
        Options::default().create_if_missing(true).shards(1),
    )
    .unwrap();
    let plan = Arc::new(crate::serve::placement::Placement::plan(
        &placed, None, None, None, None, true,
    ));
    placed.close().unwrap();
    let p = Server::start_custom("", limits(), 1, move |svc| svc.placement(&plan));
    let r = Server::start_replica(test_dir(), &p.url, None);
    for (server, prefix) in [(&p, "pigeonhole_serve_"), (&r, "pigeonhole_replica_")] {
        let rendered = names(&server.shared.router.metrics_text(), prefix);
        assert!(!rendered.is_empty(), "{prefix}");
        assert_eq!(
            names(guide, prefix),
            rendered,
            "{prefix}: the guide's table against /metrics"
        );
    }
}
