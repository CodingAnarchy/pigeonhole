use super::*;

#[test]
fn http_json_writes_and_reads_a_cell() {
    let s = Server::start("", limits());
    let (status, body) = s.post(
        "Mutate",
        &put("r1", "f", "q", json!({"bytes": b64(b"hello")})),
        None,
    );
    assert_eq!(status, 200, "{body}");
    assert!(
        body["commit"]["seqno"].is_string(),
        "64-bit integers are strings: {body}"
    );
    let (status, body) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"r1"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["cell"]["value"], b64(b"hello"));
    // A cell that isn't there is an empty reply, not an error.
    let (status, body) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"nope"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!((status, body.get("cell").is_none()), (200, true), "{body}");
}

#[test]
fn errors_carry_their_stable_code_in_both_protocols() {
    let s = Server::start("", limits());
    // HTTP: the status and the ErrorDetail body.
    let (status, body) = s.post(
        "Get",
        &json!({"table": "missing", "row": b64(b"r"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!(status, 404, "{body}");
    assert_eq!(body["code"], "TableNotFound");
    assert!(body.get("retry_safe").is_none(), "not retry-safe: {body}");
    let (status, body) = s.post("Nope", &json!({}), None);
    assert_eq!((status, body["code"].as_str()), (404, Some("NotFound")));
    // gRPC: the status code and the ErrorDetail in its details.
    let mut client = s.grpc();
    let err = s
        .block_on(client.get(proto::GetRequest {
            table: "missing".into(),
            row: b"r".to_vec().into(),
            ..Default::default()
        }))
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    #[derive(Clone, PartialEq, Message)]
    struct Status {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
        #[prost(message, repeated, tag = "3")]
        details: Vec<AnyMsg>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct AnyMsg {
        #[prost(string, tag = "1")]
        type_url: String,
        #[prost(bytes = "vec", tag = "2")]
        value: Vec<u8>,
    }
    let status = Status::decode(err.details()).unwrap();
    let detail = proto::ErrorDetail::decode(status.details[0].value.as_slice()).unwrap();
    assert_eq!(detail.code, "TableNotFound");
}

#[test]
fn grpc_covers_the_data_writes() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    let row = |r: &str, writes: Vec<proto::Write>| proto::RowWrites {
        row: Some(proto::RowKey {
            table: "t".into(),
            row: r.as_bytes().to_vec().into(),
        }),
        writes,
    };
    let put = |q: &str, v: &[u8]| proto::Write {
        op: Some(proto::write::Op::Put(proto::Put {
            family: "f".into(),
            qualifier: q.as_bytes().to_vec().into(),
            timestamp: None,
            value: Some(proto::Value {
                kind: Some(proto::value::Kind::Bytes(v.to_vec().into())),
            }),
        })),
    };
    let incr = |n: i64| proto::Write {
        op: Some(proto::write::Op::Incr(proto::Incr {
            family: "n".into(),
            qualifier: b"c".to_vec().into(),
            timestamp: None,
            delta: n,
        })),
    };
    // CheckAndMutate: absent, so it applies; then present, so it doesn't.
    let absent = proto::Condition {
        kind: Some(proto::condition::Kind::Absent(proto::Column {
            family: "f".into(),
            qualifier: b"lock".to_vec().into(),
        })),
    };
    let cm = |c: &mut DataClient<_>| {
        let req = proto::CheckAndMutateRequest {
            writes: Some(row("job", vec![put("lock", b"me")])),
            condition: Some(absent.clone()),
            durability: 0,
        };
        s.block_on(c.check_and_mutate(req)).unwrap().into_inner()
    };
    assert!(cm(&mut c).applied);
    let second = cm(&mut c);
    assert!(!second.applied && second.commit.is_none());
    // Batch across rows.
    let batch = proto::BatchRequest {
        rows: vec![
            row("a", vec![put("x", b"1")]),
            row("b", vec![put("x", b"2")]),
        ],
        durability: proto::Durability::Sync as i32,
    };
    let committed = s
        .block_on(c.batch(batch))
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    assert_eq!(committed.durability, proto::Durability::Sync as i32);
    // MutateReturning hands back the counter after the commit.
    for want in [3, 5] {
        let req = proto::MutateRequest {
            writes: Some(row("ctr", vec![incr(if want == 3 { 3 } else { 2 })])),
            durability: 0,
        };
        let r = s.block_on(c.mutate_returning(req)).unwrap().into_inner();
        assert_eq!(
            r.cells[0].value,
            Some(proto::returned_cell::Value::I64(want))
        );
    }
    // MultiGet: any order and duplicates in, key order out.
    let mg = proto::MultiGetRequest {
        table: "t".into(),
        keys: vec![
            b"b".to_vec().into(),
            b"a".to_vec().into(),
            b"b".to_vec().into(),
        ],
        ..Default::default()
    };
    let rows = s.block_on(c.multi_get(mg)).unwrap().into_inner().rows;
    assert_eq!(
        rows.iter().map(|r| r.key.to_vec()).collect::<Vec<_>>(),
        [b"a".to_vec(), b"b".to_vec()]
    );
}

#[test]
fn a_wide_row_reads_in_pages_cut_by_bytes() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    let writes: Vec<proto::Write> = (0..40)
        .map(|i| proto::Write {
            op: Some(proto::write::Op::Put(proto::Put {
                family: "f".into(),
                qualifier: format!("q{i:03}").into_bytes().into(),
                timestamp: None,
                value: Some(proto::Value {
                    kind: Some(proto::value::Kind::Bytes(vec![b'x'; 1000].into())),
                }),
            })),
        })
        .collect();
    let rw = proto::RowWrites {
        row: Some(proto::RowKey {
            table: "t".into(),
            row: b"wide".to_vec().into(),
        }),
        writes,
    };
    s.block_on(c.mutate(proto::MutateRequest {
        writes: Some(rw),
        durability: 0,
    }))
    .unwrap();
    let (mut after, mut seen, mut pages) = (Vec::new(), Vec::new(), 0);
    loop {
        let req = proto::ReadRowRequest {
            table: "t".into(),
            row: b"wide".to_vec().into(),
            page_bytes: 8000,
            after: after.clone().into(),
            ..Default::default()
        };
        let page = s.block_on(c.read_row(req)).unwrap().into_inner();
        pages += 1;
        assert!(
            page.bytes <= 8000 + 8,
            "a page stays within its budget: {}",
            page.bytes
        );
        seen.extend(
            page.row
                .unwrap()
                .cells
                .into_iter()
                .map(|c| c.qualifier.to_vec()),
        );
        if page.next.is_empty() {
            break;
        }
        after = page.next.to_vec();
    }
    assert!(pages >= 5, "40 KB of cells in 8 KB pages: {pages}");
    assert_eq!(
        seen,
        (0..40)
            .map(|i| format!("q{i:03}").into_bytes())
            .collect::<Vec<_>>(),
        "every cell once, in order"
    );
}

#[test]
fn tokens_and_scopes_decide_who_may_do_what() {
    let s = Server::start("read r3ad\nwrite wr1te\n", limits());
    let write = put("r", "f", "q", json!({"bytes": b64(b"v")}));
    let get = json!({"table": "t", "row": b64(b"r"), "family": "f", "qualifier": b64(b"q")});
    assert_eq!(s.post("Get", &get, None).0, 401, "a token is required");
    assert_eq!(s.post("Get", &get, Some("guess")).0, 401);
    let (status, body) = s.post("Mutate", &write, Some("r3ad"));
    assert_eq!(
        (status, body["code"].as_str()),
        (403, Some("PermissionDenied")),
        "{body}"
    );
    assert_eq!(s.post("Mutate", &write, Some("wr1te")).0, 200);
    assert_eq!(s.post("Get", &get, Some("r3ad")).0, 200);
    // Health needs no token; metrics do.
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    assert_eq!(
        agent
            .get(format!("{}/v1/health", s.url))
            .call()
            .unwrap()
            .status()
            .as_u16(),
        200
    );
    assert_eq!(
        agent
            .get(format!("{}/metrics", s.url))
            .call()
            .unwrap()
            .status()
            .as_u16(),
        401
    );
    let mut m = agent
        .get(format!("{}/metrics", s.url))
        .header("Authorization", "Bearer r3ad")
        .call()
        .unwrap();
    let text = m.body_mut().read_to_string().unwrap();
    assert!(
        text.contains("pigeonhole_serve_requests_total{rpc=\"Data.Mutate\",outcome=\"ok\"} 1"),
        "{text}"
    );
    // The engine's metrics, under D316's names (D323), `async_sync_reads` among them as
    // before, now with the database's name.
    for family in [
        "pigeonhole_async_sync_reads_total",
        "pigeonhole_commits_total",
        "pigeonhole_write_stall_time_seconds_total",
        "pigeonhole_failed_ratio",
    ] {
        assert!(
            text.contains(&format!("{family}{{pigeonhole_db_name=\"")),
            "{family}:\n{text}"
        );
    }
    // The engine's names stay out of the server's own namespaces, so they can't clash as
    // either side grows (perf287's nit 1 on #1687).
    for m in pigeonhole_otel::metrics() {
        let name = m.prometheus_name();
        assert!(
            !name.starts_with("pigeonhole_serve_") && !name.starts_with("pigeonhole_replica_"),
            "{name}"
        );
    }
    // Each family is declared once: Prometheus refuses a scrape that repeats one.
    let mut types: Vec<&str> = text.lines().filter(|l| l.starts_with("# TYPE ")).collect();
    let n = types.len();
    types.sort_unstable();
    types.dedup();
    assert_eq!(types.len(), n, "{text}");
}

#[test]
fn a_request_over_the_size_limit_is_refused() {
    let mut l = limits();
    l.max_request_bytes = 1024;
    let s = Server::start("", l);
    let (status, body) = s.post(
        "Mutate",
        &put("r", "f", "q", json!({"bytes": b64(&[7u8; 4096])})),
        None,
    );
    assert_eq!(
        (status, body["code"].as_str()),
        (413, Some("RequestTooLarge")),
        "{body}"
    );
}

#[test]
fn a_write_whose_client_went_away_still_commits() {
    // simp724's B2 on D313: the commit is its own task; dropping the request loses only the
    // reply, which is logged and counted.
    let s = Server::start("", limits());
    let service = &s.shared.router.service;
    let get = || {
        ServiceOp::Get(proto::GetRequest {
            table: "t".into(),
            row: b"gone".to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        })
    };
    // The table is opened once first, so the write reaches its spawn on the first poll.
    s.block_on(service.call(get(), &Caller::anonymous(), None))
        .unwrap();
    let write = ServiceOp::Mutate(proto::MutateRequest {
        writes: Some(proto::RowWrites {
            row: Some(proto::RowKey {
                table: "t".into(),
                row: b"gone".to_vec().into(),
            }),
            writes: vec![proto::Write {
                op: Some(proto::write::Op::Put(proto::Put {
                    family: "f".into(),
                    qualifier: b"q".to_vec().into(),
                    timestamp: None,
                    value: Some(proto::Value {
                        kind: Some(proto::value::Kind::Bytes(b"landed".to_vec().into())),
                    }),
                })),
            }],
        }),
        durability: 0,
    });
    s.block_on(async {
        let caller = Caller::anonymous();
        let call = service.call(write, &caller, None);
        tokio::select! {
            biased;
            _ = call => panic!("the write answered before its first await"),
            () = std::future::ready(()) => {}
        }
    });
    // The request is gone; the write isn't.
    let landed = (0..100).any(|_| {
        std::thread::sleep(Duration::from_millis(20));
        let r = s.block_on(service.call(get(), &Caller::anonymous(), None)).unwrap();
        matches!(r, super::service::ServiceReply::Get(g) if g.cell.as_ref().is_some_and(|c| &c.value[..] == b"landed"))
    });
    assert!(landed, "the detached commit landed");
    // The task counts the lost reply once its commit returns, which can be just after the
    // write became visible.
    let counted = (0..100).any(|_| {
        std::thread::sleep(Duration::from_millis(10));
        s.shared.metrics.replies_lost.load(Ordering::Relaxed) == 1
    });
    assert!(counted, "the lost reply is counted");
    // simp724's S1 on #1501: the write held its bytes until it finished, then gave them
    // back; none leaked when its request went away.
    assert_eq!(service.inflight_available(), limits().inflight_bytes);
}

#[test]
fn phdb_serve_refuses_a_reachable_address_without_tls_and_tokens() {
    // The bind rule runs before the database opens: the file needn't exist.
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let status = crate::run(
        [
            "phdb",
            "serve",
            "/nonexistent/never-opened.phdb",
            "--listen",
            "0.0.0.0:0",
        ],
        &mut out,
        &mut err,
    );
    let err = String::from_utf8_lossy(&err);
    assert_eq!(status, crate::ExitStatus::USAGE, "{err}");
    assert!(
        err.contains("refusing to serve") && err.contains("--insecure-no-tls-no-auth"),
        "{err}"
    );
}

#[test]
fn tls_files_that_dont_load_are_a_clear_error() {
    let e = super::tls::acceptor(
        std::path::Path::new("/nonexistent/cert.pem"),
        std::path::Path::new("/nonexistent/key.pem"),
    )
    .err()
    .unwrap();
    // The message isn't printed on failure: CodeQL reads a string from loading a
    // certificate as sensitive (rust/cleartext-logging), though it holds only the path.
    assert!(
        e.contains("/nonexistent/cert.pem"),
        "the error names the file"
    );
}

#[test]
fn a_scan_streams_every_row_once_in_byte_budgeted_pages() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let pages = grpc_scan(&s, &mut c, scan_request(1000, 0, Vec::new()));
    assert!(pages.len() >= 10, "36 KB in 1 KB pages: {}", pages.len());
    let (last, rest) = pages.split_last().unwrap();
    assert!(last.next.is_empty(), "the last page has no position");
    assert!(rest.iter().all(|p| !p.next.is_empty()), "the others do");
    assert!(
        rest.iter().all(|p| p.bytes <= 1000 + 450),
        "only one column may go over a page's budget"
    );
    assert_eq!(
        join(&pages),
        whole_rows(30),
        "the prefix's rows, whole, in order"
    );
}

#[test]
fn a_scans_limit_counts_rows_across_pages_and_resumes() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    // Pages cut inside rows: a continued row isn't counted twice.
    let pages = grpc_scan(&s, &mut c, scan_request(1000, 7, Vec::new()));
    assert_eq!(join(&pages), whole_rows(7));
    assert!(pages.last().unwrap().next.is_empty());
    // A client resumes from a page's position with the rows it still wants: a row the
    // position is inside finishes without counting, as the client has already seen it.
    let first = grpc_scan(&s, &mut c, scan_request(1000, 0, Vec::new()));
    let resume = first[0].next.to_vec();
    let mut resumed = vec![first[0].clone()];
    resumed[0].next = Default::default();
    let seen = join(&resumed).len();
    assert!(
        join(&resumed).last().unwrap().1.len() < 3,
        "the first page ends inside a row, so the resume continues it"
    );
    resumed.extend(grpc_scan(&s, &mut c, scan_request(1000, 3, resume)));
    assert_eq!(join(&resumed), whole_rows(seen + 3));
}

#[test]
fn a_limit_holds_when_the_row_a_position_is_inside_is_deleted() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    // A position inside a row, as a 1 KB page leaves it.
    let first = grpc_scan(&s, &mut c, scan_request(1000, 0, Vec::new()));
    let mut taken = vec![first[0].clone()];
    taken[0].next = Default::default();
    let (inside, cells) = join(&taken).last().unwrap().clone();
    assert!(cells.len() < 3, "the first page ends inside a row");
    // That row is deleted before the next page (no snapshot_id): the resumed page asks for
    // one row more than remain, and every row it brings is new (simp724's B1 on #1510).
    s.block_on(c.mutate(proto::MutateRequest {
        writes: Some(proto::RowWrites {
            row: Some(proto::RowKey {
                table: "t".into(),
                row: inside.clone().into(),
            }),
            writes: vec![proto::Write {
                op: Some(proto::write::Op::DeleteRow(proto::DeleteRow {})),
            }],
        }),
        durability: 0,
    }))
    .unwrap();
    let pages = grpc_scan(&s, &mut c, scan_request(100_000, 3, first[0].next.to_vec()));
    let at = whole_rows(30)
        .iter()
        .position(|(k, _)| *k == inside)
        .unwrap();
    assert_eq!(
        join(&pages),
        whole_rows(at + 4)[at + 1..],
        "exactly the limit's rows, after the deleted one"
    );
    assert!(
        pages.last().unwrap().next.is_empty(),
        "and the scan finished"
    );
}

/// A scan straight through the service: its pages are taken only when the test polls.
fn direct_scan(s: &Server, req: proto::ScanRequest) -> super::service::ScanPages {
    let service = &s.shared.router.service;
    match s
        .block_on(service.call(ServiceOp::Scan(req), &Caller::anonymous(), None))
        .unwrap()
    {
        super::service::ServiceReply::Scan(pages) => pages,
        _ => unreachable!(),
    }
}

/// The scan's next item, or `None` if none comes within `wait`.
fn next_within(
    s: &Server,
    pages: &mut super::service::ScanPages,
    wait: Duration,
) -> Option<Option<Result<proto::ScanPage, super::errors::ApiError>>> {
    use futures_core::Stream as _;
    s.block_on(async {
        tokio::time::timeout(
            wait,
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut *pages).poll_next(cx)),
        )
        .await
    })
    .ok()
}

#[test]
fn scan_pages_hold_the_in_flight_budget_until_taken() {
    // Room for one page, not two: a full page of a 1 KB budget is over 1 KB (perf287's G1
    // on #1510).
    let mut l = limits();
    l.inflight_bytes = 1500;
    let s = Server::start("", l);
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let service = &s.shared.router.service;
    let mut a = direct_scan(&s, scan_request(1000, 0, Vec::new()));
    wait_for("scan a's first page takes its budget", || {
        service.inflight_available() < 1500
    });
    // Scan b's first page waits for the budget a's unread page holds.
    let mut b = direct_scan(&s, scan_request(1000, 0, Vec::new()));
    assert!(
        next_within(&s, &mut b, Duration::from_millis(300)).is_none(),
        "b waits while a's page holds the budget"
    );
    // Once a's page is taken, its budget goes to b, which waited first.
    assert!(matches!(
        next_within(&s, &mut a, Duration::from_secs(5)),
        Some(Some(Ok(_)))
    ));
    assert!(matches!(
        next_within(&s, &mut b, Duration::from_secs(5)),
        Some(Some(Ok(_)))
    ));
    drop((a, b));
    wait_for("both scans' budget comes back", || {
        service.inflight_available() == 1500
    });
}

#[test]
fn a_client_that_stops_reading_loses_its_scan_after_the_idle_timeout() {
    let mut l = limits();
    l.max_scans = 1;
    l.stream_idle = Duration::from_millis(200);
    let s = Server::start("", l);
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let service = &s.shared.router.service;
    let mut stalled = direct_scan(&s, scan_request(1000, 0, Vec::new()));
    let Some(Some(Ok(first))) = next_within(&s, &mut stalled, Duration::from_secs(5)) else {
        panic!("the first page");
    };
    assert!(!first.next.is_empty());
    // The client stops reading: its queued page and budget are taken back, its slot freed,
    // and the stream ends with DeadlineExceeded (perf287's G2 on #1510).
    std::thread::sleep(Duration::from_millis(600));
    wait_for("the stalled scan's budget comes back", || {
        service.inflight_available() == limits().inflight_bytes
    });
    let Some(Some(Err(e))) = next_within(&s, &mut stalled, Duration::from_secs(5)) else {
        panic!("the stream ends with an error");
    };
    assert_eq!(e.code, "DeadlineExceeded", "{e:?}");
    assert!(e.message.contains("--stream-idle-ms"), "{e:?}");
    assert!(matches!(
        next_within(&s, &mut stalled, Duration::from_secs(1)),
        Some(None)
    ));
    // The client resumes from the last page it took, in another scan: the slot is free.
    let mut pages = vec![first.clone()];
    pages.extend(grpc_scan(
        &s,
        &mut c,
        scan_request(1000, 0, first.next.to_vec()),
    ));
    assert_eq!(join(&pages), whole_rows(30));
    let text = s.shared.metrics.render();
    assert!(
        text.contains(
            r#"pigeonhole_serve_requests_total{rpc="Data.Scan",outcome="DeadlineExceeded"} 1"#
        ),
        "{text}"
    );
}

#[test]
fn a_scan_over_http_is_ndjson_or_server_sent_events() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let body = json!({"table": "t", "range": {"prefix": b64(b"r")}, "page_bytes": "1000"});
    let decode = |page: &Json| -> proto::ScanPage { serde_json::from_value(page.clone()).unwrap() };

    let mut resp = agent
        .post(format!("{}/v1/Data/Scan", s.url))
        .send_json(&body)
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/x-ndjson"
    );
    let text = resp.body_mut().read_to_string().unwrap();
    let pages: Vec<proto::ScanPage> = text
        .lines()
        .map(|l| decode(&serde_json::from_str(l).unwrap()))
        .collect();
    assert!(pages.len() >= 10);
    assert_eq!(join(&pages), whole_rows(30));

    let mut resp = agent
        .post(format!("{}/v1/Data/Scan", s.url))
        .header("Accept", "text/event-stream")
        .send_json(&body)
        .unwrap();
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    let text = resp.body_mut().read_to_string().unwrap();
    let events: Vec<&str> = text.split("\n\n").filter(|e| !e.is_empty()).collect();
    assert!(events.iter().all(|e| e.starts_with("data: ")), "{text}");
    let pages: Vec<proto::ScanPage> = events
        .iter()
        .map(|e| decode(&serde_json::from_str(&e["data: ".len()..]).unwrap()))
        .collect();
    assert_eq!(join(&pages), whole_rows(30));

    // A bad request fails the call before any stream starts.
    let (status, err) = s.post(
        "Scan",
        &json!({"table": "t", "after": b64(b"not a position")}),
        None,
    );
    assert_eq!(
        (status, err["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{err}"
    );
    let (status, err) = s.post("Scan", &json!({"table": "nope"}), None);
    assert_eq!(err["code"], "TableNotFound", "{status} {err}");
}

#[test]
fn scans_past_the_scan_limit_are_refused_until_one_ends() {
    let mut l = limits();
    l.max_scans = 1;
    let s = Server::start("", l);
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let mut held = s.block_on(async {
        let mut stream = c
            .scan(scan_request(1000, 0, Vec::new()))
            .await
            .unwrap()
            .into_inner();
        stream.message().await.unwrap().unwrap();
        stream
    });
    let refused = s
        .block_on(c.scan(scan_request(1000, 0, Vec::new())))
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");
    assert!(refused.message().contains("scan limit"), "{refused:?}");
    // The held scan's stream stays readable to its end; once it ends, its slot is free.
    let rest = s.block_on(async {
        let mut n = 0;
        while held.message().await.unwrap().is_some() {
            n += 1;
        }
        n
    });
    assert!(rest > 0);
    drop(held);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match s.block_on(c.scan(scan_request(100_000, 0, Vec::new()))) {
            Ok(r) => {
                let mut stream = r.into_inner();
                s.block_on(async { while stream.message().await.unwrap().is_some() {} });
                break;
            }
            Err(e) if std::time::Instant::now() < deadline => {
                assert_eq!(e.code(), tonic::Code::Unavailable);
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("the slot never came back: {e:?}"),
        }
    }
}

#[test]
fn grpc_reserves_the_budget_before_decoding_and_honors_grpc_timeout() {
    use tower::ServiceExt as _;
    let mut l = limits();
    l.request_timeout = Duration::from_millis(500);
    let s = Server::start("", l);
    let service = &s.shared.router.service;
    // A raw gRPC call through the router: tonic's client enforces `grpc-timeout` itself, so
    // it would race the server's own answer.
    let call = |timeout: Option<&str>| {
        let msg = proto::GetRequest {
            table: "t".into(),
            row: b"r".to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        }
        .encode_to_vec();
        let mut framed = vec![0];
        framed.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        framed.extend_from_slice(&msg);
        let mut req = axum::http::Request::post("/pigeonhole.v1.Data/Get")
            .header("content-type", "application/grpc")
            .header("te", "trailers");
        if let Some(t) = timeout {
            req = req.header("grpc-timeout", t);
        }
        let req = req.body(axum::body::Body::from(framed)).unwrap();
        let app = http::router(Arc::clone(&s.shared));
        let resp = s.block_on(app.oneshot(req)).unwrap();
        // An error before the handler is a trailers-only reply: the status is in the headers.
        resp.headers()
            .get("grpc-status")
            .map(|v| v.to_str().unwrap().to_owned())
    };
    let all = s.block_on(service.reserve(l.inflight_bytes)).unwrap();
    // The client's deadline is shorter than the server's: DeadlineExceeded (4).
    assert_eq!(call(Some("100m")).as_deref(), Some("4"));
    // No client deadline: the server's request timeout, then Overloaded (UNAVAILABLE, 14).
    assert_eq!(call(None).as_deref(), Some("14"));
    drop(all);
    let mut c = s.grpc();
    let get = proto::GetRequest {
        table: "t".into(),
        row: b"r".to_vec().into(),
        family: "f".into(),
        qualifier: b"q".to_vec().into(),
        ..Default::default()
    };
    s.block_on(c.get(get)).unwrap();
    // Every reservation came back.
    assert_eq!(service.inflight_available(), l.inflight_bytes);
}

#[test]
fn metrics_include_each_rpcs_durations() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    grpc_scan(&s, &mut c, scan_request(0, 0, Vec::new()));
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    // A scan is counted when its task ends, just after its last page.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let text = loop {
        let text = agent
            .get(format!("{}/metrics", s.url))
            .call()
            .unwrap()
            .body_mut()
            .read_to_string()
            .unwrap();
        if text.contains("rpc=\"Data.Scan\",outcome=\"ok\"") || std::time::Instant::now() > deadline
        {
            break text;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(
        text.contains("pigeonhole_serve_requests_total{rpc=\"Data.Mutate\",outcome=\"ok\"} 31"),
        "{text}"
    );
    assert!(
        text.contains(
            "pigeonhole_serve_request_duration_seconds_bucket{rpc=\"Data.Mutate\",le=\"+Inf\"} 31"
        ),
        "{text}"
    );
    assert!(
        text.contains("pigeonhole_serve_request_duration_seconds_count{rpc=\"Data.Scan\"} 1"),
        "{text}"
    );
    assert!(
        !text.contains("duration_seconds_count{rpc=\"Data.Get\"}"),
        "an RPC never called has no histogram: {text}"
    );
}

/// A table open that a catalog change (`tables_changed`, as a replica applies one) overtook
/// keeps nothing, since it may have resolved the name before a drop and create; the next
/// call opens it again (#1632).
#[test]
fn a_table_open_overtaken_by_a_catalog_change_keeps_nothing() {
    let s = Server::start("", limits());
    let svc = &s.shared.router.service;
    // The steps of `Service::table`, with a catalog change between the open and the keep.
    let started = svc.table_generation();
    let t = svc.db.table("t").unwrap().open().unwrap();
    svc.tables_changed();
    assert!(
        svc.keep_table("t", &t, started).is_none(),
        "a stale open is not kept"
    );
    assert!(!svc.table_cached("t"));
    // A call opens it afresh, and keeps it.
    let value = json!({"bytes": b64(b"v")});
    assert_eq!(s.post("Mutate", &put("r", "f", "q", value), None).0, 200);
    assert!(svc.table_cached("t"));
}
