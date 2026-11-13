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
        // A read's reply comes with its charge (#1567).
        matches!(r.uncharge().0, super::service::ServiceReply::Get(g) if g.cell.as_ref().is_some_and(|c| &c.value[..] == b"landed"))
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
fn grpc_checks_the_token_first_and_bounds_the_message_read() {
    // audit120's S-S1: `prepay` reserved the in-flight budget before the token was
    // checked, and waited for a message with no deadline.
    use tower::ServiceExt as _;
    /// A body that sends its bytes, then nothing more, never ending.
    struct Stall(Option<axum::body::Bytes>);
    impl futures_core::Stream for Stall {
        type Item = Result<axum::body::Bytes, std::io::Error>;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            match self.0.take() {
                Some(b) => std::task::Poll::Ready(Some(Ok(b))),
                None => std::task::Poll::Pending,
            }
        }
    }
    let mut l = limits();
    l.request_timeout = Duration::from_millis(300);
    let s = Server::start("read r3ad\n", l);
    let service = &s.shared.router.service;
    let framed = {
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
        framed
    };
    // The grpc-status of a reply that ended before the handler (trailers-only), and how long
    // it took.
    let call = |path: &str, token: Option<&str>, body: axum::body::Body| {
        let mut req = axum::http::Request::post(path)
            .header("content-type", "application/grpc")
            .header("te", "trailers");
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let req = req.body(body).unwrap();
        let app = http::router(Arc::clone(&s.shared));
        let started = std::time::Instant::now();
        let resp = s.block_on(app.oneshot(req)).unwrap();
        let status = resp
            .headers()
            .get("grpc-status")
            .map(|v| v.to_str().unwrap().to_owned());
        (status, started.elapsed())
    };
    let get = "/pigeonhole.v1.Data/Get";
    // With the budget full, a call without a valid token is Unauthenticated (16) at once,
    // not left waiting for bytes it may not use.
    let all = s.block_on(service.reserve(l.inflight_bytes)).unwrap();
    for token in [None, Some("wrong")] {
        let (status, took) = call(get, token, axum::body::Body::from(framed.clone()));
        assert_eq!(status.as_deref(), Some("16"), "{token:?}");
        assert!(took < Duration::from_millis(200), "{token:?}: {took:?}");
    }
    drop(all);
    // Health still takes no token.
    // An empty message: its 5-byte header, length 0.
    let (status, _) = call(
        "/pigeonhole.v1.Admin/Health",
        None,
        axum::body::Body::from(vec![0u8; 5]),
    );
    assert_eq!(
        status, None,
        "a reply from the handler: its status is a trailer"
    );
    // A call whose header never comes, and one whose message stops after its header (a
    // 1 MiB message, reserved): DeadlineExceeded (4) at the request timeout, and the
    // reservation comes back.
    let header = axum::body::Bytes::from(vec![0, 0, 0x10, 0, 0]);
    for sent in [None, Some(header)] {
        let (status, took) = call(
            get,
            Some("r3ad"),
            axum::body::Body::from_stream(Stall(sent)),
        );
        assert_eq!(status.as_deref(), Some("4"));
        assert!(took >= Duration::from_millis(300), "{took:?}");
        assert_eq!(service.inflight_available(), l.inflight_bytes);
    }
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
    let t = svc.part().unwrap().db.table("t").unwrap().open().unwrap();
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

// ---- Aggregate queries (audit120's F4) ----

/// Table `agg`: rows `r000`.. each with a quantile sketch (`q`) and a bloom filter (`b`),
/// and row `wide` with `wide_columns` quantile columns.
fn aggregate_rows(
    s: &Server,
    c: &mut DataClient<tonic::transport::Channel>,
    rows: usize,
    wide_columns: usize,
) {
    let spec = json!({"name": "agg", "if_missing": false, "families": [
        {"name": "q", "aggregate": {"kind": "AGGREGATION_KIND_QUANTILES"}},
        {"name": "b", "aggregate": {"kind": "AGGREGATION_KIND_BLOOM", "bloom_bits": 1024, "bloom_hashes": 3}},
    ]});
    let (status, body) = s.rpc("Schema/CreateTable", &spec, None);
    assert_eq!(status, 200, "{body}");
    let input = |family: &str, qualifier: &[u8], kind: proto::value::Kind| proto::Write {
        op: Some(proto::write::Op::Aggregate(proto::AggregateInput {
            family: family.into(),
            qualifier: qualifier.to_vec().into(),
            timestamp: None,
            input: Some(proto::Value { kind: Some(kind) }),
        })),
    };
    let mut mutate = |row: &[u8], writes: Vec<proto::Write>| {
        s.block_on(c.mutate(proto::MutateRequest {
            writes: Some(proto::RowWrites {
                row: Some(proto::RowKey {
                    table: "agg".into(),
                    row: row.to_vec().into(),
                }),
                writes,
            }),
            durability: 0,
        }))
        .unwrap();
    };
    for i in 0..rows {
        let row = format!("r{i:03}").into_bytes();
        mutate(
            &row,
            vec![
                input("q", b"x", proto::value::Kind::F64(i as f64)),
                input("b", b"x", proto::value::Kind::I64(i as i64)),
            ],
        );
    }
    let wide = (0..wide_columns)
        .map(|i| {
            input(
                "q",
                format!("c{i:03}").as_bytes(),
                proto::value::Kind::F64(1.0),
            )
        })
        .collect();
    mutate(b"wide", wide);
}

fn aggregate_query(quantiles: usize, bloom_i64: usize, bloom_bytes: usize) -> proto::Selection {
    proto::Selection {
        aggregate: Some(proto::AggregateQuery {
            quantiles: (0..quantiles)
                .map(|i| i as f64 / quantiles as f64)
                .collect(),
            bloom_i64: (0..bloom_i64 as i64).collect(),
            bloom_bytes: (0..bloom_bytes)
                .map(|i| format!("{i}").into_bytes().into())
                .collect(),
            set_members: false,
        }),
        ..Default::default()
    }
}

#[test]
fn an_aggregate_query_over_its_probe_caps_is_refused_on_every_read() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    aggregate_rows(&s, &mut c, 2, 2);
    let over = [
        aggregate_query(1025, 0, 0),
        aggregate_query(0, 1000, 25),
        aggregate_query(0, 0, 1025),
    ];
    for sel in over {
        let refused = |r: Result<tonic::Code, tonic::Status>| {
            assert_eq!(
                r.map_err(|e| e.code()),
                Err(tonic::Code::InvalidArgument),
                "{sel:?}"
            )
        };
        let q = sel.aggregate.clone();
        refused(
            s.block_on(c.get(proto::GetRequest {
                table: "agg".into(),
                row: b"r000".to_vec().into(),
                family: "q".into(),
                qualifier: b"x".to_vec().into(),
                aggregate: q,
                ..Default::default()
            }))
            .map(|_| tonic::Code::Ok),
        );
        refused(
            s.block_on(c.multi_get(proto::MultiGetRequest {
                table: "agg".into(),
                keys: vec![b"r000".to_vec().into()],
                selection: Some(sel.clone()),
                ..Default::default()
            }))
            .map(|_| tonic::Code::Ok),
        );
        refused(
            s.block_on(c.read_row(proto::ReadRowRequest {
                table: "agg".into(),
                row: b"r000".to_vec().into(),
                selection: Some(sel.clone()),
                ..Default::default()
            }))
            .map(|_| tonic::Code::Ok),
        );
        refused(
            s.block_on(c.scan(proto::ScanRequest {
                table: "agg".into(),
                selection: Some(sel.clone()),
                ..Default::default()
            }))
            .map(|_| tonic::Code::Ok),
        );
    }
    // At the caps, the answers come back: one per probe.
    let at = aggregate_query(1024, 1000, 24);
    let row = s
        .block_on(c.read_row(proto::ReadRowRequest {
            table: "agg".into(),
            row: b"r000".to_vec().into(),
            selection: Some(at),
            ..Default::default()
        }))
        .unwrap()
        .into_inner()
        .row
        .unwrap();
    let view = |f: &str| {
        row.cells
            .iter()
            .find(|c| c.family == f)
            .and_then(|c| c.aggregate.clone())
            .unwrap()
    };
    assert_eq!(view("q").quantiles.unwrap().values.len(), 1024);
    assert_eq!(view("b").bloom.unwrap().contains.len(), 1024);
}

#[test]
fn aggregate_answers_count_against_a_pages_budget() {
    // 200 rows of a quantile sketch and a bloom filter are a few hundred bytes each, so
    // without the answers one 64 KiB page would hold them all, and with 1,024 quantiles and
    // 1,024 bloom probes on each the reply would be about 1.8 MB.
    let s = Server::start("", limits());
    let mut c = s.grpc();
    aggregate_rows(&s, &mut c, 200, 40);
    let budget = 64 << 10;
    let sel = aggregate_query(1024, 512, 512);
    // A page may go over by one column (D308): a quantile column's answers are 8 KiB.
    let bounded = |bytes: u64, encoded: usize| {
        assert!(
            bytes <= budget,
            "a page's bytes stay within its budget: {bytes}"
        );
        assert!(
            encoded as u64 <= budget + (8 << 10),
            "a page's reply stays near its budget: {encoded}"
        );
    };
    let names = |rows: &[proto::Row]| rows.iter().map(|r| r.key.to_vec()).collect::<Vec<_>>();
    let want: Vec<Vec<u8>> = (0..200).map(|i| format!("r{i:03}").into_bytes()).collect();

    let (mut after, mut seen, mut pages) = (Vec::new(), Vec::new(), 0);
    loop {
        let page = s
            .block_on(c.multi_get(proto::MultiGetRequest {
                table: "agg".into(),
                keys: want.iter().map(|k| k.clone().into()).collect(),
                selection: Some(sel.clone()),
                page_bytes: budget,
                after: after.clone().into(),
                ..Default::default()
            }))
            .unwrap()
            .into_inner();
        pages += 1;
        bounded(page.bytes, prost::Message::encoded_len(&page));
        for row in &page.rows {
            if seen.last() != Some(&row.key.to_vec()) {
                seen.push(row.key.to_vec());
            }
        }
        if page.next.is_empty() {
            break;
        }
        after = page.next.to_vec();
    }
    assert!(pages >= 20, "about 9 KiB of answers a row: {pages} pages");
    assert_eq!(seen, want, "every row once, in order");

    let scan = grpc_scan(
        &s,
        &mut c,
        proto::ScanRequest {
            table: "agg".into(),
            range: Some(proto::KeyRange {
                range: Some(proto::key_range::Range::Prefix(b"r".to_vec().into())),
            }),
            selection: Some(sel.clone()),
            page_bytes: budget,
            ..Default::default()
        },
    );
    assert!(scan.len() >= 20, "{} pages", scan.len());
    for page in &scan {
        bounded(page.bytes, prost::Message::encoded_len(page));
    }
    let mut keys = Vec::new();
    for page in &scan {
        for k in names(&page.rows) {
            if keys.last() != Some(&k) {
                keys.push(k);
            }
        }
    }
    assert_eq!(keys, want, "every row once, in order");

    // A wide row's columns: its pages end early too.
    let (mut after, mut columns) = (Vec::new(), 0);
    loop {
        let page = s
            .block_on(c.read_row(proto::ReadRowRequest {
                table: "agg".into(),
                row: b"wide".to_vec().into(),
                selection: Some(sel.clone()),
                page_bytes: budget,
                after: after.clone().into(),
                ..Default::default()
            }))
            .unwrap()
            .into_inner();
        bounded(page.bytes, prost::Message::encoded_len(&page));
        columns += page.row.map_or(0, |r| r.cells.len());
        if page.next.is_empty() {
            break;
        }
        after = page.next.to_vec();
    }
    assert_eq!(columns, 40, "every column once");
}

#[test]
fn a_sets_members_come_back_only_when_asked_and_only_aggregate_families_have_views() {
    // audit120's S-S8: every read expanded a set's members, about 32 times its stored size,
    // uncounted, and read any plain value that parsed as a set, sketch or filter as one.
    let s = Server::start("", limits());
    let db = s.shared.router.service.part().unwrap().db.clone();
    let t = db
        .table("sets")
        .unwrap()
        .family("s", Family::aggregate(pigeonhole::Aggregation::Set))
        .family("p", Family::default())
        .create()
        .unwrap();
    // A dense set: 65,536 members in an 8 KiB container.
    let mut m = t.mutate(b"r");
    for x in 0..65_536u32 {
        m = m.set_add("s", b"x", x);
    }
    m.commit().unwrap();
    // A plain value that parses as a quantile sketch (D298's encoding, 5 bytes).
    t.mutate(b"r")
        .put(
            "p",
            b"x",
            &[pigeonhole_format::buckets::QUANTILES_ENCODING, 7, 1, 0, 0],
        )
        .commit()
        .unwrap();
    let c = s.grpc();
    let read = |set_members: bool, page_bytes: u64| {
        let selection = proto::Selection {
            aggregate: Some(proto::AggregateQuery {
                quantiles: vec![0.5],
                set_members,
                ..Default::default()
            }),
            ..Default::default()
        };
        s.block_on(c.clone().read_row(proto::ReadRowRequest {
            table: "sets".into(),
            row: b"r".to_vec().into(),
            selection: Some(selection),
            page_bytes,
            ..Default::default()
        }))
        .unwrap()
        .into_inner()
    };
    let cells = |page: &proto::ReadRowResponse| page.row.clone().unwrap().cells;
    // Not asked: no members, and the page is the stored size.
    let page = read(false, 64 << 10);
    assert!(page.bytes < 16 << 10, "{}", page.bytes);
    for cell in cells(&page) {
        assert!(
            cell.aggregate.is_none(),
            "{}: no view (a set's members weren't asked for; a plain value has none)",
            cell.family
        );
    }
    // Asked: every member, and the page counts them.
    let page = read(true, 1 << 20);
    let set = cells(&page).into_iter().find(|c| c.family == "s").unwrap();
    assert_eq!(set.aggregate.unwrap().set_members.len(), 65_536);
    // About 184 KiB of packed members, against the 8 KiB stored set.
    assert!(page.bytes > 128 << 10, "the members count: {}", page.bytes);
    let plain = cells(&page).into_iter().find(|c| c.family == "p").unwrap();
    assert!(
        plain.aggregate.is_none(),
        "a plain family's value isn't a sketch"
    );
}
