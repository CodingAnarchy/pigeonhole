use super::*;

/// Opens a raw connection and sends `head` (an HTTP/1.1 request's lines and headers).
fn raw(url: &str, head: &str) -> std::net::TcpStream {
    use std::io::Write as _;
    let addr = url.trim_start_matches("http://");
    let mut tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.write_all(head.as_bytes()).unwrap();
    tcp
}

/// Takes `s`'s one request slot (`max_concurrent` 1) with a body that never finishes:
/// larger than the pre-read reads first (#1601), so the request reaches the limit.
fn hold_a_slot(s: &Server) -> std::net::TcpStream {
    raw(
        &s.url,
        "POST /v1/Data/Get HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 100000\r\n\r\n{",
    )
}

/// One HTTP/1.1 answer off `tcp`: its status line, headers and body (by `Content-Length`).
fn read_answer(tcp: &mut std::net::TcpStream) -> std::io::Result<(String, String)> {
    use std::io::Read as _;
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        // `read_exact` retries a read a signal interrupted, as a bare `read` doesn't (EINTR
        // on a Linux runner), and it reports the connection's end as `UnexpectedEof`.
        tcp.read_exact(&mut byte)?;
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    let length = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    tcp.read_exact(&mut body)?;
    Ok((head, String::from_utf8_lossy(&body).into_owned()))
}

#[test]
fn an_answer_before_the_body_arrives_keeps_the_connection() {
    // #1601: health and a shed request are answered without their bodies. hyper closed the
    // connection when a body wasn't read yet, the late body drew a reset, and on Windows the
    // client lost the answer (WSAECONNABORTED). Each is sent with its body 50 ms after its
    // head; the answer must be whole and the connection must take another request.
    use std::io::Write as _;
    let mut l = limits();
    l.max_concurrent = 1;
    let s = Server::start("", l);
    let mut held = None;
    // Health first, with the slot free; then a `Get` shed while the slot is held.
    for (path, status) in [("Admin/Health", "200"), ("Data/Get", "503")] {
        if path == "Data/Get" {
            held = Some(hold_a_slot(&s));
            std::thread::sleep(Duration::from_millis(200));
        }
        let mut tcp = raw(
            &s.url,
            &format!(
                "POST /v1/{path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n"
            ),
        );
        tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        tcp.write_all(b"{}").unwrap();
        let (head, body) = read_answer(&mut tcp).unwrap();
        assert!(
            head.starts_with(&format!("HTTP/1.1 {status}")),
            "{path}: {head}{body}"
        );
        // The late body went somewhere: the same connection answers again.
        tcp.write_all(b"GET /v1/health HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap_or_else(|e| panic!("{path}: the connection was closed: {e}"));
        let (head, body) = read_answer(&mut tcp)
            .unwrap_or_else(|e| panic!("{path}: the connection was closed: {e}"));
        assert!(
            head.starts_with("HTTP/1.1 200"),
            "{path}, then health: {head}{body}"
        );
    }
    drop(held);
}

#[test]
fn a_body_that_never_arrives_is_answered_at_the_request_timeout_and_closed() {
    // perf287's (1) on #1601: the pre-read waits at most --request-timeout-ms, then answers
    // DeadlineExceeded and closes the connection rather than keep it with a body half read.
    use std::io::Read as _;
    let mut l = limits();
    l.request_timeout = Duration::from_millis(300);
    let s = Server::start("", l);
    let mut tcp = raw(
        &s.url,
        "POST /v1/Admin/Health HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{",
    );
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let started = std::time::Instant::now();
    let (head, body) = read_answer(&mut tcp).unwrap();
    assert!(head.starts_with("HTTP/1.1 504"), "{head}{body}");
    assert!(
        head.to_ascii_lowercase().contains("connection: close"),
        "{head}"
    );
    assert!(body.contains("DeadlineExceeded"), "{body}");
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "answered before its timeout"
    );
    let mut rest = Vec::new();
    assert_eq!(
        tcp.read_to_end(&mut rest).unwrap_or(0),
        0,
        "the connection ends"
    );
}

#[test]
fn a_slow_body_counts_toward_the_request_deadline() {
    // perf287's nit on #1613: the pre-read's time is part of the request's one deadline, so
    // a body that arrives late leaves the rest of the request less time, not a fresh
    // --request-timeout-ms. A replica read waiting for a seqno it will never apply waits out
    // that deadline: from the head's arrival, not from when the body finished.
    use std::io::Write as _;
    let p = Server::start("", limits());
    let mut short = limits();
    short.request_timeout = Duration::from_millis(1500);
    let r = Server::start_replica_with(test_dir(), &p.url, None, short);
    wait_replica(&r, "caught up", caught_up(&p, None));
    let body =
        json!({"table": "t", "row": b64(b"x"), "family": "f", "qualifier": b64(b"q")}).to_string();
    let mut tcp = raw(
        &r.url,
        &format!(
            "POST /v1/Data/Get HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n{}: 999999999\r\nContent-Length: {}\r\n\r\n",
            super::replica::MIN_PRIMARY_SEQNO,
            body.len()
        ),
    );
    tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let started = std::time::Instant::now();
    std::thread::sleep(Duration::from_millis(1000));
    tcp.write_all(body.as_bytes()).unwrap();
    let (head, answer) = read_answer(&mut tcp).unwrap();
    let took = started.elapsed();
    assert!(head.starts_with("HTTP/1.1 503"), "{head}{answer}");
    assert!(answer.contains("ReplicaBehind"), "{answer}");
    // About 1.5 s from the head; counted from the body's end it would be about 2.5 s.
    assert!(
        took >= Duration::from_millis(1400) && took < Duration::from_millis(2200),
        "answered after {took:?}: the deadline didn't start at the head"
    );
}

#[test]
fn health_and_metrics_answer_while_requests_are_shed() {
    // perf287's F4 on #1501: one slot, held by a request whose body never finishes. Over
    // the pre-read's 64 KiB (#1601), so it takes the slot before its body is read.
    let mut l = limits();
    l.max_concurrent = 1;
    let s = Server::start("", l);
    let _held = hold_a_slot(&s);
    std::thread::sleep(Duration::from_millis(200));
    let (status, body) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"r"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!(
        (status, body["code"].as_str()),
        (503, Some("Overloaded")),
        "{body}"
    );
    assert_eq!(body["retry_safe"], true);
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
    let mut m = agent.get(format!("{}/metrics", s.url)).call().unwrap();
    assert_eq!(m.status().as_u16(), 200);
    assert!(
        m.body_mut()
            .read_to_string()
            .unwrap()
            .contains("pigeonhole_serve_shed_total 1")
    );
}

#[test]
fn a_body_without_a_length_is_refused_before_it_is_read() {
    // perf287's F2 on #1501: the budget is reserved from Content-Length, so a chunked body
    // (no length) is refused up front.
    use std::io::Read as _;
    let s = Server::start("", limits());
    let mut tcp = raw(
        &s.url,
        "POST /v1/Data/Get HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
    );
    let mut reply = String::new();
    let _ = tcp.read_to_string(&mut reply);
    assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");
    assert!(reply.contains("Content-Length"), "{reply}");
}
