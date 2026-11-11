use super::*;

// ---- Connections (`--max-connections`, #1604) ----

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;

/// A server with at most `max` connections, whose requests (and silent connections) time out
/// after `timeout`. It listens apart from the other tests' servers, so the connections it
/// counts are this test's (#1668).
fn capped(max: usize, timeout: Duration) -> Server {
    let mut l = limits();
    l.max_connections = max;
    l.request_timeout = timeout;
    Server::start_apart(l)
}

fn connect(s: &Server) -> TcpStream {
    TcpStream::connect(s.url.trim_start_matches("http://")).unwrap()
}

/// The server's connection counts, and how its connections ended, for a failure's message.
fn counts(s: &Server) -> String {
    let conns = &s.shared.router.service.connections;
    format!(
        "open {}, refused {}, ended {:?}",
        conns.open(),
        conns.refused(),
        conns.ended()
    )
}

/// `GET /v1/health` on `c`, kept alive: the connection stays open, holding its slot.
fn health(c: &mut TcpStream) {
    c.write_all(b"GET /v1/health HTTP/1.1\r\nHost: test\r\n\r\n")
        .unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut buf = [0u8; 4096];
    let n = loop {
        match c.read(&mut buf) {
            // See `how_closed`: an interrupted read is read again (#1668).
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            r => break r.unwrap(),
        }
    };
    assert!(
        buf[..n].starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&buf[..n])
    );
}

/// Whether the server closes `c` within `within`: an EOF or a reset. Bytes it sends (an
/// HTTP/2 server's settings) don't count.
fn closed_within(c: &mut TcpStream, within: Duration) -> bool {
    how_closed(c, within).is_some()
}

/// How the server closed `c` within `within` (an EOF, or the read's error), or `None` if
/// it didn't. An interrupted read is read again, since it isn't a close (#1668): on Linux a
/// blocking socket read with a timeout fails with `EINTR` when the thread gets task work,
/// with no signal handler involved. The test thread opens the database, so it submits to
/// the engine's shared io_uring ring (no `DEFER_TASKRUN`), and the kernel queues task work
/// to a submitting thread. On Linux the serve tests' reads saw `EINTR` in 5 of 20 runs with
/// io_uring and 0 of 20 with `PIGEONHOLE_IO=pread`. A bare `read` doesn't retry it;
/// `read_exact` does.
fn how_closed(c: &mut TcpStream, within: Duration) -> Option<String> {
    let deadline = std::time::Instant::now() + within;
    let mut buf = [0u8; 1024];
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return None;
        }
        c.set_read_timeout(Some(left)).unwrap();
        match c.read(&mut buf) {
            Ok(0) => return Some("EOF".to_owned()),
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                return None;
            }
            Err(e) => return Some(format!("{e} ({:?})", e.kind())),
        }
    }
}

fn wait_open(s: &Server, n: usize) {
    let conns = &s.shared.router.service.connections;
    wait_for(&format!("{n} open connections"), || conns.open() == n);
}

/// At the cap, a new connection is closed as soon as it's accepted and counted as refused; a
/// slot freed by a close serves the next one. Both protocols share the count.
#[test]
fn a_connection_past_the_cap_is_closed_and_counted() {
    let s = capped(2, Duration::from_secs(5));
    let conns = &s.shared.router.service.connections;
    let mut a = connect(&s);
    health(&mut a);
    let mut b = connect(&s);
    health(&mut b);
    wait_open(&s, 2);

    let mut c = connect(&s);
    assert!(
        closed_within(&mut c, Duration::from_secs(5)),
        "a connection past the cap was served"
    );
    assert_eq!(conns.refused(), 1, "{}", counts(&s));
    assert_eq!(conns.open(), 2, "a refused connection holds no slot");
    let text = s.shared.router.metrics_text();
    for line in [
        "pigeonhole_serve_connections 2",
        "pigeonhole_serve_connections_max 2",
        "pigeonhole_serve_connections_refused_total 1",
    ] {
        assert!(text.contains(line), "{line}: {text}");
    }

    drop(a);
    wait_open(&s, 1);
    let mut d = connect(&s);
    health(&mut d);
    // gRPC (HTTP/2) is counted the same way: its channel takes the slot `b` freed.
    drop(b);
    wait_open(&s, 1);
    let mut grpc = s.grpc();
    let got = s.block_on(grpc.get(proto::GetRequest {
        table: "t".into(),
        row: b"r".to_vec().into(),
        family: "f".into(),
        qualifier: b"q".to_vec().into(),
        ..Default::default()
    }));
    assert!(got.is_ok(), "{got:?}");
    assert_eq!(conns.open(), 2, "the gRPC channel holds the other slot");
}

/// cli692's D1 on the design: a connection that sends nothing is dropped after
/// `--request-timeout-ms`, freeing its slot. The deadline ends at the first byte: an HTTP/2
/// connection that sent its preface and then idles is kept.
#[test]
fn a_silent_connection_is_dropped_and_an_idle_http2_one_is_kept() {
    let timeout = Duration::from_millis(400);
    let s = capped(1, timeout);
    let conns = &s.shared.router.service.connections;

    // Connections this server ended before the silent one (ai1249's N2 on #1695).
    let before = conns.ended().len();
    let mut silent = connect(&s);
    let started = std::time::Instant::now();
    let how = how_closed(&mut silent, Duration::from_secs(5));
    let took = started.elapsed();
    let Some(how) = how else {
        panic!("a silent connection was kept: {}", counts(&s));
    };
    // Closed early: the message says how the client saw it close, and how the server ended
    // the connection, if it did (#1668: "6 ms, open 1, refused 0"). Its task records that
    // just after the socket closes, so it gets a moment to.
    if took < timeout / 2 {
        let t = std::time::Instant::now();
        while conns.ended().len() == before && t.elapsed() < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "closed after {took:?}: the client saw {how}; {}",
            counts(&s)
        );
    }
    assert_eq!(conns.refused(), 0, "{}", counts(&s));
    wait_open(&s, 0);
    let mut next = connect(&s);
    health(&mut next);
    drop(next);
    wait_open(&s, 0);

    // The HTTP/2 preface and an empty SETTINGS frame, then nothing.
    let mut h2 = connect(&s);
    h2.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").unwrap();
    h2.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).unwrap();
    assert!(
        !closed_within(&mut h2, timeout * 3),
        "an HTTP/2 connection that sent its preface was dropped as silent"
    );
    assert_eq!(conns.open(), 1, "{}", counts(&s));
    // It holds the one slot: another connection is refused.
    let mut over = connect(&s);
    assert!(closed_within(&mut over, Duration::from_secs(5)));
    assert_eq!(conns.refused(), 1, "{}", counts(&s));
}

/// cli692's N1 on #1653: an HTTP/1.1 keep-alive connection idle between requests has sent
/// bytes, so the first-byte deadline is past. hyper's header-read timeout
/// (`--request-timeout-ms`) bounds the wait for its next request: kept while requests come
/// within it, closed after it, and its slot freed. (An idle HTTP/2 connection is kept: it
/// answers pings.)
#[test]
fn an_idle_keep_alive_connection_is_closed_after_the_request_timeout() {
    let timeout = Duration::from_millis(400);
    let s = capped(1, timeout);
    let mut c = connect(&s);
    health(&mut c);
    // A next request within the timeout is served on the same connection.
    std::thread::sleep(timeout / 4);
    health(&mut c);
    let idle = std::time::Instant::now();
    assert!(
        closed_within(&mut c, timeout * 4),
        "an idle keep-alive connection was kept past the request timeout"
    );
    assert!(
        idle.elapsed() >= timeout / 2,
        "{:?}, {}",
        idle.elapsed(),
        counts(&s)
    );
    wait_open(&s, 0);
    let mut next = connect(&s);
    health(&mut next);
}
