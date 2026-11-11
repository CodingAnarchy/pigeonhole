use super::*;

// ---- Feed.Subscribe (D313 ticket 5, R2) ----

#[test]
fn a_feed_streams_commits_under_its_prefix_and_resumes_from_a_token() {
    let s = Server::start("", limits());
    let mut events = direct_feed(&s, subscribe(&["t"], b"a"));
    let a1 = write_row(&s, b"a1");
    write_row(&s, b"b1"); // passed over: no row under the prefix
    let a2 = write_row(&s, b"a2");
    let got = drain(&s, &mut events, Duration::from_millis(300));
    assert_eq!(
        got.iter()
            .map(|(n, r, _)| (*n, r.clone()))
            .collect::<Vec<_>>(),
        [(a1, vec![b"a1".to_vec()]), (a2, vec![b"a2".to_vec()])]
    );
    // From the first event's token: the stream starts right after it, on its own feed, and
    // rejoins the shared one once it reaches it.
    let mut resumed = direct_feed(
        &s,
        proto::SubscribeRequest {
            start: Some(proto::subscribe_request::Start::Resume(
                got[0].2.clone().into(),
            )),
            ..subscribe(&["t"], b"a")
        },
    );
    let a3 = write_row(&s, b"a3");
    let again = drain(&s, &mut resumed, Duration::from_millis(300));
    assert_eq!(
        again.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(),
        [a2, a3],
        "after a1, once each"
    );
    assert!(
        s.shared
            .router
            .service
            .part()
            .unwrap()
            .feeds
            .stats
            .rejoined
            .load(Ordering::Relaxed)
            >= 1
    );
    let text = s.shared.metrics.render();
    let mut text = text;
    super::service::render_feeds(&mut text, &s.shared.router.service.part().unwrap().feeds);
    assert!(text.contains("pigeonhole_serve_feed_hubs 1"), "{text}");
}

#[test]
fn a_quiet_feed_moves_its_token_with_a_progress_event() {
    let mut l = limits();
    l.feed_quiet = Duration::from_millis(150);
    let s = Server::start("", l);
    let mut events = direct_feed(&s, subscribe(&["t"], b"a"));
    write_row(&s, b"b1");
    // Only commits outside the prefix: after a quiet interval, a progress event carries the
    // token past them (perf287's S2), the token of the last commit it covers.
    let e = next_event(&s, &mut events, Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(
        matches!(e.event, Some(proto::feed_event::Event::Progress(_))),
        "{e:?}"
    );
    // Resuming from it starts after b1.
    let mut resumed = direct_feed(
        &s,
        proto::SubscribeRequest {
            start: Some(proto::subscribe_request::Start::Resume(e.token.clone())),
            ..subscribe(&["t"], b"")
        },
    );
    let b2 = write_row(&s, b"b2");
    let got = drain(&s, &mut resumed, Duration::from_millis(300));
    assert_eq!(got.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(), [b2]);
}

#[test]
fn a_laggard_dropped_between_two_passed_over_commits_misses_nothing() {
    // feed1248's (2) on ticket 5: a client that falls off the ring, where the commits around
    // its position are ones its prefix passes over, goes on from its own token and rejoins
    // with nothing missed or repeated.
    let mut l = limits();
    l.feed_ring_bytes = 2048; // a few entries
    // A one-event buffer: a client that takes nothing stops reading the ring at once.
    l.feed_client_events = 1;
    let s = Server::start("", l);
    let mut slow = direct_feed(&s, subscribe(&["t"], b"a"));
    let mut want = vec![write_row(&s, b"a0")];
    let first = next_event(&s, &mut slow, Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(commit_of(&first).unwrap().0, want[0]);
    // While it takes nothing: two commits it passes over, then enough to push them off.
    write_row(&s, b"b1");
    write_row(&s, b"b2");
    for i in 0..40u32 {
        let row = if i % 3 == 0 {
            format!("a{i}")
        } else {
            format!("b{i}")
        };
        let n = write_row(&s, row.as_bytes());
        if i % 3 == 0 {
            want.push(n);
        }
    }
    let rest = drain(&s, &mut slow, Duration::from_millis(500));
    assert_eq!(
        rest.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(),
        want[1..],
        "every commit under the prefix, once, in order"
    );
    let stats = &s.shared.router.service.part().unwrap().feeds.stats;
    assert!(
        stats.fell_behind.load(Ordering::Relaxed) >= 1,
        "it fell off the ring"
    );
}

/// A client that takes nothing: its buffer fills, then the stream ends with
/// `DeadlineExceeded` after `--stream-idle-ms`. The events it buffered come first, in order.
fn stalled_feed(client_events: usize, client_bytes: usize, writes: usize) -> (Vec<u64>, Vec<u64>) {
    let mut l = limits();
    l.stream_idle = Duration::from_millis(300);
    l.feed_client_events = client_events;
    l.feed_client_bytes = client_bytes;
    let s = Server::start("", l);
    let mut stalled = direct_feed(&s, subscribe(&["t"], b""));
    let written: Vec<u64> = (0..writes)
        .map(|i| write_row(&s, format!("r{i:04}").as_bytes()))
        .collect();
    std::thread::sleep(Duration::from_millis(900));
    let mut got = Vec::new();
    loop {
        match next_event(&s, &mut stalled, Duration::from_secs(5)) {
            Some(Ok(e)) => got.extend(commit_of(&e).map(|(n, _)| n)),
            Some(Err(e)) => {
                assert_eq!(e.code, "DeadlineExceeded", "{e:?}");
                assert!(e.message.contains("--stream-idle-ms"), "{e:?}");
                break;
            }
            None => panic!("the stream ends with an error, after {} events", got.len()),
        }
    }
    assert!(next_event(&s, &mut stalled, Duration::from_secs(1)).is_none());
    (got, written)
}

#[test]
fn a_feed_client_that_takes_nothing_fills_its_buffer_then_times_out() {
    let (got, written) = stalled_feed(super::feed::CLIENT_EVENTS, super::feed::CLIENT_BYTES, 100);
    assert_eq!(
        got,
        written[..super::feed::CLIENT_EVENTS],
        "the full buffer, in order"
    );
}

#[test]
fn a_feed_clients_buffer_is_bounded_by_bytes_too() {
    // About 100 bytes an event: a 1 KiB buffer holds far fewer than 64 of them.
    let (got, written) = stalled_feed(super::feed::CLIENT_EVENTS, 1024, 100);
    assert!(
        !got.is_empty() && got.len() < 16,
        "{} events buffered",
        got.len()
    );
    assert_eq!(got, written[..got.len()], "in order");
}

#[test]
fn grpc_feed_events_match_the_services_shared_encoding_or_not() {
    // Encode-once (#1397 ticket 5): a client with no prefix gets each ring entry as bytes
    // encoded once for all such clients; a prefixed one gets its own message. Over the wire
    // both are exactly what a direct subscriber sees, tokens included.
    let s = Server::start("", limits());
    let url = s.url.clone();
    let grpc = |prefix: &[u8]| {
        let req = subscribe(&["t"], prefix);
        let url = url.clone();
        s.block_on(async move {
            let channel = tonic::transport::Channel::from_shared(url)
                .unwrap()
                .connect()
                .await
                .unwrap();
            proto::feed_client::FeedClient::new(channel)
                .subscribe(req)
                .await
                .unwrap()
                .into_inner()
        })
    };
    let mut all = grpc(b"");
    let mut under_a = grpc(b"a");
    let mut direct = direct_feed(&s, subscribe(&["t"], b""));
    for row in [&b"a1"[..], b"b1", b"a2"] {
        write_row(&s, row);
    }
    let take = |stream: &mut tonic::Streaming<proto::FeedEvent>, n: usize| {
        s.block_on(async {
            let mut out = Vec::new();
            while out.len() < n {
                let e = tokio::time::timeout(Duration::from_secs(5), stream.message())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                if commit_of(&e).is_some() {
                    out.push(e);
                }
            }
            out
        })
    };
    let want: Vec<proto::FeedEvent> = (0..3)
        .map(|_| {
            next_event(&s, &mut direct, Duration::from_secs(5))
                .unwrap()
                .unwrap()
        })
        .collect();
    assert_eq!(take(&mut all, 3), want, "no prefix: the shared encoding");
    let a: Vec<proto::FeedEvent> = take(&mut under_a, 2);
    assert_eq!(
        a.iter().map(|e| commit_of(e).unwrap()).collect::<Vec<_>>(),
        [&want[0], &want[2]].map(|e| commit_of(e).unwrap()),
        "a prefix: its own messages"
    );
    assert_eq!(a[0].token, want[0].token);
    assert_eq!(a[1].token, want[2].token);
}

#[test]
fn feed_streams_match_a_direct_feed_across_fall_offs() {
    // feed1248's (6): each client's stream, ring and own-stream catch-up alike, delivers
    // exactly what a direct FeedStream on the same spec does. Seeded; prints the seed.
    let seed: u64 = std::env::var("PHDB_FEED_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    eprintln!("feed_streams_match_a_direct_feed_across_fall_offs seed={seed}");
    let mut rng = seed | 1;
    let mut rand = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    let mut l = limits();
    l.feed_ring_bytes = 2048;
    // One-event buffers, so a stalled round takes clients off the ring.
    l.feed_client_events = 1;
    l.max_catch_up = 2;
    // A client stalls for runs of rounds by design; on a slow runner a run outlasted the 5 s
    // idle limit and the stream ended DeadlineExceeded (Windows, #1592's CI). Idling out has
    // its own tests: here a stall only takes a client off the ring.
    l.stream_idle = Duration::from_secs(600);
    let s = Server::start_with("", l, 2);
    let prefixes: [&[u8]; 4] = [b"", b"a", b"b", b"ab"];
    let t = s
        .shared
        .router
        .service
        .part()
        .unwrap()
        .db
        .table("t")
        .unwrap()
        .open()
        .unwrap();
    let mut direct: Vec<pigeonhole::Feed> = prefixes
        .iter()
        .map(|p| t.feed().prefix(p).open().unwrap())
        .collect();
    let mut clients: Vec<super::feed::FeedEvents> = prefixes
        .iter()
        .map(|p| direct_feed(&s, subscribe(&["t"], p)))
        .collect();
    let mut got: Vec<Vec<u64>> = vec![Vec::new(); prefixes.len()];
    for round in 0..30 {
        for _ in 0..rand(8) {
            let mut m = t.mutate(&[b'a' + rand(3) as u8, b'a' + rand(3) as u8]);
            for _ in 0..1 + rand(2) {
                m = m.put("f", format!("q{}", rand(4)).as_bytes(), &[0u8; 64]);
            }
            m.commit().unwrap();
        }
        // Some clients read now; others stall this round (and may fall off the ring). In
        // rounds 10 to 14 every client stalls, so more fall off than there are catch-up slots.
        for (i, c) in clients.iter_mut().enumerate() {
            let stall = (10..15).contains(&round) || rand(2) == 0;
            if !stall || round == 29 {
                for (n, _, _) in drain(
                    &s,
                    c,
                    Duration::from_millis(if round == 29 { 600 } else { 20 }),
                ) {
                    got[i].push(n);
                }
            }
        }
    }
    for (i, d) in direct.iter_mut().enumerate() {
        let mut want = Vec::new();
        while let Some(c) = d.next(Duration::from_millis(100)).unwrap() {
            want.push(c.seqno());
        }
        assert_eq!(got[i], want, "prefix {:?}, seed {seed}", prefixes[i]);
    }
    let stats = &s.shared.router.service.part().unwrap().feeds.stats;
    let (fell, rejoined, waits) = (
        stats.fell_behind.load(Ordering::Relaxed),
        stats.rejoined.load(Ordering::Relaxed),
        stats.catch_up_waits.load(Ordering::Relaxed),
    );
    eprintln!("fell behind {fell}, rejoined {rejoined}, catch-up waits {waits}");
    assert!(
        fell > 0 && rejoined > 0,
        "the run took clients off the ring and back, seed {seed}"
    );
    drop(t);
}

#[test]
fn feed_streams_over_http_carry_their_token_as_the_sse_id_and_resume_from_it() {
    let s = Server::start("", limits());
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_millis(800)))
        .build()
        .into();
    // A stream doesn't end on its own: write first, read what arrives before the timeout.
    let read = |last_event_id: Option<&str>| -> String {
        let mut req = agent
            .post(format!("{}/v1/Feed/Subscribe", s.url))
            .header("Accept", "text/event-stream");
        if let Some(id) = last_event_id {
            req = req.header("Last-Event-ID", id);
        }
        let body = json!({"tables": ["t"], "from_oldest": true});
        let mut resp = req.send_json(&body).unwrap();
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
        let mut text = String::new();
        let _ = std::io::Read::read_to_string(&mut resp.body_mut().as_reader(), &mut text);
        text
    };
    let first = write_row(&s, b"r1");
    let second = write_row(&s, b"r2");
    let text = read(None);
    let events: Vec<&str> = text
        .split("\n\n")
        .filter(|e| e.starts_with("id: "))
        .collect();
    assert!(events.len() >= 2, "{text}");
    let id = events[0]
        .lines()
        .next()
        .unwrap()
        .strip_prefix("id: ")
        .unwrap();
    let data: Json = serde_json::from_str(
        events[0]
            .lines()
            .find_map(|l| l.strip_prefix("data: "))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(data["token"], id, "the id is the event's token");
    assert_eq!(data["commit"]["seqno"], first.to_string());
    // Last-Event-ID wins over the body's from_oldest: the stream starts after r1.
    let text = read(Some(id));
    let data: Json =
        serde_json::from_str(text.lines().find_map(|l| l.strip_prefix("data: ")).unwrap()).unwrap();
    assert_eq!(data["commit"]["seqno"], second.to_string(), "{text}");
}

#[test]
fn feed_streams_and_hubs_are_capped() {
    let mut l = limits();
    l.max_streams = 2;
    l.max_feed_hubs = 1;
    let s = Server::start("", l);
    s.shared
        .router
        .service
        .part()
        .unwrap()
        .db
        .table("u")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let _a = direct_feed(&s, subscribe(&["t"], b""));
    // Another table set needs a second hub.
    let e = s
        .block_on(s.shared.router.service.call(
            ServiceOp::Subscribe(subscribe(&["u"], b"")),
            &Caller::anonymous(),
            None,
        ))
        .unwrap_err();
    assert!(e.message.contains("--max-feed-hubs"), "{e:?}");
    // The same set shares the hub; a third stream is past --max-streams.
    let _b = direct_feed(&s, subscribe(&["t"], b"x"));
    let e = s
        .block_on(s.shared.router.service.call(
            ServiceOp::Subscribe(subscribe(&["t"], b"")),
            &Caller::anonymous(),
            None,
        ))
        .unwrap_err();
    assert!(e.message.contains("--max-streams"), "{e:?}");
    let e = s.block_on(s.shared.router.service.call(
        ServiceOp::Subscribe(subscribe(&[], b"")),
        &Caller::anonymous(),
        None,
    ));
    assert!(e.is_err());
}

#[test]
fn catch_up_streams_past_the_cap_wait_their_turn() {
    // perf287's (2) on ticket 5: at most --max-catch-up clients read their own feed at once;
    // the rest wait, and are counted, then deliver everything all the same.
    let mut l = limits();
    l.max_catch_up = 1;
    // A one-event buffer: the first, untaken, keeps its catch-up slot while the second
    // asks for it. With a deeper one it could buffer all five, rejoin and free the slot
    // first.
    l.feed_client_events = 1;
    // Not an idle-limit test: out of reach of a slow runner (#1531).
    l.stream_idle = Duration::from_secs(600);
    let s = Server::start("", l);
    let written: Vec<u64> = (0..5)
        .map(|i| write_row(&s, format!("r{i}").as_bytes()))
        .collect();
    let oldest = || proto::SubscribeRequest {
        start: Some(proto::subscribe_request::Start::FromOldest(true)),
        ..subscribe(&["t"], b"")
    };
    let mut first = direct_feed(&s, oldest());
    let mut second = direct_feed(&s, oldest());
    let feeds = &s.shared.router.service.part().unwrap().feeds;
    wait_for("the second waits for the one slot", || {
        feeds.stats.catch_up_waits.load(Ordering::Relaxed) == 1
    });
    // Either client may hold the slot: their tasks race for it. Whichever does gets it back
    // only once it has caught up and rejoined the ring (up to a REJOIN_TICK later), and
    // with a one-event buffer it catches up only as its events are taken. So take from both
    // streams in turn, never one to the end first: reading the waiting one first would
    // starve the holder (#1531: on Windows CI, nothing moved for 10 s). Collect until every
    // commit is in, or a long deadline passes.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut got = [Vec::new(), Vec::new()];
    while got.iter().any(|g| g.len() < written.len()) && std::time::Instant::now() < deadline {
        for (i, events) in [&mut first, &mut second].into_iter().enumerate() {
            if got[i].len() < written.len()
                && let Some(e) = next_event(&s, events, Duration::from_millis(50))
            {
                got[i].extend(commit_of(&e.unwrap()).map(|(n, _)| n));
            }
        }
    }
    assert_eq!(got[0], written, "the first stream");
    assert_eq!(got[1], written, "the second stream");
}

#[test]
fn caught_up_clients_rejoin_a_quiet_table_set_while_other_tables_write() {
    // simp724's F1 on #1518: with one catch-up slot, clients resuming from a token on a table
    // nobody writes must still get back on the ring, as other tables' writes move the
    // feed's position, and hand the slot on, so none starves.
    let mut l = limits();
    l.max_catch_up = 1;
    let s = Server::start("", l);
    let db = &s.shared.router.service.part().unwrap().db;
    db.table("u")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let u = db.table("u").unwrap().open().unwrap();
    // A token on `t` from an earlier stream.
    let mut first = direct_feed(&s, subscribe(&["t"], b""));
    let r1 = write_row(&s, b"r1");
    let token = next_event(&s, &mut first, Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .token;
    drop(first);
    let resume = || proto::SubscribeRequest {
        start: Some(proto::subscribe_request::Start::Resume(token.clone())),
        ..subscribe(&["t"], b"")
    };
    let mut a = direct_feed(&s, resume());
    let mut b = direct_feed(&s, resume());
    let mut c = direct_feed(
        &s,
        proto::SubscribeRequest {
            start: Some(proto::subscribe_request::Start::FromOldest(true)),
            ..subscribe(&["t"], b"")
        },
    );
    // Only `u` is written: `t` stays quiet.
    for i in 0..20u32 {
        u.mutate(&i.to_be_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    let feeds = &s.shared.router.service.part().unwrap().feeds;
    // The Oldest client delivers r1 first; then all three are on the ring.
    let got_c = drain(&s, &mut c, Duration::from_millis(300));
    assert_eq!(got_c.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(), [r1]);
    wait_for("all three rejoin the ring, the slot handed on", || {
        feeds.stats.rejoined.load(Ordering::Relaxed) >= 3 && feeds.gauges().catching_up == 0
    });
    // A write to `t` reaches each once.
    let r2 = write_row(&s, b"r2");
    for events in [&mut a, &mut b, &mut c] {
        let got = drain(&s, events, Duration::from_millis(300));
        assert_eq!(got.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(), [r2]);
    }
    drop(u);
}

#[test]
fn a_now_subscriber_gets_a_write_made_right_after_it_subscribed() {
    // simp724's F5 on #1518: a Now start's position and its `after` are taken together at
    // subscribe, so a commit pushed before the client's task first runs is still its.
    let s = Server::start("", limits());
    // A subscriber that keeps the hub warm.
    let _keep = direct_feed(&s, subscribe(&["t"], b""));
    write_row(&s, b"warm");
    for i in 0..100u32 {
        let row = format!("n{i:03}");
        let mut c = direct_feed(&s, subscribe(&["t"], row.as_bytes()));
        let n = write_row(&s, row.as_bytes());
        let e = next_event(&s, &mut c, Duration::from_secs(5))
            .unwrap_or_else(|| panic!("round {i}: the write never arrived"))
            .unwrap();
        assert_eq!(commit_of(&e).map(|(s, _)| s), Some(n), "round {i}");
    }
}

#[test]
fn a_single_node_refuses_a_cluster_subscriptions_fields() {
    // D317 ticket 7: allow_partial, skip_through and checkpoint_every mean something only
    // across partitions; a single node says so rather than ignore them.
    let s = Server::start("", limits());
    for r in [
        proto::SubscribeRequest {
            allow_partial: true,
            ..subscribe(&["t"], b"")
        },
        proto::SubscribeRequest {
            skip_through: vec![proto::PartitionSeqno {
                partition: "p1".into(),
                seqno: 1,
            }],
            ..subscribe(&["t"], b"")
        },
        proto::SubscribeRequest {
            checkpoint_every: 4,
            ..subscribe(&["t"], b"")
        },
    ] {
        let e = s
            .block_on(s.shared.router.service.call(
                ServiceOp::Subscribe(r.clone()),
                &Caller::anonymous(),
                None,
            ))
            .unwrap_err();
        assert_eq!(e.code, "InvalidArgument", "{r:?}: {e:?}");
        assert!(e.message.contains("cluster"), "{e:?}");
    }
}
