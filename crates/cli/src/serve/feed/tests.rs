use super::*;
use pigeonhole::{Family, FeedRetention, Options, Pigeonhole};

/// Real commits of seqnos the test can't choose, read off a feed.
fn commits(name: &str, n: usize) -> (Vec<Commit>, Pigeonhole, std::path::PathBuf) {
    let dir = crate::test_dir::fresh(&format!("phdb-feed-{name}-"));
    let db = Pigeonhole::open(
        dir.join("r.phdb"),
        Options::default()
            .create_if_missing(true)
            .shards(1)
            .change_feed(FeedRetention::bytes(1 << 20)),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let mut feed = t.feed().open().unwrap();
    let mut out = Vec::new();
    for i in 0..n {
        t.mutate(format!("r{i}").as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
        out.push(feed.next(Duration::from_secs(5)).unwrap().unwrap().clone());
    }
    drop((feed, t));
    (out, db, dir)
}

/// The next of `events`' items.
async fn next_out(events: &mut FeedEvents) -> Option<Result<Out, ApiError>> {
    std::future::poll_fn(|cx| Pin::new(&mut *events).poll_out(cx)).await
}

/// D331 PR 3b: a range move's target's component hands out each barrier just before
/// its first event past the barrier's `t_from` (feed1248's M4), several in `(t_from, id)`
/// order (S4), none before an event at exactly `t_from` (M5's start there), and a
/// component of tables moved away whole ends with `MovedAway` once it has handed out an
/// event at or past its end, then nothing.
#[tokio::test]
async fn a_targets_component_hands_out_its_barriers_before_the_events_past_them() {
    let token = |seqno: u64| {
        let mut t = vec![1u8];
        t.extend_from_slice(&[7u8; 16]);
        t.extend_from_slice(&1u32.to_le_bytes());
        t.extend_from_slice(&seqno.to_le_bytes());
        Bytes::from(t)
    };
    let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
    for seqno in [5, 10, 15, 20] {
        sink.put(Out::event(proto::FeedEvent {
            token: token(seqno),
            ..Default::default()
        }))
        .await
        .unwrap();
    }
    let barrier = |t_from, id: &str| Barrier {
        t_from,
        token: token(t_from),
        move_id: id.into(),
        source: "p1".into(),
        final_seqno: 99,
    };
    let mut events = events.with_barriers(
        vec![
            barrier(12, "mv2"),
            barrier(8, "mv1"),
            barrier(12, "mv1"),
            barrier(5, "mv0"),
        ],
        Some(15),
        None,
    );
    let mut seen = Vec::new();
    loop {
        match next_out(&mut events).await {
            Some(Ok(out)) => {
                let e = out.into_proto();
                let seqno = crate::serve::cluster::token::token_seqno(&e.token).unwrap();
                seen.push(match e.event {
                    Some(proto::feed_event::Event::MoveBarrier(b)) => {
                        format!("{}@{seqno}", b.move_id)
                    }
                    _ => format!("{seqno}"),
                });
            }
            Some(Err(e)) => {
                seen.push(e.code);
                break;
            }
            None => break,
        }
    }
    assert_eq!(
        seen,
        [
            "5",
            "mv0@5",
            "mv1@8",
            "10",
            "mv1@12",
            "mv2@12",
            "15",
            "MovedAway"
        ]
    );
    assert!(next_out(&mut events).await.is_none(), "and nothing after");
}

/// D331 PR 3b (feed1248's point 1): a hand-over taken between a running component's
/// read of an event and its look at the serving rows still puts the barrier before that
/// event. Mutant: load the version before the read, and this fails (the event goes out
/// unheld).
#[tokio::test]
async fn a_hand_over_taken_after_a_read_still_puts_its_barrier_first() {
    use crate::serve::gate::{LeaseClock, WriteGate};
    let token = |seqno: u64| {
        let mut t = vec![1u8];
        t.extend_from_slice(&[7u8; 16]);
        t.extend_from_slice(&1u32.to_le_bytes());
        t.extend_from_slice(&seqno.to_le_bytes());
        Bytes::from(t)
    };
    let gate = WriteGate::opened("seam-p", false, Arc::new(LeaseClock::default()));
    let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
    sink.put(Out::event(proto::FeedEvent {
        token: token(20),
        ..Default::default()
    }))
    .await
    .unwrap();
    let mut events = events.with_barriers(
        Vec::new(),
        None,
        Some((Arc::clone(&gate), vec!["users".into()])),
    );
    let hand_over = {
        let gate = Arc::clone(&gate);
        let token = token(10);
        move || {
            gate.ranges().add_serving(crate::serve::ranges::Moved {
                id: "mv1".into(),
                table: "users".into(),
                start: b"c".to_vec(),
                end: Some(b"f".to_vec()),
                seqno: 10,
                source: "p1".into(),
                final_seqno: 99,
                token: token.to_vec(),
                ..Default::default()
            });
        }
    };
    *AFTER_READ.lock().unwrap() = Some(("seam-p".into(), Box::new(hand_over)));
    let first = next_out(&mut events).await.unwrap().unwrap().into_proto();
    assert!(
        matches!(first.event, Some(proto::feed_event::Event::MoveBarrier(_))),
        "the barrier first: {first:?}"
    );
    let second = next_out(&mut events).await.unwrap().unwrap().into_proto();
    assert_eq!(
        crate::serve::cluster::token::token_seqno(&second.token),
        Some(20)
    );
}

/// D326 PR 2 (feed1248's F2, F3 and F5): a cluster partition's component delivers only
/// under its lease. Without one since it opened (and the node reaching a majority), it
/// waits, delivering nothing, its catch-up included; once leased, it delivers; when the
/// lease runs out, it ends with the lease's error even with nothing to deliver; and a
/// node whose last round reached no majority refuses the subscribe.
#[tokio::test]
async fn a_cluster_components_events_wait_for_a_lease_and_end_when_it_runs_out() {
    use crate::serve::gate::{LeaseClock, WriteGate};
    let clock = Arc::new(LeaseClock::default());
    let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
    let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
    sink.put(Out::event(proto::FeedEvent::default()))
        .await
        .unwrap();
    let mut events = events.leased(Arc::clone(&gate)).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), next_out(&mut events))
            .await
            .is_err(),
        "nothing before the lease"
    );
    gate.renew(clock.now() + 400_000_000);
    let first = tokio::time::timeout(Duration::from_secs(5), next_out(&mut events))
        .await
        .expect("delivered once leased");
    assert!(matches!(first, Some(Ok(_))));
    // Quiet: no event to carry it, and the lease runs out.
    let end = tokio::time::timeout(Duration::from_secs(5), next_out(&mut events))
        .await
        .expect("woken at the lease's end");
    let Some(Err(e)) = end else {
        panic!("the lease's error, not an event or the end");
    };
    assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
    assert!(e.message.contains("ran out"), "{}", e.message);
    assert!(next_out(&mut events).await.is_none(), "then the end");

    // A node whose last round reached no majority: refused at once.
    clock.set_majority(false);
    let gate = WriteGate::opened("p2", false, Arc::clone(&clock));
    let (_sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
    let e = events.leased(gate).unwrap_err();
    assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
}

/// feed1248's S1 on #1912: a component waiting for its partition's first lease takes
/// nothing, and its sink's idle limit counts from the last take. Each look while it waits
/// counts as one, so a backlog that fills the buffer meanwhile isn't dropped as idle; once
/// leased, every event is delivered.
#[tokio::test]
async fn a_component_waiting_for_its_lease_isnt_dropped_as_idle() {
    use crate::serve::gate::{LeaseClock, WriteGate};
    let clock = Arc::new(LeaseClock::default());
    let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
    let limits = FeedLimits {
        stream_idle: Duration::from_millis(300),
        client_events: 1,
        ..limits_solo_after(SOLO_AFTER)
    };
    let (sink, events, _end) = Sink::new(&limits);
    let mut events = events.leased(Arc::clone(&gate)).unwrap();
    let sink = Arc::new(sink);
    let putter = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            for _ in 0..3 {
                sink.put(Out::event(proto::FeedEvent::default())).await?;
            }
            Ok::<(), ApiError>(())
        })
    };
    // Four idle limits of waiting, the buffer full behind it.
    assert!(
        tokio::time::timeout(Duration::from_millis(1200), next_out(&mut events))
            .await
            .is_err()
    );
    assert!(!putter.is_finished(), "the putter still waits for room");
    gate.renew(clock.now() + 3_600_000_000_000);
    for i in 0..3 {
        let got = tokio::time::timeout(Duration::from_secs(5), next_out(&mut events))
            .await
            .unwrap_or_else(|_| panic!("event {i} delivered once leased"));
        assert!(matches!(got, Some(Ok(_))), "event {i}");
    }
    putter.await.unwrap().unwrap();
}

/// A hub's tracker list stays bounded without a dump (perf287's R1 on #1643): gone
/// clients and those counted under another hub are pruned as it grows.
#[test]
fn a_hubs_tracker_list_stays_bounded_without_a_dump() {
    let hub = || {
        Hub::new(
            Ring::new(SingleRing::default()),
            false,
            Arc::default(),
            Arc::new(AtomicU64::new(1)),
            Arc::default(),
        )
    };
    let (a, b) = (hub(), hub());
    // Ten thousand clients that came and went.
    for _ in 0..10_000 {
        a.track(&Arc::new(Tracker::default()));
    }
    assert!(a.clients.lock().unwrap().len() <= 32, "pruned as it grew");
    // Live clients stay; one that moved to `b` is no longer counted under `a`.
    let live: Vec<Arc<Tracker>> = (0..20).map(|_| Arc::new(Tracker::default())).collect();
    for t in &live {
        a.track(t);
    }
    b.track(&live[0]);
    for _ in 0..64 {
        a.track(&Arc::new(Tracker::default()));
    }
    let me = std::ptr::from_ref(&a) as usize;
    let counted = a
        .clients
        .lock()
        .unwrap()
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|t| t.hub.load(Ordering::Relaxed) == me)
        .count();
    assert_eq!(counted, 19, "the 19 live clients still on `a`");
}

fn ring(start: u64, commits: &[Commit], evicted: usize) -> Ring {
    let mut r = Ring::new(SingleRing {
        start_seqno: start,
        passed: commits.last().map_or(start, Commit::seqno),
        ..SingleRing::default()
    });
    let mut prev = start;
    for c in commits {
        r.entries.push_back(Arc::new(Entry {
            prev,
            ..Entry::new(c, Bytes::new()).unwrap()
        }));
        prev = c.seqno();
    }
    for _ in 0..evicted {
        r.entries.pop_front();
        r.base += 1;
    }
    r
}

/// Commit seqnos from `events` until none comes for `quiet` (durable feeds: a commit
/// arrives once synced, which a slow disk or VM can take a while).
async fn seqnos(events: &mut FeedEvents, quiet: Duration) -> Vec<u64> {
    let mut out = Vec::new();
    while let Ok(Some(e)) = tokio::time::timeout(
        quiet,
        std::future::poll_fn(|cx| Pin::new(&mut *events).poll_next(cx)),
    )
    .await
    {
        if let Some(proto::feed_event::Event::Commit(c)) = e.unwrap().event {
            out.push(c.seqno);
        }
    }
    out
}

#[test]
fn a_failed_hub_is_replaced_and_its_clients_rejoin_the_new_one() {
    // feed1248's R1 and simp724's F2 on #1518.
    let (_, db, dir) = commits("fail", 0);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    let feeds = Feeds::new(
        db.clone(),
        FeedLimits {
            ring_bytes: 1 << 20,
            max_hubs: 4,
            max_catch_up: 1,
            quiet: Duration::from_secs(10),
            stream_idle: Duration::from_secs(5),
            client_events: CLIENT_EVENTS,
            client_bytes: CLIENT_BYTES,
            solo_after: SOLO_AFTER,
            coalesce: Coalesce::default(),
        },
        &FeedShared::new(1),
    );
    let slots = Arc::new(Semaphore::new(8));
    let spec = || Spec {
        tables: vec![t.clone()],
        prefix: Vec::new(),
        visible: false,
        start: Start::Now,
        skip: 0,
        floor: 0,
        announce: false,
        internal: false,
    };
    rt.block_on(async {
        let slot = || Arc::clone(&slots).try_acquire_owned().unwrap();
        let mut a = feeds.subscribe(spec(), slot(), |_| {}).unwrap();
        let first = feeds.hub(std::slice::from_ref(&t), false).unwrap();
        let w = |row: &str| {
            t.mutate(row.as_bytes())
                .put("f", b"q", b"v")
                .commit()
                .unwrap()
                .seqno
        };
        let mut want = vec![w("a1")];
        assert_eq!(seqnos(&mut a, Duration::from_secs(1)).await, want);
        // The shared feed fails (an expiry, say): its engine task ends.
        first.task().as_ref().expect("spawned").abort();
        first.fail();
        want.push(w("a2"));
        // A new subscriber gets a fresh hub, and starts now, not from the failed one.
        let mut b = feeds.subscribe(spec(), slot(), |_| {}).unwrap();
        let second = feeds.hub(std::slice::from_ref(&t), false).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        let after = vec![w("b1"), w("b2")];
        want.extend(&after);
        assert_eq!(seqnos(&mut b, Duration::from_secs(1)).await, after);
        // The old client went on from its own token, once each, and rejoined the new hub,
        // giving its catch-up slot back (one slot: a second laggard would wait for it).
        let got = seqnos(&mut a, Duration::from_secs(2)).await;
        assert_eq!(got, want[1..]);
        assert_eq!(feeds.stats.hub_failures.load(Ordering::Relaxed), 1);
        assert!(feeds.stats.rejoined.load(Ordering::Relaxed) >= 1);
        assert_eq!(feeds.gauges().catching_up, 0);
        drop((a, b, first, second));
    });
    drop(t);
    drop(rt);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A server's three runtimes, as `phdb serve` names them: requests, feed delivery and
/// hubs, one worker each but `delivery_threads` for delivery.
fn server_runtimes(
    delivery_threads: usize,
) -> (
    tokio::runtime::Runtime,
    tokio::runtime::Runtime,
    tokio::runtime::Runtime,
) {
    let rt = |threads: usize, name: Option<&str>| {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(threads).enable_all();
        if let Some(name) = name {
            b.thread_name(name);
        }
        b.build().unwrap()
    };
    (
        rt(1, None),
        rt(delivery_threads, Some("phdb-serve-feed")),
        rt(1, Some("phdb-serve-hub")),
    )
}

#[test]
fn an_out_is_two_bytes_wide_not_a_whole_message() {
    // #1758's g88: each event's `Out` is moved by value through every stream's put and
    // channel slot; with the message inline it was 136 bytes.
    let size = std::mem::size_of::<Out>();
    assert!(size <= 72, "Out is {size} bytes");
}

#[test]
fn a_put_with_room_builds_a_small_future() {
    // #1758's g86c: a cluster subscription puts every event through a `Sink`, and with
    // the slow path's timer and waits inline the future was 1.2 KB, built and copied per
    // event though a put with room needs none of it. Boxed, the slow path costs only
    // when the client is behind.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (sink, _events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
        let put = sink.put(Out::Spliced {
            body: prost::bytes::Bytes::from_static(b"x"),
            tail: prost::bytes::Bytes::new(),
        });
        let size = std::mem::size_of_val(&put);
        assert!(size <= 512, "Sink::put's future is {size} bytes");
        put.await.unwrap();
    });
}

/// A small event whose token is `i`, so the order it comes out in can be checked.
fn ev(i: u8) -> Out {
    Out::event(proto::FeedEvent {
        token: Bytes::from(vec![i]),
        ..Default::default()
    })
}

/// The token of the next event `events` gives, which must be one.
async fn next_token(events: &mut FeedEvents) -> u8 {
    match next_out(events).await {
        Some(Ok(e)) => e.token()[0],
        other => panic!("an event, not {:?}", other.map(|r| r.err())),
    }
}

/// #1737's F3-0: a batch's events come out in order, one channel place for all of them,
/// and the batch's charge stays on the client's buffer until its last event is taken
/// (simp724's (3)), so `pending` says so while the connection holds the rest.
#[tokio::test]
async fn a_batch_is_taken_in_order_and_holds_its_charge_to_its_last_event() {
    let (sink, mut events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
    sink.put_all((0..5).map(ev).collect()).await.unwrap();
    assert_eq!(
        sink.budget.available_permits(),
        sink.client_bytes - 5 * EVENT_FLOOR,
        "five tiny events, each charged the floor"
    );
    assert_eq!(sink.tx.max_capacity() - sink.tx.capacity(), 1, "one place");
    for i in 0..5 {
        assert!(
            sink.pending(),
            "charged until the batch's last event is taken"
        );
        assert_eq!(next_token(&mut events).await, i);
    }
    assert!(!sink.pending(), "the batch's charge is back");
}

/// simp724's (1) on #1737's F3-0: tiny events are charged [`EVENT_FLOOR`] each, and a
/// batch closes before an event that would take it past the buffer. With room for four,
/// ten events go as batches of four, four and two; each waits for the one before to be
/// taken to its last event, and all ten come out in order.
#[tokio::test]
async fn a_batch_closes_before_the_buffer_and_tiny_events_are_charged_a_floor() {
    let limits = FeedLimits {
        client_bytes: 4 * EVENT_FLOOR,
        ..limits_solo_after(SOLO_AFTER)
    };
    let (sink, mut events, _end) = Sink::new(&limits);
    let sink = Arc::new(sink);
    let putter = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move { sink.put_all((0..10).map(ev).collect()).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(sink.budget.available_permits(), 0, "four fill the buffer");
    for i in 0..3 {
        assert_eq!(next_token(&mut events).await, i);
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !putter.is_finished(),
        "the next four wait for the last of the first"
    );
    assert_eq!(
        sink.tx.max_capacity() - sink.tx.capacity(),
        0,
        "nothing queued"
    );
    for i in 3..10 {
        assert_eq!(next_token(&mut events).await, i);
    }
    putter.await.unwrap().unwrap();
    assert!(!sink.pending());
}

/// simp724's I1 on #1536, with batches (#1737's F3-0): a client that takes a batch's
/// events one every 200 ms, under the 250 ms idle limit, while a send waits for the room
/// the batch holds, is slow, not idle. Takes within a batch are stamped while a send
/// waits, so the wait counts from the last of them. On a paused clock, so exact.
#[tokio::test(start_paused = true)]
async fn a_client_taking_a_batch_slowly_is_slow_not_idle() {
    let limits = FeedLimits {
        client_bytes: 4 * EVENT_FLOOR,
        stream_idle: Duration::from_millis(250),
        ..limits_solo_after(SOLO_AFTER)
    };
    let (sink, mut events, _end) = Sink::new(&limits);
    let sink = Arc::new(sink);
    sink.put_all((0..4).map(ev).collect()).await.unwrap();
    let putter = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move { sink.put(ev(4)).await })
    };
    for i in 0..4 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(next_token(&mut events).await, i);
    }
    putter
        .await
        .unwrap()
        .expect("not timed out: the client took all along");
    assert_eq!(next_token(&mut events).await, 4);
}

/// #2027: a batch's events spend the task's Tokio budget as channel pops do, so a
/// connection taking a long batch gives its worker back every 128 events at most (the
/// first event's pop included), and gets every event, in order, across the yields.
/// `gate` leases the stream (simp724): a serving lease changes none of that.
async fn a_long_batch_yields_every_budget(gate: Option<Arc<crate::serve::gate::WriteGate>>) {
    const N: u16 = 300;
    let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
    let mut events = match gate {
        Some(g) => events.leased(g).unwrap(),
        None => events,
    };
    let ev = |i: u16| {
        Out::event(proto::FeedEvent {
            token: Bytes::copy_from_slice(&i.to_le_bytes()),
            ..Default::default()
        })
    };
    sink.put_all((0..N).map(ev).collect()).await.unwrap();
    let mut got = Vec::new();
    let mut rounds = Vec::new();
    while got.len() < usize::from(N) {
        // One task poll: events until the stream says Pending.
        let n = std::future::poll_fn(|cx| {
            let mut n = 0;
            loop {
                match Pin::new(&mut events).poll_out(cx) {
                    Poll::Ready(Some(Ok(e))) => {
                        got.push(u16::from_le_bytes([e.token()[0], e.token()[1]]));
                        n += 1;
                    }
                    Poll::Pending => return Poll::Ready(n),
                    Poll::Ready(other) => panic!("an event, not {:?}", other.map(|r| r.err())),
                }
            }
        })
        .await;
        rounds.push(n);
        // Out of budget: the next poll of the task gets a new one.
        tokio::task::yield_now().await;
    }
    assert!(rounds.iter().all(|&n| n <= 128), "{rounds:?}");
    assert_eq!(rounds[0], 128, "{rounds:?}");
    assert_eq!(got, (0..N).collect::<Vec<_>>(), "every event, in order");
    assert!(!sink.pending(), "the batch's charge is back");
}

#[tokio::test]
async fn a_long_batch_gives_its_worker_back_every_128_events() {
    a_long_batch_yields_every_budget(None).await;
}

#[tokio::test]
async fn a_leased_long_batch_gives_its_worker_back_and_delivers_it_all() {
    use crate::serve::gate::{LeaseClock, WriteGate};
    let clock = Arc::new(LeaseClock::default());
    let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
    gate.renew(clock.now() + 60_000_000_000);
    a_long_batch_yields_every_budget(Some(gate)).await;
}

/// simp724 on #1737's F3-0: a cluster component whose lease runs out while the connection
/// holds the rest of a batch ends with the lease's error before the batch's next event.
#[tokio::test]
async fn a_lease_that_runs_out_mid_batch_ends_the_stream_before_its_next_event() {
    use crate::serve::gate::{LeaseClock, WriteGate};
    let clock = Arc::new(LeaseClock::default());
    let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
    let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
    let mut events = events.leased(Arc::clone(&gate)).unwrap();
    gate.renew(clock.now() + 300_000_000);
    sink.put_all((0..3).map(ev).collect()).await.unwrap();
    assert_eq!(next_token(&mut events).await, 0);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let Some(Err(e)) = next_out(&mut events).await else {
        panic!("the lease's error, not the batch's next event");
    };
    assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
}

fn limits_solo_after(solo_after: Duration) -> FeedLimits {
    FeedLimits {
        ring_bytes: 16 << 20,
        max_hubs: 4,
        max_catch_up: 1,
        quiet: Duration::from_secs(10),
        stream_idle: Duration::from_secs(5),
        client_events: CLIENT_EVENTS,
        client_bytes: CLIENT_BYTES,
        solo_after,
        coalesce: Coalesce::default(),
    }
}

fn now_spec(t: &Table) -> Spec {
    Spec {
        tables: vec![t.clone()],
        prefix: Vec::new(),
        visible: false,
        start: Start::Now,
        skip: 0,
        floor: 0,
        announce: false,
        internal: false,
    }
}

/// Waits up to a second for `rt` to have `n` live tasks: a task that moved away is
/// released once its runtime gets to it.
async fn tasks_on(rt: &tokio::runtime::Runtime, n: usize) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let alive = rt.metrics().num_alive_tasks();
        if alive == n || tokio::time::Instant::now() >= deadline {
            return alive;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[test]
fn hubs_and_clients_run_on_their_own_runtimes() {
    // Ticket 5's (A) and (C), with #1689's fast path: with a server's runtimes, a
    // subscription's client task runs on the feed runtime, and so does its hub while it
    // has that one client. A second client moves the hub to the hub runtime; alone again
    // for `solo_after`, it moves back. Nothing of the feed runs on the request's runtime,
    // and events still reach the stream the request side reads (perf287's T1 on #1696).
    let (_, db, dir) = commits("feedrt", 0);
    let (requests, delivery, hubs) = server_runtimes(1);
    let t = db.table("t").unwrap().open().unwrap();
    let feeds = Feeds::new(
        db.clone(),
        limits_solo_after(Duration::from_millis(50)),
        &FeedShared::new(1),
    );
    feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
    let slots = Arc::new(Semaphore::new(8));
    let before = delivery.metrics().num_alive_tasks();
    let hubs_before = hubs.metrics().num_alive_tasks();
    requests.block_on(async {
        let put = |row: &[u8]| t.mutate(row).put("f", b"q", b"v").commit().unwrap().seqno;
        let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
        let mut a = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
        assert_eq!(
            delivery.metrics().num_alive_tasks(),
            before + 2,
            "the client and its hub, on the feed runtime"
        );
        assert_eq!(hubs.metrics().num_alive_tasks(), hubs_before);
        assert_eq!(feeds.gauges().solo_hubs, 1);
        assert_eq!(
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks(),
            0,
            "nothing of the feed on the request side"
        );
        let s1 = put(b"r1");
        assert_eq!(seqnos(&mut a, Duration::from_millis(300)).await, [s1]);

        // A second client: the next commit moves the hub to the hub runtime.
        let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
        let mut b = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
        let s2 = put(b"r2");
        assert_eq!(seqnos(&mut a, Duration::from_millis(300)).await, [s2]);
        assert_eq!(seqnos(&mut b, Duration::from_millis(300)).await, [s2]);
        assert_eq!(
            tasks_on(&hubs, hubs_before + 1).await,
            hubs_before + 1,
            "the hub"
        );
        assert_eq!(
            tasks_on(&delivery, before + 2).await,
            before + 2,
            "two clients"
        );
        assert_eq!(feeds.gauges().solo_hubs, 0);

        // Alone again once B's task has ended: it stays on the hub runtime for
        // `solo_after`, then moves back.
        drop(b);
        assert_eq!(
            tasks_on(&delivery, before + 1).await,
            before + 1,
            "B's task ended"
        );
        let s3 = put(b"r3");
        assert_eq!(seqnos(&mut a, Duration::from_millis(100)).await, [s3]);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let s4 = put(b"r4");
        assert_eq!(seqnos(&mut a, Duration::from_millis(300)).await, [s4]);
        assert_eq!(tasks_on(&hubs, hubs_before).await, hubs_before);
        assert_eq!(tasks_on(&delivery, before + 2).await, before + 2);
        assert_eq!(feeds.gauges().solo_hubs, 1);
        assert_eq!(feeds.stats.hub_moves.load(Ordering::Relaxed), 2);
        drop(a);
    });
    // The dump names the hub runtime's threads (#1719): one here.
    assert!(
        feeds.dump().contains(" hubs on 1 hub threads,"),
        "{}",
        feeds.dump()
    );
    drop(t);
    drop(requests);
    drop(delivery);
    drop(hubs);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// #1926: a new hub whose task handles its first item before [`Feeds::hub`] returns still
/// delivers. The task used to be spawned while its hub was being made (`Arc::new_cyclic`):
/// at a commit already waiting, it found no hub to upgrade and stopped, and the
/// subscription got nothing ever again (1 run in 15-60 of the cluster pin test). The seam
/// holds `hub` right after the spawn, commits, and waits for the task's first item, so
/// that order happens every time.
#[test]
fn a_hub_whose_task_runs_before_the_hub_is_returned_still_delivers() {
    let (_, db, dir) = commits("hubstart", 0);
    let (requests, delivery, hubs) = server_runtimes(1);
    let t = db.table("t").unwrap().open().unwrap();
    let feeds = Feeds::new(
        db.clone(),
        limits_solo_after(Duration::from_secs(60)),
        &FeedShared::new(1),
    );
    feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
    let seam_db = db.clone();
    *feeds.after_hub_spawn.lock().unwrap() = Some(Box::new(move |started: &Arc<Started>| {
        // A commit the hub's stream starts before, which its task reads while the hub is
        // still being opened.
        let t = seam_db.table("t").unwrap().open().unwrap();
        t.mutate(b"r0").put("f", b"q", b"v").commit().unwrap();
        assert!(
            started.wait(Duration::from_secs(10)),
            "the hub's task never handled its first item"
        );
    }));
    let slots = Arc::new(Semaphore::new(8));
    requests.block_on(async {
        let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
        let mut a = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
        *feeds.after_hub_spawn.lock().unwrap() = None;
        let s1 = t.mutate(b"r1").put("f", b"q", b"v").commit().unwrap().seqno;
        assert_eq!(
            seqnos(&mut a, Duration::from_millis(500)).await,
            [s1],
            "the hub delivers after its task handled a commit before it was returned"
        );
        drop(a);
    });
    drop(t);
    drop(requests);
    drop(delivery);
    drop(hubs);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// #1901 (A): a hub tells its clients once per batch it reads, not once per commit, and
/// at least every [`NOTIFY_EVERY`] pushes, so a hub whose feed never runs dry still
/// reaches them (perf2 on the design note). 200 commits are ready before its first poll.
#[test]
fn a_hub_tells_its_clients_once_per_batch_and_at_least_every_64_pushes() {
    let (_, db, dir) = commits("hubnotify", 0);
    let t = db.table("t").unwrap().open().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let hub = Arc::new(Hub::new(
        Ring::new(SingleRing::default()),
        false,
        Arc::default(),
        Arc::new(AtomicU64::new(1)),
        Arc::default(),
    ));
    let snapshot = db.snapshot().unwrap();
    let stream = open(std::slice::from_ref(&t), true, &Start::Snapshot(snapshot)).unwrap();
    let put = |row: &str| {
        t.mutate(row.as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    };
    for i in 0..200 {
        put(&format!("r{i:03}"));
    }
    let d = Driver {
        stream: FeedStream::new(stream),
        hub: Arc::downgrade(&hub),
        ring_bytes: 16 << 20,
        runtimes: None,
        solo_after: Duration::ZERO,
        place: Place::Feed,
        alone: None,
        coalesce: Duration::ZERO,
        unnotified: 0,
        batch: 0,
        quiet_until: None,
        started: None,
    };
    let task = rt.spawn(drive(d));
    let head_at = |n: u64| {
        rt.block_on(async {
            for _ in 0..400 {
                if hub.ring().head() >= n {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            panic!("the hub's head never reached {n}");
        });
    };
    head_at(200);
    rt.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
    assert_eq!(
        hub.notifies.load(Ordering::Relaxed),
        4,
        "at 64, 128 and 192 pushes, then once the feed had nothing more"
    );
    put("r200");
    head_at(201);
    rt.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
    assert_eq!(
        hub.notifies.load(Ordering::Relaxed),
        5,
        "one commit, one notify"
    );
    task.abort();
    drop((hub, t, rt));
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// #1901 (B): a caught-up hub waits `Coalesce::hub` after a small batch, never before one.
/// After a quiet gap a lone commit is delivered at once; two commits made during the wait
/// after it are read together when it ends, one batch and one notify. The wait is 2 s and
/// the lone commit's bound half of it, so a hub that waited before it fails by a wide
/// margin while a slow runner doesn't (#1970's Windows CI took 1.27 s with a disk sync
/// inside the old 280 ms bound).
#[test]
fn a_caught_up_hub_waits_after_a_small_batch_but_never_before_one() {
    let (_, db, dir) = commits("hubcoalesce", 0);
    let (requests, delivery, hubs) = server_runtimes(1);
    let t = db.table("t").unwrap().open().unwrap();
    let mut limits = limits_solo_after(Duration::from_secs(60));
    const WAIT: Duration = Duration::from_secs(2);
    limits.coalesce.hub = WAIT;
    let feeds = Feeds::new(db.clone(), limits, &FeedShared::new(1));
    feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
    let slots = Arc::new(Semaphore::new(8));
    requests.block_on(async {
        // The visible feed and Buffered commits: no disk sync inside the timed windows. On
        // the durable feed a commit's group sync counted toward the hub's wait, and a slow
        // Windows runner's took 1.27 s (#1970's CI).
        let put = |row: &[u8]| {
            t.mutate(row)
                .put("f", b"q", b"v")
                .durability(pigeonhole::Durability::Buffered)
                .commit()
                .unwrap()
                .seqno
        };
        let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
        let spec = Spec {
            visible: true,
            ..now_spec(&t)
        };
        let mut a = feeds.subscribe(spec, slot, |_| {}).unwrap();
        let hub = feeds
            .hubs
            .lock()
            .unwrap()
            .values()
            .find_map(Weak::upgrade)
            .unwrap();
        // No batch yet, so no wait: the first commit is read at once.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let s1 = put(b"r1");
        let t0 = Instant::now();
        assert_eq!(seqnos(&mut a, Duration::from_millis(100)).await, [s1]);
        let first = t0.elapsed();
        assert!(
            first < WAIT / 2,
            "a lone commit waited: {first:?} (its 100 ms quiet included)"
        );
        // The hub is in its wait after that batch of one: both are read after it. Apart by
        // a tenth of the wait, so a hub that didn't wait would read r2 alone.
        let notified = hub.notifies.load(Ordering::Relaxed);
        let s2 = put(b"r2");
        tokio::time::sleep(WAIT / 10).await;
        let s3 = put(b"r3");
        let mut got = Vec::new();
        while got.len() < 2 {
            let e = tokio::time::timeout(
                WAIT * 2,
                std::future::poll_fn(|cx| Pin::new(&mut a).poll_next(cx)),
            )
            .await
            .unwrap_or_else(|_| panic!("r2 and r3 not read within twice the wait: {got:?}"));
            if let Some(proto::feed_event::Event::Commit(c)) = e.unwrap().unwrap().event {
                got.push(c.seqno);
            }
        }
        assert_eq!(got, [s2, s3]);
        assert_eq!(
            hub.notifies.load(Ordering::Relaxed) - notified,
            1,
            "two commits during the wait, one batch"
        );
        drop((a, hub));
    });
    drop(t);
    drop(requests);
    drop(delivery);
    drop(hubs);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_single_nodes_hub_placement_is_mains() {
    // #1798, g102: on a node with no cluster merges, `Driver::moves` decides as main's
    // did (4a47d2a47): the hub runtime at once with two or more clients, back to the feed
    // runtime once alone for `solo_after`. B's `internal()` must never read a client-only
    // hub as a merge's, at any member count, so a single node's placement is unchanged.
    // `main` is main's rule, written out; every step's answer is checked against it.
    fn main(place: Place, members: usize, alone_due: bool) -> Option<Place> {
        match place {
            Place::Feed => (members >= 2).then_some(Place::Hubs),
            Place::Hubs if members >= 2 => None,
            Place::Hubs => alone_due.then_some(Place::Feed),
        }
    }
    let (_, db, dir) = commits("feedpin", 0);
    let t = db.table("t").unwrap().open().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let hub = Arc::new(Hub::new(
        Ring::new(SingleRing::default()),
        true,
        Arc::default(),
        Arc::new(AtomicU64::new(1)),
        Arc::default(),
    ));
    let snapshot = db.snapshot().unwrap();
    let stream = open(std::slice::from_ref(&t), true, &Start::Snapshot(snapshot)).unwrap();
    let mut d = Driver {
        stream: FeedStream::new(stream),
        hub: Arc::downgrade(&hub),
        ring_bytes: 16 << 20,
        runtimes: Some((rt.handle().clone(), rt.handle().clone())),
        solo_after: Duration::ZERO,
        place: Place::Feed,
        alone: None,
        coalesce: Duration::ZERO,
        unnotified: 0,
        batch: 0,
        quiet_until: None,
        started: None,
    };
    // One step, checked against main's rule; `alone_due`: main's answer alone on the hub
    // runtime (the step after the one that started the `solo_after` clock).
    let step = |d: &mut Driver, alone_due: bool| {
        let members = hub.members();
        assert!(!hub.internal(), "a client-only hub with {members} members");
        let got = d.moves(&hub);
        let want = main(d.place, members, alone_due);
        assert_eq!(got, want, "{members} members at {:?}", d.place);
        if let Some(p) = got {
            d.place = p;
        }
        got
    };
    let join = || hub.join(&Arc::new(Tracker::default()), false);
    let mut joined = Vec::new();
    assert_eq!(step(&mut d, false), None, "no members yet");
    joined.push(join());
    assert_eq!(step(&mut d, false), None, "one client: solo, beside it");
    joined.push(join());
    assert_eq!(step(&mut d, false), Some(Place::Hubs), "a second client");
    joined.push(join());
    assert_eq!(step(&mut d, false), None, "three: stays");
    joined.truncate(2);
    assert_eq!(step(&mut d, false), None, "two: stays");
    joined.truncate(1);
    assert_eq!(step(&mut d, false), None, "alone: the clock starts");
    assert_eq!(
        step(&mut d, true),
        Some(Place::Feed),
        "alone past solo_after"
    );
    joined.clear();
    assert_eq!(step(&mut d, false), None, "none left, on the feed runtime");
    drop((d, t));
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_hub_read_only_by_cluster_merges_runs_on_the_hub_runtime_with_a_full_share() {
    // #1798, g100: a partition hub whose one member is a cluster hub's merge isn't solo.
    // Its reader runs on the feed runtime, which also delivers every cluster subscriber's
    // events, so the hub's engine reads go to the hub runtime at its first commit and
    // stay there past `solo_after`. Its ring takes an equal share of `--feed-ring-bytes`
    // with every other ring that has a member, as a client's hub does.
    let (_, db, dir) = commits("feedint", 0);
    let (requests, delivery, hubs) = server_runtimes(1);
    let t = db.table("t").unwrap().open().unwrap();
    let u = db
        .table("u")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let shared = FeedShared::new(1);
    let feeds = Feeds::new(
        db.clone(),
        limits_solo_after(Duration::from_millis(50)),
        &shared,
    );
    feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
    let slots = Arc::new(Semaphore::new(8));
    let hubs_before = hubs.metrics().num_alive_tasks();
    requests.block_on(async {
        let put =
            |t: &Table, row: &[u8]| t.mutate(row).put("f", b"q", b"v").commit().unwrap().seqno;
        let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
        let merge = Spec {
            internal: true,
            ..now_spec(&t)
        };
        let mut m = feeds.subscribe(merge, slot, |_| {}).unwrap();
        let s1 = put(&t, b"r1");
        assert_eq!(seqnos(&mut m, Duration::from_millis(300)).await, [s1]);
        assert_eq!(
            tasks_on(&hubs, hubs_before + 1).await,
            hubs_before + 1,
            "the merge's hub, on the hub runtime"
        );
        assert_eq!(feeds.gauges().solo_hubs, 0);
        // Past `solo_after`, with another commit: still there.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let s2 = put(&t, b"r2");
        assert_eq!(seqnos(&mut m, Duration::from_millis(300)).await, [s2]);
        let s3 = put(&t, b"r3");
        assert_eq!(seqnos(&mut m, Duration::from_millis(300)).await, [s3]);
        assert_eq!(tasks_on(&hubs, hubs_before + 1).await, hubs_before + 1);
        assert_eq!(feeds.gauges().solo_hubs, 0);
        assert_eq!(feeds.stats.hub_moves.load(Ordering::Relaxed), 1);

        // A client's hub beside it: two rings with members, so each has half.
        let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
        let mut c = feeds.subscribe(now_spec(&u), slot, |_| {}).unwrap();
        assert_eq!(shared.sharers.load(Ordering::Relaxed), 2);
        let total = 16 << 20;
        for h in feeds
            .hubs
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
        {
            assert_eq!(h.ring_limit(total), total / 2);
        }
        let s4 = put(&u, b"r4");
        assert_eq!(seqnos(&mut c, Duration::from_millis(300)).await, [s4]);
        assert_eq!(feeds.gauges().solo_hubs, 1, "the client's hub is solo");
        drop(c);
        drop(m);
        // Every membership gone, once the clients' tasks end: no ring splits the budget.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while shared.sharers.load(Ordering::Relaxed) != 0 && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(shared.sharers.load(Ordering::Relaxed), 0);
    });
    drop((t, u));
    drop(requests);
    drop(delivery);
    drop(hubs);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_hub_moving_between_runtimes_mid_stream_delivers_every_event_once_in_order() {
    // #1689: a hub's task moves between the feed and hub runtimes as a second client
    // comes and goes, while commits keep coming. The stream moves with it between two
    // polls, so a client there throughout gets every commit once, in order, and each
    // passing client a gapless run of them. No hysteresis, so it moves as often as it can.
    const COMMITS: usize = 1000;
    let (_, db, dir) = commits("feedmove", 0);
    let (requests, delivery, hubs) = server_runtimes(2);
    let t = db.table("t").unwrap().open().unwrap();
    let feeds = Feeds::new(
        db.clone(),
        limits_solo_after(Duration::ZERO),
        &FeedShared::new(1),
    );
    feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
    let slots = Arc::new(Semaphore::new(8));
    requests.block_on(async {
        let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
        let mut a = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
        let writer = {
            let t = t.clone();
            std::thread::spawn(move || {
                (0..COMMITS)
                    .map(|i| {
                        if i % 5 == 0 {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        t.mutate(format!("r{i}").as_bytes())
                            .put("f", b"q", b"v")
                            .commit()
                            .unwrap()
                            .seqno
                    })
                    .collect::<Vec<u64>>()
            })
        };
        // A reads all along, on its own task, while B comes and goes.
        let reader = tokio::spawn(async move {
            let got = seqnos(&mut a, Duration::from_secs(2)).await;
            drop(a);
            got
        });
        let mut passing = Vec::new();
        while !writer.is_finished() {
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let mut b = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
            let mut run = Vec::new();
            while run.len() < 20
                && let Ok(Some(e)) = tokio::time::timeout(
                    Duration::from_millis(5),
                    std::future::poll_fn(|cx| Pin::new(&mut b).poll_next(cx)),
                )
                .await
            {
                if let Some(proto::feed_event::Event::Commit(c)) = e.unwrap().event {
                    run.push(c.seqno);
                }
            }
            passing.push(run);
            drop(b);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let want = writer.join().unwrap();
        let got = reader.await.unwrap();
        assert_eq!(got.len(), want.len(), "every commit once");
        assert_eq!(got, want, "in order");
        for run in passing.iter().filter(|r| !r.is_empty()) {
            let from = want.iter().position(|s| *s == run[0]).unwrap();
            assert_eq!(run[..], want[from..from + run.len()], "a gapless run");
        }
        let moves = feeds.stats.hub_moves.load(Ordering::Relaxed);
        assert!(moves >= 6, "the hub moved back and forth ({moves} moves)");
    });
    drop(t);
    drop(requests);
    drop(delivery);
    drop(hubs);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_hub_with_a_backlog_yields_its_feed_worker() {
    // #1397's fan-out wedge: a hub that fell behind is always ready, and its feed isn't
    // part of Tokio's budget. It must still give its worker back now and then, or other
    // hubs and every client on the feed runtime starve until its backlog is gone.
    const BACKLOG: u64 = 3000;
    let (_, db, dir) = commits("hubyield", 0);
    let requests = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let delivery = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    let feeds = Feeds::new(
        db.clone(),
        FeedLimits {
            ring_bytes: 64 << 20,
            max_hubs: 4,
            max_catch_up: 1,
            quiet: Duration::from_secs(10),
            stream_idle: Duration::from_secs(30),
            client_events: CLIENT_EVENTS,
            client_bytes: CLIENT_BYTES,
            solo_after: SOLO_AFTER,
            coalesce: Coalesce::default(),
        },
        &FeedShared::new(1),
    );
    feeds.run_on(delivery.handle().clone(), delivery.handle().clone());
    let slots = Arc::new(Semaphore::new(8));
    let seen = requests.block_on(async {
        let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
        let spec = Spec {
            tables: vec![t.clone()],
            prefix: Vec::new(),
            visible: false,
            start: Start::Now,
            skip: 0,
            floor: 0,
            announce: false,
            internal: false,
        };
        let _events = feeds.subscribe(spec, slot, |_| {}).unwrap();
        // Park the feed runtime's one worker while the backlog is written, so the hub
        // finds all of it ready at once.
        let (park, parked) = std::sync::mpsc::channel::<()>();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        delivery.spawn(async move {
            park.send(()).unwrap();
            let _ = wait.recv();
        });
        parked.recv().unwrap();
        // All but the last Buffered: 3000 group-syncs took about a minute on Windows CI
        // (#1961). The subscription reads the durable feed, so the last commit's group
        // sync makes the whole backlog durable before the hub sees any of it.
        let mut last = 0;
        for i in 0..BACKLOG {
            let durability = if i + 1 == BACKLOG {
                pigeonhole::Durability::GroupSync
            } else {
                pigeonhole::Durability::Buffered
            };
            last = t
                .mutate(format!("r{i}").as_bytes())
                .put("f", b"q", b"v")
                .durability(durability)
                .commit()
                .unwrap()
                .seqno;
        }
        assert!(
            db.durable_snapshot().unwrap().seqno() >= last,
            "the backlog is durable before the hub is released"
        );
        // Queued behind the hub's wake: it runs once the hub gives the worker back.
        let probe = {
            let feeds = Arc::clone(&feeds);
            delivery.spawn(async move {
                let hubs = feeds.hubs.lock().unwrap();
                let hub = hubs.values().next().unwrap().upgrade().unwrap();
                hub.ring().head()
            })
        };
        release.send(()).unwrap();
        probe.await.unwrap()
    });
    // Not a tighter bar: the probe waits in the runtime's injection queue, which a worker
    // checks only every `global_queue_interval` ticks while the yielded hub sits on its
    // local queue, so how far the hub gets first is Tokio's tuning (feed1248's Q1 on
    // #1599). Without the yield it gets through all of the backlog.
    assert!(
        seen < BACKLOG,
        "the hub kept its worker through {seen} of {BACKLOG} backlogged commits"
    );
    drop(t);
    drop(requests);
    drop(delivery);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_slow_client_that_keeps_taking_outlasts_a_large_events_wait() {
    // simp724's I1 on #1536: a large event waits for the whole buffer to drain. A client
    // that takes an event every 200 ms, under the 250 ms idle limit, drains it in far
    // more than 250 ms, and must not time out: the limit counts from its last take. On a
    // paused clock (#1531), so the pacing and the limit are virtual time, whatever the
    // runner's speed. The hub's task is stopped and the test pushes for it.
    let (_, db, dir) = commits("slow", 0);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    let feeds = Feeds::new(
        db.clone(),
        FeedLimits {
            ring_bytes: 16 << 20,
            max_hubs: 4,
            max_catch_up: 1,
            quiet: Duration::from_secs(3600),
            stream_idle: Duration::from_millis(250),
            client_events: CLIENT_EVENTS,
            client_bytes: 4096,
            solo_after: SOLO_AFTER,
            coalesce: Coalesce::default(),
        },
        &FeedShared::new(1),
    );
    let slots = Arc::new(Semaphore::new(1));
    let mut direct = t.feed().open().unwrap();
    let mut want = Vec::new();
    rt.block_on(async {
        let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
        hub.task().as_ref().expect("spawned").abort();
        let mut c = feeds
            .subscribe(
                Spec {
                    tables: vec![t.clone()],
                    prefix: Vec::new(),
                    visible: false,
                    start: Start::Now,
                    skip: 0,
                    floor: 0,
                    announce: false,
                    internal: false,
                },
                Arc::clone(&slots).try_acquire_owned().unwrap(),
                |_| {},
            )
            .unwrap();
        // 60 small commits, then one larger than the whole buffer, then a small one.
        let rows = (0..60)
            .map(|i| (format!("r{i:04}").into_bytes(), vec![b'v']))
            .chain([
                (b"zbig".to_vec(), vec![7u8; 8192]),
                (b"zz".to_vec(), vec![b'v']),
            ]);
        for (row, value) in rows {
            want.push(
                t.mutate(&row)
                    .put("f", b"q", &value)
                    .commit()
                    .unwrap()
                    .seqno,
            );
            let commit = direct
                .next(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .clone();
            let token = Bytes::copy_from_slice(direct.token().as_bytes());
            hub.push(commit, token, direct.position().seqno(), 16 << 20).unwrap();
        }
        let mut got = Vec::new();
        while got.len() < want.len() {
            use futures_core::Stream as _;
            let e = std::future::poll_fn(|cx| Pin::new(&mut c).poll_next(cx))
                .await
                .expect("the stream goes on")
                .unwrap_or_else(|e| panic!("after {} events: {e:?}", got.len()));
            if let Some(proto::feed_event::Event::Commit(commit)) = e.event {
                got.push(commit.seqno);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(got, want);
        drop((c, hub));
    });
    drop(t);
    drop(rt);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_client_that_stops_taking_times_out_an_idle_limit_after_its_last_take() {
    // The other half of simp724's I1 on #1536, for the one-timer wait (perf287's profile
    // on #1397): while one large event waits for the client's buffer to drain, the client
    // takes events, then stops. It's ended exactly `--stream-idle-ms` after its last
    // take: not when the limit has passed since the wait began (that was during the
    // takes), and not later. On a paused clock, so the times are exact.
    let (_, db, dir) = commits("idle-from-take", 0);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    let idle = Duration::from_millis(250);
    let feeds = Feeds::new(
        db.clone(),
        FeedLimits {
            ring_bytes: 16 << 20,
            max_hubs: 4,
            max_catch_up: 1,
            quiet: Duration::from_secs(3600),
            stream_idle: idle,
            client_events: CLIENT_EVENTS,
            client_bytes: 4096,
            solo_after: SOLO_AFTER,
            coalesce: Coalesce::default(),
        },
        &FeedShared::new(1),
    );
    let slots = Arc::new(Semaphore::new(1));
    let mut direct = t.feed().open().unwrap();
    let ended: Arc<Mutex<Option<(tokio::time::Instant, String)>>> = Arc::default();
    rt.block_on(async {
        let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
        hub.task().as_ref().expect("spawned").abort();
        let end = Arc::clone(&ended);
        let mut c = feeds
            .subscribe(
                Spec {
                    tables: vec![t.clone()],
                    prefix: Vec::new(),
                    visible: false,
                    start: Start::Now,
                    skip: 0,
                    floor: 0,
                    announce: false,
                    internal: false,
                },
                Arc::clone(&slots).try_acquire_owned().unwrap(),
                move |e| {
                    *end.lock().unwrap() = Some((
                        tokio::time::Instant::now(),
                        e.map_or_else(String::new, |e| e.code.clone()),
                    ));
                },
            )
            .unwrap();
        // 60 events of over 200 bytes (the buffer holds under 20 of them), then one larger
        // than the whole buffer, which waits for all of it to drain.
        for i in 0..61 {
            let value = if i < 60 {
                vec![b'v'; 100]
            } else {
                vec![7u8; 8192]
            };
            t.mutate(format!("r{i:04}").as_bytes())
                .put("f", b"q", &value)
                .commit()
                .unwrap();
            let commit = direct
                .next(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .clone();
            let token = Bytes::copy_from_slice(direct.token().as_bytes());
            hub.push(commit, token, direct.position().seqno(), 16 << 20).unwrap();
        }
        // 50 takes, 100 ms apart (each under the limit), then none: the large event began
        // waiting after about 40, so its wait spans the last ten or so.
        let mut last = tokio::time::Instant::now();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            use futures_core::Stream as _;
            std::future::poll_fn(|cx| Pin::new(&mut c).poll_next(cx))
                .await
                .expect("the stream goes on")
                .expect("an event");
            last = tokio::time::Instant::now();
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        let (at, code) = ended.lock().unwrap().clone().expect("the stream ended");
        assert_eq!(code, "DeadlineExceeded");
        assert_eq!(
            at - last,
            idle,
            "the idle limit counts from the last take, exactly"
        );
        drop((c, hub));
    });
    drop(t);
    drop(rt);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_now_start_keeps_a_commit_pushed_before_its_task_first_runs() {
    // simp724's F5 on #1518, made deterministic: on a current-thread runtime the client's
    // task can't run until the test awaits, so the ring gets the commit first. The start
    // taken at subscribe still covers it; one read when the task first ran would not.
    let (_, db, dir) = commits("now", 0);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    let feeds = Feeds::new(
        db.clone(),
        FeedLimits {
            ring_bytes: 1 << 20,
            max_hubs: 4,
            max_catch_up: 1,
            quiet: Duration::from_secs(10),
            stream_idle: Duration::from_secs(5),
            client_events: CLIENT_EVENTS,
            client_bytes: CLIENT_BYTES,
            solo_after: SOLO_AFTER,
            coalesce: Coalesce::default(),
        },
        &FeedShared::new(1),
    );
    let slots = Arc::new(Semaphore::new(1));
    rt.block_on(async {
        let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
        // The hub's own task stops; the test pushes for it.
        hub.task().as_ref().expect("spawned").abort();
        let mut direct = t.feed().open().unwrap();
        let mut c = feeds
            .subscribe(
                Spec {
                    tables: vec![t.clone()],
                    prefix: Vec::new(),
                    visible: false,
                    start: Start::Now,
                    skip: 0,
                    floor: 0,
                    announce: false,
                    internal: false,
                },
                Arc::clone(&slots).try_acquire_owned().unwrap(),
                |_| {},
            )
            .unwrap();
        let n = t.mutate(b"x").put("f", b"q", b"v").commit().unwrap().seqno;
        let commit = direct
            .next(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .clone();
        let token = Bytes::copy_from_slice(direct.token().as_bytes());
        hub.push(commit, token, direct.position().seqno(), 1 << 20).unwrap();
        assert_eq!(seqnos(&mut c, Duration::from_secs(1)).await, [n]);
        drop((c, hub));
    });
    drop(t);
    drop(rt);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_shared_entry_is_the_exact_encoding_of_the_event_a_client_with_no_prefix_covers() {
    // simp724's T1 on #1541: byte for byte, built here on its own, not through the
    // entry's helpers.
    let (commits, db, dir) = commits("encoded", 3);
    for c in &commits {
        let token = Bytes::from_static(b"the token right after it");
        let e = Arc::new(Entry::new(c, token.clone()).unwrap());
        let want = prost::Message::encode_to_vec(&proto::FeedEvent {
            token: token.clone(),
            event: Some(proto::feed_event::Event::Commit(proto::FeedCommit {
                seqno: c.seqno(),
                timestamp: c.timestamp(),
                changes: c
                    .changes()
                    .iter()
                    .map(|ch| proto::Change {
                        table: ch.table().to_owned(),
                        family: ch.family().to_owned(),
                        kind: change_kind(ch.kind()) as i32,
                        row: ch.row().to_vec().into(),
                        qualifier: ch.qualifier().to_vec().into(),
                        timestamp: ch.timestamp(),
                        value: ch.value().map(convert::value_out),
                    })
                    .collect(),
            })),
            partition: String::new(),
            xid: Default::default(),
            replayed: None,
        });
        assert_eq!(e.encoded().map(|b| &b[..]), Some(&want[..]));
        let mut wire = Vec::new();
        Out::Shared(Arc::clone(&e)).encode(&mut wire).unwrap();
        assert_eq!(wire, want, "the gRPC codec writes those bytes");
        assert_eq!(Out::Shared(Arc::clone(&e)).encoded_len(), want.len());
        // Counted in the ring's budget (R1, P1): the encoding, which holds the token,
        // and the fixed overhead. The token is a slice of the encoding, not a copy.
        assert_eq!(e.bytes, ENTRY_OVERHEAD + want.len());
        assert_eq!(e.token, token);
        let enc = e.encoded().unwrap();
        assert!(
            enc.as_ptr() <= e.token.as_ptr()
                && e.token.as_ptr() as usize + e.token.len() <= enc.as_ptr() as usize + enc.len(),
            "the token is held once, inside the encoding"
        );
        // A prefix client gets the changes to its rows, as a filter of the commit
        // would give them.
        for prefix in [&b""[..], b"r", b"zz"] {
            let got = e.for_prefix(prefix);
            let want = filtered(c, prefix).map(|f| commit_event(token.clone(), f, b"").unwrap());
            assert_eq!(got, want, "prefix {prefix:?}");
        }
        let decoded = <proto::FeedEvent as prost::Message>::decode(&want[..]).unwrap();
        assert_eq!(
            Out::Shared(e).into_proto(),
            decoded,
            "and HTTP sees that event"
        );
    }
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// #1780: a prefix client reads which rows an entry's changes are to off its encoding.
/// Whatever the rows, it sends the bytes the decode-and-filter path would send: the
/// shared encoding itself when every change matches, nothing when none does. Random
/// entries, seeded (`PHDB_ROWS_SEED`; the seed prints on failure), some with fields the
/// walk doesn't know; and any truncated encoding goes to the decode, never skipped.
#[test]
fn a_prefix_client_sends_what_the_decode_would_whatever_the_rows() {
    use prost::encoding::{WireType, encode_key, encode_varint};
    let seed: u64 = std::env::var("PHDB_ROWS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x1780);
    let mut x = seed | 1;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    // Short rows and prefixes over two letters, so they match, half match and miss.
    fn word(next: &mut dyn FnMut() -> u64, min: u64) -> Vec<u8> {
        let n = min + next() % (4 - min);
        (0..n).map(|_| b"ab"[(next() % 2) as usize]).collect()
    }
    // The decode's verdict on an encoding: whether the prefix keeps all, none or some of
    // its commit's changes. `None` if it doesn't decode as a commit.
    fn judge(event: &[u8], prefix: &[u8]) -> Option<Rows> {
        let ev = <proto::FeedEvent as prost::Message>::decode(event).ok()?;
        let Some(proto::feed_event::Event::Commit(c)) = ev.event else {
            return None;
        };
        let kept = c
            .changes
            .iter()
            .filter(|ch| ch.row.starts_with(prefix))
            .count();
        Some(match kept {
            0 => Rows::None,
            k if k == c.changes.len() => Rows::All,
            _ => Rows::Mixed,
        })
    }
    let (mut seen_all, mut seen_none, mut seen_mixed) = (0, 0, 0);
    for step in 0..5000 {
        let unknown = next() % 4 == 0;
        // Now and then a shape the walk leaves to the decode, or steps over (feed1248's N2
        // on #1783): 0, another event after the commit; 1, an unknown field between
        // changes; 2, a change field with a varint's wire type; 3, a key past u32::MAX.
        let odd = match next() % 16 {
            k @ 0..=3 => Some(k),
            _ => None,
        };
        let token: Vec<u8> = (0..next() % 24).map(|_| next() as u8).collect();
        let mut commit = Vec::new();
        encode_varint(1 << 3, &mut commit);
        encode_varint(next() % 1_000_000, &mut commit);
        encode_varint(2 << 3, &mut commit);
        encode_varint(next(), &mut commit);
        for _ in 0..1 + next() % 4 {
            let change = proto::Change {
                table: "busy".into(),
                family: "f".into(),
                kind: (1 + next() % 5) as i32,
                row: word(&mut next, 0).into(),
                qualifier: word(&mut next, 0).into(),
                timestamp: next() % 4,
                value: (next() % 2 == 0).then(|| proto::Value {
                    kind: Some(proto::value::Kind::Bytes(word(&mut next, 0).into())),
                }),
            };
            let mut c = prost::Message::encode_to_vec(&change);
            if unknown {
                // Fields 9-12, one of each wire type the walk steps over.
                encode_key(9, WireType::Varint, &mut c);
                encode_varint(next(), &mut c);
                encode_key(10, WireType::LengthDelimited, &mut c);
                encode_varint(2, &mut c);
                c.extend_from_slice(b"zz");
                encode_key(11, WireType::ThirtyTwoBit, &mut c);
                c.extend_from_slice(&[1, 2, 3, 4]);
                encode_key(12, WireType::SixtyFourBit, &mut c);
                c.extend_from_slice(&[0; 8]);
                // A second `row`: the decode takes the last one, so the walk must too.
                if next() % 2 == 0 {
                    let row = word(&mut next, 0);
                    encode_key(4, WireType::LengthDelimited, &mut c);
                    encode_varint(row.len() as u64, &mut c);
                    c.extend_from_slice(&row);
                }
            }
            encode_key(3, WireType::LengthDelimited, &mut commit);
            encode_varint(c.len() as u64, &mut commit);
            commit.extend_from_slice(&c);
            if odd == Some(1) {
                encode_key(9, WireType::LengthDelimited, &mut commit);
                encode_varint(1, &mut commit);
                commit.push(b'x');
            }
        }
        if odd == Some(2) {
            encode_key(3, WireType::Varint, &mut commit);
            encode_varint(7, &mut commit);
        }
        let mut event = Vec::new();
        if odd == Some(3) {
            encode_varint(1 << 35, &mut event);
            encode_varint(1, &mut event);
        }
        if !token.is_empty() {
            encode_key(1, WireType::LengthDelimited, &mut event);
            encode_varint(token.len() as u64, &mut event);
            event.extend_from_slice(&token);
        }
        encode_key(2, WireType::LengthDelimited, &mut event);
        encode_varint(commit.len() as u64, &mut event);
        event.extend_from_slice(&commit);
        if unknown {
            // A field `FeedEvent` doesn't have (9 became D331's raw commit).
            encode_key(14, WireType::Varint, &mut event);
            encode_varint(next(), &mut event);
        }
        if odd == Some(0) {
            // Another of the oneof's events (progress, partition state, D331's move
            // barrier or raw commit): the oneof's last field wins.
            encode_key(
                [4, 6, 8, 9][(next() % 4) as usize],
                WireType::LengthDelimited,
                &mut event,
            );
            encode_varint(0, &mut event);
        }
        let event = Bytes::from(event);
        let e = Arc::new(Entry {
            seqno: step,
            token: Bytes::new(),
            prev: 0,
            bytes: 0,
            encoded: Some(event.clone()),
        });
        // Mostly one letter, so every case comes up often.
        let prefix = if next() % 4 == 0 {
            word(&mut next, 1)
        } else {
            vec![b"ab"[(next() % 2) as usize]]
        };
        let why = format!("seed {seed}, step {step}, prefix {prefix:?}");
        // The decode path: the matching changes, or none (an encoding that doesn't
        // decode, or isn't a commit, sends nothing).
        let all_changes = <proto::FeedEvent as prost::Message>::decode(event.clone())
            .ok()
            .and_then(|ev| match ev.event {
                Some(proto::feed_event::Event::Commit(c)) => Some(c.changes.len()),
                _ => None,
            });
        let decoded = e.for_prefix(&prefix);
        let want = match &decoded {
            None => Rows::None,
            Some(ev) => match &ev.event {
                Some(proto::feed_event::Event::Commit(c))
                    if Some(c.changes.len()) == all_changes =>
                {
                    Rows::All
                }
                _ => Rows::Mixed,
            },
        };
        // The walk's verdict is the decode's, except that a shape it leaves to the decode
        // is `Mixed`.
        let r = rows(&event, &prefix);
        if matches!(odd, Some(0 | 2 | 3)) {
            assert!(r == Rows::Mixed || r == want, "{why}: {r:?}, odd {odd:?}");
        } else {
            assert_eq!(r, want, "{why}");
        }
        let got = prefix_out(&e, &prefix);
        match r {
            Rows::All => {
                seen_all += 1;
                assert!(matches!(got, Some(Out::Shared(_))), "{why}");
            }
            Rows::None => {
                seen_none += 1;
                assert!(got.is_none(), "{why}");
            }
            Rows::Mixed => {
                seen_mixed += 1;
                assert!(got.is_none() || matches!(got, Some(Out::Event(_))), "{why}");
            }
        }
        // The same bytes as the decode path's event (fields the encoder doesn't know
        // aren't encoded again, so only without them).
        if !unknown && odd != Some(1) {
            assert_eq!(
                got.map(|o| o.to_bytes()),
                decoded.map(|ev| Bytes::from(prost::Message::encode_to_vec(&ev))),
                "{why}"
            );
        }
        // Cut short anywhere: what the decode makes of it, or, if it can't, for it to judge.
        // Only a cut between whole fields decodes (one before a trailing unknown field).
        let cut = (next() % event.len() as u64) as usize;
        let got = rows(&event[..cut], &prefix);
        match judge(&event[..cut], &prefix) {
            Some(want) => assert_eq!(got, want, "{why}, cut at {cut}"),
            None => assert_eq!(got, Rows::Mixed, "{why}, cut at {cut}"),
        }
    }
    assert!(
        seen_all > 250 && seen_none > 250 && seen_mixed > 250,
        "seed {seed}: all {seen_all}, none {seen_none}, mixed {seen_mixed}"
    );
}

#[test]
fn a_client_rejoins_only_where_the_ring_covers_it_and_has_passed_it() {
    let (c, db, dir) = commits("ring", 3);
    let (a, b, z) = (c[0].seqno(), c[1].seqno(), c[2].seqno());
    let start = a - 1;
    let r = ring(start, &c[..2], 0);
    assert_eq!(
        r.rejoin_at(start),
        Some(0),
        "at the start: from the first entry"
    );
    assert_eq!(r.rejoin_at(a), Some(1));
    assert_eq!(r.rejoin_at(b), Some(2), "at the last entry: the head");
    // A client whose own stream is ahead of the hub (it has z, which the hub hasn't
    // added yet) must not rejoin: z would come again.
    assert_eq!(r.rejoin_at(z), None);
    // Nor one behind what the ring still covers: the evicted commits would be missed.
    let r = ring(start, &c[..3], 2);
    assert_eq!(r.rejoin_at(a), None);
    assert_eq!(r.rejoin_at(b), Some(2));
    assert_eq!(r.rejoin_at(z), Some(3));
    // A hub with no commit yet covers from its start.
    let r = ring(z, &[], 0);
    assert_eq!(r.rejoin_at(z), Some(0));
    assert_eq!(r.rejoin_at(b), None);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
