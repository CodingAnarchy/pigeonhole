//! The serving pattern end to end (#1397 step 1): a real server on a loopback port and
//! workers in threads, over HTTP.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pigeonhole_example_serving::{App, Client, ClientError, Config, serve};

/// A running server: its URL, and what stops it.
struct Server {
    url: String,
    dir: PathBuf,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn start(lease_ttl: Duration, token: Option<&str>) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // Tests run in parallel and can start in the same clock tick: a counter keeps each
        // server's directory its own.
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("phdb-serving-{}-{nanos}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = Config {
            path: dir.join("jobs.phdb"),
            lease_ttl,
            token: token.map(str::to_owned),
        };
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (ready, url) = std::sync::mpsc::channel();
        let thread = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let app = App::open(&config).unwrap();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready
                    .send(format!("http://{}", listener.local_addr().unwrap()))
                    .unwrap();
                serve(listener, Arc::clone(&app), async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
                tokio::task::spawn_blocking(move || app.close())
                    .await
                    .unwrap()
                    .unwrap();
            });
        });
        let url = url.recv().unwrap();
        Self {
            url,
            dir,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn client(&self) -> Client {
        Client::new(self.url.clone(), None)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn every_job_is_done_once_by_concurrent_workers() {
    let server = Server::start(Duration::from_secs(30), None);
    let client = server.client();
    let ids: Vec<String> = (0..60).map(|i| format!("job:{i:03}")).collect();
    for id in &ids {
        client.create_job(id, "spec").unwrap();
    }
    // A second create of the same id is refused, so a retried create can't queue it twice.
    let again = client.create_job(&ids[0], "spec").unwrap_err();
    assert!(
        matches!(&again, ClientError::Api { code, .. } if code == "Exists"),
        "{again}"
    );

    let workers: Vec<_> = (0..4)
        .map(|w| {
            let client = server.client();
            thread::spawn(move || {
                let name = format!("worker-{w}");
                client
                    .work_until_idle(&name, |id| format!("{id} by {name}"))
                    .unwrap()
            })
        })
        .collect();
    let done: Vec<String> = workers
        .into_iter()
        .flat_map(|w| w.join().unwrap())
        .collect();

    // Each job was completed by exactly one worker.
    let unique: BTreeSet<_> = done.iter().cloned().collect();
    assert_eq!(done.len(), ids.len(), "a job was completed twice: {done:?}");
    assert_eq!(unique, ids.iter().cloned().collect());
    // And the stored state agrees: every job is done, with no live claim. Small pages: a
    // page cut by its budget can end inside a row, and the next page starts with the rest of
    // it, so the pieces are merged by id.
    let mut jobs = std::collections::BTreeMap::<String, (Option<String>, Option<String>)>::new();
    let mut after = None;
    loop {
        let page = client.jobs(after.as_deref(), Some(256)).unwrap();
        for job in page.jobs {
            let entry = jobs.entry(job.id).or_default();
            entry.0 = entry.0.take().or(job.status);
            entry.1 = entry.1.take().or(job.holder);
        }
        match page.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(
        jobs.keys().cloned().collect::<BTreeSet<_>>(),
        unique,
        "paging lost a job"
    );
    for (id, (status, holder)) in &jobs {
        assert_eq!(status.as_deref(), Some("done"), "{id}");
        assert_eq!(holder.as_deref(), None, "{id}");
    }
}

#[test]
fn a_stalled_worker_cannot_overwrite_the_next_holders_work() {
    // Long enough that the claims and renewal below land within it on a loaded runner.
    let ttl = Duration::from_secs(3);
    let server = Server::start(ttl, None);
    let client = server.client();
    client.create_job("job:1", "spec").unwrap();

    let slow = client.claim("job:1", "slow").unwrap().expect("unclaimed");
    // While the claim lasts, nobody else gets the job, and the holder can renew it.
    assert_eq!(client.claim("job:1", "fast").unwrap(), None);
    assert!(client.renew("job:1", &slow).unwrap());

    // The slow worker stalls past its TTL. Its claim expires, and another worker takes over.
    thread::sleep(ttl * 2);
    assert!(
        !client.renew("job:1", &slow).unwrap(),
        "an expired claim can't be renewed"
    );
    let fast = client
        .claim("job:1", "fast")
        .unwrap()
        .expect("the expired claim freed the job");
    assert_ne!(fast, slow);

    // The stalled worker comes back and tries to record its result: fenced off.
    assert!(!client.complete("job:1", &slow, "stale").unwrap());
    assert!(client.complete("job:1", &fast, "fresh").unwrap());
    let job = client.job("job:1").unwrap();
    assert_eq!(job["result"], "fresh");
    assert_eq!(job["status"], "done");
    // A done job can't be claimed again.
    assert_eq!(client.claim("job:1", "late").unwrap(), None);
}

#[test]
fn transfers_are_atomic_and_idempotent_by_op_id() {
    let server = Server::start(Duration::from_secs(30), None);
    let client = server.client();
    client.open_account("alice", 100).unwrap();
    client.open_account("bob", 0).unwrap();

    assert!(client.transfer("op-1", "alice", "bob", 30, 5).unwrap());
    // The same op id again (a retry after a lost reply) moves nothing.
    assert!(!client.transfer("op-1", "alice", "bob", 30, 5).unwrap());
    assert_eq!(
        (
            client.balance("alice").unwrap(),
            client.balance("bob").unwrap()
        ),
        (70, 30)
    );

    let short = client
        .transfer("op-2", "bob", "alice", 1000, 5)
        .unwrap_err();
    assert!(
        matches!(&short, ClientError::Api { code, .. } if code == "Insufficient"),
        "{short}"
    );

    // Concurrent transfers both ways: transact retries the conflicts, and money is conserved.
    let threads: Vec<_> = (0..4)
        .map(|t| {
            let client = server.client();
            thread::spawn(move || {
                for i in 0..10 {
                    let (from, to) = if (t + i) % 2 == 0 {
                        ("alice", "bob")
                    } else {
                        ("bob", "alice")
                    };
                    // A refusal for insufficient funds is fine; anything else isn't.
                    match client.transfer(&format!("op-{t}-{i}"), from, to, 1, 5) {
                        Ok(_) => {}
                        Err(ClientError::Api { code, .. }) if code == "Insufficient" => {}
                        Err(e) => panic!("{e}"),
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(
        client.balance("alice").unwrap() + client.balance("bob").unwrap(),
        100
    );
}

#[test]
fn the_event_stream_sees_every_commit_and_resumes_after_an_event() {
    let server = Server::start(Duration::from_secs(30), None);
    let client = server.client();
    let mut events = client.events(None).unwrap();
    for i in 0..5 {
        client.create_job(&format!("job:{i}"), "spec").unwrap();
    }
    let mut ids = Vec::new();
    let mut rows = Vec::new();
    for _ in 0..5 {
        let e = events.next().expect("the stream stays open").unwrap();
        assert_eq!(e.event, "commit");
        ids.push(e.id.expect("every commit event carries its resume id"));
        rows.push(e.data["changes"][0]["row"].as_str().unwrap().to_owned());
    }
    assert_eq!(rows, (0..5).map(|i| format!("job:{i}")).collect::<Vec<_>>());

    // A reconnect with the second event's id continues exactly after it: the third commit,
    // then the rest, none skipped or repeated. (Ids are opaque: a resumed stream's ids are
    // positions too, but not necessarily the same bytes.)
    let mut resumed = client.events(Some(&ids[1])).unwrap();
    let rest: Vec<_> = (0..3)
        .map(|_| {
            resumed.next().unwrap().unwrap().data["changes"][0]["row"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(rest, ["job:2", "job:3", "job:4"]);
}

#[test]
fn a_configured_token_is_required() {
    let server = Server::start(Duration::from_secs(30), Some("s3cret"));
    let anonymous = server.client();
    let refused = anonymous.create_job("job:1", "spec").unwrap_err();
    assert!(
        matches!(&refused, ClientError::Api { status: 401, .. }),
        "{refused}"
    );
    let wrong = Client::new(server.url.clone(), Some("guess".into()));
    assert!(matches!(
        wrong.create_job("job:1", "spec"),
        Err(ClientError::Api { status: 401, .. })
    ));
    let right = Client::new(server.url.clone(), Some("s3cret".into()));
    right.create_job("job:1", "spec").unwrap();
}

#[test]
fn a_retried_claim_gets_its_own_token_back() {
    // simp724's L1 on #1491: a claim whose reply was lost is sent again with the same
    // claim id. It must get the same token back, not `Claimed` from its own lease.
    let server = Server::start(Duration::from_secs(30), None);
    let client = server.client();
    client.create_job("job:1", "spec").unwrap();
    let first = client
        .claim_with_id("job:1", "w1", "n-1")
        .unwrap()
        .expect("unclaimed");
    let retried = client.claim_with_id("job:1", "w1", "n-1").unwrap();
    assert_eq!(
        retried.as_ref(),
        Some(&first),
        "the retry is the same claim"
    );
    // A different claim (another claim id, the same worker or another) is refused.
    assert_eq!(client.claim_with_id("job:1", "w1", "n-2").unwrap(), None);
    assert_eq!(client.claim_with_id("job:1", "w2", "n-1").unwrap(), None);
    assert!(client.complete("job:1", &first, "done").unwrap());
}
