//! Replication between servers (#1436, ICR 0112's amendment).
//!
//! **The primary's side** is `Replication.Follow`: the primary's durable feed of every table
//! from a replica's token, as batches of encoded events (`pigeonhole`'s hidden
//! `ReplicatedEvent` encoding, opaque here). Each follower reads on a thread of its own, under
//! a `--max-streams` slot, so a replica catching up never holds a runtime worker.
//!
//! **The replica's side** is `phdb serve --replica-of`: a task that bootstraps the database
//! from the primary's `Admin.BackupDownload` (with `replica`), then follows it. Events are
//! applied in runs: a run is applied when a catalog change arrives (it applies on its own,
//! between runs), when it reaches `--replica-run` events or [`RUN_BYTES`], or when no further
//! message is ready, so a caught-up replica applies each message at once and a loaded one
//! amortizes one `GroupSync` over a run. Each apply stores the primary's token with what it
//! applied, so a reconnect, a restart or an apply error resumes exactly where the last apply
//! ended. A token the primary no longer keeps, or another database at the URL, bootstraps
//! again into a side file while the old copy keeps serving, then swaps it in.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use pigeonhole::replication::ReplicatedEvent;
use pigeonhole::{FeedToken, Pigeonhole};
use sha2::Digest;
use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};

use super::Shared;
use super::errors::ApiError;
use super::proto;
use super::service::{FOLLOW, Metrics};

/// A `Follow` message's events, at most.
const FOLLOW_EVENTS: usize = 256;

/// A `Follow` message's bytes, about: a message is cut once its events pass this.
const FOLLOW_BYTES: usize = 1 << 20;

/// How long a quiet `Follow` waits before it sends a message with no events, so the replica
/// sees the primary's frontier move and knows it is caught up.
const QUIET: Duration = Duration::from_secs(1);

/// A replica's run, in bytes at most (with `--replica-run` events at most).
pub(crate) const RUN_BYTES: usize = 8 << 20;

/// The default `--replica-run`.
pub(crate) const RUN_EVENTS: usize = 256;

/// The longest pause between a replica's attempts after an error.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// How long a follow must run before the pause after the next error starts short again.
const STEADY: Duration = Duration::from_secs(30);

// ---- The primary's side: `Replication.Follow` ----

/// A `Follow` stream: batches as the follower's thread reads them, then its error if it
/// failed.
#[derive(Debug)]
pub(crate) struct FollowBatches {
    rx: mpsc::Receiver<Result<proto::ReplicationBatch, ApiError>>,
}

impl Stream for FollowBatches {
    type Item = Result<proto::ReplicationBatch, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Opens the primary's durable feed of every table from `token` and starts its follower's
/// thread. The thread holds `slot` until the stream ends: the replica went away, took no
/// message for `idle`, or the feed failed (its error is the stream's last item).
pub(crate) fn follow(
    db: &Pigeonhole,
    token: Vec<u8>,
    slot: OwnedSemaphorePermit,
    idle: Duration,
    metrics: Arc<Metrics>,
) -> Result<FollowBatches, ApiError> {
    if token.is_empty() {
        return Err(ApiError::invalid(
            "Follow needs the token of a replica's bootstrap or of its last apply",
        ));
    }
    let feed = db
        .feed()
        .resume(&FeedToken::from_bytes(token))
        .open()
        .map_err(|e| ApiError::from_engine(&e).for_read())?;
    let (tx, rx) = mpsc::channel(2);
    let db = db.clone();
    std::thread::Builder::new()
        .name("phdb-follow".into())
        .spawn(move || {
            let started = Instant::now();
            let outcome = pump(&db, feed, &tx, idle);
            metrics.count(
                FOLLOW,
                outcome.as_ref().err().map(|e| e.code.as_str()),
                started.elapsed(),
            );
            if let Err(e) = outcome {
                let _ = tx.try_send(Err(e));
            }
            drop(slot);
        })
        .map_err(|e| ApiError::internal(format!("starting a follower's thread: {e}")))?;
    Ok(FollowBatches { rx })
}

/// The follower's loop: a message per batch of events, or per [`QUIET`] with none.
fn pump(
    db: &Pigeonhole,
    mut feed: pigeonhole::Feed,
    tx: &mpsc::Sender<Result<proto::ReplicationBatch, ApiError>>,
    idle: Duration,
) -> Result<(), ApiError> {
    let engine = |e: pigeonhole::Error| ApiError::from_engine(&e).for_read();
    loop {
        let mut events = Vec::new();
        let mut bytes = 0;
        let mut caught_up = false;
        if feed.next_event(QUIET).map_err(engine)?.is_some() {
            loop {
                let event = feed
                    .replicated()
                    .ok_or_else(|| ApiError::internal("a delivered event has no replicated form"))?
                    .encode();
                bytes += event.len();
                events.push(event.into());
                if events.len() >= FOLLOW_EVENTS || bytes >= FOLLOW_BYTES {
                    break;
                }
                if feed.try_next_event().map_err(engine)?.is_none() {
                    caught_up = true;
                    break;
                }
            }
        } else {
            caught_up = true;
        }
        let batch = proto::ReplicationBatch {
            events,
            primary_seqno: db.visible_seqno(),
            caught_up,
        };
        if !send_within(tx, batch, idle)? {
            // The replica went away.
            return Ok(());
        }
    }
}

/// How long a follower waits between tries while its replica's buffer is full.
const SEND_RETRY: Duration = Duration::from_millis(5);

/// Sends `batch`, waiting at most `idle` for room: `false` if the replica went away,
/// `DeadlineExceeded` if it took nothing for `idle`. On this thread alone, with no runtime
/// call, so it holds while the runtime shuts down (cli692 on #1583: a tokio timer from
/// `Handle::block_on` panics then). It polls only while the replica is slower than the feed.
fn send_within(
    tx: &mpsc::Sender<Result<proto::ReplicationBatch, ApiError>>,
    batch: proto::ReplicationBatch,
    idle: Duration,
) -> Result<bool, ApiError> {
    let deadline = Instant::now() + idle;
    let mut item = Ok(batch);
    loop {
        match tx.try_send(item) {
            Ok(()) => return Ok(true),
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
            Err(mpsc::error::TrySendError::Full(back)) => {
                if Instant::now() >= deadline {
                    return Err(ApiError::deadline(
                        "the replica took no message for --stream-idle-ms; it resumes from its \
                         token",
                    ));
                }
                item = back;
                std::thread::sleep(SEND_RETRY);
            }
        }
    }
}

// ---- The replica's side: `phdb serve --replica-of` ----

/// `--replica-of` and its settings.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaConfig {
    /// The primary's URL, `http://` or `https://`.
    pub primary: String,
    /// The bearer token the replica sends the primary (feed and admin scope there).
    pub token: Option<String>,
    /// A run's events, at most (`--replica-run`).
    pub run_events: usize,
}

/// What `Admin.Info` and `/metrics` report.
#[derive(Debug)]
struct Status {
    state: &'static str,
    applied: u64,
    primary_seqno: u64,
    caught_up: bool,
    /// Since when the replica hasn't been caught up (when not).
    behind_since: Instant,
    last_error: String,
}

/// A replica server's loop and its state.
#[derive(Debug)]
pub(crate) struct Replica {
    config: ReplicaConfig,
    status: Mutex<Status>,
    /// Set once `Admin.Promote` made the database a primary: writes are taken, and the
    /// replica reports nothing.
    promoted: AtomicBool,
    stop: watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Why following stopped.
enum Halt {
    /// `Admin.Promote` or shutdown.
    Stopped,
    /// The primary no longer keeps the replica's position, or is another database: bootstrap
    /// again.
    Expired(String),
    /// Anything else: retry from the stored token after a pause.
    Failed(String),
}

impl Replica {
    pub(crate) fn new(config: ReplicaConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            status: Mutex::new(Status {
                state: "starting",
                applied: 0,
                primary_seqno: 0,
                caught_up: false,
                behind_since: Instant::now(),
                last_error: String::new(),
            }),
            promoted: AtomicBool::new(false),
            stop: watch::channel(false).0,
            task: Mutex::new(None),
        })
    }

    /// The primary's URL.
    pub(crate) fn primary(&self) -> &str {
        &self.config.primary
    }

    /// Whether the server refuses writes: a replica until it is promoted.
    pub(crate) fn refuses_writes(&self) -> bool {
        !self.promoted.load(Ordering::Acquire)
    }

    fn status(&self) -> std::sync::MutexGuard<'_, Status> {
        self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_state(&self, state: &'static str) {
        self.status().state = state;
    }

    fn fail(&self, state: &'static str, error: String) {
        let mut s = self.status();
        s.state = state;
        s.last_error = error;
    }

    /// `Admin.Info`'s section: `None` once promoted.
    pub(crate) fn info(&self) -> Option<proto::ReplicaInfo> {
        if !self.refuses_writes() {
            return None;
        }
        let s = self.status();
        Some(proto::ReplicaInfo {
            primary: self.config.primary.clone(),
            state: s.state.to_owned(),
            applied_primary_seqno: s.applied,
            primary_seqno: s.primary_seqno,
            lag_commits: s.primary_seqno.saturating_sub(s.applied),
            lag_ms: if s.caught_up {
                0
            } else {
                u64::try_from(s.behind_since.elapsed().as_millis()).unwrap_or(u64::MAX)
            },
            last_error: s.last_error.clone(),
        })
    }

    /// The `/metrics` gauges.
    pub(crate) fn render(&self, out: &mut String) {
        use std::fmt::Write;
        let Some(info) = self.info() else { return };
        let following = u8::from(info.state == "following");
        let _ = writeln!(out, "# TYPE pigeonhole_replica_following gauge");
        let _ = writeln!(out, "pigeonhole_replica_following {following}");
        let _ = writeln!(out, "# TYPE pigeonhole_replica_applied_primary_seqno gauge");
        let _ = writeln!(
            out,
            "pigeonhole_replica_applied_primary_seqno {}",
            info.applied_primary_seqno
        );
        let _ = writeln!(out, "# TYPE pigeonhole_replica_lag_commits gauge");
        let _ = writeln!(out, "pigeonhole_replica_lag_commits {}", info.lag_commits);
        let _ = writeln!(out, "# TYPE pigeonhole_replica_lag_seconds gauge");
        let _ = writeln!(
            out,
            "pigeonhole_replica_lag_seconds {}",
            info.lag_ms as f64 / 1000.0
        );
    }

    /// Starts the loop on the current runtime.
    pub(crate) fn spawn(self: &Arc<Self>, shared: Arc<Shared>) {
        let me = Arc::clone(self);
        let task = tokio::spawn(async move { me.run(&shared).await });
        *self.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
    }

    /// Stops the loop and waits for it: an apply in flight finishes, and what arrived before
    /// the stop is applied.
    pub(crate) async fn stop(&self) {
        self.stop.send_replace(true);
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// `Admin.Promote`: stops the loop (an apply in flight finishes first), then makes the
    /// database a primary at the last event applied. Returns the primary's token stored with
    /// it.
    pub(crate) async fn promote(&self, db: &Pigeonhole) -> Result<Vec<u8>, ApiError> {
        if !self.refuses_writes() {
            return Err(ApiError::failed_precondition(
                "this server was already promoted",
            ));
        }
        self.stop().await;
        let db = db.clone();
        let token = tokio::task::spawn_blocking(move || {
            if db.replicated_seqno()?.is_none() {
                return Ok(None);
            }
            db.promote().map(Some)
        })
        .await
        .map_err(|e| ApiError::internal(format!("promote: {e}")))??;
        let Some(token) = token else {
            return Err(ApiError::failed_precondition(
                "the replica hasn't finished its bootstrap: there is nothing to promote",
            ));
        };
        self.promoted.store(true, Ordering::Release);
        self.set_state("promoted");
        Ok(token.map(|t| t.as_bytes().to_vec()).unwrap_or_default())
    }

    /// The loop: follow; after an error, pause (doubling, up to [`MAX_BACKOFF`]) and go on
    /// from the stored token; on an expiry, bootstrap again. The pause resets once a follow
    /// has run for [`STEADY`], so an expiry right after a bootstrap (a primary that can't
    /// serve the token it just gave) pauses too rather than downloading back to back.
    async fn run(&self, shared: &Shared) {
        let mut stop = self.stop.subscribe();
        let mut backoff = Duration::from_millis(100);
        loop {
            if *stop.borrow() {
                return;
            }
            let began = Instant::now();
            let halt = self.follow(shared, &mut stop).await;
            if began.elapsed() >= STEADY {
                backoff = Duration::from_millis(100);
            }
            let failed = match halt {
                Halt::Stopped => return,
                Halt::Expired(why) => {
                    self.fail("re-bootstrapping", why);
                    match self.bootstrap(shared, &mut stop).await {
                        Ok(()) => None,
                        Err(Halt::Stopped) => return,
                        Err(Halt::Expired(e) | Halt::Failed(e)) => Some(e),
                    }
                }
                Halt::Failed(e) => Some(e),
            };
            if let Some(e) = failed {
                self.fail("retrying", e);
            }
            tokio::select! {
                () = tokio::time::sleep(backoff) => {}
                _ = stop.changed() => return,
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// A channel to the primary.
    async fn connect(&self) -> Result<tonic::transport::Channel, String> {
        let mut endpoint = tonic::transport::Endpoint::from_shared(self.config.primary.clone())
            .map_err(|e| format!("--replica-of {}: {e}", self.config.primary))?
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30));
        if self.config.primary.starts_with("https://") {
            endpoint = endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
                .map_err(|e| format!("TLS to {}: {e}", self.config.primary))?;
        }
        endpoint
            .connect()
            .await
            .map_err(|e| format!("connecting to {}: {e}", self.config.primary))
    }

    /// `req` with the replica's bearer token.
    fn request<T>(&self, message: T) -> Result<tonic::Request<T>, String> {
        let mut req = tonic::Request::new(message);
        if let Some(token) = &self.config.token {
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| "the replica's token isn't a valid header value".to_owned())?;
            req.metadata_mut().insert("authorization", value);
        }
        Ok(req)
    }

    /// Follows the primary from the stored token (bootstrapping first if there is none)
    /// until it stops.
    async fn follow(&self, shared: &Shared, stop: &mut watch::Receiver<bool>) -> Halt {
        let db = shared.router.service.db.clone();
        let token = loop {
            let stored = {
                let db = db.clone();
                tokio::task::spawn_blocking(move || {
                    Ok::<_, pigeonhole::Error>((db.replicated_seqno()?, db.replicated_token()?))
                })
                .await
            };
            match stored {
                Ok(Ok((Some(applied), Some(token)))) => {
                    self.status().applied = applied;
                    break token;
                }
                // Not a replica yet: the first bootstrap, then follow from its token. (A
                // replica with no token can't come from `--replica-of`: its bootstrap stores
                // one.)
                // `phdb serve` checked at start that the file was empty or a replica's.
                Ok(Ok(_)) => {
                    self.set_state("bootstrapping");
                    if let Err(halt) = self.bootstrap(shared, stop).await {
                        return halt;
                    }
                }
                Ok(Err(e)) => return Halt::Failed(format!("reading the replica's state: {e}")),
                Err(e) => return Halt::Failed(format!("reading the replica's state: {e}")),
            }
        };
        let channel = match self.connect().await {
            Ok(c) => c,
            Err(e) => return Halt::Failed(e),
        };
        let mut client = proto::replication_client::ReplicationClient::new(channel)
            .max_decoding_message_size(usize::MAX);
        let req = match self.request(proto::FollowRequest {
            token: token.as_bytes().to_vec().into(),
        }) {
            Ok(r) => r,
            Err(e) => return Halt::Failed(e),
        };
        let opened = tokio::select! {
            r = client.follow(req) => r,
            _ = stop.changed() => return Halt::Stopped,
        };
        let mut stream = match opened {
            Ok(r) => r.into_inner(),
            Err(status) => return halt_of(&status),
        };
        self.set_state("following");
        let mut run = Run::default();
        let mut peeked = None;
        loop {
            let next = match peeked.take() {
                Some(m) => m,
                None => tokio::select! {
                    m = stream.message() => m,
                    _ = stop.changed() => {
                        // Promote or shutdown: what arrived is applied first.
                        return match self.apply_run(&db, &mut run).await {
                            Ok(()) => Halt::Stopped,
                            Err(e) => Halt::Failed(e),
                        };
                    }
                },
            };
            let batch = match next {
                Ok(Some(b)) => b,
                Ok(None) => {
                    return Halt::Failed(format!("{} ended the stream", self.config.primary));
                }
                Err(status) => return halt_of(&status),
            };
            {
                let mut s = self.status();
                s.primary_seqno = s.primary_seqno.max(batch.primary_seqno);
            }
            let quiet = batch.events.is_empty();
            for bytes in &batch.events {
                let event = match ReplicatedEvent::decode(bytes) {
                    Ok(e) => e,
                    Err(e) => return Halt::Failed(format!("a replicated event: {e}")),
                };
                if event.is_catalog() {
                    // D315: a catalog change applies on its own, between runs.
                    if let Err(e) = self.apply_run(&db, &mut run).await {
                        return Halt::Failed(e);
                    }
                    if let Err(e) = self.apply_catalog(shared, event).await {
                        return Halt::Failed(e);
                    }
                    continue;
                }
                run.bytes += bytes.len();
                run.events.push(event);
                if (run.events.len() >= self.config.run_events.max(1) || run.bytes >= RUN_BYTES)
                    && let Err(e) = self.apply_run(&db, &mut run).await
                {
                    return Halt::Failed(e);
                }
            }
            // The latency bound: no further message ready, so what arrived is applied now.
            if !run.events.is_empty() {
                peeked = ready(&mut stream);
                if peeked.is_none()
                    && let Err(e) = self.apply_run(&db, &mut run).await
                {
                    return Halt::Failed(e);
                }
            }
            let mut s = self.status();
            let caught_up = batch.caught_up && run.events.is_empty() && peeked.is_none();
            if caught_up {
                s.caught_up = true;
            } else if s.caught_up && !quiet {
                s.caught_up = false;
                s.behind_since = Instant::now();
            }
            if !s.last_error.is_empty() && s.state == "following" {
                s.last_error.clear();
            }
        }
    }

    /// Applies `run` (if any) in one call, with its last event's token.
    async fn apply_run(&self, db: &Pigeonhole, run: &mut Run) -> Result<(), String> {
        let Some(last) = run.events.last() else {
            return Ok(());
        };
        let applied = last.primary_seqno();
        let events = std::mem::take(&mut run.events);
        run.bytes = 0;
        let db = db.clone();
        tokio::task::spawn_blocking(move || {
            let token = events.last().map(|e| e.token().clone());
            match token {
                Some(t) => db.apply_replicated(&events, &t),
                None => Ok(()),
            }
        })
        .await
        .map_err(|e| format!("applying a run: {e}"))?
        .map_err(|e| format!("applying a run: {e}"))?;
        self.status().applied = applied;
        Ok(())
    }

    /// Applies a catalog change, then drops the service's cached table handles: a table
    /// dropped (or dropped and made again) on the primary must not serve from its old one.
    async fn apply_catalog(&self, shared: &Shared, event: ReplicatedEvent) -> Result<(), String> {
        let applied = event.primary_seqno();
        let db = shared.router.service.db.clone();
        let outcome = tokio::task::spawn_blocking(move || db.apply_catalog(&event, event.token()))
            .await
            .map_err(|e| format!("applying a catalog change: {e}"));
        shared.router.service.tables_changed();
        outcome?.map_err(|e| format!("applying a catalog change: {e}"))?;
        let mut s = self.status();
        s.applied = s.applied.max(applied);
        Ok(())
    }

    /// Downloads a bootstrap copy into a side file, makes it a replica at the copy's token,
    /// and swaps it in for the served database. The old copy serves until the swap.
    async fn bootstrap(
        &self,
        shared: &Shared,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<(), Halt> {
        let service = &shared.router.service;
        let db = service.db.clone();
        let side = side_path(db.path());
        let token = tokio::select! {
            r = self.download(&side) => r.map_err(Halt::Failed)?,
            _ = stop.changed() => return Err(Halt::Stopped),
        };
        let swap_side = side.clone();
        let swapped = tokio::task::spawn_blocking(move || {
            let copy = Pigeonhole::open(&swap_side, pigeonhole::Options::default())?;
            copy.begin_replication(&FeedToken::from_bytes(token))?;
            copy.close()?;
            db.replace_with(&swap_side)
        })
        .await
        .map_err(|e| Halt::Failed(format!("bootstrap: {e}")))?;
        // The side file is the database now, or the bootstrap failed: either way it goes.
        let _ = std::fs::remove_file(&side);
        // The served handle is on the new engine (or closed, if the swap failed late):
        // nothing cached of the old one may serve again.
        service.engine_replaced();
        swapped.map_err(|e| Halt::Failed(format!("bootstrap: {e}")))?;
        let mut s = self.status();
        s.applied = 0;
        s.caught_up = false;
        s.behind_since = Instant::now();
        s.last_error.clear();
        Ok(())
    }

    /// `Admin.BackupDownload` with `replica` into `side`, resumed after a broken stream,
    /// checked against its SHA-256. Returns the copy's feed token.
    async fn download(&self, side: &Path) -> Result<Vec<u8>, String> {
        use tokio::io::AsyncWriteExt;
        let mut client = proto::admin_client::AdminClient::new(self.connect().await?)
            .max_decoding_message_size(usize::MAX);
        let mut file = tokio::fs::File::create(side)
            .await
            .map_err(|e| format!("creating {}: {e}", side.display()))?;
        let mut hash = sha2::Sha256::new();
        let (mut backup_id, mut offset, mut token) = (String::new(), 0u64, Vec::new());
        let mut tries = 0;
        loop {
            let req = self.request(proto::BackupDownloadRequest {
                durable: true,
                backup_id: backup_id.clone(),
                offset,
                replica: true,
            })?;
            let broke = match client.backup_download(req).await {
                Ok(r) => {
                    let mut stream = r.into_inner();
                    loop {
                        match stream.message().await {
                            Ok(Some(chunk)) => {
                                if chunk.offset != offset {
                                    return Err(format!(
                                        "the bootstrap copy's chunk is at {}, expected {offset}",
                                        chunk.offset
                                    ));
                                }
                                backup_id = chunk.backup_id;
                                if token.is_empty() {
                                    token = chunk.feed_token.to_vec();
                                }
                                file.write_all(&chunk.data)
                                    .await
                                    .map_err(|e| format!("writing {}: {e}", side.display()))?;
                                hash.update(&chunk.data);
                                offset += chunk.data.len() as u64;
                                if !chunk.sha256.is_empty() {
                                    if hash.finalize().as_slice() != chunk.sha256.as_ref() {
                                        return Err(
                                            "the bootstrap copy's SHA-256 doesn't match".to_owned()
                                        );
                                    }
                                    file.sync_all()
                                        .await
                                        .map_err(|e| format!("syncing {}: {e}", side.display()))?;
                                    if token.is_empty() {
                                        return Err(format!(
                                            "{} sent no feed token with its bootstrap copy",
                                            self.config.primary
                                        ));
                                    }
                                    return Ok(token);
                                }
                            }
                            Ok(None) => break "the stream ended before the last chunk".to_owned(),
                            Err(status) => break status.message().to_owned(),
                        }
                    }
                }
                Err(status) if backup_id.is_empty() => {
                    return Err(format!(
                        "bootstrap from {}: {}",
                        self.config.primary,
                        status.message()
                    ));
                }
                Err(status) => status.message().to_owned(),
            };
            tries += 1;
            if backup_id.is_empty() || tries > 3 {
                return Err(format!("downloading the bootstrap copy: {broke}"));
            }
            tokio::time::sleep(Duration::from_millis(200 * tries)).await;
        }
    }
}

/// A run being gathered.
#[derive(Default)]
struct Run {
    events: Vec<ReplicatedEvent>,
    bytes: usize,
}

/// The next message if one is ready now, without waiting.
fn ready(
    stream: &mut tonic::Streaming<proto::ReplicationBatch>,
) -> Option<Result<Option<proto::ReplicationBatch>, tonic::Status>> {
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    match Pin::new(stream).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(b))) => Some(Ok(Some(b))),
        Poll::Ready(Some(Err(e))) => Some(Err(e)),
        Poll::Ready(None) => Some(Ok(None)),
        Poll::Pending => None,
    }
}

/// What a `Follow` error means for the replica.
fn halt_of(status: &tonic::Status) -> Halt {
    match status.code() {
        // FeedExpired; or the token is another database's (the primary at the URL changed).
        tonic::Code::OutOfRange | tonic::Code::InvalidArgument => {
            Halt::Expired(status.message().to_owned())
        }
        _ => Halt::Failed(status.message().to_owned()),
    }
}

/// The bootstrap copy's side file, beside the database.
fn side_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".bootstrap");
    path.with_file_name(name)
}
