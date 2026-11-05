//! `Vectors.BulkLoad` (D313 ticket 7b2, ICR 0100): a client's stream of vectors, loaded into
//! an empty IVF index by `Index::bulk_load_*`, which pulls them from an iterator.
//!
//! perf287's design on #1397 ((A), with B1–B4):
//! - **Each message holds its share of the in-flight budget** until the loader takes its last
//!   vector: reserved from the message's gRPC header before tonic decodes it
//!   (`grpc::Metered`), or as each chunk of an NDJSON body arrives over HTTP.
//! - **One message waits between the front-end and the loader** (a channel of 1), and the
//!   front-end reads the next only once that one is taken. A loader slower than its client
//!   stops the reading, and the transport's flow control (HTTP/2 windows, TCP) then stops the
//!   client.
//! - **The loader holds a working set of its own, outside the budget:** the crate clusters a
//!   sample of the first max(50,000, expected / target list length) vectors before it writes
//!   any, then keeps up to 256 vectors per list until each list's batch is written. **One load
//!   runs at a time per server** (the slot), which bounds that to one load's.
//! - **The loader runs on a thread of its own** holding the slot, never on the blocking pool
//!   (B1: a load runs for minutes), placed off the shards' CPUs (B2). The crate's k-means
//!   runs on that thread, single-threaded.
//! - **Gauges** show a running load's phase and the vectors and bytes handed to it (B3).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use pigeonhole_vector::{BulkStats, Index};
use prost::Message;
use prost::bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::convert::{self, Elements};
use super::errors::ApiError;
use super::placement::Placement;
use super::proto;

/// One vector of a load: its id and elements.
pub(crate) type Item = (Bytes, Elements);

/// One message's vectors, with the budget their bytes hold until the loader takes the last.
pub(crate) struct Batch {
    items: Vec<Item>,
    /// The message's encoded length, for the gauge.
    bytes: u64,
    budget: Option<OwnedSemaphorePermit>,
}

/// What a front-end hands the loader.
pub(crate) enum Msg {
    Batch(Batch),
    /// The client ended its stream: what came is the load.
    End,
    /// The stream failed; the vectors before are loaded.
    Failed(ApiError),
}

/// The loader: `Index::bulk_load_*` for the element type of the first vector
/// ([`load`]). A parameter, so a test can run one that's slow.
pub(crate) type Loader = Arc<
    dyn Fn(&Index, u64, &mut dyn Iterator<Item = Item>) -> Result<BulkStats, ApiError>
        + Send
        + Sync,
>;

/// `Vectors.BulkLoad`'s request, as the router passes it: the index and the expected count
/// from the first message, and the messages through `input`.
pub(crate) struct BulkLoadOp {
    pub(crate) index: String,
    pub(crate) expected: u64,
    pub(crate) input: mpsc::Receiver<Msg>,
    /// How long the call may wait for the one-load slot: the request timeout, or the
    /// client's deadline when that is shorter.
    pub(crate) wait: Duration,
}

impl std::fmt::Debug for BulkLoadOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkLoadOp")
            .field("index", &self.index)
            .field("expected", &self.expected)
            .finish_non_exhaustive()
    }
}

/// A source of a load's messages: the gRPC stream or the NDJSON body. Each comes with the
/// budget its bytes hold.
pub(crate) trait Source {
    async fn next(
        &mut self,
    ) -> Result<Option<(proto::VectorBulkLoadRequest, Option<OwnedSemaphorePermit>)>, ApiError>;
}

/// Feeds `src`'s messages, starting with `first`, to the loader through `tx`. The channel's
/// room is reserved before the next message is read, so the server holds two messages at
/// most: the one the loader is taking vectors from, and one waiting in the channel. Ends
/// when the stream does, on its first failure, or when the loader is gone.
pub(crate) async fn pump(
    tx: mpsc::Sender<Msg>,
    first: (proto::VectorBulkLoadRequest, Option<OwnedSemaphorePermit>),
    mut src: impl Source,
) {
    let (mut message, mut budget) = first;
    let Ok(mut room) = tx.reserve().await else {
        return;
    };
    loop {
        match batch(message, budget) {
            Ok(b) => room.send(Msg::Batch(b)),
            Err(e) => {
                room.send(Msg::Failed(e));
                return;
            }
        }
        // Room for the next message first, then read it.
        room = match tx.reserve().await {
            Ok(r) => r,
            Err(_) => return,
        };
        match src.next().await {
            Ok(Some((m, b))) => (message, budget) = (m, b),
            Ok(None) => {
                room.send(Msg::End);
                return;
            }
            Err(e) => {
                room.send(Msg::Failed(e));
                return;
            }
        }
    }
}

fn batch(
    m: proto::VectorBulkLoadRequest,
    budget: Option<OwnedSemaphorePermit>,
) -> Result<Batch, ApiError> {
    let bytes = m.encoded_len() as u64;
    let items = m
        .items
        .into_iter()
        .map(|i| {
            let v = i
                .vector
                .as_ref()
                .ok_or_else(|| ApiError::invalid("each BulkLoad item needs a vector"))?;
            Ok((i.id, convert::elements(v)?))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Batch {
        items,
        bytes,
        budget,
    })
}

/// The production loader: `bulk_load_*` for the first vector's element type.
///
/// **Every failure leaves the index loaded up to the vector that failed** (ai1249's M1 on
/// #1580). Each vector is checked against the index's spec here, by the crate's rule, before
/// the crate sees it: a vector of another element type or dimension, or of another type than
/// the first, ends the input there. The crate then writes and rebalances everything before
/// it, as for a stream that ended, and the call fails with `InvalidArgument`. (The crate's
/// own check would return at once instead, dropping the batches still waiting and skipping
/// the rebalance.)
pub(crate) fn load(
    index: &Index,
    expected: u64,
    input: &mut dyn Iterator<Item = Item>,
) -> Result<BulkStats, ApiError> {
    let spec = index.spec();
    let refused = std::cell::RefCell::new(None::<String>);
    let mut input = input
        .map_while(|(id, e)| match misfit(&spec, &e) {
            None => Some((id, e)),
            Some(why) => {
                *refused.borrow_mut() = Some(why);
                None
            }
        })
        // Fused: the crate reads on after its sample ends, and must not get past the misfit.
        .fuse()
        .peekable();
    macro_rules! typed {
        ($variant:ident, $load:ident) => {
            index.$load(
                expected,
                input
                    .by_ref()
                    .map_while(|(id, e)| match e {
                        Elements::$variant(x) => Some((id, x)),
                        _ => {
                            *refused.borrow_mut() = Some(
                                "a BulkLoad's vectors all have the first one's element type".into(),
                            );
                            None
                        }
                    })
                    .fuse(),
            )?
        };
    }
    let stats = match input.peek() {
        // Nothing to load: the crate still checks the index is empty.
        None => index.bulk_load_f32(expected, std::iter::empty::<(Bytes, Vec<f32>)>())?,
        Some((_, Elements::F32(_))) => typed!(F32, bulk_load_f32),
        Some((_, Elements::I8(_))) => typed!(I8, bulk_load_i8),
        Some((_, Elements::Bits(_))) => typed!(Bits, bulk_load_bits),
        Some((_, Elements::F16(_))) => typed!(F16, bulk_load_f16),
        Some((_, Elements::Bf16(_))) => typed!(Bf16, bulk_load_bf16),
    };
    drop(input);
    if let Some(why) = refused.into_inner() {
        return Err(ApiError::invalid(format!(
            "{why}; the vectors before it were loaded"
        )));
    }
    Ok(stats)
}

/// Why `e` doesn't fit an index of `spec`, by the crate's rule (`Input::check`): its element,
/// or `f32` into a half index, and its dimension (a bit vector's is 8 per byte).
fn misfit(spec: &pigeonhole::VectorSpec, e: &Elements) -> Option<String> {
    use pigeonhole::Element as E;
    let (element, dim) = match e {
        Elements::F32(x) => (E::F32, x.len()),
        Elements::I8(x) => (E::I8, x.len()),
        Elements::Bits(x) => (E::Bit, 8 * x.len()),
        Elements::F16(x) => (E::F16, x.len()),
        Elements::Bf16(x) => (E::BF16, x.len()),
    };
    let fits = element == spec.element()
        || (element == E::F32 && matches!(spec.element(), E::F16 | E::BF16));
    (!fits || dim != spec.dim() as usize).then(|| {
        format!(
            "the index holds vectors of {} {:?}, not {dim} {element:?}",
            spec.dim(),
            spec.element()
        )
    })
}

/// A running load's gauges (B3): its phase, and the vectors and message bytes handed to it.
/// Until the sample is clustered the load holds every vector handed to it, so `bytes` is an
/// upper bound on what it holds.
#[derive(Debug, Default)]
pub(crate) struct Gauges {
    /// 0: no load; 1: receiving; 2: the stream ended, the load is writing what's left and
    /// rebalancing.
    pub(crate) phase: AtomicU64,
    pub(crate) vectors: AtomicU64,
    pub(crate) bytes: AtomicU64,
}

/// The server's bulk loads: one at a time, each on a thread of its own.
pub(crate) struct BulkLoads {
    slot: Arc<Semaphore>,
    pub(crate) loader: Loader,
    pub(crate) placement: Option<Arc<Placement>>,
    /// The running (or last) load's thread, for `close` to wait for.
    thread: Mutex<Option<JoinHandle<()>>>,
    /// Set by `close` under `thread`'s lock: no load starts after it.
    closed: AtomicBool,
    /// Set by `stop` when the server starts shutting down: a running load stops at its next
    /// message, and no load starts.
    stopping: Arc<Stop>,
    pub(crate) gauges: Arc<Gauges>,
}

impl std::fmt::Debug for BulkLoads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkLoads")
            .field("gauges", &self.gauges)
            .finish_non_exhaustive()
    }
}

impl Default for BulkLoads {
    fn default() -> Self {
        Self {
            slot: Arc::new(Semaphore::new(1)),
            loader: Arc::new(load),
            placement: None,
            thread: Mutex::new(None),
            closed: AtomicBool::new(false),
            stopping: Arc::default(),
            gauges: Arc::default(),
        }
    }
}

fn closed() -> ApiError {
    ApiError::from_code(pigeonhole::ErrorCode::Closed, "the server is shutting down")
}

impl BulkLoads {
    /// Runs one load into `index`: waits at most `op.wait` for the slot, then loads on a
    /// thread of its own. The thread outlives a client that goes away: the load then ends
    /// with what came (the stream broke off), and its result is dropped.
    pub(crate) async fn run(&self, index: Arc<Index>, op: BulkLoadOp) -> Result<(), ApiError> {
        let slot = match tokio::time::timeout(op.wait, Arc::clone(&self.slot).acquire_owned()).await
        {
            Ok(Ok(slot)) => slot,
            Ok(Err(_)) => return Err(closed()),
            Err(_) => {
                return Err(ApiError::overloaded(
                    "another bulk load held the server's one load slot past the deadline",
                ));
            }
        };
        let (tx, done) = oneshot::channel();
        let loader = Arc::clone(&self.loader);
        let placement = self.placement.clone();
        let gauges = Arc::clone(&self.gauges);
        let (expected, rx) = (op.expected, op.input);
        let stopping = Arc::clone(&self.stopping);
        if stopping.flag.load(Ordering::Acquire) {
            return Err(closed());
        }
        {
            let mut thread = self.thread.lock().unwrap_or_else(PoisonError::into_inner);
            if self.closed.load(Ordering::Acquire) {
                return Err(closed());
            }
            // The last load's thread has released the slot, and its index before that: it
            // holds nothing of the database, so it needn't be joined.
            *thread = Some(
                std::thread::Builder::new()
                    .name("phdb-serve-bulk-load".into())
                    .spawn(move || {
                        if let Some(p) = placement {
                            p.enter();
                        }
                        gauges.phase.store(1, Ordering::Relaxed);
                        let mut input = Input {
                            rx,
                            current: Vec::new().into_iter(),
                            held: None,
                            failed: None,
                            ended: false,
                            gauges: &gauges,
                            stopping: &stopping,
                        };
                        let r = loader(&index, expected, &mut input);
                        // The loader's own failure first; else the stream's.
                        let r = r.and_then(|_| input.failed.take().map_or(Ok(()), Err));
                        drop(input);
                        drop(index);
                        for g in [&gauges.phase, &gauges.vectors, &gauges.bytes] {
                            g.store(0, Ordering::Relaxed);
                        }
                        // The slot before the result, as for a backup: a client that has
                        // its result and loads again at once must find it free.
                        drop(slot);
                        let _ = tx.send(r);
                    })
                    .map_err(|e| ApiError::internal(format!("a bulk load thread: {e}")))?,
            );
        }
        done.await
            .unwrap_or_else(|_| Err(ApiError::internal("the bulk load's thread ended early")))
    }

    /// Stops a running load at its next message and refuses new ones: called when the
    /// server starts shutting down, so a load ends within the grace period and its client
    /// hears `Closed` ("interrupted"). The load writes and rebalances what it took first.
    pub(crate) fn stop(&self) {
        self.stopping.flag.store(true, Ordering::Release);
        let waker = self
            .stopping
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// Stops new loads and waits for a running one: called at shutdown, once the front-ends
    /// are gone (so a load's input has ended), before the indexes and the database close.
    pub(crate) fn close(&self) {
        self.stop();
        let thread = {
            let mut thread = self.thread.lock().unwrap_or_else(PoisonError::into_inner);
            self.closed.store(true, Ordering::Release);
            thread.take()
        };
        if let Some(t) = thread {
            let _ = t.join();
        }
    }
}

/// The loader's input: the channel's messages, one vector at a time. A message's budget is
/// released when the loader takes its last vector.
struct Input<'g> {
    rx: mpsc::Receiver<Msg>,
    current: std::vec::IntoIter<Item>,
    held: Option<OwnedSemaphorePermit>,
    failed: Option<ApiError>,
    ended: bool,
    gauges: &'g Gauges,
    stopping: &'g Stop,
}

/// The shutdown flag, and the waker of a loader waiting for its next message. The loader
/// can't be woken through the channel: the front-end holds its one slot while it waits for
/// the client (the pump reserves room before it reads).
#[derive(Default)]
struct Stop {
    flag: AtomicBool,
    waker: Mutex<Option<std::task::Waker>>,
}

/// Wakes the loader's thread from `park`.
struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// What the loader's wait for its next message ends with.
enum Next {
    Msg(Msg),
    /// The front-end went away.
    Gone,
    /// The server is shutting down.
    Stopped,
}

impl Iterator for Input<'_> {
    type Item = Item;

    fn next(&mut self) -> Option<Item> {
        loop {
            if let Some(item) = self.current.next() {
                if self.current.len() == 0 {
                    self.held = None;
                }
                return Some(item);
            }
            if self.ended {
                return None;
            }
            match self.wait() {
                // Between messages: a shutdown stops the load here.
                Next::Stopped => self.end(Some(ApiError::from_code(
                    pigeonhole::ErrorCode::Closed,
                    "the server is shutting down: the load was interrupted, and the vectors \
                     before were loaded",
                ))),
                Next::Msg(Msg::Batch(b)) => {
                    let g = self.gauges;
                    g.vectors.fetch_add(b.items.len() as u64, Ordering::Relaxed);
                    g.bytes.fetch_add(b.bytes, Ordering::Relaxed);
                    self.current = b.items.into_iter();
                    self.held = b.budget;
                }
                Next::Msg(Msg::End) => self.end(None),
                Next::Msg(Msg::Failed(e)) => self.end(Some(e)),
                // The front-end went away without ending the stream: its client is gone.
                Next::Gone => self.end(Some(ApiError::invalid("the load's stream broke off"))),
            }
        }
    }
}

impl Input<'_> {
    /// The next message, or the shutdown flag, whichever comes first: the channel polled on
    /// this thread, which parks until the front-end sends or `stop` wakes it. The waker is
    /// registered before the flag is read, so a `stop` in between is seen.
    fn wait(&mut self) -> Next {
        let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = std::task::Context::from_waker(&waker);
        loop {
            *self
                .stopping
                .waker
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(waker.clone());
            if self.stopping.flag.load(Ordering::Acquire) {
                return Next::Stopped;
            }
            match self.rx.poll_recv(&mut cx) {
                std::task::Poll::Ready(Some(m)) => return Next::Msg(m),
                std::task::Poll::Ready(None) => return Next::Gone,
                // A spurious unpark only loops.
                std::task::Poll::Pending => std::thread::park(),
            }
        }
    }

    fn end(&mut self, failed: Option<ApiError>) {
        self.ended = true;
        self.failed = failed;
        self.held = None;
        self.gauges.phase.store(2, Ordering::Relaxed);
    }
}
