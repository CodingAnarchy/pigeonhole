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
//! - **The loader holds a working set of its own, outside the in-flight budget, and bounded
//!   by `--bulk-load-bytes`** (the audit's F7): the crate clusters a sample of the first
//!   max(50,000, lists) vectors before it writes any, lists being expected / target list
//!   length, then keeps up to 255 vectors per list until each list's batch of 256 is
//!   written ([`need`]). A load whose worst case is over the flag is refused before it
//!   starts, and a running one reserves its worst case from the node's budget, which a
//!   cluster node's partitions share. **One load runs at a time per database** (the slot).
//! - **A client that sends nothing for `--stream-idle-ms`** ends the load there, the
//!   vectors before loaded, so a stalled client can't hold the slot for its connection's
//!   life. The front-ends bound each chunk's wait (`grpc::Metered`, `http::NdjsonLines`,
//!   audit120's S-S1), and their error reaches the loader as `Msg::Failed`.
//! - **The loader runs on a thread of its own** holding the slot, never on the blocking pool
//!   (B1: a load runs for minutes), placed off the shards' CPUs (B2). The crate's k-means
//!   runs on that thread, single-threaded.
//! - **Gauges** show a running load's phase and the vectors and bytes handed to it (B3).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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

/// The crate's sample floor and batch size (`pigeonhole_vector`'s `BULK_SAMPLE` and
/// `BULK_BATCH`, index.rs): a load clusters its first max([`SAMPLE_FLOOR`], lists) vectors
/// before writing any, then each list's batch waits for 256 vectors, so up to 255 a list wait
/// unwritten.
const SAMPLE_FLOOR: u64 = 50_000;
const PENDING_PER_LIST: u64 = 255;
/// What a held vector costs beyond its elements and its id's two copies (the pair's, and the
/// crate's `waiting` map's, audit120's nit on #1802): the id's `Bytes` and the elements'
/// `Vec` headers, the pair, and the map's entry.
const PER_VECTOR: u64 = 96;
/// The id length the up-front check assumes; a longer id grows the reservation as it comes.
const ID_ALLOWANCE: u64 = 32;
/// How many times what its lists were sized for a load may send before it stops: lists ×
/// the most a list holds before it splits (the audit's P2-F2). An honest `expected` sizes
/// lists × that at twice `expected`, so this is four times what it declared; one that
/// declared too few (0, say) stops here instead of growing one list without bound, which
/// the rebalance's split then reads whole.
const SIZED_FACTOR: u64 = 2;

/// What a bulk load holds at most, by the crate's design ([`SAMPLE_FLOOR`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Need {
    /// ceil(expected / target list length), at least 1: the lists k-means makes.
    pub(crate) lists: u64,
    /// The sample's vectors: max(50,000, lists).
    pub(crate) sample_vectors: u64,
    /// The sample's bytes: each vector held, with the copy k-means clusters (`f32` but for a
    /// bit index).
    pub(crate) sample: u64,
    /// The pending batches' bytes after it: 255 vectors a list.
    pub(crate) pending: u64,
    /// The most vectors the load may send ([`SIZED_FACTOR`] × lists × the longest a list
    /// grows before it splits, twice `target`).
    pub(crate) vectors: u64,
}

impl Need {
    /// The most the load holds at once.
    pub(crate) fn bytes(&self) -> u64 {
        self.sample.max(self.pending)
    }
}

/// What a load of `expected` vectors into an index of `spec`, with lists of `target`
/// vectors, holds at most, its ids `id` bytes long (each held twice). Saturating: a huge `expected` comes out
/// as `u64::MAX`, and is refused.
pub(crate) fn need(spec: &pigeonhole::VectorSpec, target: u64, expected: u64, id: u64) -> Need {
    use pigeonhole::Element as E;
    let dim = u64::from(spec.dim());
    // What a vector holds as sent, and its clustering copy. A half index takes `f32` vectors
    // too (`misfit`), so it's counted at `f32`'s size.
    let (held, placed) = match spec.element() {
        E::I8 => (dim, dim * 4),
        E::Bit => (dim.div_ceil(8), dim.div_ceil(8)),
        _ => (dim * 4, dim * 4),
    };
    let each = held
        .saturating_add(id.saturating_mul(2))
        .saturating_add(PER_VECTOR);
    let lists = expected.div_ceil(target.max(1)).max(1);
    let sample_vectors = lists.max(SAMPLE_FLOOR);
    Need {
        lists,
        vectors: SIZED_FACTOR
            .saturating_mul(lists)
            .saturating_mul(target.max(1).saturating_mul(2)),
        sample_vectors,
        sample: sample_vectors.saturating_mul(each.saturating_add(placed)),
        pending: lists.saturating_mul(PENDING_PER_LIST).saturating_mul(each),
    }
}

/// `InvalidArgument`: a load whose worst case is over `--bulk-load-bytes`.
fn too_large(n: &Need, spec: &pigeonhole::VectorSpec, expected: u64, limit: u64) -> ApiError {
    let (what, bytes) = if n.sample >= n.pending {
        (
            format!("its sample of {} vectors", n.sample_vectors),
            n.sample,
        )
    } else {
        (
            format!("{} lists' pending batches of up to 255 vectors", n.lists),
            n.pending,
        )
    };
    ApiError::invalid(format!(
        "a BulkLoad of {expected} expected {}-dim {:?} vectors holds up to {bytes} bytes \
         ({what}), more than --bulk-load-bytes ({limit}): raise --bulk-load-bytes to at least \
         {bytes}, or load fewer vectors at once",
        spec.dim(),
        spec.element(),
    ))
}

/// The budget a load reserves its worst case from, in KiB (a semaphore counts `u32`s).
fn kib(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(1024)).unwrap_or(u32::MAX)
}

/// The node's `--bulk-load-bytes`, which its loads reserve from.
pub(crate) struct Budget<'a> {
    pub(crate) pool: &'a Arc<Semaphore>,
    pub(crate) limit: u64,
}

/// The node's `--bulk-load-bytes` as a semaphore of KiB.
pub(crate) fn budget_pool(bytes: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(
        (kib(bytes as u64) as usize).min(Semaphore::MAX_PERMITS),
    ))
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
    /// The bytes of `--bulk-load-bytes` the load reserved: its worst case.
    pub(crate) reserved: AtomicU64,
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
    /// On a cluster partition, `gate` is its write gate (D317 ticket 6): the load holds it
    /// until its thread ends, and checks it between messages, so a fence stops it there
    /// with `Fenced`, the vectors before loaded.
    ///
    /// A load whose worst case ([`need`]) is over `--bulk-load-bytes` is refused here, before
    /// it takes anything; one that fits reserves it from the node's budget for its life.
    pub(crate) async fn run(
        &self,
        index: Arc<Index>,
        op: BulkLoadOp,
        gate: Option<&Arc<super::gate::WriteGate>>,
        budget: Budget<'_>,
    ) -> Result<(), ApiError> {
        let spec = index.spec();
        // A list splits past twice its target (`Index::max_list_len`).
        let target = (index.max_list_len() / 2).max(1);
        let first = need(&spec, target, op.expected, ID_ALLOWANCE);
        if first.bytes() > budget.limit {
            return Err(too_large(&first, &spec, op.expected, budget.limit));
        }
        let started = Instant::now();
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
        // Under the slot, so a load waiting for it reserves nothing. A cluster node's other
        // partitions' loads share the budget.
        let left = op.wait.saturating_sub(started.elapsed());
        let reserved = match tokio::time::timeout(
            left,
            Arc::clone(budget.pool).acquire_many_owned(kib(first.bytes())),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => return Err(closed()),
            Err(_) => {
                return Err(ApiError::overloaded(
                    "other bulk loads held --bulk-load-bytes past the deadline",
                ));
            }
        };
        let sizing = Sizing {
            spec,
            target,
            expected: op.expected,
            limit: budget.limit,
            id: ID_ALLOWANCE,
            need: first.bytes(),
            lists: first.lists,
            vectors: first.vectors,
            taken: 0,
            pool: Arc::clone(budget.pool),
            reserved,
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
        // `Service::call` admitted the load, and holds its gate until this returns.
        let held = gate.map(super::gate::WriteGate::extend);
        let gate = gate.cloned();
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
                        gauges.reserved.store(sizing.need, Ordering::Relaxed);
                        let mut input = Input {
                            rx,
                            current: Vec::new().into_iter(),
                            held: None,
                            failed: None,
                            ended: false,
                            gauges: &gauges,
                            stopping: &stopping,
                            gate,
                            sizing,
                        };
                        let r = loader(&index, expected, &mut input);
                        // The loader's own failure first; else the stream's.
                        let r = r.and_then(|_| input.failed.take().map_or(Ok(()), Err));
                        drop(input);
                        drop(index);
                        // Its last write is done: a fence's drain may finish.
                        drop(held);
                        for g in [
                            &gauges.phase,
                            &gauges.vectors,
                            &gauges.bytes,
                            &gauges.reserved,
                        ] {
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
    /// server starts shutting down, so a load stops taking vectors at once, and its client
    /// hears `Closed` ("interrupted"). The load still writes and rebalances what it took,
    /// which for a large load can outlast the grace period (perf287 on #1580).
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
    /// A cluster partition's write gate: a fence ends the input at the next message.
    gate: Option<Arc<super::gate::WriteGate>>,
    /// The load's worst case and its reservation, grown as longer ids come.
    sizing: Sizing,
}

/// A running load's worst case ([`need`]) and what it reserved of `--bulk-load-bytes`.
struct Sizing {
    spec: pigeonhole::VectorSpec,
    target: u64,
    expected: u64,
    limit: u64,
    /// The longest id the worst case assumes.
    id: u64,
    /// The worst case's bytes, as reserved.
    need: u64,
    /// The lists `expected` makes, and the most vectors the load may send for them.
    lists: u64,
    vectors: u64,
    /// The vectors handed to the loader so far.
    taken: u64,
    pool: Arc<Semaphore>,
    /// Released with the input: when the loader returns, its working set is gone.
    reserved: OwnedSemaphorePermit,
}

impl Sizing {
    /// Takes the next vector, its id `len` bytes long: past what its lists were sized for
    /// the load stops (the audit's P2-F2), and an id longer than assumed grows the
    /// reservation, now or not at all. The error ends the input before that vector.
    fn take(&mut self, len: u64, gauges: &Gauges) -> Result<(), ApiError> {
        if self.taken >= self.vectors {
            return Err(ApiError::invalid(format!(
                "a BulkLoad that declared expected {} sent more than {} vectors, {SIZED_FACTOR} \
                 × its {} lists of up to {} each: the load stopped there, and the vectors \
                 before it were loaded. Set expected to the number of vectors you load, and \
                 Insert the rest",
                self.expected,
                self.vectors,
                self.lists,
                2 * self.target
            )));
        }
        self.taken += 1;
        if len <= self.id {
            return Ok(());
        }
        let n = need(&self.spec, self.target, self.expected, len);
        if n.bytes() > self.limit {
            return Err(ApiError::invalid(format!(
                "a BulkLoad id of {len} bytes makes the load hold up to {} bytes, more than \
                 --bulk-load-bytes ({}): the load stopped there, and the vectors before it \
                 were loaded",
                n.bytes(),
                self.limit
            )));
        }
        let more = kib(n.bytes()).saturating_sub(kib(self.need));
        if more > 0 {
            let extra = Arc::clone(&self.pool)
                .try_acquire_many_owned(more)
                .map_err(|_| {
                    ApiError::overloaded(
                        "a BulkLoad id longer than assumed needs more of --bulk-load-bytes than \
                     other loads leave: the load stopped there, and the vectors before it \
                     were loaded",
                    )
                })?;
            self.reserved.merge(extra);
        }
        self.id = len;
        self.need = n.bytes();
        gauges.reserved.store(n.bytes(), Ordering::Relaxed);
        Ok(())
    }
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
    /// The partition was fenced (D317 ticket 6).
    Fenced,
}

/// How often a load waiting for its next message looks at its partition's fence.
const FENCE_POLL: std::time::Duration = std::time::Duration::from_millis(50);

impl Iterator for Input<'_> {
    type Item = Item;

    fn next(&mut self) -> Option<Item> {
        loop {
            if let Some(item) = self.current.next() {
                if let Err(e) = self.sizing.take(item.0.len() as u64, self.gauges) {
                    self.current = Vec::new().into_iter();
                    self.end(Some(e));
                    return None;
                }
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
                Next::Fenced => self.end(Some(ApiError::fenced(
                    "the partition was fenced: the load stopped there, and the vectors before \
                     were loaded; resume from the last acknowledged one once it's unfenced or \
                     at its new node",
                ))),
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
            if self.gate.as_ref().is_some_and(|g| g.fenced().is_some()) {
                return Next::Fenced;
            }
            match self.rx.poll_recv(&mut cx) {
                std::task::Poll::Ready(Some(m)) => return Next::Msg(m),
                std::task::Poll::Ready(None) => return Next::Gone,
                // A spurious unpark only loops. With a gate, the fence is looked at again
                // every `FENCE_POLL`: nothing wakes a load when its partition is fenced.
                std::task::Poll::Pending if self.gate.is_some() => {
                    std::thread::park_timeout(FENCE_POLL);
                }
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

#[cfg(test)]
mod tests {
    use pigeonhole::{Element, Metric, VectorSpec};

    use super::{ID_ALLOWANCE, need};

    #[test]
    fn a_load_s_worst_case_is_its_sample_or_its_pending_batches() {
        // serve.md's examples at 1 GiB, at the target list lengths the crate gives these
        // dimensions (P2-S3): 1,000 scaled down so twice it fits a 4 MiB list row, 682 at
        // 768-dim f32 and 341 at 1,536-dim.
        let gib = 1u64 << 30;
        let f32s = |dim| VectorSpec::new(dim, Element::F32, Metric::L2).unwrap();
        let fits = |dim, target, lists: u64| {
            need(&f32s(dim), target, lists * target, ID_ALLOWANCE).bytes() <= gib
        };
        // 768-dim: the sample of 50,000 is about 315 MB, held and its clustering copy; the
        // pending batches allow 1,302 lists, about 888,000 vectors.
        let n = need(&f32s(768), 682, 682_000, ID_ALLOWANCE);
        assert_eq!(n.lists, 1000);
        assert_eq!(n.sample, 50_000 * (3072 + 64 + 96 + 3072));
        assert!(fits(768, 682, 1302) && !fits(768, 682, 1303));
        assert_eq!(1302 * 682, 887_964);
        // 1,536-dim: 667 lists, about 227,000 vectors. A million make 2,933 lists, whose
        // pending batches need about 4.7 GB.
        assert!(fits(1536, 341, 667) && !fits(1536, 341, 668));
        assert_eq!(667 * 341, 227_447);
        let n = need(&f32s(1536), 341, 1_000_000, ID_ALLOWANCE);
        assert_eq!(n.lists, 2933);
        assert_eq!(n.pending, 2933 * 255 * (6144 + 64 + 96));
        // The most a load sends (P2-F2): twice its lists' split length. Declaring 0 makes one
        // list, so 4 × target vectors.
        assert_eq!(need(&f32s(768), 682, 0, ID_ALLOWANCE).vectors, 2 * 2 * 682);
        assert_eq!(
            need(&f32s(768), 682, 682_000, ID_ALLOWANCE).vectors,
            4 * 682_000
        );
        // Saturating, never overflowing.
        let n = need(&f32s(4096), 1, u64::MAX, u64::MAX);
        assert_eq!(n.bytes(), u64::MAX);
        // A bit index holds a vector's bytes, and clusters them as bits.
        let bits = VectorSpec::new(1024, Element::Bit, Metric::Hamming).unwrap();
        assert_eq!(
            need(&bits, 1000, 1, ID_ALLOWANCE).sample,
            50_000 * (128 + 64 + 96 + 128)
        );
    }
}
