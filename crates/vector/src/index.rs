//! The index (D306): writes, inline split and merge, search, and verification.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use pigeonhole::{
    Element, ErrorCode, Family, KeySet, Pigeonhole, RetryPolicy, Snapshot, Table, Transaction,
    Vector, VectorSpec,
};

use crate::layout::{
    CEN, CNT, GLOBAL, LAYOUT, LIST, MAP, MARK, META, NEXT, OPTS, VEC, VERSION, centroid_row,
    count_row, list_of, list_row, map_row, queue_row, scramble,
};
use crate::point::{Input, Plan, Point, Stored, distance, two_means, unit};
use crate::rebalancer::{Rebalancer, Signal};
use crate::{Error, Result};

/// A list row stays at most this large (D306 §1): the target list length is scaled down
/// for large vectors so `2 × target` vectors fit.
const MAX_LIST_BYTES: u64 = 4 << 20;
/// Attempts of one split or merge before it leaves the list for the next trigger (rev 3 S1).
const ATTEMPTS: usize = 4;
/// Centroid reloads an insert or delete makes before it gives up with `Conflict`.
const RELOADS: usize = 32;
/// Splits and merges one insert or delete runs after its commit, at most; the rest stay
/// queued for the next call.
const MAINTENANCE_PER_CALL: usize = 16;

/// An index's options, fixed when it is created ([`Index::create`]) and stored with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexOptions {
    target: u32,
    nprobe: u32,
    reassign: u32,
    rebalance: Rebalance,
}

/// Who splits and merges an index's lists (D306 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Rebalance {
    /// Inside the insert or delete that took the list out of its bounds, before the call
    /// returns: the writer pays for the split.
    #[default]
    Inline,
    /// In a [`Rebalancer`]: a write marks the list and returns, and the rebalancer splits or
    /// merges it soon after. [`Index::open`] runs one on a thread of the crate's own;
    /// [`Index::open_with_rebalancer`] hands it to the application instead. Until it runs,
    /// a list may hold more than [`Index::max_list_len`] vectors, which costs searches that
    /// probe it, never their correctness.
    Background,
}

impl Default for IndexOptions {
    fn default() -> Self {
        Self {
            target: 1000,
            nprobe: 16,
            reassign: 2,
            rebalance: Rebalance::Inline,
        }
    }
}

impl IndexOptions {
    /// The defaults: lists of about 1,000 vectors, 16 lists probed per search, and 2
    /// neighbouring lists reassigned after a split.
    pub fn new() -> Self {
        Self::default()
    }

    /// The length a list is kept near (at least 4): a list splits above twice this, and
    /// merges into its nearest neighbour below a quarter of it. Scaled down for large
    /// vectors, so a list row stays within 4 MiB.
    pub fn target_list_len(mut self, n: u32) -> Self {
        self.target = n;
        self
    }

    /// Lists a search probes unless it says otherwise ([`Search::nprobe`]; at least 1).
    pub fn nprobe(mut self, n: u32) -> Self {
        self.nprobe = n;
        self
    }

    /// Neighbouring lists a split re-examines for vectors now nearer one of its two new
    /// centroids (SPFresh's LIRE; 0 turns it off). It improves clustering, not correctness.
    pub fn reassign_neighbours(mut self, r: u32) -> Self {
        self.reassign = r;
        self
    }

    /// Who splits and merges the lists ([`Rebalance::Inline`] by default).
    pub fn rebalance(mut self, r: Rebalance) -> Self {
        self.rebalance = r;
        self
    }

    /// Who splits and merges the lists.
    pub fn rebalance_mode(&self) -> Rebalance {
        self.rebalance
    }

    fn checked(mut self, spec: &VectorSpec) -> Result<Self> {
        if self.target < 4 || self.nprobe == 0 {
            return Err(Error::InvalidArgument(format!(
                "index options: the target list length must be at least 4 and nprobe at least \
                 1, not {} and {}",
                self.target, self.nprobe
            )));
        }
        let bytes = match spec.element() {
            Element::F32 => 4 * u64::from(spec.dim()),
            Element::Bit => u64::from(spec.dim()) / 8,
            _ => u64::from(spec.dim()),
        };
        let cap = (MAX_LIST_BYTES / (2 * bytes)).max(4);
        self.target = self.target.min(u32::try_from(cap).unwrap_or(u32::MAX));
        Ok(self)
    }

    /// Version 2: version 1's three `u32`s, then the rebalance mode (0 inline, 1
    /// background). Version 1 (PR 1) decodes as inline.
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![2];
        for v in [self.target, self.nprobe, self.reassign] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.push(match self.rebalance {
            Rebalance::Inline => 0,
            Rebalance::Background => 1,
        });
        out
    }

    fn decode(b: &[u8]) -> Option<Self> {
        let (rest, rebalance) = match b {
            [1, rest @ ..] => (rest, Rebalance::Inline),
            [2, rest @ .., 0] => (rest, Rebalance::Inline),
            [2, rest @ .., 1] => (rest, Rebalance::Background),
            _ => return None,
        };
        let (words, []) = rest.as_chunks::<4>() else {
            return None;
        };
        let [target, nprobe, reassign] = words else {
            return None;
        };
        Some(Self {
            target: u32::from_le_bytes(*target),
            nprobe: u32::from_le_bytes(*nprobe),
            reassign: u32::from_le_bytes(*reassign),
            rebalance,
        })
    }
}

/// One result of a [`Search`]: the id given at insert, and its distance to the query under
/// the index's metric (smaller is nearer), as [`VectorSpec`]'s `distance_*` give it.
///
/// Hits at an equal distance come back in the order of the lists that hold them, then by
/// id, as `nearest_*` orders a list's row then its qualifiers. That order is fixed at a
/// snapshot, but a split can move a tied id to another list, so which of several tied ids
/// makes the cut at `k` can change from one layout to the next.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Hit {
    /// The vector's id.
    pub id: Vec<u8>,
    /// Its distance to the query.
    pub distance: f64,
}

/// What a bulk load did ([`Index::bulk_load_f32`] and its forms).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct BulkStats {
    /// Vectors written (a repeated id counts once per occurrence).
    pub vectors: u64,
    /// Lists k-means made before the load (splits and merges may change the count after).
    pub lists: u32,
}

/// Vectors a bulk load clusters to choose its lists: the first this many it reads.
const BULK_SAMPLE: usize = 50_000;
/// Vectors per transaction when a bulk load writes a list.
const BULK_BATCH: usize = 256;
/// Lloyd iterations of a bulk load's k-means.
const KMEANS_ITERATIONS: usize = 10;

/// Counts from [`Index::verify`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexStats {
    /// Each list's length, in list order.
    pub list_lens: Vec<u64>,
    /// Vectors in the index.
    pub vectors: u64,
}

/// An IVF index over one table (D306). Cheap to clone; clones share the centroid cache.
///
/// ```
/// use pigeonhole::{Element, Metric, Options, Pigeonhole, VectorSpec};
/// use pigeonhole_vector::{Index, IndexOptions};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
/// let spec = VectorSpec::new(2, Element::F32, Metric::L2)?;
/// let index = Index::create(&db, "points", spec, IndexOptions::new().target_list_len(4))?;
/// for i in 0..40u32 {
///     let x = i as f32;
///     index.insert_f32(&i.to_be_bytes(), &[x, -x])?;
/// }
/// let hits = index.search_f32(&[10.2, -10.2], 3).nprobe(2).run()?;
/// assert_eq!(hits[0].id, 10u32.to_be_bytes());
///
/// assert!(index.delete(&10u32.to_be_bytes())?);
/// let stats = index.verify()?;
/// assert_eq!(stats.vectors, 39);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Index {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index")
            .field("table", &self.inner.table.name())
            .field("spec", &self.inner.spec)
            .field("options", &self.inner.opts)
            .finish_non_exhaustive()
    }
}

pub(crate) struct Inner {
    db: Pigeonhole,
    table: Table,
    spec: VectorSpec,
    /// The centroid family's spec: `f32` of the index's dimension and metric, or the
    /// index's own for bits (rev 2 S1).
    cen: VectorSpec,
    opts: IndexOptions,
    /// The centroids as last loaded at the current layout (rev 3 S4).
    layout: RwLock<Arc<Layout>>,
    /// Lists with a split or merge running in this process (rev 4).
    in_flight: Mutex<HashSet<u32>>,
    /// Lists to check against their bounds: after writes, and found by `open` (rev 3 M3).
    pending: Mutex<VecDeque<u32>>,
    /// Wakes the background rebalancer, and tells it when the last handle is gone.
    signal: Arc<Signal>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.signal.close();
    }
}

/// The centroids at one snapshot.
#[derive(Debug)]
struct Layout {
    seqno: u64,
    /// `G:layout`: centroids change only with it.
    version: u64,
    /// By list id.
    lists: Vec<List>,
}

#[derive(Debug)]
struct List {
    id: u32,
    version: u64,
    centroid: Vector,
    point: Point,
}

impl Layout {
    fn list(&self, id: u32) -> Option<&List> {
        self.lists
            .binary_search_by_key(&id, |l| l.id)
            .ok()
            .map(|i| &self.lists[i])
    }

    /// The `n` lists nearest `p`, nearest first, ties by list id, leaving out `skip`.
    fn nearest(&self, cen: &VectorSpec, p: &Point, n: usize, skip: &[u32]) -> Result<Vec<u32>> {
        let mut all = Vec::with_capacity(self.lists.len());
        for l in self.lists.iter().filter(|l| !skip.contains(&l.id)) {
            all.push((distance(cen, p, &l.centroid)?, l.id));
        }
        all.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        Ok(all.into_iter().take(n).map(|(_, id)| id).collect())
    }
}

/// How a transaction body ends besides committing.
enum Tx {
    Db(pigeonhole::Error),
    /// The layout it acted on is gone: reload the centroids and try again, or stop.
    Stale,
    Fail(Error),
}

impl From<pigeonhole::Error> for Tx {
    fn from(e: pigeonhole::Error) -> Self {
        Tx::Db(e)
    }
}

impl From<Error> for Tx {
    fn from(e: Error) -> Self {
        Tx::Fail(e)
    }
}

impl From<Tx> for Error {
    fn from(t: Tx) -> Self {
        match t {
            Tx::Db(e) => Error::Db(e),
            Tx::Stale => Error::Conflict("the index's layout changed".into()),
            Tx::Fail(e) => e,
        }
    }
}

/// A retry's pause: 5 µs doubling, at most 1 ms.
fn backoff(attempt: usize) -> Duration {
    Duration::from_micros(5u64 << attempt.min(8)).min(Duration::from_millis(1))
}

fn is_conflict(t: &Tx) -> bool {
    matches!(t, Tx::Db(e) if e.code() == ErrorCode::Conflict)
}

fn corrupt(what: &str) -> Error {
    Error::Corruption(what.to_owned())
}

/// The centroid family's spec for an index of `spec` (rev 2 S1).
fn centroid_spec(spec: &VectorSpec) -> Result<VectorSpec> {
    match spec.element() {
        Element::Bit => Ok(*spec),
        Element::F32 | Element::I8 => Ok(VectorSpec::new(spec.dim(), Element::F32, spec.metric())?),
        other => Err(Error::InvalidArgument(format!(
            "an index of {other:?} vectors is not supported"
        ))),
    }
}

fn read_u64(
    tx: &mut Transaction,
    t: &Table,
    row: &[u8],
    family: &str,
    q: &[u8],
) -> std::result::Result<Option<u64>, Tx> {
    match tx.get(t, row, family, q)? {
        None => Ok(None),
        Some(c) => c
            .as_u64()
            .map(Some)
            .ok_or_else(|| Tx::Fail(corrupt("an index counter or version is not an integer"))),
    }
}

/// `C{list}:meta:v`, or `None` when the list doesn't exist (merged away, rev 3 M1).
fn read_version(
    tx: &mut Transaction,
    t: &Table,
    list: u32,
) -> std::result::Result<Option<u64>, Tx> {
    read_u64(tx, t, &centroid_row(list), META, VERSION)
}

/// `M{id}`: the list holding `id`.
fn read_list_of(
    tx: &mut Transaction,
    t: &Table,
    id: &[u8],
) -> std::result::Result<Option<u32>, Tx> {
    read_u64(tx, t, &map_row(id), MAP, LIST)?
        .map(|l| u32::try_from(l).map_err(|_| Tx::Fail(corrupt("a mapping names no list"))))
        .transpose()
}

/// The vector `id` holds in the list row `row`, read in the transaction (rev 4 N1).
fn read_stored(
    tx: &mut Transaction,
    t: &Table,
    row: &[u8],
    id: &[u8],
) -> std::result::Result<Option<Stored>, Tx> {
    match tx.get(t, row, VEC, id)? {
        None => Ok(None),
        Some(c) => {
            let v = c
                .as_vector()
                .ok_or_else(|| Tx::Fail(corrupt("a list column is not a vector")))?;
            Ok(Some(Stored::of(v)?))
        }
    }
}

fn put_centroid(tx: &mut Transaction, t: &Table, list: u32, c: &Point) {
    let row = centroid_row(list);
    match c {
        Point::F32(x) => tx.put_vector_f32(t, &row, CEN, b"", x),
        Point::Bits(b) => tx.put_vector_bits(t, &row, CEN, b"", b),
    };
}

/// The centroids at `s`.
fn load(t: &Table, cen: &VectorSpec, s: &Snapshot) -> Result<Layout> {
    let version = t
        .get_at(s, GLOBAL, META, LAYOUT)?
        .and_then(|c| c.as_u64())
        .unwrap_or(0);
    let mut lists = Vec::new();
    for row in t
        .scan_prefix(b"C")
        .families([CEN, META])
        .snapshot(s)
        .iter()?
    {
        let row = row?;
        let id = list_of(row.key()).ok_or_else(|| corrupt("a centroid row key"))?;
        let v = row
            .get(CEN, b"")
            .and_then(|c| c.as_vector())
            .ok_or_else(|| corrupt("a list has no centroid"))?;
        let point = Point::of(v);
        let version = row
            .get(META, VERSION)
            .and_then(|c| c.as_u64())
            .ok_or_else(|| corrupt("a list has no version"))?;
        lists.push(List {
            id,
            version,
            centroid: point.to_vector(cen.dim())?,
            point,
        });
    }
    Ok(Layout {
        seqno: s.seqno(),
        version,
        lists,
    })
}

/// `k` centroids of `points` by Lloyd's k-means (D306 §5), seeded deterministically with
/// evenly spaced points; a cluster that empties keeps its centroid.
fn kmeans(cen: &VectorSpec, points: &[Point], k: usize) -> Result<Vec<Point>> {
    let metric = cen.metric();
    let dim = cen.dim();
    let mut centroids: Vec<Point> = (0..k)
        .map(|i| crate::point::unit(metric, points[i * points.len() / k].clone()))
        .collect();
    let mut owner = vec![0usize; points.len()];
    for _ in 0..KMEANS_ITERATIONS {
        let vectors = centroids
            .iter()
            .map(|c| c.to_vector(dim))
            .collect::<Result<Vec<_>>>()?;
        let mut changed = false;
        for (p, o) in points.iter().zip(owner.iter_mut()) {
            let mut best = (f64::INFINITY, 0);
            for (j, c) in vectors.iter().enumerate() {
                let d = distance(cen, p, c)?;
                if d < best.0 {
                    best = (d, j);
                }
            }
            changed |= *o != best.1;
            *o = best.1;
        }
        for (j, c) in centroids.iter_mut().enumerate() {
            let members = points
                .iter()
                .zip(&owner)
                .filter(|(_, o)| **o == j)
                .map(|(p, _)| p);
            if let Some(m) = crate::point::mean(metric, members) {
                *c = m;
            }
        }
        if !changed {
            break;
        }
    }
    Ok(centroids)
}

/// Removes its list from the in-flight set when the split or merge ends, however it ends.
struct Claim<'a> {
    set: &'a Mutex<HashSet<u32>>,
    list: u32,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.set
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.list);
    }
}

impl Index {
    /// Creates the index `name`, a new table, for vectors of `spec`, and opens it as
    /// [`open`](Self::open) does.
    ///
    /// The table holds the index's rows only; don't write to it directly. Fails with
    /// `TableExists` if a table of that name exists.
    pub fn create(
        db: &Pigeonhole,
        name: &str,
        spec: VectorSpec,
        options: IndexOptions,
    ) -> Result<Index> {
        Self::create_table(db, name, spec, options)?;
        Self::open(db, name)
    }

    /// [`create`](Self::create), opened as
    /// [`open_with_rebalancer`](Self::open_with_rebalancer) is.
    pub fn create_with_rebalancer(
        db: &Pigeonhole,
        name: &str,
        spec: VectorSpec,
        options: IndexOptions,
    ) -> Result<(Index, Rebalancer)> {
        Self::create_table(db, name, spec, options)?;
        Self::open_with_rebalancer(db, name)
    }

    fn create_table(
        db: &Pigeonhole,
        name: &str,
        spec: VectorSpec,
        options: IndexOptions,
    ) -> Result<()> {
        let opts = options.checked(&spec)?;
        let cen = centroid_spec(&spec)?;
        let table = db
            .table(name)?
            .family(VEC, Family::vector(spec).max_versions(1))
            .family(CEN, Family::vector(cen).max_versions(1))
            .family(META, Family::default().max_versions(1))
            .family(MAP, Family::default().max_versions(1))
            .family(CNT, Family::counter())
            .create()?;
        table
            .mutate(GLOBAL)
            .put(META, OPTS, &opts.encode())
            .put_u64(META, LAYOUT, 0)
            .put_u64(META, NEXT, 0)
            .commit()?;
        Ok(())
    }

    /// Opens the index `name`. Lists a crash left outside their bounds (a write committed,
    /// and its split or merge never ran) are queued, and the next insert or delete
    /// rebalances them, or with [`Rebalance::Background`] the crate's rebalancing thread
    /// does: it starts here, and stops when the last handle to this index drops. An
    /// application that runs its own threads opens with
    /// [`open_with_rebalancer`](Self::open_with_rebalancer) instead.
    pub fn open(db: &Pigeonhole, name: &str) -> Result<Index> {
        let index = Self::open_inner(db, name)?;
        if index.inner.opts.rebalance == Rebalance::Background {
            crate::rebalancer::spawn(Rebalancer::new(
                &index.inner,
                Arc::clone(&index.inner.signal),
            ))
            .map_err(|e| Error::InvalidArgument(format!("starting the rebalancer: {e}")))?;
        }
        Ok(index)
    }

    /// Opens the index `name` with its [`Rebalancer`] handed to the caller, and no thread of
    /// the crate's own: for an application that runs its threads itself (an
    /// application-owned database, D284). With [`Rebalance::Background`] the application
    /// runs the rebalancer; with [`Rebalance::Inline`] the writes rebalance, and the
    /// rebalancer only finds what `open` queued.
    pub fn open_with_rebalancer(db: &Pigeonhole, name: &str) -> Result<(Index, Rebalancer)> {
        let index = Self::open_inner(db, name)?;
        let rebalancer = Rebalancer::new(&index.inner, Arc::clone(&index.inner.signal));
        Ok((index, rebalancer))
    }

    pub(crate) fn from_inner(inner: Arc<Inner>) -> Self {
        Index { inner }
    }

    fn open_inner(db: &Pigeonhole, name: &str) -> Result<Index> {
        let table = db.table(name)?.open()?;
        let not = |why: &str| {
            Error::InvalidArgument(format!("table {name:?} is not a vector index: {why}"))
        };
        let spec = table
            .family_info(VEC)
            .and_then(|f| f.vector)
            .ok_or_else(|| not("it has no vector family `vec`"))?;
        let cen = centroid_spec(&spec)?;
        if table.family_info(CEN).and_then(|f| f.vector) != Some(cen) {
            return Err(not("its centroid family doesn't match"));
        }
        let opts = table
            .get(GLOBAL, META, OPTS)?
            .and_then(|c| IndexOptions::decode(c.value()))
            .ok_or_else(|| not("it has no options"))?;
        let s = db.snapshot()?;
        let layout = load(&table, &cen, &s)?;
        let index = Index {
            inner: Arc::new(Inner {
                db: db.clone(),
                table,
                spec,
                cen,
                opts,
                layout: RwLock::new(Arc::new(layout)),
                in_flight: Mutex::new(HashSet::new()),
                pending: Mutex::new(VecDeque::new()),
                signal: Arc::new(Signal::default()),
            }),
        };
        index.queue_out_of_bounds(&s)?;
        Ok(index)
    }

    /// The vectors' spec.
    pub fn spec(&self) -> VectorSpec {
        self.inner.spec
    }

    /// The options it was created with (the target list length as scaled).
    pub fn options(&self) -> IndexOptions {
        self.inner.opts
    }

    /// A list splits when it holds more than this many vectors.
    pub fn max_list_len(&self) -> u64 {
        2 * u64::from(self.inner.opts.target)
    }

    /// A list merges into its nearest neighbour when it holds fewer than this many
    /// vectors, unless it is the only list.
    pub fn min_list_len(&self) -> u64 {
        u64::from(self.inner.opts.target) / 4
    }

    /// Inserts the `f32` vector `vector` as `id`, or replaces `id`'s vector (an upsert).
    /// One transaction, retried on conflicts; a list that grows past its bound is split
    /// before the call returns. An error after the commit (from that split) leaves the
    /// insert committed, and retrying it is safe.
    pub fn insert_f32(&self, id: &[u8], vector: &[f32]) -> Result<()> {
        self.insert(id, Input::F32(vector))
    }

    /// [`insert_f32`](Self::insert_f32) for an `i8` index.
    pub fn insert_i8(&self, id: &[u8], vector: &[i8]) -> Result<()> {
        self.insert(id, Input::I8(vector))
    }

    /// [`insert_f32`](Self::insert_f32) for a bit index (bits packed eight to a byte, least
    /// significant first).
    pub fn insert_bits(&self, id: &[u8], vector: &[u8]) -> Result<()> {
        self.insert(id, Input::Bits(vector))
    }

    /// Deletes `id`'s vector; `false` if there was none. A list that shrinks below its
    /// bound is merged into its nearest neighbour before the call returns.
    pub fn delete(&self, id: &[u8]) -> Result<bool> {
        let inn = &*self.inner;
        let t = &inn.table;
        for attempt in 0..RELOADS {
            let r = inn.db.transact(&RetryPolicy::default(), |tx| {
                let Some(l) = read_list_of(tx, t, id)? else {
                    return Ok(None);
                };
                // A mapping to a list with no centroid can only be seen mid-merge: retry.
                if read_version(tx, t, l)?.is_none() {
                    return Err(Tx::Stale);
                }
                #[cfg(feature = "__unstable-hooks")]
                crate::hooks::hold(crate::hooks::Hold::DeleteBeforeCommit);
                tx.delete_column(t, &list_row(l), VEC, id)
                    .delete_column(t, &map_row(id), MAP, LIST)
                    .incr(t, &count_row(l), CNT, b"", -1);
                Ok(Some(l))
            });
            match r {
                Ok((None, _)) => return Ok(false),
                Ok((Some(l), _)) => {
                    self.after_write([l])?;
                    return Ok(true);
                }
                // Only a merge in flight shows a mapping to a list with no centroid: back off
                // (capped, as `RetryPolicy`'s pauses) rather than spin against it (#1268).
                Err(Tx::Stale) => std::thread::sleep(backoff(attempt)),
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::Conflict(
            "a delete kept finding its list mid-merge".into(),
        ))
    }

    /// The `k` vectors nearest the `f32` vector `query`, as a [`Search`] to run.
    pub fn search_f32<'a>(&'a self, query: &'a [f32], k: usize) -> Search<'a> {
        Search::new(self, Input::F32(query), k)
    }

    /// [`search_f32`](Self::search_f32) for an `i8` index.
    pub fn search_i8<'a>(&'a self, query: &'a [i8], k: usize) -> Search<'a> {
        Search::new(self, Input::I8(query), k)
    }

    /// [`search_f32`](Self::search_f32) for a bit index.
    pub fn search_bits<'a>(&'a self, query: &'a [u8], k: usize) -> Search<'a> {
        Search::new(self, Input::Bits(query), k)
    }

    /// Loads many vectors into an **empty** index faster than inserting them one by one: it
    /// clusters the first 50,000 vectors (or more if `expected` needs more lists) with
    /// k-means into about `expected / target` lists, writes those centroids, then streams
    /// every vector to its nearest list, a list's vectors written a batch per transaction.
    /// Lists left out of bounds are then split or merged as the index rebalances
    /// ([`Rebalance`]). `expected` is the number of vectors you're about to load, used only
    /// to choose the number of lists. The sample is the input's first vectors, so shuffle an
    /// input that is sorted by cluster. A repeated id keeps its last vector.
    ///
    /// Fails with `InvalidArgument` if the index already holds a list, or on the first
    /// vector of another shape; the vectors before that are loaded.
    pub fn bulk_load_f32<K, V>(
        &self,
        expected: u64,
        vectors: impl IntoIterator<Item = (K, V)>,
    ) -> Result<BulkStats>
    where
        K: AsRef<[u8]>,
        V: AsRef<[f32]>,
    {
        self.bulk(expected, vectors, |v: &V| Input::F32(v.as_ref()))
    }

    /// [`bulk_load_f32`](Self::bulk_load_f32) for an `i8` index.
    pub fn bulk_load_i8<K, V>(
        &self,
        expected: u64,
        vectors: impl IntoIterator<Item = (K, V)>,
    ) -> Result<BulkStats>
    where
        K: AsRef<[u8]>,
        V: AsRef<[i8]>,
    {
        self.bulk(expected, vectors, |v: &V| Input::I8(v.as_ref()))
    }

    /// [`bulk_load_f32`](Self::bulk_load_f32) for a bit index.
    pub fn bulk_load_bits<K, V>(
        &self,
        expected: u64,
        vectors: impl IntoIterator<Item = (K, V)>,
    ) -> Result<BulkStats>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.bulk(expected, vectors, |v: &V| Input::Bits(v.as_ref()))
    }

    fn bulk<K: AsRef<[u8]>, V>(
        &self,
        expected: u64,
        vectors: impl IntoIterator<Item = (K, V)>,
        input: for<'v> fn(&'v V) -> Input<'v>,
    ) -> Result<BulkStats> {
        let inn = &*self.inner;
        let t = &inn.table;
        let empty = || Error::InvalidArgument("bulk_load needs an empty index".into());
        if t.get(GLOBAL, META, NEXT)?.and_then(|c| c.as_u64()) != Some(0) {
            return Err(empty());
        }
        let target = u64::from(inn.opts.target);
        let want = usize::try_from(expected.div_ceil(target).max(1)).unwrap_or(usize::MAX);
        let mut it = vectors.into_iter();
        let sample: Vec<(K, V)> = it.by_ref().take(BULK_SAMPLE.max(want)).collect();
        if sample.is_empty() {
            return Ok(BulkStats {
                vectors: 0,
                lists: 0,
            });
        }
        let mut points = Vec::with_capacity(sample.len());
        for (_, v) in &sample {
            let v = input(v);
            v.check(&inn.spec)?;
            points.push(v.point());
        }
        let centroids = kmeans(&inn.cen, &points, want.min(sample.len()))?;
        drop(points);
        let lists = u32::try_from(centroids.len()).map_err(|_| corrupt("too many lists"))?;
        inn.db.transact(&RetryPolicy::default(), |tx| {
            if read_u64(tx, t, GLOBAL, META, NEXT)? != Some(0) {
                return Err(Tx::Fail(empty()));
            }
            for (i, c) in (0u32..).zip(&centroids) {
                put_centroid(tx, t, i, c);
                tx.put_u64(t, &centroid_row(i), META, VERSION, 1);
            }
            tx.put_u64(t, GLOBAL, META, NEXT, u64::from(lists))
                .put_u64(t, GLOBAL, META, LAYOUT, 1);
            Ok(())
        })?;
        let layout = self.reload()?;
        // Each list's unwritten batch, and which batch holds an id (a repeat replaces it).
        let mut batches: HashMap<u32, Vec<(K, V)>> = HashMap::new();
        let mut waiting: HashMap<Vec<u8>, u32> = HashMap::new();
        let mut loaded = 0u64;
        for (id, v) in sample.into_iter().chain(it) {
            let vi = input(&v);
            vi.check(&inn.spec)?;
            let l = layout.nearest(&inn.cen, &vi.point(), 1, &[])?[0];
            if let Some(prev) = waiting.insert(id.as_ref().to_vec(), l) {
                let b = batches.get_mut(&prev).expect("a waiting id's batch");
                b.retain(|(k, _)| k.as_ref() != id.as_ref());
            }
            let batch = batches.entry(l).or_default();
            batch.push((id, v));
            loaded += 1;
            if batch.len() >= BULK_BATCH {
                let full = std::mem::take(batch);
                for (k, _) in &full {
                    waiting.remove(k.as_ref());
                }
                self.write_batch(l, &full, input)?;
            }
        }
        for (l, batch) in batches {
            if !batch.is_empty() {
                self.write_batch(l, &batch, input)?;
            }
        }
        let all: Vec<u32> = layout.lists.iter().map(|l| l.id).collect();
        match inn.opts.rebalance {
            Rebalance::Inline => {
                self.queue(all);
                while self.maintain_one()?.is_some() {}
            }
            _ => self.after_write(all)?,
        }
        Ok(BulkStats {
            vectors: loaded,
            lists,
        })
    }

    /// One transaction writing `batch` into list `l`, each vector as an upsert would.
    fn write_batch<K: AsRef<[u8]>, V>(
        &self,
        l: u32,
        batch: &[(K, V)],
        input: for<'v> fn(&'v V) -> Input<'v>,
    ) -> Result<()> {
        let inn = &*self.inner;
        let t = &inn.table;
        let row = list_row(l);
        inn.db.transact(&RetryPolicy::default(), |tx| {
            if read_version(tx, t, l)?.is_none() {
                return Err(Tx::Fail(Error::Conflict(
                    "a list merged away during a bulk load".into(),
                )));
            }
            let mut added = 0i64;
            for (id, v) in batch {
                let id = id.as_ref();
                let old = read_list_of(tx, t, id)?;
                match old {
                    Some(o) if o == l => input(v).put(tx, t, &row, id),
                    Some(o) => {
                        tx.delete_column(t, &list_row(o), VEC, id)
                            .incr(t, &count_row(o), CNT, b"", -1)
                            .put_u64(t, &map_row(id), MAP, LIST, u64::from(l));
                        input(v).put(tx, t, &row, id);
                        added += 1;
                    }
                    None => {
                        tx.put_u64(t, &map_row(id), MAP, LIST, u64::from(l));
                        input(v).put(tx, t, &row, id);
                        added += 1;
                    }
                }
            }
            tx.incr(t, &count_row(l), CNT, b"", added);
            Ok(())
        })?;
        Ok(())
    }

    /// Checks the index's rows against each other at one snapshot: every id in exactly one
    /// list, its mapping naming that list, every list with a centroid, and every list's
    /// length counter equal to its vectors. Reads the whole index; for tests and repair
    /// tooling. A contradiction is `Corruption`.
    pub fn verify(&self) -> Result<IndexStats> {
        let inn = &*self.inner;
        let t = &inn.table;
        let s = inn.db.snapshot()?;
        let layout = load(t, &inn.cen, &s)?;
        let by_row: HashMap<u32, u32> = layout
            .lists
            .iter()
            .map(|l| (scramble(l.id), l.id))
            .collect();
        let mut lens: HashMap<u32, u64> = layout.lists.iter().map(|l| (l.id, 0)).collect();
        let mut home: HashMap<Vec<u8>, u32> = HashMap::new();
        let mut cells = t.scan_prefix(b"L").family(VEC).snapshot(&s).cells()?;
        while let Some((row, e)) = cells.next()? {
            let list = list_of(row)
                .and_then(|r| by_row.get(&r))
                .copied()
                .ok_or_else(|| corrupt("vectors in a list with no centroid"))?;
            if home.insert(e.qualifier.to_vec(), list).is_some() {
                return Err(Error::Corruption(format!(
                    "id {:?} is in two lists",
                    e.qualifier
                )));
            }
            *lens.get_mut(&list).expect("listed") += 1;
        }
        let mut mapped = 0usize;
        for row in t.scan_prefix(b"M").family(MAP).snapshot(&s).iter()? {
            let row = row?;
            let id = &row.key()[1..];
            let list = row.get(MAP, LIST).and_then(|c| c.as_u64());
            if list != home.get(id).map(|l| u64::from(*l)) {
                return Err(Error::Corruption(format!(
                    "id {id:?} maps to {list:?} but is in {:?}",
                    home.get(id)
                )));
            }
            mapped += 1;
        }
        if mapped != home.len() {
            return Err(corrupt("a vector has no mapping"));
        }
        for row in t.scan_prefix(b"N").family(CNT).snapshot(&s).iter()? {
            let row = row?;
            let list = list_of(row.key()).ok_or_else(|| corrupt("a count row key"))?;
            let n = row.get(CNT, b"").and_then(|c| c.as_i64()).unwrap_or(0);
            let want = lens.get(&list).copied();
            if want != Some(n.max(0) as u64) || n < 0 {
                return Err(Error::Corruption(format!(
                    "list {list} counts {n} but holds {want:?}"
                )));
            }
        }
        for l in &layout.lists {
            if lens[&l.id] > 0 && t.get_at(&s, &count_row(l.id), CNT, b"")?.is_none() {
                return Err(Error::Corruption(format!("list {} has no count", l.id)));
            }
        }
        Ok(IndexStats {
            list_lens: layout.lists.iter().map(|l| lens[&l.id]).collect(),
            vectors: home.len() as u64,
        })
    }

    fn insert(&self, id: &[u8], v: Input<'_>) -> Result<()> {
        v.check(&self.inner.spec)?;
        let point = v.point();
        let mut layout = self.layout();
        for _ in 0..RELOADS {
            let r = if layout.lists.is_empty() {
                self.bootstrap(id, v, &point)
            } else {
                self.place(&layout, id, v, &point)
            };
            match r {
                Ok(touched) => return self.after_write(touched),
                Err(Tx::Stale) => layout = self.reload()?,
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::Conflict(
            "the index's layout changed under every attempt of an insert".into(),
        ))
    }

    /// The first insert into an empty index: list 0, centred on the vector.
    fn bootstrap(
        &self,
        id: &[u8],
        v: Input<'_>,
        point: &Point,
    ) -> std::result::Result<Vec<u32>, Tx> {
        let inn = &*self.inner;
        let t = &inn.table;
        let centroid = unit(inn.cen.metric(), point.clone());
        inn.db.transact(&RetryPolicy::default(), |tx| {
            // Any list ever made means this cache is stale.
            if read_u64(tx, t, GLOBAL, META, NEXT)? != Some(0) {
                return Err(Tx::Stale);
            }
            put_centroid(tx, t, 0, &centroid);
            tx.put_u64(t, &centroid_row(0), META, VERSION, 1)
                .put_u64(t, GLOBAL, META, NEXT, 1)
                .put_u64(t, GLOBAL, META, LAYOUT, 1);
            v.put(tx, t, &list_row(0), id);
            tx.put_u64(t, &map_row(id), MAP, LIST, 0)
                .incr(t, &count_row(0), CNT, b"", 1);
            Ok(())
        })?;
        Ok(vec![0])
    }

    /// An insert or upsert into the list nearest `point` (D306 §2, rev 3's read sets): it
    /// reads `M{id}` and the versions of the lists it writes, never `N` or `G`.
    fn place(
        &self,
        layout: &Layout,
        id: &[u8],
        v: Input<'_>,
        point: &Point,
    ) -> std::result::Result<Vec<u32>, Tx> {
        let inn = &*self.inner;
        let t = &inn.table;
        let l = layout.nearest(&inn.cen, point, 1, &[])?[0];
        let want = layout.list(l).expect("a listed list").version;
        let (old, _) = inn.db.transact(&RetryPolicy::default(), |tx| {
            let old = read_list_of(tx, t, id)?;
            // Absent: merged away (M1). Another version: the cache predates a split or a
            // reassignment, so place against the current centroids (rev 2 nit 3).
            if read_version(tx, t, l)? != Some(want) {
                return Err(Tx::Stale);
            }
            match old {
                // An upsert within its list: the vector only (M2).
                Some(o) if o == l => v.put(tx, t, &list_row(l), id),
                Some(o) => {
                    if read_version(tx, t, o)?.is_none() {
                        return Err(Tx::Stale);
                    }
                    tx.delete_column(t, &list_row(o), VEC, id);
                    v.put(tx, t, &list_row(l), id);
                    tx.put_u64(t, &map_row(id), MAP, LIST, u64::from(l))
                        .incr(t, &count_row(o), CNT, b"", -1)
                        .incr(t, &count_row(l), CNT, b"", 1);
                }
                None => {
                    v.put(tx, t, &list_row(l), id);
                    tx.put_u64(t, &map_row(id), MAP, LIST, u64::from(l)).incr(
                        t,
                        &count_row(l),
                        CNT,
                        b"",
                        1,
                    );
                }
            }
            #[cfg(feature = "__unstable-hooks")]
            crate::hooks::hold(crate::hooks::Hold::PlaceBeforeCommit);
            Ok(old)
        })?;
        Ok(match old {
            Some(o) if o != l => vec![l, o],
            _ => vec![l],
        })
    }

    fn layout(&self) -> Arc<Layout> {
        Arc::clone(
            &self
                .inner
                .layout
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Loads the centroids at a fresh snapshot into the shared cache.
    fn reload(&self) -> Result<Arc<Layout>> {
        let inn = &*self.inner;
        let s = inn.db.snapshot()?;
        let fresh = Arc::new(load(&inn.table, &inn.cen, &s)?);
        let mut shared = inn.layout.write().unwrap_or_else(PoisonError::into_inner);
        if fresh.seqno >= shared.seqno {
            *shared = Arc::clone(&fresh);
        }
        Ok(fresh)
    }

    /// The centroids at `s`: the shared cache when it holds `s`'s layout, else loaded. Only
    /// a search at the current snapshot replaces the shared cache (rev 3 S4).
    fn layout_at(&self, s: &Snapshot, current: bool) -> Result<Arc<Layout>> {
        let inn = &*self.inner;
        let shared = self.layout();
        let version = inn
            .table
            .get_at(s, GLOBAL, META, LAYOUT)?
            .and_then(|c| c.as_u64())
            .unwrap_or(0);
        if version == shared.version {
            return Ok(shared);
        }
        let fresh = Arc::new(load(&inn.table, &inn.cen, s)?);
        if current {
            let mut w = inn.layout.write().unwrap_or_else(PoisonError::into_inner);
            if fresh.seqno >= w.seqno {
                *w = Arc::clone(&fresh);
            }
        }
        Ok(fresh)
    }

    fn count(&self, list: u32) -> Result<u64> {
        let n = self
            .inner
            .table
            .get(&count_row(list), CNT, b"")?
            .and_then(|c| c.as_i64())
            .unwrap_or(0);
        Ok(n.max(0) as u64)
    }

    fn queue(&self, lists: impl IntoIterator<Item = u32>) {
        let mut q = self
            .inner
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for l in lists {
            if !q.contains(&l) {
                q.push_back(l);
            }
        }
    }

    /// Queues every list outside its bounds at `s` (`open`'s scan, rev 3 M3).
    fn queue_out_of_bounds(&self, s: &Snapshot) -> Result<()> {
        let t = &self.inner.table;
        let lists = self.layout().lists.len();
        let mut out = Vec::new();
        for row in t.scan_prefix(b"N").family(CNT).snapshot(s).iter()? {
            let row = row?;
            let list = list_of(row.key()).ok_or_else(|| corrupt("a count row key"))?;
            let n = row
                .get(CNT, b"")
                .and_then(|c| c.as_i64())
                .unwrap_or(0)
                .max(0) as u64;
            if n > self.max_list_len() || (n < self.min_list_len() && lists > 1) {
                out.push(list);
            }
        }
        // Background markers: a hint, re-checked before any work (rev 3 M3).
        for row in t.scan_prefix(b"Q").family(META).snapshot(s).iter()? {
            let row = row?;
            out.push(list_of(row.key()).ok_or_else(|| corrupt("a marker row key"))?);
        }
        self.queue(out);
        Ok(())
    }

    /// Splits or merges queued lists that are outside their bounds, a bounded number per
    /// call (a bound check, never an edge: rev 3 M3).
    fn maintain(&self) -> Result<()> {
        for _ in 0..MAINTENANCE_PER_CALL {
            if self.maintain_one()?.is_none() {
                break;
            }
        }
        Ok(())
    }

    /// Checks the next queued list against its bounds, and splits or merges it: `None` when
    /// nothing is queued, else whether it was out of bounds. In background mode a list found
    /// within its bounds has its stale marker removed. An error puts the list back.
    pub(crate) fn maintain_one(&self) -> Result<Option<bool>> {
        let next = self
            .inner
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        let Some(l) = next else {
            return Ok(None);
        };
        let r = (|| {
            let n = self.count(l)?;
            if n > self.max_list_len() {
                self.split(l)?;
                Ok(true)
            } else if n < self.min_list_len() && self.layout().lists.len() > 1 {
                self.merge(l)?;
                Ok(true)
            } else {
                if self.inner.opts.rebalance == Rebalance::Background {
                    self.inner
                        .table
                        .mutate(&queue_row(l))
                        .delete_row()
                        .commit()?;
                }
                Ok(false)
            }
        })();
        if r.is_err() {
            self.queue([l]);
        }
        r.map(Some)
    }

    /// After a write committed into `lists`: rebalance them now ([`Rebalance::Inline`]), or
    /// mark those out of bounds for the rebalancer ([`Rebalance::Background`]): a `Q` row in
    /// its own transaction (rev 4 N2) and the in-process queue. A list already queued here
    /// isn't marked again.
    fn after_write(&self, lists: impl IntoIterator<Item = u32>) -> Result<()> {
        #[cfg(feature = "__unstable-hooks")]
        if crate::hooks::skipping_rebalance() {
            return Ok(());
        }
        if self.inner.opts.rebalance == Rebalance::Inline {
            self.queue(lists);
            return self.maintain();
        }
        let mut marked = false;
        for l in lists {
            let queued = self
                .inner
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&l);
            if queued {
                continue;
            }
            let n = self.count(l)?;
            let several = self.layout().lists.len() > 1;
            if n > self.max_list_len() || (n < self.min_list_len() && several) {
                self.inner
                    .table
                    .mutate(&queue_row(l))
                    .put_u64(META, MARK, 1)
                    .commit()?;
                self.queue([l]);
                marked = true;
            }
        }
        if marked {
            self.inner.signal.notify();
        }
        Ok(())
    }

    /// Marks `list` in flight in this process, or `None` if it already is (rev 4).
    fn claim(&self, list: u32) -> Option<Claim<'_>> {
        let set = &self.inner.in_flight;
        // The lock is released before a `Claim` exists: its drop takes the lock again.
        let fresh = set
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(list);
        fresh.then(|| Claim { set, list })
    }

    /// The ids and points of `list` at `s`, in id order.
    fn members(&self, s: &Snapshot, list: u32) -> Result<(Vec<Vec<u8>>, Vec<Point>)> {
        let mut cells = self
            .inner
            .table
            .row(&list_row(list))
            .family(VEC)
            .snapshot(s)
            .cells()?;
        let (mut ids, mut points) = (Vec::new(), Vec::new());
        while let Some(e) = cells.next()? {
            let v = e
                .cell
                .as_vector()
                .ok_or_else(|| corrupt("a list column is not a vector"))?;
            ids.push(e.qualifier.to_vec());
            points.push(Point::of(v));
        }
        Ok((ids, points))
    }

    /// Splits `list` (D306 §3): 2-means on a snapshot read, then one transaction moving
    /// what it reads itself (rev 3 S1, rev 4 N1); then phase 2 over its neighbours.
    fn split(&self, list: u32) -> Result<()> {
        let Some(_claim) = self.claim(list) else {
            return Ok(());
        };
        for _ in 0..ATTEMPTS {
            // The loser of a duplicate split re-checks the length before splitting the
            // fresh half (rev 3 S2).
            if self.count(list)? <= self.max_list_len() {
                return Ok(());
            }
            let s0 = self.inner.db.snapshot()?;
            let mut layout = self.layout();
            if layout.list(list).is_none() {
                layout = self.reload()?;
            }
            let Some(current) = layout.list(list) else {
                return Ok(());
            };
            let (ids, points) = self.members(&s0, list)?;
            if ids.len() as u64 <= self.max_list_len() {
                return Ok(());
            }
            #[cfg(feature = "__unstable-hooks")]
            crate::hooks::hold(crate::hooks::Hold::SplitBeforeCommit);
            let min = usize::try_from(self.min_list_len()).unwrap_or(usize::MAX);
            let plan = two_means(&self.inner.cen, &points, &current.point, min)?;
            let moving: Vec<&[u8]> = plan.moving.iter().map(|i| ids[*i].as_slice()).collect();
            match self.split_commit(list, &plan, &moving) {
                Ok(new) => {
                    let layout = self.reload()?;
                    self.queue([list, new]);
                    #[cfg(feature = "__unstable-hooks")]
                    if crate::hooks::failing_after_split() {
                        // A crash between the split's commit and its phase 2.
                        return Ok(());
                    }
                    return self.reassign(&layout, list, new, &plan);
                }
                Err(Tx::Stale) => return Ok(()),
                Err(e) if is_conflict(&e) => {}
                Err(e) => return Err(e.into()),
            }
        }
        // Lost every attempt: the next write into the list triggers it again.
        Ok(())
    }

    fn split_commit(
        &self,
        list: u32,
        plan: &Plan,
        moving: &[&[u8]],
    ) -> std::result::Result<u32, Tx> {
        let inn = &*self.inner;
        let t = &inn.table;
        let (new, _) = inn
            .db
            .transact(&RetryPolicy::default().max_attempts(1), |tx| {
                let Some(version) = read_version(tx, t, list)? else {
                    return Err(Tx::Stale);
                };
                let next = read_u64(tx, t, GLOBAL, META, NEXT)?
                    .ok_or_else(|| corrupt("no next list id"))?;
                let layout = read_u64(tx, t, GLOBAL, META, LAYOUT)?.unwrap_or(0);
                let new = u32::try_from(next).map_err(|_| corrupt("list ids exhausted"))?;
                let (from, to) = (list_row(list), list_row(new));
                let mut moved = 0i64;
                for id in moving {
                    // Deleted or moved away since the 2-means snapshot: skip it (rev 4 N1).
                    let Some(stored) = read_stored(tx, t, &from, id)? else {
                        continue;
                    };
                    stored.input().put(tx, t, &to, id);
                    tx.delete_column(t, &from, VEC, id).put_u64(
                        t,
                        &map_row(id),
                        MAP,
                        LIST,
                        u64::from(new),
                    );
                    moved += 1;
                }
                if moved == 0 {
                    return Err(Tx::Stale);
                }
                put_centroid(tx, t, list, &plan.stay);
                put_centroid(tx, t, new, &plan.go);
                tx.put_u64(t, &centroid_row(list), META, VERSION, version + 1)
                    .put_u64(t, &centroid_row(new), META, VERSION, 1)
                    .incr(t, &count_row(list), CNT, b"", -moved)
                    .incr(t, &count_row(new), CNT, b"", moved)
                    .put_u64(t, GLOBAL, META, NEXT, next + 1)
                    .put_u64(t, GLOBAL, META, LAYOUT, layout + 1);
                if inn.opts.rebalance == Rebalance::Background {
                    tx.delete_row(t, &queue_row(list));
                }
                Ok(new)
            })?;
        Ok(new)
    }

    /// Phase 2 (SPFresh's LIRE): each of the split's nearest neighbour lists gives up the
    /// vectors now nearer one of the two new centroids, one transaction per neighbour that
    /// bumps the neighbour's version (rev 3 S3). A conflict skips that neighbour: this is
    /// clustering, not correctness.
    fn reassign(&self, layout: &Layout, list: u32, new: u32, plan: &Plan) -> Result<()> {
        let inn = &*self.inner;
        let t = &inn.table;
        let r = inn.opts.reassign as usize;
        if r == 0 {
            return Ok(());
        }
        let dim = inn.cen.dim();
        let (stay, go) = (plan.stay.to_vector(dim)?, plan.go.to_vector(dim)?);
        for nbr in layout.nearest(&inn.cen, &plan.stay, r, &[list, new])? {
            let Some(n) = layout.list(nbr) else {
                continue;
            };
            let s = inn.db.snapshot()?;
            let (ids, points) = self.members(&s, nbr)?;
            let mut moves: Vec<(&[u8], u32)> = Vec::new();
            for (id, p) in ids.iter().zip(&points) {
                let here = distance(&inn.cen, p, &n.centroid)?;
                let (ds, dg) = (distance(&inn.cen, p, &stay)?, distance(&inn.cen, p, &go)?);
                let (d, dest) = if dg < ds { (dg, new) } else { (ds, list) };
                if d < here {
                    moves.push((id, dest));
                }
            }
            if moves.is_empty() {
                continue;
            }
            let done = inn
                .db
                .transact(&RetryPolicy::default().max_attempts(1), |tx| {
                    let Some(version) = read_version(tx, t, nbr)? else {
                        return Err(Tx::Stale);
                    };
                    if read_version(tx, t, list)?.is_none() || read_version(tx, t, new)?.is_none() {
                        return Err(Tx::Stale);
                    }
                    let from = list_row(nbr);
                    let mut gained: HashMap<u32, i64> = HashMap::new();
                    for (id, dest) in &moves {
                        let Some(stored) = read_stored(tx, t, &from, id)? else {
                            continue;
                        };
                        stored.input().put(tx, t, &list_row(*dest), id);
                        tx.delete_column(t, &from, VEC, id).put_u64(
                            t,
                            &map_row(id),
                            MAP,
                            LIST,
                            u64::from(*dest),
                        );
                        *gained.entry(*dest).or_default() += 1;
                    }
                    let lost: i64 = gained.values().sum();
                    if lost == 0 {
                        return Err(Tx::Stale);
                    }
                    tx.incr(t, &count_row(nbr), CNT, b"", -lost).put_u64(
                        t,
                        &centroid_row(nbr),
                        META,
                        VERSION,
                        version + 1,
                    );
                    for (dest, n) in &gained {
                        tx.incr(t, &count_row(*dest), CNT, b"", *n);
                    }
                    #[cfg(feature = "__unstable-hooks")]
                    crate::hooks::hold(crate::hooks::Hold::ReassignBeforeCommit);
                    Ok(())
                });
            match done {
                Ok(_) => self.queue([nbr, list, new]),
                Err(Tx::Stale) => {}
                Err(e) if is_conflict(&e) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Merges `list` into its nearest neighbour (D306 §3): one transaction that reads the
    /// list row whole and moves every vector (rev 3's table).
    fn merge(&self, list: u32) -> Result<()> {
        let Some(_claim) = self.claim(list) else {
            return Ok(());
        };
        let inn = &*self.inner;
        let t = &inn.table;
        for _ in 0..ATTEMPTS {
            let mut layout = self.layout();
            if layout.list(list).is_none() {
                layout = self.reload()?;
            }
            let Some(src) = layout.list(list) else {
                return Ok(());
            };
            if layout.lists.len() <= 1 || self.count(list)? >= self.min_list_len() {
                return Ok(());
            }
            let Some(&dst) = layout.nearest(&inn.cen, &src.point, 1, &[list])?.first() else {
                return Ok(());
            };
            let r = inn
                .db
                .transact(&RetryPolicy::default().max_attempts(1), |tx| {
                    if read_version(tx, t, list)?.is_none() {
                        return Ok(false);
                    }
                    // A destination merged away meanwhile: reload and choose again (M1).
                    let Some(version) = read_version(tx, t, dst)? else {
                        return Err(Tx::Stale);
                    };
                    let from = list_row(list);
                    let moving: Vec<(Vec<u8>, Stored)> = match tx.get_row(t, &from, &[VEC])? {
                        None => Vec::new(),
                        Some(row) => row
                            .iter()
                            .map(|e| {
                                let v = e
                                    .cell
                                    .as_vector()
                                    .ok_or_else(|| corrupt("a list column is not a vector"))?;
                                Ok((e.qualifier.to_vec(), Stored::of(v)?))
                            })
                            .collect::<Result<_>>()?,
                    };
                    let to = list_row(dst);
                    for (id, stored) in &moving {
                        stored.input().put(tx, t, &to, id);
                        tx.put_u64(t, &map_row(id), MAP, LIST, u64::from(dst));
                    }
                    let layout = read_u64(tx, t, GLOBAL, META, LAYOUT)?.unwrap_or(0);
                    tx.delete_family(t, &from, VEC)
                        .delete_row(t, &centroid_row(list))
                        .delete_row(t, &count_row(list))
                        .incr(t, &count_row(dst), CNT, b"", moving.len() as i64)
                        .put_u64(t, &centroid_row(dst), META, VERSION, version + 1)
                        .put_u64(t, GLOBAL, META, LAYOUT, layout + 1);
                    if inn.opts.rebalance == Rebalance::Background {
                        tx.delete_row(t, &queue_row(list));
                    }
                    Ok(true)
                });
            match r {
                Ok((merged, _)) => {
                    if merged {
                        self.reload()?;
                        self.queue([dst]);
                    }
                    return Ok(());
                }
                Err(Tx::Stale) => {
                    self.reload()?;
                }
                Err(e) if is_conflict(&e) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

/// A nearest-neighbour search under construction ([`Index::search_f32`] and its `i8` and
/// bit forms). Finish with [`run`](Search::run).
#[derive(Debug)]
#[must_use = "a search does nothing until .run()"]
pub struct Search<'a> {
    index: &'a Index,
    query: Input<'a>,
    k: usize,
    nprobe: Option<u32>,
    snapshot: Option<&'a Snapshot>,
    parallel: usize,
}

impl<'a> Search<'a> {
    fn new(index: &'a Index, query: Input<'a>, k: usize) -> Self {
        Self {
            index,
            query,
            k,
            nprobe: None,
            snapshot: None,
            parallel: 1,
        }
    }

    /// Probe the `n` lists whose centroids are nearest the query (at least 1; the index's
    /// [`IndexOptions::nprobe`] by default). More lists, better recall, more work; with
    /// every list probed the search is exact.
    pub fn nprobe(mut self, n: u32) -> Self {
        self.nprobe = Some(n);
        self
    }

    /// Search at `snapshot`: the centroids, the lists and their vectors as of it.
    pub fn snapshot(mut self, snapshot: &'a Snapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    /// Read the probed lists on up to `n` threads owned by the call (default 1). The
    /// result is the same at any `n`.
    pub fn parallel(mut self, n: usize) -> Self {
        self.parallel = n;
        self
    }

    /// Runs the search: at most `k` hits, nearest first. One snapshot covers the centroids
    /// and the lists, so a concurrent split never shows a vector twice or hides it.
    ///
    /// Hits at an equal distance come back by the list that holds them, then by id. A split
    /// can move a tied id to another list, so which of several ids tied at the `k`-th
    /// distance makes the cut can differ between layouts; at one snapshot it's fixed.
    pub fn run(self) -> Result<Vec<Hit>> {
        let index = self.index;
        let inn = &*index.inner;
        self.query.check(&inn.spec)?;
        let taken;
        let s = match self.snapshot {
            Some(s) => s,
            None => {
                taken = inn.db.snapshot()?;
                &taken
            }
        };
        let layout = index.layout_at(s, self.snapshot.is_none())?;
        if layout.lists.is_empty() || self.k == 0 {
            return Ok(Vec::new());
        }
        let probe = self.nprobe.unwrap_or(inn.opts.nprobe).max(1) as usize;
        let lists = layout.nearest(&inn.cen, &self.query.point(), probe, &[])?;
        let keys = KeySet::from_unsorted(lists.iter().map(|l| list_row(*l)));
        let read = inn.table.keys(&keys).family(VEC).snapshot(s);
        let near = match self.query {
            Input::F32(q) => read.nearest_f32(q, self.k),
            Input::I8(q) => read.nearest_i8(q, self.k),
            Input::Bits(q) => read.nearest_bits(q, self.k),
        };
        Ok(near
            .parallel(self.parallel)
            .run()?
            .into_iter()
            .map(|n| Hit {
                id: n.qualifier,
                distance: n.distance,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_round_trip_and_scale_for_large_vectors() {
        let o = IndexOptions::new()
            .target_list_len(7)
            .nprobe(3)
            .reassign_neighbours(0);
        assert_eq!(IndexOptions::decode(&o.encode()), Some(o));
        assert_eq!(IndexOptions::decode(&[2]), None);
        let wide = VectorSpec::new(4096, Element::F32, pigeonhole::Metric::L2).unwrap();
        // 2 × target × 16 KiB ≤ 4 MiB.
        assert_eq!(IndexOptions::new().checked(&wide).unwrap().target, 128);
        assert!(
            IndexOptions::new()
                .target_list_len(3)
                .checked(&wide)
                .is_err()
        );
    }
}
