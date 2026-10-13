//! Operation generators for every [`WorkloadKind`].
//!
//! Data model per workload (families are the store-neutral names every runner creates):
//!
//! | Workload | Rows | Family | Qualifiers |
//! |---|---|---|---|
//! | YCSB A–F | `user<fnv(i)>` (YCSB key order) | `ycsb` | `field0`..`field9` |
//! | sparse-wide | `sw:<i>` | `attr` | Zipfian over a 10K vocabulary, ~20 per row |
//! | time-series | `ts:<entity>:<reversed time>` (newest first), event-time timestamps | `metric` (TTL) | `v` |
//! | adjacency | `v:<vertex>` | `edge` | `edge:<dst>`, power-law degree |
//! | skewed multi-shard | `sk:<fnv(i)>` | `ycsb` | `field0` |
//! | cross-shard contention | `xs:<partition>:<fnv(i)>`, in partitions | `ycsb` | `field0` |
//! | counters | `ctr:<fnv(i)>` | each store's counter (below) | one counter per row |
//! | transfer | `acct:<fnv(i)>` | `ycsb` | `bal`, an `i64` balance |
//! | sparse-wide deletes | as sparse-wide | `attr` | as sparse-wide |
//! | adjacency prefix, time-series range | as adjacency, as time series | as theirs | as theirs |

use std::collections::BTreeSet;

use crate::rng::{Rng, Zipf, fnv64};
use crate::{BenchOp, PartitionRow, WorkloadConfig, WorkloadKind};

/// Family of the YCSB and skewed workloads.
pub const YCSB_FAMILY: &str = "ycsb";
/// Family of the sparse-wide workload.
pub const SPARSE_FAMILY: &str = "attr";
/// Family of the time-series workload; runners give it a TTL ([`TIME_SERIES_TTL`]).
pub const METRIC_FAMILY: &str = "metric";
/// Family of the adjacency workload.
pub const EDGE_FAMILY: &str = "edge";
/// Every family a runner must create, in a fixed order.
pub const FAMILIES: [&str; 4] = [YCSB_FAMILY, SPARSE_FAMILY, METRIC_FAMILY, EDGE_FAMILY];
/// TTL of [`METRIC_FAMILY`]. The time-series workload loads event timestamps that straddle
/// it: a quarter of the loaded points are already expired, and
/// every engine must skip them on read.
pub const TIME_SERIES_TTL: std::time::Duration = std::time::Duration::from_secs(86_400);

/// How long after [`WorkloadConfig::epoch_micros`] the oldest live time-series point still
/// has before it expires. A run that reads later than this after its epoch fails.
pub(crate) const LIVE_MARGIN: std::time::Duration = std::time::Duration::from_secs(6 * 3600);

/// YCSB fields per record.
pub(crate) const YCSB_FIELDS: u64 = 10;
/// Cells each [`WorkloadKind::GroupCommit`](crate::WorkloadKind::GroupCommit) operation
/// commits, all in one row: a small row mutation. Cells per second is operations per
/// second times this.
pub const GROUP_COMMIT_CELLS: u64 = 4;
/// Rows of [`WorkloadKind::GroupCommit`](crate::WorkloadKind::GroupCommit) at every preset
/// but smoke: commits overwrite them, so the store stays small and the run measures the
/// commit path, not flush and compaction. The load commits with the run's durability, so
/// it stays short.
pub const GROUP_COMMIT_ROWS: u64 = 1_000;
/// Client threads `phdb-bench group-commit` sweeps when `--threads` is not given.
pub const GROUP_COMMIT_THREADS: [usize; 3] = [1, 4, 16];
/// Partitions of [`WorkloadKind::CrossShardContention`](crate::WorkloadKind::CrossShardContention)
/// (D246): one table each on Pigeonhole, so with tablet changes off they sit one per shard
/// on the gate's 16-shard machine.
pub const XSHARD_PARTITIONS: usize = 16;
/// Client threads of [`WorkloadKind::CrossShardContention`](crate::WorkloadKind::CrossShardContention)
/// at the small and full presets (D246): two per shard on the gate's machine.
pub const XSHARD_CLIENTS: usize = 32;
/// The share of [`WorkloadKind::CrossShardContention`](crate::WorkloadKind::CrossShardContention)'s
/// commits that write one row in each of 2-4 partitions; the rest write one row (D246).
pub const XSHARD_CROSS_FRACTION: f64 = 0.5;
/// Client threads of [`WorkloadKind::Counters`](crate::WorkloadKind::Counters) at the small
/// and full presets (D247).
pub const COUNTER_THREADS: usize = 16;
/// [`WorkloadKind::Counters`](crate::WorkloadKind::Counters): the share of increments; the
/// rest read a counter.
const COUNTER_INCR_FRACTION: f64 = 0.9;
/// The operation type ([`BenchOp::type_name`]) of a [`BenchOp::Commit`] of more than one row:
/// the commit D246 judges.
pub const CROSS_SHARD_COMMIT: &str = "cross-shard commit";
/// YCSB E: scan lengths are uniform in `1..=YCSB_MAX_SCAN`.
const YCSB_MAX_SCAN: u64 = 100;
/// Sparse-wide qualifier vocabulary.
pub(crate) const SPARSE_VOCABULARY: u64 = 10_000;
/// Sparse-wide: cells per row are uniform in `0..=2 * SPARSE_MEAN_CELLS`.
const SPARSE_MEAN_CELLS: u64 = 20;
/// [`WorkloadKind::TimeSeriesRange`](crate::WorkloadKind::TimeSeriesRange)'s scans read the
/// points from 12 to 6 hours before the epoch: older than the newest ten (about 14 minutes
/// apart), so the scan skips the newer points to reach them.
const TIME_RANGE: (std::time::Duration, std::time::Duration) = (
    std::time::Duration::from_secs(12 * 3600),
    std::time::Duration::from_secs(6 * 3600),
);
/// Time series: points loaded per entity.
const POINTS_PER_ENTITY: u64 = 100;
/// Adjacency: out-degree is `1 + Zipf(ADJ_MAX_DEGREE)`; mean about 40.
const ADJ_MAX_DEGREE: u64 = 256;
const ADJ_MEAN_DEGREE: u64 = 40;

const RUN_STREAM: u64 = 0x5DEE_CE66_D1CE_4E5B;

/// Generator state shared by the load and run iterators.
#[derive(Debug)]
pub(crate) struct Gen {
    pub(crate) config: WorkloadConfig,
    /// Request distribution over the loaded items (rows, entities or vertices).
    items: Zipf,
    /// Number of loaded items.
    n_items: u64,
    /// Load operations (one per item, or per point for the time series).
    n_load: u64,
    /// Sparse-wide: qualifier popularity. Adjacency: out-degree.
    aux: Zipf,
    /// The workload's "now" in µs ([`WorkloadConfig::epoch_micros`], resolved).
    epoch: u64,
}

impl Gen {
    pub(crate) fn new(config: WorkloadConfig) -> Self {
        let n_items = match config.kind {
            WorkloadKind::TimeSeriesTtl | WorkloadKind::TimeSeriesRange => {
                (config.records / POINTS_PER_ENTITY).max(1)
            }
            WorkloadKind::Adjacency | WorkloadKind::AdjacencyPrefix => {
                (config.records / ADJ_MEAN_DEGREE).max(1)
            }
            _ => config.records.max(1),
        };
        let aux = match config.kind {
            WorkloadKind::SparseWide | WorkloadKind::SparseWideDeletes => {
                Zipf::new(SPARSE_VOCABULARY)
            }
            WorkloadKind::Adjacency | WorkloadKind::AdjacencyPrefix => Zipf::new(ADJ_MAX_DEGREE),
            _ => Zipf::new(1),
        };
        let n_load = match config.kind {
            WorkloadKind::TimeSeriesTtl | WorkloadKind::TimeSeriesRange => {
                n_items * POINTS_PER_ENTITY
            }
            _ => n_items,
        };
        let epoch = match config.epoch_micros {
            0 => crate::runners::now_micros(),
            e => e,
        };
        Self {
            items: Zipf::new(n_items),
            n_items,
            n_load,
            aux,
            epoch,
            config,
        }
    }

    /// Event time (µs) of point `t` of an entity.
    ///
    /// The oldest quarter of the loaded points `0..POINTS_PER_ENTITY` is older than
    /// [`TIME_SERIES_TTL`] by at least one step (about 14 minutes) and stays expired. The
    /// other 75 are one step apart, the oldest [`LIVE_MARGIN`] short of expiring and the
    /// newest half a step before `epoch`. Ageing is the only drift, so a run can lose live
    /// points only by outlasting the margin; `Gen::check_epoch_age`
    /// fails the run when it does. Points appended during the run are
    /// `epoch` plus one microsecond per point.
    fn point_ts(&self, t: u64) -> u64 {
        let ttl = TIME_SERIES_TTL.as_micros() as u64;
        let step = (ttl - LIVE_MARGIN.as_micros() as u64) / 75;
        let first_live = POINTS_PER_ENTITY / 4;
        if t < first_live {
            self.epoch - ttl - (first_live - t) * step
        } else if t < POINTS_PER_ENTITY {
            self.epoch - (POINTS_PER_ENTITY - 1 - t) * step - step / 2
        } else {
            self.epoch + (t - POINTS_PER_ENTITY)
        }
    }

    /// Fails when the run has outlasted [`LIVE_MARGIN`] since the epoch: time-series
    /// points may then have expired mid-run, at different moments in each engine, and the
    /// results no longer compare. Other workloads have no TTL to drift against.
    pub(crate) fn check_epoch_age(&self) -> Result<(), String> {
        let age = crate::runners::now_micros().saturating_sub(self.epoch);
        let ts = matches!(
            self.config.kind,
            WorkloadKind::TimeSeriesTtl | WorkloadKind::TimeSeriesRange
        );
        if ts && age > LIVE_MARGIN.as_micros() as u64 {
            return Err(format!(
                "time-series-ttl ran {}s past its epoch, longer than the {}s its live points \
                 have before they expire; engines may disagree on which cells are live. \
                 Use a smaller --scale or fewer --records/--ops",
                age / 1_000_000,
                LIVE_MARGIN.as_secs()
            ));
        }
        Ok(())
    }

    pub(crate) fn load(&self) -> LoadIter<'_> {
        LoadIter {
            g: self,
            rng: Rng::new(self.config.seed),
            i: 0,
        }
    }

    pub(crate) fn run(&self) -> RunIter<'_> {
        let clocks = if matches!(
            self.config.kind,
            WorkloadKind::TimeSeriesTtl | WorkloadKind::TimeSeriesRange
        ) {
            vec![POINTS_PER_ENTITY; self.n_items as usize]
        } else {
            Vec::new()
        };
        RunIter {
            g: self,
            rng: Rng::new(self.config.seed ^ RUN_STREAM),
            done: 0,
            inserted: self.config.records,
            clocks,
        }
    }

    /// A [`WorkloadKind::GroupCommit`](crate::WorkloadKind::GroupCommit) row mutation.
    fn commit(&self, row: Vec<u8>, rng: &mut Rng) -> BenchOp {
        BenchOp::Put {
            row,
            family: YCSB_FAMILY,
            cells: (0..GROUP_COMMIT_CELLS)
                .map(|f| (field(f), self.value(rng)))
                .collect(),
        }
    }

    fn value(&self, rng: &mut Rng) -> Vec<u8> {
        rng.bytes(self.config.value_len)
    }

    /// Rows per partition of the cross-shard contention workload: the loaded rows, dealt
    /// round-robin over the partitions.
    fn rows_per_partition(&self) -> u64 {
        (self.n_items / XSHARD_PARTITIONS as u64).max(1)
    }

    /// One row of a cross-shard contention commit: row `i` of `partition`, one cell.
    fn partition_row(&self, partition: usize, i: u64, rng: &mut Rng) -> PartitionRow {
        PartitionRow {
            partition,
            row: partition_key(partition, i),
            cells: vec![(field(0), self.value(rng))],
        }
    }
}

pub(crate) fn ycsb_key(i: u64) -> Vec<u8> {
    format!("user{:020}", fnv64(i)).into_bytes()
}

fn field(i: u64) -> Vec<u8> {
    format!("field{i}").into_bytes()
}

fn skewed_key(i: u64) -> Vec<u8> {
    format!("sk:{:020}", fnv64(i)).into_bytes()
}

fn partition_key(partition: usize, i: u64) -> Vec<u8> {
    format!("xs:{partition:02}:{:020}", fnv64(i)).into_bytes()
}

fn account_key(i: u64) -> Vec<u8> {
    format!("acct:{:020}", fnv64(i)).into_bytes()
}

/// Every account's balance after the transfer workload's load.
pub(crate) const OPENING_BALANCE: i64 = 1_000_000;

fn counter_key(i: u64) -> Vec<u8> {
    format!("ctr:{:020}", fnv64(i)).into_bytes()
}

fn sparse_key(i: u64) -> Vec<u8> {
    format!("sw:{i:010}").into_bytes()
}

fn sparse_qualifier(q: u64) -> Vec<u8> {
    format!("q{q:05}").into_bytes()
}

fn entity_prefix(e: u64) -> String {
    format!("ts:{e:08}:")
}

fn point_key(e: u64, t: u64) -> Vec<u8> {
    // Reversed time so a scan from the entity prefix returns the newest points first.
    format!("{}{:016x}", entity_prefix(e), u64::MAX - t).into_bytes()
}

fn vertex_key(v: u64) -> Vec<u8> {
    format!("v:{v:010}").into_bytes()
}

fn edge_qualifier(dst: u64) -> Vec<u8> {
    format!("edge:{dst:010}").into_bytes()
}

/// Iterator over the load phase.
#[derive(Debug)]
pub(crate) struct LoadIter<'a> {
    g: &'a Gen,
    rng: Rng,
    i: u64,
}

impl Iterator for LoadIter<'_> {
    type Item = BenchOp;

    fn next(&mut self) -> Option<BenchOp> {
        let g = self.g;
        let rng = &mut self.rng;
        loop {
            let i = self.i;
            if i >= g.n_load {
                return None;
            }
            self.i += 1;
            let op = match g.config.kind {
                WorkloadKind::YcsbA
                | WorkloadKind::YcsbB
                | WorkloadKind::YcsbC
                | WorkloadKind::YcsbD
                | WorkloadKind::YcsbE
                | WorkloadKind::YcsbF => BenchOp::Put {
                    row: ycsb_key(i),
                    family: YCSB_FAMILY,
                    cells: (0..YCSB_FIELDS).map(|f| (field(f), g.value(rng))).collect(),
                },
                WorkloadKind::SkewedMultiShard => BenchOp::Put {
                    row: skewed_key(i),
                    family: YCSB_FAMILY,
                    cells: vec![(field(0), g.value(rng))],
                },
                WorkloadKind::GroupCommit => g.commit(ycsb_key(i), rng),
                WorkloadKind::CrossShardContention => {
                    let p = XSHARD_PARTITIONS as u64;
                    BenchOp::Commit {
                        family: YCSB_FAMILY,
                        rows: vec![g.partition_row((i % p) as usize, i / p, rng)],
                    }
                }
                WorkloadKind::Transfer => BenchOp::Put {
                    row: account_key(i),
                    family: YCSB_FAMILY,
                    cells: vec![(b"bal".to_vec(), OPENING_BALANCE.to_le_bytes().to_vec())],
                },
                // Every counter starts at 1.
                WorkloadKind::Counters => BenchOp::Incr {
                    row: counter_key(i),
                    delta: 1,
                },
                WorkloadKind::SparseWide | WorkloadKind::SparseWideDeletes => {
                    let n = rng.range(0, 2 * SPARSE_MEAN_CELLS);
                    if n == 0 {
                        continue; // a row with no cells does not exist
                    }
                    let quals: BTreeSet<u64> = (0..n).map(|_| g.aux.sample(rng)).collect();
                    BenchOp::Put {
                        row: sparse_key(i),
                        family: SPARSE_FAMILY,
                        cells: quals
                            .into_iter()
                            .map(|q| (sparse_qualifier(q), g.value(rng)))
                            .collect(),
                    }
                }
                WorkloadKind::TimeSeriesTtl | WorkloadKind::TimeSeriesRange => {
                    // Interleave entities in time order, as live ingestion would.
                    let (t, e) = (i / g.n_items, i % g.n_items);
                    BenchOp::PutAt {
                        row: point_key(e, t),
                        family: METRIC_FAMILY,
                        ts: g.point_ts(t),
                        cells: vec![(b"v".to_vec(), g.value(rng))],
                    }
                }
                WorkloadKind::Adjacency | WorkloadKind::AdjacencyPrefix => {
                    let degree = 1 + g.aux.sample(rng);
                    let dsts: BTreeSet<u64> =
                        (0..degree).map(|_| g.items.sample_scrambled(rng)).collect();
                    BenchOp::Put {
                        row: vertex_key(i),
                        family: EDGE_FAMILY,
                        cells: dsts
                            .into_iter()
                            .map(|d| (edge_qualifier(d), g.value(rng)))
                            .collect(),
                    }
                }
            };
            return Some(op);
        }
    }
}

/// Iterator over the measured phase.
#[derive(Debug)]
pub(crate) struct RunIter<'a> {
    g: &'a Gen,
    rng: Rng,
    done: u64,
    /// YCSB D and E: records inserted so far (load plus run inserts).
    inserted: u64,
    /// Time series: next logical timestamp per entity.
    clocks: Vec<u64>,
}

impl RunIter<'_> {
    /// YCSB `readallfields=true`: all ten fields of one record.
    fn ycsb_read(&mut self) -> BenchOp {
        let i = self.g.items.sample_scrambled(&mut self.rng);
        BenchOp::GetRow {
            row: ycsb_key(i),
            family: YCSB_FAMILY,
        }
    }

    fn ycsb_update(&mut self) -> BenchOp {
        let i = self.g.items.sample_scrambled(&mut self.rng);
        BenchOp::Put {
            row: ycsb_key(i),
            family: YCSB_FAMILY,
            cells: vec![(
                field(self.rng.below(YCSB_FIELDS)),
                self.g.value(&mut self.rng),
            )],
        }
    }

    fn ycsb_insert(&mut self) -> BenchOp {
        let i = self.inserted;
        self.inserted += 1;
        BenchOp::Put {
            row: ycsb_key(i),
            family: YCSB_FAMILY,
            cells: (0..YCSB_FIELDS)
                .map(|f| (field(f), self.g.value(&mut self.rng)))
                .collect(),
        }
    }

    /// A [`WorkloadKind::SparseWide`] operation for the draw `p` in `[0, 1)`.
    fn sparse_op(&mut self, p: f64) -> BenchOp {
        match () {
            _ if p < 0.4 => BenchOp::Get {
                row: sparse_key(self.g.items.sample_scrambled(&mut self.rng)),
                family: SPARSE_FAMILY,
                qualifier: sparse_qualifier(self.g.aux.sample(&mut self.rng)),
            },
            _ if p < 0.6 => BenchOp::GetRow {
                row: sparse_key(self.g.items.sample_scrambled(&mut self.rng)),
                family: SPARSE_FAMILY,
            },
            _ if p < 0.8 => {
                let row = sparse_key(self.g.items.sample_scrambled(&mut self.rng));
                let n = self.rng.range(1, 4);
                let quals: BTreeSet<u64> =
                    (0..n).map(|_| self.g.aux.sample(&mut self.rng)).collect();
                BenchOp::Put {
                    row,
                    family: SPARSE_FAMILY,
                    cells: quals
                        .into_iter()
                        .map(|q| (sparse_qualifier(q), self.g.value(&mut self.rng)))
                        .collect(),
                }
            }
            _ => BenchOp::Scan {
                start: sparse_key(self.g.items.sample_scrambled(&mut self.rng)),
                len: 10,
            },
        }
    }
}

impl Iterator for RunIter<'_> {
    type Item = BenchOp;

    fn next(&mut self) -> Option<BenchOp> {
        if self.done >= self.g.config.operations {
            return None;
        }
        self.done += 1;
        let g = self.g;
        let p = self.rng.next_f64();
        let op = match g.config.kind {
            WorkloadKind::YcsbA if p < 0.5 => self.ycsb_read(),
            WorkloadKind::YcsbA => self.ycsb_update(),
            WorkloadKind::YcsbB if p < 0.95 => self.ycsb_read(),
            WorkloadKind::YcsbB => self.ycsb_update(),
            WorkloadKind::YcsbC => self.ycsb_read(),
            WorkloadKind::YcsbD if p < 0.95 => {
                // Read latest: recently inserted records are the most popular.
                let back = g.items.sample(&mut self.rng);
                let i = self.inserted.saturating_sub(1 + back);
                BenchOp::GetRow {
                    row: ycsb_key(i),
                    family: YCSB_FAMILY,
                }
            }
            WorkloadKind::YcsbD => self.ycsb_insert(),
            WorkloadKind::YcsbE if p < 0.95 => BenchOp::Scan {
                start: ycsb_key(g.items.sample_scrambled(&mut self.rng)),
                len: self.rng.range(1, YCSB_MAX_SCAN) as u32,
            },
            WorkloadKind::YcsbE => self.ycsb_insert(),
            WorkloadKind::YcsbF if p < 0.5 => self.ycsb_read(),
            WorkloadKind::YcsbF => BenchOp::ReadModifyWrite {
                row: ycsb_key(g.items.sample_scrambled(&mut self.rng)),
                qualifier: field(self.rng.below(YCSB_FIELDS)),
            },
            // D247: sparse-wide's mix with 5% deletes taken from its puts (20% -> 15%), half a
            // column and half a whole row.
            WorkloadKind::SparseWideDeletes if (0.75..0.775).contains(&p) => {
                BenchOp::DeleteColumn {
                    row: sparse_key(g.items.sample_scrambled(&mut self.rng)),
                    family: SPARSE_FAMILY,
                    qualifier: sparse_qualifier(g.aux.sample(&mut self.rng)),
                }
            }
            WorkloadKind::SparseWideDeletes if (0.775..0.8).contains(&p) => BenchOp::DeleteRow {
                row: sparse_key(g.items.sample_scrambled(&mut self.rng)),
            },
            WorkloadKind::SparseWide | WorkloadKind::SparseWideDeletes => self.sparse_op(p),
            WorkloadKind::TimeSeriesTtl | WorkloadKind::TimeSeriesRange => {
                let e = g.items.sample_scrambled(&mut self.rng);
                let clock = &mut self.clocks[e as usize];
                if p < 0.4 {
                    let t = *clock;
                    *clock += 1;
                    BenchOp::PutAt {
                        row: point_key(e, t),
                        family: METRIC_FAMILY,
                        ts: g.point_ts(t),
                        cells: vec![(b"v".to_vec(), g.value(&mut self.rng))],
                    }
                } else if p < 0.8 && g.config.kind == WorkloadKind::TimeSeriesRange {
                    // D247: up to ten points of one entity from 12 to 6 hours ago.
                    BenchOp::ScanFiltered {
                        start: entity_prefix(e).into_bytes(),
                        len: 10,
                        family: METRIC_FAMILY,
                        qualifier_prefix: None,
                        time_range: Some((
                            g.epoch - TIME_RANGE.0.as_micros() as u64,
                            g.epoch - TIME_RANGE.1.as_micros() as u64,
                        )),
                    }
                } else if p < 0.8 {
                    // The newest ten points of one entity.
                    BenchOp::Scan {
                        start: entity_prefix(e).into_bytes(),
                        len: 10,
                    }
                } else {
                    let t = clock.saturating_sub(1 + self.rng.below(POINTS_PER_ENTITY));
                    BenchOp::Get {
                        row: point_key(e, t),
                        family: METRIC_FAMILY,
                        qualifier: b"v".to_vec(),
                    }
                }
            }
            // D247: one vertex's out-edges to about a tenth of the vertices: a qualifier prefix
            // fixing every digit but the lowest `k` of the destination, 10^k about n/10.
            WorkloadKind::AdjacencyPrefix if p < 0.8 => {
                let start = vertex_key(g.items.sample_scrambled(&mut self.rng));
                let k = ((g.n_items as f64 / 10.0).max(1.0).log10().floor() as u32).min(9);
                let buckets = g.n_items.div_ceil(10u64.pow(k)).max(1);
                let bucket = self.rng.below(buckets);
                BenchOp::ScanFiltered {
                    start,
                    len: 1,
                    family: EDGE_FAMILY,
                    qualifier_prefix: Some(
                        format!("edge:{bucket:0w$}", w = (10 - k) as usize).into_bytes(),
                    ),
                    time_range: None,
                }
            }
            WorkloadKind::Adjacency if p < 0.8 => BenchOp::Scan {
                // Out-edges of one vertex.
                start: vertex_key(g.items.sample_scrambled(&mut self.rng)),
                len: 1,
            },
            WorkloadKind::Adjacency | WorkloadKind::AdjacencyPrefix if p < 0.9 => BenchOp::Scan {
                start: vertex_key(g.items.sample_scrambled(&mut self.rng)),
                len: 10,
            },
            WorkloadKind::Adjacency | WorkloadKind::AdjacencyPrefix => BenchOp::Put {
                row: vertex_key(g.items.sample_scrambled(&mut self.rng)),
                family: EDGE_FAMILY,
                cells: vec![(
                    edge_qualifier(g.items.sample_scrambled(&mut self.rng)),
                    g.value(&mut self.rng),
                )],
            },
            WorkloadKind::SkewedMultiShard => BenchOp::Put {
                row: skewed_key(g.items.sample_scrambled(&mut self.rng)),
                family: YCSB_FAMILY,
                cells: vec![(field(0), g.value(&mut self.rng))],
            },
            // Uniform rows: concurrent commits rarely touch the same row.
            WorkloadKind::GroupCommit => {
                let row = ycsb_key(self.rng.below(g.n_items));
                g.commit(row, &mut self.rng)
            }
            // D247: two distinct Zipfian accounts, so hot ones conflict.
            WorkloadKind::Transfer => {
                let from = g.items.sample_scrambled(&mut self.rng);
                let mut to = g.items.sample_scrambled(&mut self.rng);
                if to == from {
                    to = (from + 1) % g.n_items.max(2);
                }
                BenchOp::Transfer {
                    from: account_key(from),
                    to: account_key(to),
                    amount: 1,
                }
            }
            // D247: Zipfian rows, so hot counters take many increments.
            WorkloadKind::Counters if p < COUNTER_INCR_FRACTION => BenchOp::Incr {
                row: counter_key(g.items.sample_scrambled(&mut self.rng)),
                delta: 1,
            },
            WorkloadKind::Counters => BenchOp::ReadCounter {
                row: counter_key(g.items.sample_scrambled(&mut self.rng)),
            },
            // D246: a row in one partition, or one row in each of 2-4 consecutive partitions
            // (distinct, so on different shards when the partitions sit one per shard), the
            // rows uniform within their partitions.
            WorkloadKind::CrossShardContention => {
                let n = XSHARD_PARTITIONS as u64;
                let parts = if p < XSHARD_CROSS_FRACTION {
                    2 + self.rng.below(3)
                } else {
                    1
                };
                let first = self.rng.below(n);
                let per = g.rows_per_partition();
                let rows = (0..parts)
                    .map(|k| {
                        let i = self.rng.below(per);
                        g.partition_row(((first + k) % n) as usize, i, &mut self.rng)
                    })
                    .collect();
                BenchOp::Commit {
                    family: YCSB_FAMILY,
                    rows,
                }
            }
        };
        Some(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Workload;

    fn ops(kind: WorkloadKind, seed: u64) -> (Vec<BenchOp>, Vec<BenchOp>) {
        let mut w = Workload::new(WorkloadConfig {
            seed,
            epoch_micros: 2_000_000_000_000_000,
            ..WorkloadConfig::smoke(kind)
        });
        let load = w.load_ops().collect();
        let run = w.run_ops().collect();
        (load, run)
    }

    #[test]
    fn deterministic_per_seed() {
        for kind in WorkloadKind::EVERY {
            let seed = 11;
            assert_eq!(ops(kind, seed), ops(kind, seed), "{kind:?} seed {seed}");
            assert_ne!(
                ops(kind, seed).1,
                ops(kind, seed + 1).1,
                "{kind:?} seed {seed}"
            );
        }
    }

    #[test]
    fn run_ops_are_repeatable() {
        let mut w = Workload::new(WorkloadConfig::smoke(WorkloadKind::YcsbD));
        let a: Vec<_> = w.run_ops().collect();
        let b: Vec<_> = w.run_ops().collect();
        assert_eq!(a, b);
        assert_eq!(
            a.len() as u64,
            WorkloadConfig::smoke(WorkloadKind::YcsbD).operations
        );
    }

    #[test]
    fn mixes_match_the_definitions() {
        let seed = 5;
        let mut cfg = WorkloadConfig::smoke(WorkloadKind::YcsbA);
        cfg.seed = seed;
        cfg.operations = 20_000;
        for (kind, want_reads) in [
            (WorkloadKind::YcsbA, 0.5),
            (WorkloadKind::YcsbB, 0.95),
            (WorkloadKind::YcsbC, 1.0),
            (WorkloadKind::YcsbD, 0.95),
            (WorkloadKind::YcsbF, 0.5),
        ] {
            let mut w = Workload::new(WorkloadConfig {
                kind,
                ..cfg.clone()
            });
            let reads = w
                .run_ops()
                .filter(|op| matches!(op, BenchOp::Get { .. } | BenchOp::GetRow { .. }))
                .count() as f64
                / cfg.operations as f64;
            assert!(
                (reads - want_reads).abs() < 0.02,
                "{kind:?} reads {reads} seed {seed}"
            );
        }
        let mut w = Workload::new(WorkloadConfig {
            kind: WorkloadKind::YcsbE,
            ..cfg.clone()
        });
        let scans = w
            .run_ops()
            .filter(|op| matches!(op, BenchOp::Scan { len, .. } if (1..=100).contains(len)))
            .count() as f64
            / cfg.operations as f64;
        assert!((scans - 0.95).abs() < 0.02, "scans {scans} seed {seed}");
    }

    #[test]
    fn group_commits_are_small_row_mutations_of_loaded_rows() {
        let (load, run) = ops(WorkloadKind::GroupCommit, 7);
        let rows: BTreeSet<Vec<u8>> = load
            .iter()
            .map(|op| match op {
                BenchOp::Put { row, .. } => row.clone(),
                other => panic!("load is puts, got {other:?}"),
            })
            .collect();
        assert_eq!(
            rows.len() as u64,
            WorkloadConfig::smoke(WorkloadKind::GroupCommit).records
        );
        let mut touched = BTreeSet::new();
        for op in &run {
            let BenchOp::Put { row, cells, .. } = op else {
                panic!("group-commit runs puts, got {op:?}")
            };
            assert_eq!(cells.len() as u64, GROUP_COMMIT_CELLS);
            assert!(rows.contains(row), "seed 7");
            touched.insert(row.clone());
        }
        // Uniform: 2,000 commits over 1,000 rows touch most of them.
        assert!(touched.len() > 800, "seed 7: {} rows", touched.len());
    }

    #[test]
    fn half_the_contention_commits_cross_two_to_four_partitions() {
        let seed = 9;
        let mut config = WorkloadConfig::smoke(WorkloadKind::CrossShardContention);
        config.seed = seed;
        config.operations = 20_000;
        let mut w = Workload::new(config.clone());
        let loaded: BTreeSet<Vec<u8>> = w
            .load_ops()
            .flat_map(|op| match op {
                BenchOp::Commit { rows, .. } if rows.len() == 1 => rows.into_iter().map(|r| r.row),
                other => panic!("the load commits one row each, got {other:?}"),
            })
            .collect();
        assert_eq!(loaded.len() as u64, config.records, "seed {seed}");
        let mut cross = 0;
        for op in w.run_ops() {
            let BenchOp::Commit { rows, .. } = &op else {
                panic!("cross-shard-contention runs commits, got {op:?}")
            };
            assert!((1..=4).contains(&rows.len()), "seed {seed}: {op:?}");
            let parts: BTreeSet<usize> = rows.iter().map(|r| r.partition).collect();
            assert_eq!(parts.len(), rows.len(), "seed {seed}: distinct partitions");
            for r in rows {
                assert!(r.partition < XSHARD_PARTITIONS);
                assert!(loaded.contains(&r.row), "seed {seed}: rows are loaded rows");
            }
            if rows.len() > 1 {
                cross += 1;
                assert_eq!(op.type_name(), CROSS_SHARD_COMMIT);
            } else {
                assert_eq!(op.type_name(), "put");
            }
        }
        let share = f64::from(cross) / config.operations as f64;
        assert!(
            (share - XSHARD_CROSS_FRACTION).abs() < 0.02,
            "seed {seed}: {share}"
        );
    }

    #[test]
    fn counters_increment_nine_in_ten_and_read_the_rest() {
        let seed = 13;
        let mut config = WorkloadConfig::smoke(WorkloadKind::Counters);
        config.seed = seed;
        config.operations = 20_000;
        let mut w = Workload::new(config.clone());
        assert!(
            w.load_ops()
                .all(|op| matches!(op, BenchOp::Incr { delta: 1, .. }))
        );
        let incrs = w
            .run_ops()
            .filter(|op| matches!(op, BenchOp::Incr { .. }))
            .count() as f64
            / config.operations as f64;
        assert!(
            (incrs - COUNTER_INCR_FRACTION).abs() < 0.02,
            "seed {seed}: {incrs}"
        );
    }

    #[test]
    fn transfers_move_one_unit_between_two_loaded_accounts() {
        let (load, run) = ops(WorkloadKind::Transfer, 17);
        let accounts: BTreeSet<Vec<u8>> = load
            .iter()
            .map(|op| match op {
                BenchOp::Put { row, .. } => row.clone(),
                other => panic!("the load opens accounts, got {other:?}"),
            })
            .collect();
        for op in &run {
            let BenchOp::Transfer { from, to, amount } = op else {
                panic!("transfer runs transfers, got {op:?}")
            };
            assert_ne!(from, to, "seed 17");
            assert!(accounts.contains(from) && accounts.contains(to), "seed 17");
            assert_eq!(*amount, 1);
        }
    }

    #[test]
    fn sparse_wide_deletes_delete_five_in_a_hundred_and_keep_sparse_wides_stream_otherwise() {
        let seed = 19;
        let mut config = WorkloadConfig::smoke(WorkloadKind::SparseWideDeletes);
        config.seed = seed;
        config.operations = 40_000;
        let ops: Vec<BenchOp> = Workload::new(config.clone()).run_ops().collect();
        let share = |f: fn(&BenchOp) -> bool| {
            ops.iter().filter(|op| f(op)).count() as f64 / config.operations as f64
        };
        let columns = share(|op| matches!(op, BenchOp::DeleteColumn { .. }));
        let rows = share(|op| matches!(op, BenchOp::DeleteRow { .. }));
        let puts = share(|op| matches!(op, BenchOp::Put { .. }));
        assert!((columns - 0.025).abs() < 0.005, "seed {seed}: {columns}");
        assert!((rows - 0.025).abs() < 0.005, "seed {seed}: {rows}");
        assert!((puts - 0.15).abs() < 0.01, "seed {seed}: {puts}");
        // The load is sparse-wide's.
        let load = |kind| {
            Workload::new(WorkloadConfig {
                kind,
                ..config.clone()
            })
            .load_ops()
            .collect::<Vec<_>>()
        };
        assert_eq!(
            load(WorkloadKind::SparseWideDeletes),
            load(WorkloadKind::SparseWide)
        );
    }

    #[test]
    fn sparse_rows_are_sparse_and_bounded() {
        let (load, _) = ops(WorkloadKind::SparseWide, 3);
        let mut total = 0;
        for op in &load {
            let BenchOp::Put { cells, .. } = op else {
                panic!("load is puts")
            };
            assert!(!cells.is_empty() && cells.len() as u64 <= 2 * SPARSE_MEAN_CELLS);
            total += cells.len();
        }
        let mean = total as f64 / load.len() as f64;
        assert!((10.0..30.0).contains(&mean), "mean cells per row {mean}");
    }

    #[test]
    fn time_series_scans_start_at_newest_point() {
        assert!(point_key(3, 10) < point_key(3, 9));
        assert!(entity_prefix(3).as_bytes() < point_key(3, u64::MAX - 1).as_slice());
        assert!(point_key(3, 0) < entity_prefix(4).into_bytes());
    }

    #[test]
    fn ycsb_reads_fetch_every_field() {
        let (_, run) = ops(WorkloadKind::YcsbB, 4);
        assert!(run.iter().any(|op| matches!(op, BenchOp::GetRow { .. })));
        assert!(!run.iter().any(|op| matches!(op, BenchOp::Get { .. })));
    }

    #[test]
    fn sparse_wide_reads_whole_rows_too() {
        let (_, run) = ops(WorkloadKind::SparseWide, 4);
        let count = |f: fn(&BenchOp) -> bool| run.iter().filter(|op| f(op)).count() as f64;
        let n = run.len() as f64;
        let gets = count(|op| matches!(op, BenchOp::Get { .. })) / n;
        let rows = count(|op| matches!(op, BenchOp::GetRow { .. })) / n;
        assert!(
            (gets - 0.4).abs() < 0.05 && (rows - 0.2).abs() < 0.05,
            "{gets} {rows}"
        );
    }

    #[test]
    fn time_series_timestamps_straddle_the_ttl() {
        let epoch = 2_000_000_000_000_000;
        let ttl = TIME_SERIES_TTL.as_micros() as u64;
        let mut w = Workload::new(WorkloadConfig {
            epoch_micros: epoch,
            ..WorkloadConfig::smoke(WorkloadKind::TimeSeriesTtl)
        });
        let load: Vec<BenchOp> = w.load_ops().collect();
        let stamps: Vec<u64> = load
            .iter()
            .map(|op| match op {
                BenchOp::PutAt { ts, .. } => *ts,
                other => panic!("load op {other:?}"),
            })
            .collect();
        let expired = stamps.iter().filter(|ts| **ts + ttl <= epoch).count();
        assert_eq!(
            expired * 4,
            stamps.len(),
            "a quarter of the load is expired"
        );
        // Live points stay live for LIVE_MARGIN; expired ones are expired by a full step.
        let margin = LIVE_MARGIN.as_micros() as u64;
        for ts in &stamps {
            let expires = ts + ttl;
            assert!(expires > epoch + margin || expires + 14 * 60 * 1_000_000 < epoch);
        }
        // Appended points are live and later than any loaded one.
        let appended = w
            .run_ops()
            .find_map(|op| match op {
                BenchOp::PutAt { ts, .. } => Some(ts),
                _ => None,
            })
            .expect("the run appends points");
        assert!(appended >= epoch && stamps.iter().all(|ts| *ts < appended));
    }

    #[test]
    fn a_stale_epoch_fails_the_time_series_check() {
        let old = crate::runners::now_micros() - 7 * 3600 * 1_000_000;
        let stale = |kind| {
            Workload::new(WorkloadConfig {
                epoch_micros: old,
                ..WorkloadConfig::smoke(kind)
            })
            .check_epoch_age()
        };
        assert!(stale(WorkloadKind::TimeSeriesTtl).is_err());
        assert!(stale(WorkloadKind::YcsbA).is_ok());
        assert!(
            Workload::new(WorkloadConfig::smoke(WorkloadKind::TimeSeriesTtl))
                .check_epoch_age()
                .is_ok()
        );
    }
}
