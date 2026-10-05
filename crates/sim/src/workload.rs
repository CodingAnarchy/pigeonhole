use pigeonhole_format::Durability;

use crate::{ModelOp, Rng};

/// Shape of a generated workload.
///
/// `WorkloadSpec::default()` is a small, mixed workload (100 rows, 4 qualifiers, one family
/// `f`, 30% reads, 10% deletes, batches up to 4, values up to 16 bytes); override fields on it.
///
/// A family whose name starts with `counter` is treated as an `i64` counter family: the
/// generator writes only [`ModelOp::Incr`], 8-byte little-endian puts and deletes there, so
/// a store configured with the `i64` add operator for those families never sees a malformed
/// operand.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WorkloadSpec {
    /// Distinct rows.
    pub rows: u64,
    /// Qualifiers per row (upper bound; drawn per write).
    pub qualifiers: u64,
    /// Families, by name; every table in the run has all of them.
    pub families: Vec<String>,
    /// Zipfian skew (0.0 = uniform).
    pub zipf_theta: f64,
    /// Fraction of operations that are reads.
    pub read_fraction: f64,
    /// Fraction of writes that are deletes of some kind.
    pub delete_fraction: f64,
    /// Mutations per commit (upper bound).
    pub max_batch: u32,
    /// Largest value in bytes.
    pub max_value_len: u32,
}

impl Default for WorkloadSpec {
    fn default() -> Self {
        Self {
            rows: 100,
            qualifiers: 4,
            families: vec!["f".to_owned()],
            zipf_theta: 0.0,
            read_fraction: 0.3,
            delete_fraction: 0.1,
            max_batch: 4,
            max_value_len: 16,
        }
    }
}

/// One generated operation.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    /// Commit these mutations at this durability.
    Commit(Vec<ModelOp>, Durability),
    /// Point read of the latest version.
    Get {
        /// Row.
        row: Vec<u8>,
        /// Family.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// Ordered scan of `[start, end)`.
    Scan {
        /// Start row.
        start: Vec<u8>,
        /// End row.
        end: Vec<u8>,
    },
    /// Take a snapshot to read from later.
    Snapshot,
}

/// Zipfian rank generator (Gray et al., as in YCSB). Rank 0 is the hottest.
#[derive(Debug)]
struct Zipf {
    n: u64,
    theta: f64,
    zetan: f64,
    alpha: f64,
    eta: f64,
}

impl Zipf {
    fn new(n: u64, theta: f64) -> Self {
        let zeta = |n: u64| (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>();
        let zetan = zeta(n);
        let zeta2 = zeta(2.min(n));
        Self {
            n,
            theta,
            zetan,
            alpha: 1.0 / (1.0 - theta),
            eta: (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan),
        }
    }

    fn next(&self, rng: &mut Rng) -> u64 {
        let u = rng.unit();
        let uz = u * self.zetan;
        if uz < 1.0 {
            0
        } else if uz < 1.0 + 0.5f64.powf(self.theta) {
            1.min(self.n - 1)
        } else {
            let rank = (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
            rank.min(self.n - 1)
        }
    }
}

/// A seeded generator of [`Op`]s for one table. It never ends: take as many as you need.
///
/// Explicit timestamps (puts and cell deletes) are logical: the generator keeps a tick that
/// advances once per operation and picks timestamps within the last few ticks, so a harness
/// that sets its commit timestamp to the tick (plus a fixed base) gets explicit timestamps
/// that interleave with default ones.
///
/// ```
/// use pigeonhole_sim::{Op, Workload, WorkloadSpec};
///
/// let a: Vec<Op> = Workload::new(1, "t", WorkloadSpec::default()).take(50).collect();
/// let b: Vec<Op> = Workload::new(1, "t", WorkloadSpec::default()).take(50).collect();
/// assert_eq!(a, b);
/// assert!(a.iter().any(|op| matches!(op, Op::Commit(..))));
/// ```
#[derive(Debug)]
pub struct Workload {
    rng: Rng,
    table: String,
    spec: WorkloadSpec,
    zipf: Option<Zipf>,
    tick: u64,
}

impl Workload {
    /// A generator for `table` following `spec`.
    pub fn new(seed: u64, table: &str, mut spec: WorkloadSpec) -> Self {
        spec.rows = spec.rows.max(1);
        spec.qualifiers = spec.qualifiers.max(1);
        spec.max_batch = spec.max_batch.max(1);
        if spec.families.is_empty() {
            spec.families.push("f".to_owned());
        }
        let theta = spec.zipf_theta.clamp(0.0, 0.99);
        let zipf = (theta > 0.0).then(|| Zipf::new(spec.rows, theta));
        Self {
            rng: Rng::new(seed),
            table: table.to_owned(),
            spec,
            zipf,
            tick: 0,
        }
    }

    fn row_index(&mut self) -> u64 {
        match &self.zipf {
            Some(z) => z.next(&mut self.rng),
            None => self.rng.below(self.spec.rows),
        }
    }

    fn row_key(i: u64) -> Vec<u8> {
        format!("row{i:06}").into_bytes()
    }

    fn row(&mut self) -> Vec<u8> {
        let i = self.row_index();
        Self::row_key(i)
    }

    fn family(&mut self) -> String {
        let i = self.rng.below(self.spec.families.len() as u64) as usize;
        self.spec.families[i].clone()
    }

    fn qualifier(&mut self) -> Vec<u8> {
        format!("q{}", self.rng.below(self.spec.qualifiers)).into_bytes()
    }

    fn recent_ts(&mut self) -> u64 {
        self.tick.saturating_sub(self.rng.below(8)).max(1)
    }

    fn value(&mut self) -> Vec<u8> {
        let len = self.rng.below(u64::from(self.spec.max_value_len) + 1) as usize;
        let mut v = Vec::with_capacity(len);
        while v.len() < len {
            v.extend_from_slice(&self.rng.next_u64().to_le_bytes());
        }
        v.truncate(len);
        v
    }

    fn mutation(&mut self) -> ModelOp {
        let table = self.table.clone();
        let row = self.row();
        let family = self.family();
        let counter = family.starts_with("counter");
        if self.rng.unit() < self.spec.delete_fraction {
            return match self.rng.below(10) {
                0..=2 => ModelOp::DeleteCell {
                    table,
                    row,
                    family,
                    qualifier: self.qualifier(),
                    ts: self.recent_ts(),
                },
                3..=5 => ModelOp::DeleteColumn {
                    table,
                    row,
                    family,
                    qualifier: self.qualifier(),
                },
                6..=7 => ModelOp::DeleteFamily { table, row, family },
                _ => ModelOp::DeleteRow { table, row },
            };
        }
        let qualifier = self.qualifier();
        if counter && self.rng.chance(800_000) {
            let delta = self.rng.below(2001) as i64 - 1000;
            return ModelOp::Incr {
                table,
                row,
                family,
                qualifier,
                delta,
            };
        }
        let ts = self.rng.chance(150_000).then(|| self.recent_ts());
        let value = if counter {
            (self.rng.below(2001) as i64 - 1000).to_le_bytes().to_vec()
        } else {
            self.value()
        };
        ModelOp::Put {
            table,
            row,
            family,
            qualifier,
            ts,
            value,
        }
    }

    fn durability(&mut self) -> Durability {
        match self.rng.below(100) {
            0..=9 => Durability::None,
            10..=34 => Durability::Buffered,
            35..=74 => Durability::GroupSync,
            _ => Durability::Sync,
        }
    }
}

impl Iterator for Workload {
    type Item = Op;

    fn next(&mut self) -> Option<Op> {
        self.tick += 1;
        if self.rng.unit() < self.spec.read_fraction {
            return Some(match self.rng.below(100) {
                0..=59 => Op::Get {
                    row: self.row(),
                    family: self.family(),
                    qualifier: self.qualifier(),
                },
                60..=84 => {
                    let (a, b) = (self.row_index(), self.row_index());
                    Op::Scan {
                        start: Self::row_key(a.min(b)),
                        end: Self::row_key(a.max(b) + 1),
                    }
                }
                _ => Op::Snapshot,
            });
        }
        let n = 1 + self.rng.below(u64::from(self.spec.max_batch));
        let ops = (0..n).map(|_| self.mutation()).collect();
        Some(Op::Commit(ops, self.durability()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_seed_sensitive() {
        let gen_ = |seed| {
            Workload::new(seed, "t", WorkloadSpec::default())
                .take(200)
                .collect::<Vec<_>>()
        };
        assert_eq!(gen_(3), gen_(3));
        assert_ne!(gen_(3), gen_(4));
    }

    #[test]
    fn respects_spec_bounds_and_mix() {
        let spec = WorkloadSpec {
            rows: 10,
            qualifiers: 3,
            families: vec!["f".into(), "counter".into()],
            read_fraction: 0.5,
            delete_fraction: 0.3,
            max_batch: 5,
            max_value_len: 9,
            ..WorkloadSpec::default()
        };
        let ops: Vec<Op> = Workload::new(11, "t", spec).take(5_000).collect();
        let (mut reads, mut commits, mut deletes, mut incrs, mut snaps) = (0, 0, 0, 0, 0);
        for op in &ops {
            match op {
                Op::Commit(muts, _) => {
                    commits += 1;
                    assert!((1..=5).contains(&muts.len()));
                    for m in muts {
                        match m {
                            ModelOp::Put { family, value, .. } if family == "counter" => {
                                assert_eq!(value.len(), 8);
                            }
                            ModelOp::Put { value, .. } => assert!(value.len() <= 9),
                            ModelOp::Incr { family, .. } => {
                                assert_eq!(family, "counter");
                                incrs += 1;
                            }
                            _ => deletes += 1,
                        }
                    }
                }
                Op::Snapshot => snaps += 1,
                _ => reads += 1,
            }
        }
        assert!((2_200..2_800).contains(&(reads + snaps)), "{reads} {snaps}");
        assert!(commits > 2_000 && deletes > 500 && incrs > 100 && snaps > 100);
    }

    #[test]
    fn zipf_skews_toward_low_ranks() {
        let spec = WorkloadSpec {
            rows: 1000,
            zipf_theta: 0.9,
            read_fraction: 1.0,
            ..WorkloadSpec::default()
        };
        let mut hot = 0;
        let n = 20_000;
        for op in Workload::new(2, "t", spec).take(n) {
            if let Op::Get { row, .. } = op {
                hot += usize::from(row.as_slice() < b"row000010".as_slice());
            }
        }
        // Uniform would hit the first 10 of 1000 rows about 1% of the time.
        assert!(hot > n / 10, "{hot}");
    }

    #[test]
    fn degenerate_spec_is_clamped() {
        let spec = WorkloadSpec {
            rows: 0,
            qualifiers: 0,
            families: vec![],
            max_batch: 0,
            ..WorkloadSpec::default()
        };
        assert_eq!(Workload::new(1, "t", spec).take(100).count(), 100);
    }
}
