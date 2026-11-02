//! Hot aggregate cells under a stream of inputs (#487 PR 2, owner's check for D291): does an
//! HLL family need its own counter-checkpoint threshold? A dense HLL checkpoint writes about
//! 3 KiB into the memtable, against 9 bytes for a Sum checkpoint.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example hotagg -- KIND STEPS [WRITES] [DIR]
//! ```
//!
//! `KIND` is `sum`, `hll`, `quantiles` (#488 PR 3, perf287's N1 on #1221: a sketch of the
//! default 7 mantissa bits and 2048 buckets, fed values over about ten binades, so a cell
//! grows to about 1300 buckets, tens of KiB) or `set` (#488 PR 5b: every input adds a new
//! element, scattered over 2^22, so a cell grows past D298's 4 KiB checkpoint cap after about
//! 2,000 inputs and its later gets fold every input since the last flush) or `sethub` (as
//! `set`, each cell first put whole as a hub of 2^22 random ids below 2^25, about 5 MB, so
//! every get reads a large separated base); `STEPS` is `counter_checkpoint_steps` (D291: a get that folds
//! that many entries leaves its result as a base; 0 turns it off). `CELLS` hot cells of one
//! row take `WRITES` blind inputs round-robin, one per commit; every `READ_EVERY`-th write is
//! followed by a timed get of one of them. An HLL cell gets a new element with every input,
//! so it starts sparse and turns dense after about 800 inputs. One engine-owned shard,
//! buffered commits, compaction off, and a 4 MiB freeze threshold, so the flushes count
//! memtable bytes.
//!
//! Prints one line: the reads' p50 and p99 (µs) overall and by pending operands at the read
//! (inputs since the cell's last kept checkpoint or flush), checkpoints kept and dropped,
//! flushes, memtable bytes per write (flushes × threshold / writes, an upper bound to one
//! threshold), SST bytes flushed, and the write rate.

use std::time::Instant;

use pigeonhole_engine::{
    Durability, Engine, EngineOptions, FamilyKind, FamilyOptions, ValueRef, WriteBatch,
};

const CELLS: usize = 4;
const READ_EVERY: usize = 16;
const FREEZE: u64 = 4 << 20;
/// Pending-operand buckets: `[0, 32)`, `[32, 128)`, `[128, 512)`, `[512, ∞)`.
const BUCKETS: [usize; 4] = [0, 32, 128, 512];

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i] as f64 / 1_000.0
}

fn main() {
    let mut args = std::env::args().skip(1);
    let kind = args.next().expect("KIND: sum, hll, quantiles or set");
    let steps: u64 = args.next().expect("STEPS").parse().expect("STEPS");
    let writes: usize = args.next().map_or(200_000, |s| s.parse().expect("WRITES"));
    let base = args
        .next()
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let dir = base.join(format!("phdb-hotagg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 1;
    o.tablet_changes = false;
    o.durability = Durability::Buffered;
    o.memtable_budget = 64 << 20;
    o.memtable_freeze_bytes = FREEZE;
    o.compaction.l0_trigger = u32::MAX;
    o.counter_checkpoint_steps = steps;
    let db = Engine::open(&dir.join("hotagg.phdb"), o).unwrap();
    let quantiles = kind == "quantiles";
    let hub = kind == "sethub";
    let set = kind == "set" || hub;
    let (family, hll) = match kind.as_str() {
        "sum" => (
            FamilyOptions::default()
                .merge_operator("pigeonhole.i64_add")
                .kind(FamilyKind::Counter),
            false,
        ),
        "hll" => (
            FamilyOptions::default()
                .merge_operator("pigeonhole.hll")
                .kind(FamilyKind::Hll),
            true,
        ),
        "quantiles" => (
            FamilyOptions::default()
                .merge_operator("pigeonhole.f64_quantiles")
                .kind(FamilyKind::Quantiles)
                // No parameters: the defaults, 7 mantissa bits and 2048 buckets.
                .kind_params(Vec::new()),
            false,
        ),
        "set" | "sethub" => (
            FamilyOptions::default()
                .merge_operator("pigeonhole.set")
                .kind(FamilyKind::Set),
            false,
        ),
        other => panic!("KIND {other:?}: sum, hll, quantiles, set or sethub"),
    };
    let t = db.create_table("t", &[("a".into(), family)]).unwrap();
    let f = t.families[0].id;
    let qualifiers: Vec<Vec<u8>> = (0..CELLS).map(|c| format!("c{c}").into_bytes()).collect();
    if hub {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for q in &qualifiers {
            let mut ids = std::collections::BTreeSet::new();
            while ids.len() < 1 << 22 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                ids.insert((x >> 39) as u32);
            }
            let mut bytes = Vec::new();
            pigeonhole_format::roaring::encode_sorted(&mut bytes, ids);
            let mut wb = WriteBatch::new();
            wb.put(t.id, f, b"hot", q, None, ValueRef::Bytes(&bytes))
                .unwrap();
            db.commit(wb, None).unwrap();
        }
        db.flush().unwrap();
    }

    let mut pending = [0usize; CELLS];
    let mut reads: [Vec<u64>; 4] = Default::default();
    let mut all = Vec::new();
    let (mut kept, mut flushes) = (db.counter_checkpoints().0, db.metrics().flushes);
    let started = Instant::now();
    let mut write_nanos = 0u128;
    for i in 0..writes {
        let c = i % CELLS;
        let mut wb = WriteBatch::new();
        let mut operand = Vec::new();
        if set {
            let x = (i as u32).wrapping_mul(2_654_435_761) >> 10;
            pigeonhole_format::roaring::encode_operand(&mut operand, &[x], &[]);
        }
        let input = if set {
            ValueRef::Bytes(&operand)
        } else if quantiles {
            // 0.001 to 1000, scattered.
            ValueRef::F64((i as u64).wrapping_mul(2_654_435_761) as f64 % 1e6 * 1e-3 + 1e-3)
        } else {
            ValueRef::I64(if hll { i as i64 } else { 1 })
        };
        wb.merge(t.id, f, b"hot", &qualifiers[c], input).unwrap();
        let w0 = Instant::now();
        db.commit(wb, None).unwrap();
        write_nanos += w0.elapsed().as_nanos();
        pending[c] += 1;
        if i % READ_EVERY == READ_EVERY - 1 {
            let r = (i / READ_EVERY) % CELLS;
            let t0 = Instant::now();
            let cell = db.get_latest(t.id, f, b"hot", &qualifiers[r]).unwrap();
            let nanos = t0.elapsed().as_nanos() as u64;
            std::hint::black_box(cell);
            let bucket = BUCKETS.iter().rposition(|&b| pending[r] >= b).unwrap();
            reads[bucket].push(nanos);
            all.push(nanos);
            // A kept checkpoint (sent by the get, kept by the shard soon after) or a flush
            // resets what the next get folds.
            let now_kept = db.counter_checkpoints().0;
            if now_kept > kept {
                pending[r] = 0;
                kept = now_kept;
            }
            let now_flushes = db.metrics().flushes;
            if now_flushes > flushes {
                pending = [0; CELLS];
                flushes = now_flushes;
            }
        }
    }
    let elapsed = started.elapsed();
    db.flush().unwrap();
    let (kept, dropped) = db.counter_checkpoints();
    let flushes = db.metrics().flushes;
    let info = db.info().unwrap();
    let sst_bytes: u64 = info
        .tables
        .iter()
        .flat_map(|t| t.families.iter())
        .flat_map(|f| f.levels.iter())
        .map(|l| l.bytes)
        .sum();
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);

    all.sort_unstable();
    for r in &mut reads {
        r.sort_unstable();
    }
    let by_pending: Vec<String> = reads
        .iter()
        .zip(BUCKETS)
        .map(|(r, b)| {
            format!(
                "pending>={b}: n={} p50={:.2} p99={:.2}",
                r.len(),
                percentile(r, 0.5),
                percentile(r, 0.99)
            )
        })
        .collect();
    println!(
        "{kind}\tsteps={steps}\twrites={writes}\treads={}\tp50={:.2}\tp99={:.2}\t[{}]\t\
         checkpoints kept={kept} dropped={dropped}\tflushes={flushes}\t\
         memtable B/write<={:.0}\tsst_bytes={sst_bytes}\twrites/s={:.0}\twrite_mean_us={:.2}",
        all.len(),
        percentile(&all, 0.5),
        percentile(&all, 0.99),
        by_pending.join("; "),
        flushes as f64 * FREEZE as f64 / writes as f64,
        writes as f64 / elapsed.as_secs_f64(),
        write_nanos as f64 / writes as f64 / 1_000.0,
    );
}
