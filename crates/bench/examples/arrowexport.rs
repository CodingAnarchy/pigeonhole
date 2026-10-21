//! Arrow export throughput (#691): how fast `pigeonhole_arrow::scan_to_arrow` turns a warm
//! table into record batches, beside a plain lending scan (`RowIter::next_ref`) of the same
//! table, so the export's own cost is the difference.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example arrowexport -- DIR [MIB]
//! ```
//!
//! For each shape it writes about MIB mebibytes (default 256) of cells into `DIR/<shape>.phdb`,
//! flushes and compacts, opens it with a block cache larger than the table, fills the cache
//! with one untimed pass, and then times passes on this thread, alternating plain scan and
//! export, five each. It prints one JSON line per shape and pass kind with the median pass:
//! cells, the cells' key, qualifier and value bytes, seconds, GB/s of those bytes, and for
//! the export, the Arrow buffer bytes produced (`RecordBatch::get_array_memory_size`).
//!
//! Shapes:
//! - `narrow`: one 24-byte value per row;
//! - `wide`: 32 columns of 24-byte values per row;
//! - `large`: one 4000-byte value per row;
//! - `counters`: 8 `i64` columns per row, exported with the typed columns.
//!
//! The export uses the default options (8192 cells or 8 MiB a batch), plus `typed_values` for
//! `counters`. A batch is dropped as soon as it is counted, as a consumer streaming it would.

use std::path::Path;
use std::time::Instant;

use pigeonhole::{Durability, Family, Options, Pigeonhole, Table};
use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};

#[derive(Clone, Copy)]
struct Shape {
    name: &'static str,
    columns: usize,
    value: usize,
    counters: bool,
}

const SHAPES: [Shape; 4] = [
    Shape {
        name: "narrow",
        columns: 1,
        value: 24,
        counters: false,
    },
    Shape {
        name: "wide",
        columns: 32,
        value: 24,
        counters: false,
    },
    Shape {
        name: "large",
        columns: 1,
        value: 4000,
        counters: false,
    },
    Shape {
        name: "counters",
        columns: 8,
        value: 8,
        counters: true,
    },
];

const PASSES: usize = 5;

fn key(i: u64) -> Vec<u8> {
    format!("row{i:012}").into_bytes()
}

fn qualifier(c: usize) -> Vec<u8> {
    format!("q{c:03}").into_bytes()
}

fn load(dir: &Path, shape: Shape, mib: u64) -> (Pigeonhole, Table) {
    let path = dir.join(format!("{}.phdb", shape.name));
    let _ = std::fs::remove_file(&path);
    let cell = 15 + 4 + shape.value as u64;
    let rows = (mib << 20) / (cell * shape.columns as u64);
    let db = Pigeonhole::open(
        &path,
        Options::default().block_cache((2 * mib as usize) << 20),
    )
    .unwrap();
    let family = if shape.counters {
        Family::counter()
    } else {
        Family::default()
    };
    let t = db
        .table("t")
        .unwrap()
        .family("f", family)
        .create_if_missing()
        .unwrap();
    let value = vec![0x5a; shape.value];
    let qualifiers: Vec<Vec<u8>> = (0..shape.columns).map(qualifier).collect();
    for i in 0..rows {
        let mut m = t.mutate(&key(i)).durability(Durability::None);
        for q in &qualifiers {
            m = if shape.counters {
                m.put_i64("f", q, i as i64)
            } else {
                m.put("f", q, &value)
            };
        }
        m.commit().unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    (db, t)
}

/// What a pass saw: cells, their key, qualifier and value bytes, and Arrow buffer bytes.
type Pass = (u64, u64, u64);

/// One plain lending pass: cells and their key, qualifier and value bytes.
fn plain(t: &Table) -> Pass {
    let (mut cells, mut bytes) = (0u64, 0u64);
    let mut it = t.scan_prefix(b"").iter().unwrap();
    while let Some(row) = it.next_ref().unwrap() {
        let key = row.key().len() as u64;
        for e in row.iter() {
            cells += 1;
            bytes += key + e.qualifier.len() as u64 + e.cell.value().len() as u64;
        }
    }
    (cells, bytes, 0)
}

/// One export pass: cells, their bytes, and the Arrow buffer bytes produced.
fn export(t: &Table, options: &ArrowOptions) -> Pass {
    let (mut cells, mut bytes, mut arrow) = (0u64, 0u64, 0u64);
    for batch in scan_to_arrow(t.scan_prefix(b""), options).unwrap() {
        let batch = batch.unwrap();
        cells += batch.num_rows() as u64;
        arrow += batch.get_array_memory_size() as u64;
        for name in ["row", "qualifier", "value"] {
            let col = batch.column_by_name(name).unwrap();
            let data = col.to_data();
            bytes += data.buffers().last().map_or(0, |b| b.len() as u64);
        }
    }
    (cells, bytes, arrow)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: arrowexport DIR [MIB]");
    let mib: u64 = args.next().map_or(256, |m| m.parse().expect("MIB"));
    let dir = Path::new(&dir);
    std::fs::create_dir_all(dir).unwrap();
    for shape in SHAPES {
        let (db, t) = load(dir, shape, mib);
        let options = ArrowOptions::default().typed_values(shape.counters);
        plain(&t);
        let mut times: [Vec<(f64, Pass)>; 2] = [Vec::new(), Vec::new()];
        for _ in 0..PASSES {
            for (k, times) in times.iter_mut().enumerate() {
                let start = Instant::now();
                let r = if k == 0 {
                    plain(&t)
                } else {
                    export(&t, &options)
                };
                times.push((start.elapsed().as_secs_f64(), r));
            }
        }
        for (k, mut times) in times.into_iter().enumerate() {
            times.sort_by(|a, b| a.0.total_cmp(&b.0));
            let (secs, (cells, bytes, arrow)) = times[PASSES / 2];
            println!(
                "{{\"shape\":\"{}\",\"pass\":\"{}\",\"cells\":{cells},\"bytes\":{bytes},\
                 \"secs\":{secs:.4},\"gb_per_s\":{:.3},\"arrow_bytes\":{arrow}}}",
                shape.name,
                if k == 0 { "plain" } else { "arrow" },
                bytes as f64 / secs / 1e9,
            );
        }
        drop(t);
        db.close().unwrap();
    }
}
