//! The vector index's measured shapes (D336 Amendment 3, V1), one per run, for
//! `scripts/instructions-per-cell.sh`:
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example vecshapes -- SHAPE ITERATIONS DIR
//! ```
//!
//! | Shape | One iteration | Units (`units N` on stderr) |
//! |---|---|---|
//! | `vector-upsert` | 512 upserts (`Index::insert_f32`) of ids already in the index, each into the list it is in, over an index of 2,000 vectors written in setup | upserts |
//!
//! The index has one list (`target_list_len` 4,096, above its 2,000 vectors), so no upsert
//! moves a vector or splits a list: each is the in-list case, which writes the vector's
//! `L` cell and rewrites its `M` mapping at the commit's timestamp (option A). The vectors
//! are 16-dimension `f32`, the same on every run, and each iteration's are new values, so no
//! upsert writes the value it replaces. The shards are application-owned
//! (`support/shards.rs`), and their work is counted with the caller's.
//!
//! Only the measured work runs inside functions named `shape_*`, each counting its thread's
//! work with a `Measured` guard (`support/measure.rs`).
#[path = "support/measure.rs"]
mod measure;
#[path = "support/shards.rs"]
mod shards;
use measure::Measured;
use shards::Shards;

use pigeonhole::{Element, Metric, VectorSpec};
use pigeonhole_vector::{Index, IndexOptions};

/// Vectors in the index.
const VECTORS: usize = 2_000;
/// Upserts per iteration.
const PER_ITERATION: usize = 512;
const DIM: usize = 16;

fn main() {
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let usage = "usage: vecshapes SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let dir = base.join(format!("phdb-vecshapes-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    // `SHAPE_SETUP_ONLY=1`: the setup alone, without the measured work.
    let setup_only = std::env::var_os("SHAPE_SETUP_ONLY").is_some();
    let units = match shape.as_str() {
        "vector-upsert" => {
            let (db, shards) = Shards::open(&dir.join("v.phdb"), None);
            let spec = VectorSpec::new(DIM as u32, Element::F32, Metric::L2).unwrap();
            let options = IndexOptions::new().target_list_len(4_096);
            let ix = Index::create(&db, "bench", spec, options).unwrap();
            let ids: Vec<Vec<u8>> = (0..VECTORS)
                .map(|i| format!("id{i:06}").into_bytes())
                .collect();
            for (i, id) in ids.iter().enumerate() {
                ix.insert_f32(id, &vector(i, 0)).unwrap();
            }
            let work: Vec<(Vec<u8>, Vec<f32>)> = (0..PER_ITERATION * iters)
                .map(|n| {
                    (
                        ids[n % VECTORS].clone(),
                        vector(n % VECTORS, n / VECTORS + 1),
                    )
                })
                .collect();
            let mut upserted = 0;
            if !setup_only {
                shards.measure(&mut || upserted = shape_vector_upsert(&ix, &work));
                assert_eq!(upserted, work.len());
            }
            // Every upsert stayed in the one list: none moved a vector or split it.
            assert_eq!(ix.verify().unwrap().list_lens.len(), 1);
            drop(ix);
            db.close().unwrap();
            shards.join();
            upserted
        }
        other => panic!("unknown shape {other}: vector-upsert"),
    };
    eprintln!("units {units}");
    std::fs::remove_dir_all(&dir).ok();
}

/// Vector `i`'s value in round `round` (0 in setup): distinct across ids and rounds.
fn vector(i: usize, round: usize) -> Vec<f32> {
    (0..DIM)
        .map(|d| (i * DIM + d) as f32 * 0.001 + round as f32 * 0.0001)
        .collect()
}

#[inline(never)]
fn shape_vector_upsert(ix: &Index, work: &[(Vec<u8>, Vec<f32>)]) -> usize {
    let _measured = Measured::start();
    for (id, x) in work {
        ix.insert_f32(id, x).unwrap();
    }
    work.len()
}
