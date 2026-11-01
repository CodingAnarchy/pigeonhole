//! The Rust half of the Python bindings' per-op overhead benchmark (#696): the operations
//! `bench/overhead.py` times, through `pigeonhole` directly, with the same options and data.
//! Prints `op ns_per_unit` lines.
//!
//! `cargo run --release -p pigeonhole-python --example overhead -- DIR`

use std::hint::black_box;
use std::time::Instant;

use pigeonhole::{Durability, Family, KeySet, Options, Pigeonhole};

const ROWS: usize = 1_000;
const ROUNDS: usize = 20_000;

fn main() -> pigeonhole::Result<()> {
    let dir = std::env::args()
        .nth(1)
        .expect("a directory for the database");
    let path = std::path::Path::new(&dir).join("rust.phdb");
    let db = Pigeonhole::open(
        &path,
        Options::default().shards(1).durability(Durability::None),
    )?;
    let t = db
        .table("t")?
        .family("f", Family::default())
        .create_if_missing()?;
    let mut wb = db.write_batch();
    for i in 0..ROWS {
        wb.put(
            &t,
            format!("r{i:05}").as_bytes(),
            "f",
            b"q",
            b"value-0123456789",
        );
    }
    wb.commit()?;
    let report = |op: &str, units: usize, started: Instant| {
        println!(
            "{op} {:.0}",
            started.elapsed().as_nanos() as f64 / units as f64
        );
    };

    let started = Instant::now();
    for i in 0..ROUNDS {
        let key = format!("r{:05}", i % ROWS);
        black_box(t.get(key.as_bytes(), "f", b"q")?.map(|c| c.to_owned()));
    }
    report("get", ROUNDS, started);

    let started = Instant::now();
    for i in 0..ROUNDS {
        let key = format!("p{i:06}");
        t.mutate(key.as_bytes())
            .put("f", b"q", b"value-0123456789")
            .commit()?;
    }
    report("put", ROUNDS, started);

    let started = Instant::now();
    for i in 0..ROUNDS {
        let key = format!("m{i:06}");
        t.mutate(key.as_bytes())
            .put("f", b"a", b"x")
            .put_i64("f", b"b", 7)
            .put_f64("f", b"c", 0.5)
            .commit()?;
    }
    report("mutate3", ROUNDS, started);

    let started = Instant::now();
    let mut rows = 0;
    for _ in 0..20 {
        let mut it = t.scan_prefix(b"r").iter()?;
        while let Some(r) = it.next_ref()? {
            black_box(r.to_owned());
            rows += 1;
        }
    }
    report("scan_row", rows, started);

    let keys = KeySet::from_unsorted((0..64).map(|i| format!("r{:05}", i * 15)));
    let started = Instant::now();
    let mut got = 0;
    for _ in 0..500 {
        let mut it = t.keys(&keys).read()?;
        while let Some((_, r)) = it.next()? {
            black_box(r.to_owned());
            got += 1;
        }
    }
    report("multi_get_key", got, started);
    db.close()
}
