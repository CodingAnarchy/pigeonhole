//! #691's round trip: for seeded random tables, scans and options, the batches decoded back
//! to cells equal a plain scan's cells, in order. The data mixes binary and UTF-8 keys, the
//! value types, versions, deletes, a counter family and explicit timestamps; the scans mix
//! prefixes, families, versions, time ranges, limits and snapshots; the options mix every
//! column choice with batch limits small enough to split rows across batches.
//!
//! `PIGEONHOLE_ARROW_SEEDS=first..end` picks the seeds (default `1..41`); a failure names
//! its seed.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int32Type, TimestampMicrosecondType, UInt64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use pigeonhole::{Family, Options, Pigeonhole, Scan, Table};
use pigeonhole_arrow::{ArrowOptions, TsType, scan_to_arrow};
use pigeonhole_io::sim::SimVfs;

/// A cell as both sides see it: row, family, qualifier, timestamp, value type, value bytes.
type Cell = (Vec<u8>, String, Vec<u8>, u64, &'static str, Vec<u8>);

/// xorshift64*: small, seeded, no dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn key(rng: &mut Rng, utf8: bool, prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    for _ in 0..1 + rng.below(6) {
        k.push(if utf8 {
            b'a' + rng.below(4) as u8
        } else {
            rng.below(256) as u8
        });
    }
    k
}

fn populate(db: &Pigeonhole, t: &Table, rng: &mut Rng, utf8: bool) -> Option<pigeonhole::Snapshot> {
    let prefixes: [&[u8]; 2] = [b"p", b"q"];
    let mut snapshot = None;
    let writes = 20 + rng.below(80);
    for w in 0..writes {
        let prefix = prefixes[rng.below(2) as usize];
        let row = key(rng, utf8, prefix);
        let q = key(rng, utf8, b"");
        let ts = 1 + rng.below(50);
        let m = t.mutate(&row);
        let m = match rng.below(10) {
            0 => m.put("a", &q, &vec![rng.below(256) as u8; rng.below(40) as usize]),
            1 => m.put_at("a", &q, ts, &rng.next().to_le_bytes()),
            2 => m.put_i64_at("a", &q, ts, rng.next() as i64),
            3 => m.put_f64_at("b", &q, ts, rng.below(1000) as f64 / 8.0),
            4 => m.put_at("b", &q, ts, b"v"),
            5 => m.incr("c", &q, rng.below(10) as i64 - 3),
            6 => m.incr_at("c", &q, ts, 1),
            7 => m.delete_column("a", &q),
            8 => m.delete_cell("b", &q, ts),
            _ => {
                if rng.chance(30) {
                    m.delete_row()
                } else {
                    m.put("b", &q, b"")
                }
            }
        };
        m.commit().unwrap();
        if snapshot.is_none() && w == writes / 2 && rng.chance(40) {
            snapshot = Some(db.snapshot().unwrap());
        }
    }
    snapshot
}

/// A random scan of `t`.
fn scan<'t>(t: &'t Table, rng: &mut Rng, snapshot: Option<&pigeonhole::Snapshot>) -> Scan<'t> {
    let mut s = match rng.below(3) {
        0 => t.scan_prefix(b""),
        1 => t.scan_prefix(b"p"),
        _ => t.scan(&b"p"[..]..&b"q\x80"[..]),
    };
    match rng.below(4) {
        0 => s = s.family("a"),
        1 => s = s.families(["c", "b"]),
        _ => {}
    }
    if rng.chance(50) {
        s = s.versions(rng.below(4) as u32);
    }
    if rng.chance(25) {
        let lo = rng.below(30);
        s = s.time_range(lo..lo + 1 + rng.below(30));
    }
    if rng.chance(20) {
        s = s.limit(rng.below(10));
    }
    if let Some(snap) = snapshot
        && rng.chance(60)
    {
        s = s.snapshot(snap);
    }
    s
}

/// Random options, with their cell limit and whether they add the typed columns.
fn options(rng: &mut Rng, utf8: bool) -> (ArrowOptions, usize, bool) {
    let typed = rng.chance(50);
    let mut cells = 8192;
    let mut o = ArrowOptions::default()
        .row_dictionary(rng.chance(50))
        .typed_values(typed)
        .utf8_keys(utf8 && rng.chance(70))
        .ts(if rng.chance(50) {
            TsType::Timestamp
        } else {
            TsType::UInt64
        });
    if rng.chance(80) {
        cells = 1 + rng.below(12) as usize;
        o = o.batch_cells(cells);
    }
    if rng.chance(40) {
        o = o.batch_bytes(1 + rng.below(200) as usize);
    }
    (o, cells, typed)
}

fn type_name(v: pigeonhole::Value<'_>) -> &'static str {
    match v {
        pigeonhole::Value::Bytes(_) => "bytes",
        pigeonhole::Value::I64(_) => "i64",
        pigeonhole::Value::F64(_) => "f64",
        pigeonhole::Value::Varint(_) => "varint",
        other => panic!("a value type this test doesn't know: {other:?}"),
    }
}

fn plain(scan: Scan<'_>) -> Vec<Cell> {
    let mut it = scan.iter().unwrap();
    let mut out = Vec::new();
    while let Some(row) = it.next_ref().unwrap() {
        for e in row.iter() {
            out.push((
                row.key().to_vec(),
                e.family.to_owned(),
                e.qualifier.to_vec(),
                e.cell.timestamp(),
                type_name(e.cell.typed()),
                e.cell.value().to_vec(),
            ));
        }
    }
    out
}

/// A byte column (`Binary`, `Utf8`, or a dictionary of either) as one `Vec<u8>` per row.
fn bytes_column(a: &dyn Array) -> Vec<Vec<u8>> {
    match a.data_type() {
        DataType::Binary => a
            .as_binary::<i32>()
            .iter()
            .map(|v| v.unwrap().to_vec())
            .collect(),
        DataType::Utf8 => a
            .as_string::<i32>()
            .iter()
            .map(|v| v.unwrap().as_bytes().to_vec())
            .collect(),
        DataType::Dictionary(..) => {
            let d = a.as_dictionary::<Int32Type>();
            let values = bytes_column(d.values().as_ref());
            d.keys()
                .iter()
                .map(|k| values[k.unwrap() as usize].clone())
                .collect()
        }
        other => panic!("not a byte column: {other}"),
    }
}

fn strings(a: &dyn Array) -> Vec<String> {
    bytes_column(a)
        .into_iter()
        .map(|b| String::from_utf8(b).unwrap())
        .collect()
}

/// Decodes `batch` back to cells, checking the typed columns against `value_type`.
fn decode(batch: &RecordBatch, typed: bool) -> Vec<Cell> {
    let col = |name: &str| batch.column_by_name(name).unwrap().as_ref();
    let rows = bytes_column(col("row"));
    let families = strings(col("family"));
    let qualifiers = bytes_column(col("qualifier"));
    let ts: Vec<u64> = match col("ts").data_type() {
        DataType::UInt64 => col("ts").as_primitive::<UInt64Type>().values().to_vec(),
        DataType::Timestamp(..) => col("ts")
            .as_primitive::<TimestampMicrosecondType>()
            .values()
            .iter()
            .map(|&v| u64::try_from(v).unwrap())
            .collect(),
        other => panic!("ts is {other}"),
    };
    let values = bytes_column(col("value"));
    let types = col("value_type").as_dictionary::<Int8Type>();
    let type_names = strings(types.values().as_ref());
    let mut out = Vec::new();
    for i in 0..batch.num_rows() {
        let kind = type_names[types.keys().value(i) as usize].clone();
        let kind: &'static str = match kind.as_str() {
            "bytes" => "bytes",
            "i64" => "i64",
            "f64" => "f64",
            "varint" => "varint",
            other => panic!("value_type {other}"),
        };
        if typed {
            let ints = col("value_i64").as_primitive::<arrow_array::types::Int64Type>();
            let floats = col("value_f64").as_primitive::<arrow_array::types::Float64Type>();
            let v = &values[i];
            match kind {
                "i64" | "varint" => {
                    assert!(ints.is_valid(i) && floats.is_null(i));
                    assert_eq!(ints.value(i).to_le_bytes().as_slice(), v.as_slice());
                }
                "f64" => {
                    assert!(floats.is_valid(i) && ints.is_null(i));
                    assert_eq!(floats.value(i).to_le_bytes().as_slice(), v.as_slice());
                }
                _ => assert!(ints.is_null(i) && floats.is_null(i)),
            }
        }
        out.push((
            rows[i].clone(),
            families[i].clone(),
            qualifiers[i].clone(),
            ts[i],
            kind,
            values[i].clone(),
        ));
    }
    out
}

fn seeds() -> std::ops::Range<u64> {
    let Ok(s) = std::env::var("PIGEONHOLE_ARROW_SEEDS") else {
        return 1..41;
    };
    let (a, b) = s.split_once("..").expect("first..end");
    a.parse().unwrap()..b.parse().unwrap()
}

#[test]
fn batches_decode_to_a_plain_scans_cells() {
    for seed in seeds() {
        let mut rng = Rng::new(seed);
        let utf8 = rng.chance(50);
        let vfs = SimVfs::new(seed);
        let db = Pigeonhole::open(
            "/db/rt.phdb",
            Options::default()
                .vfs(Arc::clone(&vfs) as _)
                .shards(1 + rng.below(2) as usize),
        )
        .unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("a", Family::default())
            .family("b", Family::default().max_versions(2))
            .family("c", Family::counter())
            .create_if_missing()
            .unwrap();
        let snapshot = populate(&db, &t, &mut rng, utf8);
        if rng.chance(30) {
            db.flush().unwrap();
        }
        for round in 0..4 {
            let scan_seed = rng.next();
            let (o, cells, typed) = options(&mut rng, utf8);
            let want = plain(scan(&t, &mut Rng::new(scan_seed), snapshot.as_ref()));
            let export = scan_to_arrow(scan(&t, &mut Rng::new(scan_seed), snapshot.as_ref()), &o)
                .unwrap_or_else(|e| panic!("seed {seed} round {round}: {e}"));
            let schema = export.schema();
            assert_eq!(schema, pigeonhole_arrow::schema(&o));
            let mut got = Vec::new();
            for batch in export {
                let batch = batch.unwrap_or_else(|e| panic!("seed {seed} round {round}: {e}"));
                assert_eq!(batch.schema(), schema);
                assert!(batch.num_rows() > 0, "seed {seed}: an empty batch");
                assert!(batch.num_rows() <= cells, "seed {seed}");
                got.extend(decode(&batch, typed));
            }
            assert_eq!(
                got, want,
                "seed {seed} round {round}: the batches are not the plain scan's cells ({o:?})"
            );
        }
        drop(t);
        db.close().unwrap();
    }
}
