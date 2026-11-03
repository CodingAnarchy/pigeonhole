//! #1283 lever 2: a scan lane returns a plain value of 256 B to 4 KiB pinned where it lies
//! (its memtable or SST block) instead of copied. Every cell a scan returns, over values on
//! both sides of those bounds in memtables and SSTs, with overwrites, deletes and every
//! version, is the value written.

use crate::common;

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineShard, FamilyOptions, ScanSpec, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

fn drive<F: Future + Unpin>(shards: &mut [EngineShard], mut pending: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
            return r;
        }
    }
    panic!("never resolved");
}

/// A value of `len` bytes that names its row, column and write.
fn value(i: u32, q: u8, write: u32, len: usize) -> Vec<u8> {
    let mut v = format!("{i}/{q}/{write}/").into_bytes();
    v.resize(len, (i as u8) ^ q ^ (write as u8));
    v
}

#[test]
fn a_scan_returns_every_pinned_value_as_written() {
    let vfs = SimVfs::new(0x1283);
    let opts = || common::options(Arc::clone(&vfs), 2, 64 << 20);
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), opts()).unwrap();
    let t = engine
        .create_table("t", &[("e".into(), FamilyOptions::default())])
        .unwrap();
    let e = t.families[0].id;
    // Around the copy/pin bounds: below 256, at it, a vector's size, and around 4 KiB.
    let lens = [200usize, 255, 256, 257, 300, 1536, 4095, 4096, 4097, 6000];
    // (row, qualifier) -> every version, newest first.
    let mut model: BTreeMap<(u32, u8), Vec<Vec<u8>>> = BTreeMap::new();
    let rows = 300u32;
    for write in 0..3u32 {
        // In batches of 20 rows: a record holds a few hundred KiB.
        for chunk in (0..rows).step_by(20) {
            let mut wb = WriteBatch::new();
            for i in chunk..chunk + 20 {
                // Each write overwrites a different part of the rows.
                if write > 0 && i % (write + 2) != 0 {
                    continue;
                }
                for q in *b"ab" {
                    let len = lens[(i as usize + q as usize + write as usize) % lens.len()];
                    let v = value(i, q, write, len);
                    let row = format!("r{i:04}").into_bytes();
                    wb.put(t.id, e, &row, &[q], None, ValueRef::Bytes(&v))
                        .unwrap();
                    model.entry((i, q)).or_default().insert(0, v);
                }
            }
            drive(
                &mut shards,
                engine.submit(wb, Some(Durability::Buffered)).unwrap(),
            )
            .unwrap();
        }
        if write < 2 {
            // The first two writes go to SSTs, the last stays in the memtable.
            drive(&mut shards, engine.submit_flush().unwrap()).unwrap();
        }
    }
    let mut wb = WriteBatch::new();
    for i in (3..rows).step_by(17) {
        wb.delete_row(t.id, format!("r{i:04}").as_bytes(), None)
            .unwrap();
        model.retain(|(r, _), _| *r != i);
    }
    drive(
        &mut shards,
        engine.submit(wb, Some(Durability::Buffered)).unwrap(),
    )
    .unwrap();

    let snap = engine.snapshot().unwrap();
    for versions in [1u32, 0] {
        let mut spec = ScanSpec::new(Bound::Unbounded, Bound::Unbounded);
        spec.read.versions = versions;
        let mut cur = engine.scan(&snap, t.id, spec).unwrap();
        let mut got: BTreeMap<(u32, u8), Vec<Vec<u8>>> = BTreeMap::new();
        while cur.next_row().unwrap() {
            let row = cur.row().to_vec();
            let i: u32 = std::str::from_utf8(&row[1..]).unwrap().parse().unwrap();
            while let Some(c) = cur.next_cell().unwrap() {
                got.entry((i, c.qualifier[0]))
                    .or_default()
                    .push(c.stored.to_vec());
            }
        }
        let want: BTreeMap<(u32, u8), Vec<Vec<u8>>> = model
            .iter()
            .map(|(k, vs)| {
                let n = if versions == 0 { vs.len() } else { 1 };
                (*k, vs[..n].iter().map(|v| stored(v)).collect())
            })
            .collect();
        assert_eq!(got.len(), want.len(), "versions {versions}: columns");
        for (k, vs) in &want {
            assert_eq!(got.get(k), Some(vs), "versions {versions}: {k:?}");
        }
    }
    drop(snap);
    engine.close().unwrap();
}

/// The stored form of a `Bytes` value.
fn stored(v: &[u8]) -> Vec<u8> {
    let mut s = Vec::new();
    pigeonhole_format::value::encode_value(&mut s, pigeonhole_format::value::ValueRef::Bytes(v));
    s
}
