//! #1286 (ICR 0122): `Engine::reduce_async` reads in one pass with one reducer, and gives what
//! `reduce_into` gives when it fans out over tablet groups and merges them (D296 rev 1.2), for
//! a byte budget over a range and over a key set, at budgets across row and column bounds.

use crate::common;

use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    ByteBudget, CellRef, CellVisitor, Engine, FamilyId, FamilyOptions, KeySet, ReadInput,
    ReduceSpec, Timestamp, ValueRef, Visit, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

fn block_on<F: Future + Unpin>(mut f: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = Pin::new(&mut f).poll(&mut cx) {
            return v;
        }
        std::thread::yield_now();
    }
}

type SizeFn = fn(&CellRef<'_>, &Vec<u8>) -> u64;

struct Copy;

impl CellVisitor for Copy {
    type Item = Vec<u8>;
    fn visit(&mut self, _: &[u8], _: FamilyId, _: &[u8], _: Timestamp, v: &[u8]) -> Visit<Vec<u8>> {
        Visit::Emit(v.to_vec())
    }
}

#[allow(clippy::ptr_arg)]
fn size(c: &CellRef<'_>, v: &Vec<u8>) -> u64 {
    c.qualifier.len() as u64 + v.len() as u64 + 16
}

#[test]
fn an_async_bounded_read_equals_a_fanned_out_one() {
    let vfs = SimVfs::new(1286);
    let db = Engine::open(
        Path::new("/db/data.phdb"),
        common::options(Arc::clone(&vfs), 4, 64 << 20),
    )
    .unwrap();
    let t = db
        .create_table(
            "t",
            &[
                ("f".into(), FamilyOptions::default()),
                ("g".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    let (f, g) = (t.families[0].id, t.families[1].id);
    // Four tablets over the four shards, so the sync read has groups to fan out over.
    db.split_table(
        t.id,
        [100u32, 200, 300]
            .map(|i| format!("r{i:04}").into_bytes())
            .to_vec(),
        &[0, 1, 2, 3],
    )
    .unwrap();
    for i in 0..400u32 {
        let mut wb = WriteBatch::new();
        let row = format!("r{i:04}");
        for c in 0..(1 + i % 5) {
            let fam = if c % 2 == 0 { f } else { g };
            let v = vec![b'a' + c as u8; 4 + (i as usize * 7 + c as usize) % 60];
            wb.put(
                t.id,
                fam,
                row.as_bytes(),
                &[b'q', c as u8],
                None,
                ValueRef::Bytes(&v),
            )
            .unwrap();
        }
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        if i == 200 {
            db.flush().unwrap();
        }
    }
    let snap = db.snapshot().unwrap();
    let keys = Arc::new(KeySet::from_unsorted(
        (0..400u32)
            .step_by(7)
            .map(|i| format!("r{i:04}").into_bytes()),
    ));
    let inputs = [
        ReadInput::Ranges(vec![(Bound::Unbounded, Bound::Unbounded)]),
        ReadInput::Keys(keys),
    ];
    for input in inputs {
        for budget in [0, 30, 500, 5_000, 40_000, u64::MAX] {
            let mut spec = ReduceSpec::new(input.clone());
            spec.read.families = vec![f, g];
            // Fanned out whenever there are groups.
            spec.parallelism = 4;
            spec.parallel_floor = 0;
            let (sync, stats) = db
                .reduce_into(
                    &snap,
                    t.id,
                    &spec,
                    || Copy,
                    || ByteBudget::new(budget, size),
                )
                .unwrap();
            let fut = db
                .reduce_async(
                    &snap,
                    t.id,
                    &spec,
                    Copy,
                    ByteBudget::new(budget, size as SizeFn),
                )
                .unwrap();
            let async_page = block_on(fut).unwrap().into_page();
            let sync_page = sync.into_page();
            let what = format!("{input:?} budget {budget} ({} groups)", stats.groups);
            if matches!(input, ReadInput::Ranges(_)) && budget == u64::MAX {
                assert!(stats.groups > 1, "the sync read fans out: {what}");
            }
            assert_eq!(async_page.items, sync_page.items, "{what}");
            assert_eq!(async_page.bytes, sync_page.bytes, "{what}");
            assert_eq!(
                async_page.overflow.map(|o| o.bytes),
                sync_page.overflow.map(|o| o.bytes),
                "{what}"
            );
        }
    }
    drop(snap);
    db.close().unwrap();
}
