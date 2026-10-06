//! Property test: random operation sequences agree with a `Vec<u8>` model on both backends,
//! and a fault-free power loss restores exactly the last synced model state: the bytes as of
//! the last sync of either kind, at the length as of the last `sync_all`.

mod common;

use common::Backend;
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{ErrorKind, OpenOptions, Vfs};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Write(u64, Vec<u8>),
    SetLen(u64),
    Allocate(u64, u64),
    Read(u64, usize),
    SyncData,
    SyncAll,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..6000u64, proptest::collection::vec(any::<u8>(), 1..700))
            .prop_map(|(o, d)| Op::Write(o, d)),
        1 => (0..7000u64).prop_map(Op::SetLen),
        1 => (0..6000u64, 0..2000u64).prop_map(|(o, l)| Op::Allocate(o, l)),
        3 => (0..7000u64, 0..600usize).prop_map(|(o, l)| Op::Read(o, l)),
        1 => Just(Op::SyncData),
        1 => Just(Op::SyncAll),
    ]
}

/// Runs `ops` against `b` and the model; returns what a fault-free power loss keeps: the
/// bytes as of the last sync, resized to the length as of the last `sync_all`.
fn run(b: &Backend, ops: &[Op]) -> Result<Vec<u8>, TestCaseError> {
    let f = b.create("model");
    let mut model: Vec<u8> = Vec::new();
    let mut synced = Vec::new();
    let mut synced_len = 0;
    for op in ops {
        match op {
            Op::Write(off, data) => {
                f.write_at(data, *off).unwrap();
                let end = *off as usize + data.len();
                if model.len() < end {
                    model.resize(end, 0);
                }
                model[*off as usize..end].copy_from_slice(data);
            }
            Op::SetLen(len) => {
                f.set_len(*len).unwrap();
                model.resize(*len as usize, 0);
            }
            Op::Allocate(off, len) => {
                f.allocate(*off, *len).unwrap();
                model.resize(model.len().max((off + len) as usize), 0);
            }
            Op::Read(off, len) => {
                let mut buf = vec![0; *len];
                let end = *off as usize + len;
                match f.read_at(&mut buf, *off) {
                    Ok(()) => {
                        prop_assert!(*len == 0 || end <= model.len(), "{}: read past end", b.name);
                        if *len > 0 {
                            prop_assert_eq!(&buf[..], &model[*off as usize..end], "{}", b.name);
                        }
                    }
                    Err(e) => {
                        prop_assert_eq!(e.kind, ErrorKind::UnexpectedEof, "{}", b.name);
                        prop_assert!(end > model.len(), "{}", b.name);
                    }
                }
            }
            Op::SyncData => {
                f.sync_data().unwrap();
                synced.clone_from(&model);
            }
            Op::SyncAll => {
                f.sync_all().unwrap();
                synced.clone_from(&model);
                synced_len = model.len();
            }
        }
        prop_assert_eq!(f.len().unwrap(), model.len() as u64, "{}", b.name);
    }
    synced.resize(synced_len, 0);
    Ok(synced)
}

proptest! {
    #[test]
    fn sim_matches_model(ops in proptest::collection::vec(op(), 1..40), seed in any::<u64>()) {
        run(&Backend::sim(seed), &ops)?;
    }

    #[test]
    #[cfg(not(miri))]
    fn pread_matches_model(ops in proptest::collection::vec(op(), 1..40)) {
        run(&Backend::pread("model"), &ops)?;
    }

    #[test]
    fn power_loss_restores_last_sync(
        ops in proptest::collection::vec(op(), 1..40),
        seed in any::<u64>(),
    ) {
        let sim = SimVfs::new(seed);
        let b = Backend::sim_from(sim.clone());
        let synced = run(&b, &ops)?;
        sim.sync_dir(&b.root).unwrap();
        sim.crash(CrashKind::Power);
        let f = sim.open(&b.path("model"), OpenOptions::read()).unwrap();
        let mut buf = vec![0; f.len().unwrap() as usize];
        f.read_at(&mut buf, 0).unwrap();
        prop_assert_eq!(buf, synced, "seed {}", seed);
    }
}
