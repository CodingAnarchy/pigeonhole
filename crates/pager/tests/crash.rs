//! Crash safety: a crash at every write point never yields an unopenable file and always
//! recovers to the last committed root (or the one being committed when the crash hit).
//!
//! The workload mimics the engine: each step writes data extents, writes a "manifest"
//! snapshot naming every live extent and the tag its bytes were written with, commits a root
//! pointing at that snapshot, then retires and reclaims what the new root dropped. Recovery
//! reads the snapshot named by the superblock, checks every live extent's bytes, rebuilds the
//! allocator from the live set and keeps going.

mod common;

use std::path::Path;

use common::{Rng, overlaps};
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{ErrorKind, VfsRef};
use pigeonhole_pager::{Error, Extent, Pager, Root};

const PATH: &str = "/db/data.phdb";
const STEPS: u64 = 10;
const STAMP: usize = 4096;

/// A live extent and the tag its bytes were written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Data {
    extent: Extent,
    tag: u64,
}

fn stamp(tag: u64, extent: Extent, at_end: bool) -> Vec<u8> {
    let mut rng = Rng(tag ^ extent.page.rotate_left(17) ^ u64::from(at_end));
    let mut v = Vec::with_capacity(STAMP);
    while v.len() < STAMP {
        v.extend_from_slice(&rng.next().to_le_bytes());
    }
    v
}

fn write_data(pager: &Pager, d: Data) -> pigeonhole_pager::Result<()> {
    pager.write(d.extent, 0, &stamp(d.tag, d.extent, false))?;
    pager.write(
        d.extent,
        d.extent.len() - STAMP as u64,
        &stamp(d.tag, d.extent, true),
    )
}

fn check_data(pager: &pigeonhole_io::FileRef, d: Data) {
    let mut buf = vec![0u8; STAMP];
    pager.read_at(&mut buf, d.extent.offset()).unwrap();
    assert_eq!(buf, stamp(d.tag, d.extent, false), "head of {d:?}");
    pager
        .read_at(&mut buf, d.extent.offset() + d.extent.len() - STAMP as u64)
        .unwrap();
    assert_eq!(buf, stamp(d.tag, d.extent, true), "tail of {d:?}");
}

fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn encode_manifest(version: u64, live: &[Data]) -> Vec<u8> {
    let mut v = version.to_le_bytes().to_vec();
    v.extend_from_slice(&(live.len() as u32).to_le_bytes());
    for d in live {
        v.extend_from_slice(&d.extent.page.to_le_bytes());
        v.push(d.extent.size_class);
        v.extend_from_slice(&d.tag.to_le_bytes());
    }
    let sum = fnv(&v);
    v.extend_from_slice(&sum.to_le_bytes());
    v
}

fn decode_manifest(b: &[u8]) -> (u64, Vec<Data>) {
    let (body, sum) = b.split_at(b.len() - 8);
    assert_eq!(
        fnv(body),
        u64::from_le_bytes(sum.try_into().unwrap()),
        "manifest checksum"
    );
    let version = u64::from_le_bytes(body[..8].try_into().unwrap());
    let n = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
    let live = (0..n)
        .map(|i| {
            let r = &body[12 + i * 17..12 + (i + 1) * 17];
            Data {
                extent: Extent {
                    page: u64::from_le_bytes(r[..8].try_into().unwrap()),
                    size_class: r[8],
                },
                tag: u64::from_le_bytes(r[9..17].try_into().unwrap()),
            }
        })
        .collect();
    (version, live)
}

/// What the workload knows about commits when it stops.
#[derive(Debug, Clone, Default)]
struct Progress {
    created: bool,
    /// The last root whose commit returned `Ok`.
    committed: Root,
    /// A root whose commit was started but did not return `Ok`.
    attempted: Option<Root>,
}

/// The workload state carried across steps.
struct State {
    pager: Pager,
    version: u64,
    live: Vec<Data>,
    snapshot: Option<Extent>,
}

fn crashed(e: &Error) -> bool {
    matches!(e, Error::Io(io) if io.kind == ErrorKind::Crashed)
}

/// One engine-like step: allocate, write, publish, retire, reclaim. Updates `progress`.
fn step(st: &mut State, rng: &mut Rng, progress: &mut Progress) -> pigeonhole_pager::Result<()> {
    let pager = &st.pager;
    let version = st.version + 1;
    // Drop some live extents and add new ones.
    let mut next: Vec<Data> = Vec::new();
    let mut dropped: Vec<Extent> = Vec::new();
    for d in &st.live {
        if rng.below(3) == 0 {
            dropped.push(d.extent);
        } else {
            next.push(*d);
        }
    }
    for _ in 0..1 + rng.below(3) {
        let bytes = (64 << 10) << rng.below(3);
        let d = Data {
            extent: pager.allocate(bytes)?,
            tag: rng.next(),
        };
        write_data(pager, d)?;
        next.push(d);
    }
    // An abandoned output: allocated, written, never published.
    if rng.below(4) == 0 {
        let e = pager.allocate(64 << 10)?;
        pager.write(e, 0, &[0xAB; 512])?;
        pager.abandon(e);
    }
    let manifest = encode_manifest(version, &next);
    let snapshot = pager.allocate(manifest.len() as u64)?;
    pager.write(snapshot, 0, &manifest)?;
    let root = Root {
        snapshot: Some(snapshot),
        snapshot_len: manifest.len() as u32,
        manifest_version: version,
        ..Root::default()
    };
    progress.attempted = Some(root);
    if version.is_multiple_of(2) {
        pager.submit_commit_root(root).wait()?;
    } else {
        pager.commit_root(root)?;
    }
    progress.committed = root;
    progress.attempted = None;
    // No view outlives the commit in this workload, so everything dropped is reclaimable.
    for e in dropped.into_iter().chain(st.snapshot) {
        pager.retire(e, version);
    }
    pager.reclaim(version);
    if rng.below(4) == 0 {
        pager.truncate_tail()?;
    }
    st.version = version;
    st.live = next;
    st.snapshot = Some(snapshot);
    Ok(())
}

/// Runs the workload until it finishes or the first error (the injected crash).
fn workload(vfs: &VfsRef, seed: u64, progress: &mut Progress) -> pigeonhole_pager::Result<()> {
    let mut rng = Rng(seed);
    let pager = Pager::create(vfs, Path::new(PATH))?;
    progress.created = true;
    let mut st = State {
        pager,
        version: 0,
        live: Vec::new(),
        snapshot: None,
    };
    for _ in 0..STEPS {
        step(&mut st, &mut rng, progress)?;
    }
    st.pager.mark_clean()
}

/// Opens after a crash and checks the recovered root, every live byte and that recovery
/// can continue. Returns the recovered manifest version.
fn recover(vfs: &VfsRef, progress: &Progress, seed: u64) -> u64 {
    let opened = Pager::open(vfs, Path::new(PATH), true)
        .unwrap_or_else(|e| panic!("unopenable after crash ({progress:?}): {e}"));
    let root = opened.root();
    let acceptable = root == progress.committed || Some(root) == progress.attempted;
    assert!(acceptable, "recovered {root:?}, expected {progress:?}");
    let live = match root.snapshot {
        None => Vec::new(),
        Some(snapshot) => {
            let mut buf = vec![0u8; root.snapshot_len as usize];
            opened.file().read_at(&mut buf, snapshot.offset()).unwrap();
            let (version, live) = decode_manifest(&buf);
            assert_eq!(version, root.manifest_version);
            for d in &live {
                check_data(opened.file(), *d);
            }
            live
        }
    };
    let extents: Vec<Extent> = live.iter().map(|d| d.extent).chain(root.snapshot).collect();
    let pager = opened.finish(extents.iter().copied()).unwrap();

    // Keep going: new allocations never touch recovered data, and a new root commits.
    let mut st = State {
        pager,
        version: root.manifest_version,
        live,
        snapshot: root.snapshot,
    };
    let mut rng = Rng(seed ^ 0xC0FFEE);
    let mut after = Progress {
        created: true,
        committed: root,
        attempted: None,
    };
    let fresh = st.pager.allocate(256 << 10).unwrap();
    assert!(
        !extents.iter().any(|&e| overlaps(e, fresh)),
        "{fresh:?} overlaps live data"
    );
    st.pager.abandon(fresh);
    step(&mut st, &mut rng, &mut after).unwrap();
    drop(st);
    let reopened = Pager::open(vfs, Path::new(PATH), false).unwrap();
    assert_eq!(reopened.root(), after.committed);
    root.manifest_version
}

fn sweep(name: &str, plan: FaultPlan, seeds: &[u64]) {
    for &seed in seeds {
        // A clean run sizes the sweep.
        let clean = SimVfs::new(seed);
        let clean_ref: VfsRef = clean.clone();
        workload(&clean_ref, seed, &mut Progress::default()).unwrap();
        let total = clean.mutating_ops();
        let mut recovered_versions = std::collections::BTreeSet::new();
        for n in 1..=total {
            let mut p = plan.clone();
            p.crash_after_ops = Some(n);
            let sim = SimVfs::with_faults(seed, p);
            let vfs: VfsRef = sim.clone();
            let mut progress = Progress::default();
            let r = workload(&vfs, seed, &mut progress);
            match &r {
                Err(e) if crashed(e) => {}
                Ok(()) => {} // the crash hit the very last operation
                Err(e) => panic!("{name} seed {seed} n {n}: unexpected error {e}"),
            }
            // Crash at n fires after the n-th op; the next op saw it. Make sure the state
            // really is the post-crash one even when the workload had already finished.
            sim.crash(CrashKind::Power);
            sim.set_faults(FaultPlan::none());
            if !progress.created {
                continue; // create never returned: nothing was promised
            }
            let v = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                recover(&vfs, &progress, seed)
            }))
            .unwrap_or_else(|e| {
                eprintln!("{name}: failed at seed {seed} crash_after_ops {n} of {total}");
                std::panic::resume_unwind(e)
            });
            recovered_versions.insert(v);
        }
        // The sweep really crossed every commit.
        assert!(
            recovered_versions.len() as u64 >= STEPS,
            "{name}: recovered only {recovered_versions:?}"
        );
    }
}

fn seeds() -> Vec<u64> {
    let s = common::seed();
    vec![s, s.wrapping_add(1), s.wrapping_add(2)]
}

#[test]
fn power_loss_at_every_write_point() {
    sweep("power", FaultPlan::none(), &seeds());
}

#[test]
fn torn_writes_at_every_write_point() {
    let mut plan = FaultPlan::none();
    plan.torn_writes = true;
    sweep("torn", plan, &seeds());
}

#[test]
fn reordered_unsynced_writes_at_every_write_point() {
    let mut plan = FaultPlan::none();
    plan.torn_writes = true;
    plan.reorder_unsynced = true;
    sweep("reorder", plan, &seeds());
}

#[test]
fn process_crash_keeps_every_completed_write() {
    let seed = common::seed();
    for crash_at in 1..=STEPS {
        let sim = SimVfs::new(seed);
        let vfs: VfsRef = sim.clone();
        let mut rng = Rng(seed);
        let mut progress = Progress::default();
        let mut st = State {
            pager: Pager::create(&vfs, Path::new(PATH)).unwrap(),
            version: 0,
            live: Vec::new(),
            snapshot: None,
        };
        progress.created = true;
        for _ in 0..crash_at {
            step(&mut st, &mut rng, &mut progress).unwrap();
        }
        sim.crash(CrashKind::Process);
        let v = recover(&vfs, &progress, seed);
        assert_eq!(v, crash_at, "a process crash loses no committed root");
    }
}

#[test]
fn clean_close_is_recorded_and_cleared() {
    let vfs: VfsRef = SimVfs::new(common::seed());
    let mut progress = Progress::default();
    workload(&vfs, 11, &mut progress).unwrap();
    let opened = Pager::open(&vfs, Path::new(PATH), true).unwrap();
    assert!(opened.clean_shutdown());
    assert_eq!(opened.root(), progress.committed);
    let root = opened.root();
    let pager = opened.finish(root.snapshot).unwrap();
    pager.commit_root(root).unwrap();
    drop(pager);
    assert!(
        !Pager::open(&vfs, Path::new(PATH), false)
            .unwrap()
            .clean_shutdown()
    );
}
