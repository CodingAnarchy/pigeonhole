//! Crash safety: a crash at every write point never yields an unopenable file and always
//! recovers to the last committed root (or the one being committed when the crash hit).
//!
//! The workload mimics the engine: each step writes data extents, sometimes relocates
//! published ones toward the start of the file (online shrink), then records the new live set
//! as a manifest: appended as a delta past the live end of the delta log, or, when the log is
//! full, written as a new snapshot with a fresh empty log (D7). It commits a root naming them,
//! then retires and reclaims what the new root dropped. Recovery reads the snapshot and the
//! live log named by the superblock, checks every live extent's bytes, rebuilds the allocator
//! from the live set and keeps going.

mod common;

use std::path::Path;

use common::{Rng, overlaps};
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{ErrorKind, FileRef, VfsRef};
use pigeonhole_pager::{Error, Extent, Pager, Root};

const PATH: &str = "/db/data.phdb";
const STEPS: u64 = 12;
const STAMP: usize = 4096;
/// Delta-log extent size (D7).
const LOG_EXTENT: u64 = 256 << 10;
/// Bytes of deltas after which the workload rolls over to a new snapshot and log; small so
/// the sweep crosses several rollovers.
const LOG_BUDGET: u32 = 1024;

/// A live extent and the tag its bytes were written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Data {
    extent: Extent,
    tag: u64,
}

/// The stamp depends only on the tag, so a relocated copy still verifies.
fn stamp(tag: u64, at_end: bool) -> Vec<u8> {
    let mut rng = Rng(tag ^ u64::from(at_end));
    let mut v = Vec::with_capacity(STAMP);
    while v.len() < STAMP {
        v.extend_from_slice(&rng.next().to_le_bytes());
    }
    v
}

fn write_data(pager: &Pager, d: Data) -> pigeonhole_pager::Result<()> {
    pager.write(d.extent, 0, &stamp(d.tag, false))?;
    pager.write(d.extent, d.extent.len() - STAMP as u64, &stamp(d.tag, true))
}

fn check_data(file: &FileRef, d: Data) {
    let mut buf = vec![0u8; STAMP];
    file.read_at(&mut buf, d.extent.offset()).unwrap();
    assert_eq!(buf, stamp(d.tag, false), "head of {d:?}");
    file.read_at(&mut buf, d.extent.offset() + d.extent.len() - STAMP as u64)
        .unwrap();
    assert_eq!(buf, stamp(d.tag, true), "tail of {d:?}");
}

fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A manifest record: the version and the full live data set, checksummed. Snapshots hold
/// one; the delta log holds a sequence of `[u32 len][record]`.
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

/// Reads the manifest a root names: the snapshot, then every delta in the live log; the
/// last record wins. Returns the version and live data set.
fn read_manifest(file: &FileRef, root: &Root) -> (u64, Vec<Data>) {
    let Some(snapshot) = root.snapshot else {
        assert!(root.log.is_none());
        return (0, Vec::new());
    };
    let mut buf = vec![0u8; root.snapshot_len as usize];
    file.read_at(&mut buf, snapshot.offset()).unwrap();
    let mut latest = decode_manifest(&buf);
    if let Some(log) = root.log {
        let mut buf = vec![0u8; root.log_len as usize];
        file.read_at(&mut buf, log.offset()).unwrap();
        let mut at = 0;
        while at < buf.len() {
            let len = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
            let (v, live) = decode_manifest(&buf[at + 4..at + 4 + len]);
            assert!(v > latest.0, "deltas move forward");
            latest = (v, live);
            at += 4 + len;
        }
        assert_eq!(at, buf.len(), "log_len ends on a delta boundary");
    } else {
        assert_eq!(root.log_len, 0);
    }
    latest
}

/// What the workload knows about commits when it stops.
#[derive(Debug, Clone, Default)]
struct Progress {
    created: bool,
    /// The last root whose commit returned `Ok`.
    committed: Root,
    /// A root whose commit was started but did not return `Ok`.
    attempted: Option<Root>,
    /// Coverage counters: deltas appended, snapshot rollovers, extents relocated.
    appends: u64,
    rollovers: u64,
    relocations: u64,
}

/// The workload state carried across steps.
struct State {
    pager: Pager,
    live: Vec<Data>,
    root: Root,
}

fn crashed(e: &Error) -> bool {
    matches!(e, Error::Io(io) if io.kind == ErrorKind::Crashed)
}

/// One engine-like step: allocate, write, relocate, record the manifest, publish, retire,
/// reclaim. Updates `progress`.
fn step(st: &mut State, rng: &mut Rng, progress: &mut Progress) -> pigeonhole_pager::Result<()> {
    let pager = &st.pager;
    let version = st.root.manifest_version + 1;
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
    // Online shrink: move published data named by the plan toward the start of the file.
    if version.is_multiple_of(3) {
        for e in pager.shrink_plan() {
            let Some(d) = next.iter_mut().find(|d| d.extent == e) else {
                continue; // the manifest or log; this workload moves data only
            };
            match pager.relocate(e) {
                Ok(moved) => {
                    d.extent = moved;
                    dropped.push(e);
                    progress.relocations += 1;
                }
                Err(Error::NoSpace) => {}
                Err(err) => return Err(err),
            }
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
    // Record the manifest: a delta appended past the live end of the log, or a rollover.
    let record = encode_manifest(version, &next);
    let mut root = Root {
        manifest_version: version,
        ..st.root
    };
    match st.root.log {
        // Roll over when the log is full, and every fifth version regardless.
        Some(log)
            if !version.is_multiple_of(5)
                && st.root.log_len + 4 + record.len() as u32 <= LOG_BUDGET =>
        {
            let mut delta = (record.len() as u32).to_le_bytes().to_vec();
            delta.extend_from_slice(&record);
            pager.write(log, u64::from(st.root.log_len), &delta)?;
            root.log_len += delta.len() as u32;
            progress.appends += 1;
        }
        _ => {
            let snapshot = pager.allocate(record.len() as u64)?;
            pager.write(snapshot, 0, &record)?;
            let log = pager.allocate(LOG_EXTENT)?;
            root.snapshot = Some(snapshot);
            root.snapshot_len = record.len() as u32;
            root.log = Some(log);
            root.log_len = 0;
            dropped.extend(st.root.snapshot);
            dropped.extend(st.root.log);
            progress.rollovers += 1;
        }
    }
    progress.attempted = Some(root);
    if version.is_multiple_of(2) {
        pager.submit_commit_root(root).wait()?;
    } else {
        pager.commit_root(root)?;
    }
    progress.committed = root;
    progress.attempted = None;
    // No view outlives the commit in this workload, so everything dropped is reclaimable.
    for e in dropped {
        pager.retire(e, version);
    }
    pager.reclaim(version);
    if rng.below(4) == 0 {
        pager.truncate_tail()?;
    }
    st.live = next;
    st.root = root;
    Ok(())
}

/// Runs the workload until it finishes or the first error (the injected crash).
fn workload(vfs: &VfsRef, seed: u64, progress: &mut Progress) -> pigeonhole_pager::Result<()> {
    let mut rng = Rng(seed);
    let pager = Pager::create(vfs, Path::new(PATH))?;
    progress.created = true;
    let mut st = State {
        pager,
        live: Vec::new(),
        root: Root::default(),
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
    let (version, live) = read_manifest(opened.file(), &root);
    assert_eq!(version, root.manifest_version);
    for d in &live {
        check_data(opened.file(), *d);
    }
    let extents: Vec<Extent> = (live.iter().map(|d| d.extent))
        .chain(root.snapshot)
        .chain(root.log)
        .collect();
    let mut st = State {
        pager: opened.finish(extents.iter().copied()).unwrap(),
        live,
        root,
    };

    // Keep going: new allocations never touch recovered data, and a new root commits.
    let mut rng = Rng(seed ^ 0xC0FFEE);
    let mut after = Progress {
        created: true,
        committed: root,
        ..Progress::default()
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
    let mut relocations = 0;
    for &seed in seeds {
        // A clean run sizes the sweep.
        let clean = SimVfs::new(seed);
        let clean_ref: VfsRef = clean.clone();
        let mut coverage = Progress::default();
        workload(&clean_ref, seed, &mut coverage).unwrap();
        println!(
            "{name} seed {seed}: {} appends, {} rollovers, {} relocations",
            coverage.appends, coverage.rollovers, coverage.relocations
        );
        assert!(
            coverage.appends > 0 && coverage.rollovers >= 2,
            "{coverage:?}"
        );
        relocations += coverage.relocations;
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
    assert!(relocations > 0, "{name}: no seed relocated anything");
}

/// Two fresh seeds plus a fixed one whose workload is known to relocate (relocation needs
/// a hole below a published extent, which a random seed does not always produce).
fn seeds() -> Vec<u64> {
    const RELOCATES: u64 = 3_766_993_824_058_584_241;
    let s = common::seed();
    vec![s, s.wrapping_add(1), RELOCATES]
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
            live: Vec::new(),
            root: Root::default(),
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
