//! The allocator against a simple model: random sequences of allocate, abandon, publish
//! (commit a root that adds pending extents and drops live ones), pin and unpin views,
//! reclaim, online shrink, truncate and reopen.
//!
//! Checked after every step:
//! - no extent is ever double-allocated: a new extent overlaps nothing pending, live,
//!   retired, or reachable from any pinned view;
//! - a freed extent is unreachable from every live view: reclaim frees exactly the retired
//!   extents no pinned view can reach, so a reused extent was never in a live view's root;
//! - stats match the model, and the file always covers every extent still in use.

mod common;

use std::path::Path;

use common::{overlaps, span};
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_pager::{Error, Extent, Pager, Root};
use proptest::prelude::*;

const PATH: &str = "/db/data.phdb";

#[derive(Debug, Clone)]
enum Op {
    /// Allocate an extent of at least this many bytes.
    Allocate(u64),
    /// Abandon the pending extent at this index (mod len).
    Abandon(usize),
    /// Trim the pending extent at this index (mod len) to this many bytes (mod its length).
    Trim(usize, u64),
    /// Commit a new root adding every pending extent and dropping the live extents whose
    /// index bit is set in the mask.
    Publish(u64),
    /// Pin a view of the current root.
    Pin,
    /// Release the pinned view at this index (mod len).
    Unpin(usize),
    /// Reclaim below the oldest pinned view.
    Reclaim,
    /// Relocate every extent the shrink plan names, publish, retire the old ones.
    Shrink,
    /// Truncate the file after its last allocated extent.
    Truncate,
    /// Close and reopen: views and pending output are gone, live extents survive.
    Reopen,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (0u64..=common::miri_scaled(300 << 10, 64 << 10)).prop_map(Op::Allocate),
        1 => (0u64..=common::miri_scaled(4 << 20, 256 << 10)).prop_map(Op::Allocate),
        2 => any::<usize>().prop_map(Op::Abandon),
        2 => (any::<usize>(), any::<u64>()).prop_map(|(i, b)| Op::Trim(i, b)),
        4 => any::<u64>().prop_map(Op::Publish),
        2 => Just(Op::Pin),
        2 => any::<usize>().prop_map(Op::Unpin),
        3 => Just(Op::Reclaim),
        1 => Just(Op::Shrink),
        1 => Just(Op::Truncate),
        1 => Just(Op::Reopen),
    ]
}

#[derive(Default)]
struct Model {
    version: u64,
    pending: Vec<Extent>,
    live: Vec<Extent>,
    retired: Vec<(Extent, u64)>,
    /// Pinned views: the manifest version and every extent its root reaches.
    views: Vec<(u64, Vec<Extent>)>,
}

impl Model {
    fn in_use(&self) -> impl Iterator<Item = Extent> + '_ {
        self.pending
            .iter()
            .chain(&self.live)
            .copied()
            .chain(self.retired.iter().map(|r| r.0))
    }

    fn oldest_live(&self) -> u64 {
        self.views.iter().map(|v| v.0).min().unwrap_or(self.version)
    }

    fn bytes(extents: impl Iterator<Item = Extent>) -> u64 {
        extents.map(|e| e.len()).sum()
    }
}

struct Harness {
    vfs: VfsRef,
    pager: Pager,
    m: Model,
}

impl Harness {
    fn new(seed: u64) -> Self {
        let vfs: VfsRef = SimVfs::new(seed);
        let pager = Pager::create(&vfs, Path::new(PATH)).unwrap();
        Self {
            vfs,
            pager,
            m: Model::default(),
        }
    }

    fn commit(&mut self, added: Vec<Extent>, dropped: Vec<Extent>) {
        let version = self.m.version + 1;
        self.pager
            .commit_root(Root {
                manifest_version: version,
                ..Root::default()
            })
            .unwrap();
        self.m.version = version;
        self.m.live.retain(|e| !dropped.contains(e));
        self.m.live.extend(added);
        for e in dropped {
            self.pager.retire(e, version);
            self.m.retired.push((e, version));
        }
    }

    fn check_new(&self, e: Extent, bytes: u64) {
        assert!(
            e.len() >= bytes.max(64 << 10),
            "{e:?} too small for {bytes}"
        );
        assert!(
            e.len() < bytes.max(64 << 10) * 2,
            "{e:?} not the smallest for {bytes}"
        );
        assert!(
            e.page >= 16 && e.page.is_multiple_of(e.len() / 4096),
            "{e:?} misaligned"
        );
        for x in self.m.in_use() {
            assert!(
                !overlaps(e, x),
                "double allocation: {e:?} overlaps in-use {x:?}"
            );
        }
        for (v, reach) in &self.m.views {
            for &x in reach {
                assert!(
                    !overlaps(e, x),
                    "{e:?} overlaps {x:?}, reachable from view {v}"
                );
            }
        }
    }

    fn apply(&mut self, op: &Op) {
        match *op {
            Op::Allocate(bytes) => match self.pager.allocate(bytes) {
                Ok(e) => {
                    self.check_new(e, bytes);
                    self.m.pending.push(e);
                }
                Err(Error::TooLarge) => assert!(bytes > 64 << 20),
                Err(e) => panic!("allocate({bytes}): {e}"),
            },
            Op::Abandon(i) => {
                if !self.m.pending.is_empty() {
                    let e = self.m.pending.swap_remove(i % self.m.pending.len());
                    self.pager.abandon(e);
                }
            }
            Op::Trim(i, bytes) => {
                if !self.m.pending.is_empty() {
                    let i = i % self.m.pending.len();
                    let e = self.m.pending[i];
                    let bytes = bytes % (e.len() + 1);
                    let t = self.pager.trim(e, bytes);
                    assert_eq!(t.page, e.page, "trim keeps the first page");
                    assert!(
                        t.len() >= bytes.max(64 << 10),
                        "{t:?} too small for {bytes}"
                    );
                    assert!(
                        t.len() < bytes.max(64 << 10) * 2,
                        "{t:?} not trimmed to {bytes}"
                    );
                    self.m.pending[i] = t;
                }
            }
            Op::Publish(mask) => {
                let dropped: Vec<Extent> = (self.m.live.iter().enumerate())
                    .filter(|(i, _)| mask & (1 << (i % 64)) != 0)
                    .map(|(_, e)| *e)
                    .collect();
                let added = std::mem::take(&mut self.m.pending);
                self.commit(added, dropped);
            }
            Op::Pin => self.m.views.push((self.m.version, self.m.live.clone())),
            Op::Unpin(i) => {
                if !self.m.views.is_empty() {
                    self.m.views.swap_remove(i % self.m.views.len());
                }
            }
            Op::Reclaim => {
                let oldest = self.m.oldest_live();
                let before = self.m.retired.len();
                self.m.retired.retain(|r| r.1 > oldest);
                assert_eq!(self.pager.reclaim(oldest), before - self.m.retired.len());
            }
            Op::Shrink => {
                let plan = self.pager.shrink_plan();
                let mut moves = Vec::new();
                for e in plan {
                    // The pager cannot tell published from pending output; the engine
                    // moves only what its manifest names.
                    if self.m.pending.contains(&e) {
                        continue;
                    }
                    assert!(self.m.live.contains(&e), "plan names {e:?}, not in use");
                    match self.pager.relocate(e) {
                        Ok(n) => {
                            assert!(n.page < e.page && n.size_class == e.size_class);
                            self.check_new(n, e.len());
                            self.m.pending.push(n);
                            moves.push((e, n));
                        }
                        Err(Error::NoSpace) => {}
                        Err(err) => panic!("relocate: {err}"),
                    }
                }
                if !moves.is_empty() {
                    let added: Vec<Extent> = moves.iter().map(|m| m.1).collect();
                    self.m.pending.retain(|e| !added.contains(e));
                    self.commit(added, moves.iter().map(|m| m.0).collect());
                }
            }
            Op::Truncate => {
                let before = self.pager.stats().file_bytes;
                let released = self.pager.truncate_tail().unwrap();
                assert_eq!(self.pager.stats().file_bytes, before - released);
                let end = self.m.in_use().map(|e| span(e).1).max().unwrap_or(64 << 10);
                assert_eq!(
                    self.pager.stats().file_bytes,
                    end,
                    "truncated to the last extent"
                );
            }
            Op::Reopen => {
                let live = self.m.live.clone();
                let version = self.m.version;
                let vfs = self.vfs.clone();
                // Replace the pager first so the old one is dropped.
                let opened = Pager::open(&vfs, Path::new(PATH), true).unwrap();
                assert_eq!(opened.root().manifest_version, version);
                self.pager = opened.finish(live).unwrap();
                self.m.pending.clear();
                self.m.retired.clear();
                self.m.views.clear();
            }
        }
        self.check();
    }

    fn check(&self) {
        let s = self.pager.stats();
        let busy = Model::bytes(self.m.pending.iter().chain(&self.m.live).copied());
        assert_eq!(s.allocated_bytes, busy);
        assert_eq!(
            s.retired_bytes,
            Model::bytes(self.m.retired.iter().map(|r| r.0))
        );
        assert!(s.allocated_bytes + s.retired_bytes + (64 << 10) <= s.file_bytes);
        assert_eq!(self.pager.file().len().unwrap(), s.file_bytes);
        for e in self.m.in_use() {
            assert!(span(e).1 <= s.file_bytes, "{e:?} past the end of the file");
        }
    }
}

proptest! {
    #![proptest_config(common::config(128))]

    #[test]
    fn allocator_matches_model(seed in any::<u64>(), ops in proptest::collection::vec(op(), 1..common::miri_scaled(200, 24) as usize)) {
        let mut h = Harness::new(seed);
        for op in &ops {
            h.apply(op);
        }
    }
}

#[test]
fn reclaim_waits_for_the_oldest_view() {
    let mut h = Harness::new(common::seed());
    h.apply(&Op::Allocate(64 << 10));
    h.apply(&Op::Publish(0)); // version 1 holds the extent
    let old = h.m.live[0];
    h.apply(&Op::Pin); // a snapshot of version 1
    h.apply(&Op::Publish(1)); // version 2 drops it
    h.apply(&Op::Reclaim);
    assert_eq!(
        h.pager.stats().retired_bytes,
        64 << 10,
        "pinned view keeps it"
    );
    for _ in 0..8 {
        let e = h.pager.allocate(64 << 10).unwrap();
        assert!(!overlaps(e, old));
        h.m.pending.push(e);
    }
    h.apply(&Op::Unpin(0));
    h.apply(&Op::Reclaim);
    assert_eq!(h.pager.stats().retired_bytes, 0);
    let reused = h.pager.allocate(64 << 10).unwrap();
    assert_eq!(
        reused, old,
        "lowest free extent is reused once no view can reach it"
    );
}

#[test]
fn online_shrink_relocates_tail_and_truncates() {
    let mut h = Harness::new(common::seed());
    let size = common::miri_scaled(256 << 10, 64 << 10);
    for _ in 0..64 {
        h.apply(&Op::Allocate(size));
    }
    h.apply(&Op::Publish(0));
    // Drop everything except every eighth extent, spread over the file.
    let keep: u64 = (0..64).filter(|i| i % 8 == 7).map(|i| 1u64 << i).sum();
    h.apply(&Op::Publish(!keep));
    h.apply(&Op::Reclaim);
    let before = h.pager.stats().file_bytes;
    h.apply(&Op::Shrink);
    h.apply(&Op::Reclaim);
    h.apply(&Op::Truncate);
    let after = h.pager.stats().file_bytes;
    // Eight extents pack near the start (unit 0 plus alignment waste below 1 MiB).
    assert!(
        after <= size * 8 + (1 << 20),
        "file still {after} bytes (was {before})"
    );
    assert!(h.pager.shrink_plan().is_empty());
    h.apply(&Op::Reopen);
}
