//! D285: the cross-shard protocol rides each shard's priority lane, and a shard handles what
//! arrived on it during a group at a safe point right after the group (plus at most one extra
//! group for what that queued), not at the next turn. Engine-owned shards under concurrent
//! cross-shard commits: the safe point runs, and every commit is applied whole. With the safe
//! point off (`safe_point_messages = 0`) it never runs, and the same holds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, TableInfo, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

const SHARDS: usize = 4;
const THREADS: usize = 8;

/// Tables owned by shards 0 to `SHARDS - 1`, one each.
fn tables(engine: &Engine) -> Vec<Arc<TableInfo>> {
    let mut owned: Vec<Option<Arc<TableInfo>>> = vec![None; SHARDS];
    for i in 0.. {
        let t = engine
            .create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let snap = engine.snapshot().unwrap();
        let (_, shard) = snap.view().tablets().route(t.id, b"row").unwrap();
        let slot = &mut owned[usize::from(shard.0)];
        if slot.is_none() {
            *slot = Some(t);
        }
        if owned.iter().all(Option::is_some) || i > 1_000 {
            break;
        }
    }
    owned.into_iter().map(Option::unwrap).collect()
}

/// Runs cross-shard commits from `THREADS` threads (each writes its own row, `row`, in two
/// shards' tables, with the same value) until the safe point has handled messages or `max`
/// commits per thread ran. Returns the safe point's messages and extra groups, summed.
fn run(safe_point_messages: usize, max: u32) -> (u64, u64) {
    let dir = std::env::temp_dir().join(format!(
        "phdb-safe-points-{}-{safe_point_messages}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = SHARDS;
    o.pin_threads = false;
    o.durability = Durability::Buffered;
    o.safe_point_messages = safe_point_messages;
    let engine = Arc::new(Engine::open(&dir.join("sp.phdb"), o).unwrap());
    let tables = Arc::new(tables(&engine));
    let counts = |e: &Engine| {
        (0..SHARDS).fold((0, 0), |(m, g), s| {
            let (dm, dg) = e.safe_point_counts(s);
            (m + dm, g + dg)
        })
    };
    let done = Arc::new(AtomicBool::new(false));
    let threads: Vec<_> = (0..THREADS)
        .map(|t| {
            let (engine, tables, done) =
                (Arc::clone(&engine), Arc::clone(&tables), Arc::clone(&done));
            std::thread::spawn(move || {
                let row = format!("row{t}");
                let mut last = 0;
                for i in 1..=max {
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    let (a, b) = (t % SHARDS, (t + 1 + (i as usize) % (SHARDS - 1)) % SHARDS);
                    let mut wb = WriteBatch::new();
                    for x in [a, b] {
                        let tb = &tables[x];
                        wb.put(
                            tb.id,
                            tb.families[0].id,
                            row.as_bytes(),
                            b"q",
                            None,
                            ValueRef::Bytes(&i.to_le_bytes()),
                        )
                        .unwrap();
                    }
                    engine.commit(wb, None).unwrap();
                    last = i;
                }
                (row, last)
            })
        })
        .collect();
    // Stop once the safe point has run a while (it needs a message to land mid-group, which
    // concurrent threads make likely within a few thousand commits).
    if safe_point_messages > 0 {
        while !threads.iter().all(|h| h.is_finished()) {
            if counts(&engine).0 >= 100 {
                done.store(true, Ordering::Relaxed);
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    let lasts: Vec<_> = threads.into_iter().map(|h| h.join().unwrap()).collect();
    // Every commit was applied whole: each thread's row holds its last value in every table
    // it last wrote, and no table holds a value newer than the thread's last commit.
    let snap = engine.snapshot().unwrap();
    for (t, (row, last)) in lasts.iter().enumerate() {
        let a = t % SHARDS;
        let b = (t + 1 + (*last as usize) % (SHARDS - 1)) % SHARDS;
        for (x, tb) in tables.iter().enumerate() {
            let got = engine
                .get(&snap, tb.id, tb.families[0].id, row.as_bytes(), b"q")
                .unwrap()
                .map(|c| match c.value() {
                    ValueRef::Bytes(v) => u32::from_le_bytes(v[..4].try_into().unwrap()),
                    other => panic!("unexpected value {other:?}"),
                });
            if x == a || x == b {
                assert_eq!(got, Some(*last), "thread {t}'s row in shard {x}'s table");
            } else {
                assert!(
                    got.is_none_or(|v| v < *last),
                    "thread {t}'s row in shard {x}"
                );
            }
        }
    }
    let c = counts(&engine);
    drop(snap);
    Arc::try_unwrap(engine).ok().unwrap().close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    c
}

#[test]
fn the_safe_point_handles_protocol_messages_that_arrive_during_a_group() {
    let (messages, groups) = run(64, 20_000);
    assert!(messages > 0, "no message handled at a safe point");
    // Each extra group follows a pass that handled at least one message.
    assert!(
        groups <= messages,
        "{groups} extra groups for {messages} messages"
    );
}

#[test]
fn with_the_safe_point_off_it_never_runs() {
    assert_eq!(run(0, 2_000), (0, 0));
}
