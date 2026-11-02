//! #1313: a full compaction ends when a slot's lone last-level SST is just over one output
//! piece. The planner judged such an SST by its whole length (index, filters and footer
//! included) and asked for it to be cut, while the sink, cutting by data length, wrote the same
//! SST back, so `compact()` rewrote it about once a millisecond for as long as it ran (47 GB
//! in 91 s on the box). Many small cells make the index and filters large enough to open that
//! window, as the text index's counters did.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

/// Runs a full compaction on another thread and waits up to `limit` for it: before the fix,
/// it never returned.
fn compact_within(engine: &Arc<Engine>, limit: Duration) -> bool {
    let (tx, rx) = mpsc::channel();
    let e = Arc::clone(engine);
    std::thread::spawn(move || {
        let _ = tx.send(e.compact(None));
    });
    match rx.recv_timeout(limit) {
        Ok(r) => {
            r.expect("compact");
            true
        }
        Err(_) => false,
    }
}

/// Each SST's `(level, length)`.
type Ssts = Vec<(u8, u64)>;

/// One shard, one table of 256-byte blocks, `rows` rows of one 8-byte cell each, flushed and
/// fully compacted.
/// Returns the engine and its SSTs' `(level, length)`, or `None` if a compaction did not end.
fn build(dir: &std::path::Path, rows: u64) -> Option<(Arc<Engine>, Ssts)> {
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(2));
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    let engine = Engine::open(&dir.join("data.phdb"), o).expect("open");
    let t = engine
        .create_table(
            "t",
            &[(
                "f".into(),
                // Small blocks make the index large enough to lift the whole length above the
                // data by more than the sink's slack (on the box: about 2.3 KB).
                FamilyOptions::default().block_size(256),
            )],
        )
        .expect("table");
    let f = t.families[0].id;
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for chunk in 0..rows.div_ceil(256) {
        let mut b = WriteBatch::new();
        for i in chunk * 256..((chunk + 1) * 256).min(rows) {
            // Incompressible values, so the SST's length follows the row count.
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            b.put(
                t.id,
                f,
                &i.to_be_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&x.to_le_bytes()),
            )
            .unwrap();
        }
        engine
            .commit(b, Some(Durability::Buffered))
            .expect("commit");
    }
    engine.flush().expect("flush");
    if !compact_within(&engine, Duration::from_secs(30)) {
        return None;
    }
    let ssts = engine.sst_lens().iter().map(|s| (s.0, s.1)).collect();
    Some((engine, ssts))
}

#[test]
fn a_full_compaction_ends_on_a_lone_sst_just_over_one_piece() {
    let dir = std::env::temp_dir().join(format!("pigeonhole-1313-{}", std::process::id()));
    // Step the row count until the full compaction leaves one last-level SST between one
    // piece (64 KiB) and two: the shape that looped. With 256-byte blocks the sink cuts only
    // at about 77 KB, so the window is wide (2,000 rows leave a 74 KB SST); each step adds
    // about 900 bytes.
    let mut found = None;
    for rows in (1500..2100u64).step_by(25) {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let Some((engine, ssts)) = build(&dir, rows) else {
            panic!("{rows} rows: the first full compaction never ended (#1313)");
        };
        if let [(level, len)] = ssts[..]
            && level > 0
            && len > 64 << 10
            && len < 128 << 10
        {
            found = Some((rows, len, engine));
            break;
        }
        engine.close().unwrap();
    }
    let (rows, len, engine) = found.expect("no row count left a lone SST just over one piece");
    // Another full compaction of the same slot ends too, and leaves the SST as it is.
    assert!(
        compact_within(&engine, Duration::from_secs(30)),
        "{rows} rows ({len}-byte SST): a full compaction never ended (#1313)"
    );
    let after: Vec<(u8, u64)> = engine.sst_lens().iter().map(|s| (s.0, s.1)).collect();
    assert_eq!(after.len(), 1, "{rows} rows: {after:?}");
    engine.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
