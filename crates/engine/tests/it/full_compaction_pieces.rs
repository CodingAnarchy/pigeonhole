//! A full compaction's outputs fit their pieces as the planner measures them (#1313, #1324).
//!
//! `plan_full` cuts a lone last-level SST larger than one output piece, judging it by its whole
//! length (index, filters and footer included). The sink used to cut a piece by its data alone,
//! so an output could end just over its piece. The next full compaction asked for it to be cut
//! again and got the same SST back: `compact()` rewrote it about once a millisecond for as long
//! as it ran (#1313, 47 GB in 91 s on the box, until its once-per-request guard). The sink now
//! cuts by the whole length too (#1324), so no such SST is written, and a second full compaction
//! leaves the outputs alone. Small blocks make the index large enough to open that window, as
//! the text index's many small counters did.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

/// Runs a full compaction on another thread and waits up to `limit` for it: before the fix,
/// it never returned. On a timeout, the close stops it (#991), so it doesn't go on writing
/// beside the binary's other tests.
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
        Err(_) => {
            let _ = engine.close();
            false
        }
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
fn a_full_compactions_outputs_fit_their_pieces_and_stay_put() {
    let dir = std::env::temp_dir().join(format!(
        "phdb-full-compaction-pieces-{}",
        std::process::id()
    ));
    // With 256-byte blocks, the data-length cut left one SST of up to about 77 KB, above one
    // piece (64 KiB): 2,000 rows left a 74 KB SST that every full compaction rewrote whole.
    for rows in (1500..2100u64).step_by(100) {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let Some((engine, ssts)) = build(&dir, rows) else {
            panic!("{rows} rows: the first full compaction never ended (#1313)");
        };
        // Every output fits one piece by its whole length: at these sizes, 64 KiB.
        assert!(
            ssts.iter()
                .all(|&(level, len)| level > 0 && len <= 64 << 10),
            "{rows} rows: {ssts:?}"
        );
        // So a second full compaction plans nothing: the same SSTs, by id.
        let ids = |e: &Engine| {
            let mut v: Vec<u64> = e.sst_levels().iter().map(|s| s.2).collect();
            v.sort_unstable();
            v
        };
        let before = ids(&engine);
        assert!(
            compact_within(&engine, Duration::from_secs(30)),
            "{rows} rows: a second full compaction never ended (#1313)"
        );
        assert_eq!(ids(&engine), before, "{rows} rows: rewritten again (#1324)");
        engine.close().unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
