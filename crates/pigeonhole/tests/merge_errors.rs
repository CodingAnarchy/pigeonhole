//! A user merge operator that fails (#695's coverage report): a read that meets the failure
//! returns `MergeFailed` naming the operator, and flush and compaction, whose folds fail the
//! same way, keep the operands apart unchanged, so nothing is lost: once the operator
//! accepts them again the read returns the full fold, every operand in order.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pigeonhole::{ErrorCode, Family, MergeError, MergeOperator, Options, Pigeonhole};
use pigeonhole_io::sim::SimVfs;

/// Fails while `strict` is set and an operand is `bad`.
static STRICT: AtomicBool = AtomicBool::new(true);

/// Concatenates byte values, oldest first (as the trait's example), refusing `bad` while
/// [`STRICT`] is set.
#[derive(Debug)]
struct PickyAppend;

const NAME: &str = "test.picky_append";

fn check(operand: &[u8]) -> Result<(), MergeError> {
    if STRICT.load(Ordering::Relaxed) && &operand[1..] == b"bad" {
        return Err(MergeError::new(NAME, "a bad operand"));
    }
    Ok(())
}

impl MergeOperator for PickyAppend {
    fn name(&self) -> &str {
        NAME
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        check(acc)?;
        check(older)?;
        let newer = acc.split_off(1);
        acc.extend_from_slice(&older[1..]);
        acc.extend_from_slice(&newer);
        Ok(())
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        check(acc)?;
        if let Some(base) = base {
            let newer = acc.split_off(1);
            acc.extend_from_slice(&base[1..]);
            acc.extend_from_slice(&newer);
        }
        Ok(())
    }
}

#[test]
fn a_failing_merge_operator_fails_the_read_and_flush_and_compaction_lose_nothing() {
    let vfs = SimVfs::new(11);
    let db = Pigeonhole::open(
        "/db/merge-errors.phdb",
        Options::default()
            .vfs(Arc::clone(&vfs) as _)
            .shards(1)
            .merge_operator(Arc::new(PickyAppend)),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("l", Family::default().merge_operator(NAME))
        .create_if_missing()
        .unwrap();
    STRICT.store(true, Ordering::Relaxed);
    t.mutate(b"r").put("l", b"q", b"a").commit().unwrap();
    // Operands through both write paths: `RowMutation::merge` and `WriteBatch::merge`.
    t.mutate(b"r").merge("l", b"q", b"b").commit().unwrap();
    let mut wb = db.write_batch();
    wb.merge(&t, b"r", "l", b"q", b"bad")
        .merge(&t, b"s", "l", b"q", b"x");
    wb.commit().unwrap();
    t.mutate(b"r").merge("l", b"q", b"c").commit().unwrap();
    // A row of operands only (no base): compaction's own combining (D186) meets the bad one.
    for operand in [&b"x"[..], b"y", b"bad", b"z", b"w"] {
        t.mutate(b"u").merge("l", b"q", operand).commit().unwrap();
    }

    // The read meets the failure: `MergeFailed`, naming the operator.
    let err = t.get(b"r", "l", b"q").unwrap_err();
    assert_eq!(err.code(), ErrorCode::MergeFailed, "{err}");
    assert!(err.to_string().contains(NAME), "{err}");
    // A row the bad operand isn't in reads as usual.
    assert_eq!(t.get(b"s", "l", b"q").unwrap().unwrap().value(), b"x");

    // Flush and compaction fold what they can and keep the rest apart: their failed folds
    // lose nothing.
    db.flush().unwrap();
    db.compact().unwrap();
    let err = t.get(b"r", "l", b"q").unwrap_err();
    assert_eq!(
        err.code(),
        ErrorCode::MergeFailed,
        "after compaction: {err}"
    );

    assert_eq!(
        t.get(b"u", "l", b"q").unwrap_err().code(),
        ErrorCode::MergeFailed
    );

    // Accepted again, every operand is still there, in order.
    STRICT.store(false, Ordering::Relaxed);
    assert_eq!(t.get(b"u", "l", b"q").unwrap().unwrap().value(), b"xybadzw");
    assert_eq!(t.get(b"r", "l", b"q").unwrap().unwrap().value(), b"abbadc");
    // And a compaction now folds them for good.
    db.compact().unwrap();
    assert_eq!(t.get(b"r", "l", b"q").unwrap().unwrap().value(), b"abbadc");
    assert_eq!(t.get(b"u", "l", b"q").unwrap().unwrap().value(), b"xybadzw");
    drop(t);
    db.close().unwrap();
}
