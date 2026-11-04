//! SCRATCH #1477 (not for merge): memtable freezes by reason and table/family.
use std::collections::HashMap;
use std::sync::Mutex;

static M: Mutex<Option<HashMap<(&'static str, String), (u64, u64, u64)>>> = Mutex::new(None);

pub(crate) fn record(reason: &'static str, name: String, bytes: u64) {
    let mut g = M.lock().unwrap();
    let e = g
        .get_or_insert_with(HashMap::new)
        .entry((reason, name))
        .or_insert((0, 0, u64::MAX));
    e.0 += 1;
    e.1 += bytes;
    e.2 = e.2.min(bytes);
}

/// Prints and clears the table: count, mean and smallest size per (reason, table/family).
pub fn scratch_freeze_report(label: &str) {
    let mut g = M.lock().unwrap();
    let Some(m) = g.take() else {
        eprintln!("SCRATCH-FREEZE {label}: none");
        return;
    };
    let mut v: Vec<_> = m.into_iter().collect();
    v.sort_by_key(|(_, (n, _, _))| std::cmp::Reverse(*n));
    let total: u64 = v.iter().map(|(_, (n, _, _))| n).sum();
    eprintln!("SCRATCH-FREEZE {label}: {total} freezes");
    for ((reason, name), (n, bytes, min)) in v {
        eprintln!(
            "SCRATCH-FREEZE {label} | {reason:6} | {name:40} | n {n:6} | mean {:9.1} KiB | min {:9.1} KiB",
            bytes as f64 / n as f64 / 1024.0,
            min as f64 / 1024.0
        );
    }
}
