//! SCRATCH #1497 (not for merge): the shard's state during room waits over a second.
use std::collections::HashMap;
use std::sync::Mutex;

static M: Mutex<Option<HashMap<String, u64>>> = Mutex::new(None);

pub(crate) fn record(state: String) {
    let mut g = M.lock().unwrap();
    *g.get_or_insert_with(HashMap::new).entry(state).or_default() += 1;
}

/// Prints and clears the table, most frequent first.
pub fn scratch_room_report(label: &str) {
    let m = M.lock().unwrap().take();
    let Some(m) = m else {
        eprintln!("SCRATCH-ROOM {label}: no wait over 1 s");
        return;
    };
    let mut v: Vec<_> = m.into_iter().collect();
    v.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    for (state, n) in v.into_iter().take(25) {
        eprintln!("SCRATCH-ROOM {label} | n {n:6} | {state}");
    }
}
