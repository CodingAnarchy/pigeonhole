//! D324: `crash.rs`'s durability matrix on direct streams (`WalOptions::direct`). The
//! simulator takes direct I/O and checks its alignment, and with deferred I/O a direct write
//! completes late: appends, syncs and rollovers queue behind it. `common::opts` turns direct
//! on for this target.

#[path = "crash.rs"]
mod suite;
