//! Fuzzes the wal decoders; any panic is a bug. The harness is shared with the proptest
//! arbitrary-input tests.
#![no_main]

#[path = "../../tests/common/harness.rs"]
mod harness;

libfuzzer_sys::fuzz_target!(|data: &[u8]| harness::wal(data));
