//! Ruby bindings for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) (#697, ICR
//! 0111), over magnus and rb-sys, published to RubyGems as `pigeonhole` and required as
//! `require "pigeonhole"`.
//!
//! The native extension is `pigeonhole/pigeonhole_native`; `lib/pigeonhole.rb` requires it
//! and adds the pure-Ruby parts (the keyword-taking public methods, the block forms, the
//! scan `Enumerator`). The native methods take positional arguments (`nil` for "not
//! given"); those the Ruby layer wraps are private and named `native_*`. Every engine call
//! runs with the GVL released (`gvl::without_gvl`), its arguments converted before and its
//! results after.
//!
//! **Unsafe.** The crate denies `unsafe_code`. The exceptions are the items magnus's macros
//! expand (`#[magnus::init]` here, `#[magnus::wrap]` in `classes.rs`), each marked, and
//! `gvl.rs`, the one hand-written module (the owner's O3 on #697).
#![deny(unsafe_code)]

use magnus::{Error, Module, Object, RClass, RModule, Ruby, function, method, prelude::*};

mod classes;
mod convert;
mod db;
mod error;
mod fork;
mod gvl;
mod read;
mod table;

use classes::{
    Cell, Condition, Database, FamilyInfo, Mutation, Reader, Row, ScanCursor, ScanSpec, Snapshot,
    Table, TableBuilder, Transaction, WriteBatch,
};

/// Defines `Pigeonhole::<name>` with no allocator: instances come only from the library.
fn class(ruby: &Ruby, m: RModule, name: &str) -> Result<RClass, Error> {
    let c = m.define_class(name, ruby.class_object())?;
    c.undef_default_alloc_func();
    Ok(c)
}

/// Defines the write methods `WriteBatch` and `Transaction` share on `$c` (private; Ruby
/// wraps them).
macro_rules! writes {
    ($c:expr, $t:ty) => {{
        let c = $c;
        c.define_private_method("native_put", method!(db::w_put::<$t>, 6))?;
        c.define_private_method("native_put_u64", method!(db::w_put_u64::<$t>, 6))?;
        c.define_private_method("native_incr", method!(db::w_incr::<$t>, 6))?;
        c.define_private_method("native_aggregate", method!(db::w_aggregate::<$t>, 6))?;
        c.define_private_method("native_delete", method!(db::w_delete::<$t>, 5))?;
    }};
}

/// The extension's entry point, in a private module: the `Init_pigeonhole_native` function
/// `#[magnus::init]` generates is exported by its symbol name, and carries no docs.
mod entry {
    #[allow(unsafe_code)] // `#[magnus::init]` expands to the `extern "C"` `Init_` function.
    #[magnus::init(name = "pigeonhole_native")]
    fn init(ruby: &magnus::Ruby) -> Result<(), magnus::Error> {
        super::init(ruby)
    }
}

/// Defines the `Pigeonhole` module, its classes and their native methods.
fn init(ruby: &Ruby) -> Result<(), Error> {
    let m = ruby.define_module("Pigeonhole")?;
    m.const_set("NATIVE_VERSION", env!("CARGO_PKG_VERSION"))?;
    let native = m.define_module("Native")?;
    error::register(ruby, m, native)?;

    m.singleton_class()?
        .define_private_method("native_open", function!(db::open, 9))?;
    m.singleton_class()?
        .define_private_method("native_open_reader", function!(db::open_reader, 4))?;
    m.singleton_class()?
        .define_private_method("native_retry_check", function!(db::retry_check, 2))?;
    m.singleton_class()?
        .define_private_method("native_retry_pause", function!(db::retry_pause, 5))?;

    let c = class(ruby, m, "Database")?;
    c.define_method("table", method!(Database::table, 1))?;
    c.define_method("tables", method!(Database::tables, 0))?;
    c.define_method("drop_table", method!(Database::drop_table, 1))?;
    c.define_method("flush", method!(Database::flush, 0))?;
    c.define_method("compact", method!(Database::compact, 0))?;
    c.define_method("close", method!(Database::close, 0))?;
    c.define_method("closed?", method!(Database::is_closed, 0))?;
    c.define_private_method("native_snapshot", method!(Database::snapshot, 1))?;
    c.define_private_method("native_write_batch", method!(Database::write_batch, 0))?;
    c.define_private_method("native_transaction", method!(Database::transaction, 0))?;

    let c = class(ruby, m, "Reader")?;
    c.define_method("table", method!(Reader::table, 1))?;
    c.define_method("tables", method!(Reader::tables, 0))?;
    c.define_method("close", method!(Reader::close, 0))?;
    c.define_method("closed?", method!(Reader::is_closed, 0))?;
    c.define_private_method("native_snapshot", method!(Reader::snapshot, 1))?;

    let c = class(ruby, m, "TableBuilder")?;
    c.define_private_method("native_family", method!(TableBuilder::family, 2))?;
    c.define_method("create", method!(TableBuilder::create, 0))?;
    c.define_method(
        "create_if_missing",
        method!(TableBuilder::create_if_missing, 0),
    )?;
    c.define_method("open", method!(TableBuilder::open, 0))?;

    let c = class(ruby, m, "Table")?;
    c.define_method("name", method!(Table::name, 0))?;
    c.define_method("families", method!(Table::families, 0))?;
    c.define_method("family_info", method!(Table::family_info, 1))?;
    c.define_method("mutate", method!(Table::mutate, 1))?;
    c.define_method("inspect", method!(Table::inspect, 0))?;
    c.define_private_method("native_get", method!(Table::get, 4))?;
    c.define_private_method("native_row", method!(Table::row, 6))?;
    c.define_private_method("native_scan_spec", method!(Table::scan_spec, 9))?;
    c.define_private_method("native_put", method!(Table::put, 6))?;
    c.define_private_method("native_incr", method!(Table::incr, 6))?;
    c.define_private_method("native_delete", method!(Table::delete, 5))?;

    let c = class(ruby, m, "Mutation")?;
    c.define_private_method("native_put", method!(Mutation::put, 4))?;
    c.define_private_method("native_put_u64", method!(Mutation::put_u64, 4))?;
    c.define_private_method("native_incr", method!(Mutation::incr, 4))?;
    c.define_private_method("native_aggregate", method!(Mutation::aggregate, 4))?;
    c.define_private_method("native_delete", method!(Mutation::delete, 3))?;
    c.define_private_method("native_durability", method!(Mutation::set_durability, 1))?;
    c.define_method("commit", method!(Mutation::commit, 0))?;
    c.define_method("commit_if", method!(Mutation::commit_if, 1))?;

    let c = class(ruby, m, "WriteBatch")?;
    writes!(c, WriteBatch);
    c.define_method("size", method!(WriteBatch::size, 0))?;
    c.define_method("length", method!(WriteBatch::size, 0))?;
    c.define_private_method("native_commit", method!(WriteBatch::commit, 1))?;
    c.define_private_method("native_discard", method!(WriteBatch::discard, 0))?;
    c.define_private_method("native_open?", method!(WriteBatch::is_open, 0))?;

    let c = class(ruby, m, "Transaction")?;
    writes!(c, Transaction);
    c.define_method("get", method!(Transaction::get, 4))?;
    c.define_method("get_row", method!(Transaction::get_row, 3))?;
    c.define_method("rollback", method!(Transaction::rollback, 0))?;
    c.define_private_method("native_commit", method!(Transaction::commit, 1))?;
    c.define_private_method("native_discard", method!(Transaction::discard, 0))?;
    c.define_private_method("native_open?", method!(Transaction::is_open, 0))?;

    let c = class(ruby, m, "Snapshot")?;
    c.define_method("seqno", method!(Snapshot::seqno, 0))?;
    c.define_method("release", method!(Snapshot::release, 0))?;

    let c = class(ruby, m, "Cell")?;
    c.define_method("value", method!(Cell::value, 0))?;
    c.define_method("ts", method!(Cell::ts, 0))?;
    c.define_method("typed", method!(Cell::typed, 0))?;
    c.define_method("as_int", method!(Cell::as_int, 0))?;
    c.define_method("as_u64", method!(Cell::as_u64, 0))?;
    c.define_method("as_float", method!(Cell::as_float, 0))?;
    c.define_method("hll_estimate", method!(Cell::hll_estimate, 0))?;
    c.define_method("sum_f64", method!(Cell::sum_f64, 0))?;
    c.define_method("mean_var", method!(Cell::mean_var, 0))?;
    c.define_method("histogram_counts", method!(Cell::histogram_counts, 0))?;
    c.define_method("quantile", method!(Cell::quantile, 1))?;
    c.define_method("bloom_contains", method!(Cell::bloom_contains, 1))?;
    c.define_method("==", method!(Cell::eq, 1))?;
    c.define_method("inspect", method!(Cell::inspect, 0))?;

    let c = class(ruby, m, "Row")?;
    c.define_method("key", method!(Row::key, 0))?;
    c.define_method("get", method!(Row::get, 2))?;
    c.define_method("cells", method!(Row::cells, 0))?;
    c.define_method("size", method!(Row::size, 0))?;
    c.define_method("length", method!(Row::size, 0))?;
    c.define_method("inspect", method!(Row::inspect, 0))?;

    let c = class(ruby, m, "Family")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_new", function!(table::family_new, 7))?;
    s.define_private_method("native_counter", function!(table::family_counter, 2))?;
    s.define_private_method("native_aggregate", function!(table::family_aggregate, 8))?;

    let c = class(ruby, m, "FamilyInfo")?;
    c.define_method("name", method!(FamilyInfo::name, 0))?;
    c.define_method("counter?", method!(FamilyInfo::counter, 0))?;
    c.define_method("aggregation", method!(FamilyInfo::aggregation, 0))?;
    c.define_method("max_versions", method!(FamilyInfo::max_versions, 0))?;
    c.define_method("ttl", method!(FamilyInfo::ttl, 0))?;
    c.define_method("bloom_bits", method!(FamilyInfo::bloom_bits, 0))?;
    c.define_method("blob_threshold", method!(FamilyInfo::blob_threshold, 0))?;
    c.define_method("block_size", method!(FamilyInfo::block_size, 0))?;
    c.define_method("compression", method!(FamilyInfo::compression, 0))?;
    c.define_method("zstd_level", method!(FamilyInfo::zstd_level, 0))?;
    c.define_method("merge_operator", method!(FamilyInfo::merge_operator, 0))?;
    c.define_method("cache_priority", method!(FamilyInfo::cache_priority, 0))?;
    c.define_method("compaction", method!(FamilyInfo::compaction, 0))?;
    c.define_method("family", method!(FamilyInfo::family, 0))?;
    c.define_method("inspect", method!(FamilyInfo::inspect, 0))?;

    let c = class(ruby, m, "Condition")?;
    c.define_singleton_method("absent", function!(Condition::absent, 2))?;
    c.define_singleton_method("exists", function!(Condition::exists, 2))?;
    c.define_singleton_method("equals", function!(Condition::equals, 3))?;

    let c = class(ruby, native, "ScanSpec")?;
    c.define_method("open", method!(ScanSpec::open, 1))?;
    let c = class(ruby, native, "ScanCursor")?;
    c.define_method("next_chunk", method!(ScanCursor::next_chunk, 0))?;
    c.define_method("close", method!(ScanCursor::close, 0))?;
    Ok(())
}
