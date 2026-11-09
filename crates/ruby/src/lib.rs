//! Ruby bindings for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) (#697, ICR
//! 0111), over magnus and rb-sys, published to RubyGems as `pigeonhole-db` and required as
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

mod budget;
mod classes;
mod convert;
mod db;
mod error;
mod feed;
#[cfg(unix)]
mod fiber;
mod fork;
mod gvl;
mod index;
mod memory;
mod read;
mod table;
mod vector;
mod waits;
mod walk;

use classes::{
    Cell, Condition, Database, FamilyInfo, Feed, Incremented, KeysPosition, Mutation, Pages,
    Reader, Row, RowPosition, ScanCursor, ScanPosition, ScanSpec, Seen, Snapshot, Table,
    TableBuilder, Transaction, VectorSpec, WriteBatch,
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
        c.define_private_method("native_put_vector", method!(db::w_put_vector::<$t>, 6))?;
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

/// The fiber bridge's native classes and the ops of each waiting call (#1574, ICR 0126).
/// Unix only: without them, `lib/pigeonhole/fiber.rb` keeps every call blocking.
#[cfg(unix)]
fn fiber_classes(ruby: &Ruby, m: RModule, native: RModule) -> Result<(), Error> {
    use fiber::{FiberBridge, FiberOp};
    m.singleton_class()?
        .define_private_method("native_fiber_bridge", function!(FiberBridge::new, 0))?;
    let c = class(ruby, native, "FiberBridgeCore")?;
    c.define_method("fd", method!(FiberBridge::fd, 0))?;
    c.define_method("drain", method!(FiberBridge::drain, 0))?;
    c.define_method("pending", method!(FiberBridge::pending, 0))?;
    let c = class(ruby, native, "FiberOp")?;
    c.define_method("id", method!(FiberOp::id, 0))?;
    c.define_method("done?", method!(FiberOp::is_done, 0))?;
    c.define_method("take", method!(FiberOp::take, 0))?;
    c.define_method("cancel", method!(FiberOp::cancel, 0))?;

    #[cfg(feature = "test-hooks")]
    {
        use fiber::hooks::Gate;
        let c = class(ruby, native, "Gate")?;
        c.define_singleton_method("new", function!(Gate::new, 0))?;
        c.define_method("op", method!(Gate::op, 1))?;
        c.define_method("open", method!(Gate::open, 0))?;
        c.define_method("counts", method!(Gate::counts, 0))?;
    }

    let c = m.const_get::<_, RClass>("Mutation")?;
    c.define_private_method("native_commit_op", method!(Mutation::commit_op, 1))?;
    c.define_private_method("native_commit_if_op", method!(Mutation::commit_if_op, 2))?;
    c.define_private_method(
        "native_commit_returning_op",
        method!(Mutation::commit_returning_op, 1),
    )?;
    c.define_private_method(
        "native_commit_if_returning_op",
        method!(Mutation::commit_if_returning_op, 2),
    )?;
    let c = m.const_get::<_, RClass>("Feed")?;
    c.define_private_method("native_next_op", method!(crate::classes::Feed::next_op, 1))?;
    let mem_mod = m.const_get::<_, RModule>("Memory")?;
    let c = mem_mod.const_get::<_, RClass>("Watch")?;
    c.define_private_method("native_next_op", method!(crate::classes::Watch::next_op, 1))?;
    let c = m.const_get::<_, RClass>("WriteBatch")?;
    c.define_private_method("native_commit_op", method!(WriteBatch::commit_op, 2))?;
    let c = m.const_get::<_, RClass>("Transaction")?;
    c.define_private_method("native_commit_op", method!(Transaction::commit_op, 2))?;
    Ok(())
}

/// Defines the `Pigeonhole` module, its classes and their native methods.
fn init(ruby: &Ruby) -> Result<(), Error> {
    let m = ruby.define_module("Pigeonhole")?;
    m.const_set("NATIVE_VERSION", env!("CARGO_PKG_VERSION"))?;
    let native = m.define_module("Native")?;
    error::register(ruby, m, native)?;

    m.singleton_class()?
        .define_private_method("native_open", function!(db::open, 13))?;
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
    c.define_private_method("native_incr_returning", method!(Table::incr_returning, 6))?;
    c.define_private_method("native_delete", method!(Table::delete, 5))?;
    c.define_private_method("native_multi_get", method!(Table::multi_get, 6))?;
    c.define_private_method("native_traverse", method!(Table::traverse, 11))?;
    c.define_private_method("native_pages", method!(Table::pages, 7))?;
    c.define_private_method("native_nearest", method!(Table::nearest, 11))?;
    c.define_private_method("native_scan_within", method!(Table::scan_within, 11))?;
    c.define_private_method("native_feed", method!(Table::feed, 6))?;
    c.define_private_method(
        "native_multi_get_within",
        method!(Table::multi_get_within, 8),
    )?;

    let c = class(ruby, m, "Mutation")?;
    c.define_private_method("native_put", method!(Mutation::put, 4))?;
    c.define_private_method("native_put_u64", method!(Mutation::put_u64, 4))?;
    c.define_private_method("native_incr", method!(Mutation::incr, 4))?;
    c.define_private_method("native_aggregate", method!(Mutation::aggregate, 4))?;
    c.define_private_method("native_put_vector", method!(Mutation::put_vector, 4))?;
    c.define_private_method("native_delete", method!(Mutation::delete, 3))?;
    c.define_private_method("native_durability", method!(Mutation::set_durability, 1))?;
    c.define_private_method("native_commit", method!(Mutation::commit, 0))?;
    c.define_private_method("native_commit_if", method!(Mutation::commit_if, 1))?;
    c.define_private_method(
        "native_return_previous",
        method!(Mutation::return_previous, 2),
    )?;
    c.define_private_method(
        "native_commit_returning",
        method!(Mutation::commit_returning, 0),
    )?;
    c.define_private_method(
        "native_return_current",
        method!(Mutation::return_current, 2),
    )?;
    c.define_private_method(
        "native_commit_if_returning",
        method!(Mutation::commit_if_returning, 1),
    )?;

    let c = class(ruby, m, "Incremented")?;
    c.define_method("seqno", method!(Incremented::seqno, 0))?;
    c.define_private_method("native_previous", method!(Incremented::previous, 2))?;
    c.define_private_method("native_current", method!(Incremented::current, 2))?;
    c.define_private_method("native_value", method!(Incremented::value, 3))?;
    c.define_method("to_h", method!(Incremented::to_h, 0))?;
    c.define_method("size", method!(Incremented::size, 0))?;
    c.define_method("inspect", method!(Incremented::inspect, 0))?;

    let c = class(ruby, m, "Seen")?;
    c.define_private_method("native_current", method!(Seen::current, 2))?;

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
    c.define_method("as_vector", method!(Cell::as_vector, 0))?;
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
    s.define_private_method("native_vector", function!(table::family_vector, 5))?;

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
    c.define_method("vector", method!(FamilyInfo::vector, 0))?;
    c.define_method("family", method!(FamilyInfo::family, 0))?;
    c.define_method("inspect", method!(FamilyInfo::inspect, 0))?;

    let c = class(ruby, m, "Condition")?;
    c.define_singleton_method("absent", function!(Condition::absent, 2))?;
    c.define_singleton_method("exists", function!(Condition::exists, 2))?;
    c.define_singleton_method("equals", function!(Condition::equals, 3))?;

    let c = class(ruby, m, "VectorSpec")?;
    c.define_singleton_method("new", function!(VectorSpec::new, 3))?;
    c.define_method("dim", method!(VectorSpec::dim, 0))?;
    c.define_method("element", method!(VectorSpec::element, 0))?;
    c.define_method("metric", method!(VectorSpec::metric, 0))?;
    c.define_method("==", method!(VectorSpec::eq, 1))?;
    c.define_method("eql?", method!(VectorSpec::eq, 1))?;
    c.define_method("hash", method!(VectorSpec::hash, 0))?;
    c.define_method("inspect", method!(VectorSpec::inspect, 0))?;

    /// Defines a position class and its shared methods; evaluates to the class.
    macro_rules! position {
        ($name:literal, $t:ty) => {{
            let c = class(ruby, m, $name)?;
            c.define_singleton_method("from_bytes", function!(<$t>::from_bytes, 1))?;
            c.define_method("to_bytes", method!(<$t>::to_bytes, 0))?;
            c.define_method("==", method!(<$t>::eq, 1))?;
            c.define_method("eql?", method!(<$t>::eq, 1))?;
            c.define_method("hash", method!(<$t>::hash, 0))?;
            c
        }};
    }
    position!("RowPosition", RowPosition);
    position!("ScanPosition", ScanPosition).define_method("row", method!(ScanPosition::row, 0))?;
    position!("KeysPosition", KeysPosition).define_method("key", method!(KeysPosition::key, 0))?;

    let c = class(ruby, m, "Feed")?;
    c.define_private_method("native_next", method!(Feed::next, 1))?;
    c.define_private_method("native_try_next", method!(Feed::try_next, 0))?;
    c.define_method("token", method!(Feed::token, 0))?;
    c.define_method("oldest_retained", method!(Feed::oldest_retained, 0))?;
    c.define_method("close", method!(Feed::close, 0))?;
    c.define_method("closed?", method!(Feed::is_closed, 0))?;

    let c = class(ruby, m, "Pages")?;
    c.define_method("position", method!(Pages::position, 0))?;
    c.define_private_method("native_next", method!(Pages::next_page, 0))?;

    let c = class(ruby, native, "ScanSpec")?;
    c.define_method("open", method!(ScanSpec::open, 1))?;
    let c = class(ruby, native, "ScanCursor")?;
    c.define_method("next_chunk", method!(ScanCursor::next_chunk, 0))?;
    c.define_method("close", method!(ScanCursor::close, 0))?;
    memory_classes(ruby, m)?;
    index_classes(ruby, m)?;
    #[cfg(unix)]
    fiber_classes(ruby, m, native)?;
    Ok(())
}

/// `Pigeonhole::Text` and `Pigeonhole::Vector`'s native classes (PR 3b);
/// `lib/pigeonhole/index.rb` adds the rest.
fn index_classes(ruby: &Ruby, m: RModule) -> Result<(), Error> {
    use classes::{
        Placed, Rebalancer, TextIndex, TextQuery, TextReadIndex, VectorBulkLoad, VectorIndex,
        VectorReadIndex,
    };
    let t = m.define_module("Text")?;

    let c = class(ruby, t, "Query")?;
    c.define_singleton_method("new", function!(TextQuery::new, 0))?;
    c.define_singleton_method("parse", function!(TextQuery::parse, 1))?;
    c.define_method("term", method!(TextQuery::term, 1))?;
    c.define_method("text", method!(TextQuery::text, 1))?;
    c.define_private_method("native_phrase", method!(TextQuery::phrase, 2))?;
    c.define_private_method("native_fuzzy", method!(TextQuery::fuzzy, 2))?;
    c.define_method("==", method!(TextQuery::eq, 1))?;
    c.define_method("inspect", method!(TextQuery::inspect, 0))?;

    let c = class(ruby, t, "Index")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_create", function!(TextIndex::create, 6))?;
    s.define_private_method("native_open", function!(TextIndex::open, 2))?;
    s.define_private_method("native_open_reader", function!(TextIndex::open_reader, 2))?;
    c.define_method("put", method!(TextIndex::put, 2))?;
    c.define_method("put_in", method!(TextIndex::put_in, 3))?;
    c.define_method("delete", method!(TextIndex::delete, 1))?;
    c.define_method("delete_in", method!(TextIndex::delete_in, 2))?;
    c.define_private_method("native_rebuild", method!(TextIndex::rebuild, 0))?;
    c.define_private_method("native_repair", method!(TextIndex::repair, 0))?;
    c.define_private_method("native_verify", method!(TextIndex::verify, 0))?;
    c.define_private_method("native_options", method!(TextIndex::options, 0))?;
    c.define_private_method("native_source", method!(TextIndex::source, 0))?;
    c.define_private_method("native_search", method!(TextIndex::search, 3))?;

    let c = class(ruby, t, "ReadIndex")?;
    c.define_private_method("native_verify", method!(TextReadIndex::verify, 0))?;
    c.define_private_method("native_options", method!(TextReadIndex::options, 0))?;
    c.define_private_method("native_source", method!(TextReadIndex::source, 0))?;
    c.define_private_method("native_search", method!(TextReadIndex::search, 3))?;

    let v = m.define_module("Vector")?;
    let c = class(ruby, v, "Index")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_create", function!(VectorIndex::create, 5))?;
    s.define_private_method("native_open", function!(VectorIndex::open, 3))?;
    s.define_private_method("native_open_reader", function!(VectorIndex::open_reader, 2))?;
    c.define_method("spec", method!(VectorIndex::spec, 0))?;
    c.define_method("rebalance", method!(VectorIndex::rebalance, 0))?;
    c.define_method("max_list_len", method!(VectorIndex::max_list_len, 0))?;
    c.define_method("min_list_len", method!(VectorIndex::min_list_len, 0))?;
    c.define_method("insert", method!(VectorIndex::insert, 2))?;
    c.define_method("delete", method!(VectorIndex::delete, 1))?;
    c.define_method("insert_in", method!(VectorIndex::insert_in, 4))?;
    c.define_method("delete_in", method!(VectorIndex::delete_in, 3))?;
    c.define_method("after_commit", method!(VectorIndex::after_commit, 1))?;
    c.define_private_method("native_verify", method!(VectorIndex::verify, 0))?;
    c.define_private_method("native_search", method!(VectorIndex::search, 3))?;
    c.define_private_method("native_bulk_load", method!(VectorIndex::bulk_load, 1))?;

    let c = class(ruby, v, "ReadIndex")?;
    c.define_method("spec", method!(VectorReadIndex::spec, 0))?;
    c.define_private_method("native_search", method!(VectorReadIndex::search, 3))?;

    let c = class(ruby, v, "Placed")?;
    c.define_singleton_method("new", function!(Placed::new, 0))?;

    let c = class(ruby, v, "Rebalancer")?;
    c.define_method("run_once", method!(Rebalancer::run_once, 1))?;
    c.define_method("wait", method!(Rebalancer::wait, 1))?;
    c.define_method("closed?", method!(Rebalancer::is_closed, 0))?;

    let c = class(ruby, v, "BulkLoad")?;
    c.define_method("push", method!(VectorBulkLoad::push, 1))?;
    c.define_private_method("native_finish", method!(VectorBulkLoad::finish, 0))?;
    Ok(())
}

/// `Pigeonhole::Memory`'s native classes (PR 3a); `lib/pigeonhole/memory.rb` adds the rest.
fn memory_classes(ruby: &Ruby, m: RModule) -> Result<(), Error> {
    use classes::{
        ConsolidatePolicy, EmbeddingModel, Expand, ForgetPolicy, IvfOptions, Memory, MemoryId,
        NearDedup, ReadStore, Store, Weights,
    };
    let mm = m.define_module("Memory")?;
    // `ConsolidateError`'s code number (memory.rb builds it).
    mm.const_set(
        "MERGE_FAILED_CODE_NUM",
        pigeonhole::ErrorCode::MergeFailed as u32,
    )?;

    let c = class(ruby, mm, "MemoryId")?;
    c.define_singleton_method("new", function!(MemoryId::new, 1))?;
    c.define_singleton_method("from_bytes", function!(MemoryId::from_bytes, 1))?;
    c.define_method("to_bytes", method!(MemoryId::to_bytes, 0))?;
    c.define_method("namespace", method!(MemoryId::namespace, 0))?;
    c.define_method("created_us", method!(MemoryId::created_us, 0))?;
    c.define_method("to_s", method!(MemoryId::to_s, 0))?;
    c.define_method("inspect", method!(MemoryId::inspect, 0))?;
    c.define_method("==", method!(MemoryId::eq, 1))?;
    c.define_method("eql?", method!(MemoryId::eq, 1))?;
    c.define_method("hash", method!(MemoryId::hash, 0))?;
    c.define_method("<=>", method!(MemoryId::cmp, 1))?;

    let c = class(ruby, mm, "EmbeddingModel")?;
    c.define_singleton_method("new", function!(EmbeddingModel::new, 3))?;
    c.define_method("id", method!(EmbeddingModel::id, 0))?;
    c.define_method("dim", method!(EmbeddingModel::dim, 0))?;
    c.define_method("element", method!(EmbeddingModel::element, 0))?;
    c.define_method("==", method!(EmbeddingModel::eq, 1))?;
    c.define_method("eql?", method!(EmbeddingModel::eq, 1))?;
    c.define_method("hash", method!(EmbeddingModel::hash, 0))?;
    c.define_method("inspect", method!(EmbeddingModel::inspect, 0))?;

    let c = class(ruby, mm, "Weights")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_new", function!(Weights::native_new, 7))?;
    c.define_method("to_a", method!(Weights::to_a, 0))?;
    c.define_method("==", method!(Weights::eq, 1))?;
    c.define_method("eql?", method!(Weights::eq, 1))?;
    c.define_method("hash", method!(Weights::hash, 0))?;

    let c = class(ruby, mm, "Expand")?;
    let s = c.singleton_class()?;
    c.const_set("MAX_HOPS", pigeonhole_memory::Expand::MAX_HOPS)?;
    s.define_private_method("native_new", function!(Expand::native_new, 2))?;
    c.define_method("hops", method!(Expand::hops, 0))?;
    c.define_method("max", method!(Expand::max, 0))?;
    c.define_method("==", method!(Expand::eq, 1))?;
    c.define_method("eql?", method!(Expand::eq, 1))?;
    c.define_method("hash", method!(Expand::hash, 0))?;

    let c = class(ruby, mm, "ForgetPolicy")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_new", function!(ForgetPolicy::native_new, 6))?;
    c.define_method("min_retention", method!(ForgetPolicy::min_retention, 0))?;
    c.define_method("max_age", method!(ForgetPolicy::max_age, 0))?;
    c.define_method("half_life", method!(ForgetPolicy::half_life, 0))?;
    c.define_method("use_weight", method!(ForgetPolicy::use_weight, 0))?;
    c.define_method("max_per_pass", method!(ForgetPolicy::max_per_pass, 0))?;
    c.define_method("interval", method!(ForgetPolicy::interval, 0))?;
    c.define_method("==", method!(ForgetPolicy::eq, 1))?;
    c.define_method("inspect", method!(ForgetPolicy::inspect, 0))?;

    let c = class(ruby, mm, "ConsolidatePolicy")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_new", function!(ConsolidatePolicy::native_new, 6))?;
    c.define_method("min_age", method!(ConsolidatePolicy::min_age, 0))?;
    c.define_method(
        "group_similarity",
        method!(ConsolidatePolicy::group_similarity, 0),
    )?;
    c.define_method("min_group", method!(ConsolidatePolicy::min_group, 0))?;
    c.define_method("max_group", method!(ConsolidatePolicy::max_group, 0))?;
    c.define_method("max_groups", method!(ConsolidatePolicy::max_groups, 0))?;
    c.define_method("sources", method!(ConsolidatePolicy::sources, 0))?;
    c.define_method("==", method!(ConsolidatePolicy::eq, 1))?;
    c.define_method("inspect", method!(ConsolidatePolicy::inspect, 0))?;

    let c = class(ruby, mm, "NearDedup")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_new", function!(NearDedup::native_new, 2))?;
    c.define_method("similarity", method!(NearDedup::similarity, 0))?;
    c.define_method("policy", method!(NearDedup::policy, 0))?;
    c.define_method("==", method!(NearDedup::eq, 1))?;
    c.define_method("inspect", method!(NearDedup::inspect, 0))?;

    let c = class(ruby, mm, "Memory")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_new", function!(Memory::native_new, 8))?;
    c.define_method("text", method!(Memory::text, 0))?;
    c.define_method("inspect", method!(Memory::inspect, 0))?;

    let c = class(ruby, mm, "IvfOptions")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_new", function!(IvfOptions::native_new, 3))?;
    c.define_method(
        "min_namespace_len",
        method!(IvfOptions::min_namespace_len, 0),
    )?;
    c.define_method("nprobe", method!(IvfOptions::nprobe, 0))?;
    c.define_method("target_list_len", method!(IvfOptions::target_list_len, 0))?;
    c.define_method("==", method!(IvfOptions::eq, 1))?;
    c.define_method("inspect", method!(IvfOptions::inspect, 0))?;

    let c = class(ruby, mm, "Store")?;
    let s = c.singleton_class()?;
    s.define_private_method("native_create", function!(Store::create, 8))?;
    s.define_private_method("native_open", function!(Store::open, 3))?;
    s.define_private_method("native_open_reader", function!(Store::open_reader, 3))?;
    c.define_method("close", method!(Store::close, 0))?;
    c.define_method("closed?", method!(Store::is_closed, 0))?;
    c.define_method("embedding_model", method!(Store::embedding_model, 0))?;
    c.define_method("normalization", method!(Store::normalization, 0))?;
    c.define_method("ivf", method!(Store::ivf, 0))?;
    c.define_private_method("native_remember", method!(Store::remember, 9))?;
    c.define_private_method("native_remember_in", method!(Store::remember_in, 10))?;
    c.define_method("delete", method!(Store::delete, 1))?;
    c.define_method("delete_in", method!(Store::delete_in, 2))?;
    c.define_private_method("native_link", method!(Store::link, 4))?;
    c.define_method("supersede", method!(Store::supersede, 2))?;
    c.define_private_method("native_mark_used", method!(Store::mark_used, 1))?;
    c.define_private_method("native_get", method!(Store::get, 1))?;
    c.define_private_method("native_verify", method!(Store::verify, 0))?;
    c.define_private_method("native_recall", method!(Store::recall, 1))?;
    c.define_private_method("native_forget", method!(Store::forget, 3))?;
    c.define_method("forget_in", method!(Store::forget_in, 3))?;
    c.define_method("forget_policy", method!(Store::forget_policy, 0))?;
    c.define_method("forget_policy=", method!(Store::set_forget_policy, 1))?;
    c.define_method("near_dedup", method!(Store::near_dedup, 0))?;
    c.define_method("near_dedup=", method!(Store::set_near_dedup, 1))?;
    c.define_private_method("native_near_duplicate", method!(Store::near_duplicate, 3))?;
    c.define_private_method("native_consolidate", method!(Store::consolidate, 5))?;
    c.define_private_method("native_watch", method!(Store::watch, 1))?;
    c.define_private_method(
        "native_consolidate_loop",
        method!(Store::consolidate_loop, 4),
    )?;

    let c = class(ruby, mm, "ReadStore")?;
    c.define_method("close", method!(ReadStore::close, 0))?;
    c.define_method("closed?", method!(ReadStore::is_closed, 0))?;
    c.define_method("embedding_model", method!(ReadStore::embedding_model, 0))?;
    c.define_method("normalization", method!(ReadStore::normalization, 0))?;
    c.define_method("ivf", method!(ReadStore::ivf, 0))?;
    c.define_private_method("native_get", method!(ReadStore::get, 1))?;
    c.define_private_method("native_verify", method!(ReadStore::verify, 0))?;
    c.define_private_method("native_recall", method!(ReadStore::recall, 1))?;
    c.define_private_method("native_watch", method!(ReadStore::watch, 1))?;

    let c = class(ruby, mm, "Watch")?;
    c.define_private_method("native_next", method!(classes::Watch::next, 1))?;
    c.define_private_method("native_try_next", method!(classes::Watch::try_next, 0))?;
    c.define_method("token", method!(classes::Watch::token, 0))?;
    c.define_method(
        "oldest_retained",
        method!(classes::Watch::oldest_retained, 0),
    )?;
    c.define_method("close", method!(classes::Watch::close, 0))?;
    c.define_method("closed?", method!(classes::Watch::is_closed, 0))?;

    let c = class(ruby, mm, "ConsolidateLoop")?;
    c.define_private_method(
        "native_run_once",
        method!(classes::ConsolidateLoop::run_once, 2),
    )?;
    c.define_method("next_due", method!(classes::ConsolidateLoop::next_due, 0))?;
    c.define_method("close", method!(classes::ConsolidateLoop::close, 0))?;
    Ok(())
}
