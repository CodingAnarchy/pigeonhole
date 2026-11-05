//! Python bindings for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) (D307), over
//! PyO3, published to PyPI as `pigeonhole-db` and imported as `pigeonhole`.
//!
//! The extension module is `pigeonhole._native`; the `pigeonhole` package re-exports it and
//! carries the typed stubs (`__init__.pyi`), which are the Python API's snapshot (ICR 0102,
//! checked by `mypy.stubtest`). Every engine call runs with the GIL released, its arguments
//! converted before and its results after.
#![forbid(unsafe_code)]

use pyo3::prelude::*;

mod aio;
mod arrow;
mod budget;
mod convert;
mod db;
mod error;
mod feed;
mod memory;
mod read;
mod table;
mod vector;
mod walk;

/// The native module.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(db::open, m)?)?;
    m.add_function(wrap_pyfunction!(db::open_reader, m)?)?;
    m.add_class::<db::Database>()?;
    m.add_class::<db::Reader>()?;
    m.add_class::<db::WriteBatch>()?;
    m.add_class::<db::Transaction>()?;
    m.add_class::<table::Table>()?;
    m.add_class::<table::TableBuilder>()?;
    m.add_class::<table::Family>()?;
    m.add_class::<table::Condition>()?;
    m.add_class::<table::FamilyInfo>()?;
    m.add_class::<table::Mutation>()?;
    m.add_class::<table::Incremented>()?;
    // ICR 0113: `Returned`, the Rust struct's name since 1.2, is the same class.
    m.add("Returned", m.getattr("Incremented")?)?;
    m.add_class::<read::Cell>()?;
    m.add_class::<read::Row>()?;
    m.add_class::<read::Snapshot>()?;
    m.add_class::<read::ScanIter>()?;
    m.add_class::<arrow::ArrowStream>()?;
    m.add_class::<aio::Retry>()?;
    #[cfg(feature = "test-hooks")]
    {
        m.add_class::<aio::hooks::Gate>()?;
        m.add_function(wrap_pyfunction!(aio::hooks::transaction_retries, m)?)?;
    }
    m.add_class::<vector::VectorSpec>()?;
    m.add_class::<walk::Hop>()?;
    m.add_class::<walk::Traversal>()?;
    m.add_class::<walk::RowPosition>()?;
    m.add_class::<walk::Pages>()?;
    m.add_class::<walk::Neighbour>()?;
    m.add_class::<budget::ScanPosition>()?;
    m.add_class::<budget::KeysPosition>()?;
    m.add_class::<budget::ScanPage>()?;
    m.add_class::<budget::KeysPage>()?;
    m.add_class::<budget::NearestPage>()?;
    m.add_class::<feed::Feed>()?;
    m.add_class::<feed::FeedIter>()?;
    m.add_class::<feed::Commit>()?;
    m.add_class::<feed::Change>()?;
    error::register(m)?;
    // `pigeonhole.memory` (D311 T6): a native submodule, re-exported by `memory.py`. Its
    // name starts with `_` so the package's `from pigeonhole._native import *` leaves it
    // out, and `pigeonhole.memory` is the Python module.
    let memory = PyModule::new(m.py(), "_memory")?;
    memory::register(&memory)?;
    m.add_submodule(&memory)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

/// Applies an optional builder setting: `b.maybe(opt, |b, v| b.method(v))` is `b` with
/// `method(v)` when `opt` is `Some(v)`, and `b` unchanged otherwise. Every binding method
/// that maps optional keyword arguments onto a Rust builder chains these, one per argument.
pub(crate) trait Maybe: Sized {
    fn maybe<T>(self, value: Option<T>, set: impl FnOnce(Self, T) -> Self) -> Self {
        match value {
            Some(v) => set(self, v),
            None => self,
        }
    }
}

impl<B> Maybe for B {}
